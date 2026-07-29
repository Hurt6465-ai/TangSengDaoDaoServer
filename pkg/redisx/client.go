package redisx

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	rd "github.com/go-redis/redis"
)

// Client provides Redis commands required by the partner modules without
// modifying TangSengDaoDaoServerLib's limited redis.Conn wrapper.
type Client struct {
	client *rd.Client
}

var clients sync.Map // map[*config.Context]*Client

func redisEnvInt(key string, fallback, minValue, maxValue int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < minValue || n > maxValue {
		return fallback
	}
	return n
}

// FromContext returns one shared advanced Redis client per app context.
func FromContext(ctx *config.Context) *Client {
	if ctx == nil || ctx.GetConfig() == nil {
		return nil
	}
	if cached, ok := clients.Load(ctx); ok {
		return cached.(*Client)
	}
	addr := strings.TrimSpace(ctx.GetConfig().DB.RedisAddr)
	if addr == "" {
		return nil
	}
	poolSize := redisEnvInt("TS_DD_PARTNER_REDIS_POOL_SIZE", 64, 8, 512)
	minIdle := redisEnvInt("TS_DD_PARTNER_REDIS_MIN_IDLE", 8, 0, 128)
	if minIdle > poolSize {
		minIdle = poolSize
	}
	created := &Client{client: rd.NewClient(&rd.Options{
		Addr:         addr,
		Password:     ctx.GetConfig().DB.RedisPass,
		MaxRetries:   1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolTimeout:  time.Second,
		PoolSize:     poolSize,
		MinIdleConns: minIdle,
	})}
	actual, loaded := clients.LoadOrStore(ctx, created)
	if loaded {
		_ = created.client.Close()
		return actual.(*Client)
	}
	return created
}

func (c *Client) available() error {
	if c == nil || c.client == nil {
		return errors.New("Redis高级客户端不可用")
	}
	return nil
}

func (c *Client) SetNX(key string, value interface{}, expire time.Duration) (bool, error) {
	if err := c.available(); err != nil {
		return false, err
	}
	return c.client.SetNX(key, value, expire).Result()
}

func (c *Client) Expire(key string, expire time.Duration) error {
	if err := c.available(); err != nil {
		return err
	}
	return c.client.Expire(key, expire).Err()
}

func (c *Client) CompareAndDelete(key, expected string) (bool, error) {
	if err := c.available(); err != nil {
		return false, err
	}
	const script = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	result, err := c.client.Eval(script, []string{key}, expected).Int64()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

func (c *Client) EvalInt(script string, keys []string, args ...interface{}) (int64, error) {
	if err := c.available(); err != nil {
		return 0, err
	}
	return c.client.Eval(script, keys, args...).Int64()
}

// TouchPresence updates foreground and last-active scores and acquires the
// throttled persistence lock in one Redis round trip. This keeps thousands of
// heartbeat requests from issuing five separate commands each.
func (c *Client) TouchPresence(foregroundKey, lastActiveKey, writeLockKey, uid string, foregroundExpireAt, lastActiveAt int64, writeTTL time.Duration, token string) (bool, error) {
	if err := c.available(); err != nil {
		return false, err
	}
	if strings.TrimSpace(uid) == "" || strings.TrimSpace(writeLockKey) == "" {
		return false, errors.New("Redis在线状态参数为空")
	}
	ttlMS := writeTTL.Milliseconds()
	if ttlMS <= 0 {
		ttlMS = 1
	}
	const script = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[3])
redis.call('EXPIRE', KEYS[1], 86400)
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
redis.call('EXPIRE', KEYS[2], 864000)
local locked = redis.call('SET', KEYS[3], ARGV[4], 'PX', ARGV[5], 'NX')
if locked then return 1 end
return 0`
	result, err := c.client.Eval(script, []string{foregroundKey, lastActiveKey, writeLockKey}, foregroundExpireAt, lastActiveAt, uid, token, ttlMS).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (c *Client) HMGetMap(key string, fields []string) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	if len(fields) == 0 {
		return out, nil
	}
	if err := c.available(); err != nil {
		return nil, err
	}
	values, err := c.client.HMGet(key, fields...).Result()
	if err != nil && err != rd.Nil {
		return nil, err
	}
	for i, value := range values {
		if value == nil || i >= len(fields) {
			continue
		}
		switch v := value.(type) {
		case string:
			out[fields[i]] = v
		case []byte:
			out[fields[i]] = string(v)
		default:
			out[fields[i]] = fmt.Sprint(v)
		}
	}
	return out, nil
}

func (c *Client) ZCard(key string) (int64, error) {
	if err := c.available(); err != nil {
		return 0, err
	}
	return c.client.ZCard(key).Result()
}

func (c *Client) ZRevRangeByScore(key string, zrangeBy rd.ZRangeBy) ([]string, error) {
	if err := c.available(); err != nil {
		return nil, err
	}
	values, err := c.client.ZRevRangeByScore(key, zrangeBy).Result()
	if err == rd.Nil {
		return nil, nil
	}
	return values, err
}

func (c *Client) ZScores(key string, members []string) (map[string]float64, error) {
	out := make(map[string]float64, len(members))
	if len(members) == 0 {
		return out, nil
	}
	if err := c.available(); err != nil {
		return nil, err
	}
	pipe := c.client.Pipeline()
	cmds := make([]*rd.FloatCmd, 0, len(members))
	valid := make([]string, 0, len(members))
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" {
			continue
		}
		cmds = append(cmds, pipe.ZScore(key, member))
		valid = append(valid, member)
	}
	_, err := pipe.Exec()
	if err != nil && err != rd.Nil {
		return nil, err
	}
	for i, cmd := range cmds {
		score, scoreErr := cmd.Result()
		if scoreErr == nil && i < len(valid) {
			out[valid[i]] = score
		}
	}
	return out, nil
}

func (c *Client) ZRemRangeByScore(key, min, max string) (int64, error) {
	if err := c.available(); err != nil {
		return 0, err
	}
	return c.client.ZRemRangeByScore(key, min, max).Result()
}

func (c *Client) ZRemFromKeys(keys []string, member string) error {
	if len(keys) == 0 || strings.TrimSpace(member) == "" {
		return nil
	}
	if err := c.available(); err != nil {
		return err
	}
	pipe := c.client.Pipeline()
	for _, key := range keys {
		if strings.TrimSpace(key) != "" {
			pipe.ZRem(key, member)
		}
	}
	_, err := pipe.Exec()
	return err
}

func zMembers(scoremember ...interface{}) ([]rd.Z, error) {
	if len(scoremember)%2 != 0 {
		return nil, errors.New("Redis ZADD 参数必须为 score/member 成对出现")
	}
	members := make([]rd.Z, 0, len(scoremember)/2)
	for i := 0; i < len(scoremember); i += 2 {
		var score float64
		switch v := scoremember[i].(type) {
		case float64:
			score = v
		case float32:
			score = float64(v)
		case int:
			score = float64(v)
		case int64:
			score = float64(v)
		case string:
			parsed, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, err
			}
			score = parsed
		default:
			return nil, fmt.Errorf("Redis ZADD 不支持的 score 类型 %T", scoremember[i])
		}
		members = append(members, rd.Z{Score: score, Member: scoremember[i+1]})
	}
	return members, nil
}

// ZAdd avoids the multi-member indexing bug in the ServerLib wrapper version
// used by this project.
func (c *Client) ZAdd(key string, scoremember ...interface{}) error {
	if err := c.available(); err != nil {
		return err
	}
	members, err := zMembers(scoremember...)
	if err != nil || len(members) == 0 {
		return err
	}
	return c.client.ZAdd(key, members...).Err()
}

// ZAddAndRegister uses MULTI/EXEC so the pool key registry and sorted-set data
// are committed together.
func (c *Client) ZAddAndRegister(registryKey, zsetKey string, ttl time.Duration, scoremember ...interface{}) error {
	if err := c.available(); err != nil {
		return err
	}
	members, err := zMembers(scoremember...)
	if err != nil {
		return err
	}
	if len(members) == 0 {
		return nil
	}
	pipe := c.client.TxPipeline()
	pipe.SAdd(registryKey, zsetKey)
	pipe.ZAdd(zsetKey, members...)
	if ttl > 0 {
		pipe.Expire(registryKey, ttl)
		pipe.Expire(zsetKey, ttl)
	}
	_, err = pipe.Exec()
	return err
}

func (c *Client) RPush(key string, values ...interface{}) (int64, error) {
	if err := c.available(); err != nil {
		return 0, err
	}
	return c.client.RPush(key, values...).Result()
}

// PushBounded appends values to a Redis list and trims the oldest entries in the
// same Lua script. It returns the retained size. Call PushBoundedDetailed when
// the caller must account for every trimmed entry.
func (c *Client) PushBounded(key string, maxLen int64, values ...interface{}) (int64, error) {
	size, _, err := c.PushBoundedDetailed(key, maxLen, values...)
	return size, err
}

// PushBoundedDetailed also returns the exact number of entries trimmed by this
// append. This makes overload loss observable instead of treating "size == max"
// as proof that an entry was dropped.
func (c *Client) PushBoundedDetailed(key string, maxLen int64, values ...interface{}) (int64, int64, error) {
	if err := c.available(); err != nil {
		return 0, 0, err
	}
	if strings.TrimSpace(key) == "" || len(values) == 0 {
		return 0, 0, nil
	}
	if maxLen <= 0 {
		maxLen = 1
	}
	const script = `
for i = 1, #ARGV - 1 do
  redis.call('RPUSH', KEYS[1], ARGV[i])
end
local maxLen = tonumber(ARGV[#ARGV])
local size = redis.call('LLEN', KEYS[1])
local dropped = 0
if size > maxLen then
  dropped = size - maxLen
  redis.call('LTRIM', KEYS[1], dropped, -1)
  size = maxLen
end
return {size, dropped}`
	args := make([]interface{}, 0, len(values)+1)
	args = append(args, values...)
	args = append(args, maxLen)
	raw, err := c.client.Eval(script, []string{key}, args...).Result()
	if err != nil {
		return 0, 0, err
	}
	items, ok := raw.([]interface{})
	if !ok || len(items) != 2 {
		return 0, 0, fmt.Errorf("Redis有界队列返回类型异常: %T", raw)
	}
	size, err := redisResultInt64(items[0])
	if err != nil {
		return 0, 0, err
	}
	dropped, err := redisResultInt64(items[1])
	if err != nil {
		return 0, 0, err
	}
	return size, dropped, nil
}

func redisResultInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("Redis整数返回类型异常: %T", value)
	}
}

// PopBatch removes and returns up to limit oldest list entries atomically.
func (c *Client) PopBatch(key string, limit int) ([]string, error) {
	if err := c.available(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(key) == "" || limit <= 0 {
		return []string{}, nil
	}
	if limit > 1000 {
		limit = 1000
	}
	const script = `
local out = {}
local limit = tonumber(ARGV[1])
for i = 1, limit do
  local value = redis.call('LPOP', KEYS[1])
  if not value then break end
  out[#out + 1] = value
end
return out`
	result, err := c.client.Eval(script, []string{key}, limit).Result()
	if err == rd.Nil {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}
	rawValues, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Redis批量弹出返回类型异常: %T", result)
	}
	values := make([]string, 0, len(rawValues))
	for _, raw := range rawValues {
		switch value := raw.(type) {
		case string:
			values = append(values, value)
		case []byte:
			values = append(values, string(value))
		default:
			values = append(values, fmt.Sprint(value))
		}
	}
	return values, nil
}

func (c *Client) LLen(key string) (int64, error) {
	if err := c.available(); err != nil {
		return 0, err
	}
	return c.client.LLen(key).Result()
}
