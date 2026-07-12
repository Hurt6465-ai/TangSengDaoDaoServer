package redisx

import (
	"errors"
	"fmt"
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
	created := &Client{client: rd.NewClient(&rd.Options{
		Addr:       addr,
		Password:   ctx.GetConfig().DB.RedisPass,
		MaxRetries: 3,
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
