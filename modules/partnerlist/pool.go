package partnerlist

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	rd "github.com/go-redis/redis"
)

type poolService struct {
	ctx *config.Context
	db  *db
}
type poolMeta struct {
	Version       string `json:"version"`
	BuiltAt       int64  `json:"built_at"`
	EligibleCount int    `json:"eligible_count"`
}

func newPoolService(ctx *config.Context, db *db) *poolService { return &poolService{ctx: ctx, db: db} }

func (p *poolService) eligibleCount() int {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil {
		return 0
	}
	version := p.currentVersion()
	if version == "" {
		return 0
	}
	if count, err := p.ctx.GetRedisConn().ZCard(poolEligibleKey(version)); err == nil {
		return int(count)
	}
	raw, err := p.ctx.GetRedisConn().GetString(poolMetaKey(version))
	if err != nil || raw == "" {
		return 0
	}
	var meta poolMeta
	if json.Unmarshal([]byte(raw), &meta) != nil {
		return 0
	}
	return meta.EligibleCount
}

func (p *poolService) currentVersion() string {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil {
		return ""
	}
	v, _ := p.ctx.GetRedisConn().GetString(poolCurrentVersionKey)
	return strings.TrimSpace(v)
}
func (p *poolService) ensure() (string, error) {
	if v := p.currentVersion(); v != "" {
		return v, nil
	}
	if err := p.rebuild(); err != nil {
		return "", err
	}
	for i := 0; i < 20; i++ {
		if v := p.currentVersion(); v != "" {
			return v, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("列表语伴共享池尚未准备完成")
}

func (p *poolService) rebuild() error {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil {
		return fmt.Errorf("Redis不可用，无法构建列表语伴共享池")
	}
	conn := p.ctx.GetRedisConn()
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	ok, err := conn.SetNX(poolBuildLockKey, token, 2*time.Hour)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	defer conn.CompareAndDelete(poolBuildLockKey, token)
	started := time.Now()
	nowMS := started.UnixMilli()
	cutoff := started.Add(-warmWindow).UnixMilli()
	version := started.In(businessLocation).Format("20060102150405") + "-" + strconv.FormatInt(started.UnixNano()%100000, 10)
	if err = conn.SetAndExpire(poolBuildingVersionKey, version, 2*time.Hour); err != nil {
		return err
	}
	defer conn.CompareAndDelete(poolBuildingVersionKey, version)
	afterUID := ""
	total := 0
	for {
		rows, err := p.db.eligibleProfilesBatch(afterUID, PoolScanBatchSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		ids := make([]string, 0, len(rows))
		for _, r := range rows {
			if r != nil && r.UID != "" {
				ids = append(ids, r.UID)
			}
		}
		latest := p.lastActiveScores(ids)
		groups := map[string][]interface{}{}
		for _, r := range rows {
			if r == nil || r.UID == "" {
				continue
			}
			afterUID = r.UID
			if a := latest[r.UID]; a > r.LastActiveAt {
				r.LastActiveAt = a
			}
			if r.LastActiveAt < cutoff {
				continue
			}
			addProfileGroups(groups, version, r, nowMS)
			total++
		}
		for key, pairs := range groups {
			if len(pairs) == 0 {
				continue
			}
			if err = conn.ZAdd(key, pairs...); err != nil {
				return err
			}
			if err = conn.SAdd(poolKeysKey(version), key); err != nil {
				return err
			}
			_ = conn.Expire(key, poolVersionTTL)
		}
		_ = conn.Expire(poolKeysKey(version), poolVersionTTL)
		if len(rows) < PoolScanBatchSize {
			break
		}
	}
	// Keyset replay with overlap; idempotent updates make the overlap safe.
	cursorMS := started.Add(-10 * time.Second).UnixMilli()
	cursorUID := ""
	for {
		rows, err := p.db.changedProfilesAfter(cursorMS, cursorUID, 1000)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if row == nil {
				continue
			}
			if a := p.lastActiveScores([]string{row.UID})[row.UID]; a > row.LastActiveAt {
				row.LastActiveAt = a
			}
			if err = p.applyChangedProfileE(version, row); err != nil {
				return err
			}
			cursorMS = row.UpdatedAtMS
			cursorUID = row.UID
		}
		if len(rows) < 1000 {
			break
		}
	}
	meta, _ := json.Marshal(poolMeta{Version: version, BuiltAt: nowMS, EligibleCount: total})
	if err = conn.SetAndExpire(poolMetaKey(version), string(meta), poolVersionTTL); err != nil {
		return err
	}
	if err = conn.Set(poolCurrentVersionKey, version); err != nil {
		return err
	}
	return nil
}

func addProfileGroups(groups map[string][]interface{}, version string, p *poolProfile, nowMS int64) {
	score := float64(p.LastActiveAt)
	addZPair(groups, poolEligibleKey(version), score, p.UID)
	for _, l := range p.nativeLanguages() {
		addZPair(groups, poolNativeKey(version, l), score, p.UID)
	}
	for _, l := range p.learningLanguages() {
		addZPair(groups, poolLearningKey(version, l), score, p.UID)
	}
	if p.ProfileCompletedAtMS > 0 && nowMS-p.ProfileCompletedAtMS <= int64(newcomerWindow/time.Millisecond) {
		addZPair(groups, poolNewKey(version), float64(p.ProfileCompletedAtMS), p.UID)
	}
}
func addZPair(groups map[string][]interface{}, key string, score float64, member string) {
	groups[key] = append(groups[key], score, member)
}

func (p *poolService) versionKeys(version string) ([]string, error) {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || version == "" {
		return nil, nil
	}
	keys, err := p.ctx.GetRedisConn().SMembers(poolKeysKey(version))
	if err != nil {
		return nil, err
	}
	return uniqueIDs(keys, 0), nil
}
func (p *poolService) removeUser(version, uid string) error {
	if version == "" || uid == "" {
		return nil
	}
	keys, err := p.versionKeys(version)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	return p.ctx.GetRedisConn().ZRemFromKeys(keys, uid)
}
func (p *poolService) addProfileToVersion(version string, profile *poolProfile) error {
	if profile == nil || profile.UID == "" || version == "" {
		return nil
	}
	conn := p.ctx.GetRedisConn()
	groups := map[string][]interface{}{}
	addProfileGroups(groups, version, profile, time.Now().UnixMilli())
	for key, pairs := range groups {
		if err := conn.ZAdd(key, pairs...); err != nil {
			return err
		}
		if err := conn.SAdd(poolKeysKey(version), key); err != nil {
			return err
		}
		_ = conn.Expire(key, poolVersionTTL)
	}
	_ = conn.Expire(poolKeysKey(version), poolVersionTTL)
	return nil
}
func (p *poolService) activeVersions() []string {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil {
		return nil
	}
	versions := []string{p.currentVersion()}
	if building, err := p.ctx.GetRedisConn().GetString(poolBuildingVersionKey); err == nil {
		versions = append(versions, strings.TrimSpace(building))
	}
	return uniqueIDs(versions, 0)
}

func (p *poolService) syncUser(uid string, activeAtMS int64) { _ = p.syncUserE(uid, activeAtMS) }
func (p *poolService) syncUserE(uid string, activeAtMS int64) error {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || uid == "" {
		return nil
	}
	versions := p.activeVersions()
	if len(versions) == 0 {
		return nil
	}
	profile, profileErr := p.db.poolProfile(uid)
	for _, version := range versions {
		if err := p.removeUser(version, uid); err != nil {
			return err
		}
	}
	if profileErr != nil || profile == nil {
		return profileErr
	}
	if activeAtMS > profile.LastActiveAt {
		profile.LastActiveAt = activeAtMS
	}
	if profile.LastActiveAt < time.Now().Add(-warmWindow).UnixMilli() {
		return nil
	}
	for _, version := range versions {
		if err := p.addProfileToVersion(version, profile); err != nil {
			return err
		}
	}
	return nil
}
func (p *poolService) applyChangedProfile(version string, row *changedPoolProfile) {
	_ = p.applyChangedProfileE(version, row)
}
func (p *poolService) applyChangedProfileE(version string, row *changedPoolProfile) error {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || version == "" || row == nil || row.UID == "" {
		return nil
	}
	if err := p.removeUser(version, row.UID); err != nil {
		return err
	}
	if a := p.lastActiveScores([]string{row.UID})[row.UID]; a > row.LastActiveAt {
		row.LastActiveAt = a
	}
	if row.Eligible != 1 || row.LastActiveAt < time.Now().Add(-warmWindow).UnixMilli() || len(row.nativeLanguages()) == 0 || len(row.learningLanguages()) == 0 {
		return nil
	}
	return p.addProfileToVersion(version, &poolProfile{UID: row.UID, NativeLanguagesRaw: row.NativeLanguagesRaw, LearningLanguagesRaw: row.LearningLanguagesRaw, LastActiveAt: row.LastActiveAt, ProfileCompletedAtMS: row.ProfileCompletedAtMS, ProfileScore: row.ProfileScore, Intro: row.Intro, TagsRaw: row.TagsRaw, CountryCode: row.CountryCode, Birthday: row.Birthday})
}

func (p *poolService) candidateUIDs(viewer *viewerProfile, includeWarm bool) ([]string, string, error) {
	version, err := p.ensure()
	if err != nil {
		return nil, "", err
	}
	if viewer == nil {
		return []string{}, version, nil
	}
	now := time.Now()
	minAt := now.Add(-hotWindow).UnixMilli()
	maxAt := now.UnixMilli() + int64(time.Minute/time.Millisecond)
	if includeWarm {
		minAt = now.Add(-warmWindow).UnixMilli()
		maxAt = now.Add(-hotWindow).UnixMilli()
	}
	keys := []string{}
	for _, l := range viewer.LearningLanguages {
		keys = append(keys, poolNativeKey(version, l))
	}
	for _, l := range viewer.NativeLanguages {
		keys = append(keys, poolLearningKey(version, l))
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, 512)
	for _, key := range uniqueIDs(keys, 0) {
		vals, e := p.ctx.GetRedisConn().ZRevRangeByScore(key, rd.ZRangeBy{Min: strconv.FormatInt(minAt, 10), Max: strconv.FormatInt(maxAt, 10), Offset: 0, Count: PoolHardCandidateLimit})
		if e != nil {
			return nil, version, e
		}
		for _, uid := range vals {
			if uid == viewer.UID {
				continue
			}
			if _, ok := seen[uid]; ok {
				continue
			}
			seen[uid] = struct{}{}
			out = append(out, uid)
			if len(out) >= PoolHardCandidateLimit {
				break
			}
		}
		if len(out) >= PoolHardCandidateLimit {
			break
		}
	}
	return out, version, nil
}

func (p *poolService) newcomerUIDs(version string, nowMS int64, limit int) ([]string, error) {
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || version == "" || limit <= 0 {
		return []string{}, nil
	}
	return p.ctx.GetRedisConn().ZRevRangeByScore(poolNewKey(version), rd.ZRangeBy{
		Min:    strconv.FormatInt(nowMS-int64(newcomerWindow/time.Millisecond), 10),
		Max:    strconv.FormatInt(nowMS+int64(time.Minute/time.Millisecond), 10),
		Offset: 0,
		Count:  int64(limit),
	})
}
func deterministicSample(values []string, seed string, limit int) []string {
	if len(values) <= limit || limit <= 0 {
		return values
	}
	type item struct {
		value string
		score uint64
	}
	items := make([]item, 0, len(values))
	for _, v := range values {
		h := fnv.New64a()
		_, _ = h.Write([]byte(seed + ":" + v))
		items = append(items, item{v, h.Sum64()})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score == items[j].score {
			return items[i].value < items[j].value
		}
		return items[i].score < items[j].score
	})
	out := make([]string, 0, limit)
	for i := 0; i < limit && i < len(items); i++ {
		out = append(out, items[i].value)
	}
	return out
}
func (p *poolService) foregroundOnlineSet(uids []string, nowMS int64) map[string]struct{} {
	out := map[string]struct{}{}
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || len(uids) == 0 {
		return out
	}
	scores, err := p.ctx.GetRedisConn().ZScores(foregroundOnlineKey, uniqueIDs(uids, 0))
	if err != nil {
		return out
	}
	for uid, score := range scores {
		if int64(score) > nowMS {
			out[uid] = struct{}{}
		}
	}
	return out
}
func (p *poolService) lastActiveScores(uids []string) map[string]int64 {
	out := map[string]int64{}
	if p == nil || p.ctx == nil || p.ctx.GetRedisConn() == nil || len(uids) == 0 {
		return out
	}
	scores, err := p.ctx.GetRedisConn().ZScores(lastActiveKey, uniqueIDs(uids, 0))
	if err != nil {
		return out
	}
	for uid, score := range scores {
		out[uid] = int64(score)
	}
	return out
}
