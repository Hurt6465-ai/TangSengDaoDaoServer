package partnerlist

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	appredis "github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/redisx"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
)

var (
	ErrProfileIncomplete  = errors.New("请先完善语伴资料")
	ErrRecommendationBusy = errors.New("语伴列表正在生成，请稍后重试")
)

type Service struct {
	ctx    *config.Context
	db     *db
	pool   *poolService
	redisx *appredis.Client
}

func NewService(ctx *config.Context) *Service {
	d := newDB(ctx)
	rx := appredis.FromContext(ctx)
	s := &Service{ctx: ctx, db: d, redisx: rx, pool: newPoolService(ctx, d, rx)}
	s.startPoolJobs()
	return s
}

func (s *Service) Recommendations(uid string) (*RecommendationResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	now := time.Now()
	nowMS := now.UnixMilli()
	dayKey := recommendationDayKey(now)
	_, _ = s.Heartbeat(uid) // Redis-only fast path; profile synchronization is event-driven.
	day, err := s.loadDay(uid, dayKey)
	if err != nil {
		return nil, err
	}
	if day == nil {
		day, err = s.generateInitial(uid, dayKey, nowMS)
		if err != nil {
			return nil, err
		}
	}
	added, removed := []string{}, []string{}
	updatedCount := 0
	day, a, r, err := s.repairInvalid(day, nowMS)
	if err != nil {
		return nil, err
	}
	added = append(added, a...)
	removed = append(removed, r...)
	updatedCount += len(a)
	dueAt := day.RotateAt
	if day.RotationRetryAt > dueAt {
		dueAt = day.RotationRetryAt
	}
	if day.RotationDone == 0 && nowMS >= dueAt && recommendationDayKey(now) == day.DayKey {
		day, a, r, err = s.rotate(day, nowMS)
		if err != nil {
			return nil, err
		}
		added = append(added, a...)
		removed = append(removed, r...)
		updatedCount += len(a)
	}
	users, err := s.usersInOrder(uid, day.currentIDs(), nowMS)
	if err != nil {
		return nil, err
	}
	used, remaining := s.greetingQuota(uid, dayKey)
	return &RecommendationResp{DayKey: day.DayKey, AlgorithmVersion: day.AlgorithmVersion, ListVersion: day.ListVersion, FirstServedAt: day.FirstServedAt, RotateAt: day.RotateAt, RotationRetryAt: day.RotationRetryAt, RotationDone: day.RotationDone == 1, UpdatedCount: updatedCount, UniqueAssignedCount: day.UniqueAssignedCount, DailyCandidateLimit: DailyUniqueLimit, GreetingLimit: GreetingDailyLimit, GreetingUsed: used, GreetingRemaining: remaining, AddedUserIDs: uniqueIDs(added, 0), RemovedUserIDs: uniqueIDs(removed, 0), Users: users, ServerTime: nowMS}, nil
}

func scoresFor(users []*ListUser) map[string]float64 {
	m := map[string]float64{}
	for _, u := range users {
		if u != nil && u.UID != "" {
			m[u.UID] = u.Score
		}
	}
	return m
}
func mergeScores(raw string, users []*ListUser) string {
	m := decodeScoreMap(raw)
	for k, v := range scoresFor(users) {
		m[k] = v
	}
	return encodeScoreMap(m)
}

func (s *Service) generateInitial(uid, dayKey string, nowMS int64) (*recommendationDay, error) {
	key := generationLockKey(dayKey, uid)
	token, locked, err := s.acquireLock(key, 30*time.Second)
	if err != nil {
		return nil, err
	}
	if !locked {
		for i := 0; i < 20; i++ {
			time.Sleep(75 * time.Millisecond)
			d, e := s.loadDay(uid, dayKey)
			if e != nil {
				return nil, e
			}
			if d != nil {
				return d, nil
			}
		}
		return nil, ErrRecommendationBusy
	}
	defer s.releaseLock(key, token)
	if d, e := s.loadDay(uid, dayKey); e != nil || d != nil {
		return d, e
	}
	viewer, err := s.db.viewer(uid)
	if err != nil || viewer == nil {
		return nil, ErrProfileIncomplete
	}
	selected, poolVersion, bucket, err := s.selectCandidates(viewer, dayKey, map[string]struct{}{}, InitialListLimit, nowMS)
	if err != nil {
		return nil, err
	}
	ids := userIDs(selected)
	day := &recommendationDay{ViewerUID: uid, DayKey: dayKey, AlgorithmVersion: AlgorithmVersion, PoolVersion: poolVersion, FirstServedAt: nowMS, RotateAt: rotationDeadline(nowMS), RotationRetryAt: 0, RotationDone: 0, InitialCandidateIDsRaw: encodeIDs(ids), CurrentCandidateIDsRaw: encodeIDs(ids), AllAssignedCandidateIDsRaw: encodeIDs(ids), RotatedInIDsRaw: "[]", RotatedOutIDsRaw: "[]", AbnormalReplacementIDsRaw: "[]", CandidateScoresRaw: encodeScoreMap(scoresFor(selected)), UniqueAssignedCount: len(ids), ListVersion: 1}
	if err = s.db.insertDay(day, bucket, ids, nowMS); err != nil {
		if existing, e := s.db.loadDay(uid, dayKey); e == nil && existing != nil {
			s.cacheDay(existing)
			return existing, nil
		}
		return nil, err
	}
	s.recordViewerSeen(uid, ids, nowMS)
	s.cacheDay(day)
	go s.flushAssignmentOutbox()
	return day, nil
}

func (s *Service) repairInvalid(day *recommendationDay, nowMS int64) (*recommendationDay, []string, []string, error) {
	if day == nil {
		return nil, nil, nil, nil
	}
	current := day.currentIDs()
	validUsers, err := s.db.profilesByUIDs(day.ViewerUID, current)
	if err != nil {
		return day, nil, nil, err
	}
	valid := map[string]struct{}{}
	for _, u := range validUsers {
		valid[u.UID] = struct{}{}
	}
	kept, removed := []string{}, []string{}
	for _, uid := range current {
		if _, ok := valid[uid]; ok {
			kept = append(kept, uid)
		} else {
			removed = append(removed, uid)
		}
	}
	if len(removed) == 0 {
		return day, nil, nil, nil
	}
	remaining := DailyUniqueLimit - day.UniqueAssignedCount
	missing := InitialListLimit - len(kept)
	if missing < 0 {
		missing = 0
	}
	if missing > remaining {
		missing = remaining
	}
	addedUsers := []*ListUser{}
	bucket := ""
	poolVersion := day.PoolVersion
	if missing > 0 {
		viewer, e := s.db.viewer(day.ViewerUID)
		if e != nil {
			return day, nil, nil, e
		}
		addedUsers, poolVersion, bucket, e = s.selectCandidates(viewer, day.DayKey, toSet(day.allAssignedIDs()), missing, nowMS)
		if e != nil {
			return day, nil, nil, e
		}
	}
	added := userIDs(addedUsers)
	expected := day.ListVersion
	all := append(day.allAssignedIDs(), added...)
	day.PoolVersion = poolVersion
	day.CurrentCandidateIDsRaw = encodeIDs(append(kept, added...))
	day.AllAssignedCandidateIDsRaw = encodeIDs(all)
	day.AbnormalReplacementIDsRaw = encodeIDs(append(day.abnormalReplacementIDs(), added...))
	day.CandidateScoresRaw = mergeScores(day.CandidateScoresRaw, addedUsers)
	day.UniqueAssignedCount = len(uniqueIDs(all, 0))
	day.ListVersion++
	updated, err := s.db.updateDay(day, expected, bucket, added, nowMS)
	if err != nil {
		return day, nil, nil, err
	}
	if !updated {
		d, e := s.db.loadDay(day.ViewerUID, day.DayKey)
		return d, nil, nil, e
	}
	s.recordViewerSeen(day.ViewerUID, added, nowMS)
	s.cacheDay(day)
	go s.flushAssignmentOutbox()
	return day, added, removed, nil
}

func (s *Service) rotate(day *recommendationDay, nowMS int64) (*recommendationDay, []string, []string, error) {
	if day == nil || day.RotationDone == 1 {
		return day, nil, nil, nil
	}
	key := rotationLockKey(day.DayKey, day.ViewerUID)
	token, locked, err := s.acquireLock(key, 30*time.Second)
	if err != nil {
		return day, nil, nil, err
	}
	if !locked {
		return day, nil, nil, nil
	}
	defer s.releaseLock(key, token)
	fresh, err := s.db.loadDay(day.ViewerUID, day.DayKey)
	if err != nil || fresh == nil {
		return day, nil, nil, err
	}
	day = fresh
	due := day.RotateAt
	if day.RotationRetryAt > due {
		due = day.RotationRetryAt
	}
	if day.RotationDone == 1 || nowMS < due {
		return day, nil, nil, nil
	}
	remaining := DailyUniqueLimit - day.UniqueAssignedCount
	if remaining <= 0 {
		expected := day.ListVersion
		day.RotationDone = 1
		day.RotationRetryAt = 0
		day.ListVersion++
		ok, e := s.db.updateDay(day, expected, "", nil, nowMS)
		if e != nil {
			return day, nil, nil, e
		}
		if ok {
			s.cacheDay(day)
		}
		return day, nil, nil, nil
	}
	limit := RotationAddLimit
	if limit > remaining {
		limit = remaining
	}
	viewer, err := s.db.viewer(day.ViewerUID)
	if err != nil {
		return day, nil, nil, err
	}
	newUsers, poolVersion, bucket, err := s.selectCandidates(viewer, day.DayKey, toSet(day.allAssignedIDs()), limit, nowMS)
	if err != nil {
		return day, nil, nil, err
	}
	newIDs := userIDs(newUsers)
	if len(newIDs) == 0 {
		next := time.UnixMilli(nowMS).Add(20 * time.Minute)
		boundary := nextRecommendationBoundary(time.UnixMilli(nowMS))
		expected := day.ListVersion
		if !next.Before(boundary) {
			day.RotationDone = 1
			day.RotationRetryAt = 0
		} else {
			day.RotationRetryAt = next.UnixMilli()
		}
		day.ListVersion++
		ok, e := s.db.updateDay(day, expected, "", nil, nowMS)
		if e != nil {
			return day, nil, nil, e
		}
		if !ok {
			d, le := s.db.loadDay(day.ViewerUID, day.DayKey)
			return d, nil, nil, le
		}
		s.cacheDay(day)
		return day, nil, nil, nil
	}
	currentUsers, err := s.usersInOrder(day.ViewerUID, day.currentIDs(), nowMS)
	if err != nil {
		return day, nil, nil, err
	}
	scoreMap := decodeScoreMap(day.CandidateScoresRaw)
	removeCount := len(currentUsers) + len(newIDs) - InitialListLimit
	if removeCount < 0 {
		removeCount = 0
	}
	if removeCount > len(currentUsers) {
		removeCount = len(currentUsers)
	}
	byLowest := append([]*ListUser(nil), currentUsers...)
	sort.SliceStable(byLowest, func(i, j int) bool {
		si := scoreMap[byLowest[i].UID]
		sj := scoreMap[byLowest[j].UID]
		if si == sj {
			return byLowest[i].UID > byLowest[j].UID
		}
		return si < sj
	})
	drop := map[string]struct{}{}
	for i := 0; i < removeCount; i++ {
		drop[byLowest[i].UID] = struct{}{}
	}
	validCurrent := toSet(userIDs(currentUsers))
	kept, removed := []string{}, []string{}
	for _, uid := range day.currentIDs() {
		if _, ok := validCurrent[uid]; !ok {
			continue
		}
		if _, ok := drop[uid]; ok {
			removed = append(removed, uid)
		} else {
			kept = append(kept, uid)
		}
	}
	current := interleaveIDs(kept, newIDs, InitialListLimit)
	all := append(day.allAssignedIDs(), newIDs...)
	expected := day.ListVersion
	day.PoolVersion = poolVersion
	day.RotationDone = 1
	day.RotationRetryAt = 0
	day.CurrentCandidateIDsRaw = encodeIDs(current)
	day.AllAssignedCandidateIDsRaw = encodeIDs(all)
	day.RotatedInIDsRaw = encodeIDs(newIDs)
	day.RotatedOutIDsRaw = encodeIDs(removed)
	day.CandidateScoresRaw = mergeScores(day.CandidateScoresRaw, newUsers)
	day.UniqueAssignedCount = len(uniqueIDs(all, 0))
	day.ListVersion++
	ok, err := s.db.updateDay(day, expected, bucket, newIDs, nowMS)
	if err != nil {
		return day, nil, nil, err
	}
	if !ok {
		d, e := s.db.loadDay(day.ViewerUID, day.DayKey)
		return d, nil, nil, e
	}
	s.recordViewerSeen(day.ViewerUID, newIDs, nowMS)
	s.cacheDay(day)
	go s.flushAssignmentOutbox()
	return day, newIDs, removed, nil
}

func (s *Service) stratifiedCandidateSample(values []string, version string, viewer *viewerProfile, dayKey string, nowMS int64, limit int) []string {
	values = uniqueIDs(values, PoolHardCandidateLimit)
	if len(values) <= limit || limit <= 0 {
		return values
	}
	candidateSet := toSet(values)
	out := make([]string, 0, limit)
	seen := map[string]struct{}{}
	add := func(uid string) {
		if uid == "" || len(out) >= limit {
			return
		}
		if _, ok := candidateSet[uid]; !ok {
			return
		}
		if _, ok := seen[uid]; ok {
			return
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	// Values arrive in last-active descending order. Keep the strongest active
	// cohort, then explicitly reserve newcomer, low-assignment and exploration slots.
	for i := 0; i < len(values) && i < 1200; i++ {
		add(values[i])
	}
	if newcomers, err := s.pool.newcomerUIDs(version, nowMS, 500); err == nil {
		added := 0
		for _, uid := range newcomers {
			before := len(out)
			add(uid)
			if len(out) > before {
				added++
			}
			if added >= 200 {
				break
			}
		}
	}
	assign := map[string]int{}
	if s.redisx != nil {
		assign = readAssign(s.redisx, assignmentGlobalKey(dayKey), values)
	}
	low := append([]string(nil), values...)
	sort.SliceStable(low, func(i, j int) bool {
		if assign[low[i]] == assign[low[j]] {
			return normalizedHash(viewer.UID+":"+dayKey+":"+low[i], 1) < normalizedHash(viewer.UID+":"+dayKey+":"+low[j], 1)
		}
		return assign[low[i]] < assign[low[j]]
	})
	addedLow := 0
	for _, uid := range low {
		before := len(out)
		add(uid)
		if len(out) > before {
			addedLow++
		}
		if addedLow >= 400 || len(out) >= limit {
			break
		}
	}
	for _, uid := range deterministicSample(values, viewer.UID+":"+dayKey+":"+version+":explore", 400) {
		add(uid)
		if len(out) >= limit {
			break
		}
	}
	for _, uid := range values {
		add(uid)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func (s *Service) selectCandidates(viewer *viewerProfile, dayKey string, excluded map[string]struct{}, limit int, nowMS int64) ([]*ListUser, string, string, error) {
	if viewer == nil || limit <= 0 {
		return []*ListUser{}, "", "", nil
	}
	hot, version, err := s.pool.candidateUIDs(viewer, false)
	if err != nil {
		return nil, "", "", err
	}
	hot = withoutSet(hot, excluded)
	universeCount := len(hot)
	if len(hot) > PoolDirectScoreLimit {
		hot = s.stratifiedCandidateSample(hot, version, viewer, dayKey, nowMS, 2000)
	}
	profiles, err := s.db.profilesByUIDs(viewer.UID, hot)
	if err != nil {
		return nil, "", "", err
	}
	if len(profiles) < 120 {
		warm, wv, e := s.pool.candidateUIDs(viewer, true)
		if e != nil {
			return nil, "", "", e
		}
		if wv != "" {
			version = wv
		}
		combined := uniqueIDs(append(hot, withoutSet(warm, excluded)...), PoolHardCandidateLimit)
		universeCount = len(combined)
		if len(combined) > PoolDirectScoreLimit {
			combined = s.stratifiedCandidateSample(combined, version, viewer, dayKey, nowMS, 2000)
		}
		profiles, err = s.db.profilesByUIDs(viewer.UID, combined)
		if err != nil {
			return nil, "", "", err
		}
	}
	if len(profiles) == 0 {
		return []*ListUser{}, version, languageBucket(viewer), nil
	}
	ids := userIDs(profiles)
	active := s.pool.lastActiveScores(ids)
	for _, p := range profiles {
		if a := active[p.UID]; a > p.LastActiveAt {
			p.LastActiveAt = a
		}
	}
	bucket := languageBucket(viewer)
	today, yesterday, baseline, fairness := s.assignmentContext(dayKey, bucket, ids, universeCount)
	loads, err := s.db.inboxLoads(ids, nowMS-int64(24*time.Hour/time.Millisecond))
	if err != nil {
		return nil, "", "", err
	}
	foreground := s.pool.foregroundOnlineSet(ids, nowMS)
	imOnline, err := s.db.imOnlineUIDs(ids)
	if err != nil {
		return nil, "", "", err
	}
	for _, p := range profiles {
		if _, ok := imOnline[p.UID]; ok {
			p.Online = 1
		} else {
			p.Online = 0
		}
	}
	scored := scoreAndSort(profiles, scoreContext{viewer: viewer, dayKey: dayKey, nowMS: nowMS, online: foreground, repeatDays: s.repeatDays(viewer.UID, dayKey), todayAssign: today, yesterdayAssign: yesterday, assignmentBaseline: baseline, fairnessFactor: fairness, inboxLoads: loads})
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored, version, bucket, nil
}

func (s *Service) usersInOrder(viewerUID string, ids []string, nowMS int64) ([]*ListUser, error) {
	ids = uniqueIDs(ids, InitialListLimit)
	if len(ids) == 0 {
		return []*ListUser{}, nil
	}
	profiles, err := s.db.profilesByUIDs(viewerUID, ids)
	if err != nil {
		return nil, err
	}
	m := map[string]*ListUser{}
	for _, p := range profiles {
		m[p.UID] = p
	}
	foreground := s.pool.foregroundOnlineSet(ids, nowMS)
	active := s.pool.lastActiveScores(ids)
	im, err := s.db.imOnlineUIDs(ids)
	if err != nil {
		return nil, err
	}
	out := []*ListUser{}
	for _, uid := range ids {
		p := m[uid]
		if p == nil {
			continue
		}
		if a := active[uid]; a > p.LastActiveAt {
			p.LastActiveAt = a
		}
		_, iok := im[uid]
		_, fok := foreground[uid]
		if iok && fok {
			p.Online = 1
		} else {
			p.Online = 0
		}
		out = append(out, p)
	}
	return out, nil
}

func (s *Service) repeatDays(viewerUID, currentDay string) map[string]int {
	out := map[string]int{}
	rows, err := s.db.recentDays(viewerUID, 8)
	if err != nil {
		return out
	}
	current, err := time.ParseInLocation("2006-01-02", currentDay, businessLocation)
	if err != nil {
		return out
	}
	for _, row := range rows {
		day, e := time.ParseInLocation("2006-01-02", row.DayKey, businessLocation)
		if e != nil {
			continue
		}
		diff := int(current.Sub(day).Hours() / 24)
		if diff < 1 || diff > 7 {
			continue
		}
		for _, uid := range decodeIDs(row.AssignedRaw) {
			if old, ok := out[uid]; !ok || diff < old {
				out[uid] = diff
			}
		}
	}
	return out
}

func readAssign(conn *appredis.Client, key string, ids []string) map[string]int {
	out := map[string]int{}
	vals, err := conn.HMGetMap(key, ids)
	if err != nil {
		return out
	}
	for uid, v := range vals {
		n, _ := strconv.Atoi(v)
		out[uid] = n
	}
	return out
}
func redisHashInt(conn interface {
	Hget(string, string) (string, error)
}, key, field string) int {
	value, err := conn.Hget(key, field)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(value)
	return n
}

func (s *Service) assignmentContext(dayKey, bucket string, ids []string, bucketEligible int) (map[string]int, map[string]int, float64, float64) {
	today, yesterday := map[string]int{}, map[string]int{}
	if s.ctx == nil || s.ctx.GetRedisConn() == nil || s.redisx == nil {
		return today, yesterday, 1, 0
	}
	c := s.ctx.GetRedisConn()
	globalTodayKey := assignmentGlobalKey(dayKey)
	bucketTodayKey := assignmentBucketKey(dayKey, bucket)
	previous := previousDayKey(dayKey)
	globalYesterdayKey := assignmentGlobalKey(previous)
	bucketYesterdayKey := assignmentBucketKey(previous, bucket)
	gt := readAssign(s.redisx, globalTodayKey, ids)
	bt := readAssign(s.redisx, bucketTodayKey, ids)
	gy := readAssign(s.redisx, globalYesterdayKey, ids)
	by := readAssign(s.redisx, bucketYesterdayKey, ids)
	for _, uid := range ids {
		today[uid] = gt[uid]*7 + bt[uid]*3
		yesterday[uid] = gy[uid]*7 + by[uid]*3
	}
	globalEligible := s.pool.eligibleCount()
	if globalEligible <= 0 {
		globalEligible = bucketEligible
	}
	if globalEligible <= 0 {
		globalEligible = 1
	}
	if bucketEligible <= 0 {
		bucketEligible = len(ids)
	}
	if bucketEligible <= 0 {
		bucketEligible = 1
	}
	globalLoad := (float64(redisHashInt(c, globalTodayKey, "__total__")) + float64(redisHashInt(c, globalYesterdayKey, "__total__"))*0.5) / float64(globalEligible)
	bucketLoad := (float64(redisHashInt(c, bucketTodayKey, "__total__")) + float64(redisHashInt(c, bucketYesterdayKey, "__total__"))*0.5) / float64(bucketEligible)
	baseline := globalLoad*7 + bucketLoad*3
	if baseline < 1 {
		baseline = 1
	}
	return today, yesterday, baseline, math.Min(1, float64(bucketEligible)/300.0)
}

func (s *Service) recordViewerSeen(viewer string, ids []string, at int64) {
	if s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	pairs := []interface{}{}
	for _, uid := range uniqueIDs(ids, 0) {
		pairs = append(pairs, float64(at), uid)
	}
	if len(pairs) > 0 && s.redisx != nil {
		_ = s.redisx.ZAdd(viewerSeenKey(viewer), pairs...)
		_ = s.ctx.GetRedisConn().Expire(viewerSeenKey(viewer), viewerSeenTTL)
	}
}

func (s *Service) flushAssignmentOutbox() {
	if s.ctx == nil || s.ctx.GetRedisConn() == nil || s.redisx == nil {
		return
	}
	rows, err := s.db.pendingAssignmentOutbox(time.Now().UnixMilli(), 500)
	if err != nil || len(rows) == 0 {
		return
	}
	const script = `
local applied = KEYS[1]
local global = KEYS[2]
local bucket = KEYS[3]
local id = ARGV[1]
local candidate = ARGV[2]
local ttl = tonumber(ARGV[3])
if redis.call('SADD', applied, id) == 1 then
  redis.call('HINCRBY', global, candidate, 1)
  redis.call('HINCRBY', bucket, candidate, 1)
  redis.call('HINCRBY', global, '__total__', 1)
  redis.call('HINCRBY', bucket, '__total__', 1)
end
redis.call('EXPIRE', applied, ttl)
redis.call('EXPIRE', global, ttl)
redis.call('EXPIRE', bucket, ttl)
return 1`
	ttlSeconds := int64(recommendationTTL / time.Second)
	for _, row := range rows {
		_, err = s.redisx.EvalInt(script, []string{assignmentAppliedKey(row.DayKey), assignmentGlobalKey(row.DayKey), assignmentBucketKey(row.DayKey, row.Bucket)}, strconv.FormatInt(row.ID, 10), row.CandidateUID, ttlSeconds)
		if err == nil {
			err = s.db.markAssignmentOutboxDone(row.ID)
		}
		if err != nil {
			_ = s.db.markAssignmentOutboxRetry(row.ID, row.Attempts, err.Error())
		}
	}
}

func (s *Service) Heartbeat(uid string) (*HeartbeatResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	now := time.Now()
	nowMS := now.UnixMilli()
	expire := now.Add(foregroundTTL).UnixMilli()
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		c := s.ctx.GetRedisConn()
		_ = c.ZAdd(foregroundOnlineKey, float64(expire), uid)
		_ = c.Expire(foregroundOnlineKey, 24*time.Hour)
		_ = c.ZAdd(lastActiveKey, float64(nowMS), uid)
		_ = c.Expire(lastActiveKey, 10*24*time.Hour)
		write := false
		var e error
		if s.redisx != nil {
			write, e = s.redisx.SetNX(activeWriteLockKey(uid), "1", activeWriteTTL)
		}
		if e == nil && write {
			go func() { _ = s.db.touchActive(uid, nowMS); _ = s.pool.syncUserE(uid, nowMS) }()
		}
	}
	return &HeartbeatResp{UID: uid, LastActiveAt: nowMS, OnlineExpireAt: expire, NextHeartbeatIn: 60}, nil
}
func (s *Service) SetEnabled(uid string, enabled int) (*PartnerSettingsResp, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New("请先登录")
	}
	if enabled != 1 {
		enabled = 0
	}
	if err := s.db.setPartnerEnabled(uid, enabled); err != nil {
		return nil, err
	}
	_ = s.pool.syncUserE(uid, 0)
	return &PartnerSettingsResp{Enabled: enabled}, nil
}

func (s *Service) OnlineBatch(uids []string) (*OnlineBatchResp, error) {
	uids = uniqueIDs(uids, 50)
	now := time.Now().UnixMilli()
	im, err := s.db.imOnlineUIDs(uids)
	if err != nil {
		return nil, err
	}
	dbActive, err := s.db.activityByUIDs(uids)
	if err != nil {
		return nil, err
	}
	fg := s.pool.foregroundOnlineSet(uids, now)
	ra := s.pool.lastActiveScores(uids)
	states := []OnlineState{}
	for _, uid := range uids {
		online := 0
		if _, a := im[uid]; a {
			if _, b := fg[uid]; b {
				online = 1
			}
		}
		last := dbActive[uid]
		if ra[uid] > last {
			last = ra[uid]
		}
		states = append(states, OnlineState{UID: uid, Online: online, LastActiveAt: last})
	}
	return &OnlineBatchResp{Users: states, ServerTime: now}, nil
}
func (s *Service) greetingQuota(uid, dayKey string) (int, int) {
	if _, e := time.ParseInLocation("2006-01-02", dayKey, businessLocation); e != nil {
		return 0, GreetingDailyLimit
	}
	used := s.db.greetingUsed(uid, dayKey)
	if used > GreetingDailyLimit {
		used = GreetingDailyLimit
	}
	remaining := GreetingDailyLimit - used
	if remaining < 0 {
		remaining = 0
	}
	return used, remaining
}

func (s *Service) loadDay(uid, dayKey string) (*recommendationDay, error) {
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		raw, e := s.ctx.GetRedisConn().GetString(recommendationCacheKey(dayKey, uid))
		if e == nil && strings.TrimSpace(raw) != "" {
			var d recommendationDay
			if json.Unmarshal([]byte(raw), &d) == nil {
				return &d, nil
			}
		}
	}
	d, e := s.db.loadDay(uid, dayKey)
	if e == nil && d != nil {
		s.cacheDay(d)
	}
	return d, e
}
func (s *Service) cacheDay(day *recommendationDay) {
	if day == nil || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	data, e := json.Marshal(day)
	if e == nil {
		_ = s.ctx.GetRedisConn().SetAndExpire(recommendationCacheKey(day.DayKey, day.ViewerUID), string(data), recommendationTTL)
	}
}
func (s *Service) acquireLock(key string, ttl time.Duration) (string, bool, error) {
	if s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return "", true, nil
	}
	if s.redisx == nil {
		return "", false, errors.New("Redis高级客户端不可用")
	}
	token := fmt.Sprintf("%d", time.Now().UnixNano())
	ok, e := s.redisx.SetNX(key, token, ttl)
	return token, ok, e
}
func (s *Service) releaseLock(key, token string) {
	if token != "" && s.redisx != nil {
		_, _ = s.redisx.CompareAndDelete(key, token)
	}
}

func (s *Service) startPoolJobs() {
	go func() {
		time.Sleep(5 * time.Second)
		_, _ = s.pool.ensure()
		for {
			next := nextRecommendationBoundary(time.Now())
			if !next.After(time.Now()) {
				next = next.Add(24 * time.Hour)
			}
			time.Sleep(time.Until(next))
			_ = s.pool.rebuild()
		}
	}()
	go func() {
		time.Sleep(10 * time.Second)
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			if s.ctx == nil || s.ctx.GetRedisConn() == nil {
				continue
			}
			for i := 0; i < 200; i++ {
				uid, e := s.ctx.GetRedisConn().Lpop(poolDirtyQueueKey)
				if e != nil || uid == "" {
					break
				}
				if e = s.pool.syncUserE(uid, 0); e != nil {
					if s.redisx != nil {
						_, _ = s.redisx.RPush(poolDirtyQueueKey, uid)
					} else {
						_, _ = s.ctx.GetRedisConn().LPUSH(poolDirtyQueueKey, uid)
					}
					break
				}
			}
		}
	}()
	go func() {
		time.Sleep(20 * time.Second)
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			s.foldChangedProfiles()
			<-t.C
		}
	}()
	go func() {
		time.Sleep(5 * time.Second)
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			s.flushAssignmentOutbox()
			<-t.C
		}
	}()
}
func (s *Service) foldChangedProfiles() {
	if s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	raw, _ := s.ctx.GetRedisConn().GetString(poolChangeCursorKey)
	cursorMS := time.Now().Add(-10 * time.Minute).UnixMilli()
	cursorUID := ""
	if parts := strings.Split(raw, "|"); len(parts) == 2 {
		if n, e := strconv.ParseInt(parts[0], 10, 64); e == nil {
			cursorMS = n
		}
		cursorUID = parts[1]
	}
	versions := s.pool.activeVersions()
	if len(versions) == 0 {
		return
	}
	for {
		rows, e := s.db.changedProfilesAfter(cursorMS, cursorUID, 1000)
		if e != nil {
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			for _, version := range versions {
				if e = s.pool.applyChangedProfileE(version, row); e != nil {
					return
				}
			}
			cursorMS = row.UpdatedAtMS
			cursorUID = row.UID
			_ = s.ctx.GetRedisConn().Set(poolChangeCursorKey, fmt.Sprintf("%d|%s", cursorMS, cursorUID))
		}
		if len(rows) < 1000 {
			break
		}
	}
	// Keep an overlap window after a successful pass. MySQL TIMESTAMP values may
	// only have second precision, and replaying is idempotent, so duplicates are
	// safer than missing a profile change on the cursor boundary.
	overlap := cursorMS - int64(10*time.Second/time.Millisecond)
	if overlap < 0 {
		overlap = 0
	}
	_ = s.ctx.GetRedisConn().Set(poolChangeCursorKey, fmt.Sprintf("%d|", overlap))
}

func languageBucket(v *viewerProfile) string {
	if v == nil {
		return "unknown"
	}
	n, l := "unknown", "unknown"
	if len(v.NativeLanguages) > 0 {
		n = v.NativeLanguages[0]
	}
	if v.PrimaryLearning != "" {
		l = v.PrimaryLearning
	}
	return n + ":" + l
}
func userIDs(users []*ListUser) []string {
	ids := []string{}
	for _, u := range users {
		if u != nil && u.UID != "" {
			ids = append(ids, u.UID)
		}
	}
	return uniqueIDs(ids, 0)
}
func toSet(ids []string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, id := range ids {
		if id != "" {
			m[id] = struct{}{}
		}
	}
	return m
}
func withoutSet(ids []string, excluded map[string]struct{}) []string {
	out := []string{}
	for _, id := range ids {
		if _, ok := excluded[id]; !ok {
			out = append(out, id)
		}
	}
	return uniqueIDs(out, 0)
}
func interleaveIDs(kept, added []string, limit int) []string {
	kept = uniqueIDs(kept, 0)
	added = uniqueIDs(added, 0)
	if len(added) == 0 {
		return uniqueIDs(kept, limit)
	}
	out := []string{}
	j := 0
	for i, uid := range kept {
		out = append(out, uid)
		if (i+1)%3 == 0 && j < len(added) {
			out = append(out, added[j])
			j++
		}
	}
	out = append(out, added[j:]...)
	return uniqueIDs(out, limit)
}
