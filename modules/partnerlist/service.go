package partnerlist

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appredis "github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/redisx"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"go.uber.org/zap"
)

var (
	ErrProfileIncomplete  = errors.New("请先完善语伴资料")
	ErrRecommendationBusy = errors.New("语伴列表正在生成，请稍后重试")
)

const (
	defaultActiveWriteWorkers = 8
	defaultActiveWriteQueue   = 8192
	defaultCandidateWorkers   = 8
	imOnlineSnapshotFreshFor  = 15 * time.Second
)

var activeWriteSpillMax = int64(boundedEnvInt("TS_DD_PARTNER_ACTIVE_SPILL_MAX", 65536, 5000, 500000))

type activeWriteTask struct {
	uid   string
	atMS  int64
	token string
}

type activeWriteSpillPayload struct {
	UID   string `json:"uid"`
	AtMS  int64  `json:"at_ms"`
	Token string `json:"token"`
}

type Service struct {
	ctx    *config.Context
	db     *db
	pool   *poolService
	redisx *appredis.Client
	log.Log

	jobMu      sync.Mutex
	jobRunning map[string]bool

	activeWriteQueue chan activeWriteTask
	candidateSlots   chan struct{}
	assignmentWake   chan struct{}

	imOnlineMu        sync.RWMutex
	imOnlineRefreshMu sync.Mutex
	imOnlineSnapshot  map[string]struct{}
	imOnlineLoadedAt  int64
	imOnlineRetryAt   atomic.Int64

	activeQueueSpilled atomic.Uint64
	activeQueueDropped atomic.Uint64
	activeWorkerErrors atomic.Uint64
	jobLockErrors      atomic.Uint64
	snapshotErrors     atomic.Uint64
	presenceErrors     atomic.Uint64
	lastPressureWarnAt atomic.Int64
}

func NewService(ctx *config.Context) *Service {
	d := newDB(ctx)
	rx := appredis.FromContext(ctx)
	activeWorkers := boundedEnvInt("TS_DD_PARTNER_ACTIVE_WORKERS", defaultActiveWriteWorkers, 1, 64)
	activeQueueSize := boundedEnvInt("TS_DD_PARTNER_ACTIVE_QUEUE", defaultActiveWriteQueue, 512, 65536)
	candidateWorkers := boundedEnvInt("TS_DD_PARTNER_CANDIDATE_WORKERS", defaultCandidateWorkers, 1, 64)
	s := &Service{
		ctx: ctx, db: d, redisx: rx, pool: newPoolService(ctx, d, rx), jobRunning: make(map[string]bool), Log: log.NewTLog("partnerlistService"),
		activeWriteQueue: make(chan activeWriteTask, activeQueueSize),
		candidateSlots:   make(chan struct{}, candidateWorkers),
		assignmentWake:   make(chan struct{}, 1),
		imOnlineSnapshot: make(map[string]struct{}),
	}
	_ = s.refreshIMOnlineSnapshot()
	s.startActiveWriteWorkers(activeWorkers)
	s.startRuntimeMetrics()
	s.startPoolJobs()
	return s
}

func boundedEnvInt(key string, fallback, minValue, maxValue int) int {
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
	day, a, r, validatedUsers, err := s.repairInvalid(day, nowMS)
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
		validatedUsers = nil // rotate reloads and may replace the current recommendation set.
		day, a, r, err = s.rotate(day, nowMS)
		if err != nil {
			return nil, err
		}
		added = append(added, a...)
		removed = append(removed, r...)
		updatedCount += len(a)
	}
	var users []*ListUser
	if validatedUsers != nil {
		users, err = s.usersInOrderFromProfiles(day.currentIDs(), validatedUsers, nowMS)
	} else {
		users, err = s.usersInOrder(uid, day.currentIDs(), nowMS)
	}
	if err != nil {
		return nil, err
	}
	used, remaining := s.greetingQuota(uid, dayKey)
	return &RecommendationResp{DayKey: day.DayKey, AlgorithmVersion: day.AlgorithmVersion, ListVersion: day.ListVersion, FirstServedAt: day.FirstServedAt, RotateAt: day.RotateAt, RotationRetryAt: day.RotationRetryAt, RotationDone: day.RotationDone == 1, UpdatedCount: updatedCount, UniqueAssignedCount: day.UniqueAssignedCount, DailyCandidateLimit: DailyUniqueLimit, GreetingLimit: GreetingDailyLimit, GreetingUsed: used, GreetingRemaining: remaining, AddedUserIDs: uniqueIDs(added, 0), RemovedUserIDs: uniqueIDs(removed, 0), Users: users, ServerTime: nowMS, OnlineAsOf: s.onlineSnapshotLoadedAt(), OnlineRefreshAfterMS: onlineRefreshAfterMS(uid, now)}, nil
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
	s.signalAssignmentOutbox()
	return day, nil
}

func (s *Service) repairInvalid(day *recommendationDay, nowMS int64) (*recommendationDay, []string, []string, []*ListUser, error) {
	if day == nil {
		return nil, nil, nil, nil, nil
	}
	current := day.currentIDs()
	validUsers, err := s.db.profilesByUIDs(day.ViewerUID, current)
	if err != nil {
		return day, nil, nil, nil, err
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
		return day, nil, nil, validUsers, nil
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
			return day, nil, nil, nil, e
		}
		addedUsers, poolVersion, bucket, e = s.selectCandidates(viewer, day.DayKey, toSet(day.allAssignedIDs()), missing, nowMS)
		if e != nil {
			return day, nil, nil, nil, e
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
		return day, nil, nil, nil, err
	}
	if !updated {
		d, e := s.db.loadDay(day.ViewerUID, day.DayKey)
		return d, nil, nil, nil, e
	}
	s.recordViewerSeen(day.ViewerUID, added, nowMS)
	s.cacheDay(day)
	s.signalAssignmentOutbox()
	return day, added, removed, append(validUsers, addedUsers...), nil
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
	s.signalAssignmentOutbox()
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
	if !s.acquireCandidateSlot(2 * time.Second) {
		return nil, "", "", ErrRecommendationBusy
	}
	defer s.releaseCandidateSlot()
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
	imOnline, err := s.imOnlineUIDs(ids)
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
	return s.usersInOrderFromProfiles(ids, profiles, nowMS)
}

func (s *Service) usersInOrderFromProfiles(ids []string, profiles []*ListUser, nowMS int64) ([]*ListUser, error) {
	ids = uniqueIDs(ids, InitialListLimit)
	if len(ids) == 0 {
		return []*ListUser{}, nil
	}
	m := make(map[string]*ListUser, len(profiles))
	for _, profile := range profiles {
		if profile != nil && profile.UID != "" {
			m[profile.UID] = profile
		}
	}
	foreground := s.pool.foregroundOnlineSet(ids, nowMS)
	active := s.pool.lastActiveScores(ids)
	im, err := s.imOnlineUIDs(ids)
	if err != nil {
		return nil, err
	}
	out := make([]*ListUser, 0, len(ids))
	for _, uid := range ids {
		profile := m[uid]
		if profile == nil {
			continue
		}
		if lastActive := active[uid]; lastActive > profile.LastActiveAt {
			profile.LastActiveAt = lastActive
		}
		_, imOnline := im[uid]
		_, foregroundOnline := foreground[uid]
		if imOnline && foregroundOnline {
			profile.Online = 1
		} else {
			profile.Online = 0
		}
		out = append(out, profile)
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
	viewer = strings.TrimSpace(viewer)
	if s == nil || viewer == "" || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	pairs := make([]interface{}, 0, len(ids)*2)
	for _, uid := range uniqueIDs(ids, 0) {
		pairs = append(pairs, float64(at), uid)
	}
	if len(pairs) == 0 {
		return
	}
	key := viewerSeenKey(viewer)
	var writeErr error
	advancedWrite := false
	if s.redisx != nil {
		writeErr = s.redisx.ZAdd(key, pairs...)
		advancedWrite = writeErr == nil
	}
	// TangSengDaoDaoServerLib's ZAdd wrapper has a multi-member indexing bug.
	// Use one member per call only as a compatibility fallback when redisx is
	// unavailable or temporarily unhealthy.
	if s.redisx == nil || writeErr != nil {
		writeErr = nil
		for i := 0; i+1 < len(pairs); i += 2 {
			if err := s.ctx.GetRedisConn().ZAdd(key, pairs[i], pairs[i+1]); err != nil {
				writeErr = err
				break
			}
		}
	}
	if writeErr != nil {
		// A compatibility fallback can fail after writing some members. Keep even a
		// partially written key bounded instead of leaving an accidental permanent
		// de-duplication record.
		if s.redisx != nil {
			_ = s.redisx.Expire(key, viewerSeenTTL)
		}
		_ = s.ctx.GetRedisConn().Expire(key, viewerSeenTTL)
		s.warnPressure("语伴列表已看记录写入Redis失败", zap.String("viewer_uid", viewer), zap.Int("candidate_count", len(pairs)/2), zap.Error(writeErr))
		return
	}
	var expireErr error
	if advancedWrite {
		expireErr = s.redisx.Expire(key, viewerSeenTTL)
	}
	if !advancedWrite || expireErr != nil {
		expireErr = s.ctx.GetRedisConn().Expire(key, viewerSeenTTL)
	}
	if expireErr != nil {
		s.warnPressure("语伴列表已看记录设置过期时间失败", zap.String("viewer_uid", viewer), zap.Error(expireErr))
	}
}

func (s *Service) flushAssignmentOutbox() {
	s.runExclusiveJob(assignmentOutboxFlushLockKey, 5*time.Minute, s.flushAssignmentOutboxLocked)
}

func (s *Service) flushAssignmentOutboxLocked() {
	if s.ctx == nil || s.ctx.GetRedisConn() == nil || s.redisx == nil {
		return
	}
	rows, err := s.db.pendingAssignmentOutbox(time.Now().UnixMilli(), 1000)
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
	doneIDs := make([]int64, 0, len(rows))
	consecutiveRedisErrors := 0
	for _, row := range rows {
		_, err = s.redisx.EvalInt(script, []string{assignmentAppliedKey(row.DayKey), assignmentGlobalKey(row.DayKey), assignmentBucketKey(row.DayKey, row.Bucket)}, strconv.FormatInt(row.ID, 10), row.CandidateUID, ttlSeconds)
		if err == nil {
			doneIDs = append(doneIDs, row.ID)
			consecutiveRedisErrors = 0
			continue
		}
		_ = s.db.markAssignmentOutboxRetry(row.ID, row.Attempts, err.Error())
		consecutiveRedisErrors++
		// A Redis outage is normally shared by the whole batch. Do not turn one
		// failing connection into hundreds of sequential timeouts every two seconds.
		if consecutiveRedisErrors >= 3 {
			break
		}
	}
	_ = s.db.markAssignmentOutboxDoneBatch(doneIDs)
}

func (s *Service) Heartbeat(uid string) (*HeartbeatResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	now := time.Now()
	nowMS := now.UnixMilli()
	expire := now.Add(foregroundTTL).UnixMilli()
	presenceUpdated := false
	if s.redisx != nil {
		token := strconv.FormatInt(now.UnixNano(), 10)
		write, err := s.redisx.TouchPresence(foregroundOnlineKey, lastActiveKey, activeWriteLockKey(uid), uid, expire, nowMS, activeWriteTTL, token)
		if err == nil {
			presenceUpdated = true
			if write {
				if !s.enqueueActiveWrite(activeWriteTask{uid: uid, atMS: nowMS, token: token}) {
					_, _ = s.redisx.CompareAndDelete(activeWriteLockKey(uid), token)
				}
			}
		} else {
			s.presenceErrors.Add(1)
			s.warnPressure("语伴高级Redis心跳更新失败，已尝试基础Redis降级", zap.String("uid", uid), zap.Error(err))
		}
	}
	if !presenceUpdated && s.ctx != nil && s.ctx.GetRedisConn() != nil {
		// Keep foreground status alive even if the advanced client/pool is temporarily
		// unhealthy. The ServerLib Redis wrapper has no SetNX method, so its compatible
		// fallback uses INCR + EXPIRE and deletes the key if expiry setup fails.
		c := s.ctx.GetRedisConn()
		if err := c.ZAdd(foregroundOnlineKey, float64(expire), uid); err != nil {
			s.presenceErrors.Add(1)
		}
		if err := c.Expire(foregroundOnlineKey, 24*time.Hour); err != nil {
			s.presenceErrors.Add(1)
		}
		if err := c.ZAdd(lastActiveKey, float64(nowMS), uid); err != nil {
			s.presenceErrors.Add(1)
		}
		if err := c.Expire(lastActiveKey, 10*24*time.Hour); err != nil {
			s.presenceErrors.Add(1)
		}
		lockKey := activeWriteLockKey(uid)
		count, lockErr := c.Incr(lockKey)
		if lockErr != nil {
			s.presenceErrors.Add(1)
		} else if count == 1 {
			task := activeWriteTask{uid: uid, atMS: nowMS, token: "1"}
			if expireErr := c.Expire(lockKey, activeWriteTTL); expireErr != nil {
				// Keep the original fail-open persistence behavior. The bounded queue
				// protects MySQL even though this one write no longer has a Redis guard.
				s.presenceErrors.Add(1)
				_ = c.Del(lockKey)
				task.token = ""
			}
			if !s.enqueueActiveWrite(task) && task.token == "1" {
				_ = c.Del(lockKey)
			}
		}
	}
	return &HeartbeatResp{UID: uid, LastActiveAt: nowMS, OnlineExpireAt: expire, NextHeartbeatIn: heartbeatIntervalSeconds(uid, now)}, nil
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
	im, onlineAsOf := s.imOnlineUIDsFromSnapshot(uids)
	realtime := onlineAsOf > 0 && now-onlineAsOf <= int64(imOnlineSnapshotFreshFor/time.Millisecond)
	imUnavailable := false
	if onlineAsOf <= 0 {
		// Build one complete snapshot under a single-flight mutex. A partial per-request
		// query must never mark the global snapshot as loaded, otherwise users omitted
		// from that request would be reported offline until the next full refresh.
		if err := s.ensureIMOnlineSnapshot(); err != nil {
			// Do not make every waiting request retry the same failing MySQL query. During
			// the short retry window, foreground heartbeats below become the online proxy.
			imUnavailable = true
			realtime = false
			s.warnPressure("语伴IM在线快照暂不可用，已降级为前台心跳状态", zap.Error(err))
		} else {
			im, onlineAsOf = s.imOnlineUIDsFromSnapshot(uids)
			now = time.Now().UnixMilli()
			realtime = onlineAsOf > 0 && now-onlineAsOf <= int64(imOnlineSnapshotFreshFor/time.Millisecond)
		}
	}
	fg, ra, presenceErr := s.onlinePresenceScores(uids, now)
	allowDBFallback := presenceErr == nil && !imUnavailable
	if presenceErr != nil {
		s.presenceErrors.Add(1)
		s.warnPressure("语伴在线批量读取Redis前台状态失败，已降级为IM在线快照", zap.Error(presenceErr))
		realtime = false
		// Prefer a temporary false-positive (IM connected but app may be backgrounded)
		// over marking every user offline during a Redis outage. The response is
		// explicitly non-realtime so clients retry with jitter.
		fg = make(map[string]struct{}, len(im))
		for uid := range im {
			fg[uid] = struct{}{}
		}
		ra = map[string]int64{}
	} else if imUnavailable {
		// A recent authenticated foreground heartbeat is the safest available proxy
		// when the very first IM/MySQL snapshot cannot be built.
		im = make(map[string]struct{}, len(fg))
		for uid := range fg {
			im[uid] = struct{}{}
		}
	}
	missing := make([]string, 0, len(uids))
	for _, uid := range uids {
		if _, ok := ra[uid]; !ok {
			missing = append(missing, uid)
		}
	}
	dbActive := map[string]int64{}
	// When Redis itself is unhealthy, querying partner_profiles for every online-batch
	// request would turn a cache outage into a MySQL outage. Keep the IM snapshot
	// result and temporarily omit last-active enrichment instead.
	if allowDBFallback && len(missing) > 0 {
		var activityErr error
		dbActive, activityErr = s.db.activityByUIDs(missing)
		if activityErr != nil {
			s.snapshotErrors.Add(1)
			s.warnPressure("语伴在线批量补查最后活跃失败，保留已有在线状态", zap.Error(activityErr))
			dbActive = map[string]int64{}
		}
	}
	states := make([]OnlineState, 0, len(uids))
	for _, uid := range uids {
		online := 0
		if _, a := im[uid]; a {
			if _, b := fg[uid]; b {
				online = 1
			}
		}
		last := ra[uid]
		if dbActive[uid] > last {
			last = dbActive[uid]
		}
		states = append(states, OnlineState{UID: uid, Online: online, LastActiveAt: last})
	}
	return &OnlineBatchResp{Users: states, ServerTime: now, OnlineAsOf: onlineAsOf, Realtime: realtime}, nil
}

func (s *Service) onlinePresenceScores(uids []string, nowMS int64) (map[string]struct{}, map[string]int64, error) {
	uids = uniqueIDs(uids, 50)
	foreground := make(map[string]struct{}, len(uids))
	lastActive := make(map[string]int64, len(uids))
	if len(uids) == 0 {
		return foreground, lastActive, nil
	}
	read := func(scoresOnline, scoresActive map[string]float64) {
		for uid, score := range scoresOnline {
			if int64(score) > nowMS {
				foreground[uid] = struct{}{}
			}
		}
		for uid, score := range scoresActive {
			lastActive[uid] = int64(score)
		}
	}
	var advancedErr error
	if s.redisx != nil {
		onlineScores, errOnline := s.redisx.ZScores(foregroundOnlineKey, uids)
		activeScores, errActive := s.redisx.ZScores(lastActiveKey, uids)
		if errOnline == nil && errActive == nil {
			read(onlineScores, activeScores)
			return foreground, lastActive, nil
		}
		if errOnline != nil {
			advancedErr = errOnline
		} else {
			advancedErr = errActive
		}
	}
	if advancedErr != nil {
		return foreground, lastActive, advancedErr
	}
	return foreground, lastActive, errors.New("Redis在线状态连接不可用")
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

func (s *Service) acquireCandidateSlot(wait time.Duration) bool {
	if s == nil || s.candidateSlots == nil {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case s.candidateSlots <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

func (s *Service) releaseCandidateSlot() {
	if s == nil || s.candidateSlots == nil {
		return
	}
	select {
	case <-s.candidateSlots:
	default:
	}
}

func onlineRefreshAfterMS(uid string, now time.Time) int64 {
	// The IM snapshot is refreshed every five seconds. A fixed three-second client
	// interval creates synchronized request spikes without improving freshness.
	// Spread each user across a deterministic 5-9 second window instead.
	h := uint32(2166136261)
	seed := uid + ":" + strconv.FormatInt(now.Unix()/30, 10)
	for i := 0; i < len(seed); i++ {
		h ^= uint32(seed[i])
		h *= 16777619
	}
	return 5000 + int64(h%4001)
}

func heartbeatIntervalSeconds(uid string, now time.Time) int {
	// Fixed intervals synchronize thousands of clients into periodic request spikes.
	// A deterministic 50-70 second interval spreads heartbeats while staying safely
	// below the 120-second foreground TTL.
	h := uint32(2166136261)
	seed := uid + ":" + strconv.FormatInt(now.Unix()/300, 10)
	for i := 0; i < len(seed); i++ {
		h ^= uint32(seed[i])
		h *= 16777619
	}
	return 50 + int(h%21)
}

func (s *Service) startActiveWriteWorkers(count int) {
	if s == nil || s.activeWriteQueue == nil || count <= 0 {
		return
	}
	for i := 0; i < count; i++ {
		go func() {
			for task := range s.activeWriteQueue {
				if err := s.processActiveWrite(task); err != nil && !s.spillActiveWrite(task) {
					s.releaseActiveWriteGuard(task)
					s.activeQueueDropped.Add(1)
					s.warnPressure("语伴活跃写库执行失败且无法进入Redis溢出队列", zap.String("uid", task.uid), zap.Error(err))
				}
			}
		}()
	}
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			s.drainActiveWriteSpill(100)
			<-t.C
		}
	}()
}

func (s *Service) processActiveWrite(task activeWriteTask) error {
	dbErr := s.db.touchActive(task.uid, task.atMS)
	poolErr := s.pool.syncUserE(task.uid, task.atMS)
	if dbErr != nil || poolErr != nil {
		s.activeWorkerErrors.Add(1)
		if dbErr != nil {
			return dbErr
		}
		return poolErr
	}
	return nil
}

func (s *Service) releaseActiveWriteGuard(task activeWriteTask) {
	if s == nil || task.token == "" || task.uid == "" {
		return
	}
	key := activeWriteLockKey(task.uid)
	// Token "1" is reserved for the ServerLib INCR+EXPIRE fallback path.
	if task.token == "1" {
		if s.ctx != nil && s.ctx.GetRedisConn() != nil {
			_ = s.ctx.GetRedisConn().Del(key)
		}
		return
	}
	if s.redisx != nil {
		_, _ = s.redisx.CompareAndDelete(key, task.token)
	}
}

func (s *Service) enqueueActiveWrite(task activeWriteTask) bool {
	if s == nil || s.activeWriteQueue == nil || task.uid == "" {
		return false
	}
	select {
	case s.activeWriteQueue <- task:
		return true
	default:
	}
	if s.spillActiveWrite(task) {
		return true
	}
	s.activeQueueDropped.Add(1)
	s.warnPressure("语伴活跃写库队列已满且无法写入Redis溢出队列", zap.String("uid", task.uid), zap.Int("queue_len", len(s.activeWriteQueue)), zap.Int("queue_cap", cap(s.activeWriteQueue)))
	return false
}

func (s *Service) spillActiveWrite(task activeWriteTask) bool {
	if s == nil || s.redisx == nil || strings.TrimSpace(task.uid) == "" {
		return false
	}
	payload, err := json.Marshal(activeWriteSpillPayload{UID: task.uid, AtMS: task.atMS, Token: task.token})
	if err != nil {
		return false
	}
	size, dropped, err := s.redisx.PushBoundedDetailed(activeWriteSpillKey, activeWriteSpillMax, string(payload))
	if err != nil {
		return false
	}
	s.activeQueueSpilled.Add(1)
	if dropped > 0 {
		s.activeQueueDropped.Add(uint64(dropped))
		s.warnPressure("语伴活跃写库溢出队列达到上限，最旧任务已被裁剪", zap.Int64("spill_size", size), zap.Int64("trimmed", dropped))
	}
	return true
}

func (s *Service) drainActiveWriteSpill(limit int) {
	if s == nil || s.redisx == nil || limit <= 0 {
		return
	}
	raws, err := s.redisx.PopBatch(activeWriteSpillKey, limit)
	if err != nil {
		s.activeWorkerErrors.Add(1)
		s.warnPressure("读取语伴活跃Redis溢出队列失败", zap.Error(err))
		return
	}
	if len(raws) == 0 {
		return
	}
	for i, raw := range raws {
		var payload activeWriteSpillPayload
		if json.Unmarshal([]byte(raw), &payload) != nil || strings.TrimSpace(payload.UID) == "" {
			s.activeQueueDropped.Add(1)
			continue
		}
		task := activeWriteTask{uid: payload.UID, atMS: payload.AtMS, token: payload.Token}
		if err = s.processActiveWrite(task); err != nil {
			remaining := make([]interface{}, 0, len(raws)-i)
			remaining = append(remaining, raw)
			for _, value := range raws[i+1:] {
				remaining = append(remaining, value)
			}
			_, trimmed, pushErr := s.redisx.PushBoundedDetailed(activeWriteSpillKey, activeWriteSpillMax, remaining...)
			if pushErr != nil {
				s.releaseActiveWriteGuard(task)
				s.activeQueueDropped.Add(uint64(len(remaining)))
				s.warnPressure("语伴活跃溢出任务处理失败后无法重新入队", zap.Int("lost_count", len(remaining)), zap.Error(pushErr))
			} else if trimmed > 0 {
				s.activeQueueDropped.Add(uint64(trimmed))
				s.warnPressure("语伴活跃溢出任务重新入队时发生裁剪", zap.Int64("trimmed", trimmed))
			}
			return
		}
	}
}

func (s *Service) warnPressure(message string, fields ...zap.Field) {
	if s == nil {
		return
	}
	now := time.Now().UnixMilli()
	last := s.lastPressureWarnAt.Load()
	if last > 0 && now-last < int64(30*time.Second/time.Millisecond) {
		return
	}
	if s.lastPressureWarnAt.CompareAndSwap(last, now) {
		s.Warn(message, fields...)
	}
}

func (s *Service) startRuntimeMetrics() {
	if s == nil {
		return
	}
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for range t.C {
			spilled := s.activeQueueSpilled.Swap(0)
			dropped := s.activeQueueDropped.Swap(0)
			workerErrors := s.activeWorkerErrors.Swap(0)
			lockErrors := s.jobLockErrors.Swap(0)
			snapshotErrors := s.snapshotErrors.Swap(0)
			presenceErrors := s.presenceErrors.Swap(0)
			spillLen := int64(0)
			if s.redisx != nil {
				spillLen, _ = s.redisx.LLen(activeWriteSpillKey)
			}
			if spilled+dropped+workerErrors+lockErrors+snapshotErrors+presenceErrors > 0 || spillLen > 0 || len(s.activeWriteQueue)*4 >= cap(s.activeWriteQueue)*3 {
				s.Warn("语伴列表运行压力指标",
					zap.Uint64("active_spilled", spilled), zap.Uint64("active_dropped", dropped),
					zap.Uint64("active_worker_errors", workerErrors), zap.Uint64("job_lock_errors", lockErrors),
					zap.Uint64("snapshot_errors", snapshotErrors), zap.Uint64("presence_errors", presenceErrors), zap.Int64("active_spill_len", spillLen),
					zap.Int("active_queue_len", len(s.activeWriteQueue)), zap.Int("active_queue_cap", cap(s.activeWriteQueue)))
			}
		}
	}()
}

func (s *Service) signalAssignmentOutbox() {
	if s == nil || s.assignmentWake == nil {
		return
	}
	select {
	case s.assignmentWake <- struct{}{}:
	default:
	}
}

func (s *Service) refreshIMOnlineSnapshot() error {
	if s == nil || s.db == nil {
		return errors.New("语伴在线快照服务未初始化")
	}
	s.imOnlineRefreshMu.Lock()
	defer s.imOnlineRefreshMu.Unlock()
	return s.refreshIMOnlineSnapshotLocked()
}

func (s *Service) ensureIMOnlineSnapshot() error {
	if s == nil || s.db == nil {
		return errors.New("语伴在线快照服务未初始化")
	}
	now := time.Now().UnixMilli()
	if retryAt := s.imOnlineRetryAt.Load(); retryAt > now {
		return fmt.Errorf("语伴在线快照等待重试: %dms", retryAt-now)
	}
	s.imOnlineMu.RLock()
	loadedAt := s.imOnlineLoadedAt
	s.imOnlineMu.RUnlock()
	if loadedAt > 0 {
		return nil
	}
	s.imOnlineRefreshMu.Lock()
	defer s.imOnlineRefreshMu.Unlock()
	// Another request may have completed the first full snapshot or failed and
	// entered backoff while this one was waiting for the single-flight mutex.
	s.imOnlineMu.RLock()
	loadedAt = s.imOnlineLoadedAt
	s.imOnlineMu.RUnlock()
	if loadedAt > 0 {
		return nil
	}
	now = time.Now().UnixMilli()
	if retryAt := s.imOnlineRetryAt.Load(); retryAt > now {
		return fmt.Errorf("语伴在线快照等待重试: %dms", retryAt-now)
	}
	return s.refreshIMOnlineSnapshotLocked()
}

func (s *Service) refreshIMOnlineSnapshotLocked() error {
	online, err := s.db.allIMOnlineUIDs()
	if err != nil {
		s.snapshotErrors.Add(1)
		// Negative-cache the failure for one refresh period. Without this, thousands
		// of first-load requests queue behind the mutex and each retries MySQL.
		s.imOnlineRetryAt.Store(time.Now().Add(5 * time.Second).UnixMilli())
		return err
	}
	s.imOnlineMu.Lock()
	s.imOnlineSnapshot = online
	s.imOnlineLoadedAt = time.Now().UnixMilli()
	s.imOnlineMu.Unlock()
	s.imOnlineRetryAt.Store(0)
	return nil
}

func (s *Service) imOnlineUIDsFromSnapshot(uids []string) (map[string]struct{}, int64) {
	out := make(map[string]struct{}, len(uids))
	if s == nil {
		return out, 0
	}
	s.imOnlineMu.RLock()
	loadedAt := s.imOnlineLoadedAt
	for _, uid := range uniqueIDs(uids, 0) {
		if _, ok := s.imOnlineSnapshot[uid]; ok {
			out[uid] = struct{}{}
		}
	}
	s.imOnlineMu.RUnlock()
	return out, loadedAt
}

func (s *Service) imOnlineUIDs(uids []string) (map[string]struct{}, error) {
	uids = uniqueIDs(uids, 0)
	out := make(map[string]struct{}, len(uids))
	if len(uids) == 0 {
		return out, nil
	}
	s.imOnlineMu.RLock()
	loadedAt := s.imOnlineLoadedAt
	if loadedAt > 0 {
		for _, uid := range uids {
			if _, ok := s.imOnlineSnapshot[uid]; ok {
				out[uid] = struct{}{}
			}
		}
		s.imOnlineMu.RUnlock()
		return out, nil
	}
	s.imOnlineMu.RUnlock()
	if err := s.ensureIMOnlineSnapshot(); err != nil {
		return nil, err
	}
	s.imOnlineMu.RLock()
	for _, uid := range uids {
		if _, ok := s.imOnlineSnapshot[uid]; ok {
			out[uid] = struct{}{}
		}
	}
	s.imOnlineMu.RUnlock()
	return out, nil
}

func (s *Service) onlineSnapshotLoadedAt() int64 {
	if s == nil {
		return 0
	}
	s.imOnlineMu.RLock()
	at := s.imOnlineLoadedAt
	s.imOnlineMu.RUnlock()
	return at
}

func (s *Service) runExclusiveJob(key string, ttl time.Duration, action func()) {
	if s == nil || action == nil || strings.TrimSpace(key) == "" {
		return
	}
	// Always acquire the in-process guard first. If Redis goes down while another
	// local worker still owns a distributed lock, a fallback worker must not overlap it.
	s.jobMu.Lock()
	if s.jobRunning[key] {
		s.jobMu.Unlock()
		return
	}
	s.jobRunning[key] = true
	s.jobMu.Unlock()
	defer func() {
		s.jobMu.Lock()
		delete(s.jobRunning, key)
		s.jobMu.Unlock()
	}()

	token := fmt.Sprintf("%d:%p", time.Now().UnixNano(), s)
	if s.redisx != nil {
		locked, err := s.redisx.SetNX(key, token, ttl)
		if err != nil {
			s.jobLockErrors.Add(1)
			s.warnPressure("语伴后台任务获取Redis锁失败，本轮已跳过", zap.String("lock_key", key), zap.Error(err))
			return
		}
		if !locked {
			return
		}
		defer func() { _, _ = s.redisx.CompareAndDelete(key, token) }()
	}
	action()
}

func (s *Service) cleanupPresenceIndexes() {
	if s == nil || s.redisx == nil {
		return
	}
	nowMS := time.Now().UnixMilli()
	_, _ = s.redisx.ZRemRangeByScore(foregroundOnlineKey, "-inf", strconv.FormatInt(nowMS, 10))
	lastActiveCutoff := nowMS - int64(10*24*time.Hour/time.Millisecond)
	_, _ = s.redisx.ZRemRangeByScore(lastActiveKey, "-inf", strconv.FormatInt(lastActiveCutoff, 10))
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
			s.runExclusiveJob(profileFoldLockKey, 10*time.Minute, s.foldChangedProfiles)
			<-t.C
		}
	}()
	go func() {
		time.Sleep(30 * time.Second)
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			s.runExclusiveJob(presenceCleanupLockKey, 2*time.Minute, s.cleanupPresenceIndexes)
			<-t.C
		}
	}()
	go func() {
		time.Sleep(5 * time.Second)
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			s.flushAssignmentOutbox()
			select {
			case <-t.C:
			case <-s.assignmentWake:
			}
		}
	}()
	go func() {
		time.Sleep(time.Second)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			_ = s.refreshIMOnlineSnapshot()
			<-t.C
		}
	}()
	go func() {
		time.Sleep(2 * time.Minute)
		t := time.NewTicker(15 * time.Minute)
		defer t.Stop()
		for {
			s.runExclusiveJob(maintenanceCleanupLockKey, 10*time.Minute, func() {
				_ = s.db.cleanupOperationalRows(time.Now().UnixMilli())
			})
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
