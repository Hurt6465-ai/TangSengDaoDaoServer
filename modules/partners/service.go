package partners

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	appredis "github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/redisx"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
	"go.uber.org/zap"
)

var (
	ErrGreetingSelf        = errors.New("不能给自己打招呼")
	ErrGreetingTargetMiss  = errors.New("语伴不存在")
	ErrGreetingBlacklisted = errors.New("对方暂时不能接收打招呼")
	ErrGreetingHourLimit   = errors.New("打招呼太频繁，请稍后再试")
	ErrGreetingDayLimit    = errors.New("今天打招呼次数已用完")
	ErrGreetingDuplicate   = errors.New("已经打过招呼，请过几天再试")
	ErrPendingMessageLimit = errors.New("对方还没回复，最多只能发送3条消息")
)

const (
	defaultPartnerActiveWorkers = 4
	defaultPartnerActiveQueue   = 8192
	defaultProfileSyncWorkers   = 4
	defaultProfileSyncQueue     = 4096
	defaultExposureWorkers      = 4
	defaultExposureQueue        = 8192
	partnerActiveSpillKey       = "partners:active:spill"
	partnerProfileSpillKey      = "partners:profile:spill"
	partnerExposureSpillKey     = "partners:exposure:spill"
)

var (
	partnerActiveSpillMax   = int64(partnerBoundedEnvInt("TS_DD_PARTNERS_ACTIVE_SPILL_MAX", 65536, 5000, 500000))
	partnerProfileSpillMax  = int64(partnerBoundedEnvInt("TS_DD_PARTNERS_PROFILE_SPILL_MAX", 32768, 5000, 250000))
	partnerExposureSpillMax = int64(partnerBoundedEnvInt("TS_DD_PARTNERS_EXPOSURE_SPILL_MAX", 20000, 5000, 500000))
)

type partnerActiveTask struct {
	uid        string
	atMS       int64
	online     int
	key        string
	token      string
	basicGuard bool
}

type partnerProfileSyncTask struct {
	uid         string
	key         string
	token       string
	distributed bool
}

type partnerExposureTask struct {
	uid   string
	items []ExposureItem
}

type partnerActiveSpillPayload struct {
	UID        string `json:"uid"`
	AtMS       int64  `json:"at_ms"`
	Online     int    `json:"online"`
	Key        string `json:"key"`
	Token      string `json:"token"`
	BasicGuard bool   `json:"basic_guard,omitempty"`
}

type partnerProfileSpillPayload struct {
	UID         string `json:"uid"`
	Key         string `json:"key"`
	Token       string `json:"token"`
	Distributed bool   `json:"distributed"`
}

type partnerExposureSpillPayload struct {
	UID   string         `json:"uid"`
	Items []ExposureItem `json:"items"`
}

type Service struct {
	ctx    *config.Context
	db     *db
	redisx *appredis.Client
	log.Log

	// candidateMu is kept as a fallback for very old callers, but normal candidate rebuilds
	// use candidateLocks so different users do not block each other.
	candidateMu    sync.Mutex
	candidateLocks sync.Map

	jobMu       sync.Mutex
	jobRunning  map[string]bool
	profileMu   sync.Mutex
	profileSync map[string]int64

	activeTouchQueue chan partnerActiveTask
	profileSyncQueue chan partnerProfileSyncTask
	exposureQueue    chan partnerExposureTask
	permissionWake   chan struct{}

	activeSpilled      atomic.Uint64
	activeDropped      atomic.Uint64
	profileSpilled     atomic.Uint64
	profileDropped     atomic.Uint64
	exposureSpilled    atomic.Uint64
	exposureDropped    atomic.Uint64
	workerErrors       atomic.Uint64
	jobLockErrors      atomic.Uint64
	lastPressureWarnAt atomic.Int64
}

func NewService(ctx *config.Context) *Service {
	activeWorkers := partnerBoundedEnvInt("TS_DD_PARTNERS_ACTIVE_WORKERS", defaultPartnerActiveWorkers, 1, 32)
	activeQueueSize := partnerBoundedEnvInt("TS_DD_PARTNERS_ACTIVE_QUEUE", defaultPartnerActiveQueue, 512, 65536)
	profileWorkers := partnerBoundedEnvInt("TS_DD_PARTNERS_PROFILE_WORKERS", defaultProfileSyncWorkers, 1, 16)
	profileQueueSize := partnerBoundedEnvInt("TS_DD_PARTNERS_PROFILE_QUEUE", defaultProfileSyncQueue, 256, 32768)
	exposureWorkers := partnerBoundedEnvInt("TS_DD_PARTNERS_EXPOSURE_WORKERS", defaultExposureWorkers, 1, 16)
	exposureQueueSize := partnerBoundedEnvInt("TS_DD_PARTNERS_EXPOSURE_QUEUE", defaultExposureQueue, 512, 65536)
	svc := &Service{
		ctx:              ctx,
		db:               newDB(ctx),
		redisx:           appredis.FromContext(ctx),
		Log:              log.NewTLog("partnersService"),
		jobRunning:       make(map[string]bool),
		profileSync:      make(map[string]int64),
		activeTouchQueue: make(chan partnerActiveTask, activeQueueSize),
		profileSyncQueue: make(chan partnerProfileSyncTask, profileQueueSize),
		exposureQueue:    make(chan partnerExposureTask, exposureQueueSize),
		permissionWake:   make(chan struct{}, 1),
	}
	ctx.AddMessagesListener(svc.listenerMessages)
	svc.startPartnerWorkers(activeWorkers, profileWorkers, exposureWorkers)
	svc.startRuntimeMetrics()
	svc.startBackgroundJobs()
	return svc
}

func partnerBoundedEnvInt(key string, fallback, minValue, maxValue int) int {
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

func (s *Service) startPartnerWorkers(activeWorkers, profileWorkers, exposureWorkers int) {
	for i := 0; i < activeWorkers; i++ {
		go func() {
			for task := range s.activeTouchQueue {
				if err := s.processPartnerActiveTask(task); err != nil && !s.spillActiveTask(task) {
					s.releasePartnerActiveGuard(task)
					s.activeDropped.Add(1)
					s.warnPressure("语伴活跃同步执行失败且无法进入Redis溢出队列", zap.String("uid", task.uid), zap.Error(err))
				}
			}
		}()
	}
	for i := 0; i < profileWorkers; i++ {
		go func() {
			for task := range s.profileSyncQueue {
				if err := s.processPartnerProfileTask(task); err != nil && !s.spillProfileTask(task) {
					s.releasePartnerProfileGuard(task)
					s.profileDropped.Add(1)
					s.warnPressure("语伴资料同步执行失败且无法进入Redis溢出队列", zap.String("uid", task.uid), zap.Error(err))
				}
			}
		}()
	}
	for i := 0; i < exposureWorkers; i++ {
		go func() {
			for task := range s.exposureQueue {
				if err := s.processPartnerExposureTask(task); err != nil && !s.spillExposureTask(task) {
					s.exposureDropped.Add(1)
					s.warnPressure("语伴曝光写库执行失败且无法进入Redis溢出队列", zap.String("uid", task.uid), zap.Int("item_count", len(task.items)), zap.Error(err))
				}
			}
		}()
	}
	go s.runPartnerSpillWorker(partnerActiveSpillKey, 100, s.processActiveSpillPayload)
	go s.runPartnerSpillWorker(partnerProfileSpillKey, 50, s.processProfileSpillPayload)
	go s.runPartnerSpillWorker(partnerExposureSpillKey, 100, s.processExposureSpillPayload)
}

func (s *Service) processPartnerActiveTask(task partnerActiveTask) error {
	if err := s.db.touchPartnerActive(task.uid, task.atMS, task.online); err != nil {
		s.workerErrors.Add(1)
		return err
	}
	return nil
}

func (s *Service) releasePartnerActiveGuard(task partnerActiveTask) {
	if task.token == "" || task.key == "" {
		return
	}
	if task.basicGuard {
		if s != nil && s.ctx != nil && s.ctx.GetRedisConn() != nil {
			_ = s.ctx.GetRedisConn().Del(task.key)
		}
		return
	}
	if s != nil && s.redisx != nil {
		_, _ = s.redisx.CompareAndDelete(task.key, task.token)
	}
}

func (s *Service) processPartnerProfileTask(task partnerProfileSyncTask) error {
	if err := s.db.syncPartnerProfileFromUser(task.uid); err != nil {
		s.workerErrors.Add(1)
		return err
	}
	return nil
}

func (s *Service) releasePartnerProfileGuard(task partnerProfileSyncTask) {
	s.profileMu.Lock()
	delete(s.profileSync, task.uid)
	s.profileMu.Unlock()
	if task.distributed && s.redisx != nil {
		_, _ = s.redisx.CompareAndDelete(task.key, task.token)
	}
}

func (s *Service) processPartnerExposureTask(task partnerExposureTask) error {
	if err := s.db.recordExposureItems(task.uid, task.items); err != nil {
		s.workerErrors.Add(1)
		return err
	}
	return nil
}

func (s *Service) runPartnerSpillWorker(key string, limit int, processor func(string) error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		if s.redisx != nil {
			raws, err := s.redisx.PopBatch(key, limit)
			if err != nil {
				s.workerErrors.Add(1)
				s.warnPressure("读取语伴Redis溢出队列失败", zap.String("spill_key", key), zap.Error(err))
			} else {
				for i, raw := range raws {
					if err = processor(raw); err != nil {
						remaining := make([]interface{}, 0, len(raws)-i)
						for _, value := range raws[i:] {
							remaining = append(remaining, value)
						}
						maxLen := partnerExposureSpillMax
						if key == partnerActiveSpillKey {
							maxLen = partnerActiveSpillMax
						} else if key == partnerProfileSpillKey {
							maxLen = partnerProfileSpillMax
						}
						_, trimmed, pushErr := s.redisx.PushBoundedDetailed(key, maxLen, remaining...)
						if pushErr != nil {
							lost := uint64(len(remaining))
							s.releaseLostSpillGuards(key, raws[i:])
							if key == partnerActiveSpillKey {
								s.activeDropped.Add(lost)
							} else if key == partnerProfileSpillKey {
								s.profileDropped.Add(lost)
							} else {
								s.exposureDropped.Add(lost)
							}
							s.warnPressure("语伴溢出任务处理失败后无法重新入队", zap.String("spill_key", key), zap.Uint64("lost_count", lost), zap.Error(pushErr))
						} else if trimmed > 0 {
							if key == partnerActiveSpillKey {
								s.activeDropped.Add(uint64(trimmed))
							} else if key == partnerProfileSpillKey {
								s.profileDropped.Add(uint64(trimmed))
							} else {
								s.exposureDropped.Add(uint64(trimmed))
							}
							s.warnPressure("语伴溢出任务重新入队时发生裁剪", zap.String("spill_key", key), zap.Int64("trimmed", trimmed))
						}
						break
					}
				}
			}
		}
		<-t.C
	}
}

func (s *Service) releaseLostSpillGuards(key string, raws []string) {
	for _, raw := range raws {
		if key == partnerActiveSpillKey {
			var payload partnerActiveSpillPayload
			if json.Unmarshal([]byte(raw), &payload) == nil {
				s.releasePartnerActiveGuard(partnerActiveTask{uid: payload.UID, key: payload.Key, token: payload.Token, basicGuard: payload.BasicGuard})
			}
		} else if key == partnerProfileSpillKey {
			var payload partnerProfileSpillPayload
			if json.Unmarshal([]byte(raw), &payload) == nil {
				s.releasePartnerProfileGuard(partnerProfileSyncTask{uid: payload.UID, key: payload.Key, token: payload.Token, distributed: payload.Distributed})
			}
		}
	}
}

func (s *Service) processActiveSpillPayload(raw string) error {
	var payload partnerActiveSpillPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || strings.TrimSpace(payload.UID) == "" {
		s.activeDropped.Add(1)
		return nil
	}
	return s.processPartnerActiveTask(partnerActiveTask{uid: payload.UID, atMS: payload.AtMS, online: payload.Online, key: payload.Key, token: payload.Token, basicGuard: payload.BasicGuard})
}

func (s *Service) processProfileSpillPayload(raw string) error {
	var payload partnerProfileSpillPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || strings.TrimSpace(payload.UID) == "" {
		s.profileDropped.Add(1)
		return nil
	}
	return s.processPartnerProfileTask(partnerProfileSyncTask{uid: payload.UID, key: payload.Key, token: payload.Token, distributed: payload.Distributed})
}

func (s *Service) processExposureSpillPayload(raw string) error {
	var payload partnerExposureSpillPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || strings.TrimSpace(payload.UID) == "" || len(payload.Items) == 0 {
		s.exposureDropped.Add(1)
		return nil
	}
	return s.processPartnerExposureTask(partnerExposureTask{uid: payload.UID, items: payload.Items})
}

func (s *Service) spillActiveTask(task partnerActiveTask) bool {
	if s.redisx == nil {
		return false
	}
	payload, err := json.Marshal(partnerActiveSpillPayload{UID: task.uid, AtMS: task.atMS, Online: task.online, Key: task.key, Token: task.token, BasicGuard: task.basicGuard})
	if err != nil {
		return false
	}
	size, dropped, err := s.redisx.PushBoundedDetailed(partnerActiveSpillKey, partnerActiveSpillMax, string(payload))
	if err != nil {
		return false
	}
	s.activeSpilled.Add(1)
	if dropped > 0 {
		s.activeDropped.Add(uint64(dropped))
		s.warnPressure("语伴活跃溢出队列达到上限，最旧任务已被裁剪", zap.Int64("spill_size", size), zap.Int64("trimmed", dropped))
	}
	return true
}

func (s *Service) spillProfileTask(task partnerProfileSyncTask) bool {
	if s.redisx == nil {
		return false
	}
	payload, err := json.Marshal(partnerProfileSpillPayload{UID: task.uid, Key: task.key, Token: task.token, Distributed: task.distributed})
	if err != nil {
		return false
	}
	size, dropped, err := s.redisx.PushBoundedDetailed(partnerProfileSpillKey, partnerProfileSpillMax, string(payload))
	if err != nil {
		return false
	}
	s.profileSpilled.Add(1)
	if dropped > 0 {
		s.profileDropped.Add(uint64(dropped))
		s.warnPressure("语伴资料溢出队列达到上限，最旧任务已被裁剪", zap.Int64("spill_size", size), zap.Int64("trimmed", dropped))
	}
	return true
}

func (s *Service) spillExposureTask(task partnerExposureTask) bool {
	if s.redisx == nil {
		return false
	}
	payload, err := json.Marshal(partnerExposureSpillPayload{UID: task.uid, Items: task.items})
	if err != nil {
		return false
	}
	size, dropped, err := s.redisx.PushBoundedDetailed(partnerExposureSpillKey, partnerExposureSpillMax, string(payload))
	if err != nil {
		return false
	}
	s.exposureSpilled.Add(1)
	if dropped > 0 {
		s.exposureDropped.Add(uint64(dropped))
		s.warnPressure("语伴曝光溢出队列达到上限，最旧分析记录已被裁剪", zap.Int64("spill_size", size), zap.Int64("trimmed", dropped))
	}
	return true
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
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for range t.C {
			activeSpilled, activeDropped := s.activeSpilled.Swap(0), s.activeDropped.Swap(0)
			profileSpilled, profileDropped := s.profileSpilled.Swap(0), s.profileDropped.Swap(0)
			exposureSpilled, exposureDropped := s.exposureSpilled.Swap(0), s.exposureDropped.Swap(0)
			workerErrors, lockErrors := s.workerErrors.Swap(0), s.jobLockErrors.Swap(0)
			activeSpill, profileSpill, exposureSpill := int64(0), int64(0), int64(0)
			if s.redisx != nil {
				activeSpill, _ = s.redisx.LLen(partnerActiveSpillKey)
				profileSpill, _ = s.redisx.LLen(partnerProfileSpillKey)
				exposureSpill, _ = s.redisx.LLen(partnerExposureSpillKey)
			}
			if activeSpilled+activeDropped+profileSpilled+profileDropped+exposureSpilled+exposureDropped+workerErrors+lockErrors > 0 || activeSpill+profileSpill+exposureSpill > 0 || len(s.activeTouchQueue)*4 >= cap(s.activeTouchQueue)*3 || len(s.profileSyncQueue)*4 >= cap(s.profileSyncQueue)*3 || len(s.exposureQueue)*4 >= cap(s.exposureQueue)*3 {
				s.Warn("语伴服务运行压力指标",
					zap.Uint64("active_spilled", activeSpilled), zap.Uint64("active_dropped", activeDropped),
					zap.Uint64("profile_spilled", profileSpilled), zap.Uint64("profile_dropped", profileDropped),
					zap.Uint64("exposure_spilled", exposureSpilled), zap.Uint64("exposure_dropped", exposureDropped),
					zap.Uint64("worker_errors", workerErrors), zap.Uint64("job_lock_errors", lockErrors),
					zap.Int64("active_spill_len", activeSpill), zap.Int64("profile_spill_len", profileSpill), zap.Int64("exposure_spill_len", exposureSpill),
					zap.Int("active_queue_len", len(s.activeTouchQueue)), zap.Int("profile_queue_len", len(s.profileSyncQueue)), zap.Int("exposure_queue_len", len(s.exposureQueue)))
			}
		}
	}()
}

func (s *Service) signalPartnerIMPermissionOutbox() {
	if s == nil || s.permissionWake == nil {
		return
	}
	select {
	case s.permissionWake <- struct{}{}:
	default:
	}
}

func (s *Service) List(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	s.touchActive(loginUID, time.Now().UnixMilli(), 1)
	// 用户刚改过资料但还没上传定位时，也要尽快同步到 partner_profiles。
	// 翻页和重复进入不能每次都执行一次 MySQL UPSERT。
	s.syncPartnerProfileIfDue(loginUID)
	if req.NearbyOnly {
		return s.listRealtime(loginUID, req)
	}
	list, hasMore, err := s.listFromCandidatePool(loginUID, req)
	if err == nil {
		return list, hasMore, nil
	}
	return s.listRealtime(loginUID, req)
}

func (s *Service) syncPartnerProfileIfDue(uid string) {
	uid = strings.TrimSpace(uid)
	if uid == "" || s == nil || s.db == nil {
		return
	}
	now := time.Now().UnixMilli()
	s.profileMu.Lock()
	if last := s.profileSync[uid]; last > 0 && now-last < int64(10*time.Minute/time.Millisecond) {
		s.profileMu.Unlock()
		return
	}
	if len(s.profileSync) > 10000 {
		cutoff := now - int64(20*time.Minute/time.Millisecond)
		for key, at := range s.profileSync {
			if at < cutoff {
				delete(s.profileSync, key)
			}
		}
	}
	s.profileSync[uid] = now
	s.profileMu.Unlock()

	key := "partner:profile:sync:" + uid
	token := strconv.FormatInt(now, 10)
	distributed := false
	if s.redisx != nil {
		locked, err := s.redisx.SetNX(key, token, 10*time.Minute)
		if err == nil {
			if !locked {
				return
			}
			distributed = true
		}
	}
	task := partnerProfileSyncTask{uid: uid, key: key, token: token, distributed: distributed}
	select {
	case s.profileSyncQueue <- task:
		return
	default:
	}
	if s.spillProfileTask(task) {
		return
	}
	s.profileDropped.Add(1)
	s.warnPressure("语伴资料同步队列已满且无法写入Redis溢出队列", zap.String("uid", uid), zap.Int("queue_len", len(s.profileSyncQueue)), zap.Int("queue_cap", cap(s.profileSyncQueue)))
	// Release both guards so a later list request can retry this profile sync.
	s.profileMu.Lock()
	delete(s.profileSync, uid)
	s.profileMu.Unlock()
	if distributed && s.redisx != nil {
		_, _ = s.redisx.CompareAndDelete(key, token)
	}
}

func (s *Service) listRealtime(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	queryReq := req
	queryReq.Limit = PartnerCandidateSQLLimit
	queryReq.Cursor = ""
	queryReq.Page = 1
	list, _, err := s.db.list(loginUID, queryReq)
	if err != nil {
		return nil, 0, err
	}
	viewerProfile, _ := s.db.profileMe(loginUID)
	list = RankPartnersWithSeed(list, loginUID, req.Round(), viewerProfile, req.RandomSeed())
	list = filterFeedPartners(list)
	limit := clampLimit(req.Limit)
	hasMore := 0
	if len(list) > limit {
		hasMore = 1
		list = list[:limit]
	}
	s.markServed(loginUID, list)
	return list, hasMore, nil
}

func (s *Service) listFromCandidatePool(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	limit := clampLimit(req.Limit)
	pool, err := s.getCandidatePool(loginUID, req)
	if err != nil {
		return nil, 0, err
	}

	// 第一层：正常推荐。只返回本小时没 served、当天没真实曝光过的人。
	list, hasMore, err := s.listFromPoolWindow(loginUID, req, pool, limit, false, false)
	if err != nil {
		return nil, 0, err
	}
	if len(list) > 0 {
		return list, hasMore, nil
	}

	// 第二层：池子被 served 消耗完时，只清短期 served，强制重建个人池。
	// 注意这里不能再读旧 Redis 个人池，否则测试期人少时会一直拿旧池，最终前端看到“暂无语伴”。
	s.clearServed(loginUID)
	pool, err = s.forceRebuildCandidatePoolLocked(loginUID, req)
	if err != nil {
		return nil, 0, err
	}
	list, hasMore, err = s.listFromPoolWindow(loginUID, req, pool, limit, false, false)
	if err != nil {
		return nil, 0, err
	}
	if len(list) > 0 {
		return list, hasMore, nil
	}

	// 第三层：测试期/小用户量兜底。允许“已经看过的人”低频回流，但仍然不返回：自己、已关注、已打招呼、已建立会话、拉黑/被拉黑、无照片/无语言的人。
	// 语伴不是短视频，宁愿换一批重复旧人，也不要直接让用户看到空页。
	pool = shuffleCandidateUIDs(pool, loginUID+":recycle:"+time.Now().Format("20060102150405"))
	list, hasMore, err = s.listFromPoolWindow(loginUID, req, pool, limit, true, true)
	if err != nil {
		return nil, 0, err
	}
	if len(list) > 0 {
		return list, 1, nil
	}

	// 第四层：Redis 池为空或全是失效 UID 时，退回实时查询。实时查询不按 seen_day 绝对过滤，可以把旧语伴重新洗出来。
	list, hasMore, err = s.listRealtime(loginUID, req)
	if err != nil {
		return nil, 0, err
	}
	if len(list) > 0 {
		return list, 1, nil
	}

	// 只有全站确实没有可展示语伴，或候选都已关注/打招呼/拉黑/资料不完整时，才会真正为空。
	return []*PartnerUser{}, 0, nil
}

func filterFeedPartners(list []*PartnerUser) []*PartnerUser {
	if len(list) == 0 {
		return list
	}
	out := make([]*PartnerUser, 0, len(list))
	now := time.Now().UnixMilli()
	for _, p := range list {
		if p == nil || p.UID == "" {
			continue
		}
		if shouldHidePartnerFromFeed(p, now) {
			continue
		}
		out = append(out, p)
	}
	return out
}

func shouldHidePartnerFromFeed(p *PartnerUser, now int64) bool {
	if p == nil {
		return true
	}
	if p.Follow == 1 || len(p.ProfileImages) == 0 {
		return true
	}
	// 打过招呼或已经进入待回复/已激活会话的人，不继续出现在沉浸式语伴流里。
	// 这样语伴页只负责第一条随机招呼，后续两条在聊天窗口完成。
	if p.ContactStatus == PartnerContactStatusPending || p.ContactStatus == PartnerContactStatusActive {
		return true
	}
	lastGreetAt := normalizeMillis(p.LastGreetAt)
	if lastGreetAt > 0 && now-lastGreetAt < int64(GreetingSameTargetCooldown/time.Millisecond) {
		return true
	}
	return false
}

func (s *Service) getCandidatePool(loginUID string, req listReq) ([]string, error) {
	key := s.candidatePoolKey(loginUID, req.SessionID)
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		if raw, err := s.ctx.GetRedisConn().GetString(key); err == nil && strings.TrimSpace(raw) != "" {
			var uids []string
			if json.Unmarshal([]byte(raw), &uids) == nil && len(uids) > 0 {
				return compactUIDs(uids, PartnerCandidatePoolSize), nil
			}
		}
	}
	return s.rebuildCandidatePoolLocked(loginUID, req)
}

func (s *Service) rebuildCandidatePoolLocked(loginUID string, req listReq) ([]string, error) {
	return s.withCandidatePoolLock(loginUID, req.SessionID, func() ([]string, error) {
		key := s.candidatePoolKey(loginUID, req.SessionID)
		if s.ctx != nil && s.ctx.GetRedisConn() != nil {
			if raw, err := s.ctx.GetRedisConn().GetString(key); err == nil && strings.TrimSpace(raw) != "" {
				var uids []string
				if json.Unmarshal([]byte(raw), &uids) == nil && len(uids) > 0 {
					return compactUIDs(uids, PartnerCandidatePoolSize), nil
				}
			}
		}
		return s.rebuildCandidatePool(loginUID, req)
	})
}

func (s *Service) forceRebuildCandidatePoolLocked(loginUID string, req listReq) ([]string, error) {
	return s.withCandidatePoolLock(loginUID, req.SessionID, func() ([]string, error) {
		if s.ctx != nil && s.ctx.GetRedisConn() != nil {
			_ = s.ctx.GetRedisConn().Del(s.candidatePoolKey(loginUID, req.SessionID))
		}
		return s.rebuildCandidatePool(loginUID, req)
	})
}

func (s *Service) withCandidatePoolLock(loginUID, sessionID string, fn func() ([]string, error)) ([]string, error) {
	_ = sessionID
	// Lock by user, not globally. This prevents user A rebuilding candidates from
	// blocking user B/C/D, while still avoiding duplicate rebuilds for the same user.
	lockKey := "partner_candidate_lock:" + strings.TrimSpace(loginUID)
	if lockKey == "partner_candidate_lock:" {
		// Fallback for unexpected empty uid. This path should be rare, but keeps behavior safe.
		s.candidateMu.Lock()
		defer s.candidateMu.Unlock()
		return fn()
	}
	value, _ := s.candidateLocks.LoadOrStore(lockKey, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	return fn()
}

func (s *Service) rebuildCandidatePool(loginUID string, req listReq) ([]string, error) {
	personal, err := s.db.candidateUIDs(loginUID, req, PartnerCandidateSQLLimit)
	if err != nil {
		return nil, err
	}
	global, _ := s.getGlobalCandidatePool()
	uids := mergeCandidateBuckets(personal, global, PartnerCandidatePoolSize)
	uids = shuffleCandidateUIDs(uids, loginUID+":"+req.RandomSeed())
	if s.ctx != nil && s.ctx.GetRedisConn() != nil && len(uids) > 0 {
		key := s.candidatePoolKey(loginUID, req.SessionID)
		_ = s.ctx.GetRedisConn().SetAndExpire(key, util.ToJson(uids), PartnerCandidatePoolTTL)
	}
	return uids, nil
}

func (s *Service) getGlobalCandidatePool() ([]string, error) {
	key := s.globalCandidatePoolKey()
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		if raw, err := s.ctx.GetRedisConn().GetString(key); err == nil && strings.TrimSpace(raw) != "" {
			var uids []string
			if json.Unmarshal([]byte(raw), &uids) == nil && len(uids) > 0 {
				return compactUIDs(uids, PartnerGlobalCandidateSQLLimit), nil
			}
		}
	}
	return s.rebuildGlobalCandidatePool()
}

func (s *Service) rebuildGlobalCandidatePool() ([]string, error) {
	uids, err := s.db.globalCandidateUIDs(PartnerGlobalCandidateSQLLimit)
	if err != nil {
		return nil, err
	}
	uids = compactUIDs(uids, PartnerGlobalCandidateSQLLimit)
	uids = shuffleCandidateUIDs(uids, "global:"+time.Now().Format("200601021504"))
	if s.ctx != nil && s.ctx.GetRedisConn() != nil && len(uids) > 0 {
		_ = s.ctx.GetRedisConn().SetAndExpire(s.globalCandidatePoolKey(), util.ToJson(uids), PartnerGlobalPoolTTL)
	}
	return uids, nil
}

func mergeCandidateBuckets(primary []string, secondary []string, max int) []string {
	out := make([]string, 0, max)
	seen := map[string]struct{}{}
	appendOne := func(uid string) bool {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			return false
		}
		if _, ok := seen[uid]; ok {
			return false
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
		return max > 0 && len(out) >= max
	}
	// 先放个性多路召回，再用全局 20 分钟批量池补足。
	for _, uid := range primary {
		if appendOne(uid) {
			return out
		}
	}
	for _, uid := range secondary {
		if appendOne(uid) {
			return out
		}
	}
	return out
}

func (s *Service) listFromPoolWindow(loginUID string, req listReq, pool []string, limit int, allowSeen bool, ignoreServed bool) ([]*PartnerUser, int, error) {
	window := s.pickCandidateWindowWithOptions(loginUID, pool, PartnerRankWindowSize, allowSeen, ignoreServed)
	if len(window) == 0 {
		return []*PartnerUser{}, 0, nil
	}
	list, err := s.db.listByUIDs(loginUID, req, window)
	if err != nil {
		return nil, 0, err
	}
	viewerProfile, _ := s.db.profileMe(loginUID)
	list = RankPartnersWithSeed(list, loginUID, req.Round(), viewerProfile, req.RandomSeed())
	list = filterFeedPartners(list)
	hasMore := 0
	if len(list) > limit {
		hasMore = 1
		list = list[:limit]
	} else if len(pool) > len(window) || allowSeen {
		hasMore = 1
	}
	s.markServed(loginUID, list)
	return list, hasMore, nil
}

func (s *Service) pickCandidateWindow(loginUID string, pool []string, max int) []string {
	return s.pickCandidateWindowWithOptions(loginUID, pool, max, false, false)
}

func (s *Service) pickCandidateWindowWithOptions(loginUID string, pool []string, max int, allowSeen bool, ignoreServed bool) []string {
	if max <= 0 {
		max = PartnerRankWindowSize
	}
	served := map[string]struct{}{}
	if !ignoreServed {
		served = s.redisSetMembers(s.servedKey(loginUID))
	}
	seen := map[string]struct{}{}
	if !allowSeen {
		seen = s.redisSetMembers(s.seenDayKey(loginUID))
	}
	out := make([]string, 0, max)
	for _, uid := range pool {
		uid = strings.TrimSpace(uid)
		if uid == "" || uid == loginUID {
			continue
		}
		if _, ok := served[uid]; ok {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		out = append(out, uid)
		if len(out) >= max {
			break
		}
	}
	return out
}

func (s *Service) redisSetMembers(key string) map[string]struct{} {
	out := map[string]struct{}{}
	if s.ctx == nil || s.ctx.GetRedisConn() == nil || key == "" {
		return out
	}
	members, err := s.ctx.GetRedisConn().SMembers(key)
	if err != nil {
		return out
	}
	for _, m := range members {
		m = strings.TrimSpace(m)
		if m != "" {
			out[m] = struct{}{}
		}
	}
	return out
}

func (s *Service) markServed(loginUID string, list []*PartnerUser) {
	if loginUID == "" || len(list) == 0 || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	members := make([]interface{}, 0, len(list))
	for _, p := range list {
		if p != nil && p.UID != "" {
			members = append(members, p.UID)
		}
	}
	if len(members) == 0 {
		return
	}
	key := s.servedKey(loginUID)
	_ = s.ctx.GetRedisConn().SAdd(key, members...)
	_ = s.ctx.GetRedisConn().Expire(key, PartnerServedTTL)
}

func (s *Service) clearServed(loginUID string) {
	if loginUID == "" || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	_ = s.ctx.GetRedisConn().Del(s.servedKey(loginUID))
}

func (s *Service) clearSeenDay(loginUID string) {
	if loginUID == "" || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return
	}
	_ = s.ctx.GetRedisConn().Del(s.seenDayKey(loginUID))
}

func (s *Service) candidatePoolKey(uid string, sessionID string) string {
	seed := normalizePoolSeed(sessionID)
	if seed == "" {
		seed = time.Now().Format("2006010215")
	}
	return "partner_candidate_pool:" + uid + ":" + time.Now().Format("20060102") + ":" + seed
}

func normalizePoolSeed(seed string) string {
	seed = strings.TrimSpace(seed)
	if seed == "" {
		return ""
	}
	seed = strings.ReplaceAll(seed, "-", "")
	if len(seed) > 24 {
		seed = seed[:24]
	}
	return seed
}

func shuffleCandidateUIDs(uids []string, seed string) []string {
	if len(uids) <= 1 {
		return uids
	}
	out := append([]string(nil), uids...)
	sort.SliceStable(out, func(i, j int) bool {
		a := deterministicRandom(seed+":"+out[i], 1000000)
		b := deterministicRandom(seed+":"+out[j], 1000000)
		if a == b {
			return out[i] < out[j]
		}
		return a > b
	})
	return out
}

func (s *Service) servedKey(uid string) string {
	return "partner_served:" + uid + ":" + time.Now().Format("20060102")
}

func (s *Service) seenDayKey(uid string) string {
	return "partner_seen_day:" + uid + ":" + time.Now().Format("20060102")
}

func (s *Service) seenZSetKey(uid string) string {
	return "partner_seen:" + uid
}

func compactUIDs(values []string, max int) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func (s *Service) RecordExposures(uid string, req ExposureReq) (*ExposureResp, error) {
	if uid == "" {
		return &ExposureResp{Status: 401, Msg: "未登录"}, nil
	}
	s.touchActive(uid, time.Now().UnixMilli(), 1)
	now := time.Now().UnixMilli()
	items := make([]ExposureItem, 0, len(req.Items))
	seenKeys := map[string]struct{}{}
	for _, item := range req.Items {
		toUID := strings.TrimSpace(item.ToUID)
		if toUID == "" || toUID == uid {
			continue
		}
		seenAt := normalizeMillis(item.SeenAt)
		if seenAt <= 0 || seenAt > now+int64(time.Hour/time.Millisecond) {
			seenAt = now
		}
		if item.DurationMS < 0 {
			item.DurationMS = 0
		}
		eventType := normalizeExposureEventType(item.EventType, item.DurationMS)
		key := toUID + ":" + eventType
		if _, ok := seenKeys[key]; ok {
			continue
		}
		seenKeys[key] = struct{}{}
		items = append(items, ExposureItem{ToUID: toUID, SeenAt: seenAt, DurationMS: item.DurationMS, EventType: eventType, Source: normalizeExposureSource(item.Source), PhotoIndex: item.PhotoIndex})
		if len(items) >= PartnerExposureBatchMax {
			break
		}
	}
	if len(items) == 0 {
		return &ExposureResp{Status: 200, Count: 0, Msg: "ok"}, nil
	}
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		members := make([]interface{}, 0, len(items))
		zPairs := make([]interface{}, 0, len(items)*2)
		redisSeenFailed := false
		for _, item := range items {
			if !shouldCountExposureEvent(item.EventType, item.DurationMS) {
				continue
			}
			members = append(members, item.ToUID)
			zPairs = append(zPairs, float64(item.SeenAt), item.ToUID)
		}
		zAddAdvanced := false
		if len(zPairs) > 0 {
			var zAddErr error
			if s.redisx != nil {
				zAddErr = s.redisx.ZAdd(s.seenZSetKey(uid), zPairs...)
				zAddAdvanced = zAddErr == nil
			}
			// The ServerLib wrapper used by this project is only safe here one member
			// at a time. Use it as a compatibility fallback when redisx is unavailable
			// or its connection is temporarily unhealthy.
			if s.redisx == nil || zAddErr != nil {
				zAddErr = nil
				for i := 0; i+1 < len(zPairs); i += 2 {
					if err := s.ctx.GetRedisConn().ZAdd(s.seenZSetKey(uid), zPairs[i], zPairs[i+1]); err != nil {
						zAddErr = err
						break
					}
				}
			}
			if zAddErr != nil {
				redisSeenFailed = true
			}
		}
		if len(members) > 0 {
			if err := s.ctx.GetRedisConn().SAdd(s.seenDayKey(uid), members...); err != nil {
				redisSeenFailed = true
			}
			if err := s.ctx.GetRedisConn().Expire(s.seenDayKey(uid), PartnerSeenTTL); err != nil {
				redisSeenFailed = true
			}
			var historyExpireErr error
			if zAddAdvanced {
				historyExpireErr = s.redisx.Expire(s.seenZSetKey(uid), PartnerSeenHistoryTTL)
			}
			if !zAddAdvanced || historyExpireErr != nil {
				historyExpireErr = s.ctx.GetRedisConn().Expire(s.seenZSetKey(uid), PartnerSeenHistoryTTL)
			}
			if historyExpireErr != nil {
				redisSeenFailed = true
			}
		}
		if redisSeenFailed {
			s.workerErrors.Add(1)
			s.warnPressure("语伴曝光Redis即时排除状态写入失败", zap.String("uid", uid), zap.Int("item_count", len(items)))
		}
	}
	// Exposure persistence is best-effort analytics/history. Redis seen state above is
	// authoritative for immediate recommendation de-duplication. A bounded queue avoids
	// creating thousands of goroutines when 5,000 clients report exposure together.
	queuedItems := append([]ExposureItem(nil), items...)
	exposureTask := partnerExposureTask{uid: uid, items: queuedItems}
	select {
	case s.exposureQueue <- exposureTask:
	default:
		if !s.spillExposureTask(exposureTask) {
			s.exposureDropped.Add(1)
			s.warnPressure("语伴曝光写库队列已满且无法写入Redis溢出队列", zap.String("uid", uid), zap.Int("item_count", len(queuedItems)), zap.Int("queue_len", len(s.exposureQueue)), zap.Int("queue_cap", cap(s.exposureQueue)))
		}
	}
	return &ExposureResp{Status: 200, Count: len(items), Msg: "ok"}, nil
}

func (s *Service) ProfileMe(uid string) (*ProfileMeResp, error) {
	_ = s.db.syncPartnerProfileFromUser(uid)
	s.touchActive(uid, time.Now().UnixMilli(), 1)
	return s.db.profileMe(uid)
}

func (s *Service) SaveLocation(uid string, req LocationReq) (*locationModel, error) {
	loc, err := s.db.upsertLocation(uid, req)
	if err != nil {
		return nil, err
	}
	if err := s.db.syncPartnerProfileFromUser(uid); err != nil {
		return nil, err
	}
	if err := s.db.syncPartnerLocation(uid, loc); err != nil {
		return nil, err
	}
	return loc, nil
}

func (s *Service) GreetingQuota(uid string) GreetingQuotaResp {
	stats, err := s.db.greetingStats(uid, "", time.Now().UnixMilli())
	if err != nil || stats == nil {
		return GreetingQuotaResp{GreetingDayLimit: GreetingDayLimit, GreetingDayRemaining: GreetingDayLimit, GreetingHourLimit: GreetingHourLimit, GreetingHourRemaining: GreetingDayLimit}
	}
	dayUsed := stats.DayCount
	if dayUsed < 0 {
		dayUsed = 0
	}
	dayRemaining := GreetingDayLimit - dayUsed
	if dayRemaining < 0 {
		dayRemaining = 0
	}
	// 每小时限额已关闭，保留 hour 字段只是兼容旧前端；剩余值跟随当天剩余，不参与服务端限制。
	hourUsed := 0
	hourRemaining := dayRemaining
	return GreetingQuotaResp{
		GreetingDayLimit:      GreetingDayLimit,
		GreetingDayUsed:       dayUsed,
		GreetingDayRemaining:  dayRemaining,
		GreetingHourLimit:     GreetingHourLimit,
		GreetingHourUsed:      hourUsed,
		GreetingHourRemaining: hourRemaining,
	}
}

func (s *Service) fillGreetingQuota(uid string, resp *GreetingResp) *GreetingResp {
	if resp == nil {
		return nil
	}
	resp.GreetingQuotaResp = s.GreetingQuota(uid)
	return resp
}

func (s *Service) RecordGreeting(uid string, req GreetReq) (*GreetingResp, error) {
	toUID := req.Target()
	if uid == "" || toUID == "" || uid == toUID {
		return nil, ErrGreetingSelf
	}
	exists, err := s.db.userExists(toUID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrGreetingTargetMiss
	}
	blocked, err := s.db.hasAnyBlacklist(uid, toUID)
	if err != nil {
		return nil, err
	}
	if blocked {
		return nil, ErrGreetingBlacklisted
	}

	now := time.Now().UnixMilli()
	recoveredFailedGreeting := false
	contact, err := s.db.getPartnerContact(uid, toUID)
	if err != nil {
		return nil, err
	}
	if contact != nil {
		if contact.Status == PartnerContactStatusActive {
			return s.fillGreetingQuota(uid, &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusActive, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: "已经可以聊天"}), nil
		}
		if contact.Status == PartnerContactStatusBlocked || contact.Status == PartnerContactStatusIgnored {
			return nil, ErrGreetingBlacklisted
		}
		if contact.Status == PartnerContactStatusPending {
			if contact.RequesterUID != uid {
				return s.fillGreetingQuota(uid, &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: "对方已打招呼，可以直接回复"}), nil
			}
			// Unknown results (status=0) are retried with the same stable WuKong
			// client_msg_no. Definite failures (status=2) must be rolled back instead;
			// resending them here can loop forever on a permanent IM 4xx.
			if contact.RequesterMsgCount <= 1 {
				if delivery, deliveryErr := s.db.greetingDelivery(uid, toUID); deliveryErr != nil {
					return nil, deliveryErr
				} else if delivery != nil && delivery.SendStatus == 2 {
					if rollbackErr := s.db.rollbackPendingGreetingSend(uid, toUID, delivery.LastGreetAt); rollbackErr != nil {
						return s.fillGreetingQuota(uid, &GreetingResp{Status: 503, ToUID: toUID, TargetUID: toUID, LastGreetAt: delivery.LastGreetAt, HelloSent: 0, GreetingStatus: 0, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Text: delivery.Text, Msg: "上次招呼发送失败，状态恢复中，请稍后重试"}), rollbackErr
					}
					s.signalPartnerIMPermissionOutbox()
					// The rollback transaction removed the stale pending contact and daily
					// reservation. Continue below and treat this request as a fresh greeting.
					contact = nil
					recoveredFailedGreeting = true
				} else if delivery != nil && delivery.SendStatus == 0 {
					if retryErr := s.deliverGreetingRow(delivery); retryErr != nil {
						return s.fillGreetingQuota(uid, &GreetingResp{Status: 503, ToUID: toUID, TargetUID: toUID, LastGreetAt: delivery.LastGreetAt, HelloSent: 0, GreetingStatus: 0, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Text: delivery.Text, Msg: "招呼消息投递确认中，请稍后重试"}), retryErr
					}
				}
			}
			if contact != nil {
				// Existing pending relationships must never send the second or third message
				// through this endpoint. They go exclusively through /v1/message/send so
				// client_msg_no, payload hash and pre-delivery counting share one state machine.
				status := 200
				msg := "请在聊天窗口继续发送，等待对方回复前最多3条消息"
				if contact.RequesterMsgCount >= MaxPendingGreetingMessages {
					status = 429
					msg = ErrPendingMessageLimit.Error()
				}
				return s.fillGreetingQuota(uid, &GreetingResp{Status: status, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: msg}), nil
			}
		}
	}

	stats, err := s.db.greetingStats(uid, toUID, now)
	if err != nil {
		return nil, err
	}
	if stats.DayCount >= GreetingDayLimit {
		return nil, ErrGreetingDayLimit
	}
	cooldownMs := int64(GreetingSameTargetCooldown / time.Millisecond)
	if stats.LastTargetGreetAt > 0 && now-stats.LastTargetGreetAt < cooldownMs {
		resp := &GreetingResp{Status: 429, ToUID: toUID, TargetUID: toUID, LastGreetAt: stats.LastTargetGreetAt, NextAllowedAt: stats.LastTargetGreetAt + cooldownMs, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: MaxPendingGreetingMessages, MaxGreetingCount: MaxPendingGreetingMessages, Msg: ErrGreetingDuplicate.Error()}
		return s.fillGreetingQuota(uid, resp), ErrGreetingDuplicate
	}
	if !recoveredFailedGreeting && !s.allowNewGreetingRate(uid, now) {
		return nil, ErrGreetingHourLimit
	}
	reservedDaily, _, err := s.db.reserveGreetingDailyTarget(uid, toUID, now, GreetingDayLimit)
	if err != nil {
		return nil, err
	}
	if !reservedDaily {
		// The same target was already reserved in this recommendation day. This also
		// closes the race where two first-greeting requests both observed no contact.
		// Failed sends remove the daily target, so an existing row must not send again.
		return nil, ErrGreetingDuplicate
	}
	releaseDaily := func() {
		if reservedDaily {
			_ = s.db.releaseGreetingDailyTarget(uid, toUID, now)
		}
	}
	text := normalizeGreetingText(req.Text)
	source := normalizeGreetingSource(req.Source)
	resp, err := s.db.recordGreeting(uid, toUID, text, source)
	if err != nil {
		releaseDaily()
		return nil, err
	}
	if err := s.db.ensurePendingContact(uid, toUID, resp.LastGreetAt); err != nil {
		_ = s.db.markGreetingSendStatus(uid, toUID, resp.LastGreetAt, 2, err.Error())
		releaseDaily()
		return nil, err
	}
	resp.RequesterMsgCount = 1
	resp.MaxGreetingCount = MaxPendingGreetingMessages
	// Permission changes are committed to partner_im_permission_outbox together with
	// the pending relationship. deliverGreetingRow applies this exact pair immediately;
	// do not drain up to 50 unrelated outbox rows on the user request path.
	delivery := &greetingDeliveryRow{UID: uid, ToUID: toUID, Text: resp.Text, Source: source, LastGreetAt: resp.LastGreetAt, SendStatus: 0}
	if err := s.deliverGreetingRow(delivery); err != nil {
		// Permission, search, timeout and post-send database errors are all retryable.
		// Keep the pending relation and daily reservation; retries use the same stable
		// IM client_msg_no, so this cannot create a second visible greeting.
		if !isDefiniteGreetingSendError(err) {
			return s.fillGreetingQuota(uid, &GreetingResp{Status: 503, ToUID: toUID, TargetUID: toUID, LastGreetAt: resp.LastGreetAt, HelloSent: 0, GreetingStatus: 0, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: 1, MaxGreetingCount: MaxPendingGreetingMessages, Text: resp.Text, Msg: "招呼消息投递确认中，请稍后重试"}), err
		}
		// A definite IM 4xx means the message was not accepted and is safe to roll back.
		// Contact counters, permission cleanup and the daily reservation are committed
		// atomically so a retry cannot decrement any of them twice.
		if rollbackErr := s.db.rollbackPendingGreetingSend(uid, toUID, resp.LastGreetAt); rollbackErr != nil {
			return s.fillGreetingQuota(uid, &GreetingResp{Status: 503, ToUID: toUID, TargetUID: toUID, LastGreetAt: resp.LastGreetAt, HelloSent: 0, GreetingStatus: 0, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: 1, MaxGreetingCount: MaxPendingGreetingMessages, Text: resp.Text, Msg: "招呼发送失败，状态回滚处理中，请稍后重试"}), rollbackErr
		}
		s.signalPartnerIMPermissionOutbox()
		return nil, err
	}
	return s.fillGreetingQuota(uid, resp), nil
}

func (s *Service) allowNewGreetingRate(uid string, nowMS int64) bool {
	if uid == "" || s == nil || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return false
	}
	const script = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local member = ARGV[2]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - 60000)
local last = redis.call('ZREVRANGE', key, 0, 0, 'WITHSCORES')
if #last >= 2 and now - tonumber(last[2]) < 10000 then
  return -1
end
if redis.call('ZCARD', key) >= 3 then
  return -2
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, 120000)
return 1`
	key := "partner:greeting:rate:{" + uid + "}"
	if s.redisx == nil {
		return false
	}
	result, err := s.redisx.EvalInt(script, []string{key}, nowMS, fmt.Sprintf("%d", time.Now().UnixNano()))
	if err != nil {
		// 新建陌生关系属于安全边界；Redis异常时拒绝本次操作，避免短时限流被绕过。
		return false
	}
	return result == 1
}

func normalizeGreetingSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "partner_browse"
	}
	if utf8.RuneCountInString(source) > 32 {
		source = string([]rune(source)[:32])
	}
	return source
}

func normalizeGreetingText(text string) string {
	text = strings.TrimSpace(text)
	// App may send its localized default text. Treat all built-in defaults as an empty
	// greeting so the server can pick a short random phrase and keep the product
	// behavior consistent across Chinese, English and Burmese clients.
	if text == "" || isDefaultGreetingText(text) {
		text = randomGreetingText()
	}
	runes := []rune(text)
	if len(runes) > GreetingMaxTextLen {
		text = string(runes[:GreetingMaxTextLen])
	}
	return text
}

func isDefaultGreetingText(text string) bool {
	switch strings.TrimSpace(text) {
	case "你好，我们可以一起练语言吗？",
		"Hi, can we practice languages together?",
		"မင်္ဂလာပါ၊ ဘာသာစကား အတူလေ့ကျင့်လို့ရမလား?":
		return true
	default:
		return false
	}
}

func randomGreetingText() string {
	texts := []string{
		"你好，可以一起练语言吗？",
		"嗨，我们可以互相练习一下吗？",
		"你好，我正在找语伴，可以聊几句吗？",
		"嗨，一起练口语吗？",
		"你好，我想练习你的语言，可以吗？",
	}
	idx := int(time.Now().UnixNano() % int64(len(texts)))
	return texts[idx]
}

func (s *Service) addPartnerWhitelist(uid, toUID string) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	// pending 阶段只允许接收方直接回复发起方。
	// 发起方的第 2、3 条消息必须经过 /v1/message/send，在悟空IM投递前原子校验。
	return s.ctx.IMWhitelistAdd(config.ChannelWhitelistReq{
		ChannelReq: config.ChannelReq{ChannelID: uid, ChannelType: common.ChannelTypePerson.Uint8()},
		UIDs:       []string{toUID},
	})
}

func (s *Service) removePartnerSenderWhitelist(uid, toUID string) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	return s.ctx.IMWhitelistRemove(config.ChannelWhitelistReq{
		ChannelReq: config.ChannelReq{ChannelID: toUID, ChannelType: common.ChannelTypePerson.Uint8()},
		UIDs:       []string{uid},
	})
}

type stableGreetingSendReq struct {
	Header      config.MsgHeader `json:"header"`
	FromUID     string           `json:"from_uid"`
	ChannelID   string           `json:"channel_id"`
	ChannelType uint8            `json:"channel_type"`
	Payload     []byte           `json:"payload"`
	ClientMsgNo string           `json:"client_msg_no"`
}

type stableGreetingSendResp struct {
	MessageID   int64  `json:"message_id"`
	MessageSeq  uint32 `json:"message_seq"`
	ClientMsgNo string `json:"client_msg_no"`
	Data        struct {
		MessageID   int64  `json:"message_id"`
		MessageSeq  uint32 `json:"message_seq"`
		ClientMsgNo string `json:"client_msg_no"`
	} `json:"data"`
}

type greetingSendError struct {
	uncertain bool
	err       error
}

func (e *greetingSendError) Error() string {
	if e == nil || e.err == nil {
		return "IM发送失败"
	}
	return e.err.Error()
}

func isUncertainGreetingSendError(err error) bool {
	var target *greetingSendError
	return errors.As(err, &target) && target.uncertain
}

func isDefiniteGreetingSendError(err error) bool {
	var target *greetingSendError
	return errors.As(err, &target) && !target.uncertain
}

func greetingIMClientMsgNo(uid, toUID string, at int64) string {
	sum := sha256.Sum256([]byte(uid + "\x00" + toUID + "\x00" + strconv.FormatInt(at, 10)))
	return "partner-greeting:" + hex.EncodeToString(sum[:])[:52]
}

func (s *Service) greetingAlreadyDelivered(uid, toUID string, at int64) (bool, error) {
	clientNo := greetingIMClientMsgNo(uid, toUID, at)
	resp, err := s.ctx.IMSearchMessages(&config.MsgSearchReq{LoginUID: uid, ChannelID: toUID, ChannelType: common.ChannelTypePerson.Uint8(), ClientMsgNos: []string{clientNo}})
	if err != nil {
		return false, err
	}
	return resp != nil && len(resp.Messages) > 0, nil
}

func (s *Service) ensurePendingPermissionsNow(requesterUID, receiverUID string) error {
	if requesterUID == "" || receiverUID == "" || requesterUID == receiverUID {
		return errors.New("无效的语伴临时会话")
	}
	// The critical direction is removed first: while pending, the requester must
	// never be able to send directly into the receiver's personal channel.
	if err := s.removePartnerSenderWhitelist(requesterUID, receiverUID); err != nil {
		return err
	}
	// The receiver must be allowed to reply to the requester's channel.
	if err := s.addPartnerWhitelist(requesterUID, receiverUID); err != nil {
		return err
	}
	return nil
}

func (s *Service) deliverGreetingRow(row *greetingDeliveryRow) error {
	if row == nil || row.UID == "" || row.ToUID == "" || row.LastGreetAt <= 0 {
		return errors.New("无效的招呼投递记录")
	}
	// Search first. A message may already be visible while the final status update
	// failed. Reapplying pending permissions before this check can remove one side of
	// an already-active conversation when the receiver replied in the meantime.
	delivered, searchErr := s.greetingAlreadyDelivered(row.UID, row.ToUID, row.LastGreetAt)
	if searchErr != nil {
		err := &greetingSendError{uncertain: true, err: searchErr}
		_ = s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 0, err.Error())
		return err
	}
	if delivered {
		return s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 1, "")
	}

	pending, stateErr := s.db.hasPendingGreetingContact(row.UID, row.ToUID)
	if stateErr != nil {
		err := &greetingSendError{uncertain: true, err: stateErr}
		_ = s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 0, err.Error())
		return err
	}
	if !pending {
		err := &greetingSendError{uncertain: false, err: errors.New("招呼临时会话不存在或状态已变化")}
		if markErr := s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 2, err.Error()); markErr != nil {
			return &greetingSendError{uncertain: true, err: fmt.Errorf("记录招呼会话状态失败: %w", markErr)}
		}
		return err
	}

	if err := s.ensurePendingPermissionsNow(row.UID, row.ToUID); err != nil {
		_ = s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 0, err.Error())
		return err
	}
	// The exact pending-transition permissions have just been applied synchronously.
	// Mark their durable tasks done to avoid two redundant WuKongIM calls per greeting.
	_ = s.db.markIMPermissionDoneByKeys([]string{
		permissionTransitionKey("pending:add", row.UID, row.ToUID, row.LastGreetAt),
		permissionTransitionKey("pending:remove", row.ToUID, row.UID, row.LastGreetAt),
	})
	if err := s.sendGreetingMessage(row.UID, row.ToUID, row.Text, row.Source, row.LastGreetAt, 1); err != nil {
		status := 2
		if isUncertainGreetingSendError(err) {
			status = 0
		}
		if markErr := s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, status, err.Error()); markErr != nil {
			// The IM result may be definite, but without a durable status transition the
			// rollback worker cannot safely claim it. Keep the delivery retryable with
			// the same client_msg_no until the database accepts the state update.
			return &greetingSendError{uncertain: true, err: fmt.Errorf("记录招呼发送状态失败: %w; IM结果: %v", markErr, err)}
		}
		return err
	}
	return s.db.markGreetingSendStatus(row.UID, row.ToUID, row.LastGreetAt, 1, "")
}

func (s *Service) reconcileGreetingDeliveries() {
	s.runExclusiveJob("partner:job:greeting_delivery", 5*time.Minute, func() {
		// A definite IM rejection is not resendable. Finish any rollback that failed
		// on the request path before processing new delivery retries.
		failedRows, err := s.db.failedGreetingRollbacks(100)
		if err != nil {
			return
		}
		for _, row := range failedRows {
			if rollbackErr := s.db.rollbackPendingGreetingSend(row.UID, row.ToUID, row.LastGreetAt); rollbackErr != nil {
				break
			}
			s.signalPartnerIMPermissionOutbox()
		}

		rows, err := s.db.pendingGreetingDeliveries(20)
		if err != nil {
			return
		}
		consecutiveUncertainErrors := 0
		for _, row := range rows {
			deliveryErr := s.deliverGreetingRow(row)
			if deliveryErr == nil {
				consecutiveUncertainErrors = 0
				continue
			}
			if isDefiniteGreetingSendError(deliveryErr) {
				if rollbackErr := s.db.rollbackPendingGreetingSend(row.UID, row.ToUID, row.LastGreetAt); rollbackErr == nil {
					s.signalPartnerIMPermissionOutbox()
				}
				consecutiveUncertainErrors = 0
				continue
			}
			// Permission setup and network errors may be plain errors rather than a
			// greetingSendError. Any non-definite failure is treated as transient here.
			consecutiveUncertainErrors++
			if consecutiveUncertainErrors >= 3 {
				break
			}
		}
	})
}

func (s *Service) sendGreetingMessage(uid, toUID, text, source string, at int64, requesterMsgCount int) error {
	if uid == "" || toUID == "" || text == "" {
		return errors.New("招呼消息参数不能为空")
	}
	payload := []byte(util.ToJson(map[string]interface{}{
		"content":                text,
		"type":                   common.Text,
		"partner_greeting":       1,
		"source":                 source,
		"requester_uid":          uid,
		"partner_contact_status": PartnerContactStatusPending,
		"requester_msg_count":    requesterMsgCount,
		"max_greeting_count":     MaxPendingGreetingMessages,
		"created_at":             at,
	}))
	clientNo := greetingIMClientMsgNo(uid, toUID, at)
	body, err := json.Marshal(stableGreetingSendReq{Header: config.MsgHeader{RedDot: 1}, FromUID: uid, ChannelID: toUID, ChannelType: common.ChannelTypePerson.Uint8(), Payload: payload, ClientMsgNo: clientNo})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(s.ctx.GetConfig().WuKongIM.APIURL, "/")+"/message/send", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return &greetingSendError{uncertain: true, err: err}
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return &greetingSendError{uncertain: true, err: readErr}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message := strings.TrimSpace(string(data))
		message = truncatePartnerRunes(message, 300)
		lower := strings.ToLower(message)
		uncertain := resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests || strings.Contains(lower, "duplicate") || strings.Contains(lower, "client_msg_no") || strings.Contains(message, "重复") || strings.Contains(message, "已存在")
		return &greetingSendError{uncertain: uncertain, err: fmt.Errorf("IM服务返回状态[%d]: %s", resp.StatusCode, message)}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	var parsed stableGreetingSendResp
	if err = json.Unmarshal(data, &parsed); err != nil {
		return &greetingSendError{uncertain: true, err: err}
	}
	return nil
}

func (s *Service) listenerMessages(messages []*config.MessageResp) {
	if len(messages) == 0 {
		return
	}
	for _, msg := range messages {
		if msg == nil || msg.ChannelType != common.ChannelTypePerson.Uint8() || msg.FromUID == "" || msg.ChannelID == "" || msg.FromUID == msg.ChannelID {
			continue
		}
		if isPartnerGreetingPayload(msg) {
			continue
		}
		createdAt := int64(msg.Timestamp) * 1000
		if createdAt <= 0 {
			createdAt = time.Now().UnixMilli()
		}
		s.touchActive(msg.FromUID, createdAt, 1)
		s.touchActive(msg.ChannelID, createdAt, 0)

		contact, _ := s.db.getPartnerContact(msg.FromUID, msg.ChannelID)
		if contact != nil && contact.Status == PartnerContactStatusPending && contact.RequesterUID == msg.FromUID {
			if businessClientMsgNo := partnerPendingGatewayClientMsgNo(msg); businessClientMsgNo != "" {
				// /v1/message/send 已在投递前完成原子计数。Webhook 只做投递确认，不能再次+1。
				_ = s.db.markPendingMessageDelivered(msg.FromUID, businessClientMsgNo, msg.ClientMsgNo, msg.MessageID, createdAt)
				continue
			}
			// 旧客户端或异常白名单穿透的兜底。正常新客户端不会走到这里。
			count, err := s.db.incrementPendingRequesterMsgCount(msg.FromUID, msg.ChannelID, createdAt)
			if err == nil && count >= MaxPendingGreetingMessages {
				_ = s.removePartnerSenderWhitelist(msg.FromUID, msg.ChannelID)
			}
			continue
		}

		activated, _ := s.db.activateContactOnReply(msg.FromUID, msg.ChannelID, createdAt)
		if activated {
			s.signalPartnerIMPermissionOutbox()
		}
	}
}

func partnerPendingGatewayClientMsgNo(msg *config.MessageResp) string {
	if msg == nil {
		return ""
	}
	payload, err := msg.GetPayloadMap()
	if err != nil || payload == nil || fmt.Sprint(payload["partner_pending_gateway"]) != "1" {
		return ""
	}
	value := strings.TrimSpace(fmt.Sprint(payload["partner_client_msg_no"]))
	if value == "" || len(value) > 100 {
		return ""
	}
	return value
}

func isPartnerGreetingPayload(msg *config.MessageResp) bool {
	payload, err := msg.GetPayloadMap()
	if err != nil || payload == nil {
		return false
	}
	if fmt.Sprint(payload["partner_greeting"]) == "1" {
		return true
	}
	if fmt.Sprint(payload["source"]) == "partner_browse" && fmt.Sprint(payload["requester_uid"]) != "" {
		return true
	}
	return false
}

func (s *Service) globalCandidatePoolKey() string {
	// 20 分钟一个全局候选池。正常用户请求只从这个池 + 个性池里切片，不反复扫大表。
	now := time.Now()
	minuteBucket := (now.Minute() / 20) * 20
	return "partner_global_candidate_pool:" + now.Format("2006010215") + fmt.Sprintf("%02d", minuteBucket)
}

func (s *Service) startBackgroundJobs() {
	go func() {
		time.Sleep(3 * time.Second)
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			s.reconcilePendingWhitelists()
			<-ticker.C
		}
	}()
	go func() {
		time.Sleep(3 * time.Second)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			s.flushPartnerIMPermissionOutbox()
			select {
			case <-ticker.C:
			case <-s.permissionWake:
			}
		}
	}()
	go func() {
		time.Sleep(5 * time.Second)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			s.reconcileGreetingDeliveries()
			<-ticker.C
		}
	}()
	go func() {
		time.Sleep(5 * time.Second)
		ticker := time.NewTicker(PartnerGlobalPoolRefresh)
		defer ticker.Stop()
		for {
			s.runCandidateWarmupOnce()
			<-ticker.C
		}
	}()
	go func() {
		time.Sleep(2 * time.Minute)
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			s.runExclusiveJob("partner:job:operational_cleanup", 8*time.Minute, func() {
				_ = s.db.cleanupPartnerOperationalRows(time.Now().UnixMilli())
			})
			<-ticker.C
		}
	}()
}

func (s *Service) reconcilePendingWhitelists() {
	s.runExclusiveJob("partner:job:pending_whitelist_reconcile", 30*time.Minute, func() {
		if s == nil || s.db == nil {
			return
		}
		afterRequester, afterReceiver := "", ""
		for {
			pairs, err := s.db.pendingContactPairsAfter(afterRequester, afterReceiver, 500)
			if err != nil || len(pairs) == 0 {
				return
			}
			valid := make([]pendingContactPair, 0, len(pairs))
			for _, pair := range pairs {
				// The cursor must advance even across malformed legacy rows, otherwise one
				// bad tail row can make every subsequent repair pass read the same page.
				afterRequester, afterReceiver = pair.RequesterUID, pair.ReceiverUID
				if pair.RequesterUID == "" || pair.ReceiverUID == "" || pair.RequesterUID == pair.ReceiverUID {
					continue
				}
				valid = append(valid, pair)
			}
			if err = s.db.enqueuePendingPermissionRepairs(valid, time.Now().UnixMilli()); err != nil {
				return
			}
			if len(pairs) < 500 {
				break
			}
		}
		s.signalPartnerIMPermissionOutbox()
	})
}

func (s *Service) flushPartnerIMPermissionOutbox() {
	s.runExclusiveJob("partner:job:im_permission_outbox", 15*time.Minute, func() {
		if s == nil || s.db == nil || s.ctx == nil {
			return
		}
		rows, err := s.db.pendingIMPermissionTasks(time.Now().UnixMilli(), 50)
		if err != nil {
			return
		}
		consecutiveApplyErrors := 0
		for _, row := range rows {
			desired, desiredErr := s.db.desiredPartnerIMPermission(row.ChannelUID, row.MemberUID)
			if desiredErr != nil {
				_ = s.db.markIMPermissionRetry(row.ID, row.Attempts, desiredErr.Error())
				// A database error is normally shared by all rows in this batch.
				break
			}
			req := config.ChannelWhitelistReq{ChannelReq: config.ChannelReq{ChannelID: row.ChannelUID, ChannelType: common.ChannelTypePerson.Uint8()}, UIDs: []string{row.MemberUID}}
			var applyErr error
			if desired == "remove" {
				applyErr = s.ctx.IMWhitelistRemove(req)
			} else {
				applyErr = s.ctx.IMWhitelistAdd(req)
			}
			if applyErr == nil {
				_ = s.db.markIMPermissionDone(row.ID)
				consecutiveApplyErrors = 0
				continue
			}
			_ = s.db.markIMPermissionRetry(row.ID, row.Attempts, applyErr.Error())
			consecutiveApplyErrors++
			// During a shared WuKongIM outage, avoid issuing the remaining 47 calls.
			if consecutiveApplyErrors >= 3 {
				break
			}
		}
	})
}

func (s *Service) runCandidateWarmupOnce() {
	s.runExclusiveJob("partner:job:candidate_warmup", 15*time.Minute, func() {
		if s == nil || s.db == nil {
			return
		}
		_, _ = s.db.syncRecentPartnerProfiles(PartnerGlobalCandidateSQLLimit)
		_ = s.db.syncOnlineProfiles()
		_, _ = s.rebuildGlobalCandidatePool()
	})
}

func (s *Service) runExclusiveJob(key string, ttl time.Duration, action func()) {
	if s == nil || action == nil || strings.TrimSpace(key) == "" {
		return
	}
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

	token := fmt.Sprintf("%d", time.Now().UnixNano())
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

func (s *Service) touchActive(uid string, at int64, online int) {
	uid = strings.TrimSpace(uid)
	if uid == "" || s == nil || s.db == nil {
		return
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	// partner_profiles.last_active_at is used for recommendation freshness, not message
	// permission. Use one atomic SETNX and a bounded worker queue so a synchronized
	// 5,000-user foreground/message burst cannot create 5,000 MySQL goroutines.
	key := "partner:active:touch:" + uid
	token := strconv.FormatInt(at, 10)
	lockedByAdvanced := false
	if s.redisx != nil {
		locked, err := s.redisx.SetNX(key, token, 10*time.Minute)
		if err == nil {
			if !locked {
				return
			}
			lockedByAdvanced = true
		}
	}
	basicGuard := false
	if !lockedByAdvanced {
		token = ""
		// Preserve the original ServerLib-compatible Redis throttle when the
		// advanced pool is unavailable. This prevents a Redis pool incident from
		// turning every chat/list event into a MySQL UPDATE.
		if s.ctx != nil && s.ctx.GetRedisConn() != nil {
			conn := s.ctx.GetRedisConn()
			count, err := conn.Incr(key)
			if err == nil {
				if count > 1 {
					return
				}
				if err = conn.Expire(key, 10*time.Minute); err != nil {
					_ = conn.Del(key)
				} else {
					token = "1"
					basicGuard = true
				}
			}
		}
	}
	task := partnerActiveTask{uid: uid, atMS: at, online: online, key: key, token: token, basicGuard: basicGuard}
	select {
	case s.activeTouchQueue <- task:
		return
	default:
	}
	if s.spillActiveTask(task) {
		return
	}
	s.activeDropped.Add(1)
	s.warnPressure("语伴活跃同步队列已满且无法写入Redis溢出队列", zap.String("uid", uid), zap.Int("queue_len", len(s.activeTouchQueue)), zap.Int("queue_cap", cap(s.activeTouchQueue)))
	// Release the dedupe key so a later event can retry once capacity recovers.
	s.releasePartnerActiveGuard(task)
}

func cursorToken() string {
	return time.Now().Format("20060102") + ":" + strconv.FormatInt(time.Now().UnixMilli(), 10)
}
