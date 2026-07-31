package dating

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/user"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
)

const serviceLockShards = 256

type Service struct {
	ctx               *config.Context
	db                *db
	userService       user.IService
	locks             [serviceLockShards]sync.Mutex
	lastServedCleanup int64
}

func NewService(ctx *config.Context) *Service {
	return &Service{ctx: ctx, db: newDB(ctx), userService: user.NewService(ctx)}
}

// lock serializes quota and pair mutations inside one server process. A multi-instance
// deployment should replace this with a Redis/distributed lock or transactional quota rows.
func (s *Service) lock(key string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	mu := &s.locks[int(h.Sum32())%len(s.locks)]
	mu.Lock()
	return mu.Unlock
}

func applyQuotaToProfile(profile *DatingProfileResp, q QuotaResp) {
	if profile == nil {
		return
	}
	profile.LikeLimit = q.LikeLimit
	profile.LikeUsed = q.LikeUsed
	profile.LikeRemaining = q.LikeRemaining
	profile.FavoriteLimit = q.FavoriteLimit
	profile.FavoriteUsed = q.FavoriteUsed
	profile.FavoriteRemaining = q.FavoriteRemaining
	profile.RewindLimit = q.RewindLimit
	profile.RewindUsed = q.RewindUsed
	profile.RewindRemaining = q.RewindRemaining
}

func (s *Service) applyCurrentQuota(uid string, profile *DatingProfileResp) {
	if profile == nil {
		return
	}
	likeUsed, favoriteUsed, rewindUsed, err := s.db.countTodayActions(uid)
	if err != nil {
		return
	}
	applyQuotaToProfile(profile, quotaResp(profile.Sex, likeUsed, favoriteUsed, rewindUsed))
}

func (s *Service) profileForAction(uid string) (*DatingProfileResp, error) {
	profile, err := s.db.profileForUse(uid)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	s.db.touchActive(uid, now)
	if profile != nil {
		profile.LastActiveAt = now
	}
	return profile, nil
}

func (s *Service) ProfileMe(uid string) (*DatingProfileResp, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New("请先登录")
	}
	profile, err := s.db.profileMe(uid)
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	s.db.touchActive(uid, now)
	if profile != nil {
		profile.LastActiveAt = now
	}
	s.applyCurrentQuota(uid, profile)
	return profile, nil
}

func (s *Service) CopyPartnerProfile(uid string) (*DatingProfileResp, error) {
	// Compatibility endpoint. Shared fields are now synchronized automatically.
	return s.ProfileMe(uid)
}

func (s *Service) SaveProfile(uid string, req SaveProfileReq) (*DatingProfileResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	unlock := s.lock("profile:" + uid)
	defer unlock()

	current, err := s.db.profileMe(uid)
	if err != nil {
		return nil, err
	}
	if current != nil && current.Status != DatingProfileNormal {
		return current, ErrDatingProfileUnavailable
	}

	rawIntent := strings.TrimSpace(req.Intent)
	if rawIntent == "" {
		rawIntent = strings.TrimSpace(req.RelationshipGoal)
	}
	if rawIntent == "" && current != nil {
		rawIntent = current.Intent
	}
	intent, ok := normalizeDatingIntent(rawIntent)
	if !ok {
		return current, ErrDatingInvalidIntent
	}
	req.Intent = intent
	req.RelationshipGoal = intent

	var photos []string
	if req.Photos == nil && req.ProfileImages == nil {
		if current != nil {
			photos = current.Photos
		}
	} else if req.Photos != nil {
		photos = req.Photos
	} else {
		photos = req.ProfileImages
	}
	cleanPhotos, ok := sanitizeDatingPhotos(uid, photos)
	if !ok {
		return current, ErrDatingInvalidPhoto
	}
	req.Photos = cleanPhotos
	req.ProfileImages = cleanPhotos

	// 新版本只使用一套照片；兼容字段 card_photos 返回与 photos 相同的 URL。
	req.CardPhotos = append([]string(nil), cleanPhotos...)
	if req.ShowDistance == nil {
		value := 1
		if current != nil {
			value = current.ShowDistance
		}
		req.ShowDistance = &value
	}
	if req.AllowVoice == nil {
		value := 1
		if current != nil {
			value = current.AllowVoice
		}
		req.AllowVoice = &value
	}
	if req.AllowVideo == nil {
		value := 0
		if current != nil {
			value = current.AllowVideo
		}
		req.AllowVideo = &value
	}

	// 保存资料本身不改展示状态，避免“先关闭、再开启”的两次写入在中途失败后
	// 把原本展示中的用户意外留在关闭状态。user_paused 是跨设备权威状态。
	profile, err := s.db.saveProfile(uid, req)
	if err != nil {
		return nil, err
	}
	if profile == nil || profile.Status != DatingProfileNormal {
		return profile, ErrDatingProfileUnavailable
	}
	desiredEnabled := 0
	if profile.Complete && profile.Age >= 18 && profile.UserPaused != 1 {
		desiredEnabled = 1
	}
	if profile.Enabled != desiredEnabled {
		profile, err = s.db.setEnabled(uid, desiredEnabled, profile.UserPaused)
		if err != nil {
			return nil, err
		}
	}
	s.applyCurrentQuota(uid, profile)
	return profile, nil
}

func (s *Service) EnableProfile(uid string, enabled int) (*DatingProfileResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	unlock := s.lock("profile:" + uid)
	defer unlock()
	profile, err := s.db.profileMe(uid)
	if err != nil {
		return nil, err
	}
	if profile == nil {
		return nil, ErrDatingProfileIncomplete
	}
	if enabled == 1 {
		if profile.Status != DatingProfileNormal {
			return profile, ErrDatingProfileUnavailable
		}
		if profile.Age < 18 {
			return profile, ErrDatingAgeTooSmall
		}
		if !profile.Complete {
			return profile, ErrDatingProfileIncomplete
		}
	}
	userPaused := 0
	if enabled != 1 {
		userPaused = 1
	}
	profile, err = s.db.setEnabled(uid, enabled, userPaused)
	if err == nil && enabled == 1 && (profile == nil || profile.Enabled != 1 || profile.Status != DatingProfileNormal) {
		return profile, ErrDatingProfileUnavailable
	}
	if err == nil {
		s.applyCurrentQuota(uid, profile)
	}
	return profile, err
}

func (s *Service) SaveLocation(uid string, req LocationReq) (*DatingProfileResp, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New("请先登录")
	}
	lat, lng := req.NormalizedLatLng()
	if !validLatLng(lat, lng) {
		return nil, errors.New("定位参数无效")
	}
	return s.db.upsertLocation(uid, req)
}

func (s *Service) Recommend(uid string, req RecommendReq) (*RecommendResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	now := time.Now().UnixMilli()
	s.maybeScheduleServedCleanup(now)
	viewer, err := s.profileForAction(uid)
	if err != nil {
		return nil, err
	}
	if viewer == nil || viewer.Enabled != 1 || !viewer.Complete {
		return nil, ErrDatingProfileIncomplete
	}
	if viewer.Status != DatingProfileNormal {
		return nil, ErrDatingProfileUnavailable
	}
	if strings.TrimSpace(req.SessionID) == "" {
		req.SessionID = fmt.Sprintf("%s-%d", uid, time.Now().UnixNano())
	}
	intentFilter, ok := normalizeDatingIntentFilter(req.Intent)
	if !ok {
		return nil, ErrDatingInvalidIntent
	}
	req.Intent = intentFilter
	scope := normalizeScope(req.Scope)
	pageLimit := clampLimit(req.Limit)
	freshQuota, exploreQuota := recommendationLaneQuotas(pageLimit)
	normalLimit := pageLimit - freshQuota - exploreQuota
	if normalLimit < 1 {
		normalLimit = pageLimit
		freshQuota, exploreQuota = 0, 0
	}
	baseReq := RecommendReq{
		Scope: scope, SessionID: req.SessionID, CountryMode: req.CountryMode,
		Gender: req.Gender, AgeMin: req.AgeMin, AgeMax: req.AgeMax, Intent: req.Intent,
		AllowRepeat: req.AllowRepeat,
	}

	normalReq := baseReq
	normalReq.Limit = normalLimit
	normalReq.Cursor = req.Cursor
	normal, nextCursor, normalHasMore, err := s.db.recommend(uid, viewer, normalReq)
	if err != nil {
		return nil, err
	}
	normal = rankDatingProfiles(normal, viewer, scope)
	excluded := profileUIDs(normal)

	fresh := []*DatingProfileResp{}
	freshHasMore := 0
	if freshQuota > 0 {
		freshReq := baseReq
		freshReq.Limit = freshQuota
		freshReq.FreshOnly = true
		freshReq.ExcludeUIDs = excluded
		fresh, _, freshHasMore, err = s.db.recommend(uid, viewer, freshReq)
		if err != nil {
			return nil, err
		}
		fresh = rankDatingProfiles(fresh, viewer, scope)
		excluded = append(excluded, profileUIDs(fresh)...)
	}

	var explore *DatingProfileResp
	exploreHasMore := 0
	if exploreQuota > 0 {
		exploreReq := baseReq
		exploreReq.Limit = 1
		exploreReq.ExploreOnly = true
		exploreReq.ExcludeUIDs = excluded
		exploreList, _, hasMore, laneErr := s.db.recommend(uid, viewer, exploreReq)
		if laneErr != nil {
			return nil, laneErr
		}
		exploreHasMore = hasMore
		if len(exploreList) > 0 {
			explore = exploreList[0]
			excluded = append(excluded, explore.UID)
		}
	}

	list := composeReservedRecommendation(normal, fresh, explore, pageLimit)
	for attempts := 0; len(list) < pageLimit && normalHasMore == 1 && attempts < 4; attempts++ {
		fillReq := baseReq
		fillReq.Limit = pageLimit - len(list)
		fillReq.Cursor = nextCursor
		fillReq.ExcludeUIDs = excluded
		moreNormal, cursor, hasMore, laneErr := s.db.recommend(uid, viewer, fillReq)
		if laneErr != nil {
			return nil, laneErr
		}
		nextCursor, normalHasMore = cursor, hasMore
		if len(moreNormal) == 0 {
			break
		}
		moreNormal = rankDatingProfiles(moreNormal, viewer, scope)
		normal = append(normal, moreNormal...)
		excluded = append(excluded, profileUIDs(moreNormal)...)
		list = composeReservedRecommendation(normal, fresh, explore, pageLimit)
	}
	redactPrivateDistances(list)
	if err := s.db.markServed(uid, req.SessionID, list); err != nil {
		return nil, err
	}
	hasMore := normalHasMore
	if freshHasMore == 1 || exploreHasMore == 1 {
		hasMore = 1
	}
	// Never return an empty page with has_more=1. Older Android clients retry
	// such a response with the same session/cursor and can enter a request loop.
	if len(list) == 0 {
		hasMore = 0
		nextCursor = ""
	} else if hasMore == 0 {
		nextCursor = ""
	}
	likeUsed, favoriteUsed, rewindUsed, _ := s.db.countTodayActions(uid)
	q := quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)
	return &RecommendResp{
		Items: list, List: list, Users: list, Cursor: nextCursor, HasMore: hasMore,
		Scope: scope, SessionID: req.SessionID, ServerTime: now, QuotaResp: q,
	}, nil
}

func redactPrivateDistances(profiles []*DatingProfileResp) {
	for _, profile := range profiles {
		if profile == nil {
			continue
		}
		if profile.ShowDistance == 1 && profile.DistanceMeters > 0 {
			bucket := distanceBucket(profile.DistanceMeters)
			profile.DistanceBucket = bucket
			profile.DistanceLevel = bucket
			profile.DistanceLabel = formatDistance(profile.DistanceMeters)
		} else {
			profile.DistanceBucket = 0
			profile.DistanceLevel = 0
			profile.DistanceLabel = ""
			profile.Nearby = 0
		}
		// 不向客户端暴露精确距离或坐标推算值。
		profile.DistanceMeters = 0
		profile.DistanceKM = 0
		profile.PhotoCards = buildPhotoCards(profile)
	}
}

func recommendationLaneQuotas(limit int) (fresh, explore int) {
	if limit >= 4 {
		fresh = 1
	}
	if limit >= 10 {
		fresh = 2
	}
	if limit >= 6 {
		explore = 1
	}
	return fresh, explore
}

func profileUIDs(profiles []*DatingProfileResp) []string {
	out := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		if profile != nil && strings.TrimSpace(profile.UID) != "" {
			out = append(out, profile.UID)
		}
	}
	return out
}

func composeReservedRecommendation(normal, fresh []*DatingProfileResp, explore *DatingProfileResp, limit int) []*DatingProfileResp {
	if limit <= 0 {
		return []*DatingProfileResp{}
	}
	reserved := make(map[int]*DatingProfileResp, 3)
	if len(fresh) > 0 {
		reserved[minInt(2, limit-1)] = fresh[0]
	}
	if len(fresh) > 1 {
		reserved[minInt(7, limit-1)] = fresh[1]
	}
	if explore != nil {
		position := minInt(5, limit-1)
		for reserved[position] != nil && position+1 < limit {
			position++
		}
		if reserved[position] == nil {
			reserved[position] = explore
		}
	}
	seen := make(map[string]bool, limit)
	out := make([]*DatingProfileResp, 0, limit)
	normalIndex := 0
	for position := 0; position < limit; position++ {
		if profile := reserved[position]; profile != nil && !seen[profile.UID] {
			out = append(out, profile)
			seen[profile.UID] = true
			continue
		}
		for normalIndex < len(normal) {
			profile := normal[normalIndex]
			normalIndex++
			if profile == nil || seen[profile.UID] {
				continue
			}
			out = append(out, profile)
			seen[profile.UID] = true
			break
		}
	}
	for _, group := range [][]*DatingProfileResp{fresh, normal} {
		for _, profile := range group {
			if len(out) >= limit {
				return out
			}
			if profile != nil && !seen[profile.UID] {
				out = append(out, profile)
				seen[profile.UID] = true
			}
		}
	}
	if explore != nil && len(out) < limit && !seen[explore.UID] {
		out = append(out, explore)
	}
	return out
}

func (s *Service) maybeScheduleServedCleanup(now int64) {
	const interval = int64(time.Hour / time.Millisecond)
	last := atomic.LoadInt64(&s.lastServedCleanup)
	if now-last < interval {
		return
	}
	if !atomic.CompareAndSwapInt64(&s.lastServedCleanup, last, now) {
		return
	}
	go func(cutoff int64) {
		for i := 0; i < 5; i++ {
			deleted, err := s.db.cleanupExpiredServedBatch(cutoff, 1000)
			if err != nil {
				atomic.StoreInt64(&s.lastServedCleanup, 0)
				return
			}
			if deleted < 1000 {
				return
			}
		}
	}(now)
}

func composeRecommendationPage(candidates []*DatingProfileResp, limit int, seed string, now int64) []*DatingProfileResp {
	if limit <= 0 || len(candidates) == 0 {
		return []*DatingProfileResp{}
	}
	if limit > len(candidates) {
		limit = len(candidates)
	}
	freshQuota := limit / 5
	if freshQuota < 1 && limit >= 4 {
		freshQuota = 1
	}
	if freshQuota > 2 {
		freshQuota = 2
	}
	exploreQuota := 0
	if limit >= 6 && len(candidates) > limit/2 {
		exploreQuota = 1
	}

	selected := make(map[string]bool, limit)
	fresh := make([]*DatingProfileResp, 0, freshQuota)
	for _, p := range candidates {
		if p == nil || p.ProfileScore < 40 || !isFreshDatingProfile(p, now) {
			continue
		}
		fresh = append(fresh, p)
		selected[p.UID] = true
		if len(fresh) >= freshQuota {
			break
		}
	}

	var explore *DatingProfileResp
	if exploreQuota > 0 {
		start := len(candidates) / 2
		if start < 1 {
			start = 1
		}
		span := len(candidates) - start
		if span > 0 {
			h := fnv.New32a()
			_, _ = h.Write([]byte(seed))
			for n := 0; n < span; n++ {
				idx := start + (int(h.Sum32())+n)%span
				p := candidates[idx]
				if p != nil && !selected[p.UID] {
					explore = p
					selected[p.UID] = true
					break
				}
			}
		}
	}

	// 固定槽位保留给新用户和探索用户，普通序列在槽位出现前不能提前消费它们。
	selected = make(map[string]bool, limit)
	reserved := make(map[string]bool, freshQuota+exploreQuota)
	for _, p := range fresh {
		if p != nil {
			reserved[p.UID] = true
		}
	}
	if explore != nil {
		reserved[explore.UID] = true
	}
	freshSlots := map[int]*DatingProfileResp{}
	if len(fresh) > 0 {
		freshSlots[minInt(2, limit-1)] = fresh[0]
	}
	if len(fresh) > 1 {
		freshSlots[minInt(7, limit-1)] = fresh[1]
	}
	exploreSlot := -1
	if explore != nil {
		exploreSlot = minInt(5, limit-1)
		for freshSlots[exploreSlot] != nil && exploreSlot+1 < limit {
			exploreSlot++
		}
	}

	out := make([]*DatingProfileResp, 0, limit)
	normalIndex := 0
	for pos := 0; pos < limit; pos++ {
		if p := freshSlots[pos]; p != nil && !selected[p.UID] {
			out = append(out, p)
			selected[p.UID] = true
			continue
		}
		if pos == exploreSlot && explore != nil && !selected[explore.UID] {
			out = append(out, explore)
			selected[explore.UID] = true
			continue
		}
		for normalIndex < len(candidates) {
			p := candidates[normalIndex]
			normalIndex++
			if p == nil || selected[p.UID] || reserved[p.UID] {
				continue
			}
			out = append(out, p)
			selected[p.UID] = true
			break
		}
	}
	for _, p := range candidates {
		if len(out) >= limit {
			break
		}
		if p != nil && !selected[p.UID] {
			out = append(out, p)
			selected[p.UID] = true
		}
	}
	return out
}

func isFreshDatingProfile(p *DatingProfileResp, now int64) bool {
	if p == nil || p.CreatedAtUnix <= 0 {
		return false
	}
	created := normalizeMillis(p.CreatedAtUnix)
	age := now - created
	return age >= 0 && age <= int64(30*24*time.Hour/time.Millisecond)
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.TrimSpace(a[i]) != strings.TrimSpace(b[i]) {
			return false
		}
	}
	return true
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *Service) Swipe(uid string, req SwipeReq) (*SwipeResp, error) {
	uid = strings.TrimSpace(uid)
	toUID := req.Target()
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	if toUID == "" {
		return nil, errors.New("目标用户不能为空")
	}
	if uid == toUID {
		return nil, ErrDatingSelf
	}
	if !validActionInput(req.Action) {
		return nil, ErrDatingInvalidAction
	}
	action := normalizeAction(req.Action)

	unlock := s.lock("swipe:" + uid)
	defer unlock()
	unlockPair := s.lock("pair:" + pairKey(uid, toUID))
	defer unlockPair()

	viewer, err := s.profileForAction(uid)
	if err != nil {
		return nil, err
	}
	if viewer == nil || viewer.Enabled != 1 || !viewer.Complete {
		return nil, ErrDatingProfileIncomplete
	}
	if viewer.Status != DatingProfileNormal {
		return nil, ErrDatingProfileUnavailable
	}

	// A blocked pair always wins over a stale match row. Existing pairs are checked
	// before any swipe/quota write: only an active-match like retry is idempotently
	// returned; pass/favorite and canceled/blocked matches are rejected.
	blocked, err := s.db.isPairBlocked(uid, toUID)
	if err != nil {
		return nil, err
	}
	if blocked {
		return nil, ErrDatingTargetMiss
	}
	existing, err := s.db.getMatchByPair(uid, toUID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if action == DatingActionLike && existing.Status == DatingMatchActive {
			likeUsed, favoriteUsed, rewindUsed, _ := s.db.countTodayActions(uid)
			resp := &SwipeResp{Status: 1, ToUID: toUID, TargetUID: toUID, Action: action,
				QuotaResp: quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)}
			return s.finishActiveMatch(resp, existing, uid, toUID)
		}
		return nil, ErrDatingTargetMiss
	}

	target, err := s.db.getProfile(toUID)
	if err != nil {
		return nil, err
	}
	if target == nil || target.Enabled != 1 || !target.Complete || target.Status != DatingProfileNormal {
		return nil, ErrDatingTargetMiss
	}
	if !fitsMutualFilters(viewer, target) || !fitsRequestFilters(viewer, target, RecommendReq{}) {
		return nil, ErrDatingTargetMiss
	}

	now := time.Now().UnixMilli()
	isRetry, err := s.db.recentSameSwipe(uid, toUID, action, now-SwipeRetryWindowMS)
	if err != nil {
		return nil, err
	}
	if action == DatingActionPass && !isRetry {
		recentPasses, err := s.db.countRecentPasses(uid, now-int64(time.Minute/time.Millisecond))
		if err != nil {
			return nil, err
		}
		if recentPasses >= PassPerMinuteLimit {
			return nil, ErrDatingTooFast
		}
	}
	likeUsed, favoriteUsed, rewindUsed, err := s.db.countTodayActions(uid)
	if err != nil {
		return nil, err
	}
	q := quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)
	if !isRetry && action == DatingActionLike && q.LikeRemaining <= 0 {
		return nil, ErrDatingLikeLimit
	}
	if !isRetry && action == DatingActionFavorite && q.FavoriteRemaining <= 0 {
		return nil, ErrDatingFavoriteLimit
	}
	if _, err = s.db.recordSwipe(uid, SwipeReq{ToUID: toUID, Action: action, Source: req.Source, PhotoIndex: req.PhotoIndex, SessionID: req.SessionID}); err != nil {
		return nil, err
	}
	likeUsed, favoriteUsed, rewindUsed, _ = s.db.countTodayActions(uid)
	q = quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)
	resp := &SwipeResp{Status: 1, ToUID: toUID, TargetUID: toUID, Action: action, QuotaResp: q}
	if action == DatingActionFavorite {
		resp.Message, resp.Msg = "已收藏", "已收藏"
		return resp, nil
	}
	if action != DatingActionLike {
		resp.Message, resp.Msg = "已跳过", "已跳过"
		return resp, nil
	}

	likedBack, err := s.db.hasLiked(toUID, uid)
	if err != nil {
		return nil, err
	}
	if !likedBack {
		resp.Message, resp.Msg = "已喜欢", "已喜欢"
		return resp, nil
	}
	match, _, err := s.db.createMatch(uid, toUID)
	if err != nil {
		return nil, err
	}
	if match == nil || match.Status != DatingMatchActive {
		return resp, nil
	}
	return s.finishActiveMatch(resp, match, uid, toUID)
}

func (s *Service) syncActiveMatchFriend(match *datingMatchModel, uid, toUID string) (bool, bool, error) {
	if match == nil || strings.TrimSpace(match.MatchID) == "" {
		return false, false, nil
	}
	// 重新读取当前状态，避免其他实例已经取消或屏蔽后，旧请求仍补建好友。
	current, err := s.db.getMatchByID(match.MatchID)
	if err != nil {
		return false, false, err
	}
	if current == nil || current.Status != DatingMatchActive {
		return false, false, nil
	}
	match.NoticeSent = current.NoticeSent
	if current.FriendSynced == 1 {
		match.FriendAutoCreated = current.FriendAutoCreated
		match.FriendSynced = current.FriendSynced
		match.Status = current.Status
		return current.FriendAutoCreated == 1, true, nil
	}

	sourceVercode := "dating_match:" + current.MatchID
	managedBySource, err := s.userService.EnsureMutualFriends(uid, toUID, sourceVercode)
	if err != nil {
		return false, true, err
	}
	marked, err := s.db.markMatchFriendSynced(current.MatchID, managedBySource)
	if err != nil {
		return false, true, err
	}
	if !marked {
		// 好友事务完成后，匹配可能已被另一实例取消或屏蔽。
		// 仅清理由本匹配来源管理的好友，原有好友不会被误删。
		if managedBySource {
			if _, cleanupErr := s.userService.RemoveMutualFriendsIfSource(uid, toUID, sourceVercode); cleanupErr != nil {
				return false, false, cleanupErr
			}
		}
		return false, false, nil
	}
	match.FriendAutoCreated = boolInt(managedBySource)
	match.FriendSynced = 1
	match.Status = DatingMatchActive
	return managedBySource, true, nil
}

func (s *Service) trySendMatchNotice(matchID, uid, toUID string, current int) bool {
	if current == 1 {
		return true
	}
	claimed, err := s.db.claimMatchNotice(matchID)
	if err != nil || !claimed {
		return false
	}
	if err = s.sendMatchNotices(matchID, uid, toUID); err != nil {
		s.db.releaseMatchNoticeClaim(matchID)
		return false
	}
	return s.db.markMatchNoticeSent(matchID) == nil
}

func (s *Service) finishActiveMatch(resp *SwipeResp, match *datingMatchModel, uid, toUID string) (*SwipeResp, error) {
	if resp == nil {
		resp = &SwipeResp{Status: 1, ToUID: toUID, TargetUID: toUID, Action: DatingActionLike}
	}
	friendCreated, active, err := s.syncActiveMatchFriend(match, uid, toUID)
	if err != nil {
		// 匹配行已经存在；客户端重试同一次喜欢时会继续修复好友关系。
		return nil, err
	}
	if !active {
		return nil, ErrDatingTargetMiss
	}

	noticeSent := s.trySendMatchNotice(match.MatchID, uid, toUID, match.NoticeSent)
	resp.Matched = true
	resp.Match = true
	resp.MatchID = match.MatchID
	resp.NoticeSent = noticeSent
	resp.SystemNoticeSent = noticeSent
	resp.CanChat = true
	resp.FriendCreated = friendCreated
	resp.FriendsCreated = friendCreated
	if friendCreated {
		resp.Message = "你们互相喜欢，已自动成为好友"
	} else {
		resp.Message = "你们互相喜欢了，现在可以聊天"
	}
	resp.Msg = resp.Message
	return resp, nil
}

func (s *Service) UndoSwipe(uid string) (*UndoResp, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil, errors.New("请先登录")
	}
	unlock := s.lock("swipe:" + uid)
	defer unlock()
	viewer, err := s.profileForAction(uid)
	if err != nil {
		return nil, err
	}
	if viewer == nil || viewer.Enabled != 1 || !viewer.Complete {
		return nil, ErrDatingProfileIncomplete
	}
	if viewer.Status != DatingProfileNormal {
		return nil, ErrDatingProfileUnavailable
	}
	likeUsed, favoriteUsed, rewindUsed, err := s.db.countTodayActions(uid)
	if err != nil {
		return nil, err
	}
	q := quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)
	if q.RewindRemaining <= 0 {
		return nil, ErrDatingRewindLimit
	}
	event, err := s.db.latestUndoableEvent(uid)
	if err != nil {
		return nil, err
	}
	if event == nil {
		return nil, ErrDatingNothingToUndo
	}
	if event.Action == DatingActionLike {
		_, active, err := s.db.hasActiveMatch(uid, event.ToUID)
		if err != nil {
			return nil, err
		}
		if active {
			return nil, ErrDatingMatchedCannotUndo
		}
	}
	if err = s.db.undoSwipe(uid, event); err != nil {
		return nil, err
	}
	restored, _ := s.db.getProfile(event.ToUID)
	likeUsed, favoriteUsed, rewindUsed, _ = s.db.countTodayActions(uid)
	return &UndoResp{Status: 1, Action: event.Action, TargetUID: event.ToUID, Restored: restored, Message: "已撤回", QuotaResp: quotaResp(viewer.Sex, likeUsed, favoriteUsed, rewindUsed)}, nil
}

func (s *Service) RecordExposures(uid string, req ExposureReq) (*ExposureResp, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New("请先登录")
	}
	if len(req.Items) > DatingExposureMax {
		req.Items = req.Items[:DatingExposureMax]
	}
	count, err := s.db.recordExposures(uid, req)
	if err != nil {
		return nil, err
	}
	return &ExposureResp{Status: 1, Count: count}, nil
}

func (s *Service) Matches(uid string, limit int) (*MatchesResp, error) {
	list, err := s.db.matches(uid, limit)
	if err != nil {
		return nil, err
	}
	// 兼容升级前已经存在的活跃匹配：用户打开匹配列表时按需补齐好友关系。
	// 若同步期间匹配已被另一实例取消或屏蔽，则不把该过期项返回给客户端。
	out := make([]*DatingMatchResp, 0, len(list))
	for _, item := range list {
		if item == nil || item.User == nil {
			continue
		}
		if item.FriendSynced != 1 {
			match := &datingMatchModel{
				MatchID:           item.MatchID,
				Status:            DatingMatchActive,
				FriendAutoCreated: item.FriendAutoCreated,
				FriendSynced:      item.FriendSynced,
			}
			created, active, syncErr := s.syncActiveMatchFriend(match, uid, item.User.UID)
			if syncErr == nil {
				if !active {
					continue
				}
				item.FriendSynced = 1
				item.FriendAutoCreated = boolInt(created)
			}
		}
		if item.NoticeSent != 1 {
			item.NoticeSent = boolInt(s.trySendMatchNotice(item.MatchID, uid, item.User.UID, item.NoticeSent))
		}
		out = append(out, item)
	}
	return &MatchesResp{List: out, Matches: out, ServerTime: time.Now().UnixMilli()}, nil
}

func (s *Service) Favorites(uid string, limit int) (*FavoritesResp, error) {
	list, total, err := s.db.favorites(uid, limit)
	if err != nil {
		return nil, err
	}
	return &FavoritesResp{Items: list, List: list, Total: total, ServerTime: time.Now().UnixMilli()}, nil
}

func (s *Service) RemoveFavorite(uid string, req RemoveFavoriteReq) (*BasicResp, error) {
	toUID := req.Target()
	if uid == "" || toUID == "" {
		return nil, errors.New("参数不能为空")
	}
	if err := s.db.removeFavorite(uid, toUID); err != nil {
		return nil, err
	}
	return &BasicResp{Status: 1, Msg: "已取消收藏"}, nil
}

func (s *Service) ReceivedLikes(uid string, limit int, reveal bool) (*ReceivedLikesResp, error) {
	list, total, err := s.db.receivedLikes(uid, limit, reveal)
	if err != nil {
		return nil, err
	}
	return &ReceivedLikesResp{Total: total, Locked: !reveal, Items: list, ServerTime: time.Now().UnixMilli()}, nil
}

func (s *Service) CancelMatch(uid, matchID string) (*BasicResp, error) {
	uid = strings.TrimSpace(uid)
	matchID = strings.TrimSpace(matchID)
	if uid == "" || matchID == "" {
		return nil, errors.New("参数不能为空")
	}
	match, err := s.db.getMatchByID(matchID)
	if err != nil {
		return nil, err
	}
	if match == nil || (match.UIDA != uid && match.UIDB != uid) {
		return nil, errors.New("匹配不存在")
	}
	otherUID := match.UIDA
	if otherUID == uid {
		otherUID = match.UIDB
	}
	unlockPair := s.lock("pair:" + pairKey(uid, otherUID))
	defer unlockPair()

	// 进入锁后重新读取，避免同进程的匹配补好友与取消操作交错。
	match, err = s.db.getMatchByID(matchID)
	if err != nil {
		return nil, err
	}
	if match == nil || (match.UIDA != uid && match.UIDB != uid) {
		return nil, errors.New("匹配不存在")
	}
	if err = s.db.cancelMatch(uid, matchID); err != nil {
		return nil, err
	}
	// 只清理由本次匹配创建的好友关系，原本已经是好友的用户不会被误删。
	if _, err = s.userService.RemoveMutualFriendsIfSource(uid, otherUID, "dating_match:"+match.MatchID); err != nil {
		return nil, err
	}
	if err = s.removeDatingWhitelistIfUnused(uid, otherUID); err != nil {
		return nil, err
	}
	if err = s.removeDatingWhitelistIfUnused(otherUID, uid); err != nil {
		return nil, err
	}
	return &BasicResp{Status: 1, Msg: "已取消匹配"}, nil
}

func (s *Service) Block(uid string, req BlockReq) (*BasicResp, error) {
	uid = strings.TrimSpace(uid)
	toUID := req.Target()
	if uid == "" || toUID == "" {
		return nil, errors.New("参数不能为空")
	}
	if uid == toUID {
		return nil, ErrDatingSelf
	}
	exists, err := s.db.userExists(toUID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrDatingTargetMiss
	}
	unlockPair := s.lock("pair:" + pairKey(uid, toUID))
	defer unlockPair()

	if err = s.db.block(uid, BlockReq{ToUID: toUID, Reason: req.Reason}); err != nil {
		return nil, err
	}
	// 数据库黑名单已落盘后，即使 IM 临时失败也继续清理交友来源好友和白名单。
	// 客户端重试时 SetBlacklist/Block 都是幂等的，可继续修复 IM 状态。
	blacklistErr := s.userService.SetBlacklist(uid, toUID, true)
	match, matchErr := s.db.getMatchByPair(uid, toUID)
	if matchErr != nil {
		return nil, matchErr
	}
	if match != nil {
		if _, err = s.userService.RemoveMutualFriendsIfSource(uid, toUID, "dating_match:"+match.MatchID); err != nil {
			return nil, err
		}
	}
	removeForwardErr := s.removeDatingWhitelistIfUnused(uid, toUID)
	removeReverseErr := s.removeDatingWhitelistIfUnused(toUID, uid)
	if blacklistErr != nil {
		return nil, blacklistErr
	}
	if removeForwardErr != nil {
		return nil, removeForwardErr
	}
	if removeReverseErr != nil {
		return nil, removeReverseErr
	}
	return &BasicResp{Status: 1, Msg: "已屏蔽"}, nil
}

func (s *Service) Report(uid string, req ReportReq) (*BasicResp, error) {
	toUID := req.Target()
	if uid == "" || toUID == "" {
		return nil, errors.New("参数不能为空")
	}
	if uid == toUID {
		return nil, ErrDatingSelf
	}
	exists, err := s.db.userExists(toUID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrDatingTargetMiss
	}
	req.ToUID = toUID
	if err := s.db.report(uid, req); err != nil {
		return nil, err
	}
	return &BasicResp{Status: 1, Msg: "已提交举报"}, nil
}

func (s *Service) ChatCheck(uid, toUID string) (*ChatCheckResp, error) {
	uid = strings.TrimSpace(uid)
	toUID = strings.TrimSpace(toUID)
	if uid == "" || toUID == "" {
		return nil, errors.New("参数不能为空")
	}
	if uid == toUID {
		return nil, ErrDatingSelf
	}
	match, err := s.db.getMatchByPair(uid, toUID)
	if err != nil {
		return nil, err
	}
	if match == nil || match.Status != DatingMatchActive {
		return &ChatCheckResp{CanChat: false, Matched: false, ToUID: toUID, TargetUID: toUID, ServerTime: time.Now().UnixMilli()}, nil
	}
	_, active, err := s.syncActiveMatchFriend(match, uid, toUID)
	if err != nil {
		return nil, err
	}
	if !active {
		return &ChatCheckResp{CanChat: false, Matched: false, ToUID: toUID, TargetUID: toUID, ServerTime: time.Now().UnixMilli()}, nil
	}
	return &ChatCheckResp{CanChat: true, Matched: true, MatchID: match.MatchID, ToUID: toUID, TargetUID: toUID, ServerTime: time.Now().UnixMilli()}, nil
}

func (s *Service) removeDatingWhitelist(channelUID, memberUID string) error {
	if channelUID == "" || memberUID == "" || channelUID == memberUID {
		return nil
	}
	return s.ctx.IMWhitelistRemove(config.ChannelWhitelistReq{ChannelReq: config.ChannelReq{ChannelID: channelUID, ChannelType: common.ChannelTypePerson.Uint8()}, UIDs: []string{memberUID}})
}

func (s *Service) removeDatingWhitelistIfUnused(channelUID, memberUID string) error {
	other, err := s.db.hasOtherChatRelationship(channelUID, memberUID)
	if err != nil {
		return err
	}
	if other {
		return nil
	}
	return s.removeDatingWhitelist(channelUID, memberUID)
}

func (s *Service) sendMatchNotices(matchID, uid, toUID string) error {
	if err := s.sendMatchNotice(matchID, uid, toUID); err != nil {
		return err
	}
	return s.sendMatchNotice(matchID, toUID, uid)
}

func (s *Service) sendMatchNotice(matchID, receiverUID, targetUID string) error {
	target, _ := s.db.getProfile(targetUID)
	name := targetUID
	if target != nil && strings.TrimSpace(target.Name) != "" {
		name = target.Name
	}
	content := fmt.Sprintf("你和 %s 互相喜欢了，现在可以聊天", name)
	payload := []byte(util.ToJson(map[string]interface{}{
		"content": content, "type": common.Text, "dating_match": 1, "match_id": matchID, "target_uid": targetUID,
		"action": "open_dating_chat", "data": map[string]interface{}{"match_id": matchID, "target_uid": targetUID, "action": "open_dating_chat"},
	}))
	return s.ctx.SendMessage(&config.MsgSendReq{
		FromUID: s.ctx.GetConfig().Account.SystemUID, ChannelID: receiverUID, ChannelType: common.ChannelTypePerson.Uint8(),
		Payload: payload, Header: config.MsgHeader{RedDot: 1},
	})
}
