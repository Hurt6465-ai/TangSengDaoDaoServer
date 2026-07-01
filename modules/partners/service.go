package partners

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
)

var (
	ErrGreetingSelf        = errors.New("不能给自己打招呼")
	ErrGreetingTargetMiss  = errors.New("语伴不存在")
	ErrGreetingBlacklisted = errors.New("对方暂时不能接收打招呼")
	ErrGreetingHourLimit   = errors.New("打招呼太频繁，请稍后再试")
	ErrGreetingDayLimit    = errors.New("今天打招呼次数已用完")
	ErrGreetingDuplicate   = errors.New("已经打过招呼，请过几天再试")
)

type Service struct {
	ctx *config.Context
	db  *db

	candidateMu sync.Mutex
}

func NewService(ctx *config.Context) *Service {
	svc := &Service{ctx: ctx, db: newDB(ctx)}
	ctx.AddMessagesListener(svc.listenerMessages)
	return svc
}

func (s *Service) List(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	if req.NearbyOnly {
		return s.listRealtime(loginUID, req)
	}
	list, hasMore, err := s.listFromCandidatePool(loginUID, req)
	if err == nil {
		return list, hasMore, nil
	}
	return s.listRealtime(loginUID, req)
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
	window := s.pickCandidateWindow(loginUID, pool, PartnerRankWindowSize)
	if len(window) == 0 {
		// 当天 served/seen 已经把池子消耗完时，刷新候选池再试一次。
		// 语伴早期用户量少，允许进入下一轮随机循环，不让前端刷到尽头黑住。
		s.clearServed(loginUID)
		s.clearSeenDay(loginUID)
		pool, err = s.rebuildCandidatePoolLocked(loginUID, req)
		if err != nil {
			return nil, 0, err
		}
		window = s.pickCandidateWindow(loginUID, pool, PartnerRankWindowSize)
	}
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
	} else if len(pool) > 0 {
		hasMore = 1
	}
	s.markServed(loginUID, list)
	return list, hasMore, nil
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
	s.candidateMu.Lock()
	defer s.candidateMu.Unlock()

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
}

func (s *Service) rebuildCandidatePool(loginUID string, req listReq) ([]string, error) {
	uids, err := s.db.candidateUIDs(loginUID, req, PartnerCandidateSQLLimit)
	if err != nil {
		return nil, err
	}
	uids = compactUIDs(uids, PartnerCandidatePoolSize)
	uids = shuffleCandidateUIDs(uids, loginUID+":"+req.RandomSeed())
	if s.ctx != nil && s.ctx.GetRedisConn() != nil && len(uids) > 0 {
		key := s.candidatePoolKey(loginUID, req.SessionID)
		_ = s.ctx.GetRedisConn().SetAndExpire(key, util.ToJson(uids), PartnerCandidatePoolTTL)
	}
	return uids, nil
}

func (s *Service) pickCandidateWindow(loginUID string, pool []string, max int) []string {
	if max <= 0 {
		max = PartnerRankWindowSize
	}
	served := s.redisSetMembers(s.servedKey(loginUID))
	seen := s.redisSetMembers(s.seenDayKey(loginUID))
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
	_ = s.ctx.GetRedisConn().Expire(key, 24*time.Hour)
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
	now := time.Now().UnixMilli()
	items := make([]ExposureItem, 0, len(req.Items))
	seenUIDs := map[string]struct{}{}
	for _, item := range req.Items {
		toUID := strings.TrimSpace(item.ToUID)
		if toUID == "" || toUID == uid {
			continue
		}
		if _, ok := seenUIDs[toUID]; ok {
			continue
		}
		seenUIDs[toUID] = struct{}{}
		seenAt := normalizeMillis(item.SeenAt)
		if seenAt <= 0 || seenAt > now+int64(time.Hour/time.Millisecond) {
			seenAt = now
		}
		if item.DurationMS < 0 {
			item.DurationMS = 0
		}
		items = append(items, ExposureItem{ToUID: toUID, SeenAt: seenAt, DurationMS: item.DurationMS})
		if len(items) >= PartnerExposureBatchMax {
			break
		}
	}
	if len(items) == 0 {
		return &ExposureResp{Status: 200, Count: 0, Msg: "ok"}, nil
	}
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		members := make([]interface{}, 0, len(items))
		for _, item := range items {
			members = append(members, item.ToUID)
			_ = s.ctx.GetRedisConn().ZAdd(s.seenZSetKey(uid), float64(item.SeenAt), item.ToUID)
		}
		_ = s.ctx.GetRedisConn().SAdd(s.seenDayKey(uid), members...)
		_ = s.ctx.GetRedisConn().Expire(s.seenDayKey(uid), 24*time.Hour)
		_ = s.ctx.GetRedisConn().Expire(s.seenZSetKey(uid), 45*24*time.Hour)
	}
	go func(items []ExposureItem) {
		_ = s.db.recordExposureItems(uid, items)
	}(items)
	return &ExposureResp{Status: 200, Count: len(items), Msg: "ok"}, nil
}

func (s *Service) ProfileMe(uid string) (*ProfileMeResp, error) {
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
	contact, err := s.db.getPartnerContact(uid, toUID)
	if err != nil {
		return nil, err
	}
	if contact != nil {
		if contact.Status == PartnerContactStatusActive {
			return &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusActive, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: "已经可以聊天"}, nil
		}
		if contact.Status == PartnerContactStatusBlocked || contact.Status == PartnerContactStatusIgnored {
			return nil, ErrGreetingBlacklisted
		}
		if contact.Status == PartnerContactStatusPending {
			if contact.RequesterUID != uid {
				return &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: "对方已打招呼，可以直接回复"}, nil
			}
			allowed, nextAt, msg := canSendPendingGreeting(contact.RequesterMsgCount, contact.LastMsgAt, now)
			if !allowed {
				return &GreetingResp{Status: 429, ToUID: toUID, TargetUID: toUID, LastGreetAt: contact.LastMsgAt, NextAllowedAt: nextAt, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: contact.RequesterMsgCount, MaxGreetingCount: MaxPendingGreetingMessages, Msg: msg}, ErrGreetingDuplicate
			}
			stats, err := s.db.greetingStats(uid, toUID, now)
			if err != nil {
				return nil, err
			}
			if stats.HourCount >= GreetingHourLimit {
				return nil, ErrGreetingHourLimit
			}
			if stats.DayCount >= GreetingDayLimit {
				return nil, ErrGreetingDayLimit
			}
			text := normalizeGreetingText(req.Text)
			source := normalizeGreetingSource(req.Source)
			resp, err := s.db.recordGreeting(uid, toUID, text, source)
			if err != nil {
				return nil, err
			}
			count, err := s.db.incrementPendingRequesterMsgCount(uid, toUID, resp.LastGreetAt)
			if err != nil {
				return nil, err
			}
			resp.RequesterMsgCount = count
			resp.MaxGreetingCount = MaxPendingGreetingMessages
			if err := s.addPartnerWhitelist(uid, toUID); err != nil {
				return nil, err
			}
			if err := s.sendGreetingMessage(uid, toUID, resp.Text, source, resp.LastGreetAt, resp.RequesterMsgCount); err != nil {
				return nil, err
			}
			if count >= MaxPendingGreetingMessages {
				_ = s.removePartnerSenderWhitelist(uid, toUID)
			}
			return resp, nil
		}
	}

	stats, err := s.db.greetingStats(uid, toUID, now)
	if err != nil {
		return nil, err
	}
	if stats.HourCount >= GreetingHourLimit {
		return nil, ErrGreetingHourLimit
	}
	if stats.DayCount >= GreetingDayLimit {
		return nil, ErrGreetingDayLimit
	}
	cooldownMs := int64(GreetingSameTargetCooldown / time.Millisecond)
	if stats.LastTargetGreetAt > 0 && now-stats.LastTargetGreetAt < cooldownMs {
		resp := &GreetingResp{Status: 429, ToUID: toUID, TargetUID: toUID, LastGreetAt: stats.LastTargetGreetAt, NextAllowedAt: stats.LastTargetGreetAt + cooldownMs, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: MaxPendingGreetingMessages, MaxGreetingCount: MaxPendingGreetingMessages, Msg: ErrGreetingDuplicate.Error()}
		return resp, ErrGreetingDuplicate
	}
	text := normalizeGreetingText(req.Text)
	source := normalizeGreetingSource(req.Source)
	resp, err := s.db.recordGreeting(uid, toUID, text, source)
	if err != nil {
		return nil, err
	}
	if err := s.db.ensurePendingContact(uid, toUID, resp.LastGreetAt); err != nil {
		return nil, err
	}
	resp.RequesterMsgCount = 1
	resp.MaxGreetingCount = MaxPendingGreetingMessages
	if err := s.addPartnerWhitelist(uid, toUID); err != nil {
		return nil, err
	}
	if err := s.sendGreetingMessage(uid, toUID, resp.Text, source, resp.LastGreetAt, resp.RequesterMsgCount); err != nil {
		return nil, err
	}
	return resp, nil
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

func canSendPendingGreeting(count int, lastMsgAt int64, now int64) (bool, int64, string) {
	// 产品规则：对方未回复前，发起人最多可以连续打 3 条招呼。
	// 这里不再用 30 分钟/24 小时冷却卡第 2、3 条，否则实际体验会像“只能发 1 条”。
	if count >= MaxPendingGreetingMessages {
		return false, 0, "对方还没回复，最多只能打招呼3次"
	}
	return true, 0, ""
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
	// 待回复阶段需要两种能力：
	// 1. 对方可以直接回复这条招呼。
	// 2. 发起人从消息列表进入聊天页后，最多还能补发 2 条普通私信。
	// 超过 3 条后，listenerMessages 会移除发起人继续发送到对方个人频道的白名单。
	return s.addBidirectionalPartnerWhitelist(uid, toUID)
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

func (s *Service) addBidirectionalPartnerWhitelist(uid, toUID string) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	if err := s.ctx.IMWhitelistAdd(config.ChannelWhitelistReq{
		ChannelReq: config.ChannelReq{ChannelID: uid, ChannelType: common.ChannelTypePerson.Uint8()},
		UIDs:       []string{toUID},
	}); err != nil {
		return err
	}
	return s.ctx.IMWhitelistAdd(config.ChannelWhitelistReq{
		ChannelReq: config.ChannelReq{ChannelID: toUID, ChannelType: common.ChannelTypePerson.Uint8()},
		UIDs:       []string{uid},
	})
}

func (s *Service) sendGreetingMessage(uid, toUID, text, source string, at int64, requesterMsgCount int) error {
	if uid == "" || toUID == "" || text == "" {
		return nil
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
	return s.ctx.SendMessage(&config.MsgSendReq{
		FromUID:     uid,
		ChannelID:   toUID,
		ChannelType: common.ChannelTypePerson.Uint8(),
		Payload:     payload,
		Header: config.MsgHeader{
			RedDot: 1,
		},
	})
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

		contact, _ := s.db.getPartnerContact(msg.FromUID, msg.ChannelID)
		if contact != nil && contact.Status == PartnerContactStatusPending && contact.RequesterUID == msg.FromUID {
			count, err := s.db.incrementPendingRequesterMsgCount(msg.FromUID, msg.ChannelID, createdAt)
			if err == nil && count >= MaxPendingGreetingMessages {
				// 第 1 条来自语伴页随机招呼，第 2/3 条允许在聊天窗口补发。
				// 达到 3 条后，保留对方回复能力，但移除发起人继续追发的白名单。
				_ = s.removePartnerSenderWhitelist(msg.FromUID, msg.ChannelID)
			}
			continue
		}

		activated, _ := s.db.activateContactOnReply(msg.FromUID, msg.ChannelID, createdAt)
		if activated {
			_ = s.addBidirectionalPartnerWhitelist(msg.FromUID, msg.ChannelID)
		}
	}
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

func cursorToken() string {
	return time.Now().Format("20060102") + ":" + strconv.FormatInt(time.Now().UnixMilli(), 10)
}
