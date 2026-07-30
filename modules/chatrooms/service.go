package chatrooms

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/util"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"go.uber.org/zap"
)

var (
	ErrPermissionDenied = errors.New("该用户无权执行此操作")
	ErrRoomExpired      = errors.New("该话题已结束")
)

type Service struct {
	ctx               *config.Context
	db                *db
	TTL               time.Duration
	topicChannelMu    sync.RWMutex
	topicChannelCache map[string]topicChannelCacheItem
	lastReplyMu       sync.Mutex
	lastReplyPending  map[string]*lastReplyFlushItem
	recentMessageIDs  map[string]int64
	flushStartOnce    sync.Once
	workerWG          sync.WaitGroup
	deleteNotifySem   chan struct{}
}

type lastReplyFlushItem struct {
	Latest       *MessageWebhookReq
	Count        int
	Participants map[string]*MessageWebhookReq
}

type topicChannelCacheItem struct {
	OK        bool
	ExpiresAt int64
}

const (
	topicChannelCacheTTL             = 5 * time.Minute
	topicChannelCacheMaxEntries      = 4096
	topicLastReplyFlushInterval      = 3 * time.Second
	topicLastReplyFlushRetries       = 3
	topicRecentMessageIDTTL          = 10 * time.Minute
	topicRecentMessageIDMaxEntries   = 20000
	topicRoomDeletedNotifyBatchSize  = 100
	topicRoomDeletedNotifyBatchSleep = 30 * time.Millisecond
	topicRoomCleanupGrace            = 10 * time.Second
	topicRoomDeleteNotifyConcurrency = 8
)

func NewService(ctx *config.Context) *Service {
	svc := &Service{
		ctx:               ctx,
		db:                newDB(ctx),
		TTL:               DefaultTTL,
		topicChannelCache: map[string]topicChannelCacheItem{},
		lastReplyPending:  map[string]*lastReplyFlushItem{},
		recentMessageIDs:  map[string]int64{},
		deleteNotifySem:   make(chan struct{}, topicRoomDeleteNotifyConcurrency),
	}
	ctx.AddMessagesListener(svc.listenerMessages)
	return svc
}

func (s *Service) Start(ctx context.Context) {
	s.flushStartOnce.Do(func() {
		s.startLastReplyFlushLoop(ctx)
	})
}

func (s *Service) WaitWorkers() {
	s.workerWG.Wait()
}

func (s *Service) List(uid string, isAdmin bool, req RoomListReq) ([]*TopicRoom, string, int, error) {
	rooms, cursor, hasMore, err := s.db.list(uid, req)
	if err != nil {
		return nil, "", 0, err
	}
	for _, room := range rooms {
		s.prepareRoomForResponse(room, uid, isAdmin)
	}
	return rooms, cursor, hasMore, nil
}

func (s *Service) Create(req CreateReq, loginUID string, isAdmin bool) (*TopicRoom, error) {
	title := strings.TrimSpace(req.Title)
	if title == "" {
		return nil, errors.New("话题名不能为空")
	}
	if len([]rune(title)) > MaxRoomTitleLength {
		return nil, errors.New("话题名过长")
	}
	if loginUID == "" {
		return nil, errors.New("未登录")
	}

	language := strings.TrimSpace(req.Language)
	if len([]rune(language)) > MaxRoomLanguageLength {
		return nil, errors.New("语言字段过长")
	}

	requestNo := strings.TrimSpace(req.CreateRequestNo)
	if len(requestNo) > MaxCreateRequestLength {
		return nil, errors.New("create_request_no过长")
	}
	if requestNo != "" {
		existing, err := s.db.getByCreateRequest(loginUID, requestNo)
		if err == nil && existing != nil {
			if existing.Status != 1 || existing.ExpireAt <= time.Now().UnixMilli() {
				return nil, ErrRoomExpired
			}
			if err := s.syncIMChannelWithStoredMembers(existing); err != nil {
				return nil, err
			}
			s.prepareRoomForResponse(existing, loginUID, isAdmin)
			return existing, nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	user, err := s.db.queryUserMeta(loginUID)
	if err != nil {
		return nil, err
	}
	if user.UID == "" {
		user.UID = loginUID
	}

	ts := time.Now().UnixMilli()
	roomID := "topic_" + util.GenerUUID()
	room := &TopicRoom{
		RoomID:             roomID,
		CreateRequestNo:    requestNo,
		Title:              title,
		Tag:                normalizeRoomTag(req.Tag),
		Language:           language,
		BackgroundIndex:    int(ts%20) + 1,
		ChannelID:          roomID,
		ChannelType:        ChannelTypeGroup,
		CreatorUID:         user.UID,
		CreatorName:        user.Name,
		CreatorAvatar:      user.Avatar,
		CreatorCountryCode: user.CountryCode,
		CreatorCountry:     user.Country,
		ParticipantCount:   1,
		ReplyUsers:         []ReplyAvatar{{UID: user.UID, Name: user.Name, Avatar: user.Avatar, CountryCode: user.CountryCode, Country: user.Country}},
		CreatedAt:          ts,
		ExpireAt:           ts + int64(s.ttl()/time.Millisecond),
	}
	if room.Language == "" {
		room.Language = "中文"
	}

	if err := s.db.create(room); err != nil {
		if requestNo != "" {
			if existing, queryErr := s.db.getByCreateRequest(loginUID, requestNo); queryErr == nil && existing != nil {
				if existing.Status != 1 || existing.ExpireAt <= time.Now().UnixMilli() {
					return nil, ErrRoomExpired
				}
				if err := s.syncIMChannelWithStoredMembers(existing); err != nil {
					return nil, err
				}
				s.prepareRoomForResponse(existing, loginUID, isAdmin)
				return existing, nil
			}
		}
		return nil, err
	}

	s.setTopicChannelCache(room.ChannelID, true)
	if err := s.syncIMChannel(room, []string{room.CreatorUID}); err != nil {
		s.setTopicChannelCache(room.ChannelID, false)
		if cleanupErr := s.db.deleteFailedCreate(room.RoomID); cleanupErr != nil {
			log.Error("回滚创建失败的话题聊天室失败", zap.Error(cleanupErr), zap.String("room_id", room.RoomID))
		}
		if deleteErr := s.deleteIMChannel(room.ChannelID); deleteErr != nil {
			log.Error("回滚创建失败的话题IM频道失败", zap.Error(deleteErr), zap.String("channel_id", room.ChannelID))
		}
		return nil, err
	}

	s.prepareRoomForResponse(room, loginUID, isAdmin)
	return room, nil
}

func (s *Service) Enter(req RoomReq, uid string, isAdmin bool) (*TopicRoom, error) {
	roomID := roomIDFromReq(req)
	if roomID == "" {
		return nil, ErrNotFound
	}
	if uid == "" {
		return nil, errors.New("未登录")
	}

	room, err := s.db.get(roomID)
	if err != nil {
		return nil, err
	}
	s.setTopicChannelCache(room.ChannelID, true)

	user, err := s.db.queryUserMeta(uid)
	if err != nil {
		return nil, err
	}
	if user.UID == "" {
		user.UID = uid
	}
	if err := s.db.addMemberToRoom(room, uid, user.Name, user.Avatar); err != nil {
		return nil, err
	}

	// 外部 IM 写入失败时保留数据库成员记录；客户端重试 Enter 是幂等的，
	// 会再次补加订阅者，避免回滚数据库后又与已经成功的 IM 写入产生反向不一致。
	if err := s.addIMSubscribers(room.ChannelID, []string{uid}); err != nil {
		return nil, err
	}
	if err := s.db.markRead(room.RoomID, uid, time.Now().UnixMilli()); err != nil {
		return nil, err
	}

	room, err = s.db.get(room.RoomID)
	if err != nil {
		return nil, err
	}
	_ = s.db.loadUnread(room, uid)
	s.prepareRoomForResponse(room, uid, isAdmin)
	return room, nil
}

func (s *Service) Read(req RoomReq, uid string, isAdmin bool) (*TopicRoom, error) {
	roomID := roomIDFromReq(req)
	if roomID == "" {
		return nil, ErrNotFound
	}
	if uid == "" {
		return nil, errors.New("未登录")
	}
	room, err := s.db.get(roomID)
	if err != nil {
		return nil, err
	}
	if err := s.db.markRead(room.RoomID, uid, time.Now().UnixMilli()); err != nil {
		return nil, err
	}
	room.UnreadCount = 0
	s.prepareRoomForResponse(room, uid, isAdmin)
	return room, nil
}

func (s *Service) Pin(req RoomReq, requesterUID string, isAdmin bool) (*TopicRoom, error) {
	if !isAdmin {
		return nil, ErrPermissionDenied
	}
	roomID := roomIDFromReq(req)
	if roomID == "" {
		return nil, ErrNotFound
	}
	room, err := s.db.updatePin(roomID, req.Pinned)
	if err != nil {
		return nil, err
	}
	s.prepareRoomForResponse(room, requesterUID, isAdmin)
	return room, nil
}

func (s *Service) Delete(req RoomReq, requesterUID string, isAdmin bool) error {
	roomID := roomIDFromReq(req)
	if roomID == "" {
		return ErrNotFound
	}
	room, err := s.db.get(roomID)
	if err != nil {
		return err
	}
	if !isAdmin && requesterUID != room.CreatorUID {
		return ErrPermissionDenied
	}

	uids := s.topicRoomMemberUIDs(room)
	if err := s.db.softDelete(room.RoomID); err != nil {
		return err
	}
	s.setTopicChannelCache(room.ChannelID, false)
	s.notifyTopicRoomDeleted(room.ChannelID, uids, "deleted")
	if err := s.deleteIMChannel(room.ChannelID); err != nil {
		log.Error("删除话题IM频道失败", zap.Error(err), zap.String("channel_id", room.ChannelID))
	}
	return nil
}

func (s *Service) CleanupExpired(limit uint64) (int, error) {
	if limit == 0 {
		limit = 300
	}
	rooms, err := s.db.expired(time.Now().Add(-topicRoomCleanupGrace).UnixMilli(), limit)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, room := range rooms {
		if room == nil {
			continue
		}
		uids := s.topicRoomMemberUIDs(room)
		if err := s.db.softDelete(room.RoomID); err != nil {
			log.Error("软删除过期话题聊天室失败", zap.Error(err), zap.String("room_id", room.RoomID))
			continue
		}
		s.setTopicChannelCache(room.ChannelID, false)
		s.notifyTopicRoomDeleted(room.ChannelID, uids, "expired")
		if err := s.deleteIMChannel(room.ChannelID); err != nil {
			log.Error("删除过期话题IM频道失败", zap.Error(err), zap.String("channel_id", room.ChannelID))
		}
		count++
	}
	return count, nil
}

func (s *Service) IsTopicChannel(channelID string) bool {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" || !strings.HasPrefix(channelID, "topic_") {
		return false
	}

	now := time.Now().UnixMilli()
	s.topicChannelMu.RLock()
	item, ok := s.topicChannelCache[channelID]
	s.topicChannelMu.RUnlock()
	if ok && item.ExpiresAt > now {
		return item.OK
	}

	ok = s.db.isTopicChannel(channelID)
	s.setTopicChannelCache(channelID, ok)
	return ok
}

func (s *Service) Subscribers(channelID string) ([]string, error) {
	return s.db.memberUIDs(channelID)
}

func (s *Service) ChannelGet(channelID string, loginUID string) (*TopicRoom, error) {
	room, err := s.db.get(channelID)
	if err != nil {
		return nil, err
	}
	if loginUID != "" {
		_ = s.db.loadUnread(room, loginUID)
	}
	s.prepareRoomForResponse(room, loginUID, false)
	return room, nil
}

func (s *Service) listenerMessages(messages []*config.MessageResp) {
	if len(messages) == 0 {
		return
	}

	validMessages := make([]*config.MessageResp, 0, len(messages))
	uids := make([]string, 0, len(messages))
	for _, msg := range messages {
		if msg == nil || msg.ChannelType != common.ChannelTypeGroup.Uint8() || msg.ChannelID == "" {
			continue
		}
		if !s.IsTopicChannel(msg.ChannelID) {
			continue
		}
		validMessages = append(validMessages, msg)
		if msg.FromUID != "" {
			uids = append(uids, msg.FromUID)
		}
	}
	if len(validMessages) == 0 {
		return
	}

	userMap, err := s.db.queryUserMetas(uids)
	if err != nil {
		log.Error("批量查询话题回复用户资料失败", zap.Error(err))
		userMap = map[string]UserMeta{}
	}

	for _, msg := range validMessages {
		user := userMap[msg.FromUID]
		if user.UID == "" {
			user.UID = msg.FromUID
			user.Avatar = fmt.Sprintf("users/%s/avatar", msg.FromUID)
		}
		text, msgType := messagePreview(msg)
		createdAt := int64(msg.Timestamp) * 1000
		if createdAt <= 0 {
			createdAt = time.Now().UnixMilli()
		}
		messageID := ""
		if msg.MessageID > 0 {
			messageID = strconv.FormatInt(msg.MessageID, 10)
		}
		s.queueLastReply(msg.ChannelID, &MessageWebhookReq{
			MessageID:       messageID,
			ChannelID:       msg.ChannelID,
			FromUID:         user.UID,
			FromName:        user.Name,
			FromAvatar:      user.Avatar,
			FromCountryCode: user.CountryCode,
			FromCountry:     user.Country,
			Text:            text,
			MessageType:     msgType,
			CreatedAt:       createdAt,
		})
	}
}

func messagePreview(msg *config.MessageResp) (string, string) {
	payload, err := msg.GetPayloadMap()
	if err != nil || payload == nil {
		return "[消息]", "message"
	}

	msgType := "message"
	if v, ok := payload["type"]; ok {
		msgType = fmt.Sprint(v)
	}
	for _, key := range []string{"content", "text", "summary"} {
		if v, ok := payload[key]; ok {
			text := strings.TrimSpace(fmt.Sprint(v))
			if text != "" && text != "<nil>" {
				return text, msgType
			}
		}
	}
	return previewText(msgType), msgType
}

func (s *Service) startLastReplyFlushLoop(ctx context.Context) {
	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		ticker := time.NewTicker(topicLastReplyFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.FlushPendingLastReplies()
				return
			case <-ticker.C:
				s.FlushPendingLastReplies()
			}
		}
	}()
}

func (s *Service) queueLastReply(roomID string, req *MessageWebhookReq) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" || req == nil {
		return
	}

	cloned := *req
	if cloned.RoomID == "" {
		cloned.RoomID = roomID
	}
	if cloned.ChannelID == "" {
		cloned.ChannelID = roomID
	}
	if cloned.CreatedAt <= 0 {
		cloned.CreatedAt = time.Now().UnixMilli()
	}

	participantKey := strings.TrimSpace(cloned.FromUID)
	if participantKey == "" {
		participantKey = strings.TrimSpace(cloned.FromAvatar)
	}

	s.lastReplyMu.Lock()
	defer s.lastReplyMu.Unlock()

	if cloned.MessageID != "" {
		now := time.Now().UnixMilli()
		messageKey := roomID + ":" + cloned.MessageID
		if expiresAt := s.recentMessageIDs[messageKey]; expiresAt > now {
			return
		}
		if s.recentMessageIDs == nil {
			s.recentMessageIDs = map[string]int64{}
		}
		if len(s.recentMessageIDs) >= topicRecentMessageIDMaxEntries {
			for key, expiresAt := range s.recentMessageIDs {
				if expiresAt <= now || len(s.recentMessageIDs) >= topicRecentMessageIDMaxEntries {
					delete(s.recentMessageIDs, key)
				}
				if len(s.recentMessageIDs) < topicRecentMessageIDMaxEntries {
					break
				}
			}
		}
		s.recentMessageIDs[messageKey] = time.Now().Add(topicRecentMessageIDTTL).UnixMilli()
	}

	if s.lastReplyPending == nil {
		s.lastReplyPending = map[string]*lastReplyFlushItem{}
	}
	item := s.lastReplyPending[roomID]
	if item == nil {
		item = &lastReplyFlushItem{Participants: map[string]*MessageWebhookReq{}}
		s.lastReplyPending[roomID] = item
	}
	item.Count++
	if item.Latest == nil || cloned.CreatedAt >= item.Latest.CreatedAt {
		latest := cloned
		item.Latest = &latest
	}
	if participantKey != "" {
		old := item.Participants[participantKey]
		if old == nil || cloned.CreatedAt >= old.CreatedAt {
			participant := cloned
			item.Participants[participantKey] = &participant
		}
	}
}

func (s *Service) FlushPendingLastReplies() {
	s.lastReplyMu.Lock()
	pending := s.lastReplyPending
	s.lastReplyPending = map[string]*lastReplyFlushItem{}
	s.lastReplyMu.Unlock()

	for roomID, item := range pending {
		if item == nil || item.Latest == nil || item.Count <= 0 {
			continue
		}
		if err := s.flushLastReplyItem(roomID, item); err != nil {
			log.Error("刷新话题最后回复失败，已重新入队", zap.Error(err), zap.String("room_id", roomID), zap.Int("count", item.Count))
			s.requeueLastReplyItem(roomID, item)
		}
	}
}

func (s *Service) flushLastReplyItem(roomID string, item *lastReplyFlushItem) error {
	participants := make([]*MessageWebhookReq, 0, len(item.Participants))
	for _, participant := range item.Participants {
		if participant != nil {
			cloned := *participant
			participants = append(participants, &cloned)
		}
	}
	sort.SliceStable(participants, func(i, j int) bool {
		return participants[i].CreatedAt > participants[j].CreatedAt
	})

	var err error
	for attempt := 1; attempt <= topicLastReplyFlushRetries; attempt++ {
		_, err = s.db.updateLastReplyBatch(roomID, item.Latest, item.Count, participants, s.ttl())
		if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrRoomExpired) {
			return nil
		}
		if attempt < topicLastReplyFlushRetries {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
	}
	return err
}

func (s *Service) requeueLastReplyItem(roomID string, failed *lastReplyFlushItem) {
	if failed == nil || failed.Latest == nil || failed.Count <= 0 {
		return
	}
	s.lastReplyMu.Lock()
	defer s.lastReplyMu.Unlock()
	if s.lastReplyPending == nil {
		s.lastReplyPending = map[string]*lastReplyFlushItem{}
	}
	current := s.lastReplyPending[roomID]
	if current == nil {
		current = &lastReplyFlushItem{Participants: map[string]*MessageWebhookReq{}}
		s.lastReplyPending[roomID] = current
	}
	current.Count += failed.Count
	if current.Latest == nil || failed.Latest.CreatedAt >= current.Latest.CreatedAt {
		cloned := *failed.Latest
		current.Latest = &cloned
	}
	for key, participant := range failed.Participants {
		if participant == nil {
			continue
		}
		old := current.Participants[key]
		if old == nil || participant.CreatedAt >= old.CreatedAt {
			cloned := *participant
			current.Participants[key] = &cloned
		}
	}
}

func (s *Service) setTopicChannelCache(channelID string, ok bool) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return
	}

	now := time.Now().UnixMilli()
	s.topicChannelMu.Lock()
	defer s.topicChannelMu.Unlock()
	if s.topicChannelCache == nil {
		s.topicChannelCache = map[string]topicChannelCacheItem{}
	}
	if len(s.topicChannelCache) >= topicChannelCacheMaxEntries {
		for key, item := range s.topicChannelCache {
			if item.ExpiresAt <= now || len(s.topicChannelCache) >= topicChannelCacheMaxEntries {
				delete(s.topicChannelCache, key)
			}
			if len(s.topicChannelCache) < topicChannelCacheMaxEntries {
				break
			}
		}
	}
	s.topicChannelCache[channelID] = topicChannelCacheItem{OK: ok, ExpiresAt: time.Now().Add(topicChannelCacheTTL).UnixMilli()}
}

func (s *Service) syncIMChannelWithStoredMembers(room *TopicRoom) error {
	if room == nil {
		return nil
	}
	uids, err := s.db.memberUIDs(room.ChannelID)
	if err != nil {
		return err
	}
	if len(uids) == 0 && room.CreatorUID != "" {
		uids = []string{room.CreatorUID}
	}
	return s.syncIMChannel(room, uids)
}

func (s *Service) syncIMChannel(room *TopicRoom, subscribers []string) error {
	if room == nil || room.ChannelID == "" {
		return nil
	}
	return s.ctx.IMCreateOrUpdateChannel(&config.ChannelCreateReq{
		ChannelID:   room.ChannelID,
		ChannelType: common.ChannelTypeGroup.Uint8(),
		Subscribers: compactUIDs(subscribers),
	})
}

func (s *Service) addIMSubscribers(channelID string, subscribers []string) error {
	if channelID == "" {
		return nil
	}
	uids := compactUIDs(subscribers)
	if len(uids) == 0 {
		return nil
	}
	return s.ctx.IMAddSubscriber(&config.SubscriberAddReq{
		ChannelID:   channelID,
		ChannelType: common.ChannelTypeGroup.Uint8(),
		Subscribers: uids,
	})
}

func (s *Service) topicRoomMemberUIDs(room *TopicRoom) []string {
	if room == nil || room.ChannelID == "" {
		return nil
	}
	uids, err := s.db.memberUIDs(room.ChannelID)
	if err != nil {
		log.Error("查询话题房成员失败", zap.Error(err), zap.String("channel_id", room.ChannelID))
	}
	if len(uids) == 0 && room.CreatorUID != "" {
		uids = []string{room.CreatorUID}
	}
	return compactUIDs(uids)
}

func (s *Service) notifyTopicRoomDeleted(channelID string, subscribers []string, reason string) {
	channelID = strings.TrimSpace(channelID)
	if channelID == "" {
		return
	}
	uids := compactUIDs(subscribers)
	if len(uids) == 0 {
		return
	}
	if reason == "" {
		reason = "deleted"
	}
	uidCopy := append([]string(nil), uids...)
	if s.deleteNotifySem != nil {
		s.deleteNotifySem <- struct{}{}
	}
	go func() {
		if s.deleteNotifySem != nil {
			defer func() { <-s.deleteNotifySem }()
		}
		s.sendTopicRoomDeletedBatches(channelID, uidCopy, reason)
	}()
}

func (s *Service) sendTopicRoomDeletedBatches(channelID string, uids []string, reason string) {
	for i, uid := range uids {
		if err := s.ctx.SendCMD(config.MsgCMDReq{
			ChannelID:   uid,
			ChannelType: common.ChannelTypePerson.Uint8(),
			CMD:         "topicRoomDeleted",
			Param: map[string]interface{}{
				"channel_id":   channelID,
				"room_id":      channelID,
				"channel_type": common.ChannelTypeGroup.Uint8(),
				"reason":       reason,
			},
		}); err != nil {
			log.Error("发送话题房删除通知失败", zap.Error(err), zap.String("channel_id", channelID), zap.String("uid", uid))
		}
		if (i+1)%topicRoomDeletedNotifyBatchSize == 0 {
			time.Sleep(topicRoomDeletedNotifyBatchSleep)
		}
	}
}

func (s *Service) deleteIMChannel(channelID string) error {
	if channelID == "" {
		return nil
	}
	return s.ctx.IMDelChannel(&config.ChannelDeleteReq{
		ChannelID:   channelID,
		ChannelType: common.ChannelTypeGroup.Uint8(),
	})
}

func compactUIDs(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, uid := range in {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		out = append(out, uid)
	}
	return out
}

func roomIDFromReq(req RoomReq) string {
	roomID := strings.TrimSpace(req.RoomID)
	if roomID == "" {
		roomID = strings.TrimSpace(req.ChannelID)
	}
	return roomID
}

func (s *Service) prepareRoomForResponse(room *TopicRoom, loginUID string, isAdmin bool) {
	if room == nil {
		return
	}
	room.Tag = normalizeRoomTag(room.Tag)
	room.CanPin = 0
	room.CanDelete = 0
	if isAdmin {
		room.CanPin = 1
		room.CanDelete = 1
	} else if loginUID != "" && room.CreatorUID == loginUID {
		room.CanDelete = 1
	}
}

func (s *Service) ttl() time.Duration {
	if s.TTL <= 0 {
		return DefaultTTL
	}
	return s.TTL
}
