package chatrooms

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/base/event"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkevent"
)

type Service struct {
	ctx *config.Context
	db  *db
	TTL time.Duration
}

func NewService(ctx *config.Context) *Service {
	svc := &Service{ctx: ctx, db: newDB(ctx), TTL: DefaultTTL}
	// 事件驱动更新：避免话题有回复但 expire_at 不续期，导致“正在聊的话题 3 小时后消失”。
	ctx.AddMessagesListener(svc.listenerMessages)
	return svc
}

func (s *Service) List(uid string) ([]*TopicRoom, error) {
	return s.db.list(uid)
}

func (s *Service) Create(req CreateReq, loginUID string) (*TopicRoom, error) {
	title := strings.TrimSpace(req.Title)
	if title == "" {
		return nil, errors.New("话题名不能为空")
	}
	if loginUID == "" {
		return nil, errors.New("未登录")
	}
	if req.Tag == "" {
		req.Tag = "闲谈"
	}
	if req.Language == "" {
		req.Language = "中文"
	}
	user, err := s.db.queryUserMeta(loginUID)
	if err != nil {
		return nil, err
	}
	if user.UID == "" {
		user.UID = loginUID
	}
	ts := time.Now().UnixMilli()
	roomID := fmt.Sprintf("topic_%d", time.Now().UnixNano())
	room := &TopicRoom{
		RoomID:             roomID,
		Title:              title,
		Tag:                req.Tag,
		Language:           req.Language,
		BackgroundIndex:    int(ts%12) + 1,
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
	if err := s.db.create(room); err != nil {
		return nil, err
	}
	if err := s.syncIMChannel(room, []string{room.CreatorUID}); err != nil {
		_ = s.db.softDelete(room.RoomID)
		return nil, err
	}
	_ = s.refreshGroupAvatar(room.ChannelID, []string{room.CreatorUID})
	return room, nil
}

func (s *Service) Enter(req RoomReq, uid string) (*TopicRoom, error) {
	roomID := req.RoomID
	if roomID == "" {
		roomID = req.ChannelID
	}
	room, err := s.db.get(roomID)
	if err != nil {
		return nil, err
	}
	if uid != "" {
		user, _ := s.db.queryUserMeta(uid)
		if err := s.db.addMember(room.RoomID, room.ChannelID, uid, user.Name, user.Avatar); err != nil {
			return nil, err
		}
		uids, _ := s.db.memberUIDs(room.ChannelID)
		if len(uids) == 0 {
			uids = []string{uid}
		}
		if err := s.syncIMChannel(room, uids); err != nil {
			return nil, err
		}
		if err := s.addIMSubscribers(room.ChannelID, []string{uid}); err != nil {
			return nil, err
		}
		_ = s.refreshGroupAvatar(room.ChannelID, uids)
	}
	return room, nil
}

func (s *Service) Pin(req RoomReq) (*TopicRoom, error) {
	roomID := req.RoomID
	if roomID == "" {
		roomID = req.ChannelID
	}
	return s.db.updatePin(roomID, req.Pinned)
}

func (s *Service) Delete(req RoomReq) error {
	roomID := req.RoomID
	if roomID == "" {
		roomID = req.ChannelID
	}
	room, _ := s.db.get(roomID)
	uids := s.topicRoomMemberUIDs(room)
	if err := s.db.softDelete(roomID); err != nil {
		return err
	}
	if room != nil {
		s.notifyTopicRoomDeleted(room.ChannelID, uids, "deleted")
		_ = s.deleteIMChannel(room.ChannelID)
	}
	return nil
}

func (s *Service) MessageWebhook(req *MessageWebhookReq) (*TopicRoom, error) {
	roomID := req.RoomID
	if roomID == "" {
		roomID = req.ChannelID
	}
	if roomID == "" {
		return nil, errors.New("缺少 room_id/channel_id")
	}
	return s.db.updateLastReply(roomID, req)
}

func (s *Service) CleanupExpired(limit uint64) (int, error) {
	if limit == 0 {
		limit = 100
	}
	rooms, err := s.db.expired(time.Now().UnixMilli(), limit)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, room := range rooms {
		if room == nil {
			continue
		}
		uids := s.topicRoomMemberUIDs(room)
		if err := s.db.softDelete(room.RoomID); err == nil {
			s.notifyTopicRoomDeleted(room.ChannelID, uids, "expired")
			_ = s.deleteIMChannel(room.ChannelID)
			count++
		}
	}
	return count, nil
}

func (s *Service) IsTopicChannel(channelID string) bool {
	return s.db.isTopicChannel(channelID)
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
	return room, nil
}

func (s *Service) listenerMessages(messages []*config.MessageResp) {
	if len(messages) == 0 {
		return
	}
	for _, msg := range messages {
		if msg == nil || msg.ChannelType != common.ChannelTypeGroup.Uint8() || msg.ChannelID == "" {
			continue
		}
		if !s.db.isTopicChannel(msg.ChannelID) {
			continue
		}
		user, _ := s.db.queryUserMeta(msg.FromUID)
		if user.UID == "" {
			user.UID = msg.FromUID
		}
		text, msgType := messagePreview(msg)
		createdAt := int64(msg.Timestamp) * 1000
		if createdAt <= 0 {
			createdAt = time.Now().UnixMilli()
		}
		_, _ = s.db.updateLastReply(msg.ChannelID, &MessageWebhookReq{
			ChannelID:   msg.ChannelID,
			FromUID:     user.UID,
			FromName:    user.Name,
			FromAvatar:  user.Avatar,
			Text:        text,
			MessageType: msgType,
			CreatedAt:   createdAt,
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
	uids, _ := s.db.memberUIDs(room.ChannelID)
	if len(uids) == 0 && room.CreatorUID != "" {
		uids = []string{room.CreatorUID}
	}
	return compactUIDs(uids)
}

func (s *Service) notifyTopicRoomDeleted(channelID string, subscribers []string, reason string) {
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
	for _, uid := range uids {
		_ = s.ctx.SendCMD(config.MsgCMDReq{
			ChannelID:   uid,
			ChannelType: common.ChannelTypePerson.Uint8(),
			CMD:         "topicRoomDeleted",
			Param: map[string]interface{}{
				"channel_id":   channelID,
				"room_id":      channelID,
				"channel_type": common.ChannelTypeGroup.Uint8(),
				"reason":       reason,
			},
		})
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

func (s *Service) refreshGroupAvatar(channelID string, uids []string) error {
	if channelID == "" {
		return nil
	}
	members := compactUIDs(uids)
	if len(members) == 0 {
		return nil
	}
	if len(members) > 9 {
		members = members[:9]
	}
	eventID, err := s.ctx.EventBegin(&wkevent.Data{
		Event: event.GroupAvatarUpdate,
		Type:  wkevent.CMD,
		Data: &config.CMDGroupAvatarUpdateReq{
			GroupNo: channelID,
			Members: members,
		},
	}, nil)
	if err != nil {
		return err
	}
	s.ctx.EventCommit(eventID)
	return nil
}

func (s *Service) ttl() time.Duration {
	if s.TTL <= 0 {
		return DefaultTTL
	}
	return s.TTL
}
