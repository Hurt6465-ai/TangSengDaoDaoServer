package chatrooms

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
)

type Service struct {
	ctx *config.Context
	db  *db
	TTL time.Duration
}

func NewService(ctx *config.Context) *Service {
	return &Service{ctx: ctx, db: newDB(ctx), TTL: DefaultTTL}
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
		RoomID:          roomID,
		Title:           title,
		Tag:             req.Tag,
		Language:        req.Language,
		BackgroundIndex: int(ts%12) + 1,
		ChannelID:       roomID,
		ChannelType:     ChannelTypeGroup,
		CreatorUID:      user.UID,
		CreatorName:     user.Name,
		CreatorAvatar:   user.Avatar,
		CreatedAt:       ts,
		ExpireAt:        ts + int64(s.ttl()/time.Millisecond),
	}
	if err := s.db.create(room); err != nil {
		return nil, err
	}
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
		_ = s.db.addMember(room.RoomID, room.ChannelID, uid, user.Name, user.Avatar)
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
	return s.db.softDelete(roomID)
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
		if err := s.db.softDelete(room.RoomID); err == nil {
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

func (s *Service) ttl() time.Duration {
	if s.TTL <= 0 {
		return DefaultTTL
	}
	return s.TTL
}
