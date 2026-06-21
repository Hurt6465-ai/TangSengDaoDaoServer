package chatrooms

import "time"

const (
	DefaultTTL             = 3 * time.Hour
	ChannelTypeGroup       = 2
	DefaultMaxReplyAvatars = 6
)

type TopicRoom struct {
	RoomID             string        `json:"room_id" db:"room_id"`
	Title              string        `json:"title" db:"title"`
	Tag                string        `json:"tag" db:"tag"`
	Language           string        `json:"language" db:"language"`
	BackgroundURL      string        `json:"background_url" db:"background_url"`
	BackgroundIndex    int           `json:"background_index" db:"background_index"`
	ChannelID          string        `json:"channel_id" db:"channel_id"`
	ChannelType        int           `json:"channel_type" db:"channel_type"`
	CreatorUID         string        `json:"creator_uid" db:"creator_uid"`
	CreatorName        string        `json:"creator_name" db:"creator_name"`
	CreatorAvatar      string        `json:"creator_avatar" db:"creator_avatar"`
	LastReplyUID       string        `json:"last_reply_uid" db:"last_reply_uid"`
	LastReplyName      string        `json:"last_reply_name" db:"last_reply_name"`
	LastReplyAvatar    string        `json:"last_reply_avatar" db:"last_reply_avatar"`
	LastReplyText      string        `json:"last_reply_text" db:"last_reply_text"`
	LastReplyType      string        `json:"last_reply_type" db:"last_reply_type"`
	LastReplyAt        int64         `json:"last_reply_at" db:"last_reply_at"`
	ReplyCount         int           `json:"reply_count" db:"reply_count"`
	ParticipantCount   int           `json:"participant_count" db:"participant_count"`
	ReplyUsers         []ReplyAvatar `json:"reply_users" db:"-"`
	ReplyUsersJSON     string        `json:"-" db:"reply_users_json"`
	UnreadCount        int           `json:"unread_count" db:"-"`
	MentionUnreadCount int           `json:"mention_unread_count" db:"-"`
	Pinned             int           `json:"pinned" db:"pinned"`
	Hot                int           `json:"hot" db:"hot"`
	HotUntil           int64         `json:"hot_until" db:"hot_until"`
	Status             int           `json:"-" db:"status"`
	CreatedAt          int64         `json:"created_at" db:"created_at"`
	ExpireAt           int64         `json:"expire_at" db:"expire_at"`
}

type ReplyAvatar struct {
	UID            string `json:"uid"`
	Name           string `json:"name"`
	Avatar         string `json:"avatar"`
	AvatarCacheKey string `json:"avatar_cache_key,omitempty"`
	Flag           string `json:"flag,omitempty"`
}

type CreateReq struct {
	Title    string `json:"title" binding:"required"`
	Tag      string `json:"tag"`
	Language string `json:"language"`
}

type RoomReq struct {
	RoomID      string `json:"room_id"`
	ChannelID   string `json:"channel_id"`
	ChannelType int    `json:"channel_type"`
	Pinned      int    `json:"pinned"`
}

type ListResp struct {
	Rooms      []*TopicRoom `json:"rooms"`
	ServerTime int64        `json:"server_time"`
}

type MessageWebhookReq struct {
	RoomID      string   `json:"room_id"`
	ChannelID   string   `json:"channel_id"`
	FromUID     string   `json:"from_uid"`
	FromName    string   `json:"from_name"`
	FromAvatar  string   `json:"from_avatar"`
	Text        string   `json:"text"`
	Summary     string   `json:"summary"`
	MessageType string   `json:"message_type"`
	MentionUIDs []string `json:"mention_uids"`
	CreatedAt   int64    `json:"created_at"`
}

type UserMeta struct {
	UID    string
	Name   string
	Avatar string
}
