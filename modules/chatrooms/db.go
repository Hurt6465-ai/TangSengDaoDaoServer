package chatrooms

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/gocraft/dbr/v2"
)

var ErrNotFound = errors.New("chatroom not found")

type db struct {
	session *dbr.Session
	ctx     *config.Context
}

func newDB(ctx *config.Context) *db {
	return &db{session: ctx.DB(), ctx: ctx}
}

func (d *db) create(room *TopicRoom) error {
	users, _ := json.Marshal(room.ReplyUsers)
	_, err := d.session.InsertInto("topic_rooms").Columns(
		"room_id", "title", "tag", "language", "background_url", "background_index", "channel_id", "channel_type",
		"creator_uid", "creator_name", "creator_avatar", "last_reply_uid", "last_reply_name", "last_reply_avatar",
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "reply_users_json",
		"pinned", "hot", "hot_until", "status", "created_at", "expire_at",
	).Values(
		room.RoomID, room.Title, room.Tag, room.Language, room.BackgroundURL, room.BackgroundIndex, room.ChannelID, room.ChannelType,
		room.CreatorUID, room.CreatorName, room.CreatorAvatar, room.LastReplyUID, room.LastReplyName, room.LastReplyAvatar,
		room.LastReplyText, room.LastReplyType, room.LastReplyAt, room.ReplyCount, string(users),
		room.Pinned, room.Hot, room.HotUntil, 1, room.CreatedAt, room.ExpireAt,
	).Exec()
	if err != nil {
		return err
	}
	return d.addMember(room.RoomID, room.ChannelID, room.CreatorUID, room.CreatorName, room.CreatorAvatar)
}

func (d *db) addMember(roomID, channelID, uid, name, avatar string) error {
	if roomID == "" || uid == "" {
		return nil
	}
	_, err := d.session.InsertBySql(`INSERT INTO topic_room_members(room_id,channel_id,uid,name,avatar,last_read_at,created_at,updated_at)
        VALUES(?,?,?,?,?,0,UNIX_TIMESTAMP()*1000,NOW())
        ON DUPLICATE KEY UPDATE name=VALUES(name), avatar=VALUES(avatar), updated_at=NOW()`, roomID, channelID, uid, name, avatar).Exec()
	return err
}

func (d *db) memberUIDs(channelID string) ([]string, error) {
	var uids []string
	_, err := d.session.Select("uid").From("topic_room_members").Where("channel_id=?", channelID).Load(&uids)
	return uids, err
}

func (d *db) list(loginUID string) ([]*TopicRoom, error) {
	rooms := make([]*TopicRoom, 0)
	_, err := d.session.Select("room_id", "title", "tag", "language", "background_url", "background_index", "channel_id", "channel_type",
		"creator_uid", "creator_name", "creator_avatar", "last_reply_uid", "last_reply_name", "last_reply_avatar",
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "reply_users_json", "pinned", "hot", "hot_until", "status", "created_at", "expire_at").
		From("topic_rooms").
		Where("status=1").
		OrderBy("pinned DESC").
		OrderBy("(hot_until > UNIX_TIMESTAMP()*1000) DESC").
		OrderBy("COALESCE(NULLIF(last_reply_at,0),created_at) DESC").
		Limit(200).
		Load(&rooms)
	if err != nil {
		return nil, err
	}
	for _, r := range rooms {
		decodeReplyUsers(r)
		if loginUID != "" {
			_ = d.loadUnread(r, loginUID)
		}
	}
	return rooms, nil
}

func (d *db) get(roomID string) (*TopicRoom, error) {
	if roomID == "" {
		return nil, ErrNotFound
	}
	rooms := make([]*TopicRoom, 0)
	_, err := d.session.Select("room_id", "title", "tag", "language", "background_url", "background_index", "channel_id", "channel_type",
		"creator_uid", "creator_name", "creator_avatar", "last_reply_uid", "last_reply_name", "last_reply_avatar",
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "reply_users_json", "pinned", "hot", "hot_until", "status", "created_at", "expire_at").
		From("topic_rooms").Where("status=1 AND (room_id=? OR channel_id=?)", roomID, roomID).Limit(1).Load(&rooms)
	if err != nil {
		return nil, err
	}
	if len(rooms) == 0 {
		return nil, ErrNotFound
	}
	decodeReplyUsers(rooms[0])
	return rooms[0], nil
}

func (d *db) isTopicChannel(channelID string) bool {
	if channelID == "" {
		return false
	}
	var count int
	err := d.session.Select("count(*)").From("topic_rooms").Where("status=1 AND channel_id=?", channelID).LoadOne(&count)
	return err == nil && count > 0
}

func (d *db) updatePin(roomID string, pinned int) (*TopicRoom, error) {
	if pinned != 0 {
		pinned = 1
	}
	_, err := d.session.Update("topic_rooms").Set("pinned", pinned).Set("updated_at", dbr.Expr("NOW()")).Where("status=1 AND (room_id=? OR channel_id=?)", roomID, roomID).Exec()
	if err != nil {
		return nil, err
	}
	return d.get(roomID)
}

func (d *db) softDelete(roomID string) error {
	_, err := d.session.Update("topic_rooms").Set("status", 0).Set("updated_at", dbr.Expr("NOW()")).Where("room_id=? OR channel_id=?", roomID, roomID).Exec()
	return err
}

func (d *db) updateLastReply(roomID string, update *MessageWebhookReq) (*TopicRoom, error) {
	room, err := d.get(roomID)
	if err != nil {
		return nil, err
	}
	ts := update.CreatedAt
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	text := strings.TrimSpace(update.Text)
	if text == "" {
		text = strings.TrimSpace(update.Summary)
	}
	if text == "" {
		text = previewText(update.MessageType)
	}
	users := dedupReplyUsers(room.CreatorUID, append([]ReplyAvatar{{UID: update.FromUID, Name: update.FromName, Avatar: update.FromAvatar}}, room.ReplyUsers...), DefaultMaxReplyAvatars)
	usersJSON, _ := json.Marshal(users)
	expireAt := ts + int64(DefaultTTL/time.Millisecond)
	hotUntil := ts + int64(10*time.Minute/time.Millisecond)
	_, err = d.session.Update("topic_rooms").
		Set("last_reply_uid", update.FromUID).
		Set("last_reply_name", update.FromName).
		Set("last_reply_avatar", update.FromAvatar).
		Set("last_reply_text", text).
		Set("last_reply_type", update.MessageType).
		Set("last_reply_at", ts).
		Set("reply_count", dbr.Expr("reply_count+1")).
		Set("reply_users_json", string(usersJSON)).
		Set("expire_at", expireAt).
		Set("hot", dbr.Expr("IF(reply_count>=10,1,hot)")).
		Set("hot_until", dbr.Expr("IF(reply_count>=10,?,hot_until)", hotUntil)).
		Set("updated_at", dbr.Expr("NOW()")).
		Where("status=1 AND (room_id=? OR channel_id=?)", roomID, roomID).Exec()
	if err != nil {
		return nil, err
	}
	return d.get(roomID)
}

func (d *db) expired(now int64, limit uint64) ([]*TopicRoom, error) {
	rooms := make([]*TopicRoom, 0)
	_, err := d.session.Select("room_id", "title", "channel_id", "channel_type").From("topic_rooms").Where("status=1 AND expire_at<=?", now).Limit(limit).Load(&rooms)
	return rooms, err
}

func (d *db) queryUserMeta(uid string) (UserMeta, error) {
	if uid == "" {
		return UserMeta{}, nil
	}
	var user struct {
		UID  string `db:"uid"`
		Name string `db:"name"`
	}
	rows := make([]*struct {
		UID  string `db:"uid"`
		Name string `db:"name"`
	}, 0)
	_, err := d.session.Select("uid", "name").From("user").Where("uid=?", uid).Limit(1).Load(&rows)
	if err != nil {
		return UserMeta{}, err
	}
	if len(rows) > 0 {
		user.UID = rows[0].UID
		user.Name = rows[0].Name
	}
	return UserMeta{UID: uid, Name: user.Name, Avatar: fmt.Sprintf("users/%s/avatar", uid)}, nil
}

func (d *db) loadUnread(r *TopicRoom, uid string) error {
	var lastReadAt int64
	err := d.session.Select("IFNULL(last_read_at,0)").From("topic_room_members").Where("room_id=? AND uid=?", r.RoomID, uid).LoadOne(&lastReadAt)
	if err != nil {
		return nil
	}
	if lastReadAt <= 0 {
		lastReadAt = r.CreatedAt
	}
	if r.LastReplyAt > lastReadAt {
		r.UnreadCount = 1
	}
	return nil
}

func decodeReplyUsers(r *TopicRoom) {
	if r == nil || r.ReplyUsersJSON == "" {
		return
	}
	_ = json.Unmarshal([]byte(r.ReplyUsersJSON), &r.ReplyUsers)
}

func dedupReplyUsers(creatorUID string, in []ReplyAvatar, max int) []ReplyAvatar {
	seen := map[string]struct{}{}
	out := make([]ReplyAvatar, 0, max)
	for _, u := range in {
		key := u.UID
		if key == "" {
			key = u.Avatar
		}
		if key == "" || key == creatorUID {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, u)
		if len(out) >= max {
			break
		}
	}
	return out
}

func previewText(t string) string {
	switch strings.ToLower(t) {
	case "image", "pic", "photo":
		return "[图片]"
	case "voice", "audio":
		return "[语音]"
	case "video":
		return "[视频]"
	case "gif", "sticker", "emoji":
		return "[表情]"
	default:
		if t == "" {
			return "[消息]"
		}
		return fmt.Sprintf("[%s]", t)
	}
}
