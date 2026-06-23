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
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertInto("topic_rooms").Columns(
		"room_id", "title", "tag", "language", "background_url", "background_index", "channel_id", "channel_type",
		"creator_uid", "creator_name", "creator_avatar", "last_reply_uid", "last_reply_name", "last_reply_avatar",
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "participant_count", "reply_users_json",
		"pinned", "hot", "hot_until", "status", "created_at", "expire_at",
	).Values(
		room.RoomID, room.Title, room.Tag, room.Language, room.BackgroundURL, room.BackgroundIndex, room.ChannelID, room.ChannelType,
		room.CreatorUID, room.CreatorName, room.CreatorAvatar, room.LastReplyUID, room.LastReplyName, room.LastReplyAvatar,
		room.LastReplyText, room.LastReplyType, room.LastReplyAt, room.ReplyCount, room.ParticipantCount, string(users),
		room.Pinned, room.Hot, room.HotUntil, 1, room.CreatedAt, room.ExpireAt,
	).Exec()
	if err != nil {
		return err
	}
	if err = d.ensureNativeGroupTx(tx, room); err != nil {
		return err
	}
	if err = d.addMemberTx(tx, room.RoomID, room.ChannelID, room.CreatorUID, room.CreatorName, room.CreatorAvatar, 1, false); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) addMember(roomID, channelID, uid, name, avatar string) error {
	if roomID == "" || uid == "" {
		return nil
	}
	room, err := d.get(roomID)
	if err != nil {
		return err
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	if err = d.ensureNativeGroupTx(tx, room); err != nil {
		return err
	}
	role := 0
	if uid == room.CreatorUID {
		role = 1
	}
	if err = d.addMemberTx(tx, room.RoomID, room.ChannelID, uid, name, avatar, role, true); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) ensureNativeGroupTx(tx *dbr.Tx, room *TopicRoom) error {
	if room == nil || room.ChannelID == "" {
		return nil
	}
	version := time.Now().UnixMilli()
	_, err := tx.InsertBySql("INSERT INTO `group`(group_no,name,avatar,creator,status,forbidden,invite,forbidden_add_friend,allow_view_history_msg,version,group_type,category,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NOW(),NOW()) ON DUPLICATE KEY UPDATE name=VALUES(name), avatar=IF(avatar='',VALUES(avatar),avatar), creator=IF(creator='',VALUES(creator),creator), status=1, category='topic_room', updated_at=NOW()",
		room.ChannelID, room.Title, room.CreatorAvatar, room.CreatorUID, 1, 0, 0, 0, 1, version, 0, "topic_room").Exec()
	return err
}

func (d *db) addMemberTx(tx *dbr.Tx, roomID, channelID, uid, name, avatar string, role int, countParticipant bool) error {
	if roomID == "" || channelID == "" || uid == "" {
		return nil
	}
	if role != 1 {
		role = 0
	}
	version := time.Now().UnixMilli()
	res, err := tx.InsertBySql(`INSERT INTO topic_room_members(room_id,channel_id,uid,name,avatar,last_read_at,created_at,updated_at)
        VALUES(?,?,?,?,?,0,UNIX_TIMESTAMP()*1000,NOW())
        ON DUPLICATE KEY UPDATE name=VALUES(name), avatar=VALUES(avatar), updated_at=NOW()`, roomID, channelID, uid, name, avatar).Exec()
	if err != nil {
		return err
	}
	if countParticipant && res != nil {
		if affected, _ := res.RowsAffected(); affected == 1 {
			if _, err = tx.Update("topic_rooms").Set("participant_count", dbr.Expr("participant_count+1")).Set("updated_at", dbr.Expr("NOW()")).Where("room_id=?", roomID).Exec(); err != nil {
				return err
			}
		}
	}
	_, err = tx.InsertBySql(`INSERT INTO group_member(group_no,uid,remark,role,version,is_deleted,status,vercode,robot,invite_uid,created_at,updated_at)
        VALUES(?,?,?,?,?,0,1,CONCAT(?, '@2'),0,'',NOW(),NOW())
        ON DUPLICATE KEY UPDATE role=IF(role=1,1,VALUES(role)), version=VALUES(version), is_deleted=0, status=1, updated_at=NOW()`,
		channelID, uid, "", role, version, uid).Exec()
	return err
}

func (d *db) memberUIDs(channelID string) ([]string, error) {
	var uids []string
	_, err := d.session.Select("uid").From("topic_room_members").Where("channel_id=?", channelID).OrderBy("updated_at DESC").Load(&uids)
	return uids, err
}

func (d *db) list(loginUID string) ([]*TopicRoom, error) {
	rooms := make([]*TopicRoom, 0)
	// 不再按 loginUID 隐藏已加入话题。之前隐藏会导致用户发完/进完话题后，聊天室广场立刻“消失”。
	_, err := d.session.Select("room_id", "title", "tag", "language", "background_url", "background_index", "channel_id", "channel_type",
		"creator_uid", "creator_name", "creator_avatar", "last_reply_uid", "last_reply_name", "last_reply_avatar",
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "participant_count", "reply_users_json", "pinned", "hot", "hot_until", "status", "created_at", "expire_at").
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
		d.enrichRoomUserCountries(r)
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
		"last_reply_text", "last_reply_type", "last_reply_at", "reply_count", "participant_count", "reply_users_json", "pinned", "hot", "hot_until", "status", "created_at", "expire_at").
		From("topic_rooms").Where("status=1 AND (room_id=? OR channel_id=?)", roomID, roomID).Limit(1).Load(&rooms)
	if err != nil {
		return nil, err
	}
	if len(rooms) == 0 {
		return nil, ErrNotFound
	}
	decodeReplyUsers(rooms[0])
	d.enrichRoomUserCountries(rooms[0])
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
	room, _ := d.get(roomID)
	channelID := roomID
	if room != nil && room.ChannelID != "" {
		channelID = room.ChannelID
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	if _, err = tx.Update("topic_rooms").Set("status", 0).Set("updated_at", dbr.Expr("NOW()")).Where("room_id=? OR channel_id=?", roomID, roomID).Exec(); err != nil {
		return err
	}
	// 解散原生群并删除成员。客户端下一次同步会话时不再把该公开话题当成有效群。
	_, _ = tx.Update("group_member").Set("is_deleted", 1).Set("updated_at", dbr.Expr("NOW()")).Where("group_no=?", channelID).Exec()
	_, _ = tx.Update("group").Set("status", 2).Set("updated_at", dbr.Expr("NOW()")).Where("group_no=?", channelID).Exec()
	return tx.Commit()
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
	if update.FromUID != "" {
		_ = d.addMember(room.RoomID, room.ChannelID, update.FromUID, update.FromName, update.FromAvatar)
	}
	fromCountryCode := ""
	fromCountry := ""
	if update.FromUID != "" {
		if userMeta, err := d.queryUserMeta(update.FromUID); err == nil {
			if update.FromName == "" {
				update.FromName = userMeta.Name
			}
			if update.FromAvatar == "" {
				update.FromAvatar = userMeta.Avatar
			}
			fromCountryCode = userMeta.CountryCode
			fromCountry = userMeta.Country
		}
	}
	users := dedupReplyUsers("", append([]ReplyAvatar{{UID: update.FromUID, Name: update.FromName, Avatar: update.FromAvatar, CountryCode: fromCountryCode, Country: fromCountry}}, room.ReplyUsers...), DefaultMaxReplyAvatars)
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
		Set("hot", dbr.Expr("IF(reply_count+1>=10,1,hot)")).
		Set("hot_until", dbr.Expr("IF(reply_count+1>=10,?,hot_until)", hotUntil)).
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
	meta := UserMeta{UID: uid, Name: user.Name, Avatar: fmt.Sprintf("users/%s/avatar", uid)}
	if !d.hasUserCountryColumns() {
		return meta, nil
	}
	countryRows := make([]*struct {
		UID         string `db:"uid"`
		CountryCode string `db:"country_code"`
		Country     string `db:"country"`
	}, 0)
	_, err = d.session.Select("uid", "country_code", "country").From("user").Where("uid=?", uid).Limit(1).Load(&countryRows)
	if err != nil {
		return meta, nil
	}
	if len(countryRows) > 0 {
		meta.CountryCode = countryRows[0].CountryCode
		meta.Country = countryRows[0].Country
	}
	return meta, nil
}

func (d *db) enrichRoomUserCountries(r *TopicRoom) {
	if r == nil || !d.hasUserCountryColumns() {
		return
	}
	uids := make([]string, 0, len(r.ReplyUsers)+1)
	if r.CreatorUID != "" {
		uids = append(uids, r.CreatorUID)
	}
	for _, u := range r.ReplyUsers {
		if u.UID != "" {
			uids = append(uids, u.UID)
		}
	}
	uids = compactUIDsForDB(uids)
	if len(uids) == 0 {
		return
	}
	rows := make([]*struct {
		UID         string `db:"uid"`
		CountryCode string `db:"country_code"`
		Country     string `db:"country"`
	}, 0)
	_, err := d.session.Select("uid", "country_code", "country").From("user").Where("uid in ?", uids).Load(&rows)
	if err != nil {
		return
	}
	meta := map[string]*struct {
		UID         string `db:"uid"`
		CountryCode string `db:"country_code"`
		Country     string `db:"country"`
	}{}
	for _, row := range rows {
		meta[row.UID] = row
	}
	if row := meta[r.CreatorUID]; row != nil {
		r.CreatorCountryCode = row.CountryCode
		r.CreatorCountry = row.Country
	}
	for i := range r.ReplyUsers {
		if row := meta[r.ReplyUsers[i].UID]; row != nil {
			r.ReplyUsers[i].CountryCode = row.CountryCode
			r.ReplyUsers[i].Country = row.Country
		}
	}
}

func (d *db) hasUserCountryColumns() bool {
	var count int
	err := d.session.Select("COUNT(*)").
		From("information_schema.COLUMNS").
		Where("TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME in ?", "user", []string{"country_code", "country"}).
		LoadOne(&count)
	return err == nil && count >= 2
}

func compactUIDsForDB(in []string) []string {
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
		if key == "" {
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
