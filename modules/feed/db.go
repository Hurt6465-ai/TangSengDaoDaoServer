package feed

import (
	"fmt"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/gocraft/dbr/v2"
)

type db struct {
	session *dbr.Session
	ctx     *config.Context
}

func newDB(ctx *config.Context) *db {
	return &db{session: ctx.DB(), ctx: ctx}
}

func (d *db) listRecommend(loginUID string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	limit = clampLimit(limit)
	offset := offsetFrom(page, cursor, limit)
	var posts []*FeedPost
	_, err := d.session.SelectBySql(`SELECT p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        WHERE p.status=1 AND p.visibility='public'
        ORDER BY p.score DESC, p.last_active_at DESC, p.created_at DESC
        LIMIT ? OFFSET ?`, loginUID, limit+1, offset).Load(&posts)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if len(posts) > limit {
		hasMore = 1
		posts = posts[:limit]
	}
	if err := d.fillPosts(loginUID, posts); err != nil {
		return nil, 0, err
	}
	return posts, hasMore, nil
}

func (d *db) listByUser(loginUID, uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	limit = clampLimit(limit)
	offset := offsetFrom(page, cursor, limit)
	var posts []*FeedPost
	_, err := d.session.SelectBySql(`SELECT p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        WHERE p.status=1 AND p.visibility='public' AND p.uid=?
        ORDER BY p.created_at DESC
        LIMIT ? OFFSET ?`, loginUID, uid, limit+1, offset).Load(&posts)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if len(posts) > limit {
		hasMore = 1
		posts = posts[:limit]
	}
	if err := d.fillPosts(loginUID, posts); err != nil {
		return nil, 0, err
	}
	return posts, hasMore, nil
}

func (d *db) createPost(uid string, req PublishReq) (*FeedPost, error) {
	feedID := fmt.Sprintf("feed_%d", time.Now().UnixNano())
	text := strings.TrimSpace(req.Text)
	title := strings.TrimSpace(req.Title)
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = "public"
	}
	nowMs := time.Now().UnixMilli()
	tx, err := d.session.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertBySql(`INSERT INTO feed_posts(feed_id,uid,text,title,status,visibility,last_active_at,score,created_at,updated_at)
        VALUES(?,?,?,?,1,?,?,0,NOW(),NOW())`, feedID, uid, text, title, visibility, nowMs).Exec()
	if err != nil {
		return nil, err
	}
	for i, m := range req.Media {
		if m == nil {
			continue
		}
		if m.Sort == 0 {
			m.Sort = i
		}
		if m.Type == "" {
			m.Type = "image"
		}
		_, err = tx.InsertBySql(`INSERT INTO feed_media(feed_id,type,thumb_url,display_url,origin_url,cover_url,play_url_480p,play_url_540p,play_url_720p,width,height,duration_ms,size,sort,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),NOW())`, feedID, m.Type, m.ThumbURL, m.DisplayURL, m.OriginURL, m.CoverURL, m.PlayURL480P, m.PlayURL540P, m.PlayURL720P, m.Width, m.Height, m.DurationMS, m.Size, m.Sort).Exec()
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	post := &FeedPost{FeedID: feedID, UID: uid, Text: text, Title: title, Status: 1, Visibility: visibility, CreatedAt: nowMs, UpdatedAt: nowMs, LastActiveAt: nowMs}
	_ = d.fillPosts(uid, []*FeedPost{post})
	return post, nil
}

func (d *db) fillPosts(loginUID string, posts []*FeedPost) error {
	if len(posts) == 0 {
		return nil
	}
	feedIDs := make([]string, 0, len(posts))
	uids := make([]string, 0, len(posts))
	postMap := map[string]*FeedPost{}
	for _, p := range posts {
		if p == nil {
			continue
		}
		feedIDs = append(feedIDs, p.FeedID)
		uids = append(uids, p.UID)
		postMap[p.FeedID] = p
	}
	var media []*FeedMedia
	_, err := d.session.Select("id", "feed_id", "type", "thumb_url", "display_url", "origin_url", "cover_url", "play_url_480p", "play_url_540p", "play_url_720p", "width", "height", "duration_ms", "size", "sort").From("feed_media").Where("feed_id in ?", feedIDs).OrderBy("sort ASC").Load(&media)
	if err != nil {
		return err
	}
	for _, m := range media {
		if p := postMap[m.FeedID]; p != nil {
			p.Media = append(p.Media, m)
		}
	}
	users, err := d.users(loginUID, uids)
	if err != nil {
		return err
	}
	for _, p := range posts {
		if p == nil {
			continue
		}
		p.User = users[p.UID]
	}
	return nil
}

func (d *db) users(loginUID string, uids []string) (map[string]*FeedUser, error) {
	out := map[string]*FeedUser{}
	if len(uids) == 0 {
		return out, nil
	}
	var users []*FeedUser
	_, err := d.session.SelectBySql(`SELECT u.uid AS user_uid,u.name AS user_name,u.username,u.country_code,u.country,u.birthday,u.native_languages,u.learning_languages,u.vercode,
        IFNULL(fr.follow,0) AS follow
        FROM user u
        LEFT JOIN (SELECT to_uid,1 AS follow FROM friend WHERE uid=? AND is_deleted=0) fr ON fr.to_uid=u.uid
        WHERE u.uid in ?`, loginUID, uids).Load(&users)
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		u.Normalize()
		out[u.UID] = u
	}
	return out, nil
}

func (d *db) toggleLike(uid, feedID string) (int, int, error) {
	var exists int
	_, err := d.session.Select("count(*)").From("feed_likes").Where("feed_id=? and uid=?", feedID, uid).Load(&exists)
	if err != nil {
		return 0, 0, err
	}
	tx, err := d.session.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.RollbackUnlessCommitted()
	liked := 1
	if exists > 0 {
		liked = 0
		if _, err = tx.DeleteFrom("feed_likes").Where("feed_id=? and uid=?", feedID, uid).Exec(); err != nil {
			return 0, 0, err
		}
		_, err = tx.Update("feed_posts").Set("like_count", dbr.Expr("GREATEST(like_count-1,0)")).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
	} else {
		if _, err = tx.InsertBySql("INSERT IGNORE INTO feed_likes(feed_id,uid,created_at) VALUES(?,?,NOW())", feedID, uid).Exec(); err != nil {
			return 0, 0, err
		}
		_, err = tx.Update("feed_posts").Set("like_count", dbr.Expr("like_count+1")).Set("last_active_at", time.Now().UnixMilli()).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
	}
	if err != nil {
		return 0, 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, 0, err
	}
	var count int
	_, _ = d.session.Select("like_count").From("feed_posts").Where("feed_id=?", feedID).Load(&count)
	return liked, count, nil
}

func (d *db) addComment(uid, feedID string, req CommentReq) (*FeedComment, error) {
	content := strings.TrimSpace(req.Content)
	if content == "" {
		return nil, nil
	}
	commentID := fmt.Sprintf("cmt_%d", time.Now().UnixNano())
	nowMs := time.Now().UnixMilli()
	tx, err := d.session.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertBySql(`INSERT INTO feed_comments(comment_id,feed_id,uid,content,reply_to_comment_id,status,created_at,updated_at)
        VALUES(?,?,?,?,?,1,NOW(),NOW())`, commentID, feedID, uid, content, req.ReplyToCommentID).Exec()
	if err != nil {
		return nil, err
	}
	_, err = tx.Update("feed_posts").Set("comment_count", dbr.Expr("comment_count+1")).Set("last_active_at", nowMs).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	comment := &FeedComment{CommentID: commentID, FeedID: feedID, UID: uid, Content: content, ReplyToCommentID: req.ReplyToCommentID, CreatedAt: nowMs}
	users, _ := d.users(uid, []string{uid})
	comment.User = users[uid]
	return comment, nil
}

func (d *db) comments(loginUID, feedID string, page, limit int, cursor string) ([]*FeedComment, int, error) {
	limit = clampLimit(limit)
	offset := offsetFrom(page, cursor, limit)
	var list []*FeedComment
	_, err := d.session.SelectBySql(`SELECT comment_id,feed_id,uid,content,reply_to_comment_id,UNIX_TIMESTAMP(created_at)*1000 AS created_at_ms
        FROM feed_comments WHERE feed_id=? AND status=1 ORDER BY created_at ASC LIMIT ? OFFSET ?`, feedID, limit+1, offset).Load(&list)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if len(list) > limit {
		hasMore = 1
		list = list[:limit]
	}
	uids := make([]string, 0, len(list))
	for _, c := range list {
		if c != nil {
			uids = append(uids, c.UID)
		}
	}
	users, err := d.users(loginUID, uids)
	if err != nil {
		return nil, 0, err
	}
	for _, c := range list {
		if c != nil {
			c.User = users[c.UID]
		}
	}
	return list, hasMore, nil
}

func (d *db) recordExposure(uid string, posts []*FeedPost) {
	if uid == "" || len(posts) == 0 {
		return
	}
	tx, err := d.session.Begin()
	if err != nil {
		return
	}
	defer tx.RollbackUnlessCommitted()
	now := time.Now().UnixMilli()
	for _, p := range posts {
		if p == nil || p.FeedID == "" {
			continue
		}
		_, _ = tx.InsertBySql(`INSERT INTO feed_exposures(uid,feed_id,seen_count,last_seen_at,created_at,updated_at)
            VALUES(?,?,1,?,NOW(),NOW())
            ON DUPLICATE KEY UPDATE seen_count=seen_count+1,last_seen_at=VALUES(last_seen_at),updated_at=NOW()`, uid, p.FeedID, now).Exec()
	}
	_ = tx.Commit()
}
