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
	poolLimit := limit*6 + 8
	if poolLimit < 40 {
		poolLimit = 40
	}
	if poolLimit > 180 {
		poolLimit = 180
	}

	var raw []*FeedPost
	_, err := d.session.SelectBySql(`SELECT p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        LEFT JOIN feed_exposures ex ON ex.feed_id=p.feed_id AND ex.uid=?
        LEFT JOIN feed_reports self_report ON self_report.feed_id=p.feed_id AND self_report.uid=?
        LEFT JOIN feed_follows ff ON ff.following_uid=p.uid AND ff.follower_uid=?
        LEFT JOIN feed_recommend_stats rs ON rs.feed_id=p.feed_id
        LEFT JOIN user author ON author.uid=p.uid
        LEFT JOIN user viewer ON viewer.uid=?
        WHERE p.status=1
          AND p.visibility='public'
          AND p.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
          AND (?='' OR p.uid<>?)
          AND self_report.id IS NULL
          AND IFNULL(ex.seen_count,0) < 3
          AND IFNULL(rs.report_count,0) < 5
        ORDER BY (
            p.score
            + CASE
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 6 HOUR) THEN 10
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 7
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 72 HOUR) THEN 4
                ELSE 1
              END
            + CASE
                WHEN viewer.sex=1 AND author.sex=0 THEN 8
                WHEN viewer.sex=0 AND author.sex=1 THEN 8
                WHEN viewer.sex=author.sex THEN -2
                ELSE 0
              END
            + IF(ff.following_uid IS NULL,0,6)
            + (p.like_count / GREATEST(IFNULL(rs.exposure_count,0),10)) * 30
            + (p.comment_count / GREATEST(IFNULL(rs.exposure_count,0),10)) * 45
            + (p.share_count / GREATEST(IFNULL(rs.exposure_count,0),10)) * 55
            + IFNULL(rs.avg_percent,0) / 12
            + (IFNULL(rs.complete_count,0) / GREATEST(IFNULL(rs.watch_count,0),5)) * 18
            - IFNULL(ex.seen_count,0) * 3
            - CASE WHEN IFNULL(ex.seen_count,0) >= 2 AND l.uid IS NULL THEN 6 ELSE 0 END
            - IFNULL(rs.report_count,0) * 10
            - IFNULL(rs.skip_count,0) * 2
            - IFNULL(rs.dislike_count,0) * 6
            - CASE WHEN IFNULL(rs.exposure_count,0) >= 10 AND (p.like_count+p.comment_count+p.share_count)=0 THEN LEAST(12,IFNULL(rs.exposure_count,0)*0.4) ELSE 0 END
        ) DESC, p.last_active_at DESC, p.created_at DESC
        LIMIT ? OFFSET ?`, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, poolLimit+1, offset).Load(&raw)
	if err != nil {
		// 兼容旧库：如果新推荐统计表/关注表迁移暂未执行，严格推荐 SQL 会失败。
		// 用基础表兜底，避免前端直接显示“暂无作品”。
		if offset == 0 {
			if posts, hasMore, fbErr := d.listRecommendFallback(loginUID, limit); fbErr == nil {
				return posts, hasMore, nil
			}
		}
		return nil, 0, err
	}
	if len(raw) == 0 && offset == 0 {
		// 推荐池过滤比较严格：最近 7 天、排除自己、已曝光过多降权等。
		// 新站、测试服或内容少的时候，严格池可能为空，前端就会显示“暂无作品”。
		// 这里做一个兜底召回：放宽时间和曝光限制，并允许召回自己的公开作品，保证有内容时不会空屏。
		return d.listRecommendFallback(loginUID, limit)
	}

	hasMore := 0
	if len(raw) > poolLimit {
		hasMore = 1
		raw = raw[:poolLimit]
	}
	posts := d.limitOneBySameAuthor(raw, limit)
	if len(raw) > limit || hasMore == 1 {
		hasMore = 1
	}
	if err := d.fillPosts(loginUID, posts); err != nil {
		return nil, 0, err
	}
	return posts, hasMore, nil
}

func (d *db) listRecommendFallback(loginUID string, limit int) ([]*FeedPost, int, error) {
	limit = clampLimit(limit)
	var raw []*FeedPost
	_, err := d.session.SelectBySql(`SELECT p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        LEFT JOIN feed_reports self_report ON self_report.feed_id=p.feed_id AND self_report.uid=?
        WHERE p.status=1
          AND p.visibility='public'
          AND self_report.id IS NULL
        ORDER BY p.created_at DESC, p.last_active_at DESC, p.score DESC
        LIMIT ?`, loginUID, loginUID, limit+1).Load(&raw)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if len(raw) > limit {
		hasMore = 1
		raw = raw[:limit]
	}
	posts := d.limitOneBySameAuthor(raw, limit)
	if err := d.fillPosts(loginUID, posts); err != nil {
		return nil, 0, err
	}
	return posts, hasMore, nil
}

func (d *db) limitOneBySameAuthor(raw []*FeedPost, limit int) []*FeedPost {
	if len(raw) == 0 || limit <= 0 {
		return []*FeedPost{}
	}
	out := make([]*FeedPost, 0, limit)
	delayed := make([]*FeedPost, 0)
	for _, p := range raw {
		if p == nil {
			continue
		}
		if len(out) == 0 || out[len(out)-1].UID != p.UID {
			out = append(out, p)
			if len(out) >= limit {
				return out
			}
		} else {
			delayed = append(delayed, p)
		}
	}
	for len(out) < limit && len(delayed) > 0 {
		used := -1
		lastUID := ""
		if len(out) > 0 {
			lastUID = out[len(out)-1].UID
		}
		for i, p := range delayed {
			if p != nil && p.UID != lastUID {
				used = i
				break
			}
		}
		if used < 0 {
			// 极端情况下召回池只有同一个作者，只能补足结果，否则列表会空。
			used = 0
		}
		out = append(out, delayed[used])
		delayed = append(delayed[:used], delayed[used+1:]...)
	}
	return out
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

func (d *db) listFollowing(loginUID string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	limit = clampLimit(limit)
	offset := offsetFrom(page, cursor, limit)
	if strings.TrimSpace(loginUID) == "" {
		return []*FeedPost{}, 0, nil
	}
	var posts []*FeedPost
	_, err := d.session.SelectBySql(`SELECT p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p
        INNER JOIN feed_follows ff ON ff.following_uid=p.uid AND ff.follower_uid=?
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        WHERE p.status=1 AND p.visibility='public'
        ORDER BY p.last_active_at DESC,p.created_at DESC
        LIMIT ? OFFSET ?`, loginUID, loginUID, limit+1, offset).Load(&posts)
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
	if len([]rune(text)) > 280 {
		text = string([]rune(text)[:280])
	}
	if len(req.Media) == 0 {
		return nil, fmt.Errorf("请选择图片或视频")
	}
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
        VALUES(?,?,?,?,1,?,?,1,NOW(),NOW())`, feedID, uid, text, title, visibility, nowMs).Exec()
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
		if m.Type != "image" && m.Type != "video" {
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
	post := &FeedPost{FeedID: feedID, UID: uid, Text: text, Title: title, Status: 1, Visibility: visibility, CreatedAt: nowMs, UpdatedAt: nowMs, LastActiveAt: nowMs, Score: 1}
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
	uids = compactStrings(uids)
	if len(uids) == 0 {
		return out, nil
	}
	var users []*FeedUser
	_, err := d.session.SelectBySql(`SELECT u.uid AS user_uid,u.name AS user_name,u.username,u.country_code,u.country,u.sex,u.birthday,u.native_languages,u.learning_languages,u.vercode,
        IFNULL(ff.following_uid<>'',0) AS follow
        FROM user u
        LEFT JOIN feed_follows ff ON ff.following_uid=u.uid AND ff.follower_uid=?
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

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
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
		res, err := tx.DeleteFrom("feed_likes").Where("feed_id=? and uid=?", feedID, uid).Exec()
		if err != nil {
			return 0, 0, err
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			_, err = tx.Update("feed_posts").Set("like_count", dbr.Expr("GREATEST(like_count-1,0)")).Set("score", dbr.Expr("GREATEST(score-2,0)")).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
		}
	} else {
		res, err := tx.InsertBySql("INSERT IGNORE INTO feed_likes(feed_id,uid,created_at) VALUES(?,?,NOW())", feedID, uid).Exec()
		if err != nil {
			return 0, 0, err
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			_, err = tx.Update("feed_posts").Set("like_count", dbr.Expr("like_count+1")).Set("score", dbr.Expr("score+2")).Set("last_active_at", time.Now().UnixMilli()).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
		} else {
			liked = 1
		}
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
	maxLen := 500
	if strings.HasPrefix(content, "voice:") || strings.HasPrefix(content, "voice_local:") {
		// 语音评论会把 音频地址|时长|waveformBase64 放在 content 内，500 字容易截断导致无法播放。
		maxLen = 4096
	}
	if len([]rune(content)) > maxLen {
		content = string([]rune(content)[:maxLen])
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
	_, err = tx.Update("feed_posts").Set("comment_count", dbr.Expr("comment_count+1")).Set("score", dbr.Expr("score+4")).Set("last_active_at", nowMs).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=?", feedID).Exec()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	comment := &FeedComment{CommentID: commentID, FeedID: feedID, UID: uid, Content: content, ReplyToCommentID: req.ReplyToCommentID, CreatedAt: nowMs}
	users, _ := d.users(uid, []string{uid})
	comment.FillUser(users[uid])
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
			c.FillUser(users[c.UID])
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

func (d *db) follow(uid, targetUID string) error {
	_, err := d.session.InsertBySql(`INSERT IGNORE INTO feed_follows(follower_uid,following_uid,created_at,updated_at) VALUES(?,?,NOW(),NOW())`, uid, targetUID).Exec()
	return err
}

func (d *db) unfollow(uid, targetUID string) error {
	_, err := d.session.DeleteFrom("feed_follows").Where("follower_uid=? AND following_uid=?", uid, targetUID).Exec()
	return err
}

func (d *db) share(uid, feedID string) (int, error) {
	var exists int
	_, err := d.session.Select("count(*)").From("feed_posts").Where("feed_id=? AND status=1", feedID).Load(&exists)
	if err != nil {
		return 0, err
	}
	if exists == 0 {
		return 0, fmt.Errorf("动态不存在")
	}
	var userShared int
	_, err = d.session.Select("count(*)").From("feed_shares").Where("feed_id=? AND uid=?", feedID, uid).Load(&userShared)
	if err != nil {
		return 0, err
	}
	if userShared > 0 {
		var count int
		_, _ = d.session.Select("share_count").From("feed_posts").Where("feed_id=?", feedID).Load(&count)
		return count, nil
	}
	nowMs := time.Now().UnixMilli()
	tx, err := d.session.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.RollbackUnlessCommitted()
	res, err := tx.InsertBySql(`INSERT IGNORE INTO feed_shares(feed_id,uid,created_at) VALUES(?,?,NOW())`, feedID, uid).Exec()
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	if affected > 0 {
		_, err = tx.Update("feed_posts").Set("share_count", dbr.Expr("share_count+1")).Set("last_active_at", nowMs).Set("score", dbr.Expr("score+3")).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=? AND status=1", feedID).Exec()
		if err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	var count int
	_, _ = d.session.Select("share_count").From("feed_posts").Where("feed_id=?", feedID).Load(&count)
	return count, nil
}

func (d *db) report(uid, feedID string, req ReportReq) error {
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "other"
	}
	var reported int
	_, err := d.session.Select("count(*)").From("feed_reports").Where("feed_id=? AND uid=?", feedID, uid).Load(&reported)
	if err != nil {
		return err
	}
	if reported > 0 {
		return nil
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	res, err := tx.InsertBySql(`INSERT IGNORE INTO feed_reports(feed_id,uid,reason,status,created_at,updated_at) VALUES(?,?,?,0,NOW(),NOW())`, feedID, uid, reason).Exec()
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return tx.Commit()
	}
	if _, err = tx.Update("feed_posts").Set("score", dbr.Expr("GREATEST(score-8,0)")).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=? AND status=1", feedID).Exec(); err != nil {
		return err
	}
	var reportCount int
	_, _ = tx.Select("count(*)").From("feed_reports").Where("feed_id=? AND status=0", feedID).Load(&reportCount)
	if reportCount >= 10 {
		_, _ = tx.Update("feed_posts").Set("status", 3).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=? AND status=1", feedID).Exec()
	}
	return tx.Commit()
}

func (d *db) event(uid, feedID string, req EventReq) error {
	eventType := req.NormalizedEventType()
	if d.isRecentDuplicateEvent(uid, feedID, eventType) {
		return nil
	}
	if len([]rune(req.Extra)) > 1000 {
		req.Extra = string([]rune(req.Extra)[:1000])
	}
	_, err := d.session.InsertBySql(`INSERT INTO feed_events(feed_id,uid,event_type,watch_ms,duration_ms,percent,media_type,extra,created_at)
        VALUES(?,?,?,?,?,?,?,?,NOW())`, feedID, uid, eventType, req.WatchMS, req.DurationMS, req.Percent, strings.TrimSpace(req.MediaType), strings.TrimSpace(req.Extra)).Exec()
	if err != nil {
		return err
	}
	delta := d.eventScoreDelta(req)
	if delta != 0 {
		_, _ = d.session.Update("feed_posts").Set("score", dbr.Expr("GREATEST(score+?,0)", delta)).Set("last_active_at", time.Now().UnixMilli()).Set("updated_at", dbr.Expr("NOW()")).Where("feed_id=? AND status=1", feedID).Exec()
	}
	return nil
}

func (d *db) isRecentDuplicateEvent(uid, feedID, eventType string) bool {
	if uid == "" || feedID == "" {
		return true
	}
	window := eventDedupeWindow(eventType)
	if window <= 0 {
		return false
	}
	var recent int
	_, err := d.session.SelectBySql(`SELECT COUNT(*) FROM feed_events WHERE uid=? AND feed_id=? AND event_type=? AND created_at>=?`, uid, feedID, eventType, time.Now().Add(-window)).Load(&recent)
	if err != nil {
		return false
	}
	return recent > 0
}

func eventDedupeWindow(eventType string) time.Duration {
	switch eventType {
	case "watch":
		return 30 * time.Second
	case "complete":
		return 10 * time.Minute
	case "skip":
		return 45 * time.Second
	case "dislike":
		return 6 * time.Hour
	case "expose":
		return 60 * time.Second
	default:
		return 60 * time.Second
	}
}

func (d *db) shouldScoreEvent(uid, feedID, eventType string) bool {
	if uid == "" || feedID == "" {
		return false
	}
	var recent int
	_, err := d.session.SelectBySql(`SELECT COUNT(*) FROM feed_events WHERE uid=? AND feed_id=? AND event_type=? AND created_at>=DATE_SUB(NOW(), INTERVAL 60 SECOND)`, uid, feedID, eventType).Load(&recent)
	if err != nil {
		return true
	}
	// 当前事件已经插入，60 秒内只有 1 条代表不是刷。
	return recent <= 1
}

func (d *db) eventScoreDelta(req EventReq) float64 {
	eventType := req.NormalizedEventType()
	switch eventType {
	case "complete":
		return 1.2
	case "watch":
		if req.Percent >= 80 || req.WatchMS >= 5000 {
			return 0.8
		}
		if req.Percent <= 10 && req.WatchMS <= 1500 {
			return -0.5
		}
	case "skip":
		return -1
	case "dislike":
		return -3
	}
	return 0
}

func (d *db) deletePost(uid, feedID string) ([]string, error) {
	var owner string
	_, err := d.session.Select("uid").From("feed_posts").Where("feed_id=? AND status<>0", feedID).Load(&owner)
	if err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, fmt.Errorf("动态不存在")
	}
	if owner != uid {
		return nil, fmt.Errorf("只能删除自己的作品")
	}
	return d.hardDeletePost(feedID)
}

func (d *db) hardDeletePost(feedID string) ([]string, error) {
	var media []*FeedMedia
	_, err := d.session.Select("id", "feed_id", "type", "thumb_url", "display_url", "origin_url", "cover_url", "play_url_480p", "play_url_540p", "play_url_720p").From("feed_media").Where("feed_id=?", feedID).Load(&media)
	if err != nil {
		return nil, err
	}
	paths := mediaPaths(media)
	tx, err := d.session.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()
	if _, err = tx.DeleteFrom("feed_likes").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_comments").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_exposures").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_reports").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_shares").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_events").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_recommend_stats").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_media").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if _, err = tx.DeleteFrom("feed_posts").Where("feed_id=?", feedID).Exec(); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return paths, nil
}

func mediaPaths(media []*FeedMedia) []string {
	seen := map[string]struct{}{}
	paths := make([]string, 0)
	add := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" {
			return
		}
		if _, ok := seen[v]; ok {
			return
		}
		seen[v] = struct{}{}
		paths = append(paths, v)
	}
	for _, m := range media {
		if m == nil {
			continue
		}
		add(m.ThumbURL)
		add(m.DisplayURL)
		add(m.OriginURL)
		add(m.CoverURL)
		add(m.PlayURL480P)
		add(m.PlayURL540P)
		add(m.PlayURL720P)
	}
	return paths
}

type expiredFeedItem struct {
	FeedID string `db:"feed_id"`
}

func (d *db) expiredVideoPosts(cutoffMs int64, limit int) ([]*expiredFeedItem, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var list []*expiredFeedItem
	_, err := d.session.SelectBySql(`SELECT DISTINCT p.feed_id
        FROM feed_posts p
        INNER JOIN feed_media m ON m.feed_id=p.feed_id AND m.type='video'
        WHERE p.status=1 AND UNIX_TIMESTAMP(p.created_at)*1000 < ?
        ORDER BY p.created_at ASC LIMIT ?`, cutoffMs, limit).Load(&list)
	return list, err
}

func (d *db) deleteOldEvents(cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	res, err := d.session.UpdateBySql(`DELETE FROM feed_events WHERE created_at<? LIMIT ?`, cutoff, limit).Exec()
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return affected, nil
}

func (d *db) rebuildRecommendStats() error {
	_, err := d.session.InsertBySql(`REPLACE INTO feed_recommend_stats(
        feed_id,exposure_count,exposed_users,like_count,comment_count,share_count,report_count,watch_count,complete_count,skip_count,dislike_count,avg_watch_ms,avg_percent,updated_at
    )
    SELECT p.feed_id,
           IFNULL(ex.exposure_count,0),IFNULL(ex.exposed_users,0),
           p.like_count,p.comment_count,p.share_count,
           IFNULL(r.report_count,0),
           IFNULL(ev.watch_count,0),IFNULL(ev.complete_count,0),IFNULL(ev.skip_count,0),IFNULL(ev.dislike_count,0),
           IFNULL(ev.avg_watch_ms,0),IFNULL(ev.avg_percent,0),NOW()
    FROM feed_posts p
    LEFT JOIN (SELECT feed_id,SUM(seen_count) exposure_count,COUNT(*) exposed_users FROM feed_exposures GROUP BY feed_id) ex ON ex.feed_id=p.feed_id
    LEFT JOIN (SELECT feed_id,COUNT(*) report_count FROM feed_reports WHERE status=0 GROUP BY feed_id) r ON r.feed_id=p.feed_id
    LEFT JOIN (
        SELECT feed_id,
               SUM(event_type='watch') watch_count,
               SUM(event_type='complete') complete_count,
               SUM(event_type='skip') skip_count,
               SUM(event_type='dislike') dislike_count,
               AVG(CASE WHEN watch_ms>0 THEN watch_ms ELSE NULL END) avg_watch_ms,
               AVG(CASE WHEN percent>0 THEN percent ELSE NULL END) avg_percent
        FROM feed_events GROUP BY feed_id
    ) ev ON ev.feed_id=p.feed_id
    WHERE p.status<>0`).Exec()
	if err != nil {
		return err
	}
	_, err = d.session.UpdateBySql(`UPDATE feed_posts p
        LEFT JOIN feed_recommend_stats rs ON rs.feed_id=p.feed_id
        SET p.score=GREATEST(0,
            1
            + CASE
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 6 HOUR) THEN 10
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 7
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 72 HOUR) THEN 4
                WHEN p.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1
                ELSE 0
              END
            + p.like_count*0.6
            + p.comment_count*1.5
            + p.share_count*2.0
            + IFNULL(rs.avg_percent,0)/12
            + IFNULL(rs.complete_count,0)*1.2
            - IFNULL(rs.report_count,0)*10
            - IFNULL(rs.skip_count,0)*1.5
            - IFNULL(rs.dislike_count,0)*6
            - CASE WHEN IFNULL(rs.exposure_count,0)>=10 AND (p.like_count+p.comment_count+p.share_count)=0 THEN LEAST(12,IFNULL(rs.exposure_count,0)*0.4) ELSE 0 END
        )
        WHERE p.status=1`).Exec()
	return err
}
