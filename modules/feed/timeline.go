package feed

import (
	"fmt"
	"strings"
)

func (d *db) listTimeline(loginUID string, following bool, limit int, cursorValue string) ([]*FeedPost, string, int, error) {
	limit = clampLimit(limit)
	if limit > 20 {
		limit = 20
	}
	cursor, err := decodeTimelineCursor(cursorValue)
	if err != nil {
		return nil, "", 0, fmt.Errorf("分页游标无效")
	}

	joinFollowing := ""
	args := make([]interface{}, 0, 12)
	if following {
		if strings.TrimSpace(loginUID) == "" {
			return []*FeedPost{}, "", 0, nil
		}
		joinFollowing = " INNER JOIN feed_follows tf ON tf.following_uid=p.uid AND tf.follower_uid=? "
		args = append(args, loginUID)
	}

	sql := `SELECT p.id,p.feed_id,p.uid,p.text,p.title,p.status,p.visibility,p.like_count,p.comment_count,p.share_count,p.score,
        IFNULL(l.uid<>'',0) AS liked,
        UNIX_TIMESTAMP(p.created_at)*1000 AS created_at_ms,
        UNIX_TIMESTAMP(p.updated_at)*1000 AS updated_at_ms,
        IFNULL(p.last_active_at,UNIX_TIMESTAMP(p.updated_at)*1000) AS last_active_at
        FROM feed_posts p ` + joinFollowing + `
        LEFT JOIN feed_likes l ON l.feed_id=p.feed_id AND l.uid=?
        WHERE p.status=1 AND p.visibility='public'
          AND EXISTS (
              SELECT 1 FROM user active_author
              WHERE active_author.uid=p.uid AND active_author.is_destroy=0
          )
          AND NOT EXISTS (
              SELECT 1 FROM feed_reports self_report
              WHERE self_report.feed_id=p.feed_id AND self_report.uid=?
          )
          AND NOT EXISTS (
              SELECT 1 FROM user_setting us_blocked_by_me
              WHERE us_blocked_by_me.uid=? AND us_blocked_by_me.to_uid=p.uid AND us_blocked_by_me.blacklist=1
          )
          AND NOT EXISTS (
              SELECT 1 FROM user_setting us_blocked_me
              WHERE us_blocked_me.uid=p.uid AND us_blocked_me.to_uid=? AND us_blocked_me.blacklist=1
          )
          AND EXISTS (
              SELECT 1 FROM feed_media fm
              WHERE fm.feed_id=p.feed_id AND fm.type IN ('image','tiktok')
          )
          AND NOT EXISTS (
              SELECT 1 FROM feed_media fm_video
              WHERE fm_video.feed_id=p.feed_id AND fm_video.type='video'
          )`
	args = append(args, loginUID, loginUID, loginUID, loginUID)
	if cursor.ID > 0 {
		sql += ` AND (p.created_at < FROM_UNIXTIME(?) OR (p.created_at=FROM_UNIXTIME(?) AND p.id<?))`
		seconds := cursor.CreatedAt / 1000
		args = append(args, seconds, seconds, cursor.ID)
	}
	sql += ` ORDER BY p.created_at DESC,p.id DESC LIMIT ?`
	args = append(args, limit+1)

	var posts []*FeedPost
	if _, err = d.session.SelectBySql(sql, args...).Load(&posts); err != nil {
		return nil, "", 0, err
	}
	hasMore := 0
	if len(posts) > limit {
		hasMore = 1
		posts = posts[:limit]
	}
	if err = d.fillPosts(loginUID, posts); err != nil {
		return nil, "", 0, err
	}
	next := ""
	if hasMore == 1 && len(posts) > 0 {
		last := posts[len(posts)-1]
		next = encodeTimelineCursor(last.CreatedAt, last.ID)
	}
	return posts, next, hasMore, nil
}
