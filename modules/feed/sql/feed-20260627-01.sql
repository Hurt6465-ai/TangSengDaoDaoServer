-- +migrate Up

CREATE TABLE IF NOT EXISTS feed_posts (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '动态ID',
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '发布者UID',
  text VARCHAR(1000) NOT NULL DEFAULT '' COMMENT '正文',
  title VARCHAR(200) NOT NULL DEFAULT '' COMMENT '标题',
  status SMALLINT NOT NULL DEFAULT 1 COMMENT '1正常 0删除 2审核中 3拒绝',
  visibility VARCHAR(20) NOT NULL DEFAULT 'public' COMMENT 'public/friends/private',
  like_count INT NOT NULL DEFAULT 0,
  comment_count INT NOT NULL DEFAULT 0,
  share_count INT NOT NULL DEFAULT 0,
  last_active_at BIGINT NOT NULL DEFAULT 0 COMMENT '最后活跃毫秒',
  score DOUBLE NOT NULL DEFAULT 0 COMMENT '推荐分',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_feed_posts_feed_id(feed_id),
  KEY idx_feed_posts_recommend(status,visibility,score,last_active_at),
  KEY idx_feed_posts_uid(uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现动态';

CREATE TABLE IF NOT EXISTS feed_media (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '' COMMENT '动态ID',
  type VARCHAR(20) NOT NULL DEFAULT 'image' COMMENT 'image/video',
  thumb_url VARCHAR(500) NOT NULL DEFAULT '',
  display_url VARCHAR(500) NOT NULL DEFAULT '',
  origin_url VARCHAR(500) NOT NULL DEFAULT '',
  cover_url VARCHAR(500) NOT NULL DEFAULT '',
  play_url_480p VARCHAR(500) NOT NULL DEFAULT '',
  play_url_540p VARCHAR(500) NOT NULL DEFAULT '',
  play_url_720p VARCHAR(500) NOT NULL DEFAULT '',
  width INT NOT NULL DEFAULT 0,
  height INT NOT NULL DEFAULT 0,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  size BIGINT NOT NULL DEFAULT 0,
  sort INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_feed_media_feed(feed_id,sort)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现媒体';

CREATE TABLE IF NOT EXISTS feed_likes (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  uid VARCHAR(40) NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_feed_like(feed_id,uid),
  KEY idx_feed_like_uid(uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现点赞';

CREATE TABLE IF NOT EXISTS feed_comments (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  comment_id VARCHAR(64) NOT NULL DEFAULT '',
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  uid VARCHAR(40) NOT NULL DEFAULT '',
  content TEXT NOT NULL COMMENT '评论内容，支持 voice: 音频协议',
  reply_to_comment_id VARCHAR(64) NOT NULL DEFAULT '',
  status SMALLINT NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_feed_comment(comment_id),
  KEY idx_feed_comment_feed(feed_id,status,created_at),
  KEY idx_feed_comment_uid(uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现评论';

CREATE TABLE IF NOT EXISTS feed_exposures (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '',
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  seen_count INT NOT NULL DEFAULT 0,
  last_seen_at BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_feed_exposure(uid,feed_id),
  KEY idx_feed_exposure_uid(uid,last_seen_at),
  KEY idx_feed_exposure_feed(feed_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现曝光';

CREATE TABLE IF NOT EXISTS feed_reports (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  uid VARCHAR(40) NOT NULL DEFAULT '',
  reason VARCHAR(200) NOT NULL DEFAULT '',
  status SMALLINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_feed_report_feed(feed_id,status),
  KEY idx_feed_report_uid(uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现举报';
