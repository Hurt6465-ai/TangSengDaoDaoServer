-- +migrate Up

CREATE TABLE IF NOT EXISTS feed_follows (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  follower_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '关注者',
  following_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '被关注者',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_feed_follow(follower_uid,following_uid),
  KEY idx_feed_following_uid(following_uid,created_at),
  KEY idx_feed_follower_uid(follower_uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现关注关系';

CREATE TABLE IF NOT EXISTS feed_shares (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  uid VARCHAR(40) NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_feed_share_feed(feed_id,created_at),
  KEY idx_feed_share_uid(uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现分享记录';

CREATE TABLE IF NOT EXISTS feed_events (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  feed_id VARCHAR(64) NOT NULL DEFAULT '',
  uid VARCHAR(40) NOT NULL DEFAULT '',
  event_type VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'expose/watch/complete/skip/dislike等',
  watch_ms BIGINT NOT NULL DEFAULT 0,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  percent INT NOT NULL DEFAULT 0,
  media_type VARCHAR(20) NOT NULL DEFAULT '',
  extra VARCHAR(1000) NOT NULL DEFAULT '',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_feed_event_feed(feed_id,event_type,created_at),
  KEY idx_feed_event_uid(uid,event_type,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现行为事件';
