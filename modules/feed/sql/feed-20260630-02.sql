-- +migrate Up

CREATE TABLE IF NOT EXISTS feed_recommend_stats (
  feed_id VARCHAR(64) NOT NULL PRIMARY KEY,
  exposure_count INT NOT NULL DEFAULT 0 COMMENT '总曝光次数，累加 seen_count',
  exposed_users INT NOT NULL DEFAULT 0 COMMENT '曝光用户数',
  like_count INT NOT NULL DEFAULT 0,
  comment_count INT NOT NULL DEFAULT 0,
  share_count INT NOT NULL DEFAULT 0,
  report_count INT NOT NULL DEFAULT 0,
  watch_count INT NOT NULL DEFAULT 0,
  complete_count INT NOT NULL DEFAULT 0,
  skip_count INT NOT NULL DEFAULT 0,
  dislike_count INT NOT NULL DEFAULT 0,
  avg_watch_ms BIGINT NOT NULL DEFAULT 0,
  avg_percent INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_feed_rec_stats_report(report_count),
  KEY idx_feed_rec_stats_exposure(exposure_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='发现推荐离线统计';

CREATE INDEX idx_feed_posts_recent_pool ON feed_posts(status,visibility,created_at,score,last_active_at);
CREATE INDEX idx_feed_reports_user_feed ON feed_reports(uid,feed_id,status);
CREATE INDEX idx_feed_events_user_feed_type_time ON feed_events(uid,feed_id,event_type,created_at);
