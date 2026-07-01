-- +migrate Up

-- Make feed share/report idempotent under concurrent requests.
DELETE s1 FROM feed_shares s1
INNER JOIN feed_shares s2
  ON s1.feed_id=s2.feed_id AND s1.uid=s2.uid AND s1.id>s2.id;

DELETE r1 FROM feed_reports r1
INNER JOIN feed_reports r2
  ON r1.feed_id=r2.feed_id AND r1.uid=r2.uid AND r1.id>r2.id;

SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_shares' AND INDEX_NAME='uk_feed_share'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_shares ADD UNIQUE KEY uk_feed_share(feed_id,uid)',
  'SELECT 1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_reports' AND INDEX_NAME='uk_feed_report'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_reports ADD UNIQUE KEY uk_feed_report(feed_id,uid)',
  'SELECT 1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_posts' AND INDEX_NAME='idx_feed_posts_cursor'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_posts ADD KEY idx_feed_posts_cursor(status,visibility,last_active_at,created_at,feed_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_posts' AND INDEX_NAME='idx_feed_posts_user_cursor'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_posts ADD KEY idx_feed_posts_user_cursor(uid,status,visibility,created_at,feed_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
