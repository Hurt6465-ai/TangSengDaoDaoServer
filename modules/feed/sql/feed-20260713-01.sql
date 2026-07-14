-- +migrate Up

SET @col_exists := (SELECT COUNT(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_provider');
SET @sql := IF(@col_exists=0, 'ALTER TABLE feed_media ADD COLUMN external_provider VARCHAR(32) NOT NULL DEFAULT '''' AFTER play_url_720p', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @col_exists := (SELECT COUNT(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_id');
SET @sql := IF(@col_exists=0, 'ALTER TABLE feed_media ADD COLUMN external_id VARCHAR(80) NOT NULL DEFAULT '''' AFTER external_provider', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @col_exists := (SELECT COUNT(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_url');
SET @sql := IF(@col_exists=0, 'ALTER TABLE feed_media ADD COLUMN external_url VARCHAR(600) NOT NULL DEFAULT '''' AFTER external_id', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @col_exists := (SELECT COUNT(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_title');
SET @sql := IF(@col_exists=0, 'ALTER TABLE feed_media ADD COLUMN external_title VARCHAR(500) NOT NULL DEFAULT '''' AFTER external_url', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @col_exists := (SELECT COUNT(1) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_author');
SET @sql := IF(@col_exists=0, 'ALTER TABLE feed_media ADD COLUMN external_author VARCHAR(200) NOT NULL DEFAULT '''' AFTER external_title', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (SELECT COUNT(1) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_posts' AND INDEX_NAME='idx_feed_posts_timeline_v2');
SET @sql := IF(@idx_exists=0, 'ALTER TABLE feed_posts ADD KEY idx_feed_posts_timeline_v2(status,visibility,created_at,id)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (SELECT COUNT(1) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_posts' AND INDEX_NAME='idx_feed_posts_user_timeline_v2');
SET @sql := IF(@idx_exists=0, 'ALTER TABLE feed_posts ADD KEY idx_feed_posts_user_timeline_v2(uid,status,visibility,created_at,id)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (SELECT COUNT(1) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND INDEX_NAME='idx_feed_media_external');
SET @sql := IF(@idx_exists=0, 'ALTER TABLE feed_media ADD KEY idx_feed_media_external(external_provider,external_id)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
