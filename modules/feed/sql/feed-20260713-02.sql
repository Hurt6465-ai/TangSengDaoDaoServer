-- +migrate Up
-- Compatibility patch for installations that already recorded feed-20260713-01.sql.
-- TikTok CDN cover URLs can be signed and much longer than the original 500 characters.

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='thumb_url');
SET @sql := IF(@col_len>0 AND @col_len<2048, CONCAT('ALTER TABLE feed_media MODIFY COLUMN thumb_url VARCHAR(2048) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='display_url');
SET @sql := IF(@col_len>0 AND @col_len<2048, CONCAT('ALTER TABLE feed_media MODIFY COLUMN display_url VARCHAR(2048) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='cover_url');
SET @sql := IF(@col_len>0 AND @col_len<2048, CONCAT('ALTER TABLE feed_media MODIFY COLUMN cover_url VARCHAR(2048) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_url');
SET @sql := IF(@col_len>0 AND @col_len<600, CONCAT('ALTER TABLE feed_media MODIFY COLUMN external_url VARCHAR(600) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_title');
SET @sql := IF(@col_len>0 AND @col_len<500, CONCAT('ALTER TABLE feed_media MODIFY COLUMN external_title VARCHAR(500) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @col_len := (SELECT IFNULL(MAX(CHARACTER_MAXIMUM_LENGTH),0) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND COLUMN_NAME='external_author');
SET @sql := IF(@col_len>0 AND @col_len<200, CONCAT('ALTER TABLE feed_media MODIFY COLUMN external_author VARCHAR(200) NOT NULL DEFAULT ', CHAR(39), CHAR(39)), 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
