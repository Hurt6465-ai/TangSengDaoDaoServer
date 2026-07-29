-- +migrate Up

-- Partner recommendation excludes reports in both directions. Keep these migrations
-- in the report module so fresh installs create the report table before ALTER TABLE.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='report'
                 AND INDEX_NAME='idx_report_uid_channel');
SET @sql := IF(@exist=0,
  'ALTER TABLE report ADD INDEX idx_report_uid_channel(uid,channel_type,channel_id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='report'
                 AND INDEX_NAME='idx_report_channel_uid');
SET @sql := IF(@exist=0,
  'ALTER TABLE report ADD INDEX idx_report_channel_uid(channel_id,channel_type,uid)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
