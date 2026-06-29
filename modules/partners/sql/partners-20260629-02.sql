-- +migrate Up

-- v25: 补充打招呼频控索引。字段 text/source 已由 v24 迁移补齐；这里继续保持幂等。
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND INDEX_NAME = 'idx_partner_greeting_from_time');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD INDEX idx_partner_greeting_from_time(uid,last_greet_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposures' AND INDEX_NAME = 'idx_partner_exposure_uid_seen');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposures ADD INDEX idx_partner_exposure_uid_seen(uid,last_seen_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
