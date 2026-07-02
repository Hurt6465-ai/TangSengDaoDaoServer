-- +migrate Up

-- v28: 真实曝光时长、事件流水、打招呼发送状态。
-- 这些字段全部幂等补齐，重复执行安全。
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposures' AND COLUMN_NAME = 'last_duration_ms');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposures ADD COLUMN last_duration_ms BIGINT NOT NULL DEFAULT 0 COMMENT ''最后一次有效曝光停留毫秒'' AFTER last_seen_at', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS partner_exposure_events (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '浏览者UID',
  to_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '被浏览语伴UID',
  event_type VARCHAR(24) NOT NULL DEFAULT 'expose' COMMENT 'expose/skip/profile_open/photo_swipe/hello',
  source VARCHAR(32) NOT NULL DEFAULT 'partner_browse' COMMENT '来源',
  duration_ms BIGINT NOT NULL DEFAULT 0 COMMENT '本次停留毫秒',
  photo_index INT NOT NULL DEFAULT 0 COMMENT '图片索引',
  event_at BIGINT NOT NULL DEFAULT 0 COMMENT '事件发生时间毫秒',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_partner_event_uid_time(uid,event_at),
  KEY idx_partner_event_to_time(to_uid,event_at),
  KEY idx_partner_event_type_time(event_type,event_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴曝光/跳过/停留事件流水';

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposure_events' AND INDEX_NAME = 'idx_partner_event_uid_time');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposure_events ADD INDEX idx_partner_event_uid_time(uid,event_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposure_events' AND INDEX_NAME = 'idx_partner_event_to_time');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposure_events ADD INDEX idx_partner_event_to_time(to_uid,event_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposure_events' AND INDEX_NAME = 'idx_partner_event_type_time');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposure_events ADD INDEX idx_partner_event_type_time(event_type,event_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND COLUMN_NAME = 'send_status');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD COLUMN send_status TINYINT NOT NULL DEFAULT 1 COMMENT ''0待发送 1已发送 2发送失败'' AFTER last_greet_at', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND COLUMN_NAME = 'last_send_at');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD COLUMN last_send_at BIGINT NOT NULL DEFAULT 0 COMMENT ''最后发送尝试毫秒'' AFTER send_status', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND COLUMN_NAME = 'failed_reason');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD COLUMN failed_reason VARCHAR(200) NOT NULL DEFAULT '''' COMMENT ''发送失败原因'' AFTER last_send_at', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND INDEX_NAME = 'idx_partner_greeting_send_status');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD INDEX idx_partner_greeting_send_status(uid,to_uid,send_status,last_greet_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
