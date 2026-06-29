-- +migrate Up

-- v27: 低配服务器优化字段：定位 source、pending requester 首条限制。
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_locations' AND COLUMN_NAME = 'source');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_locations ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT ''network'' COMMENT ''定位来源 gps/network/ip'' AFTER geohash', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND COLUMN_NAME = 'requester_msg_count');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD COLUMN requester_msg_count INT NOT NULL DEFAULT 1 COMMENT ''pending下发起人已发送消息数'' AFTER status', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND INDEX_NAME = 'idx_partner_contact_pending_guard');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD INDEX idx_partner_contact_pending_guard(uid,to_uid,status,requester_uid)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_exposures' AND INDEX_NAME = 'idx_partner_exposure_uid_to_seen');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_exposures ADD INDEX idx_partner_exposure_uid_to_seen(uid,to_uid,last_seen_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
