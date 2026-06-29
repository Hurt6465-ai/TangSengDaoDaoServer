-- +migrate Up

-- v26: 方案 B：非好友语伴招呼临时会话。
-- pending/active 的 partner_contacts 会并入个人频道白名单，让对方能在会话列表看到真实 IM 招呼。
CREATE TABLE IF NOT EXISTS partner_contacts (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '当前用户UID',
  to_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '对方UID',
  requester_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '最初打招呼发起人UID',
  status TINYINT NOT NULL DEFAULT 0 COMMENT '0 pending,1 active,2 ignored,3 blocked',
  last_msg_at BIGINT NOT NULL DEFAULT 0 COMMENT '最后消息时间毫秒',
  created_at BIGINT NOT NULL DEFAULT 0 COMMENT '创建时间毫秒',
  updated_at BIGINT NOT NULL DEFAULT 0 COMMENT '更新时间毫秒',
  UNIQUE KEY uk_partner_contact_pair(uid,to_uid),
  KEY idx_partner_contact_uid_status(uid,status,last_msg_at),
  KEY idx_partner_contact_to_status(to_uid,status,last_msg_at),
  KEY idx_partner_contact_requester(requester_uid,status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴临时会话关系';

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND INDEX_NAME = 'uk_partner_contact_pair');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD UNIQUE KEY uk_partner_contact_pair(uid,to_uid)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND INDEX_NAME = 'idx_partner_contact_uid_status');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD INDEX idx_partner_contact_uid_status(uid,status,last_msg_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND INDEX_NAME = 'idx_partner_contact_to_status');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD INDEX idx_partner_contact_to_status(to_uid,status,last_msg_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_contacts' AND INDEX_NAME = 'idx_partner_contact_requester');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_contacts ADD INDEX idx_partner_contact_requester(requester_uid,status)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
