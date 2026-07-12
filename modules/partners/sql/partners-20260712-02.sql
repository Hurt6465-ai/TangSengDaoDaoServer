-- +migrate Up

-- Independent eligibility flags. `status` remains a compatibility aggregate and must
-- never overwrite partner_enabled or review_status during profile synchronization.
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND COLUMN_NAME='account_eligible');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD COLUMN account_eligible TINYINT NOT NULL DEFAULT 1 COMMENT ''账号是否允许推荐'' AFTER status','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND COLUMN_NAME='partner_enabled');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD COLUMN partner_enabled TINYINT NOT NULL DEFAULT 1 COMMENT ''用户是否开启语伴'' AFTER account_eligible','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND COLUMN_NAME='profile_completed');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD COLUMN profile_completed TINYINT NOT NULL DEFAULT 0 COMMENT ''语伴必填资料是否完成'' AFTER partner_enabled','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND COLUMN_NAME='review_status');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD COLUMN review_status TINYINT NOT NULL DEFAULT 1 COMMENT ''审核状态 1通过 0待审 2拒绝'' AFTER profile_completed','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND COLUMN_NAME='profile_completed_at');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD COLUMN profile_completed_at BIGINT NOT NULL DEFAULT 0 COMMENT ''必填资料首次完成时间毫秒'' AFTER review_status','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE partner_profiles pp
JOIN user u ON u.uid=pp.uid
SET pp.account_eligible=IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0),
    pp.partner_enabled=IF(pp.status=0 AND u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService') AND pp.has_photo=1 AND IFNULL(pp.native_languages,'') NOT IN ('','[]','null') AND IFNULL(pp.learning_languages,'') NOT IN ('','[]','null'),0,pp.partner_enabled),
    pp.profile_completed=IF(pp.has_photo=1 AND IFNULL(pp.native_languages,'') NOT IN ('','[]','null') AND IFNULL(pp.learning_languages,'') NOT IN ('','[]','null'),1,0),
    pp.profile_completed_at=IF(pp.profile_completed_at>0,pp.profile_completed_at,IF(pp.has_photo=1 AND IFNULL(pp.native_languages,'') NOT IN ('','[]','null') AND IFNULL(pp.learning_languages,'') NOT IN ('','[]','null'),UNIX_TIMESTAMP(pp.updated_at)*1000,0)),
    pp.status=IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService') AND pp.partner_enabled=1 AND pp.review_status=1 AND pp.has_photo=1 AND IFNULL(pp.native_languages,'') NOT IN ('','[]','null') AND IFNULL(pp.learning_languages,'') NOT IN ('','[]','null'),1,0);

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_profiles' AND INDEX_NAME='idx_partner_profile_eligibility');
SET @sql := IF(@exist=0,'ALTER TABLE partner_profiles ADD INDEX idx_partner_profile_eligibility(account_eligible,partner_enabled,profile_completed,review_status,last_active_at,uid)','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Strong idempotency metadata for pending messages.
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_pending_message' AND COLUMN_NAME='payload_hash');
SET @sql := IF(@exist=0,'ALTER TABLE partner_pending_message ADD COLUMN payload_hash CHAR(64) NOT NULL DEFAULT '''' COMMENT ''canonical payload SHA-256'' AFTER content_type','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_pending_message' AND COLUMN_NAME='next_check_at');
SET @sql := IF(@exist=0,'ALTER TABLE partner_pending_message ADD COLUMN next_check_at BIGINT NOT NULL DEFAULT 0 COMMENT ''uncertain reconciliation time'' AFTER failed_reason','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Reliable IM permission outbox. A pair state change and its permission intents are
-- committed together; a worker retries partial WuKongIM failures.
CREATE TABLE IF NOT EXISTS partner_im_permission_outbox (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  idempotency_key VARCHAR(160) NOT NULL DEFAULT '',
  channel_uid VARCHAR(40) NOT NULL DEFAULT '',
  member_uid VARCHAR(40) NOT NULL DEFAULT '',
  action VARCHAR(16) NOT NULL DEFAULT '' COMMENT 'add/remove',
  status TINYINT NOT NULL DEFAULT 0 COMMENT '0 pending,1 done,2 retry',
  attempts INT NOT NULL DEFAULT 0,
  next_retry_at BIGINT NOT NULL DEFAULT 0,
  last_error VARCHAR(500) NOT NULL DEFAULT '',
  created_at BIGINT NOT NULL DEFAULT 0,
  updated_at BIGINT NOT NULL DEFAULT 0,
  UNIQUE KEY uk_partner_im_permission_idempotency(idempotency_key),
  KEY idx_partner_im_permission_retry(status,next_retry_at,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴悟空IM白名单可靠任务';
