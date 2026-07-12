-- +migrate Up

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_list_recommendation_day' AND COLUMN_NAME='rotation_retry_at');
SET @sql := IF(@exist=0,'ALTER TABLE partner_list_recommendation_day ADD COLUMN rotation_retry_at BIGINT NOT NULL DEFAULT 0 COMMENT ''轮换无候选后的下次重试时间毫秒'' AFTER rotate_at','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='partner_list_recommendation_day' AND COLUMN_NAME='candidate_scores');
SET @sql := IF(@exist=0,'ALTER TABLE partner_list_recommendation_day ADD COLUMN candidate_scores JSON NULL COMMENT ''首次/轮换完整推荐分数快照'' AFTER abnormal_replacement_ids','SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

CREATE TABLE IF NOT EXISTS partner_list_assignment_outbox (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  viewer_uid VARCHAR(40) NOT NULL DEFAULT '',
  candidate_uid VARCHAR(40) NOT NULL DEFAULT '',
  day_key VARCHAR(10) NOT NULL DEFAULT '',
  bucket VARCHAR(40) NOT NULL DEFAULT '',
  assigned_at BIGINT NOT NULL DEFAULT 0,
  status TINYINT NOT NULL DEFAULT 0 COMMENT '0 pending,1 done,2 retry',
  attempts INT NOT NULL DEFAULT 0,
  next_retry_at BIGINT NOT NULL DEFAULT 0,
  last_error VARCHAR(500) NOT NULL DEFAULT '',
  created_at BIGINT NOT NULL DEFAULT 0,
  updated_at BIGINT NOT NULL DEFAULT 0,
  UNIQUE KEY uk_partner_list_assignment(viewer_uid,candidate_uid,day_key),
  KEY idx_partner_list_assignment_retry(status,next_retry_at,id),
  KEY idx_partner_list_assignment_day(day_key,candidate_uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='列表语伴分配计数可靠任务';
