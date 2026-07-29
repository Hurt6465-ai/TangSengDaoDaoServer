-- +migrate Up

-- Background delivery scans pending greetings globally by status and retry time.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='partner_greetings'
                 AND INDEX_NAME='idx_partner_greeting_pending');
SET @sql := IF(@exist=0,
  'ALTER TABLE partner_greetings ADD INDEX idx_partner_greeting_pending(send_status,last_send_at)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
