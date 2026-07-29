-- +migrate Up

-- Hourly maintenance removes completed assignment jobs after their Redis
-- accounting keys have outlived the ten-day recommendation retention window.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='partner_list_assignment_outbox'
                 AND INDEX_NAME='idx_partner_list_assignment_done');
SET @sql := IF(@exist=0,
  'ALTER TABLE partner_list_assignment_outbox ADD INDEX idx_partner_list_assignment_done(status,updated_at,id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
