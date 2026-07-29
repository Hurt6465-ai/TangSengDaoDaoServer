-- +migrate Up

-- Raw exposure events are high-volume analytics data. Long-term recommendation
-- state is retained in partner_exposures, so keep a cleanup-friendly index for
-- bounded 30-day raw-event retention.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='partner_exposure_events'
                 AND INDEX_NAME='idx_partner_event_cleanup');
SET @sql := IF(@exist=0,
  'ALTER TABLE partner_exposure_events ADD INDEX idx_partner_event_cleanup(event_at,id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
