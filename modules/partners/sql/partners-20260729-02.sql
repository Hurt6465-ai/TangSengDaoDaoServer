-- +migrate Up

-- Hourly maintenance deletes completed permission jobs by status and age.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='partner_im_permission_outbox'
                 AND INDEX_NAME='idx_partner_im_permission_done');
SET @sql := IF(@exist=0,
  'ALTER TABLE partner_im_permission_outbox ADD INDEX idx_partner_im_permission_done(status,updated_at,id)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Keyset scan used by the periodic pending-permission reconciliation job.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='partner_contacts'
                 AND INDEX_NAME='idx_partner_contact_pending_repair');
SET @sql := IF(@exist=0,
  'ALTER TABLE partner_contacts ADD INDEX idx_partner_contact_pending_repair(status,requester_uid,to_uid,uid)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
