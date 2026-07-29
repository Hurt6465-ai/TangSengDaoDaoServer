-- +migrate Up

-- Online batch checks filter by online and return uid. A covering composite
-- index avoids table lookups when the partner service refreshes its 5-second
-- in-memory IM-online snapshot for thousands of connected users.
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS
               WHERE TABLE_SCHEMA=DATABASE()
                 AND TABLE_NAME='user_online'
                 AND INDEX_NAME='idx_user_online_online_uid');
SET @sql := IF(@exist=0,
  'ALTER TABLE user_online ADD INDEX idx_user_online_online_uid(online,uid)',
  'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
