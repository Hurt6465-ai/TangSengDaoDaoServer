-- +migrate Up

-- 幂等补索引：用户经常重复部署/重跑迁移，直接 ALTER ADD KEY 容易因为 Duplicate key name 中断启动。
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_rooms' AND INDEX_NAME = 'idx_topic_rooms_sort_cursor');
SET @sql := IF(@exist = 0, 'ALTER TABLE topic_rooms ADD INDEX idx_topic_rooms_sort_cursor(status,pinned,hot_until,last_reply_at,created_at,room_id)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_room_members' AND INDEX_NAME = 'idx_topic_room_members_uid_room_read');
SET @sql := IF(@exist = 0, 'ALTER TABLE topic_room_members ADD INDEX idx_topic_room_members_uid_room_read(uid,room_id,last_read_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_room_members' AND INDEX_NAME = 'idx_topic_room_members_channel_updated');
SET @sql := IF(@exist = 0, 'ALTER TABLE topic_room_members ADD INDEX idx_topic_room_members_channel_updated(channel_id,updated_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- +migrate Down
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_room_members' AND INDEX_NAME = 'idx_topic_room_members_channel_updated');
SET @sql := IF(@exist > 0, 'ALTER TABLE topic_room_members DROP INDEX idx_topic_room_members_channel_updated', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_room_members' AND INDEX_NAME = 'idx_topic_room_members_uid_room_read');
SET @sql := IF(@exist > 0, 'ALTER TABLE topic_room_members DROP INDEX idx_topic_room_members_uid_room_read', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'topic_rooms' AND INDEX_NAME = 'idx_topic_rooms_sort_cursor');
SET @sql := IF(@exist > 0, 'ALTER TABLE topic_rooms DROP INDEX idx_topic_rooms_sort_cursor', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
