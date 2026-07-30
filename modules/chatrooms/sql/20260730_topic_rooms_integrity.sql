-- +migrate Up

-- 创建请求幂等号：NULL 允许旧客户端继续创建多个房间；新客户端传入请求号后按创建者唯一。
SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND COLUMN_NAME = 'create_request_no'
);
SET @sql := IF(
  @exist = 0,
  'ALTER TABLE topic_rooms ADD COLUMN create_request_no VARCHAR(64) NULL DEFAULT NULL AFTER room_id',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 即使服务器上曾手工添加过同名字段，也统一修正为可空类型。
ALTER TABLE topic_rooms MODIFY COLUMN create_request_no VARCHAR(64) NULL DEFAULT NULL;

-- 兼容可能存在的半成品迁移，空字符串统一改为 NULL，避免唯一索引冲突。
UPDATE topic_rooms SET create_request_no = NULL WHERE create_request_no = '';

-- 若曾部署过未加唯一索引的半成品版本，保留最早一条，其余重复请求号清空。
UPDATE topic_rooms newer
JOIN topic_rooms older
  ON newer.creator_uid = older.creator_uid
 AND newer.create_request_no = older.create_request_no
 AND newer.id > older.id
SET newer.create_request_no = NULL
WHERE newer.create_request_no IS NOT NULL;

-- 旧版本曾把“游戏”提交为“音乐”，统一清洗为“游戏”。
UPDATE topic_rooms SET tag = '游戏' WHERE tag = '音乐';

SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND INDEX_NAME = 'uk_topic_rooms_creator_request'
);
SET @sql := IF(
  @exist = 0,
  'ALTER TABLE topic_rooms ADD UNIQUE INDEX uk_topic_rooms_creator_request(creator_uid, create_request_no)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 过期清理按 expire_at 顺序扫描，补充 room_id 作为稳定尾键。
SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND INDEX_NAME = 'idx_topic_rooms_expire_room'
);
SET @sql := IF(
  @exist = 0,
  'ALTER TABLE topic_rooms ADD INDEX idx_topic_rooms_expire_room(status, expire_at, room_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- +migrate Down
SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND INDEX_NAME = 'idx_topic_rooms_expire_room'
);
SET @sql := IF(
  @exist > 0,
  'ALTER TABLE topic_rooms DROP INDEX idx_topic_rooms_expire_room',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND INDEX_NAME = 'uk_topic_rooms_creator_request'
);
SET @sql := IF(
  @exist > 0,
  'ALTER TABLE topic_rooms DROP INDEX uk_topic_rooms_creator_request',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'topic_rooms'
    AND COLUMN_NAME = 'create_request_no'
);
SET @sql := IF(
  @exist > 0,
  'ALTER TABLE topic_rooms DROP COLUMN create_request_no',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
