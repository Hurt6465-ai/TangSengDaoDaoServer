-- +migrate Up

-- 老版本已经创建 partner_locations 时，补齐 v23 字段。
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_locations' AND COLUMN_NAME = 'accuracy');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_locations ADD COLUMN accuracy DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT ''定位精度米'' AFTER lng', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_locations' AND COLUMN_NAME = 'radius_meters');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_locations ADD COLUMN radius_meters INT NOT NULL DEFAULT 70000 COMMENT ''附近半径米，默认70km'' AFTER accuracy', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_locations' AND INDEX_NAME = 'idx_partner_location_lat_lng');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_locations ADD INDEX idx_partner_location_lat_lng(lat,lng)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 老版本已经创建 partner_greetings 时，补齐私信式招呼字段。
SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND COLUMN_NAME = 'text');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD COLUMN text VARCHAR(200) NOT NULL DEFAULT '''' COMMENT ''招呼文本'' AFTER to_uid', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND COLUMN_NAME = 'source');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT ''partner_browse'' COMMENT ''来源'' AFTER text', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_greetings' AND INDEX_NAME = 'idx_partner_greeting_to_time');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_greetings ADD INDEX idx_partner_greeting_to_time(to_uid,last_greet_at)', 'SELECT 1');
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
