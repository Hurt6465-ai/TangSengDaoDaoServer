-- +migrate Up

-- 语伴个人主页字段。生产库可能已经手动加过部分字段，所以保持幂等。

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'tags') = 0,
  'ALTER TABLE `user` ADD COLUMN `tags` VARCHAR(500) NOT NULL DEFAULT '''' COMMENT ''个人主页标签JSON数组''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'profile_cover') = 0,
  'ALTER TABLE `user` ADD COLUMN `profile_cover` VARCHAR(500) NOT NULL DEFAULT '''' COMMENT ''个人主页背景墙图片路径''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'profile_images') = 0,
  'ALTER TABLE `user` ADD COLUMN `profile_images` VARCHAR(2000) NOT NULL DEFAULT '''' COMMENT ''个人主页照片墙JSON数组''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
