-- +migrate Up

-- 用户资料扩展字段。这里不能直接 ADD COLUMN，因为生产库可能已经手动/自动加过部分字段；
-- 直接 ADD 会导致容器启动时报 Duplicate column 并反复重启。
-- 使用 information_schema + PREPARE 做成幂等迁移：字段不存在才添加，存在就跳过。

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'intro') = 0,
  'ALTER TABLE `user` ADD COLUMN `intro` VARCHAR(500) NOT NULL DEFAULT '''' COMMENT ''自我介绍''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'country_code') = 0,
  'ALTER TABLE `user` ADD COLUMN `country_code` VARCHAR(10) NOT NULL DEFAULT '''' COMMENT ''国籍/地区ISO代码''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'country') = 0,
  'ALTER TABLE `user` ADD COLUMN `country` VARCHAR(80) NOT NULL DEFAULT '''' COMMENT ''国籍/地区显示名''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'native_languages') = 0,
  'ALTER TABLE `user` ADD COLUMN `native_languages` VARCHAR(500) NOT NULL DEFAULT '''' COMMENT ''母语JSON数组，最多5个''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'learning_languages') = 0,
  'ALTER TABLE `user` ADD COLUMN `learning_languages` VARCHAR(500) NOT NULL DEFAULT '''' COMMENT ''学习语言JSON数组，最多5个''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'user' AND COLUMN_NAME = 'birthday') = 0,
  'ALTER TABLE `user` ADD COLUMN `birthday` VARCHAR(20) NOT NULL DEFAULT '''' COMMENT ''出生日期 yyyy-MM-dd''',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
