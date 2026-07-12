-- +migrate Up

-- 全局曝光计数用于固定探索位；旧数据按聚合表回填。
SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_profiles' AND COLUMN_NAME = 'exposure_count') = 0,
  'ALTER TABLE `dating_profiles` ADD COLUMN `exposure_count` BIGINT NOT NULL DEFAULT 0 AFTER `profile_score`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

UPDATE `dating_profiles` dp
LEFT JOIN (
  SELECT `to_uid`, SUM(`seen_count`) AS total_count
  FROM `dating_exposures`
  GROUP BY `to_uid`
) e ON e.`to_uid` = dp.`uid`
SET dp.`exposure_count` = IFNULL(e.`total_count`, 0)
WHERE dp.`exposure_count` = 0;

-- 新客户端提供 event_id；旧客户端由服务端生成稳定事件键。NULL 允许旧流水并存。
SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_exposure_events' AND COLUMN_NAME = 'event_id') = 0,
  'ALTER TABLE `dating_exposure_events` ADD COLUMN `event_id` VARCHAR(64) NULL AFTER `id`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_exposure_events' AND INDEX_NAME = 'uk_dating_event_uid_event') = 0,
  'ALTER TABLE `dating_exposure_events` ADD UNIQUE KEY `uk_dating_event_uid_event` (`uid`,`event_id`)',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_profiles' AND INDEX_NAME = 'idx_dating_recommend_cursor') = 0,
  'ALTER TABLE `dating_profiles` ADD INDEX `idx_dating_recommend_cursor` (`enabled`,`status`,`has_photo`,`online`,`last_active_at`,`profile_score`,`updated_at`,`uid`)',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_profiles' AND INDEX_NAME = 'idx_dating_explore') = 0,
  'ALTER TABLE `dating_profiles` ADD INDEX `idx_dating_explore` (`enabled`,`status`,`has_photo`,`exposure_count`,`last_active_at`,`profile_score`)',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

-- 未识别的历史意向不能继续伪装成完整资料；关闭后让用户重新选择。
UPDATE `dating_profiles`
SET `enabled` = 0, `intent` = ''
WHERE TRIM(`intent`) <> ''
  AND TRIM(`intent`) NOT IN (
    'long_term','long_term_open_short','short_term_open_long',
    'short_term','friends','open'
  );
