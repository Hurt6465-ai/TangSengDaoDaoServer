-- +migrate Up

-- 推荐页独立使用 720 派生图；详情页继续使用 photos 中的 1080/1440 主图。
-- 旧资料没有 card_photos 时由服务端自动回退到 photos，无需重新上传。
SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_profiles' AND COLUMN_NAME = 'card_photos') = 0,
  'ALTER TABLE `dating_profiles` ADD COLUMN `card_photos` VARCHAR(3000) NOT NULL DEFAULT '''' AFTER `photos`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dating_served' AND INDEX_NAME = 'idx_dating_served_expire') = 0,
  'ALTER TABLE `dating_served` ADD INDEX `idx_dating_served_expire` (`expires_at`)',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

-- intent 统一迁移为稳定业务 code；客户端自行按语言显示文案。
UPDATE `dating_profiles`
SET `intent` = CASE TRIM(`intent`)
  WHEN '寻找长期伴侣' THEN 'long_term'
  WHEN '长期伴侣，但不拒绝短期交往' THEN 'long_term_open_short'
  WHEN '短期伴侣，但不拒绝长期交往' THEN 'short_term_open_long'
  WHEN '享受短期交往的乐趣' THEN 'short_term'
  WHEN '结交新朋友' THEN 'friends'
  WHEN '顺其自然' THEN 'open'
  WHEN '认真恋爱' THEN 'long_term'
  WHEN 'love' THEN 'long_term'
  WHEN 'dating' THEN 'long_term'
  WHEN 'marriage' THEN 'long_term'
  WHEN 'friend' THEN 'friends'
  WHEN 'chat' THEN 'open'
  ELSE LOWER(TRIM(`intent`))
END
WHERE TRIM(`intent`) <> '';
