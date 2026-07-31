-- +migrate Up

-- 大型 JSON/图片列表字段必须使用 TEXT。utf8mb4 下多个 VARCHAR(1000~3000)
-- 会按每字符最多 4 字节计入 InnoDB 行大小，可能超过 65535 字节。
-- 本迁移兼容已经创建过旧版 dating_profiles 的部署；新部署在 20260701-01 中已直接使用 TEXT。
ALTER TABLE `dating_profiles`
  MODIFY COLUMN `native_languages` TEXT NULL,
  MODIFY COLUMN `learning_languages` TEXT NULL,
  MODIFY COLUMN `tags` TEXT NULL,
  MODIFY COLUMN `personality_tags` TEXT NULL,
  MODIFY COLUMN `pet_tags` TEXT NULL,
  MODIFY COLUMN `sport_tags` TEXT NULL,
  MODIFY COLUMN `movie_tags` TEXT NULL,
  MODIFY COLUMN `dealbreakers` TEXT NULL,
  MODIFY COLUMN `photos` TEXT NULL,
  MODIFY COLUMN `card_photos` TEXT NULL;

ALTER TABLE `dating_reports`
  MODIFY COLUMN `images` TEXT NULL;
