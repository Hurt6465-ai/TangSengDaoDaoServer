-- +migrate Up

-- 用户主动暂停状态需要跨设备保存；完整资料首次保存时可自动开启。
SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dating_profiles' AND COLUMN_NAME='user_paused')=0,
  'ALTER TABLE `dating_profiles` ADD COLUMN `user_paused` SMALLINT NOT NULL DEFAULT 0 AFTER `enabled`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

-- 仅把升级前“确实完整但关闭”的账号视为主动暂停。
-- 不完整空壳不能标记暂停，否则用户以后补全生日/性别时永远不会首次自动开启。
UPDATE `dating_profiles` dp
LEFT JOIN `user` u ON u.uid=dp.uid
SET dp.`user_paused`=1
WHERE dp.`enabled`=0
  AND dp.`status`=1
  AND (
        (IFNULL(TRIM(dp.`photos`),'')<>'' AND TRIM(dp.`photos`)<>'[]')
        OR
        (IFNULL(TRIM(dp.`card_photos`),'')<>'' AND TRIM(dp.`card_photos`)<>'[]')
      )
  AND IFNULL(TRIM(dp.`intent`),'')<>''
  AND CASE WHEN u.sex IN (0,1) THEN u.sex ELSE dp.sex END IN (0,1)
  AND STR_TO_DATE(COALESCE(NULLIF(u.birthday,''),dp.birthday),'%Y-%m-%d') IS NOT NULL
  AND TIMESTAMPDIFF(
        YEAR,
        STR_TO_DATE(COALESCE(NULLIF(u.birthday,''),dp.birthday),'%Y-%m-%d'),
        CURDATE()
      )>=18;

-- 匹配与好友同步状态，用于网络失败后的幂等修复。
SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dating_matches' AND COLUMN_NAME='friend_auto_created')=0,
  'ALTER TABLE `dating_matches` ADD COLUMN `friend_auto_created` SMALLINT NOT NULL DEFAULT 0 AFTER `notice_sent`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

SET @dating_sql = IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='dating_matches' AND COLUMN_NAME='friend_synced')=0,
  'ALTER TABLE `dating_matches` ADD COLUMN `friend_synced` SMALLINT NOT NULL DEFAULT 0 AFTER `friend_auto_created`',
  'SELECT 1'
);
PREPARE dating_stmt FROM @dating_sql;
EXECUTE dating_stmt;
DEALLOCATE PREPARE dating_stmt;

-- 新版本只保留一套交友照片。旧数据只有 card_photos 时先回填 photos，
-- card_photos 暂时保留为兼容镜像，旧客户端不会丢图。
UPDATE `dating_profiles`
SET `photos`=`card_photos`
WHERE (IFNULL(TRIM(`photos`),'')='' OR TRIM(`photos`)='[]')
  AND IFNULL(TRIM(`card_photos`),'')<>'' AND TRIM(`card_photos`)<>'[]';

UPDATE `dating_profiles`
SET `card_photos`=`photos`,
    `has_photo`=IF(IFNULL(TRIM(`photos`),'')<>'' AND TRIM(`photos`)<>'[]',1,0);

-- 定位统一最多缓存7天；历史30天定位立即收口。
UPDATE `dating_profiles`
SET `expires_at`=LEAST(`expires_at`,`location_updated_at`+604800000)
WHERE `location_updated_at`>0
  AND `expires_at`>`location_updated_at`+604800000;

UPDATE `dating_profiles`
SET `expires_at`=0
WHERE `location_updated_at`<=0 AND (`lat`<>0 OR `lng`<>0);
