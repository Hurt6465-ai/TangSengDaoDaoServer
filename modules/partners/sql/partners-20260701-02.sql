-- +migrate Up

-- v27: 语伴推荐聚合表。推荐/附近只查 partner_profiles，避免每次扫 user + user_online。
CREATE TABLE IF NOT EXISTS partner_profiles (
  uid VARCHAR(40) NOT NULL PRIMARY KEY COMMENT '用户UID',
  name VARCHAR(80) NOT NULL DEFAULT '' COMMENT '昵称',
  username VARCHAR(80) NOT NULL DEFAULT '' COMMENT '用户名',
  sex TINYINT NOT NULL DEFAULT 0 COMMENT '性别',
  birthday VARCHAR(20) NOT NULL DEFAULT '' COMMENT '生日',
  intro VARCHAR(500) NOT NULL DEFAULT '' COMMENT '自我介绍',
  country_code VARCHAR(10) NOT NULL DEFAULT '' COMMENT '国家/地区代码',
  country VARCHAR(80) NOT NULL DEFAULT '' COMMENT '国家/地区名',
  native_languages VARCHAR(500) NOT NULL DEFAULT '' COMMENT '母语JSON',
  learning_languages VARCHAR(500) NOT NULL DEFAULT '' COMMENT '学习语言JSON',
  tags VARCHAR(1000) NOT NULL DEFAULT '' COMMENT '标签JSON',
  profile_cover VARCHAR(500) NOT NULL DEFAULT '' COMMENT '背景图',
  profile_images VARCHAR(2000) NOT NULL DEFAULT '' COMMENT '语伴照片JSON',
  vercode VARCHAR(40) NOT NULL DEFAULT '' COMMENT '验证码/加好友码',
  has_photo TINYINT NOT NULL DEFAULT 0 COMMENT '是否有语伴照片',
  profile_score DOUBLE NOT NULL DEFAULT 0 COMMENT '资料完整度分',
  online TINYINT NOT NULL DEFAULT 0 COMMENT '是否在线',
  last_offline INT NOT NULL DEFAULT 0 COMMENT '最后离线秒',
  last_active_at BIGINT NOT NULL DEFAULT 0 COMMENT '最后活跃毫秒',
  lat DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '纬度',
  lng DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '经度',
  accuracy DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '定位精度米',
  radius_meters INT NOT NULL DEFAULT 70000 COMMENT '附近半径米',
  geohash VARCHAR(32) NOT NULL DEFAULT '' COMMENT '粗略地理格子',
  location_updated_at BIGINT NOT NULL DEFAULT 0 COMMENT '定位更新时间毫秒',
  expires_at BIGINT NOT NULL DEFAULT 0 COMMENT '定位过期时间毫秒',
  status TINYINT NOT NULL DEFAULT 1 COMMENT '推荐状态 1正常',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_partner_profile_feed(status,has_photo,online,last_active_at,updated_at),
  KEY idx_partner_profile_geo(status,has_photo,expires_at,lat,lng,last_active_at),
  KEY idx_partner_profile_country(status,country_code,last_active_at),
  KEY idx_partner_profile_active(status,last_active_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴推荐资料聚合表';

-- 幂等补索引，避免旧库已有表但缺索引。
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_profiles' AND INDEX_NAME = 'idx_partner_profile_feed');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_profiles ADD INDEX idx_partner_profile_feed(status,has_photo,online,last_active_at,updated_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_profiles' AND INDEX_NAME = 'idx_partner_profile_geo');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_profiles ADD INDEX idx_partner_profile_geo(status,has_photo,expires_at,lat,lng,last_active_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_profiles' AND INDEX_NAME = 'idx_partner_profile_country');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_profiles ADD INDEX idx_partner_profile_country(status,country_code,last_active_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_profiles' AND INDEX_NAME = 'idx_partner_profile_active');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_profiles ADD INDEX idx_partner_profile_active(status,last_active_at)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 首次回填 user 基础资料。
INSERT INTO partner_profiles(uid,name,username,sex,birthday,intro,country_code,country,native_languages,learning_languages,tags,profile_cover,profile_images,vercode,has_photo,profile_score,status,last_active_at,created_at,updated_at)
SELECT u.uid,IFNULL(u.name,''),IFNULL(u.username,''),IFNULL(u.sex,0),IFNULL(u.birthday,''),IFNULL(u.intro,''),IFNULL(u.country_code,''),IFNULL(u.country,''),IFNULL(u.native_languages,''),IFNULL(u.learning_languages,''),IFNULL(u.tags,''),IFNULL(u.profile_cover,''),IFNULL(u.profile_images,''),IFNULL(u.vercode,''),
  IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',1,0) AS has_photo,
  (IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',20,0)+IF(IFNULL(u.native_languages,'')<>'',10,0)+IF(IFNULL(u.learning_languages,'')<>'',10,0)+IF(IFNULL(u.intro,'')<>'',5,0)+IF(IFNULL(u.country_code,'')<>'',5,0)) AS profile_score,
  IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0) AS status,
  GREATEST(UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,UNIX_TIMESTAMP(IFNULL(u.created_at,NOW()))*1000),NOW(),NOW()
FROM user u
ON DUPLICATE KEY UPDATE
  name=VALUES(name),username=VALUES(username),sex=VALUES(sex),birthday=VALUES(birthday),intro=VALUES(intro),country_code=VALUES(country_code),country=VALUES(country),
  native_languages=VALUES(native_languages),learning_languages=VALUES(learning_languages),tags=VALUES(tags),profile_cover=VALUES(profile_cover),profile_images=VALUES(profile_images),vercode=VALUES(vercode),
  has_photo=VALUES(has_photo),profile_score=VALUES(profile_score),status=VALUES(status),last_active_at=GREATEST(IFNULL(partner_profiles.last_active_at,0),VALUES(last_active_at)),updated_at=NOW();

-- 回填定位。
UPDATE partner_profiles pp
JOIN partner_locations pl ON pl.uid=pp.uid
SET pp.lat=pl.lat,pp.lng=pl.lng,pp.accuracy=IFNULL(pl.accuracy,0),pp.radius_meters=IFNULL(pl.radius_meters,70000),pp.geohash=IFNULL(pl.geohash,''),pp.location_updated_at=IFNULL(pl.updated_at_ms,0),pp.expires_at=IFNULL(pl.expires_at,0),pp.updated_at=NOW();

-- 回填在线/活跃。这里一次性聚合 user_online，后续由在线监听增量同步。
UPDATE partner_profiles pp
LEFT JOIN (
  SELECT uid,MAX(online) AS online,MAX(last_offline) AS last_offline,MAX(GREATEST(last_online,last_offline))*1000 AS last_active_at
  FROM user_online GROUP BY uid
) onl ON onl.uid=pp.uid
SET pp.online=IFNULL(onl.online,0),pp.last_offline=IFNULL(onl.last_offline,pp.last_offline),pp.last_active_at=GREATEST(IFNULL(pp.last_active_at,0),IFNULL(onl.last_active_at,0)),pp.updated_at=NOW();

-- 附近查询辅助索引，重复执行安全。
SET @exist := (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'partner_locations' AND INDEX_NAME = 'idx_partner_location_exp_geo');
SET @sql := IF(@exist = 0, 'ALTER TABLE partner_locations ADD INDEX idx_partner_location_exp_geo(expires_at,lat,lng,uid)', 'SELECT 1');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
