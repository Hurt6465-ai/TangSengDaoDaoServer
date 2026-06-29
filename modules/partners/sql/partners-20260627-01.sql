-- +migrate Up

CREATE TABLE IF NOT EXISTS partner_locations (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '用户UID',
  lat DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '纬度',
  lng DECIMAL(10,6) NOT NULL DEFAULT 0 COMMENT '经度',
  accuracy DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT '定位精度米',
  radius_meters INT NOT NULL DEFAULT 70000 COMMENT '附近半径米，默认70km',
  geohash VARCHAR(32) NOT NULL DEFAULT '' COMMENT '粗略地理格子',
  updated_at_ms BIGINT NOT NULL DEFAULT 0 COMMENT '定位更新时间毫秒',
  expires_at BIGINT NOT NULL DEFAULT 0 COMMENT '定位过期时间毫秒，默认30天',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_partner_location_uid(uid),
  KEY idx_partner_location_geo(geohash),
  KEY idx_partner_location_expires(expires_at),
  KEY idx_partner_location_lat_lng(lat,lng)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴定位缓存';

CREATE TABLE IF NOT EXISTS partner_exposures (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '浏览者UID',
  to_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '被曝光语伴UID',
  seen_count INT NOT NULL DEFAULT 0 COMMENT '曝光次数',
  last_seen_at BIGINT NOT NULL DEFAULT 0 COMMENT '最后曝光毫秒',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_partner_exposure(uid,to_uid),
  KEY idx_partner_exposure_uid_seen(uid,last_seen_at),
  KEY idx_partner_exposure_to_uid(to_uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴曝光记录';

CREATE TABLE IF NOT EXISTS partner_greetings (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '发起打招呼UID，由token确定',
  to_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '目标UID',
  text VARCHAR(200) NOT NULL DEFAULT '' COMMENT '招呼文本',
  source VARCHAR(32) NOT NULL DEFAULT 'partner_browse' COMMENT '来源',
  greet_count INT NOT NULL DEFAULT 0 COMMENT '打招呼次数',
  last_greet_at BIGINT NOT NULL DEFAULT 0 COMMENT '最后打招呼毫秒',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_partner_greeting(uid,to_uid),
  KEY idx_partner_greeting_uid_time(uid,last_greet_at),
  KEY idx_partner_greeting_to_uid(to_uid),
  KEY idx_partner_greeting_to_time(to_uid,last_greet_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴私信式打招呼记录';
