-- +migrate Up
CREATE TABLE IF NOT EXISTS topic_rooms (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  room_id VARCHAR(64) NOT NULL UNIQUE,
  title VARCHAR(120) NOT NULL,
  tag VARCHAR(32) NOT NULL DEFAULT '闲谈',
  language VARCHAR(32) NOT NULL DEFAULT '中文',
  background_url VARCHAR(255) NOT NULL DEFAULT '',
  background_index INT NOT NULL DEFAULT 1,
  channel_id VARCHAR(64) NOT NULL,
  channel_type INT NOT NULL DEFAULT 2,
  creator_uid VARCHAR(64) NOT NULL,
  creator_name VARCHAR(80) NOT NULL DEFAULT '',
  creator_avatar VARCHAR(255) NOT NULL DEFAULT '',
  last_reply_uid VARCHAR(64) NOT NULL DEFAULT '',
  last_reply_name VARCHAR(80) NOT NULL DEFAULT '',
  last_reply_avatar VARCHAR(255) NOT NULL DEFAULT '',
  last_reply_text VARCHAR(255) NOT NULL DEFAULT '',
  last_reply_type VARCHAR(32) NOT NULL DEFAULT 'text',
  last_reply_at BIGINT NOT NULL DEFAULT 0,
  reply_count INT NOT NULL DEFAULT 0,
  reply_users_json TEXT NULL,
  pinned TINYINT NOT NULL DEFAULT 0,
  hot TINYINT NOT NULL DEFAULT 0,
  hot_until BIGINT NOT NULL DEFAULT 0,
  status TINYINT NOT NULL DEFAULT 1,
  created_at BIGINT NOT NULL,
  expire_at BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_topic_rooms_sort (status, pinned, hot_until, last_reply_at, created_at),
  KEY idx_topic_rooms_expire (status, expire_at),
  KEY idx_topic_rooms_channel (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS topic_room_members (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  room_id VARCHAR(64) NOT NULL,
  channel_id VARCHAR(64) NOT NULL,
  uid VARCHAR(64) NOT NULL,
  name VARCHAR(80) NOT NULL DEFAULT '',
  avatar VARCHAR(255) NOT NULL DEFAULT '',
  last_read_at BIGINT NOT NULL DEFAULT 0,
  created_at BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_topic_room_member (room_id, uid),
  KEY idx_topic_room_members_channel (channel_id),
  KEY idx_topic_room_members_uid (uid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- +migrate Down
DROP TABLE IF EXISTS topic_room_members;
DROP TABLE IF EXISTS topic_rooms;
