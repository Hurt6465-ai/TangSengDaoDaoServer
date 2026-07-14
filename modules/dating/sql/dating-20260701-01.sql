-- +migrate Up

-- 交友模块基础表。该模块以前未在 internal/modules.go 中导入，导致路由与迁移都没有注册。
-- 全部使用 IF NOT EXISTS，兼容已经手工创建过表的部署。
CREATE TABLE IF NOT EXISTS `dating_profiles` (
  `uid` VARCHAR(40) NOT NULL,
  `name` VARCHAR(100) NOT NULL DEFAULT '',
  `username` VARCHAR(40) NOT NULL DEFAULT '',
  `sex` SMALLINT NOT NULL DEFAULT -1,
  `birthday` VARCHAR(20) NOT NULL DEFAULT '',
  `enabled` SMALLINT NOT NULL DEFAULT 0,
  `intent` VARCHAR(80) NOT NULL DEFAULT '',
  `cross_border_preference` VARCHAR(40) NOT NULL DEFAULT 'open_foreign',
  `gender_preference` SMALLINT NOT NULL DEFAULT -1,
  `min_age` INT NOT NULL DEFAULT 18,
  `max_age` INT NOT NULL DEFAULT 99,
  `city` VARCHAR(80) NOT NULL DEFAULT '',
  `country_code` VARCHAR(10) NOT NULL DEFAULT '',
  `country` VARCHAR(80) NOT NULL DEFAULT '',
  `height_cm` INT NOT NULL DEFAULT 0,
  `weight_kg` INT NOT NULL DEFAULT 0,
  `job` VARCHAR(100) NOT NULL DEFAULT '',
  `job_status` VARCHAR(80) NOT NULL DEFAULT '',
  `education` VARCHAR(80) NOT NULL DEFAULT '',
  `relationship_status` VARCHAR(40) NOT NULL DEFAULT '',
  `sexual_orientation` VARCHAR(40) NOT NULL DEFAULT '',
  `drinking` VARCHAR(40) NOT NULL DEFAULT '',
  `smoking` VARCHAR(40) NOT NULL DEFAULT '',
  `bio` VARCHAR(500) NOT NULL DEFAULT '',
  `ideal_partner` VARCHAR(200) NOT NULL DEFAULT '',
  `native_languages` TEXT NULL,
  `learning_languages` TEXT NULL,
  `tags` TEXT NULL,
  `personality_tags` TEXT NULL,
  `pet_tags` TEXT NULL,
  `sport_tags` TEXT NULL,
  `movie_tags` TEXT NULL,
  `dealbreakers` TEXT NULL,
  `photos` TEXT NULL,
  `card_photos` TEXT NULL,
  `has_photo` SMALLINT NOT NULL DEFAULT 0,
  `show_distance` SMALLINT NOT NULL DEFAULT 1,
  `allow_voice` SMALLINT NOT NULL DEFAULT 1,
  `allow_video` SMALLINT NOT NULL DEFAULT 0,
  `profile_score` INT NOT NULL DEFAULT 0,
  `exposure_count` BIGINT NOT NULL DEFAULT 0,
  `status` SMALLINT NOT NULL DEFAULT 1,
  `online` SMALLINT NOT NULL DEFAULT 0,
  `last_active_at` BIGINT NOT NULL DEFAULT 0,
  `lat` DOUBLE NOT NULL DEFAULT 0,
  `lng` DOUBLE NOT NULL DEFAULT 0,
  `accuracy` DOUBLE NOT NULL DEFAULT 0,
  `radius_meters` INT NOT NULL DEFAULT 70000,
  `location_updated_at` BIGINT NOT NULL DEFAULT 0,
  `expires_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`uid`),
  KEY `idx_dating_recommend_cursor` (`enabled`,`status`,`has_photo`,`online`,`last_active_at`,`profile_score`,`updated_at`,`uid`),
  KEY `idx_dating_explore` (`enabled`,`status`,`has_photo`,`exposure_count`,`last_active_at`,`profile_score`),
  KEY `idx_dating_location` (`enabled`,`status`,`expires_at`,`lat`,`lng`),
  KEY `idx_dating_active` (`enabled`,`status`,`last_active_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_served` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `session_id` VARCHAR(80) NOT NULL DEFAULT '',
  `to_uid` VARCHAR(40) NOT NULL,
  `served_at` BIGINT NOT NULL DEFAULT 0,
  `expires_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_served` (`uid`,`session_id`,`to_uid`),
  KEY `idx_dating_served_expire` (`expires_at`),
  KEY `idx_dating_served_uid_time` (`uid`,`served_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_swipes` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `action` VARCHAR(20) NOT NULL DEFAULT 'pass',
  `source` VARCHAR(32) NOT NULL DEFAULT '',
  `photo_index` INT NOT NULL DEFAULT 0,
  `session_id` VARCHAR(80) NOT NULL DEFAULT '',
  `swiped_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_swipe_pair` (`uid`,`to_uid`),
  KEY `idx_dating_swipes_target` (`to_uid`,`action`,`swiped_at`),
  KEY `idx_dating_swipes_uid_time` (`uid`,`swiped_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_swipe_events` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `action` VARCHAR(20) NOT NULL DEFAULT 'pass',
  `source` VARCHAR(32) NOT NULL DEFAULT '',
  `photo_index` INT NOT NULL DEFAULT 0,
  `session_id` VARCHAR(80) NOT NULL DEFAULT '',
  `swiped_at` BIGINT NOT NULL DEFAULT 0,
  `undone` SMALLINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_dating_swipe_events_uid_day` (`uid`,`swiped_at`,`undone`,`action`),
  KEY `idx_dating_swipe_events_undo` (`uid`,`undone`,`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_favorites` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `favorited_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_favorite_pair` (`uid`,`to_uid`),
  KEY `idx_dating_favorites_uid_time` (`uid`,`favorited_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_undo_events` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `swipe_event_id` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  `to_uid` VARCHAR(40) NOT NULL,
  `action` VARCHAR(20) NOT NULL DEFAULT '',
  `undone_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_dating_undo_uid_time` (`uid`,`undone_at`),
  KEY `idx_dating_undo_swipe` (`swipe_event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_matches` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `match_id` VARCHAR(100) NOT NULL,
  `pair_key` VARCHAR(100) NOT NULL,
  `uid_a` VARCHAR(40) NOT NULL,
  `uid_b` VARCHAR(40) NOT NULL,
  `status` SMALLINT NOT NULL DEFAULT 1,
  `notice_sent` SMALLINT NOT NULL DEFAULT 0,
  `matched_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_match_id` (`match_id`),
  UNIQUE KEY `uk_dating_match_pair` (`pair_key`),
  KEY `idx_dating_matches_uid_a` (`uid_a`,`status`,`updated_at`),
  KEY `idx_dating_matches_uid_b` (`uid_b`,`status`,`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_blocks` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `reason` VARCHAR(200) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_block_pair` (`uid`,`to_uid`),
  KEY `idx_dating_blocks_target` (`to_uid`,`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_reports` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `reason` VARCHAR(80) NOT NULL DEFAULT '',
  `description` VARCHAR(500) NOT NULL DEFAULT '',
  `images` TEXT NULL,
  `status` SMALLINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_dating_reports_target` (`to_uid`,`status`,`created_at`),
  KEY `idx_dating_reports_uid` (`uid`,`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_exposures` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `seen_count` INT NOT NULL DEFAULT 0,
  `last_seen_at` BIGINT NOT NULL DEFAULT 0,
  `last_duration_ms` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_exposure_pair` (`uid`,`to_uid`),
  KEY `idx_dating_exposure_cooldown` (`uid`,`last_seen_at`,`to_uid`),
  KEY `idx_dating_exposure_target` (`to_uid`,`last_seen_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `dating_exposure_events` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `event_id` VARCHAR(64) NULL,
  `uid` VARCHAR(40) NOT NULL,
  `to_uid` VARCHAR(40) NOT NULL,
  `event_type` VARCHAR(24) NOT NULL DEFAULT 'expose',
  `source` VARCHAR(32) NOT NULL DEFAULT '',
  `duration_ms` BIGINT NOT NULL DEFAULT 0,
  `photo_index` INT NOT NULL DEFAULT 0,
  `event_at` BIGINT NOT NULL DEFAULT 0,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dating_event_uid_event` (`uid`,`event_id`),
  KEY `idx_dating_exposure_events_uid` (`uid`,`event_at`),
  KEY `idx_dating_exposure_events_target` (`to_uid`,`event_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
