-- +migrate Up

CREATE TABLE IF NOT EXISTS `user_third_identity` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `uid` VARCHAR(64) NOT NULL,
  `provider` VARCHAR(32) NOT NULL,
  `provider_user_id` VARCHAR(255) NOT NULL,
  `email` VARCHAR(255) NOT NULL DEFAULT '',
  `email_verified` TINYINT NOT NULL DEFAULT 0,
  `display_name` VARCHAR(255) NOT NULL DEFAULT '',
  `avatar_url` VARCHAR(1000) NOT NULL DEFAULT '',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_third_identity_provider_user` (`provider`, `provider_user_id`),
  UNIQUE KEY `uk_third_identity_uid_provider` (`uid`, `provider`),
  KEY `idx_third_identity_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户第三方登录身份';

-- +migrate Down

DROP TABLE IF EXISTS `user_third_identity`;
