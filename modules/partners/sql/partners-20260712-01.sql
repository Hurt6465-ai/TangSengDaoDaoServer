-- +migrate Up

-- 列表/全屏语伴共用的每日陌生人额度。推荐日固定按 Asia/Shanghai 凌晨 4 点切换。
CREATE TABLE IF NOT EXISTS partner_greeting_daily_usage (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  sender_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '打招呼发起人UID',
  day_key VARCHAR(10) NOT NULL DEFAULT '' COMMENT 'Asia/Shanghai凌晨4点切换的推荐日',
  used_count INT NOT NULL DEFAULT 0 COMMENT '当天已联系的不同陌生人数',
  created_at BIGINT NOT NULL DEFAULT 0 COMMENT '创建时间毫秒',
  updated_at BIGINT NOT NULL DEFAULT 0 COMMENT '更新时间毫秒',
  UNIQUE KEY uk_partner_greeting_usage_sender_day(sender_uid,day_key),
  KEY idx_partner_greeting_usage_day(day_key,updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴每日陌生人打招呼额度';

CREATE TABLE IF NOT EXISTS partner_greeting_daily_target (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  sender_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '打招呼发起人UID',
  receiver_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '当天首次联系的陌生人UID',
  day_key VARCHAR(10) NOT NULL DEFAULT '' COMMENT 'Asia/Shanghai凌晨4点切换的推荐日',
  created_at BIGINT NOT NULL DEFAULT 0 COMMENT '创建时间毫秒',
  UNIQUE KEY uk_partner_greeting_target_sender_receiver_day(sender_uid,receiver_uid,day_key),
  KEY idx_partner_greeting_target_sender_day(sender_uid,day_key,created_at),
  KEY idx_partner_greeting_target_day(day_key,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴每日已联系陌生人明细';

-- pending 关系消息统一经业务后端投递。该表负责 client_msg_no 幂等与三条消息预占。
CREATE TABLE IF NOT EXISTS partner_pending_message (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  sender_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '发送者UID',
  receiver_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '接收者UID',
  client_msg_no VARCHAR(100) NOT NULL DEFAULT '' COMMENT '业务客户端消息唯一号',
  content_type INT NOT NULL DEFAULT 0 COMMENT '消息内容类型',
  reserved_count INT NOT NULL DEFAULT 0 COMMENT '本次是否预占pending消息次数',
  status TINYINT NOT NULL DEFAULT 0 COMMENT '0 reserved,1 delivered,2 failed',
  im_client_msg_no VARCHAR(100) NOT NULL DEFAULT '' COMMENT '悟空IM返回的client_msg_no',
  im_message_id VARCHAR(40) NOT NULL DEFAULT '' COMMENT '悟空IM返回的message_id',
  failed_reason VARCHAR(255) NOT NULL DEFAULT '' COMMENT '失败原因',
  created_at BIGINT NOT NULL DEFAULT 0 COMMENT '创建时间毫秒',
  updated_at BIGINT NOT NULL DEFAULT 0 COMMENT '更新时间毫秒',
  UNIQUE KEY uk_partner_pending_sender_client(sender_uid,client_msg_no),
  KEY idx_partner_pending_pair(sender_uid,receiver_uid,status,updated_at),
  KEY idx_partner_pending_status(status,updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='语伴pending消息投递幂等记录';
