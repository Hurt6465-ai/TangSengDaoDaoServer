-- +migrate Up

CREATE TABLE IF NOT EXISTS partner_list_recommendation_day (
  id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  viewer_uid VARCHAR(40) NOT NULL DEFAULT '' COMMENT '查看者UID',
  day_key VARCHAR(10) NOT NULL DEFAULT '' COMMENT 'Asia/Shanghai凌晨4点切换的推荐日',
  algorithm_version INT NOT NULL DEFAULT 1 COMMENT '推荐算法版本',
  pool_version VARCHAR(40) NOT NULL DEFAULT '' COMMENT '生成时共享池版本',
  first_served_at BIGINT NOT NULL DEFAULT 0 COMMENT '首次成功返回时间毫秒',
  rotate_at BIGINT NOT NULL DEFAULT 0 COMMENT '允许正常轮换时间毫秒',
  rotation_done TINYINT NOT NULL DEFAULT 0 COMMENT '当天正常轮换是否完成',
  initial_candidate_ids JSON NULL COMMENT '首次名单UID，最多80',
  current_candidate_ids JSON NULL COMMENT '当前名单UID，最多80',
  all_assigned_candidate_ids JSON NULL COMMENT '当天所有分配过的UID，最多100',
  rotated_in_ids JSON NULL COMMENT '正常轮换加入UID',
  rotated_out_ids JSON NULL COMMENT '正常轮换移出UID',
  abnormal_replacement_ids JSON NULL COMMENT '异常补位加入UID',
  unique_assigned_count INT NOT NULL DEFAULT 0 COMMENT '当天不同语伴数量',
  list_version INT NOT NULL DEFAULT 1 COMMENT '客户端名单版本',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_partner_list_viewer_day(viewer_uid,day_key),
  KEY idx_partner_list_day(day_key,updated_at),
  KEY idx_partner_list_viewer_created(viewer_uid,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='独立列表语伴每日推荐状态';
