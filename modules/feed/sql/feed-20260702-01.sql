-- +migrate Up

-- Image-first launch: these indexes make image-only feed candidate queries avoid scanning feed_media repeatedly.
SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_media' AND INDEX_NAME='idx_feed_media_feed_type_sort'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_media ADD KEY idx_feed_media_feed_type_sort(feed_id,type,sort)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
  SELECT COUNT(1) FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='feed_posts' AND INDEX_NAME='idx_feed_posts_img_candidate'
);
SET @sql := IF(@idx_exists=0,
  'ALTER TABLE feed_posts ADD KEY idx_feed_posts_img_candidate(status,visibility,created_at,last_active_at,score,feed_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
