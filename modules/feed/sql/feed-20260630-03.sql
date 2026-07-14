-- +migrate Up

ALTER TABLE feed_comments MODIFY COLUMN content TEXT NOT NULL COMMENT '评论内容，支持 voice: 音频协议';

-- +migrate Down

