# Chatrooms 模块

开放式话题聊天室：

- 发布话题：`POST /v1/chatrooms/create`
- 话题列表：`GET /v1/chatrooms/list`
- 进入房间：`POST /v1/chatrooms/enter`
- 置顶/取消置顶：`POST /v1/chatrooms/pin`
- 删除：`POST /v1/chatrooms/delete`
- 消息回调更新最后回复：`POST /v1/chatrooms/message-hook`

不走普通 `group/create`，所以不会触发好友关系校验。

房间底层仍用 WuKongIM group channel_type=2，但 channel datasource 由本模块优先处理。`internal/modules.go` 里 `chatrooms` 必须位于 `group` 之前。

过期规则：

- 没回复：`created_at + 3h` 过期
- 有回复：每次消息回调更新 `expire_at = last_reply_at + 3h`

卡片数据：

- 发布者大头像：`creator_avatar`
- 最后 6 个去重回复者：`reply_users`
- 最后一条预览：`last_reply_name + last_reply_text`
- 热议：`hot/hot_until`
