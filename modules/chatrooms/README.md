# 开放式话题聊天室

后端路由：

- 列表：`GET /v1/chatrooms/list?limit=30&cursor=...`
- 创建：`POST /v1/chatrooms/create`，支持 `create_request_no` 幂等号
- 进入并标记已读：`POST /v1/chatrooms/enter`
- 再次标记已读：`POST /v1/chatrooms/read`（客户端离开聊天页或回到前台时调用）
- 全局置顶：`POST /v1/chatrooms/pin`，仅管理员可用
- 删除：`POST /v1/chatrooms/delete`，仅创建者或管理员可用

最后回复、参与人数和过期时间由服务端真实 IM 消息监听更新。普通登录用户不再拥有可伪造回复数据的 `message-hook` 接口。

房间默认在最后一条回复后 3 小时过期。清理任务每分钟运行，使用 Redis 分布式锁防止多实例重复清理。

标签统一使用：练口语、找搭子、工作、影视、游戏、学习、交友、闲谈。历史“音乐”标签由迁移自动转换为“游戏”。

二次复查补充：发送者自己的消息会同步推进 last_read_at；过期清理保留 10 秒合并缓冲，避免临界消息尚未落库就先删除房间。
