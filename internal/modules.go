package modules

// 引入模块
import (
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/base"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/channel"
	// chatrooms 必须放在 group 前面。开放话题房也使用 group channel_type，
	// 先注册 chatrooms 可让 datasource 优先识别 topic room，避免走普通群好友校验。
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/chatrooms"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/common"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/file"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/group"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/message"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/openapi"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/qrcode"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/report"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/robot"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/search"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/statistics"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/user"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/webhook"
	_ "github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/workplace"
)
