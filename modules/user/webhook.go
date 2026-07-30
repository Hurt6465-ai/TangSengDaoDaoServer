package user

import (
	"fmt"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"go.uber.org/zap"
)

const (
	friendUIDCacheTTL      = 30 * time.Minute
	emptyFriendUIDCacheTTL = 5 * time.Minute
)

// 处理上下线通知
func (u *User) handleOnlineStatus(onlineStatuses []config.OnlineStatus) {

	u.Debug("收到在线通知")

	if len(onlineStatuses) == 0 {
		return
	}

	for _, onlineStatus := range onlineStatuses {
		if u.ctx.GetConfig().IsVisitor(onlineStatus.UID) { // 如果是访客不做处理
			continue
		}
		if !onlineStatus.Online && onlineStatus.OnlineCount > 0 { // 如果DeviceFlag下还有其他设备在线，则不做离线逻辑处理
			continue
		}
		mainFlag := u.getMainDeviceFlag()                               // 客户端显示的设备名
		isMain := false                                                 // 是否需要设置主设备
		allOffline := false                                             // 是否所有设备都离线了
		if onlineStatus.Online && mainFlag == onlineStatus.DeviceFlag { // 当前上线的用户为主设备
			isMain = true
		}
		if !isMain {
			mainDeviceFlagM, err := u.getOnlineMainDeviceFlagModel(onlineStatus) // 获取最优显示的主设备
			if err != nil {
				u.Error("判断是否需要将在线状态推送给好友失败！", zap.Error(err), zap.String("uid", onlineStatus.UID))
				continue
			}
			if mainDeviceFlagM != nil {
				mainFlag = mainDeviceFlagM.DeviceFlag
			} else {
				allOffline = true
				mainFlag = onlineStatus.DeviceFlag
			}
		}

		friendUIDs, err := u.getFriendUidsAndSetCache(onlineStatus.UID)
		if err != nil {
			u.Error("获取好友uid集合失败！", zap.Error(err), zap.String("uid", onlineStatus.UID))
			// 单个用户失败不应中断同一批次中其他用户的在线通知。
			continue
		}

		subscribers := make([]string, 0, len(friendUIDs)+1)
		subscribers = append(subscribers, friendUIDs...)
		if onlineStatus.DeviceFlag != config.APP.Uint8() {
			// PC或Web上下线时通知自己的其他设备，即使当前没有好友也不能漏发。
			subscribers = append(subscribers, onlineStatus.UID)
		}
		subscribers = uniqueNonEmptyUIDs(subscribers)
		if len(subscribers) == 0 {
			continue
		}

		online := 0
		if onlineStatus.Online {
			online = 1
		}
		param := map[string]interface{}{
			"online":           online,
			"device_flag":      onlineStatus.DeviceFlag,
			"uid":              onlineStatus.UID,
			"main_device_flag": mainFlag,
		}
		if allOffline {
			param["all_offline"] = 1
		}
		err = u.ctx.SendCMD(config.MsgCMDReq{
			Subscribers: subscribers,
			CMD:         common.CMDOnlineStatus,
			NoPersist:   true,
			Param:       param,
		})
		if err != nil {
			u.Warn("发送在线状态cmd失败！", zap.Error(err), zap.String("uid", onlineStatus.UID), zap.Int("subscriberCount", len(subscribers)))
			continue
		}
	}
}

// 获取在线的主设备
func (u *User) getOnlineMainDeviceFlagModel(onlineStatus config.OnlineStatus) (*onlineStatusModel, error) {

	onlineMaxWeightStatus, err := u.onlineDB.queryOnlineMaxWeightWithUID(onlineStatus.UID)
	if err != nil {
		u.Error("获取在线设备里最大权重的设备失败！", zap.Error(err), zap.String("uid", onlineStatus.UID))
		return nil, err
	}
	if onlineMaxWeightStatus != nil {
		return onlineMaxWeightStatus, nil
	}
	return nil, nil

}

// 获取好友uid并缓存。空好友集合使用短期标记，避免无好友用户每次上线都查询数据库；
// 非空好友集合设置过期时间，异常情况下也不会永久保留陈旧数据。
func (u *User) getFriendUidsAndSetCache(uid string) ([]string, error) {
	friendKey := fmt.Sprintf("%s%s", CacheKeyFriends, uid)
	emptyKey := friendKey + ":empty"

	members, err := u.ctx.GetRedisConn().SMembers(friendKey)
	if err != nil {
		return nil, err
	}
	members = uniqueNonEmptyUIDs(members)
	if len(members) > 0 {
		_ = u.ctx.GetRedisConn().Expire(friendKey, friendUIDCacheTTL)
		return members, nil
	}

	empty, err := u.ctx.GetRedisConn().GetString(emptyKey)
	if err != nil {
		return nil, err
	}
	if empty == "1" {
		return make([]string, 0), nil
	}

	friendModels, err := u.friendDB.QueryFriends(uid)
	if err != nil {
		return nil, err
	}
	members = make([]string, 0, len(friendModels))
	for _, friendModel := range friendModels {
		members = append(members, friendModel.ToUID)
	}
	members = uniqueNonEmptyUIDs(members)

	if len(members) == 0 {
		if err = u.ctx.GetRedisConn().SetAndExpire(emptyKey, "1", emptyFriendUIDCacheTTL); err != nil {
			return nil, err
		}
		return members, nil
	}

	memberObjs := make([]interface{}, 0, len(members))
	for _, member := range members {
		memberObjs = append(memberObjs, member)
	}
	if err = u.ctx.GetRedisConn().SAdd(friendKey, memberObjs...); err != nil {
		return nil, err
	}
	if err = u.ctx.GetRedisConn().Expire(friendKey, friendUIDCacheTTL); err != nil {
		return nil, err
	}
	return members, nil
}

func uniqueNonEmptyUIDs(uids []string) []string {
	if len(uids) == 0 {
		return make([]string, 0)
	}
	result := make([]string, 0, len(uids))
	seen := make(map[string]struct{}, len(uids))
	for _, rawUID := range uids {
		uid := strings.TrimSpace(rawUID)
		if uid == "" {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		result = append(result, uid)
	}
	return result
}

// 获取主设备标记
func (u *User) getMainDeviceFlag() uint8 {
	deviceFlagModels, err := u.getDeviceFlags()
	if err != nil {
		return uint8(config.APP)
	}
	var mainDeviceFlagM *deviceFlagModel
	for _, deviceFlagM := range deviceFlagModels {
		if mainDeviceFlagM == nil {
			mainDeviceFlagM = deviceFlagM
			continue
		}
		if deviceFlagM.Weight > mainDeviceFlagM.Weight {
			mainDeviceFlagM = deviceFlagM
		}

	}
	if mainDeviceFlagM == nil {
		return config.Web.Uint8()
	}
	return mainDeviceFlagM.DeviceFlag

}

func (u *User) getDeviceFlags() ([]*deviceFlagModel, error) {
	if u.deviceFlagsCache == nil {
		var err error
		u.deviceFlagsCache, err = u.deviceFlagDB.queryAll()
		if err != nil {
			return nil, err
		}
		if u.deviceFlagsCache == nil {
			u.deviceFlagsCache = make([]*deviceFlagModel, 0)
		}
	}
	return u.deviceFlagsCache, nil
}
