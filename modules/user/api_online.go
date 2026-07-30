package user

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const maxOnlineStatusUIDs = 1000

// 退出pc登录
func (u *User) pcQuit(c *wkhttp.Context) {

	err := u.ctx.QuitUserDevice(c.GetLoginUID(), int(config.Web)) // 退出web
	if err != nil {
		u.Error("退出web设备失败", zap.Error(err))
		c.ResponseError(errors.New("退出web设备失败"))
		return
	}

	err = u.ctx.QuitUserDevice(c.GetLoginUID(), int(config.PC))
	if err != nil {
		u.Error("退出PC设备失败", zap.Error(err))
		c.ResponseError(errors.New("退出PC设备失败"))
		return
	}

	err = u.ctx.SendCMD(config.MsgCMDReq{
		NoPersist:   true,
		ChannelID:   c.GetLoginUID(),
		ChannelType: common.ChannelTypePerson.Uint8(),
		CMD:         common.CMDPCQuit,
	})
	if err != nil {
		c.ResponseErrorf("发送指令失败！", err)
		return
	}

	c.ResponseOK()
}

func (u *User) onlinelistWithUIDs(c *wkhttp.Context) {
	var rawUIDs []string
	if err := c.BindJSON(&rawUIDs); err != nil {
		c.ResponseError(err)
		return
	}

	uids, overflow := normalizeOnlineUIDs(rawUIDs, maxOnlineStatusUIDs)
	if overflow {
		c.ResponseError(fmt.Errorf("一次最多查询%d个用户的在线状态", maxOnlineStatusUIDs))
		return
	}
	if len(uids) == 0 {
		c.JSON(http.StatusOK, make([]*userOnlineResp, 0))
		return
	}

	// 在线状态功能关闭时也返回完整UID集合，避免客户端沿用旧的在线缓存。
	if !u.ctx.GetConfig().OnlineStatusOn {
		c.JSON(http.StatusOK, buildCompleteOnlineResponses(uids, nil))
		return
	}

	onlines, err := u.onlineDB.queryUserOnlineRecets(uids)
	if err != nil {
		u.Error("查询用户在线状态失败！", zap.Error(err))
		c.ResponseError(errors.New("查询用户在线状态失败！"))
		return
	}

	// 保持请求顺序并补齐所有UID。数据库没有返回的用户明确视为离线，
	// 防止离线超过24小时的用户在客户端继续显示旧的在线状态。
	c.JSON(http.StatusOK, buildCompleteOnlineResponses(uids, onlines))
}

func normalizeOnlineUIDs(rawUIDs []string, limit int) ([]string, bool) {
	if len(rawUIDs) == 0 {
		return make([]string, 0), false
	}

	uids := make([]string, 0, len(rawUIDs))
	seen := make(map[string]struct{}, len(rawUIDs))
	for _, rawUID := range rawUIDs {
		uid := strings.TrimSpace(rawUID)
		if uid == "" {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		if len(uids) >= limit {
			return uids, true
		}
		uids = append(uids, uid)
	}
	return uids, false
}

func buildCompleteOnlineResponses(uids []string, onlines []*onlineStatusWeightModel) []*userOnlineResp {
	onlineMap := make(map[string]*onlineStatusWeightModel, len(onlines))
	for _, online := range onlines {
		if online == nil || online.UID == "" {
			continue
		}
		onlineMap[online.UID] = online
	}

	resps := make([]*userOnlineResp, 0, len(uids))
	for _, uid := range uids {
		if online := onlineMap[uid]; online != nil {
			resps = append(resps, newUserOnlineResp(online))
			continue
		}
		resps = append(resps, &userOnlineResp{
			UID:    uid,
			Online: 0,
		})
	}
	return resps
}

// onlineList 查询在线用户 包含我的pc设备
func (u *User) onlineList(c *wkhttp.Context) {
	if !u.ctx.GetConfig().OnlineStatusOn {
		c.Response(onlineFriendAndDeviceResp{
			Friends: make([]*config.OnlinestatusResp, 0),
		})
		return
	}
	loginUID := c.MustGet("uid").(string)
	friends, err := u.friendDB.QueryFriends(loginUID)
	if err != nil {
		u.Error("查询用户好友失败", zap.Error(err))
		c.ResponseError(errors.New("查询用户好友失败"))
		return
	}
	uids := make([]string, 0, len(friends))
	uidSet := make(map[string]struct{}, len(friends))
	for _, friend := range friends {
		uid := strings.TrimSpace(friend.ToUID)
		if uid == "" {
			continue
		}
		if _, ok := uidSet[uid]; ok {
			continue
		}
		uidSet[uid] = struct{}{}
		uids = append(uids, uid)
	}
	resps, err := u.onlineService.GetUserLastOnlineStatus(uids)
	if err != nil {
		c.ResponseErrorf("获取用户在线状态失败！", err)
		return
	}
	pcOnlineB, err := u.onlineDB.exist(c.GetLoginUID(), config.PC.Uint8(), 1)
	if err != nil {
		c.ResponseErrorf("查询指定在线设备失败！", err)
		return
	}
	webOnline := 0
	if !pcOnlineB {
		webOnlineB, err := u.onlineDB.exist(c.GetLoginUID(), config.Web.Uint8(), 1)
		if err != nil {
			c.ResponseErrorf("查询指定在线设备失败！", err)
			return
		}
		if webOnlineB {
			webOnline = 1
		}
	}
	var pcResp *pcOnlineResp
	if pcOnlineB || webOnline == 1 {
		myM, err := u.db.QueryByUID(c.GetLoginUID())
		if err != nil {
			c.ResponseErrorf("获取我的个人数据失败！", err)
			return
		}
		deviceFlag := config.Web
		if pcOnlineB {
			deviceFlag = config.PC
		}
		pcResp = &pcOnlineResp{
			Online:     1,
			MuteOfApp:  myM.MuteOfApp,
			DeviceFlag: deviceFlag.Uint8(),
		}
	}

	c.Response(onlineFriendAndDeviceResp{
		Friends: resps,
		PC:      pcResp,
	})
}

func (u *User) onlineStatusCheck() {

	u.Debug("开始检查在线状态...")

	onlines, err := u.onlineDB.queryOnlinesMoreThan(time.Minute, 1000)
	if err != nil {
		u.Error("【在线状态检查】查询在线用户数失败！", zap.Error(err))
		return
	}
	if len(onlines) == 0 {
		return
	}
	u.Debug("检查到需要矫正的在线数量", zap.Int("onlines", len(onlines)))

	onlineUIDs := make([]string, 0, len(onlines))
	uidSet := make(map[string]struct{}, len(onlines))
	for _, online := range onlines {
		if online == nil || online.UID == "" {
			continue
		}
		if _, ok := uidSet[online.UID]; ok {
			continue
		}
		uidSet[online.UID] = struct{}{}
		onlineUIDs = append(onlineUIDs, online.UID)
	}
	if len(onlineUIDs) == 0 {
		return
	}

	onlineStatusResps, err := u.ctx.IMSOnlineStatus(onlineUIDs)
	if err != nil {
		u.Error("【在线状态检查】获取在线状态失败！", zap.Error(err))
		return
	}
	u.Debug("检查到需要矫正的在线数量-->", zap.Int("onlineStatusResps", len(onlineStatusResps)))

	// 使用集合比较，避免原先候选设备数 × IM返回设备数的双重循环。
	actualOnlineSet := make(map[string]struct{}, len(onlineStatusResps))
	for _, onlineStatusResp := range onlineStatusResps {
		key := onlineStatusDeviceKey(onlineStatusResp.UID, onlineStatusResp.DeviceFlag)
		actualOnlineSet[key] = struct{}{}
	}

	makeOfflines := make([]*onlineStatusModel, 0, len(onlines))
	for _, online := range onlines {
		if online == nil {
			continue
		}
		if _, ok := actualOnlineSet[onlineStatusDeviceKey(online.UID, online.DeviceFlag)]; !ok {
			makeOfflines = append(makeOfflines, online)
		}
	}

	if len(makeOfflines) > 0 {
		u.Debug("改变在线状态！", zap.Int("offlineCount", len(makeOfflines)))
		tx, err := u.ctx.DB().Begin()
		if err != nil {
			u.Error("开启事务失败！", zap.Error(err))
			return
		}
		defer func() {
			if err := recover(); err != nil {
				tx.RollbackUnlessCommitted()
				panic(err)
			}
		}()

		now := time.Now()
		nowUnix := int(now.Unix())
		versionBase := now.UnixNano() / 1000
		for index, onlineStatusResp := range makeOfflines {
			err := u.onlineDB.insertOrUpdateUserOnlineTx(&onlineStatusModel{
				UID:         onlineStatusResp.UID,
				DeviceFlag:  onlineStatusResp.DeviceFlag,
				LastOffline: nowUnix,
				LastOnline:  nowUnix,
				Online:      0,
				Version:     versionBase + int64(index),
			}, tx)
			if err != nil {
				tx.Rollback()
				u.Error("【在线状态检查】添加或更新用户在线状态失败！", zap.Error(err))
				return
			}
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			u.Error("【在线状态检查】提交在线状态数据库的事务失败！！", zap.Error(err))
			return
		}
	}

}

func onlineStatusDeviceKey(uid string, deviceFlag uint8) string {
	return fmt.Sprintf("%s:%d", uid, deviceFlag)
}

type onlineFriendAndDeviceResp struct {
	PC      *pcOnlineResp              `json:"pc,omitempty"`
	Friends []*config.OnlinestatusResp `json:"friends,omitempty"` // 我的最近在线的好友
}

type pcOnlineResp struct {
	Online     int   `json:"online"`      // pc是否在线
	DeviceFlag uint8 `json:"device_flag"` // 设备类型
	MuteOfApp  int   `json:"mute_of_app"` // app是否开启禁音
}
