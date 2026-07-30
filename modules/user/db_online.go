package user

import (
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/db"
	"github.com/gocraft/dbr/v2"
)

// DB DB
type onlineDB struct {
	session *dbr.Session
	ctx     *config.Context
}

// newOnlineDB newOnlineDB
func newOnlineDB(ctx *config.Context) *onlineDB {
	return &onlineDB{
		session: ctx.DB(),
		ctx:     ctx,
	}
}

// insertOrUpdateUserOnlineTx 插入或更新用户在线信息
func (o *onlineDB) insertOrUpdateUserOnlineTx(m *onlineStatusModel, tx *dbr.Tx) error {
	var err error
	if m.Online == 1 {
		_, err = tx.UpdateBySql("insert into user_online (uid,device_flag,last_online,online,version) values(?,?,?,1,?) ON DUPLICATE KEY UPDATE last_online=VALUES(last_online),online=VALUES(online),updated_at=NOW(),version=VALUES(version)", m.UID, m.DeviceFlag, m.LastOnline, m.Version).Exec()
	} else {
		_, err = tx.UpdateBySql("insert into user_online (uid,device_flag,last_offline,online,version) values(?,?,?,0,?) ON DUPLICATE KEY UPDATE last_offline=VALUES(last_offline),online=VALUES(online),updated_at=NOW(),version=VALUES(version)", m.UID, m.DeviceFlag, m.LastOffline, m.Version).Exec()
	}
	if err != nil {
		return err
	}
	// 同步语伴推荐聚合表。这里按 user_online 当前所有设备重新聚合，避免单设备离线误把多端在线用户置为离线。
	_, _ = tx.UpdateBySql(`UPDATE partner_profiles pp
		LEFT JOIN (
			SELECT uid,MAX(online) AS online,MAX(last_offline) AS last_offline,MAX(GREATEST(last_online,last_offline))*1000 AS last_active_at
			FROM user_online WHERE uid=? GROUP BY uid
		) onl ON onl.uid=pp.uid
		SET pp.online=IFNULL(onl.online,0),pp.last_offline=IFNULL(onl.last_offline,pp.last_offline),pp.last_active_at=GREATEST(IFNULL(pp.last_active_at,0),IFNULL(onl.last_active_at,0)),pp.updated_at=NOW()
		WHERE pp.uid=?`, m.UID, m.UID).Exec()

	// 同步交友推荐聚合字段。推荐查询直接读取 dating_profiles，避免每次请求聚合 user_online。
	_, _ = tx.UpdateBySql(`UPDATE dating_profiles dp
		LEFT JOIN (
			SELECT uid,MAX(online) AS online,MAX(GREATEST(last_online,last_offline))*1000 AS last_active_at
			FROM user_online WHERE uid=? GROUP BY uid
		) onl ON onl.uid=dp.uid
		SET dp.online=IFNULL(onl.online,0),dp.last_active_at=GREATEST(IFNULL(dp.last_active_at,0),IFNULL(onl.last_active_at,0)),dp.updated_at=dp.updated_at
		WHERE dp.uid=?`, m.UID, m.UID).Exec()
	return nil
}

// queryUserOnlineRecets 查询最近在线的用户（最近24小时离线或当前在线）。
// 未返回的UID由API层明确补为离线，避免客户端保留旧状态。
func (o *onlineDB) queryUserOnlineRecets(uids []string) ([]*onlineStatusWeightModel, error) {
	if len(uids) == 0 {
		return make([]*onlineStatusWeightModel, 0), nil
	}

	var models []*onlineStatusWeightModel
	_, err := o.session.
		Select("user_online.uid,user_online.device_flag,user_online.last_online,user_online.last_offline,user_online.online,IFNULL(device_flag.weight,0) weight").
		From("user_online").
		LeftJoin("device_flag", "user_online.device_flag=device_flag.device_flag").
		Where("user_online.uid in ? and (user_online.online=1 or user_online.last_offline>unix_timestamp(now())-?)", uids, int64((24 * time.Hour).Seconds())).
		Load(&models)
	if err != nil {
		return nil, err
	}
	return selectBestOnlineStatusByUID(models), nil
}

func (o *onlineDB) queryUserLastNewOnlines(uids []string) ([]*onlineStatusWeightModel, error) {
	if len(uids) == 0 {
		return make([]*onlineStatusWeightModel, 0), nil
	}

	var models []*onlineStatusWeightModel
	_, err := o.session.
		Select("user_online.uid,user_online.device_flag,user_online.last_online,user_online.last_offline,user_online.online,IFNULL(device_flag.weight,0) weight").
		From("user_online").
		LeftJoin("device_flag", "user_online.device_flag=device_flag.device_flag").
		Where("user_online.uid in ?", uids).
		Load(&models)
	if err != nil {
		return nil, err
	}
	return selectBestOnlineStatusByUID(models), nil
}

// selectBestOnlineStatusByUID 为每个用户选择最适合客户端展示的一台设备：
// 在线优先；多台在线时设备权重高者优先；全部离线时最近离线者优先。
func selectBestOnlineStatusByUID(models []*onlineStatusWeightModel) []*onlineStatusWeightModel {
	if len(models) == 0 {
		return make([]*onlineStatusWeightModel, 0)
	}

	onlineStatusMap := make(map[string]*onlineStatusWeightModel, len(models))
	uidOrder := make([]string, 0, len(models))
	for _, model := range models {
		if model == nil || model.UID == "" {
			continue
		}
		oldOnline := onlineStatusMap[model.UID]
		if oldOnline == nil {
			onlineStatusMap[model.UID] = model
			uidOrder = append(uidOrder, model.UID)
			continue
		}
		if shouldReplaceOnlineStatus(oldOnline, model) {
			onlineStatusMap[model.UID] = model
		}
	}

	newModels := make([]*onlineStatusWeightModel, 0, len(onlineStatusMap))
	for _, uid := range uidOrder {
		if model := onlineStatusMap[uid]; model != nil {
			newModels = append(newModels, model)
		}
	}
	return newModels
}

func shouldReplaceOnlineStatus(current, candidate *onlineStatusWeightModel) bool {
	if current == nil {
		return true
	}
	if candidate == nil {
		return false
	}
	if candidate.Online == 1 && current.Online == 0 {
		return true
	}
	if candidate.Online == 0 && current.Online == 1 {
		return false
	}
	if candidate.Online == 1 {
		if candidate.Weight != current.Weight {
			return candidate.Weight > current.Weight
		}
		return candidate.LastOnline > current.LastOnline
	}
	if candidate.LastOffline != current.LastOffline {
		return candidate.LastOffline > current.LastOffline
	}
	return candidate.Weight > current.Weight
}

func (o *onlineDB) queryOnlinesMoreThan(t time.Duration, limit uint64) ([]*onlineStatusModel, error) {
	var models []*onlineStatusModel
	_, err := o.session.Select("uid,device_flag,last_online,last_offline,online,version").From("user_online").Where("`online`=1 and last_online<?", time.Now().Add(-t).Unix()).OrderAsc("last_online").Limit(limit).Load(&models)
	return models, err
}

// 查询用户最近在线设备
func (o *onlineDB) queryLastOnlineDeviceWithUID(uid string) (*onlineStatusModel, error) {
	var model *onlineStatusModel
	_, err := o.session.Select("*").From("user_online").Where("uid=?", uid).OrderDesc("online=1").OrderDesc("last_offline").Limit(1).Load(&model)
	return model, err
}

func (o *onlineDB) queryOnlineDevice(uid string, deviceFlag config.DeviceFlag) (*onlineStatusModel, error) {
	var onlineStatusModel *onlineStatusModel
	_, err := o.session.Select("*").From("user_online").Where("uid=? and device_flag=?", uid, deviceFlag.Uint8()).Load(&onlineStatusModel)
	return onlineStatusModel, err
}

func (o *onlineDB) exist(uid string, deviceFlag uint8, online int) (bool, error) {
	var cn int
	_, err := o.session.Select("count(*)").From("user_online").Where("uid=? and device_flag=? and `online`=?", uid, deviceFlag, online).Load(&cn)
	return cn > 0, err
}

// 查询用户在线设备里最大权重的在线状态
func (o *onlineDB) queryOnlineMaxWeightWithUID(uid string) (*onlineStatusModel, error) {
	var onlineStatusModel *onlineStatusModel
	_, err := o.session.Select("user_online.*").From("user_online").LeftJoin("device_flag", "user_online.device_flag=device_flag.device_flag").Where("user_online.uid=? and user_online.online=1", uid).OrderDesc("device_flag.weight").Limit(1).Load(&onlineStatusModel)
	return onlineStatusModel, err
}

// 查询在线用户总数量
func (o *onlineDB) queryOnlineCount() (int64, error) {
	var count int64
	_, err := o.session.SelectBySql("select count(distinct uid) as count from user_online where online=1").Load(&count)
	return count, err
}

// OnlineStatusModel 在线状态model
type onlineStatusModel struct {
	UID         string
	DeviceFlag  uint8 // 设备标记 0. APP 1.web
	LastOnline  int   // 最后一次在线时间
	LastOffline int   // 最后一次离线时间
	Online      int
	Version     int64 // 数据版本
	db.BaseModel
}

type onlineStatusWeightModel struct {
	onlineStatusModel
	Weight int // 设备权重
}
