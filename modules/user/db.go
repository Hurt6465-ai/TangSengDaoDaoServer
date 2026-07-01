package user

import (
	"context"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/db"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
	"github.com/gocraft/dbr/v2"
)

// DB 用户db操作
type DB struct {
	session *dbr.Session
	ctx     *config.Context
}

// NewDB NewDB
func NewDB(ctx *config.Context) *DB {
	return &DB{
		session: ctx.DB(),
		ctx:     ctx,
	}
}

// QueryByKeyword 通过用户名查询用户信息

func (d *DB) ensureProfileColumns() error {
	columns := []struct {
		name string
		ddl  string
	}{
		{"intro", "ALTER TABLE `user` ADD COLUMN `intro` VARCHAR(500) NOT NULL DEFAULT '' COMMENT '自我介绍'"},
		{"country_code", "ALTER TABLE `user` ADD COLUMN `country_code` VARCHAR(10) NOT NULL DEFAULT '' COMMENT '国籍/地区ISO代码'"},
		{"country", "ALTER TABLE `user` ADD COLUMN `country` VARCHAR(80) NOT NULL DEFAULT '' COMMENT '国籍/地区显示名'"},
		{"native_languages", "ALTER TABLE `user` ADD COLUMN `native_languages` VARCHAR(500) NOT NULL DEFAULT '' COMMENT '母语JSON数组，最多5个'"},
		{"learning_languages", "ALTER TABLE `user` ADD COLUMN `learning_languages` VARCHAR(500) NOT NULL DEFAULT '' COMMENT '学习语言JSON数组，最多5个'"},
		{"birthday", "ALTER TABLE `user` ADD COLUMN `birthday` VARCHAR(20) NOT NULL DEFAULT '' COMMENT '出生日期 yyyy-MM-dd'"},
		{"tags", "ALTER TABLE `user` ADD COLUMN `tags` VARCHAR(500) NOT NULL DEFAULT '' COMMENT '个人主页标签JSON数组'"},
		{"profile_cover", "ALTER TABLE `user` ADD COLUMN `profile_cover` VARCHAR(500) NOT NULL DEFAULT '' COMMENT '个人主页背景墙图片路径'"},
		{"profile_images", "ALTER TABLE `user` ADD COLUMN `profile_images` VARCHAR(2000) NOT NULL DEFAULT '' COMMENT '个人主页照片墙JSON数组'"},
	}
	for _, column := range columns {
		exists, err := d.userColumnExists(column.name)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		if _, err = d.session.UpdateBySql(column.ddl).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) userColumnExists(columnName string) (bool, error) {
	var count int
	err := d.session.Select("COUNT(*)").
		From("information_schema.COLUMNS").
		Where("TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", "user", columnName).
		LoadOne(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (d *DB) QueryByKeyword(keyword string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("(short_no=? and short_no<>'') or (username=? and username<>'') or (phone=? and phone<>'') ", keyword, keyword, keyword).Load(&model)
	return model, err
}

// QueryByUsername 通过用户名查询用户信息
func (d *DB) QueryByUsername(username string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("username=? or concat(zone,phone)=? or email=?", username, username, username).Load(&model)
	return model, err
}

// QueryUIDsByUsernames 通过用户名查询用户uids
func (d *DB) QueryUIDsByUsernames(usernames []string) ([]string, error) {
	var uids []string
	_, err := d.session.Select("uid").From("user").Where("username in ?", usernames).Load(&uids)
	return uids, err
}

// QueryByUsernameCxt 通过用户名查询用户信息
func (d *DB) QueryByUsernameCxt(ctx context.Context, username string) (*Model, error) {
	span, _ := d.ctx.Tracer().StartSpanFromContext(ctx, "QueryByUsername")
	defer span.Finish()
	return d.QueryByUsername(username)
}

// QueryByPhone 通过手机号和区号查询用户信息
func (d *DB) QueryByPhone(zone string, phone string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("zone=? and phone=?", zone, phone).Load(&model)
	return model, err
}

// 查询多个手机号用户
func (d *DB) QueryByPhones(phones []string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("CONCAT(`zone`,`phone`) in ?", phones).Load(&models)
	return models, err
}

// Insert 添加用户
func (d *DB) Insert(m *Model) error {
	_, err := d.session.InsertInto("user").Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
	return err
}

// Insert 添加用户
func (d *DB) insertTx(m *Model, tx *dbr.Tx) error {
	_, err := tx.InsertInto("user").Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
	return err
}

// QueryByUID 通过用户uid查询用户信息
func (d *DB) QueryByUID(uid string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("uid=?", uid).Load(&model)
	return model, err
}

// QueryByVercode 通过用户vercode查询用户信息
func (d *DB) QueryByVercode(vercode string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("vercode=?", vercode).Load(&model)
	return model, err
}

// queryByQRVerCode 通过用户QRVercode查询用户信息
func (d *DB) queryByQRVerCode(QRVercode string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("qr_vercode=?", QRVercode).Load(&model)
	return model, err
}

func (d *DB) queryByUIDs(uids []string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("uid in ?", uids).Load(&models)
	return models, err
}
func (d *DB) queryAll() ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("is_destroy=0 and bench_no='' ").Load(&models)
	return models, err
}

// QueryDetailByUID 查询用户详情
func (d *DB) QueryDetailByUID(uid string, loginUID string) (*Detail, error) {
	var detail *Detail
	_, err := d.session.Select("user.*,IFNULL(user_setting.mute,0) mute,IFNULL(user_setting.top,0) top,IFNULL(user_setting.chat_pwd_on,0) chat_pwd_on,IFNULL(user_setting.revoke_remind,0) revoke_remind,IFNULL(user_setting.screenshot,0) screenshot,IFNULL(user_setting.receipt,0) receipt").From("user").LeftJoin("user_setting", "user.uid=user_setting.to_uid and user_setting.uid=?").Where("user.uid=?", loginUID, uid).Load(&detail)
	return detail, err
}

// QueryDetailByUIDs 查询用户详情集合
func (d *DB) QueryDetailByUIDs(uids []string, loginUID string) ([]*Detail, error) {
	if len(uids) <= 0 {
		return nil, nil
	}
	var details []*Detail
	_, err := d.session.Select("user.*,IFNULL(user_setting.mute,0) mute,IFNULL(user_setting.top,0) top,IFNULL(user_setting.chat_pwd_on,0) chat_pwd_on,IFNULL(user_setting.revoke_remind,1) revoke_remind,IFNULL(user_setting.screenshot,0) screenshot,IFNULL(user_setting.receipt,0) receipt").From("user").LeftJoin("user_setting", "user.uid=user_setting.to_uid and user_setting.uid=?").Where("user.uid in ?", loginUID, uids).Load(&details)
	return details, err
}

// QueryByUIDs 根据用户uid查询用户信息
func (d *DB) QueryByUIDs(uids []string) ([]*Model, error) {
	if len(uids) <= 0 {
		return nil, nil
	}
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("uid in ?", uids).Load(&models)
	return models, err
}

// QueryUserWithOnlyShortNo 通过short_no获取用户信息
func (d *DB) QueryUserWithOnlyShortNo(shortNo string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("user.name,user.username").From("user").Where("short_no=?", shortNo).Load(&model)
	return model, err
}

// UpdateUsersWithField 修改用户基本资料
func (d *DB) UpdateUsersWithField(field string, value string, uid string) error {
	_, err := d.session.Update("user").Set(field, value).Where("uid=?", uid).Exec()
	if err != nil {
		return err
	}
	// 语伴推荐用 partner_profiles 聚合表，用户资料改动后同步一份。
	// 同步失败不阻断用户资料保存；后续迁移/backfill 会兜底修复。
	_ = d.syncPartnerProfileFromUser(uid)
	return nil
}

func (d *DB) syncPartnerProfileFromUser(uid string) error {
	if uid == "" {
		return nil
	}
	_, err := d.session.UpdateBySql(`INSERT INTO partner_profiles(uid,name,username,sex,birthday,intro,country_code,country,native_languages,learning_languages,tags,profile_cover,profile_images,vercode,has_photo,profile_score,status,last_active_at,created_at,updated_at)
		SELECT u.uid,IFNULL(u.name,''),IFNULL(u.username,''),IFNULL(u.sex,0),IFNULL(u.birthday,''),IFNULL(u.intro,''),IFNULL(u.country_code,''),IFNULL(u.country,''),IFNULL(u.native_languages,''),IFNULL(u.learning_languages,''),IFNULL(u.tags,''),IFNULL(u.profile_cover,''),IFNULL(u.profile_images,''),IFNULL(u.vercode,''),
		IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',1,0) AS has_photo,
		(IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',20,0)+IF(IFNULL(u.native_languages,'')<>'',10,0)+IF(IFNULL(u.learning_languages,'')<>'',10,0)+IF(IFNULL(u.intro,'')<>'',5,0)+IF(IFNULL(u.country_code,'')<>'',5,0)) AS profile_score,
		IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0) AS status,
		GREATEST(UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,UNIX_TIMESTAMP(IFNULL(u.created_at,NOW()))*1000),NOW(),NOW()
		FROM user u WHERE u.uid=?
		ON DUPLICATE KEY UPDATE name=VALUES(name),username=VALUES(username),sex=VALUES(sex),birthday=VALUES(birthday),intro=VALUES(intro),country_code=VALUES(country_code),country=VALUES(country),native_languages=VALUES(native_languages),learning_languages=VALUES(learning_languages),tags=VALUES(tags),profile_cover=VALUES(profile_cover),profile_images=VALUES(profile_images),vercode=VALUES(vercode),has_photo=VALUES(has_photo),profile_score=VALUES(profile_score),status=VALUES(status),last_active_at=GREATEST(IFNULL(last_active_at,0),VALUES(last_active_at)),updated_at=NOW()`, uid).Exec()
	return err
}

// AddOrRemoveBlacklist 添加黑名单
func (d *DB) AddOrRemoveBlacklistTx(uid string, touid string, blacklist int, version int64, tx *dbr.Tx) error {
	_, err := tx.Update("user_setting").Set("blacklist", blacklist).Set("version", version).Set("updated_at", dbr.Expr("Now()")).Where("uid=? and to_uid=?", uid, touid).Exec()
	return err
}

// Blacklists  黑名单列表
func (d *DB) Blacklists(uid string) ([]*BlacklistModel, error) {
	var models []*BlacklistModel
	_, err := d.session.Select("user.name,user.username,user.uid,user.country_code,user.country,user.is_upload_avatar").From("user").LeftJoin("user_setting", "user.uid=user_setting.to_uid and user_setting.blacklist=1").Where("user_setting.uid=? and user.is_destroy=0", uid).Load(&models)
	return models, err
}

// QueryByCategory 根据用户分类查询用户列表
func (d *DB) QueryByCategory(category string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("category=?", category).Load(&models)
	return models, err
}

func (d *DB) queryWithCategories(categories []string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("category in ?", categories).Load(&models)
	return models, err
}

// QueryWithAppID 根据appID查询用户列表
func (d *DB) QueryWithAppID(appID string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("app_id=? and status=1", appID).Load(&models)
	return models, err
}

// 查询总用户
func (d *DB) queryUserCount() (int64, error) {
	var count int64
	_, err := d.session.Select("count(*)").From("user").Load(&count)
	return count, err
}

// 查询某天的注册数量
func (d *DB) queryRegisterCountWithDate(date string) (int64, error) {
	var count int64
	_, err := d.session.Select("count(*)").From("user").Where("date_format(created_at,'%Y-%m-%d')=?", date).Load(&count)
	return count, err
}

// 查询某个区间的注册数量
func (d *DB) queryRegisterCountWithDateSpace(startDate, endDate string) ([]*Model, error) {
	var models []*Model
	_, err := d.session.Select("*").From("user").Where("date_format(created_at,'%Y-%m-%d')>=? and date_format(created_at,'%Y-%m-%d')<=?", startDate, endDate).Load(&models)
	return models, err
}

func (d *DB) updateUser(userMap map[string]interface{}, uid string) error {
	_, err := d.session.Update("user").SetMap(userMap).Where("uid=?", uid).Exec()
	return err
}

func (d *DB) updatePassword(password string, uid string) error {
	_, err := d.session.Update("user").Set("password", password).Where("uid=?", uid).Exec()
	return err
}

// 注销账户
func (d *DB) destroyAccount(uid, username, phone string) error {
	_, err := d.session.Update("user").SetMap(map[string]interface{}{
		"phone":              phone,
		"username":           username,
		"name":               "已注销用户",
		"intro":              "",
		"country_code":       "",
		"country":            "",
		"native_languages":   "",
		"learning_languages": "",
		"birthday":           "",
		"tags":               "",
		"profile_cover":      "",
		"profile_images":     "",
		"is_upload_avatar":   0,
		"is_destroy":         1,
		"updated_at":         dbr.Expr("Now()"),
	}).Where("uid=?", uid).Exec()
	return err
}

func (d *DB) queryWithWXOpenIDAndWxUnionidCtx(ctx context.Context, wxOpenid, wxUnionid string) (*Model, error) {
	span, _ := d.ctx.Tracer().StartSpanFromContext(ctx, "queryWithWXOpenIDAndWxUnionid")
	defer span.Finish()
	return d.queryWithWXOpenIDAndWxUnionid(wxOpenid, wxUnionid)
}

// 通过微信openid和unionid查询用户
func (d *DB) queryWithWXOpenIDAndWxUnionid(wxOpenid, wxUnionid string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("wx_openid=? and wx_unionid=?", wxOpenid, wxUnionid).Load(&model)
	return model, err
}

// 通过gitee uid查询用户
func (d *DB) queryWithGiteeUID(giteeUID string) (*Model, error) {

	var model *Model
	_, err := d.session.Select("*").From("user").Where("gitee_uid=?", giteeUID).Load(&model)
	return model, err
}

// 通过github uid查询用户
func (d *DB) queryWithGithubUID(githubUID string) (*Model, error) {
	var model *Model
	_, err := d.session.Select("*").From("user").Where("github_uid=?", githubUID).Load(&model)
	return model, err
}

func (d *DB) updateUserMsgExpireSecond(uid string, msgExpireSecond int64) error {
	_, err := d.session.Update("user").Set("msg_expire_second", msgExpireSecond).Where("uid=?", uid).Exec()
	return err
}
func (d *DB) queryUserRedDot(uid, category string) (*userRedDotModel, error) {
	var model *userRedDotModel
	_, err := d.session.Select("*").From("user_red_dot").Where("uid=? and category=?", uid, category).Load(&model)
	return model, err
}
func (d *DB) insertUserRedDotTx(m *userRedDotModel, tx *dbr.Tx) error {
	_, err := tx.InsertInto("user_red_dot").Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
	return err
}

func (d *DB) insertUserRedDot(m *userRedDotModel) error {
	_, err := d.session.InsertInto("user_red_dot").Columns(util.AttrToUnderscore(m)...).Record(m).Exec()
	return err
}
func (d *DB) updateUserRedDot(m *userRedDotModel) error {
	_, err := d.session.Update("user_red_dot").SetMap(map[string]interface{}{
		"count":  m.Count,
		"is_dot": m.IsDot,
	}).Where("uid=? and category=?", m.UID, m.Category).Exec()
	return err
}

func (d *DB) updateUserRedDotTx(m *userRedDotModel, tx *dbr.Tx) error {
	_, err := tx.Update("user_red_dot").SetMap(map[string]interface{}{
		"count":  m.Count,
		"is_dot": m.IsDot,
	}).Where("uid=? and category=?", m.UID, m.Category).Exec()
	return err
}

// ------------ model ------------

// BlacklistModel 黑名单用户
type BlacklistModel struct {
	UID            string // 用户唯一id
	Name           string // 用户名称
	Username       string // 用户名
	CountryCode    string // 国籍/地区ISO代码
	Country        string // 国籍/地区显示名
	IsUploadAvatar int    // 是否上传过头像
	db.BaseModel
}

// Detail 详情
type Detail struct {
	Model
	Mute         int // 免打扰
	Top          int // 置顶
	ChatPwdOn    int //是否开启聊天密码
	Screenshot   int //截屏通知
	RevokeRemind int //撤回提醒
	Receipt      int //消息回执
	db.BaseModel
}

// Model 用户db model
type Model struct {
	AppID             string //app id
	UID               string // 用户唯一id
	Name              string // 用户名称
	Username          string // 用户名
	Email             string // email地址
	Password          string // 用户密码
	Category          string //用户分类
	Sex               int    //性别
	Intro             string //自我介绍
	CountryCode       string //国籍/地区ISO代码
	Country           string //国籍/地区显示名
	NativeLanguages   string //母语JSON数组
	LearningLanguages string //学习语言JSON数组
	Birthday          string //出生日期
	Tags              string //个人主页标签JSON数组
	ProfileCover      string //个人主页背景墙图片路径
	ProfileImages     string //个人主页照片墙JSON数组
	ShortNo           string //唯一短编号
	ShortStatus       int    //唯一短编号是否修改0.否1.是
	Zone              string //区号
	Phone             string //手机号
	ChatPwd           string //聊天密码
	LockScreenPwd     string // 锁屏密码
	LockAfterMinute   int    // 在几分钟后锁屏 0表示立即
	DeviceLock        int    //是否开启设备锁
	SearchByPhone     int    //是否可以通过手机号搜索0.否1.是
	SearchByShort     int    //是否可以通过短编号搜索0.否1.是
	NewMsgNotice      int    //新消息通知0.否1.是
	MsgShowDetail     int    //显示消息通知详情0.否1.是
	VoiceOn           int    //声音0.否1.是
	ShockOn           int    //震动0.否1.是
	OfflineProtection int    // 离线保护
	Version           int64
	Status            int    // 状态 0.禁用 1.启用
	Vercode           string //验证码
	QRVercode         string // 二维码验证码
	IsUploadAvatar    int    // 是否上传过头像0:未上传1:已上传
	Role              string // 角色 admin/superAdmin
	Robot             int    // 机器人0.否1.是
	MuteOfApp         int    // app是否禁音（当pc登录的时候app可以设置禁音，当pc登录后有效）
	IsDestroy         int    // 是否已注销0.否1.是
	WXOpenid          string // 微信openid
	WXUnionid         string // 微信unionid
	GiteeUID          string // gitee uid
	GithubUID         string // github uid
	Web3PublicKey     string // web3公钥
	MsgExpireSecond   int64  // 消息过期时长
	db.BaseModel
}

// type userSetting struct {
// 	UID          string
// 	ToUID        string
// 	Top          int
// 	Mute         int
// 	Blacklist    int //是否在黑名单
// 	ChatPwdOn    int // 是否开启聊天密码
// 	Screenshot   int //截屏通知
// 	RevokeRemind int //撤回提醒
// 	Receipt      int //消息回执
// }

type userRedDotModel struct {
	UID      string
	Count    int    // 未读数量
	IsDot    int    // 是否显示红点 1.是 0.否
	Category string // 红点分类
	db.BaseModel
}
