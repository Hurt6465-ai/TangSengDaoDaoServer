package user

import (
	"context"
	"crypto/subtle"
	"fmt"
	"os"
	"strings"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/gocraft/dbr/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/api/idtoken"
	"gopkg.in/yaml.v3"
)

type googleLoginReq struct {
	IDToken string     `json:"id_token"`
	Nonce   string     `json:"nonce"`
	Device  *deviceReq `json:"device"`
}

type googleYAMLConfig struct {
	Google struct {
		WebClientID string `yaml:"webClientID"`
	} `yaml:"google"`
}

// googleLogin 使用 Android Credential Manager 返回的 Google ID Token 登录。
// 客户端只负责获取凭证；签名、aud、iss、exp 和 nonce 均在服务端验证。
func (u *User) googleLogin(c *wkhttp.Context) {
	var req googleLoginReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	req.IDToken = strings.TrimSpace(req.IDToken)
	req.Nonce = strings.TrimSpace(req.Nonce)
	if req.IDToken == "" {
		c.ResponseError(errors.New("Google ID Token 不能为空"))
		return
	}
	if req.Nonce == "" {
		c.ResponseError(errors.New("Google 登录 nonce 不能为空"))
		return
	}
	if req.Device == nil || strings.TrimSpace(req.Device.DeviceID) == "" {
		c.ResponseError(errors.New("登录设备信息不能为空"))
		return
	}

	webClientID, err := u.googleWebClientID()
	if err != nil {
		u.Error("读取 Google 登录配置失败", zap.Error(err))
		c.ResponseError(errors.New("Google 登录尚未配置"))
		return
	}

	loginSpan := u.ctx.Tracer().StartSpan(
		"user.googleLogin",
		opentracing.ChildOf(c.GetSpanContext()),
	)
	defer loginSpan.Finish()
	loginSpanCtx := u.ctx.Tracer().ContextWithSpan(context.Background(), loginSpan)

	payload, err := idtoken.Validate(c.Request.Context(), req.IDToken, webClientID)
	if err != nil {
		u.Warn("Google ID Token 验证失败", zap.Error(err))
		c.ResponseError(errors.New("Google 登录凭证无效或已过期"))
		return
	}

	sub := strings.TrimSpace(payload.Subject)
	if sub == "" {
		sub = googleClaimString(payload.Claims["sub"])
	}
	if sub == "" {
		c.ResponseError(errors.New("Google 登录凭证缺少用户标识"))
		return
	}

	tokenNonce := googleClaimString(payload.Claims["nonce"])
	if tokenNonce == "" || subtle.ConstantTimeCompare([]byte(tokenNonce), []byte(req.Nonce)) != 1 {
		c.ResponseError(errors.New("Google 登录安全校验失败，请重新登录"))
		return
	}

	email := strings.TrimSpace(strings.ToLower(googleClaimString(payload.Claims["email"])))
	if email == "" || !googleClaimBool(payload.Claims["email_verified"]) {
		c.ResponseError(errors.New("Google 账号邮箱未验证"))
		return
	}
	displayName := strings.TrimSpace(googleClaimString(payload.Claims["name"]))
	avatarURL := strings.TrimSpace(googleClaimString(payload.Claims["picture"]))

	identity, err := u.db.queryThirdIdentity(googleIdentityProvider, sub)
	if err != nil {
		u.Error("查询 Google 登录身份失败", zap.Error(err))
		c.ResponseError(errors.New("查询 Google 登录身份失败"))
		return
	}
	if identity != nil {
		userInfo, queryErr := u.db.QueryByUID(identity.UID)
		if queryErr != nil {
			u.Error("查询 Google 登录用户失败", zap.Error(queryErr))
			c.ResponseError(errors.New("查询登录用户失败"))
			return
		}
		if userInfo != nil && userInfo.IsDestroy == 0 {
			if updateErr := u.db.updateThirdIdentityMetadata(googleIdentityProvider, sub, email, displayName, avatarURL); updateErr != nil {
				u.Warn("更新 Google 登录身份资料失败", zap.Error(updateErr))
			}
			u.execLoginAndRespose(userInfo, config.APP, req.Device, loginSpanCtx, c)
			return
		}
		// 用户已注销或身份映射已失效，释放 Google 身份后允许重新注册。
		if deleteErr := u.db.deleteThirdIdentity(googleIdentityProvider, sub); deleteErr != nil {
			u.Error("清理失效 Google 登录身份失败", zap.Error(deleteErr))
			c.ResponseError(errors.New("Google 登录身份状态异常"))
			return
		}
	}

	// 不按照邮箱静默合并账号，避免把一个 Google 身份错误绑定到已有账号。
	existingUser, err := u.db.queryByEmailExact(email)
	if err != nil {
		u.Error("查询 Google 邮箱关联用户失败", zap.Error(err))
		c.ResponseError(errors.New("查询邮箱关联账号失败"))
		return
	}
	if existingUser != nil {
		c.ResponseError(errors.New("该 Google 邮箱已关联现有账号，请先使用原登录方式进入账号后再绑定 Google"))
		return
	}

	uid := util.GenerUUID()
	thirdIdentity := &thirdIdentityModel{
		UID:            uid,
		Provider:       googleIdentityProvider,
		ProviderUserID: sub,
		Email:          email,
		EmailVerified:  1,
		DisplayName:    displayName,
		AvatarURL:      avatarURL,
	}
	createModel := &createUserModel{
		UID:    uid,
		Name:   "", // 保持为空，让 Android 进入完善资料页。
		Email:  email,
		Sex:    2,
		Flag:   int(config.APP),
		Device: req.Device,
		BeforeCommit: func(tx *dbr.Tx) error {
			return u.db.insertThirdIdentityTx(thirdIdentity, tx)
		},
	}
	u.createUser(loginSpanCtx, createModel, c, nil)
}

func (u *User) googleWebClientID() (string, error) {
	configPath := strings.TrimSpace(u.ctx.GetConfig().ConfigFileUsed())
	if configPath == "" {
		configPath = "configs/tsdd.yaml"
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("读取配置文件 %s 失败: %w", configPath, err)
	}
	var cfg googleYAMLConfig
	if err = yaml.Unmarshal(content, &cfg); err != nil {
		return "", fmt.Errorf("解析配置文件 %s 失败: %w", configPath, err)
	}
	clientID := strings.TrimSpace(cfg.Google.WebClientID)
	if clientID == "" {
		return "", errors.New("google.webClientID 未配置")
	}
	if !strings.HasSuffix(clientID, ".apps.googleusercontent.com") {
		return "", errors.New("google.webClientID 格式不正确")
	}
	return clientID, nil
}

func googleClaimString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		if value == nil {
			return ""
		}
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	}
}

func googleClaimBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true") || strings.TrimSpace(v) == "1"
	case float64:
		return v == 1
	case int:
		return v == 1
	case int64:
		return v == 1
	default:
		return false
	}
}
