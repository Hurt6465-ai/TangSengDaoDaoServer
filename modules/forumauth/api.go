package forumauth

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServer/modules/user"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/golang-jwt/jwt/v4"
	"go.uber.org/zap"
)

const (
	defaultIssuer   = "tangsengdaodao"
	defaultAudience = "bbs-go-forum"
	defaultTTL      = 5 * time.Minute
	minTTL          = time.Minute
	maxTTL          = 15 * time.Minute
)

type API struct {
	ctx *config.Context
	db  *user.DB
	log.Log
}

type tokenConfig struct {
	secret   string
	issuer   string
	audience string
	ttl      time.Duration
}

type forumClaims struct {
	jwt.RegisteredClaims

	UID               string   `json:"uid"`
	Nickname          string   `json:"nickname"`
	Avatar            string   `json:"avatar"`
	Sex               int      `json:"sex"`
	Description       string   `json:"description"`
	CountryCode       string   `json:"country_code"`
	Country           string   `json:"country"`
	NativeLanguages   []string `json:"native_languages"`
	LearningLanguages []string `json:"learning_languages"`
	ProfileVersion    int64    `json:"profile_version"`
	UserStatus        int      `json:"user_status"`
	IsDestroy         int      `json:"is_destroy"`
}

type tokenResponse struct {
	Token     string `json:"token"`
	TokenType string `json:"token_type"`
	ExpiresIn int64  `json:"expires_in"`
	ExpiresAt int64  `json:"expires_at"`
}

func New(ctx *config.Context) *API {
	return &API{
		ctx: ctx,
		db:  user.NewDB(ctx),
		Log: log.NewTLog("forumauth"),
	}
}

func (a *API) Route(r *wkhttp.WKHttp) {
	group := r.Group("/v1/forum", a.ctx.AuthMiddleware(r))
	group.POST("/token", a.issueToken)
}

func (a *API) issueToken(c *wkhttp.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	uid := strings.TrimSpace(c.GetLoginUID())
	if uid == "" {
		c.ResponseError(errors.New("登录状态无效"))
		return
	}

	cfg, err := loadTokenConfig()
	if err != nil {
		a.Error("论坛登录配置无效", zap.Error(err))
		c.ResponseError(errors.New("论坛登录暂未配置"))
		return
	}

	model, err := a.db.QueryByUID(uid)
	if err != nil {
		a.Error("查询论坛登录用户失败", zap.Error(err), zap.String("uid", uid))
		c.ResponseError(errors.New("读取用户资料失败"))
		return
	}
	if model == nil || model.Status != 1 || model.IsDestroy != 0 {
		c.ResponseError(errors.New("当前账号不可使用论坛"))
		return
	}

	now := time.Now()
	expiresAt := now.Add(cfg.ttl)
	jti, err := randomID(16)
	if err != nil {
		a.Error("生成论坛登录凭证编号失败", zap.Error(err))
		c.ResponseError(errors.New("生成论坛登录凭证失败"))
		return
	}

	// user.UpdateUsersWithField does not consistently update user.updated_at.
	// Use token issue time as a monotonic snapshot version so an older short token
	// can never overwrite profile data synced by a newer token.
	profileVersion := now.UnixMilli()

	claims := forumClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    cfg.issuer,
			Subject:   uid,
			Audience:  jwt.ClaimStrings{cfg.audience},
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			// Backdate the validation timestamps slightly so a small clock offset
			// between the TangSeng and forum containers does not reject a fresh token.
			NotBefore: jwt.NewNumericDate(now.Add(-30 * time.Second)),
			IssuedAt:  jwt.NewNumericDate(now.Add(-30 * time.Second)),
			ID:        jti,
		},
		UID:               uid,
		Nickname:          truncateRunes(model.Name, 16),
		Avatar:            a.avatarURL(uid),
		Sex:               model.Sex,
		Description:       truncateRunes(model.Intro, 2000),
		CountryCode:       truncateRunes(model.CountryCode, 16),
		Country:           truncateRunes(model.Country, 80),
		NativeLanguages:   parseStringList(model.NativeLanguages, 5),
		LearningLanguages: parseStringList(model.LearningLanguages, 5),
		ProfileVersion:    profileVersion,
		UserStatus:        model.Status,
		IsDestroy:         model.IsDestroy,
	}

	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(cfg.secret))
	if err != nil {
		a.Error("签发论坛登录凭证失败", zap.Error(err), zap.String("uid", uid))
		c.ResponseError(errors.New("生成论坛登录凭证失败"))
		return
	}

	c.JSON(http.StatusOK, tokenResponse{
		Token:     token,
		TokenType: "Bearer",
		ExpiresIn: int64(cfg.ttl / time.Second),
		ExpiresAt: expiresAt.Unix(),
	})
}

func loadTokenConfig() (tokenConfig, error) {
	secret := strings.TrimSpace(os.Getenv("FORUM_TOKEN_HMAC_SECRET"))
	if len(secret) < 32 {
		return tokenConfig{}, errors.New("FORUM_TOKEN_HMAC_SECRET must contain at least 32 characters")
	}

	issuer := strings.TrimSpace(os.Getenv("FORUM_TOKEN_ISSUER"))
	if issuer == "" {
		issuer = defaultIssuer
	}
	audience := strings.TrimSpace(os.Getenv("FORUM_TOKEN_AUDIENCE"))
	if audience == "" {
		audience = defaultAudience
	}

	ttl := defaultTTL
	if raw := strings.TrimSpace(os.Getenv("FORUM_TOKEN_TTL_SECONDS")); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil {
			return tokenConfig{}, fmt.Errorf("invalid FORUM_TOKEN_TTL_SECONDS: %w", err)
		}
		ttl = time.Duration(seconds) * time.Second
	}
	if ttl < minTTL || ttl > maxTTL {
		return tokenConfig{}, fmt.Errorf("FORUM_TOKEN_TTL_SECONDS must be between %d and %d", int(minTTL/time.Second), int(maxTTL/time.Second))
	}

	return tokenConfig{secret: secret, issuer: issuer, audience: audience, ttl: ttl}, nil
}

func (a *API) avatarURL(uid string) string {
	avatarPath := strings.TrimSpace(a.ctx.GetConfig().GetAvatarPath(uid))
	if strings.HasPrefix(strings.ToLower(avatarPath), "https://") || strings.HasPrefix(strings.ToLower(avatarPath), "http://") {
		return avatarPath
	}
	baseURL := strings.TrimSpace(os.Getenv("FORUM_PUBLIC_API_BASE_URL"))
	if baseURL == "" {
		baseURL = strings.TrimSpace(a.ctx.GetConfig().External.APIBaseURL)
	}
	if baseURL == "" || avatarPath == "" {
		return ""
	}
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(avatarPath, "/")
}

func parseStringList(raw string, limit int) []string {
	if limit <= 0 {
		return []string{}
	}
	var values []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &values); err != nil {
		return []string{}
	}
	result := make([]string, 0, minInt(len(values), limit))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = truncateRunes(value, 32)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
		if len(result) == limit {
			break
		}
	}
	return result
}

func truncateRunes(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func randomID(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
