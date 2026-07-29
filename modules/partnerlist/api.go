package partnerlist

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"go.uber.org/zap"
)

type PartnerList struct {
	ctx     *config.Context
	service *Service
	log.Log
}

func New(ctx *config.Context) *PartnerList {
	return &PartnerList{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("partnerlist")}
}

func (p *PartnerList) Route(r *wkhttp.WKHttp) {
	group := r.Group("/v1/partner-list", p.ctx.AuthMiddleware(r))
	{
		group.GET("/recommendations", p.recommendations)
		group.GET("/list", p.recommendations)
		group.POST("/online/batch", p.onlineBatch)
		group.POST("/activity/heartbeat", p.heartbeat)
		group.PUT("/settings", p.settings)
	}
}

func (p *PartnerList) recommendations(c *wkhttp.Context) {
	resp, err := p.service.Recommendations(c.GetLoginUID())
	if err != nil {
		uid := c.GetLoginUID()
		if errors.Is(err, ErrRecommendationBusy) {
			retryAfterMS := recommendationRetryAfterMS(uid, time.Now())
			c.Header("Retry-After", strconv.FormatInt((retryAfterMS+999)/1000, 10))
			c.JSON(http.StatusTooManyRequests, map[string]interface{}{
				"msg": err.Error(), "status": http.StatusTooManyRequests, "retry_after_ms": retryAfterMS,
			})
			return
		}
		p.Warn("查询列表语伴失败", zap.Error(err), zap.String("uid", uid))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (p *PartnerList) onlineBatch(c *wkhttp.Context) {
	var req OnlineBatchReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := p.service.OnlineBatch(req.UIDs)
	if err != nil {
		p.Warn("批量查询语伴在线状态失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (p *PartnerList) heartbeat(c *wkhttp.Context) {
	resp, err := p.service.Heartbeat(c.GetLoginUID())
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (p *PartnerList) settings(c *wkhttp.Context) {
	var req PartnerSettingsReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := p.service.SetEnabled(c.GetLoginUID(), req.Enabled)
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func recommendationRetryAfterMS(uid string, now time.Time) int64 {
	h := uint32(2166136261)
	seed := uid + ":" + strconv.FormatInt(now.Unix()/10, 10)
	for i := 0; i < len(seed); i++ {
		h ^= uint32(seed[i])
		h *= 16777619
	}
	return 3000 + int64(h%5001)
}
