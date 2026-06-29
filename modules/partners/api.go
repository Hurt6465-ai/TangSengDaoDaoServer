package partners

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

type Partners struct {
	ctx     *config.Context
	service *Service
	log.Log
}

func New(ctx *config.Context) *Partners {
	return &Partners{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("partners")}
}

func (p *Partners) Route(r *wkhttp.WKHttp) {
	partners := r.Group("/v1/partners", p.ctx.AuthMiddleware(r))
	{
		partners.GET("", p.list)
		partners.GET("/list", p.list)
		partners.GET("/feed", p.list)
		partners.GET("/nearby", p.nearby)
		partners.GET("/profile/me", p.profileMe)
		partners.POST("/location", p.location)
		partners.POST("/exposures", p.exposures)
		partners.POST("/greet", p.greeting)
		partners.POST("/greetings", p.greeting)
	}
}

func (p *Partners) list(c *wkhttp.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultPartnerLimit)))
	req := listReq{Page: page, Limit: limit, Cursor: c.Query("cursor"), SessionID: c.Query("session_id"), UseLoginLocation: true}
	list, hasMore, err := p.service.List(c.GetLoginUID(), req)
	if err != nil {
		p.Error("查询语伴推荐失败", zap.Error(err))
		c.ResponseError(errors.New("查询语伴推荐失败"))
		return
	}
	cursor := ""
	if hasMore == 1 {
		cursor = cursorToken()
	}
	c.JSON(http.StatusOK, ListResp{List: list, Users: list, Partners: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli(), SessionID: c.Query("session_id")})
}

func (p *Partners) nearby(c *wkhttp.Context) {
	lat, _ := strconv.ParseFloat(c.Query("lat"), 64)
	lng, _ := strconv.ParseFloat(c.Query("lng"), 64)
	if !validLatLng(lat, lng) {
		c.ResponseError(errors.New("定位参数无效"))
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultPartnerLimit)))
	radius, _ := strconv.Atoi(c.DefaultQuery("radius_meters", strconv.Itoa(NearbyRadiusMeters)))
	req := listReq{Page: page, Limit: limit, Cursor: c.Query("cursor"), SessionID: c.Query("session_id"), NearbyOnly: true, Radius: radius, Location: &locationModel{Lat: lat, Lng: lng}}
	list, hasMore, err := p.service.List(c.GetLoginUID(), req)
	if err != nil {
		p.Error("查询附近语伴失败", zap.Error(err))
		c.ResponseError(errors.New("查询附近语伴失败"))
		return
	}
	cursor := ""
	if hasMore == 1 {
		cursor = cursorToken()
	}
	c.JSON(http.StatusOK, ListResp{List: list, Users: list, Partners: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli(), SessionID: c.Query("session_id")})
}

func (p *Partners) profileMe(c *wkhttp.Context) {
	resp, err := p.service.ProfileMe(c.GetLoginUID())
	if err != nil {
		p.Error("查询语伴资料完整性失败", zap.Error(err))
		c.ResponseError(errors.New("查询语伴资料完整性失败"))
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (p *Partners) location(c *wkhttp.Context) {
	var req LocationReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	lat, lng := req.NormalizedLatLng()
	if !validLatLng(lat, lng) {
		c.ResponseError(errors.New("定位参数无效"))
		return
	}
	req.Lat = lat
	req.Lng = lng
	loc, err := p.service.SaveLocation(c.GetLoginUID(), req)
	if err != nil {
		p.Error("保存语伴定位失败", zap.Error(err))
		c.ResponseError(errors.New("保存语伴定位失败"))
		return
	}
	c.JSON(http.StatusOK, LocationResp{UID: loc.UID, Lat: loc.Lat, Lng: loc.Lng, Accuracy: loc.Accuracy, RadiusMeters: loc.RadiusMeters, ExpiresAt: loc.ExpiresAt, Source: loc.Source})
}

func (p *Partners) exposures(c *wkhttp.Context) {
	var req ExposureReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := p.service.RecordExposures(c.GetLoginUID(), req)
	if err != nil {
		p.Error("记录语伴曝光失败", zap.Error(err))
		c.ResponseError(errors.New("记录语伴曝光失败"))
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (p *Partners) greeting(c *wkhttp.Context) {
	var req GreetReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := p.service.RecordGreeting(c.GetLoginUID(), req)
	if err != nil {
		p.Warn("语伴打招呼失败", zap.Error(err), zap.String("to_uid", req.Target()))
		if errors.Is(err, ErrGreetingDuplicate) && resp != nil {
			c.JSON(http.StatusOK, resp)
			return
		}
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}
