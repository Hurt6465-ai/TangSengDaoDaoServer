package partners

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/gin-gonic/gin"
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
		partners.GET("/nearby", p.nearby)
		partners.POST("/location", p.location)
		partners.POST("/greet", p.greet)
	}
}

func (p *Partners) list(c *wkhttp.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultPartnerLimit)))
	req := listReq{Page: page, Limit: limit, Cursor: c.Query("cursor"), UseLoginLocation: true}
	list, hasMore, err := p.service.List(c.GetLoginUID(), req)
	if err != nil {
		p.Error("查询语伴推荐失败", zap.Error(err))
		c.ResponseError(errors.New("查询语伴推荐失败"))
		return
	}
	cursor := ""
	if hasMore == 1 {
		cursor = strconv.Itoa(req.Offset() + len(list))
	}
	c.JSON(http.StatusOK, ListResp{List: list, Users: list, Partners: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
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
	req := listReq{Page: page, Limit: limit, Cursor: c.Query("cursor"), NearbyOnly: true, Radius: radius, Location: &locationModel{Lat: lat, Lng: lng}}
	list, hasMore, err := p.service.List(c.GetLoginUID(), req)
	if err != nil {
		p.Error("查询附近语伴失败", zap.Error(err))
		c.ResponseError(errors.New("查询附近语伴失败"))
		return
	}
	cursor := ""
	if hasMore == 1 {
		cursor = strconv.Itoa(req.Offset() + len(list))
	}
	c.JSON(http.StatusOK, ListResp{List: list, Users: list, Partners: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
}

func (p *Partners) location(c *wkhttp.Context) {
	var req LocationReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	if !validLatLng(req.Lat, req.Lng) {
		c.ResponseError(errors.New("定位参数无效"))
		return
	}
	loc, err := p.service.SaveLocation(c.GetLoginUID(), req.Lat, req.Lng)
	if err != nil {
		p.Error("保存语伴定位失败", zap.Error(err))
		c.ResponseError(errors.New("保存语伴定位失败"))
		return
	}
	c.JSON(http.StatusOK, LocationResp{UID: loc.UID, Lat: loc.Lat, Lng: loc.Lng, ExpiresAt: loc.ExpiresAt})
}

func (p *Partners) greet(c *wkhttp.Context) {
	var req GreetReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	if req.ToUID == "" || req.ToUID == c.GetLoginUID() {
		c.ResponseError(errors.New("语伴ID无效"))
		return
	}
	if err := p.service.RecordGreeting(c.GetLoginUID(), req.ToUID); err != nil {
		p.Error("记录打招呼失败", zap.Error(err))
		c.ResponseError(errors.New("记录打招呼失败"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK})
}
