package chatrooms

import (
	"errors"
	"net/http"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Chatrooms struct {
	ctx     *config.Context
	service *Service
	log.Log
}

func New(ctx *config.Context) *Chatrooms {
	return &Chatrooms{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("chatrooms")}
}

func (cr *Chatrooms) Route(r *wkhttp.WKHttp) {
	room := r.Group("/v1/chatrooms", cr.ctx.AuthMiddleware(r))
	{
		room.GET("/list", cr.list)
		room.POST("/create", cr.create)
		room.POST("/enter", cr.enter)
		room.POST("/pin", cr.pin)
		room.POST("/delete", cr.delete)
		room.POST("/message-hook", cr.messageHook)
		room.POST("/im/webhook", cr.messageHook)
	}
}

func (cr *Chatrooms) list(c *wkhttp.Context) {
	rooms, err := cr.service.List(c.GetLoginUID())
	if err != nil {
		cr.Error("查询话题聊天室列表失败", zap.Error(err))
		c.ResponseError(errors.New("查询话题聊天室列表失败"))
		return
	}
	c.JSON(http.StatusOK, ListResp{Rooms: rooms, ServerTime: time.Now().UnixMilli()})
}

func (cr *Chatrooms) create(c *wkhttp.Context) {
	var req CreateReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.Create(req, c.GetLoginUID())
	if err != nil {
		cr.Error("创建话题聊天室失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, room)
}

func (cr *Chatrooms) enter(c *wkhttp.Context) {
	var req RoomReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.Enter(req, c.GetLoginUID())
	if err != nil {
		cr.Error("进入话题聊天室失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, room)
}

func (cr *Chatrooms) pin(c *wkhttp.Context) {
	var req RoomReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.Pin(req)
	if err != nil {
		cr.Error("置顶话题聊天室失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, room)
}

func (cr *Chatrooms) delete(c *wkhttp.Context) {
	var req RoomReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	if err := cr.service.Delete(req); err != nil {
		cr.Error("删除话题聊天室失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (cr *Chatrooms) messageHook(c *wkhttp.Context) {
	var req MessageWebhookReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.MessageWebhook(&req)
	if err != nil {
		cr.Error("更新话题最后回复失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, room)
}
