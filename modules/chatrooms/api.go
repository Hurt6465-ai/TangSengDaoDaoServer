package chatrooms

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
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

	workerCancel context.CancelFunc
	startOnce    sync.Once
	stopOnce     sync.Once
}

func New(ctx *config.Context) *Chatrooms {
	return &Chatrooms{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("chatrooms")}
}

func (cr *Chatrooms) Start() error {
	cr.startOnce.Do(func() {
		workerCtx, cancel := context.WithCancel(context.Background())
		cr.workerCancel = cancel
		cr.service.Start(workerCtx)
		StartCleanupLoop(workerCtx, cr.service, time.Minute, 300)
		StartPurgeLoop(workerCtx, cr.service, time.Hour, 200)
	})
	return nil
}

func (cr *Chatrooms) Stop() error {
	cr.stopOnce.Do(func() {
		if cr.workerCancel != nil {
			cr.workerCancel()
		}
		cr.service.WaitWorkers()
		cr.service.FlushPendingLastReplies()
	})
	return nil
}

func (cr *Chatrooms) Route(r *wkhttp.WKHttp) {
	room := r.Group("/v1/chatrooms", cr.ctx.AuthMiddleware(r))
	{
		room.GET("/list", cr.list)
		room.POST("/create", cr.create)
		room.POST("/enter", cr.enter)
		room.POST("/read", cr.read)
		room.POST("/pin", cr.pin)
		room.POST("/delete", cr.delete)
	}
}

func (cr *Chatrooms) list(c *wkhttp.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultRoomListLimit)))
	rooms, cursor, hasMore, err := cr.service.List(c.GetLoginUID(), isManager(c), RoomListReq{Limit: limit, Cursor: c.Query("cursor")})
	if err != nil {
		cr.Error("查询话题聊天室列表失败", zap.Error(err))
		c.ResponseError(errors.New("查询话题聊天室列表失败"))
		return
	}
	c.JSON(http.StatusOK, ListResp{Rooms: rooms, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
}

func (cr *Chatrooms) create(c *wkhttp.Context) {
	var req CreateReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.Create(req, c.GetLoginUID(), isManager(c))
	if err != nil {
		cr.Error("创建话题聊天室失败", zap.Error(err))
		respondChatroomError(c, err)
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
	room, err := cr.service.Enter(req, c.GetLoginUID(), isManager(c))
	if err != nil {
		cr.Error("进入话题聊天室失败", zap.Error(err))
		respondChatroomError(c, err)
		return
	}
	c.JSON(http.StatusOK, room)
}

func (cr *Chatrooms) read(c *wkhttp.Context) {
	var req RoomReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	room, err := cr.service.Read(req, c.GetLoginUID(), isManager(c))
	if err != nil {
		cr.Error("标记话题聊天室已读失败", zap.Error(err))
		respondChatroomError(c, err)
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
	room, err := cr.service.Pin(req, c.GetLoginUID(), isManager(c))
	if err != nil {
		cr.Error("置顶话题聊天室失败", zap.Error(err))
		respondChatroomError(c, err)
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
	if err := cr.service.Delete(req, c.GetLoginUID(), isManager(c)); err != nil {
		cr.Error("删除话题聊天室失败", zap.Error(err))
		respondChatroomError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func isManager(c *wkhttp.Context) bool {
	role := c.GetLoginRole()
	return role == string(wkhttp.Admin) || role == string(wkhttp.SuperAdmin)
}

func respondChatroomError(c *wkhttp.Context, err error) {
	switch {
	case errors.Is(err, ErrPermissionDenied):
		c.JSON(http.StatusForbidden, gin.H{"msg": err.Error(), "status": http.StatusForbidden})
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrRoomExpired):
		c.JSON(http.StatusGone, gin.H{"msg": "该话题已结束", "status": http.StatusGone})
	default:
		c.ResponseError(err)
	}
}
