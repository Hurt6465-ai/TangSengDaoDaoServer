package feed

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type Feed struct {
	ctx     *config.Context
	service *Service
	log.Log
}

func New(ctx *config.Context) *Feed {
	return &Feed{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("feed")}
}

func (f *Feed) Route(r *wkhttp.WKHttp) {
	feed := r.Group("/v1/feed", f.ctx.AuthMiddleware(r))
	{
		feed.GET("/recommend", f.recommend)
		feed.GET("/user/:uid", f.userFeeds)
		feed.POST("/publish", f.publish)
		feed.GET("/:feed_id/comments", f.comments)
		feed.POST("/:feed_id/comments", f.addComment)
		feed.POST("/:feed_id/like", f.like)
	}
}

func (f *Feed) recommend(c *wkhttp.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultFeedLimit)))
	list, hasMore, err := f.service.Recommend(c.GetLoginUID(), page, limit, c.Query("cursor"))
	if err != nil {
		f.Error("查询发现推荐失败", zap.Error(err))
		c.ResponseError(errors.New("查询发现推荐失败"))
		return
	}
	offset := offsetFrom(page, c.Query("cursor"), limit)
	cursor := ""
	if hasMore == 1 {
		cursor = strconv.Itoa(offset + len(list))
	}
	c.JSON(http.StatusOK, ListResp{List: list, Feeds: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
}

func (f *Feed) userFeeds(c *wkhttp.Context) {
	uid := c.Param("uid")
	if strings.TrimSpace(uid) == "" {
		c.ResponseError(errors.New("用户ID不能为空"))
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultFeedLimit)))
	list, hasMore, err := f.service.UserFeeds(c.GetLoginUID(), uid, page, limit, c.Query("cursor"))
	if err != nil {
		f.Error("查询用户发现失败", zap.Error(err))
		c.ResponseError(errors.New("查询用户发现失败"))
		return
	}
	offset := offsetFrom(page, c.Query("cursor"), limit)
	cursor := ""
	if hasMore == 1 {
		cursor = strconv.Itoa(offset + len(list))
	}
	c.JSON(http.StatusOK, ListResp{List: list, Feeds: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
}

func (f *Feed) publish(c *wkhttp.Context) {
	var req PublishReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	post, err := f.service.Publish(c.GetLoginUID(), req)
	if err != nil {
		f.Error("发布发现失败", zap.Error(err))
		c.ResponseError(errors.New("发布发现失败"))
		return
	}
	c.JSON(http.StatusOK, post)
}

func (f *Feed) like(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	liked, count, err := f.service.ToggleLike(c.GetLoginUID(), feedID)
	if err != nil {
		f.Error("点赞失败", zap.Error(err))
		c.ResponseError(errors.New("点赞失败"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "liked": liked, "like_count": count})
}

func (f *Feed) addComment(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	var req CommentReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	comment, err := f.service.AddComment(c.GetLoginUID(), feedID, req)
	if err != nil {
		f.Error("评论失败", zap.Error(err))
		c.ResponseError(errors.New("评论失败"))
		return
	}
	if comment == nil {
		c.ResponseError(errors.New("评论内容不能为空"))
		return
	}
	c.JSON(http.StatusOK, comment)
}

func (f *Feed) comments(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "30"))
	list, hasMore, err := f.service.Comments(c.GetLoginUID(), feedID, page, limit, c.Query("cursor"))
	if err != nil {
		f.Error("查询评论失败", zap.Error(err))
		c.ResponseError(errors.New("查询评论失败"))
		return
	}
	offset := offsetFrom(page, c.Query("cursor"), limit)
	cursor := ""
	if hasMore == 1 {
		cursor = strconv.Itoa(offset + len(list))
	}
	c.JSON(http.StatusOK, CommentListResp{List: list, Comments: list, Cursor: cursor, HasMore: hasMore, ServerTime: time.Now().UnixMilli()})
}
