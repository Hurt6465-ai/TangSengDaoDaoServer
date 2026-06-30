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
		feed.GET("/following", f.following)
		feed.GET("/user/:uid", f.userFeeds)
		feed.POST("/publish", f.publish)
		feed.POST("/follow", f.follow)
		feed.DELETE("/follow", f.unfollow)
		feed.GET("/:feed_id/comments", f.comments)
		feed.POST("/:feed_id/comments", f.addComment)
		feed.POST("/:feed_id/like", f.like)
		feed.POST("/:feed_id/share", f.share)
		feed.POST("/:feed_id/report", f.report)
		feed.POST("/:feed_id/event", f.event)
		feed.DELETE("/:feed_id", f.deleteFeed)
	}
}

func (f *Feed) recommend(c *wkhttp.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultFeedLimit)))
	mode := strings.TrimSpace(c.Query("mode"))
	var list []*FeedPost
	var hasMore int
	var err error
	if mode == "following" {
		list, hasMore, err = f.service.Following(c.GetLoginUID(), page, limit, c.Query("cursor"))
	} else {
		list, hasMore, err = f.service.Recommend(c.GetLoginUID(), page, limit, c.Query("cursor"))
	}
	if err != nil {
		f.Error("查询发现推荐失败", zap.Error(err))
		c.ResponseError(errors.New("查询发现推荐失败"))
		return
	}
	writeFeedList(c, list, hasMore, page, limit, c.Query("cursor"))
}

func (f *Feed) following(c *wkhttp.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultFeedLimit)))
	list, hasMore, err := f.service.Following(c.GetLoginUID(), page, limit, c.Query("cursor"))
	if err != nil {
		f.Error("查询关注发现失败", zap.Error(err))
		c.ResponseError(errors.New("查询关注发现失败"))
		return
	}
	writeFeedList(c, list, hasMore, page, limit, c.Query("cursor"))
}

func writeFeedList(c *wkhttp.Context, list []*FeedPost, hasMore int, page, limit int, cursorValue string) {
	offset := offsetFrom(page, cursorValue, limit)
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
	writeFeedList(c, list, hasMore, page, limit, c.Query("cursor"))
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

func (f *Feed) deleteFeed(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	if err := f.service.Delete(c.GetLoginUID(), feedID); err != nil {
		f.Error("删除动态失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.ResponseOK()
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

func (f *Feed) share(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	count, err := f.service.Share(c.GetLoginUID(), feedID)
	if err != nil {
		f.Error("分享失败", zap.Error(err))
		c.ResponseError(errors.New("分享失败"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "share_count": count})
}

func (f *Feed) report(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	var req ReportReq
	if c.Request != nil && c.Request.ContentLength > 0 {
		_ = c.BindJSON(&req)
	}
	if err := f.service.Report(c.GetLoginUID(), feedID, req); err != nil {
		f.Error("举报失败", zap.Error(err))
		c.ResponseError(errors.New("举报失败"))
		return
	}
	c.ResponseOK()
}

func (f *Feed) event(c *wkhttp.Context) {
	feedID := c.Param("feed_id")
	if strings.TrimSpace(feedID) == "" {
		c.ResponseError(errors.New("动态ID不能为空"))
		return
	}
	var req EventReq
	if c.Request != nil && c.Request.ContentLength > 0 {
		_ = c.BindJSON(&req)
	}
	if err := f.service.Event(c.GetLoginUID(), feedID, req); err != nil {
		f.Error("记录行为失败", zap.Error(err))
		c.ResponseError(errors.New("记录行为失败"))
		return
	}
	c.ResponseOK()
}

func (f *Feed) follow(c *wkhttp.Context) {
	var req FollowReq
	if c.Request != nil && c.Request.ContentLength > 0 {
		_ = c.BindJSON(&req)
	}
	targetUID := strings.TrimSpace(req.UID)
	if targetUID == "" {
		targetUID = strings.TrimSpace(req.FollowingUID)
	}
	if targetUID == "" {
		targetUID = strings.TrimSpace(c.Query("uid"))
	}
	if targetUID == "" {
		c.ResponseError(errors.New("关注用户不能为空"))
		return
	}
	if err := f.service.Follow(c.GetLoginUID(), targetUID); err != nil {
		f.Error("关注失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.ResponseOK()
}

func (f *Feed) unfollow(c *wkhttp.Context) {
	var req FollowReq
	if c.Request != nil && c.Request.ContentLength > 0 {
		_ = c.BindJSON(&req)
	}
	targetUID := strings.TrimSpace(req.UID)
	if targetUID == "" {
		targetUID = strings.TrimSpace(req.FollowingUID)
	}
	if targetUID == "" {
		targetUID = strings.TrimSpace(c.Query("uid"))
	}
	if targetUID == "" {
		c.ResponseError(errors.New("关注用户不能为空"))
		return
	}
	if err := f.service.Unfollow(c.GetLoginUID(), targetUID); err != nil {
		f.Error("取消关注失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.ResponseOK()
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
