package dating

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/wkhttp"
	"go.uber.org/zap"
)

type Dating struct {
	ctx     *config.Context
	service *Service
	log.Log
}

func New(ctx *config.Context) *Dating {
	return &Dating{ctx: ctx, service: NewService(ctx), Log: log.NewTLog("dating")}
}

func (d *Dating) Route(r *wkhttp.WKHttp) {
	group := r.Group("/v1/dating", d.ctx.AuthMiddleware(r))
	{
		group.GET("/profile/me", d.profileMe)
		group.POST("/profile", d.saveProfile)
		group.POST("/profile/copy_partner", d.copyPartnerProfile)
		group.POST("/profile/enable", d.enableProfile)
		group.POST("/location", d.location)

		group.GET("/recommend", d.recommend)
		group.GET("/list", d.recommend)
		group.POST("/swipes", d.swipe)
		group.POST("/swipes/undo", d.undoSwipe)
		group.POST("/exposures", d.exposures)

		group.GET("/favorites", d.favorites)
		group.POST("/favorites/remove", d.removeFavorite)
		group.GET("/likes/received", d.receivedLikes)

		group.GET("/matches", d.matches)
		group.POST("/matches/:match_id/cancel", d.cancelMatch)
		group.GET("/chat/check", d.chatCheck)

		group.POST("/block", d.block)
		group.POST("/report", d.report)
	}
}

func (d *Dating) profileMe(c *wkhttp.Context) {
	resp, err := d.service.ProfileMe(c.GetLoginUID())
	if err != nil {
		d.Error("查询交友资料失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) saveProfile(c *wkhttp.Context) {
	var req SaveProfileReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.SaveProfile(c.GetLoginUID(), req)
	if err != nil {
		d.Warn("保存交友资料失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) copyPartnerProfile(c *wkhttp.Context) {
	resp, err := d.service.CopyPartnerProfile(c.GetLoginUID())
	if err != nil {
		d.Warn("同步共享资料失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) enableProfile(c *wkhttp.Context) {
	var req EnableProfileReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.EnableProfile(c.GetLoginUID(), req.Enabled)
	if err != nil {
		d.Warn("切换交友状态失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) location(c *wkhttp.Context) {
	var req LocationReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.SaveLocation(c.GetLoginUID(), req)
	if err != nil {
		d.Warn("保存交友定位失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) recommend(c *wkhttp.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(DefaultDatingLimit)))
	ageMin, _ := strconv.Atoi(c.DefaultQuery("age_min", "0"))
	ageMax, _ := strconv.Atoi(c.DefaultQuery("age_max", "0"))
	repeat, _ := strconv.Atoi(c.DefaultQuery("repeat", "0"))
	resp, err := d.service.Recommend(c.GetLoginUID(), RecommendReq{
		Limit: limit, Cursor: c.Query("cursor"), SessionID: c.Query("session_id"),
		Scope:       strings.TrimSpace(c.DefaultQuery("scope", DatingScopeGlobal)),
		CountryMode: strings.TrimSpace(c.Query("country_mode")), Gender: strings.TrimSpace(c.Query("gender")),
		AgeMin: ageMin, AgeMax: ageMax, Intent: strings.TrimSpace(c.Query("intent")), AllowRepeat: repeat == 1,
	})
	if err != nil {
		d.Warn("查询交友推荐失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) swipe(c *wkhttp.Context) {
	var req SwipeReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.Swipe(c.GetLoginUID(), req)
	if err != nil {
		d.Warn("记录交友滑动失败", zap.Error(err), zap.String("to_uid", req.Target()))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) undoSwipe(c *wkhttp.Context) {
	resp, err := d.service.UndoSwipe(c.GetLoginUID())
	if err != nil {
		d.Warn("撤回交友滑动失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) exposures(c *wkhttp.Context) {
	var req ExposureReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.RecordExposures(c.GetLoginUID(), req)
	if err != nil {
		d.Warn("记录交友曝光失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) favorites(c *wkhttp.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	resp, err := d.service.Favorites(c.GetLoginUID(), limit)
	if err != nil {
		d.Warn("查询交友收藏失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) removeFavorite(c *wkhttp.Context) {
	var req RemoveFavoriteReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.RemoveFavorite(c.GetLoginUID(), req)
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) receivedLikes(c *wkhttp.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "30"))
	// 当前没有会员系统，先只返回数量并锁定列表；接会员后把 reveal 改为权益判断。
	resp, err := d.service.ReceivedLikes(c.GetLoginUID(), limit, false)
	if err != nil {
		d.Warn("查询谁喜欢我失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) matches(c *wkhttp.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "30"))
	resp, err := d.service.Matches(c.GetLoginUID(), limit)
	if err != nil {
		d.Warn("查询交友匹配失败", zap.Error(err))
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) cancelMatch(c *wkhttp.Context) {
	resp, err := d.service.CancelMatch(c.GetLoginUID(), c.Param("match_id"))
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) chatCheck(c *wkhttp.Context) {
	resp, err := d.service.ChatCheck(c.GetLoginUID(), strings.TrimSpace(c.Query("to_uid")))
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) block(c *wkhttp.Context) {
	var req BlockReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.Block(c.GetLoginUID(), req)
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}

func (d *Dating) report(c *wkhttp.Context) {
	var req ReportReq
	if err := c.BindJSON(&req); err != nil {
		c.ResponseError(errors.New("请求数据格式有误"))
		return
	}
	resp, err := d.service.Report(c.GetLoginUID(), req)
	if err != nil {
		c.ResponseError(err)
		return
	}
	c.JSON(http.StatusOK, resp)
}
