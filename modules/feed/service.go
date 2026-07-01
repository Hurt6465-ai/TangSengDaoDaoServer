package feed

import (
	"errors"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"go.uber.org/zap"
)

type Service struct {
	ctx *config.Context
	db  *db
	log.Log
}

func NewService(ctx *config.Context) *Service {
	return &Service{ctx: ctx, db: newDB(ctx), Log: log.NewTLog("feedService")}
}

func (s *Service) StartMaintenanceLoop() {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.Warn("发现定时维护退出", zap.Any("recover", r))
			}
		}()
		// 启动 2 分钟后先跑一次轻量统计，避免新部署后 score 长时间为空。
		timer := time.NewTimer(2 * time.Minute)
		<-timer.C
		s.RunDailyMaintenance()
		for {
			timer.Reset(delayUntilLocal(3, 30))
			<-timer.C
			s.RunDailyMaintenance()
		}
	}()
}

func delayUntilLocal(hour, minute int) time.Duration {
	now := time.Now()
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next.Sub(now)
}

func (s *Service) RunDailyMaintenance() {
	if err := s.RebuildRecommendScores(); err != nil {
		s.Warn("重建发现推荐分失败", zap.Error(err))
	}
	s.CleanupExpiredVideos()
	s.CleanupOldEvents()
}

func (s *Service) RebuildRecommendScores() error {
	return s.db.rebuildRecommendStats()
}

func (s *Service) Recommend(uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	list, hasMore, err := s.db.listRecommend(uid, page, limit, cursor)
	if err != nil {
		return nil, 0, err
	}
	s.db.recordExposure(uid, list)
	return list, hasMore, nil
}

func (s *Service) Following(uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	list, hasMore, err := s.db.listFollowing(uid, page, limit, cursor)
	if err != nil {
		return nil, 0, err
	}
	s.db.recordExposure(uid, list)
	return list, hasMore, nil
}

func (s *Service) UserFeeds(loginUID, uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	return s.db.listByUser(loginUID, uid, page, limit, cursor)
}

func (s *Service) Publish(uid string, req PublishReq) (*FeedPost, error) {
	return s.db.createPost(uid, req)
}

func (s *Service) Delete(uid, feedID string) error {
	if strings.TrimSpace(uid) == "" {
		return errors.New("未登录")
	}
	paths, err := s.db.deletePost(uid, feedID)
	if err != nil {
		return err
	}
	go s.deleteFilesBestEffort(paths)
	return nil
}

func (s *Service) ToggleLike(uid, feedID string) (int, int, error) {
	return s.db.toggleLike(uid, feedID)
}

func (s *Service) Share(uid, feedID string) (int, error) {
	return s.db.share(uid, feedID)
}

func (s *Service) Report(uid, feedID string, req ReportReq) error {
	return s.db.report(uid, feedID, req)
}

func (s *Service) Event(uid, feedID string, req EventReq) error {
	return s.db.event(uid, feedID, req)
}

func (s *Service) Follow(uid, targetUID string) error {
	if uid == "" {
		return errors.New("未登录")
	}
	if targetUID == "" || uid == targetUID {
		return errors.New("关注用户无效")
	}
	return s.db.follow(uid, targetUID)
}

func (s *Service) Unfollow(uid, targetUID string) error {
	if uid == "" {
		return errors.New("未登录")
	}
	if targetUID == "" || uid == targetUID {
		return errors.New("关注用户无效")
	}
	return s.db.unfollow(uid, targetUID)
}

func (s *Service) AddComment(uid, feedID string, req CommentReq) (*FeedComment, error) {
	return s.db.addComment(uid, feedID, req)
}

func (s *Service) Comments(loginUID, feedID string, page, limit int, cursor string) ([]*FeedComment, int, error) {
	return s.db.comments(loginUID, feedID, page, limit, cursor)
}

func (s *Service) CleanupExpiredVideos() {
	cutoff := time.Now().Add(-FeedVideoTTL).UnixMilli()
	for {
		items, err := s.db.expiredVideoPosts(cutoff, 50)
		if err != nil {
			s.Warn("查询过期视频失败", zap.Error(err))
			return
		}
		if len(items) == 0 {
			return
		}
		for _, item := range items {
			if item == nil || item.FeedID == "" {
				continue
			}
			paths, err := s.db.hardDeletePost(item.FeedID)
			if err != nil {
				s.Warn("清理过期视频失败", zap.String("feed_id", item.FeedID), zap.Error(err))
				continue
			}
			s.deleteFilesBestEffort(paths)
		}
	}
}

func (s *Service) CleanupOldEvents() {
	cutoff := time.Now().Add(-FeedEventTTL)
	for i := 0; i < 20; i++ {
		affected, err := s.db.deleteOldEvents(cutoff, 1000)
		if err != nil {
			s.Warn("清理发现行为事件失败", zap.Error(err))
			return
		}
		if affected <= 0 {
			return
		}
	}
}
