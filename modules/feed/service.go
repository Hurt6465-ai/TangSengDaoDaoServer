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
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		return s.listCachedCandidates(uid, "discover", page, limit, cursor, feedCandidateTTL, func(candidateLimit int) ([]string, error) {
			return s.db.listRecommendCandidateIDs(uid, candidateLimit)
		})
	}
	// Redis disabled fallback: still do not mark returned items as exposed; Android reports real expose events.
	return s.db.listRecommend(uid, page, limit, cursor)
}

func (s *Service) Following(uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	if s.ctx != nil && s.ctx.GetRedisConn() != nil {
		return s.listCachedCandidates(uid, "following", page, limit, cursor, feedFollowingTTL, func(candidateLimit int) ([]string, error) {
			return s.db.listFollowingCandidateIDs(uid, candidateLimit)
		})
	}
	return s.db.listFollowing(uid, page, limit, cursor)
}

func (s *Service) UserFeeds(loginUID, uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	return s.db.listByUser(loginUID, uid, page, limit, cursor)
}

func (s *Service) Publish(uid string, req PublishReq) (*FeedPost, error) {
	post, err := s.db.createPost(uid, req)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return post, err
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

func (s *Service) SetLike(uid, feedID string, desired *bool) (int, int, error) {
	if strings.TrimSpace(uid) == "" {
		return 0, 0, errors.New("未登录")
	}
	liked, count, err := s.db.setLike(uid, feedID, desired)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return liked, count, err
}

func (s *Service) Share(uid, feedID string) (int, error) {
	count, err := s.db.share(uid, feedID)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return count, err
}

func (s *Service) Report(uid, feedID string, req ReportReq) error {
	err := s.db.report(uid, feedID, req)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return err
}

func (s *Service) Event(uid, feedID string, req EventReq) error {
	err := s.db.event(uid, feedID, req)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return err
}

func (s *Service) Follow(uid, targetUID string) error {
	if uid == "" {
		return errors.New("未登录")
	}
	if targetUID == "" || uid == targetUID {
		return errors.New("关注用户无效")
	}
	err := s.db.follow(uid, targetUID)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return err
}

func (s *Service) Unfollow(uid, targetUID string) error {
	if uid == "" {
		return errors.New("未登录")
	}
	if targetUID == "" || uid == targetUID {
		return errors.New("关注用户无效")
	}
	err := s.db.unfollow(uid, targetUID)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return err
}

func (s *Service) AddComment(uid, feedID string, req CommentReq) (*FeedComment, error) {
	comment, err := s.db.addComment(uid, feedID, req)
	if err == nil {
		s.invalidateCandidateCache(uid)
	}
	return comment, err
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
