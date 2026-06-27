package feed

import "github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"

type Service struct {
	ctx *config.Context
	db  *db
}

func NewService(ctx *config.Context) *Service {
	return &Service{ctx: ctx, db: newDB(ctx)}
}

func (s *Service) Recommend(uid string, page, limit int, cursor string) ([]*FeedPost, int, error) {
	list, hasMore, err := s.db.listRecommend(uid, page, limit, cursor)
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

func (s *Service) ToggleLike(uid, feedID string) (int, int, error) {
	return s.db.toggleLike(uid, feedID)
}

func (s *Service) AddComment(uid, feedID string, req CommentReq) (*FeedComment, error) {
	return s.db.addComment(uid, feedID, req)
}

func (s *Service) Comments(loginUID, feedID string, page, limit int, cursor string) ([]*FeedComment, int, error) {
	return s.db.comments(loginUID, feedID, page, limit, cursor)
}
