package partners

import "github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"

type Service struct {
	ctx *config.Context
	db  *db
}

func NewService(ctx *config.Context) *Service {
	return &Service{ctx: ctx, db: newDB(ctx)}
}

func (s *Service) List(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	list, hasMore, err := s.db.list(loginUID, req)
	if err != nil {
		return nil, 0, err
	}
	RankPartners(list, loginUID, req.Page)
	s.db.recordExposure(loginUID, list)
	return list, hasMore, nil
}

func (s *Service) SaveLocation(uid string, lat, lng float64) (*locationModel, error) {
	return s.db.upsertLocation(uid, lat, lng)
}

func (s *Service) RecordGreeting(uid, toUID string) error {
	return s.db.recordGreeting(uid, toUID)
}
