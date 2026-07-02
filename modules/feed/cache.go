package feed

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	feedCandidateCacheVersion = "v2"
	feedCandidateLimit        = 260
	feedCandidateTTL          = 45 * time.Second
	feedFollowingTTL          = 30 * time.Second
)

func (s *Service) listCachedCandidates(loginUID string, mode string, page, limit int, cursor string, ttl time.Duration, loader func(int) ([]string, error)) ([]*FeedPost, int, error) {
	limit = clampLimit(limit)
	offset := offsetFrom(page, cursor, limit)
	ids, ok := s.getCandidateCache(loginUID, mode)
	if !ok || len(ids) == 0 || offset+limit > len(ids) {
		loaded, err := loader(feedCandidateLimit)
		if err != nil {
			return nil, 0, err
		}
		ids = loaded
		if len(ids) > 0 {
			s.setCandidateCache(loginUID, mode, ids, ttl)
		}
	}
	if offset >= len(ids) {
		return []*FeedPost{}, 0, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	pageIDs := ids[offset:end]
	posts, err := s.db.listByFeedIDs(loginUID, pageIDs)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if end < len(ids) {
		hasMore = 1
	}
	return posts, hasMore, nil
}

func (s *Service) getCandidateCache(loginUID string, mode string) ([]string, bool) {
	if s == nil || s.ctx == nil || s.ctx.GetRedisConn() == nil {
		return nil, false
	}
	raw, err := s.ctx.GetRedisConn().GetString(s.candidateCacheKey(loginUID, mode))
	if err != nil || strings.TrimSpace(raw) == "" {
		return nil, false
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil || len(ids) == 0 {
		return nil, false
	}
	return ids, true
}

func (s *Service) setCandidateCache(loginUID string, mode string, ids []string, ttl time.Duration) {
	if s == nil || s.ctx == nil || s.ctx.GetRedisConn() == nil || len(ids) == 0 {
		return
	}
	content, err := json.Marshal(ids)
	if err != nil {
		return
	}
	if ttl <= 0 {
		ttl = feedCandidateTTL
	}
	_ = s.ctx.GetRedisConn().SetAndExpire(s.candidateCacheKey(loginUID, mode), string(content), ttl)
}

func (s *Service) invalidateCandidateCache(loginUID string) {
	if s == nil || s.ctx == nil || s.ctx.GetRedisConn() == nil || strings.TrimSpace(loginUID) == "" {
		return
	}
	_ = s.ctx.GetRedisConn().Del(s.candidateCacheKey(loginUID, "discover"))
	_ = s.ctx.GetRedisConn().Del(s.candidateCacheKey(loginUID, "following"))
}

func (s *Service) candidateCacheKey(loginUID string, mode string) string {
	loginUID = strings.TrimSpace(loginUID)
	if loginUID == "" {
		loginUID = "anon"
	}
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		mode = "discover"
	}
	return fmt.Sprintf("feed:candidates:%s:%s:%s", feedCandidateCacheVersion, mode, loginUID)
}
