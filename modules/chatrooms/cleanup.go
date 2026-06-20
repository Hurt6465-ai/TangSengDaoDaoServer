package chatrooms

import (
	"context"
	"time"
)

func StartCleanupLoop(ctx context.Context, svc *Service, interval time.Duration, limit uint64) {
	if interval <= 0 {
		interval = time.Minute
	}
	if limit <= 0 {
		limit = 100
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = svc.CleanupExpired(limit)
			}
		}
	}()
}
