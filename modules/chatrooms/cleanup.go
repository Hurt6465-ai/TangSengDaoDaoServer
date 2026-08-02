package chatrooms

import (
	"context"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/redisx"
	"github.com/TangSengDaoDao/TangSengDaoDaoServer/pkg/util"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/log"
	"go.uber.org/zap"
)

const (
	topicRoomCleanupLockKey    = "chatrooms:cleanup:lock"
	topicRoomCleanupLockTTL    = 15 * time.Minute
	topicRoomCleanupMaxBatches = 20

	topicRoomPurgeLockKey    = "chatrooms:purge:lock"
	topicRoomPurgeLockTTL    = 30 * time.Minute
	topicRoomPurgeMaxBatches = 10
	topicRoomPurgeRetention  = 72 * time.Hour
)

func StartCleanupLoop(ctx context.Context, svc *Service, interval time.Duration, limit uint64) {
	if svc == nil {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	if limit <= 0 {
		limit = 300
	}

	svc.workerWG.Add(1)
	go func() {
		defer svc.workerWG.Done()
		runTopicRoomCleanup(ctx, svc, limit)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runTopicRoomCleanup(ctx, svc, limit)
			}
		}
	}()
}

func runTopicRoomCleanup(ctx context.Context, svc *Service, limit uint64) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	client := redisx.FromContext(svc.ctx)
	if client == nil {
		log.Error("话题聊天室清理未执行：Redis客户端不可用")
		return
	}

	token := util.GenerUUID()
	locked, err := client.SetNX(topicRoomCleanupLockKey, token, topicRoomCleanupLockTTL)
	if err != nil {
		log.Error("获取话题聊天室清理锁失败", zap.Error(err))
		return
	}
	if !locked {
		return
	}
	defer func() {
		if _, err := client.CompareAndDelete(topicRoomCleanupLockKey, token); err != nil {
			log.Error("释放话题聊天室清理锁失败", zap.Error(err))
		}
	}()

	for batch := 0; batch < topicRoomCleanupMaxBatches; batch++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		count, err := svc.CleanupExpired(limit)
		if err != nil {
			log.Error("清理过期话题聊天室失败", zap.Error(err))
			return
		}
		if count < int(limit) {
			return
		}
	}
}

// StartPurgeLoop permanently removes topic-room business records after the
// soft-deletion retention period. It is intentionally independent from the
// one-minute expiration loop because physical deletion is heavier and does not
// need sub-minute precision.
func StartPurgeLoop(ctx context.Context, svc *Service, interval time.Duration, limit uint64) {
	if svc == nil {
		return
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if limit <= 0 {
		limit = 200
	}

	svc.workerWG.Add(1)
	go func() {
		defer svc.workerWG.Done()
		runTopicRoomPurge(ctx, svc, limit)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runTopicRoomPurge(ctx, svc, limit)
			}
		}
	}()
}

func runTopicRoomPurge(ctx context.Context, svc *Service, limit uint64) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	client := redisx.FromContext(svc.ctx)
	if client == nil {
		log.Error("话题聊天室物理清理未执行：Redis客户端不可用")
		return
	}

	token := util.GenerUUID()
	locked, err := client.SetNX(topicRoomPurgeLockKey, token, topicRoomPurgeLockTTL)
	if err != nil {
		log.Error("获取话题聊天室物理清理锁失败", zap.Error(err))
		return
	}
	if !locked {
		return
	}
	defer func() {
		if _, err := client.CompareAndDelete(topicRoomPurgeLockKey, token); err != nil {
			log.Error("释放话题聊天室物理清理锁失败", zap.Error(err))
		}
	}()

	for batch := 0; batch < topicRoomPurgeMaxBatches; batch++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		count, err := svc.PurgeDeleted(topicRoomPurgeRetention, limit)
		if err != nil {
			log.Error("物理清理过期话题聊天室失败", zap.Error(err))
			return
		}
		if count < int(limit) {
			return
		}
	}
}
