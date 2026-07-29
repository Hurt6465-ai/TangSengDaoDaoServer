package partnerlist

import "fmt"

const (
	poolCurrentVersionKey        = "partnerlist:pool:current_version"
	poolBuildLockKey             = "partnerlist:pool:build_lock"
	poolBuildingVersionKey       = "partnerlist:pool:building_version"
	lastActiveKey                = "partnerlist:last_active"
	foregroundOnlineKey          = "partnerlist:foreground_online"
	poolDirtyQueueKey            = "partnerlist:pool:dirty_queue"
	poolChangeCursorKey          = "partnerlist:pool:change_cursor"
	assignmentOutboxFlushLockKey = "partnerlist:assignment:outbox_flush_lock"
	profileFoldLockKey           = "partnerlist:pool:profile_fold_lock"
	presenceCleanupLockKey       = "partnerlist:presence:cleanup_lock"
	maintenanceCleanupLockKey    = "partnerlist:maintenance:cleanup_lock"
	activeWriteSpillKey          = "partnerlist:active_write:spill"
)

func poolEligibleKey(version string) string {
	return fmt.Sprintf("partnerlist:pool:%s:eligible", version)
}
func poolNativeKey(version, language string) string {
	return fmt.Sprintf("partnerlist:pool:%s:native:%s", version, language)
}
func poolLearningKey(version, language string) string {
	return fmt.Sprintf("partnerlist:pool:%s:learning:%s", version, language)
}
func poolNewKey(version string) string  { return fmt.Sprintf("partnerlist:pool:%s:new", version) }
func poolMetaKey(version string) string { return fmt.Sprintf("partnerlist:pool:%s:meta", version) }
func poolKeysKey(version string) string { return fmt.Sprintf("partnerlist:pool:%s:keys", version) }
func poolMembershipKey(version, uid string) string {
	return fmt.Sprintf("partnerlist:pool:%s:membership:%s", version, uid)
}
func recommendationCacheKey(dayKey, uid string) string {
	return fmt.Sprintf("partnerlist:recommend:%s:%s", dayKey, uid)
}
func generationLockKey(dayKey, uid string) string {
	return fmt.Sprintf("partnerlist:recommend:lock:%s:%s", dayKey, uid)
}
func rotationLockKey(dayKey, uid string) string {
	return fmt.Sprintf("partnerlist:rotate:lock:%s:%s", dayKey, uid)
}
func viewerSeenKey(uid string) string { return fmt.Sprintf("partnerlist:viewer_seen:%s", uid) }
func assignmentGlobalKey(dayKey string) string {
	return fmt.Sprintf("partnerlist:assignment:{%s}:global", dayKey)
}
func assignmentBucketKey(dayKey, bucket string) string {
	return fmt.Sprintf("partnerlist:assignment:{%s}:bucket:%s", dayKey, bucket)
}
func assignmentAppliedKey(dayKey string) string {
	return fmt.Sprintf("partnerlist:assignment:{%s}:applied", dayKey)
}

func activeWriteLockKey(uid string) string { return fmt.Sprintf("partnerlist:active:write:%s", uid) }
