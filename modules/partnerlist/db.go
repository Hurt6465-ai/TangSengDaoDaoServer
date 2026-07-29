package partnerlist

import (
	"errors"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/gocraft/dbr/v2"
)

type db struct {
	session *dbr.Session
	ctx     *config.Context
}

func newDB(ctx *config.Context) *db { return &db{session: ctx.DB(), ctx: ctx} }

func truncatePartnerListRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

const eligibleSQL = `pp.account_eligible=1 AND pp.partner_enabled=1 AND pp.profile_completed=1 AND pp.review_status=1
 AND pp.status=1 AND pp.has_photo=1
 AND u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')=''
 AND IFNULL(u.category,'') NOT IN ('system','customerService')
 AND IFNULL(pp.profile_images,'') NOT IN ('','[]','null')
 AND IFNULL(pp.native_languages,'') NOT IN ('','[]','null')
 AND IFNULL(pp.learning_languages,'') NOT IN ('','[]','null')`

func (d *db) eligibleProfilesBatch(afterUID string, limit int) ([]*poolProfile, error) {
	if limit <= 0 || limit > 5000 {
		limit = PoolScanBatchSize
	}
	var rows []*poolProfile
	_, err := d.session.SelectBySql(`SELECT pp.uid,pp.native_languages,pp.learning_languages,
 IFNULL(pp.last_active_at,0) last_active_at,IFNULL(pp.profile_completed_at,0) profile_completed_at_ms,
 IFNULL(pp.profile_score,0) profile_score,IFNULL(pp.intro,'') intro,IFNULL(pp.tags,'') tags,
 IFNULL(pp.country_code,'') country_code,IFNULL(pp.birthday,'') birthday
 FROM partner_profiles pp INNER JOIN user u ON u.uid=pp.uid
 WHERE pp.uid>? AND `+eligibleSQL+` ORDER BY pp.uid ASC LIMIT ?`, afterUID, limit).Load(&rows)
	return rows, err
}

func (d *db) viewer(uid string) (*viewerProfile, error) {
	var row struct {
		UID                  string `db:"uid"`
		NativeLanguagesRaw   string `db:"native_languages"`
		LearningLanguagesRaw string `db:"learning_languages"`
	}
	err := d.session.SelectBySql(`SELECT pp.uid,pp.native_languages,pp.learning_languages
 FROM partner_profiles pp INNER JOIN user u ON u.uid=pp.uid WHERE pp.uid=? AND `+eligibleSQL+` LIMIT 1`, uid).LoadOne(&row)
	if err != nil {
		return nil, err
	}
	native := parseStringList(row.NativeLanguagesRaw, 5)
	learning := parseStringList(row.LearningLanguagesRaw, 5)
	if len(native) == 0 || len(learning) == 0 {
		return nil, errors.New("请先完善语伴资料")
	}
	return &viewerProfile{UID: row.UID, NativeLanguages: native, LearningLanguages: learning, PrimaryLearning: learning[0]}, nil
}

func (d *db) poolProfile(uid string) (*poolProfile, error) {
	var rows []*poolProfile
	_, err := d.session.SelectBySql(`SELECT pp.uid,pp.native_languages,pp.learning_languages,
 IFNULL(pp.last_active_at,0) last_active_at,IFNULL(pp.profile_completed_at,0) profile_completed_at_ms,
 IFNULL(pp.profile_score,0) profile_score,IFNULL(pp.intro,'') intro,IFNULL(pp.tags,'') tags,
 IFNULL(pp.country_code,'') country_code,IFNULL(pp.birthday,'') birthday
 FROM partner_profiles pp INNER JOIN user u ON u.uid=pp.uid WHERE pp.uid=? AND `+eligibleSQL+` LIMIT 1`, uid).Load(&rows)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func (d *db) profilesByUIDs(viewerUID string, uids []string) ([]*ListUser, error) {
	uids = uniqueIDs(uids, 0)
	if len(uids) == 0 {
		return []*ListUser{}, nil
	}
	excluded, err := d.excludedUIDs(viewerUID, uids)
	if err != nil {
		return nil, err
	}
	out := make([]*ListUser, 0, len(uids))
	for start := 0; start < len(uids); start += CandidateChunkSize {
		end := start + CandidateChunkSize
		if end > len(uids) {
			end = len(uids)
		}
		var rows []*ListUser
		_, err = d.session.SelectBySql(`SELECT pp.uid,pp.name,pp.username,pp.sex,pp.birthday,pp.intro,pp.country_code,pp.country,
 pp.native_languages,pp.learning_languages,pp.tags,pp.profile_cover,pp.profile_images,pp.vercode,
 IFNULL(pp.online,0) online,IFNULL(pp.last_offline,0) last_offline,IFNULL(pp.last_active_at,0) last_active_at,
 IFNULL(pp.profile_score,0) profile_score,IFNULL(pp.profile_completed_at,0) profile_completed_at_ms
 FROM partner_profiles pp INNER JOIN user u ON u.uid=pp.uid
 WHERE pp.uid IN ? AND pp.uid<>? AND `+eligibleSQL, uids[start:end], viewerUID).Load(&rows)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row == nil {
				continue
			}
			if _, blocked := excluded[row.UID]; blocked {
				continue
			}
			row.normalize()
			out = append(out, row)
		}
	}
	return out, nil
}

func (d *db) excludedUIDs(viewerUID string, uids []string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	if viewerUID == "" || len(uids) == 0 {
		return out, nil
	}
	for start := 0; start < len(uids); start += CandidateChunkSize {
		end := start + CandidateChunkSize
		if end > len(uids) {
			end = len(uids)
		}
		chunk := uids[start:end]
		var rows []string
		_, err := d.session.SelectBySql(`SELECT target_uid FROM (
 SELECT to_uid AS target_uid FROM friend WHERE uid=? AND is_deleted=0 AND to_uid IN ?
 UNION ALL
 SELECT CASE WHEN uid=? THEN to_uid ELSE uid END AS target_uid FROM user_setting
   WHERE blacklist=1 AND ((uid=? AND to_uid IN ?) OR (to_uid=? AND uid IN ?))
 UNION ALL
 SELECT to_uid AS target_uid FROM partner_greetings WHERE uid=? AND to_uid IN ? AND IFNULL(send_status,1)<>2
 UNION ALL
 SELECT to_uid AS target_uid FROM partner_contacts WHERE uid=? AND to_uid IN ? AND status IN (0,1,2,3)
 UNION ALL
 SELECT channel_id AS target_uid FROM report WHERE uid=? AND channel_type=1 AND channel_id IN ?
 UNION ALL
 SELECT uid AS target_uid FROM report WHERE uid IN ? AND channel_type=1 AND channel_id=?
) excluded`, viewerUID, chunk,
			viewerUID, viewerUID, chunk, viewerUID, chunk,
			viewerUID, chunk,
			viewerUID, chunk,
			viewerUID, chunk,
			chunk, viewerUID).Load(&rows)
		if err != nil {
			return nil, err // safety exclusions fail closed
		}
		for _, uid := range rows {
			if uid = strings.TrimSpace(uid); uid != "" {
				out[uid] = struct{}{}
			}
		}
	}
	return out, nil
}

func (d *db) loadDay(viewerUID, dayKey string) (*recommendationDay, error) {
	var row *recommendationDay
	_, err := d.session.Select("id", "viewer_uid", "day_key", "algorithm_version", "pool_version", "first_served_at", "rotate_at", "rotation_retry_at", "rotation_done",
		"IFNULL(initial_candidate_ids,'[]') initial_candidate_ids", "IFNULL(current_candidate_ids,'[]') current_candidate_ids",
		"IFNULL(all_assigned_candidate_ids,'[]') all_assigned_candidate_ids", "IFNULL(rotated_in_ids,'[]') rotated_in_ids",
		"IFNULL(rotated_out_ids,'[]') rotated_out_ids", "IFNULL(abnormal_replacement_ids,'[]') abnormal_replacement_ids",
		"IFNULL(candidate_scores,'{}') candidate_scores", "unique_assigned_count", "list_version").From("partner_list_recommendation_day").
		Where("viewer_uid=? AND day_key=?", viewerUID, dayKey).Load(&row)
	return row, err
}

func enqueueAssignments(tx *dbr.Tx, day *recommendationDay, bucket string, ids []string, atMS int64) error {
	ids = uniqueIDs(ids, 0)
	if len(ids) == 0 {
		return nil
	}
	var sql strings.Builder
	sql.WriteString(`INSERT IGNORE INTO partner_list_assignment_outbox(viewer_uid,candidate_uid,day_key,bucket,assigned_at,status,attempts,next_retry_at,last_error,created_at,updated_at) VALUES `)
	args := make([]interface{}, 0, len(ids)*7)
	for i, candidateUID := range ids {
		if i > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("(?,?,?,?,?,0,0,0,'',?,?)")
		args = append(args, day.ViewerUID, candidateUID, day.DayKey, bucket, atMS, atMS, atMS)
	}
	_, err := tx.InsertBySql(sql.String(), args...).Exec()
	return err
}

func (d *db) insertDay(day *recommendationDay, bucket string, assigned []string, atMS int64) error {
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertBySql(`INSERT INTO partner_list_recommendation_day(
 viewer_uid,day_key,algorithm_version,pool_version,first_served_at,rotate_at,rotation_retry_at,rotation_done,
 initial_candidate_ids,current_candidate_ids,all_assigned_candidate_ids,rotated_in_ids,rotated_out_ids,
 abnormal_replacement_ids,candidate_scores,unique_assigned_count,list_version,created_at,updated_at)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),NOW())`,
		day.ViewerUID, day.DayKey, day.AlgorithmVersion, day.PoolVersion, day.FirstServedAt, day.RotateAt, day.RotationRetryAt, day.RotationDone,
		day.InitialCandidateIDsRaw, day.CurrentCandidateIDsRaw, day.AllAssignedCandidateIDsRaw, day.RotatedInIDsRaw,
		day.RotatedOutIDsRaw, day.AbnormalReplacementIDsRaw, day.CandidateScoresRaw, day.UniqueAssignedCount, day.ListVersion).Exec()
	if err != nil {
		return err
	}
	if err = enqueueAssignments(tx, day, bucket, assigned, atMS); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) updateDay(day *recommendationDay, expectedVersion int, bucket string, newlyAssigned []string, atMS int64) (bool, error) {
	tx, err := d.session.Begin()
	if err != nil {
		return false, err
	}
	defer tx.RollbackUnlessCommitted()
	result, err := tx.Update("partner_list_recommendation_day").
		Set("pool_version", day.PoolVersion).Set("rotate_at", day.RotateAt).Set("rotation_retry_at", day.RotationRetryAt).
		Set("rotation_done", day.RotationDone).Set("current_candidate_ids", day.CurrentCandidateIDsRaw).
		Set("all_assigned_candidate_ids", day.AllAssignedCandidateIDsRaw).Set("rotated_in_ids", day.RotatedInIDsRaw).
		Set("rotated_out_ids", day.RotatedOutIDsRaw).Set("abnormal_replacement_ids", day.AbnormalReplacementIDsRaw).
		Set("candidate_scores", day.CandidateScoresRaw).Set("unique_assigned_count", day.UniqueAssignedCount).
		Set("list_version", day.ListVersion).Set("updated_at", dbr.Expr("NOW()")).
		Where("viewer_uid=? AND day_key=? AND list_version=?", day.ViewerUID, day.DayKey, expectedVersion).Exec()
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return false, tx.Commit()
	}
	if err = enqueueAssignments(tx, day, bucket, newlyAssigned, atMS); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (d *db) recentDays(viewerUID string, limit int) ([]recentDayRow, error) {
	if limit <= 0 || limit > 10 {
		limit = 8
	}
	var rows []recentDayRow
	_, err := d.session.Select("day_key", "IFNULL(all_assigned_candidate_ids,'[]') all_assigned_candidate_ids").From("partner_list_recommendation_day").Where("viewer_uid=?", viewerUID).OrderDir("day_key", false).Limit(uint64(limit)).Load(&rows)
	return rows, err
}

func (d *db) inboxLoads(uids []string, sinceMS int64) (map[string]int, error) {
	out := map[string]int{}
	type row struct {
		UID   string `db:"uid"`
		Count int    `db:"cnt"`
	}
	for start := 0; start < len(uids); start += CandidateChunkSize {
		end := start + CandidateChunkSize
		if end > len(uids) {
			end = len(uids)
		}
		var rows []row
		_, err := d.session.SelectBySql(`SELECT uid,COUNT(*) cnt FROM partner_contacts WHERE uid IN ? AND status=0 AND requester_uid<>uid AND last_msg_at>=? GROUP BY uid`, uids[start:end], sinceMS).Load(&rows)
		if err != nil {
			return nil, err
		}
		for _, item := range rows {
			out[item.UID] += item.Count
		}
	}
	return out, nil
}

func (d *db) greetingUsed(uid, dayKey string) int {
	var count int
	if uid == "" || dayKey == "" {
		return 0
	}
	if err := d.session.Select("IFNULL(MAX(used_count),0)").From("partner_greeting_daily_usage").Where("sender_uid=? AND day_key=?", uid, dayKey).LoadOne(&count); err != nil || count < 0 {
		return 0
	}
	return count
}

func (d *db) touchActive(uid string, atMS int64) error {
	if uid == "" {
		return nil
	}
	_, err := d.session.UpdateBySql(`UPDATE partner_profiles SET last_active_at=GREATEST(IFNULL(last_active_at,0),?) WHERE uid=?`, atMS, uid).Exec()
	return err
}

func (d *db) imOnlineUIDs(uids []string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	for start := 0; start < len(uids); start += CandidateChunkSize {
		end := start + CandidateChunkSize
		if end > len(uids) {
			end = len(uids)
		}
		var rows []string
		if _, err := d.session.SelectBySql(`SELECT uid FROM user_online WHERE uid IN ? GROUP BY uid HAVING MAX(online)=1`, uids[start:end]).Load(&rows); err != nil {
			return nil, err
		}
		for _, uid := range rows {
			out[uid] = struct{}{}
		}
	}
	return out, nil
}

func (d *db) allIMOnlineUIDs() (map[string]struct{}, error) {
	out := map[string]struct{}{}
	var rows []string
	_, err := d.session.SelectBySql(`SELECT DISTINCT uid FROM user_online WHERE online=1`).Load(&rows)
	if err != nil {
		return nil, err
	}
	for _, uid := range rows {
		uid = strings.TrimSpace(uid)
		if uid != "" {
			out[uid] = struct{}{}
		}
	}
	return out, nil
}

type activityRow struct {
	UID          string `db:"uid"`
	LastActiveAt int64  `db:"last_active_at"`
}

func (d *db) activityByUIDs(uids []string) (map[string]int64, error) {
	out := map[string]int64{}
	uids = uniqueIDs(uids, 50)
	if len(uids) == 0 {
		return out, nil
	}
	var rows []activityRow
	_, err := d.session.SelectBySql(`SELECT uid,IFNULL(last_active_at,0) last_active_at FROM partner_profiles WHERE uid IN ?`, uids).Load(&rows)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		out[row.UID] = row.LastActiveAt
	}
	return out, nil
}

type changedPoolProfile struct {
	UID                  string  `db:"uid"`
	NativeLanguagesRaw   string  `db:"native_languages"`
	LearningLanguagesRaw string  `db:"learning_languages"`
	LastActiveAt         int64   `db:"last_active_at"`
	ProfileCompletedAtMS int64   `db:"profile_completed_at_ms"`
	ProfileScore         float64 `db:"profile_score"`
	Intro                string  `db:"intro"`
	TagsRaw              string  `db:"tags"`
	CountryCode          string  `db:"country_code"`
	Birthday             string  `db:"birthday"`
	Eligible             int     `db:"eligible"`
	HasPhoto             int     `db:"has_photo"`
	ProfileImagesRaw     string  `db:"profile_images"`
	UpdatedAtMS          int64   `db:"updated_at_ms"`
}

func (p *changedPoolProfile) nativeLanguages() []string {
	return parseStringList(p.NativeLanguagesRaw, 5)
}
func (p *changedPoolProfile) learningLanguages() []string {
	return parseStringList(p.LearningLanguagesRaw, 5)
}

func (d *db) changedProfilesAfter(cursorMS int64, cursorUID string, limit int) ([]*changedPoolProfile, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	var rows []*changedPoolProfile
	_, err := d.session.SelectBySql(`SELECT pp.uid,pp.native_languages,pp.learning_languages,IFNULL(pp.last_active_at,0) last_active_at,
 IFNULL(pp.profile_completed_at,0) profile_completed_at_ms,IFNULL(pp.profile_score,0) profile_score,IFNULL(pp.intro,'') intro,
 IFNULL(pp.tags,'') tags,IFNULL(pp.country_code,'') country_code,IFNULL(pp.birthday,'') birthday,
 IF(`+eligibleSQL+`,1,0) eligible,IFNULL(pp.has_photo,0) has_photo,IFNULL(pp.profile_images,'') profile_images,
 UNIX_TIMESTAMP(GREATEST(pp.updated_at,u.updated_at))*1000 updated_at_ms
 FROM partner_profiles pp INNER JOIN user u ON u.uid=pp.uid
 WHERE (UNIX_TIMESTAMP(GREATEST(pp.updated_at,u.updated_at))*1000>? OR (UNIX_TIMESTAMP(GREATEST(pp.updated_at,u.updated_at))*1000=? AND pp.uid>?))
 ORDER BY updated_at_ms ASC,pp.uid ASC LIMIT ?`, cursorMS, cursorMS, cursorUID, limit).Load(&rows)
	return rows, err
}

type assignmentOutboxRow struct {
	ID           int64  `db:"id"`
	ViewerUID    string `db:"viewer_uid"`
	CandidateUID string `db:"candidate_uid"`
	DayKey       string `db:"day_key"`
	Bucket       string `db:"bucket"`
	AssignedAt   int64  `db:"assigned_at"`
	Attempts     int    `db:"attempts"`
}

func (d *db) pendingAssignmentOutbox(nowMS int64, limit int) ([]assignmentOutboxRow, error) {
	var rows []assignmentOutboxRow
	_, err := d.session.SelectBySql(`SELECT id,viewer_uid,candidate_uid,day_key,bucket,assigned_at,attempts FROM partner_list_assignment_outbox WHERE status IN (0,2) AND next_retry_at<=? ORDER BY id ASC LIMIT ?`, nowMS, limit).Load(&rows)
	return rows, err
}
func (d *db) markAssignmentOutboxDoneBatch(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := d.session.Update("partner_list_assignment_outbox").Set("status", 1).Set("updated_at", time.Now().UnixMilli()).Where("id IN ?", ids).Exec()
	return err
}
func (d *db) markAssignmentOutboxRetry(id int64, attempts int, reason string) error {
	reason = truncatePartnerListRunes(reason, 500)
	delay := time.Duration(1<<minInt(attempts, 10)) * time.Second
	_, err := d.session.Update("partner_list_assignment_outbox").Set("status", 2).Set("attempts", attempts+1).Set("next_retry_at", time.Now().Add(delay).UnixMilli()).Set("last_error", reason).Set("updated_at", time.Now().UnixMilli()).Where("id=?", id).Exec()
	return err
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (d *db) cleanupOperationalRows(nowMS int64) error {
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	outboxCutoff := nowMS - int64(24*time.Hour/time.Millisecond)
	if _, err := d.session.DeleteBySql(`DELETE FROM partner_list_assignment_outbox WHERE status=1 AND updated_at<? ORDER BY updated_at ASC,id ASC LIMIT 10000`, outboxCutoff).Exec(); err != nil {
		return err
	}
	dayCutoff := recommendationDayKey(time.UnixMilli(nowMS).Add(-45 * 24 * time.Hour))
	_, err := d.session.DeleteBySql(`DELETE FROM partner_list_recommendation_day WHERE day_key<? ORDER BY day_key ASC,id ASC LIMIT 5000`, dayCutoff).Exec()
	return err
}

func (d *db) setPartnerEnabled(uid string, enabled int) error {
	if enabled != 1 {
		enabled = 0
	}
	_, err := d.session.UpdateBySql(`UPDATE partner_profiles
 SET partner_enabled=?,status=IF(account_eligible=1 AND ?=1 AND profile_completed=1 AND review_status=1,1,0),updated_at=NOW()
 WHERE uid=?`, enabled, enabled, uid).Exec()
	if err == nil && d.ctx != nil && d.ctx.GetRedisConn() != nil {
		_, _ = d.ctx.GetRedisConn().LPUSH(poolDirtyQueueKey, uid)
		_ = d.ctx.GetRedisConn().Expire(poolDirtyQueueKey, 24*time.Hour)
	}
	return err
}
