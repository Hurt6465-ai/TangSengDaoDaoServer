package partners

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/gocraft/dbr/v2"
)

type db struct {
	session *dbr.Session
	ctx     *config.Context
}

func newDB(ctx *config.Context) *db {
	return &db{session: ctx.DB(), ctx: ctx}
}

const greetingRollbackMarker = "__rollback_done__:"

func truncatePartnerRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

type partnerContactModel struct {
	UID               string `db:"uid"`
	ToUID             string `db:"to_uid"`
	RequesterUID      string `db:"requester_uid"`
	Status            int    `db:"status"`
	RequesterMsgCount int    `db:"requester_msg_count"`
	LastMsgAt         int64  `db:"last_msg_at"`
}

type locationModel struct {
	UID          string  `db:"uid"`
	Lat          float64 `db:"lat"`
	Lng          float64 `db:"lng"`
	Accuracy     float64 `db:"accuracy"`
	RadiusMeters int     `db:"radius_meters"`
	Geohash      string  `db:"geohash"`
	UpdatedAt    int64   `db:"updated_at_ms"`
	ExpiresAt    int64   `db:"expires_at"`
	Source       string  `db:"source"`
}

func (d *db) upsertLocation(uid string, req LocationReq) (*locationModel, error) {
	lat, lng := req.NormalizedLatLng()
	now := time.Now().UnixMilli()
	expires := now + LocationTTLMillis
	radius := req.RadiusMeters
	if radius <= 0 || radius > NearbyRadiusMeters {
		radius = NearbyRadiusMeters
	}
	if req.ExpiresDays > 0 && req.ExpiresDays <= 60 {
		expires = now + int64(req.ExpiresDays)*int64(24*time.Hour/time.Millisecond)
	}
	geohash := roughGeoHash(lat, lng)
	source := strings.TrimSpace(req.Source)
	if source == "" {
		source = "network"
	}
	source = truncatePartnerRunes(source, 16)
	_, err := d.session.InsertBySql(`INSERT INTO partner_locations(uid,lat,lng,accuracy,radius_meters,geohash,source,updated_at_ms,expires_at,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE lat=VALUES(lat),lng=VALUES(lng),accuracy=VALUES(accuracy),radius_meters=VALUES(radius_meters),geohash=VALUES(geohash),source=VALUES(source),updated_at_ms=VALUES(updated_at_ms),expires_at=VALUES(expires_at),updated_at=NOW()`,
		uid, lat, lng, req.Accuracy, radius, geohash, source, now, expires).Exec()
	if err != nil {
		return nil, err
	}
	return &locationModel{UID: uid, Lat: lat, Lng: lng, Accuracy: req.Accuracy, RadiusMeters: radius, Geohash: geohash, UpdatedAt: now, ExpiresAt: expires, Source: source}, nil
}

func (d *db) getLocation(uid string) (*locationModel, error) {
	if uid == "" {
		return nil, nil
	}
	var model *locationModel
	_, err := d.session.Select("uid", "lat", "lng", "IFNULL(accuracy,0) accuracy", "IFNULL(radius_meters,70000) radius_meters", "geohash", "IFNULL(source,'') source", "updated_at_ms", "expires_at").
		From("partner_locations").
		Where("uid=? and expires_at>?", uid, time.Now().UnixMilli()).
		Load(&model)
	if err != nil {
		return nil, err
	}
	return model, nil
}

func (d *db) profileMe(uid string) (*ProfileMeResp, error) {
	if uid == "" {
		return &ProfileMeResp{}, nil
	}
	var row struct {
		ProfileImagesRaw    string `db:"profile_images"`
		NativeLanguagesRaw  string `db:"native_languages"`
		LearningLanguageRaw string `db:"learning_languages"`
		TagsRaw             string `db:"tags"`
		ProfileCover        string `db:"profile_cover"`
	}
	_, err := d.session.Select("profile_images", "native_languages", "learning_languages", "tags", "profile_cover").From("user").Where("uid=?", uid).Load(&row)
	if err != nil {
		return nil, err
	}
	images := parseImageList(row.ProfileImagesRaw, 9)
	return &ProfileMeResp{
		HasPartnerPhoto:   len(images) > 0,
		ProfileImages:     images,
		NativeLanguages:   parseStringList(row.NativeLanguagesRaw, 5),
		LearningLanguages: parseStringList(row.LearningLanguageRaw, 5),
		Tags:              parseStringList(row.TagsRaw, 20),
		ProfileCover:      strings.TrimSpace(row.ProfileCover),
	}, nil
}

func (d *db) list(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	limit := clampLimit(req.Limit)
	offset := req.Offset()
	viewerLoc := req.Location
	if viewerLoc == nil && req.UseLoginLocation {
		loc, _ := d.getLocation(loginUID)
		viewerLoc = loc
	}

	selectDistanceArgs := make([]interface{}, 0)
	whereDistanceArgs := make([]interface{}, 0)
	distanceExpr := "0"
	distanceSelect := "0 AS distance_meters"
	locationWhere := ""
	if viewerLoc != nil && validLatLng(viewerLoc.Lat, viewerLoc.Lng) {
		distanceExpr = `IF(pp.expires_at>? AND pp.lat<>0 AND pp.lng<>0, IFNULL(CAST((6371000 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(pp.lat - ?)/2),2)+COS(RADIANS(?))*COS(RADIANS(pp.lat))*POWER(SIN(RADIANS(pp.lng - ?)/2),2)))) AS UNSIGNED),0),0)`
		distanceSelect = distanceExpr + " AS distance_meters"
		selectDistanceArgs = append(selectDistanceArgs, time.Now().UnixMilli(), viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng)
		if req.NearbyOnly {
			minLat, maxLat, minLng, maxLng := latLngBounds(viewerLoc.Lat, viewerLoc.Lng, req.RadiusMeters())
			locationWhere = " AND pp.expires_at>? AND pp.lat BETWEEN ? AND ? AND pp.lng BETWEEN ? AND ? AND " + distanceExpr + " <= ? "
			whereDistanceArgs = append(whereDistanceArgs, time.Now().UnixMilli(), minLat, maxLat, minLng, maxLng, time.Now().UnixMilli(), viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng, req.RadiusMeters())
		}
	}

	sql := `SELECT pp.uid,pp.name,pp.username,'' AS avatar,pp.sex,pp.intro,pp.country_code,pp.country,pp.native_languages,pp.learning_languages,pp.birthday,pp.tags,pp.profile_cover,pp.profile_images,pp.vercode,
		IFNULL(fr.follow,0) AS follow,
		IFNULL(pp.online,0) AS online,
		IFNULL(pp.last_offline,0) AS last_offline,
		IFNULL(pp.last_active_at,0) AS last_active_at,
		IFNULL(pe.seen_count,0) AS seen_count,
		IFNULL(pe.last_seen_at,0) AS last_seen_at,
		IFNULL(pg.greet_count,0) AS greet_count,
		IFNULL(pg.last_greet_at,0) AS last_greet_at,
		IF(IFNULL(pg.last_greet_at,0)>0,1,0) AS hello_sent,
		IF(IFNULL(pg.last_greet_at,0)>0,1,0) AS greeting_status,
		IFNULL(pc.status,-1) AS contact_status,
		IFNULL(pc.requester_msg_count,0) AS requester_msg_count,
		UNIX_TIMESTAMP(pp.created_at) AS created_at_unix,
		UNIX_TIMESTAMP(pp.updated_at) AS updated_at_unix,
		` + distanceSelect + `
		FROM partner_profiles pp
		LEFT JOIN (
			SELECT to_uid, 1 AS follow FROM friend WHERE uid=? AND is_deleted=0
		) fr ON fr.to_uid=pp.uid
		LEFT JOIN partner_exposures pe ON pe.uid=? AND pe.to_uid=pp.uid
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid AND IFNULL(pg.send_status,1)<>2
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid<>? AND pp.status=1 AND pp.account_eligible=1 AND pp.partner_enabled=1 AND pp.profile_completed=1 AND pp.review_status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND NOT EXISTS (
			SELECT 1 FROM report r1
			WHERE r1.uid=? AND r1.channel_type=1 AND r1.channel_id=pp.uid
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM report r2
			WHERE r2.channel_id=? AND r2.channel_type=1 AND r2.uid=pp.uid
		  )
		  AND IFNULL(fr.follow,0)=0
		  AND IFNULL(pg.last_greet_at,0)=0
		  AND IFNULL(pc.status,-1) NOT IN (0,1,2,3)
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''
		` + locationWhere + `
		ORDER BY IFNULL(pe.last_seen_at,0) ASC, IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.updated_at DESC
		LIMIT ? OFFSET ?`

	orderedArgs := make([]interface{}, 0, len(selectDistanceArgs)+len(whereDistanceArgs)+11)
	orderedArgs = append(orderedArgs, selectDistanceArgs...)
	orderedArgs = append(orderedArgs, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID)
	orderedArgs = append(orderedArgs, whereDistanceArgs...)
	orderedArgs = append(orderedArgs, limit+1, offset)

	var list []*PartnerUser
	_, err := d.session.SelectBySql(sql, orderedArgs...).Load(&list)
	if err != nil {
		return nil, 0, err
	}
	hasMore := 0
	if len(list) > limit {
		hasMore = 1
		list = list[:limit]
	}
	return list, hasMore, nil
}

func (d *db) recordExposure(loginUID string, users []*PartnerUser) {
	if loginUID == "" || len(users) == 0 {
		return
	}
	tx, err := d.session.Begin()
	if err != nil {
		return
	}
	defer tx.RollbackUnlessCommitted()
	now := time.Now().UnixMilli()
	for _, u := range users {
		if u == nil || u.UID == "" {
			continue
		}
		_, _ = tx.InsertBySql(`INSERT INTO partner_exposures(uid,to_uid,seen_count,last_seen_at,created_at,updated_at)
            VALUES(?,?,1,?,NOW(),NOW())
            ON DUPLICATE KEY UPDATE seen_count=seen_count+1,last_seen_at=VALUES(last_seen_at),updated_at=NOW()`, loginUID, u.UID, now).Exec()
	}
	_ = tx.Commit()
}

func (d *db) candidateUIDs(loginUID string, req listReq, limit int) ([]string, error) {
	if limit <= 0 || limit > PartnerCandidateSQLLimit {
		limit = PartnerCandidateSQLLimit
	}
	viewer, _ := d.profileMe(loginUID)
	out := make([]string, 0, limit)
	appendBucket := func(values []string, max int) {
		if max <= 0 {
			max = len(values)
		}
		added := 0
		for _, uid := range values {
			uid = strings.TrimSpace(uid)
			if uid == "" || uid == loginUID {
				continue
			}
			out = append(out, uid)
			added++
			if added >= max {
				break
			}
		}
	}

	// 多路召回：语言互补优先，活跃/附近/新用户/探索混入，避免在线用户永远霸榜。
	if values, err := d.candidateUIDsByLanguages(loginUID, viewer, limit/2+80); err == nil {
		appendBucket(values, limit/2+40)
	}
	if values, err := d.candidateUIDsNearby(loginUID, req, limit/5+50); err == nil {
		appendBucket(values, limit/5+30)
	}
	if values, err := d.candidateUIDsActive(loginUID, limit/3+80); err == nil {
		appendBucket(values, limit/3+50)
	}
	if values, err := d.candidateUIDsNewest(loginUID, limit/5+50); err == nil {
		appendBucket(values, limit/5+30)
	}
	if len(out) < limit {
		if values, err := d.candidateUIDsExplore(loginUID, limit); err == nil {
			appendBucket(values, limit-len(out)+30)
		}
	}
	return compactUIDs(out, limit), nil
}

func (d *db) candidateUIDsByLanguages(loginUID string, viewer *ProfileMeResp, limit int) ([]string, error) {
	if viewer == nil || (len(viewer.NativeLanguages) == 0 && len(viewer.LearningLanguages) == 0) {
		return []string{}, nil
	}
	likes := make([]string, 0)
	args := make([]interface{}, 0)
	for _, lang := range viewer.LearningLanguages {
		lang = strings.ToLower(strings.TrimSpace(lang))
		if lang == "" {
			continue
		}
		likes = append(likes, "LOWER(pp.native_languages) LIKE ?")
		args = append(args, "%"+lang+"%")
	}
	for _, lang := range viewer.NativeLanguages {
		lang = strings.ToLower(strings.TrimSpace(lang))
		if lang == "" {
			continue
		}
		likes = append(likes, "LOWER(pp.learning_languages) LIKE ?")
		args = append(args, "%"+lang+"%")
	}
	if len(likes) == 0 {
		return []string{}, nil
	}
	return d.candidateUIDsForUser(loginUID, " AND ("+strings.Join(likes, " OR ")+") ", args, "IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.profile_score DESC", limit)
}

func (d *db) candidateUIDsNearby(loginUID string, req listReq, limit int) ([]string, error) {
	viewerLoc := req.Location
	if viewerLoc == nil && req.UseLoginLocation {
		viewerLoc, _ = d.getLocation(loginUID)
	}
	if viewerLoc == nil || !validLatLng(viewerLoc.Lat, viewerLoc.Lng) {
		return []string{}, nil
	}
	minLat, maxLat, minLng, maxLng := latLngBounds(viewerLoc.Lat, viewerLoc.Lng, req.RadiusMeters())
	distanceExpr := `IF(pp.expires_at>? AND pp.lat<>0 AND pp.lng<>0, IFNULL(CAST((6371000 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(pp.lat - ?)/2),2)+COS(RADIANS(?))*COS(RADIANS(pp.lat))*POWER(SIN(RADIANS(pp.lng - ?)/2),2)))) AS UNSIGNED),0),0)`
	args := []interface{}{time.Now().UnixMilli(), minLat, maxLat, minLng, maxLng, time.Now().UnixMilli(), viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng, req.RadiusMeters()}
	where := " AND pp.expires_at>? AND pp.lat BETWEEN ? AND ? AND pp.lng BETWEEN ? AND ? AND " + distanceExpr + " <= ? "
	return d.candidateUIDsForUser(loginUID, where, args, "IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC", limit)
}

func (d *db) candidateUIDsActive(loginUID string, limit int) ([]string, error) {
	return d.candidateUIDsForUser(loginUID, "", nil, "IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.updated_at DESC", limit)
}

func (d *db) candidateUIDsNewest(loginUID string, limit int) ([]string, error) {
	return d.candidateUIDsForUser(loginUID, "", nil, "pp.created_at DESC, IFNULL(pp.last_active_at,0) DESC", limit)
}

func (d *db) candidateUIDsExplore(loginUID string, limit int) ([]string, error) {
	return d.candidateUIDsForUser(loginUID, "", nil, "pp.updated_at DESC, pp.uid DESC", limit)
}

func (d *db) candidateUIDsForUser(loginUID, extraWhere string, extraArgs []interface{}, orderBy string, limit int) ([]string, error) {
	if limit <= 0 || limit > PartnerCandidateSQLLimit {
		limit = PartnerCandidateSQLLimit
	}
	if strings.TrimSpace(orderBy) == "" {
		orderBy = "IFNULL(pp.last_active_at,0) DESC"
	}
	sql := `SELECT pp.uid
		FROM partner_profiles pp
		LEFT JOIN (
			SELECT to_uid, 1 AS follow FROM friend WHERE uid=? AND is_deleted=0
		) fr ON fr.to_uid=pp.uid
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid AND IFNULL(pg.send_status,1)<>2
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid<>? AND pp.status=1 AND pp.account_eligible=1 AND pp.partner_enabled=1 AND pp.profile_completed=1 AND pp.review_status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND NOT EXISTS (
			SELECT 1 FROM report r1
			WHERE r1.uid=? AND r1.channel_type=1 AND r1.channel_id=pp.uid
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM report r2
			WHERE r2.channel_id=? AND r2.channel_type=1 AND r2.uid=pp.uid
		  )
		  AND IFNULL(fr.follow,0)=0
		  AND IFNULL(pg.last_greet_at,0)=0
		  AND IFNULL(pc.status,-1) NOT IN (0,1,2,3)
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''` + extraWhere + `
		ORDER BY ` + orderBy + `
		LIMIT ?`
	args := make([]interface{}, 0, 8+len(extraArgs)+1)
	args = append(args, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID)
	args = append(args, extraArgs...)
	args = append(args, limit)
	var uids []string
	_, err := d.session.SelectBySql(sql, args...).Load(&uids)
	return uids, err
}

func (d *db) globalCandidateUIDs(limit int) ([]string, error) {
	if limit <= 0 || limit > PartnerGlobalCandidateSQLLimit {
		limit = PartnerGlobalCandidateSQLLimit
	}
	sql := `SELECT pp.uid
		FROM partner_profiles pp
		WHERE pp.status=1 AND pp.account_eligible=1 AND pp.partner_enabled=1 AND pp.profile_completed=1 AND pp.review_status=1 AND pp.has_photo=1
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''
		ORDER BY IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.updated_at DESC
		LIMIT ?`
	var uids []string
	_, err := d.session.SelectBySql(sql, limit).Load(&uids)
	return uids, err
}

func (d *db) listByUIDs(loginUID string, req listReq, uids []string) ([]*PartnerUser, error) {
	if loginUID == "" || len(uids) == 0 {
		return []*PartnerUser{}, nil
	}
	viewerLoc := req.Location
	if viewerLoc == nil && req.UseLoginLocation {
		loc, _ := d.getLocation(loginUID)
		viewerLoc = loc
	}

	selectDistanceArgs := make([]interface{}, 0)
	distanceExpr := "0"
	distanceSelect := "0 AS distance_meters"
	if viewerLoc != nil && validLatLng(viewerLoc.Lat, viewerLoc.Lng) {
		distanceExpr = `IF(pp.expires_at>? AND pp.lat<>0 AND pp.lng<>0, IFNULL(CAST((6371000 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(pp.lat - ?)/2),2)+COS(RADIANS(?))*COS(RADIANS(pp.lat))*POWER(SIN(RADIANS(pp.lng - ?)/2),2)))) AS UNSIGNED),0),0)`
		distanceSelect = distanceExpr + " AS distance_meters"
		selectDistanceArgs = append(selectDistanceArgs, time.Now().UnixMilli(), viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng)
	}

	sql := `SELECT pp.uid,pp.name,pp.username,'' AS avatar,pp.sex,pp.intro,pp.country_code,pp.country,pp.native_languages,pp.learning_languages,pp.birthday,pp.tags,pp.profile_cover,pp.profile_images,pp.vercode,
		IFNULL(fr.follow,0) AS follow,
		IFNULL(pp.online,0) AS online,
		IFNULL(pp.last_offline,0) AS last_offline,
		IFNULL(pp.last_active_at,0) AS last_active_at,
		IFNULL(pe.seen_count,0) AS seen_count,
		IFNULL(pe.last_seen_at,0) AS last_seen_at,
		IFNULL(pg.greet_count,0) AS greet_count,
		IFNULL(pg.last_greet_at,0) AS last_greet_at,
		IF(IFNULL(pg.last_greet_at,0)>0,1,0) AS hello_sent,
		IF(IFNULL(pg.last_greet_at,0)>0,1,0) AS greeting_status,
		IFNULL(pc.status,-1) AS contact_status,
		IFNULL(pc.requester_msg_count,0) AS requester_msg_count,
		UNIX_TIMESTAMP(pp.created_at) AS created_at_unix,
		UNIX_TIMESTAMP(pp.updated_at) AS updated_at_unix,
		` + distanceSelect + `
		FROM partner_profiles pp
		LEFT JOIN (
			SELECT to_uid, 1 AS follow FROM friend WHERE uid=? AND is_deleted=0
		) fr ON fr.to_uid=pp.uid
		LEFT JOIN partner_exposures pe ON pe.uid=? AND pe.to_uid=pp.uid
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid AND IFNULL(pg.send_status,1)<>2
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid IN ? AND pp.uid<>? AND pp.status=1 AND pp.account_eligible=1 AND pp.partner_enabled=1 AND pp.profile_completed=1 AND pp.review_status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND NOT EXISTS (
			SELECT 1 FROM report r1
			WHERE r1.uid=? AND r1.channel_type=1 AND r1.channel_id=pp.uid
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM report r2
			WHERE r2.channel_id=? AND r2.channel_type=1 AND r2.uid=pp.uid
		  )
		  AND IFNULL(fr.follow,0)=0
		  AND IFNULL(pg.last_greet_at,0)=0
		  AND IFNULL(pc.status,-1) NOT IN (0,1,2,3)
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''`

	orderedArgs := make([]interface{}, 0, len(selectDistanceArgs)+10)
	orderedArgs = append(orderedArgs, selectDistanceArgs...)
	orderedArgs = append(orderedArgs, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, uids, loginUID, loginUID, loginUID)
	var list []*PartnerUser
	_, err := d.session.SelectBySql(sql, orderedArgs...).Load(&list)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (d *db) recordExposureItems(loginUID string, items []ExposureItem) error {
	if loginUID == "" || len(items) == 0 {
		return nil
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	for _, item := range items {
		toUID := strings.TrimSpace(item.ToUID)
		if toUID == "" || toUID == loginUID {
			continue
		}
		seenAt := normalizeMillis(item.SeenAt)
		if seenAt <= 0 {
			seenAt = time.Now().UnixMilli()
		}
		eventType := normalizeExposureEventType(item.EventType, item.DurationMS)
		source := normalizeExposureSource(item.Source)
		if item.PhotoIndex < 0 {
			item.PhotoIndex = 0
		}
		_, err = tx.InsertBySql(`INSERT INTO partner_exposure_events(uid,to_uid,event_type,source,duration_ms,photo_index,event_at,created_at)
			VALUES(?,?,?,?,?,?,?,NOW())`, loginUID, toUID, eventType, source, item.DurationMS, item.PhotoIndex, seenAt).Exec()
		if err != nil {
			return err
		}
		if shouldCountExposureEvent(eventType, item.DurationMS) {
			_, err = tx.InsertBySql(`INSERT INTO partner_exposures(uid,to_uid,seen_count,last_seen_at,last_duration_ms,created_at,updated_at)
				VALUES(?,?,1,?,?,NOW(),NOW())
				ON DUPLICATE KEY UPDATE seen_count=seen_count+1,last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at)),last_duration_ms=VALUES(last_duration_ms),updated_at=NOW()`, loginUID, toUID, seenAt, item.DurationMS).Exec()
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func normalizeExposureEventType(eventType string, durationMS int64) string {
	eventType = strings.TrimSpace(strings.ToLower(eventType))
	switch eventType {
	case "expose", "exposure", "stay", "skip", "profile_open", "photo_swipe", "hello":
		if eventType == "exposure" {
			return "expose"
		}
		return eventType
	default:
		if durationMS > 0 && durationMS < PartnerExposureMinDurationMS {
			return "skip"
		}
		return "expose"
	}
}

func normalizeExposureSource(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "partner_browse"
	}
	source = truncatePartnerRunes(source, 32)
	return source
}

func shouldCountExposureEvent(eventType string, durationMS int64) bool {
	switch eventType {
	case "skip":
		return false
	case "hello", "profile_open":
		return true
	default:
		return durationMS >= PartnerExposureMinDurationMS
	}
}

func (d *db) userExists(uid string) (bool, error) {
	if uid == "" {
		return false, nil
	}
	var count int
	err := d.session.Select("COUNT(*)").From("user").Where("uid=? AND status=1 AND IFNULL(is_destroy,0)=0", uid).LoadOne(&count)
	return count > 0, err
}

func (d *db) hasAnyBlacklist(uid, toUID string) (bool, error) {
	var count int
	err := d.session.Select("COUNT(*)").From("user_setting").Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND blacklist=1", uid, toUID, toUID, uid).LoadOne(&count)
	return count > 0, err
}

func greetingDayStartMillis(nowMS int64) int64 {
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.UnixMilli(nowMS).In(loc)
	day := now.Add(-4 * time.Hour)
	start := time.Date(day.Year(), day.Month(), day.Day(), 4, 0, 0, 0, loc)
	return start.UnixMilli()
}

func greetingDayKey(nowMS int64) string {
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	return time.UnixMilli(nowMS).In(loc).Add(-4 * time.Hour).Format("2006-01-02")
}

func (d *db) greetingStats(uid, toUID string, now int64) (*greetingStats, error) {
	stats := &greetingStats{}
	err := d.session.Select("IFNULL(MAX(used_count),0)").From("partner_greeting_daily_usage").Where("sender_uid=? AND day_key=?", uid, greetingDayKey(now)).LoadOne(&stats.DayCount)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(toUID) != "" {
		var last int64
		err = d.session.Select("IFNULL(MAX(last_greet_at),0)").From("partner_greetings").Where("uid=? AND to_uid=? AND IFNULL(send_status,1)<>2", uid, toUID).LoadOne(&last)
		if err != nil {
			return nil, err
		}
		stats.LastTargetGreetAt = last
	}
	return stats, nil
}

func (d *db) reserveGreetingDailyTarget(senderUID, receiverUID string, nowMS int64, limit int) (bool, int, error) {
	if senderUID == "" || receiverUID == "" || senderUID == receiverUID {
		return false, 0, ErrGreetingSelf
	}
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	if limit <= 0 {
		limit = GreetingDayLimit
	}
	dayKey := greetingDayKey(nowMS)
	tx, err := d.session.Begin()
	if err != nil {
		return false, 0, err
	}
	defer tx.RollbackUnlessCommitted()

	_, err = tx.InsertBySql(`INSERT INTO partner_greeting_daily_usage(sender_uid,day_key,used_count,created_at,updated_at)
		VALUES(?,?,0,?,?) ON DUPLICATE KEY UPDATE updated_at=updated_at`, senderUID, dayKey, nowMS, nowMS).Exec()
	if err != nil {
		return false, 0, err
	}
	var usageRows []struct {
		UsedCount int `db:"used_count"`
	}
	_, err = tx.SelectBySql(`SELECT used_count FROM partner_greeting_daily_usage WHERE sender_uid=? AND day_key=? FOR UPDATE`, senderUID, dayKey).Load(&usageRows)
	if err != nil || len(usageRows) == 0 {
		if err == nil {
			err = errors.New("每日打招呼额度记录不存在")
		}
		return false, 0, err
	}
	used := usageRows[0].UsedCount

	var targetRows []struct {
		ID int64 `db:"id"`
	}
	_, err = tx.SelectBySql(`SELECT id FROM partner_greeting_daily_target WHERE sender_uid=? AND receiver_uid=? AND day_key=? LIMIT 1`, senderUID, receiverUID, dayKey).Load(&targetRows)
	if err != nil {
		return false, used, err
	}
	if len(targetRows) > 0 {
		if err = tx.Commit(); err != nil {
			return false, used, err
		}
		return false, used, nil
	}
	if used >= limit {
		return false, used, ErrGreetingDayLimit
	}
	_, err = tx.InsertInto("partner_greeting_daily_target").Columns("sender_uid", "receiver_uid", "day_key", "created_at").Values(senderUID, receiverUID, dayKey, nowMS).Exec()
	if err != nil {
		return false, used, err
	}
	used++
	_, err = tx.Update("partner_greeting_daily_usage").Set("used_count", used).Set("updated_at", nowMS).Where("sender_uid=? AND day_key=?", senderUID, dayKey).Exec()
	if err != nil {
		return false, used - 1, err
	}
	if err = tx.Commit(); err != nil {
		return false, used - 1, err
	}
	return true, used, nil
}

func (d *db) releaseGreetingDailyTarget(senderUID, receiverUID string, nowMS int64) error {
	if senderUID == "" || receiverUID == "" {
		return nil
	}
	if nowMS <= 0 {
		nowMS = time.Now().UnixMilli()
	}
	dayKey := greetingDayKey(nowMS)
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	result, err := tx.DeleteFrom("partner_greeting_daily_target").Where("sender_uid=? AND receiver_uid=? AND day_key=?", senderUID, receiverUID, dayKey).Exec()
	if err != nil {
		return err
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		_, err = tx.Update("partner_greeting_daily_usage").Set("used_count", dbr.Expr("GREATEST(used_count-1,0)")).Set("updated_at", nowMS).Where("sender_uid=? AND day_key=?", senderUID, dayKey).Exec()
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

type greetingDeliveryRow struct {
	UID          string `db:"uid"`
	ToUID        string `db:"to_uid"`
	Text         string `db:"text"`
	Source       string `db:"source"`
	LastGreetAt  int64  `db:"last_greet_at"`
	SendStatus   int    `db:"send_status"`
	FailedReason string `db:"failed_reason"`
}

func (d *db) hasPendingGreetingContact(uid, toUID string) (bool, error) {
	if uid == "" || toUID == "" {
		return false, nil
	}
	var count int
	err := d.session.Select("COUNT(*)").From("partner_contacts").
		Where("uid=? AND to_uid=? AND status=? AND requester_uid=?", uid, toUID, PartnerContactStatusPending, uid).
		LoadOne(&count)
	return count > 0, err
}

func (d *db) greetingDelivery(uid, toUID string) (*greetingDeliveryRow, error) {
	if uid == "" || toUID == "" {
		return nil, nil
	}
	var row *greetingDeliveryRow
	_, err := d.session.Select("uid", "to_uid", "text", "source", "IFNULL(last_greet_at,0) last_greet_at", "IFNULL(send_status,0) send_status", "IFNULL(failed_reason,'') failed_reason").
		From("partner_greetings").Where("uid=? AND to_uid=?", uid, toUID).Load(&row)
	return row, err
}

func (d *db) pendingGreetingDeliveries(limit int) ([]*greetingDeliveryRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 20
	}
	retryBefore := time.Now().Add(-30 * time.Second).UnixMilli()
	var rows []*greetingDeliveryRow
	_, err := d.session.Select("uid", "to_uid", "text", "source", "IFNULL(last_greet_at,0) last_greet_at", "IFNULL(send_status,0) send_status", "IFNULL(failed_reason,'') failed_reason").
		From("partner_greetings").Where("send_status=0 AND last_greet_at>0 AND last_send_at<=?", retryBefore).OrderAsc("last_send_at").Limit(uint64(limit)).Load(&rows)
	return rows, err
}

func (d *db) failedGreetingRollbacks(limit int) ([]*greetingDeliveryRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var rows []*greetingDeliveryRow
	_, err := d.session.Select("uid", "to_uid", "text", "source", "IFNULL(last_greet_at,0) last_greet_at", "IFNULL(send_status,0) send_status", "IFNULL(failed_reason,'') failed_reason").
		From("partner_greetings").
		Where("send_status=2 AND last_greet_at>0 AND LEFT(IFNULL(failed_reason,''),?)<>?", len([]rune(greetingRollbackMarker)), greetingRollbackMarker).
		OrderAsc("last_send_at").Limit(uint64(limit)).Load(&rows)
	return rows, err
}
func (d *db) recordGreeting(uid, toUID, text, source string) (*GreetingResp, error) {
	now := time.Now().UnixMilli()
	_, err := d.session.InsertBySql(`INSERT INTO partner_greetings(uid,to_uid,text,source,greet_count,last_greet_at,send_status,last_send_at,failed_reason,created_at,updated_at)
        VALUES(?,?,?,?,1,?,0,?, '',NOW(),NOW())
        ON DUPLICATE KEY UPDATE text=VALUES(text),source=VALUES(source),greet_count=greet_count+1,last_greet_at=VALUES(last_greet_at),send_status=0,last_send_at=VALUES(last_send_at),failed_reason='',updated_at=NOW()`, uid, toUID, text, source, now, now).Exec()
	if err != nil {
		return nil, err
	}
	return &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: now, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: 1, MaxGreetingCount: MaxPendingGreetingMessages, Text: text, Msg: "已打招呼"}, nil
}

type greetingStats struct {
	DayCount          int
	LastTargetGreetAt int64
}

type listReq struct {
	Page             int
	Limit            int
	Cursor           string
	SessionID        string
	NearbyOnly       bool
	Radius           int
	UseLoginLocation bool
	Location         *locationModel
}

func (r listReq) Offset() int {
	if strings.TrimSpace(r.Cursor) != "" {
		n, _ := strconv.Atoi(r.Cursor)
		if n > 0 {
			return n
		}
	}
	if r.Page <= 1 {
		return 0
	}
	return (r.Page - 1) * clampLimit(r.Limit)
}

func (r listReq) Round() int {
	if r.Page > 0 {
		return r.Page
	}
	n, _ := strconv.Atoi(r.Cursor)
	if n > 0 {
		return n/clampLimit(r.Limit) + 1
	}
	return 1
}

func (r listReq) RandomSeed() string {
	seed := strings.TrimSpace(r.SessionID)
	if seed == "" {
		seed = strings.TrimSpace(r.Cursor)
	}
	if seed == "" {
		seed = time.Now().Format("2006010215")
	}
	return seed + ":" + strconv.Itoa(r.Round())
}

func (r listReq) RadiusMeters() int {
	if r.Radius <= 0 {
		return NearbyRadiusMeters
	}
	if r.Radius > NearbyRadiusMeters {
		return NearbyRadiusMeters
	}
	return r.Radius
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return DefaultPartnerLimit
	}
	if limit > MaxPartnerLimit {
		return MaxPartnerLimit
	}
	return limit
}

func roughGeoHash(lat, lng float64) string {
	// 轻量粗格子：不引入 geohash 依赖。查询先靠距离表达式，后续可升级 geohash。
	return strconv.Itoa(int((lat+90)*10)) + ":" + strconv.Itoa(int((lng+180)*10))
}

func latLngBounds(lat, lng float64, radiusMeters int) (float64, float64, float64, float64) {
	if radiusMeters <= 0 {
		radiusMeters = NearbyRadiusMeters
	}
	latDelta := float64(radiusMeters) / 111320.0
	cosLat := math.Cos(lat * math.Pi / 180)
	if math.Abs(cosLat) < 0.01 {
		cosLat = 0.01
	}
	lngDelta := float64(radiusMeters) / (111320.0 * math.Abs(cosLat))
	minLat := math.Max(-90, lat-latDelta)
	maxLat := math.Min(90, lat+latDelta)
	minLng := lng - lngDelta
	maxLng := lng + lngDelta
	if minLng < -180 {
		minLng = -180
	}
	if maxLng > 180 {
		maxLng = 180
	}
	return minLat, maxLat, minLng, maxLng
}

func (d *db) syncPartnerProfileFromUser(uid string) error {
	if uid == "" {
		return nil
	}
	_, err := d.session.UpdateBySql(`INSERT INTO partner_profiles(uid,name,username,sex,birthday,intro,country_code,country,native_languages,learning_languages,tags,profile_cover,profile_images,vercode,has_photo,profile_score,status,account_eligible,partner_enabled,profile_completed,review_status,profile_completed_at,last_active_at,created_at,updated_at)
SELECT u.uid,IFNULL(u.name,''),IFNULL(u.username,''),IFNULL(u.sex,0),IFNULL(u.birthday,''),IFNULL(u.intro,''),IFNULL(u.country_code,''),IFNULL(u.country,''),IFNULL(u.native_languages,''),IFNULL(u.learning_languages,''),IFNULL(u.tags,''),IFNULL(u.profile_cover,''),IFNULL(u.profile_images,''),IFNULL(u.vercode,''),
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null'),1,0),
(IF(IFNULL(u.intro,'')<>'',2,0)+IF(IFNULL(u.tags,'') NOT IN ('','[]','null'),2,0)+IF(IFNULL(u.country_code,'')<>'',1,0)+IF(IFNULL(u.birthday,'')<>'',1,0)),
IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService') AND IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),1,0),
IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0),1,
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),1,0),1,
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,0),
GREATEST(UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,UNIX_TIMESTAMP(IFNULL(u.created_at,NOW()))*1000),NOW(),NOW()
FROM user u WHERE u.uid=?
ON DUPLICATE KEY UPDATE name=VALUES(name),username=VALUES(username),sex=VALUES(sex),birthday=VALUES(birthday),intro=VALUES(intro),country_code=VALUES(country_code),country=VALUES(country),native_languages=VALUES(native_languages),learning_languages=VALUES(learning_languages),tags=VALUES(tags),profile_cover=VALUES(profile_cover),profile_images=VALUES(profile_images),vercode=VALUES(vercode),has_photo=VALUES(has_photo),profile_score=VALUES(profile_score),account_eligible=VALUES(account_eligible),profile_completed=VALUES(profile_completed),profile_completed_at=IF(profile_completed_at>0,profile_completed_at,VALUES(profile_completed_at)),status=IF(VALUES(account_eligible)=1 AND partner_enabled=1 AND VALUES(profile_completed)=1 AND review_status=1,1,0),last_active_at=GREATEST(IFNULL(last_active_at,0),VALUES(last_active_at))`, uid).Exec()
	if err == nil && d.ctx != nil && d.ctx.GetRedisConn() != nil {
		_, _ = d.ctx.GetRedisConn().LPUSH("partnerlist:pool:dirty_queue", uid)
		_ = d.ctx.GetRedisConn().Expire("partnerlist:pool:dirty_queue", 24*time.Hour)
	}
	return err
}

func (d *db) syncPartnerLocation(uid string, loc *locationModel) error {
	if uid == "" || loc == nil {
		return nil
	}
	_, err := d.session.UpdateBySql(`UPDATE partner_profiles
		SET lat=?,lng=?,accuracy=?,radius_meters=?,geohash=?,location_updated_at=?,expires_at=?,last_active_at=GREATEST(IFNULL(last_active_at,0),?),updated_at=NOW()
		WHERE uid=?`, loc.Lat, loc.Lng, loc.Accuracy, loc.RadiusMeters, loc.Geohash, loc.UpdatedAt, loc.ExpiresAt, loc.UpdatedAt, uid).Exec()
	return err
}

func (d *db) getPartnerContact(uid, toUID string) (*partnerContactModel, error) {
	if uid == "" || toUID == "" {
		return nil, nil
	}
	var model *partnerContactModel
	_, err := d.session.Select("uid", "to_uid", "requester_uid", "status", "IFNULL(requester_msg_count,0) requester_msg_count", "IFNULL(last_msg_at,0) last_msg_at").
		From("partner_contacts").
		Where("uid=? AND to_uid=?", uid, toUID).
		Load(&model)
	if err != nil {
		return nil, err
	}
	return model, nil
}

func (d *db) incrementPendingRequesterMsgCount(uid, toUID string, now int64) (int, error) {
	if uid == "" || toUID == "" || uid == toUID {
		return 0, nil
	}
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	result, err := d.session.Update("partner_contacts").
		Set("requester_msg_count", dbr.Expr("IFNULL(requester_msg_count,0)+1")).
		Set("last_msg_at", now).
		Set("updated_at", now).
		Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=? AND IFNULL(requester_msg_count,0)<?", uid, toUID, toUID, uid, PartnerContactStatusPending, uid, MaxPendingGreetingMessages).
		Exec()
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return MaxPendingGreetingMessages, ErrPendingMessageLimit
	}
	contact, err := d.getPartnerContact(uid, toUID)
	if err != nil || contact == nil {
		return 0, err
	}
	if contact.RequesterMsgCount > MaxPendingGreetingMessages {
		return MaxPendingGreetingMessages, ErrPendingMessageLimit
	}
	return contact.RequesterMsgCount, nil
}

func (d *db) ensurePendingContact(uid, toUID string, now int64) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	for _, row := range [][2]string{{uid, toUID}, {toUID, uid}} {
		_, err = tx.InsertBySql(`INSERT INTO partner_contacts(uid,to_uid,requester_uid,status,requester_msg_count,last_msg_at,created_at,updated_at)
 VALUES(?,?,?,?,1,?,?,?) ON DUPLICATE KEY UPDATE requester_uid=IF(status IN (2,3),requester_uid,VALUES(requester_uid)),status=IF(status IN (1,2,3),status,VALUES(status)),requester_msg_count=GREATEST(requester_msg_count,VALUES(requester_msg_count)),last_msg_at=GREATEST(last_msg_at,VALUES(last_msg_at)),updated_at=VALUES(updated_at)`, row[0], row[1], uid, PartnerContactStatusPending, now, now, now).Exec()
		if err != nil {
			return err
		}
	}
	// Receiver may reply to requester's personal channel; requester must not be able
	// to bypass the pending gateway in the opposite direction.
	if err = enqueuePartnerIMPermissionTx(tx, permissionTransitionKey("pending:add", uid, toUID, now), uid, toUID, "add", now); err != nil {
		return err
	}
	if err = enqueuePartnerIMPermissionTx(tx, permissionTransitionKey("pending:remove", toUID, uid, now), toUID, uid, "remove", now); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) partnerContactUIDs(uid string) ([]string, error) {
	if uid == "" {
		return []string{}, nil
	}
	var uids []string
	_, err := d.session.Select("to_uid").From("partner_contacts").Where("uid=? AND status IN ?", uid, []int{PartnerContactStatusPending, PartnerContactStatusActive}).Load(&uids)
	if err != nil {
		return nil, err
	}
	return uids, nil
}

func (d *db) activateContactOnReply(fromUID, toUID string, at int64) (bool, error) {
	if fromUID == "" || toUID == "" || fromUID == toUID {
		return false, nil
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	tx, err := d.session.Begin()
	if err != nil {
		return false, err
	}
	defer tx.RollbackUnlessCommitted()
	var requester string
	if err = tx.Select("requester_uid").From("partner_contacts").Where("uid=? AND to_uid=? AND status=?", fromUID, toUID, PartnerContactStatusPending).LoadOne(&requester); err != nil {
		return false, nil
	}
	if requester == "" {
		return false, nil
	}
	if requester == fromUID {
		_, err = tx.Update("partner_contacts").Set("last_msg_at", at).Set("updated_at", at).Where("(uid=? AND to_uid=?) OR (uid=? AND to_uid=?)", fromUID, toUID, toUID, fromUID).Exec()
		if err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	result, err := tx.Update("partner_contacts").Set("status", PartnerContactStatusActive).Set("last_msg_at", at).Set("updated_at", at).Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=?", fromUID, toUID, toUID, fromUID, PartnerContactStatusPending).Exec()
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return false, tx.Commit()
	}
	for _, pair := range [][2]string{{fromUID, toUID}, {toUID, fromUID}} {
		if err = enqueuePartnerIMPermissionTx(tx, permissionTransitionKey("active:add", pair[0], pair[1], at), pair[0], pair[1], "add", at); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}
func (d *db) markGreetingSendStatus(uid, toUID string, at int64, status int, reason string) error {
	if uid == "" || toUID == "" {
		return nil
	}
	reason = truncatePartnerRunes(reason, 200)
	_, err := d.session.Update("partner_greetings").
		Set("send_status", status).
		Set("last_send_at", time.Now().UnixMilli()).
		Set("failed_reason", reason).
		Set("updated_at", dbr.Expr("NOW()")).
		Where("uid=? AND to_uid=? AND last_greet_at=?", uid, toUID, at).
		Exec()
	return err
}

func (d *db) rollbackPendingGreetingSend(uid, toUID string, at int64) error {
	if uid == "" || toUID == "" {
		return nil
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()

	// Claim this failed delivery inside the same transaction as every rollback side
	// effect. The marker makes retries idempotent: a timeout after COMMIT cannot
	// decrement requester_msg_count or the daily quota a second time.
	claim, err := tx.Update("partner_greetings").
		Set("failed_reason", dbr.Expr("CONCAT('__rollback_done__:',LEFT(IFNULL(failed_reason,''),182))")).
		Set("updated_at", dbr.Expr("NOW()")).
		Where("uid=? AND to_uid=? AND last_greet_at=? AND send_status=2 AND LEFT(IFNULL(failed_reason,''),?)<>?", uid, toUID, at, len([]rune(greetingRollbackMarker)), greetingRollbackMarker).
		Exec()
	if err != nil {
		return err
	}
	claimed, _ := claim.RowsAffected()
	if claimed == 0 {
		return tx.Commit()
	}

	if _, err = tx.Update("partner_contacts").
		Set("requester_msg_count", dbr.Expr("GREATEST(IFNULL(requester_msg_count,1)-1,0)")).
		Set("updated_at", at).
		Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=?", uid, toUID, toUID, uid, PartnerContactStatusPending, uid).
		Exec(); err != nil {
		return err
	}
	result, err := tx.DeleteFrom("partner_contacts").
		Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=? AND IFNULL(requester_msg_count,0)<=0", uid, toUID, toUID, uid, PartnerContactStatusPending, uid).
		Exec()
	if err != nil {
		return err
	}
	deleted, _ := result.RowsAffected()
	if deleted > 0 {
		// Permission changes may already have been applied synchronously before the IM
		// send failed. Persist both removals so no stale personal-channel whitelist remains.
		for _, pair := range [][2]string{{uid, toUID}, {toUID, uid}} {
			if err = enqueuePartnerIMPermissionTx(tx, permissionTransitionKey("rollback:remove", pair[0], pair[1], at), pair[0], pair[1], "remove", at); err != nil {
				return err
			}
		}
	}

	dayKey := greetingDayKey(at)
	dailyResult, err := tx.DeleteFrom("partner_greeting_daily_target").
		Where("sender_uid=? AND receiver_uid=? AND day_key=?", uid, toUID, dayKey).Exec()
	if err != nil {
		return err
	}
	dailyDeleted, _ := dailyResult.RowsAffected()
	if dailyDeleted > 0 {
		if _, err = tx.Update("partner_greeting_daily_usage").
			Set("used_count", dbr.Expr("GREATEST(used_count-1,0)")).
			Set("updated_at", at).
			Where("sender_uid=? AND day_key=?", uid, dayKey).Exec(); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *db) touchPartnerActive(uid string, at int64, online int) error {
	_ = online
	if uid == "" {
		return nil
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	_, err := d.session.UpdateBySql(`UPDATE partner_profiles SET last_active_at=GREATEST(IFNULL(last_active_at,0),?) WHERE uid=?`, at, uid).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (d *db) syncOnlineProfiles() error {
	_, err := d.session.UpdateBySql(`UPDATE partner_profiles pp
		LEFT JOIN (
			SELECT uid,MAX(online) AS online,MAX(last_offline) AS last_offline,MAX(GREATEST(last_online,last_offline))*1000 AS last_active_at
			FROM user_online GROUP BY uid
		) onl ON onl.uid=pp.uid
		SET pp.online=IFNULL(onl.online,0),pp.last_offline=IFNULL(onl.last_offline,pp.last_offline),pp.last_active_at=GREATEST(IFNULL(pp.last_active_at,0),IFNULL(onl.last_active_at,0))
		WHERE IFNULL(pp.online,0)<>IFNULL(onl.online,0)
		   OR IFNULL(onl.last_offline,0)>IFNULL(pp.last_offline,0)
		   OR IFNULL(onl.last_active_at,0)>IFNULL(pp.last_active_at,0)`).Exec()
	return err
}

func (d *db) syncRecentPartnerProfiles(limit int) (int64, error) {
	if limit <= 0 || limit > PartnerGlobalCandidateSQLLimit {
		limit = PartnerGlobalCandidateSQLLimit
	}
	res, err := d.session.UpdateBySql(`INSERT INTO partner_profiles(uid,name,username,sex,birthday,intro,country_code,country,native_languages,learning_languages,tags,profile_cover,profile_images,vercode,has_photo,profile_score,status,account_eligible,partner_enabled,profile_completed,review_status,profile_completed_at,last_active_at,created_at,updated_at)
SELECT u.uid,IFNULL(u.name,''),IFNULL(u.username,''),IFNULL(u.sex,0),IFNULL(u.birthday,''),IFNULL(u.intro,''),IFNULL(u.country_code,''),IFNULL(u.country,''),IFNULL(u.native_languages,''),IFNULL(u.learning_languages,''),IFNULL(u.tags,''),IFNULL(u.profile_cover,''),IFNULL(u.profile_images,''),IFNULL(u.vercode,''),
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null'),1,0),
(IF(IFNULL(u.intro,'')<>'',2,0)+IF(IFNULL(u.tags,'') NOT IN ('','[]','null'),2,0)+IF(IFNULL(u.country_code,'')<>'',1,0)+IF(IFNULL(u.birthday,'')<>'',1,0)),
IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService') AND IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),1,0),
IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0),1,
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),1,0),1,
IF(IFNULL(u.profile_images,'') NOT IN ('','[]','null') AND IFNULL(u.native_languages,'') NOT IN ('','[]','null') AND IFNULL(u.learning_languages,'') NOT IN ('','[]','null'),UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,0),
GREATEST(UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,UNIX_TIMESTAMP(IFNULL(u.created_at,NOW()))*1000),NOW(),NOW()
FROM user u WHERE u.updated_at >= DATE_SUB(NOW(), INTERVAL 2 DAY) ORDER BY u.updated_at DESC LIMIT ?
ON DUPLICATE KEY UPDATE name=VALUES(name),username=VALUES(username),sex=VALUES(sex),birthday=VALUES(birthday),intro=VALUES(intro),country_code=VALUES(country_code),country=VALUES(country),native_languages=VALUES(native_languages),learning_languages=VALUES(learning_languages),tags=VALUES(tags),profile_cover=VALUES(profile_cover),profile_images=VALUES(profile_images),vercode=VALUES(vercode),has_photo=VALUES(has_photo),profile_score=VALUES(profile_score),account_eligible=VALUES(account_eligible),profile_completed=VALUES(profile_completed),profile_completed_at=IF(profile_completed_at>0,profile_completed_at,VALUES(profile_completed_at)),status=IF(VALUES(account_eligible)=1 AND partner_enabled=1 AND VALUES(profile_completed)=1 AND review_status=1,1,0),last_active_at=GREATEST(IFNULL(last_active_at,0),VALUES(last_active_at))`, limit).Exec()
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return res.RowsAffected()
}

func (d *db) markPendingMessageDelivered(senderUID, businessClientMsgNo, imClientMsgNo string, imMessageID int64, at int64) error {
	if senderUID == "" || businessClientMsgNo == "" {
		return nil
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	_, err := d.session.Update("partner_pending_message").
		Set("status", 1).
		Set("im_client_msg_no", imClientMsgNo).
		Set("im_message_id", strconv.FormatInt(imMessageID, 10)).
		Set("failed_reason", "").
		Set("updated_at", at).
		Where("sender_uid=? AND client_msg_no=? AND status IN ?", senderUID, businessClientMsgNo, []int{0, 3}).
		Exec()
	return err
}

type pendingContactPair struct {
	RequesterUID string `db:"requester_uid"`
	ReceiverUID  string `db:"receiver_uid"`
}

func (d *db) pendingContactPairsAfter(afterRequester, afterReceiver string, limit int) ([]pendingContactPair, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	var rows []pendingContactPair
	_, err := d.session.SelectBySql(`SELECT requester_uid,to_uid AS receiver_uid
 FROM partner_contacts WHERE status=? AND requester_uid<>'' AND to_uid<>''
 AND uid=requester_uid AND requester_uid<>to_uid
 AND (requester_uid>? OR (requester_uid=? AND to_uid>?))
 ORDER BY requester_uid ASC,to_uid ASC LIMIT ?`, PartnerContactStatusPending, afterRequester, afterRequester, afterReceiver, limit).Load(&rows)
	return rows, err
}

func (d *db) desiredPartnerIMPermission(channelUID, memberUID string) (string, error) {
	if channelUID == "" || memberUID == "" || channelUID == memberUID {
		return "remove", nil
	}
	// A normal friendship always wins over a stale partner outbox task.
	var friendCount int
	if err := d.session.Select("COUNT(*)").From("friend").Where("uid=? AND to_uid=? AND is_deleted=0", channelUID, memberUID).LoadOne(&friendCount); err != nil {
		return "", err
	}
	if friendCount > 0 {
		return "add", nil
	}
	var rows []partnerContactModel
	_, err := d.session.Select("uid", "to_uid", "requester_uid", "status", "IFNULL(requester_msg_count,0) requester_msg_count", "IFNULL(last_msg_at,0) last_msg_at").
		From("partner_contacts").Where("uid=? AND to_uid=?", channelUID, memberUID).Limit(1).Load(&rows)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "remove", nil
	}
	row := rows[0]
	switch row.Status {
	case PartnerContactStatusActive:
		return "add", nil
	case PartnerContactStatusPending:
		// Whitelist(channelUID) contains users that may send into channelUID.
		// Only the receiver may send into the requester's channel before reply.
		if row.RequesterUID == channelUID {
			return "add", nil
		}
		return "remove", nil
	default:
		return "remove", nil
	}
}

type partnerIMPermissionTask struct {
	ID         int64  `db:"id"`
	ChannelUID string `db:"channel_uid"`
	MemberUID  string `db:"member_uid"`
	Action     string `db:"action"`
	Attempts   int    `db:"attempts"`
}

func permissionTransitionKey(prefix, channelUID, memberUID string, at int64) string {
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	return prefix + ":" + channelUID + ":" + memberUID + ":" + strconv.FormatInt(at, 10)
}

func enqueuePartnerIMPermissionTx(tx *dbr.Tx, key, channelUID, memberUID, action string, now int64) error {
	_, err := tx.InsertBySql(`INSERT INTO partner_im_permission_outbox(idempotency_key,channel_uid,member_uid,action,status,attempts,next_retry_at,last_error,created_at,updated_at)
 VALUES(?,?,?,?,0,0,0,'',?,?) ON DUPLICATE KEY UPDATE channel_uid=VALUES(channel_uid),member_uid=VALUES(member_uid),action=VALUES(action),status=IF(status=1,1,0),next_retry_at=0,updated_at=VALUES(updated_at)`, key, channelUID, memberUID, action, now, now).Exec()
	return err
}
func (d *db) pendingIMPermissionTasks(now int64, limit int) ([]partnerIMPermissionTask, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	var rows []partnerIMPermissionTask
	_, err := d.session.SelectBySql(`SELECT id,channel_uid,member_uid,action,attempts FROM partner_im_permission_outbox WHERE status IN (0,2) AND next_retry_at<=? ORDER BY id ASC LIMIT ?`, now, limit).Load(&rows)
	return rows, err
}
func (d *db) markIMPermissionDoneByKeys(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := d.session.Update("partner_im_permission_outbox").
		Set("status", 1).
		Set("last_error", "").
		Set("updated_at", time.Now().UnixMilli()).
		Where("idempotency_key IN ?", keys).
		Exec()
	return err
}

func (d *db) markIMPermissionDone(id int64) error {
	_, err := d.session.Update("partner_im_permission_outbox").Set("status", 1).Set("updated_at", time.Now().UnixMilli()).Where("id=?", id).Exec()
	return err
}
func (d *db) markIMPermissionRetry(id int64, attempts int, reason string) error {
	reason = truncatePartnerRunes(reason, 500)
	delay := time.Duration(1<<uint(minPartnerInt(attempts, 10))) * time.Second
	_, err := d.session.Update("partner_im_permission_outbox").Set("status", 2).Set("attempts", attempts+1).Set("next_retry_at", time.Now().Add(delay).UnixMilli()).Set("last_error", reason).Set("updated_at", time.Now().UnixMilli()).Where("id=?", id).Exec()
	return err
}
func minPartnerInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (d *db) enqueuePendingPermissionRepairs(pairs []pendingContactPair, now int64) error {
	if len(pairs) == 0 {
		return nil
	}
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	var sql strings.Builder
	sql.WriteString(`INSERT INTO partner_im_permission_outbox(idempotency_key,channel_uid,member_uid,action,status,attempts,next_retry_at,last_error,created_at,updated_at) VALUES `)
	args := make([]interface{}, 0, len(pairs)*12)
	rowCount := 0
	appendRow := func(key, channelUID, memberUID, action string) {
		if rowCount > 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("(?,?,?,?,0,0,0,'',?,?)")
		args = append(args, key, channelUID, memberUID, action, now, now)
		rowCount++
	}
	for _, pair := range pairs {
		requesterUID := strings.TrimSpace(pair.RequesterUID)
		receiverUID := strings.TrimSpace(pair.ReceiverUID)
		if requesterUID == "" || receiverUID == "" || requesterUID == receiverUID {
			continue
		}
		appendRow("repair:pending:add:"+requesterUID+":"+receiverUID, requesterUID, receiverUID, "add")
		appendRow("repair:pending:remove:"+receiverUID+":"+requesterUID, receiverUID, requesterUID, "remove")
	}
	if rowCount == 0 {
		return nil
	}
	sql.WriteString(` ON DUPLICATE KEY UPDATE channel_uid=VALUES(channel_uid),member_uid=VALUES(member_uid),action=VALUES(action),status=0,attempts=0,next_retry_at=0,last_error='',updated_at=VALUES(updated_at)`)
	_, err := d.session.InsertBySql(sql.String(), args...).Exec()
	return err
}

func (d *db) cleanupPartnerOperationalRows(now int64) error {
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	permissionCutoff := now - int64(30*24*time.Hour/time.Millisecond)
	if _, err := d.session.DeleteBySql(`DELETE FROM partner_im_permission_outbox WHERE status=1 AND updated_at<? ORDER BY updated_at ASC,id ASC LIMIT 10000`, permissionCutoff).Exec(); err != nil {
		return err
	}
	messageCutoff := now - int64(30*24*time.Hour/time.Millisecond)
	if _, err := d.session.DeleteBySql(`DELETE FROM partner_pending_message WHERE status IN (1,2) AND updated_at<? ORDER BY updated_at ASC,id ASC LIMIT 10000`, messageCutoff).Exec(); err != nil {
		return err
	}
	// partner_exposures retains the durable aggregate; raw interaction events are
	// bounded to 30 days so high-volume swipes do not grow this table forever.
	// At 5,000 concurrent users, 200,000 deletions per hour can be slower than the
	// insert rate. Run many small indexed batches but stop after a short time budget
	// so maintenance keeps pace without one oversized transaction monopolizing MySQL.
	cleanupDeadline := time.Now().Add(20 * time.Second)
	for batch := 0; batch < 50 && time.Now().Before(cleanupDeadline); batch++ {
		result, err := d.session.DeleteBySql(`DELETE FROM partner_exposure_events WHERE event_at<? ORDER BY event_at ASC,id ASC LIMIT 20000`, messageCutoff).Exec()
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected < 20000 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	dayCutoff := greetingDayKey(now - int64(30*24*time.Hour/time.Millisecond))
	if _, err := d.session.DeleteBySql(`DELETE FROM partner_greeting_daily_target WHERE day_key<? ORDER BY day_key ASC,id ASC LIMIT 10000`, dayCutoff).Exec(); err != nil {
		return err
	}
	_, err := d.session.DeleteBySql(`DELETE FROM partner_greeting_daily_usage WHERE day_key<? ORDER BY day_key ASC,id ASC LIMIT 10000`, dayCutoff).Exec()
	return err
}
