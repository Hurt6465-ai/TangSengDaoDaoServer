package partners

import (
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
	if len(source) > 16 {
		source = source[:16]
	}
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
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid<>? AND pp.status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''
		` + locationWhere + `
		ORDER BY IFNULL(pe.last_seen_at,0) ASC, IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.updated_at DESC
		LIMIT ? OFFSET ?`

	orderedArgs := make([]interface{}, 0, len(selectDistanceArgs)+len(whereDistanceArgs)+9)
	orderedArgs = append(orderedArgs, selectDistanceArgs...)
	orderedArgs = append(orderedArgs, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID)
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
	sql := `SELECT pp.uid
		FROM partner_profiles pp
		LEFT JOIN (
			SELECT to_uid, 1 AS follow FROM friend WHERE uid=? AND is_deleted=0
		) fr ON fr.to_uid=pp.uid
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid<>? AND pp.status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND IFNULL(fr.follow,0)=0
		  AND IFNULL(pg.last_greet_at,0)=0
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''
		ORDER BY IFNULL(pp.online,0) DESC, IFNULL(pp.last_active_at,0) DESC, pp.updated_at DESC
		LIMIT ?`
	var uids []string
	_, err := d.session.SelectBySql(sql, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, limit).Load(&uids)
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
		LEFT JOIN partner_greetings pg ON pg.uid=? AND pg.to_uid=pp.uid
		LEFT JOIN partner_contacts pc ON pc.uid=? AND pc.to_uid=pp.uid
		LEFT JOIN user_setting bs1 ON bs1.uid=? AND bs1.to_uid=pp.uid
		LEFT JOIN user_setting bs2 ON bs2.uid=pp.uid AND bs2.to_uid=?
		WHERE pp.uid IN ? AND pp.uid<>? AND pp.status=1 AND pp.has_photo=1
		  AND IFNULL(bs1.blacklist,0)=0 AND IFNULL(bs2.blacklist,0)=0
		  AND IFNULL(pp.profile_images,'')<>'' AND IFNULL(pp.profile_images,'')<>'[]'
		  AND IFNULL(pp.native_languages,'')<>'' AND IFNULL(pp.learning_languages,'')<>''`

	orderedArgs := make([]interface{}, 0, len(selectDistanceArgs)+8)
	orderedArgs = append(orderedArgs, selectDistanceArgs...)
	orderedArgs = append(orderedArgs, loginUID, loginUID, loginUID, loginUID, loginUID, loginUID, uids, loginUID)
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
		_, err = tx.InsertBySql(`INSERT INTO partner_exposures(uid,to_uid,seen_count,last_seen_at,created_at,updated_at)
            VALUES(?,?,1,?,NOW(),NOW())
            ON DUPLICATE KEY UPDATE seen_count=seen_count+1,last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at)),updated_at=NOW()`, loginUID, toUID, seenAt).Exec()
		if err != nil {
			return err
		}
	}
	return tx.Commit()
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

func (d *db) greetingStats(uid, toUID string, now int64) (*greetingStats, error) {
	stats := &greetingStats{}
	dayStart := now - int64(24*time.Hour/time.Millisecond)
	hourStart := now - int64(time.Hour/time.Millisecond)
	err := d.session.Select("COUNT(*)").From("partner_greetings").Where("uid=? AND last_greet_at>=?", uid, hourStart).LoadOne(&stats.HourCount)
	if err != nil {
		return nil, err
	}
	err = d.session.Select("COUNT(*)").From("partner_greetings").Where("uid=? AND last_greet_at>=?", uid, dayStart).LoadOne(&stats.DayCount)
	if err != nil {
		return nil, err
	}
	var last int64
	err = d.session.Select("IFNULL(MAX(last_greet_at),0)").From("partner_greetings").Where("uid=? AND to_uid=?", uid, toUID).LoadOne(&last)
	if err != nil {
		return nil, err
	}
	stats.LastTargetGreetAt = last
	return stats, nil
}

func (d *db) recordGreeting(uid, toUID, text, source string) (*GreetingResp, error) {
	now := time.Now().UnixMilli()
	_, err := d.session.InsertBySql(`INSERT INTO partner_greetings(uid,to_uid,text,source,greet_count,last_greet_at,created_at,updated_at)
        VALUES(?,?,?,?,1,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE text=VALUES(text),source=VALUES(source),greet_count=greet_count+1,last_greet_at=VALUES(last_greet_at),updated_at=NOW()`, uid, toUID, text, source, now).Exec()
	if err != nil {
		return nil, err
	}
	return &GreetingResp{Status: 200, ToUID: toUID, TargetUID: toUID, LastGreetAt: now, HelloSent: 1, GreetingStatus: 1, ContactStatus: PartnerContactStatusPending, RequesterMsgCount: 1, MaxGreetingCount: MaxPendingGreetingMessages, Text: text, Msg: "已打招呼"}, nil
}

type greetingStats struct {
	HourCount         int
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
	_, err := d.session.UpdateBySql(`INSERT INTO partner_profiles(uid,name,username,sex,birthday,intro,country_code,country,native_languages,learning_languages,tags,profile_cover,profile_images,vercode,has_photo,profile_score,status,last_active_at,created_at,updated_at)
		SELECT u.uid,IFNULL(u.name,''),IFNULL(u.username,''),IFNULL(u.sex,0),IFNULL(u.birthday,''),IFNULL(u.intro,''),IFNULL(u.country_code,''),IFNULL(u.country,''),IFNULL(u.native_languages,''),IFNULL(u.learning_languages,''),IFNULL(u.tags,''),IFNULL(u.profile_cover,''),IFNULL(u.profile_images,''),IFNULL(u.vercode,''),
		IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',1,0) AS has_photo,
		(IF(IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]',20,0)+IF(IFNULL(u.native_languages,'')<>'',10,0)+IF(IFNULL(u.learning_languages,'')<>'',10,0)+IF(IFNULL(u.intro,'')<>'',5,0)+IF(IFNULL(u.country_code,'')<>'',5,0)) AS profile_score,
		IF(u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' AND IFNULL(u.category,'') NOT IN ('system','customerService'),1,0) AS status,
		GREATEST(UNIX_TIMESTAMP(IFNULL(u.updated_at,NOW()))*1000,UNIX_TIMESTAMP(IFNULL(u.created_at,NOW()))*1000),NOW(),NOW()
		FROM user u WHERE u.uid=?
		ON DUPLICATE KEY UPDATE name=VALUES(name),username=VALUES(username),sex=VALUES(sex),birthday=VALUES(birthday),intro=VALUES(intro),country_code=VALUES(country_code),country=VALUES(country),native_languages=VALUES(native_languages),learning_languages=VALUES(learning_languages),tags=VALUES(tags),profile_cover=VALUES(profile_cover),profile_images=VALUES(profile_images),vercode=VALUES(vercode),has_photo=VALUES(has_photo),profile_score=VALUES(profile_score),status=VALUES(status),last_active_at=GREATEST(IFNULL(last_active_at,0),VALUES(last_active_at)),updated_at=NOW()`, uid).Exec()
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
	_, err := d.session.Update("partner_contacts").
		Set("requester_msg_count", dbr.Expr("LEAST(IFNULL(requester_msg_count,0)+1, ?)", MaxPendingGreetingMessages)).
		Set("last_msg_at", now).
		Set("updated_at", now).
		Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=?", uid, toUID, toUID, uid, PartnerContactStatusPending, uid).
		Exec()
	if err != nil {
		return 0, err
	}
	contact, err := d.getPartnerContact(uid, toUID)
	if err != nil || contact == nil {
		return 0, err
	}
	return contact.RequesterMsgCount, nil
}

func (d *db) ensurePendingContact(uid, toUID string, now int64) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	rows := [][2]string{{uid, toUID}, {toUID, uid}}
	for _, row := range rows {
		_, err := d.session.InsertBySql(`INSERT INTO partner_contacts(uid,to_uid,requester_uid,status,requester_msg_count,last_msg_at,created_at,updated_at)
            VALUES(?,?,?,?,1,?,?,?)
            ON DUPLICATE KEY UPDATE requester_uid=IF(status IN (2,3),requester_uid,VALUES(requester_uid)),status=IF(status IN (1,2,3),status,VALUES(status)),requester_msg_count=GREATEST(requester_msg_count,VALUES(requester_msg_count)),last_msg_at=GREATEST(last_msg_at,VALUES(last_msg_at)),updated_at=VALUES(updated_at)`, row[0], row[1], uid, PartnerContactStatusPending, now, now, now).Exec()
		if err != nil {
			return err
		}
	}
	return nil
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
	var requester string
	err := d.session.Select("requester_uid").From("partner_contacts").Where("uid=? AND to_uid=? AND status=?", fromUID, toUID, PartnerContactStatusPending).LoadOne(&requester)
	if err != nil {
		return false, nil
	}
	if requester == "" {
		return false, nil
	}
	if requester != fromUID {
		_, err = d.session.Update("partner_contacts").Set("status", PartnerContactStatusActive).Set("last_msg_at", at).Set("updated_at", at).Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=?", fromUID, toUID, toUID, fromUID, PartnerContactStatusPending).Exec()
		return err == nil, err
	}
	_, err = d.session.Update("partner_contacts").Set("last_msg_at", at).Set("updated_at", at).Where("(uid=? AND to_uid=?) OR (uid=? AND to_uid=?)", fromUID, toUID, toUID, fromUID).Exec()
	return false, err
}
