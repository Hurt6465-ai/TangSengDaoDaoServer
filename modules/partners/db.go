package partners

import (
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

type locationModel struct {
	UID       string  `db:"uid"`
	Lat       float64 `db:"lat"`
	Lng       float64 `db:"lng"`
	Geohash   string  `db:"geohash"`
	UpdatedAt int64   `db:"updated_at_ms"`
	ExpiresAt int64   `db:"expires_at"`
}

func (d *db) upsertLocation(uid string, lat, lng float64) (*locationModel, error) {
	now := time.Now().UnixMilli()
	expires := now + LocationTTLMillis
	geohash := roughGeoHash(lat, lng)
	_, err := d.session.InsertBySql(`INSERT INTO partner_locations(uid,lat,lng,geohash,updated_at_ms,expires_at,created_at,updated_at)
        VALUES(?,?,?,?,?,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE lat=VALUES(lat),lng=VALUES(lng),geohash=VALUES(geohash),updated_at_ms=VALUES(updated_at_ms),expires_at=VALUES(expires_at),updated_at=NOW()`,
		uid, lat, lng, geohash, now, expires).Exec()
	if err != nil {
		return nil, err
	}
	return &locationModel{UID: uid, Lat: lat, Lng: lng, Geohash: geohash, UpdatedAt: now, ExpiresAt: expires}, nil
}

func (d *db) getLocation(uid string) (*locationModel, error) {
	if uid == "" {
		return nil, nil
	}
	var model *locationModel
	_, err := d.session.Select("uid", "lat", "lng", "geohash", "updated_at_ms", "expires_at").From("partner_locations").Where("uid=? and expires_at>?", uid, time.Now().UnixMilli()).Load(&model)
	if err != nil {
		return nil, err
	}
	return model, nil
}

func (d *db) list(loginUID string, req listReq) ([]*PartnerUser, int, error) {
	limit := clampLimit(req.Limit)
	offset := req.Offset()
	viewerLoc := req.Location
	if viewerLoc == nil && req.UseLoginLocation {
		loc, _ := d.getLocation(loginUID)
		viewerLoc = loc
	}

	args := make([]interface{}, 0)
	distanceExpr := "0"
	distanceSelect := "0 AS distance_meters"
	locationJoin := ""
	locationWhere := ""
	if viewerLoc != nil && validLatLng(viewerLoc.Lat, viewerLoc.Lng) {
		distanceExpr = `(6371000 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(pl.lat - ?)/2),2)+COS(RADIANS(?))*COS(RADIANS(pl.lat))*POWER(SIN(RADIANS(pl.lng - ?)/2),2))))`
		distanceSelect = distanceExpr + " AS distance_meters"
		args = append(args, viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng)
		locationJoin = " LEFT JOIN partner_locations pl ON pl.uid=u.uid AND pl.expires_at>UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 "
		if req.NearbyOnly {
			locationWhere = " AND pl.uid IS NOT NULL AND " + distanceExpr + " <= ? "
			args = append(args, viewerLoc.Lat, viewerLoc.Lat, viewerLoc.Lng, req.RadiusMeters())
		}
	} else {
		locationJoin = " LEFT JOIN partner_locations pl ON pl.uid=u.uid AND 1=0 "
	}

	sql := `SELECT u.uid,u.name,u.username,'' AS avatar,u.sex,u.intro,u.country_code,u.country,u.native_languages,u.learning_languages,u.birthday,u.tags,u.profile_cover,u.profile_images,u.vercode,
        IFNULL(fr.follow,0) AS follow,
        IFNULL(onl.online,0) AS online,
        IFNULL(onl.last_offline,0) AS last_offline,
        IFNULL(onl.last_active_at,0) AS last_active_at,
        UNIX_TIMESTAMP(u.created_at) AS created_at_unix,
        UNIX_TIMESTAMP(u.updated_at) AS updated_at_unix,
        ` + distanceSelect + `
        FROM user u
        LEFT JOIN (
            SELECT uid, MAX(online) AS online, MAX(last_offline) AS last_offline, MAX(GREATEST(last_online,last_offline)) * 1000 AS last_active_at
            FROM user_online GROUP BY uid
        ) onl ON onl.uid=u.uid
        LEFT JOIN (
            SELECT to_uid, 1 AS follow FROM friend WHERE uid=? AND is_deleted=0
        ) fr ON fr.to_uid=u.uid
        ` + locationJoin + `
        WHERE u.uid<>? AND u.status=1 AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.bench_no,'')='' 
          AND IFNULL(u.category,'') NOT IN ('system','customerService')
          AND IFNULL(u.profile_images,'')<>'' AND IFNULL(u.profile_images,'')<>'[]'
          AND IFNULL(u.native_languages,'')<>'' AND IFNULL(u.learning_languages,'')<>''
        ` + locationWhere + `
        ORDER BY IFNULL(onl.online,0) DESC, IFNULL(onl.last_active_at,0) DESC, u.updated_at DESC
        LIMIT ? OFFSET ?`
	args = append([]interface{}{}, args...)
	// Insert friend/login args before WHERE placeholders. The distance args must remain at the beginning for SELECT expression.
	prefixLen := 0
	if viewerLoc != nil && validLatLng(viewerLoc.Lat, viewerLoc.Lng) {
		// SQL placeholder order: SELECT distance(3), friend/login(2), optional WHERE distance(4), limit/offset.
		prefixLen = 3
	}
	orderedArgs := make([]interface{}, 0, len(args)+4)
	orderedArgs = append(orderedArgs, args[:prefixLen]...)
	orderedArgs = append(orderedArgs, loginUID, loginUID)
	if len(args) > prefixLen {
		orderedArgs = append(orderedArgs, args[prefixLen:]...)
	}
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

func (d *db) recordGreeting(uid, toUID string) error {
	if uid == "" || toUID == "" || uid == toUID {
		return nil
	}
	now := time.Now().UnixMilli()
	_, err := d.session.InsertBySql(`INSERT INTO partner_greetings(uid,to_uid,greet_count,last_greet_at,created_at,updated_at)
        VALUES(?,?,1,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE greet_count=greet_count+1,last_greet_at=VALUES(last_greet_at),updated_at=NOW()`, uid, toUID, now).Exec()
	return err
}

type listReq struct {
	Page             int
	Limit            int
	Cursor           string
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
	// 轻量粗格子：不引入 geohash 依赖。查询先靠经纬度边界/距离表达式，后续可升级 geohash。
	return strconv.Itoa(int((lat+90)*10)) + ":" + strconv.Itoa(int((lng+180)*10))
}
