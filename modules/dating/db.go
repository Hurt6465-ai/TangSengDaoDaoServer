package dating

import (
	"crypto/sha256"
	"fmt"
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

type sharedUserRow struct {
	UID               string `db:"uid"`
	Name              string `db:"name"`
	Username          string `db:"username"`
	Sex               int    `db:"sex"`
	Birthday          string `db:"birthday"`
	Intro             string `db:"intro"`
	CountryCode       string `db:"country_code"`
	Country           string `db:"country"`
	NativeLanguages   string `db:"native_languages"`
	LearningLanguages string `db:"learning_languages"`
	Tags              string `db:"tags"`
	Online            int    `db:"online"`
	LastActiveAt      int64  `db:"last_active_at"`
}

func (d *db) getSharedUser(uid string) (*sharedUserRow, error) {
	var row *sharedUserRow
	_, err := d.session.Select("uid", "IFNULL(name,'') name", "IFNULL(username,'') username", "IFNULL(sex,-1) sex", "IFNULL(birthday,'') birthday", "IFNULL(intro,'') intro", "IFNULL(country_code,'') country_code", "IFNULL(country,'') country", "IFNULL(native_languages,'') native_languages", "IFNULL(learning_languages,'') learning_languages", "IFNULL(tags,'') tags",
		"IFNULL((SELECT MAX(uo.online) FROM user_online uo WHERE uo.uid=user.uid),0) online",
		"IFNULL((SELECT MAX(GREATEST(uo.last_online,uo.last_offline))*1000 FROM user_online uo WHERE uo.uid=user.uid),0) last_active_at").
		From("user").Where("uid=? AND IFNULL(is_destroy,0)=0", uid).Load(&row)
	return row, err
}

func splitSharedTags(raw string) (relationship, jobStatus, education string, personality, pets, sports, movies []string) {
	for _, tag := range parseStringList(raw, 100) {
		lower := strings.ToLower(strings.TrimSpace(tag))
		switch {
		case strings.HasPrefix(lower, "relationship_"):
			if relationship == "" {
				relationship = tag
			}
		case strings.HasPrefix(lower, "job_"):
			if jobStatus == "" {
				jobStatus = tag
			}
		case strings.HasPrefix(lower, "education_"):
			if education == "" {
				education = tag
			}
		case strings.HasPrefix(lower, "personality_"):
			personality = append(personality, tag)
		case strings.HasPrefix(lower, "pet_"):
			pets = append(pets, tag)
		case strings.HasPrefix(lower, "sport_"):
			sports = append(sports, tag)
		case strings.HasPrefix(lower, "movie_"):
			movies = append(movies, tag)
		}
	}
	return relationship, jobStatus, education,
		compactStringList(personality, 10), compactStringList(pets, 10), compactStringList(sports, 10), compactStringList(movies, 10)
}

// syncSharedFromUser keeps account-owned fields sourced from TangSengDaoDao user data.
// Dating-only fields (photos, intent, bio, preferences) are not overwritten.
func (d *db) syncSharedFromUser(uid string) error {
	user, err := d.getSharedUser(uid)
	if err != nil || user == nil {
		return err
	}
	relationship, jobStatus, education, personality, pets, sports, movies := splitSharedTags(user.Tags)
	lastActiveAt := normalizeMillis(user.LastActiveAt)
	sharedValues := []interface{}{
		safeText(user.Name, 100), safeText(user.Username, 40), normalizeSex(user.Sex), safeText(user.Birthday, 20),
		safeText(user.CountryCode, 10), safeText(user.Country, 80), user.NativeLanguages, user.LearningLanguages,
		safeText(relationship, 40), safeText(jobStatus, 80), safeText(education, 80),
		toJSONString(personality, 10), toJSONString(pets, 10), toJSONString(sports, 10), toJSONString(movies, 10),
	}

	// Insert a profile shell only once. Online/active values are initial values;
	// subsequent presence updates are handled by user/db_online.go and touchActive.
	_, err = d.session.InsertBySql(`INSERT IGNORE INTO dating_profiles(
        uid,name,username,sex,birthday,country_code,country,bio,native_languages,learning_languages,
        relationship_status,job_status,education,personality_tags,pet_tags,sport_tags,movie_tags,
        cross_border_preference,enabled,status,show_distance,allow_voice,allow_video,online,last_active_at,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'open_foreign',0,1,1,1,0,?,?,NOW(),NOW())`,
		user.UID, sharedValues[0], sharedValues[1], sharedValues[2], sharedValues[3], sharedValues[4], sharedValues[5],
		safeText(user.Intro, 500), sharedValues[6], sharedValues[7], sharedValues[8], sharedValues[9], sharedValues[10],
		sharedValues[11], sharedValues[12], sharedValues[13], sharedValues[14], user.Online, lastActiveAt).Exec()
	if err != nil {
		return err
	}

	// Refresh account-owned fields only when one of them really changed. This
	// avoids turning every profile read into a row write/lock while still making
	// account edits visible in dating profiles.
	updateArgs := append([]interface{}{}, sharedValues...)
	updateArgs = append(updateArgs, user.UID)
	updateArgs = append(updateArgs, sharedValues...)
	_, err = d.session.UpdateBySql(`UPDATE dating_profiles SET
        name=?,username=?,sex=?,birthday=?,country_code=?,country=?,native_languages=?,learning_languages=?,
        relationship_status=?,job_status=?,education=?,personality_tags=?,pet_tags=?,sport_tags=?,movie_tags=?,updated_at=NOW()
        WHERE uid=? AND (
          NOT(name<=>?) OR NOT(username<=>?) OR NOT(sex<=>?) OR NOT(birthday<=>?) OR
          NOT(country_code<=>?) OR NOT(country<=>?) OR NOT(native_languages<=>?) OR NOT(learning_languages<=>?) OR
          NOT(relationship_status<=>?) OR NOT(job_status<=>?) OR NOT(education<=>?) OR
          NOT(personality_tags<=>?) OR NOT(pet_tags<=>?) OR NOT(sport_tags<=>?) OR NOT(movie_tags<=>?)
        )`, updateArgs...).Exec()
	return err
}

func normalizeSex(v int) int {
	if v == 0 || v == 1 {
		return v
	}
	return -1
}

func profileSelect(profileAlias, userAlias string) string {
	return fmt.Sprintf(`%[1]s.uid,
        COALESCE(NULLIF(%[2]s.name,''),%[1]s.name) AS name,
        COALESCE(NULLIF(%[2]s.username,''),%[1]s.username) AS username,
        CONCAT('users/',%[1]s.uid,'/avatar') AS avatar,
        CASE WHEN %[2]s.sex IN (0,1) THEN %[2]s.sex ELSE %[1]s.sex END AS sex,
        COALESCE(NULLIF(%[2]s.birthday,''),%[1]s.birthday) AS birthday,
        %[1]s.enabled,%[1]s.intent,%[1]s.cross_border_preference,%[1]s.gender_preference,%[1]s.min_age,%[1]s.max_age,%[1]s.city,
        COALESCE(NULLIF(%[2]s.country_code,''),%[1]s.country_code) AS country_code,
        COALESCE(NULLIF(%[2]s.country,''),%[1]s.country) AS country,
        %[1]s.height_cm,%[1]s.weight_kg,%[1]s.job,%[1]s.job_status,%[1]s.education,%[1]s.relationship_status,%[1]s.sexual_orientation,%[1]s.drinking,%[1]s.smoking,
        %[1]s.bio,%[1]s.ideal_partner,
        COALESCE(NULLIF(%[2]s.native_languages,''),%[1]s.native_languages) AS native_languages,
        COALESCE(NULLIF(%[2]s.learning_languages,''),%[1]s.learning_languages) AS learning_languages,
        %[1]s.tags,%[1]s.personality_tags,%[1]s.pet_tags,%[1]s.sport_tags,%[1]s.movie_tags,%[1]s.dealbreakers,%[1]s.photos,%[1]s.card_photos,
        %[1]s.show_distance,%[1]s.allow_voice,%[1]s.allow_video,%[1]s.profile_score,%[1]s.status,
        IFNULL(%[1]s.online,0) AS online,IFNULL(%[1]s.last_active_at,0) AS last_active_at,0 AS distance_meters,
        IFNULL(%[2]s.tags,'') AS shared_tags,
        UNIX_TIMESTAMP(%[1]s.created_at) AS created_at_unix,UNIX_TIMESTAMP(%[1]s.updated_at) AS updated_at_unix`, profileAlias, userAlias)
}

func (d *db) getProfile(uid string) (*DatingProfileResp, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, nil
	}
	var profile *DatingProfileResp
	sql := "SELECT " + profileSelect("dp", "u") + " FROM dating_profiles dp JOIN user u ON u.uid=dp.uid AND IFNULL(u.is_destroy,0)=0 WHERE dp.uid=?"
	_, err := d.session.SelectBySql(sql, uid).Load(&profile)
	if err != nil {
		return nil, err
	}
	if profile != nil {
		profile.Normalize()
	}
	return profile, nil
}

func (d *db) getProfilesByUIDs(uids []string) (map[string]*DatingProfileResp, error) {
	unique := make([]string, 0, len(uids))
	seen := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		unique = append(unique, uid)
	}
	out := make(map[string]*DatingProfileResp, len(unique))
	if len(unique) == 0 {
		return out, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(unique)), ",")
	args := make([]interface{}, 0, len(unique))
	for _, uid := range unique {
		args = append(args, uid)
	}
	query := "SELECT " + profileSelect("dp", "u") +
		" FROM dating_profiles dp JOIN user u ON u.uid=dp.uid AND IFNULL(u.is_destroy,0)=0 WHERE dp.uid IN (" + placeholders + ")"
	var profiles []*DatingProfileResp
	if _, err := d.session.SelectBySql(query, args...).Load(&profiles); err != nil {
		return nil, err
	}
	for _, profile := range profiles {
		if profile == nil {
			continue
		}
		profile.Normalize()
		out[profile.UID] = profile
	}
	return out, nil
}

func (d *db) profileMe(uid string) (*DatingProfileResp, error) {
	if err := d.syncSharedFromUser(uid); err != nil {
		return nil, err
	}
	return d.getProfile(uid)
}

// profileForUse avoids rewriting shared fields on every recommendation/swipe.
// A row is synchronized only when it does not exist; ProfileMe/SaveProfile still
// perform an explicit shared-field refresh.
func (d *db) profileForUse(uid string) (*DatingProfileResp, error) {
	profile, err := d.getProfile(uid)
	if err != nil || profile != nil {
		return profile, err
	}
	if err = d.syncSharedFromUser(uid); err != nil {
		return nil, err
	}
	return d.getProfile(uid)
}

func (d *db) userExists(uid string) (bool, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return false, nil
	}
	var count int
	err := d.session.Select("COUNT(*)").From("user").Where("uid=? AND IFNULL(is_destroy,0)=0", uid).LoadOne(&count)
	return count > 0, err
}

func (d *db) saveProfile(uid string, req SaveProfileReq) (*DatingProfileResp, error) {
	photos := req.Photos
	if len(photos) == 0 {
		photos = req.ProfileImages
	}
	photosRaw := toJSONString(photos, DatingMaxPhotos)
	cardPhotos := alignCardPhotos(parseImageList(photosRaw, DatingMaxPhotos), req.CardPhotos)
	cardPhotosRaw := toJSONString(cardPhotos, DatingMaxPhotos)
	intent := safeText(req.Intent, 80)
	if intent == "" {
		intent = safeText(req.RelationshipGoal, 80)
	}
	bio := safeText(req.Bio, 500)
	if bio == "" {
		bio = safeText(req.Intro, 500)
	}
	cross := safeText(req.CrossBorderPreference, 40)
	minAge, maxAge := normalizeAgeRange(req.MinAge, req.MaxAge)
	genderPreference := req.GenderPreference
	if genderPreference < -1 || genderPreference > 1 {
		genderPreference = -1
	}
	enabled := 0
	if req.Enabled == 1 {
		enabled = 1
	}
	_, err := d.session.Update("dating_profiles").
		Set("enabled", enabled).
		Set("intent", intent).
		Set("cross_border_preference", dbr.Expr("IF(?='',cross_border_preference,?)", cross, cross)).
		Set("gender_preference", genderPreference).
		Set("min_age", minAge).
		Set("max_age", maxAge).
		Set("city", safeText(req.City, 80)).
		Set("height_cm", clampInt(req.HeightCM, 0, 260)).
		Set("weight_kg", clampInt(req.WeightKG, 0, 400)).
		Set("sexual_orientation", safeText(req.SexualOrientation, 40)).
		Set("drinking", safeText(req.Drinking, 40)).
		Set("smoking", safeText(req.Smoking, 40)).
		Set("bio", bio).
		Set("ideal_partner", safeText(req.IdealPartner, 200)).
		Set("tags", toJSONString(req.Tags, DatingMaxTags)).
		Set("dealbreakers", toJSONString(req.Dealbreakers, DatingMaxDealbreakers)).
		Set("photos", photosRaw).
		Set("card_photos", cardPhotosRaw).
		Set("has_photo", boolInt(len(parseImageList(photosRaw, DatingMaxPhotos)) > 0)).
		Set("show_distance", optionalFlag(req.ShowDistance, 1)).
		Set("allow_voice", optionalFlag(req.AllowVoice, 1)).
		Set("allow_video", optionalFlag(req.AllowVideo, 0)).
		Set("last_active_at", time.Now().UnixMilli()).
		Set("updated_at", dbr.Expr("NOW()")).
		Where("uid=?", uid).Exec()
	if err != nil {
		return nil, err
	}
	profile, err := d.getProfile(uid)
	if err == nil && profile != nil {
		_, _ = d.session.Update("dating_profiles").Set("profile_score", profile.ProfileScore).Where("uid=?", uid).Exec()
	}
	return profile, err
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func optionalFlag(value *int, fallback int) int {
	if value == nil {
		return boolInt(fallback != 0)
	}
	return boolInt(*value != 0)
}

func (d *db) setEnabled(uid string, enabled int) (*DatingProfileResp, error) {
	if enabled != 1 {
		enabled = 0
	}
	builder := d.session.Update("dating_profiles").Set("enabled", enabled).Set("last_active_at", time.Now().UnixMilli()).Set("updated_at", dbr.Expr("NOW()")).Where("uid=?", uid)
	if enabled == 1 {
		builder = builder.Where("status=?", DatingProfileNormal)
	}
	if _, err := builder.Exec(); err != nil {
		return nil, err
	}
	return d.getProfile(uid)
}

func (d *db) upsertLocation(uid string, req LocationReq) (*DatingProfileResp, error) {
	if err := d.syncSharedFromUser(uid); err != nil {
		return nil, err
	}
	lat, lng := req.NormalizedLatLng()
	now := time.Now().UnixMilli()
	expires := now + DatingLocationTTLMS
	radius := req.RadiusMeters
	if radius <= 0 || radius > DatingRadiusMeters {
		radius = DatingRadiusMeters
	}
	if req.ExpiresDays > 0 && req.ExpiresDays <= 60 {
		expires = now + int64(req.ExpiresDays)*int64(24*time.Hour/time.Millisecond)
	}
	update := d.session.Update("dating_profiles").Set("lat", lat).Set("lng", lng).Set("accuracy", req.Accuracy).
		Set("radius_meters", radius).Set("location_updated_at", now).Set("expires_at", expires).
		Set("last_active_at", now).Set("updated_at", dbr.Expr("updated_at"))
	if city := safeText(req.City, 80); city != "" {
		update = update.Set("city", city)
	}
	if countryCode := safeText(strings.ToUpper(req.CountryCode), 10); countryCode != "" {
		update = update.Set("country_code", countryCode)
	}
	_, err := update.Where("uid=?", uid).Exec()
	if err != nil {
		return nil, err
	}
	return d.getProfile(uid)
}

type datingLocation struct {
	Lat float64 `db:"lat"`
	Lng float64 `db:"lng"`
}

func (d *db) getDatingLocation(uid string) (*datingLocation, error) {
	var loc *datingLocation
	_, err := d.session.Select("lat", "lng").From("dating_profiles").Where("uid=? AND expires_at>?", uid, time.Now().UnixMilli()).Load(&loc)
	return loc, err
}

func (d *db) recommend(loginUID string, viewer *DatingProfileResp, req RecommendReq) ([]*DatingProfileResp, string, int, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = DefaultDatingLimit
	}
	if limit > MaxDatingLimit {
		limit = MaxDatingLimit
	}
	batchSize := limit * 6
	if batchSize < 80 {
		batchSize = 80
	}
	if batchSize > 200 {
		batchSize = 200
	}
	const maxScan = 1200

	distanceSelect := "0 AS distance_meters"
	whereLocation := ""
	argsPrefix := []interface{}{}
	argsLocation := []interface{}{}
	loc, _ := d.getDatingLocation(loginUID)
	if loc != nil && validLatLng(loc.Lat, loc.Lng) {
		distanceExpr := `IF(dp.expires_at>? AND dp.lat<>0 AND dp.lng<>0, IFNULL(CAST((6371000 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(dp.lat - ?)/2),2)+COS(RADIANS(?))*COS(RADIANS(dp.lat))*POWER(SIN(RADIANS(dp.lng - ?)/2),2)))) AS UNSIGNED),0),0)`
		distanceSelect = distanceExpr + " AS distance_meters"
		argsPrefix = append(argsPrefix, time.Now().UnixMilli(), loc.Lat, loc.Lat, loc.Lng)
		if normalizeScope(req.Scope) == DatingScopeNearby {
			minLat, maxLat, minLng, maxLng := latLngBounds(loc.Lat, loc.Lng, DatingRadiusMeters)
			whereLocation = " AND dp.expires_at>? AND dp.lat BETWEEN ? AND ? AND dp.lng BETWEEN ? AND ? AND " + distanceExpr + " <= ? "
			argsLocation = append(argsLocation, time.Now().UnixMilli(), minLat, maxLat, minLng, maxLng,
				time.Now().UnixMilli(), loc.Lat, loc.Lat, loc.Lng, DatingRadiusMeters)
		}
	} else if normalizeScope(req.Scope) == DatingScopeNearby {
		return []*DatingProfileResp{}, "", 0, nil
	}

	sessionID := safeText(req.SessionID, 80)
	if sessionID == "" {
		sessionID = "default"
	}
	activeAfter := time.Now().UnixMilli() - DatingActiveWindowMS
	desiredSex := viewer.GenderPreference
	switch strings.ToLower(strings.TrimSpace(req.Gender)) {
	case "female", "woman", "0":
		desiredSex = 0
	case "male", "man", "1":
		desiredSex = 1
	}
	effectiveAgeMin, effectiveAgeMax := viewer.MinAge, viewer.MaxAge
	if req.AgeMin > effectiveAgeMin {
		effectiveAgeMin = req.AgeMin
	}
	if req.AgeMax > 0 && req.AgeMax < effectiveAgeMax {
		effectiveAgeMax = req.AgeMax
	}
	if effectiveAgeMin < 18 {
		effectiveAgeMin = 18
	}
	if effectiveAgeMax > 99 {
		effectiveAgeMax = 99
	}
	if effectiveAgeMax < effectiveAgeMin {
		return []*DatingProfileResp{}, "", 0, nil
	}

	intentFilter := safeText(req.Intent, 80)
	intentWhere := ""
	intentArgs := make([]interface{}, 0, 2)
	switch intentFilter {
	case "":
	case DatingIntentFilterSerious:
		intentWhere = " AND dp.intent IN (?,?) "
		intentArgs = append(intentArgs, DatingIntentLongTerm, DatingIntentLongTermOpenShort)
	case DatingIntentFilterMarriage:
		intentWhere = " AND dp.intent=? "
		intentArgs = append(intentArgs, DatingIntentLongTerm)
	default:
		normalized, ok := normalizeDatingIntent(intentFilter)
		if !ok {
			return []*DatingProfileResp{}, "", 0, ErrDatingInvalidIntent
		}
		intentWhere = " AND dp.intent=? "
		intentArgs = append(intentArgs, normalized)
	}

	targetCountryExpr := "UPPER(TRIM(COALESCE(NULLIF(u.country_code,''),dp.country_code)))"
	targetRejectExpr := `(LOWER(TRIM(dp.cross_border_preference)) IN ('same_country','same-country','local_only','nearby_only','no_foreign','refuse_foreign')
        OR dp.cross_border_preference LIKE '%只接受本国%' OR dp.cross_border_preference LIKE '%拒绝异国%' OR dp.cross_border_preference LIKE '%本国恋%')`
	viewerCountry := strings.ToUpper(strings.TrimSpace(viewer.CountryCode))
	countryWhere := ""
	countryArgs := make([]interface{}, 0, 2)
	countryMode := strings.ToLower(strings.TrimSpace(req.CountryMode))
	if viewer.RejectsCrossBorder() || countryMode == "same_country" || countryMode == "same-country" || countryMode == "local_only" {
		if viewerCountry == "" {
			return []*DatingProfileResp{}, "", 0, nil
		}
		countryWhere = " AND " + targetCountryExpr + "=? "
		countryArgs = append(countryArgs, viewerCountry)
	} else if countryMode == "foreign_open" || countryMode == "foreign" || countryMode == "cross_border" {
		if viewerCountry == "" {
			return []*DatingProfileResp{}, "", 0, nil
		}
		countryWhere = " AND " + targetCountryExpr + "<>'' AND " + targetCountryExpr + "<>? AND NOT " + targetRejectExpr + " "
		countryArgs = append(countryArgs, viewerCountry)
	} else if viewerCountry == "" {
		countryWhere = " AND NOT " + targetRejectExpr + " "
	} else {
		countryWhere = " AND NOT (" + targetRejectExpr + " AND (" + targetCountryExpr + "='' OR " + targetCountryExpr + "<>?)) "
		countryArgs = append(countryArgs, viewerCountry)
	}

	laneWhere := ""
	if req.FreshOnly {
		laneWhere += " AND dp.created_at>=DATE_SUB(NOW(),INTERVAL 30 DAY) AND dp.profile_score>=40 "
	}
	if req.ExploreOnly {
		laneWhere += " AND dp.profile_score>=35 "
	}
	excludeWhere := ""
	excludeArgs := make([]interface{}, 0, len(req.ExcludeUIDs))
	excludeUIDs := compactStringList(req.ExcludeUIDs, 100)
	if len(excludeUIDs) > 0 {
		excludeWhere = " AND dp.uid NOT IN (" + strings.TrimSuffix(strings.Repeat("?,", len(excludeUIDs)), ",") + ") "
		for _, excludedUID := range excludeUIDs {
			excludeArgs = append(excludeArgs, excludedUID)
		}
	}

	cursor, cursorOK := decodeRecommendCursor(req.Cursor)
	if !cursorOK || req.FreshOnly || req.ExploreOnly {
		// Rolling upgrades may send the timestamp cursor produced by older builds.
		// Reserved exploration lanes are intentionally cursorless and rely on served.
		cursor = nil
	}
	cursorWhere := ""
	cursorArgs := make([]interface{}, 0, 14)
	if cursor != nil {
		cursorWhere = ` AND (
			dp.online < ? OR
			(dp.online=? AND dp.last_active_at<?) OR
			(dp.online=? AND dp.last_active_at=? AND dp.profile_score<?) OR
			(dp.online=? AND dp.last_active_at=? AND dp.profile_score=? AND UNIX_TIMESTAMP(dp.updated_at)<?) OR
			(dp.online=? AND dp.last_active_at=? AND dp.profile_score=? AND UNIX_TIMESTAMP(dp.updated_at)=? AND dp.uid>?)
		) `
		cursorArgs = append(cursorArgs,
			cursor.Online,
			cursor.Online, cursor.LastActiveAt,
			cursor.Online, cursor.LastActiveAt, cursor.ProfileScore,
			cursor.Online, cursor.LastActiveAt, cursor.ProfileScore, cursor.UpdatedAt,
			cursor.Online, cursor.LastActiveAt, cursor.ProfileScore, cursor.UpdatedAt, cursor.UID,
		)
	}

	orderBy := "dp.online DESC,dp.last_active_at DESC,dp.profile_score DESC,dp.updated_at DESC,dp.uid ASC"
	if req.ExploreOnly {
		orderBy = "dp.exposure_count ASC,dp.last_active_at DESC,dp.profile_score DESC,dp.uid ASC"
	}
	selectColumns := strings.Replace(profileSelect("dp", "u"), "0 AS distance_meters", distanceSelect, 1)
	sql := `SELECT ` + selectColumns + `
        FROM dating_profiles dp
        JOIN user u ON u.uid=dp.uid AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.status,1)=1
        LEFT JOIN dating_swipes ds ON ds.uid=? AND ds.to_uid=dp.uid
        LEFT JOIN dating_favorites df ON df.uid=? AND df.to_uid=dp.uid
        LEFT JOIN dating_blocks b1 ON b1.uid=? AND b1.to_uid=dp.uid
        LEFT JOIN dating_blocks b2 ON b2.uid=dp.uid AND b2.to_uid=?
        LEFT JOIN dating_matches dm ON dm.pair_key=IF(dp.uid<?,CONCAT(dp.uid,':',?),CONCAT(?,':',dp.uid)) AND dm.status IN (1,2,3)
        LEFT JOIN dating_served served ON served.uid=? AND served.session_id=? AND served.to_uid=dp.uid AND served.expires_at>?
        LEFT JOIN dating_exposures de ON de.uid=? AND de.to_uid=dp.uid AND de.last_seen_at>?
        LEFT JOIN user_setting us1 ON us1.uid=? AND us1.to_uid=dp.uid
        LEFT JOIN user_setting us2 ON us2.uid=dp.uid AND us2.to_uid=?
        WHERE dp.uid<>? AND dp.enabled=1 AND dp.status=1 AND dp.has_photo=1
          AND IFNULL(u.robot,0)=0 AND IFNULL(u.category,'') NOT IN ('system','customerService') AND IFNULL(u.bench_no,'')=''
          AND dp.last_active_at>=? AND de.uid IS NULL
          AND CASE WHEN u.sex IN (0,1) THEN u.sex ELSE dp.sex END IN (0,1)
          AND (?<0 OR CASE WHEN u.sex IN (0,1) THEN u.sex ELSE dp.sex END=?)
          AND (dp.gender_preference<0 OR dp.gender_preference=?)
          AND TIMESTAMPDIFF(YEAR,STR_TO_DATE(COALESCE(NULLIF(u.birthday,''),dp.birthday),'%Y-%m-%d'),CURDATE()) BETWEEN ? AND ?
          AND ? BETWEEN dp.min_age AND dp.max_age
          ` + intentWhere + countryWhere + laneWhere + excludeWhere + `
          AND ds.uid IS NULL AND df.uid IS NULL AND b1.uid IS NULL AND b2.uid IS NULL
          AND dm.match_id IS NULL AND served.uid IS NULL
          AND IFNULL(us1.blacklist,0)=0 AND IFNULL(us2.blacklist,0)=0
          AND IFNULL(dp.photos,'')<>'' AND IFNULL(dp.photos,'')<>'[]'
          ` + cursorWhere + whereLocation + `
        ORDER BY ` + orderBy + `
        LIMIT ? OFFSET ?`

	baseArgs := make([]interface{}, 0, 52)
	baseArgs = append(baseArgs, argsPrefix...)
	baseArgs = append(baseArgs,
		loginUID, loginUID, loginUID, loginUID,
		loginUID, loginUID, loginUID,
		loginUID, sessionID, time.Now().UnixMilli(),
		loginUID, time.Now().UnixMilli()-DatingExposureCooldownMS,
		loginUID, loginUID, loginUID, activeAfter,
		desiredSex, desiredSex, viewer.Sex,
		effectiveAgeMin, effectiveAgeMax, viewer.Age,
	)
	baseArgs = append(baseArgs, intentArgs...)
	baseArgs = append(baseArgs, countryArgs...)
	baseArgs = append(baseArgs, excludeArgs...)
	baseArgs = append(baseArgs, cursorArgs...)
	baseArgs = append(baseArgs, argsLocation...)

	filtered := make([]*DatingProfileResp, 0, limit+1)
	filteredCursors := make([]string, 0, limit+1)
	offset := 0
	exhausted := false
	lastScannedCursor := ""
	for len(filtered) < limit+1 && offset < maxScan {
		currentBatch := batchSize
		if currentBatch > maxScan-offset {
			currentBatch = maxScan - offset
		}
		args := append([]interface{}{}, baseArgs...)
		args = append(args, currentBatch, offset)
		var rows []*DatingProfileResp
		_, err := d.session.SelectBySql(sql, args...).Load(&rows)
		if err != nil {
			return nil, "", 0, err
		}
		if len(rows) == 0 {
			exhausted = true
			break
		}
		for _, profile := range rows {
			if profile == nil {
				continue
			}
			rowCursor := encodeRecommendCursor(profile)
			if rowCursor != "" {
				lastScannedCursor = rowCursor
			}
			profile.Normalize()
			if !fitsMutualFilters(viewer, profile) || !fitsRequestFilters(viewer, profile, req) {
				continue
			}
			filtered = append(filtered, profile)
			filteredCursors = append(filteredCursors, rowCursor)
			if len(filtered) >= limit+1 {
				break
			}
		}
		offset += len(rows)
		if len(rows) < currentBatch {
			exhausted = true
			break
		}
	}

	if len(filtered) > limit {
		nextCursor := filteredCursors[limit-1]
		return filtered[:limit], nextCursor, 1, nil
	}
	if exhausted {
		return filtered, "", 0, nil
	}
	// The safety scan cap was reached. Advance by the last scanned SQL row even
	// when every row was filtered out, so an empty page can never loop forever.
	if lastScannedCursor != "" {
		return filtered, lastScannedCursor, 1, nil
	}
	return filtered, "", 0, nil
}

func (d *db) cleanupExpiredServedBatch(now int64, limit int) (int64, error) {
	if limit <= 0 || limit > 5000 {
		limit = 1000
	}
	result, err := d.session.DeleteBySql(`DELETE FROM dating_served WHERE expires_at<? LIMIT ?`, now, limit).Exec()
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *db) markServed(uid, sessionID string, profiles []*DatingProfileResp) error {
	if uid == "" || sessionID == "" || len(profiles) == 0 {
		return nil
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	now := time.Now().UnixMilli()
	for _, p := range profiles {
		if p == nil || p.UID == "" {
			continue
		}
		_, err = tx.InsertBySql(`INSERT INTO dating_served(uid,session_id,to_uid,served_at,expires_at,created_at)
            VALUES(?,?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE served_at=VALUES(served_at),expires_at=VALUES(expires_at)`,
			uid, sessionID, p.UID, now, now+DatingServedTTLMS).Exec()
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *db) recentSameSwipe(uid, toUID, action string, since int64) (bool, error) {
	var count int
	err := d.session.Select("COUNT(*)").From("dating_swipes").
		Where("uid=? AND to_uid=? AND action=? AND swiped_at>=?", uid, toUID, action, since).
		LoadOne(&count)
	return count > 0, err
}

func (d *db) recordSwipe(uid string, req SwipeReq) (int64, error) {
	toUID := req.Target()
	action := normalizeAction(req.Action)
	now := time.Now().UnixMilli()

	// Android 网络重试时，同一动作在短时间内可能重复到达。这里不重复写额度流水。
	var current *struct {
		Action   string `db:"action"`
		SwipedAt int64  `db:"swiped_at"`
	}
	_, _ = d.session.Select("action", "swiped_at").From("dating_swipes").Where("uid=? AND to_uid=?", uid, toUID).Load(&current)
	if current != nil && current.Action == action && now-normalizeMillis(current.SwipedAt) <= SwipeRetryWindowMS {
		_, err := d.session.Update("dating_swipes").
			Set("source", safeText(req.Source, 32)).
			Set("photo_index", clampInt(req.PhotoIndex, 0, DatingMaxPhotos-1)).
			Set("session_id", safeText(req.SessionID, 80)).
			Set("updated_at", dbr.Expr("NOW()")).
			Where("uid=? AND to_uid=?", uid, toUID).Exec()
		return 0, err
	}
	tx, err := d.session.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertBySql(`INSERT INTO dating_swipes(uid,to_uid,action,source,photo_index,session_id,swiped_at,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE action=VALUES(action),source=VALUES(source),photo_index=VALUES(photo_index),session_id=VALUES(session_id),swiped_at=VALUES(swiped_at),updated_at=NOW()`,
		uid, toUID, action, safeText(req.Source, 32), clampInt(req.PhotoIndex, 0, DatingMaxPhotos-1), safeText(req.SessionID, 80), now).Exec()
	if err != nil {
		return 0, err
	}
	result, err := tx.InsertBySql(`INSERT INTO dating_swipe_events(uid,to_uid,action,source,photo_index,session_id,swiped_at,undone,created_at)
        VALUES(?,?,?,?,?,?,?,0,NOW())`, uid, toUID, action, safeText(req.Source, 32), clampInt(req.PhotoIndex, 0, DatingMaxPhotos-1), safeText(req.SessionID, 80), now).Exec()
	if err != nil {
		return 0, err
	}
	if action == DatingActionFavorite {
		_, err = tx.InsertBySql(`INSERT INTO dating_favorites(uid,to_uid,favorited_at,created_at,updated_at)
            VALUES(?,?,?,NOW(),NOW()) ON DUPLICATE KEY UPDATE favorited_at=VALUES(favorited_at),updated_at=NOW()`, uid, toUID, now).Exec()
	} else {
		_, err = tx.DeleteFrom("dating_favorites").Where("uid=? AND to_uid=?", uid, toUID).Exec()
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	id, _ := result.LastInsertId()
	return id, nil
}

func (d *db) isPairBlocked(uid, toUID string) (bool, error) {
	var count int
	err := d.session.SelectBySql(`SELECT
        (SELECT COUNT(*) FROM dating_blocks WHERE (uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) +
        (SELECT COUNT(*) FROM user_setting WHERE ((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND blacklist=1)`,
		uid, toUID, toUID, uid, uid, toUID, toUID, uid).LoadOne(&count)
	return count > 0, err
}

func (d *db) hasLiked(uid, toUID string) (bool, error) {
	var count int
	err := d.session.Select("COUNT(*)").From("dating_swipes").Where("uid=? AND to_uid=? AND action=?", uid, toUID, DatingActionLike).LoadOne(&count)
	return count > 0, err
}

func (d *db) countRecentPasses(uid string, since int64) (int, error) {
	var count int
	err := d.session.Select("COUNT(*)").From("dating_swipe_events").
		Where("uid=? AND action=? AND undone=0 AND swiped_at>=?", uid, DatingActionPass, since).
		LoadOne(&count)
	return count, err
}

func (d *db) countTodayActions(uid string) (likeUsed, favoriteUsed, rewindUsed int, err error) {
	start := startOfTodayMillis()
	type row struct {
		Action string `db:"action"`
		Count  int    `db:"count"`
	}
	var rows []*row
	_, err = d.session.SelectBySql(`SELECT action,COUNT(*) count FROM dating_swipe_events
        WHERE uid=? AND swiped_at>=? AND undone=0 AND action IN ('like','favorite') GROUP BY action`, uid, start).Load(&rows)
	if err != nil {
		return
	}
	for _, item := range rows {
		if item == nil {
			continue
		}
		if item.Action == DatingActionLike {
			likeUsed = item.Count
		} else if item.Action == DatingActionFavorite {
			favoriteUsed = item.Count
		}
	}
	err = d.session.Select("COUNT(*)").From("dating_undo_events").Where("uid=? AND undone_at>=?", uid, start).LoadOne(&rewindUsed)
	return
}

func (d *db) latestUndoableEvent(uid string) (*swipeEventModel, error) {
	var event *swipeEventModel
	_, err := d.session.Select("id", "uid", "to_uid", "action", "swiped_at", "photo_index", "session_id").
		From("dating_swipe_events").Where("uid=? AND undone=0", uid).OrderDesc("id").Limit(1).Load(&event)
	return event, err
}

func (d *db) undoSwipe(uid string, event *swipeEventModel) error {
	if event == nil {
		return ErrDatingNothingToUndo
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	result, err := tx.Update("dating_swipe_events").Set("undone", 1).Where("id=? AND uid=? AND undone=0", event.ID, uid).Exec()
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrDatingNothingToUndo
	}
	_, err = tx.DeleteFrom("dating_swipes").Where("uid=? AND to_uid=? AND action=? AND swiped_at=?", uid, event.ToUID, event.Action, event.SwipedAt).Exec()
	if err != nil {
		return err
	}
	if event.Action == DatingActionFavorite {
		_, err = tx.DeleteFrom("dating_favorites").Where("uid=? AND to_uid=?", uid, event.ToUID).Exec()
		if err != nil {
			return err
		}
	}
	_, err = tx.InsertBySql(`INSERT INTO dating_undo_events(uid,swipe_event_id,to_uid,action,undone_at,created_at)
        VALUES(?,?,?,?,?,NOW())`, uid, event.ID, event.ToUID, event.Action, time.Now().UnixMilli()).Exec()
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) createMatch(uid, toUID string) (*datingMatchModel, bool, error) {
	a, b := orderedPair(uid, toUID)
	pk := pairKey(uid, toUID)
	matchID := "dm_" + strings.ReplaceAll(pk, ":", "_")
	result, err := d.session.InsertBySql(`INSERT INTO dating_matches(match_id,pair_key,uid_a,uid_b,status,notice_sent,matched_at,created_at,updated_at)
        VALUES(?,?,?,?,1,0,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE
          notice_sent=IF(status=2,0,notice_sent),
          matched_at=IF(status=2,VALUES(matched_at),matched_at),
          status=IF(status=3,status,1),updated_at=NOW()`, matchID, pk, a, b, time.Now().UnixMilli()).Exec()
	if err != nil {
		return nil, false, err
	}
	rows, _ := result.RowsAffected()
	model, err := d.getMatchByPair(uid, toUID)
	return model, rows > 0, err
}

func (d *db) markMatchNoticeSent(matchID string) error {
	_, err := d.session.Update("dating_matches").Set("notice_sent", 1).Set("updated_at", dbr.Expr("NOW()")).Where("match_id=?", matchID).Exec()
	return err
}

func (d *db) getMatchByID(matchID string) (*datingMatchModel, error) {
	var model *datingMatchModel
	_, err := d.session.Select("match_id", "uid_a", "uid_b", "status", "notice_sent").From("dating_matches").Where("match_id=?", matchID).Load(&model)
	return model, err
}

func (d *db) getMatchByPair(uid, toUID string) (*datingMatchModel, error) {
	var model *datingMatchModel
	_, err := d.session.Select("match_id", "uid_a", "uid_b", "status", "notice_sent").From("dating_matches").Where("pair_key=?", pairKey(uid, toUID)).Load(&model)
	return model, err
}

func (d *db) hasActiveMatch(uid, toUID string) (string, bool, error) {
	m, err := d.getMatchByPair(uid, toUID)
	if err != nil || m == nil {
		return "", false, err
	}
	return m.MatchID, m.Status == DatingMatchActive, nil
}

func (d *db) matches(uid string, limit int) ([]*DatingMatchResp, error) {
	if limit <= 0 || limit > 50 {
		limit = 30
	}
	type matchRow struct {
		MatchID   string `db:"match_id"`
		Status    int    `db:"status"`
		OtherUID  string `db:"other_uid"`
		CreatedAt int64  `db:"created_at_ms"`
		UpdatedAt int64  `db:"updated_at_ms"`
	}
	var rows []*matchRow
	_, err := d.session.SelectBySql(`SELECT match_id,status,IF(uid_a=?,uid_b,uid_a) AS other_uid,
        UNIX_TIMESTAMP(created_at)*1000 AS created_at_ms,UNIX_TIMESTAMP(updated_at)*1000 AS updated_at_ms
        FROM dating_matches WHERE (uid_a=? OR uid_b=?) AND status=1 ORDER BY updated_at DESC LIMIT ?`, uid, uid, uid, limit).Load(&rows)
	if err != nil {
		return nil, err
	}
	uids := make([]string, 0, len(rows))
	for _, item := range rows {
		if item != nil && strings.TrimSpace(item.OtherUID) != "" {
			uids = append(uids, item.OtherUID)
		}
	}
	profiles, err := d.getProfilesByUIDs(uids)
	if err != nil {
		return nil, err
	}
	out := make([]*DatingMatchResp, 0, len(rows))
	for _, item := range rows {
		if item == nil {
			continue
		}
		profile := profiles[item.OtherUID]
		if profile == nil {
			continue
		}
		out = append(out, &DatingMatchResp{MatchID: item.MatchID, Status: item.Status, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt, User: profile})
	}
	return out, nil
}

func (d *db) cancelMatch(uid, matchID string) error {
	_, err := d.session.Update("dating_matches").Set("status", DatingMatchCanceled).Set("updated_at", dbr.Expr("NOW()")).Where("match_id=? AND (uid_a=? OR uid_b=?)", matchID, uid, uid).Exec()
	return err
}

func (d *db) favorites(uid string, limit int) ([]*DatingProfileResp, int, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	selectColumns := profileSelect("p", "u")
	sql := `SELECT ` + selectColumns + ` FROM dating_favorites f JOIN dating_profiles p ON p.uid=f.to_uid JOIN user u ON u.uid=p.uid AND IFNULL(u.is_destroy,0)=0
        LEFT JOIN dating_blocks b1 ON b1.uid=f.uid AND b1.to_uid=p.uid
        LEFT JOIN dating_blocks b2 ON b2.uid=p.uid AND b2.to_uid=f.uid
        WHERE f.uid=? AND p.enabled=1 AND p.status=1 AND p.has_photo=1
          AND b1.uid IS NULL AND b2.uid IS NULL ORDER BY f.favorited_at DESC LIMIT ?`
	var profiles []*DatingProfileResp
	_, err := d.session.SelectBySql(sql, uid, limit).Load(&profiles)
	if err != nil {
		return nil, 0, err
	}
	for _, p := range profiles {
		p.Normalize()
	}
	var total int
	err = d.session.SelectBySql(`SELECT COUNT(*) FROM dating_favorites f
        JOIN dating_profiles p ON p.uid=f.to_uid AND p.enabled=1 AND p.status=1 AND p.has_photo=1
        LEFT JOIN dating_blocks b1 ON b1.uid=f.uid AND b1.to_uid=p.uid
        LEFT JOIN dating_blocks b2 ON b2.uid=p.uid AND b2.to_uid=f.uid
        WHERE f.uid=? AND b1.uid IS NULL AND b2.uid IS NULL`, uid).LoadOne(&total)
	if err != nil {
		return nil, 0, err
	}
	return profiles, total, nil
}

func (d *db) removeFavorite(uid, toUID string) error {
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.DeleteFrom("dating_favorites").Where("uid=? AND to_uid=?", uid, toUID).Exec()
	if err != nil {
		return err
	}
	_, err = tx.DeleteFrom("dating_swipes").Where("uid=? AND to_uid=? AND action=?", uid, toUID, DatingActionFavorite).Exec()
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) receivedLikes(uid string, limit int, reveal bool) ([]*DatingProfileResp, int, error) {
	var total int
	err := d.session.SelectBySql(`SELECT COUNT(*) FROM dating_swipes s
        JOIN dating_profiles p ON p.uid=s.uid AND p.enabled=1 AND p.status=1 AND p.has_photo=1
        LEFT JOIN dating_matches m ON m.pair_key=IF(s.uid<?,CONCAT(s.uid,':',?),CONCAT(?,':',s.uid)) AND m.status=1
        LEFT JOIN dating_blocks b1 ON b1.uid=? AND b1.to_uid=s.uid
        LEFT JOIN dating_blocks b2 ON b2.uid=s.uid AND b2.to_uid=?
        WHERE s.to_uid=? AND s.action='like' AND m.match_id IS NULL AND b1.uid IS NULL AND b2.uid IS NULL`, uid, uid, uid, uid, uid, uid).LoadOne(&total)
	if err != nil || !reveal {
		return nil, total, err
	}
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	selectColumns := profileSelect("p", "u")
	sql := `SELECT ` + selectColumns + ` FROM dating_swipes s JOIN dating_profiles p ON p.uid=s.uid JOIN user u ON u.uid=p.uid AND IFNULL(u.is_destroy,0)=0
        LEFT JOIN dating_matches m ON m.pair_key=IF(s.uid<?,CONCAT(s.uid,':',?),CONCAT(?,':',s.uid)) AND m.status=1
        LEFT JOIN dating_blocks b1 ON b1.uid=? AND b1.to_uid=s.uid
        LEFT JOIN dating_blocks b2 ON b2.uid=s.uid AND b2.to_uid=?
        WHERE s.to_uid=? AND s.action='like' AND p.enabled=1 AND p.status=1 AND p.has_photo=1
          AND m.match_id IS NULL AND b1.uid IS NULL AND b2.uid IS NULL
        ORDER BY s.swiped_at DESC LIMIT ?`
	var profiles []*DatingProfileResp
	_, err = d.session.SelectBySql(sql, uid, uid, uid, uid, uid, uid, limit).Load(&profiles)
	if err != nil {
		return nil, total, err
	}
	for _, p := range profiles {
		p.Normalize()
	}
	return profiles, total, nil
}

func (d *db) block(uid string, req BlockReq) error {
	toUID := req.Target()
	if uid == "" || toUID == "" {
		return nil
	}
	tx, err := d.session.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	_, err = tx.InsertBySql(`INSERT INTO dating_blocks(uid,to_uid,reason,created_at,updated_at) VALUES(?,?,?,NOW(),NOW())
        ON DUPLICATE KEY UPDATE reason=VALUES(reason),updated_at=NOW()`, uid, toUID, safeText(req.Reason, 200)).Exec()
	if err != nil {
		return err
	}
	_, err = tx.Update("dating_matches").Set("status", DatingMatchBlocked).Set("updated_at", dbr.Expr("NOW()")).Where("pair_key=?", pairKey(uid, toUID)).Exec()
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *db) report(uid string, req ReportReq) error {
	toUID := req.Target()
	_, err := d.session.InsertBySql(`INSERT INTO dating_reports(uid,to_uid,reason,description,images,status,created_at,updated_at)
        VALUES(?,?,?,?,?,0,NOW(),NOW())`, uid, toUID, safeText(req.Reason, 80), safeText(req.Description, 500), toJSONString(req.Images, 6)).Exec()
	return err
}

func (d *db) validExposureTargets(uids []string) (map[string]bool, error) {
	unique := make([]string, 0, len(uids))
	seen := make(map[string]struct{}, len(uids))
	for _, uid := range uids {
		uid = strings.TrimSpace(uid)
		if uid == "" {
			continue
		}
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		unique = append(unique, uid)
	}
	valid := make(map[string]bool, len(unique))
	if len(unique) == 0 {
		return valid, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(unique)), ",")
	args := make([]interface{}, 0, len(unique))
	for _, uid := range unique {
		args = append(args, uid)
	}
	var rows []struct {
		UID string `db:"uid"`
	}
	query := `SELECT dp.uid FROM dating_profiles dp
        JOIN user u ON u.uid=dp.uid AND IFNULL(u.is_destroy,0)=0 AND IFNULL(u.status,1)=1
        WHERE dp.enabled=1 AND dp.status=1 AND dp.has_photo=1
          AND IFNULL(u.robot,0)=0 AND IFNULL(u.category,'') NOT IN ('system','customerService')
          AND IFNULL(u.bench_no,'')='' AND dp.uid IN (` + placeholders + `)`
	if _, err := d.session.SelectBySql(query, args...).Load(&rows); err != nil {
		return nil, err
	}
	for _, row := range rows {
		valid[row.UID] = true
	}
	return valid, nil
}

func exposureEventID(uid, toUID string, item ExposureItem, eventAt int64) string {
	raw := strings.TrimSpace(item.EventID)
	if raw != "" {
		sum := sha256.Sum256([]byte(uid + ":" + raw))
		return fmt.Sprintf("client-%x", sum[:16])
	}
	// Old clients do not send event_id. A ten-minute bucket makes immediate
	// network retries idempotent while still allowing later genuine exposures.
	bucket := eventAt / int64(10*time.Minute/time.Millisecond)
	payload := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d", uid, toUID,
		safeText(item.EventType, 24), safeText(item.Source, 32), item.DurationMS,
		clampInt(item.PhotoIndex, 0, DatingMaxPhotos-1), bucket)
	sum := sha256.Sum256([]byte(payload))
	return fmt.Sprintf("auto-%x", sum[:16])
}

func (d *db) recordExposures(uid string, req ExposureReq) (int, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" || len(req.Items) == 0 {
		return 0, nil
	}
	targetUIDs := make([]string, 0, len(req.Items))
	for _, item := range req.Items {
		toUID := item.Target()
		if toUID != "" && toUID != uid {
			targetUIDs = append(targetUIDs, toUID)
		}
	}
	validTargets, err := d.validExposureTargets(targetUIDs)
	if err != nil {
		return 0, err
	}
	tx, err := d.session.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.RollbackUnlessCommitted()
	now := time.Now().UnixMilli()
	minSeenAt := now - int64(7*24*time.Hour/time.Millisecond)
	maxSeenAt := now + int64(5*time.Minute/time.Millisecond)
	count := 0
	for _, item := range req.Items {
		toUID := item.Target()
		if toUID == "" || toUID == uid || !validTargets[toUID] {
			continue
		}
		eventAt := normalizeMillis(item.SeenAt)
		if eventAt < minSeenAt || eventAt > maxSeenAt {
			eventAt = now
		}
		duration := item.DurationMS
		if duration < 0 {
			duration = 0
		}
		if duration > int64(24*time.Hour/time.Millisecond) {
			duration = int64(24 * time.Hour / time.Millisecond)
		}
		eventType := safeText(item.EventType, 24)
		if eventType == "" {
			eventType = "expose"
		}
		eventID := exposureEventID(uid, toUID, item, eventAt)
		result, err := tx.InsertBySql(`INSERT IGNORE INTO dating_exposure_events(event_id,uid,to_uid,event_type,source,duration_ms,photo_index,event_at,created_at)
            VALUES(?,?,?,?,?,?,?,?,NOW())`, eventID, uid, toUID, eventType, safeText(item.Source, 32), duration,
			clampInt(item.PhotoIndex, 0, DatingMaxPhotos-1), eventAt).Exec()
		if err != nil {
			return 0, err
		}
		inserted, _ := result.RowsAffected()
		if inserted == 0 {
			continue
		}
		_, err = tx.InsertBySql(`INSERT INTO dating_exposures(uid,to_uid,seen_count,last_seen_at,last_duration_ms,created_at,updated_at)
            VALUES(?,?,1,?,?,NOW(),NOW()) ON DUPLICATE KEY UPDATE seen_count=seen_count+1,
            last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at)),last_duration_ms=VALUES(last_duration_ms),updated_at=NOW()`,
			uid, toUID, eventAt, duration).Exec()
		if err != nil {
			return 0, err
		}
		if _, err = tx.Update("dating_profiles").Set("exposure_count", dbr.Expr("exposure_count+1")).
			Set("updated_at", dbr.Expr("updated_at")).Where("uid=?", toUID).Exec(); err != nil {
			return 0, err
		}
		count++
		if count >= DatingExposureMax {
			break
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	d.touchActive(uid, now)
	return count, nil
}

func (d *db) hasOtherChatRelationship(uid, toUID string) (bool, error) {
	if uid == "" || toUID == "" || uid == toUID {
		return false, nil
	}
	var count int
	err := d.session.SelectBySql(`SELECT
        (SELECT COUNT(*) FROM friend WHERE uid=? AND to_uid=? AND is_deleted=0) +
        (SELECT COUNT(*) FROM partner_contacts WHERE uid=? AND to_uid=? AND (status=1 OR (status=0 AND requester_uid=?)))`,
		uid, toUID, uid, toUID, uid).LoadOne(&count)
	return count > 0, err
}

func (d *db) touchActive(uid string, at int64) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return
	}
	if at <= 0 {
		at = time.Now().UnixMilli()
	}
	// Active heartbeats are throttled and must not mutate updated_at, which is
	// reserved for actual profile edits and participates in recommendation order.
	_, _ = d.session.Update("dating_profiles").Set("last_active_at", at).
		Set("updated_at", dbr.Expr("updated_at")).
		Where("uid=? AND last_active_at<?", uid, at-int64(time.Minute/time.Millisecond)).Exec()
}

func normalizeAgeRange(minAge, maxAge int) (int, int) {
	if minAge < 18 {
		minAge = 18
	}
	if maxAge <= 0 {
		maxAge = 99
	}
	if maxAge < minAge {
		maxAge = minAge
	}
	if maxAge > 99 {
		maxAge = 99
	}
	return minAge, maxAge
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
