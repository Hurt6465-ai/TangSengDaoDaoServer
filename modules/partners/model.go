package partners

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultPartnerLimit = 12
	MaxPartnerLimit     = 12
	NearbyRadiusMeters  = 70000

	// 定位有效期给 App 静默推荐使用。App 端负责 30km 移动覆盖，服务端保存 30 天兜底。
	LocationTTLMillis = int64(30 * 24 * time.Hour / time.Millisecond)

	GreetingHourLimit          = 5
	GreetingDayLimit           = 20
	GreetingSameTargetCooldown = 7 * 24 * time.Hour
	GreetingMaxTextLen         = 80

	PartnerContactStatusPending = 0
	PartnerContactStatusActive  = 1
	PartnerContactStatusIgnored = 2
	PartnerContactStatusBlocked = 3

	PartnerCandidatePoolSize = 100
	PartnerCandidateSQLLimit = 200
	PartnerRankWindowSize    = 80
	PartnerExposureBatchMax  = 20
)

type ListResp struct {
	List       []*PartnerUser `json:"list"`
	Users      []*PartnerUser `json:"users"`
	Partners   []*PartnerUser `json:"partners"`
	Cursor     string         `json:"cursor"`
	HasMore    int            `json:"has_more"`
	ServerTime int64          `json:"server_time"`
	SessionID  string         `json:"session_id,omitempty"`
}

type LocationReq struct {
	Lat          float64 `json:"lat"`
	Lng          float64 `json:"lng"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	Accuracy     float64 `json:"accuracy"`
	RadiusMeters int     `json:"radius_meters"`
	ExpiresDays  int     `json:"expires_days"`
	Source       string  `json:"source"`
}

func (r LocationReq) NormalizedLatLng() (float64, float64) {
	lat := r.Lat
	lng := r.Lng
	if lat == 0 && r.Latitude != 0 {
		lat = r.Latitude
	}
	if lng == 0 && r.Longitude != 0 {
		lng = r.Longitude
	}
	return lat, lng
}

type GreetReq struct {
	ToUID     string `json:"to_uid"`
	TargetUID string `json:"target_uid"`
	Text      string `json:"text"`
	Source    string `json:"source"`
}

func (r GreetReq) Target() string {
	if strings.TrimSpace(r.ToUID) != "" {
		return strings.TrimSpace(r.ToUID)
	}
	return strings.TrimSpace(r.TargetUID)
}

type LocationResp struct {
	UID          string  `json:"uid"`
	Lat          float64 `json:"lat"`
	Lng          float64 `json:"lng"`
	Accuracy     float64 `json:"accuracy"`
	RadiusMeters int     `json:"radius_meters"`
	ExpiresAt    int64   `json:"expires_at"`
	Source       string  `json:"source"`
}

type GreetingResp struct {
	Status         int    `json:"status"`
	ToUID          string `json:"to_uid"`
	TargetUID      string `json:"target_uid"`
	LastGreetAt    int64  `json:"last_greet_at"`
	NextAllowedAt  int64  `json:"next_allowed_at,omitempty"`
	HelloSent      int    `json:"hello_sent"`
	GreetingStatus int    `json:"greeting_status"`
	ContactStatus  int    `json:"contact_status"`
	Text           string `json:"text,omitempty"`
	Msg            string `json:"msg,omitempty"`
}



type ExposureReq struct {
	Items []ExposureItem `json:"items"`
}

type ExposureItem struct {
	ToUID      string `json:"to_uid"`
	SeenAt     int64  `json:"seen_at"`
	DurationMS int64  `json:"duration_ms"`
}

type ExposureResp struct {
	Status int    `json:"status"`
	Count  int    `json:"count"`
	Msg    string `json:"msg,omitempty"`
}

type ProfileMeResp struct {
	HasPartnerPhoto   bool     `json:"has_partner_photo"`
	ProfileImages     []string `json:"profile_images"`
	NativeLanguages   []string `json:"native_languages"`
	LearningLanguages []string `json:"learning_languages"`
	Tags              []string `json:"tags"`
	ProfileCover      string   `json:"profile_cover"`
}

type PartnerUser struct {
	UID               string   `json:"uid" db:"uid"`
	ID                string   `json:"id,omitempty" db:"-"`
	Name              string   `json:"name" db:"name"`
	Username          string   `json:"username" db:"username"`
	Avatar            string   `json:"avatar" db:"avatar"`
	AvatarCacheKey    string   `json:"avatar_cache_key" db:"-"`
	Sex               int      `json:"sex" db:"sex"`
	Age               int      `json:"age" db:"-"`
	Birthday          string   `json:"birthday" db:"birthday"`
	Intro             string   `json:"intro" db:"intro"`
	CountryCode       string   `json:"country_code" db:"country_code"`
	Country           string   `json:"country" db:"country"`
	NativeLanguages   []string `json:"native_languages" db:"-"`
	LearningLanguages []string `json:"learning_languages" db:"-"`
	Tags              []string `json:"tags" db:"-"`
	ProfileCover      string   `json:"profile_cover" db:"profile_cover"`
	ProfileImages     []string `json:"profile_images" db:"-"`
	Follow            int      `json:"follow" db:"follow"`
	Vercode           string   `json:"vercode" db:"vercode"`
	Online            int      `json:"online" db:"online"`
	LastOffline       int      `json:"last_offline" db:"last_offline"`
	LastActiveAt      int64    `json:"last_active_at" db:"last_active_at"`
	DistanceMeters    int      `json:"distance_meters" db:"distance_meters"`
	Nearby            int      `json:"nearby" db:"-"`
	Score             float64  `json:"score" db:"score"`
	HelloSent         int      `json:"hello_sent" db:"hello_sent"`
	GreetingStatus    int      `json:"greeting_status" db:"greeting_status"`

	SeenCount   int   `json:"-" db:"seen_count"`
	LastSeenAt  int64 `json:"-" db:"last_seen_at"`
	GreetCount  int   `json:"-" db:"greet_count"`
	LastGreetAt int64 `json:"-" db:"last_greet_at"`

	NativeLanguagesRaw   string `json:"-" db:"native_languages"`
	LearningLanguagesRaw string `json:"-" db:"learning_languages"`
	TagsRaw              string `json:"-" db:"tags"`
	ProfileImagesRaw     string `json:"-" db:"profile_images"`
	CreatedAtUnix        int64  `json:"-" db:"created_at_unix"`
	UpdatedAtUnix        int64  `json:"-" db:"updated_at_unix"`
}

func (p *PartnerUser) Normalize() {
	if p == nil {
		return
	}
	p.ID = p.UID
	p.NativeLanguages = parseStringList(p.NativeLanguagesRaw, 5)
	p.LearningLanguages = parseStringList(p.LearningLanguagesRaw, 5)
	p.Tags = parseStringList(p.TagsRaw, 20)
	p.ProfileImages = parseImageList(p.ProfileImagesRaw, 9)
	p.Age = ageFromBirthday(p.Birthday)
	p.LastActiveAt = normalizeMillis(p.LastActiveAt)
	if p.LastActiveAt <= 0 && p.LastOffline > 0 {
		p.LastActiveAt = normalizeMillis(int64(p.LastOffline))
	}
	if p.LastActiveAt <= 0 {
		if p.UpdatedAtUnix > 0 {
			p.LastActiveAt = normalizeMillis(p.UpdatedAtUnix)
		} else if p.CreatedAtUnix > 0 {
			p.LastActiveAt = normalizeMillis(p.CreatedAtUnix)
		}
	}
	if p.DistanceMeters > 0 && p.DistanceMeters <= NearbyRadiusMeters {
		p.Nearby = 1
	}
	if strings.TrimSpace(p.Name) == "" {
		if strings.TrimSpace(p.Username) != "" {
			p.Name = p.Username
		} else {
			p.Name = p.UID
		}
	}
}

func RankPartners(list []*PartnerUser, loginUID string, round int, viewer *ProfileMeResp) []*PartnerUser {
	nowMs := time.Now().UnixMilli()
	ranked := make([]*PartnerUser, 0, len(list))
	for _, p := range list {
		if p == nil {
			continue
		}
		p.Normalize()
		p.Score = partnerScore(p, loginUID, round, nowMs, viewer)
		if p.Score <= -900 {
			continue
		}
		ranked = append(ranked, p)
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		return ranked[i].Score > ranked[j].Score
	})
	return ranked
}

func partnerScore(p *PartnerUser, loginUID string, round int, nowMs int64, viewer *ProfileMeResp) float64 {
	score := 0.0
	score += activeScore(p, nowMs)
	score += languageScore(viewer, p)
	score += profileScore(p)
	score += deterministicRandom(loginUID+":"+p.UID+":"+strconv.Itoa(round)+":"+time.Now().Format("20060102"), 8)
	// 距离不参与推荐分：不加分、不扣分，只用于候选混入和 App 展示。
	if p.SeenCount > 0 {
		score -= float64(minInt(p.SeenCount*3, 18))
	}
	if p.LastSeenAt > 0 {
		hours := float64(nowMs-normalizeMillis(p.LastSeenAt)) / float64(time.Hour/time.Millisecond)
		if hours <= 24 {
			score -= 60
		} else if hours <= 7*24 {
			score -= 25
		} else if hours <= 30*24 {
			score -= 8
		}
	}
	if p.LastGreetAt > 0 {
		days := float64(nowMs-normalizeMillis(p.LastGreetAt)) / float64(24*time.Hour/time.Millisecond)
		if days <= 7 {
			score -= 80
		} else if days <= 30 {
			score -= 15
		}
	}
	if p.Follow == 1 {
		score -= 999
	}
	return score
}

func activeScore(p *PartnerUser, nowMs int64) float64 {
	if p.Online == 1 {
		return 35
	}
	last := normalizeMillis(p.LastActiveAt)
	if last <= 0 && p.LastOffline > 0 {
		last = normalizeMillis(int64(p.LastOffline))
	}
	if last <= 0 {
		return -20
	}
	minutes := float64(nowMs-last) / 60000.0
	switch {
	case minutes <= 5:
		return 30
	case minutes <= 10:
		return 25
	case minutes <= 20:
		return 20
	case minutes <= 30:
		return 15
	case minutes <= 60:
		return 10
	case minutes <= 180:
		return 5
	case minutes <= 1440:
		return 1
	case minutes <= 10080:
		return -20
	default:
		return -999
	}
}

func languageScore(viewer *ProfileMeResp, p *PartnerUser) float64 {
	if p == nil {
		return 0
	}
	// 没有登录者语伴资料时，至少按资料完整度给基础分，避免冷启动完全无序。
	if viewer == nil || (len(viewer.NativeLanguages) == 0 && len(viewer.LearningLanguages) == 0) {
		score := 0.0
		if len(p.NativeLanguages) > 0 {
			score += 12
		}
		if len(p.LearningLanguages) > 0 {
			score += 12
		}
		if len(p.NativeLanguages) > 0 && len(p.LearningLanguages) > 0 {
			score += 6
		}
		return score
	}
	score := 0.0
	// 对方母语命中我的学习语言：对方能教我。
	if hasIntersection(p.NativeLanguages, viewer.LearningLanguages) {
		score += 25
	}
	// 我的母语命中对方学习语言：我能教对方。
	if hasIntersection(viewer.NativeLanguages, p.LearningLanguages) {
		score += 20
	}
	// 双向互补轻微加成。
	if score >= 45 {
		score += 8
	}
	if score == 0 {
		// 不完全匹配也不要直接打死，保留探索空间。
		if len(p.NativeLanguages) > 0 {
			score += 4
		}
		if len(p.LearningLanguages) > 0 {
			score += 4
		}
	}
	return score
}

func hasIntersection(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	seen := make(map[string]struct{}, len(a))
	for _, item := range a {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			seen[item] = struct{}{}
		}
	}
	for _, item := range b {
		item = strings.ToLower(strings.TrimSpace(item))
		if _, ok := seen[item]; ok {
			return true
		}
	}
	return false
}

func profileScore(p *PartnerUser) float64 {
	score := 0.0
	if p.ProfileCover != "" {
		score += 4
	}
	if len(p.ProfileImages) > 0 {
		score += 10
	}
	if len(p.ProfileImages) >= 3 {
		score += 5
	}
	if strings.TrimSpace(p.Intro) != "" {
		score += 4
	}
	if len(p.Tags) > 0 {
		score += 3
	}
	if p.CountryCode != "" {
		score += 2
	}
	return score
}

func deterministicRandom(seed string, max int) float64 {
	h := uint32(2166136261)
	for i := 0; i < len(seed); i++ {
		h ^= uint32(seed[i])
		h *= 16777619
	}
	if max <= 0 {
		return 0
	}
	span := max*2 + 1
	return float64(int(h%uint32(span)) - max)
}

func parseStringList(value string, max int) []string {
	value = strings.TrimSpace(value)
	if value == "" || value == "null" {
		return []string{}
	}
	var arr []string
	if strings.HasPrefix(value, "[") {
		if err := json.Unmarshal([]byte(value), &arr); err == nil {
			return compact(arr, max)
		}
		var anyArr []interface{}
		if err := json.Unmarshal([]byte(value), &anyArr); err == nil {
			values := make([]string, 0, len(anyArr))
			for _, item := range anyArr {
				if s, ok := item.(string); ok {
					values = append(values, s)
				}
			}
			return compact(values, max)
		}
	}
	value = strings.NewReplacer("，", ",", "、", ",", ";", ",", "；", ",", "\n", ",").Replace(value)
	return compact(strings.Split(value, ","), max)
}

func parseImageList(value string, max int) []string {
	value = strings.TrimSpace(value)
	if value == "" || value == "null" {
		return []string{}
	}
	var arr []string
	if strings.HasPrefix(value, "[") {
		if err := json.Unmarshal([]byte(value), &arr); err == nil {
			return compactImagePaths(arr, max)
		}
		var objs []map[string]interface{}
		if err := json.Unmarshal([]byte(value), &objs); err == nil {
			out := make([]string, 0, len(objs))
			keys := []string{"display_url", "display", "url", "path", "thumb_url", "origin_url"}
			for _, obj := range objs {
				for _, key := range keys {
					if raw, ok := obj[key]; ok {
						if s := strings.TrimSpace(toString(raw)); s != "" {
							out = append(out, s)
							break
						}
					}
				}
			}
			return compactImagePaths(out, max)
		}
	}
	value = strings.NewReplacer("，", ",", "、", ",", ";", ",", "；", ",", "\n", ",").Replace(value)
	return compactImagePaths(strings.Split(value, ","), max)
}

func compact(values []string, max int) []string {
	out := make([]string, 0)
	seen := map[string]struct{}{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func compactImagePaths(values []string, max int) []string {
	out := make([]string, 0)
	seen := map[string]struct{}{}
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" || v == "null" || v == "{}" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func ageFromBirthday(birthday string) int {
	birthday = strings.TrimSpace(birthday)
	if birthday == "" {
		return 0
	}
	layouts := []string{"2006-01-02", "2006/01/02", "2006.01.02", "20060102"}
	var t time.Time
	var err error
	for _, layout := range layouts {
		t, err = time.ParseInLocation(layout, birthday, time.Local)
		if err == nil {
			break
		}
	}
	if err != nil {
		return 0
	}
	now := time.Now()
	age := now.Year() - t.Year()
	if now.YearDay() < t.YearDay() {
		age--
	}
	if age < 0 || age > 120 {
		return 0
	}
	return age
}

func normalizeMillis(value int64) int64 {
	if value <= 0 {
		return 0
	}
	// user_online 里 last_online/last_offline 是秒级；App/新接口可能是毫秒级。
	if value < 1000000000000 {
		return value * 1000
	}
	return value
}

func validLatLng(lat, lng float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lng) && lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(value)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
