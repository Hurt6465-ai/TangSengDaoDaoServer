package partners

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultPartnerLimit = 18
	MaxPartnerLimit     = 50
	NearbyRadiusMeters  = 70000
	LocationTTLMillis   = int64(7 * 24 * time.Hour / time.Millisecond)
)

type ListResp struct {
	List       []*PartnerUser `json:"list"`
	Users      []*PartnerUser `json:"users"`
	Partners   []*PartnerUser `json:"partners"`
	Cursor     string         `json:"cursor"`
	HasMore    int            `json:"has_more"`
	ServerTime int64          `json:"server_time"`
}

type LocationReq struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type GreetReq struct {
	ToUID string `json:"to_uid"`
}

type LocationResp struct {
	UID       string  `json:"uid"`
	Lat       float64 `json:"lat"`
	Lng       float64 `json:"lng"`
	ExpiresAt int64   `json:"expires_at"`
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
	p.ProfileImages = parseStringList(p.ProfileImagesRaw, 9)
	p.Age = ageFromBirthday(p.Birthday)
	if p.LastActiveAt <= 0 {
		if p.UpdatedAtUnix > 0 {
			p.LastActiveAt = p.UpdatedAtUnix * 1000
		} else if p.CreatedAtUnix > 0 {
			p.LastActiveAt = p.CreatedAtUnix * 1000
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

func RankPartners(list []*PartnerUser, loginUID string, round int) {
	nowMs := time.Now().UnixMilli()
	for _, p := range list {
		if p == nil {
			continue
		}
		p.Normalize()
		p.Score = partnerScore(p, loginUID, round, nowMs)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i] == nil {
			return false
		}
		if list[j] == nil {
			return true
		}
		return list[i].Score > list[j].Score
	})
}

func partnerScore(p *PartnerUser, loginUID string, round int, nowMs int64) float64 {
	score := 0.0
	score += activeScore(p, nowMs)
	score += languageScore(p)
	score += profileScore(p)
	score += deterministicRandom(loginUID+":"+p.UID+":"+strconv.Itoa(round), 12)
	if p.DistanceMeters > 0 && p.DistanceMeters <= NearbyRadiusMeters {
		// 附近人融入推荐，但降低出现概率。
		score -= 25
	}
	if p.Follow == 1 {
		score -= 60
	}
	return score
}

func activeScore(p *PartnerUser, nowMs int64) float64 {
	if p.Online == 1 {
		return 35
	}
	last := p.LastActiveAt
	if last <= 0 && p.LastOffline > 0 {
		last = int64(p.LastOffline) * 1000
	}
	if last <= 0 {
		return -12
	}
	minutes := float64(nowMs-last) / 60000.0
	switch {
	case minutes <= 5:
		return 30
	case minutes <= 10:
		return 26
	case minutes <= 20:
		return 22
	case minutes <= 30:
		return 18
	case minutes <= 60:
		return 12
	case minutes <= 180:
		return 5
	case minutes <= 1440:
		return 0
	case minutes <= 10080:
		return -12
	default:
		return -999
	}
}

func languageScore(p *PartnerUser) float64 {
	// 后端第一版不知道登录用户的语言偏好时，只按资料完整度给基础分；
	// 客户端或后续后端可传 login user language 后继续细化双向匹配。
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
	return float64(int(h % uint32(max+1)))
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
	}
	value = strings.NewReplacer("，", ",", "、", ",", ";", ",", "；", ",", "\n", ",").Replace(value)
	return compact(strings.Split(value, ","), max)
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

func validLatLng(lat, lng float64) bool {
	return !math.IsNaN(lat) && !math.IsNaN(lng) && lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180
}
