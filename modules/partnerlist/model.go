package partnerlist

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	InitialListLimit       = 80
	RotationAddLimit       = 20
	DailyUniqueLimit       = 100
	PoolDirectScoreLimit   = 3000
	PoolHardCandidateLimit = 6000
	CandidateChunkSize     = 500
	PoolScanBatchSize      = 1500
	AlgorithmVersion       = 1
	GreetingDailyLimit     = 10
)

var (
	hotWindow         = 3 * 24 * time.Hour
	warmWindow        = 7 * 24 * time.Hour
	newcomerWindow    = 48 * time.Hour
	rotateAfter       = 5 * time.Hour
	recommendationTTL = 10 * 24 * time.Hour
	poolVersionTTL    = 72 * time.Hour
	viewerSeenTTL     = 10 * 24 * time.Hour
	foregroundTTL     = 120 * time.Second
	activeWriteTTL    = 10 * time.Minute
)

type RecommendationResp struct {
	DayKey              string      `json:"day_key"`
	AlgorithmVersion    int         `json:"algorithm_version"`
	ListVersion         int         `json:"list_version"`
	FirstServedAt       int64       `json:"first_served_at"`
	RotateAt            int64       `json:"rotate_at"`
	RotationDone        bool        `json:"rotation_done"`
	RotationRetryAt     int64       `json:"rotation_retry_at"`
	UpdatedCount        int         `json:"updated_count"`
	UniqueAssignedCount int         `json:"unique_assigned_count"`
	DailyCandidateLimit int         `json:"daily_candidate_limit"`
	GreetingLimit       int         `json:"greeting_limit"`
	GreetingUsed        int         `json:"greeting_used"`
	GreetingRemaining   int         `json:"greeting_remaining"`
	AddedUserIDs        []string    `json:"added_user_ids"`
	RemovedUserIDs      []string    `json:"removed_user_ids"`
	Users               []*ListUser `json:"users"`
	ServerTime          int64       `json:"server_time"`
}

type ListUser struct {
	UID               string   `json:"uid" db:"uid"`
	ID                string   `json:"id" db:"-"`
	Name              string   `json:"name" db:"name"`
	Username          string   `json:"username" db:"username"`
	Avatar            string   `json:"avatar" db:"-"`
	Sex               int      `json:"sex" db:"sex"`
	Birthday          string   `json:"birthday" db:"birthday"`
	Intro             string   `json:"intro" db:"intro"`
	CountryCode       string   `json:"country_code" db:"country_code"`
	Country           string   `json:"country" db:"country"`
	NativeLanguages   []string `json:"native_languages" db:"-"`
	LearningLanguages []string `json:"learning_languages" db:"-"`
	Tags              []string `json:"tags" db:"-"`
	ProfileCover      string   `json:"profile_cover" db:"profile_cover"`
	ProfileImages     []string `json:"profile_images" db:"-"`
	Vercode           string   `json:"vercode" db:"vercode"`
	Online            int      `json:"online" db:"online"`
	LastOffline       int      `json:"last_offline" db:"last_offline"`
	LastActiveAt      int64    `json:"last_active_at" db:"last_active_at"`
	Score             float64  `json:"-" db:"-"`
	IsNew             int      `json:"is_new" db:"-"`

	NativeLanguagesRaw   string  `json:"-" db:"native_languages"`
	LearningLanguagesRaw string  `json:"-" db:"learning_languages"`
	TagsRaw              string  `json:"-" db:"tags"`
	ProfileImagesRaw     string  `json:"-" db:"profile_images"`
	ProfileScore         float64 `json:"-" db:"profile_score"`
	ProfileCompletedAtMS int64   `json:"-" db:"profile_completed_at_ms"`
}

func (u *ListUser) normalize() {
	if u == nil {
		return
	}
	u.ID = u.UID
	u.NativeLanguages = parseStringList(u.NativeLanguagesRaw, 5)
	u.LearningLanguages = parseStringList(u.LearningLanguagesRaw, 5)
	u.Tags = parseRawStringList(u.TagsRaw, 20)
	// 语伴图片统一使用账号头像。历史 profile_images 可能保存旧域名、文件路径或
	// 已删除图片，不能再覆盖稳定的用户头像接口。
	if strings.TrimSpace(u.UID) != "" {
		u.Avatar = fmt.Sprintf("users/%s/avatar", u.UID)
		u.ProfileImages = []string{u.Avatar}
	} else {
		u.ProfileImages = []string{}
	}
	if strings.TrimSpace(u.Name) == "" {
		if strings.TrimSpace(u.Username) != "" {
			u.Name = u.Username
		} else {
			u.Name = u.UID
		}
	}
}

type viewerProfile struct {
	UID               string
	NativeLanguages   []string
	LearningLanguages []string
	PrimaryLearning   string
}

type poolProfile struct {
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
}

func (p *poolProfile) nativeLanguages() []string   { return parseStringList(p.NativeLanguagesRaw, 5) }
func (p *poolProfile) learningLanguages() []string { return parseStringList(p.LearningLanguagesRaw, 5) }

type recommendationDay struct {
	ID                         int64  `db:"id"`
	ViewerUID                  string `db:"viewer_uid"`
	DayKey                     string `db:"day_key"`
	AlgorithmVersion           int    `db:"algorithm_version"`
	PoolVersion                string `db:"pool_version"`
	FirstServedAt              int64  `db:"first_served_at"`
	RotateAt                   int64  `db:"rotate_at"`
	RotationRetryAt            int64  `db:"rotation_retry_at"`
	RotationDone               int    `db:"rotation_done"`
	InitialCandidateIDsRaw     string `db:"initial_candidate_ids"`
	CurrentCandidateIDsRaw     string `db:"current_candidate_ids"`
	AllAssignedCandidateIDsRaw string `db:"all_assigned_candidate_ids"`
	RotatedInIDsRaw            string `db:"rotated_in_ids"`
	RotatedOutIDsRaw           string `db:"rotated_out_ids"`
	AbnormalReplacementIDsRaw  string `db:"abnormal_replacement_ids"`
	CandidateScoresRaw         string `db:"candidate_scores"`
	UniqueAssignedCount        int    `db:"unique_assigned_count"`
	ListVersion                int    `db:"list_version"`
}

func (d *recommendationDay) initialIDs() []string     { return decodeIDs(d.InitialCandidateIDsRaw) }
func (d *recommendationDay) currentIDs() []string     { return decodeIDs(d.CurrentCandidateIDsRaw) }
func (d *recommendationDay) allAssignedIDs() []string { return decodeIDs(d.AllAssignedCandidateIDsRaw) }
func (d *recommendationDay) rotatedInIDs() []string   { return decodeIDs(d.RotatedInIDsRaw) }
func (d *recommendationDay) rotatedOutIDs() []string  { return decodeIDs(d.RotatedOutIDsRaw) }
func (d *recommendationDay) abnormalReplacementIDs() []string {
	return decodeIDs(d.AbnormalReplacementIDsRaw)
}

type recentDayRow struct {
	DayKey      string `db:"day_key"`
	AssignedRaw string `db:"all_assigned_candidate_ids"`
}

type PartnerSettingsReq struct {
	Enabled int `json:"enabled"`
}

type PartnerSettingsResp struct {
	Enabled int `json:"enabled"`
}

type OnlineBatchReq struct {
	UIDs []string `json:"uids"`
}

type OnlineState struct {
	UID          string `json:"uid"`
	Online       int    `json:"online"`
	LastActiveAt int64  `json:"last_active_at"`
}

type OnlineBatchResp struct {
	Users      []OnlineState `json:"users"`
	ServerTime int64         `json:"server_time"`
}

type HeartbeatResp struct {
	UID             string `json:"uid"`
	LastActiveAt    int64  `json:"last_active_at"`
	OnlineExpireAt  int64  `json:"online_expire_at"`
	NextHeartbeatIn int    `json:"next_heartbeat_in_seconds"`
}

func parseStringList(raw string, max int) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" || raw == "null" {
		return []string{}
	}
	var values []string
	if strings.HasPrefix(raw, "[") {
		_ = json.Unmarshal([]byte(raw), &values)
	}
	if len(values) == 0 {
		values = strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ';' || r == '，' || r == '；' || r == '|'
		})
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = normalizeLanguage(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func parseRawStringList(raw string, max int) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "[]" || raw == "null" {
		return []string{}
	}
	var values []string
	if strings.HasPrefix(raw, "[") {
		_ = json.Unmarshal([]byte(raw), &values)
	}
	if len(values) == 0 {
		values = strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ';' || r == '，' || r == '；' || r == '|'
		})
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func normalizeLanguage(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "_", "-")
	aliases := map[string]string{
		"chinese": "zh", "中文": "zh", "汉语": "zh", "zh-cn": "zh", "zh-tw": "zh",
		"english": "en", "英语": "en", "en-us": "en", "en-gb": "en",
		"burmese": "my", "myanmar": "my", "缅甸语": "my", "缅语": "my",
		"japanese": "ja", "日语": "ja", "jp": "ja",
		"korean": "ko", "韩语": "ko", "kr": "ko",
		"thai": "th", "泰语": "th",
		"vietnamese": "vi", "越南语": "vi",
		"filipino": "fil", "tagalog": "fil", "tl": "fil",
	}
	if normalized, ok := aliases[value]; ok {
		value = normalized
	}
	supported := map[string]struct{}{
		"zh": {}, "en": {}, "my": {}, "ja": {}, "ko": {}, "th": {}, "vi": {},
		"id": {}, "ms": {}, "fil": {}, "km": {}, "lo": {}, "hi": {}, "bn": {},
		"ur": {}, "ar": {}, "fa": {}, "he": {}, "tr": {}, "ru": {}, "uk": {},
		"de": {}, "fr": {}, "es": {}, "pt": {}, "it": {}, "nl": {}, "pl": {},
		"cs": {}, "ro": {}, "hu": {}, "el": {}, "sv": {}, "no": {}, "da": {},
		"fi": {},
	}
	if _, ok := supported[value]; !ok {
		return ""
	}
	return value
}

func encodeScoreMap(scores map[string]float64) string {
	if scores == nil {
		return "{}"
	}
	data, _ := json.Marshal(scores)
	return string(data)
}

func decodeScoreMap(raw string) map[string]float64 {
	out := map[string]float64{}
	if strings.TrimSpace(raw) == "" {
		return out
	}
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func encodeIDs(ids []string) string {
	ids = uniqueIDs(ids, 0)
	data, _ := json.Marshal(ids)
	return string(data)
}

func decodeIDs(raw string) []string {
	var ids []string
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	if json.Unmarshal([]byte(raw), &ids) != nil {
		return []string{}
	}
	return uniqueIDs(ids, 0)
}

func uniqueIDs(ids []string, max int) []string {
	out := make([]string, 0, len(ids))
	seen := map[string]struct{}{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func containsString(values []string, target string) bool {
	target = normalizeLanguage(target)
	for _, value := range values {
		if normalizeLanguage(value) == target && target != "" {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
