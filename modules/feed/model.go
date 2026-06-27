package feed

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultFeedLimit = 16
	MaxFeedLimit     = 50
)

type ListResp struct {
	List       []*FeedPost `json:"list"`
	Feeds      []*FeedPost `json:"feeds"`
	Cursor     string      `json:"cursor"`
	HasMore    int         `json:"has_more"`
	ServerTime int64       `json:"server_time"`
}

type CommentListResp struct {
	List       []*FeedComment `json:"list"`
	Comments   []*FeedComment `json:"comments"`
	Cursor     string         `json:"cursor"`
	HasMore    int            `json:"has_more"`
	ServerTime int64          `json:"server_time"`
}

type PublishReq struct {
	Text       string       `json:"text"`
	Title      string       `json:"title"`
	Visibility string       `json:"visibility"`
	Media      []*FeedMedia `json:"media"`
}

type CommentReq struct {
	Content          string `json:"content"`
	ReplyToCommentID string `json:"reply_to_comment_id"`
}

type FeedPost struct {
	FeedID         string       `json:"feed_id" db:"feed_id"`
	UID            string       `json:"uid" db:"uid"`
	Text           string       `json:"text" db:"text"`
	Title          string       `json:"title" db:"title"`
	Status         int          `json:"status" db:"status"`
	Visibility     string       `json:"visibility" db:"visibility"`
	LikeCount      int          `json:"like_count" db:"like_count"`
	CommentCount   int          `json:"comment_count" db:"comment_count"`
	ShareCount     int          `json:"share_count" db:"share_count"`
	Liked          int          `json:"liked" db:"liked"`
	DistanceMeters int          `json:"distance_meters" db:"distance_meters"`
	CreatedAt      int64        `json:"created_at" db:"created_at_ms"`
	UpdatedAt      int64        `json:"updated_at" db:"updated_at_ms"`
	LastActiveAt   int64        `json:"last_active_at" db:"last_active_at"`
	Score          float64      `json:"score" db:"score"`
	User           *FeedUser    `json:"user"`
	Media          []*FeedMedia `json:"media"`
}

type FeedUser struct {
	UID               string   `json:"uid" db:"user_uid"`
	Name              string   `json:"name" db:"user_name"`
	Username          string   `json:"username" db:"username"`
	Avatar            string   `json:"avatar"`
	AvatarCacheKey    string   `json:"avatar_cache_key"`
	CountryCode       string   `json:"country_code" db:"country_code"`
	Country           string   `json:"country" db:"country"`
	Age               int      `json:"age"`
	Birthday          string   `json:"birthday" db:"birthday"`
	NativeLanguages   []string `json:"native_languages"`
	LearningLanguages []string `json:"learning_languages"`
	Follow            int      `json:"follow" db:"follow"`
	Vercode           string   `json:"vercode" db:"vercode"`

	NativeLanguagesRaw   string `json:"-" db:"native_languages"`
	LearningLanguagesRaw string `json:"-" db:"learning_languages"`
}

type FeedMedia struct {
	ID          int64  `json:"id" db:"id"`
	FeedID      string `json:"feed_id" db:"feed_id"`
	Type        string `json:"type" db:"type"`
	ThumbURL    string `json:"thumb_url" db:"thumb_url"`
	DisplayURL  string `json:"display_url" db:"display_url"`
	OriginURL   string `json:"origin_url" db:"origin_url"`
	CoverURL    string `json:"cover_url" db:"cover_url"`
	PlayURL480P string `json:"play_url_480p" db:"play_url_480p"`
	PlayURL540P string `json:"play_url_540p" db:"play_url_540p"`
	PlayURL720P string `json:"play_url_720p" db:"play_url_720p"`
	Width       int    `json:"width" db:"width"`
	Height      int    `json:"height" db:"height"`
	DurationMS  int64  `json:"duration_ms" db:"duration_ms"`
	Size        int64  `json:"size" db:"size"`
	Sort        int    `json:"sort" db:"sort"`
}

type FeedComment struct {
	CommentID        string    `json:"comment_id" db:"comment_id"`
	FeedID           string    `json:"feed_id" db:"feed_id"`
	UID              string    `json:"uid" db:"uid"`
	Content          string    `json:"content" db:"content"`
	ReplyToCommentID string    `json:"reply_to_comment_id" db:"reply_to_comment_id"`
	CreatedAt        int64     `json:"created_at" db:"created_at_ms"`
	User             *FeedUser `json:"user"`
}

func (u *FeedUser) Normalize() {
	if u == nil {
		return
	}
	u.NativeLanguages = parseStringList(u.NativeLanguagesRaw, 5)
	u.LearningLanguages = parseStringList(u.LearningLanguagesRaw, 5)
	u.Age = ageFromBirthday(u.Birthday)
	if strings.TrimSpace(u.Name) == "" {
		if strings.TrimSpace(u.Username) != "" {
			u.Name = u.Username
		} else {
			u.Name = u.UID
		}
	}
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

func clampLimit(limit int) int {
	if limit <= 0 {
		return DefaultFeedLimit
	}
	if limit > MaxFeedLimit {
		return MaxFeedLimit
	}
	return limit
}

func offsetFrom(page int, cursor string, limit int) int {
	if strings.TrimSpace(cursor) != "" {
		n, _ := strconv.Atoi(cursor)
		if n > 0 {
			return n
		}
	}
	if page <= 1 {
		return 0
	}
	return (page - 1) * clampLimit(limit)
}
