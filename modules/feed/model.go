package feed

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultFeedLimit = 16
	MaxFeedLimit     = 50
	// FeedImageOnly keeps the first launch focused on image posts.
	// Frontend should hide video upload, backend rejects video payloads as a safety net.
	FeedImageOnly    = true
	FeedVideoTTLDays = 28
	FeedEventTTLDays = 30
)

var FeedVideoTTL = time.Duration(FeedVideoTTLDays) * 24 * time.Hour
var FeedEventTTL = time.Duration(FeedEventTTLDays) * 24 * time.Hour

type ListResp struct {
	List       []*FeedPost `json:"list"`
	Feeds      []*FeedPost `json:"feeds,omitempty"`
	Cursor     string      `json:"cursor"`
	HasMore    int         `json:"has_more"`
	ServerTime int64       `json:"server_time"`
}

type CommentListResp struct {
	List       []*FeedComment `json:"list"`
	Comments   []*FeedComment `json:"comments,omitempty"`
	Cursor     string         `json:"cursor"`
	HasMore    int            `json:"has_more"`
	ServerTime int64          `json:"server_time"`
}

type TikTokPreviewReq struct {
	URL string `json:"url"`
}

type TikTokPreviewResp struct {
	Provider   string `json:"provider"`
	VideoID    string `json:"video_id"`
	URL        string `json:"url"`
	EmbedURL   string `json:"embed_url"`
	CoverURL   string `json:"cover_url"`
	Title      string `json:"title"`
	AuthorName string `json:"author_name"`
}

type timelineCursor struct {
	CreatedAt int64 `json:"t"`
	ID        int64 `json:"i"`
}

func encodeTimelineCursor(createdAt, id int64) string {
	if createdAt <= 0 || id <= 0 {
		return ""
	}
	b, err := json.Marshal(timelineCursor{CreatedAt: createdAt, ID: id})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeTimelineCursor(value string) (timelineCursor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return timelineCursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return timelineCursor{}, err
	}
	var cursor timelineCursor
	if err = json.Unmarshal(b, &cursor); err != nil {
		return timelineCursor{}, err
	}
	if cursor.CreatedAt <= 0 || cursor.ID <= 0 {
		return timelineCursor{}, fmt.Errorf("invalid timeline cursor")
	}
	return cursor, nil
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

type FollowReq struct {
	UID          string `json:"uid"`
	FollowingUID string `json:"following_uid"`
}

// LikeReq supports idempotent like from Android.
// Android wkfeed currently sends like as 1/0, while future clients may send true/false.
// If omitted or invalid, backend keeps old toggle behavior for compatibility.
type LikeReq struct {
	Like *FlexibleBool `json:"like"`
}

func (r LikeReq) Desired() *bool {
	if r.Like == nil {
		return nil
	}
	value := bool(*r.Like)
	return &value
}

type FlexibleBool bool

func (b *FlexibleBool) UnmarshalJSON(data []byte) error {
	value := strings.TrimSpace(strings.ToLower(string(data)))
	value = strings.Trim(value, `"`)
	switch value {
	case "1", "true", "yes", "y", "on":
		*b = FlexibleBool(true)
	case "0", "false", "no", "n", "off":
		*b = FlexibleBool(false)
	default:
		return fmt.Errorf("invalid boolean value")
	}
	return nil
}

type ReportReq struct {
	Reason string `json:"reason"`
}

type EventReq struct {
	EventType  string `json:"event_type"`
	Type       string `json:"type"`
	WatchMS    int64  `json:"watch_ms"`
	DurationMS int64  `json:"duration_ms"`
	Percent    int    `json:"percent"`
	MediaType  string `json:"media_type"`
	Extra      string `json:"extra"`
}

func (r EventReq) NormalizedEventType() string {
	eventType := strings.TrimSpace(r.EventType)
	if eventType == "" {
		eventType = strings.TrimSpace(r.Type)
	}
	if eventType == "" {
		eventType = "watch"
	}
	eventType = strings.ToLower(eventType)
	switch eventType {
	case "exposed", "exposure", "show", "impression":
		return "expose"
	case "stay", "stay_ms", "view":
		return "watch"
	case "not_interested", "not-interest", "notinterest":
		return "dislike"
	default:
		return eventType
	}
}

type FeedPost struct {
	ID             int64        `json:"-" db:"id"`
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
	User           *FeedUser    `json:"user" db:"-"`
	Media          []*FeedMedia `json:"media" db:"-"`
}

type FeedUser struct {
	UID               string   `json:"uid" db:"user_uid"`
	Name              string   `json:"name" db:"user_name"`
	Username          string   `json:"username" db:"username"`
	Avatar            string   `json:"avatar" db:"-"`
	AvatarCacheKey    string   `json:"avatar_cache_key" db:"-"`
	CountryCode       string   `json:"country_code" db:"country_code"`
	Country           string   `json:"country" db:"country"`
	Sex               int      `json:"sex" db:"sex"`
	Age               int      `json:"age" db:"-"`
	Birthday          string   `json:"-" db:"birthday"`
	NativeLanguages   []string `json:"native_languages" db:"-"`
	LearningLanguages []string `json:"learning_languages" db:"-"`
	Follow            int      `json:"follow" db:"follow"`
	Vercode           string   `json:"vercode" db:"vercode"`

	NativeLanguagesRaw   string `json:"-" db:"native_languages"`
	LearningLanguagesRaw string `json:"-" db:"learning_languages"`
}

type FeedMedia struct {
	ID               int64  `json:"id" db:"id"`
	FeedID           string `json:"feed_id" db:"feed_id"`
	Type             string `json:"type" db:"type"`
	ThumbURL         string `json:"thumb_url,omitempty" db:"thumb_url"`
	DisplayURL       string `json:"display_url,omitempty" db:"display_url"`
	OriginURL        string `json:"origin_url,omitempty" db:"origin_url"`
	CoverURL         string `json:"cover_url,omitempty" db:"cover_url"`
	PlayURL480P      string `json:"play_url_480p,omitempty" db:"play_url_480p"`
	PlayURL540P      string `json:"play_url_540p,omitempty" db:"play_url_540p"`
	PlayURL720P      string `json:"play_url_720p,omitempty" db:"play_url_720p"`
	ExternalProvider string `json:"external_provider,omitempty" db:"external_provider"`
	ExternalID       string `json:"external_id,omitempty" db:"external_id"`
	ExternalURL      string `json:"external_url,omitempty" db:"external_url"`
	ExternalTitle    string `json:"external_title,omitempty" db:"external_title"`
	ExternalAuthor   string `json:"external_author,omitempty" db:"external_author"`
	Width            int    `json:"width" db:"width"`
	Height           int    `json:"height" db:"height"`
	DurationMS       int64  `json:"duration_ms" db:"duration_ms"`
	Size             int64  `json:"size" db:"size"`
	Sort             int    `json:"sort" db:"sort"`
}

type FeedComment struct {
	CommentID        string    `json:"comment_id" db:"comment_id"`
	FeedID           string    `json:"feed_id" db:"feed_id"`
	UID              string    `json:"uid" db:"uid"`
	Name             string    `json:"name" db:"-"`
	Avatar           string    `json:"avatar" db:"-"`
	AvatarCacheKey   string    `json:"avatar_cache_key" db:"-"`
	CountryCode      string    `json:"country_code" db:"-"`
	Content          string    `json:"content" db:"content"`
	ReplyToCommentID string    `json:"reply_to_comment_id" db:"reply_to_comment_id"`
	ParentID         string    `json:"parent_id" db:"-"`
	CreatedAt        int64     `json:"created_at" db:"created_at_ms"`
	User             *FeedUser `json:"user" db:"-"`
}

func (c *FeedComment) FillUser(u *FeedUser) {
	if c == nil {
		return
	}
	c.ParentID = c.ReplyToCommentID
	if u == nil {
		return
	}
	c.User = u
	c.Name = u.Name
	c.Avatar = u.Avatar
	c.AvatarCacheKey = u.AvatarCacheKey
	c.CountryCode = u.CountryCode
}

func (u *FeedUser) Normalize() {
	if u == nil {
		return
	}
	u.NativeLanguages = parseStringList(u.NativeLanguagesRaw, 5)
	u.LearningLanguages = parseStringList(u.LearningLanguagesRaw, 5)
	u.Age = ageFromBirthday(u.Birthday)
	if strings.TrimSpace(u.AvatarCacheKey) == "" {
		u.AvatarCacheKey = strings.TrimSpace(u.Vercode)
	}
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
	if now.Month() < t.Month() || (now.Month() == t.Month() && now.Day() < t.Day()) {
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
