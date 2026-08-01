package dating

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	DefaultDatingLimit = 12
	MaxDatingLimit     = 20
	DatingScopeGlobal  = "global"
	DatingScopeNearby  = "nearby"

	DatingActionLike     = "like"
	DatingActionPass     = "pass"
	DatingActionFavorite = "favorite"

	DatingMatchActive   = 1
	DatingMatchCanceled = 2
	DatingMatchBlocked  = 3

	DatingProfileNormal             = 1
	DatingProfileHidden             = 0
	DatingProfileBanned             = 2
	DatingRadiusMeters              = 70000
	DatingLocationTTLMS             = int64(7 * 24 * time.Hour / time.Millisecond)
	DatingLocationRefreshIntervalMS = int64(72 * time.Hour / time.Millisecond)
	DatingActiveWindowMS            = int64(30 * 24 * time.Hour / time.Millisecond)
	DatingServedTTLMS               = int64(2 * time.Hour / time.Millisecond)
	DatingExposureCooldownMS        = int64(24 * time.Hour / time.Millisecond)
	DatingExposureMax               = 30
	DatingExposureMinMS             = int64(350)
	DatingMaxPhotos                 = 5
	DatingMaxTags                   = 20
	DatingMaxDealbreakers           = 12

	MaleLikeLimit       = 40
	FemaleLikeLimit     = 60
	MaleFavoriteLimit   = 10
	FemaleFavoriteLimit = 20
	FreeRewindLimit     = 3
	PassPerMinuteLimit  = 90
	SwipeRetryWindowMS  = int64(15 * time.Second / time.Millisecond)
)

var (
	ErrDatingSelf               = errors.New("不能操作自己")
	ErrDatingTargetMiss         = errors.New("交友对象不存在或未开启交友")
	ErrDatingProfileIncomplete  = errors.New("请先完善并开启交友资料")
	ErrDatingAgeTooSmall        = errors.New("交友功能仅限18岁及以上用户")
	ErrDatingMatchedRequired    = errors.New("互相喜欢后才可以聊天")
	ErrDatingLikeLimit          = errors.New("今天的喜欢额度已用完")
	ErrDatingFavoriteLimit      = errors.New("今天的收藏额度已用完")
	ErrDatingRewindLimit        = errors.New("今天的免费撤回额度已用完")
	ErrDatingNothingToUndo      = errors.New("没有可以撤回的操作")
	ErrDatingMatchedCannotUndo  = errors.New("已经匹配成功，不能撤回这次喜欢")
	ErrDatingTooFast            = errors.New("操作太快，请稍后再试")
	ErrDatingProfileUnavailable = errors.New("交友资料已被下架或封禁")
	ErrDatingInvalidAction      = errors.New("不支持的交友操作")
	ErrDatingInvalidIntent      = errors.New("请选择有效的恋爱意向")
	ErrDatingInvalidPhoto       = errors.New("交友照片必须是本人上传的图片")
)

type EnableProfileReq struct {
	Enabled int `json:"enabled"`
}

type SaveProfileReq struct {
	Enabled               int      `json:"enabled"`
	Intent                string   `json:"intent"`
	RelationshipGoal      string   `json:"relationship_goal"`
	CrossBorderPreference string   `json:"cross_border_preference"`
	GenderPreference      int      `json:"gender_preference"`
	MinAge                int      `json:"min_age"`
	MaxAge                int      `json:"max_age"`
	City                  string   `json:"city"`
	HeightCM              int      `json:"height_cm"`
	WeightKG              int      `json:"weight_kg"`
	Job                   string   `json:"job"`
	JobStatus             string   `json:"job_status"`
	Education             string   `json:"education"`
	RelationshipStatus    string   `json:"relationship_status"`
	SexualOrientation     string   `json:"sexual_orientation"`
	Drinking              string   `json:"drinking"`
	Smoking               string   `json:"smoking"`
	Bio                   string   `json:"bio"`
	Intro                 string   `json:"intro"`
	IdealPartner          string   `json:"ideal_partner"`
	NativeLanguages       []string `json:"native_languages"`
	LearningLanguages     []string `json:"learning_languages"`
	Tags                  []string `json:"tags"`
	PersonalityTags       []string `json:"personality_tags"`
	PetTags               []string `json:"pet_tags"`
	SportTags             []string `json:"sport_tags"`
	MovieTags             []string `json:"movie_tags"`
	Dealbreakers          []string `json:"dealbreakers"`
	Photos                []string `json:"photos"`
	CardPhotos            []string `json:"card_photos"`
	ProfileImages         []string `json:"profile_images"`
	ShowDistance          *int     `json:"show_distance,omitempty"`
	AllowVoice            *int     `json:"allow_voice,omitempty"`
	AllowVideo            *int     `json:"allow_video,omitempty"`
}

type LocationReq struct {
	Lat          float64 `json:"lat"`
	Lng          float64 `json:"lng"`
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	Accuracy     float64 `json:"accuracy"`
	City         string  `json:"city"`
	CountryCode  string  `json:"country_code"`
	RadiusMeters int     `json:"radius_meters"`
	ExpiresDays  int     `json:"expires_days"`
	Source       string  `json:"source"`
}

func (r LocationReq) NormalizedLatLng() (float64, float64) {
	lat, lng := r.Lat, r.Lng
	if lat == 0 && r.Latitude != 0 {
		lat = r.Latitude
	}
	if lng == 0 && r.Longitude != 0 {
		lng = r.Longitude
	}
	return lat, lng
}

type RecommendReq struct {
	Limit       int
	Cursor      string
	Scope       string
	SessionID   string
	CountryMode string
	Gender      string
	AgeMin      int
	AgeMax      int
	Intent      string
	AllowRepeat bool

	// Internal recommendation lanes; never bound directly from the HTTP query.
	FreshOnly   bool
	ExploreOnly bool
	ExcludeUIDs []string
}

type recommendCursor struct {
	Online       int    `json:"o"`
	LastActiveAt int64  `json:"a"`
	ProfileScore int    `json:"p"`
	UpdatedAt    int64  `json:"u"`
	UID          string `json:"i"`
}

func encodeRecommendCursor(p *DatingProfileResp) string {
	if p == nil || strings.TrimSpace(p.UID) == "" {
		return ""
	}
	payload, err := json.Marshal(recommendCursor{
		Online:       p.Online,
		LastActiveAt: normalizeMillis(p.LastActiveAt),
		ProfileScore: p.ProfileScore,
		UpdatedAt:    p.UpdatedAtUnix,
		UID:          p.UID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRecommendCursor(raw string) (*recommendCursor, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		// Older builds returned a timestamp-shaped cursor. Treat it as a fresh page
		// instead of failing the request during rolling upgrades.
		return nil, false
	}
	var cursor recommendCursor
	if err = json.Unmarshal(payload, &cursor); err != nil || strings.TrimSpace(cursor.UID) == "" {
		return nil, false
	}
	cursor.LastActiveAt = normalizeMillis(cursor.LastActiveAt)
	return &cursor, true
}

type RecommendResp struct {
	Items      []*DatingProfileResp `json:"items"`
	List       []*DatingProfileResp `json:"list"`
	Users      []*DatingProfileResp `json:"users"`
	Cursor     string               `json:"cursor"`
	HasMore    int                  `json:"has_more"`
	Scope      string               `json:"scope"`
	SessionID  string               `json:"session_id,omitempty"`
	ServerTime int64                `json:"server_time"`
	QuotaResp
}

type SwipeReq struct {
	ToUID      string `json:"to_uid"`
	TargetUID  string `json:"target_uid"`
	Action     string `json:"action"`
	Source     string `json:"source"`
	PhotoIndex int    `json:"photo_index"`
	SessionID  string `json:"session_id"`
}

func (r SwipeReq) Target() string {
	if strings.TrimSpace(r.ToUID) != "" {
		return strings.TrimSpace(r.ToUID)
	}
	return strings.TrimSpace(r.TargetUID)
}

type SwipeResp struct {
	Status           int    `json:"status"`
	ToUID            string `json:"to_uid"`
	TargetUID        string `json:"target_uid"`
	Action           string `json:"action"`
	Matched          bool   `json:"matched"`
	Match            bool   `json:"match"`
	MatchID          string `json:"match_id,omitempty"`
	NoticeSent       bool   `json:"notice_sent"`
	SystemNoticeSent bool   `json:"system_notice_sent"`
	CanChat          bool   `json:"can_chat"`
	FriendCreated    bool   `json:"friend_created"`
	FriendsCreated   bool   `json:"friends_created"`
	Message          string `json:"message,omitempty"`
	Msg              string `json:"msg,omitempty"`
	QuotaResp
}

type UndoResp struct {
	Status    int                `json:"status"`
	Action    string             `json:"action"`
	TargetUID string             `json:"target_uid"`
	Restored  *DatingProfileResp `json:"restored_profile,omitempty"`
	Message   string             `json:"message,omitempty"`
	QuotaResp
}

type ExposureReq struct {
	Items []ExposureItem `json:"items"`
}

type ExposureItem struct {
	EventID    string `json:"event_id"`
	ToUID      string `json:"to_uid"`
	TargetUID  string `json:"target_uid"`
	SeenAt     int64  `json:"seen_at"`
	DurationMS int64  `json:"duration_ms"`
	EventType  string `json:"event_type"`
	Source     string `json:"source"`
	PhotoIndex int    `json:"photo_index"`
}

func (i ExposureItem) Target() string {
	if strings.TrimSpace(i.ToUID) != "" {
		return strings.TrimSpace(i.ToUID)
	}
	return strings.TrimSpace(i.TargetUID)
}

type ExposureResp struct {
	Status int    `json:"status"`
	Count  int    `json:"count"`
	Msg    string `json:"msg,omitempty"`
}

type BlockReq struct {
	ToUID     string `json:"to_uid"`
	TargetUID string `json:"target_uid"`
	Reason    string `json:"reason"`
}

func (r BlockReq) Target() string {
	if strings.TrimSpace(r.ToUID) != "" {
		return strings.TrimSpace(r.ToUID)
	}
	return strings.TrimSpace(r.TargetUID)
}

type ReportReq struct {
	ToUID       string   `json:"to_uid"`
	TargetUID   string   `json:"target_uid"`
	Reason      string   `json:"reason"`
	Description string   `json:"description"`
	Images      []string `json:"images"`
}

func (r ReportReq) Target() string {
	if strings.TrimSpace(r.ToUID) != "" {
		return strings.TrimSpace(r.ToUID)
	}
	return strings.TrimSpace(r.TargetUID)
}

type RemoveFavoriteReq struct {
	ToUID     string `json:"to_uid"`
	TargetUID string `json:"target_uid"`
}

func (r RemoveFavoriteReq) Target() string {
	if strings.TrimSpace(r.ToUID) != "" {
		return strings.TrimSpace(r.ToUID)
	}
	return strings.TrimSpace(r.TargetUID)
}

type BasicResp struct {
	Status int    `json:"status"`
	Msg    string `json:"msg,omitempty"`
}

type ChatCheckResp struct {
	CanChat    bool   `json:"can_chat"`
	Matched    bool   `json:"matched"`
	MatchID    string `json:"match_id,omitempty"`
	ToUID      string `json:"to_uid"`
	TargetUID  string `json:"target_uid"`
	ServerTime int64  `json:"server_time"`
}

type MatchesResp struct {
	List       []*DatingMatchResp `json:"list"`
	Matches    []*DatingMatchResp `json:"matches"`
	ServerTime int64              `json:"server_time"`
}

type FavoritesResp struct {
	Items      []*DatingProfileResp `json:"items"`
	List       []*DatingProfileResp `json:"list"`
	Total      int                  `json:"total"`
	ServerTime int64                `json:"server_time"`
}

type ReceivedLikesResp struct {
	Total      int                  `json:"total"`
	Locked     bool                 `json:"locked"`
	Items      []*DatingProfileResp `json:"items,omitempty"`
	ServerTime int64                `json:"server_time"`
}

type QuotaResp struct {
	LikeLimit         int `json:"like_limit"`
	LikeUsed          int `json:"like_used"`
	LikeRemaining     int `json:"like_remaining"`
	FavoriteLimit     int `json:"favorite_limit"`
	FavoriteUsed      int `json:"favorite_used"`
	FavoriteRemaining int `json:"favorite_remaining"`
	RewindLimit       int `json:"rewind_limit"`
	RewindUsed        int `json:"rewind_used"`
	RewindRemaining   int `json:"rewind_remaining"`
}

type DatingProfileResp struct {
	UID                   string            `json:"uid" db:"uid"`
	ID                    string            `json:"id,omitempty" db:"-"`
	Name                  string            `json:"name" db:"name"`
	Username              string            `json:"username" db:"username"`
	Avatar                string            `json:"avatar" db:"avatar"`
	Sex                   int               `json:"sex" db:"sex"`
	Gender                int               `json:"gender" db:"-"`
	Age                   int               `json:"age" db:"-"`
	Birthday              string            `json:"birthday" db:"birthday"`
	Enabled               int               `json:"enabled" db:"enabled"`
	UserPaused            int               `json:"user_paused" db:"user_paused"`
	Intent                string            `json:"intent" db:"intent"`
	RelationshipGoal      string            `json:"relationship_goal" db:"-"`
	CrossBorderPreference string            `json:"cross_border_preference" db:"cross_border_preference"`
	GenderPreference      int               `json:"gender_preference" db:"gender_preference"`
	MinAge                int               `json:"min_age" db:"min_age"`
	MaxAge                int               `json:"max_age" db:"max_age"`
	City                  string            `json:"city" db:"city"`
	CountryCode           string            `json:"country_code" db:"country_code"`
	Country               string            `json:"country" db:"country"`
	HeightCM              int               `json:"height_cm" db:"height_cm"`
	WeightKG              int               `json:"weight_kg" db:"weight_kg"`
	Job                   string            `json:"job" db:"job"`
	JobStatus             string            `json:"job_status" db:"job_status"`
	Education             string            `json:"education" db:"education"`
	RelationshipStatus    string            `json:"relationship_status" db:"relationship_status"`
	SexualOrientation     string            `json:"sexual_orientation" db:"sexual_orientation"`
	Drinking              string            `json:"drinking" db:"drinking"`
	Smoking               string            `json:"smoking" db:"smoking"`
	Bio                   string            `json:"bio" db:"bio"`
	Intro                 string            `json:"intro" db:"-"`
	IdealPartner          string            `json:"ideal_partner" db:"ideal_partner"`
	NativeLanguages       []string          `json:"native_languages" db:"-"`
	LearningLanguages     []string          `json:"learning_languages" db:"-"`
	Tags                  []string          `json:"tags" db:"-"`
	PersonalityTags       []string          `json:"personality_tags" db:"-"`
	PetTags               []string          `json:"pet_tags" db:"-"`
	SportTags             []string          `json:"sport_tags" db:"-"`
	MovieTags             []string          `json:"movie_tags" db:"-"`
	Dealbreakers          []string          `json:"dealbreakers" db:"-"`
	Photos                []string          `json:"photos" db:"-"`
	CardPhotos            []string          `json:"card_photos" db:"-"`
	ProfileImages         []string          `json:"profile_images" db:"-"`
	PhotoCards            []DatingPhotoCard `json:"photo_cards" db:"-"`
	ShowDistance          int               `json:"show_distance" db:"show_distance"`
	AllowVoice            int               `json:"allow_voice" db:"allow_voice"`
	AllowVideo            int               `json:"allow_video" db:"allow_video"`
	Online                int               `json:"online" db:"online"`
	LastActiveAt          int64             `json:"last_active_at" db:"last_active_at"`
	DistanceMeters        int               `json:"distance_meters" db:"distance_meters"`
	DistanceLabel         string            `json:"distance_label" db:"-"`
	DistanceBucket        int               `json:"distance_bucket" db:"-"`
	DistanceLevel         int               `json:"distance_level" db:"-"`
	DistanceKM            float64           `json:"distance_km" db:"-"`
	Nearby                int               `json:"nearby" db:"-"`
	ProfileScore          int               `json:"profile_score" db:"profile_score"`
	Complete              bool              `json:"complete" db:"-"`
	CanRecommend          bool              `json:"can_recommend" db:"-"`
	Score                 float64           `json:"score" db:"score"`
	Status                int               `json:"status" db:"status"`
	LikeLimit             int               `json:"like_limit" db:"-"`
	LikeUsed              int               `json:"like_used" db:"-"`
	LikeRemaining         int               `json:"like_remaining" db:"-"`
	FavoriteLimit         int               `json:"favorite_limit" db:"-"`
	FavoriteUsed          int               `json:"favorite_used" db:"-"`
	FavoriteRemaining     int               `json:"favorite_remaining" db:"-"`
	RewindLimit           int               `json:"rewind_limit" db:"-"`
	RewindUsed            int               `json:"rewind_used" db:"-"`
	RewindRemaining       int               `json:"rewind_remaining" db:"-"`

	NativeLanguagesRaw   string `json:"-" db:"native_languages"`
	LearningLanguagesRaw string `json:"-" db:"learning_languages"`
	TagsRaw              string `json:"-" db:"tags"`
	PersonalityTagsRaw   string `json:"-" db:"personality_tags"`
	PetTagsRaw           string `json:"-" db:"pet_tags"`
	SportTagsRaw         string `json:"-" db:"sport_tags"`
	MovieTagsRaw         string `json:"-" db:"movie_tags"`
	DealbreakersRaw      string `json:"-" db:"dealbreakers"`
	SharedTagsRaw        string `json:"-" db:"shared_tags"`
	PhotosRaw            string `json:"-" db:"photos"`
	CardPhotosRaw        string `json:"-" db:"card_photos"`
	CreatedAtUnix        int64  `json:"-" db:"created_at_unix"`
	UpdatedAtUnix        int64  `json:"-" db:"updated_at_unix"`
}

type DatingPhotoCard struct {
	Index    int      `json:"index"`
	URL      string   `json:"url"`
	Title    string   `json:"title"`
	Subtitle string   `json:"subtitle"`
	Tags     []string `json:"tags"`
}

type DatingMatchResp struct {
	MatchID           string             `json:"match_id" db:"match_id"`
	Status            int                `json:"status" db:"status"`
	CreatedAt         int64              `json:"created_at" db:"created_at_ms"`
	UpdatedAt         int64              `json:"updated_at" db:"updated_at_ms"`
	NoticeSent        int                `json:"-" db:"notice_sent"`
	FriendAutoCreated int                `json:"-" db:"friend_auto_created"`
	FriendSynced      int                `json:"-" db:"friend_synced"`
	User              *DatingProfileResp `json:"user" db:"-"`
}

type datingMatchModel struct {
	MatchID           string `db:"match_id"`
	UIDA              string `db:"uid_a"`
	UIDB              string `db:"uid_b"`
	Status            int    `db:"status"`
	NoticeSent        int    `db:"notice_sent"`
	FriendAutoCreated int    `db:"friend_auto_created"`
	FriendSynced      int    `db:"friend_synced"`
}

type swipeEventModel struct {
	ID         int64  `db:"id"`
	UID        string `db:"uid"`
	ToUID      string `db:"to_uid"`
	Action     string `db:"action"`
	SwipedAt   int64  `db:"swiped_at"`
	PhotoIndex int    `db:"photo_index"`
	SessionID  string `db:"session_id"`
}

func (p *DatingProfileResp) Normalize() {
	if p == nil {
		return
	}
	p.ID = p.UID
	if strings.TrimSpace(p.Avatar) == "" && strings.TrimSpace(p.UID) != "" {
		p.Avatar = fmt.Sprintf("users/%s/avatar", p.UID)
	}
	p.Gender = mapGender(p.Sex)
	p.NativeLanguages = parseStringList(p.NativeLanguagesRaw, 5)
	p.LearningLanguages = parseStringList(p.LearningLanguagesRaw, 5)
	p.Tags = parseStringList(p.TagsRaw, DatingMaxTags)
	p.PersonalityTags = parseStringList(p.PersonalityTagsRaw, 10)
	p.PetTags = parseStringList(p.PetTagsRaw, 10)
	p.SportTags = parseStringList(p.SportTagsRaw, 10)
	p.MovieTags = parseStringList(p.MovieTagsRaw, 10)
	p.Dealbreakers = parseStringList(p.DealbreakersRaw, DatingMaxDealbreakers)
	sharedRelationship, sharedJobStatus, sharedEducation, sharedPersonality, sharedPets, sharedSports, sharedMovies := splitSharedTags(p.SharedTagsRaw)
	if strings.TrimSpace(sharedRelationship) != "" {
		p.RelationshipStatus = sharedRelationship
	}
	if strings.TrimSpace(sharedJobStatus) != "" {
		p.JobStatus = sharedJobStatus
	}
	if strings.TrimSpace(sharedEducation) != "" {
		p.Education = sharedEducation
	}
	if len(sharedPersonality) > 0 {
		p.PersonalityTags = sharedPersonality
	}
	if len(sharedPets) > 0 {
		p.PetTags = sharedPets
	}
	if len(sharedSports) > 0 {
		p.SportTags = sharedSports
	}
	if len(sharedMovies) > 0 {
		p.MovieTags = sharedMovies
	}
	p.Photos = parseImageList(p.PhotosRaw, DatingMaxPhotos)
	if len(p.Photos) == 0 {
		// 兼容旧版仅保存 card_photos 的资料；新版本始终只使用一套图片。
		p.Photos = parseImageList(p.CardPhotosRaw, DatingMaxPhotos)
	}
	p.CardPhotos = append([]string(nil), p.Photos...)
	if intent, ok := normalizeDatingIntent(p.Intent); ok && intent != "" {
		p.Intent = intent
	}
	p.ProfileImages = p.Photos
	p.Age = ageFromBirthday(p.Birthday)
	p.LastActiveAt = normalizeMillis(p.LastActiveAt)
	if p.LastActiveAt <= 0 {
		if p.UpdatedAtUnix > 0 {
			p.LastActiveAt = normalizeMillis(p.UpdatedAtUnix)
		} else if p.CreatedAtUnix > 0 {
			p.LastActiveAt = normalizeMillis(p.CreatedAtUnix)
		}
	}
	if p.DistanceMeters > 0 {
		p.DistanceLabel = formatDistance(p.DistanceMeters)
		p.DistanceKM = float64(p.DistanceMeters) / 1000.0
		if p.DistanceMeters <= DatingRadiusMeters {
			p.Nearby = 1
		}
	}
	if strings.TrimSpace(p.Name) == "" {
		if strings.TrimSpace(p.Username) != "" {
			p.Name = p.Username
		} else {
			p.Name = p.UID
		}
	}
	p.Intro = p.Bio
	p.RelationshipGoal = p.Intent
	p.ProfileScore = profileScore(p)
	p.Complete = len(p.Photos) >= 1 && p.Age >= 18 && (p.Sex == 0 || p.Sex == 1) && strings.TrimSpace(p.Intent) != ""
	p.CanRecommend = p.Enabled == 1 && p.Status == DatingProfileNormal && p.Complete
	p.PhotoCards = buildPhotoCards(p)
}

func (p *DatingProfileResp) RejectsCrossBorder() bool {
	if p == nil {
		return false
	}
	v := strings.ToLower(strings.TrimSpace(p.CrossBorderPreference))
	return strings.Contains(v, "same_country") || strings.Contains(v, "same-country") || strings.Contains(v, "local_only") ||
		strings.Contains(v, "nearby_only") || strings.Contains(v, "no_foreign") || strings.Contains(v, "refuse_foreign") ||
		strings.Contains(v, "只接受本国") || strings.Contains(v, "拒绝异国") || strings.Contains(v, "本国恋")
}

func mapGender(sex int) int {
	if sex == 0 {
		return 2
	}
	if sex == 1 {
		return 1
	}
	return -1
}

func alignCardPhotos(master, cards []string) []string {
	out := make([]string, 0, len(master))
	for i, value := range master {
		card := ""
		if i < len(cards) {
			card = strings.TrimSpace(cards[i])
		}
		if card == "" {
			card = strings.TrimSpace(value)
		}
		out = append(out, card)
	}
	return out
}

func buildPhotoCards(p *DatingProfileResp) []DatingPhotoCard {
	if p == nil || len(p.Photos) == 0 {
		return nil
	}
	cards := make([]DatingPhotoCard, 0, len(p.Photos))
	name := strings.TrimSpace(p.Name)
	if p.Age > 0 {
		name = fmt.Sprintf("%s, %d", name, p.Age)
	}
	location := strings.TrimSpace(p.City)
	if location == "" {
		location = strings.TrimSpace(p.Country)
	}
	if p.ShowDistance == 1 {
		distance := strings.TrimSpace(p.DistanceLabel)
		if distance == "" && p.DistanceMeters > 0 {
			distance = formatDistance(p.DistanceMeters)
		}
		if distance != "" {
			if location != "" {
				location += " · " + distance
			} else {
				location = distance
			}
		}
	}
	for i, url := range p.Photos {
		cardURL := url
		if i < len(p.CardPhotos) && strings.TrimSpace(p.CardPhotos[i]) != "" {
			cardURL = p.CardPhotos[i]
		}
		card := DatingPhotoCard{Index: i, URL: cardURL, Tags: compactStringList(p.Tags, 5)}
		switch i {
		case 0:
			card.Title, card.Subtitle = name, location
		case 1:
			card.Title, card.Subtitle = "关于我", strings.TrimSpace(p.Bio)
		case 2:
			card.Title, card.Subtitle = "恋爱意向", strings.TrimSpace(p.Intent)
		case 3:
			card.Title, card.Subtitle = "生活方式", joinNonEmpty(" · ", p.Drinking, p.Smoking, p.RelationshipStatus)
		default:
			card.Title, card.Subtitle = "希望遇见", strings.TrimSpace(p.IdealPartner)
		}
		cards = append(cards, card)
	}
	return cards
}

func profileScore(p *DatingProfileResp) int {
	if p == nil {
		return 0
	}
	score := 0
	if len(p.Photos) > 0 {
		score += 35
	}
	if len(p.Photos) >= 3 {
		score += 10
	}
	if p.Age >= 18 {
		score += 10
	}
	if strings.TrimSpace(p.Intent) != "" {
		score += 12
	}
	if strings.TrimSpace(p.Bio) != "" {
		score += 12
	}
	if strings.TrimSpace(p.CountryCode) != "" {
		score += 5
	}
	if len(p.PersonalityTags)+len(p.PetTags)+len(p.SportTags)+len(p.MovieTags)+len(p.Tags) > 0 {
		score += 8
	}
	if p.HeightCM > 0 || p.WeightKG > 0 {
		score += 4
	}
	if p.JobStatus != "" || p.Education != "" {
		score += 4
	}
	if p.RelationshipStatus != "" || p.SexualOrientation != "" {
		score += 5
	}
	if p.IdealPartner != "" || len(p.Dealbreakers) > 0 {
		score += 5
	}
	if score > 100 {
		score = 100
	}
	return score
}

func clampLimit(v int) int {
	if v <= 0 {
		return DefaultDatingLimit
	}
	if v > MaxDatingLimit {
		return MaxDatingLimit
	}
	return v
}

func normalizeScope(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == DatingScopeNearby || v == "city" || v == "local" {
		return DatingScopeNearby
	}
	return DatingScopeGlobal
}

const (
	// intent 在数据库和 API 中统一保存稳定 code；中文、英文等只属于客户端展示层。
	DatingIntentLongTerm          = "long_term"
	DatingIntentLongTermOpenShort = "long_term_open_short"
	DatingIntentShortTermOpenLong = "short_term_open_long"
	DatingIntentShortTerm         = "short_term"
	DatingIntentFriends           = "friends"
	DatingIntentOpen              = "open"

	DatingIntentFilterSerious  = "serious"
	DatingIntentFilterMarriage = "marriage"
)

var allowedDatingIntents = map[string]string{
	DatingIntentLongTerm:          DatingIntentLongTerm,
	DatingIntentLongTermOpenShort: DatingIntentLongTermOpenShort,
	DatingIntentShortTermOpenLong: DatingIntentShortTermOpenLong,
	DatingIntentShortTerm:         DatingIntentShortTerm,
	DatingIntentFriends:           DatingIntentFriends,
	DatingIntentOpen:              DatingIntentOpen,
	"friend":                      DatingIntentFriends,
	"love":                        DatingIntentLongTerm,
	"dating":                      DatingIntentLongTerm,
	"marriage":                    DatingIntentLongTerm,
	"chat":                        DatingIntentOpen,
	"寻找长期伴侣":                      DatingIntentLongTerm,
	"长期伴侣，但不拒绝短期交往": DatingIntentLongTermOpenShort,
	"短期伴侣，但不拒绝长期交往": DatingIntentShortTermOpenLong,
	"享受短期交往的乐趣":     DatingIntentShortTerm,
	"结交新朋友":         DatingIntentFriends,
	"顺其自然":          DatingIntentOpen,
	"认真恋爱":          DatingIntentLongTerm,
}

func normalizeDatingIntent(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", true
	}
	if out, ok := allowedDatingIntents[strings.ToLower(v)]; ok {
		return out, true
	}
	if out, ok := allowedDatingIntents[v]; ok {
		return out, true
	}
	return "", false
}

func normalizeDatingIntentFilter(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if v == "" || strings.EqualFold(v, "all") {
		return "", true
	}
	switch strings.ToLower(v) {
	case DatingIntentFilterSerious, "love", "dating":
		return DatingIntentFilterSerious, true
	case DatingIntentFilterMarriage:
		return DatingIntentFilterMarriage, true
	}
	return normalizeDatingIntent(v)
}

func matchesDatingIntentFilter(filter, target string) bool {
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return true
	}
	target, ok := normalizeDatingIntent(target)
	if !ok {
		return false
	}
	switch filter {
	case DatingIntentFilterSerious:
		return target == DatingIntentLongTerm || target == DatingIntentLongTermOpenShort
	case DatingIntentFilterMarriage:
		return target == DatingIntentLongTerm
	default:
		normalized, ok := normalizeDatingIntent(filter)
		return ok && normalized == target
	}
}

func validActionInput(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "pass", "left", "nope", "skip", "like", "right", "yes", "favorite", "favourite", "star", "top", "heart":
		return true
	default:
		return false
	}
}

func sanitizeDatingPhotos(uid string, photos []string) ([]string, bool) {
	uid = strings.TrimSpace(uid)
	clean := compactStringList(photos, DatingMaxPhotos)
	if len(clean) == 0 {
		return []string{}, true
	}
	if uid == "" || strings.ContainsAny(uid, "/\\?#") {
		return nil, false
	}
	expectedPrefix := "/profile/" + uid + "/dating_"
	out := make([]string, 0, len(clean))
	for _, item := range clean {
		value := strings.ReplaceAll(strings.TrimSpace(item), "\\", "/")
		parsed, err := url.Parse(value)
		if err != nil {
			return nil, false
		}
		// Never persist a caller-controlled host. Absolute URLs returned by an
		// uploader are reduced to their storage path before validation.
		path := parsed.EscapedPath()
		if path == "" {
			path = value
		}
		if decoded, err := url.PathUnescape(path); err == nil {
			path = decoded
		}
		path = strings.ReplaceAll(path, "\\", "/")
		path = "/" + strings.TrimLeft(path, "/")
		for strings.Contains(path, "//") {
			path = strings.ReplaceAll(path, "//", "/")
		}
		if strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") || strings.Contains(path, "/./") {
			return nil, false
		}
		storagePath := path
		const previewPrefix = "/file/preview/common"
		if strings.HasPrefix(storagePath, previewPrefix) {
			storagePath = strings.TrimPrefix(storagePath, previewPrefix)
		}
		if !strings.HasPrefix(storagePath, expectedPrefix) {
			return nil, false
		}
		name := strings.TrimPrefix(storagePath, expectedPrefix)
		if name == "" || strings.Contains(name, "/") {
			return nil, false
		}
		lowerName := strings.ToLower(name)
		if !(strings.HasSuffix(lowerName, ".webp") || strings.HasSuffix(lowerName, ".jpg") || strings.HasSuffix(lowerName, ".jpeg") || strings.HasSuffix(lowerName, ".png")) {
			return nil, false
		}
		out = append(out, "file/preview/common"+storagePath)
	}
	return out, true
}

func normalizeAction(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	switch v {
	case "like", "right", "yes":
		return DatingActionLike
	case "favorite", "favourite", "star", "top", "heart":
		return DatingActionFavorite
	default:
		return DatingActionPass
	}
}

func validLatLng(lat, lng float64) bool {
	return lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180 && !(lat == 0 && lng == 0)
}

func pairKey(a, b string) string {
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	if a <= b {
		return a + ":" + b
	}
	return b + ":" + a
}

func orderedPair(a, b string) (string, string) {
	if a <= b {
		return a, b
	}
	return b, a
}

func parseStringList(raw string, max int) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}
	}
	list := []string{}
	if strings.HasPrefix(raw, "[") {
		_ = json.Unmarshal([]byte(raw), &list)
	} else {
		list = strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == '，' || r == ';' || r == '；' || r == ' ' || r == '|'
		})
	}
	return compactStringList(list, max)
}

func parseImageList(raw string, max int) []string { return parseStringList(raw, max) }

func compactStringList(in []string, max int) []string {
	if max <= 0 {
		max = len(in)
	}
	out := make([]string, 0, max)
	seen := map[string]struct{}{}
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if len([]rune(item)) > 500 {
			item = string([]rune(item)[:500])
		}
		key := strings.ToLower(item)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
		if len(out) >= max {
			break
		}
	}
	return out
}

func toJSONString(list []string, max int) string {
	list = compactStringList(list, max)
	if len(list) == 0 {
		return ""
	}
	data, _ := json.Marshal(list)
	return string(data)
}

func ageFromBirthday(birthday string) int {
	birthday = strings.TrimSpace(birthday)
	if birthday == "" {
		return 0
	}
	layouts := []string{"2006-01-02", "2006/01/02", "20060102"}
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
	birthdayThisYear := time.Date(now.Year(), t.Month(), t.Day(), 0, 0, 0, 0, now.Location())
	if now.Before(birthdayThisYear) {
		age--
	}
	if age < 0 || age > 120 {
		return 0
	}
	return age
}

func normalizeMillis(v int64) int64 {
	if v <= 0 {
		return 0
	}
	if v < 10000000000 {
		return v * 1000
	}
	return v
}

func safeText(s string, max int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\x00", ""))
	if max > 0 && len([]rune(s)) > max {
		s = string([]rune(s)[:max])
	}
	return s
}

func distanceBucket(m int) int {
	switch {
	case m <= 0:
		return 0
	case m < 1000:
		return 1
	case m < 5000:
		return 2
	case m < 20000:
		return 3
	case m < 50000:
		return 4
	case m < 100000:
		return 5
	default:
		return 6
	}
}

func formatDistance(m int) string {
	switch distanceBucket(m) {
	case 1:
		return "1km内"
	case 2:
		return "1–5km"
	case 3:
		return "5–20km"
	case 4:
		return "20–50km"
	case 5:
		return "50–100km"
	case 6:
		return "100km以上"
	default:
		return ""
	}
}

func latLngBounds(lat, lng float64, radiusMeters int) (float64, float64, float64, float64) {
	if radiusMeters <= 0 {
		radiusMeters = DatingRadiusMeters
	}
	rad := float64(radiusMeters) / 6371000.0
	latRad := lat * math.Pi / 180
	deltaLat := rad * 180 / math.Pi
	cosLat := math.Cos(latRad)
	if math.Abs(cosLat) < 0.01 {
		cosLat = 0.01
	}
	deltaLng := rad * 180 / math.Pi / cosLat
	return lat - deltaLat, lat + deltaLat, lng - deltaLng, lng + deltaLng
}

func fitsRequestFilters(viewer, target *DatingProfileResp, req RecommendReq) bool {
	if viewer == nil || target == nil {
		return false
	}
	if req.AgeMin > 0 && target.Age > 0 && target.Age < req.AgeMin {
		return false
	}
	if req.AgeMax > 0 && target.Age > 0 && target.Age > req.AgeMax {
		return false
	}
	// Gender matching is enforced by the two saved profile preferences in
	// fitsMutualFilters. Request-level gender values are intentionally ignored so
	// a stale or modified client cannot override the server-side preference.
	if !matchesDatingIntentFilter(req.Intent, target.Intent) {
		return false
	}
	viewerCountry := strings.ToUpper(strings.TrimSpace(viewer.CountryCode))
	targetCountry := strings.ToUpper(strings.TrimSpace(target.CountryCode))
	different := viewerCountry != "" && targetCountry != "" && viewerCountry != targetCountry
	unknown := viewerCountry == "" || targetCountry == ""
	if (viewer.RejectsCrossBorder() || target.RejectsCrossBorder()) && (different || unknown) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(req.CountryMode)) {
	case "same_country", "same-country", "local_only":
		if unknown || different {
			return false
		}
	case "foreign_open", "foreign", "cross_border":
		if unknown || !different || viewer.RejectsCrossBorder() || target.RejectsCrossBorder() {
			return false
		}
	}
	return true
}

func fitsMutualFilters(viewer, target *DatingProfileResp) bool {
	if viewer == nil || target == nil || target.Age < 18 || len(target.Photos) < 1 {
		return false
	}
	if target.Age > 0 && (target.Age < viewer.MinAge || target.Age > viewer.MaxAge) {
		return false
	}
	if viewer.Age > 0 && (viewer.Age < target.MinAge || viewer.Age > target.MaxAge) {
		return false
	}
	viewerPreference := effectiveGenderPreference(viewer)
	if viewerPreference >= 0 && target.Sex != viewerPreference {
		return false
	}
	targetPreference := effectiveGenderPreference(target)
	if targetPreference >= 0 && viewer.Sex != targetPreference {
		return false
	}
	return true
}

// effectiveGenderPreference keeps the legacy -1 value backward compatible while
// avoiding accidental same-sex recommendations for users who never selected a
// preference. Explicit 0/1 choices are still respected, so same-sex matching is
// available only after the user deliberately selects it.
func effectiveGenderPreference(profile *DatingProfileResp) int {
	if profile == nil {
		return -1
	}
	if profile.GenderPreference == 0 || profile.GenderPreference == 1 {
		return profile.GenderPreference
	}
	switch profile.Sex {
	case 0:
		return 1
	case 1:
		return 0
	default:
		return -1
	}
}

func rankDatingProfiles(list []*DatingProfileResp, viewer *DatingProfileResp, scope string) []*DatingProfileResp {
	now := time.Now().UnixMilli()
	for _, p := range list {
		if p == nil {
			continue
		}
		p.Normalize()
		p.Score = datingScore(p, viewer, scope, now)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].Score == list[j].Score {
			return list[i].LastActiveAt > list[j].LastActiveAt
		}
		return list[i].Score > list[j].Score
	})
	return list
}

func datingScore(p, viewer *DatingProfileResp, scope string, now int64) float64 {
	if p == nil {
		return -999
	}
	score := float64(p.ProfileScore) * 0.28
	activeHours := float64(now-p.LastActiveAt) / float64(time.Hour/time.Millisecond)
	if p.Online == 1 {
		score += 18
	}
	switch {
	case activeHours < 1:
		score += 15
	case activeHours < 24:
		score += 11
	case activeHours < 72:
		score += 7
	case activeHours < 168:
		score += 3
	}
	if viewer != nil {
		if p.Intent != "" && p.Intent == viewer.Intent {
			score += 8
		}
		score += float64(tagSimilarity(p, viewer))
		if p.CountryCode != "" && p.CountryCode == viewer.CountryCode {
			score += 4
		}
	}
	if p.DistanceMeters > 0 {
		if scope == DatingScopeNearby {
			score += 18 - math.Min(12, float64(p.DistanceMeters)/6000)
		} else if p.DistanceMeters <= DatingRadiusMeters {
			score += 5
		}
	}
	return score
}

func tagSimilarity(a, b *DatingProfileResp) int {
	if a == nil || b == nil {
		return 0
	}
	left := append([]string{}, a.Tags...)
	left = append(left, a.PersonalityTags...)
	left = append(left, a.PetTags...)
	left = append(left, a.SportTags...)
	left = append(left, a.MovieTags...)
	right := append([]string{}, b.Tags...)
	right = append(right, b.PersonalityTags...)
	right = append(right, b.PetTags...)
	right = append(right, b.SportTags...)
	right = append(right, b.MovieTags...)
	set := map[string]struct{}{}
	for _, v := range left {
		set[strings.ToLower(strings.TrimSpace(v))] = struct{}{}
	}
	matches := 0
	for _, v := range right {
		if _, ok := set[strings.ToLower(strings.TrimSpace(v))]; ok {
			matches++
		}
	}
	if matches > 6 {
		matches = 6
	}
	return matches * 2
}

func quotaLimits(sex int) (int, int, int) {
	switch sex {
	case 0:
		return FemaleLikeLimit, FemaleFavoriteLimit, FreeRewindLimit
	case 1:
		return MaleLikeLimit, MaleFavoriteLimit, FreeRewindLimit
	default:
		return 0, 0, FreeRewindLimit
	}
}

func quotaResp(sex, likeUsed, favoriteUsed, rewindUsed int) QuotaResp {
	likeLimit, favoriteLimit, rewindLimit := quotaLimits(sex)
	return QuotaResp{
		LikeLimit: likeLimit, LikeUsed: likeUsed, LikeRemaining: maxInt(0, likeLimit-likeUsed),
		FavoriteLimit: favoriteLimit, FavoriteUsed: favoriteUsed, FavoriteRemaining: maxInt(0, favoriteLimit-favoriteUsed),
		RewindLimit: rewindLimit, RewindUsed: rewindUsed, RewindRemaining: maxInt(0, rewindLimit-rewindUsed),
	}
}

func startOfTodayMillis() int64 {
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).UnixMilli()
}

func joinNonEmpty(sep string, values ...string) string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return strings.Join(out, sep)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
