package chatrooms

import (
	"embed"

	wkcommon "github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/model"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/register"
)

//go:embed sql
var sqlFS embed.FS

//go:embed swagger/api.yaml
var swaggerContent string

func init() {
	register.AddModule(func(ctx interface{}) register.Module {
		x := ctx.(*config.Context)
		api := New(x)
		return register.Module{
			Name:     "chatrooms",
			SQLDir:   register.NewSQLFS(sqlFS),
			Swagger:  swaggerContent,
			Start:    api.Start,
			Stop:     api.Stop,
			SetupAPI: func() register.APIRouter { return api },
			IMDatasource: register.IMDatasource{
				HasData: func(channelID string, channelType uint8) register.IMDatasourceType {
					if channelType == wkcommon.ChannelTypeGroup.Uint8() && api.service.IsTopicChannel(channelID) {
						return register.IMDatasourceTypeChannelInfo | register.IMDatasourceTypeSubscribers | register.IMDatasourceTypeWhitelist
					}
					return register.IMDatasourceTypeNone
				},
				ChannelInfo: func(channelID string, channelType uint8) (map[string]interface{}, error) {
					room, err := api.service.ChannelGet(channelID, "")
					if err != nil {
						return nil, err
					}
					return map[string]interface{}{
						"large":      1,
						"topic_room": 1,
						"name":       room.Title,
						// 话题房固定使用发布者头像，避免每次成员变化都生成组合群头像。
						"logo":                     room.CreatorAvatar,
						"creator_uid":              room.CreatorUID,
						"creator_name":             room.CreatorName,
						"creator_avatar":           room.CreatorAvatar,
						"creator_country_code":     room.CreatorCountryCode,
						"creator_country":          room.CreatorCountry,
						"creator_avatar_cache_key": "",
						"expire_at":                room.ExpireAt,
						"reply_count":              room.ReplyCount,
						"participant_count":        room.ParticipantCount,
					}, nil
				},
				Subscribers: func(channelID string, channelType uint8) ([]string, error) {
					return api.service.Subscribers(channelID)
				},
				Whitelist: func(channelID string, channelType uint8) ([]string, error) {
					// 话题房是公开聊天室，不做普通好友校验，也不限制发送白名单。
					return []string{}, nil
				},
			},
			BussDataSource: register.BussDataSource{
				ChannelGet: func(channelID string, channelType uint8, loginUID string) (*model.ChannelResp, error) {
					if channelType != wkcommon.ChannelTypeGroup.Uint8() || !api.service.IsTopicChannel(channelID) {
						return nil, register.ErrDatasourceNotProcess
					}
					room, err := api.service.ChannelGet(channelID, loginUID)
					if err != nil {
						return nil, err
					}
					return newChannelRespWithTopicRoom(room), nil
				},
			},
		}
	})
}

func newChannelRespWithTopicRoom(room *TopicRoom) *model.ChannelResp {
	resp := &model.ChannelResp{}
	resp.Channel.ChannelID = room.ChannelID
	resp.Channel.ChannelType = uint8(wkcommon.ChannelTypeGroup)
	resp.Name = room.Title
	// 话题房固定返回发布者头像，保证群资料页、顶部标题和消息列表一致。
	resp.Logo = room.CreatorAvatar
	resp.Save = 1
	resp.Category = "topic_room"
	resp.Extra = map[string]interface{}{
		"topic_room":               1,
		"room_id":                  room.RoomID,
		"tag":                      room.Tag,
		"language":                 room.Language,
		"creator_uid":              room.CreatorUID,
		"creator_name":             room.CreatorName,
		"creator_avatar":           room.CreatorAvatar,
		"creator_country_code":     room.CreatorCountryCode,
		"creator_country":          room.CreatorCountry,
		"creator_avatar_cache_key": "",
		"last_reply_text":          room.LastReplyText,
		"last_reply_at":            room.LastReplyAt,
		"reply_count":              room.ReplyCount,
		"participant_count":        room.ParticipantCount,
		"unread_count":             room.UnreadCount,
		"mention_unread_count":     room.MentionUnreadCount,
		"can_delete":               room.CanDelete,
		"can_pin":                  room.CanPin,
		"expire_at":                room.ExpireAt,
	}
	return resp
}
