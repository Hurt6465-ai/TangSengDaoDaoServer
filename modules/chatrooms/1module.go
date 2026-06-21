package chatrooms

import (
	"context"
	"embed"
	"time"

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
		StartCleanupLoop(context.Background(), api.service, time.Minute, 100)
		return register.Module{
			Name:     "chatrooms",
			SQLDir:   register.NewSQLFS(sqlFS),
			Swagger:  swaggerContent,
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
						"topic_room":  1,
						"expire_at":   room.ExpireAt,
						"reply_count": room.ReplyCount,
					}, nil
				},
				Subscribers: func(channelID string, channelType uint8) ([]string, error) {
					return api.service.Subscribers(channelID)
				},
				Whitelist: func(channelID string, channelType uint8) ([]string, error) {
					// 话题房不做普通好友校验，不开启发送白名单限制。
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
	resp.Logo = ""
	resp.Save = 1
	resp.Category = "topic_room"
	resp.Extra = map[string]interface{}{
		"topic_room":           1,
		"room_id":              room.RoomID,
		"tag":                  room.Tag,
		"language":             room.Language,
		"last_reply_text":      room.LastReplyText,
		"last_reply_at":        room.LastReplyAt,
		"reply_count":          room.ReplyCount,
		"unread_count":         room.UnreadCount,
		"mention_unread_count": room.MentionUnreadCount,
		"expire_at":            room.ExpireAt,
	}
	return resp
}
