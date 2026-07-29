package message

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/common"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/config"
	"github.com/TangSengDaoDao/TangSengDaoDaoServerLib/pkg/util"
	"github.com/gocraft/dbr/v2"
)

const (
	partnerContactPending = 0
	partnerContactActive  = 1
	partnerContactIgnored = 2
	partnerContactBlocked = 3

	partnerPendingMessageReserved  = 0
	partnerPendingMessageDelivered = 1
	partnerPendingMessageFailed    = 2
	partnerPendingMessageUncertain = 3
	partnerPendingMaxMessages      = 3
)

var (
	errPartnerRelationDenied      = errors.New("发送者与接受者不是好友")
	errPartnerPendingLimit        = errors.New("对方还没回复，最多只能发送3条消息")
	errPartnerClientMsgNoRequired = errors.New("陌生消息client_msg_no不能为空")
	errPartnerClientMsgNoTooLong  = errors.New("陌生消息client_msg_no不能超过100个字符")
	errPartnerIdempotencyConflict = errors.New("client_msg_no已被不同的消息使用")
	errPartnerMessageInProgress   = errors.New("消息正在发送，请勿重复提交")
	errPartnerMessageUncertain    = errors.New("消息投递结果确认中，请使用相同client_msg_no重试")
	errPartnerUnsupportedContent  = errors.New("pending关系只允许发送有效内容消息")
)

type partnerPendingGuard struct {
	Handled            bool
	Pending            bool
	Requester          bool
	ReceiverReply      bool
	Reserved           bool
	ReuseReservation   bool
	DuplicateDelivered bool
	NeedsReconcile     bool
	ClientMsgNo        string
	IMClientMsgNo      string
	PayloadHash        string
	ContentType        int
	MessageCount       int
	IMMessageID        string
}

type partnerContactGuardRow struct {
	RequesterUID      string `db:"requester_uid"`
	Status            int    `db:"status"`
	RequesterMsgCount int    `db:"requester_msg_count"`
}
type partnerPendingMessageRow struct {
	ReceiverUID   string `db:"receiver_uid"`
	ClientMsgNo   string `db:"client_msg_no"`
	ContentType   int    `db:"content_type"`
	PayloadHash   string `db:"payload_hash"`
	Status        int    `db:"status"`
	ReservedCount int    `db:"reserved_count"`
	IMClientMsgNo string `db:"im_client_msg_no"`
	IMMessageID   string `db:"im_message_id"`
	UpdatedAt     int64  `db:"updated_at"`
	NextCheckAt   int64  `db:"next_check_at"`
}

// partnerMessageSendResponse is the stable wire contract used by the Android
// pending-message gateway. Keep booleans as booleans and always return the
// relationship state so clients do not remain stuck in a stale pending state.
type partnerMessageSendResponse struct {
	Status            int    `json:"status"`
	Duplicate         int    `json:"duplicate"`
	PartnerPending    bool   `json:"partner_pending"`
	ContactStatus     int    `json:"contact_status"`
	RequesterMsgCount int    `json:"requester_msg_count"`
	MaxMessageCount   int    `json:"max_message_count"`
	ClientMsgNo       string `json:"client_msg_no,omitempty"`
	IMClientMsgNo     string `json:"im_client_msg_no,omitempty"`
	MessageID         string `json:"message_id,omitempty"`
	MessageSeq        uint32 `json:"message_seq"`
	Timestamp         int64  `json:"timestamp"`
}

func newPartnerMessageSendResponse(guard *partnerPendingGuard, imResp *config.MsgSendResp, duplicate bool) *partnerMessageSendResponse {
	resp := &partnerMessageSendResponse{
		Status:          200,
		ContactStatus:   partnerContactActive,
		MaxMessageCount: partnerPendingMaxMessages,
		Timestamp:       time.Now().Unix(),
	}
	if duplicate {
		resp.Duplicate = 1
	}
	if guard != nil {
		resp.PartnerPending = guard.Pending
		if guard.Pending {
			resp.ContactStatus = partnerContactPending
		}
		resp.RequesterMsgCount = guard.MessageCount
		resp.ClientMsgNo = guard.ClientMsgNo
		resp.IMClientMsgNo = guard.IMClientMsgNo
		resp.MessageID = guard.IMMessageID
	}
	if imResp != nil {
		if imResp.ClientMsgNo != "" {
			resp.IMClientMsgNo = imResp.ClientMsgNo
		}
		if imResp.MessageID != 0 {
			resp.MessageID = strconv.FormatInt(imResp.MessageID, 10)
		}
		resp.MessageSeq = imResp.MessageSeq
	}
	return resp
}

func truncatePartnerPendingRunes(value string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= max {
		return value
	}
	return string(runes[:max])
}

func normalizePartnerClientMsgNo(topLevel string, payload map[string]interface{}) (string, error) {
	value := strings.TrimSpace(topLevel)
	if value == "" && payload != nil {
		for _, key := range []string{"client_msg_no", "clientMsgNo", "client_message_no"} {
			if item, ok := payload[key]; ok {
				value = strings.TrimSpace(fmt.Sprint(item))
				if value != "" {
					break
				}
			}
		}
	}
	if value == "" {
		return "", errPartnerClientMsgNoRequired
	}
	if len(value) > 100 {
		return "", errPartnerClientMsgNoTooLong
	}
	return value, nil
}
func canonicalPayloadHash(payload map[string]interface{}) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
func stablePartnerIMClientMsgNo(senderUID, businessNo string) string {
	sum := sha256.Sum256([]byte(senderUID + "\x00" + businessNo))
	return "partner:" + hex.EncodeToString(sum[:])[:56]
}

func partnerPayloadContentType(payload map[string]interface{}) int {
	if payload == nil {
		return 0
	}
	v, ok := payload["type"]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case int:
		return x
	case int8:
		return int(x)
	case int16:
		return int(x)
	case int32:
		return int(x)
	case int64:
		return int(x)
	case uint:
		return int(x)
	case uint8:
		return int(x)
	case uint16:
		return int(x)
	case uint32:
		return int(x)
	case uint64:
		return int(x)
	case float32:
		return int(x)
	case float64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(x))
		return n
	default:
		n, _ := strconv.Atoi(strings.TrimSpace(fmt.Sprint(v)))
		return n
	}
}
func isPartnerContentType(t int) bool {
	switch t {
	case common.Text.Int(), common.Image.Int(), common.GIF.Int(), common.Voice.Int(), common.Video.Int(), common.File.Int(), common.Location.Int(), common.Card.Int(), common.MultipleForward.Int(), common.VectorSticker.Int(), common.EmojiSticker.Int():
		return true
	}
	return false
}

func (m *Message) preparePartnerTemporaryMessage(fromUID, toUID, clientMsgNo string, payload map[string]interface{}) (*partnerPendingGuard, error) {
	if m == nil || m.ctx == nil || fromUID == "" || toUID == "" || fromUID == toUID {
		return nil, errPartnerRelationDenied
	}
	tx, err := m.ctx.DB().Begin()
	if err != nil {
		return nil, err
	}
	defer tx.RollbackUnlessCommitted()
	var contacts []partnerContactGuardRow
	_, err = tx.SelectBySql(`SELECT requester_uid,status,IFNULL(requester_msg_count,0) requester_msg_count FROM partner_contacts WHERE uid=? AND to_uid=? LIMIT 1 FOR UPDATE`, fromUID, toUID).Load(&contacts)
	if err != nil {
		return nil, err
	}
	if len(contacts) == 0 {
		return nil, errPartnerRelationDenied
	}
	contact := contacts[0]
	guard := &partnerPendingGuard{Handled: true, MessageCount: contact.RequesterMsgCount}
	switch contact.Status {
	case partnerContactActive:
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return guard, nil
	case partnerContactIgnored, partnerContactBlocked:
		return nil, errPartnerRelationDenied
	case partnerContactPending:
		guard.Pending = true
	default:
		return nil, errPartnerRelationDenied
	}
	contentType := partnerPayloadContentType(payload)
	if !isPartnerContentType(contentType) {
		return nil, errPartnerUnsupportedContent
	}
	if contact.RequesterUID != fromUID {
		guard.ReceiverReply = true
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return guard, nil
	}
	guard.Requester = true
	clientMsgNo, err = normalizePartnerClientMsgNo(clientMsgNo, payload)
	if err != nil {
		return nil, err
	}
	payloadHash, err := canonicalPayloadHash(payload)
	if err != nil {
		return nil, err
	}
	guard.ClientMsgNo = clientMsgNo
	guard.IMClientMsgNo = stablePartnerIMClientMsgNo(fromUID, clientMsgNo)
	guard.PayloadHash = payloadHash
	guard.ContentType = contentType
	var rows []partnerPendingMessageRow
	_, err = tx.SelectBySql(`SELECT receiver_uid,client_msg_no,content_type,payload_hash,status,reserved_count,im_client_msg_no,im_message_id,updated_at,IFNULL(next_check_at,0) next_check_at FROM partner_pending_message WHERE sender_uid=? AND client_msg_no=? LIMIT 1 FOR UPDATE`, fromUID, clientMsgNo).Load(&rows)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		r := rows[0]
		if r.ReceiverUID != toUID || r.ContentType != contentType || r.PayloadHash != payloadHash {
			return nil, errPartnerIdempotencyConflict
		}
		if r.IMClientMsgNo != "" {
			guard.IMClientMsgNo = r.IMClientMsgNo
		}
		guard.IMMessageID = r.IMMessageID
		switch r.Status {
		case partnerPendingMessageDelivered:
			guard.DuplicateDelivered = true
			guard.MessageCount = contact.RequesterMsgCount
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return guard, nil
		case partnerPendingMessageReserved:
			if time.Now().UnixMilli()-r.UpdatedAt < 30000 {
				return nil, errPartnerMessageInProgress
			}
			// Claim the stale reservation before reconciliation so only one retry can
			// query/re-send the stable WuKong client_msg_no at a time.
			now := time.Now().UnixMilli()
			if _, err = tx.Update("partner_pending_message").Set("status", partnerPendingMessageReserved).Set("updated_at", now).Set("next_check_at", 0).Where("sender_uid=? AND client_msg_no=?", fromUID, clientMsgNo).Exec(); err != nil {
				return nil, err
			}
			guard.ReuseReservation = true
			guard.NeedsReconcile = true
			guard.Reserved = true
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return guard, nil
		case partnerPendingMessageUncertain:
			now := time.Now().UnixMilli()
			if r.NextCheckAt > now {
				return nil, errPartnerMessageUncertain
			}
			if _, err = tx.Update("partner_pending_message").Set("status", partnerPendingMessageReserved).Set("updated_at", now).Set("next_check_at", 0).Where("sender_uid=? AND client_msg_no=? AND status=?", fromUID, clientMsgNo, partnerPendingMessageUncertain).Exec(); err != nil {
				return nil, err
			}
			guard.ReuseReservation = true
			guard.NeedsReconcile = true
			guard.Reserved = true
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return guard, nil
		case partnerPendingMessageFailed: /* reserve again below */
		default:
			return nil, errPartnerMessageInProgress
		}
	}
	if contact.RequesterMsgCount >= partnerPendingMaxMessages {
		return nil, errPartnerPendingLimit
	}
	now := time.Now().UnixMilli()
	next := contact.RequesterMsgCount + 1
	if len(rows) == 0 {
		_, err = tx.InsertInto("partner_pending_message").Columns("sender_uid", "receiver_uid", "client_msg_no", "content_type", "payload_hash", "reserved_count", "status", "im_client_msg_no", "created_at", "updated_at").Values(fromUID, toUID, clientMsgNo, contentType, payloadHash, 1, partnerPendingMessageReserved, guard.IMClientMsgNo, now, now).Exec()
	} else {
		_, err = tx.Update("partner_pending_message").Set("reserved_count", 1).Set("status", partnerPendingMessageReserved).Set("im_client_msg_no", guard.IMClientMsgNo).Set("im_message_id", "").Set("failed_reason", "").Set("next_check_at", 0).Set("updated_at", now).Where("sender_uid=? AND client_msg_no=?", fromUID, clientMsgNo).Exec()
	}
	if err != nil {
		return nil, err
	}
	result, err := tx.Update("partner_contacts").Set("requester_msg_count", next).Set("last_msg_at", now).Set("updated_at", now).Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=? AND IFNULL(requester_msg_count,0)=?", fromUID, toUID, toUID, fromUID, partnerContactPending, fromUID, contact.RequesterMsgCount).Exec()
	if err != nil {
		return nil, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return nil, errPartnerPendingLimit
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	guard.Reserved = true
	guard.MessageCount = next
	return guard, nil
}

func (m *Message) findPartnerIMMessage(senderUID, receiverUID, imClientMsgNo string) (*config.MessageResp, error) {
	if imClientMsgNo == "" {
		return nil, nil
	}
	resp, err := m.ctx.IMSearchMessages(&config.MsgSearchReq{LoginUID: senderUID, ChannelID: receiverUID, ChannelType: common.ChannelTypePerson.Uint8(), ClientMsgNos: []string{imClientMsgNo}})
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.Messages) == 0 {
		return nil, nil
	}
	return resp.Messages[0], nil
}
func (m *Message) reconcilePartnerPendingMessage(senderUID, receiverUID string, guard *partnerPendingGuard) (*config.MsgSendResp, bool, error) {
	if guard == nil || !guard.NeedsReconcile {
		return nil, false, nil
	}
	msg, err := m.findPartnerIMMessage(senderUID, receiverUID, guard.IMClientMsgNo)
	if err != nil {
		return nil, false, err
	}
	if msg == nil {
		return nil, false, nil
	}
	resp := &config.MsgSendResp{MessageID: msg.MessageID, MessageSeq: msg.MessageSeq, ClientMsgNo: msg.ClientMsgNo}
	if err = m.completePartnerPendingMessageForSender(senderUID, guard, resp); err != nil {
		return nil, false, err
	}
	return resp, true, nil
}

func (m *Message) completePartnerPendingMessageForSender(senderUID string, guard *partnerPendingGuard, imResp *config.MsgSendResp) error {
	if guard == nil || !guard.Reserved || senderUID == "" || guard.ClientMsgNo == "" {
		return nil
	}
	now := time.Now().UnixMilli()
	imNo := guard.IMClientMsgNo
	imID := ""
	if imResp != nil {
		if imResp.ClientMsgNo != "" {
			imNo = imResp.ClientMsgNo
		}
		if imResp.MessageID != 0 {
			imID = strconv.FormatInt(imResp.MessageID, 10)
		}
	}
	_, err := m.ctx.DB().Update("partner_pending_message").Set("status", partnerPendingMessageDelivered).Set("im_client_msg_no", imNo).Set("im_message_id", imID).Set("failed_reason", "").Set("next_check_at", 0).Set("updated_at", now).Where("sender_uid=? AND client_msg_no=? AND status IN ?", senderUID, guard.ClientMsgNo, []int{partnerPendingMessageReserved, partnerPendingMessageUncertain}).Exec()
	return err
}
func (m *Message) markPartnerPendingUncertain(senderUID string, guard *partnerPendingGuard, cause error) error {
	if guard == nil || !guard.Reserved {
		return nil
	}
	reason := "unknown delivery result"
	if cause != nil {
		reason = cause.Error()
	}
	reason = truncatePartnerPendingRunes(reason, 255)
	now := time.Now().UnixMilli()
	_, err := m.ctx.DB().Update("partner_pending_message").Set("status", partnerPendingMessageUncertain).Set("failed_reason", reason).Set("next_check_at", now+5000).Set("updated_at", now).Where("sender_uid=? AND client_msg_no=? AND status IN ?", senderUID, guard.ClientMsgNo, []int{partnerPendingMessageReserved, partnerPendingMessageUncertain}).Exec()
	return err
}
func (m *Message) rollbackPartnerPendingMessage(senderUID, receiverUID string, guard *partnerPendingGuard, cause error) error {
	if guard == nil || !guard.Reserved || senderUID == "" || receiverUID == "" {
		return nil
	}
	tx, err := m.ctx.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	var rows []partnerPendingMessageRow
	_, err = tx.SelectBySql(`SELECT receiver_uid,client_msg_no,content_type,payload_hash,status,reserved_count,im_client_msg_no,im_message_id,updated_at,IFNULL(next_check_at,0) next_check_at FROM partner_pending_message WHERE sender_uid=? AND client_msg_no=? LIMIT 1 FOR UPDATE`, senderUID, guard.ClientMsgNo).Load(&rows)
	if err != nil || len(rows) == 0 {
		return err
	}
	if rows[0].Status == partnerPendingMessageDelivered {
		return tx.Commit()
	}
	now := time.Now().UnixMilli()
	reason := "发送消息失败"
	if cause != nil {
		reason = cause.Error()
	}
	reason = truncatePartnerPendingRunes(reason, 255)
	if rows[0].ReservedCount > 0 {
		_, err = tx.Update("partner_contacts").Set("requester_msg_count", dbr.Expr("GREATEST(IFNULL(requester_msg_count,1)-1,1)")).Set("updated_at", now).Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid=?", senderUID, receiverUID, receiverUID, senderUID, partnerContactPending, senderUID).Exec()
		if err != nil {
			return err
		}
	}
	_, err = tx.Update("partner_pending_message").Set("status", partnerPendingMessageFailed).Set("reserved_count", 0).Set("failed_reason", reason).Set("next_check_at", 0).Set("updated_at", now).Where("sender_uid=? AND client_msg_no=?", senderUID, guard.ClientMsgNo).Exec()
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Activation and permission intents are committed atomically. The partners module
// retries the outbox until both IM whitelist directions are applied.
func (m *Message) activatePartnerContactAfterReply(fromUID, toUID string) error {
	if fromUID == "" || toUID == "" || fromUID == toUID {
		return nil
	}
	now := time.Now().UnixMilli()
	tx, err := m.ctx.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	result, err := tx.Update("partner_contacts").Set("status", partnerContactActive).Set("last_msg_at", now).Set("updated_at", now).Where("((uid=? AND to_uid=?) OR (uid=? AND to_uid=?)) AND status=? AND requester_uid<>?", fromUID, toUID, toUID, fromUID, partnerContactPending, fromUID).Exec()
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return tx.Commit()
	}
	for _, pair := range [][2]string{{fromUID, toUID}, {toUID, fromUID}} {
		key := fmt.Sprintf("active:add:%s:%s", pair[0], pair[1])
		_, err = tx.InsertBySql(`INSERT INTO partner_im_permission_outbox(idempotency_key,channel_uid,member_uid,action,status,attempts,next_retry_at,last_error,created_at,updated_at) VALUES(?,?,?,'add',0,0,0,'',?,?) ON DUPLICATE KEY UPDATE status=IF(status=1,1,0),next_retry_at=0,updated_at=VALUES(updated_at)`, key, pair[0], pair[1], now, now).Exec()
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

type partnerStableSendReq struct {
	Header      config.MsgHeader `json:"header"`
	Setting     uint8            `json:"setting"`
	FromUID     string           `json:"from_uid"`
	ChannelID   string           `json:"channel_id"`
	ChannelType uint8            `json:"channel_type"`
	StreamNo    string           `json:"stream_no"`
	Subscribers []string         `json:"subscribers"`
	Payload     []byte           `json:"payload"`
	ClientMsgNo string           `json:"client_msg_no"`
}
type partnerStableSendResponse struct {
	MessageID   int64  `json:"message_id"`
	MessageSeq  uint32 `json:"message_seq"`
	ClientMsgNo string `json:"client_msg_no"`
	Data        struct {
		MessageID   int64  `json:"message_id"`
		MessageSeq  uint32 `json:"message_seq"`
		ClientMsgNo string `json:"client_msg_no"`
	} `json:"data"`
	Msg string `json:"msg"`
}
type partnerIMSendError struct {
	uncertain bool
	err       error
}

func (e *partnerIMSendError) Error() string {
	if e == nil || e.err == nil {
		return "IM发送失败"
	}
	return e.err.Error()
}
func isUncertainPartnerIMError(err error) bool {
	var target *partnerIMSendError
	return errors.As(err, &target) && target.uncertain
}
func (m *Message) sendPartnerMessageStable(channelID string, channelType uint8, fromUID string, payload map[string]interface{}, clientMsgNo string) (*config.MsgSendResp, error) {
	body, err := json.Marshal(partnerStableSendReq{Header: config.MsgHeader{RedDot: 1}, FromUID: fromUID, ChannelID: channelID, ChannelType: channelType, Payload: []byte(util.ToJson(payload)), ClientMsgNo: clientMsgNo})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(m.ctx.GetConfig().WuKongIM.APIURL, "/")+"/message/send", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, &partnerIMSendError{uncertain: true, err: err}
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return nil, &partnerIMSendError{uncertain: true, err: readErr}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message := strings.TrimSpace(string(data))
		message = truncatePartnerPendingRunes(message, 300)
		lower := strings.ToLower(message)
		uncertain := resp.StatusCode >= 500 || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests || strings.Contains(lower, "duplicate") || strings.Contains(lower, "client_msg_no") || strings.Contains(message, "重复") || strings.Contains(message, "已存在")
		e := fmt.Errorf("IM服务返回状态[%d]: %s", resp.StatusCode, message)
		return nil, &partnerIMSendError{uncertain: uncertain, err: e}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return &config.MsgSendResp{ClientMsgNo: clientMsgNo}, nil
	}
	var parsed partnerStableSendResponse
	if err = json.Unmarshal(data, &parsed); err != nil {
		return nil, &partnerIMSendError{uncertain: true, err: err}
	}
	messageID, messageSeq, responseClientNo := parsed.MessageID, parsed.MessageSeq, parsed.ClientMsgNo
	if parsed.Data.MessageID != 0 || parsed.Data.MessageSeq != 0 || parsed.Data.ClientMsgNo != "" {
		messageID, messageSeq, responseClientNo = parsed.Data.MessageID, parsed.Data.MessageSeq, parsed.Data.ClientMsgNo
	}
	if responseClientNo == "" {
		responseClientNo = clientMsgNo
	}
	return &config.MsgSendResp{MessageID: messageID, MessageSeq: messageSeq, ClientMsgNo: responseClientNo}, nil
}
