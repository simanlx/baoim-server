// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"errors"
	"strconv"
	"time"

	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/msgprocessor"
	"baoim/protocol/constant"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/utils"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	maxSeq                 = "MAX_SEQ:"
	minSeq                 = "MIN_SEQ:"
	conversationUserMinSeq = "CON_USER_MIN_SEQ:"
	hasReadSeq             = "HAS_READ_SEQ:"

	//appleDeviceToken = "DEVICE_TOKEN"
	getuiToken  = "GETUI_TOKEN"
	getuiTaskID = "GETUI_TASK_ID"
	//signalCache      = "SIGNAL_CACHE:"
	//signalListCache  = "SIGNAL_LIST_CACHE:"
	FCM_TOKEN = "FCM_TOKEN:"

	messageCache            = "MESSAGE_CACHE:"
	messageDelUserList      = "MESSAGE_DEL_USER_LIST:"
	userDelMessagesList     = "USER_DEL_MESSAGES_LIST:"
	sendMsgFailedFlag       = "SEND_MSG_FAILED_FLAG:"
	userBadgeUnreadCountSum = "USER_BADGE_UNREAD_COUNT_SUM:"
	exTypeKeyLocker         = "EX_LOCK:"
	uidPidToken             = "UID_PID_TOKEN_STATUS:"

	////增加信令rtc标头
	//signalCache     = "SIGNAL_CACHE:"
	//signalListCache = "SIGNAL_LIST_CACHE:"
)

var concurrentLimit = 3

type SeqCache interface {
	// DelUserSeq 删除用户 最小seq 及 已读seq
	DelUserSeqCache(ctx context.Context, uid string, conversationID string) error

	SetMaxSeq(ctx context.Context, conversationID string, maxSeq int64) error
	GetMaxSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error)
	GetMaxSeq(ctx context.Context, conversationID string) (int64, error)
	SetMinSeq(ctx context.Context, conversationID string, minSeq int64) error
	SetMinSeqs(ctx context.Context, seqs map[string]int64) error
	GetMinSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error)
	GetMinSeq(ctx context.Context, conversationID string) (int64, error)
	GetConversationUserMinSeq(ctx context.Context, conversationID string, userID string) (int64, error)
	GetConversationUserMinSeqs(ctx context.Context, conversationID string, userIDs []string) (map[string]int64, error)
	SetConversationUserMinSeq(ctx context.Context, conversationID string, userID string, minSeq int64) error
	// seqs map: key userID value minSeq
	SetConversationUserMinSeqs(ctx context.Context, conversationID string, seqs map[string]int64) (err error)
	// seqs map: key conversationID value minSeq
	SetUserConversationsMinSeqs(ctx context.Context, userID string, seqs map[string]int64) error
	// has read seq
	SetHasReadSeq(ctx context.Context, userID string, conversationID string, hasReadSeq int64) error
	// k: user, v: seq
	SetHasReadSeqs(ctx context.Context, conversationID string, hasReadSeqs map[string]int64) error
	// k: conversation, v :seq
	UserSetHasReadSeqs(ctx context.Context, userID string, hasReadSeqs map[string]int64) error
	GetHasReadSeqs(ctx context.Context, userID string, conversationIDs []string) (map[string]int64, error)
	GetHasReadSeq(ctx context.Context, userID string, conversationID string) (int64, error)
}

type thirdCache interface {
	SetFcmToken(ctx context.Context, account string, platformID int, fcmToken string, expireTime int64) (err error)
	GetFcmToken(ctx context.Context, account string, platformID int) (string, error)
	DelFcmToken(ctx context.Context, account string, platformID int) error
	IncrUserBadgeUnreadCountSum(ctx context.Context, userID string) (int, error)
	SetUserBadgeUnreadCountSum(ctx context.Context, userID string, value int) error
	GetUserBadgeUnreadCountSum(ctx context.Context, userID string) (int, error)
	SetGetuiToken(ctx context.Context, token string, expireTime int64) error
	GetGetuiToken(ctx context.Context) (string, error)
	SetGetuiTaskID(ctx context.Context, taskID string, expireTime int64) error
	GetGetuiTaskID(ctx context.Context) (string, error)
}

type MsgModel interface {
	SeqCache
	thirdCache
	AddTokenFlag(ctx context.Context, userID string, platformID int, token string, flag int) error
	GetTokensWithoutError(ctx context.Context, userID string, platformID int) (map[string]int, error)
	SetTokenMapByUidPid(ctx context.Context, userID string, platformID int, m map[string]int) error
	DeleteTokenByUidPid(ctx context.Context, userID string, platformID int, fields []string) error
	GetMessagesBySeq(ctx context.Context, conversationID string, seqs []int64) (seqMsg []*sdkws.MsgData, failedSeqList []int64, err error)
	SetMessageToCache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (int, error)
	UserDeleteMsgs(ctx context.Context, conversationID string, seqs []int64, userID string) error
	DelUserDeleteMsgsList(ctx context.Context, conversationID string, seqs []int64)
	DeleteMessages(ctx context.Context, conversationID string, seqs []int64) error
	GetUserDelList(ctx context.Context, userID, conversationID string) (seqs []int64, err error)
	CleanUpOneConversationAllMsg(ctx context.Context, conversationID string) error
	DelMsgFromCache(ctx context.Context, userID string, seqList []int64) error
	SetSendMsgStatus(ctx context.Context, id string, status int32) error
	GetSendMsgStatus(ctx context.Context, id string) (int32, error)
	JudgeMessageReactionExist(ctx context.Context, clientMsgID string, sessionType int32) (bool, error)
	GetOneMessageAllReactionList(ctx context.Context, clientMsgID string, sessionType int32) (map[string]string, error)
	DeleteOneMessageKey(ctx context.Context, clientMsgID string, sessionType int32, subKey string) error
	SetMessageReactionExpire(ctx context.Context, clientMsgID string, sessionType int32, expiration time.Duration) (bool, error)
	GetMessageTypeKeyValue(ctx context.Context, clientMsgID string, sessionType int32, typeKey string) (string, error)
	SetMessageTypeKeyValue(ctx context.Context, clientMsgID string, sessionType int32, typeKey, value string) error
	LockMessageTypeKey(ctx context.Context, clientMsgID string, TypeKey string) error
	UnLockMessageTypeKey(ctx context.Context, clientMsgID string, TypeKey string) error

	///增加信令Signal
	//HandleSignalInvite(ctx context.Context, msg *sdkws.MsgData, pushToUserID string) (isSend bool, err error)
	//GetSignalInvitationInfoByClientMsgID(ctx context.Context, clientMsgID string) (invitationInfo *rtc.SignalInviteReq, err error)
	//GetAvailableSignalInvitationInfo(ctx context.Context, userID string) (invitationInfo *rtc.SignalInviteReq, err error)
	//DelUserSignalList(ctx context.Context, userID string) error
}

func NewMsgCacheModel(client redis.UniversalClient, config *config.GlobalConfig) MsgModel {
	return &msgCache{rdb: client, config: config}
}

type msgCache struct {
	metaCache
	rdb    redis.UniversalClient
	config *config.GlobalConfig
}

func (c *msgCache) getMaxSeqKey(conversationID string) string {
	return maxSeq + conversationID
}

func (c *msgCache) getMinSeqKey(conversationID string) string {
	return minSeq + conversationID
}

func (c *msgCache) getHasReadSeqKey(conversationID string, userID string) string {

	return hasReadSeq + userID + ":" + conversationID
}

func (c *msgCache) getConversationUserMinSeqKey(conversationID, userID string) string {
	return conversationUserMinSeq + conversationID + "u:" + userID
}

func (c *msgCache) setSeq(ctx context.Context, conversationID string, seq int64, getkey func(conversationID string) string) error {
	return errs.Wrap(c.rdb.Set(ctx, getkey(conversationID), seq, 0).Err())
}

// 增加 删除用户 最小seq 及 已读seq
func (c *msgCache) DelUserSeqCache(ctx context.Context, uid string, conversationID string) error {
	keys := []string{
		c.getConversationUserMinSeqKey(conversationID, uid),
		hasReadSeq + uid + ":" + conversationID,
	}
	return errs.Wrap(c.rdb.Del(ctx, keys...).Err())
}

func (c *msgCache) getSeq(ctx context.Context, conversationID string, getkey func(conversationID string) string) (int64, error) {
	val, err := c.rdb.Get(ctx, getkey(conversationID)).Int64()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return val, nil
}

func (c *msgCache) getSeqs(ctx context.Context, items []string, getkey func(s string) string) (m map[string]int64, err error) {
	m = make(map[string]int64, len(items))
	for i, v := range items {
		res, err := c.rdb.Get(ctx, getkey(v)).Result()
		if err != nil && err != redis.Nil {
			return nil, errs.Wrap(err)
		}
		val := utils.StringToInt64(res)
		if val != 0 {
			m[items[i]] = val
		}
	}

	return m, nil
}

func (c *msgCache) SetMaxSeq(ctx context.Context, conversationID string, maxSeq int64) error {
	return c.setSeq(ctx, conversationID, maxSeq, c.getMaxSeqKey)
}

func (c *msgCache) GetMaxSeqs(ctx context.Context, conversationIDs []string) (m map[string]int64, err error) {
	return c.getSeqs(ctx, conversationIDs, c.getMaxSeqKey)
}

func (c *msgCache) GetMaxSeq(ctx context.Context, conversationID string) (int64, error) {
	return c.getSeq(ctx, conversationID, c.getMaxSeqKey)
}

func (c *msgCache) SetMinSeq(ctx context.Context, conversationID string, minSeq int64) error {
	return c.setSeq(ctx, conversationID, minSeq, c.getMinSeqKey)
}

func (c *msgCache) setSeqs(ctx context.Context, seqs map[string]int64, getkey func(key string) string) error {
	for conversationID, seq := range seqs {
		if err := c.rdb.Set(ctx, getkey(conversationID), seq, 0).Err(); err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (c *msgCache) SetMinSeqs(ctx context.Context, seqs map[string]int64) error {
	return c.setSeqs(ctx, seqs, c.getMinSeqKey)
}

func (c *msgCache) GetMinSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error) {
	return c.getSeqs(ctx, conversationIDs, c.getMinSeqKey)
}

func (c *msgCache) GetMinSeq(ctx context.Context, conversationID string) (int64, error) {
	return c.getSeq(ctx, conversationID, c.getMinSeqKey)
}

func (c *msgCache) GetConversationUserMinSeq(ctx context.Context, conversationID string, userID string) (int64, error) {
	val, err := c.rdb.Get(ctx, c.getConversationUserMinSeqKey(conversationID, userID)).Int64()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	return val, nil
}

func (c *msgCache) GetConversationUserMinSeqs(ctx context.Context, conversationID string, userIDs []string) (m map[string]int64, err error) {
	return c.getSeqs(ctx, userIDs, func(userID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

func (c *msgCache) SetConversationUserMinSeq(ctx context.Context, conversationID string, userID string, minSeq int64) error {
	return errs.Wrap(c.rdb.Set(ctx, c.getConversationUserMinSeqKey(conversationID, userID), minSeq, 0).Err())
}

func (c *msgCache) SetConversationUserMinSeqs(ctx context.Context, conversationID string, seqs map[string]int64) (err error) {
	return c.setSeqs(ctx, seqs, func(userID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

func (c *msgCache) SetUserConversationsMinSeqs(ctx context.Context, userID string, seqs map[string]int64) (err error) {
	return c.setSeqs(ctx, seqs, func(conversationID string) string {
		return c.getConversationUserMinSeqKey(conversationID, userID)
	})
}

func (c *msgCache) SetHasReadSeq(ctx context.Context, userID string, conversationID string, hasReadSeq int64) error {

	return errs.Wrap(c.rdb.Set(ctx, c.getHasReadSeqKey(conversationID, userID), hasReadSeq, 0).Err())
}

func (c *msgCache) SetHasReadSeqs(ctx context.Context, conversationID string, hasReadSeqs map[string]int64) error {

	return c.setSeqs(ctx, hasReadSeqs, func(userID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

func (c *msgCache) UserSetHasReadSeqs(ctx context.Context, userID string, hasReadSeqs map[string]int64) error {

	return c.setSeqs(ctx, hasReadSeqs, func(conversationID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

func (c *msgCache) GetHasReadSeqs(ctx context.Context, userID string, conversationIDs []string) (map[string]int64, error) {

	return c.getSeqs(ctx, conversationIDs, func(conversationID string) string {
		return c.getHasReadSeqKey(conversationID, userID)
	})
}

func (c *msgCache) GetHasReadSeq(ctx context.Context, userID string, conversationID string) (int64, error) {

	val, err := c.rdb.Get(ctx, c.getHasReadSeqKey(conversationID, userID)).Int64()
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (c *msgCache) AddTokenFlag(ctx context.Context, userID string, platformID int, token string, flag int) error {
	key := uidPidToken + userID + ":" + constant.PlatformIDToName(platformID)
	return errs.Wrap(c.rdb.HSet(ctx, key, token, flag).Err())
}

func (c *msgCache) GetTokensWithoutError(ctx context.Context, userID string, platformID int) (map[string]int, error) {
	key := uidPidToken + userID + ":" + constant.PlatformIDToName(platformID)
	m, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, errs.Wrap(err)
	}
	mm := make(map[string]int)
	for k, v := range m {
		mm[k] = utils.StringToInt(v)
	}

	return mm, nil
}

func (c *msgCache) SetTokenMapByUidPid(ctx context.Context, userID string, platform int, m map[string]int) error {
	key := uidPidToken + userID + ":" + constant.PlatformIDToName(platform)
	mm := make(map[string]any)
	for k, v := range m {
		mm[k] = v
	}

	return errs.Wrap(c.rdb.HSet(ctx, key, mm).Err())
}

func (c *msgCache) DeleteTokenByUidPid(ctx context.Context, userID string, platform int, fields []string) error {
	key := uidPidToken + userID + ":" + constant.PlatformIDToName(platform)

	return errs.Wrap(c.rdb.HDel(ctx, key, fields...).Err())
}

func (c *msgCache) getMessageCacheKey(conversationID string, seq int64) string {
	return messageCache + conversationID + "_" + strconv.Itoa(int(seq))
}

func (c *msgCache) allMessageCacheKey(conversationID string) string {
	return messageCache + conversationID + "_*"
}

func (c *msgCache) GetMessagesBySeq(ctx context.Context, conversationID string, seqs []int64) (seqMsgs []*sdkws.MsgData, failedSeqs []int64, err error) {
	if c.config.Redis.EnablePipeline {
		return c.PipeGetMessagesBySeq(ctx, conversationID, seqs)
	}

	return c.ParallelGetMessagesBySeq(ctx, conversationID, seqs)
}

func (c *msgCache) PipeGetMessagesBySeq(ctx context.Context, conversationID string, seqs []int64) (seqMsgs []*sdkws.MsgData, failedSeqs []int64, err error) {
	pipe := c.rdb.Pipeline()

	results := []*redis.StringCmd{}
	for _, seq := range seqs {
		results = append(results, pipe.Get(ctx, c.getMessageCacheKey(conversationID, seq)))
	}

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return seqMsgs, failedSeqs, errs.Wrap(err, "pipe.get")
	}

	for idx, res := range results {
		seq := seqs[idx]
		if res.Err() != nil {
			log.ZError(ctx, "GetMessagesBySeq failed", err, "conversationID", conversationID, "seq", seq, "err", res.Err())
			failedSeqs = append(failedSeqs, seq)
			continue
		}

		msg := sdkws.MsgData{}
		if err = msgprocessor.String2Pb(res.Val(), &msg); err != nil {
			log.ZError(ctx, "GetMessagesBySeq Unmarshal failed", err, "res", res, "conversationID", conversationID, "seq", seq)
			failedSeqs = append(failedSeqs, seq)
			continue
		}

		if msg.Status == constant.MsgDeleted {
			failedSeqs = append(failedSeqs, seq)
			continue
		}

		seqMsgs = append(seqMsgs, &msg)
	}

	return
}

func (c *msgCache) ParallelGetMessagesBySeq(ctx context.Context, conversationID string, seqs []int64) (seqMsgs []*sdkws.MsgData, failedSeqs []int64, err error) {
	type entry struct {
		err error
		msg *sdkws.MsgData
	}

	wg := errgroup.Group{}
	wg.SetLimit(concurrentLimit)

	results := make([]entry, len(seqs)) // set slice len/cap to length of seqs.
	for idx, seq := range seqs {
		// closure safe var
		idx := idx
		seq := seq

		wg.Go(func() error {
			res, err := c.rdb.Get(ctx, c.getMessageCacheKey(conversationID, seq)).Result()
			if err != nil {
				log.ZError(ctx, "GetMessagesBySeq failed", err, "conversationID", conversationID, "seq", seq)
				results[idx] = entry{err: err}
				return nil
			}

			msg := sdkws.MsgData{}
			if err = msgprocessor.String2Pb(res, &msg); err != nil {
				log.ZError(ctx, "GetMessagesBySeq Unmarshal failed", err, "res", res, "conversationID", conversationID, "seq", seq)
				results[idx] = entry{err: err}
				return nil
			}

			if msg.Status == constant.MsgDeleted {
				results[idx] = entry{err: err}
				return nil
			}

			results[idx] = entry{msg: &msg}
			return nil
		})
	}

	_ = wg.Wait()

	for idx, res := range results {
		if res.err != nil {
			failedSeqs = append(failedSeqs, seqs[idx])
			continue
		}

		seqMsgs = append(seqMsgs, res.msg)
	}

	return
}

func (c *msgCache) SetMessageToCache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (int, error) {
	if c.config.Redis.EnablePipeline {
		return c.PipeSetMessageToCache(ctx, conversationID, msgs)
	}
	return c.ParallelSetMessageToCache(ctx, conversationID, msgs)
}

func (c *msgCache) PipeSetMessageToCache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (int, error) {
	pipe := c.rdb.Pipeline()
	for _, msg := range msgs {
		s, err := msgprocessor.Pb2String(msg)
		if err != nil {
			return 0, err
		}

		key := c.getMessageCacheKey(conversationID, msg.Seq)
		_ = pipe.Set(ctx, key, s, time.Duration(c.config.MsgCacheTimeout)*time.Second)
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	for _, res := range results {
		if res.Err() != nil {
			return 0, errs.Wrap(err)
		}
	}

	return len(msgs), nil
}

func (c *msgCache) ParallelSetMessageToCache(ctx context.Context, conversationID string, msgs []*sdkws.MsgData) (int, error) {
	wg := errgroup.Group{}
	wg.SetLimit(concurrentLimit)

	for _, msg := range msgs {
		msg := msg // closure safe var
		wg.Go(func() error {
			s, err := msgprocessor.Pb2String(msg)
			if err != nil {
				return errs.Wrap(err)
			}

			key := c.getMessageCacheKey(conversationID, msg.Seq)
			if err := c.rdb.Set(ctx, key, s, time.Duration(c.config.MsgCacheTimeout)*time.Second).Err(); err != nil {
				return errs.Wrap(err)
			}
			return nil
		})
	}

	err := wg.Wait()
	if err != nil {
		return 0, errs.Wrap(err, "wg.Wait failed")
	}

	return len(msgs), nil
}

func (c *msgCache) getMessageDelUserListKey(conversationID string, seq int64) string {
	return messageDelUserList + conversationID + ":" + strconv.Itoa(int(seq))
}

func (c *msgCache) getUserDelList(conversationID, userID string) string {
	return userDelMessagesList + conversationID + ":" + userID
}

func (c *msgCache) UserDeleteMsgs(ctx context.Context, conversationID string, seqs []int64, userID string) error {
	for _, seq := range seqs {
		delUserListKey := c.getMessageDelUserListKey(conversationID, seq)
		userDelListKey := c.getUserDelList(conversationID, userID)
		err := c.rdb.SAdd(ctx, delUserListKey, userID).Err()
		if err != nil {
			return errs.Wrap(err)
		}
		err = c.rdb.SAdd(ctx, userDelListKey, seq).Err()
		if err != nil {
			return errs.Wrap(err)
		}
		if err := c.rdb.Expire(ctx, delUserListKey, time.Duration(c.config.MsgCacheTimeout)*time.Second).Err(); err != nil {
			return errs.Wrap(err)
		}
		if err := c.rdb.Expire(ctx, userDelListKey, time.Duration(c.config.MsgCacheTimeout)*time.Second).Err(); err != nil {
			return errs.Wrap(err)
		}
	}

	return nil
	//pipe := c.rdb.Pipeline()
	//for _, seq := range seqs {
	//	delUserListKey := c.getMessageDelUserListKey(conversationID, seq)
	//	userDelListKey := c.getUserDelList(conversationID, userID)
	//	err := pipe.SAdd(ctx, delUserListKey, userID).Err()
	//	if err != nil {
	//		return errs.Wrap(err)
	//	}
	//	err = pipe.SAdd(ctx, userDelListKey, seq).Err()
	//	if err != nil {
	//		return errs.Wrap(err)
	//	}
	//	if err := pipe.Expire(ctx, delUserListKey, time.Duration(config.Config.MsgCacheTimeout)*time.Second).Err(); err != nil {
	//		return errs.Wrap(err)
	//	}
	//	if err := pipe.Expire(ctx, userDelListKey, time.Duration(config.Config.MsgCacheTimeout)*time.Second).Err(); err != nil {
	//		return errs.Wrap(err)
	//	}
	//}
	//_, err := pipe.Exec(ctx)
	//return errs.Wrap(err)
}

func (c *msgCache) GetUserDelList(ctx context.Context, userID, conversationID string) (seqs []int64, err error) {
	result, err := c.rdb.SMembers(ctx, c.getUserDelList(conversationID, userID)).Result()
	if err != nil {
		return nil, errs.Wrap(err)
	}
	seqs = make([]int64, len(result))
	for i, v := range result {
		seqs[i] = utils.StringToInt64(v)
	}

	return seqs, nil
}

func (c *msgCache) DelUserDeleteMsgsList(ctx context.Context, conversationID string, seqs []int64) {
	for _, seq := range seqs {
		delUsers, err := c.rdb.SMembers(ctx, c.getMessageDelUserListKey(conversationID, seq)).Result()
		if err != nil {
			log.ZWarn(ctx, "DelUserDeleteMsgsList failed", err, "conversationID", conversationID, "seq", seq)

			continue
		}
		if len(delUsers) > 0 {
			var failedFlag bool
			for _, userID := range delUsers {
				err = c.rdb.SRem(ctx, c.getUserDelList(conversationID, userID), seq).Err()
				if err != nil {
					failedFlag = true
					log.ZWarn(ctx, "DelUserDeleteMsgsList failed", err, "conversationID", conversationID, "seq", seq, "userID", userID)
				}
			}
			if !failedFlag {
				if err := c.rdb.Del(ctx, c.getMessageDelUserListKey(conversationID, seq)).Err(); err != nil {
					log.ZWarn(ctx, "DelUserDeleteMsgsList failed", err, "conversationID", conversationID, "seq", seq)
				}
			}
		}
	}
	//for _, seq := range seqs {
	//	delUsers, err := c.rdb.SMembers(ctx, c.getMessageDelUserListKey(conversationID, seq)).Result()
	//	if err != nil {
	//		log.ZWarn(ctx, "DelUserDeleteMsgsList failed", err, "conversationID", conversationID, "seq", seq)
	//		continue
	//	}
	//	if len(delUsers) > 0 {
	//		pipe := c.rdb.Pipeline()
	//		var failedFlag bool
	//		for _, userID := range delUsers {
	//			err = pipe.SRem(ctx, c.getUserDelList(conversationID, userID), seq).Err()
	//			if err != nil {
	//				failedFlag = true
	//				log.ZWarn(
	//					ctx,
	//					"DelUserDeleteMsgsList failed",
	//					err,
	//					"conversationID",
	//					conversationID,
	//					"seq",
	//					seq,
	//					"userID",
	//					userID,
	//				)
	//			}
	//		}
	//		if !failedFlag {
	//			if err := pipe.Del(ctx, c.getMessageDelUserListKey(conversationID, seq)).Err(); err != nil {
	//				log.ZWarn(ctx, "DelUserDeleteMsgsList failed", err, "conversationID", conversationID, "seq", seq)
	//			}
	//		}
	//		if _, err := pipe.Exec(ctx); err != nil {
	//			log.ZError(ctx, "pipe exec failed", err, "conversationID", conversationID, "seq", seq)
	//		}
	//	}
	//}
}

func (c *msgCache) DeleteMessages(ctx context.Context, conversationID string, seqs []int64) error {
	if c.config.Redis.EnablePipeline {
		return c.PipeDeleteMessages(ctx, conversationID, seqs)
	}

	return c.ParallelDeleteMessages(ctx, conversationID, seqs)
}

func (c *msgCache) ParallelDeleteMessages(ctx context.Context, conversationID string, seqs []int64) error {
	wg := errgroup.Group{}
	wg.SetLimit(concurrentLimit)

	for _, seq := range seqs {
		seq := seq
		wg.Go(func() error {
			err := c.rdb.Del(ctx, c.getMessageCacheKey(conversationID, seq)).Err()
			if err != nil {
				return errs.Wrap(err)
			}
			return nil
		})
	}

	return wg.Wait()
}

func (c *msgCache) PipeDeleteMessages(ctx context.Context, conversationID string, seqs []int64) error {
	pipe := c.rdb.Pipeline()
	for _, seq := range seqs {
		_ = pipe.Del(ctx, c.getMessageCacheKey(conversationID, seq))
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return errs.Wrap(err, "pipe.del")
	}

	for _, res := range results {
		if res.Err() != nil {
			return errs.Wrap(err)
		}
	}

	return nil
}

func (c *msgCache) CleanUpOneConversationAllMsg(ctx context.Context, conversationID string) error {
	vals, err := c.rdb.Keys(ctx, c.allMessageCacheKey(conversationID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return errs.Wrap(err)
	}
	for _, v := range vals {
		if err := c.rdb.Del(ctx, v).Err(); err != nil {
			return errs.Wrap(err)
		}
	}
	return nil
}

func (c *msgCache) DelMsgFromCache(ctx context.Context, userID string, seqs []int64) error {
	for _, seq := range seqs {
		key := c.getMessageCacheKey(userID, seq)
		result, err := c.rdb.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}

			return errs.Wrap(err)
		}
		var msg sdkws.MsgData
		err = jsonpb.UnmarshalString(result, &msg)
		if err != nil {
			return err
		}
		msg.Status = constant.MsgDeleted
		s, err := msgprocessor.Pb2String(&msg)
		if err != nil {
			return errs.Wrap(err)
		}
		if err := c.rdb.Set(ctx, key, s, time.Duration(c.config.MsgCacheTimeout)*time.Second).Err(); err != nil {
			return errs.Wrap(err)
		}
	}

	return nil
}

func (c *msgCache) SetGetuiToken(ctx context.Context, token string, expireTime int64) error {
	return errs.Wrap(c.rdb.Set(ctx, getuiToken, token, time.Duration(expireTime)*time.Second).Err())
}

func (c *msgCache) GetGetuiToken(ctx context.Context) (string, error) {
	val, err := c.rdb.Get(ctx, getuiToken).Result()
	if err != nil {
		return "", errs.Wrap(err)
	}
	return val, nil
}

func (c *msgCache) SetGetuiTaskID(ctx context.Context, taskID string, expireTime int64) error {
	return errs.Wrap(c.rdb.Set(ctx, getuiTaskID, taskID, time.Duration(expireTime)*time.Second).Err())
}

func (c *msgCache) GetGetuiTaskID(ctx context.Context) (string, error) {
	val, err := c.rdb.Get(ctx, getuiTaskID).Result()
	if err != nil {
		return "", errs.Wrap(err)
	}
	return val, nil
}

func (c *msgCache) SetSendMsgStatus(ctx context.Context, id string, status int32) error {
	return errs.Wrap(c.rdb.Set(ctx, sendMsgFailedFlag+id, status, time.Hour*24).Err())
}

func (c *msgCache) GetSendMsgStatus(ctx context.Context, id string) (int32, error) {
	result, err := c.rdb.Get(ctx, sendMsgFailedFlag+id).Int()

	return int32(result), errs.Wrap(err)
}

func (c *msgCache) SetFcmToken(ctx context.Context, account string, platformID int, fcmToken string, expireTime int64) (err error) {
	return errs.Wrap(c.rdb.Set(ctx, FCM_TOKEN+account+":"+strconv.Itoa(platformID), fcmToken, time.Duration(expireTime)*time.Second).Err())
}

func (c *msgCache) GetFcmToken(ctx context.Context, account string, platformID int) (string, error) {
	val, err := c.rdb.Get(ctx, FCM_TOKEN+account+":"+strconv.Itoa(platformID)).Result()
	if err != nil {
		return "", errs.Wrap(err)
	}
	return val, nil
}

func (c *msgCache) DelFcmToken(ctx context.Context, account string, platformID int) error {
	return errs.Wrap(c.rdb.Del(ctx, FCM_TOKEN+account+":"+strconv.Itoa(platformID)).Err())
}

func (c *msgCache) IncrUserBadgeUnreadCountSum(ctx context.Context, userID string) (int, error) {
	seq, err := c.rdb.Incr(ctx, userBadgeUnreadCountSum+userID).Result()

	return int(seq), errs.Wrap(err)
}

func (c *msgCache) SetUserBadgeUnreadCountSum(ctx context.Context, userID string, value int) error {
	return errs.Wrap(c.rdb.Set(ctx, userBadgeUnreadCountSum+userID, value, 0).Err())
}

func (c *msgCache) GetUserBadgeUnreadCountSum(ctx context.Context, userID string) (int, error) {
	val, err := c.rdb.Get(ctx, userBadgeUnreadCountSum+userID).Int()
	return val, errs.Wrap(err)
}

func (c *msgCache) LockMessageTypeKey(ctx context.Context, clientMsgID string, TypeKey string) error {
	key := exTypeKeyLocker + clientMsgID + "_" + TypeKey

	return errs.Wrap(c.rdb.SetNX(ctx, key, 1, time.Minute).Err())
}

func (c *msgCache) UnLockMessageTypeKey(ctx context.Context, clientMsgID string, TypeKey string) error {
	key := exTypeKeyLocker + clientMsgID + "_" + TypeKey

	return errs.Wrap(c.rdb.Del(ctx, key).Err())
}

func (c *msgCache) getMessageReactionExPrefix(clientMsgID string, sessionType int32) string {
	switch sessionType {
	case constant.SingleChatType:
		return "EX_SINGLE_" + clientMsgID
	case constant.GroupChatType:
		return "EX_GROUP_" + clientMsgID
	case constant.SuperGroupChatType:
		return "EX_SUPER_GROUP_" + clientMsgID
	case constant.NotificationChatType:
		return "EX_NOTIFICATION" + clientMsgID
	}

	return ""
}

func (c *msgCache) JudgeMessageReactionExist(ctx context.Context, clientMsgID string, sessionType int32) (bool, error) {
	n, err := c.rdb.Exists(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType)).Result()
	if err != nil {
		return false, errs.Wrap(err)
	}

	return n > 0, nil
}

func (c *msgCache) SetMessageTypeKeyValue(ctx context.Context, clientMsgID string, sessionType int32, typeKey, value string) error {
	return errs.Wrap(c.rdb.HSet(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType), typeKey, value).Err())
}

func (c *msgCache) SetMessageReactionExpire(ctx context.Context, clientMsgID string, sessionType int32, expiration time.Duration) (bool, error) {
	val, err := c.rdb.Expire(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType), expiration).Result()
	return val, errs.Wrap(err)
}

func (c *msgCache) GetMessageTypeKeyValue(ctx context.Context, clientMsgID string, sessionType int32, typeKey string) (string, error) {
	val, err := c.rdb.HGet(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType), typeKey).Result()
	return val, errs.Wrap(err)
}

func (c *msgCache) GetOneMessageAllReactionList(ctx context.Context, clientMsgID string, sessionType int32) (map[string]string, error) {
	val, err := c.rdb.HGetAll(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType)).Result()
	return val, errs.Wrap(err)
}

func (c *msgCache) DeleteOneMessageKey(ctx context.Context, clientMsgID string, sessionType int32, subKey string) error {
	return errs.Wrap(c.rdb.HDel(ctx, c.getMessageReactionExPrefix(clientMsgID, sessionType), subKey).Err())
}

//
//// /////////////////增加 信令redis///////////////
//// HandleSignalInvite 处理信号邀请消息，判断是否需要发送并进行缓存处理
//// ctx: 上下文，用于控制超时和取消
//// msg: 消息数据
//// pushToUserID: 推送目标用户ID
//// 返回值: 是否发送成功，错误信息
//func (c *msgCache) HandleSignalInvite(ctx context.Context, msg *sdkws.MsgData, pushToUserID string) (isSend bool, err error) {
//	// 定义信号请求结构体，用于解析消息内容
//	req := &rtc.SignalReq{}
//	// 将消息内容（protobuf格式）解析到信号请求结构体中
//	if err := proto.Unmarshal(msg.Content, req); err != nil {
//		// 解析失败时包装错误并返回
//		return false, errs.Wrap(err)
//	}
//	// 存储被邀请用户ID列表
//	var inviteeUserIDs []string
//	// 标记是否为邀请类型信号
//	var isInviteSignal bool
//	// 根据信号请求的负载类型进行不同处理（类型断言）
//	switch signalInfo := req.Payload.(type) {
//	// 处理一对一邀请信号
//	case *rtc.SignalReq_Invite:
//		// 获取被邀请用户ID列表
//		inviteeUserIDs = signalInfo.Invite.Invitation.InviteeUserIDList
//		// 标记为邀请类型信号
//		isInviteSignal = true
//	// 处理群组内邀请信号
//	case *rtc.SignalReq_InviteInGroup:
//		// 获取被邀请用户ID列表
//		inviteeUserIDs = signalInfo.InviteInGroup.Invitation.InviteeUserIDList
//		// 标记为邀请类型信号
//		isInviteSignal = true
//		// 检查推送目标用户是否在被邀请列表中，不在则不处理
//		if !utils.Contain(pushToUserID, inviteeUserIDs...) {
//			return false, nil
//		}
//	// 处理挂断、取消、拒绝、接受等信号（这些信号不需要离线推送）
//	case *rtc.SignalReq_HungUp, *rtc.SignalReq_Cancel, *rtc.SignalReq_Reject, *rtc.SignalReq_Accept:
//		return false, errs.Wrap(errors.New("signalInfo do not need offlinePush"))
//	// 其他类型信号不处理
//	default:
//		return false, nil
//	}
//
//	// 如果是邀请类型信号，进行缓存处理
//	if isInviteSignal {
//		// 创建Redis管道，用于批量执行命令提升效率
//		pipe := c.rdb.Pipeline()
//		// 遍历所有被邀请用户，为每个用户缓存信号信息
//		for _, userID := range inviteeUserIDs {
//			// 从配置中获取信号超时时间（字符串转整数）
//			timeout, err := strconv.Atoi(c.config.Rtc.SignalTimeout)
//			if err != nil {
//
//				// 转换失败时包装错误并返回
//				return false, errs.Wrap(err)
//			}
//			// 构建用户信号列表的Redis键名（格式：signalListCache+用户ID）
//			keys := signalListCache + userID
//			// 向列表左侧添加消息ID（LPush：新元素加入列表头部）
//			err = pipe.LPush(ctx, keys, msg.ClientMsgID).Err()
//			if err != nil {
//				return false, errs.Wrap(err)
//			}
//			// 设置列表的过期时间（避免缓存长期占用内存）
//			err = pipe.Expire(ctx, keys, time.Duration(timeout)*time.Second).Err()
//			if err != nil {
//				return false, errs.Wrap(err)
//			}
//			// 构建信号内容的Redis键名（格式：signalCache+消息ID）
//			key := signalCache + msg.ClientMsgID
//			// 存储消息内容到Redis，并设置过期时间
//			err = pipe.Set(ctx, key, msg.Content, time.Duration(timeout)*time.Second).Err()
//			if err != nil {
//				return false, errs.Wrap(err)
//			}
//		}
//		// 执行管道中的所有命令
//		_, err := pipe.Exec(ctx)
//		if err != nil {
//
//			return false, errs.Wrap(err)
//		}
//	}
//	// 处理完成，返回成功
//	return true, nil
//}
//
//// GetSignalInvitationInfoByClientMsgID 根据客户端消息ID获取信号邀请详情
//// ctx: 上下文
//// clientMsgID: 客户端消息ID
//// 返回值: 信号邀请请求结构体，错误信息
//func (c *msgCache) GetSignalInvitationInfoByClientMsgID(ctx context.Context, clientMsgID string) (signalInviteReq *rtc.SignalInviteReq, err error) {
//	// 从Redis中获取指定消息ID的信号内容（二进制格式）
//	bytes, err := c.rdb.Get(ctx, signalCache+clientMsgID).Bytes()
//	if err != nil {
//		return nil, errs.Wrap(err)
//	}
//	// 定义信号请求结构体，用于解析Redis中的内容
//	signalReq := &rtc.SignalReq{}
//	// 将二进制内容解析为信号请求结构体（protobuf反序列化）
//	if err = proto.Unmarshal(bytes, signalReq); err != nil {
//		return nil, errs.Wrap(err)
//	}
//	// 初始化信号邀请请求结构体
//	signalInviteReq = &rtc.SignalInviteReq{}
//	// 根据信号请求的负载类型提取邀请信息
//	switch req := signalReq.Payload.(type) {
//	// 处理一对一邀请类型
//	case *rtc.SignalReq_Invite:
//		// 复制邀请信息和用户ID
//		signalInviteReq.Invitation = req.Invite.Invitation
//		signalInviteReq.UserID = req.Invite.UserID
//	// 处理群组内邀请类型
//	case *rtc.SignalReq_InviteInGroup:
//		// 复制邀请信息和用户ID
//		signalInviteReq.Invitation = req.InviteInGroup.Invitation
//		signalInviteReq.UserID = req.InviteInGroup.UserID
//	}
//	// 返回提取到的邀请信息
//	return signalInviteReq, nil
//}
//
//// GetAvailableSignalInvitationInfo 获取用户的可用信号邀请信息（从缓存中取最新的一条）
//// ctx: 上下文
//// userID: 用户ID
//// 返回值: 信号邀请请求结构体，错误信息
//func (c *msgCache) GetAvailableSignalInvitationInfo(ctx context.Context, userID string) (invitationInfo *rtc.SignalInviteReq, err error) {
//	// 从用户的信号列表中弹出最左侧的消息ID（LPop：获取并移除列表头部元素）
//	key, err := c.rdb.LPop(ctx, signalListCache+userID).Result()
//	if err != nil {
//		return nil, errs.Wrap(err)
//	}
//	// 根据弹出的消息ID获取对应的邀请详情
//	invitationInfo, err = c.GetSignalInvitationInfoByClientMsgID(ctx, key)
//	if err != nil {
//		return nil, err
//	}
//	// 获取详情后删除该用户的信号列表，并返回邀请信息
//	return invitationInfo, errs.Wrap(c.DelUserSignalList(ctx, userID))
//}
//
//// DelUserSignalList 删除用户的信号列表缓存
//// ctx: 上下文
//// userID: 用户ID
//// 返回值: 错误信息
//func (c *msgCache) DelUserSignalList(ctx context.Context, userID string) error {
//	// 调用Redis的Del命令删除指定用户的信号列表键
//	return errs.Wrap(c.rdb.Del(ctx, signalListCache+userID).Err())
//}
