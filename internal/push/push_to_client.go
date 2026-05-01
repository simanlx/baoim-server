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

package push

import (
	"BaoIM-Server/pkg/rpccache"
	"context"
	"encoding/json"
	"errors"
	"sync"

	"BaoIM-Server/internal/push/offlinepush"
	"BaoIM-Server/internal/push/offlinepush/dummy"
	"BaoIM-Server/internal/push/offlinepush/fcm"
	"BaoIM-Server/internal/push/offlinepush/getui"
	"BaoIM-Server/internal/push/offlinepush/jpush"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"
	"BaoIM-Server/pkg/common/prommetrics"
	"BaoIM-Server/pkg/msgprocessor"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/constant"
	"baoim/protocol/conversation"
	"baoim/protocol/msggateway"
	"baoim/protocol/sdkws"
	"baoim/tools/discoveryregistry"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Pusher struct {
	config                 *config.GlobalConfig
	database               controller.PushDatabase
	discov                 discoveryregistry.SvcDiscoveryRegistry
	offlinePusher          offlinepush.OfflinePusher
	groupLocalCache        *rpccache.GroupLocalCache
	conversationLocalCache *rpccache.ConversationLocalCache
	msgRpcClient           *rpcclient.MessageRpcClient
	conversationRpcClient  *rpcclient.ConversationRpcClient
	groupRpcClient         *rpcclient.GroupRpcClient
	roomRpcClient          *rpcclient.RoomRpcClient
}

var errNoOfflinePusher = errors.New("no offlinePusher is configured")

func NewPusher(config *config.GlobalConfig, discov discoveryregistry.SvcDiscoveryRegistry, offlinePusher offlinepush.OfflinePusher, database controller.PushDatabase,
	groupLocalCache *rpccache.GroupLocalCache, conversationLocalCache *rpccache.ConversationLocalCache,
	conversationRpcClient *rpcclient.ConversationRpcClient, groupRpcClient *rpcclient.GroupRpcClient, roomRpcClient *rpcclient.RoomRpcClient, msgRpcClient *rpcclient.MessageRpcClient,
) *Pusher {
	return &Pusher{
		config:                 config,
		discov:                 discov,
		database:               database,
		offlinePusher:          offlinePusher,
		groupLocalCache:        groupLocalCache,
		conversationLocalCache: conversationLocalCache,
		msgRpcClient:           msgRpcClient,
		conversationRpcClient:  conversationRpcClient,
		groupRpcClient:         groupRpcClient,
		roomRpcClient:          roomRpcClient,
	}
}

func NewOfflinePusher(config *config.GlobalConfig, cache cache.MsgModel) offlinepush.OfflinePusher {
	var offlinePusher offlinepush.OfflinePusher
	switch config.Push.Enable {
	case "getui":
		offlinePusher = getui.NewClient(config, cache)
	case "fcm":
		offlinePusher = fcm.NewClient(config, cache)
	case "jpush":
		offlinePusher = jpush.NewClient(config)
	default:
		offlinePusher = dummy.NewClient()
	}
	return offlinePusher
}

// //
func (p *Pusher) DeleteMemberAndSetConversationSeq(ctx context.Context, groupID string, userIDs []string) error {
	conevrsationID := msgprocessor.GetConversationIDBySessionType(constant.SuperGroupChatType, groupID)
	maxSeq, err := p.msgRpcClient.GetConversationMaxSeq(ctx, conevrsationID)
	if err != nil {
		return err
	}
	return p.conversationRpcClient.SetConversationMaxSeq(ctx, userIDs, conevrsationID, maxSeq)
}

// //
func (p *Pusher) DeleteRoomMemberAndSetConversationSeq(ctx context.Context, groupID string, userIDs []string) error {
	conevrsationID := msgprocessor.GetConversationIDBySessionType(constant.GroupChatType, groupID)
	maxSeq, err := p.msgRpcClient.GetConversationMaxSeq(ctx, conevrsationID)
	if err != nil {
		return err
	}
	return p.conversationRpcClient.SetConversationMaxSeq(ctx, userIDs, conevrsationID, maxSeq)
}

func (p *Pusher) Push2User(ctx context.Context, userIDs []string, msg *sdkws.MsgData) error {

	log.ZDebug(ctx, "Get msg from msg_transfer And push msg", "userIDs", userIDs, "msg", msg.String())
	if err := callbackOnlinePush(ctx, p.config, userIDs, msg); err != nil {
		return err
	}
	// push
	wsResults, err := p.GetConnsAndOnlinePush(ctx, msg, userIDs)
	if err != nil {
		return err
	}

	isOfflinePush := utils.GetSwitchFromOptions(msg.Options, constant.IsOfflinePush)
	log.ZDebug(ctx, "push_result", "ws push result", wsResults, "sendData", msg, "isOfflinePush", isOfflinePush, "push_to_userID", userIDs)

	if !isOfflinePush {
		return nil
	}

	if len(wsResults) == 0 {
		return nil
	}
	onlinePushSuccUserIDSet := utils.SliceSet(utils.Filter(wsResults, func(e *msggateway.SingleMsgToUserResults) (string, bool) {
		return e.UserID, e.OnlinePush && e.UserID != ""
	}))
	offlinePushUserIDList := utils.Filter(wsResults, func(e *msggateway.SingleMsgToUserResults) (string, bool) {
		_, exist := onlinePushSuccUserIDSet[e.UserID]
		return e.UserID, !exist && e.UserID != "" && e.UserID != msg.SendID
	})

	if len(offlinePushUserIDList) > 0 {
		if err = callbackOfflinePush(ctx, p.config, offlinePushUserIDList, msg, &[]string{}); err != nil {
			return err
		}
		err = p.offlinePushMsg(ctx, msg.SendID, msg, offlinePushUserIDList)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pusher) UnmarshalNotificationElem(bytes []byte, t any) error {
	var notification sdkws.NotificationElem
	if err := json.Unmarshal(bytes, &notification); err != nil {
		return err
	}

	return json.Unmarshal([]byte(notification.Detail), t)
}

/*
k8s deployment,offline push group messages function.
*/
func (p *Pusher) k8sOfflinePush2SuperGroup(ctx context.Context, groupID string, msg *sdkws.MsgData, wsResults []*msggateway.SingleMsgToUserResults) error {

	var needOfflinePushUserIDs []string
	for _, v := range wsResults {
		if !v.OnlinePush {
			needOfflinePushUserIDs = append(needOfflinePushUserIDs, v.UserID)
		}
	}
	if len(needOfflinePushUserIDs) > 0 {
		var offlinePushUserIDs []string
		err := callbackOfflinePush(ctx, p.config, needOfflinePushUserIDs, msg, &offlinePushUserIDs)
		if err != nil {
			return err
		}

		if len(offlinePushUserIDs) > 0 {
			needOfflinePushUserIDs = offlinePushUserIDs
		}
		if msg.ContentType != constant.SignalingNotification {
			resp, err := p.conversationRpcClient.Client.GetConversationOfflinePushUserIDs(
				ctx,
				&conversation.GetConversationOfflinePushUserIDsReq{ConversationID: utils.GenGroupConversationID(groupID), UserIDs: needOfflinePushUserIDs},
			)
			if err != nil {
				return err
			}
			if len(resp.UserIDs) > 0 {
				err = p.offlinePushMsg(ctx, groupID, msg, resp.UserIDs)
				if err != nil {
					log.ZError(ctx, "offlinePushMsg failed", err, "groupID", groupID, "msg", msg)
					return err
				}
			}
		}

	}
	return nil
}

func (p *Pusher) Push2SuperGroup(ctx context.Context, groupID string, msg *sdkws.MsgData) (err error) {
	log.ZDebug(ctx, "Get super group msg from msg_transfer and push msg", "msg", msg.String(), "groupID", groupID)
	var pushToUserIDs []string
	if err = callbackBeforeSuperGroupOnlinePush(ctx, p.config, groupID, msg, &pushToUserIDs); err != nil {
		return err
	}
	if len(pushToUserIDs) == 0 {
		pushToUserIDs, err = p.groupLocalCache.GetGroupMemberIDs(ctx, groupID)
		if err != nil {
			return err
		}

		switch msg.ContentType {
		case constant.MemberQuitNotification:
			var tips sdkws.MemberQuitTips
			if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			defer func(groupID string, userIDs []string) {
				if err = p.DeleteMemberAndSetConversationSeq(ctx, groupID, userIDs); err != nil {
					log.ZError(ctx, "MemberQuitNotification DeleteMemberAndSetConversationSeq", err, "groupID", groupID, "userIDs", userIDs)
				}
			}(groupID, []string{tips.QuitUser.UserID})
			pushToUserIDs = append(pushToUserIDs, tips.QuitUser.UserID)
		case constant.MemberKickedNotification:
			var tips sdkws.MemberKickedTips
			if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			kickedUsers := utils.Slice(tips.KickedUserList, func(e *sdkws.GroupMemberFullInfo) string { return e.UserID })
			defer func(groupID string, userIDs []string) {
				if err = p.DeleteMemberAndSetConversationSeq(ctx, groupID, userIDs); err != nil {
					log.ZError(ctx, "MemberKickedNotification DeleteMemberAndSetConversationSeq", err, "groupID", groupID, "userIDs", userIDs)
				}
			}(groupID, kickedUsers)
			pushToUserIDs = append(pushToUserIDs, kickedUsers...)
		case constant.GroupDismissedNotification:
			// Messages arrive first, notifications arrive later
			if msgprocessor.IsNotification(msgprocessor.GetConversationIDByMsg(msg)) {
				var tips sdkws.GroupDismissedTips
				if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
					return err
				}
				log.ZInfo(ctx, "GroupDismissedNotificationInfo****", "groupID", groupID, "num", len(pushToUserIDs), "list", pushToUserIDs)
				if len(p.config.Manager.UserID) > 0 {
					ctx = mcontext.WithOpUserIDContext(ctx, p.config.Manager.UserID[0])
				}
				if len(p.config.Manager.UserID) == 0 && len(p.config.IMAdmin.UserID) > 0 {
					ctx = mcontext.WithOpUserIDContext(ctx, p.config.IMAdmin.UserID[0])
				}
				defer func(groupID string) {
					if err = p.groupRpcClient.DismissGroup(ctx, groupID); err != nil {
						log.ZError(ctx, "DismissGroup Notification clear members", err, "groupID", groupID)
					}
				}(groupID)
			}
		}
	}

	wsResults, err := p.GetConnsAndOnlinePush(ctx, msg, pushToUserIDs)
	if err != nil {
		return err
	}

	log.ZDebug(ctx, "get conn and online push success", "result", wsResults, "msg", msg)
	isOfflinePush := utils.GetSwitchFromOptions(msg.Options, constant.IsOfflinePush)
	if isOfflinePush && p.config.Envs.Discovery == "k8s" {
		return p.k8sOfflinePush2SuperGroup(ctx, groupID, msg, wsResults)
	}
	if isOfflinePush && p.config.Envs.Discovery == "zookeeper" {
		var (
			onlineSuccessUserIDs      = []string{msg.SendID}
			webAndPcBackgroundUserIDs []string
		)

		for _, v := range wsResults {
			if v.OnlinePush && v.UserID != msg.SendID {
				onlineSuccessUserIDs = append(onlineSuccessUserIDs, v.UserID)
			}

			if v.OnlinePush {
				continue
			}

			if len(v.Resp) == 0 {
				continue
			}

			for _, singleResult := range v.Resp {
				if singleResult.ResultCode != -2 {
					continue
				}

				isPC := constant.PlatformIDToName(int(singleResult.RecvPlatFormID)) == constant.TerminalPC
				isWebID := singleResult.RecvPlatFormID == constant.WebPlatformID

				if isPC || isWebID {
					webAndPcBackgroundUserIDs = append(webAndPcBackgroundUserIDs, v.UserID)
				}
			}
		}

		needOfflinePushUserIDs := utils.DifferenceString(onlineSuccessUserIDs, pushToUserIDs)

		// Use offline push messaging
		if len(needOfflinePushUserIDs) > 0 {
			var offlinePushUserIDs []string
			err = callbackOfflinePush(ctx, p.config, needOfflinePushUserIDs, msg, &offlinePushUserIDs)
			if err != nil {
				return err
			}

			if len(offlinePushUserIDs) > 0 {
				needOfflinePushUserIDs = offlinePushUserIDs
			}
			if msg.ContentType != constant.SignalingNotification {
				resp, err := p.conversationRpcClient.Client.GetConversationOfflinePushUserIDs(
					ctx,
					&conversation.GetConversationOfflinePushUserIDsReq{ConversationID: utils.GenGroupConversationID(groupID), UserIDs: needOfflinePushUserIDs},
				)
				if err != nil {
					return err
				}
				if len(resp.UserIDs) > 0 {
					err = p.offlinePushMsg(ctx, groupID, msg, resp.UserIDs)
					if err != nil {
						log.ZError(ctx, "offlinePushMsg failed", err, "groupID", groupID, "msg", msg)
						return err
					}
					if _, err := p.GetConnsAndOnlinePush(ctx, msg, utils.IntersectString(resp.UserIDs, webAndPcBackgroundUserIDs)); err != nil {
						log.ZError(ctx, "offlinePushMsg failed", err, "groupID", groupID, "msg", msg, "userIDs", utils.IntersectString(needOfflinePushUserIDs, webAndPcBackgroundUserIDs))
						return err
					}
				}
			}

		}
	}
	return nil
}

// /增加 聊天室推送  每增加离线推送哦
func (p *Pusher) Push2RoomGroup(ctx context.Context, groupID string, msg *sdkws.MsgData) (err error) {

	// 记录日志：从msg_transfer获取超级群消息并推送
	log.ZDebug(ctx, "Get super group msg from msg_transfer and push msg", "msg", msg.String(), "groupID", groupID)
	var pushToUserIDs []string

	// 回调处理超级群在线推送前的逻辑，可能会填充pushToUserIDs
	if err = callbackBeforeSuperGroupOnlinePush(ctx, p.config, groupID, msg, &pushToUserIDs); err != nil {
		return err
	}

	// 如果pushToUserIDs为空，则从本地缓存获取群成员列表
	if len(pushToUserIDs) == 0 {
		pushToUserIDs, err = p.groupLocalCache.GetGroupMemberIDs(ctx, groupID)
		if err != nil {
			return err
		}

		// 打印消息类型

		switch msg.ContentType {
		case constant.RoomMemberQuitNotification:
			// 群成员退出通知
			var tips sdkws.MemberQuitTips
			if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			// defer处理：删除成员并设置会话序列     //不用设置了直接删除了
			//defer func(groupID string, userIDs []string) {
			//	if err = p.DeleteRoomMemberAndSetConversationSeq(ctx, groupID, userIDs); err != nil {
			//		log.ZError(ctx, "MemberQuitNotification DeleteROOMMemberAndSetConversationSeq", err, "groupID", groupID, "userIDs", userIDs)
			//	}
			//}(groupID, []string{tips.QuitUser.UserID})

			defer func(groupID string, userID string) {
				//在这里删除 用户的最小seq 和 已读seq  因为在消息推送过程中会设置seq
				p.msgRpcClient.DelUserSeq(ctx, userID, "g_"+groupID)
			}(groupID, tips.QuitUser.UserID)

			println("为什么进来两次")
			// 退出的用户也需要推送
			pushToUserIDs = append(pushToUserIDs, tips.QuitUser.UserID)
		case constant.RoomMemberKickedNotification:
			// 群成员被踢通知
			var tips sdkws.MemberKickedTips
			if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
				return err
			}
			// 获取被踢用户ID列表
			kickedUsers := utils.Slice(tips.KickedUserList, func(e *sdkws.GroupMemberFullInfo) string { return e.UserID })
			// defer处理：删除成员并设置会话序列    //不用设置了直接删除了
			//defer func(groupID string, userIDs []string) {
			//	if err = p.DeleteRoomMemberAndSetConversationSeq(ctx, groupID, userIDs); err != nil {
			//		log.ZError(ctx, "MemberKickedNotification DeleteroomMemberAndSetConversationSeq", err, "groupID", groupID, "userIDs", userIDs)
			//	}
			//}(groupID, kickedUsers)
			// 被踢用户也需要推送

			defer func(groupID string, userIDs []string) {
				for _, userID := range userIDs {
					p.msgRpcClient.DelUserSeq(ctx, userID, "g_"+groupID)
				}
			}(groupID, kickedUsers)

			pushToUserIDs = append(pushToUserIDs, kickedUsers...)
		case constant.RoomGroupDismissedNotification:
			// 群解散通知，消息先到，通知后到
			if msgprocessor.IsNotification(msgprocessor.GetConversationIDByMsg(msg)) {
				var tips sdkws.GroupDismissedTips
				if p.UnmarshalNotificationElem(msg.Content, &tips) != nil {
					return err
				}
				// 记录解散通知相关信息
				log.ZInfo(ctx, "ROOMGroupDismissedNotificationInfo****", "groupID", groupID, "num", len(pushToUserIDs), "list", pushToUserIDs)
				// 设置操作用户ID为管理员
				if len(p.config.Manager.UserID) > 0 {
					ctx = mcontext.WithOpUserIDContext(ctx, p.config.Manager.UserID[0])
				}
				// 如果没有管理员，则设置为IMAdmin
				if len(p.config.Manager.UserID) == 0 && len(p.config.IMAdmin.UserID) > 0 {
					ctx = mcontext.WithOpUserIDContext(ctx, p.config.IMAdmin.UserID[0])
				}
				// defer处理：调用RPC解散群聊，清空成员
				defer func(groupID string) {

					if err = p.roomRpcClient.DismissRoom(ctx, groupID); err != nil {
						log.ZError(ctx, "DismissRoom Notification clear members", err, "groupID", groupID)
					}
				}(groupID)
			}
		}
	}

	// 获取连接并进行在线推送
	wsResults, err := p.GetConnsAndOnlinePush(ctx, msg, pushToUserIDs)
	if err != nil {
		return err
	}

	// 记录在线推送成功日志
	log.ZDebug(ctx, "get conn and online push success", "result", wsResults, "msg", msg)

	// 判断是否需要离线推送（根据消息options开关）
	isOfflinePush := utils.GetSwitchFromOptions(msg.Options, constant.IsOfflinePush)
	if isOfflinePush && p.config.Envs.Discovery == "k8s" {
		// k8s环境下的超级群离线推送
		return p.k8sOfflinePush2SuperGroup(ctx, groupID, msg, wsResults)
	}
	if isOfflinePush && p.config.Envs.Discovery == "zookeeper" {
		// zookeeper环境下处理离线推送
		var (
			onlineSuccessUserIDs      = []string{msg.SendID} // 在线推送成功的用户ID（含发送者）
			webAndPcBackgroundUserIDs []string               // Web和PC后台用户ID
		)

		// 遍历在线推送结果
		for _, v := range wsResults {
			// 非发送者且在线推送成功，加入在线成功列表
			if v.OnlinePush && v.UserID != msg.SendID {
				onlineSuccessUserIDs = append(onlineSuccessUserIDs, v.UserID)
			}

			// 在线推送成功跳过
			if v.OnlinePush {
				continue
			}

			// 没有响应则跳过
			if len(v.Resp) == 0 {
				continue
			}

			// 遍历推送响应结果
			for _, singleResult := range v.Resp {
				if singleResult.ResultCode != -2 {
					continue
				}

				// 判断平台是否为PC或Web
				isPC := constant.PlatformIDToName(int(singleResult.RecvPlatFormID)) == constant.TerminalPC
				isWebID := singleResult.RecvPlatFormID == constant.WebPlatformID

				// PC或Web后台用户，加入webAndPcBackgroundUserIDs
				if isPC || isWebID {
					webAndPcBackgroundUserIDs = append(webAndPcBackgroundUserIDs, v.UserID)
				}
			}
		}

		// 需要离线推送的用户ID
		needOfflinePushUserIDs := utils.DifferenceString(onlineSuccessUserIDs, pushToUserIDs)

		// 开始离线推送流程
		if len(needOfflinePushUserIDs) > 0 {
			var offlinePushUserIDs []string
			// 回调处理离线推送，可能进一步过滤用户
			err = callbackOfflinePush(ctx, p.config, needOfflinePushUserIDs, msg, &offlinePushUserIDs)
			if err != nil {
				return err
			}

			if len(offlinePushUserIDs) > 0 {
				needOfflinePushUserIDs = offlinePushUserIDs
			}
			// 信令通知不做离线推送
			if msg.ContentType != constant.SignalingNotification {
				// 获取会话的离线推送用户ID
				resp, err := p.conversationRpcClient.Client.GetConversationOfflinePushUserIDs(
					ctx,
					&conversation.GetConversationOfflinePushUserIDsReq{ConversationID: utils.GenGroupConversationID(groupID), UserIDs: needOfflinePushUserIDs},
				)
				if err != nil {
					return err
				}
				if len(resp.UserIDs) > 0 {
					// 离线推送消息
					err = p.offlinePushMsg(ctx, groupID, msg, resp.UserIDs)
					if err != nil {
						log.ZError(ctx, "offlinePushMsg failed", err, "groupID", groupID, "msg", msg)
						return err
					}
					// Web/PC后台用户再做一次在线推送
					if _, err := p.GetConnsAndOnlinePush(ctx, msg, utils.IntersectString(resp.UserIDs, webAndPcBackgroundUserIDs)); err != nil {
						log.ZError(ctx, "offlinePushMsg failed", err, "groupID", groupID, "msg", msg, "userIDs", utils.IntersectString(needOfflinePushUserIDs, webAndPcBackgroundUserIDs))
						return err
					}
				}
			}

		}
	}
	// 全部处理完成，返回nil表示成功
	return nil
}

func (p *Pusher) k8sOnlinePush(ctx context.Context, msg *sdkws.MsgData, pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error) {
	var usersHost = make(map[string][]string)
	for _, v := range pushToUserIDs {
		tHost, err := p.discov.GetUserIdHashGatewayHost(ctx, v)
		if err != nil {
			log.ZError(ctx, "get msggateway hash error", err)
			return nil, err
		}
		tUsers, tbl := usersHost[tHost]
		if tbl {
			tUsers = append(tUsers, v)
			usersHost[tHost] = tUsers
		} else {
			usersHost[tHost] = []string{v}
		}
	}
	log.ZDebug(ctx, "genUsers send hosts struct:", "usersHost", usersHost)
	var usersConns = make(map[*grpc.ClientConn][]string)
	for host, userIds := range usersHost {
		tconn, _ := p.discov.GetConn(ctx, host)
		usersConns[tconn] = userIds
	}
	var (
		mu         sync.Mutex
		wg         = errgroup.Group{}
		maxWorkers = p.config.Push.MaxConcurrentWorkers
	)
	if maxWorkers < 3 {
		maxWorkers = 3
	}
	wg.SetLimit(maxWorkers)
	for conn, userIds := range usersConns {
		tcon := conn
		tuserIds := userIds
		wg.Go(func() error {
			input := &msggateway.OnlineBatchPushOneMsgReq{MsgData: msg, PushToUserIDs: tuserIds}
			msgClient := msggateway.NewMsgGatewayClient(tcon)
			reply, err := msgClient.SuperGroupOnlineBatchPushOneMsg(ctx, input)
			if err != nil {
				return nil
			}
			log.ZDebug(ctx, "push result", "reply", reply)
			if reply != nil && reply.SinglePushResult != nil {
				mu.Lock()
				wsResults = append(wsResults, reply.SinglePushResult...)
				mu.Unlock()
			}
			return nil
		})
	}
	_ = wg.Wait()
	return wsResults, nil
}
func (p *Pusher) GetConnsAndOnlinePush(ctx context.Context, msg *sdkws.MsgData, pushToUserIDs []string) (wsResults []*msggateway.SingleMsgToUserResults, err error) {
	// 如果配置为k8s环境，调用k8s的在线推送逻辑
	if p.config.Envs.Discovery == "k8s" {
		return p.k8sOnlinePush(ctx, msg, pushToUserIDs)
	}
	// 获取消息网关连接
	conns, err := p.discov.GetConns(ctx, p.config.RpcRegisterName.OpenImMessageGatewayName)
	// 打印获取到的网关连接数量
	log.ZDebug(ctx, "get gateway conn", "conn length", len(conns))
	// 获取连接失败，则返回错误
	if err != nil {
		return nil, err
	}

	var (
		mu         sync.Mutex                                                                         // 互斥锁用于并发安全地写wsResults
		wg         = errgroup.Group{}                                                                 // 并发错误组，用于等待所有goroutine结束
		input      = &msggateway.OnlineBatchPushOneMsgReq{MsgData: msg, PushToUserIDs: pushToUserIDs} // 构造批量推送请求
		maxWorkers = p.config.Push.MaxConcurrentWorkers                                               // 最大并发数
	)

	// 如果最大并发数小于3，则强制设置为3
	if maxWorkers < 3 {
		maxWorkers = 3
	}

	// 设置并发上限
	wg.SetLimit(maxWorkers)

	// 遍历每个连接，发起在线消息推送
	for _, conn := range conns {
		conn := conn // 保护循环变量
		wg.Go(func() error {
			// 创建消息网关客户端
			msgClient := msggateway.NewMsgGatewayClient(conn)
			// 发送批量推送请求
			reply, err := msgClient.SuperGroupOnlineBatchPushOneMsg(ctx, input)
			// 推送失败直接返回nil，不做错误处理
			if err != nil {
				return nil
			}

			// 打印推送结果
			log.ZDebug(ctx, "push result", "reply", reply)
			// 如果推送结果不为空，且包含单用户结果则写入wsResults
			if reply != nil && reply.SinglePushResult != nil {
				mu.Lock()
				wsResults = append(wsResults, reply.SinglePushResult...)
				mu.Unlock()
			}

			return nil
		})
	}

	// 等待所有goroutine结束
	_ = wg.Wait()

	// 始终返回nil错误和最终结果
	return wsResults, nil
}

func (p *Pusher) offlinePushMsg(ctx context.Context, conversationID string, msg *sdkws.MsgData, offlinePushUserIDs []string) error {
	title, content, opts, err := p.getOfflinePushInfos(conversationID, msg)
	if err != nil {
		return err
	}
	err = p.offlinePusher.Push(ctx, offlinePushUserIDs, title, content, opts)
	if err != nil {
		prommetrics.MsgOfflinePushFailedCounter.Inc()
		return err
	}
	return nil
}

func (p *Pusher) GetOfflinePushOpts(msg *sdkws.MsgData) (opts *offlinepush.Opts, err error) {
	opts = &offlinepush.Opts{Signal: &offlinepush.Signal{}}
	//
	//if msg.ContentType > constant.SignalingNotificationBegin && msg.ContentType < constant.SignalingNotificationEnd {
	//	req := &rtc.SignalReq{}
	//	if err := proto.Unmarshal(msg.Content, req); err != nil {
	//		return nil, utils.Wrap(err, "")
	//	}
	//	switch req.Payload.(type) {
	//	case *rtc.SignalReq_Invite, *rtc.SignalReq_InviteInGroup:
	//		opts.Signal = &offlinepush.Signal{ClientMsgID: msg.ClientMsgID}
	//	}
	//}
	if msg.OfflinePushInfo != nil {
		opts.IOSBadgeCount = msg.OfflinePushInfo.IOSBadgeCount
		opts.IOSPushSound = msg.OfflinePushInfo.IOSPushSound
		opts.Ex = msg.OfflinePushInfo.Ex
	}
	return opts, nil
}

func (p *Pusher) getOfflinePushInfos(conversationID string, msg *sdkws.MsgData) (title, content string, opts *offlinepush.Opts, err error) {
	if p.offlinePusher == nil {
		err = errNoOfflinePusher
		return
	}

	type atContent struct {
		Text       string   `json:"text"`
		AtUserList []string `json:"atUserList"`
		IsAtSelf   bool     `json:"isAtSelf"`
	}

	opts, err = p.GetOfflinePushOpts(msg)
	if err != nil {
		return
	}

	if msg.OfflinePushInfo != nil {
		title = msg.OfflinePushInfo.Title
		content = msg.OfflinePushInfo.Desc
	}
	if title == "" {
		switch msg.ContentType {
		case constant.Text:
			fallthrough
		case constant.Picture:
			fallthrough
		case constant.Voice:
			fallthrough
		case constant.Video:
			fallthrough
		case constant.File:
			title = constant.ContentType2PushContent[int64(msg.ContentType)]
		case constant.AtText:
			ac := atContent{}
			_ = utils.JsonStringToStruct(string(msg.Content), &ac)
			if utils.IsContain(conversationID, ac.AtUserList) {
				title = constant.ContentType2PushContent[constant.AtText] + constant.ContentType2PushContent[constant.Common]
			} else {
				title = constant.ContentType2PushContent[constant.GroupMsg]
			}
		case constant.SignalingNotification:
			title = constant.ContentType2PushContent[constant.SignalMsg]
		default:
			title = constant.ContentType2PushContent[constant.Common]
		}
	}
	if content == "" {
		content = title
	}
	return
}
