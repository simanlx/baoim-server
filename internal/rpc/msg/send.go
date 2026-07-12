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

package msg // 消息模块

import (
	"context" // 上下文包

	"BaoIM-Server/pkg/common/prommetrics"        // 监控指标包
	"BaoIM-Server/pkg/msgprocessor"              // 消息处理器包
	"baoim/protocol/constant"                    // 常量协议包
	pbconversation "baoim/protocol/conversation" // 会话协议包
	pbmsg "baoim/protocol/msg"                   // 消息协议包
	"baoim/protocol/sdkws"                       // SDK WebSocket协议包
	"baoim/protocol/wrapperspb"                  // wrappers协议包
	"baoim/tools/errs"                           // 错误处理工具包
	"baoim/tools/log"                            // 日志工具包
	"baoim/tools/mcontext"                       // 自定义上下文工具包
	"baoim/tools/utils"                          // 工具函数包
)

// 发送消息主入口
func (m *msgServer) SendMsg(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, error error) {

	resp = &pbmsg.SendMsgResp{} // 新建响应结构体
	if req.MsgData != nil {     // 判断消息体是否为空
		flag := isMessageHasReadEnabled(req.MsgData, m.config) // 检查消息是否支持已读回执
		if !flag {                                             // 如果不支持，返回错误
			return nil, errs.ErrMessageHasReadDisable.Wrap()
		}
		m.encapsulateMsgData(req.MsgData) // 封装消息数据
		switch req.MsgData.SessionType {  // 根据会话类型分发
		case constant.SingleChatType: // 单聊
			return m.sendMsgSingleChat(ctx, req)
		case constant.NotificationChatType: // 通知消息
			return m.sendMsgNotification(ctx, req)
		case constant.SuperGroupChatType: // 超级群聊
			return m.sendMsgSuperGroupChat(ctx, req)
		case constant.GroupChatType: // 超级群聊
			return m.sendMsgRoomGroupChat(ctx, req)
		default: // 未知类型
			return nil, errs.ErrArgs.Wrap("unknown sessionType")
		}
	} else { // 消息体为空
		return nil, errs.ErrArgs.Wrap("msgData is nil")
	}
}

func (m *msgServer) MsgVerification(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.MsgVerificationResp, error error) {
	resp = &pbmsg.MsgVerificationResp{} // 新建响应结构体

	if req.MsgData != nil {
		if err := m.messageVerification(ctx, req); err != nil { // 消息验证
			return resp, err
		}
	} else { // 消息体为空
		return resp, errs.ErrArgs.Wrap("msgData is nil")
	}
	return resp, nil
}

// 发送超级群聊消息
func (m *msgServer) sendMsgSuperGroupChat(
	ctx context.Context,
	req *pbmsg.SendMsgReq,
) (resp *pbmsg.SendMsgResp, err error) {
	if err = m.messageVerification(ctx, req); err != nil { // 消息验证
		prommetrics.GroupChatMsgProcessFailedCounter.Inc() // 失败计数器+1
		return nil, err
	}
	if err = callbackBeforeSendGroupMsg(ctx, m.config, req); err != nil { // 发送前回调
		return nil, err
	}

	if err := callbackMsgModify(ctx, m.config, req); err != nil { // 消息内容修改回调
		return nil, err
	}
	err = m.MsgDatabase.MsgToMQ(ctx, utils.GenConversationUniqueKeyForGroup(req.MsgData.GroupID), req.MsgData) // 消息入消息队列
	if err != nil {
		return nil, err
	}
	if req.MsgData.ContentType == constant.AtText { // 如果是@消息
		go m.setConversationAtInfo(ctx, req.MsgData) // 异步设置@信息
	}
	if err = callbackAfterSendGroupMsg(ctx, m.config, req); err != nil { // 发送后回调
		log.ZWarn(ctx, "CallbackAfterSendGroupMsg", err)
	}
	prommetrics.GroupChatMsgProcessSuccessCounter.Inc() // 成功计数器+1
	resp = &pbmsg.SendMsgResp{}                         // 新响应体
	resp.SendTime = req.MsgData.SendTime                // 设置发送时间
	resp.ServerMsgID = req.MsgData.ServerMsgID          // 设置服务端消息ID
	resp.ClientMsgID = req.MsgData.ClientMsgID          // 设置客户端消息ID
	return resp, nil                                    // 返回响应
}

// 发送超级群聊消息
func (m *msgServer) sendMsgRoomGroupChat(
	ctx context.Context,
	req *pbmsg.SendMsgReq,
) (resp *pbmsg.SendMsgResp, err error) {
	//{"detail":"{\"group\":{\"groupID\":\"3464210001\",\"groupName\":\"港风的美颜是我\",\"notification\":\"\",\"introduction\":\"\",\"faceURL\":\"\",\"ownerUserID\":\"8025161842\",\"createTime\":1756897267148,\"memberCount\":3,\"ex\":\"\",\"status\":0,\"creatorUserID\":\"8025161842\",\"groupType\":2,\"needVerification\":0,\"lookMemberInfo\":0,\"applyMemberFriend\":0,\"notificationUpdateTime\":0,\"notificationUserID\":\"\"},\"opUser\":{\"groupID\":\"3464210001\",\"userID\":\"8025161842\",\"roleLevel\":100,\"joinTime\":1756897267156,\"nickname\":\"第一个\",\"faceURL\":\"    \",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"8025161842\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"8025161842\"},\"memberList\":[{\"groupID\":\"3464210001\",\"userID\":\"8025161842\",\"roleLevel\":100,\"joinTime\":1756897267156,\"nickname\":\"第一个\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"8025161842\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"8025161842\"}],\"operationTime\":1756897267148,\"groupOwnerUser\":{\"groupID\":\"3464210001\",\"userID\":\"8025161842\",\"roleLevel\":100,\"joinTime\":1756897267156,\"nickname\":\"第一个\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"8025161842\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"8025161842\"}}"}
	//{"detail":"{\"group\":{\"groupID\":\"3724352123\",\"groupName\":\"普通群5000存0\",\"notification\":\"\",\"introduction\":\"\",\"faceURL\":\"\"ownerUserID\":\"1200826769\",\"createTime\":1756898119764,\"memberCount\":2,\"ex\":\"\",\"status\":0,\"creatorUserID\":\"1200826769\",\"groupType\":0,\"needVerification\":0,\"lookMemberInfo\":0,\"applyMemberFriend\":0,\"notificationUpdateTime\":0,\"notificationUserID\":\"\"},\"opUser\":{\"groupID\":\"3724352123\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898119771,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"},\"memberList\":[{\"groupID\":\"3724352123\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898119771,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"}],\"operationTime\":1756898119764,\"groupOwnerUser\":{\"groupID\":\"3724352123\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898119771,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"}}"}
	//{"detail":"{\"group\":{\"groupID\":\"3856312365\",\"groupName\":\"普通群50000  \",\"notification\":\"\",\"introduction\":\"\",\"faceURL\":\"\",\"ownserID\":\"1200826769\",\"createTime\":1756898720344,\"memberCount\":2,\"ex\":\"\",\"status\":0,\"creatorUserID\":\"1200826769\",\"groupType\":0,\"needVerification\":0,\"lookMemberInfo\":0,\"applyMemberFriend\":0,\"notificationUpdateTime\":0,\"notificationUserID\":\"\"},\"opUser\":{\"groupID\":\"3856312365\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898720363,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"},\"memberList\":[{\"groupID\":\"3856312365\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898720363,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"}],\"operationTime\":1756898720344,\"groupOwnerUser\":{\"groupID\":\"3856312365\",\"userID\":\"1200826769\",\"roleLevel\":100,\"joinTime\":1756898720363,\"nickname\":\"222222222\",\"faceURL\":\"\",\"appMangerLevel\":0,\"joinSource\":2,\"operatorUserID\":\"1200826769\",\"ex\":\"\",\"muteEndTime\":0,\"inviterUserID\":\"1200826769\"}}"}

	if err = m.messageVerification(ctx, req); err != nil { // 消息验证
		prommetrics.GroupChatMsgProcessFailedCounter.Inc() // 失败计数器+1
		return nil, err
	}
	if err = callbackBeforeSendGroupMsg(ctx, m.config, req); err != nil { // 发送前回调
		return nil, err
	}

	if err := callbackMsgModify(ctx, m.config, req); err != nil { // 消息内容修改回调
		return nil, err
	}
	err = m.MsgDatabase.MsgToMQ(ctx, utils.GenConversationUniqueKeyForGroup(req.MsgData.GroupID), req.MsgData) // 消息入消息队列
	if err != nil {
		return nil, err
	}
	if req.MsgData.ContentType == constant.AtText { // 如果是@消息
		go m.setConversationAtInfo(ctx, req.MsgData) // 异步设置@信息
	}
	if err = callbackAfterSendGroupMsg(ctx, m.config, req); err != nil { // 发送后回调
		log.ZWarn(ctx, "CallbackAfterSendGroupMsg", err)
	}
	prommetrics.GroupChatMsgProcessSuccessCounter.Inc() // 成功计数器+1
	resp = &pbmsg.SendMsgResp{}                         // 新响应体
	resp.SendTime = req.MsgData.SendTime                // 设置发送时间
	resp.ServerMsgID = req.MsgData.ServerMsgID          // 设置服务端消息ID
	resp.ClientMsgID = req.MsgData.ClientMsgID          // 设置客户端消息ID
	return resp, nil                                    // 返回响应
}

// 设置会话@信息
func (m *msgServer) setConversationAtInfo(nctx context.Context, msg *sdkws.MsgData) {
	log.ZDebug(nctx, "setConversationAtInfo", "msg", msg)         // 打印调试日志
	ctx := mcontext.NewCtx("@@@" + mcontext.GetOperationID(nctx)) // 构造新上下文
	var atUserID []string                                         // 被@的用户ID列表
	conversation := &pbconversation.ConversationReq{              // 构造会话请求
		ConversationID:   msgprocessor.GetConversationIDByMsg(msg), // 获取会话ID
		ConversationType: msg.SessionType,                          // 设置会话类型
		GroupID:          msg.GroupID,                              // 设置群ID
	}
	tagAll := utils.IsContain(constant.AtAllString, msg.AtUserIDList) // 是否@所有人
	if tagAll {                                                       // 如果@所有人
		memberUserIDList, err := m.GroupLocalCache.GetGroupMemberIDs(ctx, msg.GroupID) // 获取群成员ID列表
		if err != nil {
			log.ZWarn(ctx, "GetGroupMemberIDs", err)
			return
		}
		atUserID = utils.DifferenceString([]string{constant.AtAllString}, msg.AtUserIDList) // 计算除了@all以外的@成员
		if len(atUserID) == 0 {                                                             // 仅@所有人
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll} // 设置@类型为全部
		} else { // @所有人及其他指定成员
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAllAtMe}
			err = m.Conversation.SetConversations(ctx, atUserID, conversation) // 设置@信息
			if err != nil {
				log.ZWarn(ctx, "SetConversations", err, "userID", atUserID, "conversation", conversation)
			}
			memberUserIDList = utils.DifferenceString(atUserID, memberUserIDList) // 剩下的群成员
		}
		conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtAll}   // 设置@所有人类型
		err = m.Conversation.SetConversations(ctx, memberUserIDList, conversation) // 给所有成员设置@信息
		if err != nil {
			log.ZWarn(ctx, "SetConversations", err, "userID", memberUserIDList, "conversation", conversation)
		}
	} else { // 仅@部分成员
		conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.AtMe}     // 设置@类型为@我
		err := m.Conversation.SetConversations(ctx, msg.AtUserIDList, conversation) // 设置@信息
		if err != nil {
			log.ZWarn(ctx, "SetConversations", err, msg.AtUserIDList, conversation)
		}
	}
}

// 发送通知消息
func (m *msgServer) sendMsgNotification(
	ctx context.Context,
	req *pbmsg.SendMsgReq,
) (resp *pbmsg.SendMsgResp, err error) {
	if err := m.MsgDatabase.MsgToMQ(ctx, utils.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil { // 消息入消息队列
		return nil, err
	}
	resp = &pbmsg.SendMsgResp{ // 构造响应
		ServerMsgID: req.MsgData.ServerMsgID, // 服务端消息ID
		ClientMsgID: req.MsgData.ClientMsgID, // 客户端消息ID
		SendTime:    req.MsgData.SendTime,    // 发送时间
	}
	return resp, nil // 返回响应
}

// 发送单聊消息
func (m *msgServer) sendMsgSingleChat(ctx context.Context, req *pbmsg.SendMsgReq) (resp *pbmsg.SendMsgResp, err error) {
	log.ZDebug(ctx, "sendMsgSingleChat return")             // 打印调试信息
	if err := m.messageVerification(ctx, req); err != nil { // 消息验证
		return nil, err
	}
	isSend := true                                                  // 是否发送
	isNotification := msgprocessor.IsNotificationByMsg(req.MsgData) // 是否为通知消息
	if !isNotification {                                            // 如果不是通知消息
		isSend, err = m.modifyMessageByUserMessageReceiveOpt(
			ctx,
			req.MsgData.RecvID,
			utils.GenConversationIDForSingle(req.MsgData.SendID, req.MsgData.RecvID),
			constant.SingleChatType,
			req,
		)
		if err != nil {
			return nil, err
		}
	}
	if !isSend { // 不发送
		prommetrics.SingleChatMsgProcessFailedCounter.Inc() // 单聊失败计数器+1
		return nil, nil
	} else { // 发送消息
		if err = callbackBeforeSendSingleMsg(ctx, m.config, req); err != nil { // 发送前回调
			return nil, err
		}

		if err := callbackMsgModify(ctx, m.config, req); err != nil { // 消息内容修改回调
			return nil, err
		}
		if err := m.MsgDatabase.MsgToMQ(ctx, utils.GenConversationUniqueKeyForSingle(req.MsgData.SendID, req.MsgData.RecvID), req.MsgData); err != nil { // 消息入队列
			prommetrics.SingleChatMsgProcessFailedCounter.Inc() // 失败计数器+1
			return nil, err
		}
		err = callbackAfterSendSingleMsg(ctx, m.config, req) // 发送后回调
		if err != nil {
			log.ZWarn(ctx, "CallbackAfterSendSingleMsg", err, "req", req)
		}
		resp = &pbmsg.SendMsgResp{ // 构造响应
			ServerMsgID: req.MsgData.ServerMsgID, // 服务端消息ID
			ClientMsgID: req.MsgData.ClientMsgID, // 客户端消息ID
			SendTime:    req.MsgData.SendTime,    // 发送时间
		}
		prommetrics.SingleChatMsgProcessSuccessCounter.Inc() // 单聊成功计数器+1
		return resp, nil                                     // 返回响应
	}
}

// 批量发送消息（未实现）
func (m *msgServer) BatchSendMsg(ctx context.Context, in *pbmsg.BatchSendMessageReq) (*pbmsg.BatchSendMessageResp, error) {
	return nil, nil // 直接返回空
}
