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

package msg

import (
	"context"

	"BaoIM-Server/pkg/authverify"
	"BaoIM-Server/pkg/msgprocessor"
	"baoim/protocol/constant"
	"baoim/protocol/msg"
	"baoim/protocol/sdkws"
	"baoim/tools/log"
	"baoim/tools/utils"
)

// PullMessageBySeqs 根据消息序列范围拉取消息
// ctx: 上下文，用于传递请求范围、超时控制等
// req: 拉取消息的请求参数，包含用户ID、消息序列范围等信息
// 返回值: 拉取到的消息响应，包含普通消息和通知消息；以及可能的错误
func (m *msgServer) PullMessageBySeqs(
	ctx context.Context,
	req *sdkws.PullMessageBySeqsReq,
) (*sdkws.PullMessageBySeqsResp, error) {

	// 初始化消息响应对象
	resp := &sdkws.PullMessageBySeqsResp{}
	// 初始化普通消息映射，key为会话ID，value为该会话下的拉取消息结果
	resp.Msgs = make(map[string]*sdkws.PullMsgs)
	// 初始化通知消息映射，结构同普通消息映射
	resp.NotificationMsgs = make(map[string]*sdkws.PullMsgs)
	// 遍历请求中的每个消息序列范围
	for _, seq := range req.SeqRanges {
		// 判断当前会话是否为通知会话
		if !msgprocessor.IsNotification(seq.ConversationID) {
			// 非通知会话：从本地缓存获取会话信息
			conversation, err := m.ConversationLocalCache.GetConversation(ctx, req.UserID, seq.ConversationID)
			if err != nil {
				// 获取会话信息失败，记录错误日志并继续处理下一个序列范围
				log.ZError(ctx, "GetConversation error", err, "conversationID", seq.ConversationID)
				continue
			}
			// 从消息数据库中根据序列范围拉取消息
			// 参数包括：用户ID、会话ID、开始序列、结束序列、拉取数量、会话最大序列
			minSeq, maxSeq, msgs, err := m.MsgDatabase.GetMsgBySeqsRange(ctx, req.UserID, seq.ConversationID,
				seq.Begin, seq.End, seq.Num, conversation.MaxSeq)
			if err != nil {
				// 拉取消息失败，记录警告日志并继续处理下一个序列范围
				log.ZWarn(ctx, "GetMsgBySeqsRange error", err, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			// 标记是否已拉取到范围末尾
			var isEnd bool
			switch req.Order {
			case sdkws.PullOrder_PullOrderAsc:
				// 升序拉取时，若最大序列小于等于请求结束序列，则表示已到末尾
				isEnd = maxSeq <= seq.End
			case sdkws.PullOrder_PullOrderDesc:
				// 降序拉取时，若请求开始序列小于等于最小序列，则表示已到末尾
				isEnd = seq.Begin <= minSeq
			}
			// 若拉取到的消息为空，记录警告日志并继续处理下一个序列范围
			if len(msgs) == 0 {
				log.ZWarn(ctx, "not have msgs", nil, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			// 将拉取到的普通消息存入响应中
			resp.Msgs[seq.ConversationID] = &sdkws.PullMsgs{Msgs: msgs, IsEnd: isEnd}
		} else {
			// 通知会话：构建序列列表（从开始序列到结束序列的所有整数）
			var seqs []int64
			for i := seq.Begin; i <= seq.End; i++ {
				seqs = append(seqs, i)
			}
			// 从消息数据库中根据序列列表拉取通知消息
			minSeq, maxSeq, notificationMsgs, err := m.MsgDatabase.GetMsgBySeqs(ctx, req.UserID, seq.ConversationID, seqs)
			if err != nil {
				// 拉取通知消息失败，记录警告日志并继续处理下一个序列范围
				log.ZWarn(ctx, "GetMsgBySeqs error", err, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			// 标记是否已拉取到范围末尾（逻辑同普通消息）
			var isEnd bool
			switch req.Order {
			case sdkws.PullOrder_PullOrderAsc:
				isEnd = maxSeq <= seq.End
			case sdkws.PullOrder_PullOrderDesc:
				isEnd = seq.Begin <= minSeq
			}
			// 若拉取到的通知消息为空，记录警告日志并继续处理下一个序列范围
			if len(notificationMsgs) == 0 {
				log.ZWarn(ctx, "not have notificationMsgs", nil, "conversationID", seq.ConversationID, "seq", seq)
				continue
			}
			// 将拉取到的通知消息存入响应中
			resp.NotificationMsgs[seq.ConversationID] = &sdkws.PullMsgs{Msgs: notificationMsgs, IsEnd: isEnd}
		}
	}
	// 返回拉取结果（即使部分序列范围处理失败，仍返回已成功拉取的消息）
	return resp, nil
}

func (m *msgServer) GetMaxSeq(ctx context.Context, req *sdkws.GetMaxSeqReq) (*sdkws.GetMaxSeqResp, error) {
	if err := authverify.CheckAccessV3(ctx, req.UserID, m.config); err != nil {
		return nil, err
	}
	conversationIDs, err := m.ConversationLocalCache.GetConversationIDs(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	for _, conversationID := range conversationIDs {
		conversationIDs = append(conversationIDs, utils.GetNotificationConversationIDByConversationID(conversationID))
	}
	conversationIDs = append(conversationIDs, utils.GetSelfNotificationConversationID(req.UserID))
	log.ZDebug(ctx, "GetMaxSeq", "conversationIDs", conversationIDs)
	maxSeqs, err := m.MsgDatabase.GetMaxSeqs(ctx, conversationIDs)
	if err != nil {
		log.ZWarn(ctx, "GetMaxSeqs error", err, "conversationIDs", conversationIDs, "maxSeqs", maxSeqs)
		return nil, err
	}
	resp := new(sdkws.GetMaxSeqResp)
	resp.MaxSeqs = maxSeqs
	return resp, nil
}

func (m *msgServer) DelUserSeq(ctx context.Context, req *msg.DelUserSeqReq) (*msg.DelUserSeqResp, error) {
	if err := m.MsgDatabase.DelUserSeq(ctx, req.UserID, req.ConversationID); err != nil {
		return nil, err
	}
	return &msg.DelUserSeqResp{}, nil
}

func (m *msgServer) SearchMessage(ctx context.Context, req *msg.SearchMessageReq) (resp *msg.SearchMessageResp, err error) {
	var chatLogs []*sdkws.MsgData
	var total int32
	resp = &msg.SearchMessageResp{}
	if total, chatLogs, err = m.MsgDatabase.SearchMessage(ctx, req); err != nil {
		return nil, err
	}

	var (
		sendIDs  []string
		recvIDs  []string
		groupIDs []string
		sendMap  = make(map[string]string)
		recvMap  = make(map[string]string)
		groupMap = make(map[string]*sdkws.GroupInfo)
	)
	for _, chatLog := range chatLogs {
		if chatLog.SenderNickname == "" {
			sendIDs = append(sendIDs, chatLog.SendID)
		}
		switch chatLog.SessionType {
		case constant.SingleChatType:
			recvIDs = append(recvIDs, chatLog.RecvID)
		case constant.GroupChatType, constant.SuperGroupChatType:
			groupIDs = append(groupIDs, chatLog.GroupID)
		}
	}
	if len(sendIDs) != 0 {
		sendInfos, err := m.UserLocalCache.GetUsersInfo(ctx, sendIDs)
		if err != nil {
			return nil, err
		}
		for _, sendInfo := range sendInfos {
			sendMap[sendInfo.UserID] = sendInfo.Nickname
		}
	}
	if len(recvIDs) != 0 {
		recvInfos, err := m.UserLocalCache.GetUsersInfo(ctx, recvIDs)
		if err != nil {
			return nil, err
		}
		for _, recvInfo := range recvInfos {
			recvMap[recvInfo.UserID] = recvInfo.Nickname
		}
	}
	if len(groupIDs) != 0 {
		groupInfos, err := m.GroupLocalCache.GetGroupInfos(ctx, groupIDs)
		if err != nil {
			return nil, err
		}
		for _, groupInfo := range groupInfos {
			groupMap[groupInfo.GroupID] = groupInfo
		}
	}
	for _, chatLog := range chatLogs {
		pbchatLog := &msg.ChatLog{}
		utils.CopyStructFields(pbchatLog, chatLog)
		pbchatLog.SendTime = chatLog.SendTime
		pbchatLog.CreateTime = chatLog.CreateTime
		if chatLog.SenderNickname == "" {
			pbchatLog.SenderNickname = sendMap[chatLog.SendID]
		}
		switch chatLog.SessionType {
		case constant.SingleChatType:
			pbchatLog.RecvNickname = recvMap[chatLog.RecvID]

		case constant.GroupChatType, constant.SuperGroupChatType:
			pbchatLog.SenderFaceURL = groupMap[chatLog.GroupID].FaceURL
			pbchatLog.GroupMemberCount = groupMap[chatLog.GroupID].MemberCount
			pbchatLog.RecvID = groupMap[chatLog.GroupID].GroupID
			pbchatLog.GroupName = groupMap[chatLog.GroupID].GroupName
			pbchatLog.GroupOwner = groupMap[chatLog.GroupID].OwnerUserID
			pbchatLog.GroupType = groupMap[chatLog.GroupID].GroupType
		}
		resp.ChatLogs = append(resp.ChatLogs, pbchatLog)
	}
	resp.ChatLogsNum = total
	return resp, nil
}

func (m *msgServer) GetServerTime(ctx context.Context, _ *msg.GetServerTimeReq) (*msg.GetServerTimeResp, error) {
	return &msg.GetServerTimeResp{ServerTime: utils.GetCurrentTimestampByMill()}, nil
}
