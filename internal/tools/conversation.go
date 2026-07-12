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

package tools

import (
	"context"
	"math/rand"
	"time"

	"BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/sdkws"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
)

//	func (c *MsgTool) ConversationsDestructMsgs() {
//		log.ZInfo(context.Background(), "start msg destruct cron task")
//		ctx := mcontext.NewCtx(utils.GetSelfFuncName())
//		conversations, err := c.conversationDatabase.GetConversationIDsNeedDestruct(ctx)
//		if err != nil {
//			log.ZError(ctx, "get conversation id need destruct failed", err)
//			return
//		}
//		log.ZDebug(context.Background(), "nums conversations need destruct", "nums", len(conversations))
//		for _, conversation := range conversations {
//			ctx = mcontext.NewCtx(utils.GetSelfFuncName() + "-" + utils.OperationIDGenerator() + "-" + conversation.ConversationID + "-" + conversation.OwnerUserID)
//			log.ZDebug(
//				ctx,
//				"UserMsgsDestruct",
//				"conversationID",
//				conversation.ConversationID,
//				"ownerUserID",
//				conversation.OwnerUserID,
//				"msgDestructTime",
//				conversation.MsgDestructTime,
//				"lastMsgDestructTime",
//				conversation.LatestMsgDestructTime,
//			)
//			now := time.Now()
//			seqs, err := c.msgDatabase.UserMsgsDestruct(ctx, conversation.OwnerUserID, conversation.ConversationID, conversation.MsgDestructTime, conversation.LatestMsgDestructTime)
//			if err != nil {
//				log.ZError(ctx, "user msg destruct failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID)
//				continue
//			}
//			if len(seqs) > 0 {
//				if err := c.conversationDatabase.UpdateUsersConversationField(ctx, []string{conversation.OwnerUserID}, conversation.ConversationID, map[string]interface{}{"latest_msg_destruct_time": now}); err
//
//	!= nil {
//					log.ZError(ctx, "updateUsersConversationField failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID)
//					continue
//				}
//				if err := c.msgNotificationSender.UserDeleteMsgsNotification(ctx, conversation.OwnerUserID, conversation.ConversationID, seqs); err != nil {
//					log.ZError(ctx, "userDeleteMsgsNotification failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID)
//				}
//			}
//		}
//	}
func (c *MsgTool) ConversationsDestructMsgs() {

	log.ZInfo(context.Background(), "start msg destruct cron task")     // 日志：开始消息自毁定时任务
	ctx := mcontext.NewCtx(utils.GetSelfFuncName())                     // 新建上下文，带当前函数名
	num, err := c.conversationDatabase.GetAllConversationIDsNumber(ctx) // 获取所有会话ID的数量
	if err != nil {
		log.ZError(ctx, "GetAllConversationIDsNumber failed", err) // 日志：获取数量失败
		return                                                     // 失败直接返回
	}
	const batchNum = 50                                        // 每批处理的会话数量
	log.ZDebug(ctx, "GetAllConversationIDsNumber", "num", num) // 日志：输出获取到的会话总数
	if num == 0 {
		return // 没有会话直接返回
	}
	count := int(num/batchNum + num/batchNum/2) // 计算本次循环处理的次数（稍微多处理一些）
	if count < 1 {
		count = 1 // 至少处理一次
	}
	maxPage := 1 + num/batchNum // 最大页码（分多少批处理）
	if num%batchNum != 0 {
		maxPage++ // 如果最后一页不是满的，页码再加1
	}
	for i := 0; i < count; i++ { // 循环处理，每次随机选一页
		pageNumber := rand.Int63() % maxPage // 随机选择页码
		pagination := &sdkws.RequestPagination{
			PageNumber: int32(pageNumber), // 页码
			ShowNumber: batchNum,          // 每页数量
		}
		conversationIDs, err := c.conversationDatabase.PageConversationIDs(ctx, pagination) // 分页获取会话ID
		if err != nil {
			log.ZError(ctx, "PageConversationIDs failed", err, "pageNumber", pageNumber) // 获取失败日志
			continue                                                                     // 失败跳过
		}
		log.ZError(ctx, "PageConversationIDs failed", err, "pageNumber", pageNumber, "conversationIDsNum", len(conversationIDs), "conversationIDs", conversationIDs) // 输出本次分页结果日志
		if len(conversationIDs) == 0 {
			continue // 没有会话ID跳过
		}
		conversations, err := c.conversationDatabase.GetConversationsByConversationID(ctx, conversationIDs) // 根据会话ID获取会话详情
		if err != nil {
			log.ZError(ctx, "GetConversationsByConversationID failed", err, "conversationIDs", conversationIDs) // 获取失败日志
			continue                                                                                            // 失败跳过
		}
		temp := make([]*relation.ConversationModel, 0, len(conversations)) // 临时数组，存放需要自毁消息的会话
		for i, conversation := range conversations {                       // 遍历会话
			// 判断是否开启消息自毁、时间条件是否满足
			if conversation.IsMsgDestruct && conversation.MsgDestructTime != 0 &&
				(time.Now().Unix() > (conversation.MsgDestructTime+conversation.LatestMsgDestructTime.Unix()+8*60*60)) ||
				conversation.LatestMsgDestructTime.IsZero() {
				temp = append(temp, conversations[i]) // 满足条件加入临时数组
			}
		}
		for _, conversation := range temp { // 对每个需要自毁的会话处理
			ctx = mcontext.NewCtx(utils.GetSelfFuncName() + "-" + utils.OperationIDGenerator() + "-" + conversation.ConversationID + "-" + conversation.OwnerUserID) // 新建上下文，带操作ID和会话、用户信息
			log.ZDebug(
				ctx,
				"UserMsgsDestruct",
				"conversationID",
				conversation.ConversationID,
				"ownerUserID",
				conversation.OwnerUserID,
				"msgDestructTime",
				conversation.MsgDestructTime,
				"lastMsgDestructTime",
				conversation.LatestMsgDestructTime,
			) // 日志：即将自毁消息的会话详细信息
			now := time.Now()                                                                                                                                                         // 当前时间
			seqs, err := c.msgDatabase.UserMsgsDestruct(ctx, conversation.OwnerUserID, conversation.ConversationID, conversation.MsgDestructTime, conversation.LatestMsgDestructTime) // 执行消息自毁，返回被删除消息的序号
			if err != nil {
				log.ZError(ctx, "user msg destruct failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID) // 自毁失败日志
				continue                                                                                                                                 // 跳过
			}
			if len(seqs) > 0 { // 有消息被删除
				// 更新会话的最新自毁时间
				if err := c.conversationDatabase.UpdateUsersConversationField(ctx, []string{conversation.OwnerUserID}, conversation.ConversationID, map[string]any{"latest_msg_destruct_time": now}); err != nil {
					log.ZError(ctx, "updateUsersConversationField failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID) // 更新失败日志
					continue                                                                                                                                            // 跳过
				}
				// 发送消息删除的通知
				if err := c.msgNotificationSender.UserDeleteMsgsNotification(ctx, conversation.OwnerUserID, conversation.ConversationID, seqs); err != nil {
					log.ZError(ctx, "userDeleteMsgsNotification failed", err, "conversationID", conversation.ConversationID, "ownerUserID", conversation.OwnerUserID) // 通知发送失败日志
				}
			}
		}
	}
}
