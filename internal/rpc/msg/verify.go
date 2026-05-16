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
	"baoim/tools/log" // 日志工具包
	"context"         // 上下文包
	"math/rand"       // 随机数包
	"strconv"         // 字符串与数字转换
	"time"            // 时间包

	"baoim/protocol/constant" // 协议常量
	"baoim/protocol/msg"      // 消息协议
	"baoim/protocol/sdkws"    // SDK WebSocket协议
	"baoim/tools/errs"        // 错误工具包
	"baoim/tools/utils"       // 工具函数包
)

var ExcludeContentType = []int{constant.HasReadReceipt} // 排除的内容类型（已读回执）

// 验证器接口，定义消息验证方法
type Validator interface {
	validate(pb *msg.SendMsgReq) (bool, int32, string)
}

// 消息撤回结构体
type MessageRevoked struct {
	RevokerID                   string `json:"revokerID"`                   // 撤回者ID
	RevokerRole                 int32  `json:"revokerRole"`                 // 撤回者角色
	ClientMsgID                 string `json:"clientMsgID"`                 // 客户端消息ID
	RevokerNickname             string `json:"revokerNickname"`             // 撤回者昵称
	RevokeTime                  int64  `json:"revokeTime"`                  // 撤回时间
	SourceMessageSendTime       int64  `json:"sourceMessageSendTime"`       // 源消息发送时间
	SourceMessageSendID         string `json:"sourceMessageSendID"`         // 源消息发送者ID
	SourceMessageSenderNickname string `json:"sourceMessageSenderNickname"` // 源消息发送者昵称
	SessionType                 int32  `json:"sessionType"`                 // 会话类型
	Seq                         uint32 `json:"seq"`                         // 消息序列号
}

// 消息验证方法
func (m *msgServer) messageVerification(ctx context.Context, data *msg.SendMsgReq) error {
	switch data.MsgData.SessionType { // 根据会话类型分支
	case constant.SingleChatType: // 单聊类型
		if len(m.config.Manager.UserID) > 0 && utils.IsContain(data.MsgData.SendID, m.config.Manager.UserID) {
			return nil // 管理员不做限制
		}

		if data.MsgData.ContentType == constant.Gift {
			return nil // 送花消息不限制
		}

		if utils.IsContain(data.MsgData.SendID, m.config.IMAdmin.UserID) {
			return nil // IM管理员不做限制
		}
		if data.MsgData.ContentType <= constant.NotificationEnd &&
			data.MsgData.ContentType >= constant.NotificationBegin && data.MsgData.ContentType != constant.SignalingNotification { //信令消息不包含在内
			return nil // 通知类型消息不做限制
		}
		black, err := m.FriendLocalCache.IsBlack(ctx, data.MsgData.SendID, data.MsgData.RecvID) // 判断是否黑名单
		if err != nil {
			return err // 查询黑名单失败返回错误
		}
		if black {
			return errs.ErrBlockedByPeer.Wrap() // 对方已拉黑
		}
		if m.config.MessageVerify.FriendVerify != nil && *m.config.MessageVerify.FriendVerify {
			friend, err := m.FriendLocalCache.IsFriend(ctx, data.MsgData.SendID, data.MsgData.RecvID) // 判断是否是好友
			if err != nil {
				return err // 查询失败返回错误
			}
			if !friend {
				return errs.ErrNotPeersFriend.Wrap() // 不是好友关系
			}
			return nil // 是好友返回通过
		}
		return nil // 不需要好友验证直接通过

	case constant.SuperGroupChatType: // 超级群聊类型
		groupInfo, err := m.GroupLocalCache.GetGroupInfo(ctx, data.MsgData.GroupID) // 获取群信息
		if err != nil {
			return err // 查询群信息失败
		}
		if groupInfo.Status == constant.GroupStatusDismissed &&
			data.MsgData.ContentType != constant.GroupDismissedNotification && data.MsgData.ContentType != constant.SignalingNotification {
			return errs.ErrDismissedAlready.Wrap() // 群已解散且不是解散通知
		}

		if groupInfo.GroupType == constant.SuperGroup {
			return nil // 超级群直接通过
		}
		if len(m.config.Manager.UserID) > 0 && utils.IsContain(data.MsgData.SendID, m.config.Manager.UserID) {
			return nil // 管理员直接通过
		}
		if utils.IsContain(data.MsgData.SendID, m.config.IMAdmin.UserID) {
			return nil // IM管理员直接通过
		}
		if data.MsgData.ContentType <= constant.NotificationEnd &&
			data.MsgData.ContentType >= constant.NotificationBegin {
			return nil // 通知类消息直接通过
		}
		memberIDs, err := m.GroupLocalCache.GetGroupMemberIDMap(ctx, data.MsgData.GroupID) // 获取群成员ID映射
		if err != nil {
			return err // 查询失败
		}
		if _, ok := memberIDs[data.MsgData.SendID]; !ok {
			return errs.ErrNotInGroupYet.Wrap() // 发送者不在群中
		}

		groupMemberInfo, err := m.GroupLocalCache.GetGroupMember(ctx, data.MsgData.GroupID, data.MsgData.SendID) // 获取群成员信息
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				return errs.ErrNotInGroupYet.Wrap(err.Error()) // 未入群
			}
			return err // 其他错误
		}
		if groupMemberInfo.RoleLevel == constant.GroupOwner {
			return nil // 群主直接通过
		} else {
			if groupMemberInfo.MuteEndTime >= time.Now().UnixMilli() {
				return errs.ErrMutedInGroup.Wrap() // 在群中禁言
			}
			if groupInfo.Status == constant.GroupStatusMuted && groupMemberInfo.RoleLevel != constant.GroupAdmin {
				return errs.ErrMutedGroup.Wrap() // 群全员禁言且不是管理员
			}
		}
		return nil // 其他情况通过

	case constant.GroupChatType: // 增加聊天室
		groupInfo, err := m.GroupLocalCache.GetGroupInfo(ctx, data.MsgData.GroupID) // 获取群信息
		if err != nil {
			return err // 查询群信息失败
		}
		if groupInfo.Status == constant.GroupStatusDismissed &&
			data.MsgData.ContentType != constant.RoomGroupDismissedNotification && data.MsgData.ContentType != constant.SignalingNotification {
			return errs.ErrDismissedAlready.Wrap() // 群已解散且不是解散通知
		}
		if groupInfo.GroupType == constant.NormalGroup {
			return nil // 超级群直接通过
		}
		if len(m.config.Manager.UserID) > 0 && utils.IsContain(data.MsgData.SendID, m.config.Manager.UserID) {
			return nil // 管理员直接通过
		}
		if utils.IsContain(data.MsgData.SendID, m.config.IMAdmin.UserID) {
			return nil // IM管理员直接通过
		}
		if data.MsgData.ContentType <= constant.NotificationEnd &&
			data.MsgData.ContentType >= constant.NotificationBegin {
			return nil // 通知类消息直接通过
		}
		memberIDs, err := m.GroupLocalCache.GetGroupMemberIDMap(ctx, data.MsgData.GroupID) // 获取群成员ID映射
		if err != nil {
			return err // 查询失败
		}
		if _, ok := memberIDs[data.MsgData.SendID]; !ok {
			return errs.ErrNotInGroupYet.Wrap() // 发送者不在群中
		}

		groupMemberInfo, err := m.GroupLocalCache.GetGroupMember(ctx, data.MsgData.GroupID, data.MsgData.SendID) // 获取群成员信息
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				return errs.ErrNotInGroupYet.Wrap(err.Error()) // 未入群
			}
			return err // 其他错误
		}
		if groupMemberInfo.RoleLevel == constant.GroupOwner {
			return nil // 群主直接通过
		} else {
			if groupMemberInfo.MuteEndTime >= time.Now().UnixMilli() {
				return errs.ErrMutedInGroup.Wrap() // 在群中禁言
			}
			if groupInfo.Status == constant.GroupStatusMuted && groupMemberInfo.RoleLevel != constant.GroupAdmin {
				return errs.ErrMutedGroup.Wrap() // 群全员禁言且不是管理员
			}
		}
		return nil // 其他情况通过
	default:
		return nil // 其他会话类型默认通过
	}
}

// 封装消息数据
func (m *msgServer) encapsulateMsgData(msg *sdkws.MsgData) {
	msg.ServerMsgID = GetMsgID(msg.SendID) // 设置服务端消息ID
	if msg.SendTime == 0 {                 // 如果发送时间为0
		msg.SendTime = utils.GetCurrentTimestampByMill() // 设置当前时间戳
	}
	switch msg.ContentType { // 根据消息类型设置选项
	case constant.Text:
		fallthrough
	case constant.Picture:
		fallthrough
	case constant.Voice:
		fallthrough
	case constant.Video:
		fallthrough
	case constant.File:
		fallthrough
	case constant.AtText:
		fallthrough
	case constant.Merger:
		fallthrough
	case constant.Card:
		fallthrough
	case constant.Location:
		fallthrough
	case constant.Custom:
		fallthrough
	case constant.Gift:
		fallthrough
	case constant.Quote:
	case constant.Revoke:
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false) // 撤回消息不计未读数
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false) // 撤回消息不离线推送
	case constant.HasReadReceipt: // 已读回执消息
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)       // 不更新会话
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false) // 不更新发送者会话
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)              // 不计未读数
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)              // 不离线推送
	case constant.Typing: // 正在输入
		utils.SetSwitchFromOptions(msg.Options, constant.IsHistory, false)                  // 不入历史
		utils.SetSwitchFromOptions(msg.Options, constant.IsPersistent, false)               // 不持久化
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderSync, false)               // 不同步给发送者
		utils.SetSwitchFromOptions(msg.Options, constant.IsConversationUpdate, false)       // 不更新会话
		utils.SetSwitchFromOptions(msg.Options, constant.IsSenderConversationUpdate, false) // 不更新发送者会话
		utils.SetSwitchFromOptions(msg.Options, constant.IsUnreadCount, false)              // 不计未读数
		utils.SetSwitchFromOptions(msg.Options, constant.IsOfflinePush, false)              // 不离线推送
	}
}

// 获取消息ID（服务端消息ID生成方法）
func GetMsgID(sendID string) string {
	t := time.Now().Format("2006-01-02 15:04:05")                       // 获取当前时间字符串
	return utils.Md5(t + "-" + sendID + "-" + strconv.Itoa(rand.Int())) // MD5生成唯一ID
}

// 根据用户消息接收选项修改消息
func (m *msgServer) modifyMessageByUserMessageReceiveOpt(
	ctx context.Context,
	userID, conversationID string,
	sessionType int,
	pb *msg.SendMsgReq,
) (bool, error) {
	defer log.ZDebug(ctx, "modifyMessageByUserMessageReceiveOpt return") // 结束时输出调试日志
	opt, err := m.UserLocalCache.GetUserGlobalMsgRecvOpt(ctx, userID)    // 获取用户全局消息接收选项
	if err != nil {
		return false, err // 查询失败
	}
	switch opt { // 判断全局消息接收选项
	case constant.ReceiveMessage: // 正常接收
	case constant.NotReceiveMessage: // 不接收消息
		return false, nil
	case constant.ReceiveNotNotifyMessage: // 接收但不通知
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10) // 初始化选项
		}
		utils.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false) // 不离线推送
		return true, nil
	}
	// conversationID := utils.GetConversationIDBySessionType(conversationID, sessionType) // 获取会话ID（注释掉）
	singleOpt, err := m.ConversationLocalCache.GetSingleConversationRecvMsgOpt(ctx, userID, conversationID) // 获取单会话消息接收选项
	if errs.ErrRecordNotFound.Is(err) {
		return true, nil // 未设置则正常接收
	} else if err != nil {
		return false, err // 查询失败
	}
	switch singleOpt { // 判断单会话选项
	case constant.ReceiveMessage:
		return true, nil // 正常接收
	case constant.NotReceiveMessage:
		if utils.IsContainInt(int(pb.MsgData.ContentType), ExcludeContentType) {
			return true, nil // 特殊内容类型正常接收
		}
		return false, nil // 其他情况不接收
	case constant.ReceiveNotNotifyMessage:
		if pb.MsgData.Options == nil {
			pb.MsgData.Options = make(map[string]bool, 10) // 初始化选项
		}
		utils.SetSwitchFromOptions(pb.MsgData.Options, constant.IsOfflinePush, false) // 不离线推送
		return true, nil
	}
	return true, nil // 默认正常接收
}
