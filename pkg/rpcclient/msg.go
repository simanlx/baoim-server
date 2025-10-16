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

package rpcclient

import (
	"context"
	"encoding/json"
	"fmt"

	"BaoIM-Server/pkg/common/config"
	util "BaoIM-Server/pkg/util/genutil"
	"baoim/protocol/constant"
	"baoim/protocol/msg"
	"baoim/protocol/sdkws"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/utils"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func newContentTypeConf(conf *config.GlobalConfig) map[int32]config.NotificationConf {
	return map[int32]config.NotificationConf{
		// group
		constant.GroupCreatedNotification:                 conf.Notification.GroupCreated,
		constant.GroupInfoSetNotification:                 conf.Notification.GroupInfoSet,
		constant.JoinGroupApplicationNotification:         conf.Notification.JoinGroupApplication,
		constant.MemberQuitNotification:                   conf.Notification.MemberQuit,
		constant.GroupApplicationAcceptedNotification:     conf.Notification.GroupApplicationAccepted,
		constant.GroupApplicationRejectedNotification:     conf.Notification.GroupApplicationRejected,
		constant.GroupOwnerTransferredNotification:        conf.Notification.GroupOwnerTransferred,
		constant.MemberKickedNotification:                 conf.Notification.MemberKicked,
		constant.MemberInvitedNotification:                conf.Notification.MemberInvited,
		constant.MemberEnterNotification:                  conf.Notification.MemberEnter,
		constant.GroupDismissedNotification:               conf.Notification.GroupDismissed,
		constant.GroupMutedNotification:                   conf.Notification.GroupMuted,
		constant.GroupCancelMutedNotification:             conf.Notification.GroupCancelMuted,
		constant.GroupMemberMutedNotification:             conf.Notification.GroupMemberMuted,
		constant.GroupMemberCancelMutedNotification:       conf.Notification.GroupMemberCancelMuted,
		constant.GroupMemberInfoSetNotification:           conf.Notification.GroupMemberInfoSet,
		constant.GroupMemberSetToAdminNotification:        conf.Notification.GroupMemberSetToAdmin,
		constant.GroupMemberSetToOrdinaryUserNotification: conf.Notification.GroupMemberSetToOrdinary,
		constant.GroupInfoSetAnnouncementNotification:     conf.Notification.GroupInfoSetAnnouncement,
		constant.GroupInfoSetNameNotification:             conf.Notification.GroupInfoSetName,

		///聊天室
		constant.RoomGroupCreatedNotification:                 conf.Notification.GroupCreated,
		constant.RoomGroupInfoSetNotification:                 conf.Notification.GroupInfoSet,
		constant.RoomJoinGroupApplicationNotification:         conf.Notification.JoinGroupApplication,
		constant.RoomMemberQuitNotification:                   conf.Notification.MemberQuit,
		constant.RoomGroupApplicationAcceptedNotification:     conf.Notification.GroupApplicationAccepted,
		constant.RoomGroupApplicationRejectedNotification:     conf.Notification.GroupApplicationRejected,
		constant.RoomGroupOwnerTransferredNotification:        conf.Notification.GroupOwnerTransferred,
		constant.RoomMemberKickedNotification:                 conf.Notification.MemberKicked,
		constant.RoomMemberInvitedNotification:                conf.Notification.MemberInvited,
		constant.RoomMemberEnterNotification:                  conf.Notification.MemberEnter,
		constant.RoomGroupDismissedNotification:               conf.Notification.GroupDismissed,
		constant.RoomGroupMutedNotification:                   conf.Notification.GroupMuted,
		constant.RoomGroupCancelMutedNotification:             conf.Notification.GroupCancelMuted,
		constant.RoomGroupMemberMutedNotification:             conf.Notification.GroupMemberMuted,
		constant.RoomGroupMemberCancelMutedNotification:       conf.Notification.GroupMemberCancelMuted,
		constant.RoomGroupMemberInfoSetNotification:           conf.Notification.GroupMemberInfoSet,
		constant.RoomGroupMemberSetToAdminNotification:        conf.Notification.GroupMemberSetToAdmin,
		constant.RoomGroupMemberSetToOrdinaryUserNotification: conf.Notification.GroupMemberSetToOrdinary,
		constant.RoomGroupInfoSetAnnouncementNotification:     conf.Notification.GroupInfoSetAnnouncement,
		constant.RoomGroupInfoSetNameNotification:             conf.Notification.GroupInfoSetName,

		// user
		constant.UserInfoUpdatedNotification:  conf.Notification.UserInfoUpdated,
		constant.UserStatusChangeNotification: conf.Notification.UserStatusChanged,
		// friend
		constant.FriendApplicationNotification:         conf.Notification.FriendApplicationAdded,
		constant.FriendApplicationApprovedNotification: conf.Notification.FriendApplicationApproved,
		constant.FriendApplicationRejectedNotification: conf.Notification.FriendApplicationRejected,
		constant.FriendAddedNotification:               conf.Notification.FriendAdded,
		constant.FriendDeletedNotification:             conf.Notification.FriendDeleted,
		constant.FriendRemarkSetNotification:           conf.Notification.FriendRemarkSet,
		constant.BlackAddedNotification:                conf.Notification.BlackAdded,
		constant.BlackDeletedNotification:              conf.Notification.BlackDeleted,
		constant.FriendInfoUpdatedNotification:         conf.Notification.FriendInfoUpdated,
		constant.FriendsInfoUpdateNotification:         conf.Notification.FriendInfoUpdated, //use the same FriendInfoUpdated
		// conversation
		constant.ConversationChangeNotification:      conf.Notification.ConversationChanged,
		constant.ConversationUnreadNotification:      conf.Notification.ConversationChanged,
		constant.ConversationPrivateChatNotification: conf.Notification.ConversationSetPrivate,
		// msg
		constant.MsgRevokeNotification:  {IsSendMsg: false, ReliabilityLevel: constant.ReliableNotificationNoMsg},
		constant.HasReadReceipt:         {IsSendMsg: false, ReliabilityLevel: constant.ReliableNotificationNoMsg},
		constant.DeleteMsgsNotification: {IsSendMsg: false, ReliabilityLevel: constant.ReliableNotificationNoMsg},
	}
}

func newSessionTypeConf() map[int32]int32 {
	return map[int32]int32{
		// group
		constant.GroupCreatedNotification:                 constant.SuperGroupChatType,
		constant.GroupInfoSetNotification:                 constant.SuperGroupChatType,
		constant.JoinGroupApplicationNotification:         constant.SingleChatType,
		constant.MemberQuitNotification:                   constant.SuperGroupChatType,
		constant.GroupApplicationAcceptedNotification:     constant.SingleChatType,
		constant.GroupApplicationRejectedNotification:     constant.SingleChatType,
		constant.GroupOwnerTransferredNotification:        constant.SuperGroupChatType,
		constant.MemberKickedNotification:                 constant.SuperGroupChatType,
		constant.MemberInvitedNotification:                constant.SuperGroupChatType,
		constant.MemberEnterNotification:                  constant.SuperGroupChatType,
		constant.GroupDismissedNotification:               constant.SuperGroupChatType,
		constant.GroupMutedNotification:                   constant.SuperGroupChatType,
		constant.GroupCancelMutedNotification:             constant.SuperGroupChatType,
		constant.GroupMemberMutedNotification:             constant.SuperGroupChatType,
		constant.GroupMemberCancelMutedNotification:       constant.SuperGroupChatType,
		constant.GroupMemberInfoSetNotification:           constant.SuperGroupChatType,
		constant.GroupMemberSetToAdminNotification:        constant.SuperGroupChatType,
		constant.GroupMemberSetToOrdinaryUserNotification: constant.SuperGroupChatType,
		constant.GroupInfoSetAnnouncementNotification:     constant.SuperGroupChatType,
		constant.GroupInfoSetNameNotification:             constant.SuperGroupChatType,
		//聊天室
		constant.RoomGroupCreatedNotification:         constant.GroupChatType,
		constant.RoomGroupInfoSetNotification:         constant.GroupChatType,
		constant.RoomJoinGroupApplicationNotification: constant.SingleChatType,
		constant.RoomMemberQuitNotification:           constant.GroupChatType,

		constant.RoomGroupApplicationAcceptedNotification:     constant.SingleChatType,
		constant.RoomGroupApplicationRejectedNotification:     constant.SingleChatType,
		constant.RoomGroupOwnerTransferredNotification:        constant.GroupChatType,
		constant.RoomMemberKickedNotification:                 constant.GroupChatType,
		constant.RoomMemberInvitedNotification:                constant.GroupChatType,
		constant.RoomMemberEnterNotification:                  constant.GroupChatType,
		constant.RoomGroupDismissedNotification:               constant.GroupChatType,
		constant.RoomGroupMutedNotification:                   constant.GroupChatType,
		constant.RoomGroupCancelMutedNotification:             constant.GroupChatType,
		constant.RoomGroupMemberMutedNotification:             constant.GroupChatType,
		constant.RoomGroupMemberCancelMutedNotification:       constant.GroupChatType,
		constant.RoomGroupMemberInfoSetNotification:           constant.GroupChatType,
		constant.RoomGroupMemberSetToAdminNotification:        constant.GroupChatType,
		constant.RoomGroupMemberSetToOrdinaryUserNotification: constant.GroupChatType,
		constant.RoomGroupInfoSetAnnouncementNotification:     constant.GroupChatType,
		constant.RoomGroupInfoSetNameNotification:             constant.GroupChatType,

		// user
		constant.UserInfoUpdatedNotification:  constant.SingleChatType,
		constant.UserStatusChangeNotification: constant.SingleChatType,
		// friend
		constant.FriendApplicationNotification:         constant.SingleChatType,
		constant.FriendApplicationApprovedNotification: constant.SingleChatType,
		constant.FriendApplicationRejectedNotification: constant.SingleChatType,
		constant.FriendAddedNotification:               constant.SingleChatType,
		constant.FriendDeletedNotification:             constant.SingleChatType,
		constant.FriendRemarkSetNotification:           constant.SingleChatType,
		constant.BlackAddedNotification:                constant.SingleChatType,
		constant.BlackDeletedNotification:              constant.SingleChatType,
		constant.FriendInfoUpdatedNotification:         constant.SingleChatType,
		constant.FriendsInfoUpdateNotification:         constant.SingleChatType,
		// conversation
		constant.ConversationChangeNotification:      constant.SingleChatType,
		constant.ConversationUnreadNotification:      constant.SingleChatType,
		constant.ConversationPrivateChatNotification: constant.SingleChatType,
		// delete
		constant.DeleteMsgsNotification: constant.SingleChatType,
	}
}

type Message struct {
	conn   grpc.ClientConnInterface
	Client msg.MsgClient
	discov discoveryregistry.SvcDiscoveryRegistry
	Config *config.GlobalConfig
}

func NewMessage(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) *Message {
	conn, err := discov.GetConn(context.Background(), config.RpcRegisterName.OpenImMsgName)
	if err != nil {
		util.ExitWithError(err)
	}
	client := msg.NewMsgClient(conn)
	return &Message{discov: discov, conn: conn, Client: client, Config: config}
}

type MessageRpcClient Message

func NewMessageRpcClient(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) MessageRpcClient {
	return MessageRpcClient(*NewMessage(discov, config))
}

// SendMsg sends a message through the gRPC client and returns the response.
// It wraps any encountered error for better error handling and context understanding.
func (m *MessageRpcClient) SendMsg(ctx context.Context, req *msg.SendMsgReq) (*msg.SendMsgResp, error) {
	resp, err := m.Client.SendMsg(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *MessageRpcClient) MsgVerification(ctx context.Context, req *msg.SendMsgReq) (*msg.MsgVerificationResp, error) {
	resp, err := m.Client.MsgVerification(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// GetMaxSeq retrieves the maximum sequence number from the gRPC client.
// Errors during the gRPC call are wrapped to provide additional context.
func (m *MessageRpcClient) GetMaxSeq(ctx context.Context, req *sdkws.GetMaxSeqReq) (*sdkws.GetMaxSeqResp, error) {
	resp, err := m.Client.GetMaxSeq(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *MessageRpcClient) GetMaxSeqs(ctx context.Context, conversationIDs []string) (map[string]int64, error) {
	log.ZDebug(ctx, "GetMaxSeqs", "conversationIDs", conversationIDs)
	resp, err := m.Client.GetMaxSeqs(ctx, &msg.GetMaxSeqsReq{
		ConversationIDs: conversationIDs,
	})
	return resp.MaxSeqs, err
}

func (m *MessageRpcClient) GetHasReadSeqs(ctx context.Context, userID string, conversationIDs []string) (map[string]int64, error) {
	resp, err := m.Client.GetHasReadSeqs(ctx, &msg.GetHasReadSeqsReq{
		UserID:          userID,
		ConversationIDs: conversationIDs,
	})
	return resp.MaxSeqs, err
}

func (m *MessageRpcClient) GetMsgByConversationIDs(ctx context.Context, docIDs []string, seqs map[string]int64) (map[string]*sdkws.MsgData, error) {
	resp, err := m.Client.GetMsgByConversationIDs(ctx, &msg.GetMsgByConversationIDsReq{
		ConversationIDs: docIDs,
		MaxSeqs:         seqs,
	})
	return resp.MsgDatas, err
}

//PullMessageBySeqList使用gRPC客户端按序列号检索消息。
//它直接将请求转发到gRPC客户端，并返回响应以及遇到的任何错误。

// PullMessageBySeqList retrieves messages by their sequence numbers using the gRPC client.
// It directly forwards the request to the gRPC client and returns the response along with any error encountered.
func (m *MessageRpcClient) PullMessageBySeqList(ctx context.Context, req *sdkws.PullMessageBySeqsReq) (*sdkws.PullMessageBySeqsResp, error) {

	resp, err := m.Client.PullMessageBySeqs(ctx, req)
	if err != nil {
		// Wrap the error to provide more context if the gRPC call fails.
		return nil, err
	}
	return resp, nil
}

func (m *MessageRpcClient) GetConversationMaxSeq(ctx context.Context, conversationID string) (int64, error) {
	resp, err := m.Client.GetConversationMaxSeq(ctx, &msg.GetConversationMaxSeqReq{ConversationID: conversationID})
	if err != nil {
		return 0, err
	}
	return resp.MaxSeq, nil
}

type NotificationSender struct {
	contentTypeConf map[int32]config.NotificationConf
	sessionTypeConf map[int32]int32
	sendMsg         func(ctx context.Context, req *msg.SendMsgReq) (*msg.SendMsgResp, error)
	getUserInfo     func(ctx context.Context, userID string) (*sdkws.UserInfo, error)
}

type NotificationSenderOptions func(*NotificationSender)

func WithLocalSendMsg(sendMsg func(ctx context.Context, req *msg.SendMsgReq) (*msg.SendMsgResp, error)) NotificationSenderOptions {
	return func(s *NotificationSender) {
		s.sendMsg = sendMsg
	}
}

func WithRpcClient(msgRpcClient *MessageRpcClient) NotificationSenderOptions {
	return func(s *NotificationSender) {
		s.sendMsg = msgRpcClient.SendMsg
	}
}

func WithUserRpcClient(userRpcClient *UserRpcClient) NotificationSenderOptions {
	return func(s *NotificationSender) {
		s.getUserInfo = userRpcClient.GetUserInfo
	}
}

func NewNotificationSender(config *config.GlobalConfig, opts ...NotificationSenderOptions) *NotificationSender {
	notificationSender := &NotificationSender{contentTypeConf: newContentTypeConf(config), sessionTypeConf: newSessionTypeConf()}
	for _, opt := range opts {
		opt(notificationSender)
	}
	return notificationSender
}

type notificationOpt struct {
	WithRpcGetUsername bool
}

type NotificationOptions func(*notificationOpt)

func WithRpcGetUserName() NotificationOptions {
	return func(opt *notificationOpt) {
		opt.WithRpcGetUsername = true
	}
}

func (s *NotificationSender) NotificationWithSesstionType(ctx context.Context, sendID, recvID string, contentType, sesstionType int32, m proto.Message, opts ...NotificationOptions) (err error) {
	// 构造通知元素，将消息体 m 序列化为 JSON 字符串
	n := sdkws.NotificationElem{Detail: utils.StructToJsonString(m)}
	// 将通知元素序列化为字节数组
	content, err := json.Marshal(&n)
	if err != nil {
		// 序列化失败时，构造详细错误信息并返回包装后的错误
		errInfo := fmt.Sprintf("MsgClient Notification json.Marshal failed, sendID:%s, recvID:%s, contentType:%d, msg:%s", sendID, recvID, contentType, m)
		return errs.Wrap(err, errInfo)
	}
	// 创建通知选项对象，用于后续可变参数配置
	notificationOpt := &notificationOpt{}
	// 处理所有可选参数，对 notificationOpt 进行自定义设置
	for _, opt := range opts {
		opt(notificationOpt)
	}
	// 定义消息请求对象和消息体对象
	var req msg.SendMsgReq
	var msg sdkws.MsgData
	var userInfo *sdkws.UserInfo
	// 如果需要通过 RPC 获取用户名，并且 getUserInfo 方法可用
	if notificationOpt.WithRpcGetUsername && s.getUserInfo != nil {
		// 根据 sendID 获取用户信息
		userInfo, err = s.getUserInfo(ctx, sendID)
		if err != nil {
			// 获取用户信息失败时，构造详细错误信息并返回包装后的错误
			errInfo := fmt.Sprintf("getUserInfo failed, sendID:%s", sendID)
			return errs.Wrap(err, errInfo)
		} else {
			// 获取成功则设置消息体的发送者昵称和头像
			msg.SenderNickname = userInfo.Nickname
			msg.SenderFaceURL = userInfo.FaceURL
		}
	}
	// 离线推送相关信息初始化
	var offlineInfo sdkws.OfflinePushInfo
	var title, desc, ex string
	// 设置消息发送者和接收者 ID
	msg.SendID = sendID
	msg.RecvID = recvID
	// 设置消息内容（JSON 编码）
	msg.Content = content
	// 设置消息来源为系统消息类型
	msg.MsgFrom = constant.SysMsgType
	// 设置消息内容类型
	msg.ContentType = contentType
	// 设置会话类型
	msg.SessionType = sesstionType
	// 如果是超级群聊，则设置群 ID
	if msg.SessionType == constant.SuperGroupChatType {
		msg.GroupID = recvID
	}
	if msg.SessionType == constant.GroupChatType {
		msg.GroupID = recvID
	}

	// 设置消息创建时间（毫秒时间戳）
	msg.CreateTime = utils.GetCurrentTimestampByMill()
	// 生成客户端消息 ID
	msg.ClientMsgID = utils.GetMsgID(sendID)
	// 根据内容类型获取通知配置
	optionsConfig := s.contentTypeConf[contentType]
	// 如果发送者和接收者相同且消息为已读回执，则设置为不可靠通知
	if sendID == recvID && contentType == constant.HasReadReceipt {
		optionsConfig.ReliabilityLevel = constant.UnreliableNotification
	}
	// 获取最终的消息选项配置
	options := config.GetOptionsByNotification(optionsConfig)
	// 根据内容类型设置消息选项
	s.SetOptionsByContentType(ctx, options, contentType)
	// 赋值消息选项
	msg.Options = options
	// 离线推送标题、描述、扩展字段赋值（此处为空字符串）
	offlineInfo.Title = title
	offlineInfo.Desc = desc
	offlineInfo.Ex = ex
	// 设置离线推送信息
	msg.OfflinePushInfo = &offlineInfo
	// 设置消息请求体
	req.MsgData = &msg
	// 调用 sendMsg 方法发送消息
	_, err = s.sendMsg(ctx, &req)
	if err != nil {
		// 发送失败时，构造详细错误信息并返回包装后的错误
		errInfo := fmt.Sprintf("MsgClient Notification SendMsg failed, req:%s", &req)
		return errs.Wrap(err, errInfo)
	}
	// 返回 nil 或 sendMsg 的错误
	return err
}

func (s *NotificationSender) Notification(ctx context.Context, sendID, recvID string, contentType int32, m proto.Message, opts ...NotificationOptions) error {
	return s.NotificationWithSesstionType(ctx, sendID, recvID, contentType, s.sessionTypeConf[contentType], m, opts...)
}

func (s *NotificationSender) SetOptionsByContentType(_ context.Context, options map[string]bool, contentType int32) {
	switch contentType {
	case constant.UserStatusChangeNotification:
		options[constant.IsSenderSync] = false
	default:
	}
}
