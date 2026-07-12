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

package api

import (
	"BaoIM-Server/pkg/apistruct"
	"BaoIM-Server/pkg/authverify"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/constant"
	"baoim/protocol/msg"
	"baoim/protocol/sdkws"
	"baoim/tools/a2r"
	"baoim/tools/apiresp"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/mitchellh/mapstructure"
)

// MessageApi 结构体，封装消息相关的 API 操作
type MessageApi struct {
	*rpcclient.Message                          // 消息 RPC 客户端
	validate           *validator.Validate      // 参数校验器
	userRpcClient      *rpcclient.UserRpcClient // 用户 RPC 客户端
}

// 构造函数，初始化 MessageApi
func NewMessageApi(msgRpcClient *rpcclient.Message, userRpcClient *rpcclient.User) MessageApi {
	return MessageApi{
		Message:       msgRpcClient,
		validate:      validator.New(),
		userRpcClient: rpcclient.NewUserRpcClientByUser(userRpcClient),
	}
}

// 设置消息选项开关（如历史、持久化等）
func (MessageApi) SetOptions(options map[string]bool, value bool) {
	utils.SetSwitchFromOptions(options, constant.IsHistory, value)
	utils.SetSwitchFromOptions(options, constant.IsPersistent, value)
	utils.SetSwitchFromOptions(options, constant.IsSenderSync, value)
	utils.SetSwitchFromOptions(options, constant.IsConversationUpdate, value)
}

// 构造发送消息的请求
func (m MessageApi) newUserSendMsgReq(_ *gin.Context, params *apistruct.SendMsg) *msg.SendMsgReq {
	var newContent string

	switch params.ContentType {
	case constant.OANotification:
		notification := sdkws.NotificationElem{}
		notification.Detail = utils.StructToJsonString(params.Content)
		newContent = utils.StructToJsonString(&notification)
	case constant.Text:
		fallthrough
	case constant.Picture:
		fallthrough
	case constant.Custom:
		fallthrough
	case constant.Gift:
		fallthrough
	case constant.Voice:
		fallthrough
	case constant.Video:
		fallthrough
	case constant.File:
		fallthrough
	default:
		newContent = utils.StructToJsonString(params.Content)
	}
	options := make(map[string]bool, 5)
	if params.Options != nil {
		options = params.Options
	}

	if params.IsOnlineOnly {
		m.SetOptions(options, false)
	}
	if params.NotOfflinePush {
		utils.SetSwitchFromOptions(options, constant.IsOfflinePush, false)
	}

	// 构造 protobuf 消息数据
	pbData := msg.SendMsgReq{
		MsgData: &sdkws.MsgData{
			SendID:           params.SendID,
			GroupID:          params.GroupID,
			ClientMsgID:      utils.GetMsgID(params.SendID),
			SenderPlatformID: params.SenderPlatformID,
			SenderNickname:   params.SenderNickname,
			SenderFaceURL:    params.SenderFaceURL,
			SessionType:      params.SessionType,
			MsgFrom:          constant.SysMsgType,
			ContentType:      params.ContentType,
			Content:          []byte(newContent),
			CreateTime:       utils.GetCurrentTimestampByMill(),
			SendTime:         params.SendTime,
			Options:          options,
			OfflinePushInfo:  params.OfflinePushInfo,
		},
	}
	return &pbData
}

// 获取最大消息序列号
func (m *MessageApi) GetSeq(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetMaxSeq, m.Client, c)
}

// 按序列号拉取消息
func (m *MessageApi) PullMsgBySeqs(c *gin.Context) {

	a2r.Call(msg.MsgClient.PullMessageBySeqs, m.Client, c)
}

// 撤回消息
func (m *MessageApi) RevokeMsg(c *gin.Context) {
	a2r.Call(msg.MsgClient.RevokeMsg, m.Client, c)
}

// 标记消息为已读
func (m *MessageApi) MarkMsgsAsRead(c *gin.Context) {
	a2r.Call(msg.MsgClient.MarkMsgsAsRead, m.Client, c)
}

// 标记会话为已读
func (m *MessageApi) MarkConversationAsRead(c *gin.Context) {
	a2r.Call(msg.MsgClient.MarkConversationAsRead, m.Client, c)
}

// 获取会话已读和最大序列
func (m *MessageApi) GetConversationsHasReadAndMaxSeq(c *gin.Context) {

	a2r.Call(msg.MsgClient.GetConversationsHasReadAndMaxSeq, m.Client, c)
}

// 设置会话已读序列号
func (m *MessageApi) SetConversationHasReadSeq(c *gin.Context) {

	a2r.Call(msg.MsgClient.SetConversationHasReadSeq, m.Client, c)
}

// 清空会话消息
func (m *MessageApi) ClearConversationsMsg(c *gin.Context) {
	a2r.Call(msg.MsgClient.ClearConversationsMsg, m.Client, c)
}

// 用户清空所有消息
func (m *MessageApi) UserClearAllMsg(c *gin.Context) {
	a2r.Call(msg.MsgClient.UserClearAllMsg, m.Client, c)
}

// 删除消息
func (m *MessageApi) DeleteMsgs(c *gin.Context) {
	a2r.Call(msg.MsgClient.DeleteMsgs, m.Client, c)
}

// 物理删除指定序列消息
func (m *MessageApi) DeleteMsgPhysicalBySeq(c *gin.Context) {
	a2r.Call(msg.MsgClient.DeleteMsgPhysicalBySeq, m.Client, c)
}

// 物理删除消息
func (m *MessageApi) DeleteMsgPhysical(c *gin.Context) {
	a2r.Call(msg.MsgClient.DeleteMsgPhysical, m.Client, c)
}

// 组装发送消息请求结构体并做参数校验
func (m *MessageApi) getSendMsgReq(c *gin.Context, req apistruct.SendMsg) (sendMsgReq *msg.SendMsgReq, err error) {
	var data any
	log.ZDebug(c, "getSendMsgReq", "req", req.Content)
	// 根据内容类型初始化 data
	switch req.ContentType {
	case constant.Text:
		data = apistruct.TextElem{}
	case constant.Picture:
		data = apistruct.PictureElem{}
	case constant.Voice:
		data = apistruct.SoundElem{}
	case constant.Video:
		data = apistruct.VideoElem{}
	case constant.File:
		data = apistruct.FileElem{}
	case constant.AtText:
		data = apistruct.AtElem{}
	case constant.Custom:
		data = apistruct.CustomElem{}
	case constant.Gift:
		data = apistruct.CustomElem{}
	case constant.OANotification:
		data = apistruct.OANotificationElem{}
		req.SessionType = constant.NotificationChatType
		// 校验 OA 通知合法性
		if err = m.userRpcClient.GetNotificationByID(c, req.SendID); err != nil {
			return nil, err
		}
	default:
		return nil, errs.ErrArgs.WithDetail("not support err contentType")
	}
	// 弱类型解码 content 到 data
	if err := mapstructure.WeakDecode(req.Content, &data); err != nil {
		return nil, err
	}
	log.ZDebug(c, "getSendMsgReq", "req", req.Content)
	// 参数结构体校验
	if err := m.validate.Struct(data); err != nil {
		return nil, err
	}
	// 构造最终发送消息请求

	return m.newUserSendMsgReq(c, &req), nil
}

// 验证消息接口   如是否是好友 // 是否在群祖中等
func (m *MessageApi) MsgVerification(c *gin.Context) {
	a2r.Call(msg.MsgClient.MsgVerification, m.Client, c)

	//req := msg.SendMsgReq{}
	//// 绑定请求体到结构体
	//if err := c.BindJSON(&req); err != nil {
	//	apiresp.GinError(c, errs.ErrArgs.WithDetail(err.Error()).Wrap())
	//	return
	//}
	//// 权限校验：只有 App 管理员可发送
	//if !authverify.IsAppManagerUid(c, m.Config) {
	//	apiresp.GinError(c, errs.ErrNoPermission.Wrap("only app manager can send message"))
	//	return
	//}
	//// 构造发送消息请求
	////sendMsgReq, err := m.getSendMsgReq(c, req.SendMsg)
	////if err != nil {
	////	apiresp.GinError(c, err)
	////	return
	////}
	//// 设置接收者 ID
	////sendMsgReq.MsgData.RecvID = req.RecvID
	////var status int // 记录消息发送状态
	//// 真正发送消息
	//_, err := m.Client.MsgVerification(c, &req)
	//if err != nil {
	//	apiresp.GinError(c, err)
	//	return
	//}
	//// 返回成功响应和回包
	//apiresp.GinSuccess(c, nil)
}

// 发送消息的 HTTP 处理函数
func (m *MessageApi) SendMessage(c *gin.Context) {
	req := apistruct.SendMsgReq{}
	// 绑定请求体到结构体
	if err := c.BindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs.WithDetail(err.Error()).Wrap())
		return
	}

	// 权限校验：只有 App 管理员可发送
	if !authverify.IsAppManagerUid(c, m.Config) {
		apiresp.GinError(c, errs.ErrNoPermission.Wrap("only app manager can send message"))
		return
	}
	// 构造发送消息请求
	sendMsgReq, err := m.getSendMsgReq(c, req.SendMsg)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	// 设置接收者 ID
	sendMsgReq.MsgData.RecvID = req.RecvID

	//fmt.Println(sendMsgReq.MsgData.Options) // 直接输出整个 map

	var status int // 记录消息发送状态
	// 真正发送消息
	respPb, err := m.Client.SendMsg(c, sendMsgReq)
	if err != nil {
		status = constant.MsgSendFailed
		apiresp.GinError(c, err)
		return
	}
	status = constant.MsgSendSuccessed
	// 更新消息发送状态
	_, err = m.Client.SetSendMsgStatus(c, &msg.SetSendMsgStatusReq{
		Status: int32(status),
	})
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	// 返回成功响应和回包
	apiresp.GinSuccess(c, respPb)
}

// 发送业务通知（如 OA、系统消息等）
func (m *MessageApi) SendBusinessNotification(c *gin.Context) {
	req := struct {
		Key        string `json:"key"`
		Data       string `json:"data"`
		SendUserID string `json:"sendUserID" binding:"required"`
		RecvUserID string `json:"recvUserID" binding:"required"`
	}{}
	if err := c.BindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs.WithDetail(err.Error()).Wrap())
		return
	}
	if !authverify.IsAppManagerUid(c, m.Config) {
		apiresp.GinError(c, errs.ErrNoPermission.Wrap("only app manager can send message"))
		return
	}
	// 构造业务通知消息体
	sendMsgReq := msg.SendMsgReq{
		MsgData: &sdkws.MsgData{
			SendID: req.SendUserID,
			RecvID: req.RecvUserID,
			Content: []byte(utils.StructToJsonString(&sdkws.NotificationElem{
				Detail: utils.StructToJsonString(&struct {
					Key  string `json:"key"`
					Data string `json:"data"`
				}{Key: req.Key, Data: req.Data}),
			})),
			MsgFrom:     constant.SysMsgType,
			ContentType: constant.BusinessNotification,
			SessionType: constant.SingleChatType,
			CreateTime:  utils.GetCurrentTimestampByMill(),
			ClientMsgID: utils.GetMsgID(mcontext.GetOpUserID(c)),
			Options: config.GetOptionsByNotification(config.NotificationConf{
				IsSendMsg:        false,
				ReliabilityLevel: 1,
				UnreadCount:      false,
			}),
		},
	}
	// 发送业务通知
	respPb, err := m.Client.SendMsg(c, &sendMsgReq)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	apiresp.GinSuccess(c, respPb)
}

// 批量发送消息（可一次性群发给所有用户）
func (m *MessageApi) BatchSendMsg(c *gin.Context) {
	var (
		req  apistruct.BatchSendMsgReq
		resp apistruct.BatchSendMsgResp
	)
	if err := c.BindJSON(&req); err != nil {
		apiresp.GinError(c, errs.ErrArgs.WithDetail(err.Error()).Wrap())
		return
	}
	log.ZInfo(c, "BatchSendMsg", "req", req)
	// 权限校验
	if err := authverify.CheckAdmin(c, m.Config); err != nil {
		apiresp.GinError(c, errs.ErrNoPermission.Wrap("only app manager can send message"))
		return
	}

	var recvIDs []string
	if req.IsSendAll {
		// 如果发送给所有人则分页获取所有用户 ID
		pageNumber := 1
		showNumber := 500
		for {
			recvIDsPart, err := m.userRpcClient.GetAllUserIDs(c, int32(pageNumber), int32(showNumber))
			if err != nil {
				apiresp.GinError(c, err)
				return
			}
			recvIDs = append(recvIDs, recvIDsPart...)
			if len(recvIDsPart) < showNumber {
				break
			}
			pageNumber++
		}
	} else {
		recvIDs = req.RecvIDs
	}
	log.ZDebug(c, "BatchSendMsg nums", "nums ", len(recvIDs))
	sendMsgReq, err := m.getSendMsgReq(c, req.SendMsg)
	if err != nil {
		apiresp.GinError(c, err)
		return
	}
	// 遍历所有接收者 ID 批量发送
	for _, recvID := range recvIDs {
		sendMsgReq.MsgData.RecvID = recvID
		rpcResp, err := m.Client.SendMsg(c, sendMsgReq)
		if err != nil {
			resp.FailedIDs = append(resp.FailedIDs, recvID)
			continue
		}
		resp.Results = append(resp.Results, &apistruct.SingleReturnResult{
			ServerMsgID: rpcResp.ServerMsgID,
			ClientMsgID: rpcResp.ClientMsgID,
			SendTime:    rpcResp.SendTime,
			RecvID:      recvID,
		})
	}
	apiresp.GinSuccess(c, resp)
}

// 检查消息是否发送成功
func (m *MessageApi) CheckMsgIsSendSuccess(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetSendMsgStatus, m.Client, c)
}

// 获取用户在线状态（此处实际是调用 SendMsgStatus 方法）
func (m *MessageApi) GetUsersOnlineStatus(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetSendMsgStatus, m.Client, c)
}

// 获取活跃用户信息
func (m *MessageApi) GetActiveUser(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetActiveUser, m.Client, c)
}

// 获取活跃群组信息
func (m *MessageApi) GetActiveGroup(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetActiveGroup, m.Client, c)
}

// 消息内容搜索
func (m *MessageApi) SearchMsg(c *gin.Context) {
	a2r.Call(msg.MsgClient.SearchMessage, m.Client, c)
}

// 获取服务器时间（例如用于消息时间戳同步）
func (m *MessageApi) GetServerTime(c *gin.Context) {
	a2r.Call(msg.MsgClient.GetServerTime, m.Client, c)
}
