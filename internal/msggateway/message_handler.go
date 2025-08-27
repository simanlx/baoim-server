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

package msggateway

import (
	"BaoIM-Server/pkg/msgprocessor"
	"baoim/protocol/rtc"
	"context"
	"sync"

	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/msg"
	"baoim/protocol/push"
	"baoim/protocol/sdkws"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/utils"
	"github.com/go-playground/validator/v10"
	"google.golang.org/protobuf/proto"
)

type Req struct {
	ReqIdentifier int32  `json:"reqIdentifier" validate:"required"`
	Token         string `json:"token"`
	SendID        string `json:"sendID"        validate:"required"`
	OperationID   string `json:"operationID"   validate:"required"`
	MsgIncr       string `json:"msgIncr"       validate:"required"`
	Data          []byte `json:"data"`
}

func (r *Req) String() string {
	var tReq Req
	tReq.ReqIdentifier = r.ReqIdentifier
	tReq.Token = r.Token
	tReq.SendID = r.SendID
	tReq.OperationID = r.OperationID
	tReq.MsgIncr = r.MsgIncr
	return utils.StructToJsonString(tReq)
}

var reqPool = sync.Pool{
	New: func() any {
		return new(Req)
	},
}

func getReq() *Req {
	req := reqPool.Get().(*Req)
	req.Data = nil
	req.MsgIncr = ""
	req.OperationID = ""
	req.ReqIdentifier = 0
	req.SendID = ""
	req.Token = ""
	return req
}

func freeReq(req *Req) {
	reqPool.Put(req)
}

type Resp struct {
	ReqIdentifier int32  `json:"reqIdentifier"`
	MsgIncr       string `json:"msgIncr"`
	OperationID   string `json:"operationID"`
	ErrCode       int    `json:"errCode"`
	ErrMsg        string `json:"errMsg"`
	Data          []byte `json:"data"`
}

func (r *Resp) String() string {
	var tResp Resp
	tResp.ReqIdentifier = r.ReqIdentifier
	tResp.MsgIncr = r.MsgIncr
	tResp.OperationID = r.OperationID
	tResp.ErrCode = r.ErrCode
	tResp.ErrMsg = r.ErrMsg
	return utils.StructToJsonString(tResp)
}

type MessageHandler interface {
	GetSeq(context context.Context, data *Req) ([]byte, error)
	SendMessage(context context.Context, data *Req) ([]byte, error)
	SendSignalMessage(context context.Context, data *Req) ([]byte, error)
	PullMessageBySeqList(context context.Context, data *Req) ([]byte, error)
	UserLogout(context context.Context, data *Req) ([]byte, error)
	SetUserDeviceBackground(context context.Context, data *Req) ([]byte, bool, error)
}

var _ MessageHandler = (*GrpcHandler)(nil)

type GrpcHandler struct {
	msgRpcClient *rpcclient.MessageRpcClient
	pushClient   *rpcclient.PushRpcClient
	validate     *validator.Validate
	////增加rtc
	rtcClient *rpcclient.RtcRpcClient
}

func NewGrpcHandler(validate *validator.Validate, client discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) *GrpcHandler {
	msgRpcClient := rpcclient.NewMessageRpcClient(client, config)
	pushRpcClient := rpcclient.NewPushRpcClient(client, config)
	rtcClient := rpcclient.NewRtcRpcClient(client, config)
	return &GrpcHandler{
		msgRpcClient: &msgRpcClient,
		pushClient:   &pushRpcClient,
		validate:     validate,
		rtcClient:    &rtcClient,
	}
}

func (g GrpcHandler) GetSeq(context context.Context, data *Req) ([]byte, error) {
	req := sdkws.GetMaxSeqReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.Wrap(err, "GetSeq: error unmarshaling request")
	}
	if err := g.validate.Struct(&req); err != nil {
		return nil, errs.Wrap(err, "GetSeq: validation failed")
	}
	resp, err := g.msgRpcClient.GetMaxSeq(context, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.Wrap(err, "GetSeq: error marshaling response")
	}
	return c, nil
}

// SendMessage handles the sending of messages through gRPC. It unmarshals the request data,
// validates the message, and then sends it using the message RPC client.
func (g GrpcHandler) SendMessage(ctx context.Context, data *Req) ([]byte, error) {
	// Unmarshal the message data from the request.
	var msgData sdkws.MsgData
	if err := proto.Unmarshal(data.Data, &msgData); err != nil {
		return nil, errs.Wrap(err, "error unmarshalling message data")
	}

	// Validate the message data structure.
	if err := g.validate.Struct(&msgData); err != nil {
		return nil, errs.Wrap(err, "message data validation failed")
	}

	req := msg.SendMsgReq{MsgData: &msgData}

	resp, err := g.msgRpcClient.SendMsg(ctx, &req)
	if err != nil {
		return nil, err
	}

	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.Wrap(err, "error marshaling response")
	}

	return c, nil
}

// /信令消息 实现错误 未实现完成 客户端sdk被邀请者无法正常收到信令消息
func (g GrpcHandler) SendSignalMessage(context context.Context, data *Req) ([]byte, error) {
	signalReq := rtc.SignalReq{}
	if err := proto.Unmarshal(data.Data, &signalReq); err != nil {
		return nil, err
	}
	if err := g.validate.Struct(&signalReq); err != nil {
		return nil, err
	}

	req := &rtc.SignalMessageAssembleReq{
		SignalReq: &signalReq,
	}

	// 调用RTC RPC客户端发送信号消息
	respPb, err := g.rtcClient.Client.SignalMessageAssemble(context, req)
	if err != nil {
		return nil, errs.Wrap(err, "call rtc SendSignalMessage failed")
	}
	signalResp := rtc.SignalResp{}
	signalResp.Payload = respPb.SignalResp.Payload

	msgData := sdkws.MsgData{}
	utils.CopyStructFields(&msgData, respPb.MsgData)

	msgData.Options = config.GetOptionsByNotification(config.NotificationConf{
		IsSendMsg:        false,
		ReliabilityLevel: 1,
		UnreadCount:      false,
		//OfflinePush: config.POfflinePush{
		//	true,
		//	"dddd",
		//	"",
		//	"",
		//},
	})
	//msgData.Content = data.Data

	rpcPushMsg := push.PushMsgReq{MsgData: &msgData, ConversationID: msgprocessor.GetConversationIDByMsg(&msgData)}
	_, err = g.pushClient.Client.PushMsg(context, &rpcPushMsg)
	if err != nil {
		println("推动错误", msgData.SendID, err.Error())
	}
	println("ConversationID:", msgprocessor.GetConversationIDByMsg(&msgData))
	//sendMsgReq.MsgData.Content = []byte("信令消息")
	//消息rpc msg未实现 信令相关
	//_, err = g.msgRpcClient.SendMsg(context, &msg.SendMsgReq{
	//	MsgData: &msgData,
	//})
	if err != nil {
		return nil, err
	}

	c, err := proto.Marshal(respPb)
	if err != nil {
		return nil, errs.Wrap(err, "error marshaling response")
	}

	return c, nil
}

//func (g GrpcHandler) SendSignalMessage(context context.Context, data *Req) ([]byte, error) {
//	resp, err := g.msgRpcClient.SendMsg(context, nil)
//	if err != nil {
//		return nil, err
//	}
//	c, err := proto.Marshal(resp)
//	if err != nil {
//		return nil, errs.Wrap(err, "error marshaling response")
//	}
//	return c, nil
//}

func (g GrpcHandler) PullMessageBySeqList(context context.Context, data *Req) ([]byte, error) {
	req := sdkws.PullMessageBySeqsReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.Wrap(err, "error unmarshaling request")
	}
	if err := g.validate.Struct(data); err != nil {
		return nil, errs.Wrap(err, "validation failed")
	}
	resp, err := g.msgRpcClient.PullMessageBySeqList(context, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.Wrap(err, "error marshaling response")
	}
	return c, nil
}

func (g GrpcHandler) UserLogout(context context.Context, data *Req) ([]byte, error) {
	req := push.DelUserPushTokenReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, errs.Wrap(err, "error unmarshaling request")
	}
	resp, err := g.pushClient.DelUserPushToken(context, &req)
	if err != nil {
		return nil, err
	}
	c, err := proto.Marshal(resp)
	if err != nil {
		return nil, errs.Wrap(err, "error marshaling response")
	}
	return c, nil
}

func (g GrpcHandler) SetUserDeviceBackground(_ context.Context, data *Req) ([]byte, bool, error) {
	req := sdkws.SetAppBackgroundStatusReq{}
	if err := proto.Unmarshal(data.Data, &req); err != nil {
		return nil, false, errs.Wrap(err, "error unmarshaling request")
	}
	if err := g.validate.Struct(data); err != nil {
		return nil, false, errs.Wrap(err, "validation failed")
	}
	return nil, req.IsBackground, nil
}

// func (g GrpcHandler) call[T any](ctx context.Context, data Req, m proto.Message, rpc func(ctx context.Context, req
// proto.Message)) ([]byte, error) {
//	if err := proto.Unmarshal(data.Data, m); err != nil {
//		return nil, err
//	}
//	if err := g.validate.Struct(m); err != nil {
//		return nil, err
//	}
//	rpc(ctx, m)
//	req := msg.SendMsgReq{MsgData: &msgData}
//	resp, err := g.notification.Msg.SendMsg(context, &req)
//	if err != nil {
//		return nil, err
//	}
//	c, err := proto.Marshal(resp)
//	if err != nil {
//		return nil, err
//	}
//	return c, nil
//}
