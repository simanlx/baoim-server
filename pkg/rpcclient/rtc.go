package rpcclient

import (
	"BaoIM-Server/pkg/common/config"
	util "BaoIM-Server/pkg/util/genutil"
	"baoim/protocol/rtc"

	"baoim/tools/discoveryregistry"
	"context"
	"google.golang.org/grpc"
)

type Rtc struct {
	conn grpc.ClientConnInterface
	//Client push.PushMsgServiceClient
	Client rtc.RtcServiceClient
	discov discoveryregistry.SvcDiscoveryRegistry
}

func NewRpcRtc(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) *Rtc {
	conn, err := discov.GetConn(context.Background(), config.RpcRegisterName.OpenImRtcName)
	if err != nil {
		util.ExitWithError(err)
	}
	return &Rtc{
		discov: discov,
		conn:   conn,
		Client: rtc.NewRtcServiceClient(conn),
	}
}

type RtcRpcClient Rtc

func NewRtcRpcClient(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) RtcRpcClient {
	return RtcRpcClient(*NewRpcRtc(discov, config))
}

//func (p *PushRpcClient) DelUserPushToken(ctx context.Context, req *push.DelUserPushTokenReq) (*push.DelUserPushTokenResp, error) {
//	return p.Client.DelUserPushToken(ctx, req)
//}
