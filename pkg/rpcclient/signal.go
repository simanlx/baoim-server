package rpcclient

import (
	"context"

	"baoim/tools/discoveryregistry"
	"google.golang.org/grpc"

	"baoim/protocol/rtc"

	"BaoIM-Server/pkg/common/config"
)

type Signal struct {
	conn grpc.ClientConnInterface

	Client rtc.RtcServiceClient
	discov discoveryregistry.SvcDiscoveryRegistry
}

func NewSignal(discov discoveryregistry.SvcDiscoveryRegistry) *Signal {
	conn, err := discov.GetConn(context.Background(), config.Config.RpcRegisterName.OpenImRtcName)
	if err != nil {
		panic(err)
	}
	client := rtc.NewRtcServiceClient(conn)
	return &Signal{discov: discov, conn: conn, Client: client}
}

type SignalRpcClient Signal

func NewSignalRpcClient(discov discoveryregistry.SvcDiscoveryRegistry) SignalRpcClient {
	return SignalRpcClient(*NewSignal(discov))
}
