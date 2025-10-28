package rpcclient

import (
	"BaoIM-Server/pkg/common/config"
	util "BaoIM-Server/pkg/util/genutil"
	"context"

	"baoim/protocol/room"
	"baoim/tools/discoveryregistry"
)

type Room struct {
	Client room.RoomClient
	discov discoveryregistry.SvcDiscoveryRegistry
	Config *config.GlobalConfig
}

func NewRoom(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) *Room {
	conn, err := discov.GetConn(context.Background(), config.RpcRegisterName.OpenImRoomName)
	if err != nil {
		util.ExitWithError(err)
	}
	client := room.NewRoomClient(conn)
	return &Room{discov: discov, Client: client, Config: config}
}
