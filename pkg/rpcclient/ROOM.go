package rpcclient

import (
	"BaoIM-Server/pkg/common/config"
	util "BaoIM-Server/pkg/util/genutil"
	"baoim/protocol/group"
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

type RoomRpcClient Room

func NewRoomRpcClient(discov discoveryregistry.SvcDiscoveryRegistry, config *config.GlobalConfig) RoomRpcClient {
	return RoomRpcClient(*NewRoom(discov, config))
}

// 更新聊天室在线用户
func (g *RoomRpcClient) CleanOfflineUser(ctx context.Context, userID string) error {
	_, err := g.Client.CleanOfflineUser(ctx, &room.OnlineUserReq{
		UserID: userID,
	})
	return err
}

//// 更新聊天室在线用户  //修复用户加入前检查是否离线
//func (g *RoomRpcClient) InitRoomUser(ctx context.Context, userID string, groupID string, isAdmin bool) error {
//	_, err := g.Client.UpdateRoomUser(ctx, &room.UpdateRoomUserReq{
//		UserID:  userID,
//		RoomID:  groupID,
//		IsOwner: isAdmin,
//	})
//	return err
//}
//

// 解散房间 rtc
func (g *RoomRpcClient) DismissRoom(ctx context.Context, groupID string) error {
	_, err := g.Client.DismissRoom(ctx, &group.DismissGroupReq{
		GroupID:      groupID,
		DeleteMember: true,
	})
	return err
}
