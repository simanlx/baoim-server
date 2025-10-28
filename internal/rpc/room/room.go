package room

import (
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/cache"
	pbroom "baoim/protocol/room"
	"baoim/tools/discoveryregistry"
	"context"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func Start(config *config.GlobalConfig, client discoveryregistry.SvcDiscoveryRegistry, server *grpc.Server) error {

	rdb, err := cache.NewRedis(config)
	if err != nil {
		return err
	}

	var gs roomServer

	gs.rdb = rdb
	gs.config = config
	pbroom.RegisterRoomServer(server, &gs)
	return nil
}

type roomServer struct {
	rdb redis.UniversalClient
	//db                    controller.GroupDatabase
	//User                  rpcclient.UserRpcClient
	//Notification          *notification.GroupNotificationSender
	//conversationRpcClient rpcclient.ConversationRpcClient
	//msgRpcClient          rpcclient.MessageRpcClient
	config *config.GlobalConfig
}

// 添加用户到列表
func (r roomServer) AddUser(ctx context.Context, req *pbroom.AddUserReq) (*pbroom.AddUserResp, error) {
	//TODO implement me
	panic("implement me")
}

// 在用户列表中删除用户
func (r roomServer) DeleteUser(ctx context.Context, req *pbroom.DeleteUserReq) (*pbroom.DeleteUserResp, error) {
	//TODO implement me
	panic("implement me")
}
