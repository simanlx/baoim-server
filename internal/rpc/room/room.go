package room

import (
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"

	pbroom "baoim/protocol/room"
	"baoim/tools/errs"

	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func Start(config *config.GlobalConfig, server *grpc.Server) error {

	rdb, err := cache.NewRedis(config)
	if err != nil {
		return err
	}

	database := controller.NewRoomDatabase(rdb)

	var gs roomServer
	gs.db = database
	gs.rdb = rdb
	gs.config = config
	pbroom.RegisterRoomServer(server, &gs)
	return nil
}

type roomServer struct {
	rdb redis.UniversalClient
	db  controller.RoomDatabase
	//User                  rpcclient.UserRpcClient
	//Notification          *notification.GroupNotificationSender
	//conversationRpcClient rpcclient.ConversationRpcClient
	//msgRpcClient          rpcclient.MessageRpcClient
	config *config.GlobalConfig
}

func (r roomServer) GetRoomList(ctx context.Context, req *pbroom.GetRoomListReq) (*pbroom.GetRoomListResp, error) {
	if req.PageNumber == 0 || req.ShowNumber == 0 {
		return nil, errs.Wrap(errors.New("parameter error"))
	}

	list, err := r.db.GetRoomList(ctx, req.PageNumber, req.ShowNumber)
	if err != nil {
		return nil, err
	}

	//println(list.Rooms[0].RoomID)
	return list, nil
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
