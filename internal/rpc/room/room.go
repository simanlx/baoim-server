package room

import (
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"
	pbroom "baoim/protocol/room"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/mcontext"
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func Start(config *config.GlobalConfig, client discoveryregistry.SvcDiscoveryRegistry, server *grpc.Server) error {

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

//// 启动离线用户清理定时器
//func (r *roomServer) startOfflineCleaner() {
//	ticker := time.NewTicker(1 * time.Minute)
//	go func() {
//		for range ticker.C {
//			ctx := context.Background()
//			offlineUsers, err := r.cache.CleanOfflineUsers(ctx)
//			if err != nil {
//				log.ZError(ctx, "clean offline users failed", err)
//				continue
//			}
//
//			// 处理有房间ID的离线用户
//			for _, user := range offlineUsers {
//				if user.RoomID != "" {
//					_, err := r.groupClient.QuitRoomList(ctx, &pbgroup.QuitRoomListReq{
//						RoomID: user.RoomID,
//						UserID: user.UserID,
//					})
//					if err != nil {
//						log.ZError(ctx, "call quit room failed", err, "userID", user.UserID, "roomID", user.RoomID)
//					}
//				}
//			}
//		}
//	}()
//}

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
func (r roomServer) UpdateRoomUser(ctx context.Context, req *pbroom.UpdateRoomUserReq) (*pbroom.UpdateRoomUserResp, error) {
	uid := mcontext.GetOpUserID(ctx)
	if uid == "" && req.UserID == uid {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	println("uid======", uid)

	if err := r.db.UpdateRoomUser(ctx, uid); err != nil {
		return nil, err
	}
	return &pbroom.UpdateRoomUserResp{}, nil
}

// 在用户列表中删除用户
func (r roomServer) DeleteRoomUser(ctx context.Context, req *pbroom.DeleteRoomUserReq) (*pbroom.DeleteRoomUserResp, error) {
	uid := mcontext.GetOpUserID(ctx)
	if uid == "" {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	if err := r.db.DeleteRoomUser(ctx, uid); err != nil {
		return nil, err
	}
	return &pbroom.DeleteRoomUserResp{}, nil
}
