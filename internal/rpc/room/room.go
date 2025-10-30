package room

import (
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"
	"BaoIM-Server/pkg/rpcclient"
	pbroom "baoim/protocol/room"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"time"
)

func Start(config *config.GlobalConfig, client discoveryregistry.SvcDiscoveryRegistry, server *grpc.Server) error {

	rdb, err := cache.NewRedis(config)
	if err != nil {
		return err
	}

	database := controller.NewRoomDatabase(rdb)
	groupRpcClient := rpcclient.NewGroupRpcClient(client, config)
	var gs roomServer
	gs.db = database
	gs.rdb = rdb
	gs.config = config
	gs.groupRpcClient = groupRpcClient
	gs.startOfflineCleaner()
	// 初始化用户定时器映射
	//gs.userTimers = make(map[string]*userTimerInfo)
	pbroom.RegisterRoomServer(server, &gs)
	return nil
}

// 定义用户定时器信息结构，存储定时器和关联的房间ID
//type userTimerInfo struct {
//	Timer  *time.Timer // 定时器实例
//	RoomID string      // 关联的房间ID
//}

type roomServer struct {
	rdb redis.UniversalClient
	db  controller.RoomDatabase
	//User                  rpcclient.UserRpcClient
	//Notification          *notification.GroupNotificationSender
	//conversationRpcClient rpcclient.ConversationRpcClient
	//msgRpcClient          rpcclient.MessageRpcClient
	groupRpcClient rpcclient.GroupRpcClient
	config         *config.GlobalConfig
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
func (r roomServer) UpdateRoomUser(ctx context.Context, req *pbroom.UpdateRoomUserReq) (*pbroom.UpdateRoomUserResp, error) {
	uid := mcontext.GetOpUserID(ctx)
	if uid == "" && req.UserID == uid {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	println("uid======", uid)

	if err := r.db.UpdateRoomUser(ctx, uid, req.RoomID, req.IsOwner); err != nil {
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

// 用户离线时触发，检查用户是否在房间内，如果在房间内则添加到离线列表，否则直接在在线聊表删除
func (r roomServer) CleanOfflineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {
	//获取用户是否在房间内
	roomID, err := r.db.GetRoomUserRoomID(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	//如果用户在房间内，把用户添加到离线列表,等在轮训 退出或解散群组
	if roomID != "" {
		//用户在房间内，把用户添加到离线列表
		err := r.db.AddOfflineUser(ctx, req.UserID)
		if err != nil {
			return nil, err
		}
	} else {
		//用户不在房间内，直接在 在线列表中删除
		err := r.db.DeleteRoomUser(ctx, req.UserID)
		if err != nil {
			return nil, err
		}
	}

	return &pbroom.OnlineUserResp{}, nil
}

// 启动离线用户清理定时器
func (r *roomServer) startOfflineCleaner() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		println("开始")
		for range ticker.C {
			println("轮训执行")
			ctx := context.Background()
			offlineUsers, err := r.db.CleanOfflineUsers(ctx)
			if err != nil {
				log.ZError(ctx, "clean offline users failed", err)
				continue
			}

			// 处理有房间ID的离线用户
			for _, room := range offlineUsers {
				if room["roomID"] != "" {
					// 处理有房间ID的离线用户
					if room["isOwner"] == "0" {
						//解散群组 忽略错误
						_ = r.groupRpcClient.DismissRoom(ctx, room["roomID"])

						println("执行退出群组")
					} else {
						_ = r.groupRpcClient.QuitRoom(ctx, room["roomID"], room["userID"])
						println("执行解散群组")
					}
				}
				//
			}
		}
	}()
}
