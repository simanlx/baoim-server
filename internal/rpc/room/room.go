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
	"fmt"
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

	var gs roomServer
	gs.db = database
	gs.rdb = rdb
	gs.config = config
	// 启动离线用户清理定时器

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
func (r roomServer) UpdateRoomUser(ctx context.Context, req *pbroom.UpdateRoomUserReq) (*pbroom.UpdateRoomUserResp, error) {
	uid := mcontext.GetOpUserID(ctx)
	if uid == "" && req.UserID == uid {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	println("uid======", uid)

	if err := r.db.UpdateRoomUser(ctx, uid, req.RoomID); err != nil {
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

func (r roomServer) CleanOfflineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {

	roomID, err := r.db.CleanOfflineUsers(ctx, req.UserID)
	if err != nil {
		return nil, err
	}

	if roomID != "" {

		timer := time.NewTimer(5 * time.Minute)
		go func() {
			<-timer.C
			// 任务触发时再次检查：仅离线状态才执行目标操作
			rID, err1 := r.db.GetRoomUser(context.Background(), req.UserID)
			if err1 != nil {
				// 这里不能返回值，改为打印错误日志
				fmt.Printf("查询用户[%s]房间信息失败: %v\n", req.UserID, err1)
				return // 直接return退出匿名函数
			}
			//如果 rID == roomID 说明用户上线了并且还在房间内还在房间内，不执行目标逻辑
			if rID != "" && rID != roomID {
				fmt.Printf("用户[%s]离线超过5分钟，执行目标逻辑（如清理资源/发送通知）\n", req.UserID)
			}
		}()

	}

	return &pbroom.OnlineUserResp{}, nil
}

func (r roomServer) OnlineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {

	//关闭定时器未实现
	//TODO implement me
	panic("implement me")
}
