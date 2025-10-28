package controller

import (
	"BaoIM-Server/pkg/common/db/cache"
	pbroom "baoim/protocol/room"
	"context"
	"github.com/redis/go-redis/v9"
)

type RoomDatabase interface {
	//获取聊天室列表 接口
	GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
}

func NewRoomDatabase(
	rdb redis.UniversalClient,
	// groupDB relationtb.GroupModelInterface,
	// groupMemberDB relationtb.GroupMemberModelInterface,
	// groupRequestDB relationtb.GroupRequestModelInterface,
	// ctxTx tx.CtxTx,
	// groupHash cache.GroupHash,
) RoomDatabase {
	//rcOptions := rockscache.NewDefaultOptions()
	//rcOptions.StrongConsistency = true
	//rcOptions.RandomExpireAdjustment = 0.2
	return &roomDatabase{
		//groupDB:        groupDB,
		//groupMemberDB:  groupMemberDB,
		//groupRequestDB: groupRequestDB,
		//ctxTx: ctxTx,
		cache: cache.NewRoomCacheRedis(rdb),
	}
}

type roomDatabase struct {
	//groupDB        relationtb.GroupModelInterface
	//groupMemberDB  relationtb.GroupMemberModelInterface
	//groupRequestDB relationtb.GroupRequestModelInterface
	//ctxTx          tx.CtxTx
	cache cache.RoomCache
}

func (r roomDatabase) GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error) {

	return r.cache.GetRoomListCache(ctx, page, pageSize)

}
