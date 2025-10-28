package cache

import (
	pbroom "baoim/protocol/room"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type RoomCache interface {
	//获取聊天室列表接口
	GetRoomListCache(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
	//添加用户到列表
	UpdateRoomUserCache(ctx context.Context, userID string) error
	DeleteRoomUserCache(ctx context.Context, userID string) error
}

type RoomCacheRedis struct {
	//metaCache
	//groupDB        relationtb.GroupModelInterface
	//groupMemberDB  relationtb.GroupMemberModelInterface
	//groupRequestDB relationtb.GroupRequestModelInterface
	//expireTime     time.Duration
	//rcClient       *rockscache.Client
	//groupHash      GroupHash
	rdb redis.UniversalClient ////增加redis直接调用  用于房间列表
}

func NewRoomCacheRedis(
	rdb redis.UniversalClient,
	// groupDB relationtb.GroupModelInterface,
	// groupMemberDB relationtb.GroupMemberModelInterface,
	// groupRequestDB relationtb.GroupRequestModelInterface,
	// hashCode GroupHash,
	//
	//	opts rockscache.Options,
) RoomCache {
	//rcClient := rockscache.NewClient(rdb, opts)
	//mc := NewMetaCacheRedis(rcClient)
	//g := config.Config.LocalCache.Group
	//mc.SetTopic(g.Topic)
	//log.ZDebug(context.Background(), "group local cache init", "Topic", g.Topic, "SlotNum", g.SlotNum, "SlotSize", g.SlotSize, "enable", g.Enable())
	//mc.SetRawRedisClient(rdb)
	return &RoomCacheRedis{
		//rcClient: rcClient, expireTime: groupExpireTime,
		//groupDB: groupDB, groupMemberDB: groupMemberDB, groupRequestDB: groupRequestDB,
		//groupHash: hashCode,
		//metaCache: mc,
		rdb: rdb, ////增加redis直接调用  用于房间列表

	}

}

const (
	userRoomIDKey  = "ROOM_USER:"    // 哈希键：存储用户详情
	roomOnlineKey  = "ROOM_ONLINE:"  // 有序集合：存储用户ID+最后活跃时间
	OfflineTimeout = 5 * time.Minute // 离线超时时间
)

func (g *RoomCacheRedis) UpdateRoomUserCache(ctx context.Context, userID string) error {
	pipe := g.rdb.Pipeline()
	//	pipe.Set(ctx, userRoomIDKey+userID, roomID, 0)
	pipe.ZAdd(ctx, roomOnlineKey, redis.Z{
		Score:  float64(time.Now().UnixMilli()),
		Member: userID,
	})
	_, err := pipe.Exec(ctx)
	return errs.Wrap(err, "redis pipeline exec failed")
}

// 删除用户
func (g *RoomCacheRedis) DeleteRoomUserCache(ctx context.Context, userID string) error {
	pipe := g.rdb.Pipeline()
	//pipe.HDel(ctx, userRoomIDKey, userID)
	pipe.ZRem(ctx, roomOnlineKey, userID)
	_, err := pipe.Exec(ctx)
	return errs.Wrap(err, "delete user from redis failed")
}

// 获取房间列表，按积分倒序分页
func (g *RoomCacheRedis) GetRoomListCache(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error) {
	start := int64((page - 1) * pageSize)
	end := start + int64(pageSize) - 1
	// 积分倒序
	roomIDs, err := g.rdb.ZRevRange(ctx, "ROOM_LIST:", start, end).Result()
	if err != nil {
		return nil, err
	}
	var rooms []*sdkws.RoomInfo
	for _, roomID := range roomIDs {

		//println(roomID)

		key := fmt.Sprintf("ROOM:%s", roomID)
		data, err := g.rdb.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		//num, _ := strconv.Atoi(data["num"])
		score, _ := strconv.Atoi(data["score"])

		var arr []string
		err = json.Unmarshal([]byte(data["ms"]), &arr)
		var imgs []string
		if data["img"] != "" {
			err = json.Unmarshal([]byte(data["imgs"]), &imgs)
		}

		room := sdkws.RoomInfo{
			RoomID: data["id"],
			Uid:    data["uid"],
			Name:   data["name"],
			Img:    data["img"],
			Ms:     arr,

			Imgs:  imgs,
			Score: int64(score),
		}
		rooms = append(rooms, &room)
	}

	//println(len(rooms))
	//println(len(rooms[0].RoomID))

	return &pbroom.GetRoomListResp{Rooms: rooms}, nil
}
