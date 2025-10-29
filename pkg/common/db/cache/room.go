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
)

type RoomCache interface {
	//获取聊天室列表接口
	GetRoomListCache(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
	//添加用户到列表
	UpdateRoomUserCache(ctx context.Context, userID string, roomID string) error
	//获取用户所在房间
	GetRoomUserCache(ctx context.Context, userID string) (string, error)
	DeleteRoomUserCache(ctx context.Context, userID string) error
	//清理离线用户
	CleanOfflineUsersCache(ctx context.Context, userID string) (string, error)
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
	userRoomIDKey = "ROOM_USER:"   // 哈希键：存储用户详情
	roomOnlineKey = "ROOM_ONLINE:" // 有序集合：存储用户ID+最后活跃时间
	//OfflineTimeout = 5 * time.Minute // 离线超时时间
)

func (g *RoomCacheRedis) UpdateRoomUserCache(ctx context.Context, userID string, roomID string) error {
	// 设置 key 并指定 1 小时后过期
	err := g.rdb.Set(ctx, userRoomIDKey+userID, roomID, 0).Err()
	if err != nil {
		return errs.Wrap(err, "redis set failed")
	}
	return nil
}

func (g *RoomCacheRedis) GetRoomUserCache(ctx context.Context, userID string) (string, error) {
	// 设置 key 并指定 1 小时后过期
	result, err := g.rdb.Get(ctx, userRoomIDKey+userID).Result()
	if err != nil {
		return "", errs.Wrap(err, "redis set failed")
	}
	return result, nil
}

// 删除用户
func (g *RoomCacheRedis) DeleteRoomUserCache(ctx context.Context, userID string) error {
	//pipe := g.rdb.Pipeline()
	////pipe.HDel(ctx, userRoomIDKey, userID)
	//pipe.ZRem(ctx, roomOnlineKey, userID)
	//_, err := pipe.Exec(ctx)
	//if err != nil {
	//	return errs.Wrap(err, "redis pipeline exec failed")
	//}

	err := g.rdb.Del(ctx, userRoomIDKey+userID).Err()
	if err != nil {
		return errs.Wrap(err, "redis set failed")
	}
	return nil
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

func (g *RoomCacheRedis) CleanOfflineUsersCache(ctx context.Context, userID string) (string, error) {
	//GetDel redis 版本 6.2.0+ 新增命令，用于获取并删除键值对。
	roomID, err := g.rdb.GetDel(ctx, userRoomIDKey+userID).Result()
	if err != nil {
		return "", errs.Wrap(err, "redis set failed")
	}
	return roomID, nil
}
