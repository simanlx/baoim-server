package cache

import (
	pbroom "baoim/protocol/room"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type RoomCache interface {
	//获取聊天室列表接口
	GetRoomListCache(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
	//添加用户到列表
	UpdateRoomUserCache(ctx context.Context, userID string, roomID string, isAdmin bool) error
	//获取用户所在房间
	GetRoomUserRoomIDCache(ctx context.Context, userID string) (string, error)
	DeleteRoomUserCache(ctx context.Context, userID string) error
	//清理离线用户
	CleanOfflineUsersCache(ctx context.Context) ([]map[string]string, error)

	//添加用户为离线状态
	AddOfflineUsersCache(ctx context.Context, userID string) error
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
	userRoomIDKey  = "ROOM_USER:" // 哈希键：存储用户详情
	roomOfflineKey = "ROOM_OFFLINE:"

	OfflineTimeout = 5 * time.Minute // 离线超时时间
)

func (g *RoomCacheRedis) UpdateRoomUserCache(ctx context.Context, userID string, roomID string, isOwner bool) error {
	v := map[string]interface{}{
		"userID":  userID,
		"roomID":  roomID,
		"isOwner": isOwner,
	}
	// 设置 key 并指定 1 小时后过期
	err := g.rdb.HSet(ctx, userRoomIDKey+userID, v, 0).Err()
	if err != nil {
		return errs.Wrap(err, "redis set failed")
	}
	return nil
}

func (g *RoomCacheRedis) GetRoomUserRoomIDCache(ctx context.Context, userID string) (string, error) {
	// 设置 key 并指定 1 小时后过期
	result, err := g.rdb.HGet(ctx, userRoomIDKey+userID, "roomID").Result()
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

// 清除离线用户缓存  未来参考数据量超过2000可能堵塞 redis
func (g *RoomCacheRedis) CleanOfflineUsersCache(ctx context.Context) ([]map[string]string, error) {
	// 计算过期时间戳（毫秒级）
	expireTime := time.Now().Add(-OfflineTimeout).UnixMilli()
	maxTime := fmt.Sprintf("%d", expireTime)

	// 定义Lua脚本，增加userIDs为空的快速返回逻辑
	cleanOfflineUsersScript := `
local roomOfflineKey = KEYS[1]       -- 离线用户ZSet的键
local userRoomKeyPrefix = KEYS[2]    -- 用户房间信息哈希表的前缀
local expireTime = ARGV[1]           -- 过期时间戳（字符串）

-- 1. 原子获取并删除ZSet中分数<=expireTime的用户ID
local userIDs = redis.call('ZRANGEBYSCORE', roomOfflineKey, 0, expireTime)

-- 若用户ID列表为空，直接返回空数组（提前退出，减少无效操作）
if #userIDs == 0 then
    return cjson.encode({})
end

-- 删除获取到的离线用户（仅当有用户时执行删除）
redis.call('ZREMRANGEBYSCORE', roomOfflineKey, 0, expireTime)

local result = {}  -- 最终返回的房间信息数组

-- 2. 遍历用户ID，原子获取并删除房间信息
for _, userID in ipairs(userIDs) do
    local hashKey = userRoomKeyPrefix .. userID  -- 构造用户房间信息的哈希键
    local infoArr = redis.call('HGETALL', hashKey)  -- 获取哈希表所有字段和值
    
    -- 转换哈希表结果为map（HGETALL返回[key1, val1, key2, val2]形式的数组）
    if #infoArr > 0 then
        local infoMap = {}
        for i = 1, #infoArr, 2 do
            infoMap[infoArr[i]] = infoArr[i+1]
        end
        -- 只保留有roomID的有效信息
        if infoMap.roomID and infoMap.roomID ~= "" then
            table.insert(result, infoMap)
        end
    end
    
    -- 原子删除用户房间信息哈希表
    redis.call('DEL', hashKey)
end

-- 返回JSON格式的结果
return cjson.encode(result)
`
	// 预编译Lua脚本
	script := redis.NewScript(cleanOfflineUsersScript)
	// 执行脚本
	result, err := script.Run(ctx, g.rdb, []string{roomOfflineKey, userRoomIDKey}, maxTime).Result()
	if err != nil {
		return nil, errors.Wrap(err, "原子清理离线用户缓存失败")
	}
	// 处理空结果（用户ID列表为空时直接返回）
	if result == nil {
		return nil, nil
	}
	// 解析返回结果
	resultStr, ok := result.(string)
	if !ok {
		return nil, errors.New("脚本返回结果格式错误（非字符串）")
	}
	var data []map[string]string
	if err := json.Unmarshal([]byte(resultStr), &data); err != nil {
		return nil, errors.Wrap(err, "解析脚本返回结果失败")
	}

	return data, nil

}

// 设置用户为离线状态
func (g *RoomCacheRedis) AddOfflineUsersCache(ctx context.Context, userID string) error {
	// 往有序集合添加用户（score为当前时间戳，用于后续排序或清理）
	err := g.rdb.ZAdd(ctx, roomOfflineKey, redis.Z{
		Score:  float64(time.Now().UnixMilli()), // 毫秒级时间戳作为分数，方便后续按时间筛选
		Member: userID,
	}).Err()
	if err != nil {
		return errs.Wrap(err, "redis ZAdd failed") // 仅返回error，匹配函数声明
	}
	return nil
}
