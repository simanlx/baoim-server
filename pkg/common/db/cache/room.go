package cache

import (
	"BaoIM-Server/pkg/common/cachekey"
	"BaoIM-Server/pkg/common/config"
	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/constant"
	pbroom "baoim/protocol/room"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"context"
	"encoding/json"
	"fmt"
	"github.com/dtm-labs/rockscache"
	"github.com/redis/go-redis/v9"
	"strconv"
	"time"
)

type RoomCache interface {
	metaCache
	NewCache() RoomCache

	GetRoomInfo(ctx context.Context, groupID string) (group *relationtb.GroupModel, err error)
	GetRoomMemberIDs(ctx context.Context, groupID string) (groupMemberIDs []string, err error)
	GetRoomMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error)
	GetRoomOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error)
	GetRoomRoleLevelMemberInfo(ctx context.Context, groupID string, roleLevel int32) ([]*relationtb.GroupMemberModel, error)

	DelRoomsInfo(groupIDs ...string) RoomCache
	DelRoomMembersHash(groupID string) RoomCache
	DelRoomMemberIDs(groupID string) RoomCache
	DelJoinedRoomID(userID ...string) RoomCache
	DelRoomRoleLevel(groupID string, roleLevel []int32) RoomCache
	DelRoomAllRoleLevel(groupID string) RoomCache
	DelRoomMembersInfo(groupID string, userID ...string) RoomCache
	DelRoomsMemberNum(groupID ...string) RoomCache
	DelayDelOther(ctx context.Context, roomID string, uIDs ...string) error
	//删除 用户 已读seq 及 最大seq
	DelUserRoomSeq(ctx context.Context, roomID string, uIDs ...string) error

	GetRoomMemberInfo(ctx context.Context, groupID, userID string) (groupMember *relationtb.GroupMemberModel, err error)
	GetRoomMemberNum(ctx context.Context, groupID string) (memberNum int64, err error)
	GetRoomRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error)

	// AddRoomCache 创建房间时 添加房间缓存
	AddRoomCache(ctx context.Context, room *sdkws.RoomInfo) error

	// DelRoomCache 在房间列表删除  并删除 \房间信息 \用户关联房间
	DelRoomCache(ctx context.Context, uIDs []string, roomID string) error
	// RemoveRoomUserCache 退出房间 删除用户座位序列及头像
	RemoveRoomUserCache(ctx context.Context, roomID string, uid string, img string) error
	// KickRemoveRoomUserCache 踢出房间 删除用户座位序列及头像
	KickRemoveRoomUserCache(ctx context.Context, roomID string, uid string, img string) error
	// UpdateRoomUserCache 加入房间 更新用户座位序列及头像
	UpdateRoomUserCache(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error)

	// GetRoomListCache 获取房间列表缓存
	GetRoomListCache(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
	// GetRoomInfoCache 获取当前房间信息（包括成员、头像等）
	GetRoomInfoCache(ctx context.Context, roomID string) (*sdkws.RoomInfo, error)
	// UpdateUserRoomCache 增加更新用户 关联 房间缓存
	AddUserRoomCache(ctx context.Context, uid string, roomID string, isOwner bool) error
	// GetRoomUserRoomIDCache 获取房间 用户 关联 房间ID 缓存
	GetRoomUserRoomIDCache(ctx context.Context, userID string) (string, error)
	// GetUserRoomCache 获取用户 关联 房间缓存
	GetUserRoomCache(ctx context.Context, userID string) (roomID string, isOwner bool, err error)
	// DelUserRoomCache 删除用户 关联 房间缓存
	DelUserRoomCache(ctx context.Context, uid string) error
	// BatchDelUserRoomCache 批量删除用户 关联 房间缓存
	//BatchDelUserRoomCache(ctx context.Context, userIDs []string) error

	//调价用户到在线列表
	AddOnlineUsersCache(ctx context.Context, userID string) error
	//在在线列表中移除用户
	DelOnlineUsersCache(ctx context.Context, userID string) error

	// AddOfflineUsersCache 添加离线用户缓存
	AddUsersOfflineCache(ctx context.Context, userID string) error
	// CleanOfflineUsersCache 清除离线用户缓存  未来参考数据量超过2000可能堵塞 redis
	CleanOfflineUsersCache(ctx context.Context) ([]map[string]string, error)
}

type RoomCacheRedis struct {
	metaCache
	groupDB       relationtb.GroupModelInterface
	groupMemberDB relationtb.GroupMemberModelInterface
	//groupRequestDB relationtb.GroupRequestModelInterface
	expireTime time.Duration
	rcClient   *rockscache.Client
	//groupHash      GroupHash
	rdb redis.UniversalClient ////增加redis直接调用  用于房间列表
}

func NewRoomCacheRedis(
	rdb redis.UniversalClient,
	groupDB relationtb.GroupModelInterface,
	groupMemberDB relationtb.GroupMemberModelInterface,
	// groupRequestDB relationtb.GroupRequestModelInterface,
	// hashCode GroupHash,
	//
	opts rockscache.Options,
) RoomCache {
	rcClient := rockscache.NewClient(rdb, opts)
	mc := NewMetaCacheRedis(rcClient)
	g := config.Config.LocalCache.Group
	mc.SetTopic(g.Topic)
	//log.ZDebug(context.Background(), "group local cache init", "Topic", g.Topic, "SlotNum", g.SlotNum, "SlotSize", g.SlotSize, "enable", g.Enable())
	//mc.SetRawRedisClient(rdb)
	return &RoomCacheRedis{
		rcClient:      rcClient,
		expireTime:    groupExpireTime,
		groupDB:       groupDB,
		groupMemberDB: groupMemberDB, //groupRequestDB: groupRequestDB,
		//groupHash: hashCode,
		metaCache: mc,
		rdb:       rdb, ////增加redis直接调用  用于房间列表

	}

}

func (g *RoomCacheRedis) NewCache() RoomCache {

	return &RoomCacheRedis{
		rcClient: g.rcClient,
		//expireTime:    time.Second * 60,
		groupDB:       g.groupDB,
		groupMemberDB: g.groupMemberDB,
		//groupRequestDB: g.groupRequestDB,
		metaCache: g.Copy(),
		rdb:       g.rdb, ////增加redis直接调用  用于房间列表
	}
}

const (
	userRoomIDKey  = "ROOM_USER:" // 哈希键：存储用户详情
	roomOfflineKey = "ROOM_OFFLINE:"
	roomOnlineKey  = "ROOM_ONLINE:"

	OfflineTimeout = 1 * time.Minute // 离线超时时间
)

// GetRoomInfo 获取房间信息
func (g *RoomCacheRedis) GetRoomInfo(ctx context.Context, groupID string) (group *relationtb.GroupModel, err error) {
	return getCache(ctx, g.rcClient, cachekey.GetGroupInfoKey(groupID), g.expireTime, func(ctx context.Context) (*relationtb.GroupModel, error) {
		return g.groupDB.Take(ctx, groupID)
	})
}

// GetRoomMemberIDs 获取房间成员ID列表
func (g *RoomCacheRedis) GetRoomMemberIDs(ctx context.Context, groupID string) (groupMemberIDs []string, err error) {
	return getCache(ctx, g.rcClient, cachekey.GetGroupMemberIDsKey(groupID), g.expireTime, func(ctx context.Context) ([]string, error) {
		return g.groupMemberDB.FindMemberUserID(ctx, groupID)
	})
}

// GetRoomMembersInfo 获取房间成员信息列表
func (g *RoomCacheRedis) GetRoomMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error) {
	return batchGetCache2(ctx, g.rcClient, g.expireTime, userIDs, func(userID string) string {
		return cachekey.GetGroupMemberInfoKey(groupID, userID)
	}, func(ctx context.Context, userID string) (*relationtb.GroupMemberModel, error) {
		return g.groupMemberDB.Take(ctx, groupID, userID)
	})
}

// GetRoomOwner 获取房间所有者
func (g *RoomCacheRedis) GetRoomOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error) {
	members, err := g.GetRoomRoleLevelMemberInfo(ctx, groupID, constant.GroupOwner)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errs.ErrRecordNotFound.Wrap(fmt.Sprintf("group %s owner not found", groupID))
	}
	return members[0], nil
}

// GetRoomRoleLevelMemberInfo 获取房间指定角色等级的成员信息
func (g *RoomCacheRedis) GetRoomRoleLevelMemberInfo(ctx context.Context, groupID string, roleLevel int32) ([]*relationtb.GroupMemberModel, error) {
	userIDs, err := g.GetRoomRoleLevelMemberIDs(ctx, groupID, roleLevel)
	if err != nil {
		return nil, err
	}
	return g.GetRoomMembersInfo(ctx, groupID, userIDs)
}

// //============缓存操作
func (g *RoomCacheRedis) DelRoomsInfo(groupIDs ...string) RoomCache {
	newRoomCache := g.NewCache()
	keys := make([]string, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		keys = append(keys, cachekey.GetGroupInfoKey(groupID))
	}
	newRoomCache.AddKeys(keys...)

	return newRoomCache
}
func (g *RoomCacheRedis) DelRoomMembersHash(groupID string) RoomCache {
	cache := g.NewCache()
	cache.AddKeys(cachekey.GetGroupMembersHashKey(groupID))

	return cache
}
func (g *RoomCacheRedis) DelRoomMemberIDs(groupID string) RoomCache {
	cache := g.NewCache()
	cache.AddKeys(cachekey.GetGroupMemberIDsKey(groupID))

	return cache
}
func (g *RoomCacheRedis) DelJoinedRoomID(userIDs ...string) RoomCache {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, cachekey.GetJoinedGroupsKey(userID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}
func (g *RoomCacheRedis) DelRoomRoleLevel(groupID string, roleLevels []int32) RoomCache {
	newGroupCache := g.NewCache()
	keys := make([]string, 0, len(roleLevels))
	for _, roleLevel := range roleLevels {
		keys = append(keys, cachekey.GetGroupRoleLevelMemberIDsKey(groupID, roleLevel))
	}
	newGroupCache.AddKeys(keys...)
	return newGroupCache
}
func (g *RoomCacheRedis) DelRoomAllRoleLevel(groupID string) RoomCache {
	return g.DelRoomRoleLevel(groupID, []int32{constant.GroupOwner, constant.GroupAdmin, constant.GroupOrdinaryUsers})
}
func (g *RoomCacheRedis) DelRoomMembersInfo(groupID string, userIDs ...string) RoomCache {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, cachekey.GetGroupMemberInfoKey(groupID, userID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}
func (g *RoomCacheRedis) DelRoomsMemberNum(groupID ...string) RoomCache {
	keys := make([]string, 0, len(groupID))
	for _, groupID := range groupID {
		keys = append(keys, cachekey.GetGroupMemberNumKey(groupID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}

// DelayDelMaxSeq 房间解散时 延时删除房间最大序列  延时删除设置为一天
func (g *RoomCacheRedis) DelayDelOther(ctx context.Context, roomID string, uIDs ...string) error {
	delTime := time.Second * 5 //* 60 * 12
	conversation := "g_" + roomID
	maxSeqKey := "MAX_SEQ:" + conversation
	// 先处理房间最大序列的过期时间
	if err := g.rdb.Expire(ctx, maxSeqKey, delTime).Err(); err != nil {
		return err
	}
	// 若没有需要处理的用户ID，直接返回
	if len(uIDs) == 0 {
		return nil
	}

	// 使用Pipeline批量处理用户相关键的过期时间，减少网络请求
	pipe := g.rdb.Pipeline()
	for _, uid := range uIDs {
		// 拼接用户相关的Redis键（复用conversation，减少重复拼接）
		hasReadSeqKey := "HAS_READ_SEQ:" + uid + ":" + conversation
		conUserMinSeqKey := "CON_USER_MIN_SEQ:" + conversation + "u:" + uid

		// 批量添加到Pipeline
		pipe.Expire(ctx, hasReadSeqKey, delTime)
		pipe.Expire(ctx, conUserMinSeqKey, delTime)
	}

	// 执行Pipeline并处理错误
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}
func (g *RoomCacheRedis) DelUserRoomSeq(ctx context.Context, roomID string, uIDs ...string) error {
	conversation := "g_" + roomID
	var keys []string
	for _, uid := range uIDs {
		keys = append(keys, []string{
			"CON_USER_MIN_SEQ:" + conversation + "u:" + uid,
			"HAS_READ_SEQ:" + uid + ":" + conversation,
		}...)
	}

	return g.rdb.Del(ctx, keys...).Err()
}

//// DelayDelMaxSeq 延时删除房间最大序列
//func (g *RoomCacheRedis) DelayDelMaxSeq(ctx context.Context, roomID string) error {
//
//	key := "MAX_SEQ:g_" + roomID
//	return g.rdb.Expire(ctx, key, g.expireTime).Err()
//
//}

////============缓存操作结束

// //==========通知用到
// GetRoomMemberInfo 获取房间成员信息
func (g *RoomCacheRedis) GetRoomMemberInfo(ctx context.Context, groupID, userID string) (groupMember *relationtb.GroupMemberModel, err error) {
	return getCache(ctx, g.rcClient, cachekey.GetGroupMemberInfoKey(groupID, userID), g.expireTime, func(ctx context.Context) (*relationtb.GroupMemberModel, error) {
		return g.groupMemberDB.Take(ctx, groupID, userID)
	})
}

// GetRoomMemberNum 获取房间成员数量
func (g *RoomCacheRedis) GetRoomMemberNum(ctx context.Context, groupID string) (memberNum int64, err error) {
	return getCache(ctx, g.rcClient, cachekey.GetGroupMemberNumKey(groupID), g.expireTime, func(ctx context.Context) (int64, error) {
		return g.groupMemberDB.TakeGroupMemberNum(ctx, groupID)
	})
}

// GetRoomRoleLevelMemberIDs 获取房间指定角色等级的成员ID列表
func (g *RoomCacheRedis) GetRoomRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error) {
	return getCache(ctx, g.rcClient, cachekey.GetGroupRoleLevelMemberIDsKey(groupID, roleLevel), g.expireTime, func(ctx context.Context) ([]string, error) {
		return g.groupMemberDB.FindRoleLevelUserIDs(ctx, groupID, roleLevel)
	})
}

// AddRoomCache 创建房间 添加房间缓存
func (g *RoomCacheRedis) AddRoomCache(ctx context.Context, room *sdkws.RoomInfo) error {
	key := fmt.Sprintf("ROOM:%s", room.RoomID)
	ms, err := json.Marshal(room.Ms)
	fields := map[string]interface{}{
		"id":    room.RoomID,
		"uid":   room.Uid,
		"name":  room.Name,
		"img":   room.Img,
		"ms":    ms,
		"num":   0,
		"score": room.Score,
	}

	// 创建管道
	pipe := g.rdb.Pipeline()

	// 将3个命令加入管道
	pipe.HSet(ctx, key, fields)
	pipe.HSet(ctx, userRoomIDKey+room.Uid,
		"roomID", room.RoomID,
		"isOwner", true)
	pipe.ZAdd(ctx, "ROOM_LIST:", redis.Z{
		Score:  float64(room.Score),
		Member: room.RoomID,
	})

	// 执行管道中的所有命令（一次网络往返）
	_, err = pipe.Exec(ctx)
	return err
}

// DelRoomCache 在房间列表中删除房间id 删除房间信息 同时删除群组成员关联房间缓存
func (g *RoomCacheRedis) DelRoomCache(ctx context.Context, uIDs []string, roomID string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	pipe := g.rdb.Pipeline()
	//在房间列表中删除
	pipe.ZRem(ctx, "ROOM_LIST:", roomID)
	//删除 房间信息缓存
	pipe.Del(ctx, key)

	// 遍历所有用户，删除每个用户的关联房间缓存
	var key1 []string
	for _, uid := range uIDs {
		key1 = append(key1, userRoomIDKey+uid)
	}
	pipe.Del(ctx, key1...)
	// 执行管道中的所有命令（一次网络往返）
	_, err := pipe.Exec(ctx)
	return err
}

// 原子操作：退出房间，清除当前uid及其头像URL（并发安全）
func (g *RoomCacheRedis) RemoveRoomUserCache(ctx context.Context, roomID string, uid string, img string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
-- 检查房间是否存在
if redis.call("EXISTS", KEYS[1]) == 0 then
  return cjson.encode({err = "room not found"})
end

-- 直接获取指定字段，避免全量HGETALL
local ms_json = redis.call("HGET", KEYS[1], "ms") or '["","","","","","","",""]'
local imgs_json = redis.call("HGET", KEYS[1], "imgs") or "[]"

-- 解析JSON（默认空数组避免nil判断）
local ms = cjson.decode(ms_json)
local imgs = cjson.decode(imgs_json)

-- 移除ms中匹配的uid（限制前8位）
local removed = false
for i = 1, 8 do
  if ms[i] == ARGV[1] then
    ms[i] = ""
    removed = true
    break
  end
end
if not removed then
  return cjson.encode({err = "uid not in room"})
end

-- 移除imgs中匹配的url（逆向遍历）
local img_url = ARGV[2]
for i = #imgs, 1, -1 do
  if imgs[i] == img_url then
    table.remove(imgs, i)
    break
  end
end

-- 批量更新字段，减少Redis调用
redis.call("HMSET", KEYS[1], "ms", cjson.encode(ms), "imgs", cjson.encode(imgs))

-- 返回结果
return cjson.encode({ms = ms, imgs = imgs})
`
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, uid, img).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		// 处理 room not found 或 uid not in room 错误
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

func (g *RoomCacheRedis) KickRemoveRoomUserCache(ctx context.Context, roomID string, uid string, img string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
-- 检查房间是否存在
if redis.call("EXISTS", KEYS[1]) == 0 then
 return cjson.encode({err = "room not found"})
end

-- 直接获取指定字段，避免全量HGETALL
local ms_json = redis.call("HGET", KEYS[1], "ms") or '["","","","","","","",""]'
local imgs_json = redis.call("HGET", KEYS[1], "imgs") or "[]"

-- 解析JSON（默认空数组避免nil判断）
local ms = cjson.decode(ms_json)
local imgs = cjson.decode(imgs_json)

-- 移除ms中匹配的uid（限制前8位）
local removed = false
for i = 1, 8 do
 if ms[i] == ARGV[1] then
   ms[i] = "0"
   removed = true
   break
 end
end
if not removed then
 return cjson.encode({err = "uid not in room"})
end

-- 移除imgs中匹配的url（逆向遍历）
local img_url = ARGV[2]
for i = #imgs, 1, -1 do
 if imgs[i] == img_url then
   table.remove(imgs, i)
   break
 end
end

-- 批量更新字段，减少Redis调用
redis.call("HMSET", KEYS[1], "ms", cjson.encode(ms), "imgs", cjson.encode(imgs))

-- 返回结果
return cjson.encode({ms = ms, imgs = imgs})
`
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, uid, img).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		// 处理 room not found 或 uid not in room 错误
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

// 原子操作：加入房间 并分配位置（并发安全）
func (g *RoomCacheRedis) UpdateRoomUserCache(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error) {

	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end

local ms_json = redis.call("HGET", KEYS[1], "ms") or '["","","","","","","",""]'  -- 默认为8个空座位
local imgs_json = redis.call("HGET", KEYS[1], "imgs") or "[]"


-- 2. 解析JSON并处理ms数组（填充空座位）
local ms = cjson.decode(ms_json)
local idx = nil
-- 遍历前8个座位，找到第一个空座位（兼容nil或空字符串）
for i = 1, 8 do
    if not ms[i] or ms[i] == "" then
        ms[i] = ARGV[1]  -- ARGV[1]是要添加的uid
        idx = i
        break
    end
end
if not idx then
    return {err="No empty seat"}
end

-- 3. 处理imgs数组（头部插入）
local imgs = cjson.decode(imgs_json)
table.insert(imgs, 1, ARGV[2])  -- ARGV[2]是要添加的图片url

redis.call("HSET", KEYS[1], "ms", cjson.encode(ms),"imgs", cjson.encode(imgs))


local name = redis.call("HGET", KEYS[1], "name")
local img = redis.call("HGET", KEYS[1], "img")
-- num字段自增
local num = redis.call("HINCRBY", KEYS[1], "num", 1)
local score = redis.call("HGET", KEYS[1], "score")

return cjson.encode({name=name, img=img, ms=ms, num=num, imgs=imgs, score=score})
`
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, uid, img).Result()

	resp := sdkws.RoomInfo{}
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return nil, err
		}
		// 处理 room not found 或 No empty seat 错误
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return nil, fmt.Errorf(errStr)
		}

		//num, _ := strconv.Atoi(result["mum"].(string))
		score, _ := strconv.Atoi(result["score"].(string))
		num, _ := strconv.ParseInt(fmt.Sprintf("%v", result["num"]), 10, 64)

		var ms []string
		if msArr, ok := result["ms"].([]interface{}); ok {
			for _, v := range msArr {
				ms = append(ms, fmt.Sprintf("%v", v))
			}
		}
		var imgs []string
		if imgsArr, ok := result["imgs"].([]interface{}); ok {
			for _, v := range imgsArr {
				imgs = append(imgs, fmt.Sprintf("%v", v))
			}
		}
		resp = sdkws.RoomInfo{
			RoomID: roomID,
			Uid:    uid,
			Name:   result["name"].(string),
			Img:    result["img"].(string),
			Ms:     ms,
			Imgs:   imgs,
			Num:    num,
			Score:  int64(score),
		}

	} else {
		return nil, fmt.Errorf("unexpected lua result: %v", res)
	}
	return &resp, nil

}

// GetRoomListCache 获取房间列表，按积分倒序分页
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

// 获取当前房间信息（包括成员、头像等）
func (g *RoomCacheRedis) GetRoomInfoCache(ctx context.Context, roomID string) (*sdkws.RoomInfo, error) {
	key := fmt.Sprintf("ROOM:%s", roomID)
	// 获取哈希所有字段
	data, err := g.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("room not found")
	}

	//num, _ := strconv.Atoi(data["mum"])
	score, _ := strconv.ParseInt(data["score"], 10, 64)
	num, _ := strconv.ParseInt(fmt.Sprintf("%v", data["num"]), 10, 64)
	// 解析 ms 字段
	var ms []string
	if msJson, ok := data["ms"]; ok && msJson != "" {
		_ = json.Unmarshal([]byte(msJson), &ms)
	}
	// 解析 imgs 字段
	var imgs []string
	if imgsJson, ok := data["imgs"]; ok && imgsJson != "" {
		_ = json.Unmarshal([]byte(imgsJson), &imgs)
	}

	info := &sdkws.RoomInfo{
		RoomID: data["id"],
		Uid:    data["uid"],
		Name:   data["name"],
		Img:    data["img"],
		Ms:     ms,
		Imgs:   imgs,
		Num:    num,
		Score:  score,
	}
	return info, nil
}

// UpdateUserRoomCache 更新用户 关联 房间缓存
func (g *RoomCacheRedis) AddUserRoomCache(ctx context.Context, uid string, roomID string, isOwner bool) error {
	// 更新用户房间信息
	key := userRoomIDKey + uid
	err := g.rdb.HSet(ctx, key, "roomID", roomID, "isOwner", isOwner).Err()
	return err
}

// GetUserRoomCache 获取用户 关联 房间缓存
func (g *RoomCacheRedis) GetUserRoomCache(ctx context.Context, userID string) (roomID string, isOwner bool, err error) {
	result, err := g.rdb.HGetAll(ctx, userRoomIDKey+userID).Result()
	if err != nil {
		return "", false, errs.Wrap(err, "redis set failed")
	}
	return result["roomID"], result["isOwner"] == "1", nil

}

// GetRoomUserRoomIDCache 获取用户 关联 房间ID
func (g *RoomCacheRedis) GetRoomUserRoomIDCache(ctx context.Context, userID string) (string, error) {
	// 设置 key 并指定 1 小时后过期
	result, err := g.rdb.HGet(ctx, userRoomIDKey+userID, "roomID").Result()
	if err != nil {
		return "", errs.Wrap(err, "redis set failed")
	}
	return result, nil
}

// DelUserRoomCache 删除用户 关联 房间缓存
func (g *RoomCacheRedis) DelUserRoomCache(ctx context.Context, uid string) error {
	// 更新用户房间信息
	key := userRoomIDKey + uid
	err := g.rdb.Del(ctx, key).Err()
	return err
}

//// BatchDelUserRoomCache 批量删除用户 关联 房间缓存
//func (g *RoomCacheRedis) BatchDelUserRoomCache(ctx context.Context, userIDs []string) error {
//	// 创建一个 pipeline
//	pipe := g.rdb.Pipeline()
//	for _, userID := range userIDs {
//		// 为每个用户的哈希表设置字段值，加入到 pipeline 中
//		key := userRoomIDKey + userID
//		//pipe.ZRem(ctx, roomOfflineKey, userID)
//		pipe.Del(ctx, key)
//	}
//	// 执行 pipeline 中的所有命令
//	_, err := pipe.Exec(ctx)
//	if err != nil {
//		return errs.Wrap(err, "redis pipeline exec failed")
//	}
//	return nil
//}

func (g *RoomCacheRedis) AddOnlineUsersCache(ctx context.Context, userID string) error {
	return g.rdb.SAdd(ctx, roomOnlineKey, userID).Err()
}

func (g *RoomCacheRedis) DelOnlineUsersCache(ctx context.Context, userID string) error {
	return g.rdb.SRem(ctx, roomOnlineKey, userID).Err()
}

// AddUsersOfflineCache 在在线列表中清除 判断用户是否有房间信息 如果有 则添加到离线列表
func (g *RoomCacheRedis) AddUsersOfflineCache(ctx context.Context, userID string) error {
	userRoomKey := userRoomIDKey + userID

	// 关键优化：用一个 Pipeline 执行 SRem 和 Exists（无依赖关系，可合并）
	pipe := g.rdb.Pipeline()
	// 1. 移除在线用户
	pipe.SRem(ctx, roomOnlineKey, userID)
	// 2. 检查用户房间信息键是否存在
	existsCmd := pipe.Exists(ctx, userRoomKey)
	// 执行 Pipeline（仅 1 次网络交互，1 次连接操作）
	_, err := pipe.Exec(ctx)
	if err != nil {
		return errs.Wrap(err, "pipeline exec failed")
	}
	// 处理 Exists 结果
	exists, err := existsCmd.Result()
	if err != nil {
		return errs.Wrap(err, "redis Exists failed")
	}

	// 3. 若存在，单独执行 ZAdd（依赖 exists 结果，无法合并到上面的 Pipeline）
	if exists == 1 {
		//添加用户到离线列表
		err := g.rdb.ZAdd(ctx, roomOfflineKey, redis.Z{
			Score:  float64(time.Now().UnixMilli()),
			Member: userID,
		}).Err()
		if err != nil {
			return errs.Wrap(err, "redis ZAdd offline failed")
		}
	}
	return nil
}

// CleanOfflineUsersCache 清除离线用户缓存  未来参考数据量超过2000可能堵塞 redis
func (g *RoomCacheRedis) CleanOfflineUsersCache(ctx context.Context) ([]map[string]string, error) {
	// 计算过期时间戳（毫秒级）
	expireTime := time.Now().Add(-OfflineTimeout).UnixMilli()
	members, err := g.rdb.ZRangeByScore(ctx, roomOfflineKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("(%d", expireTime)}).Result()
	if err != nil {
		return nil, errs.Wrap(err, "redis ZRange by score failed")
	}
	if len(members) == 0 {
		return nil, nil
	}
	var userKeys []string
	pipe := g.rdb.Pipeline()
	cmds := map[string]*redis.MapStringStringCmd{}
	for _, uid := range members {
		cmds[uid] = pipe.HGetAll(ctx, userRoomIDKey+uid)
		userKeys = append(userKeys, userRoomIDKey+uid)
	}
	pipe.Del(ctx, userKeys...)
	// 执行批量查询
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, errs.Wrap(err, "redis pipeline HGetAll failed")
	}

	//keys1 := []string{}

	var data []map[string]string
	for uid, r := range cmds {
		result := r.Val()
		if result["roomID"] != "" {
			maps := map[string]string{}
			maps["userID"] = uid
			maps["roomID"] = result["roomID"]
			maps["isOwner"] = result["isOwner"]
			data = append(data, maps)
		}
		//else {
		//	keys1 = append(keys1, userRoomIDKey+uid)
		//}
	}
	//// 批量删除用户 关联 房间缓存
	//err = g.rdb.Del(ctx, keys1...).Err()
	//if err != nil {
	//	return nil, errs.Wrap(err, "redis pipeline Del failed")
	//}

	return data, nil
}
