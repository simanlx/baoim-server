// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"BaoIM-Server/pkg/common/cachekey"
	"BaoIM-Server/pkg/common/config"
	"baoim/protocol/sdkws"
	"baoim/tools/mcontext"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/constant"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/utils"
	"github.com/dtm-labs/rockscache"
	"github.com/redis/go-redis/v9"
)

const (
	groupExpireTime = time.Second * 60 * 60 * 12
	//groupInfoKey        = "GROUP_INFO:"
	//groupMemberIDsKey   = "GROUP_MEMBER_IDS:"
	//groupMembersHashKey = "GROUP_MEMBERS_HASH2:"
	//groupMemberInfoKey  = "GROUP_MEMBER_INFO:"
	//joinedGroupsKey            = "JOIN_GROUPS_KEY:"
	//groupMemberNumKey          = "GROUP_MEMBER_NUM_CACHE:"
	//groupRoleLevelMemberIDsKey = "GROUP_ROLE_LEVEL_MEMBER_IDS:"
)

type GroupHash interface {
	GetGroupHash(ctx context.Context, groupID string) (uint64, error)
}

type GroupCache interface {
	metaCache
	NewCache() GroupCache
	GetGroupsInfo(ctx context.Context, groupIDs []string) (groups []*relationtb.GroupModel, err error)
	GetGroupInfo(ctx context.Context, groupID string) (group *relationtb.GroupModel, err error)
	DelGroupsInfo(groupIDs ...string) GroupCache

	GetGroupMembersHash(ctx context.Context, groupID string) (hashCode uint64, err error)
	GetGroupMemberHashMap(ctx context.Context, groupIDs []string) (map[string]*relationtb.GroupSimpleUserID, error)
	DelGroupMembersHash(groupID string) GroupCache

	GetGroupMemberIDs(ctx context.Context, groupID string) (groupMemberIDs []string, err error)
	GetGroupsMemberIDs(ctx context.Context, groupIDs []string) (groupMemberIDs map[string][]string, err error)

	DelGroupMemberIDs(groupID string) GroupCache

	GetJoinedGroupIDs(ctx context.Context, userID string) (joinedGroupIDs []string, err error)
	DelJoinedGroupID(userID ...string) GroupCache

	GetGroupMemberInfo(ctx context.Context, groupID, userID string) (groupMember *relationtb.GroupMemberModel, err error)
	GetGroupMembersInfo(ctx context.Context, groupID string, userID []string) (groupMembers []*relationtb.GroupMemberModel, err error)
	GetAllGroupMembersInfo(ctx context.Context, groupID string) (groupMembers []*relationtb.GroupMemberModel, err error)
	GetGroupMembersPage(ctx context.Context, groupID string, userID []string, showNumber, pageNumber int32) (total uint32, groupMembers []*relationtb.GroupMemberModel, err error)
	FindGroupMemberUser(ctx context.Context, groupIDs []string, userID string) ([]*relationtb.GroupMemberModel, error)

	GetGroupRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error)
	GetGroupOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error)
	GetGroupsOwner(ctx context.Context, groupIDs []string) ([]*relationtb.GroupMemberModel, error)
	DelGroupRoleLevel(groupID string, roleLevel []int32) GroupCache
	DelGroupAllRoleLevel(groupID string) GroupCache
	DelGroupMembersInfo(groupID string, userID ...string) GroupCache
	GetGroupRoleLevelMemberInfo(ctx context.Context, groupID string, roleLevel int32) ([]*relationtb.GroupMemberModel, error)
	GetGroupRolesLevelMemberInfo(ctx context.Context, groupID string, roleLevels []int32) ([]*relationtb.GroupMemberModel, error)
	GetGroupMemberNum(ctx context.Context, groupID string) (memberNum int64, err error)
	DelGroupsMemberNum(groupID ...string) GroupCache

	//创建聊天室接口
	AddRoomCache(ctx context.Context, room *sdkws.RoomInfo) error

	///加入房间
	UpdateRoomCache(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error)
	//退出房间
	RemoveRoomCache(ctx context.Context, roomID string, uid string, img string) error
	//更新房间积分
	UpdateRoomScore(ctx context.Context, roomID string, score int64) error

	// 原子操作：解散房间，清除redis缓存、从积分集合移除
	DismissRoomCache(ctx context.Context, roomID string) error
	// 原子操作：剔出房间成员，将房间位置替换为"0"（禁止站位）
	KickRoomMemberCache(ctx context.Context, roomID string, uid string, img string) error
	// 原子操作：关闭指定位置，将指定位置替换为"0"（禁止站位）
	CloseRoomSeatCache(ctx context.Context, roomID string, idx int) error
	// 原子操作：打开房间，将指定位置的"0"替换为""（允许站位）
	OpenRoomSeatCache(ctx context.Context, roomID string, idx int) error
	// 原子操作：打开房间，将所有"0"位置替换为""（允许站位）
	OpenRoomAllCache(ctx context.Context, roomID string) error
	// 原子操作：更换房间成员位置（支持头像同步移动）
	MoveRoomMemberCache(ctx context.Context, roomID string, fromIdx int, toIdx int) error
}

type GroupCacheRedis struct {
	metaCache
	groupDB        relationtb.GroupModelInterface
	groupMemberDB  relationtb.GroupMemberModelInterface
	groupRequestDB relationtb.GroupRequestModelInterface
	expireTime     time.Duration
	rcClient       *rockscache.Client
	groupHash      GroupHash
	rdb            redis.UniversalClient ////增加redis直接调用  用于房间列表
}

func NewGroupCacheRedis(
	rdb redis.UniversalClient,
	groupDB relationtb.GroupModelInterface,
	groupMemberDB relationtb.GroupMemberModelInterface,
	groupRequestDB relationtb.GroupRequestModelInterface,
	hashCode GroupHash,
	opts rockscache.Options,
) GroupCache {
	rcClient := rockscache.NewClient(rdb, opts)
	mc := NewMetaCacheRedis(rcClient)
	g := config.Config.LocalCache.Group
	mc.SetTopic(g.Topic)
	log.ZDebug(context.Background(), "group local cache init", "Topic", g.Topic, "SlotNum", g.SlotNum, "SlotSize", g.SlotSize, "enable", g.Enable())
	mc.SetRawRedisClient(rdb)
	return &GroupCacheRedis{
		rcClient: rcClient, expireTime: groupExpireTime,
		groupDB: groupDB, groupMemberDB: groupMemberDB, groupRequestDB: groupRequestDB,
		groupHash: hashCode,
		metaCache: mc,
		rdb:       rdb, ////增加redis直接调用  用于房间列表

	}
}

func (g *GroupCacheRedis) NewCache() GroupCache {

	return &GroupCacheRedis{
		rcClient:       g.rcClient,
		expireTime:     g.expireTime,
		groupDB:        g.groupDB,
		groupMemberDB:  g.groupMemberDB,
		groupRequestDB: g.groupRequestDB,
		metaCache:      g.Copy(),
	}
}

// 房间信息哈希 key: room:{roomID}
func (g *GroupCacheRedis) AddRoomCache(ctx context.Context, room *sdkws.RoomInfo) error {
	key := fmt.Sprintf("ROOM:%s", room.RoomID)
	ms, err := json.Marshal(room.Ms)
	fields := map[string]interface{}{
		"id":   room.RoomID,
		"uid":  room.Uid,
		"name": room.Name,
		"img":  room.Img,
		"ms":   ms,
		//"imgs":"",
		"num":   0,
		"score": room.Score,
	}
	// 保存聊天室哈希
	err = g.rdb.HSet(ctx, key, fields).Err()
	if err != nil {
		return err
	}

	// 房间ID加入积分排序集合
	err = g.rdb.ZAdd(ctx, "ROOM_LIST:", redis.Z{
		Score:  float64(room.Score),
		Member: room.RoomID,
	}).Err()
	return err
}

// 原子操作：解散房间，清除redis缓存、从积分集合移除
func (g *GroupCacheRedis) DismissRoomCache(ctx context.Context, roomID string) error {
	println("操作者id是44", mcontext.GetOpUserID(ctx))

	key := fmt.Sprintf("ROOM:%s", roomID)
	// 删除房间哈希
	err := g.rdb.Del(ctx, key).Err()
	if err != nil {
		return err
	}
	// 从积分排序集合移除
	err = g.rdb.ZRem(ctx, "ROOM_LIST:", roomID).Err()
	if err != nil {
		return err
	}
	return nil
}

// 原子操作：加入房间并分配位置（并发安全）
func (g *GroupCacheRedis) UpdateRoomCache(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error) {

	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local data = redis.call("HGETALL", KEYS[1])
local ms_json = nil
local imgs_json = nil
for i=1,#data,2 do
  if data[i] == "ms" then ms_json = data[i+1] end
  if data[i] == "imgs" then imgs_json = data[i+1] end
end
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local idx = nil
for i=1,8 do
  if ms[i] == "" or ms[i] == nil then
    ms[i] = ARGV[1]
    idx = i
    break
  end
end
if not idx then
  return {err="No empty seat"}
end
local imgs = {}
if imgs_json then imgs = cjson.decode(imgs_json) end
table.insert(imgs, 1, ARGV[2])
if #imgs > 5 then
  while #imgs > 5 do table.remove(imgs) end
end
redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
redis.call("HSET", KEYS[1], "imgs", cjson.encode(imgs))

local id = redis.call("HGET", KEYS[1], "id")
local uid = redis.call("HGET", KEYS[1], "uid")
local name = redis.call("HGET", KEYS[1], "name")
local img = redis.call("HGET", KEYS[1], "img")
-- num字段自增
local num = redis.call("HINCRBY", KEYS[1], "num", 1)

local score = redis.call("HGET", KEYS[1], "score")

return cjson.encode({id=id, uid=uid,name=name, img=img, uid=uid, score=score, ms=ms, num=num, imgs=imgs, idx=idx})
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
			RoomID: result["id"].(string),
			Uid:    result["uid"].(string),
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

// 原子操作：退出房间，清除当前uid及其头像URL（并发安全）
func (g *GroupCacheRedis) RemoveRoomCache(ctx context.Context, roomID string, uid string, img string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local data = redis.call("HGETALL", KEYS[1])
local ms_json = nil
local imgs_json = nil
for i=1,#data,2 do
  if data[i] == "ms" then ms_json = data[i+1] end
  if data[i] == "imgs" then imgs_json = data[i+1] end
end
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local removed = false
for i=1,8 do
  if ms[i] == ARGV[1] then
    ms[i] = ""
    removed = true
    break
  end
end
if not removed then
  return {err="uid not in room"}
end
local imgs = {}
if imgs_json then imgs = cjson.decode(imgs_json) end
local img_url = ARGV[2]
for i=#imgs,1,-1 do
  if imgs[i] == img_url then
    table.remove(imgs, i)
    break
  end
end
redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
redis.call("HSET", KEYS[1], "imgs", cjson.encode(imgs))
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

// 原子操作：剔出房间成员，将房间位置替换为"0"，并删除对应头像
func (g *GroupCacheRedis) KickRoomMemberCache(ctx context.Context, roomID string, uid string, img string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local data = redis.call("HGETALL", KEYS[1])
local ms_json = nil
local imgs_json = nil
for i=1,#data,2 do
  if data[i] == "ms" then ms_json = data[i+1] end
  if data[i] == "imgs" then imgs_json = data[i+1] end
end
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local removed = false
for i=1,8 do
  if ms[i] == ARGV[1] then
    ms[i] = "0"
    removed = true
    break
  end
end
if not removed then
  return {err="uid not in room"}
end
local imgs = {}
if imgs_json then imgs = cjson.decode(imgs_json) end
local img_url = ARGV[2]
for i=#imgs,1,-1 do
  if imgs[i] == img_url then
    table.remove(imgs, i)
    break
  end
end
redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
redis.call("HSET", KEYS[1], "imgs", cjson.encode(imgs))
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

// 原子操作：打开房间，将指定位置的"0"替换为""（允许站位）
func (g *GroupCacheRedis) OpenRoomSeatCache(ctx context.Context, roomID string, idx int) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local ms_json = redis.call("HGET", KEYS[1], "ms")
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local pos = tonumber(ARGV[1])
if not pos or pos < 1 or pos > 8 then
  return {err="invalid index"}
end
if ms[pos] == "0" then
  ms[pos] = ""
  redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
  return cjson.encode({ms = ms, msg="seat opened"})
else
  return {msg="seat not forbidden"}
end
`
	// Lua 下标从1开始，Go下标从0开始，需要+1
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, idx+1).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

// 原子操作：关闭指定位置，将指定位置替换为"0"（禁止站位）
func (g *GroupCacheRedis) CloseRoomSeatCache(ctx context.Context, roomID string, idx int) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local ms_json = redis.call("HGET", KEYS[1], "ms")
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local pos = tonumber(ARGV[1])
if not pos or pos < 1 or pos > 8 then
  return {err="invalid index"}
end
ms[pos] = "0"
redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
return cjson.encode({ms = ms, msg="seat closed"})
`
	// Lua 下标从1开始，Go下标从0开始，需要+1
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, idx+1).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

// 原子操作：打开房间，将所有"0"位置替换为""（允许站位）
func (g *GroupCacheRedis) OpenRoomAllCache(ctx context.Context, roomID string) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local ms_json = redis.call("HGET", KEYS[1], "ms")
local ms = {}
if ms_json then ms = cjson.decode(ms_json) end
local changed = false
for i=1,8 do
  if ms[i] == "0" then
    ms[i] = ""
    changed = true
  end
end
if not changed then
  return {msg="no forbidden seats"}
end
redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
return cjson.encode({ms = ms})
`
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

// 原子操作：更换房间成员位置（支持头像同步移动）
// fromIdx: 当前成员位置（Go下标，0~7），toIdx: 目标位置（Go下标，0~7）
func (g *GroupCacheRedis) MoveRoomMemberCache(ctx context.Context, roomID string, fromIdx int, toIdx int) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	luaScript := `
if redis.call("EXISTS", KEYS[1]) == 0 then
  return {err="room not found"}
end
local ms_json = redis.call("HGET", KEYS[1], "ms")
local imgs_json = redis.call("HGET", KEYS[1], "imgs")
local ms = {}
local imgs = {}
if ms_json then ms = cjson.decode(ms_json) end
if imgs_json then imgs = cjson.decode(imgs_json) end

local from_pos = tonumber(ARGV[1])
local to_pos = tonumber(ARGV[2])

if not from_pos or from_pos < 1 or from_pos > 8 then
  return {err="invalid from index"}
end
if not to_pos or to_pos < 1 or to_pos > 8 then
  return {err="invalid to index"}
end

local uid = ms[from_pos]
if uid == nil or uid == "" or uid == "0" then
  return {err="no member at from index"}
end
if ms[to_pos] ~= "" and ms[to_pos] ~= nil and ms[to_pos] ~= "0" then
  return {err="target position already occupied"}
end

-- 移动成员
ms[from_pos] = ""
ms[to_pos] = uid

-- 同步移动头像（如有头像则一起移动，否则跳过）
if imgs[from_pos] ~= nil then
  imgs[to_pos] = imgs[from_pos]
  imgs[from_pos] = nil
end

-- 整理头像数组，移除nil（保持原有顺序和长度）
local new_imgs = {}
for i=1,8 do
  if imgs[i] ~= nil then
    new_imgs[i] = imgs[i]
  else
    new_imgs[i] = ""
  end
end

redis.call("HSET", KEYS[1], "ms", cjson.encode(ms))
redis.call("HSET", KEYS[1], "imgs", cjson.encode(new_imgs))
return cjson.encode({ms = ms, imgs = new_imgs})
`
	// Lua 下标从1开始，Go下标从0开始，需要+1
	res, err := g.rdb.Eval(ctx, luaScript, []string{key}, fromIdx+1, toIdx+1).Result()
	if err != nil {
		return err
	}
	result := make(map[string]interface{})
	if str, ok := res.(string); ok {
		err := json.Unmarshal([]byte(str), &result)
		if err != nil {
			return err
		}
		if errStr, ok := result["err"].(string); ok && errStr != "" {
			return fmt.Errorf(errStr)
		}
	} else {
		return fmt.Errorf("unexpected lua result: %v", res)
	}
	return nil
}

// 更新房间积分（同时更新哈希和有序集合，保证房间列表排序正确）
func (g *GroupCacheRedis) UpdateRoomScore(ctx context.Context, roomID string, score int64) error {
	key := fmt.Sprintf("ROOM:%s", roomID)
	// 1. 更新分数到房间哈希
	err := g.rdb.HSet(ctx, key, "score", score).Err()
	if err != nil {
		return err
	}
	// 2. 更新分数到房间列表有序集合
	err = g.rdb.ZAdd(ctx, "ROOM_LIST:", redis.Z{
		Score:  float64(score),
		Member: roomID,
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (g *GroupCacheRedis) getGroupInfoKey(groupID string) string {
	return cachekey.GetGroupInfoKey(groupID)
}

func (g *GroupCacheRedis) getJoinedGroupsKey(userID string) string {
	return cachekey.GetJoinedGroupsKey(userID)
}

func (g *GroupCacheRedis) getGroupMembersHashKey(groupID string) string {
	return cachekey.GetGroupMembersHashKey(groupID)
}

func (g *GroupCacheRedis) getGroupMemberIDsKey(groupID string) string {
	return cachekey.GetGroupMemberIDsKey(groupID)
}

func (g *GroupCacheRedis) getGroupMemberInfoKey(groupID, userID string) string {
	return cachekey.GetGroupMemberInfoKey(groupID, userID)
}

func (g *GroupCacheRedis) getGroupMemberNumKey(groupID string) string {
	return cachekey.GetGroupMemberNumKey(groupID)
}

func (g *GroupCacheRedis) getGroupRoleLevelMemberIDsKey(groupID string, roleLevel int32) string {
	return cachekey.GetGroupRoleLevelMemberIDsKey(groupID, roleLevel)
}

func (g *GroupCacheRedis) GetGroupIndex(group *relationtb.GroupModel, keys []string) (int, error) {
	key := g.getGroupInfoKey(group.GroupID)
	for i, _key := range keys {
		if _key == key {
			return i, nil
		}
	}

	return 0, errIndex
}

func (g *GroupCacheRedis) GetGroupMemberIndex(groupMember *relationtb.GroupMemberModel, keys []string) (int, error) {
	key := g.getGroupMemberInfoKey(groupMember.GroupID, groupMember.UserID)
	for i, _key := range keys {
		if _key == key {
			return i, nil
		}
	}

	return 0, errIndex
}

func (g *GroupCacheRedis) GetGroupsInfo(ctx context.Context, groupIDs []string) (groups []*relationtb.GroupModel, err error) {
	return batchGetCache2(ctx, g.rcClient, g.expireTime, groupIDs, func(groupID string) string {
		return g.getGroupInfoKey(groupID)
	}, func(ctx context.Context, groupID string) (*relationtb.GroupModel, error) {
		return g.groupDB.Take(ctx, groupID)
	})
}

func (g *GroupCacheRedis) GetGroupInfo(ctx context.Context, groupID string) (group *relationtb.GroupModel, err error) {
	return getCache(ctx, g.rcClient, g.getGroupInfoKey(groupID), g.expireTime, func(ctx context.Context) (*relationtb.GroupModel, error) {
		return g.groupDB.Take(ctx, groupID)
	})
}

func (g *GroupCacheRedis) DelGroupsInfo(groupIDs ...string) GroupCache {
	newGroupCache := g.NewCache()
	keys := make([]string, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		keys = append(keys, g.getGroupInfoKey(groupID))
	}
	newGroupCache.AddKeys(keys...)

	return newGroupCache
}

func (g *GroupCacheRedis) DelGroupsOwner(groupIDs ...string) GroupCache {
	newGroupCache := g.NewCache()
	keys := make([]string, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		keys = append(keys, g.getGroupRoleLevelMemberIDsKey(groupID, constant.GroupOwner))
	}
	newGroupCache.AddKeys(keys...)

	return newGroupCache
}

func (g *GroupCacheRedis) DelGroupRoleLevel(groupID string, roleLevels []int32) GroupCache {
	newGroupCache := g.NewCache()
	keys := make([]string, 0, len(roleLevels))
	for _, roleLevel := range roleLevels {
		keys = append(keys, g.getGroupRoleLevelMemberIDsKey(groupID, roleLevel))
	}
	newGroupCache.AddKeys(keys...)
	return newGroupCache
}

func (g *GroupCacheRedis) DelGroupAllRoleLevel(groupID string) GroupCache {
	return g.DelGroupRoleLevel(groupID, []int32{constant.GroupOwner, constant.GroupAdmin, constant.GroupOrdinaryUsers})
}

func (g *GroupCacheRedis) GetGroupMembersHash(ctx context.Context, groupID string) (hashCode uint64, err error) {
	if g.groupHash == nil {
		return 0, errs.ErrInternalServer.Wrap("group hash is nil")
	}
	return getCache(ctx, g.rcClient, g.getGroupMembersHashKey(groupID), g.expireTime, func(ctx context.Context) (uint64, error) {
		return g.groupHash.GetGroupHash(ctx, groupID)
	})
}

func (g *GroupCacheRedis) GetGroupMemberHashMap(ctx context.Context, groupIDs []string) (map[string]*relationtb.GroupSimpleUserID, error) {
	if g.groupHash == nil {
		return nil, errs.ErrInternalServer.Wrap("group hash is nil")
	}
	res := make(map[string]*relationtb.GroupSimpleUserID)
	for _, groupID := range groupIDs {
		hash, err := g.GetGroupMembersHash(ctx, groupID)
		if err != nil {
			return nil, err
		}
		log.ZInfo(ctx, "GetGroupMemberHashMap", "groupID", groupID, "hash", hash)
		num, err := g.GetGroupMemberNum(ctx, groupID)
		if err != nil {
			return nil, err
		}
		res[groupID] = &relationtb.GroupSimpleUserID{Hash: hash, MemberNum: uint32(num)}
	}

	return res, nil
}

func (g *GroupCacheRedis) DelGroupMembersHash(groupID string) GroupCache {
	cache := g.NewCache()
	cache.AddKeys(g.getGroupMembersHashKey(groupID))

	return cache
}

func (g *GroupCacheRedis) GetGroupMemberIDs(ctx context.Context, groupID string) (groupMemberIDs []string, err error) {
	return getCache(ctx, g.rcClient, g.getGroupMemberIDsKey(groupID), g.expireTime, func(ctx context.Context) ([]string, error) {
		return g.groupMemberDB.FindMemberUserID(ctx, groupID)
	})
}

func (g *GroupCacheRedis) GetGroupsMemberIDs(ctx context.Context, groupIDs []string) (map[string][]string, error) {
	m := make(map[string][]string)
	for _, groupID := range groupIDs {
		userIDs, err := g.GetGroupMemberIDs(ctx, groupID)
		if err != nil {
			return nil, err
		}
		m[groupID] = userIDs
	}

	return m, nil
}

func (g *GroupCacheRedis) DelGroupMemberIDs(groupID string) GroupCache {
	cache := g.NewCache()
	cache.AddKeys(g.getGroupMemberIDsKey(groupID))

	return cache
}

func (g *GroupCacheRedis) GetJoinedGroupIDs(ctx context.Context, userID string) (joinedGroupIDs []string, err error) {
	return getCache(ctx, g.rcClient, g.getJoinedGroupsKey(userID), g.expireTime, func(ctx context.Context) ([]string, error) {
		return g.groupMemberDB.FindUserJoinedGroupID(ctx, userID)
	})
}

func (g *GroupCacheRedis) DelJoinedGroupID(userIDs ...string) GroupCache {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, g.getJoinedGroupsKey(userID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}

func (g *GroupCacheRedis) GetGroupMemberInfo(ctx context.Context, groupID, userID string) (groupMember *relationtb.GroupMemberModel, err error) {
	return getCache(ctx, g.rcClient, g.getGroupMemberInfoKey(groupID, userID), g.expireTime, func(ctx context.Context) (*relationtb.GroupMemberModel, error) {
		return g.groupMemberDB.Take(ctx, groupID, userID)
	})
}

func (g *GroupCacheRedis) GetGroupMembersInfo(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error) {
	return batchGetCache2(ctx, g.rcClient, g.expireTime, userIDs, func(userID string) string {
		return g.getGroupMemberInfoKey(groupID, userID)
	}, func(ctx context.Context, userID string) (*relationtb.GroupMemberModel, error) {
		return g.groupMemberDB.Take(ctx, groupID, userID)
	})
}

func (g *GroupCacheRedis) GetGroupMembersPage(
	ctx context.Context,
	groupID string,
	userIDs []string,
	showNumber, pageNumber int32,
) (total uint32, groupMembers []*relationtb.GroupMemberModel, err error) {
	groupMemberIDs, err := g.GetGroupMemberIDs(ctx, groupID)
	if err != nil {
		return 0, nil, err
	}
	if userIDs != nil {
		userIDs = utils.BothExist(userIDs, groupMemberIDs)
	} else {
		userIDs = groupMemberIDs
	}
	groupMembers, err = g.GetGroupMembersInfo(ctx, groupID, utils.Paginate(userIDs, int(showNumber), int(showNumber)))

	return uint32(len(userIDs)), groupMembers, err
}

func (g *GroupCacheRedis) GetAllGroupMembersInfo(ctx context.Context, groupID string) (groupMembers []*relationtb.GroupMemberModel, err error) {
	groupMemberIDs, err := g.GetGroupMemberIDs(ctx, groupID)
	if err != nil {
		return nil, err
	}

	return g.GetGroupMembersInfo(ctx, groupID, groupMemberIDs)
}

func (g *GroupCacheRedis) GetAllGroupMemberInfo(ctx context.Context, groupID string) ([]*relationtb.GroupMemberModel, error) {
	groupMemberIDs, err := g.GetGroupMemberIDs(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return g.GetGroupMembersInfo(ctx, groupID, groupMemberIDs)
}

func (g *GroupCacheRedis) DelGroupMembersInfo(groupID string, userIDs ...string) GroupCache {
	keys := make([]string, 0, len(userIDs))
	for _, userID := range userIDs {
		keys = append(keys, g.getGroupMemberInfoKey(groupID, userID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}

func (g *GroupCacheRedis) GetGroupMemberNum(ctx context.Context, groupID string) (memberNum int64, err error) {
	return getCache(ctx, g.rcClient, g.getGroupMemberNumKey(groupID), g.expireTime, func(ctx context.Context) (int64, error) {
		return g.groupMemberDB.TakeGroupMemberNum(ctx, groupID)
	})
}

func (g *GroupCacheRedis) DelGroupsMemberNum(groupID ...string) GroupCache {
	keys := make([]string, 0, len(groupID))
	for _, groupID := range groupID {
		keys = append(keys, g.getGroupMemberNumKey(groupID))
	}
	cache := g.NewCache()
	cache.AddKeys(keys...)

	return cache
}

func (g *GroupCacheRedis) GetGroupOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error) {
	members, err := g.GetGroupRoleLevelMemberInfo(ctx, groupID, constant.GroupOwner)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errs.ErrRecordNotFound.Wrap(fmt.Sprintf("group %s owner not found", groupID))
	}
	return members[0], nil
}

func (g *GroupCacheRedis) GetGroupsOwner(ctx context.Context, groupIDs []string) ([]*relationtb.GroupMemberModel, error) {
	members := make([]*relationtb.GroupMemberModel, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		items, err := g.GetGroupRoleLevelMemberInfo(ctx, groupID, constant.GroupOwner)
		if err != nil {
			return nil, err
		}
		if len(items) > 0 {
			members = append(members, items[0])
		}
	}
	return members, nil
}

func (g *GroupCacheRedis) GetGroupRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error) {
	return getCache(ctx, g.rcClient, g.getGroupRoleLevelMemberIDsKey(groupID, roleLevel), g.expireTime, func(ctx context.Context) ([]string, error) {
		return g.groupMemberDB.FindRoleLevelUserIDs(ctx, groupID, roleLevel)
	})
}

func (g *GroupCacheRedis) GetGroupRoleLevelMemberInfo(ctx context.Context, groupID string, roleLevel int32) ([]*relationtb.GroupMemberModel, error) {
	userIDs, err := g.GetGroupRoleLevelMemberIDs(ctx, groupID, roleLevel)
	if err != nil {
		return nil, err
	}
	return g.GetGroupMembersInfo(ctx, groupID, userIDs)
}

func (g *GroupCacheRedis) GetGroupRolesLevelMemberInfo(ctx context.Context, groupID string, roleLevels []int32) ([]*relationtb.GroupMemberModel, error) {
	var userIDs []string
	for _, roleLevel := range roleLevels {
		ids, err := g.GetGroupRoleLevelMemberIDs(ctx, groupID, roleLevel)
		if err != nil {
			return nil, err
		}
		userIDs = append(userIDs, ids...)
	}
	return g.GetGroupMembersInfo(ctx, groupID, userIDs)
}

func (g *GroupCacheRedis) FindGroupMemberUser(ctx context.Context, groupIDs []string, userID string) (_ []*relationtb.GroupMemberModel, err error) {
	if len(groupIDs) == 0 {
		groupIDs, err = g.GetJoinedGroupIDs(ctx, userID)
		if err != nil {
			return nil, err
		}
	}
	return batchGetCache2(ctx, g.rcClient, g.expireTime, groupIDs, func(groupID string) string {
		return g.getGroupMemberInfoKey(groupID, userID)
	}, func(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error) {
		return g.groupMemberDB.Take(ctx, groupID, userID)
	})
}
