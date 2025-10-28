package cache

//
//import (
//	"baoim/tools/log"
//	"context"
//	"encoding/json"
//	"fmt"
//	"time"
//
//	"baoim/protocol/sdkws"
//	"baoim/tools/errs"
//	"github.com/redis/go-redis/v9"
//)
//
//const (
//	// 存储前缀（与现有缓存key规范保持一致）
//	UserInfoPrefix  = "room:user:"       // 哈希键：存储用户详情
//	UserZSetKey     = "room:user:active" // 有序集合：存储用户ID+最后活跃时间（用于离线清理）
//	OfflineTimeout  = 5 * time.Minute    // 离线超时时间
//	RedisMaxRetries = 3                  // 重试次数（适配集群环境）
//)
//
//type RoomCache struct {
//	rdb redis.UniversalClient // 复用现有Redis客户端
//}
//
//func NewRoomCache(rdb redis.UniversalClient) *RoomCache {
//	return &RoomCache{rdb: rdb}
//}
//
//// AddUser 添加/更新用户（支持亿级用户：依赖Redis集群分片）
//func (c *RoomCache) AddUser(ctx context.Context, user *sdkws.UserInfo) error {
//	data, err := json.Marshal(user)
//	if err != nil {
//		return errs.Wrap(err, "marshal user info failed")
//	}
//
//	// 管道操作减少IO（批量执行命令）
//	pipe := c.rdb.Pipeline()
//	// 存储用户详情（无过期时间，依赖主动清理）
//	pipe.HSet(ctx, UserInfoPrefix, user.UserID, data)
//	// 更新活跃时间（有序集合分数=时间戳，用于范围查询）
//	pipe.ZAdd(ctx, UserZSetKey, redis.Z{
//		Score:  float64(user.LastActiveTime),
//		Member: user.UserID,
//	})
//	_, err = pipe.Exec(ctx)
//	return errs.Wrap(err, "redis pipeline exec failed")
//}
//
//// DeleteUser 删除用户
//func (c *RoomCache) DeleteUser(ctx context.Context, userID string) error {
//	pipe := c.rdb.Pipeline()
//	pipe.HDel(ctx, UserInfoPrefix, userID)
//	pipe.ZRem(ctx, UserZSetKey, userID)
//	_, err := pipe.Exec(ctx)
//	return errs.Wrap(err, "delete user from redis failed")
//}
//
//// GetUser 获取单个用户
//func (c *RoomCache) GetUser(ctx context.Context, userID string) (*sdkws.UserInfo, error) {
//	data, err := c.rdb.HGet(ctx, UserInfoPrefix, userID).Bytes()
//	if err != nil {
//		if err == redis.Nil {
//			return nil, errs.ErrRecordNotFound.Wrap("user not found")
//		}
//		return nil, errs.Wrap(err, "get user from redis failed")
//	}
//
//	var user sdkws.UserInfo
//	if err := json.Unmarshal(data, &user); err != nil {
//		return nil, errs.Wrap(err, "unmarshal user data failed")
//	}
//	return &user, nil
//}
//
//// GetAllUsers 分页获取用户（支持亿级用户：避免全量拉取）
//func (c *RoomCache) GetAllUsers(ctx context.Context, page, size int32) ([]*sdkws.UserInfo, int64, error) {
//	// 1. 获取总用户数
//	total, err := c.rdb.ZCard(ctx, UserZSetKey).Result()
//	if err != nil {
//		return nil, 0, errs.Wrap(err, "get user total failed")
//	}
//
//	// 2. 分页获取用户ID（按活跃时间倒序）
//	start := int64((page - 1) * size)
//	end := int64(page*size - 1)
//	userIDs, err := c.rdb.ZRevRange(ctx, UserZSetKey, start, end).Result()
//	if err != nil {
//		return nil, 0, errs.Wrap(err, "get user ids failed")
//	}
//
//	// 3. 批量获取用户详情
//	users := make([]*sdkws.UserInfo, 0, len(userIDs))
//	for _, uid := range userIDs {
//		user, err := c.GetUser(ctx, uid)
//		if err != nil {
//			if !errs.ErrRecordNotFound.Is(err) {
//				log.ZWarn(ctx, "skip invalid user", err, "user_id", uid)
//			}
//			continue
//		}
//		users = append(users, user)
//	}
//
//	return users, total, nil
//}
//
//// CleanOfflineUsers 清理离线用户（返回被清理的用户）
//func (c *RoomCache) CleanOfflineUsers(ctx context.Context) ([]*sdkws.UserInfo, error) {
//	// 计算5分钟前的时间戳
//	expireTime := time.Now().Add(-OfflineTimeout).UnixMilli()
//
//	// 1. 查询离线用户ID（分数<=expireTime的用户）
//	userIDs, err := c.rdb.ZRangeByScore(ctx, UserZSetKey, &redis.ZRangeBy{
//		Min: "0",
//		Max: fmt.Sprintf("%d", expireTime),
//	}).Result()
//	if err != nil {
//		return nil, errs.Wrap(err, "get offline user ids failed")
//	}
//	if len(userIDs) == 0 {
//		return nil, nil
//	}
//
//	// 2. 获取离线用户详情
//	offlineUsers := make([]*sdkws.UserInfo, 0, len(userIDs))
//	for _, uid := range userIDs {
//		user, err := c.GetUser(ctx, uid)
//		if err != nil {
//			log.ZWarn(ctx, "skip offline user", err, "user_id", uid)
//			continue
//		}
//		offlineUsers = append(offlineUsers, user)
//	}
//
//	// 3. 批量删除离线用户
//	if err := c.batchDeleteUsers(ctx, userIDs); err != nil {
//		return offlineUsers, errs.Wrap(err, "batch delete offline users failed")
//	}
//
//	return offlineUsers, nil
//}
//
//// 批量删除用户（内部方法）
//func (c *RoomCache) batchDeleteUsers(ctx context.Context, userIDs []string) error {
//	pipe := c.rdb.Pipeline()
//	// 批量删除哈希表中的用户
//	pipe.HDel(ctx, UserInfoPrefix, userIDs...)
//	// 批量删除有序集合中的用户
//	pipe.ZRem(ctx, UserZSetKey, userIDs...)
//	_, err := pipe.Exec(ctx)
//	return err
//}
