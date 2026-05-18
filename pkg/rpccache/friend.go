package rpccache

import (
	"BaoIM-Server/pkg/common/cachekey"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/localcache"
	"baoim/tools/log"
	"context"
	"github.com/redis/go-redis/v9"
)

func NewFriendLocalCache(client rpcclient.FriendRpcClient, cli redis.UniversalClient) *FriendLocalCache {
	lc := config.Config.LocalCache.Friend
	log.ZDebug(context.Background(), "FriendLocalCache", "topic", lc.Topic, "slotNum", lc.SlotNum, "slotSize", lc.SlotSize, "enable", lc.Enable())
	x := &FriendLocalCache{
		client: client,
		local: localcache.New[any](
			localcache.WithLocalSlotNum(lc.SlotNum),
			localcache.WithLocalSlotSize(lc.SlotSize),
			localcache.WithLinkSlotNum(lc.SlotNum),
			localcache.WithLocalSuccessTTL(lc.Success()),
			localcache.WithLocalFailedTTL(lc.Failed()),
		),
	}
	if lc.Enable() {
		go subscriberRedisDeleteCache(context.Background(), cli, lc.Topic, x.local.DelLocal)
	}
	return x
}

type FriendLocalCache struct {
	client rpcclient.FriendRpcClient
	local  localcache.Cache[any]
}

func (f *FriendLocalCache) IsFriend(ctx context.Context, possibleFriendUserID, userID string) (val bool, err error) {
	// 打印好友查询请求日志，包含两个用户ID
	log.ZDebug(ctx, "FriendLocalCache IsFriend req", "possibleFriendUserID", possibleFriendUserID, "userID", userID)
	// 使用 defer，在函数返回时自动打印返回结果日志
	defer func() {
		if err == nil {
			// 如果没有错误，打印返回的结果值
			log.ZDebug(ctx, "FriendLocalCache IsFriend return", "value", val)
		} else {
			// 如果有错误，打印错误信息
			log.ZError(ctx, "FriendLocalCache IsFriend return", err)
		}
	}()
	// 查询是否为好友，优先从本地缓存获取，没有则通过 client 远程调用
	return localcache.AnyValue[bool](
		f.local.GetLink(
			ctx, // 上下文参数
			cachekey.GetIsFriendKey(possibleFriendUserID, userID),
			func(ctx context.Context) (any, error) {

				log.ZDebug(ctx, "FriendLocalCache IsFriend rpc", "possibleFriendUserID", possibleFriendUserID, "userID", userID)

				return f.client.IsFriend(ctx, possibleFriendUserID, userID)
			},
			cachekey.GetFriendIDsKey(possibleFriendUserID),
			cachekey.GetFriendIDsKey(userID),
		),
	)
}

// IsBlack possibleBlackUserID selfUserID
func (f *FriendLocalCache) IsBlack(ctx context.Context, possibleBlackUserID, userID string) (val bool, err error) {
	log.ZDebug(ctx, "FriendLocalCache IsBlack req", "possibleBlackUserID", possibleBlackUserID, "userID", userID)
	defer func() {
		if err == nil {
			log.ZDebug(ctx, "FriendLocalCache IsBlack return", "value", val)
		} else {
			log.ZError(ctx, "FriendLocalCache IsBlack return", err)
		}
	}()
	return localcache.AnyValue[bool](f.local.GetLink(ctx, cachekey.GetIsBlackIDsKey(possibleBlackUserID, userID), func(ctx context.Context) (any, error) {
		log.ZDebug(ctx, "FriendLocalCache IsBlack rpc", "possibleBlackUserID", possibleBlackUserID, "userID", userID)
		return f.client.IsBlack(ctx, possibleBlackUserID, userID)
	}, cachekey.GetBlackIDsKey(userID)))
}
