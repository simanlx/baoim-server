package rpccache

import (
	"BaoIM-Server/pkg/common/cachekey"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/localcache"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"baoim/tools/log"
	"context"
	"github.com/redis/go-redis/v9"
)

func NewGroupLocalCache(client rpcclient.GroupRpcClient, cli redis.UniversalClient) *GroupLocalCache {
	lc := config.Config.LocalCache.Group
	log.ZDebug(context.Background(), "GroupLocalCache", "topic", lc.Topic, "slotNum", lc.SlotNum, "slotSize", lc.SlotSize, "enable", lc.Enable())
	x := &GroupLocalCache{
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

type GroupLocalCache struct {
	client rpcclient.GroupRpcClient
	local  localcache.Cache[any]
}

func (g *GroupLocalCache) getGroupMemberIDs(ctx context.Context, groupID string) (val *listMap[string], err error) {
	log.ZDebug(ctx, "GroupLocalCache getGroupMemberIDs req", "groupID", groupID)
	defer func() {
		if err == nil {
			log.ZDebug(ctx, "GroupLocalCache getGroupMemberIDs return", "value", val)
		} else {
			log.ZError(ctx, "GroupLocalCache getGroupMemberIDs return", err)
		}
	}()
	return localcache.AnyValue[*listMap[string]](g.local.Get(ctx, cachekey.GetGroupMemberIDsKey(groupID), func(ctx context.Context) (any, error) {
		log.ZDebug(ctx, "GroupLocalCache getGroupMemberIDs rpc", "groupID", groupID)
		return newListMap(g.client.GetGroupMemberIDs(ctx, groupID))
	}))
}

func (g *GroupLocalCache) GetGroupMember(ctx context.Context, groupID, userID string) (val *sdkws.GroupMemberFullInfo, err error) {
	log.ZDebug(ctx, "GroupLocalCache GetGroupInfo req", "groupID", groupID, "userID", userID)
	defer func() {
		if err == nil {
			log.ZDebug(ctx, "GroupLocalCache GetGroupInfo return", "value", val)
		} else {
			log.ZError(ctx, "GroupLocalCache GetGroupInfo return", err)
		}
	}()
	return localcache.AnyValue[*sdkws.GroupMemberFullInfo](g.local.Get(ctx, cachekey.GetGroupMemberInfoKey(groupID, userID), func(ctx context.Context) (any, error) {
		log.ZDebug(ctx, "GroupLocalCache GetGroupInfo rpc", "groupID", groupID, "userID", userID)
		return g.client.GetGroupMemberCache(ctx, groupID, userID)
	}))
}

func (g *GroupLocalCache) GetGroupInfo(ctx context.Context, groupID string) (val *sdkws.GroupInfo, err error) {
	log.ZDebug(ctx, "GroupLocalCache GetGroupInfo req", "groupID", groupID)
	defer func() {
		if err == nil {
			log.ZDebug(ctx, "GroupLocalCache GetGroupInfo return", "value", val)
		} else {
			log.ZError(ctx, "GroupLocalCache GetGroupInfo return", err)
		}
	}()
	return localcache.AnyValue[*sdkws.GroupInfo](g.local.Get(ctx, cachekey.GetGroupInfoKey(groupID), func(ctx context.Context) (any, error) {
		log.ZDebug(ctx, "GroupLocalCache GetGroupInfo rpc", "groupID", groupID)
		return g.client.GetGroupInfoCache(ctx, groupID)
	}))
}

func (g *GroupLocalCache) GetGroupMemberIDs(ctx context.Context, groupID string) ([]string, error) {
	res, err := g.getGroupMemberIDs(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return res.List, nil
}

func (g *GroupLocalCache) GetGroupMemberIDMap(ctx context.Context, groupID string) (map[string]struct{}, error) {
	res, err := g.getGroupMemberIDs(ctx, groupID)
	if err != nil {
		return nil, err
	}
	return res.Map, nil
}

func (g *GroupLocalCache) GetGroupInfos(ctx context.Context, groupIDs []string) ([]*sdkws.GroupInfo, error) {
	groupInfos := make([]*sdkws.GroupInfo, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		groupInfo, err := g.GetGroupInfo(ctx, groupID)
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				continue
			}
			return nil, err
		}
		groupInfos = append(groupInfos, groupInfo)
	}
	return groupInfos, nil
}

func (g *GroupLocalCache) GetGroupMembers(ctx context.Context, groupID string, userIDs []string) ([]*sdkws.GroupMemberFullInfo, error) {
	members := make([]*sdkws.GroupMemberFullInfo, 0, len(userIDs))
	for _, userID := range userIDs {
		member, err := g.GetGroupMember(ctx, groupID, userID)
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				continue
			}
			return nil, err
		}
		members = append(members, member)
	}
	return members, nil
}

func (g *GroupLocalCache) GetGroupMemberInfoMap(ctx context.Context, groupID string, userIDs []string) (map[string]*sdkws.GroupMemberFullInfo, error) {
	members := make(map[string]*sdkws.GroupMemberFullInfo)
	for _, userID := range userIDs {
		member, err := g.GetGroupMember(ctx, groupID, userID)
		if err != nil {
			if errs.ErrRecordNotFound.Is(err) {
				continue
			}
			return nil, err
		}
		members[userID] = member
	}
	return members, nil
}
