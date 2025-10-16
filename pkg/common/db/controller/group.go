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

package controller

import (
	pbgroup "baoim/protocol/group"
	"baoim/protocol/sdkws"
	"context"
	"time"

	"BaoIM-Server/pkg/common/db/cache"
	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/constant"
	"baoim/tools/pagination"
	"baoim/tools/tx"
	"baoim/tools/utils"
	"github.com/dtm-labs/rockscache"
	"github.com/redis/go-redis/v9"
)

type GroupDatabase interface {
	// CreateGroup creates new groups along with their members.
	CreateGroup(ctx context.Context, groups []*relationtb.GroupModel, groupMembers []*relationtb.GroupMemberModel) error

	//把聊天室 缓存到房间列表
	AddRoomList(ctx context.Context, room *sdkws.RoomInfo) error
	//获取聊天室列表 接口
	GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbgroup.GetRoomListResp, error)

	//加入房间更新
	JoinRoomList(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error)

	//退出聊天室
	QuitRoomList(ctx context.Context, groupID string, uid string, img string) error
	// 解散聊天室
	DismissRoom(ctx context.Context, roomID string) error
	//从redis中回去当前聊天室信息
	GetRoomInfo(ctx context.Context, roomID string) (*sdkws.RoomInfo, error)

	// TakeGroup retrieves a single group by its ID.
	TakeGroup(ctx context.Context, groupID string) (group *relationtb.GroupModel, err error)
	// FindGroup retrieves multiple groups by their IDs.
	FindGroup(ctx context.Context, groupIDs []string) (groups []*relationtb.GroupModel, err error)
	// SearchGroup searches for groups based on a keyword and pagination settings, returns total count and groups.
	SearchGroup(ctx context.Context, keyword string, pagination pagination.Pagination) (int64, []*relationtb.GroupModel, error)
	// UpdateGroup updates the properties of a group identified by its ID.
	UpdateGroup(ctx context.Context, groupID string, data map[string]any) error
	// DismissGroup disbands a group and optionally removes its members based on the deleteMember flag.
	DismissGroup(ctx context.Context, groupID string, deleteMember bool) error

	// TakeGroupMember retrieves a specific group member by group ID and user ID.
	TakeGroupMember(ctx context.Context, groupID string, userID string) (groupMember *relationtb.GroupMemberModel, err error)
	// TakeGroupOwner retrieves the owner of a group by group ID.
	TakeGroupOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error)
	// FindGroupMembers retrieves members of a group filtered by user IDs.
	FindGroupMembers(ctx context.Context, groupID string, userIDs []string) (groupMembers []*relationtb.GroupMemberModel, err error)
	// FindGroupMemberUser retrieves groups that a user is a member of, filtered by group IDs.
	FindGroupMemberUser(ctx context.Context, groupIDs []string, userID string) (groupMembers []*relationtb.GroupMemberModel, err error)
	// FindGroupMemberRoleLevels retrieves group members filtered by their role levels within a group.
	FindGroupMemberRoleLevels(ctx context.Context, groupID string, roleLevels []int32) (groupMembers []*relationtb.GroupMemberModel, err error)
	// FindGroupMemberAll retrieves all members of a group.
	FindGroupMemberAll(ctx context.Context, groupID string) (groupMembers []*relationtb.GroupMemberModel, err error)
	// FindGroupsOwner retrieves the owners for multiple groups.
	FindGroupsOwner(ctx context.Context, groupIDs []string) ([]*relationtb.GroupMemberModel, error)
	// FindGroupMemberUserID retrieves the user IDs of all members in a group.
	FindGroupMemberUserID(ctx context.Context, groupID string) ([]string, error)
	// FindGroupMemberNum retrieves the number of members in a group.
	FindGroupMemberNum(ctx context.Context, groupID string) (uint32, error)
	// FindUserManagedGroupID retrieves group IDs managed by a user.
	FindUserManagedGroupID(ctx context.Context, userID string) (groupIDs []string, err error)
	// PageGroupRequest paginates through group requests for specified groups.
	PageGroupRequest(ctx context.Context, groupIDs []string, pagination pagination.Pagination) (int64, []*relationtb.GroupRequestModel, error)
	// GetGroupRoleLevelMemberIDs retrieves user IDs of group members with a specific role level.
	GetGroupRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error)

	// PageGetJoinGroup paginates through groups that a user has joined.
	PageGetJoinGroup(ctx context.Context, userID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error)
	// PageGetGroupMember paginates through members of a group.
	PageGetGroupMember(ctx context.Context, groupID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error)
	// SearchGroupMember searches for group members based on a keyword, group ID, and pagination settings.
	SearchGroupMember(ctx context.Context, keyword string, groupID string, pagination pagination.Pagination) (int64, []*relationtb.GroupMemberModel, error)
	// HandlerGroupRequest processes a group join request with a specified result.
	HandlerGroupRequest(ctx context.Context, groupID string, userID string, handledMsg string, handleResult int32, member *relationtb.GroupMemberModel) error
	// DeleteGroupMember removes specified users from a group.
	DeleteGroupMember(ctx context.Context, groupID string, userIDs []string) error
	// MapGroupMemberUserID maps group IDs to their members' simplified user IDs.
	MapGroupMemberUserID(ctx context.Context, groupIDs []string) (map[string]*relationtb.GroupSimpleUserID, error)
	// MapGroupMemberNum maps group IDs to their member count.
	MapGroupMemberNum(ctx context.Context, groupIDs []string) (map[string]uint32, error)
	// TransferGroupOwner transfers the ownership of a group to another user.
	TransferGroupOwner(ctx context.Context, groupID string, oldOwnerUserID, newOwnerUserID string, roleLevel int32) error
	// UpdateGroupMember updates properties of a group member.
	UpdateGroupMember(ctx context.Context, groupID string, userID string, data map[string]any) error
	// UpdateGroupMembers batch updates properties of group members.
	UpdateGroupMembers(ctx context.Context, data []*relationtb.BatchUpdateGroupMember) error

	// CreateGroupRequest creates new group join requests.
	CreateGroupRequest(ctx context.Context, requests []*relationtb.GroupRequestModel) error
	// TakeGroupRequest retrieves a specific group join request.
	TakeGroupRequest(ctx context.Context, groupID string, userID string) (*relationtb.GroupRequestModel, error)
	// FindGroupRequests retrieves multiple group join requests.
	FindGroupRequests(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupRequestModel, error)
	// PageGroupRequestUser paginates through group join requests made by a user.
	PageGroupRequestUser(ctx context.Context, userID string, pagination pagination.Pagination) (int64, []*relationtb.GroupRequestModel, error)

	// CountTotal counts the total number of groups as of a certain date.
	CountTotal(ctx context.Context, before *time.Time) (count int64, err error)
	// CountRangeEverydayTotal counts the daily group creation total within a specified date range.
	CountRangeEverydayTotal(ctx context.Context, start time.Time, end time.Time) (map[string]int64, error)
	// DeleteGroupMemberHash deletes the hash entries for group members in specified groups.
	DeleteGroupMemberHash(ctx context.Context, groupIDs []string) error
}

func NewGroupDatabase(
	rdb redis.UniversalClient,
	groupDB relationtb.GroupModelInterface,
	groupMemberDB relationtb.GroupMemberModelInterface,
	groupRequestDB relationtb.GroupRequestModelInterface,
	ctxTx tx.CtxTx,
	groupHash cache.GroupHash,
) GroupDatabase {
	rcOptions := rockscache.NewDefaultOptions()
	rcOptions.StrongConsistency = true
	rcOptions.RandomExpireAdjustment = 0.2
	return &groupDatabase{
		groupDB:        groupDB,
		groupMemberDB:  groupMemberDB,
		groupRequestDB: groupRequestDB,
		ctxTx:          ctxTx,
		cache:          cache.NewGroupCacheRedis(rdb, groupDB, groupMemberDB, groupRequestDB, groupHash, rcOptions),
	}
}

type groupDatabase struct {
	groupDB        relationtb.GroupModelInterface
	groupMemberDB  relationtb.GroupMemberModelInterface
	groupRequestDB relationtb.GroupRequestModelInterface
	ctxTx          tx.CtxTx
	cache          cache.GroupCache
}

func (g *groupDatabase) FindGroupMembers(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.GetGroupMembersInfo(ctx, groupID, userIDs)
}

func (g *groupDatabase) FindGroupMemberUser(ctx context.Context, groupIDs []string, userID string) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.FindGroupMemberUser(ctx, groupIDs, userID)
}

func (g *groupDatabase) FindGroupMemberRoleLevels(ctx context.Context, groupID string, roleLevels []int32) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.GetGroupRolesLevelMemberInfo(ctx, groupID, roleLevels)
}

func (g *groupDatabase) FindGroupMemberAll(ctx context.Context, groupID string) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.GetAllGroupMembersInfo(ctx, groupID)
}

func (g *groupDatabase) FindGroupsOwner(ctx context.Context, groupIDs []string) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.GetGroupsOwner(ctx, groupIDs)
}

func (g *groupDatabase) GetGroupRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error) {
	return g.cache.GetGroupRoleLevelMemberIDs(ctx, groupID, roleLevel)
}
func (g *groupDatabase) CreateGroup(ctx context.Context, groups []*relationtb.GroupModel, groupMembers []*relationtb.GroupMemberModel) error {
	// 如果 groups 和 groupMembers 都为空，则直接返回 nil，无需创建
	if len(groups)+len(groupMembers) == 0 {
		return nil
	}
	// 使用事务保证原子性
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		// 创建新的缓存对象
		c := g.cache.NewCache()
		// 如果有群组需要创建
		if len(groups) > 0 {
			// 调用 groupDB 的 Create 方法创建群组
			if err := g.groupDB.Create(ctx, groups); err != nil {
				return err // 创建失败则返回错误
			}
			// 对每个群组进行缓存删除操作
			for _, group := range groups {
				c = c.DelGroupsInfo(group.GroupID). // 删除群组信息缓存
									DelGroupMembersHash(group.GroupID). // 删除群组成员哈希缓存
									DelGroupMembersHash(group.GroupID). // 再次删除群组成员哈希缓存（可能是冗余）
									DelGroupsMemberNum(group.GroupID).  // 删除群组成员数量缓存
									DelGroupMemberIDs(group.GroupID).   // 删除群组成员ID列表缓存
									DelGroupAllRoleLevel(group.GroupID) // 删除群组所有成员角色等级缓存
			}
		}
		// 如果有群组成员需要创建
		if len(groupMembers) > 0 {
			// 调用 groupMemberDB 的 Create 方法创建群组成员
			if err := g.groupMemberDB.Create(ctx, groupMembers); err != nil {
				return err // 创建失败则返回错误
			}
			// 对每个群组成员进行缓存删除操作
			for _, groupMember := range groupMembers {
				c = c.DelGroupMembersHash(groupMember.GroupID). // 删除群组成员哈希缓存
										DelGroupsMemberNum(groupMember.GroupID).                      // 删除群组成员数量缓存
										DelGroupMemberIDs(groupMember.GroupID).                       // 删除群组成员ID列表缓存
										DelJoinedGroupID(groupMember.UserID).                         // 删除用户已加入群组ID缓存
										DelGroupMembersInfo(groupMember.GroupID, groupMember.UserID). // 删除群组成员信息缓存
										DelGroupAllRoleLevel(groupMember.GroupID)                     // 删除群组所有成员角色等级缓存
			}
		}
		// 执行所有缓存删除操作
		return c.ExecDel(ctx, true)
	})
}

func (g *groupDatabase) AddRoomList(ctx context.Context, room *sdkws.RoomInfo) error {
	return g.cache.AddRoomCache(ctx, room)
}

func (g *groupDatabase) JoinRoomList(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error) {
	return g.cache.UpdateRoomCache(ctx, roomID, uid, img)
}

// 退出聊天室
func (g *groupDatabase) QuitRoomList(ctx context.Context, roomID string, uid string, img string) error {
	return g.cache.RemoveRoomCache(ctx, roomID, uid, img)
}

// 解散聊天室
func (g *groupDatabase) DismissRoom(ctx context.Context, roomID string) error {
	return g.cache.DismissRoomCache(ctx, roomID)
}
func (g *groupDatabase) GetRoomInfo(ctx context.Context, roomID string) (*sdkws.RoomInfo, error) {
	return g.cache.GetRoomInfoCache(ctx, roomID)
}

// /获取聊天室列表
func (g *groupDatabase) GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbgroup.GetRoomListResp, error) {
	return g.cache.GetRoomListCache(ctx, page, pageSize)
}

func (g *groupDatabase) FindGroupMemberUserID(ctx context.Context, groupID string) ([]string, error) {
	return g.cache.GetGroupMemberIDs(ctx, groupID)
}

func (g *groupDatabase) FindGroupMemberNum(ctx context.Context, groupID string) (uint32, error) {
	num, err := g.cache.GetGroupMemberNum(ctx, groupID)
	if err != nil {
		return 0, err
	}
	return uint32(num), nil
}

func (g *groupDatabase) TakeGroup(ctx context.Context, groupID string) (*relationtb.GroupModel, error) {
	return g.cache.GetGroupInfo(ctx, groupID)
}

func (g *groupDatabase) FindGroup(ctx context.Context, groupIDs []string) ([]*relationtb.GroupModel, error) {
	return g.cache.GetGroupsInfo(ctx, groupIDs)
}

func (g *groupDatabase) SearchGroup(ctx context.Context, keyword string, pagination pagination.Pagination) (int64, []*relationtb.GroupModel, error) {
	return g.groupDB.Search(ctx, keyword, pagination)
}

func (g *groupDatabase) UpdateGroup(ctx context.Context, groupID string, data map[string]any) error {
	if err := g.groupDB.UpdateMap(ctx, groupID, data); err != nil {
		return err
	}
	return g.cache.DelGroupsInfo(groupID).ExecDel(ctx)
}

func (g *groupDatabase) DismissGroup(ctx context.Context, groupID string, deleteMember bool) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		c := g.cache.NewCache()
		if err := g.groupDB.UpdateStatus(ctx, groupID, constant.GroupStatusDismissed); err != nil {
			return err
		}
		if deleteMember {
			userIDs, err := g.cache.GetGroupMemberIDs(ctx, groupID)
			if err != nil {
				return err
			}
			if err := g.groupMemberDB.Delete(ctx, groupID, nil); err != nil {
				return err
			}
			c = c.DelJoinedGroupID(userIDs...).
				DelGroupMemberIDs(groupID).
				DelGroupsMemberNum(groupID).
				DelGroupMembersHash(groupID).
				DelGroupAllRoleLevel(groupID).
				DelGroupMembersInfo(groupID, userIDs...)
		}
		return c.DelGroupsInfo(groupID).ExecDel(ctx)
	})
}

func (g *groupDatabase) TakeGroupMember(ctx context.Context, groupID string, userID string) (*relationtb.GroupMemberModel, error) {
	return g.cache.GetGroupMemberInfo(ctx, groupID, userID)
}

func (g *groupDatabase) TakeGroupOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error) {
	return g.cache.GetGroupOwner(ctx, groupID)
}

func (g *groupDatabase) FindUserManagedGroupID(ctx context.Context, userID string) (groupIDs []string, err error) {
	return g.groupMemberDB.FindUserManagedGroupID(ctx, userID)
}

func (g *groupDatabase) PageGroupRequest(ctx context.Context, groupIDs []string, pagination pagination.Pagination) (int64, []*relationtb.GroupRequestModel, error) {
	return g.groupRequestDB.PageGroup(ctx, groupIDs, pagination)
}

func (g *groupDatabase) PageGetJoinGroup(ctx context.Context, userID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error) {
	groupIDs, err := g.cache.GetJoinedGroupIDs(ctx, userID)
	if err != nil {
		return 0, nil, err
	}
	for _, groupID := range utils.Paginate(groupIDs, int(pagination.GetPageNumber()), int(pagination.GetShowNumber())) {
		groupMembers, err := g.cache.GetGroupMembersInfo(ctx, groupID, []string{userID})
		if err != nil {
			return 0, nil, err
		}
		totalGroupMembers = append(totalGroupMembers, groupMembers...)
	}
	return int64(len(groupIDs)), totalGroupMembers, nil
}

func (g *groupDatabase) PageGetGroupMember(ctx context.Context, groupID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error) {
	groupMemberIDs, err := g.cache.GetGroupMemberIDs(ctx, groupID)
	if err != nil {
		return 0, nil, err
	}
	pageIDs := utils.Paginate(groupMemberIDs, int(pagination.GetPageNumber()), int(pagination.GetShowNumber()))
	if len(pageIDs) == 0 {
		return int64(len(groupMemberIDs)), nil, nil
	}
	members, err := g.cache.GetGroupMembersInfo(ctx, groupID, pageIDs)
	if err != nil {
		return 0, nil, err
	}
	return int64(len(groupMemberIDs)), members, nil
}

func (g *groupDatabase) SearchGroupMember(ctx context.Context, keyword string, groupID string, pagination pagination.Pagination) (int64, []*relationtb.GroupMemberModel, error) {
	return g.groupMemberDB.SearchMember(ctx, keyword, groupID, pagination)
}

func (g *groupDatabase) HandlerGroupRequest(ctx context.Context, groupID string, userID string, handledMsg string, handleResult int32, member *relationtb.GroupMemberModel) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		if err := g.groupRequestDB.UpdateHandler(ctx, groupID, userID, handledMsg, handleResult); err != nil {
			return err
		}
		if member != nil {
			if err := g.groupMemberDB.Create(ctx, []*relationtb.GroupMemberModel{member}); err != nil {
				return err
			}
			c := g.cache.DelGroupMembersHash(groupID).
				DelGroupMembersInfo(groupID, member.UserID).
				DelGroupMemberIDs(groupID).
				DelGroupsMemberNum(groupID).
				DelJoinedGroupID(member.UserID).
				DelGroupRoleLevel(groupID, []int32{member.RoleLevel})
			if err := c.ExecDel(ctx); err != nil {
				return err
			}
		}
		return nil
	})
}

func (g *groupDatabase) DeleteGroupMember(ctx context.Context, groupID string, userIDs []string) error {
	if err := g.groupMemberDB.Delete(ctx, groupID, userIDs); err != nil {
		return err
	}
	return g.cache.DelGroupMembersHash(groupID).
		DelGroupMemberIDs(groupID).
		DelGroupsMemberNum(groupID).
		DelJoinedGroupID(userIDs...).
		DelGroupMembersInfo(groupID, userIDs...).
		DelGroupAllRoleLevel(groupID).
		ExecDel(ctx)
}

func (g *groupDatabase) MapGroupMemberUserID(ctx context.Context, groupIDs []string) (map[string]*relationtb.GroupSimpleUserID, error) {
	return g.cache.GetGroupMemberHashMap(ctx, groupIDs)
}

func (g *groupDatabase) MapGroupMemberNum(ctx context.Context, groupIDs []string) (m map[string]uint32, err error) {
	m = make(map[string]uint32)
	for _, groupID := range groupIDs {
		num, err := g.cache.GetGroupMemberNum(ctx, groupID)
		if err != nil {
			return nil, err
		}
		m[groupID] = uint32(num)
	}
	return m, nil
}

func (g *groupDatabase) TransferGroupOwner(ctx context.Context, groupID string, oldOwnerUserID, newOwnerUserID string, roleLevel int32) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		if err := g.groupMemberDB.UpdateRoleLevel(ctx, groupID, oldOwnerUserID, roleLevel); err != nil {
			return err
		}
		if err := g.groupMemberDB.UpdateRoleLevel(ctx, groupID, newOwnerUserID, constant.GroupOwner); err != nil {
			return err
		}
		return g.cache.DelGroupMembersInfo(groupID, oldOwnerUserID, newOwnerUserID).
			DelGroupAllRoleLevel(groupID).
			DelGroupMembersHash(groupID).ExecDel(ctx)
	})
}

func (g *groupDatabase) UpdateGroupMember(ctx context.Context, groupID string, userID string, data map[string]any) error {
	if err := g.groupMemberDB.Update(ctx, groupID, userID, data); err != nil {
		return err
	}
	c := g.cache.DelGroupMembersInfo(groupID, userID)
	if g.groupMemberDB.IsUpdateRoleLevel(data) {
		c = c.DelGroupAllRoleLevel(groupID)
	}
	return c.ExecDel(ctx)
}

func (g *groupDatabase) UpdateGroupMembers(ctx context.Context, data []*relationtb.BatchUpdateGroupMember) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		c := g.cache.NewCache()
		for _, item := range data {
			if err := g.groupMemberDB.Update(ctx, item.GroupID, item.UserID, item.Map); err != nil {
				return err
			}
			if g.groupMemberDB.IsUpdateRoleLevel(item.Map) {
				c = c.DelGroupAllRoleLevel(item.GroupID)
			}
			c = c.DelGroupMembersInfo(item.GroupID, item.UserID).DelGroupMembersHash(item.GroupID)
		}
		return c.ExecDel(ctx, true)
	})
}

func (g *groupDatabase) CreateGroupRequest(ctx context.Context, requests []*relationtb.GroupRequestModel) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		for _, request := range requests {
			if err := g.groupRequestDB.Delete(ctx, request.GroupID, request.UserID); err != nil {
				return err
			}
		}
		return g.groupRequestDB.Create(ctx, requests)
	})
}

func (g *groupDatabase) TakeGroupRequest(
	ctx context.Context,
	groupID string,
	userID string,
) (*relationtb.GroupRequestModel, error) {
	return g.groupRequestDB.Take(ctx, groupID, userID)
}

func (g *groupDatabase) PageGroupRequestUser(ctx context.Context, userID string, pagination pagination.Pagination) (int64, []*relationtb.GroupRequestModel, error) {
	return g.groupRequestDB.Page(ctx, userID, pagination)
}

func (g *groupDatabase) CountTotal(ctx context.Context, before *time.Time) (count int64, err error) {
	return g.groupDB.CountTotal(ctx, before)
}

func (g *groupDatabase) CountRangeEverydayTotal(ctx context.Context, start time.Time, end time.Time) (map[string]int64, error) {
	return g.groupDB.CountRangeEverydayTotal(ctx, start, end)
}

func (g *groupDatabase) FindGroupRequests(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupRequestModel, error) {
	return g.groupRequestDB.FindGroupRequests(ctx, groupID, userIDs)
}

func (g *groupDatabase) DeleteGroupMemberHash(ctx context.Context, groupIDs []string) error {
	if len(groupIDs) == 0 {
		return nil
	}
	c := g.cache.NewCache()
	for _, groupID := range groupIDs {
		c = c.DelGroupMembersHash(groupID)
	}
	return c.ExecDel(ctx)
}
