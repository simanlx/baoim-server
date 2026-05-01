package controller

import (
	"BaoIM-Server/pkg/common/db/cache"
	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/constant"
	pbroom "baoim/protocol/room"
	"baoim/protocol/sdkws"
	"baoim/tools/pagination"
	"baoim/tools/tx"
	"baoim/tools/utils"
	"context"
	"github.com/dtm-labs/rockscache"
	"github.com/redis/go-redis/v9"
)

type RoomDatabase interface {
	CreateRoom(ctx context.Context, groups []*relationtb.GroupModel, groupMembers []*relationtb.GroupMemberModel) error
	DismissRoom(ctx context.Context, groupID string, deleteMember bool) error
	UpdateRoomMember(ctx context.Context, groupID string, userID string, data map[string]any) error
	DeleteRoomMember(ctx context.Context, groupID string, userIDs []string) error
	PageGetRoomMember(ctx context.Context, groupID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error)
	TakeRoom(ctx context.Context, groupID string) (*relationtb.GroupModel, error)
	TakeRoomOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error)
	FindRoomMemberUserID(ctx context.Context, groupID string) ([]string, error)
	FindRoomMembers(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error)

	FindRoomMemberNum(ctx context.Context, groupID string) (uint32, error)
	TakeRoomMember(ctx context.Context, groupID string, userID string) (*relationtb.GroupMemberModel, error)
	GetRoomRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error)

	AddRoomList(ctx context.Context, room *sdkws.RoomInfo) error
	// DelRoom 在房间列表删除  并删除 \房间信息 \用户关联房间
	DelRoom(ctx context.Context, uIDs []string, roomID string) error
	// GetRoomList 获取聊天室列表
	GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error)
	// GetRoomInfo 获取聊天室信息
	GetRoomInfo(ctx context.Context, roomID string) (*sdkws.RoomInfo, error)
	// JoinRoomList 加入聊天室
	JoinRoomList(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error)
	// RemoveRoomUser 从聊天室移除用户
	RemoveRoomUser(ctx context.Context, roomID string, uid string, img string) error

	// KickRemoveRoomUser 踢出聊天室 移除 用户序列 及头像
	KickRemoveRoomUser(ctx context.Context, roomID string, uid string, img string) error
	// AddUserRoom 添加用户 关联 房间信息
	AddUserRoom(ctx context.Context, uid string, roomID string, isOwner bool) error
	// GetUserRoom 获取用户所在聊天室信息
	GetUserRoom(ctx context.Context, userID string) (roomID string, isOwner bool, err error)
	// GetRoomUserRoomID 获取用户所在聊天室ID
	GetRoomUserRoomID(ctx context.Context, userID string) (string, error)
	// DelUserRoom 删除用户聊天室信息
	DelUserRoom(ctx context.Context, uid string) error

	// AddOfflineUser 添加用户为离线状态
	AddOfflineUser(ctx context.Context, userID string) error
	// CleanOfflineUsers 轮训清理离线用户 并返回离线用户ID列表
	CleanOfflineUsers(ctx context.Context) ([]map[string]string, error)

	//添加用户到在线列表
	AddOnlineUsersCache(ctx context.Context, userID string) error
	// DelOnlineUsersCache 删除在线用户缓存
	DelOnlineUsersCache(ctx context.Context, userID string) error
}

func NewRoomDatabase(
	rdb redis.UniversalClient,
	groupDB relationtb.GroupModelInterface,
	groupMemberDB relationtb.GroupMemberModelInterface,
	// groupRequestDB relationtb.GroupRequestModelInterface,
	ctxTx tx.CtxTx,
	// groupHash cache.GroupHash,
) RoomDatabase {
	rcOptions := rockscache.NewDefaultOptions()
	rcOptions.StrongConsistency = true
	rcOptions.RandomExpireAdjustment = 0.2
	return &roomDatabase{
		groupDB:       groupDB,
		groupMemberDB: groupMemberDB,
		//groupRequestDB: groupRequestDB,
		ctxTx: ctxTx,
		cache: cache.NewRoomCacheRedis(rdb, groupDB, groupMemberDB, rcOptions),
	}
}

type roomDatabase struct {
	groupDB       relationtb.GroupModelInterface
	groupMemberDB relationtb.GroupMemberModelInterface
	//groupRequestDB relationtb.GroupRequestModelInterface
	ctxTx tx.CtxTx
	cache cache.RoomCache
}

// CreateRoom 创建聊天室 包括创建聊天室和聊天室成员
func (g *roomDatabase) CreateRoom(ctx context.Context, groups []*relationtb.GroupModel, groupMembers []*relationtb.GroupMemberModel) error {
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
				c = c.DelRoomsInfo(group.GroupID). // 删除群组信息缓存
									DelRoomMembersHash(group.GroupID). // 删除群组成员哈希缓存
									DelRoomMembersHash(group.GroupID). // 再次删除群组成员哈希缓存（可能是冗余）
									DelRoomsMemberNum(group.GroupID).  // 删除群组成员数量缓存
									DelRoomMemberIDs(group.GroupID).   // 删除群组成员ID列表缓存
									DelRoomAllRoleLevel(group.GroupID) // 删除群组所有成员角色等级缓存
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
				c = c.DelRoomMembersHash(groupMember.GroupID). // 删除群组成员哈希缓存
										DelRoomsMemberNum(groupMember.GroupID).                      // 删除群组成员数量缓存
										DelRoomMemberIDs(groupMember.GroupID).                       // 删除群组成员ID列表缓存
										DelJoinedRoomID(groupMember.UserID).                         // 删除用户已加入群组ID缓存
										DelRoomMembersInfo(groupMember.GroupID, groupMember.UserID). // 删除群组成员信息缓存
										DelRoomAllRoleLevel(groupMember.GroupID)                     // 删除群组所有成员角色等级缓存
			}
		}
		// 执行所有缓存删除操作
		return c.ExecDel(ctx, true)
	})
}

// DismissRoom 解散聊天室 包括解散聊天室和聊天室成员
func (g *roomDatabase) DismissRoom(ctx context.Context, groupID string, deleteMember bool) error {
	return g.ctxTx.Transaction(ctx, func(ctx context.Context) error {
		c := g.cache.NewCache()

		if deleteMember {
			userIDs, err := g.cache.GetRoomMemberIDs(ctx, groupID)
			if err != nil {
				return err
			}
			if err := g.groupMemberDB.Delete(ctx, groupID, nil); err != nil {
				return err
			}

			//增加的在MDB中删除 群信息
			if err := g.groupDB.DeleteOne(ctx, groupID); err != nil {
				return err
			}
			c = c.DelJoinedRoomID(userIDs...).
				DelRoomMemberIDs(groupID).
				DelRoomsMemberNum(groupID).
				DelRoomMembersHash(groupID).
				DelRoomAllRoleLevel(groupID).
				DelRoomMembersInfo(groupID, userIDs...)

			//延时删除 其他数据  群最大seq 用户最小seq和 用户已读seq
			err = g.cache.DelayDelOther(ctx, groupID, userIDs...)
			if err != nil {
				return err
			}
		} else {
			if err := g.groupDB.UpdateStatus(ctx, groupID, constant.GroupStatusDismissed); err != nil {
				return err
			}
		}

		err := c.DelRoomsInfo(groupID).ExecDel(ctx)
		if err != nil {
			return err
		}

		return nil
	})
}

// UpdateRoomMember 更新聊天室成员信息 包括更新聊天室成员角色等级
func (g *roomDatabase) UpdateRoomMember(ctx context.Context, groupID string, userID string, data map[string]any) error {
	if err := g.groupMemberDB.Update(ctx, groupID, userID, data); err != nil {
		return err
	}
	c := g.cache.DelRoomMembersInfo(groupID, userID)
	if g.groupMemberDB.IsUpdateRoleLevel(data) {
		c = c.DelRoomAllRoleLevel(groupID)
	}
	return c.ExecDel(ctx)
}

// DeleteRoomMember 删除聊天室成员 包括删除聊天室成员和聊天室成员角色等级
func (g *roomDatabase) DeleteRoomMember(ctx context.Context, groupID string, userIDs []string) error {
	if err := g.groupMemberDB.Delete(ctx, groupID, userIDs); err != nil {
		return err
	}
	//增加的 清空用户 已读seq 和最大seq 不在这里删除了 在消息推送时删除  因为在推送过程中会重写seq
	//if err := g.cache.DelUserRoomSeq(ctx, groupID, userIDs...); err != nil {
	//	return err
	//}

	return g.cache.DelRoomMembersHash(groupID).
		DelRoomMemberIDs(groupID).
		DelRoomsMemberNum(groupID).
		DelJoinedRoomID(userIDs...).
		DelRoomMembersInfo(groupID, userIDs...).
		DelRoomAllRoleLevel(groupID).
		ExecDel(ctx)
}

// PageGetRoomMember 分页查询聊天室成员 包括查询聊天室成员和聊天室成员角色等级
func (g *roomDatabase) PageGetRoomMember(ctx context.Context, groupID string, pagination pagination.Pagination) (total int64, totalGroupMembers []*relationtb.GroupMemberModel, err error) {
	groupMemberIDs, err := g.cache.GetRoomMemberIDs(ctx, groupID)
	if err != nil {
		return 0, nil, err
	}
	pageIDs := utils.Paginate(groupMemberIDs, int(pagination.GetPageNumber()), int(pagination.GetShowNumber()))
	if len(pageIDs) == 0 {
		return int64(len(groupMemberIDs)), nil, nil
	}
	members, err := g.cache.GetRoomMembersInfo(ctx, groupID, pageIDs)
	if err != nil {
		return 0, nil, err
	}
	return int64(len(groupMemberIDs)), members, nil
}

// TakeRoom 获取聊天室信息
func (g *roomDatabase) TakeRoom(ctx context.Context, groupID string) (*relationtb.GroupModel, error) {
	return g.cache.GetRoomInfo(ctx, groupID)
}

// TakeRoomOwner 获取聊天室所有者
func (g *roomDatabase) TakeRoomOwner(ctx context.Context, groupID string) (*relationtb.GroupMemberModel, error) {
	return g.cache.GetRoomOwner(ctx, groupID)
}

// FindRoomMemberUserID 查询聊天室成员用户ID列表
func (g *roomDatabase) FindRoomMemberUserID(ctx context.Context, groupID string) ([]string, error) {
	return g.cache.GetRoomMemberIDs(ctx, groupID)
}

// TakeRoomMember 获取聊天室成员信息 包括查询聊天室成员角色等级
func (g *roomDatabase) TakeRoomMember(ctx context.Context, groupID string, userID string) (*relationtb.GroupMemberModel, error) {
	return g.cache.GetRoomMemberInfo(ctx, groupID, userID)
}

// FindRoomMemberNum 查询聊天室成员数量
func (g *roomDatabase) FindRoomMemberNum(ctx context.Context, groupID string) (uint32, error) {
	num, err := g.cache.GetRoomMemberNum(ctx, groupID)
	if err != nil {
		return 0, err
	}
	return uint32(num), nil
}

// GetRoomRoleLevelMemberIDs 查询聊天室指定角色等级成员用户ID列表
func (g *roomDatabase) GetRoomRoleLevelMemberIDs(ctx context.Context, groupID string, roleLevel int32) ([]string, error) {
	return g.cache.GetRoomRoleLevelMemberIDs(ctx, groupID, roleLevel)
}

// FindRoomMembers 查询聊天室成员信息 包括查询聊天室成员角色等级
func (g *roomDatabase) FindRoomMembers(ctx context.Context, groupID string, userIDs []string) ([]*relationtb.GroupMemberModel, error) {
	return g.cache.GetRoomMembersInfo(ctx, groupID, userIDs)
}

// AddRoomList 添加聊天室房间列表
func (g *roomDatabase) AddRoomList(ctx context.Context, room *sdkws.RoomInfo) error {
	return g.cache.AddRoomCache(ctx, room)
}

func (g *roomDatabase) DelRoom(ctx context.Context, uIDs []string, roomID string) error {
	return g.cache.DelRoomCache(ctx, uIDs, roomID)
}

// GetRoomList 获取聊天室房间列表
func (r *roomDatabase) GetRoomList(ctx context.Context, page int32, pageSize int32) (*pbroom.GetRoomListResp, error) {
	return r.cache.GetRoomListCache(ctx, page, pageSize)
}

func (g *roomDatabase) GetRoomInfo(ctx context.Context, roomID string) (*sdkws.RoomInfo, error) {
	return g.cache.GetRoomInfoCache(ctx, roomID)
}

// JoinRoomList 加入聊天室 更新用户座位序列 及头像
func (g *roomDatabase) JoinRoomList(ctx context.Context, roomID string, uid string, img string) (*sdkws.RoomInfo, error) {
	return g.cache.UpdateRoomUserCache(ctx, roomID, uid, img)
}

// RemoveRoomUser 退出聊天室 移除 用户序列 及头像
func (g *roomDatabase) RemoveRoomUser(ctx context.Context, roomID string, uid string, img string) error {
	return g.cache.RemoveRoomUserCache(ctx, roomID, uid, img)
}

// KickRemoveRoomUser 踢出聊天室 移除 用户序列 及头像
func (g *roomDatabase) KickRemoveRoomUser(ctx context.Context, roomID string, uid string, img string) error {
	return g.cache.KickRemoveRoomUserCache(ctx, roomID, uid, img)
}

// AddUserRoom 新增 更新用户 关联 房间信息
func (g *roomDatabase) AddUserRoom(ctx context.Context, uid string, roomID string, isOwner bool) error {
	return g.cache.AddUserRoomCache(ctx, uid, roomID, isOwner)
}

// GetUserRoom 获取用户 关联 房间信息
func (g *roomDatabase) GetUserRoom(ctx context.Context, userID string) (roomID string, isOwner bool, err error) {
	return g.cache.GetUserRoomCache(ctx, userID)
}

// GetRoomUserRoomID 获取用户 关联 房间ID
func (r *roomDatabase) GetRoomUserRoomID(ctx context.Context, userID string) (string, error) {
	return r.cache.GetRoomUserRoomIDCache(ctx, userID)
}

// DelUserRoom 删除用户 关联 房间信息
func (g *roomDatabase) DelUserRoom(ctx context.Context, uid string) error {
	return g.cache.DelUserRoomCache(ctx, uid)
}

// AddOfflineUser 用户离线触发
func (r *roomDatabase) AddOfflineUser(ctx context.Context, userID string) error {
	return r.cache.AddUsersOfflineCache(ctx, userID)
}

// CleanOfflineUsers 轮训清理离线用户 并返回离线用户ID列表
func (r *roomDatabase) CleanOfflineUsers(ctx context.Context) ([]map[string]string, error) {
	return r.cache.CleanOfflineUsersCache(ctx)
}

// AddOnlineUsersCache 添加在线用户缓存
func (r *roomDatabase) AddOnlineUsersCache(ctx context.Context, userID string) error {
	return r.cache.AddOnlineUsersCache(ctx, userID)
}

// DelOnlineUsersCache 删除在线用户缓存
func (r *roomDatabase) DelOnlineUsersCache(ctx context.Context, userID string) error {
	return r.cache.DelOnlineUsersCache(ctx, userID)
}
