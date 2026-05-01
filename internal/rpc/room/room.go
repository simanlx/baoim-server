package room

import (
	"BaoIM-Server/pkg/authverify"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/convert"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"
	"BaoIM-Server/pkg/common/db/mgo"
	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"BaoIM-Server/pkg/common/db/unrelation"
	"BaoIM-Server/pkg/rpcclient"
	"BaoIM-Server/pkg/rpcclient/notification"
	"baoim/protocol/constant"
	pbgroup "baoim/protocol/group"
	pbroom "baoim/protocol/room"
	"baoim/protocol/sdkws"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/mw/specialerror"
	"baoim/tools/tx"
	"baoim/tools/utils"
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func Start(config *config.GlobalConfig, client discoveryregistry.SvcDiscoveryRegistry, server *grpc.Server) error {
	mongo, err := unrelation.NewMongo(config)
	if err != nil {
		return err
	}
	rdb, err := cache.NewRedis(config)
	if err != nil {
		return err
	}
	groupDB, err := mgo.NewGroupMongo(mongo.GetDatabase(config.Mongo.Database))
	if err != nil {
		return err
	}
	groupMemberDB, err := mgo.NewGroupMember(mongo.GetDatabase(config.Mongo.Database))
	if err != nil {
		return err
	}
	database := controller.NewRoomDatabase(rdb, groupDB, groupMemberDB, tx.NewMongo(mongo.GetClient()))
	userRpcClient := rpcclient.NewUserRpcClient(client, config)
	msgRpcClient := rpcclient.NewMessageRpcClient(client, config)
	groupRpcClient := rpcclient.NewGroupRpcClient(client, config)
	conversationRpcClient := rpcclient.NewConversationRpcClient(client, config)
	var gs roomServer
	gs.db = database
	gs.rdb = rdb
	gs.config = config
	gs.groupRpcClient = groupRpcClient
	gs.User = userRpcClient
	gs.conversationRpcClient = conversationRpcClient

	gs.Notification = notification.NewRoomNotificationSender(database, &msgRpcClient, &userRpcClient, config, func(ctx context.Context, userIDs []string) ([]notification.CommonUser, error) {
		users, err := userRpcClient.GetUsersInfo(ctx, userIDs)
		if err != nil {
			return nil, err
		}
		return utils.Slice(users, func(e *sdkws.UserInfo) notification.CommonUser { return e }), nil
	})

	gs.startOfflineCleaner()
	// 初始化用户定时器映射
	//gs.userTimers = make(map[string]*userTimerInfo)
	pbroom.RegisterRoomServer(server, &gs)
	return nil
}

// 定义用户定时器信息结构，存储定时器和关联的房间ID
//type userTimerInfo struct {
//	Timer  *time.Timer // 定时器实例
//	RoomID string      // 关联的房间ID
//}

type roomServer struct {
	rdb                   redis.UniversalClient
	db                    controller.RoomDatabase
	User                  rpcclient.UserRpcClient
	Notification          *notification.RoomNotificationSender
	conversationRpcClient rpcclient.ConversationRpcClient
	//conversationRpcClient rpcclient.ConversationRpcClient
	//msgRpcClient          rpcclient.MessageRpcClient
	groupRpcClient rpcclient.GroupRpcClient
	config         *config.GlobalConfig
}

// GetRoomList 获取聊天室房间列表
func (r roomServer) GetRoomList(ctx context.Context, req *pbroom.GetRoomListReq) (*pbroom.GetRoomListResp, error) {
	if req.PageNumber == 0 || req.ShowNumber == 0 {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	list, err := r.db.GetRoomList(ctx, req.PageNumber, req.ShowNumber)
	if err != nil {
		return nil, err
	}

	//println(list.Rooms[0].RoomID)
	return list, nil
}

// GetRoomInfo 获取聊天室信息
func (r roomServer) GetRoomInfo(ctx context.Context, req *pbroom.GetRoomInfoReq) (*pbroom.GetRoomInfoResp, error) {
	if req.RoomID == "" {
		return nil, errs.Wrap(errors.New("parameter error"))
	}
	// 创建返回的响应结构体
	resp := &pbroom.GetRoomInfoResp{}
	// 检查请求的群组ID列表是否为空
	if len(req.RoomID) == 0 {
		return nil, errs.ErrArgs.Wrap("groupID is empty") // 群组ID为空，返回参数错误
	}
	// 从数据库查找所有请求的群组信息
	info, err := r.db.GetRoomInfo(ctx, req.RoomID)
	if err != nil {
		return nil, errs.ErrArgs.Wrap(err.Error())
	}
	resp.RoomInfo = info

	// 返回群组信息列表响应
	return resp, nil
}

// CloseHistoryRoom 关闭用户历史房间  如果是群主 则解散群组  如果是普通成员 则退出群组
func (s *roomServer) CloseHistoryRoom(ctx context.Context, userID string) error {
	roomID, isOwner, err := s.db.GetUserRoom(ctx, userID)
	if err != nil {
		return err
	}
	if roomID != "" {
		if isOwner {
			// 解散群组
			_, err = s.DismissRoom(ctx, &pbgroup.DismissGroupReq{
				GroupID:      roomID,
				DeleteMember: false,
			})
			if err != nil {
				return err
			}
		} else {
			// 退出群组
			_, err = s.QuitRoom(ctx, &pbgroup.QuitGroupReq{
				GroupID: roomID,
				UserID:  userID,
			})
			if err != nil {
				return err
			}

		}

	}
	return nil
}

// CreateRoom 创建聊天室
func (s *roomServer) CreateRoom(ctx context.Context, req *pbgroup.CreateGroupReq) (*pbgroup.CreateGroupResp, error) {
	// 校验群类型是否合法，仅支0 类型
	if req.GroupInfo.GroupType != constant.NormalGroup {
		return nil, errs.ErrArgs.Wrap(fmt.Sprintf("group type only supports %d", constant.NormalGroup))
	}
	// 校验群主是否为空
	if req.OwnerUserID == "" {
		return nil, errs.ErrArgs.Wrap("no group owner")
	}

	//创建前判断是否加入及创建了其他房间
	if err := s.CloseHistoryRoom(ctx, req.OwnerUserID); err != nil {
		return nil, err
	}

	// 校验群主的权限  是系统配置（config）中定义的管理员（Manager.UserID） //是系统配置中定义的 IM 管理员（IMAdmin.UserID） //是资源的所有者（ownerUserID）
	if err := authverify.CheckAccessV3(ctx, req.OwnerUserID, s.config); err != nil {
		return nil, err
	}

	// 获取管理员信息
	userInfo, err := s.User.GetUserInfo(ctx, req.OwnerUserID)
	if err != nil {
		return nil, err
	}

	// 转换群信息为数据库模型
	group := convert.Pb2DBGroupInfo(req.GroupInfo)
	// 生成群ID
	if err := s.GenGroupID(ctx, &group.GroupID); err != nil {
		return nil, err
	}

	groupMember := &relationtb.GroupMemberModel{
		GroupID:        group.GroupID,   // 群ID
		UserID:         req.OwnerUserID, // 用户ID
		Nickname:       userInfo.Nickname,
		FaceURL:        userInfo.FaceURL,
		RoleLevel:      constant.GroupOwner,       // 角色等级
		OperatorUserID: req.OwnerUserID,           // 操作者ID
		JoinSource:     constant.JoinByInvitation, // 加群方式
		InviterUserID:  req.OwnerUserID,           // 邀请人ID
		JoinTime:       time.Now(),                // 加群时间
		MuteEndTime:    time.UnixMilli(0),         // 禁言结束时间，默认不禁言
	}

	// 在数据库中创建群及成员
	if err := s.db.CreateRoom(ctx, []*relationtb.GroupModel{group}, []*relationtb.GroupMemberModel{groupMember}); err != nil {
		return nil, err
	}
	///创建群组是  把房间 \]房间列表 \用户关联房间 写到redis缓存
	if err := s.db.AddRoomList(ctx, &sdkws.RoomInfo{
		RoomID: group.GroupID,
		Uid:    group.CreatorUserID,
		Name:   group.GroupName,
		Img:    group.FaceURL,
		Ms:     []string{"", "", "", "", "", "", "", ""},
		Score:  50,
	}); err != nil {
		return nil, err
	}
	// 构造返回体响应
	resp := &pbgroup.CreateGroupResp{GroupInfo: &sdkws.GroupInfo{}}
	// 转换数据库群信息为PB结构体
	resp.GroupInfo = convert.Db2PbGroupInfo(group, req.OwnerUserID, 1)
	// 设置群成员数量
	resp.GroupInfo.MemberCount = 1
	// 构造群创建提示
	// 普通群发送群创建通知
	tips := &sdkws.GroupCreatedTips{
		Group:          resp.GroupInfo,
		OperationTime:  group.CreateTime.UnixMilli(),
		GroupOwnerUser: s.groupMemberDB2PB(groupMember, userInfo.AppMangerLevel),
	}
	tips.MemberList = append(tips.MemberList, s.groupMemberDB2PB(groupMember, userInfo.AppMangerLevel))
	tips.OpUser = s.groupMemberDB2PB(groupMember, userInfo.AppMangerLevel)

	s.Notification.RoomCreatedNotification(ctx, tips)

	// 返回响应
	return resp, nil
}

// DismissRoom 解散聊天室  会执行两次 先执行一次 然后通知完之后再执行一次
func (s *roomServer) DismissRoom(ctx context.Context, req *pbgroup.DismissGroupReq) (*pbgroup.DismissGroupResp, error) {
	defer log.ZInfo(ctx, "DismissRoom.return") // 方法返回时记录日志

	println("剑三了")

	resp := &pbgroup.DismissGroupResp{}                // 创建返回响应对象
	owner, err := s.db.TakeRoomOwner(ctx, req.GroupID) // 查询群主信息
	if err != nil {
		return nil, err // 查询群主失败返回错误
	}
	println("发的点点滴滴222", owner.UserID, mcontext.GetOpUserID(ctx))
	if !authverify.IsAppManagerUid(ctx, s.config) { // 判断操作人是否为App管理员
		if owner.UserID != mcontext.GetOpUserID(ctx) { // 如果不是管理员则判断是否为群主
			return nil, errs.ErrNoPermission.Wrap("not group owner") // 不是群主无权限
		}
	}
	println("发的点点滴滴11")
	group, err := s.db.TakeRoom(ctx, req.GroupID) // 查询群组详情
	if err != nil {
		return nil, err // 查询失败返回错误
	}
	if !req.DeleteMember && group.Status == constant.GroupStatusDismissed { // 如果不删除成员且群已解散
		return nil, errs.ErrDismissedAlready.Wrap("group status is dismissed") // 群已解散，返回已解散错误
	}
	println("req.DeleteMember", req.DeleteMember)
	if !req.DeleteMember { // 如果不删除群成员
		num, err := s.db.FindRoomMemberNum(ctx, req.GroupID) // 查询群成员数量
		if err != nil {
			return nil, err // 查询失败返回错误
		}

		println("发的66666点点滴滴")
		tips := &sdkws.GroupDismissedTips{
			Group:  s.roomDB2PB(group, owner.UserID, num), // 构建群信息
			OpUser: &sdkws.GroupMemberFullInfo{},          // 操作人信息
		}
		if mcontext.GetOpUserID(ctx) == owner.UserID { // 如果操作人为群主
			tips.OpUser = s.groupMemberDB2PB(owner, 0) // 填充群主信息
		}
		println("发的点点滴滴44444444")
		membersIDs, err := s.db.FindRoomMemberUserID(ctx, group.GroupID) // 查询群成员ID列表
		if err != nil {
			return nil, err // 查询失败返回错误
		}

		//if len(membersIDs) > 0 {
		//
		//	//批量清理用户房间信息
		//	err := s.db.BatchDelRoomUser(ctx, membersIDs)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

		//把管理员增加到用户列表  确保管理员也会被删除
		//membersIDs = append(membersIDs, owner.UserID)

		s.db.DelRoom(ctx, membersIDs, group.GroupID)
		_ = s.Notification.RoomDismissedNotification(ctx, tips) // 发送聊天室解散通知

	} else {

		membersIDs, err := s.db.FindRoomMemberUserID(ctx, group.GroupID) // 查询群成员ID列表
		if err != nil {
			return nil, err // 查询失败返回错误
		}
		//延时 删除聊天室会话
		err = s.conversationRpcClient.DismissRoomDeleteConversation(ctx, req.GroupID, membersIDs)
		if err != nil {
			return nil, err
		}
	}

	if err := s.db.DismissRoom(ctx, req.GroupID, req.DeleteMember); err != nil { // 执行解散群操作
		return nil, err // 解散失败返回错误
	}

	println("发的点点滴滴")
	//
	//reqCall := &callbackstruct.CallbackDisMissGroupReq{
	//	GroupID:   req.GroupID,             // 群组ID
	//	OwnerID:   owner.UserID,            // 群主ID
	//	MembersID: membersID,               // 所有群成员ID
	//	GroupType: string(group.GroupType), // 群类型
	//}
	//if err := CallbackDismissGroup(ctx, s.config, reqCall); err != nil { // 回调通知业务方群解散
	//	return nil, err // 回调失败返回错
	return resp, nil // 返回响应
}

// JoinRoom 加入聊天室
func (s *roomServer) JoinRoom(ctx context.Context, req *pbgroup.JoinGroupReq) (resp *pbroom.JoinRoomResp, err error) {
	// 函数返回时记录日志
	defer log.ZInfo(ctx, "JoinRoom.Return")
	// 获取邀请人的用户信息
	user, err := s.User.GetUserInfo(ctx, req.InviterUserID)
	if err != nil {
		return nil, err // 查询用户失败，返回错误
	}
	//加入群组前退出 其他加入或创建的房间
	if err := s.CloseHistoryRoom(ctx, req.InviterUserID); err != nil {
		return nil, err
	}
	// 获取群组信息
	group, err := s.db.TakeRoom(ctx, req.GroupID)
	if err != nil {
		return nil, err // 查询群组失败，返回错误
	}
	// 判断群组是否已经被解散
	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap() // 已解散，返回错误
	}
	// 检查用户是否已经在群组中
	_, err = s.db.TakeRoomMember(ctx, req.GroupID, req.InviterUserID)
	if err == nil {
		return nil, errs.ErrArgs.Wrap("already in group") // 已在群组，返回错误
	} else if !s.IsNotFound(err) && utils.Unwrap(err) != errs.ErrRecordNotFound {
		return nil, err // 不是未找到记录的错误，直接返回错误
	}
	// 记录日志，展示群组信息及是否需要验证
	log.ZInfo(ctx, "JoinGroup.groupInfo", "group", group, "eq", group.NeedVerification == constant.Directly)
	resp = &pbroom.JoinRoomResp{}
	// 判断群组是否允许直接加入
	if group.NeedVerification == constant.Directly {
		// 构造群成员模型
		groupMember := &relationtb.GroupMemberModel{
			GroupID:        group.GroupID, // 群组ID
			UserID:         user.UserID,   // 用户ID
			Nickname:       user.Nickname,
			FaceURL:        user.FaceURL,
			RoleLevel:      constant.GroupOrdinaryUsers, // 普通成员
			OperatorUserID: mcontext.GetOpUserID(ctx),   // 操作人ID
			InviterUserID:  req.InviterUserID,           // 邀请人ID
			JoinTime:       time.Now(),                  // 加入时间
			MuteEndTime:    time.UnixMilli(0),           // 禁言结束时间
		}

		///先缓存加入房间 如果无错误在向下执行  房间满返回错误
		JoinMap, err := s.db.JoinRoomList(ctx, group.GroupID, user.UserID, user.FaceURL)
		if err != nil {
			return nil, err
		}
		//更新用户关联房间信息
		err = s.db.AddUserRoom(ctx, user.UserID, group.GroupID, false)
		if err != nil {
			return nil, err
		}
		// 创建群组成员
		if err := s.db.CreateRoom(ctx, nil, []*relationtb.GroupMemberModel{groupMember}); err != nil {
			return nil, err // 创建失败，返回错误
		}

		//创建群聊会话
		if err := s.conversationRpcClient.RoomGroupChatFirstCreateConversation(ctx, req.GroupID, []string{req.InviterUserID}); err != nil {
			return nil, err // 创建会话失败，返回错误
		}

		//查询群组成员
		_, members, err1 := s.db.PageGetRoomMember(ctx, group.GroupID, &sdkws.RequestPagination{
			PageNumber: 1,
			ShowNumber: 8,
		})
		// 检查数据库查询是否出错
		if err1 != nil {
			return nil, err
		}
		resp = &pbroom.JoinRoomResp{
			Rooms:   JoinMap,
			Members: utils.Batch(convert.Db2PbGroupMember, members), // 将数据库模型批量转换为protobuf模型并赋值给响应
		}

		s.Notification.RoomMemberEnterNotification(ctx, req.GroupID, req.InviterUserID, resp.Rooms)

		// 返回成功响应
		return resp, nil
	}

	return resp, nil
}

// QuitRoom 退出聊天室
func (s *roomServer) QuitRoom(ctx context.Context, req *pbgroup.QuitGroupReq) (*pbgroup.QuitGroupResp, error) {
	resp := &pbgroup.QuitGroupResp{}
	if req.UserID == "" {
		req.UserID = mcontext.GetOpUserID(ctx)
	} else {
		if err := authverify.CheckAccessV3(ctx, req.UserID, s.config); err != nil {
			return nil, err
		}
	}

	// 退出时  清除用户关联房间信息
	err := s.db.DelUserRoom(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	member, err := s.db.TakeRoomMember(ctx, req.GroupID, req.UserID)
	if err != nil {
		return nil, err
	}
	if member.RoleLevel == constant.GroupOwner {
		return nil, errs.ErrNoPermission.Wrap("group owner can't quit")
	}

	//在mdb 中删除用户  及用户缓存
	err = s.db.DeleteRoomMember(ctx, req.GroupID, []string{req.UserID})
	if err != nil {
		return nil, err
	}

	//退出房间 清除当前uid及其头像URL（并发安全）
	err = s.db.RemoveRoomUser(ctx, req.GroupID, req.UserID, member.FaceURL)
	if err != nil {
		return nil, errs.ErrArgs.Wrap(err.Error())
	}

	_ = s.Notification.RoomMemberQuitNotification(ctx, s.groupMemberDB2PB(member, 0))
	//删除 会话
	s.conversationRpcClient.DeleteUserRoomConversation(ctx, req.GroupID, req.UserID)
	return resp, nil
}

// 踢出聊天室
func (s *roomServer) KickRoomMember(ctx context.Context, req *pbgroup.KickGroupMemberReq) (*pbgroup.KickGroupMemberResp, error) {
	// 初始化响应对象
	resp := &pbgroup.KickGroupMemberResp{}
	// 被踢时  清除用户关联房间信息
	err := s.db.DelUserRoom(ctx, req.KickedUserIDs[0])
	// 从数据库获取群组信息
	group, err := s.db.TakeRoom(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}

	// 检查被踢用户ID列表是否为空
	if len(req.KickedUserIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("KickedUserIDs empty")
	}

	// 检查被踢用户ID列表是否有重复
	if utils.IsDuplicateStringSlice(req.KickedUserIDs) {
		return nil, errs.ErrArgs.Wrap("KickedUserIDs duplicate")
	}
	// 从上下文中获取操作人用户ID
	opUserID := mcontext.GetOpUserID(ctx)

	// 检查操作人是否在被踢用户列表中（不允许自己踢自己）
	if utils.IsContain(opUserID, req.KickedUserIDs) {
		return nil, errs.ErrArgs.Wrap("opUserID in KickedUserIDs")
	}

	// 从数据库查询被踢用户和操作人的群成员信息
	members, err := s.db.FindRoomMembers(ctx, req.GroupID, append(req.KickedUserIDs, opUserID))
	if err != nil {
		return nil, err
	}

	// 填充群成员的详细信息
	//if err := s.PopulateGroupMember(ctx, members...); err != nil {
	//	return nil, err
	//}

	// 将群成员信息存入map，便于快速查询
	memberMap := make(map[string]*relationtb.GroupMemberModel)
	for i, member := range members {
		memberMap[member.UserID] = members[i]
	}

	// 检查操作人是否为应用管理员
	isAppManagerUid := authverify.IsAppManagerUid(ctx, s.config)

	// 获取操作人的群成员信息
	opMember := memberMap[opUserID]

	// 遍历被踢用户列表，检查权限
	for _, userID := range req.KickedUserIDs {
		// 检查被踢用户是否为群成员
		member, ok := memberMap[userID]
		if !ok {
			return nil, errs.ErrUserIDNotFound.Wrap(userID)
		}

		// 如果不是应用管理员，则需要检查群内权限
		if !isAppManagerUid {
			// 检查操作人是否为群成员
			if opMember == nil {
				return nil, errs.ErrNoPermission.Wrap("opUserID no in group")
			}
			// 根据操作人的角色级别判断是否有权限踢人
			switch opMember.RoleLevel {
			case constant.GroupOwner:
				// 群主拥有踢人权限，无需额外检查
			case constant.GroupAdmin:
				// 群管理员不能踢群主或其他管理员
				if member.RoleLevel == constant.GroupOwner || member.RoleLevel == constant.GroupAdmin {
					return nil, errs.ErrNoPermission.Wrap("group admins cannot remove the group owner and other admins")
				}
			case constant.GroupOrdinaryUsers:
				// 普通成员没有踢人权限
				return nil, errs.ErrNoPermission.Wrap("opUserID no permission")
			default:
				// 未知角色级别，无权限
				return nil, errs.ErrNoPermission.Wrap("opUserID roleLevel unknown")
			}
		}

		// 退出时  清除用户关联房间信息
		err := s.db.DelUserRoom(ctx, userID)
		if err != nil {
			return nil, err
		}

	}
	// 获取群成员总数
	num, err := s.db.FindRoomMemberNum(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}

	// 获取群主的用户ID列表
	ownerUserIDs, err := s.db.GetRoomRoleLevelMemberIDs(ctx, req.GroupID, constant.GroupOwner)
	if err != nil {
		return nil, err
	}
	// 取第一个群主ID（通常群只有一个群主）
	var ownerUserID string
	if len(ownerUserIDs) > 0 {
		ownerUserID = ownerUserIDs[0]
	}

	// 从数据库中删除被踢的群成员
	if err := s.db.DeleteRoomMember(ctx, group.GroupID, req.KickedUserIDs); err != nil {
		return nil, err
	}
	///在数据库中删除 被踢用户的聊天室信息  不做错无处理
	_ = s.db.KickRemoveRoomUser(ctx, group.GroupID, memberMap[req.KickedUserIDs[0]].UserID, memberMap[req.KickedUserIDs[0]].FaceURL)
	//通知rpc 更新踢出用户在线信息
	//_ = s.roomRpcClient.UpdateRoomUser(ctx, memberMap[req.KickedUserIDs[0]].UserID, "", false)

	// 构建成员被踢的通知信息
	tips := &sdkws.MemberKickedTips{
		Group: &sdkws.GroupInfo{
			GroupID:                group.GroupID,
			GroupName:              group.GroupName,
			Notification:           group.Notification,
			Introduction:           group.Introduction,
			FaceURL:                group.FaceURL,
			OwnerUserID:            ownerUserID,
			CreateTime:             group.CreateTime.UnixMilli(),
			MemberCount:            num,
			Ex:                     group.Ex,
			Status:                 group.Status,
			CreatorUserID:          group.CreatorUserID,
			GroupType:              group.GroupType,
			NeedVerification:       group.NeedVerification,
			LookMemberInfo:         group.LookMemberInfo,
			ApplyMemberFriend:      group.ApplyMemberFriend,
			NotificationUpdateTime: group.NotificationUpdateTime.UnixMilli(),
			NotificationUserID:     group.NotificationUserID,
		},
		KickedUserList: []*sdkws.GroupMemberFullInfo{},
	}

	// 设置操作人的信息到通知中
	if opMember, ok := memberMap[opUserID]; ok {
		tips.OpUser = convert.Db2PbGroupMember(opMember)
	}

	// 添加被踢用户的信息到通知中
	for _, userID := range req.KickedUserIDs {
		tips.KickedUserList = append(tips.KickedUserList, convert.Db2PbGroupMember(memberMap[userID]))
	}

	// 发送成员被踢的通知  //注意
	s.Notification.RoomMemberKickedNotification(ctx, tips)

	//删除 会话
	s.conversationRpcClient.DeleteUserRoomConversation(ctx, req.GroupID, req.KickedUserIDs[0])

	// 删除被踢成员并更新会话序列号 //注意
	//if err := s.deleteRoomMemberAndSetConversationSeq(ctx, req.GroupID, req.KickedUserIDs); err != nil {
	//	return nil, err
	//}

	// 返回成功响应
	return resp, nil
}

// MuteGroupMember 禁言聊天室成员
func (s *roomServer) MuteRoomMember(ctx context.Context, req *pbgroup.MuteGroupMemberReq) (*pbgroup.MuteGroupMemberResp, error) {
	// 初始化禁言响应对象
	resp := &pbgroup.MuteGroupMemberResp{}

	// 从数据库中获取要禁言的群成员信息
	member, err := s.db.TakeRoomMember(ctx, req.GroupID, req.UserID)
	if err != nil {
		return nil, err // 获取成员信息失败时返回错误
	}

	// 填充群成员的详细信息（可能包括额外的关联数据）
	//if err := s.PopulateGroupMember(ctx, member); err != nil {
	//	return nil, err // 填充信息失败时返回错误
	//}

	// 检查操作人是否为应用管理员（非应用管理员需要进一步权限检查）
	if !authverify.IsAppManagerUid(ctx, s.config) {
		// 获取操作人的群成员信息
		opMember, err := s.db.TakeRoomMember(ctx, req.GroupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return nil, err // 获取操作人信息失败时返回错误
		}

		// 根据被禁言成员的角色级别进行权限校验
		switch member.RoleLevel {
		case constant.GroupOwner:
			// 群主不能被禁言，返回无权限错误
			return nil, errs.ErrNoPermission.Wrap("set group owner mute")
		case constant.GroupAdmin:
			// 只有群主可以禁言管理员
			if opMember.RoleLevel != constant.GroupOwner {
				return nil, errs.ErrNoPermission.Wrap("set group admin mute")
			}
		case constant.GroupOrdinaryUsers:
			// 只有群主或管理员可以禁言普通成员
			if !(opMember.RoleLevel == constant.GroupAdmin || opMember.RoleLevel == constant.GroupOwner) {
				return nil, errs.ErrNoPermission.Wrap("set group ordinary users mute")
			}
		}
	}

	// 计算禁言结束时间（当前时间 + 请求的禁言秒数），并生成更新数据
	data := map[string]any{
		"mute_end_time": time.Now().Add(time.Second * time.Duration(req.MutedSeconds)),
	}
	// 更新数据库中该成员的禁言时间
	if err := s.db.UpdateRoomMember(ctx, member.GroupID, member.UserID, data); err != nil {
		return nil, err // 更新禁言时间失败时返回错误
	}
	// 发送群成员被禁言的通知
	s.Notification.RoomMemberMutedNotification(ctx, req.GroupID, req.UserID, req.MutedSeconds)

	// 返回禁言成功的响应
	return resp, nil
}

// CancelMuteRoomMember 取消禁言聊天室成员
func (s *roomServer) CancelMuteRoomMember(ctx context.Context, req *pbgroup.CancelMuteGroupMemberReq) (*pbgroup.CancelMuteGroupMemberResp, error) {
	// 从数据库查询目标群成员（被取消禁言的用户）的基础信息
	// req.GroupID：目标群的唯一标识
	// req.UserID：被取消禁言的用户唯一标识
	member, err := s.db.TakeRoomMember(ctx, req.GroupID, req.UserID)
	// 若查询数据库失败（如网络错误、数据不存在），直接返回错误
	if err != nil {
		return nil, err
	}

	//// 补充目标群成员的完整信息（如角色、权限等基础查询未返回的字段）
	//if err := s.PopulateGroupMember(ctx, member); err != nil {
	//	return nil, err
	//}

	// 权限校验：判断当前操作人是否为「应用管理员」（超管权限，跳过后续群内角色校验）
	// IsAppManagerUid：检查操作人UID是否在应用管理员名单中
	// s.config：服务配置，存储应用管理员信息等
	if !authverify.IsAppManagerUid(ctx, s.config) {
		// 非应用管理员，需查询「操作人」在当前群内的成员信息
		// mcontext.GetOpUserID(ctx)：从上下文获取操作人的用户ID
		opMember, err := s.db.TakeRoomMember(ctx, req.GroupID, mcontext.GetOpUserID(ctx))
		// 若查询操作人信息失败，返回错误
		if err != nil {
			return nil, err
		}

		// 根据「被取消禁言用户」的群内角色，判断操作人是否有权限
		switch member.RoleLevel {
		//  case 1：被取消禁言的是「群主」
		case constant.GroupOwner:
			// 群主不可被取消禁言（逻辑上群主默认无禁言状态，或不允许操作群主），返回无权限错误
			return nil, errs.ErrNoPermission.Wrap("set group owner mute")

		//  case 2：被取消禁言的是「群管理员」
		case constant.GroupAdmin:
			// 仅群主有权取消群管理员的禁言，若操作人不是群主，返回无权限错误
			if opMember.RoleLevel != constant.GroupOwner {
				return nil, errs.ErrNoPermission.Wrap("set group admin mute")
			}

		//  case 3：被取消禁言的是「普通群成员」
		case constant.GroupOrdinaryUsers:
			// 仅群主或群管理员有权取消普通成员的禁言，否则返回无权限错误
			if !(opMember.RoleLevel == constant.GroupAdmin || opMember.RoleLevel == constant.GroupOwner) {
				return nil, errs.ErrNoPermission.Wrap("set group ordinary users mute")
			}
		}
	}

	// 构造更新数据：将群成员的禁言时间设置为「Unix时间原点」（表示取消禁言）
	// time.Unix(0, 0)：对应时间 1970-01-01 00:00:00 UTC，代表无禁言状态
	data := map[string]any{
		"mute_end_time": time.Unix(0, 0),
	}

	// 调用数据库接口，更新目标群成员的禁言时间（执行取消禁言操作）
	if err := s.db.UpdateRoomMember(ctx, member.GroupID, member.UserID, data); err != nil {
		return nil, err
	}

	// 发送「取消群成员禁言」的通知（如推送消息给群内成员或被取消禁言的用户）
	s.Notification.RoomMemberCancelMutedNotification(ctx, req.GroupID, req.UserID)

	// 取消禁言操作成功，返回空响应（无额外业务数据需返回）
	return &pbgroup.CancelMuteGroupMemberResp{}, nil
}

func (s *roomServer) IsNotFound(err error) bool {
	return errs.ErrRecordNotFound.Is(specialerror.ErrCode(errs.Unwrap(err)))
}
func (s *roomServer) GenGroupID(ctx context.Context, groupID *string) error {
	if *groupID != "" {
		_, err := s.db.TakeRoom(ctx, *groupID)
		if err == nil {
			return errs.ErrGroupIDExisted.Wrap("group id existed " + *groupID)
		} else if s.IsNotFound(err) {
			return nil
		} else {
			return err
		}
	}
	for i := 0; i < 10; i++ {
		id := utils.Md5(strings.Join([]string{mcontext.GetOperationID(ctx), strconv.FormatInt(time.Now().UnixNano(), 10), strconv.Itoa(rand.Int())}, ",;,"))
		bi := big.NewInt(0)
		bi.SetString(id[0:8], 16)
		id = bi.String()
		_, err := s.db.TakeRoom(ctx, id)
		if err == nil {
			continue
		} else if s.IsNotFound(err) {
			*groupID = id
			return nil
		} else {
			return err
		}
	}
	return errs.ErrData.Wrap("group id gen error")
}

// AddOnlineUser 添加用户为在线状态
func (r roomServer) AddOnlineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {
	err := r.db.AddOnlineUsersCache(ctx, req.UserID)
	if err != nil {
		return nil, err
	}

	return &pbroom.OnlineUserResp{}, nil
}

// DelOnlineUser 删除用户为在线状态
func (r roomServer) DelOnlineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {
	err := r.db.DelOnlineUsersCache(ctx, req.UserID)
	if err != nil {
		return nil, err
	}

	return &pbroom.OnlineUserResp{}, nil
}

// 用户离线时触发，检查用户是否在房间内，如果在房间内则添加到离线列表，否则直接在在线聊表删除
func (r roomServer) CleanOfflineUser(ctx context.Context, req *pbroom.OnlineUserReq) (*pbroom.OnlineUserResp, error) {
	//在在线列表中删除  如果用户有房间信息 则添加到离线列表等待轮训
	err := r.db.AddOfflineUser(ctx, req.UserID)
	if err != nil {
		return nil, err
	}

	return &pbroom.OnlineUserResp{}, nil
}

// 启动离线用户清理定时器
func (r *roomServer) startOfflineCleaner() {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {

		for range ticker.C {
			println("轮训执行")

			ctx := context.Background()
			offlineUsers, err := r.db.CleanOfflineUsers(ctx)
			if err != nil {
				log.ZError(ctx, "clean offline users failed", err)
				continue
			}

			ctx = mcontext.SetOperationID(ctx, strconv.Itoa(rand.Int()))
			// 处理有房间ID的离线用户
			for _, room := range offlineUsers {
				println("轮训", room["roomID"])
				if room["roomID"] != "" {
					ctx = mcontext.WithOpUserIDContext(ctx, room["userID"])
					//ctx = mcontext.WithOpUserIDContext(ctx, p.config.Manager.UserID[0])
					//ctx = mcontext.WithOpUserIDContext(ctx, p.config.IMAdmin.UserID[0])
					// 处理有房间ID的离线用户
					if room["isOwner"] == "0" {
						println("退出群组")
						//解散群组 忽略错误
						r.QuitRoom(ctx, &pbgroup.QuitGroupReq{
							GroupID: room["roomID"],
							UserID:  room["userID"],
						})

					} else {

						println("执行解散群组")

						_, err := r.DismissRoom(ctx, &pbgroup.DismissGroupReq{
							GroupID:      room["roomID"],
							DeleteMember: false, ///这里false   //  发完通知完成之后会删除
						})
						//泽丽回头取消
						if err != nil {
							log.ZError(ctx, "dismiss room failed", err)
							continue
						}

					}
				}
				//
			}
		}
	}()
}
