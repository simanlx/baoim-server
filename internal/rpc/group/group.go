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

package group

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"BaoIM-Server/pkg/authverify"
	"BaoIM-Server/pkg/callbackstruct"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/convert"
	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/controller"
	"BaoIM-Server/pkg/common/db/mgo"
	relationtb "BaoIM-Server/pkg/common/db/table/relation"
	"BaoIM-Server/pkg/common/db/unrelation"
	"BaoIM-Server/pkg/msgprocessor"
	"BaoIM-Server/pkg/rpcclient"
	"BaoIM-Server/pkg/rpcclient/grouphash"
	"BaoIM-Server/pkg/rpcclient/notification"
	"baoim/protocol/constant"
	pbconversation "baoim/protocol/conversation"
	pbgroup "baoim/protocol/group"
	"baoim/protocol/sdkws"
	"baoim/protocol/wrapperspb"
	"baoim/tools/discoveryregistry"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/mw/specialerror"
	"baoim/tools/tx"
	"baoim/tools/utils"
	"google.golang.org/grpc"
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
	groupRequestDB, err := mgo.NewGroupRequestMgo(mongo.GetDatabase(config.Mongo.Database))
	if err != nil {
		return err
	}
	userRpcClient := rpcclient.NewUserRpcClient(client, config)
	msgRpcClient := rpcclient.NewMessageRpcClient(client, config)
	conversationRpcClient := rpcclient.NewConversationRpcClient(client, config)
	var gs groupServer
	database := controller.NewGroupDatabase(rdb, groupDB, groupMemberDB, groupRequestDB, tx.NewMongo(mongo.GetClient()), grouphash.NewGroupHashFromGroupServer(&gs))
	gs.db = database
	gs.User = userRpcClient
	gs.Notification = notification.NewGroupNotificationSender(database, &msgRpcClient, &userRpcClient, config, func(ctx context.Context, userIDs []string) ([]notification.CommonUser, error) {
		users, err := userRpcClient.GetUsersInfo(ctx, userIDs)
		if err != nil {
			return nil, err
		}
		return utils.Slice(users, func(e *sdkws.UserInfo) notification.CommonUser { return e }), nil
	})
	gs.conversationRpcClient = conversationRpcClient
	gs.msgRpcClient = msgRpcClient
	gs.config = config
	pbgroup.RegisterGroupServer(server, &gs)
	return nil
}

type groupServer struct {
	db                    controller.GroupDatabase
	User                  rpcclient.UserRpcClient
	Notification          *notification.GroupNotificationSender
	conversationRpcClient rpcclient.ConversationRpcClient
	msgRpcClient          rpcclient.MessageRpcClient
	config                *config.GlobalConfig
}

func (s *groupServer) GetJoinedGroupIDs(ctx context.Context, req *pbgroup.GetJoinedGroupIDsReq) (*pbgroup.GetJoinedGroupIDsResp, error) {
	//TODO implement me
	panic("implement me")
}

func (s *groupServer) NotificationUserInfoUpdate(ctx context.Context, req *pbgroup.NotificationUserInfoUpdateReq) (*pbgroup.NotificationUserInfoUpdateResp, error) {
	members, err := s.db.FindGroupMemberUser(ctx, nil, req.UserID)
	if err != nil {
		return nil, err
	}
	groupIDs := make([]string, 0, len(members))
	for _, member := range members {
		if member.Nickname != "" && member.FaceURL != "" {
			continue
		}
		groupIDs = append(groupIDs, member.GroupID)
	}
	for _, groupID := range groupIDs {
		if err = s.Notification.GroupMemberInfoSetNotification(ctx, groupID, req.UserID); err != nil {
			return nil, err
		}
	}
	if err = s.db.DeleteGroupMemberHash(ctx, groupIDs); err != nil {
		return nil, err
	}

	return &pbgroup.NotificationUserInfoUpdateResp{}, nil
}

func (s *groupServer) CheckGroupAdmin(ctx context.Context, groupID string) error {
	if !authverify.IsAppManagerUid(ctx, s.config) {
		groupMember, err := s.db.TakeGroupMember(ctx, groupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return err
		}
		if !(groupMember.RoleLevel == constant.GroupOwner || groupMember.RoleLevel == constant.GroupAdmin) {
			return errs.ErrNoPermission.Wrap("no group owner or admin")
		}
	}
	return nil
}

func (s *groupServer) GetPublicUserInfoMap(ctx context.Context, userIDs []string, complete bool) (map[string]*sdkws.PublicUserInfo, error) {
	if len(userIDs) == 0 {
		return map[string]*sdkws.PublicUserInfo{}, nil
	}
	users, err := s.User.GetPublicUserInfos(ctx, userIDs, complete)
	if err != nil {
		return nil, err
	}
	return utils.SliceToMapAny(users, func(e *sdkws.PublicUserInfo) (string, *sdkws.PublicUserInfo) {
		return e.UserID, e
	}), nil
}

func (s *groupServer) IsNotFound(err error) bool {
	return errs.ErrRecordNotFound.Is(specialerror.ErrCode(errs.Unwrap(err)))
}

func (s *groupServer) GenGroupID(ctx context.Context, groupID *string) error {
	if *groupID != "" {
		_, err := s.db.TakeGroup(ctx, *groupID)
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
		_, err := s.db.TakeGroup(ctx, id)
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

func (s *groupServer) CreateGroup(ctx context.Context, req *pbgroup.CreateGroupReq) (*pbgroup.CreateGroupResp, error) {
	if req.GroupInfo.GroupType != constant.WorkingGroup {
		return nil, errs.ErrArgs.Wrap(fmt.Sprintf("group type only supports %d", constant.WorkingGroup))
	}
	if req.OwnerUserID == "" {
		return nil, errs.ErrArgs.Wrap("no group owner")
	}
	if err := authverify.CheckAccessV3(ctx, req.OwnerUserID, s.config); err != nil {
		return nil, err
	}
	userIDs := append(append(req.MemberUserIDs, req.AdminUserIDs...), req.OwnerUserID)
	opUserID := mcontext.GetOpUserID(ctx)
	if !utils.Contain(opUserID, userIDs...) {
		userIDs = append(userIDs, opUserID)
	}
	if utils.Duplicate(userIDs) {
		return nil, errs.ErrArgs.Wrap("group member repeated")
	}
	userMap, err := s.User.GetUsersInfoMap(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	if len(userMap) != len(userIDs) {
		return nil, errs.ErrUserIDNotFound.Wrap("user not found")
	}
	// Callback Before create Group
	if err := CallbackBeforeCreateGroup(ctx, s.config, req); err != nil {
		return nil, err
	}
	var groupMembers []*relationtb.GroupMemberModel
	group := convert.Pb2DBGroupInfo(req.GroupInfo)
	if err := s.GenGroupID(ctx, &group.GroupID); err != nil {
		return nil, err
	}
	joinGroup := func(userID string, roleLevel int32) error {
		groupMember := &relationtb.GroupMemberModel{
			GroupID:        group.GroupID,
			UserID:         userID,
			RoleLevel:      roleLevel,
			OperatorUserID: opUserID,
			JoinSource:     constant.JoinByInvitation,
			InviterUserID:  opUserID,
			JoinTime:       time.Now(),
			MuteEndTime:    time.UnixMilli(0),
		}
		if err := CallbackBeforeMemberJoinGroup(ctx, s.config, groupMember, group.Ex); err != nil {
			return err
		}
		groupMembers = append(groupMembers, groupMember)
		return nil
	}
	if err := joinGroup(req.OwnerUserID, constant.GroupOwner); err != nil {
		return nil, err
	}
	for _, userID := range req.AdminUserIDs {
		if err := joinGroup(userID, constant.GroupAdmin); err != nil {
			return nil, err
		}
	}
	for _, userID := range req.MemberUserIDs {
		if err := joinGroup(userID, constant.GroupOrdinaryUsers); err != nil {
			return nil, err
		}
	}
	if err := s.db.CreateGroup(ctx, []*relationtb.GroupModel{group}, groupMembers); err != nil {
		return nil, err
	}
	resp := &pbgroup.CreateGroupResp{GroupInfo: &sdkws.GroupInfo{}}
	resp.GroupInfo = convert.Db2PbGroupInfo(group, req.OwnerUserID, uint32(len(userIDs)))
	resp.GroupInfo.MemberCount = uint32(len(userIDs))
	tips := &sdkws.GroupCreatedTips{
		Group:          resp.GroupInfo,
		OperationTime:  group.CreateTime.UnixMilli(),
		GroupOwnerUser: s.groupMemberDB2PB(groupMembers[0], userMap[groupMembers[0].UserID].AppMangerLevel),
	}
	for _, member := range groupMembers {
		member.Nickname = userMap[member.UserID].Nickname
		tips.MemberList = append(tips.MemberList, s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel))
		if member.UserID == opUserID {
			tips.OpUser = s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel)
			break
		}
	}
	if req.GroupInfo.GroupType == constant.SuperGroup {
		go func() {
			for _, userID := range userIDs {
				s.Notification.SuperGroupNotification(ctx, userID, userID)
			}
		}()
	} else {
		// s.Notification.GroupCreatedNotification(ctx, group, groupMembers, userMap)
		tips := &sdkws.GroupCreatedTips{
			Group:          resp.GroupInfo,
			OperationTime:  group.CreateTime.UnixMilli(),
			GroupOwnerUser: s.groupMemberDB2PB(groupMembers[0], userMap[groupMembers[0].UserID].AppMangerLevel),
		}
		for _, member := range groupMembers {
			member.Nickname = userMap[member.UserID].Nickname
			tips.MemberList = append(tips.MemberList, s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel))
			if member.UserID == opUserID {
				tips.OpUser = s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel)
				break
			}
		}
		s.Notification.GroupCreatedNotification(ctx, tips)
	}
	reqCallBackAfter := &pbgroup.CreateGroupReq{
		MemberUserIDs: userIDs,
		GroupInfo:     resp.GroupInfo,
		OwnerUserID:   req.OwnerUserID,
		AdminUserIDs:  req.AdminUserIDs,
	}

	if err := CallbackAfterCreateGroup(ctx, s.config, reqCallBackAfter); err != nil {
		return nil, err
	}

	return resp, nil
}

// /创建聊天室
func (s *groupServer) CreateGroupRoom(ctx context.Context, req *pbgroup.CreateGroupReq) (*pbgroup.CreateGroupResp, error) {

	// 校验群类型是否合法，仅支0 类型
	if req.GroupInfo.GroupType != constant.NormalGroup {
		return nil, errs.ErrArgs.Wrap(fmt.Sprintf("group type only supports %d", constant.NormalGroup))
	}
	// 校验群主是否为空
	if req.OwnerUserID == "" {
		return nil, errs.ErrArgs.Wrap("no group owner")
	}
	// 校验群主的权限
	if err := authverify.CheckAccessV3(ctx, req.OwnerUserID, s.config); err != nil {
		return nil, err
	}
	// 汇总所有成员ID（成员、管理员、群主）
	userIDs := append(append(req.MemberUserIDs, req.AdminUserIDs...), req.OwnerUserID)
	// 获取操作人ID
	opUserID := mcontext.GetOpUserID(ctx)
	// 操作人不是群成员则加入
	if !utils.Contain(opUserID, userIDs...) {
		userIDs = append(userIDs, opUserID)
	}
	// 检查群成员是否有重复
	if utils.Duplicate(userIDs) {
		return nil, errs.ErrArgs.Wrap("group member repeated")
	}
	// 获取所有成员的用户信息映射
	userMap, err := s.User.GetUsersInfoMap(ctx, userIDs)
	if err != nil {
		return nil, err
	}

	// 校验所有成员都存在
	if len(userMap) != len(userIDs) {
		return nil, errs.ErrUserIDNotFound.Wrap("user not found")
	}
	// 创建群前回调（预处理逻辑）
	if err := CallbackBeforeCreateGroup(ctx, s.config, req); err != nil {
		return nil, err
	}
	// 群成员列表
	var groupMembers []*relationtb.GroupMemberModel
	// 转换群信息为数据库模型
	group := convert.Pb2DBGroupInfo(req.GroupInfo)
	// 生成群ID
	if err := s.GenGroupID(ctx, &group.GroupID); err != nil {
		return nil, err
	}
	// 内部函数：添加成员到群成员列表
	joinGroup := func(userID string, nickname string, faceURL string, roleLevel int32) error {
		groupMember := &relationtb.GroupMemberModel{
			GroupID:        group.GroupID, // 群ID
			UserID:         userID,        // 用户ID
			Nickname:       nickname,
			FaceURL:        faceURL,
			RoleLevel:      roleLevel,                 // 角色等级
			OperatorUserID: opUserID,                  // 操作者ID
			JoinSource:     constant.JoinByInvitation, // 加群方式
			InviterUserID:  opUserID,                  // 邀请人ID
			JoinTime:       time.Now(),                // 加群时间
			MuteEndTime:    time.UnixMilli(0),         // 禁言结束时间，默认不禁言
		}
		// 成员加入群前回调（预处理逻辑）
		if err := CallbackBeforeMemberJoinGroup(ctx, s.config, groupMember, group.Ex); err != nil {
			return err
		}
		// 添加到群成员列表
		groupMembers = append(groupMembers, groupMember)
		return nil
	}
	// 添加群主到群成员列表
	if err := joinGroup(req.OwnerUserID, userMap[req.OwnerUserID].Nickname, userMap[req.OwnerUserID].FaceURL, constant.GroupOwner); err != nil {
		return nil, err
	}
	// 添加管理员到群成员列表
	for _, userID := range req.AdminUserIDs {
		if err := joinGroup(userID, userMap[userID].UserID, userMap[userID].FaceURL, constant.GroupAdmin); err != nil {
			return nil, err
		}
	}
	// 添加普通成员到群成员列表
	for _, userID := range req.MemberUserIDs {
		if err := joinGroup(userID, userMap[userID].UserID, userMap[userID].FaceURL, constant.GroupOrdinaryUsers); err != nil {
			return nil, err
		}
	}
	// 在数据库中创建群及成员
	if err := s.db.CreateGroup(ctx, []*relationtb.GroupModel{group}, groupMembers); err != nil {
		return nil, err
	}

	///创建群组是  把房间缓存到redis 及排行 列表
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
	resp.GroupInfo = convert.Db2PbGroupInfo(group, req.OwnerUserID, uint32(len(userIDs)))
	// 设置群成员数量
	resp.GroupInfo.MemberCount = uint32(len(userIDs))
	// 构造群创建提示
	tips := &sdkws.GroupCreatedTips{
		Group:          resp.GroupInfo,                                                                      // 群信息
		OperationTime:  group.CreateTime.UnixMilli(),                                                        // 操作时间
		GroupOwnerUser: s.groupMemberDB2PB(groupMembers[0], userMap[groupMembers[0].UserID].AppMangerLevel), // 群主信息
	}
	// 设置群成员昵称和成员列表
	for _, member := range groupMembers {
		member.Nickname = userMap[member.UserID].Nickname // 设置成员昵称
		tips.MemberList = append(tips.MemberList, s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel))
		// 设置操作人信息
		if member.UserID == opUserID {
			tips.OpUser = s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel)
			break
		}
	}

	// 如果是超级群，异步发送超级群通知
	if req.GroupInfo.GroupType == constant.SuperGroup {
		go func() {
			for _, userID := range userIDs {
				s.Notification.SuperGroupNotification(ctx, userID, userID)
			}
		}()
	} else {
		// 普通群发送群创建通知
		// s.Notification.GroupCreatedNotification(ctx, group, groupMembers, userMap)
		tips := &sdkws.GroupCreatedTips{
			Group:          resp.GroupInfo,
			OperationTime:  group.CreateTime.UnixMilli(),
			GroupOwnerUser: s.groupMemberDB2PB(groupMembers[0], userMap[groupMembers[0].UserID].AppMangerLevel),
		}
		for _, member := range groupMembers {
			member.Nickname = userMap[member.UserID].Nickname
			tips.MemberList = append(tips.MemberList, s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel))
			if member.UserID == opUserID {
				tips.OpUser = s.groupMemberDB2PB(member, userMap[member.UserID].AppMangerLevel)
				break
			}
		}

		s.Notification.RoomGroupCreatedNotification(ctx, tips)
	}
	// 构造群创建后回调请求体
	reqCallBackAfter := &pbgroup.CreateGroupReq{
		MemberUserIDs: userIDs,
		GroupInfo:     resp.GroupInfo,
		OwnerUserID:   req.OwnerUserID,
		AdminUserIDs:  req.AdminUserIDs,
	}

	// 创建群后回调（后处理逻辑）
	if err := CallbackAfterCreateGroup(ctx, s.config, reqCallBackAfter); err != nil {
		return nil, err
	}

	// 返回响应
	return resp, nil
}

func (s *groupServer) GetRoomList(ctx context.Context, req *pbgroup.GetRoomListReq) (*pbgroup.GetRoomListResp, error) {

	if req.PageNumber == 0 || req.ShowNumber == 0 {
		return nil, errs.Wrap(errors.New("parameter error"))
	}

	list, err := s.db.GetRoomList(ctx, req.PageNumber, req.ShowNumber)
	if err != nil {
		return nil, err
	}

	//println(list.Rooms[0].RoomID)
	return list, nil

}

func (s *groupServer) GetJoinedGroupList(ctx context.Context, req *pbgroup.GetJoinedGroupListReq) (*pbgroup.GetJoinedGroupListResp, error) {
	resp := &pbgroup.GetJoinedGroupListResp{}
	if err := authverify.CheckAccessV3(ctx, req.FromUserID, s.config); err != nil {
		return nil, err
	}
	total, members, err := s.db.PageGetJoinGroup(ctx, req.FromUserID, req.Pagination)
	if err != nil {
		return nil, err
	}
	resp.Total = uint32(total)
	if len(members) == 0 {
		return resp, nil
	}
	groupIDs := utils.Slice(members, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	groups, err := s.db.FindGroup(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	groupMemberNum, err := s.db.MapGroupMemberNum(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	owners, err := s.db.FindGroupsOwner(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	ownerMap := utils.SliceToMap(owners, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	resp.Groups = utils.Slice(utils.Order(groupIDs, groups, func(group *relationtb.GroupModel) string {
		return group.GroupID
	}), func(group *relationtb.GroupModel) *sdkws.GroupInfo {
		var userID string
		if user := ownerMap[group.GroupID]; user != nil {
			userID = user.UserID
		}
		return convert.Db2PbGroupInfo(group, userID, groupMemberNum[group.GroupID])
	})
	return resp, nil
}

func (s *groupServer) InviteUserToGroup(ctx context.Context, req *pbgroup.InviteUserToGroupReq) (*pbgroup.InviteUserToGroupResp, error) {
	resp := &pbgroup.InviteUserToGroupResp{}

	if len(req.InvitedUserIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("user empty")
	}
	if utils.Duplicate(req.InvitedUserIDs) {
		return nil, errs.ErrArgs.Wrap("userID duplicate")
	}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}

	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap()
	}
	userMap, err := s.User.GetUsersInfoMap(ctx, req.InvitedUserIDs)
	if err != nil {
		return nil, err
	}
	if len(userMap) != len(req.InvitedUserIDs) {
		return nil, errs.ErrRecordNotFound.Wrap("user not found")
	}
	var groupMember *relationtb.GroupMemberModel
	var opUserID string
	if !authverify.IsAppManagerUid(ctx, s.config) {
		opUserID = mcontext.GetOpUserID(ctx)
		var err error
		groupMember, err = s.db.TakeGroupMember(ctx, req.GroupID, opUserID)
		if err != nil {
			return nil, err
		}
		if err := s.PopulateGroupMember(ctx, groupMember); err != nil {
			return nil, err
		}
	}

	if err := CallbackBeforeInviteUserToGroup(ctx, s.config, req); err != nil {
		return nil, err
	}
	if group.NeedVerification == constant.AllNeedVerification {
		if !authverify.IsAppManagerUid(ctx, s.config) {
			if !(groupMember.RoleLevel == constant.GroupOwner || groupMember.RoleLevel == constant.GroupAdmin) {
				var requests []*relationtb.GroupRequestModel
				for _, userID := range req.InvitedUserIDs {
					requests = append(requests, &relationtb.GroupRequestModel{
						UserID:        userID,
						GroupID:       req.GroupID,
						JoinSource:    constant.JoinByInvitation,
						InviterUserID: opUserID,
						ReqTime:       time.Now(),
						HandledTime:   time.Unix(0, 0),
					})
				}
				if err := s.db.CreateGroupRequest(ctx, requests); err != nil {
					return nil, err
				}
				for _, request := range requests {
					s.Notification.JoinGroupApplicationNotification(ctx, &pbgroup.JoinGroupReq{
						GroupID:       request.GroupID,
						ReqMessage:    request.ReqMsg,
						JoinSource:    request.JoinSource,
						InviterUserID: request.InviterUserID,
					})
				}
				return resp, nil
			}
		}
	}
	var groupMembers []*relationtb.GroupMemberModel
	for _, userID := range req.InvitedUserIDs {
		member := &relationtb.GroupMemberModel{
			GroupID:        req.GroupID,
			UserID:         userID,
			RoleLevel:      constant.GroupOrdinaryUsers,
			OperatorUserID: opUserID,
			InviterUserID:  opUserID,
			JoinSource:     constant.JoinByInvitation,
			JoinTime:       time.Now(),
			MuteEndTime:    time.UnixMilli(0),
		}
		if err := CallbackBeforeMemberJoinGroup(ctx, s.config, member, group.Ex); err != nil {
			return nil, err
		}
		groupMembers = append(groupMembers, member)
	}
	if err := s.db.CreateGroup(ctx, nil, groupMembers); err != nil {
		return nil, err
	}
	if err := s.conversationRpcClient.GroupChatFirstCreateConversation(ctx, req.GroupID, req.InvitedUserIDs); err != nil {
		return nil, err
	}
	s.Notification.MemberInvitedNotification(ctx, req.GroupID, req.Reason, req.InvitedUserIDs)
	return resp, nil
}

func (s *groupServer) GetGroupAllMember(ctx context.Context, req *pbgroup.GetGroupAllMemberReq) (*pbgroup.GetGroupAllMemberResp, error) {
	members, err := s.db.FindGroupMemberAll(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	resp := &pbgroup.GetGroupAllMemberResp{}
	resp.Members = utils.Slice(members, func(e *relationtb.GroupMemberModel) *sdkws.GroupMemberFullInfo {
		return convert.Db2PbGroupMember(e)
	})
	return resp, nil
}

// GetGroupMemberList 获取群成员列表
// ctx: 上下文，用于传递请求范围的截止时间、取消信号等
// req: 包含群ID、分页信息和搜索关键词的请求参数
// 返回值: 群成员列表响应和可能的错误
func (s *groupServer) GetGroupMemberList(ctx context.Context, req *pbgroup.GetGroupMemberListReq) (*pbgroup.GetGroupMemberListResp, error) {
	// 初始化群成员列表响应对象
	resp := &pbgroup.GetGroupMemberListResp{}
	// 声明变量：总成员数、成员列表、错误信息
	var (
		total   int64
		members []*relationtb.GroupMemberModel
		err     error
	)

	// 判断是否有搜索关键词
	if req.Keyword == "" {
		// 无关键词时，直接分页查询群成员
		total, members, err = s.db.PageGetGroupMember(ctx, req.GroupID, req.Pagination)
	} else {
		// 有关键词时，先查询该群所有成员
		members, err = s.db.FindGroupMemberAll(ctx, req.GroupID)
	}

	// 检查数据库查询是否出错
	if err != nil {
		return nil, err
	}

	// 填充群成员的详细信息（可能是补充用户资料等操作）
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}

	// 再次检查是否有搜索关键词（需要处理搜索逻辑）
	if req.Keyword != "" {
		// 创建一个新的切片用于存储符合搜索条件的成员
		groupMembers := make([]*relationtb.GroupMemberModel, 0)
		// 遍历所有成员，筛选出符合关键词的成员
		for _, member := range members {
			// 匹配用户ID
			if member.UserID == req.Keyword {
				groupMembers = append(groupMembers, member)
				total++  // 累加符合条件的成员总数
				continue // 继续下一个成员的检查
			}
			// 匹配昵称
			if member.Nickname == req.Keyword {
				groupMembers = append(groupMembers, member)
				total++  // 累加符合条件的成员总数
				continue // 继续下一个成员的检查
			}
		}

		// 对筛选后的成员列表进行分页处理
		GMembers := utils.Paginate(groupMembers, int(req.Pagination.GetPageNumber()), int(req.Pagination.GetShowNumber()))
		// 将数据库模型批量转换为protobuf模型并赋值给响应
		resp.Members = utils.Batch(convert.Db2PbGroupMember, GMembers)
		// 设置符合条件的成员总数
		resp.Total = uint32(total)
		return resp, nil
	}

	// 无搜索关键词时，直接使用分页查询的结果
	resp.Total = uint32(total)
	// 将数据库模型批量转换为protobuf模型并赋值给响应
	resp.Members = utils.Batch(convert.Db2PbGroupMember, members)
	return resp, nil
}

func (s *groupServer) KickGroupMember(ctx context.Context, req *pbgroup.KickGroupMemberReq) (*pbgroup.KickGroupMemberResp, error) {
	resp := &pbgroup.KickGroupMemberResp{}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if len(req.KickedUserIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("KickedUserIDs empty")
	}
	if utils.IsDuplicateStringSlice(req.KickedUserIDs) {
		return nil, errs.ErrArgs.Wrap("KickedUserIDs duplicate")
	}
	opUserID := mcontext.GetOpUserID(ctx)
	if utils.IsContain(opUserID, req.KickedUserIDs) {
		return nil, errs.ErrArgs.Wrap("opUserID in KickedUserIDs")
	}
	members, err := s.db.FindGroupMembers(ctx, req.GroupID, append(req.KickedUserIDs, opUserID))
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	memberMap := make(map[string]*relationtb.GroupMemberModel)
	for i, member := range members {
		memberMap[member.UserID] = members[i]
	}
	isAppManagerUid := authverify.IsAppManagerUid(ctx, s.config)
	opMember := memberMap[opUserID]
	for _, userID := range req.KickedUserIDs {
		member, ok := memberMap[userID]
		if !ok {
			return nil, errs.ErrUserIDNotFound.Wrap(userID)
		}
		if !isAppManagerUid {
			if opMember == nil {
				return nil, errs.ErrNoPermission.Wrap("opUserID no in group")
			}
			switch opMember.RoleLevel {
			case constant.GroupOwner:
			case constant.GroupAdmin:
				if member.RoleLevel == constant.GroupOwner || member.RoleLevel == constant.GroupAdmin {
					return nil, errs.ErrNoPermission.Wrap("group admins cannot remove the group owner and other admins")
				}
			case constant.GroupOrdinaryUsers:
				return nil, errs.ErrNoPermission.Wrap("opUserID no permission")
			default:
				return nil, errs.ErrNoPermission.Wrap("opUserID roleLevel unknown")
			}
		}
	}
	num, err := s.db.FindGroupMemberNum(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	ownerUserIDs, err := s.db.GetGroupRoleLevelMemberIDs(ctx, req.GroupID, constant.GroupOwner)
	if err != nil {
		return nil, err
	}
	var ownerUserID string
	if len(ownerUserIDs) > 0 {
		ownerUserID = ownerUserIDs[0]
	}
	if err := s.db.DeleteGroupMember(ctx, group.GroupID, req.KickedUserIDs); err != nil {
		return nil, err
	}
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
	if opMember, ok := memberMap[opUserID]; ok {
		tips.OpUser = convert.Db2PbGroupMember(opMember)
	}
	for _, userID := range req.KickedUserIDs {
		tips.KickedUserList = append(tips.KickedUserList, convert.Db2PbGroupMember(memberMap[userID]))
	}
	s.Notification.MemberKickedNotification(ctx, tips)
	if err := s.deleteMemberAndSetConversationSeq(ctx, req.GroupID, req.KickedUserIDs); err != nil {
		return nil, err
	}

	if err := CallbackKillGroupMember(ctx, s.config, req); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *groupServer) GetGroupMembersInfo(ctx context.Context, req *pbgroup.GetGroupMembersInfoReq) (*pbgroup.GetGroupMembersInfoResp, error) {
	resp := &pbgroup.GetGroupMembersInfoResp{}
	if len(req.UserIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("userIDs empty")
	}
	if req.GroupID == "" {
		return nil, errs.ErrArgs.Wrap("groupID empty")
	}
	members, err := s.db.FindGroupMembers(ctx, req.GroupID, req.UserIDs)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	resp.Members = utils.Slice(members, func(e *relationtb.GroupMemberModel) *sdkws.GroupMemberFullInfo {
		return convert.Db2PbGroupMember(e)
	})
	return resp, nil
}

func (s *groupServer) GetGroupApplicationList(ctx context.Context, req *pbgroup.GetGroupApplicationListReq) (*pbgroup.GetGroupApplicationListResp, error) {
	groupIDs, err := s.db.FindUserManagedGroupID(ctx, req.FromUserID)
	if err != nil {
		return nil, err
	}
	resp := &pbgroup.GetGroupApplicationListResp{}
	if len(groupIDs) == 0 {
		return resp, nil
	}
	total, groupRequests, err := s.db.PageGroupRequest(ctx, groupIDs, req.Pagination)
	if err != nil {
		return nil, err
	}
	resp.Total = uint32(total)
	if len(groupRequests) == 0 {
		return resp, nil
	}
	var userIDs []string

	for _, gr := range groupRequests {
		userIDs = append(userIDs, gr.UserID)
	}
	userIDs = utils.Distinct(userIDs)
	userMap, err := s.User.GetPublicUserInfoMap(ctx, userIDs, true)
	if err != nil {
		return nil, err
	}
	groups, err := s.db.FindGroup(ctx, utils.Distinct(groupIDs))
	if err != nil {
		return nil, err
	}
	groupMap := utils.SliceToMap(groups, func(e *relationtb.GroupModel) string {
		return e.GroupID
	})
	if ids := utils.Single(utils.Keys(groupMap), groupIDs); len(ids) > 0 {
		return nil, errs.ErrGroupIDNotFound.Wrap(strings.Join(ids, ","))
	}
	groupMemberNumMap, err := s.db.MapGroupMemberNum(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	owners, err := s.db.FindGroupsOwner(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, owners...); err != nil {
		return nil, err
	}
	ownerMap := utils.SliceToMap(owners, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	resp.GroupRequests = utils.Slice(groupRequests, func(e *relationtb.GroupRequestModel) *sdkws.GroupRequest {
		var ownerUserID string
		if owner, ok := ownerMap[e.GroupID]; ok {
			ownerUserID = owner.UserID
		}
		return convert.Db2PbGroupRequest(e, userMap[e.UserID], convert.Db2PbGroupInfo(groupMap[e.GroupID], ownerUserID, groupMemberNumMap[e.GroupID]))
	})
	return resp, nil
}

func (s *groupServer) GetGroupsInfo(ctx context.Context, req *pbgroup.GetGroupsInfoReq) (*pbgroup.GetGroupsInfoResp, error) {

	// 创建返回的响应结构体
	resp := &pbgroup.GetGroupsInfoResp{}
	// 检查请求的群组ID列表是否为空
	if len(req.GroupIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("groupID is empty") // 群组ID为空，返回参数错误
	}
	// 从数据库查找所有请求的群组信息
	groups, err := s.db.FindGroup(ctx, req.GroupIDs)
	if err != nil {
		return nil, err // 查找群组信息失败，返回错误
	}
	// 获取每个群组的成员数量映射
	groupMemberNumMap, err := s.db.MapGroupMemberNum(ctx, req.GroupIDs)
	if err != nil {
		return nil, err // 获取群组成员数量失败，返回错误
	}
	// 查找每个群组的群主信息
	owners, err := s.db.FindGroupsOwner(ctx, req.GroupIDs)
	if err != nil {
		return nil, err // 查找群主失败，返回错误
	}
	// 填充群主成员的其他信息（如昵称、头像等）
	if err := s.PopulateGroupMember(ctx, owners...); err != nil {
		return nil, err // 填充群主信息失败，返回错误
	}
	// 将群主信息列表转为以群组ID为key的map，便于后续查找
	ownerMap := utils.SliceToMap(owners, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	// 遍历所有群组，将数据库模型转换为响应的群组信息结构体
	resp.GroupInfos = utils.Slice(groups, func(e *relationtb.GroupModel) *sdkws.GroupInfo {
		var ownerUserID string
		// 获取群主用户ID
		if owner, ok := ownerMap[e.GroupID]; ok {
			ownerUserID = owner.UserID
		}
		// 数据库模型转为响应结构体，包含群主ID和成员数量
		return convert.Db2PbGroupInfo(e, ownerUserID, groupMemberNumMap[e.GroupID])
	})

	//	fmt.Printf("我看看  %+v\n", resp.GroupInfos)

	// 返回群组信息列表响应
	return resp, nil
}

// 获取聊天室信息
func (s *groupServer) GetRoomInfo(ctx context.Context, req *pbgroup.GetRoomInfoReq) (*pbgroup.GetRoomInfoResp, error) {
	// 创建返回的响应结构体
	resp := &pbgroup.GetRoomInfoResp{}
	// 检查请求的群组ID列表是否为空
	if len(req.RoomID) == 0 {
		return nil, errs.ErrArgs.Wrap("groupID is empty") // 群组ID为空，返回参数错误
	}
	// 从数据库查找所有请求的群组信息
	info, err := s.db.GetRoomInfo(ctx, req.RoomID)
	if err != nil {
		return nil, errs.ErrArgs.Wrap(err.Error())
	}
	resp.RoomInfo = info

	// 返回群组信息列表响应
	return resp, nil
}

func (s *groupServer) GroupApplicationResponse(ctx context.Context, req *pbgroup.GroupApplicationResponseReq) (*pbgroup.GroupApplicationResponseResp, error) {

	defer log.ZInfo(ctx, utils.GetFuncName()+" Return")
	if !utils.Contain(req.HandleResult, constant.GroupResponseAgree, constant.GroupResponseRefuse) {
		return nil, errs.ErrArgs.Wrap("HandleResult unknown")
	}
	if !authverify.IsAppManagerUid(ctx, s.config) {
		groupMember, err := s.db.TakeGroupMember(ctx, req.GroupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return nil, err
		}
		if !(groupMember.RoleLevel == constant.GroupOwner || groupMember.RoleLevel == constant.GroupAdmin) {
			return nil, errs.ErrNoPermission.Wrap("no group owner or admin")
		}
	}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	groupRequest, err := s.db.TakeGroupRequest(ctx, req.GroupID, req.FromUserID)
	if err != nil {
		return nil, err
	}
	if groupRequest.HandleResult != 0 {
		return nil, errs.ErrGroupRequestHandled.Wrap("group request already processed")
	}
	var inGroup bool
	if _, err := s.db.TakeGroupMember(ctx, req.GroupID, req.FromUserID); err == nil {
		inGroup = true // Already in group
	} else if !s.IsNotFound(err) {
		return nil, err
	}
	if _, err := s.User.GetPublicUserInfo(ctx, req.FromUserID); err != nil {
		return nil, err
	}
	var member *relationtb.GroupMemberModel
	if (!inGroup) && req.HandleResult == constant.GroupResponseAgree {
		member = &relationtb.GroupMemberModel{
			GroupID:        req.GroupID,
			UserID:         req.FromUserID,
			Nickname:       "",
			FaceURL:        "",
			RoleLevel:      constant.GroupOrdinaryUsers,
			JoinTime:       time.Now(),
			JoinSource:     groupRequest.JoinSource,
			MuteEndTime:    time.Unix(0, 0),
			InviterUserID:  groupRequest.InviterUserID,
			OperatorUserID: mcontext.GetOpUserID(ctx),
			Ex:             groupRequest.Ex,
		}
		if err = CallbackBeforeMemberJoinGroup(ctx, s.config, member, group.Ex); err != nil {
			return nil, err
		}
	}
	log.ZDebug(ctx, "GroupApplicationResponse", "inGroup", inGroup, "HandleResult", req.HandleResult, "member", member)
	if err := s.db.HandlerGroupRequest(ctx, req.GroupID, req.FromUserID, req.HandledMsg, req.HandleResult, member); err != nil {
		return nil, err
	}
	switch req.HandleResult {
	case constant.GroupResponseAgree:
		if err := s.conversationRpcClient.GroupChatFirstCreateConversation(ctx, req.GroupID, []string{req.FromUserID}); err != nil {
			return nil, err
		}
		s.Notification.GroupApplicationAcceptedNotification(ctx, req)
		if member == nil {
			log.ZDebug(ctx, "GroupApplicationResponse", "member is nil")
		} else {
			s.Notification.MemberEnterNotification(ctx, req.GroupID, req.FromUserID)
		}
	case constant.GroupResponseRefuse:
		s.Notification.GroupApplicationRejectedNotification(ctx, req)
	}

	return &pbgroup.GroupApplicationResponseResp{}, nil
}

func (s *groupServer) JoinGroup(ctx context.Context, req *pbgroup.JoinGroupReq) (resp *pbgroup.JoinGroupResp, err error) {
	defer log.ZInfo(ctx, "JoinGroup.Return")
	user, err := s.User.GetUserInfo(ctx, req.InviterUserID)
	if err != nil {
		return nil, err
	}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap()
	}

	reqCall := &callbackstruct.CallbackJoinGroupReq{
		GroupID:    req.GroupID,
		GroupType:  string(group.GroupType),
		ApplyID:    req.InviterUserID,
		ReqMessage: req.ReqMessage,
		Ex:         req.Ex,
	}

	if err = CallbackApplyJoinGroupBefore(ctx, s.config, reqCall); err != nil {
		return nil, err
	}
	_, err = s.db.TakeGroupMember(ctx, req.GroupID, req.InviterUserID)
	if err == nil {
		return nil, errs.ErrArgs.Wrap("already in group")
	} else if !s.IsNotFound(err) && utils.Unwrap(err) != errs.ErrRecordNotFound {
		return nil, err
	}
	log.ZInfo(ctx, "JoinGroup.groupInfo", "group", group, "eq", group.NeedVerification == constant.Directly)
	resp = &pbgroup.JoinGroupResp{}
	if group.NeedVerification == constant.Directly {
		groupMember := &relationtb.GroupMemberModel{
			GroupID:        group.GroupID,
			UserID:         user.UserID,
			RoleLevel:      constant.GroupOrdinaryUsers,
			OperatorUserID: mcontext.GetOpUserID(ctx),
			InviterUserID:  req.InviterUserID,
			JoinTime:       time.Now(),
			MuteEndTime:    time.UnixMilli(0),
		}
		if err := CallbackBeforeMemberJoinGroup(ctx, s.config, groupMember, group.Ex); err != nil {
			return nil, err
		}
		if err := s.db.CreateGroup(ctx, nil, []*relationtb.GroupMemberModel{groupMember}); err != nil {
			return nil, err
		}

		if err := s.conversationRpcClient.GroupChatFirstCreateConversation(ctx, req.GroupID, []string{req.InviterUserID}); err != nil {
			return nil, err
		}
		s.Notification.MemberEnterNotification(ctx, req.GroupID, req.InviterUserID)
		if err = CallbackAfterJoinGroup(ctx, s.config, req); err != nil {
			return nil, err
		}
		return resp, nil
	}
	groupRequest := relationtb.GroupRequestModel{
		UserID:      req.InviterUserID,
		ReqMsg:      req.ReqMessage,
		GroupID:     req.GroupID,
		JoinSource:  req.JoinSource,
		ReqTime:     time.Now(),
		HandledTime: time.Unix(0, 0),
		Ex:          req.Ex,
	}
	if err = s.db.CreateGroupRequest(ctx, []*relationtb.GroupRequestModel{&groupRequest}); err != nil {
		return nil, err
	}
	s.Notification.JoinGroupApplicationNotification(ctx, req)
	return resp, nil
}

// JoinRoom 加入聊天室
func (s *groupServer) JoinRoom(ctx context.Context, req *pbgroup.JoinGroupReq) (resp *pbgroup.JoinRoomResp, err error) {

	// 函数返回时记录日志
	defer log.ZInfo(ctx, "JoinGroup.Return")
	// 获取邀请人的用户信息
	user, err := s.User.GetUserInfo(ctx, req.InviterUserID)
	if err != nil {
		return nil, err // 查询用户失败，返回错误
	}
	// 获取群组信息
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err // 查询群组失败，返回错误
	}
	// 判断群组是否已经被解散
	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap() // 已解散，返回错误
	}
	// 检查用户是否已经在群组中
	_, err = s.db.TakeGroupMember(ctx, req.GroupID, req.InviterUserID)
	if err == nil {
		return nil, errs.ErrArgs.Wrap("already in group") // 已在群组，返回错误
	} else if !s.IsNotFound(err) && utils.Unwrap(err) != errs.ErrRecordNotFound {
		return nil, err // 不是未找到记录的错误，直接返回错误
	}
	// 记录日志，展示群组信息及是否需要验证
	log.ZInfo(ctx, "JoinGroup.groupInfo", "group", group, "eq", group.NeedVerification == constant.Directly)
	resp = &pbgroup.JoinRoomResp{}
	// 判断群组是否允许直接加入     //////是是是
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

		// 创建群组成员
		if err := s.db.CreateGroup(ctx, nil, []*relationtb.GroupMemberModel{groupMember}); err != nil {
			return nil, err // 创建失败，返回错误
		}

		//创建群聊会话   取消群组会话
		if err := s.conversationRpcClient.RoomGroupChatFirstCreateConversation(ctx, req.GroupID, []string{req.InviterUserID}); err != nil {
			return nil, err // 创建会话失败，返回错误
		}
		// 发送成员进入群组通知
		//s.Notification.RoomMemberEnterNotification(ctx, req.GroupID, req.InviterUserID)
		//// 加入群组后回调
		//if err = CallbackAfterJoinGroup(ctx, s.config, req); err != nil {
		//	return nil, err // 回调失败，返回错误
		//}

		//查询群组成员
		_, members, err1 := s.db.PageGetGroupMember(ctx, group.GroupID, &sdkws.RequestPagination{
			PageNumber: 1,
			ShowNumber: 8,
		})

		// 检查数据库查询是否出错
		if err1 != nil {
			return nil, err
		}
		resp = &pbgroup.JoinRoomResp{
			Rooms:   JoinMap,
			Members: utils.Batch(convert.Db2PbGroupMember, members), // 将数据库模型批量转换为protobuf模型并赋值给响应
		}
		s.Notification.RoomMemberEnterNotification(ctx, req.GroupID, req.InviterUserID, resp.Rooms)

		// 返回成功响应
		return resp, nil
	}

	return resp, nil
}

func (s *groupServer) QuitGroup(ctx context.Context, req *pbgroup.QuitGroupReq) (*pbgroup.QuitGroupResp, error) {
	resp := &pbgroup.QuitGroupResp{}
	if req.UserID == "" {
		req.UserID = mcontext.GetOpUserID(ctx)
	} else {
		if err := authverify.CheckAccessV3(ctx, req.UserID, s.config); err != nil {
			return nil, err
		}
	}
	member, err := s.db.TakeGroupMember(ctx, req.GroupID, req.UserID)
	if err != nil {
		return nil, err
	}
	if member.RoleLevel == constant.GroupOwner {
		return nil, errs.ErrNoPermission.Wrap("group owner can't quit")
	}
	if err := s.PopulateGroupMember(ctx, member); err != nil {
		return nil, err
	}
	err = s.db.DeleteGroupMember(ctx, req.GroupID, []string{req.UserID})
	if err != nil {
		return nil, err
	}
	_ = s.Notification.MemberQuitNotification(ctx, s.groupMemberDB2PB(member, 0))
	if err := s.deleteMemberAndSetConversationSeq(ctx, req.GroupID, []string{req.UserID}); err != nil {
		return nil, err
	}

	// callback
	if err := CallbackQuitGroup(ctx, s.config, req); err != nil {
		return nil, err
	}
	return resp, nil
}

// QuitRoom 退出聊天室
func (s *groupServer) QuitRoom(ctx context.Context, req *pbgroup.QuitGroupReq) (*pbgroup.QuitGroupResp, error) {
	resp := &pbgroup.QuitGroupResp{}
	if req.UserID == "" {
		req.UserID = mcontext.GetOpUserID(ctx)
	} else {
		if err := authverify.CheckAccessV3(ctx, req.UserID, s.config); err != nil {
			return nil, err
		}
	}

	member, err := s.db.TakeGroupMember(ctx, req.GroupID, req.UserID)

	if err != nil {
		return nil, err
	}
	if member.RoleLevel == constant.GroupOwner {
		return nil, errs.ErrNoPermission.Wrap("group owner can't quit")
	}
	if err := s.PopulateGroupMember(ctx, member); err != nil {
		return nil, err
	}
	err = s.db.DeleteGroupMember(ctx, req.GroupID, []string{req.UserID})

	///清空缓存
	err = s.db.QuitRoomList(ctx, req.GroupID, req.UserID, member.FaceURL)
	if err != nil {
		return nil, errs.ErrArgs.Wrap(err.Error())
	}

	if err != nil {
		return nil, err
	}
	_ = s.Notification.RoomMemberQuitNotification(ctx, s.groupMemberDB2PB(member, 0))
	if err := s.deleteRoomMemberAndSetConversationSeq(ctx, req.GroupID, []string{req.UserID}); err != nil {
		return nil, err
	}

	// callback
	if err := CallbackQuitGroup(ctx, s.config, req); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *groupServer) deleteRoomMemberAndSetConversationSeq(ctx context.Context, groupID string, userIDs []string) error {
	conevrsationID := msgprocessor.GetConversationIDBySessionType(constant.GroupChatType, groupID)
	maxSeq, err := s.msgRpcClient.GetConversationMaxSeq(ctx, conevrsationID)
	if err != nil {
		return err
	}
	return s.conversationRpcClient.SetConversationMaxSeq(ctx, userIDs, conevrsationID, maxSeq)
}

func (s *groupServer) deleteMemberAndSetConversationSeq(ctx context.Context, groupID string, userIDs []string) error {
	conevrsationID := msgprocessor.GetConversationIDBySessionType(constant.SuperGroupChatType, groupID)
	maxSeq, err := s.msgRpcClient.GetConversationMaxSeq(ctx, conevrsationID)
	if err != nil {
		return err
	}
	return s.conversationRpcClient.SetConversationMaxSeq(ctx, userIDs, conevrsationID, maxSeq)
}

func (s *groupServer) SetGroupInfo(ctx context.Context, req *pbgroup.SetGroupInfoReq) (*pbgroup.SetGroupInfoResp, error) {
	var opMember *relationtb.GroupMemberModel
	if !authverify.IsAppManagerUid(ctx, s.config) {
		var err error
		opMember, err = s.db.TakeGroupMember(ctx, req.GroupInfoForSet.GroupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return nil, err
		}
		if !(opMember.RoleLevel == constant.GroupOwner || opMember.RoleLevel == constant.GroupAdmin) {
			return nil, errs.ErrNoPermission.Wrap("no group owner or admin")
		}
		if err := s.PopulateGroupMember(ctx, opMember); err != nil {
			return nil, err
		}
	}
	if err := CallbackBeforeSetGroupInfo(ctx, s.config, req); err != nil {
		return nil, err
	}
	group, err := s.db.TakeGroup(ctx, req.GroupInfoForSet.GroupID)
	if err != nil {
		return nil, err
	}
	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.Wrap(errs.ErrDismissedAlready)
	}
	resp := &pbgroup.SetGroupInfoResp{}
	count, err := s.db.FindGroupMemberNum(ctx, group.GroupID)
	if err != nil {
		return nil, err
	}
	owner, err := s.db.TakeGroupOwner(ctx, group.GroupID)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, owner); err != nil {
		return nil, err
	}
	update := UpdateGroupInfoMap(ctx, req.GroupInfoForSet)
	if len(update) == 0 {
		return resp, nil
	}
	if err := s.db.UpdateGroup(ctx, group.GroupID, update); err != nil {
		return nil, err
	}
	group, err = s.db.TakeGroup(ctx, req.GroupInfoForSet.GroupID)
	if err != nil {
		return nil, err
	}
	tips := &sdkws.GroupInfoSetTips{
		Group:    s.groupDB2PB(group, owner.UserID, count),
		MuteTime: 0,
		OpUser:   &sdkws.GroupMemberFullInfo{},
	}
	if opMember != nil {
		tips.OpUser = s.groupMemberDB2PB(opMember, 0)
	}
	num := len(update)
	if req.GroupInfoForSet.Notification != "" {
		num--
		func() {
			conversation := &pbconversation.ConversationReq{
				ConversationID:   msgprocessor.GetConversationIDBySessionType(constant.SuperGroupChatType, req.GroupInfoForSet.GroupID),
				ConversationType: constant.SuperGroupChatType,
				GroupID:          req.GroupInfoForSet.GroupID,
			}
			resp, err := s.GetGroupMemberUserIDs(ctx, &pbgroup.GetGroupMemberUserIDsReq{GroupID: req.GroupInfoForSet.GroupID})
			if err != nil {
				log.ZWarn(ctx, "GetGroupMemberIDs", err)
				return
			}
			conversation.GroupAtType = &wrapperspb.Int32Value{Value: constant.GroupNotification}
			if err := s.conversationRpcClient.SetConversations(ctx, resp.UserIDs, conversation); err != nil {
				log.ZWarn(ctx, "SetConversations", err, resp.UserIDs, conversation)
			}
		}()
		_ = s.Notification.GroupInfoSetAnnouncementNotification(ctx, &sdkws.GroupInfoSetAnnouncementTips{Group: tips.Group, OpUser: tips.OpUser})
	}
	if req.GroupInfoForSet.GroupName != "" {
		num--
		_ = s.Notification.GroupInfoSetNameNotification(ctx, &sdkws.GroupInfoSetNameTips{Group: tips.Group, OpUser: tips.OpUser})
	}
	if num > 0 {
		_ = s.Notification.GroupInfoSetNotification(ctx, tips)
	}
	if err := CallbackAfterSetGroupInfo(ctx, s.config, req); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *groupServer) TransferGroupOwner(ctx context.Context, req *pbgroup.TransferGroupOwnerReq) (*pbgroup.TransferGroupOwnerResp, error) {
	resp := &pbgroup.TransferGroupOwnerResp{}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap("")
	}
	if req.OldOwnerUserID == req.NewOwnerUserID {
		return nil, errs.ErrArgs.Wrap("OldOwnerUserID == NewOwnerUserID")
	}
	members, err := s.db.FindGroupMembers(ctx, req.GroupID, []string{req.OldOwnerUserID, req.NewOwnerUserID})
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	memberMap := utils.SliceToMap(members, func(e *relationtb.GroupMemberModel) string { return e.UserID })
	if ids := utils.Single([]string{req.OldOwnerUserID, req.NewOwnerUserID}, utils.Keys(memberMap)); len(ids) > 0 {
		return nil, errs.ErrArgs.Wrap("user not in group " + strings.Join(ids, ","))
	}
	oldOwner := memberMap[req.OldOwnerUserID]
	if oldOwner == nil {
		return nil, errs.ErrArgs.Wrap("OldOwnerUserID not in group " + req.NewOwnerUserID)
	}
	newOwner := memberMap[req.NewOwnerUserID]
	if newOwner == nil {
		return nil, errs.ErrArgs.Wrap("NewOwnerUser not in group " + req.NewOwnerUserID)
	}
	if !authverify.IsAppManagerUid(ctx, s.config) {
		if !(mcontext.GetOpUserID(ctx) == oldOwner.UserID && oldOwner.RoleLevel == constant.GroupOwner) {
			return nil, errs.ErrNoPermission.Wrap("no permission transfer group owner")
		}
	}
	if err := s.db.TransferGroupOwner(ctx, req.GroupID, req.OldOwnerUserID, req.NewOwnerUserID, newOwner.RoleLevel); err != nil {
		return nil, err
	}

	if err := CallbackAfterTransferGroupOwner(ctx, s.config, req); err != nil {
		return nil, err
	}
	s.Notification.GroupOwnerTransferredNotification(ctx, req)
	return resp, nil
}

func (s *groupServer) GetGroups(ctx context.Context, req *pbgroup.GetGroupsReq) (*pbgroup.GetGroupsResp, error) {
	resp := &pbgroup.GetGroupsResp{}
	var (
		group []*relationtb.GroupModel
		err   error
	)
	if req.GroupID != "" {
		group, err = s.db.FindGroup(ctx, []string{req.GroupID})
		resp.Total = uint32(len(group))
	} else {
		var total int64
		total, group, err = s.db.SearchGroup(ctx, req.GroupName, req.Pagination)
		resp.Total = uint32(total)
	}
	if err != nil {
		return nil, err
	}

	var groups []*relationtb.GroupModel
	for _, v := range group {
		if v.Status == constant.GroupStatusDismissed {
			resp.Total--
			continue
		}
		groups = append(groups, v)
	}
	groupIDs := utils.Slice(groups, func(e *relationtb.GroupModel) string {
		return e.GroupID
	})
	ownerMembers, err := s.db.FindGroupsOwner(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	ownerMemberMap := utils.SliceToMap(ownerMembers, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	groupMemberNumMap, err := s.db.MapGroupMemberNum(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	resp.Groups = utils.Slice(groups, func(group *relationtb.GroupModel) *pbgroup.CMSGroup {
		var (
			userID   string
			username string
		)
		if member, ok := ownerMemberMap[group.GroupID]; ok {
			userID = member.UserID
			username = member.Nickname
		}
		return convert.Db2PbCMSGroup(group, userID, username, groupMemberNumMap[group.GroupID])
	})
	return resp, nil
}

func (s *groupServer) GetGroupMembersCMS(ctx context.Context, req *pbgroup.GetGroupMembersCMSReq) (*pbgroup.GetGroupMembersCMSResp, error) {
	resp := &pbgroup.GetGroupMembersCMSResp{}
	total, members, err := s.db.SearchGroupMember(ctx, req.UserName, req.GroupID, req.Pagination)
	if err != nil {
		return nil, err
	}
	resp.Total = uint32(total)
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	resp.Members = utils.Slice(members, func(e *relationtb.GroupMemberModel) *sdkws.GroupMemberFullInfo {
		return convert.Db2PbGroupMember(e)
	})
	return resp, nil
}

func (s *groupServer) GetUserReqApplicationList(ctx context.Context, req *pbgroup.GetUserReqApplicationListReq) (*pbgroup.GetUserReqApplicationListResp, error) {
	resp := &pbgroup.GetUserReqApplicationListResp{}
	user, err := s.User.GetPublicUserInfo(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	total, requests, err := s.db.PageGroupRequestUser(ctx, req.UserID, req.Pagination)
	if err != nil {
		return nil, err
	}
	resp.Total = uint32(total)
	if len(requests) == 0 {
		return resp, nil
	}
	groupIDs := utils.Distinct(utils.Slice(requests, func(e *relationtb.GroupRequestModel) string {
		return e.GroupID
	}))
	groups, err := s.db.FindGroup(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	groupMap := utils.SliceToMap(groups, func(e *relationtb.GroupModel) string {
		return e.GroupID
	})
	owners, err := s.db.FindGroupsOwner(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, owners...); err != nil {
		return nil, err
	}
	ownerMap := utils.SliceToMap(owners, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	groupMemberNum, err := s.db.MapGroupMemberNum(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	resp.GroupRequests = utils.Slice(requests, func(e *relationtb.GroupRequestModel) *sdkws.GroupRequest {
		var ownerUserID string
		if owner, ok := ownerMap[e.GroupID]; ok {
			ownerUserID = owner.UserID
		}
		return convert.Db2PbGroupRequest(e, user, convert.Db2PbGroupInfo(groupMap[e.GroupID], ownerUserID, groupMemberNum[e.GroupID]))
	})
	return resp, nil
}

func (s *groupServer) DismissGroup(ctx context.Context, req *pbgroup.DismissGroupReq) (*pbgroup.DismissGroupResp, error) {
	defer log.ZInfo(ctx, "DismissGroup.return")
	resp := &pbgroup.DismissGroupResp{}
	owner, err := s.db.TakeGroupOwner(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if !authverify.IsAppManagerUid(ctx, s.config) {
		if owner.UserID != mcontext.GetOpUserID(ctx) {
			return nil, errs.ErrNoPermission.Wrap("not group owner")
		}
	}
	if err := s.PopulateGroupMember(ctx, owner); err != nil {
		return nil, err
	}
	group, err := s.db.TakeGroup(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	if !req.DeleteMember && group.Status == constant.GroupStatusDismissed {
		return nil, errs.ErrDismissedAlready.Wrap("group status is dismissed")
	}
	if err := s.db.DismissGroup(ctx, req.GroupID, req.DeleteMember); err != nil {
		return nil, err
	}
	if !req.DeleteMember {
		num, err := s.db.FindGroupMemberNum(ctx, req.GroupID)
		if err != nil {
			return nil, err
		}
		tips := &sdkws.GroupDismissedTips{
			Group:  s.groupDB2PB(group, owner.UserID, num),
			OpUser: &sdkws.GroupMemberFullInfo{},
		}
		if mcontext.GetOpUserID(ctx) == owner.UserID {
			tips.OpUser = s.groupMemberDB2PB(owner, 0)
		}
		s.Notification.GroupDismissedNotification(ctx, tips)
	}
	membersID, err := s.db.FindGroupMemberUserID(ctx, group.GroupID)
	if err != nil {
		return nil, err
	}
	reqCall := &callbackstruct.CallbackDisMissGroupReq{
		GroupID:   req.GroupID,
		OwnerID:   owner.UserID,
		MembersID: membersID,
		GroupType: string(group.GroupType),
	}
	if err := CallbackDismissGroup(ctx, s.config, reqCall); err != nil {
		return nil, err
	}

	return resp, nil
}

// 解散聊天室
func (s *groupServer) DismissRoom(ctx context.Context, req *pbgroup.DismissGroupReq) (*pbgroup.DismissGroupResp, error) {
	defer log.ZInfo(ctx, "DismissGroup.return")         // 方法返回时记录日志
	resp := &pbgroup.DismissGroupResp{}                 // 创建返回响应对象
	owner, err := s.db.TakeGroupOwner(ctx, req.GroupID) // 查询群主信息
	if err != nil {
		return nil, err // 查询群主失败返回错误
	}
	if !authverify.IsAppManagerUid(ctx, s.config) { // 判断操作人是否为App管理员
		if owner.UserID != mcontext.GetOpUserID(ctx) { // 如果不是管理员则判断是否为群主
			return nil, errs.ErrNoPermission.Wrap("not group owner") // 不是群主无权限
		}
	}
	if err := s.PopulateGroupMember(ctx, owner); err != nil { // 填充群成员信息
		return nil, err // 填充失败返回错误
	}
	group, err := s.db.TakeGroup(ctx, req.GroupID) // 查询群组详情
	if err != nil {
		return nil, err // 查询失败返回错误
	}
	if !req.DeleteMember && group.Status == constant.GroupStatusDismissed { // 如果不删除成员且群已解散
		return nil, errs.ErrDismissedAlready.Wrap("group status is dismissed") // 群已解散，返回已解散错误
	}
	if err := s.db.DismissGroup(ctx, req.GroupID, req.DeleteMember); err != nil { // 执行解散群操作
		return nil, err // 解散失败返回错误
	}
	if !req.DeleteMember { // 如果不删除群成员
		num, err := s.db.FindGroupMemberNum(ctx, req.GroupID) // 查询群成员数量
		if err != nil {
			return nil, err // 查询失败返回错误
		}
		tips := &sdkws.GroupDismissedTips{
			Group:  s.groupDB2PB(group, owner.UserID, num), // 构建群信息
			OpUser: &sdkws.GroupMemberFullInfo{},           // 操作人信息
		}
		if mcontext.GetOpUserID(ctx) == owner.UserID { // 如果操作人为群主
			tips.OpUser = s.groupMemberDB2PB(owner, 0) // 填充群主信息
		}

		//解散群组
		err = s.db.DismissRoom(ctx, req.GroupID)
		if err != nil {
			return nil, errs.ErrArgs.Wrap(err.Error())
		}

		s.Notification.RoomDismissedNotification(ctx, tips) // 发送聊天室解散通知
	}
	membersID, err := s.db.FindGroupMemberUserID(ctx, group.GroupID) // 查询群成员ID列表
	if err != nil {
		return nil, err // 查询失败返回错误
	}
	reqCall := &callbackstruct.CallbackDisMissGroupReq{
		GroupID:   req.GroupID,             // 群组ID
		OwnerID:   owner.UserID,            // 群主ID
		MembersID: membersID,               // 所有群成员ID
		GroupType: string(group.GroupType), // 群类型
	}
	if err := CallbackDismissGroup(ctx, s.config, reqCall); err != nil { // 回调通知业务方群解散
		return nil, err // 回调失败返回错误
	}

	return resp, nil // 返回响应
}
func (s *groupServer) MuteGroupMember(ctx context.Context, req *pbgroup.MuteGroupMemberReq) (*pbgroup.MuteGroupMemberResp, error) {
	resp := &pbgroup.MuteGroupMemberResp{}
	member, err := s.db.TakeGroupMember(ctx, req.GroupID, req.UserID)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, member); err != nil {
		return nil, err
	}
	if !authverify.IsAppManagerUid(ctx, s.config) {
		opMember, err := s.db.TakeGroupMember(ctx, req.GroupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return nil, err
		}
		switch member.RoleLevel {
		case constant.GroupOwner:
			return nil, errs.ErrNoPermission.Wrap("set group owner mute")
		case constant.GroupAdmin:
			if opMember.RoleLevel != constant.GroupOwner {
				return nil, errs.ErrNoPermission.Wrap("set group admin mute")
			}
		case constant.GroupOrdinaryUsers:
			if !(opMember.RoleLevel == constant.GroupAdmin || opMember.RoleLevel == constant.GroupOwner) {
				return nil, errs.ErrNoPermission.Wrap("set group ordinary users mute")
			}
		}
	}
	data := UpdateGroupMemberMutedTimeMap(time.Now().Add(time.Second * time.Duration(req.MutedSeconds)))
	if err := s.db.UpdateGroupMember(ctx, member.GroupID, member.UserID, data); err != nil {
		return nil, err
	}
	s.Notification.GroupMemberMutedNotification(ctx, req.GroupID, req.UserID, req.MutedSeconds)
	return resp, nil
}

func (s *groupServer) CancelMuteGroupMember(ctx context.Context, req *pbgroup.CancelMuteGroupMemberReq) (*pbgroup.CancelMuteGroupMemberResp, error) {
	member, err := s.db.TakeGroupMember(ctx, req.GroupID, req.UserID)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, member); err != nil {
		return nil, err
	}
	if !authverify.IsAppManagerUid(ctx, s.config) {
		opMember, err := s.db.TakeGroupMember(ctx, req.GroupID, mcontext.GetOpUserID(ctx))
		if err != nil {
			return nil, err
		}
		switch member.RoleLevel {
		case constant.GroupOwner:
			return nil, errs.ErrNoPermission.Wrap("set group owner mute")
		case constant.GroupAdmin:
			if opMember.RoleLevel != constant.GroupOwner {
				return nil, errs.ErrNoPermission.Wrap("set group admin mute")
			}
		case constant.GroupOrdinaryUsers:
			if !(opMember.RoleLevel == constant.GroupAdmin || opMember.RoleLevel == constant.GroupOwner) {
				return nil, errs.ErrNoPermission.Wrap("set group ordinary users mute")
			}
		}
	}
	data := UpdateGroupMemberMutedTimeMap(time.Unix(0, 0))
	if err := s.db.UpdateGroupMember(ctx, member.GroupID, member.UserID, data); err != nil {
		return nil, err
	}
	s.Notification.GroupMemberCancelMutedNotification(ctx, req.GroupID, req.UserID)
	return &pbgroup.CancelMuteGroupMemberResp{}, nil
}

func (s *groupServer) MuteGroup(ctx context.Context, req *pbgroup.MuteGroupReq) (*pbgroup.MuteGroupResp, error) {
	resp := &pbgroup.MuteGroupResp{}
	if err := s.CheckGroupAdmin(ctx, req.GroupID); err != nil {
		return nil, err
	}
	if err := s.db.UpdateGroup(ctx, req.GroupID, UpdateGroupStatusMap(constant.GroupStatusMuted)); err != nil {
		return nil, err
	}
	s.Notification.GroupMutedNotification(ctx, req.GroupID)
	return resp, nil
}

func (s *groupServer) CancelMuteGroup(ctx context.Context, req *pbgroup.CancelMuteGroupReq) (*pbgroup.CancelMuteGroupResp, error) {
	resp := &pbgroup.CancelMuteGroupResp{}
	if err := s.CheckGroupAdmin(ctx, req.GroupID); err != nil {
		return nil, err
	}
	if err := s.db.UpdateGroup(ctx, req.GroupID, UpdateGroupStatusMap(constant.GroupOk)); err != nil {
		return nil, err
	}
	s.Notification.GroupCancelMutedNotification(ctx, req.GroupID)
	return resp, nil
}

func (s *groupServer) SetGroupMemberInfo(ctx context.Context, req *pbgroup.SetGroupMemberInfoReq) (*pbgroup.SetGroupMemberInfoResp, error) {
	resp := &pbgroup.SetGroupMemberInfoResp{}
	if len(req.Members) == 0 {
		return nil, errs.ErrArgs.Wrap("members empty")
	}
	opUserID := mcontext.GetOpUserID(ctx)
	if opUserID == "" {
		return nil, errs.ErrNoPermission.Wrap("no op user id")
	}
	isAppManagerUid := authverify.IsAppManagerUid(ctx, s.config)
	for i := range req.Members {
		req.Members[i].FaceURL = nil
	}
	groupMembers := make(map[string][]*pbgroup.SetGroupMemberInfo)
	for i, member := range req.Members {
		if member.RoleLevel != nil {
			switch member.RoleLevel.Value {
			case constant.GroupOwner:
				return nil, errs.ErrNoPermission.Wrap("cannot set ungroup owner")
			case constant.GroupAdmin, constant.GroupOrdinaryUsers:
			default:
				return nil, errs.ErrArgs.Wrap("invalid role level")
			}
		}
		groupMembers[member.GroupID] = append(groupMembers[member.GroupID], req.Members[i])
	}
	for groupID, members := range groupMembers {
		temp := make(map[string]struct{})
		userIDs := make([]string, 0, len(members)+1)
		for _, member := range members {
			if _, ok := temp[member.UserID]; ok {
				return nil, errs.ErrArgs.Wrap(fmt.Sprintf("repeat group %s user %s", member.GroupID, member.UserID))
			}
			temp[member.UserID] = struct{}{}
			userIDs = append(userIDs, member.UserID)
		}
		if _, ok := temp[opUserID]; !ok {
			userIDs = append(userIDs, opUserID)
		}
		dbMembers, err := s.db.FindGroupMembers(ctx, groupID, userIDs)
		if err != nil {
			return nil, err
		}
		opUserIndex := -1
		for i, member := range dbMembers {
			if member.UserID == opUserID {
				opUserIndex = i
				break
			}
		}
		switch len(userIDs) - len(dbMembers) {
		case 0:
			if !isAppManagerUid {
				roleLevel := dbMembers[opUserIndex].RoleLevel
				if roleLevel != constant.GroupOwner {
					switch roleLevel {
					case constant.GroupAdmin:
						for _, member := range dbMembers {
							if member.RoleLevel == constant.GroupOwner {
								return nil, errs.ErrNoPermission.Wrap("admin can not change group owner")
							}
							if member.RoleLevel == constant.GroupAdmin && member.UserID != opUserID {
								return nil, errs.ErrNoPermission.Wrap("admin can not change other group admin")
							}
						}
					case constant.GroupOrdinaryUsers:
						for _, member := range dbMembers {
							if !(member.RoleLevel == constant.GroupOrdinaryUsers && member.UserID == opUserID) {
								return nil, errs.ErrNoPermission.Wrap("ordinary users can not change other role level")
							}
						}
					default:
						for _, member := range dbMembers {
							if member.RoleLevel >= roleLevel {
								return nil, errs.ErrNoPermission.Wrap("can not change higher role level")
							}
						}
					}
				}
			}
		case 1:
			if opUserIndex >= 0 {
				return nil, errs.ErrArgs.Wrap("user not in group")
			}
			if !isAppManagerUid {
				return nil, errs.ErrNoPermission.Wrap("user not in group")
			}
		default:
			return nil, errs.ErrArgs.Wrap("user not in group")
		}
	}
	for i := 0; i < len(req.Members); i++ {
		if err := CallbackBeforeSetGroupMemberInfo(ctx, s.config, req.Members[i]); err != nil {
			return nil, err
		}
	}
	if err := s.db.UpdateGroupMembers(ctx, utils.Slice(req.Members, func(e *pbgroup.SetGroupMemberInfo) *relationtb.BatchUpdateGroupMember {
		return &relationtb.BatchUpdateGroupMember{
			GroupID: e.GroupID,
			UserID:  e.UserID,
			Map:     UpdateGroupMemberMap(e),
		}
	})); err != nil {
		return nil, err
	}
	for _, member := range req.Members {
		if member.RoleLevel != nil {
			switch member.RoleLevel.Value {
			case constant.GroupAdmin:
				s.Notification.GroupMemberSetToAdminNotification(ctx, member.GroupID, member.UserID)
			case constant.GroupOrdinaryUsers:
				s.Notification.GroupMemberSetToOrdinaryUserNotification(ctx, member.GroupID, member.UserID)
			}
		}
		if member.Nickname != nil || member.FaceURL != nil || member.Ex != nil {
			s.Notification.GroupMemberInfoSetNotification(ctx, member.GroupID, member.UserID)
		}
	}
	for i := 0; i < len(req.Members); i++ {
		if err := CallbackAfterSetGroupMemberInfo(ctx, s.config, req.Members[i]); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func (s *groupServer) GetGroupAbstractInfo(ctx context.Context, req *pbgroup.GetGroupAbstractInfoReq) (*pbgroup.GetGroupAbstractInfoResp, error) {
	// 创建响应结构体
	resp := &pbgroup.GetGroupAbstractInfoResp{}
	// 检查请求的群组ID列表是否为空
	if len(req.GroupIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("groupIDs empty") // 群组ID为空，返回参数错误
	}
	// 检查群组ID列表是否有重复
	if utils.Duplicate(req.GroupIDs) {
		return nil, errs.ErrArgs.Wrap("groupIDs duplicate") // 群组ID重复，返回参数错误
	}
	// 从数据库查找群组信息
	groups, err := s.db.FindGroup(ctx, req.GroupIDs)
	if err != nil {
		return nil, err // 查找群组失败，返回错误
	}
	// 检查请求的群组ID是否都在数据库查到
	if ids := utils.Single(req.GroupIDs, utils.Slice(groups, func(group *relationtb.GroupModel) string {
		return group.GroupID
	})); len(ids) > 0 {
		return nil, errs.ErrGroupIDNotFound.Wrap("not found group " + strings.Join(ids, ",")) // 有群组未找到，返回错误
	}
	// 获取每个群组对应的用户成员信息
	groupUserMap, err := s.db.MapGroupMemberUserID(ctx, req.GroupIDs)
	if err != nil {
		return nil, err // 获取群组成员失败，返回错误
	}
	// 检查所有群组都能查到成员信息
	if ids := utils.Single(req.GroupIDs, utils.Keys(groupUserMap)); len(ids) > 0 {
		return nil, errs.ErrGroupIDNotFound.Wrap(fmt.Sprintf("group %s not found member", strings.Join(ids, ","))) // 有群组查不到成员，返回错误
	}
	// 构建群组摘要信息列表
	resp.GroupAbstractInfos = utils.Slice(groups, func(group *relationtb.GroupModel) *pbgroup.GroupAbstractInfo {
		users := groupUserMap[group.GroupID]                                              // 获取群组成员信息
		return convert.Db2PbGroupAbstractInfo(group.GroupID, users.MemberNum, users.Hash) // 转换为响应格式
	})
	// 返回群组摘要信息响应
	return resp, nil
}

func (s *groupServer) GetUserInGroupMembers(ctx context.Context, req *pbgroup.GetUserInGroupMembersReq) (*pbgroup.GetUserInGroupMembersResp, error) {
	resp := &pbgroup.GetUserInGroupMembersResp{}
	if len(req.GroupIDs) == 0 {
		return nil, errs.ErrArgs.Wrap("groupIDs empty")
	}
	members, err := s.db.FindGroupMemberUser(ctx, req.GroupIDs, req.UserID)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	resp.Members = utils.Slice(members, func(e *relationtb.GroupMemberModel) *sdkws.GroupMemberFullInfo {
		return convert.Db2PbGroupMember(e)
	})
	return resp, nil
}

func (s *groupServer) GetGroupMemberUserIDs(ctx context.Context, req *pbgroup.GetGroupMemberUserIDsReq) (resp *pbgroup.GetGroupMemberUserIDsResp, err error) {
	resp = &pbgroup.GetGroupMemberUserIDsResp{}
	resp.UserIDs, err = s.db.FindGroupMemberUserID(ctx, req.GroupID)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *groupServer) GetGroupMemberRoleLevel(ctx context.Context, req *pbgroup.GetGroupMemberRoleLevelReq) (*pbgroup.GetGroupMemberRoleLevelResp, error) {
	resp := &pbgroup.GetGroupMemberRoleLevelResp{}
	if len(req.RoleLevels) == 0 {
		return nil, errs.ErrArgs.Wrap("RoleLevels empty")
	}
	members, err := s.db.FindGroupMemberRoleLevels(ctx, req.GroupID, req.RoleLevels)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, members...); err != nil {
		return nil, err
	}
	resp.Members = utils.Slice(members, func(e *relationtb.GroupMemberModel) *sdkws.GroupMemberFullInfo {
		return convert.Db2PbGroupMember(e)
	})
	return resp, nil
}

func (s *groupServer) GetGroupUsersReqApplicationList(ctx context.Context, req *pbgroup.GetGroupUsersReqApplicationListReq) (*pbgroup.GetGroupUsersReqApplicationListResp, error) {
	resp := &pbgroup.GetGroupUsersReqApplicationListResp{}
	requests, err := s.db.FindGroupRequests(ctx, req.GroupID, req.UserIDs)
	if err != nil {
		return nil, err
	}
	if len(requests) == 0 {
		return resp, nil
	}
	groupIDs := utils.Distinct(utils.Slice(requests, func(e *relationtb.GroupRequestModel) string {
		return e.GroupID
	}))
	groups, err := s.db.FindGroup(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	groupMap := utils.SliceToMap(groups, func(e *relationtb.GroupModel) string {
		return e.GroupID
	})
	if ids := utils.Single(groupIDs, utils.Keys(groupMap)); len(ids) > 0 {
		return nil, errs.ErrGroupIDNotFound.Wrap(strings.Join(ids, ","))
	}
	owners, err := s.db.FindGroupsOwner(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	if err := s.PopulateGroupMember(ctx, owners...); err != nil {
		return nil, err
	}
	ownerMap := utils.SliceToMap(owners, func(e *relationtb.GroupMemberModel) string {
		return e.GroupID
	})
	groupMemberNum, err := s.db.MapGroupMemberNum(ctx, groupIDs)
	if err != nil {
		return nil, err
	}
	resp.GroupRequests = utils.Slice(requests, func(e *relationtb.GroupRequestModel) *sdkws.GroupRequest {
		var ownerUserID string
		if owner, ok := ownerMap[e.GroupID]; ok {
			ownerUserID = owner.UserID
		}
		return convert.Db2PbGroupRequest(e, nil, convert.Db2PbGroupInfo(groupMap[e.GroupID], ownerUserID, groupMemberNum[e.GroupID]))
	})
	resp.Total = int64(len(resp.GroupRequests))
	return resp, nil
}
