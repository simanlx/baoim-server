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
	"time"

	"BaoIM-Server/pkg/apistruct"
	"BaoIM-Server/pkg/callbackstruct"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/table/relation"
	"BaoIM-Server/pkg/common/http"
	"baoim/protocol/constant"
	"baoim/protocol/group"
	pbgroup "baoim/protocol/group"
	"baoim/protocol/wrapperspb"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
)

func CallbackBeforeCreateGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *group.CreateGroupReq) (err error) {
	// 如果未启用 CallbackBeforeCreateGroup 回调，直接返回 nil，无需处理
	if !globalConfig.Callback.CallbackBeforeCreateGroup.Enable {
		return nil
	}
	// 构建回调请求结构体，设置回调命令/OperationID/群信息
	cbReq := &callbackstruct.CallbackBeforeCreateGroupReq{
		CallbackCommand: callbackstruct.CallbackBeforeCreateGroupCommand, // 回调命令标识
		OperationID:     mcontext.GetOperationID(ctx),                    // 获取当前操作ID
		GroupInfo:       req.GroupInfo,                                   // 群组基本信息
	}
	// 初始化成员列表，加入群主信息
	cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
		UserID:    req.OwnerUserID,     // 群主ID
		RoleLevel: constant.GroupOwner, // 群主角色
	})
	// 将管理员成员加入初始化成员列表
	for _, userID := range req.AdminUserIDs {
		cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
			UserID:    userID,              // 管理员ID
			RoleLevel: constant.GroupAdmin, // 管理员角色
		})
	}
	// 普通成员加入初始化成员列表
	for _, userID := range req.MemberUserIDs {
		cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
			UserID:    userID,                      // 普通成员ID
			RoleLevel: constant.GroupOrdinaryUsers, // 普通成员角色
		})
	}
	// 构建回调响应结构体
	resp := &callbackstruct.CallbackBeforeCreateGroupResp{}
	// 调用回调接口，传递请求与响应结构体，并根据配置执行回调
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, cbReq, resp, globalConfig.Callback.CallbackBeforeCreateGroup); err != nil {
		return err // 回调出错则返回错误
	}
	// 用回调响应结果替换原始请求中的群组信息字段（若不为空）
	utils.NotNilReplace(&req.GroupInfo.GroupID, resp.GroupID)                   // 群组ID
	utils.NotNilReplace(&req.GroupInfo.GroupName, resp.GroupName)               // 群组名称
	utils.NotNilReplace(&req.GroupInfo.Notification, resp.Notification)         // 群公告
	utils.NotNilReplace(&req.GroupInfo.Introduction, resp.Introduction)         // 群简介
	utils.NotNilReplace(&req.GroupInfo.FaceURL, resp.FaceURL)                   // 群头像
	utils.NotNilReplace(&req.GroupInfo.OwnerUserID, resp.OwnerUserID)           // 群主ID
	utils.NotNilReplace(&req.GroupInfo.Ex, resp.Ex)                             // 扩展字段
	utils.NotNilReplace(&req.GroupInfo.Status, resp.Status)                     // 群状态
	utils.NotNilReplace(&req.GroupInfo.CreatorUserID, resp.CreatorUserID)       // 创建者ID
	utils.NotNilReplace(&req.GroupInfo.GroupType, resp.GroupType)               // 群类型
	utils.NotNilReplace(&req.GroupInfo.NeedVerification, resp.NeedVerification) // 是否需验证
	utils.NotNilReplace(&req.GroupInfo.LookMemberInfo, resp.LookMemberInfo)     // 是否可查看成员信息
	return nil                                                                  // 回调及字段替换均成功后返回 nil
}

func CallbackAfterCreateGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *group.CreateGroupReq) (err error) {
	if !globalConfig.Callback.CallbackAfterCreateGroup.Enable {
		return nil
	}
	cbReq := &callbackstruct.CallbackAfterCreateGroupReq{
		CallbackCommand: callbackstruct.CallbackAfterCreateGroupCommand,
		GroupInfo:       req.GroupInfo,
	}
	cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
		UserID:    req.OwnerUserID,
		RoleLevel: constant.GroupOwner,
	})
	for _, userID := range req.AdminUserIDs {
		cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
			UserID:    userID,
			RoleLevel: constant.GroupAdmin,
		})
	}
	for _, userID := range req.MemberUserIDs {
		cbReq.InitMemberList = append(cbReq.InitMemberList, &apistruct.GroupAddMemberInfo{
			UserID:    userID,
			RoleLevel: constant.GroupOrdinaryUsers,
		})
	}
	resp := &callbackstruct.CallbackAfterCreateGroupResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, cbReq, resp, globalConfig.Callback.CallbackAfterCreateGroup); err != nil {
		return err
	}
	return nil
}

func CallbackBeforeMemberJoinGroup(
	ctx context.Context,
	globalConfig *config.GlobalConfig,
	groupMember *relation.GroupMemberModel,
	groupEx string,
) (err error) {
	if !globalConfig.Callback.CallbackBeforeMemberJoinGroup.Enable {
		return nil
	}
	callbackReq := &callbackstruct.CallbackBeforeMemberJoinGroupReq{
		CallbackCommand: callbackstruct.CallbackBeforeMemberJoinGroupCommand,
		GroupID:         groupMember.GroupID,
		UserID:          groupMember.UserID,
		Ex:              groupMember.Ex,
		GroupEx:         groupEx,
	}
	resp := &callbackstruct.CallbackBeforeMemberJoinGroupResp{}
	err = http.CallBackPostReturn(
		ctx,
		globalConfig.Callback.CallbackUrl,
		callbackReq,
		resp,
		globalConfig.Callback.CallbackBeforeMemberJoinGroup,
	)
	if err != nil {
		return err
	}
	if resp.MuteEndTime != nil {
		groupMember.MuteEndTime = time.UnixMilli(*resp.MuteEndTime)
	}
	utils.NotNilReplace(&groupMember.FaceURL, resp.FaceURL)
	utils.NotNilReplace(&groupMember.Ex, resp.Ex)
	utils.NotNilReplace(&groupMember.Nickname, resp.Nickname)
	utils.NotNilReplace(&groupMember.RoleLevel, resp.RoleLevel)
	return nil
}

func CallbackBeforeSetGroupMemberInfo(ctx context.Context, globalConfig *config.GlobalConfig, req *group.SetGroupMemberInfo) (err error) {
	if !globalConfig.Callback.CallbackBeforeSetGroupMemberInfo.Enable {
		return nil
	}
	callbackReq := callbackstruct.CallbackBeforeSetGroupMemberInfoReq{
		CallbackCommand: callbackstruct.CallbackBeforeSetGroupMemberInfoCommand,
		GroupID:         req.GroupID,
		UserID:          req.UserID,
	}
	if req.Nickname != nil {
		callbackReq.Nickname = &req.Nickname.Value
	}
	if req.FaceURL != nil {
		callbackReq.FaceURL = &req.FaceURL.Value
	}
	if req.RoleLevel != nil {
		callbackReq.RoleLevel = &req.RoleLevel.Value
	}
	if req.Ex != nil {
		callbackReq.Ex = &req.Ex.Value
	}
	resp := &callbackstruct.CallbackBeforeSetGroupMemberInfoResp{}
	err = http.CallBackPostReturn(
		ctx,
		globalConfig.Callback.CallbackUrl,
		callbackReq,
		resp,
		globalConfig.Callback.CallbackBeforeSetGroupMemberInfo,
	)
	if err != nil {
		return err
	}
	if resp.FaceURL != nil {
		req.FaceURL = wrapperspb.String(*resp.FaceURL)
	}
	if resp.Nickname != nil {
		req.Nickname = wrapperspb.String(*resp.Nickname)
	}
	if resp.RoleLevel != nil {
		req.RoleLevel = wrapperspb.Int32(*resp.RoleLevel)
	}
	if resp.Ex != nil {
		req.Ex = wrapperspb.String(*resp.Ex)
	}
	return nil
}
func CallbackAfterSetGroupMemberInfo(ctx context.Context, globalConfig *config.GlobalConfig, req *group.SetGroupMemberInfo) (err error) {
	if !globalConfig.Callback.CallbackBeforeSetGroupMemberInfo.Enable {
		return nil
	}
	callbackReq := callbackstruct.CallbackAfterSetGroupMemberInfoReq{
		CallbackCommand: callbackstruct.CallbackAfterSetGroupMemberInfoCommand,
		GroupID:         req.GroupID,
		UserID:          req.UserID,
	}
	if req.Nickname != nil {
		callbackReq.Nickname = &req.Nickname.Value
	}
	if req.FaceURL != nil {
		callbackReq.FaceURL = &req.FaceURL.Value
	}
	if req.RoleLevel != nil {
		callbackReq.RoleLevel = &req.RoleLevel.Value
	}
	if req.Ex != nil {
		callbackReq.Ex = &req.Ex.Value
	}
	resp := &callbackstruct.CallbackAfterSetGroupMemberInfoResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, callbackReq, resp, globalConfig.Callback.CallbackAfterSetGroupMemberInfo); err != nil {
		return err
	}
	return nil
}

func CallbackQuitGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *group.QuitGroupReq) (err error) {
	if !globalConfig.Callback.CallbackQuitGroup.Enable {
		return nil
	}
	cbReq := &callbackstruct.CallbackQuitGroupReq{
		CallbackCommand: callbackstruct.CallbackQuitGroupCommand,
		GroupID:         req.GroupID,
		UserID:          req.UserID,
	}
	resp := &callbackstruct.CallbackQuitGroupResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, cbReq, resp, globalConfig.Callback.CallbackQuitGroup); err != nil {
		return err
	}
	return nil
}

func CallbackKillGroupMember(ctx context.Context, globalConfig *config.GlobalConfig, req *pbgroup.KickGroupMemberReq) (err error) {
	if !globalConfig.Callback.CallbackKillGroupMember.Enable {
		return nil
	}
	cbReq := &callbackstruct.CallbackKillGroupMemberReq{
		CallbackCommand: callbackstruct.CallbackKillGroupCommand,
		GroupID:         req.GroupID,
		KickedUserIDs:   req.KickedUserIDs,
	}
	resp := &callbackstruct.CallbackKillGroupMemberResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, cbReq, resp, globalConfig.Callback.CallbackQuitGroup); err != nil {
		return err
	}
	return nil
}

func CallbackDismissGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *callbackstruct.CallbackDisMissGroupReq) (err error) {
	if !globalConfig.Callback.CallbackDismissGroup.Enable {
		return nil
	}
	req.CallbackCommand = callbackstruct.CallbackDisMissGroupCommand
	resp := &callbackstruct.CallbackDisMissGroupResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, req, resp, globalConfig.Callback.CallbackQuitGroup); err != nil {
		return err
	}
	return nil
}

func CallbackApplyJoinGroupBefore(ctx context.Context, globalConfig *config.GlobalConfig, req *callbackstruct.CallbackJoinGroupReq) (err error) {
	if !globalConfig.Callback.CallbackBeforeJoinGroup.Enable {
		return nil
	}

	req.CallbackCommand = callbackstruct.CallbackBeforeJoinGroupCommand

	resp := &callbackstruct.CallbackJoinGroupResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, req, resp, globalConfig.Callback.CallbackBeforeJoinGroup); err != nil {
		return err
	}

	return nil
}

func CallbackAfterTransferGroupOwner(ctx context.Context, globalConfig *config.GlobalConfig, req *pbgroup.TransferGroupOwnerReq) (err error) {
	if !globalConfig.Callback.CallbackAfterTransferGroupOwner.Enable {
		return nil
	}

	cbReq := &callbackstruct.CallbackTransferGroupOwnerReq{
		CallbackCommand: callbackstruct.CallbackAfterTransferGroupOwner,
		GroupID:         req.GroupID,
		OldOwnerUserID:  req.OldOwnerUserID,
		NewOwnerUserID:  req.NewOwnerUserID,
	}

	resp := &callbackstruct.CallbackTransferGroupOwnerResp{}
	if err = http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, cbReq, resp, globalConfig.Callback.CallbackAfterTransferGroupOwner); err != nil {
		return err
	}
	return nil
}
func CallbackBeforeInviteUserToGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *group.InviteUserToGroupReq) (err error) {
	if !globalConfig.Callback.CallbackBeforeInviteUserToGroup.Enable {
		return nil
	}

	callbackReq := &callbackstruct.CallbackBeforeInviteUserToGroupReq{
		CallbackCommand: callbackstruct.CallbackBeforeInviteJoinGroupCommand,
		OperationID:     mcontext.GetOperationID(ctx),
		GroupID:         req.GroupID,
		Reason:          req.Reason,
		InvitedUserIDs:  req.InvitedUserIDs,
	}

	resp := &callbackstruct.CallbackBeforeInviteUserToGroupResp{}
	err = http.CallBackPostReturn(
		ctx,
		globalConfig.Callback.CallbackUrl,
		callbackReq,
		resp,
		globalConfig.Callback.CallbackBeforeInviteUserToGroup,
	)

	if err != nil {
		return err
	}

	if len(resp.RefusedMembersAccount) > 0 {
		// Handle the scenario where certain members are refused
		// You might want to update the req.Members list or handle it as per your business logic
	}
	return nil
}

func CallbackAfterJoinGroup(ctx context.Context, globalConfig *config.GlobalConfig, req *group.JoinGroupReq) error {
	if !globalConfig.Callback.CallbackAfterJoinGroup.Enable {
		return nil
	}
	callbackReq := &callbackstruct.CallbackAfterJoinGroupReq{
		CallbackCommand: callbackstruct.CallbackAfterJoinGroupCommand,
		OperationID:     mcontext.GetOperationID(ctx),
		GroupID:         req.GroupID,
		ReqMessage:      req.ReqMessage,
		JoinSource:      req.JoinSource,
		InviterUserID:   req.InviterUserID,
	}
	resp := &callbackstruct.CallbackAfterJoinGroupResp{}
	if err := http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, callbackReq, resp, globalConfig.Callback.CallbackAfterJoinGroup); err != nil {
		return err
	}
	return nil
}

func CallbackBeforeSetGroupInfo(ctx context.Context, globalConfig *config.GlobalConfig, req *group.SetGroupInfoReq) error {
	if !globalConfig.Callback.CallbackBeforeSetGroupInfo.Enable {
		return nil
	}
	callbackReq := &callbackstruct.CallbackBeforeSetGroupInfoReq{
		CallbackCommand: callbackstruct.CallbackBeforeSetGroupInfoCommand,
		GroupID:         req.GroupInfoForSet.GroupID,
		Notification:    req.GroupInfoForSet.Notification,
		Introduction:    req.GroupInfoForSet.Introduction,
		FaceURL:         req.GroupInfoForSet.FaceURL,
		GroupName:       req.GroupInfoForSet.GroupName,
	}

	if req.GroupInfoForSet.Ex != nil {
		callbackReq.Ex = req.GroupInfoForSet.Ex.Value
	}
	log.ZDebug(ctx, "debug CallbackBeforeSetGroupInfo", callbackReq.Ex)
	if req.GroupInfoForSet.NeedVerification != nil {
		callbackReq.NeedVerification = req.GroupInfoForSet.NeedVerification.Value
	}
	if req.GroupInfoForSet.LookMemberInfo != nil {
		callbackReq.LookMemberInfo = req.GroupInfoForSet.LookMemberInfo.Value
	}
	if req.GroupInfoForSet.ApplyMemberFriend != nil {
		callbackReq.ApplyMemberFriend = req.GroupInfoForSet.ApplyMemberFriend.Value
	}
	resp := &callbackstruct.CallbackBeforeSetGroupInfoResp{}

	if err := http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, callbackReq, resp, globalConfig.Callback.CallbackBeforeSetGroupInfo); err != nil {
		return err
	}

	if resp.Ex != nil {
		req.GroupInfoForSet.Ex = wrapperspb.String(*resp.Ex)
	}
	if resp.NeedVerification != nil {
		req.GroupInfoForSet.NeedVerification = wrapperspb.Int32(*resp.NeedVerification)
	}
	if resp.LookMemberInfo != nil {
		req.GroupInfoForSet.LookMemberInfo = wrapperspb.Int32(*resp.LookMemberInfo)
	}
	if resp.ApplyMemberFriend != nil {
		req.GroupInfoForSet.ApplyMemberFriend = wrapperspb.Int32(*resp.ApplyMemberFriend)
	}
	utils.NotNilReplace(&req.GroupInfoForSet.GroupID, &resp.GroupID)
	utils.NotNilReplace(&req.GroupInfoForSet.GroupName, &resp.GroupName)
	utils.NotNilReplace(&req.GroupInfoForSet.FaceURL, &resp.FaceURL)
	utils.NotNilReplace(&req.GroupInfoForSet.Introduction, &resp.Introduction)
	return nil
}
func CallbackAfterSetGroupInfo(ctx context.Context, globalConfig *config.GlobalConfig, req *group.SetGroupInfoReq) error {
	if !globalConfig.Callback.CallbackAfterSetGroupInfo.Enable {
		return nil
	}
	callbackReq := &callbackstruct.CallbackAfterSetGroupInfoReq{
		CallbackCommand: callbackstruct.CallbackAfterSetGroupInfoCommand,
		GroupID:         req.GroupInfoForSet.GroupID,
		Notification:    req.GroupInfoForSet.Notification,
		Introduction:    req.GroupInfoForSet.Introduction,
		FaceURL:         req.GroupInfoForSet.FaceURL,
		GroupName:       req.GroupInfoForSet.GroupName,
	}
	if req.GroupInfoForSet.Ex != nil {
		callbackReq.Ex = &req.GroupInfoForSet.Ex.Value
	}
	if req.GroupInfoForSet.NeedVerification != nil {
		callbackReq.NeedVerification = &req.GroupInfoForSet.NeedVerification.Value
	}
	if req.GroupInfoForSet.LookMemberInfo != nil {
		callbackReq.LookMemberInfo = &req.GroupInfoForSet.LookMemberInfo.Value
	}
	if req.GroupInfoForSet.ApplyMemberFriend != nil {
		callbackReq.ApplyMemberFriend = &req.GroupInfoForSet.ApplyMemberFriend.Value
	}
	resp := &callbackstruct.CallbackAfterSetGroupInfoResp{}
	if err := http.CallBackPostReturn(ctx, globalConfig.Callback.CallbackUrl, callbackReq, resp, globalConfig.Callback.CallbackAfterSetGroupInfo); err != nil {
		return err
	}
	return nil
}
