package notification

import (
	"BaoIM-Server/pkg/authverify"
	"BaoIM-Server/pkg/common/config"
	"BaoIM-Server/pkg/common/db/controller"
	"BaoIM-Server/pkg/common/db/table/relation"
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/constant"
	"baoim/protocol/sdkws"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
	"context"
	"fmt"
)

func NewRoomNotificationSender(
	db controller.RoomDatabase,
	msgRpcClient *rpcclient.MessageRpcClient,
	userRpcClient *rpcclient.UserRpcClient,
	config *config.GlobalConfig,
	fn func(ctx context.Context, userIDs []string) ([]CommonUser, error),
) *RoomNotificationSender {
	return &RoomNotificationSender{
		NotificationSender: rpcclient.NewNotificationSender(config, rpcclient.WithRpcClient(msgRpcClient), rpcclient.WithUserRpcClient(userRpcClient)),
		getUsersInfo:       fn,
		db:                 db,
		config:             config,
	}
}

type RoomNotificationSender struct {
	*rpcclient.NotificationSender
	getUsersInfo func(ctx context.Context, userIDs []string) ([]CommonUser, error)
	db           controller.RoomDatabase
	config       *config.GlobalConfig
}

func (g *RoomNotificationSender) roomMemberDB2PB(member *relation.GroupMemberModel, appMangerLevel int32) *sdkws.GroupMemberFullInfo {
	return &sdkws.GroupMemberFullInfo{
		GroupID:        member.GroupID,
		UserID:         member.UserID,
		RoleLevel:      member.RoleLevel,
		JoinTime:       member.JoinTime.UnixMilli(),
		Nickname:       member.Nickname,
		FaceURL:        member.FaceURL,
		AppMangerLevel: appMangerLevel,
		JoinSource:     member.JoinSource,
		OperatorUserID: member.OperatorUserID,
		Ex:             member.Ex,
		MuteEndTime:    member.MuteEndTime.UnixMilli(),
		InviterUserID:  member.InviterUserID,
	}
}

func (g *RoomNotificationSender) RoomCreatedNotification(ctx context.Context, tips *sdkws.GroupCreatedTips) (err error) {

	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	if err := g.fillOpUser(ctx, &tips.OpUser, tips.Group.GroupID); err != nil {
		return err
	}

	return g.Notification(ctx, mcontext.GetOpUserID(ctx), tips.Group.GroupID, constant.RoomGroupCreatedNotification, tips)
}

func (g *RoomNotificationSender) RoomMemberEnterNotification(ctx context.Context, groupID string, entrantUserID string, RoomInfo *sdkws.RoomInfo) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	group, err := g.getRoomInfo(ctx, groupID)
	if err != nil {
		return err
	}
	user, err := g.getRoomMember(ctx, groupID, entrantUserID)
	if err != nil {
		return err
	}
	tips := &sdkws.MemberEnterTips{Group: group, EntrantUser: user}
	tips.RoomInfo = RoomInfo //增加
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), group.GroupID, constant.RoomMemberEnterNotification, tips)
}

func (g *RoomNotificationSender) RoomDismissedNotification(ctx context.Context, tips *sdkws.GroupDismissedTips) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	if err := g.fillOpUser(ctx, &tips.OpUser, tips.Group.GroupID); err != nil {
		return err
	}
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), tips.Group.GroupID, constant.RoomGroupDismissedNotification, tips)
}

func (g *RoomNotificationSender) RoomMemberMutedNotification(ctx context.Context, groupID, groupMemberUserID string, mutedSeconds uint32) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	group, err := g.getRoomInfo(ctx, groupID)
	if err != nil {
		return err
	}
	user, err := g.getRoomMemberMap(ctx, groupID, []string{mcontext.GetOpUserID(ctx), groupMemberUserID})
	if err != nil {
		return err
	}
	tips := &sdkws.GroupMemberMutedTips{
		Group: group, MutedSeconds: mutedSeconds,
		OpUser: user[mcontext.GetOpUserID(ctx)], MutedUser: user[groupMemberUserID],
	}
	if err := g.fillOpUser(ctx, &tips.OpUser, tips.Group.GroupID); err != nil {
		return err
	}
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), group.GroupID, constant.RoomGroupMemberMutedNotification, tips)
}

// /取消聊天室 禁言
func (g *RoomNotificationSender) RoomMemberCancelMutedNotification(ctx context.Context, groupID, groupMemberUserID string) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	group, err := g.getRoomInfo(ctx, groupID)
	if err != nil {
		return err
	}
	user, err := g.getRoomMemberMap(ctx, groupID, []string{mcontext.GetOpUserID(ctx), groupMemberUserID})
	if err != nil {
		return err
	}
	tips := &sdkws.GroupMemberCancelMutedTips{Group: group, OpUser: user[mcontext.GetOpUserID(ctx)], MutedUser: user[groupMemberUserID]}
	if err := g.fillOpUser(ctx, &tips.OpUser, tips.Group.GroupID); err != nil {
		return err
	}
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), group.GroupID, constant.RoomGroupMemberCancelMutedNotification, tips)
}

// 退出聊天室通知
func (g *RoomNotificationSender) RoomMemberQuitNotification(ctx context.Context, member *sdkws.GroupMemberFullInfo) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	group, err := g.getRoomInfo(ctx, member.GroupID)
	if err != nil {
		return err
	}
	tips := &sdkws.MemberQuitTips{Group: group, QuitUser: member}
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), member.GroupID, constant.RoomMemberQuitNotification, tips)
}

// /聊天室 踢成员通知
func (g *RoomNotificationSender) RoomMemberKickedNotification(ctx context.Context, tips *sdkws.MemberKickedTips) (err error) {
	defer log.ZDebug(ctx, "return")
	defer func() {
		if err != nil {
			log.ZError(ctx, utils.GetFuncName(1)+" failed", err)
		}
	}()
	if err := g.fillOpUser(ctx, &tips.OpUser, tips.Group.GroupID); err != nil {
		return err
	}
	return g.Notification(ctx, mcontext.GetOpUserID(ctx), tips.Group.GroupID, constant.RoomMemberKickedNotification, tips)
}

// 填写操作用户
func (g *RoomNotificationSender) fillOpUser(ctx context.Context, opUser **sdkws.GroupMemberFullInfo, groupID string) (err error) {
	if opUser == nil {
		return errs.ErrInternalServer.Wrap("**sdkws.GroupMemberFullInfo is nil")
	}
	userID := mcontext.GetOpUserID(ctx)
	if groupID != "" {
		if authverify.IsManagerUserID(userID, g.config) {
			*opUser = &sdkws.GroupMemberFullInfo{
				GroupID:        groupID,
				UserID:         userID,
				RoleLevel:      constant.GroupAdmin,
				AppMangerLevel: constant.AppAdmin,
			}
		} else {
			member, err := g.db.TakeRoomMember(ctx, groupID, userID)
			if err == nil {
				*opUser = g.roomMemberDB2PB(member, 0)
			} else if !errs.ErrRecordNotFound.Is(err) {
				return err
			}
		}
	}
	user, err := g.getUser(ctx, userID)
	if err != nil {
		return err
	}
	if *opUser == nil {
		*opUser = &sdkws.GroupMemberFullInfo{
			GroupID:        groupID,
			UserID:         userID,
			Nickname:       user.Nickname,
			FaceURL:        user.FaceURL,
			OperatorUserID: userID,
		}
	} else {
		if (*opUser).Nickname == "" {
			(*opUser).Nickname = user.Nickname
		}
		if (*opUser).FaceURL == "" {
			(*opUser).FaceURL = user.FaceURL
		}
	}
	return nil
}

func (g *RoomNotificationSender) getUser(ctx context.Context, userID string) (*sdkws.PublicUserInfo, error) {
	users, err := g.getUsersInfo(ctx, []string{userID})
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, errs.ErrUserIDNotFound.Wrap(fmt.Sprintf("user %s not found", userID))
	}
	return &sdkws.PublicUserInfo{
		UserID:   users[0].GetUserID(),
		Nickname: users[0].GetNickname(),
		FaceURL:  users[0].GetFaceURL(),
		Ex:       users[0].GetEx(),
	}, nil
}

func (g *RoomNotificationSender) getRoomInfo(ctx context.Context, groupID string) (*sdkws.GroupInfo, error) {
	gm, err := g.db.TakeRoom(ctx, groupID)
	if err != nil {
		return nil, err
	}
	num, err := g.db.FindRoomMemberNum(ctx, groupID)
	if err != nil {
		return nil, err
	}
	ownerUserIDs, err := g.db.GetRoomRoleLevelMemberIDs(ctx, groupID, constant.GroupOwner)
	if err != nil {
		return nil, err
	}
	var ownerUserID string
	if len(ownerUserIDs) > 0 {
		ownerUserID = ownerUserIDs[0]
	}
	return &sdkws.GroupInfo{
		GroupID:                gm.GroupID,
		GroupName:              gm.GroupName,
		Notification:           gm.Notification,
		Introduction:           gm.Introduction,
		FaceURL:                gm.FaceURL,
		OwnerUserID:            ownerUserID,
		CreateTime:             gm.CreateTime.UnixMilli(),
		MemberCount:            num,
		Ex:                     gm.Ex,
		Status:                 gm.Status,
		CreatorUserID:          gm.CreatorUserID,
		GroupType:              gm.GroupType,
		NeedVerification:       gm.NeedVerification,
		LookMemberInfo:         gm.LookMemberInfo,
		ApplyMemberFriend:      gm.ApplyMemberFriend,
		NotificationUpdateTime: gm.NotificationUpdateTime.UnixMilli(),
		NotificationUserID:     gm.NotificationUserID,
	}, nil
}

func (g *RoomNotificationSender) getRoomMember(ctx context.Context, groupID string, userID string) (*sdkws.GroupMemberFullInfo, error) {
	members, err := g.getRoomMembers(ctx, groupID, []string{userID})
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errs.ErrInternalServer.Wrap(fmt.Sprintf("group %s member %s not found", groupID, userID))
	}
	return members[0], nil
}

func (g *RoomNotificationSender) getRoomMembers(ctx context.Context, groupID string, userIDs []string) ([]*sdkws.GroupMemberFullInfo, error) {
	members, err := g.db.FindRoomMembers(ctx, groupID, userIDs)
	if err != nil {
		return nil, err
	}
	if err := g.PopulateRoomMember(ctx, members...); err != nil {
		return nil, err
	}
	log.ZDebug(ctx, "getGroupMembers", "members", members)
	res := make([]*sdkws.GroupMemberFullInfo, 0, len(members))
	for _, member := range members {
		res = append(res, g.roomMemberDB2PB(member, 0))
	}
	return res, nil
}
func (g *RoomNotificationSender) PopulateRoomMember(ctx context.Context, members ...*relation.GroupMemberModel) error {
	if len(members) == 0 {
		return nil
	}
	emptyUserIDs := make(map[string]struct{})
	for _, member := range members {
		if member.Nickname == "" || member.FaceURL == "" {
			emptyUserIDs[member.UserID] = struct{}{}
		}
	}
	if len(emptyUserIDs) > 0 {
		users, err := g.getUsersInfo(ctx, utils.Keys(emptyUserIDs))
		if err != nil {
			return err
		}
		userMap := make(map[string]CommonUser)
		for i, user := range users {
			userMap[user.GetUserID()] = users[i]
		}
		for i, member := range members {
			user, ok := userMap[member.UserID]
			if !ok {
				continue
			}
			if member.Nickname == "" {
				members[i].Nickname = user.GetNickname()
			}
			if member.FaceURL == "" {
				members[i].FaceURL = user.GetFaceURL()
			}
		}
	}
	return nil
}
func (g *RoomNotificationSender) getRoomMemberMap(ctx context.Context, groupID string, userIDs []string) (map[string]*sdkws.GroupMemberFullInfo, error) {
	members, err := g.getRoomMembers(ctx, groupID, userIDs)
	if err != nil {
		return nil, err
	}
	m := make(map[string]*sdkws.GroupMemberFullInfo)
	for i, member := range members {
		m[member.UserID] = members[i]
	}
	return m, nil
}
