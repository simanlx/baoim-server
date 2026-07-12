package api

import (
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/room"
	"baoim/tools/a2r"
	"github.com/gin-gonic/gin"
)

type RoomApi rpcclient.Room

func NewRoomApi(client rpcclient.Room) RoomApi {
	return RoomApi(client)
}

// 创建聊天室
func (o *RoomApi) CreateRoom(c *gin.Context) {
	a2r.Call(room.RoomClient.CreateRoom, o.Client, c)
}

// 解散聊天室
func (o *RoomApi) DismissRoom(c *gin.Context) {
	a2r.Call(room.RoomClient.DismissRoom, o.Client, c)
}

// 加入聊天室
func (o *RoomApi) JoinRoom(c *gin.Context) {
	a2r.Call(room.RoomClient.JoinRoom, o.Client, c)
}

// 退出聊天室
func (o *RoomApi) QuitRoom(c *gin.Context) {
	a2r.Call(room.RoomClient.QuitRoom, o.Client, c)
}

// 踢出聊天室成员
func (o *RoomApi) KickRoomMember(c *gin.Context) {
	a2r.Call(room.RoomClient.KickRoomMember, o.Client, c)
}

// 禁言聊天室成员
func (o *RoomApi) MuteRoomMember(c *gin.Context) {
	a2r.Call(room.RoomClient.MuteRoomMember, o.Client, c)
}

// 取消禁言聊天室成员
func (o *RoomApi) CancelMuteRoomMember(c *gin.Context) {
	a2r.Call(room.RoomClient.CancelMuteRoomMember, o.Client, c)
}

// /增加 获取聊天室列表
func (o *RoomApi) GetRoomList1(c *gin.Context) {
	a2r.Call(room.RoomClient.GetRoomList, o.Client, c)
}

// 获取聊天室信息
func (o *RoomApi) GetRoomInfo(c *gin.Context) {
	a2r.Call(room.RoomClient.GetRoomInfo, o.Client, c)
}

func (o *RoomApi) AddRoomOnline(c *gin.Context) {
	a2r.Call(room.RoomClient.AddOnlineUser, o.Client, c)
}
func (o *RoomApi) DelRoomOnline(c *gin.Context) {
	a2r.Call(room.RoomClient.DelOnlineUser, o.Client, c)
}
