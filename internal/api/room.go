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

// /增加 获取聊天室列表
func (o *RoomApi) GetRoomList1(c *gin.Context) {
	a2r.Call(room.RoomClient.GetRoomList, o.Client, c)
}

// 增加 用户到聊天室
func (o *RoomApi) UpdateRoomUser(c *gin.Context) {
	a2r.Call(room.RoomClient.UpdateRoomUser, o.Client, c)
}

// 删除 用户从聊天室
func (o *RoomApi) DeleteRoomUser(c *gin.Context) {
	a2r.Call(room.RoomClient.DeleteRoomUser, o.Client, c)
}
