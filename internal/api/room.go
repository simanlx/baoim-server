package api

import (
	"BaoIM-Server/pkg/rpcclient"
	"baoim/protocol/room"
	"baoim/tools/a2r"
	"github.com/gin-gonic/gin"
)

type RoomApi rpcclient.Room

// /增加 获取聊天室列表
func (o *RoomApi) GetRoomList1(c *gin.Context) {
	a2r.Call(room.RoomClient.GetRoomList, o.Client, c)
}

func NewRoomApi(client rpcclient.Room) RoomApi {
	return RoomApi(client)
}
