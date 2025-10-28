package main

import (
	"BaoIM-Server/internal/rpc/room"
	"BaoIM-Server/pkg/common/cmd"
	util "BaoIM-Server/pkg/util/genutil"
)

func main() {
	rpcCmd := cmd.NewRpcCmd(cmd.RpcRoomServer, room.Start)
	rpcCmd.AddPortFlag()
	rpcCmd.AddPrometheusPortFlag()
	if err := rpcCmd.Exec(); err != nil {
		util.ExitWithError(err)
	}
}
