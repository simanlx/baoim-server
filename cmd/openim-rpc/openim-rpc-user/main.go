// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

package main

import (
	"BaoIM-Server/internal/rpc/user"
	"BaoIM-Server/pkg/common/cmd"
	"BaoIM-Server/pkg/common/config"
)

func main() {

	rpcCmd := cmd.NewRpcCmd(cmd.RpcUserServer)
	rpcCmd.AddPortFlag()
	rpcCmd.AddPrometheusPortFlag()
	if err := rpcCmd.Exec(); err != nil {
		panic(err.Error())
	}
	if err := rpcCmd.StartSvr(config.Config.RpcRegisterName.OpenImUserName, user.Start); err != nil {
		panic(err.Error())
	}
}
