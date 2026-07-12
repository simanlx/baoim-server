package api

import (
	"github.com/gin-gonic/gin"

	"baoim/protocol/encryption"
	"baoim/tools/a2r"

	"BaoIM-Server/pkg/rpcclient"
)

type EncryptionApi rpcclient.Encryption

func NewEncryptionApi(client rpcclient.Encryption) EncryptionApi {
	return EncryptionApi(client)
}

func (e *EncryptionApi) GetEncryptionKey(c *gin.Context) {
	a2r.Call(encryption.EncryptionClient.GetEncryptionKey, e.Client, c)
}
