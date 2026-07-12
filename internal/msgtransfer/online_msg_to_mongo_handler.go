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

package msgtransfer

import (
	"context"

	"BaoIM-Server/pkg/common/config"        // 导入配置包
	"BaoIM-Server/pkg/common/db/controller" // 导入数据库控制器包
	kfk "BaoIM-Server/pkg/common/kafka"     // 导入kafka包
	"BaoIM-Server/pkg/common/prommetrics"   // 导入监控指标包
	pbmsg "baoim/protocol/msg"              // 导入消息协议包
	"baoim/tools/log"                       // 导入日志工具包
	"github.com/IBM/sarama"                 // 导入sarama kafka库
	"google.golang.org/protobuf/proto"      // 导入protobuf库
)

// 在线历史消息Mongo消费者处理器结构体
type OnlineHistoryMongoConsumerHandler struct {
	historyConsumerGroup *kfk.MConsumerGroup          // Kafka消费组对象
	msgDatabase          controller.CommonMsgDatabase // 消息数据库操作对象
}

// 构造在线历史消息Mongo消费者处理器
func NewOnlineHistoryMongoConsumerHandler(config *config.GlobalConfig, database controller.CommonMsgDatabase) (*OnlineHistoryMongoConsumerHandler, error) {
	var tlsConfig *kfk.TLSConfig // TLS配置初始化
	if config.Kafka.TLS != nil { // 如果配置了TLS
		tlsConfig = &kfk.TLSConfig{ // 填充TLS配置信息
			CACrt:              config.Kafka.TLS.CACrt,
			ClientCrt:          config.Kafka.TLS.ClientCrt,
			ClientKey:          config.Kafka.TLS.ClientKey,
			ClientKeyPwd:       config.Kafka.TLS.ClientKeyPwd,
			InsecureSkipVerify: false,
		}
	}
	// 创建Kafka消费组
	historyConsumerGroup, err := kfk.NewMConsumerGroup(&kfk.MConsumerGroupConfig{
		KafkaVersion:   sarama.V2_0_0_0,       // Kafka版本
		OffsetsInitial: sarama.OffsetNewest,   // 初始偏移量
		IsReturnErr:    false,                 // 是否返回错误
		UserName:       config.Kafka.Username, // Kafka用户名
		Password:       config.Kafka.Password, // Kafka密码
	}, []string{config.Kafka.MsgToMongo.Topic}, // 订阅的Topic
		config.Kafka.Addr,                       // Kafka地址
		config.Kafka.ConsumerGroupID.MsgToMongo, // 消费组ID
		tlsConfig,                               // TLS配置
	)
	if err != nil { // 如果创建消费组失败
		return nil, err
	}

	mc := &OnlineHistoryMongoConsumerHandler{ // 初始化处理器对象
		historyConsumerGroup: historyConsumerGroup, // 设置消费组
		msgDatabase:          database,             // 设置消息数据库
	}
	return mc, nil // 返回处理器对象
}

// 处理聊天消息从ws到Mongo的流程
func (mc *OnlineHistoryMongoConsumerHandler) handleChatWs2Mongo(
	ctx context.Context, // 上下文对象
	cMsg *sarama.ConsumerMessage, // Kafka消费到的消息
	key string, // 消息Key
	session sarama.ConsumerGroupSession, // 消费组Session
) {
	msg := cMsg.Value                       // 获取消息内容
	msgFromMQ := pbmsg.MsgDataToMongoByMQ{} // 定义MQ消息协议结构体
	err := proto.Unmarshal(msg, &msgFromMQ) // 反序列化消息内容
	if err != nil {                         // 反序列化失败处理
		log.ZError(ctx, "unmarshall failed", err, "key", key, "len", len(msg))
		return
	}
	if len(msgFromMQ.MsgData) == 0 { // 消息数据为空处理
		log.ZError(ctx, "msgFromMQ.MsgData is empty", nil, "cMsg", cMsg)
		return
	}
	log.ZInfo(ctx, "mongo consumer recv msg", "msgs", msgFromMQ.String())                                        // 打印收到的消息日志
	err = mc.msgDatabase.BatchInsertChat2DB(ctx, msgFromMQ.ConversationID, msgFromMQ.MsgData, msgFromMQ.LastSeq) // 批量插入消息到数据库
	if err != nil {                                                                                              // 插入数据库失败处理
		log.ZError(
			ctx,
			"single data insert to mongo err",
			err,
			"msg",
			msgFromMQ.MsgData,
			"conversationID",
			msgFromMQ.ConversationID,
		)
		prommetrics.MsgInsertMongoFailedCounter.Inc() // 增加失败指标计数
	} else {
		prommetrics.MsgInsertMongoSuccessCounter.Inc() // 增加成功指标计数
	}
	var seqs []int64                        // 消息序列号数组
	for _, msg := range msgFromMQ.MsgData { // 遍历消息数据
		seqs = append(seqs, msg.Seq) // 收集每条消息的序列号
	}
	err = mc.msgDatabase.DeleteMessagesFromCache(ctx, msgFromMQ.ConversationID, seqs) // 从缓存删除这些消息
	if err != nil {                                                                   // 删除缓存失败处理
		log.ZError(
			ctx,
			"remove cache msg from redis err",
			err,
			"msg",
			msgFromMQ.MsgData,
			"conversationID",
			msgFromMQ.ConversationID,
		)
	}
	mc.msgDatabase.DelUserDeleteMsgsList(ctx, msgFromMQ.ConversationID, seqs) // 删除用户删除消息列表
}

// Setup 消费组的Setup方法（启动前调用，空实现即可）
func (OnlineHistoryMongoConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup 消费组的Cleanup方法（结束后调用，空实现即可）
func (OnlineHistoryMongoConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 消费组的主消费方法
func (mc *OnlineHistoryMongoConsumerHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession, // 消费组Session
	claim sarama.ConsumerGroupClaim, // 消费组Claim
) error { // a instance in the consumer group
	log.ZDebug(context.Background(), "online new session msg come", "highWaterMarkOffset",
		claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition()) // 打印会话调试信息
	for msg := range claim.Messages() { // 遍历消费到的所有消息
		ctx := mc.historyConsumerGroup.GetContextFromMsg(msg) // 获取消息上下文
		if len(msg.Value) != 0 {                              // 消息内容不为空
			mc.handleChatWs2Mongo(ctx, msg, string(msg.Key), sess) // 处理消息存储到Mongo
		} else {
			log.ZError(ctx, "mongo msg get from kafka but is nil", nil, "conversationID", msg.Key) // 空消息错误日志
		}
		sess.MarkMessage(msg, "") // 标记消息已消费
	}
	return nil // 消费完成返回
}
