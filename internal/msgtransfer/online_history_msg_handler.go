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

package msgtransfer // 包名为msgtransfer，消息传输相关代码

import (
	"BaoIM-Server/pkg/common/config"        // 引入配置相关
	"BaoIM-Server/pkg/common/db/controller" // 数据库控制器
	"BaoIM-Server/pkg/common/kafka"         // Kafka相关
	"BaoIM-Server/pkg/msgprocessor"         // 消息处理器
	"BaoIM-Server/pkg/rpcclient"            // RPC客户端
	"baoim/protocol/constant"               // 协议常量
	"baoim/protocol/sdkws"                  // 协议消息结构体
	"baoim/tools/errs"                      // 错误处理工具
	"baoim/tools/log"                       // 日志工具
	"baoim/tools/mcontext"                  // 消息上下文工具
	"baoim/tools/utils"                     // 工具包
	"context"                               // 上下文处理包
	"github.com/IBM/sarama"                 // Kafka客户端库
	"github.com/go-redis/redis"             // Redis客户端库
	"google.golang.org/protobuf/proto"      // Protobuf序列化
	"strconv"                               // 字符串与数字转换
	"strings"                               // 字符串处理
	"sync"                                  // 并发同步原语
	"sync/atomic"                           // 原子操作
	"time"                                  // 时间相关
)

const (
	ConsumerMsgs   = 3   // 消费消息命令常量
	SourceMessages = 4   // 源消息命令常量
	MongoMessages  = 5   // Mongo消息命令常量
	ChannelNum     = 100 // 通道数量常量
)

type MsgChannelValue struct { // 消息通道值结构体
	uniqueKey  string          // 唯一键
	ctx        context.Context // 上下文信息
	ctxMsgList []*ContextMsg   // 上下文消息列表
}

type TriggerChannelValue struct { // 触发通道值结构体
	ctx      context.Context           // 上下文信息
	cMsgList []*sarama.ConsumerMessage // Kafka消费消息列表
}

type Cmd2Value struct { // 命令值结构体
	Cmd   int // 命令类型
	Value any // 命令值
}
type ContextMsg struct { // 上下文消息结构体
	message *sdkws.MsgData  // 消息数据
	ctx     context.Context // 上下文信息
}

type OnlineHistoryRedisConsumerHandler struct { // 在线历史Redis消费处理器
	historyConsumerGroup *kafka.MConsumerGroup      // Kafka消费组
	chArrays             [ChannelNum]chan Cmd2Value // 处理消息的通道数组
	msgDistributionCh    chan Cmd2Value             // 消息分发通道

	// singleMsgSuccessCount      uint64 // 单条消息成功计数（已注释）
	// singleMsgFailedCount       uint64 // 单条消息失败计数（已注释）
	// singleMsgSuccessCountMutex sync.Mutex // 成功计数锁（已注释）
	// singleMsgFailedCountMutex  sync.Mutex // 失败计数锁（已注释）

	msgDatabase           controller.CommonMsgDatabase     // 消息数据库
	conversationRpcClient *rpcclient.ConversationRpcClient // 会话RPC客户端
	groupRpcClient        *rpcclient.GroupRpcClient        // 群组RPC客户端
}

// 构造函数，创建新的在线历史Redis消费处理器
func NewOnlineHistoryRedisConsumerHandler(
	config *config.GlobalConfig, // 全局配置
	database controller.CommonMsgDatabase, // 消息数据库
	conversationRpcClient *rpcclient.ConversationRpcClient, // 会话RPC客户端
	groupRpcClient *rpcclient.GroupRpcClient, // 群组RPC客户端
) (*OnlineHistoryRedisConsumerHandler, error) {
	var och OnlineHistoryRedisConsumerHandler    // 创建处理器实例
	och.msgDatabase = database                   // 赋值消息数据库
	och.msgDistributionCh = make(chan Cmd2Value) // 创建无缓冲消息分发通道
	go och.MessagesDistributionHandle()          // 启动消息分发处理协程
	for i := 0; i < ChannelNum; i++ {            // 初始化通道数组
		och.chArrays[i] = make(chan Cmd2Value, 50) // 创建带缓冲的通道
		go och.Run(i)                              // 启动每个通道的处理协程
	}
	och.conversationRpcClient = conversationRpcClient // 赋值会话RPC客户端
	och.groupRpcClient = groupRpcClient               // 赋值群组RPC客户端
	var err error                                     // 错误变量

	var tlsConfig *kafka.TLSConfig // TLS配置
	if config.Kafka.TLS != nil {   // 判断是否配置TLS
		tlsConfig = &kafka.TLSConfig{ // 构建TLS配置
			CACrt:              config.Kafka.TLS.CACrt,
			ClientCrt:          config.Kafka.TLS.ClientCrt,
			ClientKey:          config.Kafka.TLS.ClientKey,
			ClientKeyPwd:       config.Kafka.TLS.ClientKeyPwd,
			InsecureSkipVerify: false,
		}
	}

	och.historyConsumerGroup, err = kafka.NewMConsumerGroup(&kafka.MConsumerGroupConfig{
		KafkaVersion:   sarama.V2_0_0_0,       // Kafka版本
		OffsetsInitial: sarama.OffsetNewest,   // 初始偏移量
		IsReturnErr:    false,                 // 是否返回错误
		UserName:       config.Kafka.Username, // 用户名
		Password:       config.Kafka.Password, // 密码
	}, []string{config.Kafka.LatestMsgToRedis.Topic}, // 主题
		config.Kafka.Addr,                       // Kafka地址
		config.Kafka.ConsumerGroupID.MsgToRedis, // 消费组ID
		tlsConfig,                               // TLS配置
	)
	// statistics.NewStatistics(&och.singleMsgSuccessCount, config.Config.ModuleName.MsgTransferName, fmt.Sprintf("%d
	// second singleMsgCount insert to mongo", constant.StatisticsTimeInterval), constant.StatisticsTimeInterval)
	return &och, err // 返回处理器实例和错误
}

// 处理每个通道的消息
func (och *OnlineHistoryRedisConsumerHandler) Run(channelID int) {
	for cmd := range och.chArrays[channelID] { // 遍历通道中的命令
		switch cmd.Cmd { // 根据命令类型分支
		case SourceMessages: // 源消息类型
			msgChannelValue := cmd.Value.(MsgChannelValue) // 断言为MsgChannelValue
			ctxMsgList := msgChannelValue.ctxMsgList       // 获取消息列表
			ctx := msgChannelValue.ctx                     // 获取上下文
			log.ZDebug(                                    // 打印调试日志
				ctx,
				"msg arrived channel",
				"channel id",
				channelID,
				"msgList length",
				len(ctxMsgList),
				"uniqueKey",
				msgChannelValue.uniqueKey,
			)
			storageMsgList, notStorageMsgList, storageNotificationList, notStorageNotificationList, modifyMsgList := och.getPushStorageMsgList(
				ctxMsgList, // 获取消息分类列表
			)
			log.ZDebug( // 打印消息长度分类日志
				ctx,
				"msg lens",
				"storageMsgList",
				len(storageMsgList),
				"notStorageMsgList",
				len(notStorageMsgList),
				"storageNotificationList",
				len(storageNotificationList),
				"notStorageNotificationList",
				len(notStorageNotificationList),
				"modifyMsgList",
				len(modifyMsgList),
			)
			conversationIDMsg := msgprocessor.GetChatConversationIDByMsg(ctxMsgList[0].message)                  // 获取会话ID
			conversationIDNotification := msgprocessor.GetNotificationConversationIDByMsg(ctxMsgList[0].message) // 获取通知会话ID
			och.handleMsg(ctx, msgChannelValue.uniqueKey, conversationIDMsg, storageMsgList, notStorageMsgList)  // 处理消息
			och.handleNotification(
				ctx,
				msgChannelValue.uniqueKey,
				conversationIDNotification,
				storageNotificationList,
				notStorageNotificationList,
			) // 处理通知
			if err := och.msgDatabase.MsgToModifyMQ(ctx, msgChannelValue.uniqueKey, conversationIDNotification, modifyMsgList); err != nil {
				log.ZError(ctx, "msg to modify mq error", err, "uniqueKey", msgChannelValue.uniqueKey, "modifyMsgList", modifyMsgList) // 错误日志
			}
		}
	}
}

// 获取消息/通知存储列表、未存储列表等
func (och *OnlineHistoryRedisConsumerHandler) getPushStorageMsgList(
	totalMsgs []*ContextMsg, // 总消息列表
) (storageMsgList, notStorageMsgList, storageNotificatoinList, notStorageNotificationList, modifyMsgList []*sdkws.MsgData) {
	isStorage := func(msg *sdkws.MsgData) bool { // 判断消息是否需要存储
		options2 := msgprocessor.Options(msg.Options)
		if options2.IsHistory() {
			return true
		} else {
			// if !(!options2.IsSenderSync() && conversationID == msg.MsgData.SendID) {
			// 	return false
			// }
			return false
		}
	}
	for _, v := range totalMsgs { // 遍历每一条消息
		options := msgprocessor.Options(v.message.Options)
		if !options.IsNotNotification() { // 如果不是通知消息
			// clone msg from notificationMsg
			if options.IsSendMsg() { // 如果是发送消息
				msg := proto.Clone(v.message).(*sdkws.MsgData) // 克隆消息
				// message
				if v.message.Options != nil {
					msg.Options = msgprocessor.NewMsgOptions() // 新建消息选项
				}
				if options.IsOfflinePush() { // 是否离线推送
					v.message.Options = msgprocessor.WithOptions(
						v.message.Options,
						msgprocessor.WithOfflinePush(false),
					)
					msg.Options = msgprocessor.WithOptions(msg.Options, msgprocessor.WithOfflinePush(true))
				}
				if options.IsUnreadCount() { // 是否未读计数
					v.message.Options = msgprocessor.WithOptions(
						v.message.Options,
						msgprocessor.WithUnreadCount(false),
					)
					msg.Options = msgprocessor.WithOptions(msg.Options, msgprocessor.WithUnreadCount(true))
				}
				storageMsgList = append(storageMsgList, msg) // 加入存储消息列表
			}
			if isStorage(v.message) { // 判断是否存储
				storageNotificatoinList = append(storageNotificatoinList, v.message)
			} else {
				notStorageNotificationList = append(notStorageNotificationList, v.message)
			}
		} else { // 是普通消息
			if isStorage(v.message) { // 判断是否存储
				storageMsgList = append(storageMsgList, v.message)
			} else {
				notStorageMsgList = append(notStorageMsgList, v.message)
			}
		}
		if v.message.ContentType == constant.ReactionMessageModifier ||
			v.message.ContentType == constant.ReactionMessageDeleter {
			modifyMsgList = append(modifyMsgList, v.message) // 修改消息加入modify列表
		}
	}
	return
}

// 处理通知消息
func (och *OnlineHistoryRedisConsumerHandler) handleNotification(
	ctx context.Context, // 上下文
	key, conversationID string, // 唯一键和会话ID
	storageList, notStorageList []*sdkws.MsgData, // 存储和未存储列表
) {
	och.toPushTopic(ctx, key, conversationID, notStorageList) // 未存储消息直接推送
	if len(storageList) > 0 {                                 // 如果有存储消息
		lastSeq, _, err := och.msgDatabase.BatchInsertChat2Cache(ctx, conversationID, storageList) // 批量插入Redis
		if err != nil {
			log.ZError(
				ctx,
				"notification batch insert to redis error",
				err,
				"conversationID",
				conversationID,
				"storageList",
				storageList,
			)
			return
		}
		log.ZDebug(ctx, "success to next topic", "conversationID", conversationID)
		err = och.msgDatabase.MsgToMongoMQ(ctx, key, conversationID, storageList, lastSeq) // 存入Mongo队列
		if err != nil {
			log.ZError(ctx, "MsgToMongoMQ error", err)
		}
		och.toPushTopic(ctx, key, conversationID, storageList) // 存储消息推送
	}
}

// 推送消息到MQ（实际为异步队列或MQ）
func (och *OnlineHistoryRedisConsumerHandler) toPushTopic(
	ctx context.Context, // 上下文
	key, conversationID string, // 唯一键和会话ID
	msgs []*sdkws.MsgData, // 消息列表
) {
	for _, v := range msgs { // 遍历每条消息
		och.msgDatabase.MsgToPushMQ(ctx, key, conversationID, v) // 推送到MQ，不处理错误
	}
}

// 处理普通消息
func (och *OnlineHistoryRedisConsumerHandler) handleMsg(
	ctx context.Context, // 上下文
	key, conversationID string, // 唯一键和会话ID
	storageList, notStorageList []*sdkws.MsgData, // 存储和未存储列表
) {
	och.toPushTopic(ctx, key, conversationID, notStorageList) // 未存储消息推送
	if len(storageList) > 0 {                                 // 有存储消息
		lastSeq, isNewConversation, err := och.msgDatabase.BatchInsertChat2Cache(ctx, conversationID, storageList) // 插入Redis
		if err != nil && errs.Unwrap(err) != redis.Nil {
			log.ZError(ctx, "batch data insert to redis err", err, "storageMsgList", storageList)
			return
		}
		if isNewConversation { // 是否新会话
			switch storageList[0].SessionType { // 根据会话类型分支
			case constant.SuperGroupChatType: // 超级群聊
				log.ZInfo(ctx, "group chat first create conversation", "conversationID",
					conversationID)
				userIDs, err := och.groupRpcClient.GetGroupMemberIDs(ctx, storageList[0].GroupID) // 获取群成员ID
				if err != nil {
					log.ZWarn(ctx, "get group member ids error", err, "conversationID",
						conversationID)
				} else {
					if err := och.conversationRpcClient.GroupChatFirstCreateConversation(ctx,
						storageList[0].GroupID, userIDs); err != nil {
						log.ZWarn(ctx, "single chat first create conversation error", err,
							"conversationID", conversationID)
					}
				}
			case constant.GroupChatType: ///增加聊天室识别
				log.ZInfo(ctx, "group chat first create conversation", "conversationID",
					conversationID)

				//聊天室创建时创建会话  房间会话先删除
				userIDs, err := och.groupRpcClient.GetGroupMemberIDs(ctx, storageList[0].GroupID) // 获取群成员ID
				if err != nil {
					log.ZWarn(ctx, "get group member ids error", err, "conversationID",
						conversationID)
				} else {

					//创建会话
					if err := och.conversationRpcClient.RoomGroupChatFirstCreateConversation(ctx,
						storageList[0].GroupID, userIDs); err != nil {
						log.ZWarn(ctx, "single chat first create conversation error", err,
							"conversationID", conversationID)
					}
				}
			case constant.SingleChatType, constant.NotificationChatType: // 单聊或通知
				if err := och.conversationRpcClient.SingleChatFirstCreateConversation(ctx, storageList[0].RecvID,
					storageList[0].SendID, conversationID, storageList[0].SessionType); err != nil {
					log.ZWarn(ctx, "single chat or notification first create conversation error", err,
						"conversationID", conversationID, "sessionType", storageList[0].SessionType)
				}
			default: // 未知类型
				log.ZWarn(ctx, "unknown session type", nil, "sessionType",
					storageList[0].SessionType)
			}
		}

		log.ZDebug(ctx, "success incr to next topic")
		err = och.msgDatabase.MsgToMongoMQ(ctx, key, conversationID, storageList, lastSeq) // 存入Mongo队列
		if err != nil {
			log.ZError(ctx, "MsgToMongoMQ error", err)
		}
		och.toPushTopic(ctx, key, conversationID, storageList) // 存储消息推送
	}
}

// 消息分发处理协程
func (och *OnlineHistoryRedisConsumerHandler) MessagesDistributionHandle() {
	for {
		aggregationMsgs := make(map[string][]*ContextMsg, ChannelNum) // 创建聚合消息map
		select {
		case cmd := <-och.msgDistributionCh: // 监听分发通道
			switch cmd.Cmd { // 根据命令类型分支
			case ConsumerMsgs: // 消费消息类型
				triggerChannelValue := cmd.Value.(TriggerChannelValue) // 断言为TriggerChannelValue
				ctx := triggerChannelValue.ctx                         // 获取上下文
				consumerMessages := triggerChannelValue.cMsgList       // 获取消息列表
				// Aggregation map[userid]message list
				log.ZDebug(ctx, "batch messages come to distribution center", "length", len(consumerMessages))
				for i := 0; i < len(consumerMessages); i++ { // 遍历每条消息
					ctxMsg := &ContextMsg{}                                      // 创建上下文消息
					msgFromMQ := &sdkws.MsgData{}                                // 创建消息数据对象
					err := proto.Unmarshal(consumerMessages[i].Value, msgFromMQ) // 反序列化消息
					if err != nil {
						log.ZError(ctx, "msg_transfer Unmarshal msg err", err, string(consumerMessages[i].Value))
						continue
					}
					var arr []string
					for i, header := range consumerMessages[i].Headers { // 遍历消息头
						arr = append(arr, strconv.Itoa(i), string(header.Key), string(header.Value))
					}
					log.ZInfo(
						ctx,
						"consumer.kafka.GetContextWithMQHeader",
						"len",
						len(consumerMessages[i].Headers),
						"header",
						strings.Join(arr, ", "),
					)
					ctxMsg.ctx = kafka.GetContextWithMQHeader(consumerMessages[i].Headers) // 通过消息头获取上下文
					ctxMsg.message = msgFromMQ                                             // 赋值消息体
					log.ZDebug(
						ctx,
						"single msg come to distribution center",
						"message",
						msgFromMQ,
						"key",
						string(consumerMessages[i].Key),
					)
					// aggregationMsgs[string(consumerMessages[i].Key)] =
					// append(aggregationMsgs[string(consumerMessages[i].Key)], ctxMsg)
					if oldM, ok := aggregationMsgs[string(consumerMessages[i].Key)]; ok {
						oldM = append(oldM, ctxMsg) // 已有key则追加
						aggregationMsgs[string(consumerMessages[i].Key)] = oldM
					} else {
						m := make([]*ContextMsg, 0, 100) // 新建切片
						m = append(m, ctxMsg)
						aggregationMsgs[string(consumerMessages[i].Key)] = m
					}
				}
				log.ZDebug(ctx, "generate map list users len", "length", len(aggregationMsgs))
				for uniqueKey, v := range aggregationMsgs { // 遍历聚合后的用户key
					if len(v) >= 0 { // 有消息
						hashCode := utils.GetHashCode(uniqueKey) // 获取hash值
						channelID := hashCode % ChannelNum       // 计算通道ID
						newCtx := withAggregationCtx(ctx, v)     // 聚合上下文
						log.ZDebug(
							newCtx,
							"generate channelID",
							"hashCode",
							hashCode,
							"channelID",
							channelID,
							"uniqueKey",
							uniqueKey,
						)
						och.chArrays[channelID] <- Cmd2Value{Cmd: SourceMessages, Value: MsgChannelValue{uniqueKey: uniqueKey, ctxMsgList: v, ctx: newCtx}} // 发送到通道
					}
				}
			}
		}
	}
}

// 聚合上下文，将每条消息的operationID拼接
func withAggregationCtx(ctx context.Context, values []*ContextMsg) context.Context {
	var allMessageOperationID string // operationID聚合字符串
	for i, v := range values {
		if opid := mcontext.GetOperationID(v.ctx); opid != "" { // 获取每条消息的operationID
			if i == 0 {
				allMessageOperationID += opid
			} else {
				allMessageOperationID += "$" + opid // 多个用$分隔
			}
		}
	}
	return mcontext.SetOperationID(ctx, allMessageOperationID) // 设置到上下文中
}

// Kafka消费组接口实现：初始化
func (och *OnlineHistoryRedisConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Kafka消费组接口实现：清理
func (och *OnlineHistoryRedisConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Kafka消费组接口实现：实际消费处理
func (och *OnlineHistoryRedisConsumerHandler) ConsumeClaim(
	sess sarama.ConsumerGroupSession, // 消费组会话
	claim sarama.ConsumerGroupClaim, // 消费组声明
) error { // 一个消费组实例
	for {
		if sess == nil { // 会话为空时等待
			log.ZWarn(context.Background(), "sess == nil, waiting", nil)
			time.Sleep(100 * time.Millisecond) // 休眠100毫秒
		} else {
			break // 会话正常则跳出循环
		}
	}
	log.ZDebug(context.Background(), "online new session msg come", "highWaterMarkOffset",
		claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition())

	var (
		split    = 1000                                     // 单次处理最大消息数
		rwLock   = new(sync.RWMutex)                        // 读写锁
		messages = make([]*sarama.ConsumerMessage, 0, 1000) // 消息缓冲区
		ticker   = time.NewTicker(time.Millisecond * 100)   // 定时器100ms

		wg      = sync.WaitGroup{} // 等待组
		running = new(atomic.Bool) // 标记是否运行
	)
	running.Store(true) // 设置标志为运行

	wg.Add(1)
	go func() { // 定时批量处理协程
		defer wg.Done()

		for {
			select {
			case <-ticker.C: // 每隔100ms触发
				// 如果缓冲区为空且已停止则退出循环
				if len(messages) == 0 {
					if !running.Load() {
						return
					}
					continue // 继续等待
				}

				rwLock.Lock()
				buffer := make([]*sarama.ConsumerMessage, 0, len(messages)) // 新建临时缓冲区
				buffer = append(buffer, messages...)

				// 复用切片，清空
				messages = messages[:0]
				rwLock.Unlock()

				start := time.Now()
				ctx := mcontext.WithTriggerIDContext(context.Background(), utils.OperationIDGenerator())
				log.ZDebug(ctx, "timer trigger msg consumer start", "length", len(buffer))
				for i := 0; i < len(buffer)/split; i++ { // 分批处理消息
					och.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
						ctx: ctx, cMsgList: buffer[i*split : (i+1)*split],
					}}
				}
				if (len(buffer) % split) > 0 { // 处理剩余消息
					och.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
						ctx: ctx, cMsgList: buffer[split*(len(buffer)/split):],
					}}
				}

				log.ZDebug(ctx, "timer trigger msg consumer end",
					"length", len(buffer), "time_cost", time.Since(start),
				)
			}
		}
	}()

	wg.Add(1)
	go func() { // 消息接收协程
		defer wg.Done()

		for running.Load() { // 只要标志未停止
			select {
			case msg, ok := <-claim.Messages(): // 从Kafka消费消息
				if !ok { // 通道关闭则停止
					running.Store(false)
					return
				}

				if len(msg.Value) == 0 { // 空消息跳过
					continue
				}

				rwLock.Lock()
				messages = append(messages, msg) // 加入缓冲区
				rwLock.Unlock()

				sess.MarkMessage(msg, "") // 标记消息已消费

			case <-sess.Context().Done(): // 会话结束
				running.Store(false)
				return
			}
		}
	}()

	wg.Wait() // 等待所有协程结束
	return nil
}
