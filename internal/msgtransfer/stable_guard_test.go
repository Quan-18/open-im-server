package msgtransfer

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/open-im-server/v3/pkg/stablepolicy"
	"github.com/openimsdk/protocol/constant"
	pbmsg "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/mq"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

type guardedQueueMessage struct {
	data      []byte
	marked    bool
	committed bool
}

func (m *guardedQueueMessage) Context() context.Context { return context.Background() }
func (m *guardedQueueMessage) Key() string              { return "a_b" }
func (m *guardedQueueMessage) Value() []byte            { return m.data }
func (m *guardedQueueMessage) Mark()                    { m.marked = true }
func (m *guardedQueueMessage) Commit()                  { m.committed = true }

func TestStableQueueGuardsPersistQuarantineBeforeAck(t *testing.T) {
	uri := os.Getenv("OPENIM_STABLE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("需要隔离副本集")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	db := client.Database("stable_guard_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	defer client.Disconnect(ctx)
	defer db.Drop(ctx)
	config.SetStableMode(true)
	defer config.SetStableMode(false)
	message := &sdkws.MsgData{SendID: "a", RecvID: "b", ClientMsgID: "legacy", SessionType: constant.SingleChatType, ContentType: constant.Text, Content: []byte("不得落正式历史"), Options: msgprocessor.NewMsgOptions()}
	raw, err := proto.Marshal(&pbmsg.MsgDataToMQ{Token: "test-secret-must-not-persist", MsgData: message})
	require.NoError(t, err)
	redisMessage := &guardedQueueMessage{data: raw}
	redisHandler := OnlineHistoryRedisConsumerHandler{legacyGuard: func(c context.Context, m mq.Message) (bool, error) { return stablepolicy.GuardRedisMessage(c, db, m) }}
	require.NoError(t, redisHandler.HandlerRedisMessage(redisMessage))
	require.True(t, redisMessage.marked)
	require.True(t, redisMessage.committed)
	require.NoError(t, redisHandler.HandlerRedisMessage(redisMessage))
	count, err := db.Collection("stable_message_quarantine").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
	var row struct {
		Message []byte `bson:"message"`
	}
	require.NoError(t, db.Collection("stable_message_quarantine").FindOne(ctx, bson.M{}).Decode(&row))
	require.NotContains(t, string(row.Message), "test-secret-must-not-persist")
	message.Seq = 999
	raw, err = proto.Marshal(&pbmsg.MsgDataToMongoByMQ{ConversationID: "si_a_b", MsgData: []*sdkws.MsgData{message}})
	require.NoError(t, err)
	mongoMessage := &guardedQueueMessage{data: raw}
	mongoHandler := OnlineHistoryMongoConsumerHandler{legacyGuard: func(c context.Context, m mq.Message) (bool, error) { return stablepolicy.GuardMongoMessage(c, db, m) }}
	require.NoError(t, mongoHandler.HandleChatWs2Mongo(mongoMessage))
	require.True(t, mongoMessage.marked)
	require.True(t, mongoMessage.committed)
	for _, collection := range []string{"msg", "seq", "stable_message_watermarks", "stable_message_receipts"} {
		count, err = db.Collection(collection).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Zero(t, count)
	}
	count, err = db.Collection("stable_message_quarantine").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	// 独立通知不占正式位置；复制为正式聊天消息的通知必须被拦截。
	notification := &sdkws.MsgData{SendID: "a", RecvID: "b", SessionType: constant.SingleChatType, ContentType: constant.BusinessNotification, Options: msgprocessor.NewOptions()}
	require.NoError(t, stablepolicy.CheckMessage(notification))
	notification.Options[constant.IsSendMsg] = true
	require.Error(t, stablepolicy.CheckMessage(notification))
	config.SetStableMode(false)
	require.NoError(t, stablepolicy.CheckMessage(message))
}

func TestFailedQuarantineDoesNotAcknowledgeMessage(t *testing.T) {
	uri := os.Getenv("OPENIM_STABLE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("需要隔离副本集")
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri))
	require.NoError(t, err)
	require.NoError(t, client.Disconnect(context.Background()))
	config.SetStableMode(true)
	defer config.SetStableMode(false)
	raw, err := proto.Marshal(&pbmsg.MsgDataToMongoByMQ{ConversationID: "si_a_b"})
	require.NoError(t, err)
	message := &guardedQueueMessage{data: raw}
	_, err = stablepolicy.GuardMongoMessage(context.Background(), client.Database("closed_connection"), message)
	require.Error(t, err)
	require.False(t, message.marked)
	require.False(t, message.committed)
}

func TestTransferStartupRejectsDisabledPersistentStableMode(t *testing.T) {
	uri := os.Getenv("OPENIM_STABLE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("需要隔离副本集")
	}
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	db := client.Database("stable_guard_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	defer client.Disconnect(ctx)
	defer db.Drop(ctx)
	require.NoError(t, stablepolicy.VerifyMode(ctx, db, config.StableMessage{}))
	_, err = db.Collection("stable_message_mode").InsertOne(ctx, bson.M{"_id": "mode", "version": 1, "namespace": "guard-test"})
	require.NoError(t, err)
	cfg := Config{MongodbConfig: config.Mongo{URI: uri, Database: db.Name(), MongoMode: "standalone", MaxRetry: 1, MaxPoolSize: 2}}
	err = Start(ctx, &cfg, nil, nil)
	require.Error(t, err)
	require.False(t, config.StableMode())
	require.NoError(t, stablepolicy.VerifyMode(ctx, db, config.StableMessage{Enabled: true, SourceNamespace: "guard-test"}))
}
