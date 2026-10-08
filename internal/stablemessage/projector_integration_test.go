package stablemessage

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/cachekey"
	cacheRedis "github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache/redis"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"
	"github.com/openimsdk/protocol/conversation"
	"github.com/openimsdk/tools/mq/redismq"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
)

type projectionConversations struct {
	conversation.ConversationClient
	db *mongo.Database
}

func (c *projectionConversations) CreateSingleChatConversations(ctx context.Context, r *conversation.CreateSingleChatConversationsReq, _ ...grpc.CallOption) (*conversation.CreateSingleChatConversationsResp, error) {
	for _, owner := range []string{r.SendID, r.RecvID} {
		_, err := c.db.Collection("test_conversations").UpdateOne(ctx, bson.M{"_id": owner + ":" + r.ConversationID}, bson.M{"$setOnInsert": bson.M{"owner": owner, "conversation": r.ConversationID, "sender": r.SendID, "receiver": r.RecvID}}, options.Update().SetUpsert(true))
		if err != nil {
			return nil, err
		}
	}
	return &conversation.CreateSingleChatConversationsResp{}, nil
}
func (c *projectionConversations) GetConversation(ctx context.Context, r *conversation.GetConversationReq, _ ...grpc.CallOption) (*conversation.GetConversationResp, error) {
	if err := c.db.Collection("test_conversations").FindOne(ctx, bson.M{"_id": r.OwnerUserID + ":" + r.ConversationID}).Err(); err != nil {
		return nil, err
	}
	var row struct {
		Sender   string `bson:"sender"`
		Receiver string `bson:"receiver"`
	}
	if err := c.db.Collection("test_conversations").FindOne(ctx, bson.M{"_id": r.OwnerUserID + ":" + r.ConversationID}).Decode(&row); err != nil {
		return nil, err
	}
	peer := row.Sender
	if peer == r.OwnerUserID {
		peer = row.Receiver
	}
	return &conversation.GetConversationResp{Conversation: &conversation.Conversation{OwnerUserID: r.OwnerUserID, UserID: peer, ConversationID: r.ConversationID, ConversationType: 1}}, nil
}

type projectionProducer struct {
	redis     redis.UniversalClient
	key       string
	failAfter bool
}

func (p *projectionProducer) SendMessage(ctx context.Context, key string, data []byte) error {
	if err := redismq.NewProducer(p.redis, p.key, 0).SendMessage(ctx, key, data); err != nil {
		return err
	}
	if p.failAfter {
		p.failAfter = false
		return errors.New("测试发布响应丢失")
	}
	return nil
}
func (p *projectionProducer) Close() error { return nil }

func TestProjectionReplayAndRedisRebuildKeepCanonicalHistory(t *testing.T) {
	s := integrationStore(t)
	address := os.Getenv("OPENIM_STABLE_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("需要隔离 Redis")
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: address, Password: os.Getenv("OPENIM_STABLE_TEST_REDIS_PASSWORD")})
	require.NoError(t, rdb.Ping(ctx).Err())
	defer rdb.Close()
	msgDB, err := mgo.NewMsgMongo(s.DB)
	require.NoError(t, err)
	cache := cacheRedis.NewMsgCache(rdb, msgDB)
	producer := &projectionProducer{redis: rdb, key: "stable-test-push:" + uuid.NewString(), failAfter: true}
	p := NewProjector(s, rdb, &rpcli.ConversationClient{ConversationClient: &projectionConversations{db: s.DB}}, producer, cache)
	keys := []string{p.ProjectionKey(), p.ProjectionKey() + ":initializing", producer.key, cachekey.GetMsgCacheKey("si_a_b", 1), cachekey.GetMallocSeqKey("si_a_b")}
	require.NoError(t, rdb.Del(ctx, keys...).Err())
	t.Cleanup(func() { _ = rdb.Del(context.Background(), keys...).Err() })
	body := []byte("持久正文")
	d := testDescriptor(body)
	receipt, err := s.Submit(ctx, d, body)
	require.NoError(t, err)
	_, err = s.DB.Collection("seq_user").InsertOne(ctx, bson.M{"conversation_id": d.ConversationID, "user_id": "b", "read_seq": int64(0)})
	require.NoError(t, err)
	_, err = p.Step(ctx)
	require.Error(t, err)
	retry, err := s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.NoError(t, err)
	require.Equal(t, receipt, retry)
	progress, err := p.Step(ctx)
	require.NoError(t, err)
	require.True(t, progress)
	require.Equal(t, int64(2), rdb.XLen(ctx, producer.key).Val())
	var row receiptRecord
	require.NoError(t, s.DB.Collection(ReceiptCollection).FindOne(ctx, s.sourceFilter(d.SourceNamespace, d.SourceID)).Decode(&row))
	require.False(t, row.Pending)
	require.NoError(t, rdb.Del(ctx, p.ProjectionKey(), cachekey.GetMsgCacheKey(d.ConversationID, 1)).Err())
	require.NoError(t, rdb.HSet(ctx, cachekey.GetMallocSeqKey(d.ConversationID), "CURR", 999, "LAST", 1000).Err())
	progress, err = p.Step(ctx)
	require.NoError(t, err)
	require.True(t, progress)
	require.Equal(t, int64(2), rdb.XLen(ctx, producer.key).Val())
	config.SetStableMode(true)
	defer config.SetStableMode(false)
	seqDB, err := mgo.NewSeqConversationMongo(s.DB)
	require.NoError(t, err)
	reader := cacheRedis.NewSeqConversationCacheRedis(rdb, seqDB)
	max, err := reader.GetMaxSeq(ctx, d.ConversationID)
	require.NoError(t, err)
	require.Equal(t, int64(1), max)
	var read struct {
		Seq int64 `bson:"read_seq"`
	}
	require.NoError(t, s.DB.Collection("seq_user").FindOne(ctx, bson.M{"user_id": "b"}).Decode(&read))
	require.Zero(t, read.Seq)
	current := rdb.Get(ctx, p.ProjectionKey()).Val()
	epoch, err := rdb.Get(ctx, p.ProjectionKey()).Int64()
	require.NoError(t, err)
	value, err := p.publishEpoch(ctx, p.ProjectionKey(), epoch-1)
	require.NoError(t, err)
	require.Equal(t, epoch, value)
	require.Equal(t, current, rdb.Get(ctx, p.ProjectionKey()).Val())
	require.NoError(t, rdb.Set(ctx, p.ProjectionKey()+":initializing", "new-owner", 0).Err())
	require.NoError(t, p.releaseEpochLock(ctx, p.ProjectionKey()+":initializing", "old-owner"))
	require.Equal(t, "new-owner", rdb.Get(ctx, p.ProjectionKey()+":initializing").Val())
	ids, err := s.Conversations(ctx, "b")
	require.NoError(t, err)
	require.Equal(t, []string{d.ConversationID}, ids)
	messages, _, err := s.ReadRange(ctx, d.ConversationID, "b", 1, 1, 1, true)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, string(body), messages[0].Content)
}
