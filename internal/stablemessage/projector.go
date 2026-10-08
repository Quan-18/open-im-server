package stablemessage

import (
	"context"

	"errors"
	"fmt"

	"github.com/google/uuid"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/open-im-server/v3/pkg/common/convert"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/cache"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcli"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/conversation"
	msgpb "github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	"github.com/openimsdk/tools/mq"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

// Projector 从 Mongo 持久水位推进投影；队列只承担可恢复投递。
type Projector struct {
	store        *Store
	redis        redis.UniversalClient
	conversation *rpcli.ConversationClient
	push         mq.Producer
	cache        cache.MsgCache
}

// NewProjector 装配真实缓存、会话和队列端口。
func NewProjector(s *Store, r redis.UniversalClient, c *rpcli.ConversationClient, p mq.Producer, cache cache.MsgCache) *Projector {
	return &Projector{s, r, c, p, cache}
}

type projectionWater struct {
	ID           string `bson:"_id"`
	Seq          int64  `bson:"seq"`
	Projected    int64  `bson:"projected_seq"`
	Epoch        int64  `bson:"epoch"`
	Generation   int64  `bson:"generation"`
	RebuildUntil int64  `bson:"rebuild_until"`
}

// Run 不让一次投影错误结束后台恢复。
func (p *Projector) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := 0; i < 100; i++ {
				progress, err := p.Step(ctx)
				if err != nil {
					log.ZWarn(ctx, "稳定消息投影待恢复", ErrUnavailable)
					break
				}
				if !progress {
					break
				}
			}
		}
	}
}

func (p *Projector) epoch(ctx context.Context) (int64, error) {
	key := "STABLE_PROJECTION:" + p.store.Namespace
	value, err := p.redis.Get(ctx, key).Int64()
	if err == nil {
		var mode struct {
			Epoch int64 `bson:"projection_epoch"`
		}
		if err = p.store.DB.Collection(ModeCollection).FindOne(ctx, bson.M{"_id": "mode"}).Decode(&mode); err != nil {
			return 0, err
		}
		if value > mode.Epoch {
			return 0, ErrIntegrity
		}
		return p.publishEpoch(ctx, key, mode.Epoch)
	}
	if !errors.Is(err, redis.Nil) {
		return 0, err
	}
	owner := uuid.NewString()
	locked, err := p.redis.SetNX(ctx, key+":initializing", owner, 10*time.Second).Result()
	if err != nil {
		return 0, err
	}
	if !locked {
		return 0, ErrUnavailable
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.releaseEpochLock(cleanup, key+":initializing", owner)
	}()
	var mode struct {
		Epoch int64 `bson:"projection_epoch"`
	}
	err = p.store.DB.Collection(ModeCollection).FindOneAndUpdate(ctx, bson.M{"_id": "mode"}, bson.M{"$inc": bson.M{"projection_epoch": 1}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&mode)
	if err != nil {
		return 0, err
	}
	return p.publishEpoch(ctx, key, mode.Epoch)
}

func (p *Projector) publishEpoch(ctx context.Context, key string, epoch int64) (int64, error) {
	return p.redis.Eval(ctx, `local current=tonumber(redis.call('GET',KEYS[1]) or '0'); local wanted=tonumber(ARGV[1]); if wanted>current then redis.call('SET',KEYS[1],ARGV[1]); return wanted; end; return current`, []string{key}, epoch).Int64()
}

func (p *Projector) releaseEpochLock(ctx context.Context, key, owner string) error {
	return p.redis.Eval(ctx, `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]); end; return 0`, []string{key}, owner).Err()
}

// Step 按会话租约处理下一条已提交消息；旧代次不能完成新待办。
func (p *Projector) Step(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctx = mcontext.SetOperationID(ctx, "stable_projection_"+uuid.NewString())
	ctx = authverify.WithTempAdmin(ctx)
	epoch, err := p.epoch(ctx)
	if err != nil {
		return false, err
	}
	watermarks := p.store.DB.Collection(WatermarkCollection)
	// 重建仅重置派生进度，并记录快照边界；既有正文与 Seq 保持不变。
	_, err = watermarks.UpdateMany(ctx, bson.M{"epoch": bson.M{"$lt": epoch}}, mongo.Pipeline{bson.D{{Key: "$set", Value: bson.M{"epoch": epoch, "projected_seq": int64(0), "rebuild_until": "$seq", "lease_until": time.Time{}, "generation": bson.M{"$add": bson.A{"$generation", 1}}}}}})
	if err != nil {
		return false, err
	}
	var w projectionWater
	err = watermarks.FindOneAndUpdate(ctx, bson.M{"epoch": epoch, "lease_until": bson.M{"$lte": time.Now()}, "$expr": bson.M{"$lt": bson.A{"$projected_seq", "$seq"}}}, bson.M{"$set": bson.M{"lease_until": time.Now().Add(30 * time.Second)}, "$inc": bson.M{"generation": 1}}, options.FindOneAndUpdate().SetSort(bson.D{{Key: "_id", Value: 1}}).SetReturnDocument(options.After)).Decode(&w)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	filter := bson.M{"_id": w.ID, "epoch": epoch, "generation": w.Generation}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = watermarks.UpdateOne(cleanup, filter, bson.M{"$set": bson.M{"lease_until": time.Time{}}})
	}()
	seq := w.Projected + 1
	var receipt receiptRecord
	err = p.store.DB.Collection(ReceiptCollection).FindOne(ctx, bson.M{"descriptor.conversation_id": w.ID, "seq": seq, "status": Committed}).Decode(&receipt)
	if err != nil {
		return false, err
	}
	message, err := p.store.Message(ctx, w.ID, seq)
	if err != nil {
		return false, err
	}
	if err = p.store.verify(ctx, receipt); err != nil {
		return false, err
	}
	if err = p.cache.SetMessageBySeqs(ctx, w.ID, []*model.MsgInfoModel{{Msg: message, DelList: []string{}}}); err != nil {
		return false, err
	}
	if err = p.ensureConversation(ctx, receipt); err != nil {
		return false, err
	}
	if seq > w.RebuildUntil || receipt.Pending {
		data, e := proto.Marshal(&msgpb.PushMsgDataToMQ{ConversationID: w.ID, MsgData: convert.MsgDB2Pb(message)})
		if e != nil {
			return false, e
		}
		if e = p.push.SendMessage(ctx, w.ID, data); e != nil {
			return false, e
		}
	}
	_, err = p.store.transaction(ctx, true, func(sc mongo.SessionContext) (*pb.Receipt, error) {
		res, e := watermarks.UpdateOne(sc, filter, bson.M{"$set": bson.M{"projected_seq": seq, "lease_until": time.Time{}}})
		if e != nil {
			return nil, e
		}
		if res.MatchedCount != 1 {
			return nil, ErrUnavailable
		}
		_, e = p.store.DB.Collection(ReceiptCollection).UpdateOne(sc, p.store.sourceFilter(receipt.Namespace, receipt.SourceID), bson.M{"$set": bson.M{"pending": false}})
		return nil, e
	})
	return err == nil, err
}

func (p *Projector) ensureConversation(ctx context.Context, row receiptRecord) error {
	d := row.Descriptor
	for _, user := range []string{d.SenderID, d.ReceiverID} {
		peer := d.SenderID
		if user == d.SenderID {
			peer = d.ReceiverID
		}
		existing, err := p.conversation.GetConversation(ctx, d.ConversationID, user)
		if err == nil && existing != nil && existing.ConversationID == d.ConversationID {
			if existing.OwnerUserID != user || existing.UserID != peer || existing.ConversationType != constant.SingleChatType {
				return ErrIntegrity
			}
			continue
		}
		if err = p.conversation.CreateSingleChatConversations(ctx, &conversation.CreateSingleChatConversationsReq{SendID: d.SenderID, RecvID: d.ReceiverID, ConversationID: d.ConversationID, ConversationType: constant.SingleChatType}); err != nil {
			return err
		}
		existing, err = p.conversation.GetConversation(ctx, d.ConversationID, user)
		if err != nil || existing == nil || existing.ConversationID != d.ConversationID {
			return ErrUnavailable
		}
		if existing.OwnerUserID != user || existing.UserID != peer || existing.ConversationType != constant.SingleChatType {
			return ErrIntegrity
		}
	}
	return nil
}

// ReadCommittedWater 读取包含时间的正式水位，供诊断与测试复核。
func (s *Store) ReadCommittedWater(ctx context.Context, conversation string) (int64, error) {
	var row projectionWater
	if err := s.DB.Collection(WatermarkCollection).FindOne(ctx, bson.M{"_id": conversation}).Decode(&row); err != nil {
		return 0, err
	}
	return row.Seq, nil
}

// ProjectionKey 返回不含凭证的恢复哨兵键。
func (p *Projector) ProjectionKey() string {
	return fmt.Sprintf("STABLE_PROJECTION:%s", p.store.Namespace)
}
