package stablemessage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/protocol/constant"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Store 只通过 Mongo 本地事务提交正式消息，Redis 不参与确认。
type Store struct {
	DB        *mongo.Database
	Namespace string
}

type receiptRecord struct {
	Namespace   string     `bson:"namespace"`
	SourceID    string     `bson:"source_id"`
	Descriptor  Descriptor `bson:"descriptor"`
	Status      string     `bson:"status"`
	ClientMsgID string     `bson:"client_msg_id"`
	ServerMsgID string     `bson:"server_msg_id"`
	Seq         int64      `bson:"seq"`
	CommittedAt int64      `bson:"committed_at"`
	SealedAt    int64      `bson:"sealed_at"`
	Pending     bool       `bson:"pending"`
}

func (r receiptRecord) proto() *pb.Receipt {
	return &pb.Receipt{Status: r.Status, Descriptor_: r.Descriptor.Proto(), ClientMsgId: r.ClientMsgID, ServerMsgId: r.ServerMsgID, Seq: r.Seq, CommittedAt: r.CommittedAt, SealedAt: r.SealedAt}
}

// Open 验证部署模式、真实副本集、遗留数据与索引。
func Open(ctx context.Context, db *mongo.Database, rdb redis.UniversalClient, cfg config.StableMessage) (*Store, error) {
	if db == nil {
		return nil, ErrUnavailable
	}
	mode := db.Collection(ModeCollection)
	var existing struct {
		Namespace string `bson:"namespace"`
		Version   int    `bson:"version"`
	}
	err := mode.FindOne(ctx, bson.M{"_id": "mode"}).Decode(&existing)
	if !cfg.Enabled {
		if err == nil {
			return nil, fmt.Errorf("stable mode cannot be disabled after activation")
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		return nil, nil
	}
	db = db.Client().Database(db.Name(), options.Database().SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary()).SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true))))
	mode = db.Collection(ModeCollection)
	if !namespacePattern.MatchString(cfg.SourceNamespace) || len(cfg.AdminUserIDs) == 0 {
		return nil, ErrInvalid
	}
	var hello struct {
		SetName        string `bson:"setName"`
		SessionTimeout *int64 `bson:"logicalSessionTimeoutMinutes"`
	}
	if e := db.Client().Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); e != nil {
		return nil, e
	}
	if hello.SetName == "" || hello.SessionTimeout == nil {
		return nil, fmt.Errorf("stable messages require MongoDB replica set transactions")
	}
	if err == nil {
		if existing.Version != 1 || existing.Namespace != cfg.SourceNamespace {
			return nil, fmt.Errorf("stable mode identity mismatch")
		}
	} else if errors.Is(err, mongo.ErrNoDocuments) {
		for _, check := range []struct {
			name   string
			filter bson.M
		}{{"msg", bson.M{"doc_id": bson.M{"$regex": "^si_"}}}, {"seq", bson.M{"conversation_id": bson.M{"$regex": "^si_"}}}, {"conversation", bson.M{"conversation_id": bson.M{"$regex": "^si_"}}}, {ReceiptCollection, bson.M{}}, {WatermarkCollection, bson.M{}}} {
			n, e := db.Collection(check.name).CountDocuments(ctx, check.filter, options.Count().SetLimit(1))
			if e != nil {
				return nil, e
			}
			if n > 0 {
				return nil, fmt.Errorf("stable activation requires empty chat state: %s", check.name)
			}
		}
		if rdb != nil {
			for _, pattern := range []string{"mq:*", "MALLOC_SEQ:si_*"} {
				var cursor uint64
				for {
					keys, next, e := rdb.Scan(ctx, cursor, pattern, 100).Result()
					if e != nil {
						return nil, e
					}
					for _, key := range keys {
						if strings.HasPrefix(key, "mq:") {
							n, e := rdb.XLen(ctx, key).Result()
							if e != nil {
								return nil, e
							}
							if n == 0 {
								continue
							}
						}
						return nil, fmt.Errorf("stable activation found legacy queue or sequence")
					}
					cursor = next
					if cursor == 0 {
						break
					}
				}
			}
		}
		_, e := mode.InsertOne(ctx, bson.M{"_id": "mode", "namespace": cfg.SourceNamespace, "version": 1, "projection_epoch": int64(0)})
		if e != nil && !mongo.IsDuplicateKeyError(e) {
			return nil, e
		}
		if e = mode.FindOne(ctx, bson.M{"_id": "mode"}).Decode(&existing); e != nil {
			return nil, e
		}
		if existing.Namespace != cfg.SourceNamespace || existing.Version != 1 {
			return nil, ErrIntegrity
		}
	} else {
		return nil, err
	}
	store := &Store{DB: db, Namespace: cfg.SourceNamespace}
	for _, item := range []struct {
		name  string
		index mongo.IndexModel
	}{
		{ReceiptCollection, mongo.IndexModel{Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "source_id", Value: 1}}, Options: options.Index().SetUnique(true).SetName("stable_source")}},
		{ReceiptCollection, mongo.IndexModel{Keys: bson.D{{Key: "descriptor.conversation_id", Value: 1}, {Key: "seq", Value: 1}}, Options: options.Index().SetUnique(true).SetPartialFilterExpression(bson.M{"status": Committed}).SetName("stable_position")}},
		{ReceiptCollection, mongo.IndexModel{Keys: bson.D{{Key: "pending", Value: 1}, {Key: "descriptor.conversation_id", Value: 1}, {Key: "seq", Value: 1}}}},
		{"msg", mongo.IndexModel{Keys: bson.D{{Key: "doc_id", Value: 1}}, Options: options.Index().SetUnique(true)}},
	} {
		if _, e := db.Collection(item.name).Indexes().CreateOne(ctx, item.index); e != nil {
			return nil, e
		}
	}
	// 真正启动一次事务读，拒绝只有配置声明而没有事务能力的环境。
	_, err = store.transaction(ctx, false, func(sc mongo.SessionContext) (*pb.Receipt, error) { _, e := store.serverTime(sc); return nil, e })
	return store, err
}

func (s *Store) sourceFilter(namespace, id string) bson.M {
	return bson.M{"namespace": namespace, "source_id": id}
}

func (s *Store) transaction(ctx context.Context, writes bool, fn func(mongo.SessionContext) (*pb.Receipt, error)) (*pb.Receipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	sess, err := s.DB.Client().StartSession()
	if err != nil {
		return nil, ErrUnavailable
	}
	defer sess.EndSession(ctx)
	hasLabel := func(err error, label string) bool {
		var labeled interface{ HasErrorLabel(string) bool }
		return errors.As(err, &labeled) && labeled.HasErrorLabel(label)
	}
	abort := func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = sess.AbortTransaction(cleanup)
	}
	for ctx.Err() == nil {
		if err = sess.StartTransaction(options.Transaction().SetReadPreference(readpref.Primary()).SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true)))); err != nil {
			return nil, err
		}
		sc := mongo.NewSessionContext(ctx, sess)
		result, operationErr := fn(sc)
		if operationErr != nil {
			abort()
			if hasLabel(operationErr, "TransientTransactionError") && ctx.Err() == nil {
				continue
			}
			return nil, operationErr
		}
		// 直接使用有预算的 CommitTransaction；v1.14 WithTransaction 会移除提交阶段的截止。
		attempted := false
		for {
			if ctx.Err() != nil {
				if writes && attempted {
					return nil, ErrUnknownCommit
				}
				abort()
				return nil, ErrUnavailable
			}
			attempted = true
			err = sess.CommitTransaction(sc)
			if err == nil {
				return result, nil
			}
			if hasLabel(err, "UnknownTransactionCommitResult") {
				if ctx.Err() != nil {
					if writes {
						return nil, ErrUnknownCommit
					}
					return nil, ErrUnavailable
				}
				continue
			}
			if hasLabel(err, "TransientTransactionError") && ctx.Err() == nil {
				abort()
				break
			}
			if writes && ctx.Err() != nil {
				return nil, ErrUnknownCommit
			}
			return nil, err
		}
	}
	return nil, ErrUnavailable
}
func (s *Store) serverTime(ctx context.Context) (int64, error) {
	cursor, err := s.DB.Collection(ModeCollection).Aggregate(ctx, mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"_id": "mode"}}}, bson.D{{Key: "$project", Value: bson.M{"now": "$$NOW"}}}})
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)
	if !cursor.Next(ctx) {
		return 0, ErrIntegrity
	}
	var row struct {
		Now time.Time `bson:"now"`
	}
	if err = cursor.Decode(&row); err != nil {
		return 0, err
	}
	return row.Now.UnixMilli(), nil
}

func (s *Store) get(ctx context.Context, namespace, id string) (receiptRecord, error) {
	var row receiptRecord
	err := s.DB.Collection(ReceiptCollection).FindOne(ctx, s.sourceFilter(namespace, id)).Decode(&row)
	return row, err
}

// GetReceipt 在同一快照中校核收据及正文，不以缓存代替证据。
func (s *Store) GetReceipt(ctx context.Context, namespace, id string) (*pb.Receipt, error) {
	if namespace != s.Namespace || !ValidSource(namespace, id) {
		return nil, ErrInvalid
	}
	return s.transaction(ctx, false, func(sc mongo.SessionContext) (*pb.Receipt, error) {
		row, e := s.get(sc, namespace, id)
		if errors.Is(e, mongo.ErrNoDocuments) {
			return &pb.Receipt{Status: NotFound}, nil
		}
		if e != nil {
			return nil, e
		}
		if e = s.verify(sc, row); e != nil {
			return nil, e
		}
		return row.proto(), nil
	})
}

func (s *Store) verify(ctx context.Context, row receiptRecord) error {
	if row.Namespace != s.Namespace || row.Descriptor.SourceNamespace != row.Namespace || row.Descriptor.SourceID != row.SourceID || row.Descriptor.Validate() != nil {
		return ErrIntegrity
	}
	if row.Status == Expired {
		if row.Seq != 0 || row.SealedAt < row.Descriptor.ExpiresAt || row.ClientMsgID != "" || row.ServerMsgID != "" || row.CommittedAt != 0 {
			return ErrIntegrity
		}
		return nil
	}
	if row.Status != Committed || row.Seq <= 0 || row.ClientMsgID != row.SourceID || row.CommittedAt <= 0 || row.SealedAt != 0 {
		return ErrIntegrity
	}
	msg, err := s.Message(ctx, row.Descriptor.ConversationID, row.Seq)
	if err != nil {
		return err
	}
	if msg.ClientMsgID != row.ClientMsgID || msg.ServerMsgID != row.ServerMsgID || msg.Seq != row.Seq || msg.SendID != row.Descriptor.SenderID || msg.RecvID != row.Descriptor.ReceiverID || msg.ContentType != row.Descriptor.ContentType || msg.SenderPlatformID != row.Descriptor.SenderPlatformID || CheckContent(row.Descriptor, []byte(msg.Content)) != nil {
		return ErrIntegrity
	}
	if msg.SessionType != constant.SingleChatType || msg.Status != constant.MsgStatusSendSuccess || msg.SendTime != row.CommittedAt || msg.CreateTime != row.CommittedAt {
		return ErrIntegrity
	}
	var water struct {
		Seq int64 `bson:"seq"`
	}
	if e := s.DB.Collection(WatermarkCollection).FindOne(ctx, bson.M{"_id": row.Descriptor.ConversationID}).Decode(&water); e != nil || water.Seq < row.Seq {
		return ErrIntegrity
	}
	return nil
}

// Message 只从正式消息块读取指定位置。
func (s *Store) Message(ctx context.Context, conversation string, seq int64) (*model.MsgDataModel, error) {
	if seq <= 0 {
		return nil, ErrInvalid
	}
	var block model.MsgDocModel
	key := new(model.MsgDocModel).GetDocID(conversation, seq)
	if err := s.DB.Collection("msg").FindOne(ctx, bson.M{"doc_id": key}).Decode(&block); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrIntegrity
		}
		return nil, err
	}
	index := new(model.MsgDocModel).GetMsgIndex(seq)
	if int64(len(block.Msg)) <= index || block.Msg[index] == nil || block.Msg[index].Msg == nil {
		return nil, ErrIntegrity
	}
	block.Msg[index].Msg.IsRead = block.Msg[index].Msg.IsRead || block.Msg[index].IsRead
	return block.Msg[index].Msg, nil
}

// Submit 把去重、正文、顺序及待传播事实放入同一事务。
func (s *Store) Submit(ctx context.Context, d Descriptor, content []byte) (*pb.Receipt, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if d.SourceNamespace != s.Namespace {
		return nil, ErrUnauthorized
	}
	if err := CheckContent(d, content); err != nil {
		return nil, err
	}
	return s.resolve(ctx, d, content, false)
}

// ResolveOrExpire 与提交使用同一唯一键，封存不需要正文。
func (s *Store) ResolveOrExpire(ctx context.Context, d Descriptor) (*pb.Receipt, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if d.SourceNamespace != s.Namespace {
		return nil, ErrUnauthorized
	}
	return s.resolve(ctx, d, nil, true)
}

func (s *Store) resolve(ctx context.Context, d Descriptor, content []byte, sealOnly bool) (*pb.Receipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for attempt := 0; attempt < 8; attempt++ {
		result, err := s.transaction(ctx, true, func(sc mongo.SessionContext) (*pb.Receipt, error) {
			old, e := s.get(sc, d.SourceNamespace, d.SourceID)
			if e == nil {
				if old.Descriptor != d {
					return nil, ErrConflict
				}
				if e = s.verify(sc, old); e != nil {
					return nil, e
				}
				return old.proto(), nil
			}
			if !errors.Is(e, mongo.ErrNoDocuments) {
				return nil, e
			}
			now, e := s.serverTime(sc)
			if e != nil {
				return nil, e
			}
			row := receiptRecord{Namespace: d.SourceNamespace, SourceID: d.SourceID, Descriptor: d}
			if now >= d.ExpiresAt {
				row.Status = Expired
				row.SealedAt = now
			} else if sealOnly {
				return &pb.Receipt{Status: NotDue}, nil
			} else {
				if e = s.checkUsers(sc, d); e != nil {
					return nil, e
				}
				var water struct {
					Seq int64 `bson:"seq"`
				}
				e = s.DB.Collection(WatermarkCollection).FindOneAndUpdate(sc, bson.M{"_id": d.ConversationID}, bson.M{"$inc": bson.M{"seq": int64(1)}, "$set": bson.M{"time": now}, "$setOnInsert": bson.M{"sender_id": d.SenderID, "receiver_id": d.ReceiverID, "projected_seq": int64(0), "epoch": int64(0), "generation": int64(0), "lease_until": time.Time{}}}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&water)
				if e != nil {
					return nil, e
				}
				row.Status = Committed
				row.Seq = water.Seq
				row.ClientMsgID = d.SourceID
				row.ServerMsgID = strings.ReplaceAll(uuid.NewString(), "-", "")
				row.CommittedAt = now
				row.Pending = true
				msg := &model.MsgDataModel{SendID: d.SenderID, RecvID: d.ReceiverID, ClientMsgID: row.ClientMsgID, ServerMsgID: row.ServerMsgID, SenderPlatformID: d.SenderPlatformID, SessionType: constant.SingleChatType, MsgFrom: constant.SysMsgType, ContentType: d.ContentType, Content: string(content), Seq: row.Seq, SendTime: now, CreateTime: now, Status: constant.MsgStatusSendSuccess, Options: map[string]bool{constant.IsOfflinePush: false, constant.IsHistory: true, constant.IsPersistent: true, constant.IsUnreadCount: true, constant.IsSenderSync: true, constant.IsNotNotification: true, constant.IsConversationUpdate: true}}
				encoded, e := bson.Marshal(msg)
				if e != nil {
					return nil, e
				}
				if len(encoded) > MaxMessageBytes {
					return nil, ErrTooLarge
				}
				if e = s.insertMessage(sc, d.ConversationID, msg); e != nil {
					return nil, e
				}
			}
			if _, e = s.DB.Collection(ReceiptCollection).InsertOne(sc, row); e != nil {
				return nil, e
			}
			return row.proto(), nil
		})
		if !mongo.IsDuplicateKeyError(err) {
			return result, err
		}
		// 同来源或首次会话／块的唯一键竞争均重新进入新快照核对。
		select {
		case <-ctx.Done():
			return nil, ErrUnavailable
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return nil, ErrUnavailable
}

func (s *Store) checkUsers(ctx context.Context, d Descriptor) error {
	for _, id := range []string{d.SenderID, d.ReceiverID} {
		var user model.User
		err := s.DB.Collection("user").FindOne(ctx, bson.M{"user_id": id}).Decode(&user)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ErrInvalid
		}
		if err != nil {
			return err
		}
		if user.AppMangerLevel >= constant.AppAdmin {
			return ErrInvalid
		}
	}
	return nil
}

func (s *Store) insertMessage(ctx context.Context, conversation string, msg *model.MsgDataModel) error {
	table := new(model.MsgDocModel)
	docID := table.GetDocID(conversation, msg.Seq)
	index := table.GetMsgIndex(msg.Seq)
	var block model.MsgDocModel
	err := s.DB.Collection("msg").FindOne(ctx, bson.M{"doc_id": docID}).Decode(&block)
	if errors.Is(err, mongo.ErrNoDocuments) {
		block.DocID = docID
		block.Msg = make([]*model.MsgInfoModel, 100)
		for i := range block.Msg {
			block.Msg[i] = &model.MsgInfoModel{DelList: []string{}}
		}
		block.Msg[index].Msg = msg
		_, err = s.DB.Collection("msg").InsertOne(ctx, block)
		return err
	}
	if err != nil {
		return err
	}
	if len(block.Msg) != 100 || block.Msg[index] == nil || block.Msg[index].Msg != nil {
		return ErrIntegrity
	}
	field := fmt.Sprintf("msgs.%d.msg", index)
	r, err := s.DB.Collection("msg").UpdateOne(ctx, bson.M{"doc_id": docID, field: nil}, bson.M{"$set": bson.M{field: msg}})
	if err != nil {
		return err
	}
	if r.MatchedCount != 1 {
		return ErrIntegrity
	}
	return nil
}
