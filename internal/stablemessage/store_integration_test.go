package stablemessage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func integrationStore(t *testing.T, monitors ...*event.CommandMonitor) *Store {
	t.Helper()
	uri := os.Getenv("OPENIM_STABLE_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("需要显式配置隔离 Mongo 副本集")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name := "stable_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	clientOptions := options.Client().ApplyURI(uri).SetAppName(name)
	if len(monitors) > 0 {
		clientOptions.SetMonitor(monitors[0])
	}
	client, err := mongo.Connect(ctx, clientOptions)
	require.NoError(t, err)
	db := client.Database(name)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, db.Drop(cleanup))
		require.NoError(t, client.Disconnect(cleanup))
	})
	store, err := Open(ctx, db, nil, config.StableMessage{Enabled: true, SourceNamespace: "stable-test", AdminUserIDs: []string{"admin"}})
	require.NoError(t, err)
	_, err = db.Collection("user").InsertMany(ctx, []any{model.User{UserID: "a"}, model.User{UserID: "b"}})
	require.NoError(t, err)
	return store
}

func TestStoreConcurrentReplayKeepsOnePosition(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	content := []byte("同一原件")
	d := testDescriptor(content)
	const count = 16
	results := make([]*pb.Receipt, count)
	failures := make([]error, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func(index int) { defer workers.Done(); results[index], failures[index] = s.Submit(ctx, d, content) }(i)
	}
	workers.Wait()
	for i := range results {
		require.NoError(t, failures[i])
		require.Equal(t, results[0], results[i])
	}
	require.Equal(t, int64(1), results[0].Seq)
	require.Equal(t, d.SourceID, results[0].ClientMsgId)
	n, err := s.DB.Collection(ReceiptCollection).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	read, err := s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.NoError(t, err)
	require.Equal(t, results[0], read)
	other := testDescriptor(content)
	r, err := s.Submit(ctx, other, content)
	require.NoError(t, err)
	require.Equal(t, int64(2), r.Seq)
}

func TestStoreConflictAndExpiredTombstone(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	content := []byte("原文")
	d := testDescriptor(content)
	r, err := s.Submit(ctx, d, content)
	require.NoError(t, err)
	changed := d
	digest := sha256.Sum256(TransportBytes(changed, []byte("替换")))
	changed.TransportDigest = hex.EncodeToString(digest[:])
	_, err = s.Submit(ctx, changed, []byte("替换"))
	require.ErrorIs(t, err, ErrConflict)
	r2, err := s.ResolveOrExpire(ctx, d)
	require.NoError(t, err)
	require.Equal(t, r, r2)
	expired := testDescriptor(content)
	expired.ExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	digest = sha256.Sum256(TransportBytes(expired, content))
	expired.TransportDigest = hex.EncodeToString(digest[:])
	sealed, err := s.ResolveOrExpire(ctx, expired)
	require.NoError(t, err)
	require.Equal(t, Expired, sealed.Status)
	require.Zero(t, sealed.Seq)
	late, err := s.Submit(ctx, expired, content)
	require.NoError(t, err)
	require.Equal(t, sealed, late)
	water, err := s.ReadCommittedWater(ctx, d.ConversationID)
	require.NoError(t, err)
	require.Equal(t, int64(1), water)
}

func TestStoreBlockBoundaryAndIntegrity(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	var final *pb.Receipt
	for i := 0; i < 101; i++ {
		d := testDescriptor([]byte("边界"))
		r, err := s.Submit(ctx, d, []byte("边界"))
		require.NoError(t, err)
		require.Equal(t, int64(i+1), r.Seq)
		final = r
	}
	n, err := s.DB.Collection("msg").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	_, err = s.DB.Collection("msg").UpdateOne(ctx, bson.M{"doc_id": "si_a_b:1"}, bson.M{"$set": bson.M{"msgs.0.msg.content": "损坏"}})
	require.NoError(t, err)
	_, err = s.GetReceipt(ctx, final.Descriptor_.SourceNamespace, final.Descriptor_.SourceId)
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestStoreDeadlineRaceHasOneTerminal(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	body := []byte("竞争")
	d := testDescriptor(body)
	d.ExpiresAt = time.Now().Add(20 * time.Millisecond).UnixMilli()
	hash := sha256.Sum256(TransportBytes(d, body))
	d.TransportDigest = hex.EncodeToString(hash[:])
	var group sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if index%2 == 0 {
				_, errs[index] = s.Submit(ctx, d, body)
			} else {
				time.Sleep(30 * time.Millisecond)
				_, errs[index] = s.ResolveOrExpire(ctx, d)
			}
		}(i)
	}
	group.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	r, err := s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.NoError(t, err)
	require.Contains(t, []string{Committed, Expired}, r.Status)
	n, err := s.DB.Collection(ReceiptCollection).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	if r.Status == Expired {
		n, err = s.DB.Collection("msg").CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Zero(t, n)
	} else {
		require.Equal(t, int64(1), r.Seq)
	}
}

func TestStoreModeCannotBeDisabledOrRenamed(t *testing.T) {
	s := integrationStore(t)
	_, err := Open(context.Background(), s.DB, nil, config.StableMessage{})
	require.Error(t, err)
	_, err = Open(context.Background(), s.DB, nil, config.StableMessage{Enabled: true, SourceNamespace: "other", AdminUserIDs: []string{"admin"}})
	require.Error(t, err)
}

func TestCorruptExpiredReceiptIsNeverReturnedAsTerminal(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	d := testDescriptor([]byte("过期"))
	d.ExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	sealed, err := s.ResolveOrExpire(ctx, d)
	require.NoError(t, err)
	require.Equal(t, Expired, sealed.Status)
	_, err = s.DB.Collection(ReceiptCollection).UpdateOne(ctx, s.sourceFilter(d.SourceNamespace, d.SourceID), bson.M{"$set": bson.M{"sealed_at": d.ExpiresAt - 1}})
	require.NoError(t, err)
	_, err = s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.ErrorIs(t, err, ErrIntegrity)
}

func TestReceiptInsertFailureRollsBackMessageAndSequence(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	body := []byte("事务回滚")
	d := testDescriptor(body)
	require.NoError(t, s.DB.RunCommand(ctx, bson.D{{Key: "collMod", Value: ReceiptCollection}, {Key: "validator", Value: bson.M{"source_id": bson.M{"$ne": d.SourceID}}}, {Key: "validationLevel", Value: "strict"}}).Err())
	_, err := s.Submit(ctx, d, body)
	require.Error(t, err)
	for _, name := range []string{ReceiptCollection, WatermarkCollection, "msg"} {
		count, e := s.DB.Collection(name).CountDocuments(ctx, bson.M{})
		require.NoError(t, e)
		require.Zero(t, count)
	}
	require.NoError(t, s.DB.RunCommand(ctx, bson.D{{Key: "collMod", Value: ReceiptCollection}, {Key: "validator", Value: bson.M{}}}).Err())
	r, err := s.Submit(ctx, d, body)
	require.NoError(t, err)
	require.Equal(t, int64(1), r.Seq)
	require.Equal(t, d.SourceID, r.ClientMsgId)
}

func TestInitialStableModeRejectsLegacyData(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	require.NoError(t, s.DB.Collection(ModeCollection).Drop(ctx))
	_, err := s.DB.Collection("msg").InsertOne(ctx, bson.M{"doc_id": "si_a_b:0", "msgs": bson.A{}})
	require.NoError(t, err)
	_, err = Open(ctx, s.DB, nil, config.StableMessage{Enabled: true, SourceNamespace: s.Namespace, AdminUserIDs: []string{"admin"}})
	require.Error(t, err)
	n, err := s.DB.Collection(ModeCollection).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Zero(t, n)
	ordinary, err := Open(ctx, s.DB, nil, config.StableMessage{})
	require.NoError(t, err)
	require.Nil(t, ordinary)
}

func TestUnknownCommitReturnsWithinBudgetAndRecoversOriginalReceipt(t *testing.T) {
	if os.Getenv("OPENIM_STABLE_TEST_FAILPOINTS") != "1" {
		t.Skip("需要显式开启隔离 Mongo failpoint")
	}
	var armed atomic.Bool
	var injected atomic.Int64
	commitObserved := make(chan struct{}, 1)
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		if armed.Load() && e.CommandName == "commitTransaction" && e.Reply.Lookup("writeConcernError").Type != 0 {
			injected.Add(1)
			select {
			case commitObserved <- struct{}{}:
			default:
			}
		}
	}}
	s := integrationStore(t, monitor)
	ctx := context.Background()
	admin := s.DB.Client().Database("admin")
	body := []byte("提交响应未知")
	d := testDescriptor(body)
	command := bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "alwaysOn"}, {Key: "data", Value: bson.M{"failCommands": bson.A{"commitTransaction"}, "appName": s.DB.Name(), "writeConcernError": bson.M{"code": 64, "errmsg": "isolated commit uncertainty"}, "errorLabels": bson.A{"UnknownTransactionCommitResult"}}}}
	require.NoError(t, admin.RunCommand(ctx, command).Err())
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, admin.RunCommand(cleanup, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err())
	}()
	budget, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	start := time.Now()
	armed.Store(true)
	finished := make(chan error, 1)
	go func() { _, err := s.Submit(budget, d, body); finished <- err }()
	select {
	case <-commitObserved:
	case early := <-finished:
		require.FailNow(t, "未在提交阶段观察到注入错误", "%T", early)
	case <-budget.Done():
		require.FailNow(t, "提交阶段未在预算内到达")
	}
	cancel()
	var err error
	select {
	case err = <-finished:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "取消后的未知提交未及时返回")
	}
	require.ErrorIs(t, err, ErrUnknownCommit)
	require.Positive(t, injected.Load())
	require.Less(t, time.Since(start), 10*time.Second)
	require.NoError(t, admin.RunCommand(ctx, bson.D{{Key: "configureFailPoint", Value: "failCommand"}, {Key: "mode", Value: "off"}}).Err())
	r, err := s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.NoError(t, err)
	require.Equal(t, Committed, r.Status)
	require.Equal(t, d.SourceID, r.ClientMsgId)
	require.Equal(t, int64(1), r.Seq)
	replayed, err := s.Submit(ctx, d, body)
	require.NoError(t, err)
	require.Equal(t, r, replayed)
}
