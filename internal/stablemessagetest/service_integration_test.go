package stablemessagetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	authservice "github.com/openimsdk/open-im-server/v3/internal/rpc/auth"
	conversationservice "github.com/openimsdk/open-im-server/v3/internal/rpc/conversation"
	msgservice "github.com/openimsdk/open-im-server/v3/internal/rpc/msg"
	userservice "github.com/openimsdk/open-im-server/v3/internal/rpc/user"
	"github.com/openimsdk/open-im-server/v3/internal/stablemessage"
	"github.com/openimsdk/open-im-server/v3/pkg/authverify"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcsecure"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/auth"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/msg"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/protocol/user"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/mcontext"
	grpcclient "github.com/openimsdk/tools/mw/grpc/client"
	grpcserver "github.com/openimsdk/tools/mw/grpc/server"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type fixedDiscovery struct {
	discovery.SvcDiscoveryRegistry
	conn grpc.ClientConnInterface
}

func (d *fixedDiscovery) GetConn(context.Context, string, ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	return d.conn, nil
}
func (d *fixedDiscovery) GetConns(context.Context, string, ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	return []grpc.ClientConnInterface{d.conn}, nil
}

type captureRegistrar struct{}

func (captureRegistrar) RegisterService(*grpc.ServiceDesc, any) {}

// TestRealServiceAssembly 同时覆盖真实 Auth/User/Msg 及两种调用装配。
func TestRealServiceAssembly(t *testing.T) {
	for _, network := range []bool{false, true} {
		name := "inprocess"
		if network {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) { runAssembly(t, network) })
	}
}

func runAssembly(t *testing.T, network bool) {
	uri := os.Getenv("OPENIM_STABLE_TEST_MONGO_URI")
	redisAddr := os.Getenv("OPENIM_STABLE_TEST_REDIS_ADDR")
	if uri == "" || redisAddr == "" {
		t.Skip("需要隔离副本集和 Redis")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	databaseName := "stable_rpc_" + suffix
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("OPENIM_STABLE_TEST_REDIS_PASSWORD"), DB: 14})
	require.NoError(t, rdb.Ping(ctx).Err())
	size, err := rdb.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size, "DB14 必须为空，不能覆盖已有实例")
	require.NoError(t, rdb.Set(ctx, "STABLE_TEST_OWNER", suffix, 0).Err())
	defer func() {
		if rdb.Get(context.Background(), "STABLE_TEST_OWNER").Val() == suffix {
			_ = rdb.FlushDB(context.Background()).Err()
		}
		_ = rdb.Close()
	}()
	parsed, err := url.Parse(uri)
	require.NoError(t, err)
	password, _ := parsed.User.Password()
	dbconfig := config.Mongo{URI: uri, Address: strings.Split(parsed.Host, ","), Database: databaseName, Username: parsed.User.Username(), Password: password, AuthSource: "admin", MongoMode: "replicaSet", MaxPoolSize: 20, MaxRetry: 2, ReplicaSet: config.ReplicaSetConfig{Name: parsed.Query().Get("replicaSet"), Hosts: strings.Split(parsed.Host, ","), ReadConcern: "majority"}, ReadPreference: config.ReadPrefConfig{Mode: "primary"}, WriteConcern: config.WriteConcernConfig{W: "majority", J: true}}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_ = client.Database(databaseName).Drop(cleanup)
		_ = client.Disconnect(cleanup)
	}()
	redisconfig := config.Redis{Address: []string{redisAddr}, Password: os.Getenv("OPENIM_STABLE_TEST_REDIS_PASSWORD"), RedisMode: "standalone", DB: 14, PoolSize: 20, MaxRetry: 2}
	adminID := "admin-" + suffix
	senderID := "a-" + suffix
	receiverID := "b-" + suffix
	secret := uuid.NewString()
	share := config.Share{Secret: secret, Queue: config.EngineSelector{Engine: "redis"}, StableMessage: config.StableMessage{Enabled: true, SourceNamespace: "rpc-test", AdminUserIDs: []string{adminID}}}
	share.IMAdminUser.UserIDs = []string{adminID}
	share.IMAdminUser.Nicknames = []string{"测试管理员"}
	disc := config.Discovery{}
	disc.RpcService.Auth = "auth"
	disc.RpcService.User = "user"
	disc.RpcService.Msg = "msg"
	disc.RpcService.Conversation = "conversation"
	disc.RpcService.Friend = "friend"
	disc.RpcService.Group = "group"
	logs := t.TempDir()
	require.NoError(t, log.InitLoggerFromConfig("stable-test", "test", "", "", 6, false, true, logs, 1, 1, "test", false))
	var registry discovery.SvcDiscoveryRegistry
	var registrar grpc.ServiceRegistrar
	var conn grpc.ClientConnInterface
	var serve func()
	var stop func()
	if network {
		tlsConfig := config.RPCTLS{Enabled: true, CAFile: os.Getenv("OPENIM_STABLE_TEST_TLS_CA"), CertFile: os.Getenv("OPENIM_STABLE_TEST_TLS_CERT"), KeyFile: os.Getenv("OPENIM_STABLE_TEST_TLS_KEY"), ServerName: "localhost"}
		if tlsConfig.CAFile == "" {
			t.Skip("需要测试 TLS 证书")
		}
		serverCred, err := rpcsecure.ServerCredentials(tlsConfig, true)
		require.NoError(t, err)
		clientCred, err := rpcsecure.ClientCredentials(tlsConfig, true)
		require.NoError(t, err)
		server := grpc.NewServer(grpc.Creds(serverCred), rpcsecure.ServerLogger(), grpcserver.GrpcServerErrorConvert(), grpcserver.GrpcServerMetadataContext(), grpc.ChainUnaryInterceptor(func(c context.Context, r any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return next(authverify.WithIMAdminUserIDs(c, share.IMAdminUser.UserIDs), r)
		}))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		cc, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientCred), rpcsecure.ClientLogger(), rpcsecure.ClientContext(), grpcclient.GrpcClientErrorConvert())
		require.NoError(t, err)
		conn = cc
		registry = &fixedDiscovery{conn: conn}
		registrar = server
		serve = func() { go server.Serve(listener) }
		stop = func() { server.Stop(); _ = cc.Close(); _ = listener.Close() }
	} else {
		local := rpcsecure.NewLocalRegistry(&fixedDiscovery{}, captureRegistrar{})
		registry = local
		registrar = local
		conn, err = local.GetConn(ctx, "msg")
		require.NoError(t, err)
		serve = func() {}
		stop = func() {}
	}
	defer stop()
	defer config.SetStableMode(false)
	ac := authservice.Config{MongoConfig: dbconfig, RedisConfig: redisconfig, Share: share, Discovery: disc}
	ac.RpcConfig.TokenPolicy.Expire = 1
	require.NoError(t, authservice.Start(ctx, &ac, registry, registrar))
	require.NoError(t, conversationservice.Start(ctx, &conversationservice.Config{MongodbConfig: dbconfig, RedisConfig: redisconfig, Share: share, Discovery: disc}, registry, registrar))
	require.NoError(t, msgservice.Start(ctx, &msgservice.Config{MongodbConfig: dbconfig, RedisConfig: redisconfig, Share: share, Discovery: disc}, registry, registrar))
	require.NoError(t, userservice.Start(ctx, &userservice.Config{MongodbConfig: dbconfig, RedisConfig: redisconfig, Share: share, Discovery: disc}, registry, registrar))
	serve()
	call, cancelCall := context.WithTimeout(authverify.WithIMAdminUserIDs(mcontext.SetOpUserID(mcontext.SetOperationID(ctx, suffix), adminID), []string{adminID}), 20*time.Second)
	defer cancelCall()
	token, err := auth.NewAuthClient(conn).GetAdminToken(call, &auth.GetAdminTokenReq{Secret: secret, UserID: adminID})
	require.NoError(t, err)
	_, err = user.NewUserClient(conn).UserRegister(call, &user.UserRegisterReq{Users: []*sdkws.UserInfo{{UserID: senderID}, {UserID: receiverID}}})
	require.NoError(t, err)
	terminal, err := auth.NewAuthClient(conn).GetUserToken(call, &auth.GetUserTokenReq{UserID: senderID, PlatformID: constant.WebPlatformID})
	require.NoError(t, err)
	body := []byte("私密正文-" + suffix)
	source := strings.ReplaceAll(uuid.NewString(), "-", "")
	d := stablemessage.Descriptor{ProtocolVersion: 1, SourceNamespace: "rpc-test", SourceID: source, BusinessEncodingVersion: 1, BusinessDigest: strings.Repeat("0", 64), TransportEncodingVersion: 1, SenderID: senderID, ReceiverID: receiverID, ConversationID: conversationutil.GenConversationIDForSingle(senderID, receiverID), SenderPlatformID: constant.WebPlatformID, ContentType: constant.Text, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}
	hash := sha256.Sum256(stablemessage.TransportBytes(d, body))
	d.TransportDigest = hex.EncodeToString(hash[:])
	stable := pb.NewStableMessageClient(conn)
	authorized := metadata.AppendToOutgoingContext(call, "stable-token", token.Token)
	receipt, err := stable.Submit(authorized, &pb.SubmitRequest{Descriptor_: d.Proto(), ContentBytes: body})
	require.NoError(t, err)
	require.Equal(t, stablemessage.Committed, receipt.Status)
	require.Equal(t, source, receipt.ClientMsgId)
	reread, err := stable.GetReceipt(authorized, &pb.GetReceiptRequest{ProtocolVersion: 1, SourceNamespace: d.SourceNamespace, SourceId: source})
	require.NoError(t, err)
	require.Equal(t, receipt, reread)
	_, err = stable.GetReceipt(call, &pb.GetReceiptRequest{ProtocolVersion: 1, SourceNamespace: d.SourceNamespace, SourceId: source})
	require.Error(t, err)
	_, err = stable.GetReceipt(metadata.AppendToOutgoingContext(call, "stable-token", terminal.Token), &pb.GetReceiptRequest{ProtocolVersion: 1, SourceNamespace: d.SourceNamespace, SourceId: source})
	require.Error(t, err)
	_, err = stable.GetReceipt(authorized, &pb.GetReceiptRequest{ProtocolVersion: 1, SourceNamespace: "wrong", SourceId: source})
	require.Error(t, err)
	_, err = msg.NewMsgClient(conn).GetServerTime(call, &msg.GetServerTimeReq{})
	require.NoError(t, err)
	_, err = msg.NewMsgClient(conn).SendMsg(call, &msg.SendMsgReq{MsgData: &sdkws.MsgData{SendID: senderID, RecvID: receiverID, SessionType: constant.SingleChatType, ContentType: constant.Text, Content: body}})
	require.Error(t, err)
	pulled, err := msg.NewMsgClient(conn).PullMessageBySeqs(call, &sdkws.PullMessageBySeqsReq{UserID: receiverID, SeqRanges: []*sdkws.SeqRange{{ConversationID: d.ConversationID, Begin: 1, End: 1, Num: 1}}})
	require.NoError(t, err)
	require.Len(t, pulled.Msgs[d.ConversationID].Msgs, 1)
	require.Equal(t, body, pulled.Msgs[d.ConversationID].Msgs[0].Content)
	cancel()
	time.Sleep(20 * time.Millisecond)
	log.Flush()
	require.NoError(t, filepath.WalkDir(logs, func(path string, item os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if item.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, sensitive := range []string{token.Token, terminal.Token, string(body)} {
			require.False(t, strings.Contains(string(data), sensitive), "日志包含敏感载荷")
		}
		return nil
	}))
}
