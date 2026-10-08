package stablemessage

import (
	"context"
	"github.com/google/uuid"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRealStandaloneRejectsStableModeAndKeepsOrdinaryMode(t *testing.T) {
	uri := os.Getenv("OPENIM_STABLE_TEST_STANDALONE_URI")
	if uri == "" {
		t.Skip("需要显式隔离 standalone 实例")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database("stable_standalone_" + strings.ReplaceAll(uuid.NewString(), "-", ""))
	defer db.Drop(context.Background())
	var hello bson.M
	require.NoError(t, client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello))
	_, replica := hello["setName"]
	require.False(t, replica)
	_, err = Open(ctx, db, nil, config.StableMessage{Enabled: true, SourceNamespace: "test", AdminUserIDs: []string{"admin"}})
	require.Error(t, err)
	count, err := db.Collection(ModeCollection).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Zero(t, count)
	ordinary, err := Open(ctx, db, nil, config.StableMessage{})
	require.NoError(t, err)
	require.Nil(t, ordinary)
}
