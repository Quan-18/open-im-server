package stablemessage

import (
	"context"
	"testing"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/database/mgo"
	"github.com/openimsdk/open-im-server/v3/pkg/common/storage/model"
	"github.com/stretchr/testify/require"
)

func TestLegacyCleanupKeepsStableMessagesAndCleansNotifications(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	body := []byte("正式消息")
	d := testDescriptor(body)
	original, err := s.Submit(ctx, d, body)
	require.NoError(t, err)
	_, err = s.DB.Collection("msg").InsertOne(ctx, &model.MsgDocModel{DocID: "n_a_b:0", Msg: []*model.MsgInfoModel{{Msg: &model.MsgDataModel{Seq: 1, SendTime: time.Now().Add(-time.Hour).UnixMilli()}}}})
	require.NoError(t, err)
	config.SetStableMode(true)
	defer config.SetStableMode(false)
	messages, err := mgo.NewMsgMongo(s.DB)
	require.NoError(t, err)
	candidates, err := messages.GetRandBeforeMsg(ctx, time.Now().Add(time.Hour).UnixMilli(), 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "n_a_b:0", candidates[0].DocID)
	require.NoError(t, messages.DeleteDoc(ctx, candidates[0].DocID))
	require.Error(t, messages.DeleteDoc(ctx, "si_a_b:0"))
	after, err := s.GetReceipt(ctx, d.SourceNamespace, d.SourceID)
	require.NoError(t, err)
	require.Equal(t, original, after)
}
