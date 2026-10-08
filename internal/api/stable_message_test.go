package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/stretchr/testify/require"
)

func TestGenericRPCBridgeCannotCallStableService(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	bridge := internalApi{secret: "test-secret"}
	r.POST("/rpc", bridge.RpcInvoke)
	body := []byte(`{"service":"msg","method":"/openim.stablemessage.v1.StableMessage/GetReceipt","secret":"test-secret","op_user_id":"admin","admin":true,"request":""}`)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/rpc", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(response, request)
	var envelope struct {
		ErrCode int `json:"errCode"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	require.NotZero(t, envelope.ErrCode)
}

func TestStableReceiptDoesNotInventInapplicableTerminalFields(t *testing.T) {
	for _, test := range []struct {
		reply  *pb.Receipt
		absent []string
	}{{&pb.Receipt{Status: "committed", ClientMsgId: "a", ServerMsgId: "b", Seq: 1, CommittedAt: 123}, []string{"sealedAt"}}, {&pb.Receipt{Status: "expired", SealedAt: 124}, []string{"clientMsgId", "serverMsgId", "seq", "committedAt"}}} {
		r := gin.New()
		r.POST("/receipt", func(c *gin.Context) { stableResponse(c, test.reply, nil) })
		response := httptest.NewRecorder()
		r.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/receipt", nil))
		var envelope struct {
			Data map[string]any `json:"data"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
		for _, key := range test.absent {
			_, exists := envelope.Data[key]
			require.False(t, exists)
		}
	}
}
