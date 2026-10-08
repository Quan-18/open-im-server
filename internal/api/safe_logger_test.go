package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"

	"testing"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/tools/log"
	"github.com/stretchr/testify/require"
)

func TestHTTPLogsNeverIncludeTokenOrRequestResponseBodies(t *testing.T) {
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = original; _ = writer.Close(); _ = reader.Close() }()
	captured := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(reader); captured <- data }()
	require.NoError(t, log.InitLoggerFromConfig("stable-http-test", "test", "", "", 6, true, true, "", 1, 1, "test", false))
	r := gin.New()
	r.Use(safeHTTPLogger(), safeRecovery())
	secret := "test-token-private-material"
	body := "test-private-body-material"
	r.POST("/success", func(c *gin.Context) { data, _ := io.ReadAll(c.Request.Body); c.Data(200, "text/plain", data) })
	r.POST("/failure", func(c *gin.Context) { panic(body) })
	for _, path := range []string{"/success", "/failure"} {
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
		request.Header.Set("token", secret)
		r.ServeHTTP(httptest.NewRecorder(), request)
	}
	log.Flush()
	require.NoError(t, writer.Close())
	os.Stdout = original
	data := <-captured
	require.False(t, bytes.Contains(data, []byte(secret)), "日志包含凭证")
	require.False(t, bytes.Contains(data, []byte(body)), "日志包含正文")
	// 后续测试恢复常规输出，不保留指向关闭管道的日志句柄。
	require.NoError(t, log.InitLoggerFromConfig("stable-http-test", "test", "", "", 3, true, true, "", 1, 1, "test", false))
}
