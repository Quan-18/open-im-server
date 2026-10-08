package api

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"net/http"
	"time"
)

// safeHTTPLogger 保留请求大小上限与关联标识，不记录凭证、正文或返回载荷。
func safeHTTPLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Header("X-HTTP-ID", uuid.NewString())
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024*1024)
		}
		c.Next()
		log.ZInfo(c, "HTTP 调用完成", "route", c.FullPath(), "method", c.Request.Method, "status", c.Writer.Status(), "elapsed", time.Since(start))
	}
}

// safeRecovery 不把完整请求头或异常载荷写入恢复日志。
func safeRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recover() != nil {
				log.ZError(c, "HTTP 处理异常", errs.ErrInternalServer, "route", c.FullPath())
				apiresp.GinError(c, errs.ErrInternalServer)
				c.Abort()
			}
		}()
		c.Next()
	}
}
