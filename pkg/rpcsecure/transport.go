package rpcsecure

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/tools/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ValidateDiscovery 拒绝会覆盖 TLS 选项且尚未验收的稳定模式发现路径。
func ValidateDiscovery(enabled bool, discoveryMode, runtime string) error {
	if enabled && (discoveryMode == config.KUBERNETES || (discoveryMode == "" && runtime == config.KUBERNETES)) {
		return fmt.Errorf("stable mode does not support Kubernetes discovery")
	}
	return nil
}

// ClientCredentials 为现有连接统一选择 TLS，不允许稳定模式网络降级。
func ClientCredentials(cfg config.RPCTLS, required bool) (credentials.TransportCredentials, error) {
	if !cfg.Enabled {
		if required {
			return nil, fmt.Errorf("stable network RPC requires TLS")
		}
		return insecure.NewCredentials(), nil
	}
	ca, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid RPC CA")
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: cfg.ServerName}), nil
}

// ServerCredentials 仅装载服务端证书，应用凭证仍由处理器独立验证。
func ServerCredentials(cfg config.RPCTLS, required bool) (credentials.TransportCredentials, error) {
	if !cfg.Enabled {
		if required {
			return nil, fmt.Errorf("stable network RPC requires TLS")
		}
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}), nil
}

// ServerLogger 只记录方法和耗时，避免认证请求及正文被通用日志序列化。
func ServerLogger() grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		log.ZInfo(ctx, "RPC 调用完成", "method", info.FullMethod, "elapsed", time.Since(start), "success", err == nil)
		return resp, err
	})
}

// ClientLogger 不把请求、响应或凭证写入日志。
func ClientLogger() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := next(ctx, method, req, reply, cc, opts...)
		log.ZInfo(ctx, "RPC 请求完成", "method", method, "elapsed", time.Since(start), "success", err == nil)
		return err
	})
}
