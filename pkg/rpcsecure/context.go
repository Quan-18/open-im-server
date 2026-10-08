package rpcsecure

import (
	"context"
	"github.com/google/uuid"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ClientContext 保留显式稳定凭证 metadata，同时传递原生关联和受信任内部上下文。
func ClientContext() grpc.DialOption {
	return grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		if md == nil {
			md = metadata.MD{}
		}
		if keys, _ := ctx.Value(constant.RpcCustomHeader).([]string); len(keys) > 0 {
			for _, key := range keys {
				value, ok := ctx.Value(key).([]string)
				if !ok || len(value) == 0 {
					return errs.ErrArgs.WrapMsg("RPC context header missing")
				}
				md.Set(key, value...)
			}
			md.Set(constant.RpcCustomHeader, keys...)
		}
		for _, key := range []string{constant.OperationID, constant.OpUserID, constant.OpUserPlatform, constant.ConnID} {
			if value, ok := ctx.Value(key).(string); ok && value != "" {
				md.Set(key, value)
			}
		}
		if len(md.Get(constant.OperationID)) == 0 {
			md.Set(constant.OperationID, uuid.NewString())
		}
		return next(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	})
}
