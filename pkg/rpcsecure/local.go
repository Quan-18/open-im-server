package rpcsecure

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/openimsdk/tools/discovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type localMethod struct {
	desc grpc.MethodDesc
	impl any
}

// LocalRegistry 复用真实处理器，避免依赖中的全请求进程内日志。
type LocalRegistry struct {
	discovery.SvcDiscoveryRegistry
	registrar grpc.ServiceRegistrar
	mu        sync.RWMutex
	methods   map[string]localMethod
}

// NewLocalRegistry 包装已有进程内发现与注册，不创建第二服务实现。
func NewLocalRegistry(d discovery.SvcDiscoveryRegistry, r grpc.ServiceRegistrar) *LocalRegistry {
	return &LocalRegistry{SvcDiscoveryRegistry: d, registrar: r, methods: make(map[string]localMethod)}
}

// RegisterService 同时保存真实处理器并保留原发现机制。
func (r *LocalRegistry) RegisterService(desc *grpc.ServiceDesc, impl any) {
	r.mu.Lock()
	for _, method := range desc.Methods {
		key := "/" + desc.ServiceName + "/" + method.MethodName
		if _, ok := r.methods[key]; ok {
			r.mu.Unlock()
			panic("duplicate RPC service")
		}
		r.methods[key] = localMethod{method, impl}
	}
	r.mu.Unlock()
	r.registrar.RegisterService(desc, impl)
}

// GetConn 返回受控本地调用连接，认证处理器不会被替换或跳过。
func (r *LocalRegistry) GetConn(ctx context.Context, name string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	cc, err := r.SvcDiscoveryRegistry.GetConn(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	return &localConnection{ClientConnInterface: cc, registry: r}, nil
}

// GetConns 仅包装本地首项，远程广播保留独立入口屏障。
func (r *LocalRegistry) GetConns(ctx context.Context, name string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	cc, err := r.SvcDiscoveryRegistry.GetConns(ctx, name, opts...)
	if err == nil && len(cc) > 0 {
		cc[0] = &localConnection{ClientConnInterface: cc[0], registry: r}
	}
	return cc, err
}

type localConnection struct {
	grpc.ClientConnInterface
	registry *LocalRegistry
}

func (c *localConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	c.registry.mu.RLock()
	entry, ok := c.registry.methods[method]
	c.registry.mu.RUnlock()
	if !ok {
		if SensitiveMethod(method) {
			return fmt.Errorf("sensitive RPC service unavailable")
		}
		return c.ClientConnInterface.Invoke(ctx, method, args, reply, opts...)
	}
	data, err := marshal(args)
	if err != nil {
		return err
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		ctx = metadata.NewIncomingContext(ctx, md.Copy())
	}
	resp, err := entry.desc.Handler(entry.impl, ctx, func(target any) error {
		p, ok := target.(proto.Message)
		if !ok {
			return fmt.Errorf("invalid RPC message")
		}
		return proto.Unmarshal(data, p)
	}, nil)
	if err != nil {
		return err
	}
	data, err = marshal(resp)
	if err != nil {
		return err
	}
	if raw, ok := reply.(*[]byte); ok {
		*raw = append((*raw)[:0], data...)
		return nil
	}
	p, ok := reply.(proto.Message)
	if !ok {
		return fmt.Errorf("invalid RPC response")
	}
	return proto.Unmarshal(data, p)
}

func marshal(value any) ([]byte, error) {
	if raw, ok := value.([]byte); ok {
		return raw, nil
	}
	p, ok := value.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid RPC payload")
	}
	return proto.Marshal(p)
}

// SensitiveMethod 标记不允许缺失处理器后退到全量日志路径的方法。
func SensitiveMethod(method string) bool {
	return strings.HasPrefix(method, "/openim.stablemessage.") || strings.HasPrefix(method, "/openim.auth.")
}

// StableMethod 用于禁止通用跨实例 RPC 代理代传稳定凭证。
func StableMethod(method string) bool { return strings.HasPrefix(method, "/openim.stablemessage.") }
