// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package startrpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"syscall"
	"time"

	conf "github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/tools/utils/datautil"
	"github.com/openimsdk/tools/utils/jsonutil"
	"github.com/openimsdk/tools/utils/network"
	"github.com/openimsdk/tools/utils/runtimeenv"
	"google.golang.org/grpc/status"

	kdisc "github.com/openimsdk/open-im-server/v3/pkg/common/discovery"
	"github.com/openimsdk/open-im-server/v3/pkg/common/prommetrics"
	"github.com/openimsdk/open-im-server/v3/pkg/rpcsecure"
	"github.com/openimsdk/tools/discovery"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	grpccli "github.com/openimsdk/tools/mw/grpc/client"
	grpcsrv "github.com/openimsdk/tools/mw/grpc/server"
	"google.golang.org/grpc"
)

func init() {
	prommetrics.RegistryAll()
}

func Start[T any](ctx context.Context, disc *conf.Discovery, circuitBreakerConfig *conf.CircuitBreaker, rateLimiterConfig *conf.RateLimiter, prometheusConfig *conf.Prometheus, listenIP,
	registerIP string, autoSetPorts bool, rpcPorts []int, index int, rpcRegisterName string, notification *conf.Notification, config T,
	watchConfigNames []string, watchServiceNames []string,
	rpcFn func(ctx context.Context, config T, client discovery.SvcDiscoveryRegistry, server grpc.ServiceRegistrar) error,
	options ...grpc.ServerOption) error {

	if notification != nil {
		conf.InitNotification(notification)
	}

	maxRequestBody := getConfigRpcMaxRequestBody(reflect.ValueOf(config))
	shareConfig := getConfigShare(reflect.ValueOf(config))
	var tlsConfig conf.RPCTLS
	var stableRequired bool
	if shareConfig != nil {
		tlsConfig = shareConfig.RPCTLS
		stableRequired = shareConfig.StableMessage.Enabled
		conf.SetStableMode(stableRequired)
	}
	if err := rpcsecure.ValidateDiscovery(stableRequired, disc.Enable, runtimeenv.RuntimeEnvironment()); err != nil {
		return err
	}
	clientTransport, err := rpcsecure.ClientCredentials(tlsConfig, stableRequired)
	if err != nil {
		return err
	}
	serverTransport, err := rpcsecure.ServerCredentials(tlsConfig, stableRequired)
	if err != nil {
		return err
	}
	if serverTransport != nil {
		options = append(options, grpc.Creds(serverTransport))
	}

	log.ZDebug(ctx, "rpc start", "rpcMaxRequestBody", maxRequestBody, "rpcRegisterName", rpcRegisterName, "registerIP", registerIP, "listenIP", listenIP)

	options = append(options,
		grpcsrv.GrpcServerMetadataContext(),
		grpcsrv.GrpcServerErrorConvert(),
		rpcsecure.ServerLogger(),
		grpcsrv.GrpcServerRequestValidate(),
		grpcsrv.GrpcServerPanicCapture(),
	)
	if shareConfig != nil && len(shareConfig.IMAdminUser.UserIDs) > 0 {
		options = append(options, grpcServerIMAdminUserID(shareConfig.IMAdminUser.UserIDs))
	}
	var clientOptions []grpc.DialOption
	if maxRequestBody != nil {
		if maxRequestBody.RequestMaxBodySize > 0 {
			options = append(options, grpc.MaxRecvMsgSize(maxRequestBody.RequestMaxBodySize))
			clientOptions = append(clientOptions, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxRequestBody.RequestMaxBodySize)))
		}
		if maxRequestBody.ResponseMaxBodySize > 0 {
			options = append(options, grpc.MaxSendMsgSize(maxRequestBody.ResponseMaxBodySize))
			clientOptions = append(clientOptions, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRequestBody.ResponseMaxBodySize)))
		}
	}

	if circuitBreakerConfig != nil && circuitBreakerConfig.Enable {
		cb := &CircuitBreaker{
			Enable:  circuitBreakerConfig.Enable,
			Success: circuitBreakerConfig.Success,
			Request: circuitBreakerConfig.Request,
			Bucket:  circuitBreakerConfig.Bucket,
			Window:  circuitBreakerConfig.Window,
		}

		breaker := NewCircuitBreaker(cb)

		options = append(options,
			UnaryCircuitBreakerInterceptor(breaker),
			StreamCircuitBreakerInterceptor(breaker),
		)

		log.ZInfo(ctx, "RPC circuit breaker enabled",
			"service", rpcRegisterName,
			"window", circuitBreakerConfig.Window,
			"bucket", circuitBreakerConfig.Bucket,
			"success", circuitBreakerConfig.Success,
			"requestThreshold", circuitBreakerConfig.Request)
	}

	if rateLimiterConfig != nil && rateLimiterConfig.Enable {
		limiter := NewRateLimiter((*RateLimiter)(rateLimiterConfig))

		options = append(options,
			UnaryRateLimitInterceptor(limiter),
			StreamRateLimitInterceptor(limiter),
		)

		log.ZInfo(ctx, "RPC rate limiter enabled",
			"service", rpcRegisterName,
			"window", rateLimiterConfig.Window,
			"bucket", rateLimiterConfig.Bucket,
			"cpuThreshold", rateLimiterConfig.CPUThreshold)
	}

	registerIP, err = network.GetRpcRegisterIP(registerIP)
	if err != nil {
		return err
	}
	var prometheusListenAddr string
	if autoSetPorts {
		prometheusListenAddr = net.JoinHostPort(listenIP, "0")
	} else {
		prometheusPort, err := datautil.GetElemByIndex(prometheusConfig.Ports, index)
		if err != nil {
			return err
		}
		prometheusListenAddr = net.JoinHostPort(listenIP, strconv.Itoa(prometheusPort))
	}

	watchConfigNames = append(watchConfigNames, conf.LogConfigFileName)

	client, err := kdisc.NewDiscoveryRegister(disc, watchServiceNames)
	if err != nil {
		return err
	}

	defer client.Close()
	client.AddOption(
		grpc.WithTransportCredentials(clientTransport),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, "round_robin")),

		rpcsecure.ClientLogger(),
		rpcsecure.ClientContext(),
		grpccli.GrpcClientErrorConvert(),
	)
	if len(clientOptions) > 0 {
		client.AddOption(clientOptions...)
	}

	ctx, cancel := context.WithCancelCause(ctx)

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-ctx.Done():
			return
		case val := <-sigs:
			log.ZDebug(ctx, "recv signal", "signal", val.String())
			cancel(fmt.Errorf("signal %s", val.String()))
		}
	}()

	if prometheusListenAddr != "" {
		options = append(
			options,
			prommetricsUnaryInterceptor(rpcRegisterName),
			prommetricsStreamInterceptor(rpcRegisterName),
		)
		prometheusListener, prometheusPort, err := listenTCP(prometheusListenAddr)
		if err != nil {
			return err
		}
		log.ZDebug(ctx, "prometheus start", "addr", prometheusListener.Addr(), "rpcRegisterName", rpcRegisterName)
		target, err := jsonutil.JsonMarshal(prommetrics.BuildDefaultTarget(registerIP, prometheusPort))
		if err != nil {
			return err
		}
		if autoSetPorts {
			if err = client.SetWithLease(ctx, prommetrics.BuildDiscoveryKey(rpcRegisterName, index), target, prommetrics.TTL); err != nil {
				if !errors.Is(err, discovery.ErrNotSupported) {
					return err
				}
			}
		}
		go func() {
			err := prommetrics.Start(prometheusListener)
			if err == nil {
				err = fmt.Errorf("listener done")
			}
			cancel(fmt.Errorf("prommetrics %s %w", rpcRegisterName, err))
		}()
	}

	var (
		rpcServer       *grpc.Server
		rpcListener     net.Listener
		rpcGracefulStop chan struct{}
	)

	onGrpcServiceRegistrar := func(desc *grpc.ServiceDesc, impl any) {
		if rpcServer == nil {
			var port int
			if !autoSetPorts {
				var portErr error
				port, portErr = datautil.GetElemByIndex(rpcPorts, index)
				if portErr != nil {
					cancel(portErr)
					return
				}
			}
			var listenErr error
			rpcListener, listenErr = net.Listen("tcp", net.JoinHostPort(listenIP, strconv.Itoa(port)))
			if listenErr != nil {
				cancel(listenErr)
				return
			}
			rpcServer = grpc.NewServer(options...)
		}
		rpcServer.RegisterService(desc, impl)
	}
	err = rpcFn(ctx, config, client, &grpcServiceRegistrar{onRegisterService: onGrpcServiceRegistrar})
	if err != nil {
		return err
	}
	<-ctx.Done()
	log.ZDebug(ctx, "cmd wait done", "err", context.Cause(ctx))
	if rpcGracefulStop != nil {
		timeout := time.NewTimer(time.Second * 15)
		defer timeout.Stop()
		select {
		case <-timeout.C:
			log.ZWarn(ctx, "rcp graceful stop timeout", nil)
		case <-rpcGracefulStop:
			log.ZDebug(ctx, "rcp graceful stop done")
		}
	}
	return context.Cause(ctx)
}

func listenTCP(addr string) (net.Listener, int, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, 0, errs.WrapMsg(err, "listen err", "addr", addr)
	}
	return listener, listener.Addr().(*net.TCPAddr).Port, nil
}

func prommetricsUnaryInterceptor(rpcRegisterName string) grpc.ServerOption {
	getCode := func(err error) int {
		if err == nil {
			return 0
		}
		rpcErr, ok := err.(interface{ GRPCStatus() *status.Status })
		if !ok {
			return -1
		}
		return int(rpcErr.GRPCStatus().Code())
	}
	return grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		prommetrics.RPCCall(rpcRegisterName, info.FullMethod, getCode(err))
		return resp, err
	})
}

func prommetricsStreamInterceptor(rpcRegisterName string) grpc.ServerOption {
	return grpc.ChainStreamInterceptor()
}

type grpcServiceRegistrar struct {
	onRegisterService func(desc *grpc.ServiceDesc, impl any)
}

func (x *grpcServiceRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	x.onRegisterService(desc, impl)
}
