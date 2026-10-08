package stablemessage

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/protocol/auth"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/mw/specialerror"
	"google.golang.org/grpc/metadata"
)

// Service 在每个入口独立验证原始服务 Token。
type Service struct {
	pb.UnimplementedStableMessageServer
	Store  *Store
	Auth   auth.AuthClient
	Config config.StableMessage
}

func (s *Service) authorize(ctx context.Context, namespace string) error {
	if !s.Config.Enabled || s.Store == nil {
		return ErrDisabled
	}
	md, _ := metadata.FromIncomingContext(ctx)
	tokens := md.Get("stable-token")
	if len(tokens) != 1 || tokens[0] == "" {
		return ErrUnauthorized
	}
	if namespace != s.Config.SourceNamespace {
		return ErrUnauthorized
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	claims, err := s.Auth.ParseToken(ctx, &auth.ParseTokenReq{Token: tokens[0]})
	if err != nil {
		code := specialerror.ErrCode(err)
		if code == nil || code.Code() == errs.ErrInternalServer.Code() {
			return ErrUnavailable
		}
		return ErrUnauthorized
	}
	if claims.PlatformID != constant.AdminPlatformID || claims.ExpireTimeSeconds <= time.Now().Unix() || !slices.Contains(s.Config.AdminUserIDs, claims.UserID) {
		return ErrUnauthorized
	}
	return nil
}

func sourceError(err error) error {
	if err == nil {
		return nil
	}
	if code := specialerror.ErrCode(err); code != nil && code.Code() >= 16001 && code.Code() <= 16009 {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrUnavailable
	}
	return ErrUnavailable
}

// Submit 接受冻结载荷，只有多数派确认后的事务才返回终态。
func (s *Service) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.Receipt, error) {
	if req == nil || req.Descriptor_ == nil {
		return nil, ErrInvalid
	}
	if err := s.authorize(ctx, req.Descriptor_.SourceNamespace); err != nil {
		return nil, err
	}
	d, err := DescriptorFromProto(req.Descriptor_)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.Submit(ctx, d, req.ContentBytes)
	return r, sourceError(err)
}

// GetReceipt 从源端事务快照查证。
func (s *Service) GetReceipt(ctx context.Context, req *pb.GetReceiptRequest) (*pb.Receipt, error) {
	if req == nil {
		return nil, ErrInvalid
	}
	if err := s.authorize(ctx, req.SourceNamespace); err != nil {
		return nil, err
	}
	if req.ProtocolVersion != 1 {
		return nil, ErrInvalid
	}
	r, err := s.Store.GetReceipt(ctx, req.SourceNamespace, req.SourceId)
	return r, sourceError(err)
}

// ResolveOrExpire 通过相同来源键封存迟到提交。
func (s *Service) ResolveOrExpire(ctx context.Context, req *pb.ResolveOrExpireRequest) (*pb.Receipt, error) {
	if req == nil || req.Descriptor_ == nil {
		return nil, ErrInvalid
	}
	if err := s.authorize(ctx, req.Descriptor_.SourceNamespace); err != nil {
		return nil, err
	}
	d, err := DescriptorFromProto(req.Descriptor_)
	if err != nil {
		return nil, err
	}
	r, err := s.Store.ResolveOrExpire(ctx, d)
	return r, sourceError(err)
}
