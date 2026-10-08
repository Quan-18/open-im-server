package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/open-im-server/v3/internal/stablemessage"
	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/tools/apiresp"
	"github.com/openimsdk/tools/errs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type stableSubmitHTTP struct {
	stablemessage.Descriptor
	ContentBytes []byte `json:"contentBytes"`
}
type stableQueryHTTP struct {
	ProtocolVersion uint32 `json:"protocolVersion"`
	SourceNamespace string `json:"sourceNamespace"`
	SourceID        string `json:"sourceId"`
}
type stableReceiptHTTP struct {
	Status      string                    `json:"status"`
	Descriptor  *stablemessage.Descriptor `json:"descriptor"`
	ClientMsgID string                    `json:"clientMsgId,omitempty"`
	ServerMsgID string                    `json:"serverMsgId,omitempty"`
	Seq         int64                     `json:"seq,omitempty"`
	CommittedAt int64                     `json:"committedAt,omitempty"`
	SealedAt    int64                     `json:"sealedAt,omitempty"`
}

func bindStable(c *gin.Context, value any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 96*1024)
	dec := json.NewDecoder(c.Request.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return stablemessage.ErrInvalid
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return stablemessage.ErrInvalid
	}
	return nil
}

func stableResponse(c *gin.Context, r *pb.Receipt, err error) {
	if err != nil {
		if st, ok := status.FromError(err); ok {
			err = errs.NewCodeError(int(st.Code()), st.Message())
		}
		apiresp.GinError(c, err)
		return
	}
	if r == nil {
		apiresp.GinError(c, stablemessage.ErrUnavailable)
		return
	}
	out := stableReceiptHTTP{Status: r.Status, ClientMsgID: r.ClientMsgId, ServerMsgID: r.ServerMsgId, Seq: r.Seq, CommittedAt: r.CommittedAt, SealedAt: r.SealedAt}
	if r.Descriptor_ != nil {
		d, e := stablemessage.DescriptorFromProto(r.Descriptor_)
		if e != nil {
			apiresp.GinError(c, stablemessage.ErrIntegrity)
			return
		}
		out.Descriptor = &d
	}
	apiresp.GinSuccess(c, out)
}

func registerStableRoutes(r *gin.Engine, connection grpc.ClientConnInterface) {
	client := pb.NewStableMessageClient(connection)
	r.POST("/msg/stable/submit", func(c *gin.Context) {
		var req stableSubmitHTTP
		if err := bindStable(c, &req); err != nil {
			stableResponse(c, nil, err)
			return
		}
		if err := req.Descriptor.Validate(); err != nil {
			stableResponse(c, nil, err)
			return
		}
		ctx := metadata.AppendToOutgoingContext(c, "stable-token", c.GetHeader("token"))
		reply, err := client.Submit(ctx, &pb.SubmitRequest{Descriptor_: req.Descriptor.Proto(), ContentBytes: req.ContentBytes})
		stableResponse(c, reply, err)
	})
	r.POST("/msg/stable/get_receipt", func(c *gin.Context) {
		var req stableQueryHTTP
		if err := bindStable(c, &req); err != nil {
			stableResponse(c, nil, err)
			return
		}
		ctx := metadata.AppendToOutgoingContext(c, "stable-token", c.GetHeader("token"))
		reply, err := client.GetReceipt(ctx, &pb.GetReceiptRequest{ProtocolVersion: req.ProtocolVersion, SourceNamespace: req.SourceNamespace, SourceId: req.SourceID})
		stableResponse(c, reply, err)
	})
	r.POST("/msg/stable/resolve_or_expire", func(c *gin.Context) {
		var req stablemessage.Descriptor
		if err := bindStable(c, &req); err != nil {
			stableResponse(c, nil, err)
			return
		}
		if err := req.Validate(); err != nil {
			stableResponse(c, nil, err)
			return
		}
		ctx := metadata.AppendToOutgoingContext(c, "stable-token", c.GetHeader("token"))
		reply, err := client.ResolveOrExpire(ctx, &pb.ResolveOrExpireRequest{Descriptor_: req.Proto()})
		stableResponse(c, reply, err)
	})
}
