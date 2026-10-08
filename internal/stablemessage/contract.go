package stablemessage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"unicode/utf8"

	pb "github.com/openimsdk/open-im-server/v3/pkg/stablemessage/v1"
	"github.com/openimsdk/open-im-server/v3/pkg/util/conversationutil"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
)

const (
	Committed           = "committed"
	Expired             = "expired"
	NotFound            = "not_found"
	NotDue              = "not_due"
	MaxEnvelopeBytes    = 48 * 1024
	MaxMessageBytes     = 64 * 1024
	ReceiptCollection   = "stable_message_receipts"
	WatermarkCollection = "stable_message_watermarks"
	ModeCollection      = "stable_message_mode"
)

var (
	ErrConflict      = errs.NewCodeError(16001, "STABLE_CONFLICT")
	ErrInvalid       = errs.NewCodeError(16002, "STABLE_INVALID_ARGUMENT")
	ErrUnauthorized  = errs.NewCodeError(16003, "STABLE_UNAUTHORIZED")
	ErrUnavailable   = errs.NewCodeError(16004, "STABLE_UNAVAILABLE")
	ErrUnknownCommit = errs.NewCodeError(16005, "STABLE_UNKNOWN_COMMIT")
	ErrIntegrity     = errs.NewCodeError(16006, "STABLE_INTEGRITY_FAILURE")
	ErrDisabled      = errs.NewCodeError(16007, "STABLE_DISABLED")
	ErrTooLarge      = errs.NewCodeError(16008, "STABLE_PAYLOAD_TOO_LARGE")
	ErrLegacy        = errs.NewCodeError(16009, "STABLE_LEGACY_WRITE_BLOCKED")
	sourcePattern    = regexp.MustCompile(`^[0-9a-f]{32}$`)
	namespacePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

// Descriptor 同时用于 HTTP 与持久记录，摘要采用规范十六进制。
type Descriptor struct {
	ProtocolVersion          uint32 `json:"protocolVersion" bson:"protocol_version"`
	SourceNamespace          string `json:"sourceNamespace" bson:"source_namespace"`
	SourceID                 string `json:"sourceId" bson:"source_id"`
	BusinessEncodingVersion  uint32 `json:"businessEncodingVersion" bson:"business_encoding_version"`
	BusinessDigest           string `json:"businessDigest" bson:"business_digest"`
	TransportEncodingVersion uint32 `json:"transportEncodingVersion" bson:"transport_encoding_version"`
	TransportDigest          string `json:"transportDigest" bson:"transport_digest"`
	SenderID                 string `json:"senderId" bson:"sender_id"`
	ReceiverID               string `json:"receiverId" bson:"receiver_id"`
	ConversationID           string `json:"conversationId" bson:"conversation_id"`
	SenderPlatformID         int32  `json:"senderPlatformId" bson:"sender_platform_id"`
	ContentType              int32  `json:"contentType" bson:"content_type"`
	ExpiresAt                int64  `json:"expiresAt" bson:"expires_at"`
}

// DescriptorFromProto 严格转换不变描述。
func DescriptorFromProto(d *pb.ImmutableDescriptor) (Descriptor, error) {
	if d == nil {
		return Descriptor{}, ErrInvalid
	}
	x := Descriptor{d.ProtocolVersion, d.SourceNamespace, d.SourceId, d.BusinessEncodingVersion, hex.EncodeToString(d.BusinessDigest), d.TransportEncodingVersion, hex.EncodeToString(d.TransportDigest), d.SenderId, d.ReceiverId, d.ConversationId, d.SenderPlatformId, d.ContentType, d.ExpiresAt}
	return x, x.Validate()
}

// Proto 转换为内部协议，不改变摘要或标识。
func (d Descriptor) Proto() *pb.ImmutableDescriptor {
	b, _ := hex.DecodeString(d.BusinessDigest)
	t, _ := hex.DecodeString(d.TransportDigest)
	return &pb.ImmutableDescriptor{ProtocolVersion: d.ProtocolVersion, SourceNamespace: d.SourceNamespace, SourceId: d.SourceID, BusinessEncodingVersion: d.BusinessEncodingVersion, BusinessDigest: b, TransportEncodingVersion: d.TransportEncodingVersion, TransportDigest: t, SenderId: d.SenderID, ReceiverId: d.ReceiverID, ConversationId: d.ConversationID, SenderPlatformId: d.SenderPlatformID, ContentType: d.ContentType, ExpiresAt: d.ExpiresAt}
}

// Validate 校验线格式和单聊地址关系。
func (d Descriptor) Validate() error {
	if d.ProtocolVersion != 1 || d.BusinessEncodingVersion != 1 || d.TransportEncodingVersion != 1 || !ValidSource(d.SourceNamespace, d.SourceID) || d.ExpiresAt <= 0 {
		return ErrInvalid
	}
	for _, digest := range []string{d.BusinessDigest, d.TransportDigest} {
		b, e := hex.DecodeString(digest)
		if e != nil || len(b) != 32 || hex.EncodeToString(b) != digest {
			return ErrInvalid
		}
	}
	if d.SenderID == "" || d.ReceiverID == "" || d.SenderID == d.ReceiverID || len(d.SenderID) > 256 || len(d.ReceiverID) > 256 || !utf8.ValidString(d.SenderID) || !utf8.ValidString(d.ReceiverID) {
		return ErrInvalid
	}
	if d.ConversationID != conversationutil.GenConversationIDForSingle(d.SenderID, d.ReceiverID) {
		return ErrInvalid
	}
	if _, ok := constant.PlatformID2Name[int(d.SenderPlatformID)]; !ok || d.SenderPlatformID == constant.AdminPlatformID {
		return ErrInvalid
	}
	if d.ContentType != constant.Text && d.ContentType != constant.Custom {
		return ErrInvalid
	}
	return nil
}

// ValidSource 拒绝非规范来源与空 UUID。
func ValidSource(namespace, id string) bool {
	return namespacePattern.MatchString(namespace) && sourcePattern.MatchString(id) && id != "00000000000000000000000000000000"
}

// TransportBytes 实现跨语言固定宽度、大端规范编码。
func TransportBytes(d Descriptor, content []byte) []byte {
	var b bytes.Buffer
	b.WriteString("DW-STABLE-TRANSPORT")
	b.WriteByte(0)
	_ = binary.Write(&b, binary.BigEndian, uint16(d.TransportEncodingVersion))
	_ = binary.Write(&b, binary.BigEndian, uint16(d.BusinessEncodingVersion))
	h, _ := hex.DecodeString(d.BusinessDigest)
	b.Write(h)
	for _, s := range []string{d.SourceNamespace, d.SourceID, d.SenderID, d.ReceiverID, d.ConversationID} {
		_ = binary.Write(&b, binary.BigEndian, int32(len([]byte(s))))
		b.WriteString(s)
	}
	_ = binary.Write(&b, binary.BigEndian, d.SenderPlatformID)
	_ = binary.Write(&b, binary.BigEndian, d.ContentType)
	_ = binary.Write(&b, binary.BigEndian, int32(len(content)))
	b.Write(content)
	_ = binary.Write(&b, binary.BigEndian, d.ExpiresAt)
	return b.Bytes()
}

// CheckContent 校验原件大小、Unicode 及冻结摘要。
func CheckContent(d Descriptor, content []byte) error {
	if !utf8.Valid(content) || len(content) == 0 {
		return ErrInvalid
	}
	wire := TransportBytes(d, content)
	if len(wire) > MaxEnvelopeBytes {
		return ErrTooLarge
	}
	h := sha256.Sum256(wire)
	if hex.EncodeToString(h[:]) != d.TransportDigest {
		return ErrInvalid
	}
	return nil
}
