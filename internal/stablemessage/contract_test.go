package stablemessage

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openimsdk/protocol/constant"
	"github.com/stretchr/testify/require"
)

func testDescriptor(content []byte) Descriptor {
	d := Descriptor{ProtocolVersion: 1, SourceNamespace: "stable-test", SourceID: strings.ReplaceAll(uuid.NewString(), "-", ""), BusinessEncodingVersion: 1, BusinessDigest: hex.EncodeToString(make([]byte, 32)), TransportEncodingVersion: 1, SenderID: "a", ReceiverID: "b", ConversationID: "si_a_b", SenderPlatformID: constant.WebPlatformID, ContentType: constant.Text, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}
	digest := sha256.Sum256(TransportBytes(d, content))
	d.TransportDigest = hex.EncodeToString(digest[:])
	return d
}

func TestDescriptorValidationAndDigest(t *testing.T) {
	content := []byte("甲\n乙")
	d := testDescriptor(content)
	require.NoError(t, d.Validate())
	require.NoError(t, CheckContent(d, content))
	require.ErrorIs(t, CheckContent(d, []byte("甲\r\n乙")), ErrInvalid)
	wrong := d
	wrong.ConversationID = "si_other_pair"
	require.ErrorIs(t, wrong.Validate(), ErrInvalid)
	copy, err := DescriptorFromProto(d.Proto())
	require.NoError(t, err)
	require.Equal(t, d, copy)
}

func TestTransportEncodingIsFrozenAndVersioned(t *testing.T) {
	d := testDescriptor([]byte("x"))
	d.ExpiresAt = 1893456000123
	d.SourceID = "11111111111111111111111111111111"
	a := TransportBytes(d, []byte("x"))
	require.Equal(t, "DW-STABLE-TRANSPORT\x00", string(a[:20]))
	d.SenderPlatformID++
	require.NotEqual(t, a, TransportBytes(d, []byte("x")))
}

func TestEnvelopeLimitIncludesDescriptor(t *testing.T) {
	d := testDescriptor([]byte("x"))
	require.ErrorIs(t, CheckContent(d, make([]byte, MaxEnvelopeBytes+1)), ErrTooLarge)
}

func TestTransportEncodingCrossLanguageVector(t *testing.T) {
	d := Descriptor{ProtocolVersion: 1, SourceNamespace: "test", SourceID: strings.Repeat("1", 32), BusinessEncodingVersion: 1, BusinessDigest: strings.Repeat("0", 64), TransportEncodingVersion: 1, SenderID: "a", ReceiverID: "b", ConversationID: "si_a_b", SenderPlatformID: 1, ContentType: 110, ExpiresAt: 1800000000000}
	wire := TransportBytes(d, []byte{0, 1, 2, 255})
	require.Len(t, wire, 144)
	digest := sha256.Sum256(wire)
	require.Equal(t, "aac542bcd89d2d3bff718b664b75795eefc95e0179e0a4c6bf23397c8bc3bbb7", hex.EncodeToString(digest[:]))
}
