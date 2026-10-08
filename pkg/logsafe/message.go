package logsafe

import "github.com/openimsdk/protocol/sdkws"

// MessageSummary 只暴露排障需要的消息定位，不包含正文、附件地址或凭证。
type MessageSummary struct {
	ClientMsgID string
	ServerMsgID string
	Seq         int64
	ContentType int32
}

// Message 必须接收原消息指针，不先序列化可能含敏感内容的载荷。
func Message(msg *sdkws.MsgData) MessageSummary {
	if msg == nil {
		return MessageSummary{}
	}
	return MessageSummary{ClientMsgID: msg.ClientMsgID, ServerMsgID: msg.ServerMsgID, Seq: msg.Seq, ContentType: msg.ContentType}
}
