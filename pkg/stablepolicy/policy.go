package stablepolicy

import (
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/openimsdk/open-im-server/v3/pkg/msgprocessor"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/sdkws"
	"github.com/openimsdk/tools/errs"
	"strings"
)

var ErrLegacy = errs.NewCodeError(16009, "STABLE_LEGACY_WRITE_BLOCKED")
var ErrIntegrity = errs.NewCodeError(16006, "STABLE_INTEGRITY_FAILURE")

// Configure 支持独立服务直接装配时同样初始化写入屏障。
func Configure(enabled bool) { config.SetStableMode(enabled) }

// Protected 识别正式单聊会话及其块，不影响独立通知会话。
func Protected(id string) bool { return config.StableMode() && strings.HasPrefix(id, "si_") }

// CheckWrite 在最终存储边界禁止旧单聊写入、分号或清理。
func CheckWrite(id string) error {
	if Protected(id) {
		return ErrLegacy
	}
	return nil
}

// CheckMessage 按最终落点判断通知副本，保留不占正式位置的通知。
func CheckMessage(msg *sdkws.MsgData) error {
	if !config.StableMode() || msg == nil {
		return nil
	}
	if msg.SessionType != constant.SingleChatType {
		return nil
	}
	opts := msgprocessor.Options(msg.Options)
	if !msgprocessor.IsNotificationByMsg(msg) || opts.IsSendMsg() {
		return ErrLegacy
	}
	return nil
}
