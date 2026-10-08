package config

import "sync/atomic"

// StableMessage 限定专用实例的稳定单聊及服务身份。
type StableMessage struct {
	Enabled         bool     `yaml:"enabled"`
	SourceNamespace string   `yaml:"sourceNamespace"`
	AdminUserIDs    []string `yaml:"adminUserIDs"`
}

// RPCTLS 配置现有 RPC 端口的服务端证书与客户端信任。
type RPCTLS struct {
	Enabled    bool   `yaml:"enabled"`
	CAFile     string `yaml:"caFile"`
	CertFile   string `yaml:"certFile"`
	KeyFile    string `yaml:"keyFile"`
	ServerName string `yaml:"serverName"`
}

var stableMode atomic.Bool

// SetStableMode 在开始装配服务之前固定进程的写入策略。
func SetStableMode(enabled bool) { stableMode.Store(enabled) }

// StableMode 返回正式单聊是否只能走稳定事务。
func StableMode() bool { return stableMode.Load() }
