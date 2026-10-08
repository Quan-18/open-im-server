# 稳定消息服务接口

三个入口均为 POST，使用 `token` 请求头提供专用 OpenIM 管理员 Token，`operationID` 仅用于关联请求。JSON 字段采用 camelCase，拒绝未知字段。摘要使用 64 个小写十六进制字符；ContentBytes 使用标准 Base64；时间为 UTC Unix 毫秒，序号为 JSON 整数。

| 入口 | 请求 |
|---|---|
| `/msg/stable/submit` | `descriptor 的全部字段，加 `contentBytes`，字段均在顶层` |
| `/msg/stable/get_receipt` | `{ "protocolVersion": 1, "sourceNamespace": "...", "sourceId": "32位小写UUID" }` |
| `/msg/stable/resolve_or_expire` | `descriptor 的全部字段，均在顶层` |

descriptor 完整字段为：`protocolVersion`、`sourceNamespace`、`sourceId`、`businessEncodingVersion`、`businessDigest`、`transportEncodingVersion`、`transportDigest`、`senderId`、`receiverId`、`conversationId`、`senderPlatformId`、`contentType`、`expiresAt`。三个版本当前必须为 1。单聊会话必须等于发送者、接收者排序后以 `_` 连接并加 `si_` 前缀的值。

成功响应使用 OpenIM 信封 `{ "errCode": 0, "errMsg": "", "errDlt": "", "data": { ... } }`。data 完整字段为 `status`、`descriptor`、`clientMsgId`、`serverMsgId`、`seq`、`committedAt`、`sealedAt`；终态完整回显 descriptor。`status` 仅有 `committed`、`expired`、`not_found`、`not_due`。非终态的 descriptor 可以为 null；仅适用特定终态的消息身份、序号及时间字段在不适用时省略，不以空字符串或 0 冒充值。HTTP 正常处理统一返回 200；调用方必须检查 errCode。

| errCode | errMsg | 语义 |
|---|---|---|
| 16001 | STABLE_CONFLICT | 同来源键的不可变描述不同 |
| 16002 | STABLE_INVALID_ARGUMENT | 格式、版本、摘要或技术范围无效 |
| 16003 | STABLE_UNAUTHORIZED | 服务 Token 或命名空间无权 |
| 16004 | STABLE_UNAVAILABLE | 暂不可用，不能解释为 NotFound |
| 16005 | STABLE_UNKNOWN_COMMIT | 事务结果未知，继续以原来源查证 |
| 16006 | STABLE_INTEGRITY_FAILURE | 收据、消息块或序号互相矛盾 |
| 16007 | STABLE_DISABLED | 当前实例未启用稳定写 |
| 16008 | STABLE_PAYLOAD_TOO_LARGE | 封套或最终消息超限 |
| 16009 | STABLE_LEGACY_WRITE_BLOCKED | 稳定实例禁止旧正式单聊写入或清理 |

gRPC 服务为 `openim.stablemessage.v1.StableMessage`，三个方法名称与入口一致；原始凭证通过 `stable-token` metadata 传递。生成类型只属于 Fork，未修改外部 protocol 模块。网络调用要求 TLS，服务端仍逐次独立验证 Token。所有恢复沿用同一 descriptor，终态收据一致性必须完整比对。

## 运行配置与边界

在独立配置目录的 `share.yml` 设置 `stableMessage.enabled=true`、固定 `sourceNamespace` 和专用管理员 `adminUserIDs`。该账号必须同时属于 OpenIM 管理员配置。首次启用要求专用 Mongo 数据库与 Redis 队列没有旧正式单聊数据；源端建立持久模式标记，不能通过关闭配置恢复旧写入。稳定路径强制验证实际 Mongo 副本集能力，不能在 standalone 上降级。

网络 RPC 沿用现有 msg 端口，通过 `rpcTLS` 配置 CA、服务端证书、私钥与目标名称。API 桥接在 `rpcTLS.enabled=true` 时使用相同证书提供 HTTPS；生产的非回环入口只能在 TLS 终结和受控服务网络后开放，不得把明文桥接直接暴露。回环映射的本地验收是独立测试配置，不证明生产网络安全。稳定模式显式拒绝尚未验证且会覆盖 TLS 选项的 Kubernetes 服务发现；普通模式保持原发现行为。

新服务可与原 Msg 服务在同一连接注册；全部服务登记后才启动网络监听。单进程使用 Fork 内的受控连接包装调用同一真实处理器，凭证验证不会跳过，通用跨实例 RpcInvoke 不转发 Stable 方法。HTTP、RPC、认证缓存和稳定消息下行日志不输出正文、完整凭证或完整请求。

正式消息、来源收据、已提交水位和待传播记录在同一 Mongo 事务内提交。提交结果未知时不得更换键；到期封存与提交竞争同一来源键。30 天是调用方的普通恢复窗口，未知结果不能触发原件删除；源端收据和 Expired 墓碑不自动回收。

后台恢复会话内串行推进缓存、双方会话元数据和在线推送。每次投影具有独立 operationID，沿用正式身份与 Seq，消息到达或重建不推进已读。Redis 重建使用持久代次，旧初始化者不能降低代次或删除他人锁；MQ 发布结果未知允许重投同一消息。当前稳定消息显式关闭第三方离线推送及其修改型回调，正常正式存储、在线投递和历史查询保留。

本专题保留既有 push 消费者的确认行为，不把处理结束一律视为成功 ACK，也不新增通知送达收据平台。通知可能重复，既有 ACK 行为可能保留队列 pending；正式消息、稳定回执和增量历史不依赖 WebSocket 送达或队列清零。通知消费、积压及容量边界仍须在后续实时专题评估，不能据本专题宣称恰好投递一次或所有通知 pending 已收敛。

误入旧 toRedis／toMongo 通道的正式单聊先写 Mongo 多数确认的隔离记录，再确认队列；隔离中不保存旧队列的外层认证 Token。旧写入、分号、撤回、删除和清理存在最终屏障，定时消息清理的候选过滤保留独立通知维护。正文、消息块或收据不一致时报告完整性故障，不补造消息。

## 协议生成与验证

工具版本及 Windows protoc 压缩包 SHA-256 固定在 `scripts/stable-proto-tools.json`，不用浮动 latest。执行 `scripts/Generate-StableProto.ps1` 生成 Fork 内的 Go 文件；执行该脚本加 `-Verify` 在隔离目录重生成并逐文件比较。生成文件随源码交付，Linux 编译直接使用这些文件，不下载另一份 protocol 源码。

独立存储测试位于 `internal/stablemessage`，真实 Auth／User／Conversation／Msg 的进程内及固定地址 TLS 装配位于 `internal/stablemessagetest`，旧队列隔离测试位于 `internal/msgtransfer`。集成测试要求显式的 `OPENIM_STABLE_TEST_MONGO_URI`、`OPENIM_STABLE_TEST_REDIS_ADDR` 与对应密码；TLS 装配另读取 `OPENIM_STABLE_TEST_TLS_CA/CERT/KEY`。测试创建唯一数据库；真实装配只在经过空库与独占标记检查的专用 Redis DB14 执行，不复用正式实例。

Mongo failpoint 用例额外要求 `OPENIM_STABLE_TEST_FAILPOINTS=1` 和隔离 mongod 的 `enableTestCommands`，按测试连接的唯一 appName 限定影响；不得在生产开启。未配置依赖时集成用例明确跳过，跳过不是业务通过。实际命令、制品指纹和结果由验收环境记录，不能只凭本文或测试源码宣称完整验收已完成。
