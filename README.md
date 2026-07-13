# privchat-load

PrivChat 压测 & 语义验证工具，独立项目，与 privchat-server、privchat-sdk 同级。

## 快速开始

```bash
cd privchat-load

# 使用默认 test.conf
cargo run --release

# 指定配置文件
cargo run --release -- --config test_a.conf
```

首次运行会自动注册测试账号并写入 `data/users.txt`，后续复用不再重复注册。

## 项目结构

```
privchat-load/
├── src/
│   ├── main.rs           # 入口，配置加载，场景分发
│   ├── config.rs         # 配置结构体 & 默认值
│   ├── scenario.rs       # 标准场景（group_broadcast, friend_flow）
│   ├── semantic.rs       # Scenario A: 语义正确性验证
│   ├── backpressure.rs   # Scenario B: 背压验证
│   ├── auth_peak.rs      # Scenario C: 认证峰值验证
│   ├── user_store.rs     # 账号注册 & data/users.txt 管理
│   └── assertions.rs     # 断言框架
├── data/
│   ├── users.txt         # 持久化账号（自动生成）
│   └── users/            # 各用户 SDK 本地数据
├── test.conf             # 高负载配置（1000 用户）
├── test_a.conf           # Scenario A 配置
├── test_b.conf           # Scenario B 配置
└── test_c.conf           # Scenario C 配置
```

## 配置说明

配置文件为 TOML 格式，各节说明：

| 节 | 字段 | 默认值 | 说明 |
|---|------|--------|------|
| `[server]` | `host` | `"127.0.0.1"` | 服务器地址 |
| | `port` | `9001` | 服务器端口 |
| | `protocol` | `"quic"` | 传输协议：`quic` / `tcp` / `websocket` |
| | `allow_self_signed_quic` | `auto` | QUIC 是否接受自签证书；默认仅本地 QUIC 压测开启 |
| `[users]` | `total` | `100` | 测试用户数（不足时自动注册） |
| `[scenario]` | `type` | `"group_broadcast"` | 场景类型（见下方） |
| `[rate]` | `connect_per_sec` | `0` | 连接速率限制（0 = 全速并发） |
| | `connect_concurrent` | `50` | 全速时最大并发连接数 |
| | `msg_per_sec` | `500` | 每秒发送消息数 |
| `[run]` | `duration_sec` | `60` | 测试持续时间（秒） |
| | `warmup_sec` | `0` | 预热时间（不计入统计） |

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LOAD_TEST_PASSWORD` | `"loadtest123"` | 测试账号密码 |
| `LOAD_TEST_ENV` | `"local"` | 环境模式：`local`（宽松阈值）/ `ci`（严格阈值） |
| `LOAD_TEST_METRICS_URL` | - | Prometheus metrics URL（auth_peak 用） |
| `LOAD_TEST_PROTOCOL` | 配置文件值 | 临时强制单协议：`quic` / `tcp` / `websocket` |
| `LOAD_TEST_QUIC_ACCEPT_SELF_SIGNED` | `auto` | 强制控制 QUIC 自签证书：`1` 开启，`0` 关闭 |

## 场景

### Scenario A: 语义正确性验证 (`semantic_verify`)

验证消息系统的三大核心不变量。主账号建群 -> 邀请 bot -> 发送 N 条消息 -> 各 bot 分页拉取验证。

```bash
cargo run --release -- --config test_a.conf   # 20 用户, 200 条消息
```

**断言项（9 项）：**

| 断言 | 说明 |
|------|------|
| `pts_strictly_increasing` | 每个 channel 内 pts 严格递增 |
| `server_message_id_channel_unique` | channel 内 server_msg_id 不重复 |
| `server_message_id_window_unique` | 观察窗口内 server_msg_id 不重复 |
| `receiver_message_complete` | 接收端拉取到全部消息（分页） |
| `no_business_ghost_messages` | 无幽灵业务消息 |
| `system_ghost_messages_bounded` | 系统消息幽灵数 <= 20 |
| `no_per_user_duplicate_delivery` | 每用户无重复投递 |
| `unread_converged` | 未读计数收敛到预期值 |
| `connect_rate` | 连接成功率 >= 90% |

### Scenario B: 背压验证 (`backpressure`)

验证慢消费者不会拖慢快发送者。约 50% bot 模拟慢消费（每条消息延迟 500ms），测量发送端延迟和内存增长。

```bash
cargo run --release -- --config test_b.conf   # 20 用户
```

**断言项：**

| 断言 | 阈值 |
|------|------|
| `sender_p99_latency_ms` | <= 1000ms |
| `rss_growth_mb` | <= 200MB |
| `error_rate` | <= 10% |
| `sender_tps` | >= 50 |

### Scenario C: 认证峰值验证 (`auth_peak`)

验证系统在高并发认证突发下的表现。先顺序连接建立基线，再全部断开后并发重连测量峰值。

```bash
cargo run --release -- --config test_c.conf   # 50 用户
```

**断言项：**

| 断言 | 阈值 |
|------|------|
| `baseline_success_rate` | >= 95% |
| `peak_success_rate` | >= 90% |
| `peak_auth_p99_ms` | <= 5000ms |

### 标准场景

| 场景 | `scenario.type` | 说明 |
|------|----------------|------|
| 群消息压测 | `group_broadcast` | 主账号建群 -> 邀请 bot -> 按速率发群消息，统计 TPS/延迟/错误 |
| 加好友流程 | `friend_flow` | 主账号对每个 bot 发好友申请 -> bot 接受，统计成功数 |

## 示例配置

```toml
# test_a.conf - 语义正确性验证
[server]
host = "127.0.0.1"
port = 9001
protocol = "quic"
# 本地 QUIC 默认自动允许自签证书；远端压测建议显式设为 false。
# allow_self_signed_quic = true

[users]
total = 20

[scenario]
type = "semantic_verify"

[rate]
msg_per_sec = 20

[run]
duration_sec = 10
```

```toml
# test.conf - 高负载群消息压测
[server]
host = "127.0.0.1"
port = 9001
protocol = "quic"
# 本地 QUIC 默认自动允许自签证书；远端压测建议显式设为 false。
# allow_self_signed_quic = true

[users]
total = 1000

[scenario]
type = "group_broadcast"

[rate]
connect_concurrent = 1000
msg_per_sec = 2000

[run]
duration_sec = 60
```

## data/users.txt 格式

每行 4 个字段（空格分隔）：

```
username device_id token user_id
loadtest_000000 0ecb1e99-... eyJ0eXAi... 100000086
loadtest_000001 1b2ec31a-... eyJ0eXAi... 100000087
```

首次运行自动注册生成，后续复用。第一个用户为主账号（负责建群、发消息），其余为机器人。
