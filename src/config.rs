//! 压测配置：test.conf

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct TestConfig {
    pub server: ServerConfig,
    pub users: UsersConfig,
    pub scenario: ScenarioConfig,
    #[serde(default)]
    pub rate: RateConfig,
    #[serde(default)]
    pub run: RunConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// Allow local/load-test QUIC clients to accept the server's self-signed cert.
    ///
    /// None means "auto": enabled only for local QUIC targets. Set false for a
    /// production-like strict certificate verification run.
    #[serde(default)]
    pub allow_self_signed_quic: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UsersConfig {
    #[serde(default = "default_total")]
    pub total: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ScenarioConfig {
    #[serde(rename = "type", default = "default_scenario_type")]
    pub type_: String,
}

impl ScenarioConfig {
    pub fn scenario_type(&self) -> ScenarioType {
        match self.type_.as_str() {
            "group_broadcast" => ScenarioType::GroupBroadcast,
            "friend_flow" => ScenarioType::FriendFlow,
            "private_chat" => ScenarioType::PrivateChat,
            "fanout_stress" => ScenarioType::FanoutStress,
            "mixed" => ScenarioType::Mixed,
            "semantic_verify" => ScenarioType::SemanticVerify,
            "backpressure" => ScenarioType::Backpressure,
            // auth_peak = 密码登录峰值（含 bcrypt）；reconnect_token_peak = 真实重连风暴
            // （connect + token authenticate，无 bcrypt）。G8 拆两场景，见各自 run_*。
            "auth_peak" | "auth_peak_login" => ScenarioType::AuthPeak,
            "reconnect_token_peak" => ScenarioType::ReconnectTokenPeak,
            // scenario typo 同样 fail-fast（与未知 protocol 一致），不静默跑错场景。
            other => {
                eprintln!(
                    "[load] FATAL: unknown scenario type {other:?} — expected one of group_broadcast|\
                     friend_flow|private_chat|fanout_stress|mixed|semantic_verify|backpressure|\
                     auth_peak|auth_peak_login|reconnect_token_peak"
                );
                std::process::exit(1);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioType {
    GroupBroadcast,
    FriendFlow,
    PrivateChat,
    FanoutStress,
    Mixed,
    SemanticVerify,
    Backpressure,
    /// 密码登录峰值：connect + password login(bcrypt) + authenticate。
    AuthPeak,
    /// 重连风暴：warmup 拿 token → 峰值 connect + token authenticate（无 bcrypt，真实 reconnect 路径）。
    ReconnectTokenPeak,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateConfig {
    #[serde(default = "default_connect_per_sec")]
    pub connect_per_sec: u32,
    /// 并发建连上限（connect_per_sec=0 时生效），避免客户端线程数远超服务端
    #[serde(default = "default_connect_concurrent")]
    pub connect_concurrent: usize,
    #[serde(default = "default_msg_per_sec")]
    pub msg_per_sec: u32,
    /// backpressure 发送端 TPS 下限断言。**不设 = smoke 不断言 TPS（SKIP）**；
    /// gate profile 设为 50（或对齐 REVIEW_CHECKLIST §B）。避免 gate 阈值套在 smoke 规模上假失败。
    #[serde(default)]
    pub min_sender_tps: Option<f64>,
    /// gate 硬要求：true 时 server RSS 必须有效（缺 `LOAD_TEST_SERVER_PID` / 采样失败 → **FAIL 而非 SKIP**），
    /// 防止用错 PID 跑 gate 仍假 exit 0。smoke 不设 = false。
    #[serde(default)]
    pub require_server_rss: bool,
}

impl Default for RateConfig {
    fn default() -> Self {
        Self {
            connect_per_sec: default_connect_per_sec(),
            connect_concurrent: default_connect_concurrent(),
            msg_per_sec: default_msg_per_sec(),
            min_sender_tps: None,
            require_server_rss: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunConfig {
    #[serde(default = "default_duration_sec")]
    pub duration_sec: u64,
    /// 预热时间（秒），建连完成后等待此时间再开始采集
    #[serde(default = "default_warmup_sec")]
    pub warmup_sec: u64,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            duration_sec: default_duration_sec(),
            warmup_sec: default_warmup_sec(),
        }
    }
}

/// 压测环境：local（本地开发，阈值宽松）或 ci（CI 环境，阈值严格）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadTestEnv {
    Local,
    CI,
}

impl LoadTestEnv {
    /// 从 LOAD_TEST_ENV 环境变量读取，默认 local
    pub fn from_env() -> Self {
        match std::env::var("LOAD_TEST_ENV").unwrap_or_default().as_str() {
            "ci" | "CI" => Self::CI,
            _ => Self::Local,
        }
    }
}

/// 环境相关的阈值配置
pub struct EnvThresholds {
    /// 建连成功率下限
    pub min_connect_rate: f64,
    /// 消息 p99 延迟上限 (ms)
    pub max_p99_latency_ms: f64,
    /// 错误率上限
    pub max_error_rate: f64,
    /// 最低 TPS
    pub min_tps: f64,
}

impl EnvThresholds {
    pub fn for_env(env: LoadTestEnv) -> Self {
        match env {
            LoadTestEnv::Local => Self {
                min_connect_rate: 0.90,
                max_p99_latency_ms: 500.0,
                max_error_rate: 0.10,
                min_tps: 100.0,
            },
            LoadTestEnv::CI => Self {
                min_connect_rate: 0.95,
                max_p99_latency_ms: 200.0,
                max_error_rate: 0.05,
                min_tps: 500.0,
            },
        }
    }
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    9001
}
fn default_protocol() -> String {
    "quic".to_string()
}
fn default_total() -> u32 {
    100
}
fn default_scenario_type() -> String {
    "group_broadcast".to_string()
}
fn default_connect_per_sec() -> u32 {
    0
}
fn default_connect_concurrent() -> usize {
    50
}
fn default_msg_per_sec() -> u32 {
    500
}
fn default_duration_sec() -> u64 {
    60
}
fn default_warmup_sec() -> u64 {
    0
}

impl TestConfig {
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = std::fs::read_to_string(path)?;
        let mut c: TestConfig = toml::from_str(&s)?;
        if c.scenario.type_.is_empty() {
            c.scenario.type_ = default_scenario_type();
        }
        Ok(c)
    }
}

impl ServerConfig {
    pub fn effective_protocol(&self) -> String {
        std::env::var("LOAD_TEST_PROTOCOL")
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| self.protocol.trim().to_ascii_lowercase())
    }

    pub fn allow_self_signed_quic(&self) -> bool {
        if let Some(value) = env_bool("LOAD_TEST_QUIC_ACCEPT_SELF_SIGNED") {
            return value;
        }

        self.allow_self_signed_quic
            .unwrap_or_else(|| self.effective_protocol() == "quic" && is_local_host(&self.host))
    }
}

fn env_bool(key: &str) -> Option<bool> {
    let value = std::env::var(key).ok()?.trim().to_ascii_lowercase();
    match value.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn is_local_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "0.0.0.0")
}
