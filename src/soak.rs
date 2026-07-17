//! 场景 G10：soak —— 长时持续负载下检测 **server** 资源无持续单调上涨（泄漏）。
//!
//! LOAD_GATE_EXECUTION_SPEC §2.4 / PRIVCHAT_SERVER_REMEDIATION_TASKS §3.1（阈值权威源）。
//!
//! **§3.1 硬判据**：RSS 净增长+末段趋平 / FD 净增长 / tokio task 净增<5% / handler 错误率<0.1%
//! / 周期重连成功率>99% / submit ACK RTT / 真实 recipient timeline delivery rate、p99 与劣化。
//! **防假绿**：唯一 msgid；所有 profile 强制有效 server PID + metrics URL，采样期任一失效即 FAIL；
//! profile 枚举 fail-fast + 最短时长校验；发送量 ≥ 目标 90%；`msg_per_sec>0`。

use crate::assertions::AssertionReport;
use crate::scenario::{rpc_call_fire, rpc_call_typed, SubmitMessagePayload};
use crate::user_store::{server_endpoints, UserRecord};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use hdrhistogram::Histogram;
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::{ClientSubmitRequest, ClientSubmitResponse};
use privchat_sdk::{PrivchatConfig, PrivchatSdk, SdkEvent};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{info, warn};

/// soak profile：决定最短 duration/warmup 与 §3.1 阈值。未知值 fail-fast。未设 → `Dev`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SoakProfile {
    Dev,
    /// Short directional delivery gate. It validates cross-node routing and
    /// recovery semantics; long-horizon leak assertions remain the 1h/4h gate.
    CrossNode,
    Smoke,
    Gate1h,
    Gate4h,
}

impl SoakProfile {
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "dev" => Ok(Self::Dev),
            "cross_node" | "cross-node" | "crossnode" => Ok(Self::CrossNode),
            "smoke" => Ok(Self::Smoke),
            "gate" | "gate_1h" | "gate1h" => Ok(Self::Gate1h),
            "gate_4h" | "gate4h" => Ok(Self::Gate4h),
            other => Err(format!(
                "unknown LOAD_TEST_SOAK_PROFILE {other:?} — expected dev|cross_node|smoke|gate_1h|gate_4h"
            )),
        }
    }
    fn min_duration_sec(&self) -> u64 {
        match self {
            Self::Dev => 0,
            Self::CrossNode => 60,
            Self::Smoke => 900,
            Self::Gate1h => 3600,
            Self::Gate4h => 14400,
        }
    }
    fn min_warmup_sec(&self) -> u64 {
        match self {
            Self::Dev => 0,
            Self::CrossNode => 0,
            _ => 600,
        }
    }
    fn rss_net_max_mb(&self) -> f64 {
        match self {
            Self::Dev => 30.0,
            Self::CrossNode => 30.0,
            Self::Smoke => 40.0,
            Self::Gate1h => 100.0,
            Self::Gate4h => 200.0,
        }
    }
    fn rss_tail_slope_max(&self) -> f64 {
        match self {
            Self::Dev => 3.0,
            Self::CrossNode => 3.0,
            Self::Smoke => 2.0,
            _ => 1.0,
        }
    }
    fn fd_net_max(&self) -> f64 {
        match self {
            Self::Dev => 32.0,
            Self::CrossNode => 32.0,
            Self::Smoke => 40.0,
            _ => 50.0,
        }
    }
    fn tokio_task_net_pct_max(&self) -> f64 {
        match self {
            Self::Dev => 0.10,
            Self::CrossNode => 0.10,
            _ => 0.05,
        }
    }
    fn label(&self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::CrossNode => "cross_node",
            Self::Smoke => "smoke",
            Self::Gate1h => "gate_1h",
            Self::Gate4h => "gate_4h",
        }
    }
}

fn next_local_message_id() -> u64 {
    static SEED: AtomicU64 = AtomicU64::new(0);
    if SEED.load(Ordering::Relaxed) == 0 {
        let ts = chrono::Utc::now().timestamp_millis() as u64;
        let _ = SEED.compare_exchange(
            0,
            ts.saturating_mul(1_000_000),
            Ordering::SeqCst,
            Ordering::Relaxed,
        );
    }
    SEED.fetch_add(1, Ordering::Relaxed).max(1)
}

fn rss_mb_of(pid: &str) -> f64 {
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", pid])
        .output()
    {
        if let Ok(kb) = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>() {
            return kb as f64 / 1024.0;
        }
    }
    0.0
}

fn server_fd_count(pid: &str) -> Option<u64> {
    // Linux containers expose the authoritative descriptor set directly. This avoids
    // making the production gate depend on optional diagnostic packages such as lsof.
    let proc_fd = format!("/proc/{pid}/fd");
    if Path::new(&proc_fd).exists() {
        return std::fs::read_dir(proc_fd)
            .ok()
            .map(|entries| entries.filter_map(Result::ok).count() as u64);
    }

    // macOS/local development fallback where procfs is not available.
    let out = std::process::Command::new("lsof")
        .args(["-p", pid])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let n = String::from_utf8_lossy(&out.stdout).lines().count() as u64;
    Some(n.saturating_sub(1))
}

pub(crate) fn linreg_slope(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len();
    if n < 2 || n != ys.len() {
        return 0.0;
    }
    let nf = n as f64;
    let sx: f64 = xs.iter().sum();
    let sy: f64 = ys.iter().sum();
    let sxx: f64 = xs.iter().map(|x| x * x).sum();
    let sxy: f64 = xs.iter().zip(ys).map(|(x, y)| x * y).sum();
    let denom = nf * sxx - sx * sx;
    if denom.abs() < f64::EPSILON {
        return 0.0;
    }
    (nf * sxy - sx * sy) / denom
}

/// 稳态 RPC 错误率判定（纯函数、可测）。`Err(())` = 请求增量 < 最低期望（指标未动/缺失 → 防假绿）。
pub(crate) fn steady_rpc_error_rate(d_ok: f64, d_err: f64, min_total: f64) -> Result<f64, ()> {
    let total = d_ok + d_err;
    if total < min_total {
        return Err(());
    }
    Ok(d_err / total.max(1.0))
}

/// 重连探针判定（纯函数、可测），把「init 失败 / att==0(非dev) / att==0(dev)」三条假绿路径显式化。
#[derive(Debug, PartialEq)]
pub(crate) enum ReconnectVerdict {
    InitFail,
    ZeroAttemptFail,
    SkipDev,
    Rate(f64),
}

pub(crate) fn reconnect_verdict(
    init_failed: bool,
    att: u64,
    ok: u64,
    is_dev: bool,
) -> ReconnectVerdict {
    if init_failed {
        return ReconnectVerdict::InitFail;
    }
    if att == 0 {
        return if is_dev {
            ReconnectVerdict::SkipDev
        } else {
            ReconnectVerdict::ZeroAttemptFail
        };
    }
    ReconnectVerdict::Rate(ok as f64 / att as f64)
}

pub(crate) fn net_delta(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let k = (n / 4).clamp(1, 5);
    let first: f64 = values[..k].iter().sum::<f64>() / k as f64;
    let last: f64 = values[n - k..].iter().sum::<f64>() / k as f64;
    last - first
}

/// server /metrics 采样。`*_found` 记录该 family 是否实际出现（缺失 → 硬失败，防假绿）。
#[derive(Clone, Copy, Default)]
struct SrvMetrics {
    rpc_ok: f64,
    rpc_error: f64,
    rpc_found: bool,
    tokio_tasks: f64,
    tokio_found: bool,
}

async fn fetch_srv_metrics(url: &str) -> Option<SrvMetrics> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let text = client.get(url).send().await.ok()?.text().await.ok()?;
    let mut m = SrvMetrics::default();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // privchat_rpc_requests_total{result="ok"} N（server init 已预注册 ok/error=0）
        if line.starts_with("privchat_rpc_requests_total{") {
            let val = line
                .rsplit('}')
                .next()
                .and_then(|s| s.trim().parse::<f64>().ok());
            if let Some(v) = val {
                m.rpc_found = true;
                if line.contains("result=\"ok\"") {
                    m.rpc_ok += v;
                } else if line.contains("result=\"error\"") {
                    m.rpc_error += v;
                }
            }
        } else if let Some(rest) = line.strip_prefix("privchat_tokio_alive_tasks ") {
            if let Ok(v) = rest.trim().parse::<f64>() {
                m.tokio_tasks = v;
                m.tokio_found = true;
            }
        }
    }
    Some(m)
}

pub struct SoakResult {
    pub report: AssertionReport,
}

async fn connect_login_auth(
    record: &UserRecord,
    data_dir: &Path,
    subdir: &str,
    host: &str,
    port: u16,
    protocol: &str,
) -> Result<Arc<PrivchatSdk>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = server_endpoints(host, port, protocol);
    let dir = data_dir.join(subdir).join(&record.username);
    std::fs::create_dir_all(&dir)?;
    let config = PrivchatConfig {
        endpoints,
        connection_timeout_secs: 30,
        data_dir: dir.to_string_lossy().to_string(),
    };
    let sdk = PrivchatSdk::new(config);
    sdk.connect().await?;
    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let login = sdk
        .login(record.username.clone(), password, record.device_id.clone())
        .await?;
    sdk.authenticate(login.user_id, login.token, login.device_id)
        .await?;
    Ok(Arc::new(sdk))
}

type Sample = (f64, f64, Option<u64>, f64); // (t, rss_mb, fd, tokio_tasks)
const MIN_P99_SAMPLES: usize = 10_000;

fn delivery_correlation_content(raw_content: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw_content)
        .ok()
        .and_then(|value| {
            value
                .get("content")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| raw_content.to_string())
}

fn p99_gate_eligible(sample_count: usize) -> bool {
    sample_count >= MIN_P99_SAMPLES
}

#[allow(clippy::too_many_arguments)]
pub async fn run_soak(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    duration_sec: u64,
    msg_per_sec: u32,
    warmup_sec: u64,
    metrics_url: Option<&str>,
) -> Result<SoakResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let _ = std::fs::remove_dir_all(data_dir.join("soak"));
    let _ = std::fs::remove_dir_all(data_dir.join("soak_probe"));

    let profile =
        match SoakProfile::parse(&std::env::var("LOAD_TEST_SOAK_PROFILE").unwrap_or_default()) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("[soak] FATAL: {e}");
                std::process::exit(1);
            }
        };
    if duration_sec < profile.min_duration_sec() || warmup_sec < profile.min_warmup_sec() {
        eprintln!(
            "[soak] FATAL: profile={} 要求 duration>={}s warmup>={}s，实际 {}s/{}s",
            profile.label(),
            profile.min_duration_sec(),
            profile.min_warmup_sec(),
            duration_sec,
            warmup_sec
        );
        std::process::exit(1);
    }
    if msg_per_sec == 0 {
        eprintln!("[soak] FATAL: msg_per_sec 必须 > 0（代表性负载）");
        std::process::exit(1);
    }

    // 强制有效 server PID + metrics URL（G10 测 server）。
    let pid = match std::env::var("LOAD_TEST_SERVER_PID")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        Some(p) => p,
        None => {
            report.fail("soak_server_pid_set", "必须设 LOAD_TEST_SERVER_PID");
            return Ok(SoakResult { report });
        }
    };
    let murl = match metrics_url {
        Some(u) => u.to_string(),
        None => {
            report.fail(
                "soak_metrics_url_set",
                "必须设 LOAD_TEST_METRICS_URL（采 tokio task/handler 错误率）",
            );
            return Ok(SoakResult { report });
        }
    };
    let rss0 = rss_mb_of(&pid);
    if rss0 <= 0.0 {
        report.fail("soak_server_pid_valid", "RSS 读不到=无效 PID");
        return Ok(SoakResult { report });
    }
    if server_fd_count(&pid).is_none() {
        report.fail("soak_server_fd_available", "server FD(lsof)不可用");
        return Ok(SoakResult { report });
    }
    let m_start = match fetch_srv_metrics(&murl).await {
        Some(m) => m,
        None => {
            report.fail("soak_metrics_reachable", "启动即拉不到 /metrics");
            return Ok(SoakResult { report });
        }
    };
    // 缺 metric family → 硬失败（防"指标缺失也 PASS"）。
    if !m_start.tokio_found {
        report.fail(
            "soak_metric_tokio_present",
            "/metrics 缺 privchat_tokio_alive_tasks",
        );
        return Ok(SoakResult { report });
    }
    if !m_start.rpc_found {
        report.fail(
            "soak_metric_rpc_present",
            "/metrics 缺 privchat_rpc_requests_total（server 需重建含预注册）",
        );
        return Ok(SoakResult { report });
    }
    info!(
        "[soak] profile={} duration={}s warmup={}s msg_per_sec={} users={} pid={} rss0={:.1}MB tasks0={} rpc_ok0={} rpc_err0={}",
        profile.label(), duration_sec, warmup_sec, msg_per_sec, records.len(), pid, rss0, m_start.tokio_tasks, m_start.rpc_ok, m_start.rpc_error
    );

    // === 1. 建 N 个认证连接 ===
    let mut sdks: Vec<Arc<PrivchatSdk>> = Vec::new();
    let mut user_ids: Vec<u64> = Vec::new();
    let secondary_port = std::env::var("LOAD_TEST_SECONDARY_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok());
    let secondary_host = std::env::var("LOAD_TEST_SECONDARY_SERVER_HOST").ok();
    let cross_node_mode = secondary_host.is_some();
    for (index, rec) in records.iter().enumerate() {
        let target_port = if index > 0 {
            secondary_port.unwrap_or(server_port)
        } else {
            server_port
        };
        let target_host = if index > 0 {
            secondary_host.as_deref().unwrap_or(server_host)
        } else {
            server_host
        };
        match connect_login_auth(
            rec,
            data_dir,
            "soak",
            target_host,
            target_port,
            server_protocol,
        )
        .await
        {
            Ok(sdk) => {
                sdks.push(sdk);
                user_ids.push(rec.user_id);
            }
            Err(e) => warn!("[soak] connect {} failed: {}", rec.username, e),
        }
    }
    let conn_n = sdks.len();
    report.assert_gte(
        "soak_connect_rate",
        conn_n as f64 / records.len().max(1) as f64,
        0.90,
    );
    if sdks.is_empty() {
        report.fail("soak_no_connections", "0 连接");
        return Ok(SoakResult { report });
    }

    // === 2. 建群 + 成员 ===
    let main_sdk = sdks[0].clone();
    let create_req = GroupCreateRequest {
        name: format!("soak_{}", chrono::Utc::now().timestamp()),
        description: None,
        member_ids: None,
        creator_id: 0,
    };
    let group_id = match rpc_call_typed::<_, GroupCreateResponse>(
        &main_sdk,
        routes::group::CREATE,
        &create_req,
    )
    .await
    {
        Ok(res) => res.group_id,
        Err(e) => {
            report.fail("soak_group_created", &format!("建群失败: {e}"));
            for sdk in &sdks {
                sdk.shutdown().await;
            }
            return Ok(SoakResult { report });
        }
    };
    let mut joined = 0u64;
    for &uid in &user_ids {
        let add_req = GroupMemberAddRequest {
            group_id,
            user_id: uid,
            role: None,
            inviter_id: 0,
        };
        if rpc_call_fire(&main_sdk, routes::group_member::ADD, &add_req)
            .await
            .is_ok()
        {
            joined += 1;
        }
    }
    report.assert_gte(
        "soak_member_join_rate",
        joined as f64 / user_ids.len().max(1) as f64,
        0.90,
    );
    info!(
        "[soak] group {} created, {}/{} joined",
        group_id,
        joined,
        user_ids.len()
    );

    // A real recipient measures commit-to-local-timeline latency. Correlation
    // uses the unique message content inserted before submit, so a push that
    // wins the race against the submit response is still measured.
    let delivery_pending = Arc::new(Mutex::new(HashMap::<String, (Instant, f64)>::new()));
    let delivery_samples = Arc::new(Mutex::new(Vec::<(f64, u64)>::new()));
    let delivery_lagged = Arc::new(AtomicU64::new(0));
    let delivery_events_seen = Arc::new(AtomicU64::new(0));
    let delivery_unmatched = Arc::new(AtomicU64::new(0));
    let delivery_message_missing = Arc::new(AtomicU64::new(0));
    let delivery_message_errors = Arc::new(AtomicU64::new(0));
    let delivery_recipient = sdks.get(1).cloned();
    if let Some(recipient) = &delivery_recipient {
        match recipient.run_bootstrap_sync().await {
            Ok(()) => report.assert_gte("soak_delivery_recipient_bootstrap", 1.0, 1.0),
            Err(error) => report.fail(
                "soak_delivery_recipient_bootstrap",
                &format!("接收端 bootstrap sync 失败: {error}"),
            ),
        }
    }
    let delivery_listener = if let Some(recipient) = delivery_recipient {
        let mut events = recipient.subscribe_events();
        let (lookup_tx, mut lookup_rx) = tokio::sync::mpsc::unbounded_channel();
        let lagged = delivery_lagged.clone();
        let events_seen = delivery_events_seen.clone();
        let event_receiver = tokio::spawn(async move {
            loop {
                match events.recv().await {
                    Ok(SdkEvent::TimelineUpdated {
                        channel_id,
                        message_id,
                        ..
                    }) if channel_id == group_id => {
                        events_seen.fetch_add(1, Ordering::Relaxed);
                        if lookup_tx.send((Instant::now(), message_id)).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        lagged.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        let pending = delivery_pending.clone();
        let samples = delivery_samples.clone();
        let unmatched = delivery_unmatched.clone();
        let missing = delivery_message_missing.clone();
        let message_errors = delivery_message_errors.clone();
        let lookup_worker = tokio::spawn(async move {
            const MAX_LOOKUPS: usize = 256;
            let mut lookups = FuturesUnordered::new();
            let mut receiver_closed = false;
            loop {
                tokio::select! {
                    item = lookup_rx.recv(), if !receiver_closed && lookups.len() < MAX_LOOKUPS => {
                        match item {
                            Some((received_at, message_id)) => {
                            let recipient = recipient.clone();
                            lookups.push(async move {
                                (
                                    received_at,
                                    message_id,
                                    recipient.get_message_by_id(message_id).await,
                                )
                            });
                            }
                            None => receiver_closed = true,
                        }
                    }
                    Some((received_at, message_id, result)) = lookups.next(), if !lookups.is_empty() => {
                        let message = match result {
                            Ok(Some(message)) => message,
                            Ok(None) => {
                                if missing.fetch_add(1, Ordering::Relaxed) < 3 {
                                    warn!(
                                        "[soak] timeline event references missing local message id={message_id}"
                                    );
                                }
                                continue;
                            }
                            Err(error) => {
                                if message_errors.fetch_add(1, Ordering::Relaxed) < 3 {
                                    warn!(
                                        "[soak] timeline message lookup failed id={message_id}: {error}"
                                    );
                                }
                                continue;
                            }
                        };
                        let raw_content = message.content;
                        let correlation = delivery_correlation_content(&raw_content);
                        let sent = pending
                            .lock()
                            .ok()
                            .and_then(|mut map| map.remove(&correlation));
                        if let Some((started, t)) = sent {
                            if let Ok(mut samples) = samples.lock() {
                                samples.push((
                                    t,
                                    received_at
                                        .saturating_duration_since(started)
                                        .as_micros() as u64,
                                ));
                            }
                        } else if unmatched.fetch_add(1, Ordering::Relaxed) < 3 {
                            warn!(
                                "[soak] unmatched timeline content raw={:?} correlation={:?}",
                                raw_content, correlation
                            );
                        }
                    }
                    else => {
                        if receiver_closed && lookups.is_empty() {
                            break;
                        }
                    }
                }
            }
        });
        Some((event_receiver, lookup_worker))
    } else {
        None
    };

    // === 真 token 重连探针（§3.1 重连成功率）===
    // 建**一个已认证** probe（独立 device_id/dir，不踢保活连接）。每轮：要求 disconnect() 成功且确认
    // transport 已断（!is_connected）→ connect()（原 token 自动重连）→ 断言 Authenticated →
    // **再跑一次真认证 RPC（submit 到群）**确认会话真恢复（不只信内存 ConnectionState）。任一步失败该轮不计成功。
    let reconn_ok = Arc::new(AtomicU64::new(0));
    let reconn_att = Arc::new(AtomicU64::new(0));
    let probe_init_failed = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let mut probe = {
        let mut r = records[0].clone();
        r.device_id = uuid::Uuid::new_v4().to_string();
        let dd = data_dir.to_path_buf();
        let host = server_host.to_string();
        let proto = server_protocol.to_string();
        let gid = group_id;
        let (ok, att, st, initf) = (
            reconn_ok.clone(),
            reconn_att.clone(),
            stop.clone(),
            probe_init_failed.clone(),
        );
        tokio::spawn(async move {
            let sdk =
                match connect_login_auth(&r, &dd, "soak_probe", &host, server_port, &proto).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("[soak] reconnect probe initial auth failed: {e}");
                        initf.store(true, Ordering::Relaxed);
                        return;
                    }
                };
            let mut tick = interval(Duration::from_secs(30));
            tick.tick().await;
            while !st.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = async {
                        while !st.load(Ordering::Relaxed) {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    } => break,
                }
                if st.load(Ordering::Relaxed) {
                    break;
                }
                att.fetch_add(1, Ordering::Relaxed);
                // 1) 必须真断开
                if sdk.disconnect().await.is_err() || sdk.is_connected().await.unwrap_or(true) {
                    warn!("[soak] reconnect: disconnect 未成功/未断开，不计成功");
                    continue;
                }
                // 2) 原 token 重连 + Authenticated
                if let Err(e) = sdk.connect().await {
                    warn!("[soak] reconnect connect failed: {e}");
                    continue;
                }
                if !matches!(
                    sdk.connection_state().await,
                    Ok(privchat_sdk::ConnectionState::Authenticated)
                ) {
                    warn!("[soak] reconnect state != Authenticated");
                    continue;
                }
                // 3) 恢复后跑真认证 RPC（submit 到群），成功才算真重连成功。
                let probe_req = ClientSubmitRequest {
                    local_message_id: next_local_message_id(),
                    channel_id: gid,
                    channel_type: 2,
                    last_pts: 0,
                    command_type: "send_message".to_string(),
                    payload: serde_json::to_value(SubmitMessagePayload {
                        content: "soak_reconnect_probe".to_string(),
                        message_type: 0,
                    })
                    .expect("typed reconnect payload must serialize"),
                    client_timestamp: chrono::Utc::now().timestamp_millis(),
                    device_id: None,
                };
                match rpc_call_typed::<_, ClientSubmitResponse>(
                    &sdk,
                    routes::sync::SUBMIT,
                    &probe_req,
                )
                .await
                {
                    Ok(_) => {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => warn!("[soak] reconnect post-auth RPC failed: {e}"),
                }
            }
            sdk.shutdown().await;
        })
    };

    // === 3. 发流量（timed submit → delivery 延迟）+ 采样 ===
    let send_interval_ms = (1000 / msg_per_sec as u64).max(1);
    let sample_interval_sec = (duration_sec / 30).clamp(2, 30);
    let start = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut invalid_sample = false;
    let mut deliver: Vec<(f64, u64)> = Vec::new(); // (t, latency_us)
    let mut send_tick = interval(Duration::from_millis(send_interval_ms));
    // 不 burst 追赶：submit 偶发变慢时跳过错过的 tick，避免排队延迟污染 delivery p99；
    // 若因此持续达不到 msg_per_sec，90% 发送量断言会正确失败（负载不足信号）。
    send_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sample_tick = interval(Duration::from_secs(sample_interval_sec));
    sample_tick.tick().await;
    let mut sent = 0u64;
    let mut steady_sent = 0u64;
    let mut send_err = 0u64;
    let mut send_sequence = 0u64;
    let mut submit_inflight = FuturesUnordered::new();
    let observed_pts = Arc::new(AtomicU64::new(0));
    // 稳态 RPC 计数基线：在 warmup 结束后的**第一个**采样点捕获（不含连接/登录/建群初始化期）。
    let mut steady_rpc_base: Option<(f64, f64)> = None;
    loop {
        let accepting_submits = start.elapsed().as_secs() < duration_sec;
        if !accepting_submits && submit_inflight.is_empty() {
            break;
        }
        tokio::select! {
            _ = send_tick.tick(), if accepting_submits && submit_inflight.len() < 64 => {
                let t = start.elapsed().as_secs_f64();
                let content = format!("soak_{send_sequence}");
                if let Ok(mut pending) = delivery_pending.lock() {
                    pending.insert(content.clone(), (Instant::now(), t));
                }
                let req = ClientSubmitRequest {
                    local_message_id: next_local_message_id(),
                    channel_id: group_id,
                    channel_type: 2,
                    last_pts: observed_pts.load(Ordering::Relaxed),
                    command_type: "send_message".to_string(),
                    payload: serde_json::to_value(SubmitMessagePayload {
                        content: content.clone(),
                        message_type: 0,
                    })
                    .expect("typed soak payload must serialize"),
                    client_timestamp: chrono::Utc::now().timestamp_millis(),
                    device_id: None,
                };
                // The cross-node gate is directional by design: the primary-node
                // client sends and the secondary-node client is the independent
                // realtime recipient. Keeping the recipient read-only avoids
                // coupling delivery measurement to its local SQLite lookup load.
                let sender_index = if cross_node_mode {
                    0
                } else {
                    send_sequence as usize % sdks.len()
                };
                let sdk = sdks[sender_index].clone();
                send_sequence += 1;
                submit_inflight.push(async move {
                    let started = Instant::now();
                    let result = tokio::time::timeout(
                        Duration::from_secs(10),
                        rpc_call_typed::<_, ClientSubmitResponse>(
                            &sdk,
                            routes::sync::SUBMIT,
                            &req,
                        ),
                    )
                    .await;
                    (content, t, started.elapsed().as_micros() as u64, result)
                });
            }
            Some((content, t, ack_latency_us, result)) = submit_inflight.next(), if !submit_inflight.is_empty() => {
                match result {
                    Ok(Ok(response)) => {
                        observed_pts.fetch_max(response.current_pts, Ordering::Relaxed);
                        sent += 1;
                        if t >= warmup_sec as f64 {
                            steady_sent += 1;
                        }
                        deliver.push((t, ack_latency_us));
                    }
                    Ok(Err(_)) | Err(_) => {
                        send_err += 1;
                        if let Ok(mut pending) = delivery_pending.lock() {
                            pending.remove(&content);
                        }
                    }
                }
            }
            _ = sample_tick.tick(), if accepting_submits => {
                let t = start.elapsed().as_secs_f64();
                let rss = rss_mb_of(&pid);
                let fd = server_fd_count(&pid);
                let sm = fetch_srv_metrics(&murl).await;
                let tasks = sm.map(|m| m.tokio_tasks).unwrap_or(0.0);
                let rpc_ok = sm.map(|m| m.rpc_found).unwrap_or(false);
                if rss <= 0.0 || fd.is_none() || tasks <= 0.0 || !rpc_ok {
                    invalid_sample = true;
                    warn!("[soak] t={:.0}s INVALID sample rss={:.1} fd={:?} tasks={} rpc_found={}", t, rss, fd, tasks, rpc_ok);
                }
                if steady_rpc_base.is_none() && t >= warmup_sec as f64 {
                    if let Some(m) = sm { steady_rpc_base = Some((m.rpc_ok, m.rpc_error)); }
                }
                samples.push((t, rss, fd, tasks));
                info!("[soak] t={:.0}s rss={:.1}MB fd={:?} tasks={} sent={} err={}", t, rss, fd, tasks, sent, send_err);
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    if tokio::time::timeout(Duration::from_secs(10), &mut probe)
        .await
        .is_err()
    {
        warn!("[soak] reconnect probe shutdown timed out; aborting probe task");
        probe.abort();
        let _ = probe.await;
    }
    // Drain event-to-message correlation without blocking the broadcast
    // receiver. A long run can have a small SQLite lookup backlog even though
    // TimelineUpdated arrived on time; cap the drain so a real loss still
    // fails instead of hanging the gate.
    for _ in 0..300 {
        if delivery_pending
            .lock()
            .map(|pending| pending.is_empty())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if let Some((event_receiver, lookup_worker)) = delivery_listener {
        event_receiver.abort();
        lookup_worker.abort();
        let _ = event_receiver.await;
        let _ = lookup_worker.await;
    }
    let m_end = fetch_srv_metrics(&murl).await;

    report.assert_lte(
        "soak_sampling_valid",
        if invalid_sample { 1.0 } else { 0.0 },
        0.0,
    );

    // 发送量 90% + 错误率
    let expected = (msg_per_sec as u64).saturating_mul(duration_sec);
    report.assert_gte(
        "soak_messages_sent_90pct",
        sent as f64,
        (expected as f64 * 0.9).max(1.0),
    );
    let total_send = sent + send_err;
    let send_err_rate = if total_send > 0 {
        send_err as f64 / total_send as f64
    } else {
        1.0
    };
    report.assert_lte("soak_send_error_rate", send_err_rate, 0.001);

    // === 4. 稳态段（丢 warmup）分析 ===
    let steady: Vec<&Sample> = samples
        .iter()
        .filter(|(t, ..)| *t >= warmup_sec as f64)
        .collect();
    let need_min = if matches!(profile, SoakProfile::Dev | SoakProfile::CrossNode) {
        3
    } else {
        6
    };
    if steady.len() < need_min {
        report.fail(
            "soak_steady_samples",
            &format!("稳态样本 {} < {}", steady.len(), need_min),
        );
        for sdk in &sdks {
            sdk.shutdown().await;
        }
        return Ok(SoakResult { report });
    }
    let mut rss_net = 0.0;
    let mut tail_slope = 0.0;
    let mut task_net_pct = 0.0;
    if profile == SoakProfile::CrossNode {
        // Startup allocation and maintenance-task scheduling dominate a 60s
        // directional cross-node run. Leak assertions require the dedicated
        // 1h/4h profiles, while this profile owns routing correctness only.
        report.skip("soak_rss_net_growth_mb", "cross_node uses gate_1h/gate_4h");
        report.skip(
            "soak_rss_tail_slope_mb_per_min",
            "cross_node uses gate_1h/gate_4h",
        );
        report.skip("soak_fd_net_growth", "cross_node uses gate_1h/gate_4h");
        report.skip("soak_tokio_task_net_pct", "cross_node uses gate_1h/gate_4h");
    } else {
        let s_ts: Vec<f64> = steady.iter().map(|s| s.0).collect();
        let s_rss: Vec<f64> = steady.iter().map(|s| s.1).collect();

        // RSS 净增长 + 末段趋平（§3.1「末 30min」：稳态跨度≥30min 即取固定末 30min，否则末 ½）
        rss_net = net_delta(&s_rss);
        report.assert_lte("soak_rss_net_growth_mb", rss_net, profile.rss_net_max_mb());
        let span = s_ts.last().unwrap() - s_ts.first().unwrap();
        let tail_window = if span >= 1800.0 { 1800.0 } else { span / 2.0 };
        let tail_start = s_ts.last().unwrap() - tail_window;
        let tail_ts: Vec<f64> = s_ts.iter().cloned().filter(|t| *t >= tail_start).collect();
        let tail_rss: Vec<f64> = steady
            .iter()
            .filter(|s| s.0 >= tail_start)
            .map(|s| s.1)
            .collect();
        tail_slope = linreg_slope(&tail_ts, &tail_rss) * 60.0;
        report.assert_lte(
            "soak_rss_tail_slope_mb_per_min",
            tail_slope,
            profile.rss_tail_slope_max(),
        );

        // FD 净增长
        let s_fd: Vec<f64> = steady
            .iter()
            .filter_map(|s| s.2.map(|v| v as f64))
            .collect();
        if s_fd.len() >= need_min {
            report.assert_lte("soak_fd_net_growth", net_delta(&s_fd), profile.fd_net_max());
        } else {
            report.fail("soak_fd_net_growth", "稳态 FD 样本不足");
        }

        // tokio task 净增 < 5% 基线
        let s_tasks: Vec<f64> = steady.iter().map(|s| s.3).collect();
        let task_base = s_tasks.first().copied().unwrap_or(0.0).max(1.0);
        task_net_pct = net_delta(&s_tasks) / task_base;
        report.assert_lte(
            "soak_tokio_task_net_pct",
            task_net_pct,
            profile.tokio_task_net_pct_max(),
        );
    }

    // RPC 真错误率（**稳态段** Δerror/(Δok+Δerror)，作用域=RPC dispatch，code!=0 计 error）。
    // 基线取 warmup 结束点（steady_rpc_base），排除连接/登录/建群初始化期。
    match (m_end, steady_rpc_base) {
        (Some(me), Some((base_ok, base_err))) if me.rpc_found => {
            let d_ok = (me.rpc_ok - base_ok).max(0.0);
            let d_err = (me.rpc_error - base_err).max(0.0);
            let d_total = d_ok + d_err;
            info!("[soak] rpc steady: Δok={:.0} Δerror={:.0}", d_ok, d_err);
            let _ = d_total;
            // 请求增量必须与稳态发送量匹配，否则=指标没在动/采样失效 → 硬失败（防缺失假绿）。
            let min_expected = (steady_sent as f64 * 0.9).max(10.0);
            match steady_rpc_error_rate(d_ok, d_err, min_expected) {
                Ok(rate) => report.assert_lte("soak_rpc_error_rate", rate, 0.001),
                Err(()) => report.fail(
                    "soak_rpc_requests_delta",
                    &format!(
                        "稳态 RPC 增量 {:.0} < 预期 {min_expected:.0}（指标未动?）",
                        d_ok + d_err
                    ),
                ),
            }
        }
        (Some(me), _) if !me.rpc_found => {
            report.fail("soak_rpc_error_rate", "结束 /metrics 缺 rpc_requests_total")
        }
        (Some(_), None) => report.fail(
            "soak_rpc_error_rate",
            "未捕获 warmup-end RPC 基线（稳态样本不足）",
        ),
        _ => report.fail("soak_rpc_error_rate", "结束拉不到 /metrics"),
    }

    // submit ACK RTT p99 末段较首段劣化 < 20%。
    // ⚠️ 这是 sender `sync/submit` 往返（server 收到并 ACK），**不是端到端 delivery**（不含 fanout
    // 到接收端 + 事件到达）。§3.1 的真 delivery p99 待实现（recipient 用 server_msg_id↔
    // TimelineUpdated.message_id 关联 + 丢投递计失败）。故此处名为 submit_ack，不冒充 delivery。
    let post_deliver: Vec<&(f64, u64)> = deliver
        .iter()
        .filter(|(t, _)| *t >= warmup_sec as f64)
        .collect();
    let dn = post_deliver.len();
    if p99_gate_eligible(dn) {
        let half = dn / 2;
        let mut h1: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
        let mut h2: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
        for (i, (_, us)) in post_deliver.iter().enumerate() {
            let _ = if i < half {
                h1.record(*us)
            } else {
                h2.record(*us)
            };
        }
        let p1 = h1.value_at_quantile(0.99) as f64;
        let p2 = h2.value_at_quantile(0.99) as f64;
        let degr = if p1 > 0.0 { (p2 - p1) / p1 } else { 0.0 };
        info!(
            "[soak] submit_ack p99: first={:.0}us last={:.0}us degradation={:.1}% (n={})",
            p1,
            p2,
            degr * 100.0,
            dn
        );
        report.assert_lte("soak_submit_ack_p99_degradation", degr, 0.20);
    } else {
        report.skip(
            "soak_submit_ack_p99_degradation",
            &format!("n={dn} < 10000，仅记录不判 p99"),
        );
    }
    // True end-to-end delivery: sender submit start -> recipient local
    // TimelineUpdated. Missing events count against delivery rate, and a
    // broadcast lag is a hard failure rather than a skipped sample.
    let e2e = delivery_samples
        .lock()
        .map(|v| v.clone())
        .unwrap_or_default();
    let post_e2e: Vec<&(f64, u64)> = e2e
        .iter()
        .filter(|(t, _)| *t >= warmup_sec as f64)
        .collect();
    let expected_e2e = post_deliver.len();
    let delivery_rate = if expected_e2e == 0 {
        0.0
    } else {
        post_e2e.len() as f64 / expected_e2e as f64
    };
    info!(
        "[soak] delivery correlation: timeline_events={} matched={} expected={} pending={} missing={} lookup_errors={} unmatched={}",
        delivery_events_seen.load(Ordering::Relaxed),
        post_e2e.len(),
        expected_e2e,
        delivery_pending.lock().map(|map| map.len()).unwrap_or(0),
        delivery_message_missing.load(Ordering::Relaxed),
        delivery_message_errors.load(Ordering::Relaxed),
        delivery_unmatched.load(Ordering::Relaxed)
    );
    report.assert_gte("soak_delivery_rate", delivery_rate, 0.99);
    report.assert_lte(
        "soak_delivery_event_lagged",
        delivery_lagged.load(Ordering::Relaxed) as f64,
        0.0,
    );
    if post_e2e.len() >= 20 {
        let half = post_e2e.len() / 2;
        let mut all = Histogram::<u64>::new_with_max(60_000_000, 3).unwrap();
        let mut first = Histogram::<u64>::new_with_max(60_000_000, 3).unwrap();
        let mut last = Histogram::<u64>::new_with_max(60_000_000, 3).unwrap();
        for (index, (_, latency_us)) in post_e2e.iter().enumerate() {
            let _ = all.record(*latency_us);
            let _ = if index < half {
                first.record(*latency_us)
            } else {
                last.record(*latency_us)
            };
        }
        let p99_us = all.value_at_quantile(0.99) as f64;
        let first_p99 = first.value_at_quantile(0.99) as f64;
        let last_p99 = last.value_at_quantile(0.99) as f64;
        let degradation = if first_p99 > 0.0 {
            (last_p99 - first_p99) / first_p99
        } else {
            0.0
        };
        info!(
            "[soak] delivery e2e: received={}/{} rate={:.2}% p99={:.2}ms degradation={:.1}% lagged={}",
            post_e2e.len(),
            expected_e2e,
            delivery_rate * 100.0,
            p99_us / 1000.0,
            degradation * 100.0,
            delivery_lagged.load(Ordering::Relaxed)
        );
        // MESSAGE_SPEC's 200ms target starts after commit and ends at the
        // transport/offline handoff. This client-observed value starts before
        // submit, so its matching healthy-path budget is 2s. Per the release
        // review checklist, p99 is only a gate with at least 10,000 samples.
        if p99_gate_eligible(post_e2e.len()) {
            report.assert_lte("soak_delivery_p99_ms", p99_us / 1000.0, 2_000.0);
            report.assert_lte("soak_delivery_p99_degradation", degradation, 0.20);
        } else {
            report.skip(
                "soak_delivery_p99_ms",
                &format!(
                    "observed={:.2}ms, n={} < 10000",
                    p99_us / 1000.0,
                    post_e2e.len()
                ),
            );
            report.skip(
                "soak_delivery_p99_degradation",
                &format!(
                    "observed={:.1}%, n={} < 10000",
                    degradation * 100.0,
                    post_e2e.len()
                ),
            );
        }
    } else {
        report.fail(
            "soak_delivery_p99_end_to_end",
            &format!("端到端 delivery 样本不足: {}", post_e2e.len()),
        );
    }

    // 周期重连成功率 > 99%（三条假绿路径显式化，见 reconnect_verdict）
    let att = reconn_att.load(Ordering::Relaxed);
    let ok = reconn_ok.load(Ordering::Relaxed);
    info!("[soak] reconnect probe: {}/{} ok", ok, att);
    match reconnect_verdict(
        probe_init_failed.load(Ordering::Relaxed),
        att,
        ok,
        profile == SoakProfile::Dev,
    ) {
        ReconnectVerdict::InitFail => {
            report.fail("soak_reconnect_probe_init", "重连 probe 初始认证失败")
        }
        ReconnectVerdict::ZeroAttemptFail => report.fail(
            "soak_reconnect_success_rate",
            "smoke/gate 期间 0 次重连尝试（异常）",
        ),
        ReconnectVerdict::SkipDev => {
            report.skip("soak_reconnect_success_rate", "dev duration 太短无重连尝试")
        }
        ReconnectVerdict::Rate(rate) => {
            report.assert_gte("soak_reconnect_success_rate", rate, 0.99)
        }
    }

    // 连接存活
    let mut alive = 0u64;
    for sdk in &sdks {
        if sdk.is_connected().await.unwrap_or(false) {
            alive += 1;
        }
    }
    report.assert_gte(
        "soak_connections_survived",
        alive as f64 / conn_n as f64,
        0.90,
    );
    info!(
        "[soak] alive {}/{}, rss_net={:.1}MB tail_slope={:.2} task_net_pct={:.3}",
        alive, conn_n, rss_net, tail_slope, task_net_pct
    );

    for sdk in &sdks {
        sdk.shutdown().await;
    }
    Ok(SoakResult { report })
}

#[cfg(test)]
mod tests {
    use super::{
        delivery_correlation_content, linreg_slope, net_delta, p99_gate_eligible, SoakProfile,
    };

    #[test]
    fn delivery_correlation_supports_typed_and_legacy_content() {
        assert_eq!(delivery_correlation_content("soak_42"), "soak_42");
        assert_eq!(
            delivery_correlation_content(r#"{"content":"soak_43","message_type":0}"#),
            "soak_43"
        );
    }

    #[test]
    fn p99_requires_review_checklist_sample_floor() {
        assert!(!p99_gate_eligible(9_999));
        assert!(p99_gate_eligible(10_000));
    }

    #[test]
    fn profile_parse_fail_fast() {
        assert_eq!(SoakProfile::parse("").unwrap(), SoakProfile::Dev);
        assert_eq!(SoakProfile::parse("smoke").unwrap(), SoakProfile::Smoke);
        assert_eq!(SoakProfile::parse("gate").unwrap(), SoakProfile::Gate1h);
        assert_eq!(SoakProfile::parse("GATE_4H").unwrap(), SoakProfile::Gate4h);
        assert!(SoakProfile::parse("gatee").is_err());
    }

    #[test]
    fn slope_flat_line_leak_noise() {
        assert!(linreg_slope(&[0.0, 1.0, 2.0], &[5.0, 5.0, 5.0]).abs() < 1e-9);
        assert!((linreg_slope(&[0.0, 1.0, 2.0, 3.0], &[5.0, 7.0, 9.0, 11.0]) - 2.0).abs() < 1e-9);
        assert!(linreg_slope(&[0.0, 2.0, 4.0, 6.0], &[100.0, 105.0, 112.0, 121.0]) > 0.0);
        assert!(linreg_slope(&[0.0, 2.0, 4.0, 6.0], &[50.0, 50.3, 49.8, 50.1]).abs() < 0.1);
    }

    #[test]
    fn slope_degenerate_zero() {
        assert_eq!(linreg_slope(&[], &[]), 0.0);
        assert_eq!(linreg_slope(&[1.0], &[2.0]), 0.0);
        assert_eq!(linreg_slope(&[3.0, 3.0], &[1.0, 9.0]), 0.0);
    }

    #[test]
    fn net_delta_growth_flat() {
        assert!(net_delta(&[100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0]) > 50.0);
        assert!(net_delta(&[50.0, 50.4, 49.8, 50.1, 49.9, 50.2, 50.0, 49.7]).abs() < 2.0);
        assert_eq!(net_delta(&[42.0]), 0.0);
    }

    // 防假绿分支：RPC 错误率
    #[test]
    fn steady_rpc_error_rate_branches() {
        use super::steady_rpc_error_rate;
        // 零增量/不足 → Err（指标未动/缺失，不能假绿）
        assert!(steady_rpc_error_rate(0.0, 0.0, 10.0).is_err());
        assert!(steady_rpc_error_rate(3.0, 0.0, 10.0).is_err());
        // 足够增量 + 全 ok → 0 错误率
        assert_eq!(steady_rpc_error_rate(100.0, 0.0, 10.0), Ok(0.0));
        // 有错误 → 正确比率
        let r = steady_rpc_error_rate(90.0, 10.0, 10.0).unwrap();
        assert!((r - 0.1).abs() < 1e-9);
    }

    // 防假绿分支：重连判定
    #[test]
    fn reconnect_verdict_branches() {
        use super::{reconnect_verdict, ReconnectVerdict};
        // init 失败 → InitFail（不可当通过）
        assert_eq!(
            reconnect_verdict(true, 0, 0, false),
            ReconnectVerdict::InitFail
        );
        assert_eq!(
            reconnect_verdict(true, 5, 5, true),
            ReconnectVerdict::InitFail
        );
        // att==0：非 dev → 硬失败；dev → skip
        assert_eq!(
            reconnect_verdict(false, 0, 0, false),
            ReconnectVerdict::ZeroAttemptFail
        );
        assert_eq!(
            reconnect_verdict(false, 0, 0, true),
            ReconnectVerdict::SkipDev
        );
        // 正常比率
        assert_eq!(
            reconnect_verdict(false, 10, 10, false),
            ReconnectVerdict::Rate(1.0)
        );
        assert_eq!(
            reconnect_verdict(false, 10, 9, false),
            ReconnectVerdict::Rate(0.9)
        );
    }
}
