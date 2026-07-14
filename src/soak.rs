//! 场景 G10：soak —— 长时持续负载下检测 **server** 资源无持续单调上涨（泄漏）。
//!
//! LOAD_GATE_EXECUTION_SPEC §2.4 / PRIVCHAT_SERVER_REMEDIATION_TASKS §3.1。
//!
//! **防假绿**：唯一 msgid（跨进程不撞 server 幂等去重）；**所有 profile 强制有效 server PID**，
//! 采样期任一次 RSS/FD 失效即 FAIL；`LOAD_TEST_SOAK_PROFILE` 枚举解析，未知值 fail-fast，并按
//! profile 校验最短 duration/warmup；按 §3.1 真实阈值判 **RSS 净增长 + 末段趋平 + FD 净增长**（不再
//! 用宽松 MB/min）；建群/成员/发送量/错误率硬断言（无流量不 PASS）。
//!
//! **实现现状（诚实）**：已覆盖 §3.1 的 RSS/FD 净增长+趋平 + 流量/错误率 + 连接存活。**仍缺**：
//! tokio task 数、delivery p99、周期重连成功率、handler 错误率(/metrics) —— 见 REMEDIATION §3.1。
//! **未跑规定 15min smoke / 1h·4h gate**（需隔离 tier）；co-located 短跑仅 runner verification。

use crate::assertions::AssertionReport;
use crate::scenario::{rpc_call_fire, rpc_call_typed};
use crate::user_store::{server_endpoints, UserRecord};
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::ClientSubmitRequest;
use privchat_sdk::{PrivchatConfig, PrivchatSdk};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{info, warn};

/// soak profile：决定最短 duration/warmup 与 §3.1 阈值。未知值 fail-fast。
/// 未设 → `Dev`（runner 接线验证，**非 gate/smoke**，无 duration 下限，仍全硬断言）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SoakProfile {
    Dev,
    Smoke,
    Gate1h,
    Gate4h,
}

impl SoakProfile {
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "dev" => Ok(Self::Dev),
            "smoke" => Ok(Self::Smoke),
            "gate" | "gate_1h" | "gate1h" => Ok(Self::Gate1h),
            "gate_4h" | "gate4h" => Ok(Self::Gate4h),
            other => Err(format!(
                "unknown LOAD_TEST_SOAK_PROFILE {other:?} — expected dev|smoke|gate_1h|gate_4h"
            )),
        }
    }
    fn min_duration_sec(&self) -> u64 {
        match self {
            Self::Dev => 0,
            Self::Smoke => 900,
            Self::Gate1h => 3600,
            Self::Gate4h => 14400,
        }
    }
    fn min_warmup_sec(&self) -> u64 {
        match self {
            Self::Dev => 0,
            _ => 600, // §3.1：丢弃前 10min warmup
        }
    }
    /// §3.1 RSS 净增长上限（MB）。smoke 按比例收紧，dev 仅抓明显泄漏。
    fn rss_net_max_mb(&self) -> f64 {
        match self {
            Self::Dev => 30.0,
            Self::Smoke => 40.0,
            Self::Gate1h => 100.0,
            Self::Gate4h => 200.0,
        }
    }
    /// 末段（趋平）斜率上限（MB/min）——非单调上涨。
    fn rss_tail_slope_max(&self) -> f64 {
        match self {
            Self::Dev => 3.0,
            Self::Smoke => 2.0,
            _ => 1.0,
        }
    }
    /// §3.1 FD 净增上限。
    fn fd_net_max(&self) -> f64 {
        match self {
            Self::Dev => 32.0,
            Self::Smoke => 40.0,
            _ => 50.0,
        }
    }
    fn label(&self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Smoke => "smoke",
            Self::Gate1h => "gate_1h",
            Self::Gate4h => "gate_4h",
        }
    }
}

/// 唯一 message id：进程启动以毫秒时间戳播种，避免复用账号撞 server 幂等键去重。
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

/// RSS 快照（MB）。读失败/无效 PID → 0.0（调用方据此判无效）。
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

/// server 打开的 FD 数（lsof）。失败 → None。
fn server_fd_count(pid: &str) -> Option<u64> {
    let out = std::process::Command::new("lsof")
        .args(["-p", pid])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let n = String::from_utf8_lossy(&out.stdout).lines().count() as u64;
    Some(n.saturating_sub(1)) // 去表头
}

/// 最小二乘斜率 dy/dx（纯函数、可测）。
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

/// 净增长 = 末 k 个均值 − 首 k 个均值（纯函数、可测）。k 自适应 = clamp(len/4, 1, 5)。
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

pub struct SoakResult {
    pub report: AssertionReport,
}

async fn connect_login_auth(
    record: &UserRecord,
    data_dir: &Path,
    host: &str,
    port: u16,
    protocol: &str,
) -> Result<Arc<PrivchatSdk>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = server_endpoints(host, port, protocol);
    let dir = data_dir.join("soak").join(&record.username);
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

/// 采样点：(经过秒, rss_mb, fd)。
type Sample = (f64, f64, Option<u64>);

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
) -> Result<SoakResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let _ = std::fs::remove_dir_all(data_dir.join("soak"));

    // === P1-3：profile 枚举解析，未知 fail-fast ===
    let profile = match SoakProfile::parse(
        &std::env::var("LOAD_TEST_SOAK_PROFILE").unwrap_or_default(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[soak] FATAL: {e}");
            std::process::exit(1);
        }
    };
    // 按 profile 校验最短 duration/warmup（防用 60s 冒充 15min smoke / 1h gate）。
    if duration_sec < profile.min_duration_sec() || warmup_sec < profile.min_warmup_sec() {
        eprintln!(
            "[soak] FATAL: profile={} 要求 duration>={}s warmup>={}s，实际 duration={}s warmup={}s",
            profile.label(),
            profile.min_duration_sec(),
            profile.min_warmup_sec(),
            duration_sec,
            warmup_sec
        );
        std::process::exit(1);
    }

    // === P1-2：所有 profile 强制有效 server PID（G10 测 server，不测 load 自身）===
    let pid = match std::env::var("LOAD_TEST_SERVER_PID")
        .ok()
        .filter(|s| !s.trim().is_empty())
    {
        Some(p) => p,
        None => {
            report.fail("soak_server_pid_set", "G10 必须设 LOAD_TEST_SERVER_PID（测 server 泄漏）");
            return Ok(SoakResult { report });
        }
    };
    let rss0 = rss_mb_of(&pid);
    let fd0 = server_fd_count(&pid);
    info!(
        "[soak] profile={} duration={}s warmup={}s msg_per_sec={} users={} server_pid={} rss0={:.1}MB fd0={:?}",
        profile.label(), duration_sec, warmup_sec, msg_per_sec, records.len(), pid, rss0, fd0
    );
    if rss0 <= 0.0 {
        report.fail("soak_server_pid_valid", "LOAD_TEST_SERVER_PID RSS 读不到=无效 PID");
        return Ok(SoakResult { report });
    }
    if fd0.is_none() {
        report.fail("soak_server_fd_available", "server FD 采样(lsof)不可用");
        return Ok(SoakResult { report });
    }

    // === 1. 建 N 个认证连接 ===
    let mut sdks: Vec<Arc<PrivchatSdk>> = Vec::new();
    let mut user_ids: Vec<u64> = Vec::new();
    for rec in records {
        match connect_login_auth(rec, data_dir, server_host, server_port, server_protocol).await {
            Ok(sdk) => {
                sdks.push(sdk);
                user_ids.push(rec.user_id);
            }
            Err(e) => warn!("[soak] connect {} failed: {}", rec.username, e),
        }
    }
    let conn_n = sdks.len();
    info!("[soak] {}/{} connections established", conn_n, records.len());
    report.assert_gte(
        "soak_connect_rate",
        conn_n as f64 / records.len().max(1) as f64,
        0.90,
    );
    if sdks.is_empty() {
        report.fail("soak_no_connections", "0 个连接建立成功");
        return Ok(SoakResult { report });
    }

    // === 2. 建群（必须成功）+ 成员加入率 ===
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
    info!("[soak] group {} created, {}/{} joined", group_id, joined, user_ids.len());
    report.assert_gte(
        "soak_member_join_rate",
        joined as f64 / user_ids.len().max(1) as f64,
        0.90,
    );

    // === 3. 发流量（尊重 msg_per_sec）+ 采样；采样失效即记 invalid ===
    let send_interval_ms = (1000 / msg_per_sec.max(1) as u64).max(1);
    let sample_interval_sec = (duration_sec / 30).clamp(2, 30);
    let start = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut invalid_sample = false;
    let mut send_tick = interval(Duration::from_millis(send_interval_ms));
    let mut sample_tick = interval(Duration::from_secs(sample_interval_sec));
    sample_tick.tick().await;
    let mut sent = 0u64;
    let mut send_err = 0u64;
    loop {
        tokio::select! {
            _ = send_tick.tick() => {
                let req = ClientSubmitRequest {
                    local_message_id: next_local_message_id(),
                    channel_id: group_id,
                    channel_type: 2,
                    last_pts: 0,
                    command_type: "send_message".to_string(),
                    payload: serde_json::json!({"content": format!("soak_{sent}"), "message_type": 0}),
                    client_timestamp: chrono::Utc::now().timestamp_millis(),
                    device_id: None,
                };
                match rpc_call_fire(&main_sdk, routes::sync::SUBMIT, &req).await {
                    Ok(_) => sent += 1,
                    Err(_) => send_err += 1,
                }
            }
            _ = sample_tick.tick() => {
                let t = start.elapsed().as_secs_f64();
                let rss = rss_mb_of(&pid);
                let fd = server_fd_count(&pid);
                if rss <= 0.0 || fd.is_none() {
                    invalid_sample = true;
                    warn!("[soak] t={:.0}s INVALID sample rss={:.1} fd={:?}", t, rss, fd);
                }
                samples.push((t, rss, fd));
                info!("[soak] t={:.0}s rss={:.1}MB fd={:?} sent={} send_err={}", t, rss, fd, sent, send_err);
            }
        }
        if start.elapsed().as_secs() >= duration_sec {
            break;
        }
    }

    // P1-2：采样期任一 RSS/FD 失效 → FAIL（不容忍 server 消失/采样坏）。
    report.assert_lte("soak_sampling_valid", if invalid_sample { 1.0 } else { 0.0 }, 0.0);

    // === P0-3：发送量 + 错误率 ===
    let expected = (msg_per_sec as u64).saturating_mul(duration_sec);
    report.assert_gte("soak_messages_sent", sent as f64, (expected as f64 * 0.5).max(1.0));
    let total_send = sent + send_err;
    let send_err_rate = if total_send > 0 { send_err as f64 / total_send as f64 } else { 1.0 };
    report.assert_lte("soak_send_error_rate", send_err_rate, 0.001);

    // === 4. §3.1：稳态段（丢 warmup）RSS/FD 净增长 + 末段趋平 ===
    let steady: Vec<&Sample> = samples.iter().filter(|(t, ..)| *t >= warmup_sec as f64).collect();
    let need_min = if profile == SoakProfile::Dev { 3 } else { 6 };
    if steady.len() < need_min {
        report.fail(
            "soak_steady_samples",
            &format!("稳态样本 {} < {}（拉长 duration 或减 warmup）", steady.len(), need_min),
        );
        for sdk in &sdks { sdk.shutdown().await; }
        return Ok(SoakResult { report });
    }
    let steady_ts: Vec<f64> = steady.iter().map(|s| s.0).collect();
    let steady_rss: Vec<f64> = steady.iter().map(|s| s.1).collect();

    // RSS 净增长（§3.1 硬阈值）
    let rss_net = net_delta(&steady_rss);
    report.assert_lte("soak_rss_net_growth_mb", rss_net, profile.rss_net_max_mb());

    // RSS 末段趋平（非单调上涨）：末段窗口 = min(1800s, 稳态跨度/2)
    let steady_span = steady_ts.last().unwrap() - steady_ts.first().unwrap();
    let tail_window = (steady_span / 2.0).min(1800.0);
    let tail_start_t = steady_ts.last().unwrap() - tail_window;
    let tail_ts: Vec<f64> = steady_ts.iter().cloned().filter(|t| *t >= tail_start_t).collect();
    let tail_rss: Vec<f64> = steady.iter().filter(|s| s.0 >= tail_start_t).map(|s| s.1).collect();
    let tail_slope = linreg_slope(&tail_ts, &tail_rss) * 60.0;
    info!(
        "[soak] steady={} RSS net={:.1}MB (<= {:.0}) tail_slope={:.2}MB/min (<= {:.1})",
        steady.len(), rss_net, profile.rss_net_max_mb(), tail_slope, profile.rss_tail_slope_max()
    );
    report.assert_lte("soak_rss_tail_slope_mb_per_min", tail_slope, profile.rss_tail_slope_max());

    // FD 净增长
    let steady_fd: Vec<f64> = steady.iter().filter_map(|s| s.2.map(|v| v as f64)).collect();
    if steady_fd.len() >= need_min {
        let fd_net = net_delta(&steady_fd);
        info!("[soak] FD net growth = {:.0} (<= {:.0})", fd_net, profile.fd_net_max());
        report.assert_lte("soak_fd_net_growth", fd_net, profile.fd_net_max());
    } else {
        report.fail("soak_fd_net_growth", "稳态 FD 样本不足");
    }

    // === 5. 连接存活 ===
    let mut alive = 0u64;
    for sdk in &sdks {
        if sdk.is_connected().await.unwrap_or(false) {
            alive += 1;
        }
    }
    info!("[soak] connections alive: {}/{}", alive, conn_n);
    report.assert_gte("soak_connections_survived", alive as f64 / conn_n as f64, 0.90);

    // 仍缺 §3.1 指标（诚实日志，不加 skip 以免被当 pass）：
    info!("[soak] NOTE 未实现 §3.1: tokio task 数 / delivery p99 / 周期重连成功率 / handler 错误率(/metrics)");

    for sdk in &sdks {
        sdk.shutdown().await;
    }
    Ok(SoakResult { report })
}

#[cfg(test)]
mod tests {
    use super::{linreg_slope, net_delta, SoakProfile};

    #[test]
    fn profile_parse_and_fail_fast() {
        assert_eq!(SoakProfile::parse("").unwrap(), SoakProfile::Dev);
        assert_eq!(SoakProfile::parse("smoke").unwrap(), SoakProfile::Smoke);
        assert_eq!(SoakProfile::parse("gate").unwrap(), SoakProfile::Gate1h);
        assert_eq!(SoakProfile::parse("GATE_4H").unwrap(), SoakProfile::Gate4h);
        assert!(SoakProfile::parse("gatee").is_err()); // typo → err（调用方 fail-fast）
        assert!(SoakProfile::parse("1h").is_err());
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
    fn net_delta_growth_and_flat() {
        // 单调上涨 → 正净增
        assert!(net_delta(&[100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0]) > 50.0);
        // 平 → ~0（防噪声假泄漏）
        assert!(net_delta(&[50.0, 50.4, 49.8, 50.1, 49.9, 50.2, 50.0, 49.7]).abs() < 2.0);
        assert_eq!(net_delta(&[42.0]), 0.0);
    }
}
