//! 场景 G10：soak —— 长时持续负载下检测 server 资源无持续单调上涨（泄漏）。
//!
//! LOAD_GATE_EXECUTION_SPEC §2.4 / PRIVCHAT_SERVER_REMEDIATION_TASKS §3.1。
//!
//! **防假绿**：唯一 message id（跨进程不撞 server 幂等去重）；set-but-invalid server PID →
//! 硬 FAIL（0 RSS 不当 PASS）；建群/成员/发送量/错误率均硬断言（无流量不 PASS）；warmup 丢弃 +
//! 线性回归斜率判泄漏；`LOAD_TEST_SOAK_PROFILE=gate` 强制有效 server PID + 非零 RSS + FD 采样。
//!
//! **实现现状**：已覆盖 §3.1 的 RSS/FD 趋势 + warmup + 流量/错误率 + 连接存活；**仍缺**（诚实标注）：
//! tokio task 数、delivery p99、周期重连成功率、handler 错误率（/metrics）——见 REMEDIATION §3.1。
//! co-located 只作早期泄漏 smoke；官方 gate 需隔离 tier 长跑（1h/4h）。

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

/// 唯一 message id 生成器：**进程启动时以毫秒时间戳播种**，避免复用账号再次运行时
/// 撞 server 幂等键 `(sender_user_id, local_message_id)` 命中历史去重（P0）。
fn next_local_message_id() -> u64 {
    static SEED: AtomicU64 = AtomicU64::new(0);
    // 首次调用用时间戳播种（乘 1e6 留出序列空间）。
    let base = SEED.load(Ordering::Relaxed);
    if base == 0 {
        let ts = chrono::Utc::now().timestamp_millis() as u64;
        let _ = SEED.compare_exchange(0, ts.saturating_mul(1_000_000), Ordering::SeqCst, Ordering::Relaxed);
    }
    SEED.fetch_add(1, Ordering::Relaxed).max(1)
}

fn server_pid_env() -> Option<String> {
    std::env::var("LOAD_TEST_SERVER_PID")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// RSS 快照（MB）：目标 = server PID（若设）否则自身。读失败/无效 → 0.0（调用方据此判无效 PID）。
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

/// server 打开的 FD 数（lsof）。仅当设了 server PID 才测；失败 → None。
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

/// 最小二乘斜率 dy/dx（纯函数、可测）。点数 < 2 或 x 无变化 → 0。
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
    let is_gate = std::env::var("LOAD_TEST_SOAK_PROFILE")
        .map(|v| v.eq_ignore_ascii_case("gate"))
        .unwrap_or(false);
    let _ = std::fs::remove_dir_all(data_dir.join("soak"));

    // === P0-2：先校验 server PID（连接前 fail-fast）===
    let pid = server_pid_env();
    let rss_pid = pid.clone().unwrap_or_else(|| std::process::id().to_string());
    let rss0 = rss_mb_of(&rss_pid);
    info!(
        "[soak] profile={} duration={}s warmup={}s msg_per_sec={} users={} rss_pid={} rss0={:.1}MB",
        if is_gate { "gate" } else { "smoke" },
        duration_sec, warmup_sec, msg_per_sec, records.len(), rss_pid, rss0
    );
    if pid.is_some() && rss0 <= 0.0 {
        report.fail("soak_server_pid_valid", "LOAD_TEST_SERVER_PID 已设但 RSS 读不到=无效 PID");
        return Ok(SoakResult { report });
    }
    if is_gate && pid.is_none() {
        report.fail("soak_gate_requires_server_pid", "gate profile 必须设 LOAD_TEST_SERVER_PID");
        return Ok(SoakResult { report });
    }
    if is_gate && server_fd_count(&rss_pid).is_none() {
        report.fail("soak_gate_requires_fd", "gate profile 需可采样 server FD(lsof)");
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
    report.assert_gte("soak_connect_rate", conn_n as f64 / records.len().max(1) as f64, 0.90);
    if sdks.is_empty() {
        report.fail("soak_no_connections", "0 个连接建立成功");
        return Ok(SoakResult { report });
    }

    // === 2. 建群（P0-3：必须成功）+ 成员加入率断言 ===
    let main_sdk = sdks[0].clone();
    let create_req = GroupCreateRequest {
        name: format!("soak_{}", chrono::Utc::now().timestamp()),
        description: None,
        member_ids: None,
        creator_id: 0,
    };
    let group_id = match rpc_call_typed::<_, GroupCreateResponse>(&main_sdk, routes::group::CREATE, &create_req).await {
        Ok(res) => res.group_id,
        Err(e) => {
            report.fail("soak_group_created", &format!("建群失败: {e}"));
            for sdk in &sdks { sdk.shutdown().await; }
            return Ok(SoakResult { report });
        }
    };
    let mut joined = 0u64;
    for &uid in &user_ids {
        let add_req = GroupMemberAddRequest { group_id, user_id: uid, role: None, inviter_id: 0 };
        match rpc_call_fire(&main_sdk, routes::group_member::ADD, &add_req).await {
            Ok(_) => joined += 1,
            Err(e) => warn!("[soak] add member {} failed: {}", uid, e),
        }
    }
    info!("[soak] group {} created, {}/{} members joined", group_id, joined, user_ids.len());
    report.assert_gte("soak_member_join_rate", joined as f64 / user_ids.len().max(1) as f64, 0.90);

    // === 3. 发流量（尊重 msg_per_sec）+ 采样循环 ===
    let send_interval_ms = (1000 / msg_per_sec.max(1) as u64).max(1);
    let sample_interval_sec = (duration_sec / 20).clamp(2, 30);
    let start = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    let mut send_tick = interval(Duration::from_millis(send_interval_ms));
    let mut sample_tick = interval(Duration::from_secs(sample_interval_sec));
    sample_tick.tick().await; // 丢弃立即触发的第一下
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
                let rss = rss_mb_of(&rss_pid);
                let fd = pid.as_ref().and_then(|p| server_fd_count(p));
                samples.push((t, rss, fd));
                info!("[soak] t={:.0}s rss={:.1}MB fd={:?} sent={} send_err={}", t, rss, fd, sent, send_err);
            }
        }
        if start.elapsed().as_secs() >= duration_sec {
            break;
        }
    }

    // === P0-3：发送量 + 错误率硬断言（无流量不 PASS）===
    let expected = (msg_per_sec as u64).saturating_mul(duration_sec);
    report.assert_gte("soak_messages_sent", sent as f64, (expected as f64 * 0.5).max(1.0));
    let total_send = sent + send_err;
    let send_err_rate = if total_send > 0 { send_err as f64 / total_send as f64 } else { 1.0 };
    report.assert_lte("soak_send_error_rate", send_err_rate, 0.001);

    // === 4. warmup 丢弃 + 线性回归斜率判泄漏（P1）===
    let post: Vec<&Sample> = samples.iter().filter(|(t, ..)| *t >= warmup_sec as f64).collect();
    let ts: Vec<f64> = post.iter().map(|s| s.0).collect();
    let rss_series: Vec<f64> = post.iter().map(|s| s.1).collect();
    let rss_slope_per_min = linreg_slope(&ts, &rss_series) * 60.0;
    let max_slope = std::env::var("LOAD_TEST_SOAK_MAX_RSS_SLOPE_MB_PER_MIN")
        .ok().and_then(|s| s.parse().ok()).unwrap_or(5.0);
    info!("[soak] post-warmup samples={} RSS slope={:.3}MB/min (<= {:.1}), sent={} err={}",
        post.len(), rss_slope_per_min, max_slope, sent, send_err);
    if post.len() >= 3 {
        report.assert_lte("soak_rss_slope_mb_per_min", rss_slope_per_min, max_slope);
    } else if is_gate {
        report.fail("soak_rss_slope_mb_per_min", "post-warmup 样本不足(拉长 duration)");
    } else {
        report.skip("soak_rss_slope_mb_per_min", "post-warmup 样本 < 3(smoke 太短)");
    }

    // FD 斜率（连接数固定，应基本平）
    let fd_series: Vec<f64> = post.iter().filter_map(|s| s.2.map(|v| v as f64)).collect();
    let fd_ts: Vec<f64> = post.iter().filter(|s| s.2.is_some()).map(|s| s.0).collect();
    if fd_series.len() >= 3 {
        let fd_slope_per_min = linreg_slope(&fd_ts, &fd_series) * 60.0;
        info!("[soak] FD slope={:.2}/min", fd_slope_per_min);
        report.assert_lte("soak_fd_slope_per_min", fd_slope_per_min, 8.0);
    } else if is_gate {
        report.fail("soak_fd_slope_per_min", "gate 需 FD 采样");
    } else {
        report.skip("soak_fd_slope_per_min", "无 FD 采样");
    }

    // === 5. 连接存活 ===
    let mut alive = 0u64;
    for sdk in &sdks {
        if sdk.is_connected().await.unwrap_or(false) {
            alive += 1;
        }
    }
    info!("[soak] connections alive at end: {}/{}", alive, conn_n);
    report.assert_gte("soak_connections_survived", alive as f64 / conn_n as f64, 0.90);

    for sdk in &sdks {
        sdk.shutdown().await;
    }
    Ok(SoakResult { report })
}

#[cfg(test)]
mod tests {
    use super::linreg_slope;

    #[test]
    fn slope_flat_is_zero() {
        let xs = [0.0, 1.0, 2.0, 3.0];
        let ys = [50.0, 50.0, 50.0, 50.0];
        assert!(linreg_slope(&xs, &ys).abs() < 1e-9);
    }

    #[test]
    fn slope_of_known_line() {
        // y = 2x + 5 → slope 2
        let xs = [0.0, 1.0, 2.0, 3.0, 4.0];
        let ys = [5.0, 7.0, 9.0, 11.0, 13.0];
        assert!((linreg_slope(&xs, &ys) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn slope_positive_for_leak() {
        // 单调上涨 → 正斜率（泄漏信号）
        let xs = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0];
        let ys = [100.0, 105.0, 111.0, 118.0, 126.0, 135.0];
        assert!(linreg_slope(&xs, &ys) > 0.0);
    }

    #[test]
    fn degenerate_returns_zero() {
        assert_eq!(linreg_slope(&[], &[]), 0.0);
        assert_eq!(linreg_slope(&[1.0], &[2.0]), 0.0); // n<2
        assert_eq!(linreg_slope(&[3.0, 3.0, 3.0], &[1.0, 2.0, 3.0]), 0.0); // x 无变化
    }

    #[test]
    fn noise_around_flat_has_small_slope() {
        // 抖动但无趋势 → 斜率接近 0（防假绿：不因噪声误判泄漏）
        let xs = [0.0, 2.0, 4.0, 6.0, 8.0, 10.0];
        let ys = [50.0, 50.4, 49.8, 50.2, 49.9, 50.1];
        assert!(linreg_slope(&xs, &ys).abs() < 0.1);
    }
}
