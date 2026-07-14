//! 场景 G10：soak —— 长时持续负载下检测 server 资源无持续单调上涨（泄漏）。
//!
//! LOAD_GATE_EXECUTION_SPEC §2.4 / PRIVCHAT_SERVER_REMEDIATION_TASKS §3.1。
//!
//! 流程：建 N 个认证连接（保活）+ 群消息持续流量；周期采样 server RSS / FD / conns；
//! 判「后段 vs 前段」平均无显著上涨；连接全程存活。
//!
//! 注意：单机 co-located 只能作**早期泄漏 smoke**（RSS 绝对速率不可信）；官方 gate 需隔离
//! tier 长跑（1h/4h），判据（RSS/FD/task 无持续单调上涨）见 §3.1。

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

fn next_local_message_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// RSS 采样目标 PID（设 `LOAD_TEST_SERVER_PID` → server；否则自身，仅参考）。
fn rss_target_pid() -> String {
    std::env::var("LOAD_TEST_SERVER_PID")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| std::process::id().to_string())
}

/// server RSS 快照（MB）。`ps -o rss=` 在 macOS/Linux 通用。
fn rss_mb() -> f64 {
    let pid = rss_target_pid();
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
    {
        if let Ok(kb) = String::from_utf8_lossy(&out.stdout).trim().parse::<u64>() {
            return kb as f64 / 1024.0;
        }
    }
    0.0
}

/// server 打开的 FD 数（lsof）。仅当设了 `LOAD_TEST_SERVER_PID` 才测；否则/失败 → None。
fn server_fd_count() -> Option<u64> {
    let pid = std::env::var("LOAD_TEST_SERVER_PID")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let out = std::process::Command::new("lsof")
        .args(["-p", &pid])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let n = String::from_utf8_lossy(&out.stdout).lines().count() as u64;
    Some(n.saturating_sub(1)) // 去表头
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

/// 采样点：(经过秒, rss_mb, fd, 累计发送数)。
type Sample = (f64, f64, Option<u64>, u64);

/// 后段-前段平均差（用于泄漏趋势判定）。取首 1/3 与末 1/3。
fn trend_delta(values: &[f64]) -> f64 {
    if values.len() < 3 {
        return 0.0;
    }
    let third = values.len() / 3;
    let first_avg = values[..third].iter().sum::<f64>() / third as f64;
    let last_avg = values[values.len() - third..].iter().sum::<f64>() / third as f64;
    last_avg - first_avg
}

pub async fn run_soak(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    duration_sec: u64,
) -> Result<SoakResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let _ = std::fs::remove_dir_all(data_dir.join("soak"));
    info!(
        "[soak] duration={}s, users={}, rss_pid={}",
        duration_sec,
        records.len(),
        rss_target_pid()
    );

    // === 1. 建 N 个认证连接（顺序，保活）===
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
        return Ok(SoakResult { report });
    }

    // === 2. 建群 + 拉全部进群（流量目标）===
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
            warn!("[soak] group create failed: {} — 退化为纯保活 soak（无群流量）", e);
            0
        }
    };
    if group_id != 0 {
        for &uid in &user_ids {
            let add_req = GroupMemberAddRequest {
                group_id,
                user_id: uid,
                role: None,
                inviter_id: 0,
            };
            let _ = rpc_call_fire(&main_sdk, routes::group_member::ADD, &add_req).await;
        }
        info!("[soak] group {} created, {} members added", group_id, user_ids.len());
    }

    // === 3. 采样 + 发流量循环 ===
    let sample_interval_sec = (duration_sec / 20).clamp(2, 30);
    let start = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    samples.push((0.0, rss_mb(), server_fd_count(), 0));
    let mut send_tick = interval(Duration::from_millis(200));
    let mut sample_tick = interval(Duration::from_secs(sample_interval_sec));
    sample_tick.tick().await; // 立即触发第一下丢弃（已手动首采）
    let mut sent = 0u64;
    let mut send_err = 0u64;
    loop {
        tokio::select! {
            _ = send_tick.tick() => {
                if group_id != 0 {
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
            }
            _ = sample_tick.tick() => {
                let t = start.elapsed().as_secs_f64();
                let rss = rss_mb();
                let fd = server_fd_count();
                samples.push((t, rss, fd, sent));
                info!("[soak] t={:.0}s rss={:.1}MB fd={:?} sent={} send_err={}", t, rss, fd, sent, send_err);
            }
        }
        if start.elapsed().as_secs() >= duration_sec {
            break;
        }
    }

    // === 4. 泄漏趋势判定（后 1/3 vs 前 1/3 平均）===
    let rss_series: Vec<f64> = samples.iter().map(|s| s.1).collect();
    let rss_growth = trend_delta(&rss_series);
    let max_rss_growth_mb: f64 = std::env::var("LOAD_TEST_SOAK_MAX_RSS_GROWTH_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50.0);
    info!(
        "[soak] RSS trend: {} samples, first→last third growth = {:.1}MB (threshold <= {:.1}MB), sent={}",
        samples.len(),
        rss_growth,
        max_rss_growth_mb,
        sent
    );
    if rss_target_pid() != std::process::id().to_string() && samples.len() >= 3 {
        report.assert_lte("soak_rss_growth_mb", rss_growth, max_rss_growth_mb);
    } else {
        report.skip(
            "soak_rss_growth_mb",
            "未设 LOAD_TEST_SERVER_PID(测的是 load 自身) 或样本不足",
        );
    }

    // FD 趋势（若有采样）
    let fd_series: Vec<f64> = samples.iter().filter_map(|s| s.2.map(|v| v as f64)).collect();
    if fd_series.len() >= 3 {
        let fd_growth = trend_delta(&fd_series);
        info!("[soak] FD trend: first→last third growth = {:.0}", fd_growth);
        // 连接数固定，FD 应基本稳定；给宽松上限（含少量 churn）。
        report.assert_lte("soak_fd_growth", fd_growth, 64.0);
    } else {
        report.skip("soak_fd_growth", "无 FD 采样(lsof 不可用/未设 server PID)");
    }

    // === 5. 连接存活 ===
    let mut alive = 0u64;
    for sdk in &sdks {
        if sdk.is_connected().await.unwrap_or(false) {
            alive += 1;
        }
    }
    info!("[soak] connections alive at end: {}/{}", alive, conn_n);
    report.assert_gte(
        "soak_connections_survived",
        alive as f64 / conn_n as f64,
        0.90,
    );

    // 清理
    for sdk in &sdks {
        sdk.shutdown().await;
    }

    Ok(SoakResult { report })
}
