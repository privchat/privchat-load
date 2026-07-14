//! 场景 G10：soak —— 长时持续负载下检测 **server** 资源无持续单调上涨（泄漏）。
//!
//! LOAD_GATE_EXECUTION_SPEC §2.4 / PRIVCHAT_SERVER_REMEDIATION_TASKS §3.1（阈值权威源）。
//!
//! **§3.1 六项硬判据**：RSS 净增长+末段趋平 / FD 净增长 / tokio task 净增<5% / handler 错误率<0.1%
//! / 周期重连成功率>99% / delivery(submit往返) p99 末段较首段劣化<20%。
//! **防假绿**：唯一 msgid；所有 profile 强制有效 server PID + metrics URL，采样期任一失效即 FAIL；
//! profile 枚举 fail-fast + 最短时长校验；发送量 ≥ 目标 90%；`msg_per_sec>0`。

use crate::assertions::AssertionReport;
use crate::scenario::{rpc_call_fire, rpc_call_typed};
use crate::user_store::{server_endpoints, UserRecord};
use hdrhistogram::Histogram;
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::{ClientSubmitRequest, ClientSubmitResponse};
use privchat_sdk::{PrivchatConfig, PrivchatSdk};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{info, warn};

/// soak profile：决定最短 duration/warmup 与 §3.1 阈值。未知值 fail-fast。未设 → `Dev`。
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
            _ => 600,
        }
    }
    fn rss_net_max_mb(&self) -> f64 {
        match self {
            Self::Dev => 30.0,
            Self::Smoke => 40.0,
            Self::Gate1h => 100.0,
            Self::Gate4h => 200.0,
        }
    }
    fn rss_tail_slope_max(&self) -> f64 {
        match self {
            Self::Dev => 3.0,
            Self::Smoke => 2.0,
            _ => 1.0,
        }
    }
    fn fd_net_max(&self) -> f64 {
        match self {
            Self::Dev => 32.0,
            Self::Smoke => 40.0,
            _ => 50.0,
        }
    }
    fn tokio_task_net_pct_max(&self) -> f64 {
        match self {
            Self::Dev => 0.10,
            _ => 0.05,
        }
    }
    /// gate 要求 delivery 样本 n≥10000（smoke/dev 不足则 SKIP）。
    fn require_delivery_n(&self) -> bool {
        matches!(self, Self::Gate1h | Self::Gate4h)
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

/// server /metrics 采样（handler 拒绝数 / rpc 总数 / tokio 存活 task 数）。
#[derive(Clone, Copy, Default)]
struct SrvMetrics {
    handler_rejected: f64,
    rpc_total: f64,
    tokio_tasks: f64,
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
        if let Some(rest) = line.strip_prefix("privchat_handler_rejected_total ") {
            m.handler_rejected = rest.trim().parse().unwrap_or(m.handler_rejected);
        } else if line.starts_with("privchat_rpc_total{") {
            if let Some(v) = line.rsplit('}').next().and_then(|s| s.trim().parse::<f64>().ok()) {
                m.rpc_total += v;
            }
        } else if let Some(rest) = line.strip_prefix("privchat_tokio_alive_tasks ") {
            m.tokio_tasks = rest.trim().parse().unwrap_or(m.tokio_tasks);
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

    let profile = match SoakProfile::parse(
        &std::env::var("LOAD_TEST_SOAK_PROFILE").unwrap_or_default(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("[soak] FATAL: {e}");
            std::process::exit(1);
        }
    };
    if duration_sec < profile.min_duration_sec() || warmup_sec < profile.min_warmup_sec() {
        eprintln!(
            "[soak] FATAL: profile={} 要求 duration>={}s warmup>={}s，实际 {}s/{}s",
            profile.label(), profile.min_duration_sec(), profile.min_warmup_sec(), duration_sec, warmup_sec
        );
        std::process::exit(1);
    }
    if msg_per_sec == 0 {
        eprintln!("[soak] FATAL: msg_per_sec 必须 > 0（代表性负载）");
        std::process::exit(1);
    }

    // 强制有效 server PID + metrics URL（G10 测 server）。
    let pid = match std::env::var("LOAD_TEST_SERVER_PID").ok().filter(|s| !s.trim().is_empty()) {
        Some(p) => p,
        None => {
            report.fail("soak_server_pid_set", "必须设 LOAD_TEST_SERVER_PID");
            return Ok(SoakResult { report });
        }
    };
    let murl = match metrics_url {
        Some(u) => u.to_string(),
        None => {
            report.fail("soak_metrics_url_set", "必须设 LOAD_TEST_METRICS_URL（采 tokio task/handler 错误率）");
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
    info!(
        "[soak] profile={} duration={}s warmup={}s msg_per_sec={} users={} pid={} rss0={:.1}MB tasks0={}",
        profile.label(), duration_sec, warmup_sec, msg_per_sec, records.len(), pid, rss0, m_start.tokio_tasks
    );

    // === 1. 建 N 个认证连接 ===
    let mut sdks: Vec<Arc<PrivchatSdk>> = Vec::new();
    let mut user_ids: Vec<u64> = Vec::new();
    for rec in records {
        match connect_login_auth(rec, data_dir, "soak", server_host, server_port, server_protocol).await {
            Ok(sdk) => {
                sdks.push(sdk);
                user_ids.push(rec.user_id);
            }
            Err(e) => warn!("[soak] connect {} failed: {}", rec.username, e),
        }
    }
    let conn_n = sdks.len();
    report.assert_gte("soak_connect_rate", conn_n as f64 / records.len().max(1) as f64, 0.90);
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
        if rpc_call_fire(&main_sdk, routes::group_member::ADD, &add_req).await.is_ok() {
            joined += 1;
        }
    }
    report.assert_gte("soak_member_join_rate", joined as f64 / user_ids.len().max(1) as f64, 0.90);
    info!("[soak] group {} created, {}/{} joined", group_id, joined, user_ids.len());

    // === 周期重连探针（§3.1 重连成功率）===
    let reconn_ok = Arc::new(AtomicU64::new(0));
    let reconn_att = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let probe = {
        let rec = records[0].clone();
        let dd = data_dir.to_path_buf();
        let host = server_host.to_string();
        let proto = server_protocol.to_string();
        let (ok, att, st) = (reconn_ok.clone(), reconn_att.clone(), stop.clone());
        tokio::spawn(async move {
            let mut seq = 0u64;
            let mut tick = interval(Duration::from_secs(30));
            tick.tick().await;
            while !st.load(Ordering::Relaxed) {
                tick.tick().await;
                if st.load(Ordering::Relaxed) { break; }
                seq += 1;
                // 独立 device_id + 独立 dir，避免踢掉保活连接。
                let mut r = rec.clone();
                r.device_id = uuid::Uuid::new_v4().to_string();
                let sub = format!("soak_probe/{seq}");
                att.fetch_add(1, Ordering::Relaxed);
                match connect_login_auth(&r, &dd, &sub, &host, server_port, &proto).await {
                    Ok(sdk) => { ok.fetch_add(1, Ordering::Relaxed); sdk.shutdown().await; }
                    Err(e) => warn!("[soak] reconnect probe {} failed: {}", seq, e),
                }
            }
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
                let t = start.elapsed().as_secs_f64();
                let t0 = Instant::now();
                match rpc_call_typed::<_, ClientSubmitResponse>(&main_sdk, routes::sync::SUBMIT, &req).await {
                    Ok(_) => { sent += 1; deliver.push((t, t0.elapsed().as_micros() as u64)); }
                    Err(_) => send_err += 1,
                }
            }
            _ = sample_tick.tick() => {
                let t = start.elapsed().as_secs_f64();
                let rss = rss_mb_of(&pid);
                let fd = server_fd_count(&pid);
                let tasks = fetch_srv_metrics(&murl).await.map(|m| m.tokio_tasks).unwrap_or(0.0);
                if rss <= 0.0 || fd.is_none() || tasks <= 0.0 {
                    invalid_sample = true;
                    warn!("[soak] t={:.0}s INVALID sample rss={:.1} fd={:?} tasks={}", t, rss, fd, tasks);
                }
                samples.push((t, rss, fd, tasks));
                info!("[soak] t={:.0}s rss={:.1}MB fd={:?} tasks={} sent={} err={}", t, rss, fd, tasks, sent, send_err);
            }
        }
        if start.elapsed().as_secs() >= duration_sec { break; }
    }
    stop.store(true, Ordering::Relaxed);
    let _ = probe.await;
    let m_end = fetch_srv_metrics(&murl).await;

    report.assert_lte("soak_sampling_valid", if invalid_sample { 1.0 } else { 0.0 }, 0.0);

    // 发送量 90% + 错误率
    let expected = (msg_per_sec as u64).saturating_mul(duration_sec);
    report.assert_gte("soak_messages_sent_90pct", sent as f64, (expected as f64 * 0.9).max(1.0));
    let total_send = sent + send_err;
    let send_err_rate = if total_send > 0 { send_err as f64 / total_send as f64 } else { 1.0 };
    report.assert_lte("soak_send_error_rate", send_err_rate, 0.001);

    // === 4. 稳态段（丢 warmup）分析 ===
    let steady: Vec<&Sample> = samples.iter().filter(|(t, ..)| *t >= warmup_sec as f64).collect();
    let need_min = if profile == SoakProfile::Dev { 3 } else { 6 };
    if steady.len() < need_min {
        report.fail("soak_steady_samples", &format!("稳态样本 {} < {}", steady.len(), need_min));
        for sdk in &sdks { sdk.shutdown().await; }
        return Ok(SoakResult { report });
    }
    let s_ts: Vec<f64> = steady.iter().map(|s| s.0).collect();
    let s_rss: Vec<f64> = steady.iter().map(|s| s.1).collect();

    // RSS 净增长 + 末段趋平（稳态跨度≥60min 取固定末 30min，否则末 ½）
    let rss_net = net_delta(&s_rss);
    report.assert_lte("soak_rss_net_growth_mb", rss_net, profile.rss_net_max_mb());
    let span = s_ts.last().unwrap() - s_ts.first().unwrap();
    let tail_window = if span >= 3600.0 { 1800.0 } else { span / 2.0 };
    let tail_start = s_ts.last().unwrap() - tail_window;
    let tail_ts: Vec<f64> = s_ts.iter().cloned().filter(|t| *t >= tail_start).collect();
    let tail_rss: Vec<f64> = steady.iter().filter(|s| s.0 >= tail_start).map(|s| s.1).collect();
    let tail_slope = linreg_slope(&tail_ts, &tail_rss) * 60.0;
    report.assert_lte("soak_rss_tail_slope_mb_per_min", tail_slope, profile.rss_tail_slope_max());

    // FD 净增长
    let s_fd: Vec<f64> = steady.iter().filter_map(|s| s.2.map(|v| v as f64)).collect();
    if s_fd.len() >= need_min {
        report.assert_lte("soak_fd_net_growth", net_delta(&s_fd), profile.fd_net_max());
    } else {
        report.fail("soak_fd_net_growth", "稳态 FD 样本不足");
    }

    // tokio task 净增 < 5% 基线
    let s_tasks: Vec<f64> = steady.iter().map(|s| s.3).collect();
    let task_base = s_tasks.first().copied().unwrap_or(0.0).max(1.0);
    let task_net_pct = net_delta(&s_tasks) / task_base;
    report.assert_lte("soak_tokio_task_net_pct", task_net_pct, profile.tokio_task_net_pct_max());

    // handler 错误率（稳态 Δrejected/Δrpc_total）
    if let Some(me) = m_end {
        let d_rej = (me.handler_rejected - m_start.handler_rejected).max(0.0);
        let d_rpc = (me.rpc_total - m_start.rpc_total).max(0.0);
        let err_rate = if d_rpc > 0.0 { d_rej / d_rpc } else { 0.0 };
        info!("[soak] handler err: Δrejected={:.0} Δrpc={:.0} rate={:.4}%", d_rej, d_rpc, err_rate * 100.0);
        report.assert_lte("soak_handler_error_rate", err_rate, 0.001);
    } else {
        report.fail("soak_handler_error_rate", "结束拉不到 /metrics");
    }

    // delivery(submit往返) p99 末段较首段劣化 < 20%
    let post_deliver: Vec<&(f64, u64)> = deliver.iter().filter(|(t, _)| *t >= warmup_sec as f64).collect();
    let dn = post_deliver.len();
    if dn >= 20 && (!profile.require_delivery_n() || dn >= 10000) {
        let half = dn / 2;
        let mut h1: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
        let mut h2: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
        for (i, (_, us)) in post_deliver.iter().enumerate() {
            let _ = if i < half { h1.record(*us) } else { h2.record(*us) };
        }
        let p1 = h1.value_at_quantile(0.99) as f64;
        let p2 = h2.value_at_quantile(0.99) as f64;
        let degr = if p1 > 0.0 { (p2 - p1) / p1 } else { 0.0 };
        info!("[soak] delivery p99: first={:.0}us last={:.0}us degradation={:.1}% (n={})", p1, p2, degr * 100.0, dn);
        report.assert_lte("soak_delivery_p99_degradation", degr, 0.20);
    } else if profile.require_delivery_n() {
        report.fail("soak_delivery_p99_degradation", &format!("gate 需 delivery n≥10000，实际 {dn}"));
    } else {
        report.skip("soak_delivery_p99_degradation", &format!("样本 {dn} 不足(smoke/dev)"));
    }

    // 周期重连成功率 > 99%
    let att = reconn_att.load(Ordering::Relaxed);
    let ok = reconn_ok.load(Ordering::Relaxed);
    if att > 0 {
        info!("[soak] reconnect probe: {}/{} ok", ok, att);
        report.assert_gte("soak_reconnect_success_rate", ok as f64 / att as f64, 0.99);
    } else {
        report.skip("soak_reconnect_success_rate", "无重连尝试(duration 太短)");
    }

    // 连接存活
    let mut alive = 0u64;
    for sdk in &sdks {
        if sdk.is_connected().await.unwrap_or(false) { alive += 1; }
    }
    report.assert_gte("soak_connections_survived", alive as f64 / conn_n as f64, 0.90);
    info!("[soak] alive {}/{}, rss_net={:.1}MB tail_slope={:.2} task_net_pct={:.3}", alive, conn_n, rss_net, tail_slope, task_net_pct);

    for sdk in &sdks { sdk.shutdown().await; }
    Ok(SoakResult { report })
}

#[cfg(test)]
mod tests {
    use super::{linreg_slope, net_delta, SoakProfile};

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
}
