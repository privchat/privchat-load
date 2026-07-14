//! 场景 C：认证峰值验证
//!
//! 验证高并发认证（connect + authenticate）不会导致：
//! 1. 连接成功率下降
//! 2. 认证延迟过高
//! 3. DB pool 耗尽
//! 4. 服务端 spawn 数量失控
//!
//! 流程：
//! - 第一轮（基线）：以正常速率逐个 connect + authenticate
//! - 第二轮（峰值）：断开所有连接后，全部并发重连
//! - 采集服务端 /metrics 比较 pool 利用率
//! - 断言：峰值 p99 认证延迟 < 阈值，成功率 > 95%

use crate::assertions::AssertionReport;
use crate::user_store::UserRecord;
use hdrhistogram::Histogram;
use privchat_sdk::{ConnectionState, PrivchatConfig, PrivchatSdk};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

/// 服务端指标快照（从 prometheus /metrics 解析）
#[derive(Debug, Default)]
struct ServerMetrics {
    db_pool_active: Option<f64>,
    #[allow(dead_code)]
    db_pool_idle: Option<f64>,
    redis_pool_active: Option<f64>,
    #[allow(dead_code)]
    redis_pool_idle: Option<f64>,
    active_connections: Option<f64>,
}

/// 从 prometheus 文本格式解析指定 gauge 值
fn parse_gauge(metrics_text: &str, name: &str) -> Option<f64> {
    for line in metrics_text.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // format: metric_name value  or  metric_name{labels} value
        if let Some(rest) = line.strip_prefix(name) {
            let value_str = if rest.starts_with('{') {
                // has labels: metric_name{...} value
                if let Some(pos) = rest.find('}') {
                    rest[pos + 1..].trim()
                } else {
                    continue;
                }
            } else if rest.starts_with(' ') || rest.starts_with('\t') {
                rest.trim()
            } else {
                continue;
            };
            if let Ok(v) = value_str.parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

/// 从服务端拉取 metrics
async fn fetch_server_metrics(metrics_url: &str) -> Option<ServerMetrics> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;
    let text = client
        .get(metrics_url)
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;

    Some(ServerMetrics {
        db_pool_active: parse_gauge(&text, "privchat_db_pool_active"),
        db_pool_idle: parse_gauge(&text, "privchat_db_pool_idle"),
        redis_pool_active: parse_gauge(&text, "privchat_redis_pool_active"),
        redis_pool_idle: parse_gauge(&text, "privchat_redis_pool_idle"),
        active_connections: parse_gauge(&text, "privchat_connections_current"),
    })
}

/// 认证峰值验证结果
pub struct AuthPeakResult {
    pub report: AssertionReport,
    pub baseline_p99_ms: f64,
    pub peak_p99_ms: f64,
    pub baseline_success_rate: f64,
    pub peak_success_rate: f64,
}

/// 单次 connect + login 的延迟测量
async fn connect_and_auth_one(
    record: &UserRecord,
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
) -> Result<(Arc<PrivchatSdk>, f64), Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = crate::user_store::server_endpoints(server_host, server_port, server_protocol);
    let user_data_dir = data_dir.join("users").join(&record.username);
    std::fs::create_dir_all(&user_data_dir)?;

    let config = PrivchatConfig {
        endpoints,
        connection_timeout_secs: 30,
        data_dir: user_data_dir.to_string_lossy().to_string(),
    };

    let t0 = Instant::now();
    let sdk = PrivchatSdk::new(config);
    sdk.connect().await?;
    let connect_ms = t0.elapsed().as_secs_f64() * 1000.0;
    // Fix 1: connect() 返回后立即核对状态。非 Connected/LoggedIn/Authenticated → 明确报
    // connect_returned_state_xxx（归入 sdk_state_err），据此区分 SDK race 与 server 断回 New。
    let ts = Instant::now();
    match sdk.connection_state().await {
        Ok(ConnectionState::Connected)
        | Ok(ConnectionState::LoggedIn)
        | Ok(ConnectionState::Authenticated) => {}
        Ok(other) => return Err(format!("connect_returned_state_{other:?}").into()),
        Err(e) => return Err(format!("connect_returned_state_err: {e}").into()),
    }
    let state_ms = ts.elapsed().as_secs_f64() * 1000.0;
    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let tl = Instant::now();
    let login_result = sdk
        .login(record.username.clone(), password, record.device_id.clone())
        .await?;
    let login_ms = tl.elapsed().as_secs_f64() * 1000.0;
    let ta = Instant::now();
    sdk.authenticate(
        login_result.user_id,
        login_result.token,
        login_result.device_id,
    )
    .await?;
    let auth_ms = ta.elapsed().as_secs_f64() * 1000.0;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    // Fix C: 分段耗时日志——定位 37s 慢在 connect / state / login / authenticate 哪一段。
    info!(
        "[auth_peak] stage user={} connect={:.0} state={:.0} login={:.0} auth={:.0} total={:.0} ms",
        record.username, connect_ms, state_ms, login_ms, auth_ms, elapsed_ms
    );

    Ok((Arc::new(sdk), elapsed_ms))
}

/// 运行认证峰值场景
pub async fn run_auth_peak(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    metrics_url: Option<&str>,
) -> Result<AuthPeakResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let total = records.len();

    info!("[auth_peak] total users: {}", total);

    // === Phase 1: Baseline (sequential connect) ===
    info!("[auth_peak] Phase 1: baseline sequential connect");
    let mut baseline_hist: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
    let mut baseline_ok = 0u64;
    let mut baseline_sdks = Vec::with_capacity(total);

    for (i, record) in records.iter().enumerate() {
        match connect_and_auth_one(record, data_dir, server_host, server_port, server_protocol)
            .await
        {
            Ok((sdk, latency_ms)) => {
                baseline_ok += 1;
                let us = (latency_ms * 1000.0) as u64;
                let _ = baseline_hist.record(us);
                baseline_sdks.push(Some(sdk));
            }
            Err(e) => {
                warn!("[auth_peak] baseline connect {} failed: {}", i, e);
                baseline_sdks.push(None);
            }
        }
    }

    let baseline_success_rate = baseline_ok as f64 / total as f64;
    let baseline_p99_ms = if !baseline_hist.is_empty() {
        baseline_hist.value_at_quantile(0.99) as f64 / 1000.0
    } else {
        0.0
    };
    info!(
        "[auth_peak] baseline: {}/{} ok, p99={:.2}ms",
        baseline_ok, total, baseline_p99_ms
    );

    // Fetch server metrics after baseline
    let metrics_after_baseline = if let Some(url) = metrics_url {
        fetch_server_metrics(url).await
    } else {
        None
    };
    if let Some(ref m) = metrics_after_baseline {
        info!(
            "[auth_peak] server after baseline: db_active={:?} redis_active={:?} conns={:?}",
            m.db_pool_active, m.redis_pool_active, m.active_connections
        );
    }

    // === Phase 2: Disconnect all ===
    info!("[auth_peak] Phase 2: disconnect all");
    for sdk_opt in &baseline_sdks {
        if let Some(sdk) = sdk_opt {
            sdk.shutdown().await;
        }
    }
    drop(baseline_sdks);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // === Phase 3: Peak (concurrent reconnect) ===
    info!("[auth_peak] Phase 3: peak concurrent reconnect");
    let mut peak_hist: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
    let mut peak_ok = 0u64;

    let data_dir_buf = data_dir.to_path_buf();
    let server_host_str = server_host.to_string();
    let server_protocol_str = server_protocol.to_string();

    // Fix 2: 峰值**期间**采样 /metrics 记录 max（结束态采一次不可信）。
    let peak_max = Arc::new(std::sync::Mutex::new((0.0f64, 0.0f64, 0.0f64))); // (db_active, redis_active, conns)
    let metrics_samples = Arc::new(std::sync::atomic::AtomicU64::new(0)); // Fix A: 成功采样次数
    let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = metrics_url.map(|u| {
        let url = u.to_string();
        let peak_max = peak_max.clone();
        let samples = metrics_samples.clone();
        let stop = sampler_stop.clone();
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(m) = fetch_server_metrics(&url).await {
                    samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut mx = peak_max.lock().unwrap();
                    mx.0 = mx.0.max(m.db_pool_active.unwrap_or(0.0));
                    mx.1 = mx.1.max(m.redis_pool_active.unwrap_or(0.0));
                    mx.2 = mx.2.max(m.active_connections.unwrap_or(0.0));
                }
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        })
    });

    let t0 = Instant::now();
    // 峰值一次性并发 spawn 全部 N 个（并发度 = users.total，不受 rate.connect_concurrent 约束）。
    let mut handles = Vec::with_capacity(total);
    for record in records.iter() {
        let record = record.clone();
        let data_dir = data_dir_buf.clone();
        let host = server_host_str.clone();
        let protocol = server_protocol_str.clone();
        handles.push(tokio::spawn(async move {
            connect_and_auth_one(&record, &data_dir, &host, server_port, &protocol).await
        }));
    }

    let mut peak_conn_refused = 0u64;
    let mut peak_other_errors = 0u64;
    // SDK/harness 状态 race（如 "login requires connect first"）单独统计，
    // 与 server refused/timeout/error 分开——归因未完成前不能混为一谈（Codex）。
    let mut peak_state_errors = 0u64;

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok((_sdk, latency_ms))) => {
                peak_ok += 1;
                let us = (latency_ms * 1000.0) as u64;
                let _ = peak_hist.record(us);
            }
            Ok(Err(e)) => {
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("connection refused")
                    || err_str.contains("connrefused")
                    || err_str.contains("handshake")
                    || err_str.contains("timed out")
                    || err_str.contains("timeout")
                {
                    peak_conn_refused += 1;
                    warn!("[auth_peak] peak connect {} refused/timeout: {}", i, e);
                } else if err_str.contains("requires connect first")
                    || err_str.contains("invalid state")
                    || err_str.contains("connect first")
                    || err_str.contains("connect_returned_state")
                {
                    // SDK/harness 状态 race，非 server 拒绝。归因未完成前单列，不算作 server 侧失败。
                    peak_state_errors += 1;
                    warn!(
                        "[auth_peak] peak connect {} SDK-state error (归因待查): {}",
                        i, e
                    );
                } else {
                    peak_other_errors += 1;
                    warn!("[auth_peak] peak connect {} failed: {}", i, e);
                }
            }
            Err(e) => {
                peak_other_errors += 1;
                warn!("[auth_peak] peak task {} panicked: {}", i, e);
            }
        }
    }
    let peak_elapsed = t0.elapsed();
    // Fix 2: 停 sampler，取峰值期间 max（供断言用，替代结束态）
    sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(h) = sampler {
        let _ = h.await;
    }
    let (db_active_max, redis_active_max, conns_max) = *peak_max.lock().unwrap();
    info!(
        "[auth_peak] peak metrics MAX: db_active={:.0} redis_active={:.0} conns={:.0}",
        db_active_max, redis_active_max, conns_max
    );
    let _peak_total_failures = peak_conn_refused + peak_other_errors + peak_state_errors;

    let peak_success_rate = peak_ok as f64 / total as f64;
    let peak_p99_ms = if !peak_hist.is_empty() {
        peak_hist.value_at_quantile(0.99) as f64 / 1000.0
    } else {
        0.0
    };
    let conn_refused_rate = if total > 0 {
        peak_conn_refused as f64 / total as f64
    } else {
        0.0
    };
    info!(
        "[auth_peak] peak: {}/{} ok, refused/timeout={}, sdk_state_err={}, other_err={}, p99={:.2}ms, elapsed={:.2}s",
        peak_ok,
        total,
        peak_conn_refused,
        peak_state_errors,
        peak_other_errors,
        peak_p99_ms,
        peak_elapsed.as_secs_f64()
    );

    // Fetch server metrics after peak
    let metrics_after_peak = if let Some(url) = metrics_url {
        fetch_server_metrics(url).await
    } else {
        None
    };
    if let Some(ref m) = metrics_after_peak {
        info!(
            "[auth_peak] server after peak: db_active={:?} redis_active={:?} conns={:?}",
            m.db_pool_active, m.redis_pool_active, m.active_connections
        );
    }

    // === Assertions ===

    // Baseline success rate >= 95%
    report.assert_gte("baseline_success_rate", baseline_success_rate, 0.95);

    // Peak success rate >= 90% (more lenient under burst)
    report.assert_gte("peak_success_rate", peak_success_rate, 0.90);

    // Peak p99 auth latency < 5000ms
    if peak_ok > 0 {
        report.assert_lte("peak_auth_p99_ms", peak_p99_ms, 5000.0);
    } else {
        report.skip("peak_auth_p99_ms", "no successful peak connections");
    }

    // Peak p99 should not be more than 10x baseline
    if baseline_p99_ms > 0.0 && peak_ok > 0 {
        let degradation = peak_p99_ms / baseline_p99_ms;
        report.assert_lte("peak_vs_baseline_ratio", degradation, 10.0);
    } else {
        report.skip("peak_vs_baseline_ratio", "insufficient data");
    }

    // Connection refused / handshake failed rate should be near 0
    // This catches the case where p99 looks fine but system is actively rejecting connections
    report.assert_lte("peak_conn_refused_rate", conn_refused_rate, 0.05);

    // Fix A: metrics_url 已配置但一次都没成功采样 → FAIL（防 URL 配错却假 PASS）。
    let n_samples = metrics_samples.load(std::sync::atomic::Ordering::Relaxed);
    if metrics_url.is_some() {
        report.assert_gte(
            "metrics_sampled",
            if n_samples > 0 { 1.0 } else { 0.0 },
            1.0,
        );
    }
    // Fix B: db_pool gauge 是 server 60s 周期上报，峰值(数十秒)内 sampler 读到的是旧值 →
    // db_pool_active_peak 目前只算**弱证据**，标 SKIP，不作 PASS；待改用 pg_stat_activity /
    // server load-test 高频刷新 gauge 后再作硬断言。
    if metrics_url.is_some() && n_samples > 0 {
        report.skip(
            "db_pool_active_peak",
            &format!(
                "弱证据(pool gauge 60s 周期,峰值读旧值); max={db_active_max:.0} 待接 pg_stat_activity"
            ),
        );
    } else {
        report.skip("db_pool_active_peak", "metrics 未配置/未采到");
    }
    let _ = (redis_active_max, conns_max); // 已在 MAX 日志输出，保留供后续断言扩展

    Ok(AuthPeakResult {
        report,
        baseline_p99_ms,
        peak_p99_ms,
        baseline_success_rate,
        peak_success_rate,
    })
}

// ============================================================================
// 场景：重连认证峰值（G8 真实路径）——与上面的密码登录峰值(auth_peak)拆开。
//
// 真实的"服务端重启 → 全体重连"风暴走的是 connect + token authenticate，**不含**
// 密码登录/bcrypt。server profile 显示密码登录峰值里唯一用户态热点是 bcrypt，所以
// 用它无法证明 reconnect path 绿不绿。本场景把 bcrypt 挡在顺序 warmup 里，峰值只测
// 真实重连建连+认证延迟。
// ============================================================================

/// warmup 期拿到的新鲜凭证（不用 users.txt 里可能过期的 token）。
#[derive(Clone)]
struct TokenCreds {
    username: String,
    user_id: u64,
    token: String,
    device_id: String,
}

/// warmup：顺序 password login 拿一个新鲜 token 后关闭（bcrypt 在此，不计入峰值）。
async fn warmup_login_one(
    record: &UserRecord,
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
) -> Result<TokenCreds, Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = crate::user_store::server_endpoints(server_host, server_port, server_protocol);
    // 独立干净目录：避免复用 users/<user> 里旧 session 快照，否则 connect() 会用**过期 token**
    // 触发 auto-restore 直接失败（[10002] Token 已过期），login() 根本没机会跑。
    let user_data_dir = data_dir.join("reconnect_warmup").join(&record.username);
    std::fs::create_dir_all(&user_data_dir)?;
    let config = PrivchatConfig {
        endpoints,
        connection_timeout_secs: 30,
        data_dir: user_data_dir.to_string_lossy().to_string(),
    };
    let sdk = PrivchatSdk::new(config);
    sdk.connect().await?;
    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let login = sdk
        .login(record.username.clone(), password, record.device_id.clone())
        .await?;
    let creds = TokenCreds {
        username: record.username.clone(),
        user_id: login.user_id,
        token: login.token,
        device_id: login.device_id,
    };
    sdk.shutdown().await;
    Ok(creds)
}

/// 峰值：fresh SDK + connect + token authenticate（**无 password login / 无 bcrypt**）。
/// 用独立空 data_dir，避免本地 session 快照触发 auto-restore 而模糊 connect/auth 分段。
/// 返回 connect+state+auth 总延迟(ms)。
async fn connect_and_token_auth_one(
    creds: &TokenCreds,
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
) -> Result<(Arc<PrivchatSdk>, f64), Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = crate::user_store::server_endpoints(server_host, server_port, server_protocol);
    let user_data_dir = data_dir.join("reconnect_peak").join(&creds.username);
    std::fs::create_dir_all(&user_data_dir)?;
    let config = PrivchatConfig {
        endpoints,
        connection_timeout_secs: 30,
        data_dir: user_data_dir.to_string_lossy().to_string(),
    };
    let t0 = Instant::now();
    let sdk = PrivchatSdk::new(config);
    sdk.connect().await?;
    let connect_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let ts = Instant::now();
    match sdk.connection_state().await {
        Ok(ConnectionState::Connected)
        | Ok(ConnectionState::LoggedIn)
        | Ok(ConnectionState::Authenticated) => {}
        Ok(other) => return Err(format!("connect_returned_state_{other:?}").into()),
        Err(e) => return Err(format!("connect_returned_state_err: {e}").into()),
    }
    let state_ms = ts.elapsed().as_secs_f64() * 1000.0;
    let ta = Instant::now();
    sdk.authenticate(creds.user_id, creds.token.clone(), creds.device_id.clone())
        .await?;
    let auth_ms = ta.elapsed().as_secs_f64() * 1000.0;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
    info!(
        "[reconnect_token] stage user={} connect={:.0} state={:.0} auth={:.0} total={:.0} ms",
        creds.username, connect_ms, state_ms, auth_ms, elapsed_ms
    );
    Ok((Arc::new(sdk), elapsed_ms))
}

/// 场景：重连认证峰值。Phase1 warmup(顺序 login 拿新鲜 token) → Phase2 settle →
/// Phase3 peak(connect + token authenticate)。
///
/// `jittered`=false → worst-case burst（一次性并发全部）；true → 在 `arrival_window_ms`
/// 内按 ±30% 确定性 jitter 铺开到达（jittered-realistic，LOAD_GATE_EXECUTION_SPEC §4）。
pub async fn run_reconnect_token_peak(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    metrics_url: Option<&str>,
    jittered: bool,
    arrival_window_ms: u64,
) -> Result<AuthPeakResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let total = records.len();
    info!("[reconnect_token] total users: {}", total);

    // 清理上一轮的临时会话目录，保证每次跑都是干净重连（无 stale session 快照）。
    let _ = std::fs::remove_dir_all(data_dir.join("reconnect_warmup"));
    let _ = std::fs::remove_dir_all(data_dir.join("reconnect_peak"));

    // === Phase 1: warmup —— 顺序 password login 拿新鲜 token（bcrypt 在此，不计峰值）===
    info!("[reconnect_token] Phase 1: warmup logins (sequential) to get fresh tokens");
    let mut creds_list: Vec<TokenCreds> = Vec::with_capacity(total);
    for (i, record) in records.iter().enumerate() {
        match warmup_login_one(record, data_dir, server_host, server_port, server_protocol).await {
            Ok(c) => creds_list.push(c),
            Err(e) => warn!("[reconnect_token] warmup login {} failed: {}", i, e),
        }
    }
    let warmup_ok = creds_list.len();
    let warmup_rate = warmup_ok as f64 / total.max(1) as f64;
    info!(
        "[reconnect_token] warmup: {}/{} tokens acquired ({:.1}%)",
        warmup_ok,
        total,
        warmup_rate * 100.0
    );
    if creds_list.is_empty() {
        report.assert_gte("warmup_token_rate", 0.0, 0.95); // FAIL：一个 token 都没拿到
        return Ok(AuthPeakResult {
            report,
            baseline_p99_ms: 0.0,
            peak_p99_ms: 0.0,
            baseline_success_rate: 0.0,
            peak_success_rate: 0.0,
        });
    }

    // === Phase 2: settle ===
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // === Phase 3: peak —— 全部并发 connect + token authenticate ===
    info!("[reconnect_token] Phase 3: peak concurrent connect + token authenticate");
    let peak_max = Arc::new(std::sync::Mutex::new((0.0f64, 0.0f64, 0.0f64)));
    let metrics_samples = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sampler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = metrics_url.map(|u| {
        let url = u.to_string();
        let peak_max = peak_max.clone();
        let samples = metrics_samples.clone();
        let stop = sampler_stop.clone();
        tokio::spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(m) = fetch_server_metrics(&url).await {
                    samples.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut mx = peak_max.lock().unwrap();
                    mx.0 = mx.0.max(m.db_pool_active.unwrap_or(0.0));
                    mx.1 = mx.1.max(m.redis_pool_active.unwrap_or(0.0));
                    mx.2 = mx.2.max(m.active_connections.unwrap_or(0.0));
                }
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        })
    });

    let data_dir_buf = data_dir.to_path_buf();
    let host = server_host.to_string();
    let proto = server_protocol.to_string();
    let t0 = Instant::now();
    // 到达模型（LOAD_GATE_EXECUTION_SPEC §4）：
    //  - burst（默认）：一次性并发 spawn 全部 N 个（最坏情况 reconnect 突发）。
    //  - jittered：在 arrival_window_ms 内按 ±30% **确定性** jitter 铺开到达（可复现）。
    // 并发度上限 = `users.total`（不受 `rate.connect_concurrent` 约束；调并发改 users.total）。
    let n_peak = creds_list.len();
    info!(
        "[reconnect_token] arrival model = {} (window={}ms, N={})",
        if jittered { "jittered" } else { "burst" },
        if jittered { arrival_window_ms } else { 0 },
        n_peak
    );
    let mut handles = Vec::with_capacity(n_peak);
    for (i, creds) in creds_list.iter().enumerate() {
        let creds = creds.clone();
        let dd = data_dir_buf.clone();
        let h = host.clone();
        let p = proto.clone();
        // 铺开延迟：base = i/N × window；±30% 确定性 jitter（hash(i)→[0,1)）去同步。
        let delay_ms: u64 = if jittered && n_peak > 1 {
            let base = (i as f64 / n_peak as f64) * arrival_window_ms as f64;
            let hjit = ((i as u64).wrapping_mul(2_654_435_761) % 1000) as f64 / 1000.0;
            (base * (0.7 + hjit * 0.6)) as u64
        } else {
            0
        };
        handles.push(tokio::spawn(async move {
            if delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            connect_and_token_auth_one(&creds, &dd, &h, server_port, &p).await
        }));
    }

    let mut peak_hist: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
    let mut peak_ok = 0u64;
    let mut peak_conn_refused = 0u64;
    let mut peak_state_errors = 0u64;
    let mut peak_other_errors = 0u64;
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok((_sdk, latency_ms))) => {
                peak_ok += 1;
                let _ = peak_hist.record((latency_ms * 1000.0) as u64);
            }
            Ok(Err(e)) => {
                let s = e.to_string().to_lowercase();
                if s.contains("refused") || s.contains("timeout") || s.contains("handshake") {
                    peak_conn_refused += 1;
                } else if s.contains("connect_returned_state")
                    || s.contains("connect first")
                    || s.contains("invalid state")
                {
                    peak_state_errors += 1;
                } else {
                    peak_other_errors += 1;
                }
                warn!("[reconnect_token] peak {} failed: {}", i, e);
            }
            Err(e) => {
                peak_other_errors += 1;
                warn!("[reconnect_token] peak task {} panicked: {}", i, e);
            }
        }
    }
    let peak_elapsed = t0.elapsed();
    sampler_stop.store(true, std::sync::atomic::Ordering::Relaxed);
    if let Some(h) = sampler {
        let _ = h.await;
    }
    let (db_active_max, redis_active_max, conns_max) = *peak_max.lock().unwrap();

    let n = creds_list.len();
    let peak_success_rate = peak_ok as f64 / n as f64;
    let peak_p99_ms = if !peak_hist.is_empty() {
        peak_hist.value_at_quantile(0.99) as f64 / 1000.0
    } else {
        0.0
    };
    let conn_refused_rate = peak_conn_refused as f64 / n as f64;
    info!(
        "[reconnect_token] peak: {}/{} ok, refused/timeout={}, sdk_state_err={}, other_err={}, p99={:.2}ms, elapsed={:.2}s, db_max={:.0} conns_max={:.0}",
        peak_ok, n, peak_conn_refused, peak_state_errors, peak_other_errors, peak_p99_ms,
        peak_elapsed.as_secs_f64(), db_active_max, conns_max
    );

    // === Assertions（G8 真实重连路径）===
    report.assert_gte("warmup_token_rate", warmup_rate, 0.95);
    report.assert_gte("reconnect_peak_success_rate", peak_success_rate, 0.90);
    if peak_ok > 0 {
        report.assert_lte("reconnect_peak_p99_ms", peak_p99_ms, 5000.0);
    } else {
        report.skip("reconnect_peak_p99_ms", "no successful peak reconnects");
    }
    report.assert_lte("reconnect_conn_refused_rate", conn_refused_rate, 0.05);
    let n_samples = metrics_samples.load(std::sync::atomic::Ordering::Relaxed);
    if metrics_url.is_some() {
        report.assert_gte(
            "metrics_sampled",
            if n_samples > 0 { 1.0 } else { 0.0 },
            1.0,
        );
    }
    let _ = (redis_active_max, db_active_max, conns_max);

    Ok(AuthPeakResult {
        report,
        baseline_p99_ms: 0.0, // 本场景无延迟 baseline 阶段
        peak_p99_ms,
        baseline_success_rate: warmup_rate,
        peak_success_rate,
    })
}
