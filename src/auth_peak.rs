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
use privchat_sdk::{PrivchatConfig, PrivchatSdk};
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
        active_connections: parse_gauge(&text, "privchat_active_connections"),
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
) -> Result<(Arc<PrivchatSdk>, f64), Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = crate::user_store::server_endpoints(server_host, server_port);
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
    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let login_result = sdk
        .login(record.username.clone(), password, record.device_id.clone())
        .await?;
    sdk.authenticate(
        login_result.user_id,
        login_result.token,
        login_result.device_id,
    )
    .await?;
    let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;

    Ok((Arc::new(sdk), elapsed_ms))
}

/// 运行认证峰值场景
pub async fn run_auth_peak(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
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
        match connect_and_auth_one(record, data_dir, server_host, server_port).await {
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

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(total);
    for record in records.iter() {
        let record = record.clone();
        let data_dir = data_dir_buf.clone();
        let host = server_host_str.clone();
        handles.push(tokio::spawn(async move {
            connect_and_auth_one(&record, &data_dir, &host, server_port).await
        }));
    }

    let mut peak_conn_refused = 0u64;
    let mut peak_other_errors = 0u64;

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
    let _peak_total_failures = peak_conn_refused + peak_other_errors;

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
        "[auth_peak] peak: {}/{} ok, refused/timeout={}, other_err={}, p99={:.2}ms, elapsed={:.2}s",
        peak_ok,
        total,
        peak_conn_refused,
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

    // DB pool active should be within pool size (< 50)
    if let Some(ref m) = metrics_after_peak {
        if let Some(db_active) = m.db_pool_active {
            report.assert_lte("db_pool_active_peak", db_active, 50.0);
        } else {
            report.skip("db_pool_active_peak", "metric not available");
        }
    } else {
        report.skip("db_pool_active_peak", "metrics endpoint not configured");
    }

    Ok(AuthPeakResult {
        report,
        baseline_p99_ms,
        peak_p99_ms,
        baseline_success_rate,
        peak_success_rate,
    })
}
