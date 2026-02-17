//! 场景 B：Backpressure 验证
//!
//! 验证慢消费者不会拖垮快连接：
//! 1. 建群 → 拉所有 bot 进群
//! 2. 将 bot 分为 fast 组和 slow 组
//! 3. slow 组 bot 启动后台任务模拟慢消费（subscribe_events + sleep 500ms）
//! 4. 主账号按速率发消息，记录发送延迟
//! 5. 采集 RSS 内存快照
//! 6. 断言：发送端 p99 延迟 < 阈值（慢消费者不影响发送端）
//! 7. 断言：RSS 增长 < 阈值（内存不爆）

use crate::assertions::AssertionReport;
use crate::config::RateConfig;
use crate::scenario::{rpc_call_fire, rpc_call_typed};
use crate::user_store::UserRecord;
use hdrhistogram::Histogram;
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::ClientSubmitRequest;
use privchat_sdk::{PrivchatSdk, SdkEvent};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{interval, sleep};
use tracing::{info, warn};

/// RSS 内存快照（bytes）
fn rss_bytes() -> u64 {
    // macOS: use mach API via libc; fallback to 0
    #[cfg(target_os = "macos")]
    {
        use std::process;
        // read from ps command as simple cross-platform fallback
        let output = process::Command::new("ps")
            .args(["-o", "rss=", "-p", &process::id().to_string()])
            .output();
        if let Ok(out) = output {
            let s = String::from_utf8_lossy(&out.stdout);
            // ps reports RSS in KB
            if let Ok(kb) = s.trim().parse::<u64>() {
                return kb * 1024;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        // /proc/self/statm: pages; page size typically 4096
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(pages) = parts[1].parse::<u64>() {
                    return pages * 4096;
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        0
    }
}

fn rss_mb() -> f64 {
    rss_bytes() as f64 / (1024.0 * 1024.0)
}

/// 生成 local_message_id
fn next_local_message_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = chrono::Utc::now().timestamp_millis() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    (ts << 16) | (seq & 0xFFFF)
}

/// Backpressure 验证结果
pub struct BackpressureResult {
    pub report: AssertionReport,
    pub messages_sent: u64,
    pub errors: u64,
    pub p99_ms: f64,
    pub rss_start_mb: f64,
    pub rss_end_mb: f64,
}

/// 运行 Backpressure 验证场景
///
/// - `slow_count`: 慢消费者数量（默认取 bot 总数的一半，最少 1 个）
/// - `slow_delay_ms`: 慢消费者每条消息延迟（默认 500ms）
pub async fn run_backpressure(
    sdks: &[Arc<PrivchatSdk>],
    records: &[UserRecord],
    rate: &RateConfig,
    duration_sec: u64,
    slow_count: usize,
    slow_delay_ms: u64,
) -> Result<BackpressureResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let mut total_errors = 0u64;

    if sdks.len() < 2 || records.len() < 2 {
        report.skip("backpressure", "at least 2 users required");
        return Ok(BackpressureResult {
            report,
            messages_sent: 0,
            errors: 0,
            p99_ms: 0.0,
            rss_start_mb: 0.0,
            rss_end_mb: 0.0,
        });
    }

    let main_sdk = &sdks[0];
    let bot_sdks = &sdks[1..];
    let bot_user_ids: Vec<u64> = records[1..].iter().map(|r| r.user_id).collect();
    let actual_slow_count = slow_count.min(bot_sdks.len()).max(1);

    info!(
        "[backpressure] bots={}, slow={}, slow_delay={}ms, duration={}s",
        bot_sdks.len(),
        actual_slow_count,
        slow_delay_ms,
        duration_sec
    );

    // === Step 1: 建群 ===
    let group_name = format!("bp_verify_{}", chrono::Utc::now().timestamp());
    let create_req = GroupCreateRequest {
        name: group_name.clone(),
        description: None,
        member_ids: None,
        creator_id: 0,
    };
    let create_res: GroupCreateResponse =
        rpc_call_typed(main_sdk, routes::group::CREATE, &create_req).await?;
    let group_id = create_res.group_id;
    info!("[backpressure] group created: group_id={}", group_id);

    // === Step 2: 拉 bot 进群 ===
    for &uid in &bot_user_ids {
        let add_req = GroupMemberAddRequest {
            group_id,
            user_id: uid,
            role: None,
            inviter_id: 0,
        };
        if let Err(e) = rpc_call_fire(main_sdk, routes::group_member::ADD, &add_req).await {
            warn!("[backpressure] add member {} failed: {}", uid, e);
            total_errors += 1;
        }
    }

    // === Step 3: 启动慢消费者 ===
    info!(
        "[backpressure] starting {} slow consumers (delay={}ms)",
        actual_slow_count, slow_delay_ms
    );
    let mut slow_handles = Vec::new();
    for (idx, bot_sdk) in bot_sdks.iter().take(actual_slow_count).enumerate() {
        let mut rx = bot_sdk.subscribe_events();
        let delay = Duration::from_millis(slow_delay_ms);
        let handle = tokio::spawn(async move {
            let mut count = 0u64;
            loop {
                match rx.recv().await {
                    Ok(SdkEvent::TimelineUpdated { .. }) => {
                        // simulate slow processing
                        sleep(delay).await;
                        count += 1;
                    }
                    Ok(_) => {} // ignore other events
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        // expected under backpressure
                        count += n;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            (idx, count)
        });
        slow_handles.push(handle);
    }

    // === Step 4: 采集 RSS 起始值 ===
    let rss_start = rss_mb();
    info!("[backpressure] RSS start: {:.1} MB", rss_start);

    // === Step 5: 主账号按速率发消息 ===
    let mut latency_hist: Histogram<u64> = Histogram::new_with_max(60_000_000, 3).unwrap();
    let mut messages_sent = 0u64;

    let msg_interval = if rate.msg_per_sec > 0 {
        Duration::from_secs_f64(1.0 / rate.msg_per_sec as f64)
    } else {
        Duration::from_millis(100)
    };
    let mut tick = interval(msg_interval);
    tick.tick().await;

    let deadline = Instant::now() + Duration::from_secs(duration_sec);
    while Instant::now() < deadline {
        tick.tick().await;
        let t0 = Instant::now();
        let req = ClientSubmitRequest {
            local_message_id: next_local_message_id(),
            channel_id: group_id,
            channel_type: 2,
            last_pts: 0,
            command_type: "send_message".to_string(),
            payload: serde_json::json!({
                "content": format!("bp_msg_{}", messages_sent),
                "message_type": 0
            }),
            client_timestamp: chrono::Utc::now().timestamp_millis(),
            device_id: None,
        };
        match rpc_call_fire(main_sdk, routes::sync::SUBMIT, &req).await {
            Ok(_) => {
                messages_sent += 1;
                let latency_us = (t0.elapsed().as_secs_f64() * 1_000_000.0) as u64;
                let _ = latency_hist.record(latency_us);
            }
            Err(_) => total_errors += 1,
        }

        // 每 100 条消息采集一次 RSS
        if messages_sent % 100 == 0 {
            let rss_now = rss_mb();
            info!(
                "[backpressure] progress: sent={} errors={} rss={:.1}MB",
                messages_sent, total_errors, rss_now
            );
        }
    }

    // === Step 6: 采集 RSS 结束值 ===
    let rss_end = rss_mb();
    info!("[backpressure] RSS end: {:.1} MB", rss_end);

    // === Step 7: 停止慢消费者（通过 drop SDK 的事件订阅自然结束）===
    // slow_handles will be dropped, tasks will end when broadcast sender is dropped

    // === Step 8: 计算指标 ===
    let p99_ms = if !latency_hist.is_empty() {
        latency_hist.value_at_quantile(0.99) as f64 / 1000.0
    } else {
        0.0
    };
    let p50_ms = if !latency_hist.is_empty() {
        latency_hist.value_at_quantile(0.50) as f64 / 1000.0
    } else {
        0.0
    };
    let mean_ms = if !latency_hist.is_empty() {
        latency_hist.mean() / 1000.0
    } else {
        0.0
    };
    let rss_growth_mb = rss_end - rss_start;

    info!(
        "[backpressure] result: sent={} errors={} p50={:.2}ms p99={:.2}ms mean={:.2}ms rss_growth={:.1}MB",
        messages_sent, total_errors, p50_ms, p99_ms, mean_ms, rss_growth_mb
    );

    // === Step 9: 断言 ===

    // 发送端 p99 延迟 < 1000ms（慢消费者不应拖垮发送端）
    if messages_sent > 0 {
        report.assert_lte("sender_p99_latency_ms", p99_ms, 1000.0);
    } else {
        report.skip("sender_p99_latency_ms", "no messages sent");
    }

    // RSS 增长 < 200MB（内存不爆）
    if rss_start > 0.0 {
        report.assert_lte("rss_growth_mb", rss_growth_mb, 200.0);
    } else {
        report.skip("rss_growth_mb", "RSS not available on this platform");
    }

    // 错误率 < 10%
    let error_rate = if messages_sent + total_errors > 0 {
        total_errors as f64 / (messages_sent + total_errors) as f64
    } else {
        0.0
    };
    report.assert_lte("error_rate", error_rate, 0.10);

    // TPS > 50（即使有慢消费者，发送端吞吐不应太低）
    let tps = if duration_sec > 0 {
        messages_sent as f64 / duration_sec as f64
    } else {
        0.0
    };
    if messages_sent > 0 {
        report.assert_gte("sender_tps", tps, 50.0);
    } else {
        report.skip("sender_tps", "no messages sent");
    }

    Ok(BackpressureResult {
        report,
        messages_sent,
        errors: total_errors,
        p99_ms,
        rss_start_mb: rss_start,
        rss_end_mb: rss_end,
    })
}
