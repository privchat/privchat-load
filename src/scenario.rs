//! 压测场景：主账号建群/加好友/发消息，按 rate 控制
//!
//! 新 SDK API：PrivchatSdk 只暴露 rpc_call(route, body_json)，
//! 所有业务操作通过 JSON 序列化 protocol 层 request struct 发送。

use crate::config::{RateConfig, ScenarioType};
use crate::user_store::UserRecord;
use hdrhistogram::Histogram;
use privchat_protocol::rpc::account::search::{AccountSearchQueryRequest, AccountSearchResponse};
use privchat_protocol::rpc::contact::friend::{FriendAcceptRequest, FriendApplyRequest};
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::ClientSubmitRequest;
use privchat_sdk::PrivchatSdk;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{info, warn};

/// 通过 rpc_call 发送类型化请求，返回反序列化后的响应
pub async fn rpc_call_typed<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
    sdk: &PrivchatSdk,
    route: &str,
    request: &Req,
) -> Result<Resp, Box<dyn std::error::Error + Send + Sync>> {
    let body_json = serde_json::to_string(request)?;
    let raw = sdk.rpc_call(route.to_string(), body_json).await?;
    let resp: Resp = serde_json::from_str(&raw)?;
    Ok(resp)
}

/// 通过 rpc_call 发送请求，忽略响应内容（只关心成功/失败）
pub async fn rpc_call_fire(
    sdk: &PrivchatSdk,
    route: &str,
    request: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body_json = serde_json::to_string(request)?;
    let _ = sdk.rpc_call(route.to_string(), body_json).await?;
    Ok(())
}

/// 创建单个 SDK：new + connect + login
pub async fn create_sdk_for_user(
    record: &UserRecord,
    _data_dir: &Path,
    server_host: &str,
    server_port: u16,
) -> Result<Arc<PrivchatSdk>, Box<dyn std::error::Error + Send + Sync>> {
    let endpoints = crate::user_store::server_endpoints(server_host, server_port);
    let user_data_dir = _data_dir.join("users").join(&record.username);
    std::fs::create_dir_all(&user_data_dir)?;

    let config = privchat_sdk::PrivchatConfig {
        endpoints,
        connection_timeout_secs: 30,
        data_dir: user_data_dir.to_string_lossy().to_string(),
    };

    let sdk = PrivchatSdk::new(config);
    sdk.connect().await?;
    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let login_result = sdk
        .login(record.username.clone(), password, record.device_id.clone())
        .await?;
    // login is RPC-only; authenticate binds the connection identity on transport layer
    sdk.authenticate(
        login_result.user_id,
        login_result.token,
        login_result.device_id,
    )
    .await?;
    Ok(Arc::new(sdk))
}

/// 并发建连或按 rate 限速顺序建连；返回 (sdks, 建连耗时秒数)
pub async fn create_all_sdks(
    records: &[UserRecord],
    data_dir: &Path,
    server_host: &str,
    server_port: u16,
    rate: &RateConfig,
) -> Result<(Vec<Arc<PrivchatSdk>>, f64), Box<dyn std::error::Error + Send + Sync>> {
    let total = records.len();
    let data_dir = data_dir.to_path_buf();
    let server_host = server_host.to_string();
    let concurrent = rate.connect_concurrent.max(1).min(total);

    if rate.connect_per_sec > 0 {
        let mut sdks = Vec::with_capacity(total);
        let period = Duration::from_secs_f64(1.0 / rate.connect_per_sec as f64);
        let mut tick = interval(period);
        tick.tick().await;
        info!(
            "建连 {} 个账号（限速 {} 连接/秒）…",
            total, rate.connect_per_sec
        );
        let t0 = Instant::now();
        for (i, record) in records.iter().enumerate() {
            tick.tick().await;
            match create_sdk_for_user(record, &data_dir, &server_host, server_port).await {
                Ok(sdk) => sdks.push(sdk),
                Err(e) => warn!("创建 SDK 失败 [{}] {}: {}", i, record.username, e),
            }
        }
        let elapsed_secs = t0.elapsed().as_secs_f64();
        return Ok((sdks, elapsed_secs));
    }

    let sem = Arc::new(tokio::sync::Semaphore::new(concurrent));
    info!("建连 {} 个账号（并发上限 {}）…", total, concurrent);

    let t0 = Instant::now();
    let mut tasks = Vec::with_capacity(total);
    for record in records.iter() {
        let record = record.clone();
        let data_dir = data_dir.clone();
        let server_host = server_host.clone();
        let sem = sem.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            create_sdk_for_user(&record, &data_dir, &server_host, server_port).await
        }));
    }

    let mut sdks = Vec::with_capacity(total);
    for (i, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(sdk)) => sdks.push(sdk),
            Ok(Err(e)) => warn!("创建 SDK 失败 [{}]: {}", i, e),
            Err(e) => warn!("建连任务 panic [{}]: {}", i, e),
        }
    }
    let elapsed_secs = t0.elapsed().as_secs_f64();
    Ok((sdks, elapsed_secs))
}

/// 场景执行结果：发送数、错误数、延迟分布（hdrhistogram）
pub struct ScenarioStats {
    pub messages_sent: u64,
    pub errors: u64,
    pub duration_secs: f64,
    /// HdrHistogram 延迟分布（微秒精度，记录毫秒值 * 1000）
    pub latency_hist: Histogram<u64>,
}

impl Default for ScenarioStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            errors: 0,
            duration_secs: 0.0,
            // 最大记录 60_000ms (60s)，精度 3 位有效数字
            latency_hist: Histogram::new_with_max(60_000_000, 3).unwrap(),
        }
    }
}

impl ScenarioStats {
    /// 记录一次延迟（毫秒）
    pub fn record_latency_ms(&mut self, ms: f64) {
        let us = (ms * 1000.0) as u64;
        let _ = self.latency_hist.record(us);
    }

    /// 获取 p50/p95/p99 延迟（毫秒）
    pub fn p50_p95_p99(&self) -> (f64, f64, f64) {
        if self.latency_hist.is_empty() {
            return (0.0, 0.0, 0.0);
        }
        let p50 = self.latency_hist.value_at_quantile(0.50) as f64 / 1000.0;
        let p95 = self.latency_hist.value_at_quantile(0.95) as f64 / 1000.0;
        let p99 = self.latency_hist.value_at_quantile(0.99) as f64 / 1000.0;
        (p50, p95, p99)
    }

    /// 获取指定百分位延迟（毫秒）
    pub fn percentile_ms(&self, q: f64) -> f64 {
        if self.latency_hist.is_empty() {
            return 0.0;
        }
        self.latency_hist.value_at_quantile(q) as f64 / 1000.0
    }

    /// 平均延迟（毫秒）
    pub fn mean_ms(&self) -> f64 {
        if self.latency_hist.is_empty() {
            return 0.0;
        }
        self.latency_hist.mean() / 1000.0
    }

    /// TPS
    pub fn tps(&self) -> f64 {
        if self.duration_secs > 0.0 {
            self.messages_sent as f64 / self.duration_secs
        } else {
            0.0
        }
    }

    /// 错误率
    pub fn error_rate(&self) -> f64 {
        let total = self.messages_sent + self.errors;
        if total == 0 {
            0.0
        } else {
            self.errors as f64 / total as f64
        }
    }
}

/// 生成 Snowflake-like local_message_id（简单版：时间戳 + 随机）
fn next_local_message_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = chrono::Utc::now().timestamp_millis() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    (ts << 16) | (seq & 0xFFFF)
}

/// 通过 sync/submit 发送一条群消息
async fn send_group_message(
    sdk: &PrivchatSdk,
    channel_id: u64,
    content: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let req = ClientSubmitRequest {
        local_message_id: next_local_message_id(),
        channel_id,
        channel_type: 2, // group
        last_pts: 0,
        command_type: "send_message".to_string(),
        payload: serde_json::json!({
            "content": content,
            "message_type": 0
        }),
        client_timestamp: chrono::Utc::now().timestamp_millis(),
        device_id: None,
    };
    rpc_call_fire(sdk, routes::sync::SUBMIT, &req).await
}

/// group_broadcast：主账号建群并拉所有人进群，主账号按 msg_per_sec 发群消息
pub async fn run_group_broadcast(
    main_sdk: Arc<PrivchatSdk>,
    bot_user_ids: Vec<u64>,
    group_name: &str,
    rate: &RateConfig,
    duration_sec: u64,
    warmup_sec: u64,
) -> Result<ScenarioStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = ScenarioStats {
        duration_secs: duration_sec as f64,
        ..Default::default()
    };

    // 主账号建群
    let create_req = GroupCreateRequest {
        name: group_name.to_string(),
        description: None,
        member_ids: None,
        creator_id: 0, // server-filled
    };
    let create_res: GroupCreateResponse =
        match rpc_call_typed(&main_sdk, routes::group::CREATE, &create_req).await {
            Ok(res) => res,
            Err(e) => {
                warn!("建群失败: {}", e);
                stats.errors += 1;
                return Ok(stats);
            }
        };
    let group_id = create_res.group_id;
    info!("建群成功: group_id={} name={}", group_id, group_name);

    // 邀请所有机器人进群（逐个添加）
    for &uid in &bot_user_ids {
        let add_req = GroupMemberAddRequest {
            group_id,
            user_id: uid,
            role: None,
            inviter_id: 0, // server-filled
        };
        if let Err(e) = rpc_call_fire(&main_sdk, routes::group_member::ADD, &add_req).await {
            warn!("邀请 user_id={} 进群失败: {}", uid, e);
            stats.errors += 1;
        }
    }

    // warmup 阶段：发消息但不记录指标
    if warmup_sec > 0 {
        info!("预热阶段: {} 秒（不采集指标）", warmup_sec);
        let msg_interval = if rate.msg_per_sec > 0 {
            Duration::from_secs_f64(1.0 / rate.msg_per_sec as f64)
        } else {
            Duration::from_millis(100)
        };
        let mut tick = interval(msg_interval);
        tick.tick().await;
        let warmup_deadline = Instant::now() + Duration::from_secs(warmup_sec);
        while Instant::now() < warmup_deadline {
            tick.tick().await;
            let content = format!("warmup msg {}", chrono::Utc::now().timestamp_millis());
            let _ = send_group_message(&main_sdk, group_id, &content).await;
        }
        info!("预热完成，开始正式采集");
    }

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
        let content = format!(
            "privchat_load msg {}",
            chrono::Utc::now().timestamp_millis()
        );
        match send_group_message(&main_sdk, group_id, &content).await {
            Ok(_) => {
                stats.messages_sent += 1;
                stats.record_latency_ms(t0.elapsed().as_secs_f64() * 1000.0);
            }
            Err(_) => stats.errors += 1,
        }
    }

    Ok(stats)
}

/// friend_flow：主账号依次对每个 bot 发好友申请，各 bot 接受
pub async fn run_friend_flow(
    main_sdk: Arc<PrivchatSdk>,
    sdks: &[Arc<PrivchatSdk>],
    records: &[UserRecord],
) -> Result<ScenarioStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut stats = ScenarioStats::default();
    if records.len() <= 1 {
        return Ok(stats);
    }
    let main_user_id = records[0].user_id;

    for i in 1..records.len().min(sdks.len()) {
        let bot_username = &records[i].username;
        let bot_sdk = &sdks[i];

        // 搜索用户
        let search_req = AccountSearchQueryRequest {
            query: bot_username.clone(),
            page: Some(1),
            page_size: Some(10),
            from_user_id: 0, // server-filled
        };
        let search_res: AccountSearchResponse =
            match rpc_call_typed(&main_sdk, routes::account_search::QUERY, &search_req).await {
                Ok(res) => res,
                Err(e) => {
                    warn!("加好友: 搜索失败 {}: {}", bot_username, e);
                    stats.errors += 1;
                    continue;
                }
            };

        let searched_user = match search_res.users.first() {
            Some(u) => u,
            None => {
                warn!("加好友: 搜索无结果 {}", bot_username);
                stats.errors += 1;
                continue;
            }
        };
        let to_user_id = searched_user.user_id;

        // 发送好友申请
        let apply_req = FriendApplyRequest {
            target_user_id: to_user_id,
            message: Some("loadtest".to_string()),
            source: Some("search".to_string()),
            source_id: Some(searched_user.search_session_id.to_string()),
            from_user_id: 0, // server-filled
        };
        if let Err(e) = rpc_call_fire(&main_sdk, routes::friend::APPLY, &apply_req).await {
            warn!(
                "加好友: 发申请失败 {} -> {}: {}",
                records[0].username, bot_username, e
            );
            stats.errors += 1;
            continue;
        }

        // bot 接受好友申请
        let accept_req = FriendAcceptRequest {
            from_user_id: main_user_id,
            message: None,
            target_user_id: 0, // server-filled
        };
        if let Err(e) = rpc_call_fire(bot_sdk, routes::friend::ACCEPT, &accept_req).await {
            warn!(
                "加好友: 接受失败 {} <- {}: {}",
                bot_username, records[0].username, e
            );
            stats.errors += 1;
            continue;
        }
        stats.messages_sent += 1;
        info!("加好友成功: {} <-> {}", records[0].username, bot_username);
    }

    Ok(stats)
}

/// 根据场景类型执行对应逻辑（入口）
pub async fn run_scenario(
    scenario_type: ScenarioType,
    sdks: &[Arc<PrivchatSdk>],
    records: &[UserRecord],
    rate: &RateConfig,
    duration_sec: u64,
    warmup_sec: u64,
) -> Result<ScenarioStats, Box<dyn std::error::Error + Send + Sync>> {
    if sdks.is_empty() || records.is_empty() {
        return Ok(ScenarioStats::default());
    }
    let main_sdk = sdks[0].clone();
    let bot_user_ids: Vec<u64> = records.iter().skip(1).map(|r| r.user_id).collect();
    let group_name = format!("privchat_load_grp_{}", chrono::Utc::now().timestamp());

    match scenario_type {
        ScenarioType::GroupBroadcast => {
            run_group_broadcast(
                main_sdk,
                bot_user_ids,
                &group_name,
                rate,
                duration_sec,
                warmup_sec,
            )
            .await
        }
        ScenarioType::FriendFlow => run_friend_flow(main_sdk, sdks, records).await,
        ScenarioType::SemanticVerify | ScenarioType::Backpressure | ScenarioType::AuthPeak => {
            // Handled separately in main.rs
            Ok(ScenarioStats::default())
        }
        ScenarioType::PrivateChat | ScenarioType::FanoutStress | ScenarioType::Mixed => {
            run_group_broadcast(
                main_sdk,
                bot_user_ids,
                &group_name,
                rate,
                duration_sec,
                warmup_sec,
            )
            .await
        }
    }
}
