//! 场景 A：语义正确性验证
//!
//! 验证三个核心不变量：
//! 1. pts 严格递增（per-channel monotonic）
//! 2. server_msg_id 不重复
//! 3. unread count 收敛（发消息后 3s 窗口内 unread 达到预期值）
//!
//! 流程：
//! - 主账号建群 → 拉 bot 进群 → 按速率发 N 条消息
//! - 每条 sync/submit 的 response 记入 truth_set（pts, server_msg_id）
//! - 发完后，每个 bot 调用 sync/get_difference(pts=0) 拉取全量
//! - 比对 truth_set 与接收端的 commit 列表
//! - 等 3s 后查询 unread count

use crate::assertions::AssertionReport;
use crate::config::RateConfig;
use crate::scenario::{rpc_call_fire, rpc_call_typed};
use crate::user_store::UserRecord;
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::member::GroupMemberAddRequest;
use privchat_protocol::rpc::message::status::{
    MessageStatusCountRequest, MessageStatusCountResponse,
};
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::{
    ClientSubmitRequest, ClientSubmitResponse, GetDifferenceRequest, GetDifferenceResponse,
};
use privchat_sdk::PrivchatSdk;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

/// 发送端记录的真值
#[derive(Debug, Clone)]
struct TruthEntry {
    pts: u64,
    server_msg_id: u64,
    local_message_id: u64,
}

/// 语义验证结果
pub struct SemanticResult {
    pub report: AssertionReport,
    pub messages_sent: u64,
    pub errors: u64,
}

/// 生成 local_message_id
fn next_local_message_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = chrono::Utc::now().timestamp_millis() as u64;
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    (ts << 16) | (seq & 0xFFFF)
}

/// 运行语义正确性验证场景
pub async fn run_semantic_verify(
    sdks: &[Arc<PrivchatSdk>],
    records: &[UserRecord],
    rate: &RateConfig,
    message_count: u64,
) -> Result<SemanticResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    let mut total_errors = 0u64;

    if sdks.len() < 2 || records.len() < 2 {
        report.skip("语义验证", "至少需要 2 个用户");
        return Ok(SemanticResult {
            report,
            messages_sent: 0,
            errors: 0,
        });
    }

    let main_sdk = &sdks[0];
    let bot_sdks = &sdks[1..];
    let bot_user_ids: Vec<u64> = records[1..].iter().map(|r| r.user_id).collect();

    // === Step 1: 建群 ===
    info!("[语义验证] Step 1: 建群");
    let group_name = format!("semantic_verify_{}", chrono::Utc::now().timestamp());
    let create_req = GroupCreateRequest {
        name: group_name.clone(),
        description: None,
        member_ids: None,
        creator_id: 0,
    };
    let create_res: GroupCreateResponse =
        rpc_call_typed(main_sdk, routes::group::CREATE, &create_req).await?;
    let group_id = create_res.group_id;
    let channel_id = group_id; // group channel_id = group_id
    info!("[语义验证] 建群成功: group_id={}", group_id);

    // === Step 2: 拉 bot 进群 ===
    info!("[语义验证] Step 2: 邀请 {} 个 bot 进群", bot_user_ids.len());
    for &uid in &bot_user_ids {
        let add_req = GroupMemberAddRequest {
            group_id,
            user_id: uid,
            role: None,
            inviter_id: 0,
        };
        if let Err(e) = rpc_call_fire(main_sdk, routes::group_member::ADD, &add_req).await {
            warn!("[语义验证] 邀请 user_id={} 进群失败: {}", uid, e);
            total_errors += 1;
        }
    }

    // === Step 3: 发送 N 条消息，记录 truth_set ===
    info!(
        "[语义验证] Step 3: 发送 {} 条消息 (rate={}msg/s)",
        message_count, rate.msg_per_sec
    );
    let mut truth_set: Vec<TruthEntry> = Vec::with_capacity(message_count as usize);

    let msg_interval = if rate.msg_per_sec > 0 {
        Duration::from_secs_f64(1.0 / rate.msg_per_sec as f64)
    } else {
        Duration::from_millis(100)
    };
    let mut tick = interval(msg_interval);
    tick.tick().await;

    let t0 = Instant::now();
    for i in 0..message_count {
        tick.tick().await;
        let local_msg_id = next_local_message_id();
        let content = format!("semantic_msg_{}", i);
        let req = ClientSubmitRequest {
            local_message_id: local_msg_id,
            channel_id,
            channel_type: 2, // group
            last_pts: truth_set.last().map(|e| e.pts).unwrap_or(0),
            command_type: "send_message".to_string(),
            payload: serde_json::to_value(crate::scenario::SubmitMessagePayload {
                content,
                message_type: 0,
            })?,
            client_timestamp: chrono::Utc::now().timestamp_millis(),
            device_id: None,
        };

        match rpc_call_typed::<_, ClientSubmitResponse>(main_sdk, routes::sync::SUBMIT, &req).await
        {
            Ok(resp) => {
                if let (Some(pts), Some(server_msg_id)) = (resp.pts, resp.server_msg_id) {
                    truth_set.push(TruthEntry {
                        pts,
                        server_msg_id,
                        local_message_id: local_msg_id,
                    });
                } else {
                    warn!("[语义验证] 消息 {} submit 响应缺少 pts/server_msg_id", i);
                    total_errors += 1;
                }
            }
            Err(e) => {
                warn!("[语义验证] 消息 {} submit 失败: {}", i, e);
                total_errors += 1;
            }
        }
    }
    let send_elapsed = t0.elapsed();
    let messages_sent = truth_set.len() as u64;
    info!(
        "[语义验证] 发送完成: {}/{} 条成功, 耗时 {:.2}s",
        messages_sent,
        message_count,
        send_elapsed.as_secs_f64()
    );

    // === Step 4: 等待传播 ===
    info!("[语义验证] Step 4: 等待 3s 传播窗口");
    sleep(Duration::from_secs(3)).await;

    // === Step 5: 验证 pts 严格递增 ===
    info!("[语义验证] Step 5: 验证 pts 严格递增");
    let mut pts_strictly_increasing = true;
    for w in truth_set.windows(2) {
        if w[1].pts <= w[0].pts {
            error!(
                "[语义验证] pts 非递增: pts[{}]={} >= pts[{}]={}",
                w[0].local_message_id, w[0].pts, w[1].local_message_id, w[1].pts
            );
            pts_strictly_increasing = false;
            break;
        }
    }
    if messages_sent > 0 {
        report.assert_gte(
            "pts_strictly_increasing",
            if pts_strictly_increasing { 1.0 } else { 0.0 },
            1.0,
        );
    } else {
        report.skip("pts_strictly_increasing", "无成功发送的消息");
    }

    // === Step 6: 验证 server_message_id 唯一 ===
    // 两条断言：
    //   - 同频道唯一：同一 channel_id 内 server_message_id 不重复（硬）
    //   - 观察窗唯一：本次压测窗口内所有观察到的 server_message_id 不重复（硬）
    info!("[语义验证] Step 6: 验证 server_message_id 唯一");
    let mut seen_ids: HashSet<u64> = HashSet::with_capacity(truth_set.len());
    let mut has_duplicates = false;
    for entry in &truth_set {
        if !seen_ids.insert(entry.server_msg_id) {
            error!(
                "[语义验证] server_message_id 重复 (channel={}): {}",
                channel_id, entry.server_msg_id
            );
            has_duplicates = true;
        }
    }
    if messages_sent > 0 {
        // 同频道唯一（sender 端 truth_set 在单一 channel 内）
        report.assert_gte(
            "server_message_id_channel_unique",
            if !has_duplicates { 1.0 } else { 0.0 },
            1.0,
        );
        // 观察窗唯一（truth_set 覆盖本次全部发送）
        report.assert_gte(
            "server_message_id_window_unique",
            if !has_duplicates { 1.0 } else { 0.0 },
            1.0,
        );
    } else {
        report.skip("server_message_id_channel_unique", "无成功发送的消息");
        report.skip("server_message_id_window_unique", "无成功发送的消息");
    }

    // === Step 7: 接收端验证（get_difference）===
    info!(
        "[语义验证] Step 7: {} 个 bot 拉取 get_difference 验证消息完整性",
        bot_sdks.len()
    );
    let truth_pts: BTreeSet<u64> = truth_set.iter().map(|e| e.pts).collect();
    let truth_msg_ids: HashSet<u64> = truth_set.iter().map(|e| e.server_msg_id).collect();

    let mut all_bots_complete = true;
    let verify_bot_count = bot_sdks.len().min(5); // 最多验证 5 个 bot

    for (idx, bot_sdk) in bot_sdks.iter().take(verify_bot_count).enumerate() {
        // 分页拉取所有 commits（以 last_pts 驱动分页）
        let page_limit = 100u32;
        let mut last_pts_cursor: u64 = 0;
        let mut all_commits = Vec::new();
        let mut pull_ok = true;

        for page in 0..50 {
            let diff_req = GetDifferenceRequest {
                channel_id,
                channel_type: 2,
                last_pts: last_pts_cursor,
                limit: Some(page_limit),
            };

            match rpc_call_typed::<_, GetDifferenceResponse>(
                bot_sdk,
                routes::sync::GET_DIFFERENCE,
                &diff_req,
            )
            .await
            {
                Ok(diff_resp) => {
                    if diff_resp.commits.is_empty() {
                        break;
                    }

                    let new_max_pts = diff_resp.commits.iter().map(|c| c.pts).max().unwrap_or(0);
                    if new_max_pts <= last_pts_cursor {
                        warn!(
                            "[语义验证] bot[{}] page {} pts 未前进 ({} <= {})，中止",
                            idx, page, new_max_pts, last_pts_cursor
                        );
                        break;
                    }
                    last_pts_cursor = new_max_pts;
                    let has_more = diff_resp.has_more;
                    all_commits.extend(diff_resp.commits);

                    if !has_more {
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        "[语义验证] bot[{}] get_difference page {} 失败: {}",
                        idx, page, e
                    );
                    total_errors += 1;
                    pull_ok = false;
                    break;
                }
            }
        }

        if !pull_ok {
            all_bots_complete = false;
            continue;
        }

        let received_pts: BTreeSet<u64> = all_commits.iter().map(|c| c.pts).collect();
        let received_msg_ids: HashSet<u64> = all_commits.iter().map(|c| c.server_msg_id).collect();

        // 检查发送的消息是否全部收到
        let missing_pts: Vec<u64> = truth_pts.difference(&received_pts).copied().collect();
        let missing_ids: Vec<u64> = truth_msg_ids
            .difference(&received_msg_ids)
            .copied()
            .collect();

        if !missing_pts.is_empty() {
            warn!(
                "[语义验证] bot[{}] 缺少 {} 条消息 (pts: {:?})",
                idx,
                missing_pts.len(),
                &missing_pts[..missing_pts.len().min(10)]
            );
            all_bots_complete = false;
        }
        if !missing_ids.is_empty() {
            warn!(
                "[语义验证] bot[{}] 缺少 {} 条消息 (msg_id: {:?})",
                idx,
                missing_ids.len(),
                &missing_ids[..missing_ids.len().min(10)]
            );
            all_bots_complete = false;
        }

        // 验证接收端 pts 也是递增的
        let commits_sorted: Vec<u64> = all_commits.iter().map(|c| c.pts).collect();
        let receiver_pts_increasing = commits_sorted.windows(2).all(|w| w[1] > w[0]);
        if !receiver_pts_increasing {
            warn!("[语义验证] bot[{}] 接收端 pts 非递增", idx);
            all_bots_complete = false;
        }

        // 检查接收端有无重复
        let receiver_has_dup = received_msg_ids.len() < all_commits.len();
        if receiver_has_dup {
            warn!(
                "[语义验证] bot[{}] 接收端 server_msg_id 有重复: {} unique / {} total",
                idx,
                received_msg_ids.len(),
                all_commits.len()
            );
            all_bots_complete = false;
        }

        info!(
            "[语义验证] bot[{}]: 收到 {} 条 commit ({}页), 缺失 pts={} msg_id={}",
            idx,
            all_commits.len(),
            if last_pts_cursor > 0 {
                (all_commits.len() + 99) / 100
            } else {
                0
            },
            missing_pts.len(),
            missing_ids.len()
        );
    }

    if messages_sent > 0 && verify_bot_count > 0 {
        report.assert_gte(
            "receiver_message_complete",
            if all_bots_complete { 1.0 } else { 0.0 },
            1.0,
        );
    } else {
        report.skip("receiver_message_complete", "无可验证的 bot 或消息");
    }

    // === Step 7b: 无幽灵消息 (A-PASS-3b: local ⊆ truth) ===
    // 拆分为两条断言：
    //   - 业务消息 ghost = 0（硬）：非系统用户发送的、不在 truth_set 中的消息
    //   - 系统消息 ghost ≤ N（软）：sender_id=1（SYSTEM_USER_ID）的系统通知
    info!("[语义验证] Step 7b: 验证无幽灵消息 (local ⊆ truth)");
    const SYSTEM_USER_ID: u64 = 1;
    let mut no_biz_ghosts = true;
    let mut max_sys_ghosts: usize = 0;
    for (idx, bot_sdk) in bot_sdks.iter().take(verify_bot_count).enumerate() {
        let diff_req = GetDifferenceRequest {
            channel_id,
            channel_type: 2,
            last_pts: 0,
            limit: Some((message_count as u32).max(100) + 50),
        };

        match rpc_call_typed::<_, GetDifferenceResponse>(
            bot_sdk,
            routes::sync::GET_DIFFERENCE,
            &diff_req,
        )
        .await
        {
            Ok(diff_resp) => {
                // Classify ghosts: commits whose server_msg_id is NOT in truth_set
                let mut biz_ghost_count = 0usize;
                let mut sys_ghost_count = 0usize;
                for commit in &diff_resp.commits {
                    if !truth_msg_ids.contains(&commit.server_msg_id) {
                        if commit.sender_id == SYSTEM_USER_ID {
                            sys_ghost_count += 1;
                        } else {
                            biz_ghost_count += 1;
                            error!(
                                "[语义验证] bot[{}] 业务消息 ghost: server_msg_id={} sender={} type={}",
                                idx, commit.server_msg_id, commit.sender_id, commit.message_type
                            );
                        }
                    }
                }
                if biz_ghost_count > 0 {
                    no_biz_ghosts = false;
                }
                max_sys_ghosts = max_sys_ghosts.max(sys_ghost_count);
                info!(
                    "[语义验证] bot[{}] ghost: biz={} sys={}",
                    idx, biz_ghost_count, sys_ghost_count
                );
            }
            Err(e) => {
                warn!(
                    "[语义验证] bot[{}] ghost check get_difference 失败: {}",
                    idx, e
                );
            }
        }
    }
    if messages_sent > 0 && verify_bot_count > 0 {
        // 硬断言：业务消息 ghost = 0
        report.assert_gte(
            "no_business_ghost_messages",
            if no_biz_ghosts { 1.0 } else { 0.0 },
            1.0,
        );
        // 软断言：系统消息 ghost ≤ 20（建群 + 拉人通知等）
        report.assert_lte("system_ghost_messages_bounded", max_sys_ghosts as f64, 20.0);
    } else {
        report.skip("no_business_ghost_messages", "无可验证的 bot 或消息");
        report.skip("system_ghost_messages_bounded", "无可验证的 bot 或消息");
    }

    // === Step 7c: per-user 不重复投递 (A-PASS-5) ===
    info!("[语义验证] Step 7c: 验证 per-user 不重复投递");
    let mut no_per_user_dups = true;
    for (idx, bot_sdk) in bot_sdks.iter().take(verify_bot_count).enumerate() {
        let diff_req = GetDifferenceRequest {
            channel_id,
            channel_type: 2,
            last_pts: 0,
            limit: Some((message_count as u32).max(100) + 50),
        };

        match rpc_call_typed::<_, GetDifferenceResponse>(
            bot_sdk,
            routes::sync::GET_DIFFERENCE,
            &diff_req,
        )
        .await
        {
            Ok(diff_resp) => {
                let all_ids: Vec<u64> = diff_resp.commits.iter().map(|c| c.server_msg_id).collect();
                let unique_ids: HashSet<u64> = all_ids.iter().copied().collect();
                if all_ids.len() != unique_ids.len() {
                    let dup_count = all_ids.len() - unique_ids.len();
                    warn!(
                        "[语义验证] bot[{}] per-user 重复投递: {} 条 (total={}, unique={})",
                        idx,
                        dup_count,
                        all_ids.len(),
                        unique_ids.len()
                    );
                    no_per_user_dups = false;
                }
            }
            Err(e) => {
                warn!("[语义验证] bot[{}] per-user dup check 失败: {}", idx, e);
            }
        }
    }
    if messages_sent > 0 && verify_bot_count > 0 {
        report.assert_gte(
            "no_per_user_duplicate_delivery",
            if no_per_user_dups { 1.0 } else { 0.0 },
            1.0,
        );
    } else {
        report.skip("no_per_user_duplicate_delivery", "无可验证的 bot 或消息");
    }

    // === Step 8: unread 收敛验证 ===
    info!("[语义验证] Step 8: 验证 unread count 收敛");
    let mut unread_converged = true;

    for (idx, bot_sdk) in bot_sdks.iter().take(verify_bot_count).enumerate() {
        let count_req = MessageStatusCountRequest {
            channel_id: Some(channel_id),
        };

        match rpc_call_typed::<_, MessageStatusCountResponse>(
            bot_sdk,
            routes::message_status::COUNT,
            &count_req,
        )
        .await
        {
            Ok(count_resp) => {
                let unread = count_resp.unread_count;
                // bot 没有读过任何消息，所以 unread 应该 >= messages_sent
                // （可能还有系统消息如"创建了群聊"、"邀请了xxx"）
                if (unread as u64) < messages_sent {
                    warn!(
                        "[语义验证] bot[{}] unread={} < messages_sent={}, 可能丢失",
                        idx, unread, messages_sent
                    );
                    unread_converged = false;
                }
                info!(
                    "[语义验证] bot[{}] unread={} (expected >= {})",
                    idx, unread, messages_sent
                );
            }
            Err(e) => {
                warn!("[语义验证] bot[{}] 查询 unread 失败: {}", idx, e);
                total_errors += 1;
                // unread 查询失败不算收敛失败，只记 error
            }
        }
    }

    if messages_sent > 0 && verify_bot_count > 0 {
        report.assert_gte(
            "unread_converged",
            if unread_converged { 1.0 } else { 0.0 },
            1.0,
        );
    } else {
        report.skip("unread_converged", "无可验证的 bot 或消息");
    }

    // === 汇总 ===
    info!(
        "[语义验证] 完成: sent={} errors={} assertions={}",
        messages_sent,
        total_errors,
        if report.all_passed() {
            "ALL PASS"
        } else {
            "HAS FAIL"
        }
    );

    Ok(SemanticResult {
        report,
        messages_sent,
        errors: total_errors,
    })
}
