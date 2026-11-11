//! P0-3 拉黑两账号端到端验证 + P0-1 遗留 QR 扫码入群 21001 协议级验证。
//!
//! 拉黑阶段（A=records[0]，B=records[1]）：
//!   direct 频道 → 结为好友 → 双向基线发送 + get_difference 投递验证
//!   → B 拉黑 A → A 发必须 20402（BlockedByUser）/ B 发必须 20403（UserInBlacklist）
//!   → check/list 验证 → 解除 → 双向恢复发送 + 投递验证。
//!
//! 关键回归点是 BLACKLIST_SPEC §5「拉黑先于好友」：A/B 已是好友，拉黑不解除好友
//! 关系，但消息必须被拦——旧实现「好友直接放行」正是这里出过 bug。
//!
//! QR 阶段（C=第 3 个账号，场景内补注册）：
//!   注册 C → DB 置 user_type=1（需 LOAD_TEST_DATABASE_URL）→ C 首次 login
//!   → A 建群取 qr_key → C 扫码入群必须 21001（CHANNEL_SPEC §10.5 System User 禁入群）
//!   → B 扫码入群成功（阳性对照：证明 21001 是类型守卫而不是路由坏了）。
//!   时序保证无陈旧缓存：register/login 均不写 user_profile 缓存，C 的 user_type
//!   首次被读就是在 QR join 的 lookup_user_type，此时 DB 已是 1。
//!
//! 发送走 sync/submit：与 wire send 共用唯一策略判定 authorize_send_to_channel
//! （server rpc/sync/mod.rs），错误码经 privchat_sdk::Error::Server{code} 断言。

use crate::assertions::AssertionReport;
use crate::scenario::create_sdk_for_user;
use crate::user_store::{self, UserRecord};
use privchat_protocol::rpc::account::search::{AccountSearchQueryRequest, AccountSearchResponse};
use privchat_protocol::rpc::channel::direct::{
    GetOrCreateDirectChannelRequest, GetOrCreateDirectChannelResponse,
};
use privchat_protocol::rpc::contact::blacklist::{
    BlacklistAddRequest, BlacklistCheckRequest, BlacklistCheckResponse, BlacklistListRequest,
    BlacklistListResponse, BlacklistRemoveRequest,
};
use privchat_protocol::rpc::contact::friend::{
    FriendAcceptRequest, FriendApplyRequest, FriendCheckRequest, FriendCheckResponse,
};
use privchat_protocol::rpc::group::group::{GroupCreateRequest, GroupCreateResponse};
use privchat_protocol::rpc::group::qrcode::{
    GroupQRCodeGetRequest, GroupQRCodeGetResponse, GroupQRCodeJoinRequest, GroupQRCodeJoinResponse,
};
use privchat_protocol::rpc::routes;
use privchat_protocol::rpc::sync::{
    ClientSubmitRequest, ClientSubmitResponse, GetDifferenceRequest, GetDifferenceResponse,
    ServerDecision,
};
use privchat_sdk::{Error as SdkError, PrivchatSdk};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

const CODE_BLOCKED_BY_USER: u32 = 20402;
const CODE_USER_IN_BLACKLIST: u32 = 20403;
const CODE_SYSTEM_USER_NOT_GROUP_INVITABLE: u32 = 21001;

pub struct BlacklistE2eResult {
    pub report: AssertionReport,
}

/// 类型化 RPC，保留 SDK 原始错误类型（Error::Server{code} 断言需要）。
async fn rpc<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
    sdk: &PrivchatSdk,
    route: &str,
    req: &Req,
) -> Result<Resp, SdkError> {
    let body = serde_json::to_string(req)
        .map_err(|e| SdkError::Serialization(format!("encode {route}: {e}")))?;
    let raw = sdk.rpc_call(route.to_string(), body).await?;
    serde_json::from_str(&raw)
        .map_err(|e| SdkError::Serialization(format!("decode {route}: {e}; raw={raw}")))
}

fn server_code(err: &SdkError) -> Option<u32> {
    match err {
        SdkError::Server { code, .. } => Some(*code),
        _ => None,
    }
}

fn next_local_message_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let ts = chrono::Utc::now().timestamp_millis() as u64;
    (ts << 16) | (COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF)
}

/// sync/submit 发一条 direct 文本消息（wire channel_type=1）。
async fn submit_text(
    sdk: &PrivchatSdk,
    channel_id: u64,
    content: &str,
) -> Result<ClientSubmitResponse, SdkError> {
    let req = ClientSubmitRequest {
        local_message_id: next_local_message_id(),
        channel_id,
        channel_type: 1,
        last_pts: 0,
        command_type: "send_message".to_string(),
        payload: serde_json::json!({"content": content, "message_type": 0}),
        client_timestamp: chrono::Utc::now().timestamp_millis(),
        device_id: None,
    };
    rpc(sdk, routes::sync::SUBMIT, &req).await
}

fn accepted(resp: &ClientSubmitResponse) -> bool {
    matches!(
        resp.decision,
        ServerDecision::Accepted | ServerDecision::Transformed { .. }
    ) && resp.pts.is_some()
}

/// 对端 get_difference 是否含指定 pts（投递验证）。
async fn peer_has_pts(sdk: &PrivchatSdk, channel_id: u64, pts: u64) -> Result<bool, SdkError> {
    let req = GetDifferenceRequest {
        channel_id,
        channel_type: 1,
        last_pts: pts.saturating_sub(1),
        limit: Some(50),
    };
    let resp: GetDifferenceResponse = rpc(sdk, routes::sync::GET_DIFFERENCE, &req).await?;
    Ok(resp.commits.iter().any(|c| c.pts == pts))
}

/// 断言发送被指定服务端错误码拒绝。
fn expect_send_code(
    report: &mut AssertionReport,
    name: &str,
    result: Result<ClientSubmitResponse, SdkError>,
    expected: u32,
) {
    match result {
        Err(e) if server_code(&e) == Some(expected) => {
            report.check(name, true, &format!("code={expected}"), &format!("code={expected}"));
        }
        Err(e) => report.check(name, false, &format!("{e}"), &format!("code={expected}")),
        Ok(resp) => report.check(
            name,
            false,
            &format!("accepted pts={:?}", resp.pts),
            &format!("code={expected}"),
        ),
    }
}

/// 断言发送成功（accepted + pts）。
fn expect_send_ok(
    report: &mut AssertionReport,
    name: &str,
    result: Result<ClientSubmitResponse, SdkError>,
) -> Option<u64> {
    match result {
        Ok(resp) if accepted(&resp) => {
            report.check(name, true, &format!("pts={:?}", resp.pts), "accepted+pts");
            resp.pts
        }
        Ok(resp) => {
            report.check(name, false, &format!("decision={:?}", resp.decision), "accepted+pts");
            None
        }
        Err(e) => {
            report.check(name, false, &format!("{e}"), "accepted+pts");
            None
        }
    }
}

pub async fn run_blacklist_e2e(
    records: &[UserRecord],
    data_dir: &Path,
    users_file: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    password: &str,
) -> Result<BlacklistE2eResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut report = AssertionReport::new();
    if records.len() < 2 {
        report.fail("precondition", "blacklist_e2e requires at least 2 users");
        return Ok(BlacklistE2eResult { report });
    }
    let (a, b) = (&records[0], &records[1]);
    info!(
        "[blacklist_e2e] A={}({}) B={}({})",
        a.username, a.user_id, b.username, b.user_id
    );

    let sdk_a = match create_sdk_for_user(a, data_dir, server_host, server_port, server_protocol)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            report.fail("connect_a", &format!("{e}"));
            return Ok(BlacklistE2eResult { report });
        }
    };
    let sdk_b = match create_sdk_for_user(b, data_dir, server_host, server_port, server_protocol)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            report.fail("connect_b", &format!("{e}"));
            sdk_a.shutdown().await;
            return Ok(BlacklistE2eResult { report });
        }
    };
    report.check("connect_a_b", true, "2/2", "2/2");

    // --- direct 频道 ---
    let channel_id = match rpc::<_, GetOrCreateDirectChannelResponse>(
        &sdk_a,
        routes::channel::DIRECT_GET_OR_CREATE,
        &GetOrCreateDirectChannelRequest {
            target_user_id: b.user_id,
            source: None,
            source_id: None,
            user_id: 0,
        },
    )
    .await
    {
        Ok(resp) => {
            report.check(
                "direct_channel",
                true,
                &format!("channel_id={} created={}", resp.channel_id, resp.created),
                "channel_id>0",
            );
            resp.channel_id
        }
        Err(e) => {
            report.fail("direct_channel", &format!("{e}"));
            sdk_a.shutdown().await;
            sdk_b.shutdown().await;
            return Ok(BlacklistE2eResult { report });
        }
    };

    // --- 结为好友（幂等：已是好友则跳过 apply/accept）---
    // 拉黑先于好友判定（BLACKLIST_SPEC §5）——好友身份下被拉黑仍须拦截，是本场景的核心回归。
    let friendship = establish_friendship(&sdk_a, &sdk_b, a, b).await;
    match friendship {
        Ok(()) => report.check("friendship_ready", true, "is_friend=true", "is_friend=true"),
        Err(e) => {
            report.fail("friendship_ready", &format!("{e}"));
            sdk_a.shutdown().await;
            sdk_b.shutdown().await;
            return Ok(BlacklistE2eResult { report });
        }
    }

    // --- 基线：双向发送 + 投递 ---
    let pts_a = expect_send_ok(
        &mut report,
        "baseline_a_send",
        submit_text(&sdk_a, channel_id, "baseline-a").await,
    );
    let pts_b = expect_send_ok(
        &mut report,
        "baseline_b_send",
        submit_text(&sdk_b, channel_id, "baseline-b").await,
    );
    if let Some(pts) = pts_a {
        match peer_has_pts(&sdk_b, channel_id, pts).await {
            Ok(true) => report.check("baseline_delivery_to_b", true, &format!("pts={pts}"), "received"),
            Ok(false) => report.check("baseline_delivery_to_b", false, &format!("pts={pts} missing"), "received"),
            Err(e) => report.check("baseline_delivery_to_b", false, &format!("{e}"), "received"),
        }
    }
    if let Some(pts) = pts_b {
        match peer_has_pts(&sdk_a, channel_id, pts).await {
            Ok(true) => report.check("baseline_delivery_to_a", true, &format!("pts={pts}"), "received"),
            Ok(false) => report.check("baseline_delivery_to_a", false, &format!("pts={pts} missing"), "received"),
            Err(e) => report.check("baseline_delivery_to_a", false, &format!("{e}"), "received"),
        }
    }
    if pts_a.is_none() || pts_b.is_none() {
        report.fail("baseline_precondition", "baseline sends failed; block assertions meaningless");
        sdk_a.shutdown().await;
        sdk_b.shutdown().await;
        return Ok(BlacklistE2eResult { report });
    }

    // --- B 拉黑 A ---
    match rpc::<_, bool>(
        &sdk_b,
        routes::blacklist::ADD,
        &BlacklistAddRequest {
            user_id: b.user_id,
            blocked_user_id: a.user_id,
        },
    )
    .await
    {
        Ok(true) => report.check("blacklist_add", true, "true", "true"),
        Ok(false) => report.check("blacklist_add", false, "false", "true"),
        Err(e) => report.fail("blacklist_add", &format!("{e}")),
    }

    // 双向拦截：A 被对方拉黑 → 20402；B 自己拉黑了对方 → 20403。
    expect_send_code(
        &mut report,
        "blocked_a_send_20402",
        submit_text(&sdk_a, channel_id, "blocked-a").await,
        CODE_BLOCKED_BY_USER,
    );
    expect_send_code(
        &mut report,
        "blocked_b_send_20403",
        submit_text(&sdk_b, channel_id, "blocked-b").await,
        CODE_USER_IN_BLACKLIST,
    );

    // check / list 状态验证（单向：B→A 拉黑，A→B 未拉黑）。
    match rpc::<_, BlacklistCheckResponse>(
        &sdk_b,
        routes::blacklist::CHECK,
        &BlacklistCheckRequest {
            user_id: b.user_id,
            target_user_id: a.user_id,
        },
    )
    .await
    {
        Ok(resp) => report.check("blacklist_check_b_to_a", resp.blocked, &format!("blocked={}", resp.blocked), "blocked=true"),
        Err(e) => report.check("blacklist_check_b_to_a", false, &format!("{e}"), "blocked=true"),
    }
    match rpc::<_, BlacklistCheckResponse>(
        &sdk_a,
        routes::blacklist::CHECK,
        &BlacklistCheckRequest {
            user_id: a.user_id,
            target_user_id: b.user_id,
        },
    )
    .await
    {
        Ok(resp) => report.check("blacklist_check_a_to_b_one_way", !resp.blocked, &format!("blocked={}", resp.blocked), "blocked=false"),
        Err(e) => report.check("blacklist_check_a_to_b_one_way", false, &format!("{e}"), "blocked=false"),
    }
    match rpc::<_, BlacklistListResponse>(
        &sdk_b,
        routes::blacklist::LIST,
        &BlacklistListRequest { user_id: b.user_id },
    )
    .await
    {
        Ok(resp) => {
            let contains = resp.users.iter().any(|u| u.blocked_user_id == a.user_id);
            report.check("blacklist_list_contains_a", contains, &format!("len={} contains={}", resp.users.len(), contains), "contains A");
        }
        Err(e) => report.check("blacklist_list_contains_a", false, &format!("{e}"), "contains A"),
    }

    // --- 解除拉黑 ---
    match rpc::<_, bool>(
        &sdk_b,
        routes::blacklist::REMOVE,
        &BlacklistRemoveRequest {
            user_id: b.user_id,
            blocked_user_id: a.user_id,
        },
    )
    .await
    {
        Ok(true) => report.check("blacklist_remove", true, "true", "true"),
        Ok(false) => report.check("blacklist_remove", false, "false", "true"),
        Err(e) => report.fail("blacklist_remove", &format!("{e}")),
    }
    match rpc::<_, BlacklistCheckResponse>(
        &sdk_b,
        routes::blacklist::CHECK,
        &BlacklistCheckRequest {
            user_id: b.user_id,
            target_user_id: a.user_id,
        },
    )
    .await
    {
        Ok(resp) => report.check("blacklist_check_after_remove", !resp.blocked, &format!("blocked={}", resp.blocked), "blocked=false"),
        Err(e) => report.check("blacklist_check_after_remove", false, &format!("{e}"), "blocked=false"),
    }

    // --- 恢复：双向发送 + 投递 ---
    let pts_a2 = expect_send_ok(
        &mut report,
        "recovered_a_send",
        submit_text(&sdk_a, channel_id, "recovered-a").await,
    );
    expect_send_ok(
        &mut report,
        "recovered_b_send",
        submit_text(&sdk_b, channel_id, "recovered-b").await,
    );
    if let Some(pts) = pts_a2 {
        match peer_has_pts(&sdk_b, channel_id, pts).await {
            Ok(true) => report.check("recovered_delivery_to_b", true, &format!("pts={pts}"), "received"),
            Ok(false) => report.check("recovered_delivery_to_b", false, &format!("pts={pts} missing"), "received"),
            Err(e) => report.check("recovered_delivery_to_b", false, &format!("{e}"), "received"),
        }
    }

    // --- QR 阶段（P0-1 遗留：21001 协议级验证）---
    run_qr_phase(
        &mut report,
        &sdk_a,
        &sdk_b,
        data_dir,
        users_file,
        server_host,
        server_port,
        server_protocol,
        password,
    )
    .await;

    sdk_a.shutdown().await;
    sdk_b.shutdown().await;
    Ok(BlacklistE2eResult { report })
}

/// A 搜索 B → apply(source=search) → B accept；已是好友则直接返回。
async fn establish_friendship(
    sdk_a: &PrivchatSdk,
    sdk_b: &PrivchatSdk,
    a: &UserRecord,
    b: &UserRecord,
) -> Result<(), SdkError> {
    let check: FriendCheckResponse = rpc(
        sdk_a,
        routes::friend::CHECK,
        &FriendCheckRequest {
            friend_id: b.user_id,
            user_id: 0,
        },
    )
    .await?;
    if check.is_friend {
        return Ok(());
    }

    let search: AccountSearchResponse = rpc(
        sdk_a,
        routes::account_search::QUERY,
        &AccountSearchQueryRequest {
            query: b.username.clone(),
            page: Some(1),
            page_size: Some(10),
            from_user_id: 0,
        },
    )
    .await?;
    let searched = search
        .users
        .iter()
        .find(|u| u.user_id == b.user_id)
        .ok_or_else(|| {
            SdkError::Serialization(format!("search did not return user {}", b.user_id))
        })?;

    rpc::<_, serde_json::Value>(
        sdk_a,
        routes::friend::APPLY,
        &FriendApplyRequest {
            target_user_id: b.user_id,
            message: Some("blacklist_e2e".to_string()),
            source: Some("search".to_string()),
            source_id: Some(searched.search_session_id.to_string()),
            from_user_id: 0,
            grant_id: None,
        },
    )
    .await?;

    rpc::<_, serde_json::Value>(
        sdk_b,
        routes::friend::ACCEPT,
        &FriendAcceptRequest {
            from_user_id: a.user_id,
            message: None,
            target_user_id: 0,
        },
    )
    .await?;

    let check: FriendCheckResponse = rpc(
        sdk_a,
        routes::friend::CHECK,
        &FriendCheckRequest {
            friend_id: b.user_id,
            user_id: 0,
        },
    )
    .await?;
    if !check.is_friend {
        return Err(SdkError::Serialization(
            "friendship not established after accept".to_string(),
        ));
    }
    Ok(())
}

/// QR 扫码入群 21001 验证：补注册 C → DB 置 system user → 首次 login → 扫码入群被拒。
/// 缺 LOAD_TEST_DATABASE_URL 时整段 SKIP（无法把测试账号提升为 system user）。
#[allow(clippy::too_many_arguments)]
async fn run_qr_phase(
    report: &mut AssertionReport,
    sdk_a: &Arc<PrivchatSdk>,
    sdk_b: &Arc<PrivchatSdk>,
    data_dir: &Path,
    users_file: &Path,
    server_host: &str,
    server_port: u16,
    server_protocol: &str,
    password: &str,
) {
    let db_url = match std::env::var("LOAD_TEST_DATABASE_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => {
            report.skip(
                "qr_phase_21001",
                "LOAD_TEST_DATABASE_URL not set — QR phase needs DB to promote account C to system user",
            );
            return;
        }
    };

    // 补注册第 3 个账号 C（幂等：users 文件已有则复用）。
    let all = match user_store::ensure_users(
        data_dir,
        users_file,
        3,
        server_host,
        server_port,
        server_protocol,
        password,
    )
    .await
    {
        Ok(all) if all.len() >= 3 => all,
        Ok(all) => {
            report.fail("qr_setup_register_c", &format!("only {} users", all.len()));
            return;
        }
        Err(e) => {
            report.fail("qr_setup_register_c", &format!("{e}"));
            return;
        }
    };
    let c = &all[2];

    // DB 置 user_type=1。必须在 C 首次 login 之前：register/login 不写 user_profile
    // 缓存，QR join 的 lookup_user_type 首读即 DB 真值，无陈旧缓存窗口。
    let promote = async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&db_url)
            .await?;
        let result = sqlx::query("UPDATE privchat_users SET user_type = 1 WHERE user_id = $1")
            .bind(c.user_id as i64)
            .execute(&pool)
            .await?;
        pool.close().await;
        Ok::<_, sqlx::Error>(result.rows_affected())
    }
    .await;
    match promote {
        Ok(1) => report.check(
            "qr_setup_system_user",
            true,
            &format!("user_id={} user_type=1", c.user_id),
            "rows_affected=1",
        ),
        Ok(n) => {
            report.fail("qr_setup_system_user", &format!("rows_affected={n}"));
            return;
        }
        Err(e) => {
            report.fail("qr_setup_system_user", &format!("{e}"));
            return;
        }
    }

    let sdk_c = match create_sdk_for_user(c, data_dir, server_host, server_port, server_protocol)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            report.fail("qr_setup_connect_c", &format!("{e}"));
            return;
        }
    };

    let group: GroupCreateResponse = match rpc::<_, GroupCreateResponse>(
        sdk_a,
        routes::group::CREATE,
        &GroupCreateRequest {
            name: format!("blacklist-e2e-qr-{}", chrono::Utc::now().timestamp_millis()),
            description: Some("P0-3 QR 21001 verification".to_string()),
            member_ids: None,
            creator_id: 0,
        },
    )
    .await
    {
        Ok(g) => {
            report.check("qr_group_create", true, &format!("group_id={}", g.group_id), "group_id>0");
            g
        }
        Err(e) => {
            report.fail("qr_group_create", &format!("{e}"));
            sdk_c.shutdown().await;
            return;
        }
    };

    let qr_key = match rpc::<_, GroupQRCodeGetResponse>(
        sdk_a,
        routes::group_qrcode::GET,
        &GroupQRCodeGetRequest {
            group_id: group.group_id,
            operator_id: 0,
        },
    )
    .await
    {
        Ok(resp) if !resp.qr_key.is_empty() => {
            report.check("qr_key_get", true, &format!("qr_key_len={}", resp.qr_key.len()), "non-empty");
            resp.qr_key
        }
        Ok(resp) => {
            report.check("qr_key_get", false, &format!("qr_key={:?}", resp.qr_key), "non-empty");
            sdk_c.shutdown().await;
            return;
        }
        Err(e) => {
            report.fail("qr_key_get", &format!("{e}"));
            sdk_c.shutdown().await;
            return;
        }
    };

    // C（system user）扫码入群 → 必须 21001。
    let join_req = GroupQRCodeJoinRequest {
        qr_key: qr_key.clone(),
        message: Some("blacklist_e2e qr phase".to_string()),
        user_id: 0,
    };
    match rpc::<_, GroupQRCodeJoinResponse>(&sdk_c, routes::group_qrcode::JOIN, &join_req).await {
        Err(e) if server_code(&e) == Some(CODE_SYSTEM_USER_NOT_GROUP_INVITABLE) => report.check(
            "qr_system_user_join_21001",
            true,
            &format!("code={CODE_SYSTEM_USER_NOT_GROUP_INVITABLE}"),
            &format!("code={CODE_SYSTEM_USER_NOT_GROUP_INVITABLE}"),
        ),
        Err(e) => report.check(
            "qr_system_user_join_21001",
            false,
            &format!("{e}"),
            &format!("code={CODE_SYSTEM_USER_NOT_GROUP_INVITABLE}"),
        ),
        Ok(resp) => report.check(
            "qr_system_user_join_21001",
            false,
            &format!("status={}", resp.status),
            &format!("code={CODE_SYSTEM_USER_NOT_GROUP_INVITABLE}"),
        ),
    }

    // 阳性对照：普通用户 B 扫同一个码必须入群成功——证明 21001 是类型守卫而非路由故障。
    match rpc::<_, GroupQRCodeJoinResponse>(sdk_b, routes::group_qrcode::JOIN, &join_req).await {
        Ok(resp) => report.check(
            "qr_normal_user_join",
            resp.status == "joined",
            &format!("status={}", resp.status),
            "status=joined",
        ),
        Err(e) => report.check("qr_normal_user_join", false, &format!("{e}"), "status=joined"),
    }

    sdk_c.shutdown().await;
}