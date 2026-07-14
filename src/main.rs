//! PrivChat 压测工具
//!
//! - 配置：test.conf（同目录或 --config 指定）
//! - 数据：data/users.txt，每行 username device_id token user_id；不足则注册并追加
//! - 主账号：第一条用户；建群/加好友/发群消息等由主账号控制
//! - 环境：LOAD_TEST_ENV=local|ci 控制断言阈值

mod assertions;
mod auth_peak;
mod backpressure;
mod config;
mod scenario;
mod semantic;
mod soak;
mod user_store;

use assertions::AssertionReport;
use config::{EnvThresholds, LoadTestEnv, ScenarioType, TestConfig};
use scenario::ScenarioStats;
use std::path::PathBuf;
use tracing::{info, warn};

/// 配置路径：优先当前工作目录
fn find_config_path() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let p = cwd.join("test.conf");
    if p.exists() {
        return p;
    }
    PathBuf::from("test.conf")
}

fn find_data_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    cwd.join("data")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("privchat_load=info".parse()?),
        )
        .init();

    let config_path = std::env::args()
        .position(|a| a == "--config" || a == "-c")
        .and_then(|i| std::env::args().nth(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(find_config_path);

    if !config_path.exists() {
        eprintln!("config file not found: {}", config_path.display());
        eprintln!("   place test.conf in cwd or use: privchat-load --config /path/to/test.conf");
        std::process::exit(1);
    }

    let config = TestConfig::load(&config_path)?;
    let data_dir = find_data_dir();
    // 用户命名空间卫生：设 LOAD_TEST_USER_PREFIX=<x> → 全新 users_<x>.txt + <x>_NNNNNN 用户名，
    // 一条命令即可复现干净跑（免手工 SQL / 重建 users.txt / 撞旧 RS256 失效 token）；不设=默认 loadtest。
    let user_prefix =
        std::env::var("LOAD_TEST_USER_PREFIX").unwrap_or_else(|_| "loadtest".to_string());
    let users_file = if user_prefix == "loadtest" {
        data_dir.join("users.txt")
    } else {
        data_dir.join(format!("users_{}.txt", user_prefix))
    };
    let env = LoadTestEnv::from_env();

    info!("data_dir: {}", data_dir.display());
    info!("users_file: {}", users_file.display());
    info!(
        "server: {}:{} {}",
        config.server.host, config.server.port, config.server.protocol
    );
    info!("users: {}", config.users.total);
    info!("scenario: {}", config.scenario.type_);
    info!("duration: {} sec", config.run.duration_sec);
    info!("warmup: {} sec", config.run.warmup_sec);
    info!("env: {:?}", env);

    if config.server.allow_self_signed_quic() {
        privchat_sdk::set_quic_accept_self_signed_for_testing(true);
        warn!("QUIC self-signed certificate verification is disabled for this load-test process");
    }

    let password =
        std::env::var("LOAD_TEST_PASSWORD").unwrap_or_else(|_| "loadtest123".to_string());
    let server_protocol = config.server.effective_protocol();
    let records = user_store::ensure_users(
        &data_dir,
        &users_file,
        config.users.total,
        &config.server.host,
        config.server.port,
        &server_protocol,
        &password,
    )
    .await?;

    if records.len() < 2 {
        eprintln!("at least 2 users required (1 main + 1 bot)");
        std::process::exit(1);
    }

    info!(
        "main account: {} (user_id={})",
        records[0].username, records[0].user_id
    );
    let scenario_type = config.scenario.scenario_type();

    // auth_peak manages its own connect cycle; skip pre-building SDKs
    if scenario_type == ScenarioType::AuthPeak {
        let metrics_url = std::env::var("LOAD_TEST_METRICS_URL").ok();
        let result = auth_peak::run_auth_peak(
            &records,
            &data_dir,
            &config.server.host,
            config.server.port,
            &server_protocol,
            metrics_url.as_deref(),
        )
        .await?;

        println!("\n========== auth peak result ==========");
        println!(
            "  baseline success rate: {:.1}%",
            result.baseline_success_rate * 100.0
        );
        println!("  baseline p99:          {:.2} ms", result.baseline_p99_ms);
        println!(
            "  peak success rate:     {:.1}%",
            result.peak_success_rate * 100.0
        );
        println!("  peak p99:              {:.2} ms", result.peak_p99_ms);
        println!("=======================================\n");

        let report = result.report;
        report.print();

        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
        return Ok(());
    }

    // reconnect_token_peak：G8 真实重连风暴（connect + token authenticate，无 bcrypt）。
    // 与 auth_peak(密码登录峰值) 一样自管连接周期，跳过预建 SDK。
    if scenario_type == ScenarioType::ReconnectTokenPeak {
        let metrics_url = std::env::var("LOAD_TEST_METRICS_URL").ok();
        let result = auth_peak::run_reconnect_token_peak(
            &records,
            &data_dir,
            &config.server.host,
            config.server.port,
            &server_protocol,
            metrics_url.as_deref(),
            config.arrival.is_jittered(),
            config.arrival.window_ms,
        )
        .await?;

        println!("\n===== reconnect token peak result =====");
        println!(
            "  warmup token rate:     {:.1}%",
            result.baseline_success_rate * 100.0
        );
        println!(
            "  peak success rate:     {:.1}%",
            result.peak_success_rate * 100.0
        );
        println!("  peak p99 (connect+auth): {:.2} ms", result.peak_p99_ms);
        println!("=======================================\n");

        let report = result.report;
        report.print();

        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
        return Ok(());
    }

    // soak（G10）：自管连接周期 + 长时采样，跳过预建 SDK。
    if scenario_type == ScenarioType::Soak {
        let result = soak::run_soak(
            &records,
            &data_dir,
            &config.server.host,
            config.server.port,
            &server_protocol,
            config.run.duration_sec,
            config.rate.msg_per_sec,
            config.run.warmup_sec,
        )
        .await?;
        println!("\n============ soak result ============");
        println!("=====================================\n");
        let report = result.report;
        report.print();
        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
        return Ok(());
    }

    let (sdks, connect_elapsed_secs) = scenario::create_all_sdks(
        &records,
        &data_dir,
        &config.server.host,
        config.server.port,
        &server_protocol,
        &config.rate,
    )
    .await?;

    let connect_ok = sdks.len();
    let connect_rate = connect_ok as f64 / records.len() as f64;
    let connect_tps = if connect_elapsed_secs > 0.0 {
        connect_ok as f64 / connect_elapsed_secs
    } else {
        connect_ok as f64
    };
    println!("\n========== connect phase ==========");
    println!("  connected:    {} / {}", connect_ok, records.len());
    println!("  connect rate: {:.1}%", connect_rate * 100.0);
    println!("  elapsed:      {:.2} sec", connect_elapsed_secs);
    println!("  connect tps:  {:.1} conn/sec", connect_tps);
    println!("===================================\n");

    if sdks.is_empty() {
        eprintln!("no available SDK connections");
        std::process::exit(1);
    }

    if scenario_type == ScenarioType::SemanticVerify {
        // semantic correctness verification: independent flow
        let message_count = (config.run.duration_sec * config.rate.msg_per_sec as u64).max(20);
        let result =
            semantic::run_semantic_verify(&sdks, &records, &config.rate, message_count).await?;

        println!("\n========== semantic verify result ==========");
        println!("  messages sent: {}", result.messages_sent);
        println!("  errors:        {}", result.errors);
        println!("=============================================\n");

        // merge connect assertion
        let thresholds = EnvThresholds::for_env(env);
        let mut report = result.report;
        report.assert_gte("connect_rate", connect_rate, thresholds.min_connect_rate);
        report.print();

        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
    } else if scenario_type == ScenarioType::Backpressure {
        // backpressure verification: independent flow
        let slow_count = (sdks.len() - 1) / 2; // half of bots are slow
        let slow_delay_ms = 500;
        let result = backpressure::run_backpressure(
            &sdks,
            &records,
            &config.rate,
            config.run.duration_sec,
            slow_count.max(1),
            slow_delay_ms,
        )
        .await?;

        println!("\n========== backpressure result ==========");
        println!("  messages sent:  {}", result.messages_sent);
        println!("  errors:         {}", result.errors);
        println!("  sender p99:     {:.2} ms", result.p99_ms);
        println!("  RSS start:      {:.1} MB", result.rss_start_mb);
        println!("  RSS end:        {:.1} MB", result.rss_end_mb);
        println!(
            "  RSS growth:     {:.1} MB",
            result.rss_end_mb - result.rss_start_mb
        );
        println!("==========================================\n");

        let thresholds = EnvThresholds::for_env(env);
        let mut report = result.report;
        report.assert_gte("connect_rate", connect_rate, thresholds.min_connect_rate);
        report.print();

        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
    } else {
        let stats = scenario::run_scenario(
            scenario_type,
            &sdks,
            &records,
            &config.rate,
            config.run.duration_sec,
            config.run.warmup_sec,
        )
        .await?;

        print_stats(scenario_type, &stats);

        // assertion report
        let thresholds = EnvThresholds::for_env(env);
        let mut report = AssertionReport::new();

        report.assert_gte("connect_rate", connect_rate, thresholds.min_connect_rate);

        if scenario_type != ScenarioType::FriendFlow {
            if stats.messages_sent > 0 {
                report.assert_gte("TPS", stats.tps(), thresholds.min_tps);
            } else {
                report.skip("TPS", "no messages sent");
            }

            report.assert_lte("error_rate", stats.error_rate(), thresholds.max_error_rate);

            if !stats.latency_hist.is_empty() {
                report.assert_lte(
                    "p99_latency_ms",
                    stats.percentile_ms(0.99),
                    thresholds.max_p99_latency_ms,
                );
            } else {
                report.skip("p99_latency_ms", "no latency data");
            }
        } else {
            report.skip("TPS", "friend_flow scenario");
            report.skip("error_rate", "friend_flow scenario");
            report.skip("p99_latency_ms", "friend_flow scenario");
        }

        report.print();

        if !report.all_passed() {
            eprintln!("{} assertions failed, exit code 1", report.fail_count());
            std::process::exit(1);
        }
    }

    Ok(())
}

fn print_stats(scenario_type: ScenarioType, stats: &ScenarioStats) {
    let (p50, p95, p99) = stats.p50_p95_p99();
    println!("\n========== load test result ==========");
    let success_label = if scenario_type == ScenarioType::FriendFlow {
        "friend_ok"
    } else {
        "messages_sent"
    };
    println!("  {}: {}", success_label, stats.messages_sent);
    println!("  errors:     {}", stats.errors);
    println!("  duration:   {:.1} sec", stats.duration_secs);
    if scenario_type != ScenarioType::FriendFlow && stats.messages_sent > 0 {
        println!("  TPS:        {:.1}", stats.tps());
        println!("  error_rate: {:.2}%", stats.error_rate() * 100.0);
    }
    if !stats.latency_hist.is_empty() {
        println!(
            "  latency(ms): p50={:.2}  p95={:.2}  p99={:.2}  mean={:.2}",
            p50,
            p95,
            p99,
            stats.mean_ms()
        );
    }
    println!("======================================\n");
}
