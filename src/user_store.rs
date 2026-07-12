//! 账号存储：data/users.txt，每行 username device_id token user_id
//! 启动前判断条数，不足则注册并追加，不重复注册。

use privchat_sdk::{PrivchatConfig, PrivchatSdk, ServerEndpoint, TransportProtocol};
use std::path::Path;
use tracing::info;

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub device_id: String,
    pub token: String,
    pub user_id: u64,
}

impl UserRecord {
    pub fn parse_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 4 {
            return None;
        }
        let user_id: u64 = parts[3].parse().ok()?;
        Some(Self {
            username: parts[0].to_string(),
            device_id: parts[1].to_string(),
            token: parts[2].to_string(),
            user_id,
        })
    }

    pub fn to_line(&self) -> String {
        format!(
            "{} {} {} {}",
            self.username, self.device_id, self.token, self.user_id
        )
    }
}

/// 从 users.txt 加载所有记录
pub fn load_users(
    users_file: &Path,
) -> Result<Vec<UserRecord>, Box<dyn std::error::Error + Send + Sync>> {
    if !users_file.exists() {
        return Ok(Vec::new());
    }
    let s = std::fs::read_to_string(users_file)?;
    let mut out = Vec::new();
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(r) = UserRecord::parse_line(line) {
            out.push(r);
        }
    }
    Ok(out)
}

/// 追加写入一行到 users.txt
fn append_user(
    users_file: &Path,
    record: &UserRecord,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(users_file)?;
    writeln!(f, "{}", record.to_line())?;
    Ok(())
}

/// 构建 server endpoints
pub fn server_endpoints(host: &str, port: u16) -> Vec<ServerEndpoint> {
    vec![
        ServerEndpoint {
            protocol: TransportProtocol::Quic,
            host: host.to_string(),
            port,
            path: None,
            use_tls: false,
        },
        ServerEndpoint {
            protocol: TransportProtocol::Tcp,
            host: host.to_string(),
            port,
            path: None,
            use_tls: false,
        },
        ServerEndpoint {
            protocol: TransportProtocol::WebSocket,
            host: host.to_string(),
            port: if port == 9001 { 9080 } else { port + 1 },
            path: Some("/".to_string()),
            use_tls: false,
        },
    ]
}

/// 确保至少有 total 个用户：不足则注册并写入 users.txt
pub async fn ensure_users(
    data_dir: &Path,
    users_file: &Path,
    total: u32,
    server_host: &str,
    server_port: u16,
    password: &str,
) -> Result<Vec<UserRecord>, Box<dyn std::error::Error + Send + Sync>> {
    let users_root = data_dir.join("users");
    std::fs::create_dir_all(&users_root)?;

    let mut records = load_users(users_file)?;
    let need = total as usize;
    if records.len() >= need {
        info!("📋 已有 {} 个账号，满足需要 {}", records.len(), need);
        records.truncate(need);
        return Ok(records);
    }

    let to_register = need - records.len();
    info!(
        "📝 需要再注册 {} 个账号（当前 {}，目标 {}）",
        to_register,
        records.len(),
        need
    );

    let endpoints = server_endpoints(server_host, server_port);
    let base = records.len() as u32;

    let prefix = std::env::var("LOAD_TEST_USER_PREFIX").unwrap_or_else(|_| "loadtest".to_string());
    for i in 0..to_register {
        let username = format!("{}_{:06}", prefix, base + i as u32);
        let device_id = uuid::Uuid::new_v4().to_string();
        let user_data_dir = users_root.join(&username);
        std::fs::create_dir_all(&user_data_dir)?;

        let config = PrivchatConfig {
            endpoints: endpoints.clone(),
            connection_timeout_secs: 30,
            data_dir: user_data_dir.to_string_lossy().to_string(),
        };

        let sdk = PrivchatSdk::new(config);
        sdk.connect().await?;

        let result = sdk
            .register(username.clone(), password.to_string(), device_id.clone())
            .await?;

        let record = UserRecord {
            username: username.clone(),
            device_id: device_id.clone(),
            token: result.token.clone(),
            user_id: result.user_id,
        };
        append_user(users_file, &record)?;
        records.push(record);
        info!(
            "✅ 注册第 {} 个账号: {} user_id={}",
            i + 1,
            username,
            result.user_id
        );

        sdk.shutdown().await;
    }

    Ok(records)
}
