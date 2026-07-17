use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Transparent TCP proxy whose server-to-client read side can be frozen after
/// authentication. The upstream socket stays open even if the SDK gives up and
/// closes its side, so the server eventually encounters a real zero-window stall.
pub struct SocketStallProxy {
    listen_addr: SocketAddr,
    stalled: Arc<AtomicBool>,
    accepted: Arc<AtomicBool>,
    upstream_bytes: Arc<AtomicU64>,
    stall_notify: Arc<Notify>,
    task: JoinHandle<()>,
}

impl SocketStallProxy {
    pub async fn start(upstream_addr: String) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let stalled = Arc::new(AtomicBool::new(false));
        let accepted = Arc::new(AtomicBool::new(false));
        let upstream_bytes = Arc::new(AtomicU64::new(0));
        let stall_notify = Arc::new(Notify::new());

        let task_stalled = stalled.clone();
        let task_accepted = accepted.clone();
        let task_bytes = upstream_bytes.clone();
        let task_notify = stall_notify.clone();
        let task = tokio::spawn(async move {
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            let Ok(upstream) = TcpStream::connect(&upstream_addr).await else {
                return;
            };
            let _ = upstream.set_nodelay(true);
            task_accepted.store(true, Ordering::Release);

            let (mut client_read, mut client_write) = client.into_split();
            let (mut upstream_read, mut upstream_write) = upstream.into_split();
            let c2s_stalled = task_stalled.clone();
            let client_to_server = async move {
                let mut buffer = vec![0u8; 16 * 1024];
                loop {
                    match client_read.read(&mut buffer).await {
                        Ok(0) if c2s_stalled.load(Ordering::Acquire) => {
                            std::future::pending::<()>().await;
                        }
                        Ok(0) | Err(_) => break,
                        Ok(n) if upstream_write.write_all(&buffer[..n]).await.is_err() => break,
                        Ok(_) => {}
                    }
                }
            };

            let server_to_client = async move {
                let mut buffer = vec![0u8; 16 * 1024];
                loop {
                    if task_stalled.load(Ordering::Acquire) {
                        std::future::pending::<()>().await;
                    }
                    tokio::select! {
                        _ = task_notify.notified() => continue,
                        read = upstream_read.read(&mut buffer) => {
                            match read {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    task_bytes.fetch_add(n as u64, Ordering::Relaxed);
                                    if client_write.write_all(&buffer[..n]).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            };

            tokio::join!(client_to_server, server_to_client);
        });

        Ok(Self {
            listen_addr,
            stalled,
            accepted,
            upstream_bytes,
            stall_notify,
            task,
        })
    }

    pub fn port(&self) -> u16 {
        self.listen_addr.port()
    }

    pub fn activate(&self) {
        self.stalled.store(true, Ordering::Release);
        self.stall_notify.notify_waiters();
    }

    pub fn is_accepted(&self) -> bool {
        self.accepted.load(Ordering::Acquire)
    }

    pub fn upstream_bytes(&self) -> u64 {
        self.upstream_bytes.load(Ordering::Relaxed)
    }
}

impl Drop for SocketStallProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout, Duration};

    #[tokio::test]
    async fn freezes_upstream_reads_without_closing_socket() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = upstream.accept().await.unwrap();
            stream.write_all(b"before").await.unwrap();
            sleep(Duration::from_millis(100)).await;
            stream.write_all(b"after").await.unwrap();
            sleep(Duration::from_secs(1)).await;
        });

        let proxy = SocketStallProxy::start(upstream_addr.to_string())
            .await
            .unwrap();
        let mut client = TcpStream::connect(("127.0.0.1", proxy.port()))
            .await
            .unwrap();
        let mut before = [0u8; 6];
        client.read_exact(&mut before).await.unwrap();
        assert_eq!(&before, b"before");
        proxy.activate();

        let mut after = [0u8; 5];
        assert!(
            timeout(Duration::from_millis(300), client.read_exact(&mut after))
                .await
                .is_err()
        );
        assert!(proxy.is_accepted());
        assert_eq!(proxy.upstream_bytes(), 6);
        server.abort();
    }
}
