use crate::wire::Database;
use anyhow::{Context, Result, bail, ensure};
use std::{
    fs::{self, File},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tempfile::TempDir;

pub struct Server {
    child: Child,
    _data: TempDir,
    pub port: u16,
    pub password: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Server {
    pub fn check_running(&mut self) -> Result<()> {
        if let Some(status) = self.child.try_wait()? {
            bail!("disposable server exited unexpectedly: {status}");
        }
        Ok(())
    }
    pub async fn start(binary: &Path, protocol: &str, dop: u32, log: &Path) -> Result<Self> {
        ensure!(
            ["mysql", "postgres"].contains(&protocol),
            "unknown protocol"
        );
        ensure!([1, 4].contains(&dop), "max-dop must be 1 or 4");
        let data = tempfile::Builder::new()
            .prefix("thindb-external-sql-")
            .tempdir()?;
        ensure!(
            !data
                .path()
                .to_string_lossy()
                .to_ascii_lowercase()
                .contains(".wayroll-prod-db"),
            "production data paths are forbidden"
        );
        let port = loop {
            let probe = TcpListener::bind(("127.0.0.1", 0))?;
            let port = probe.local_addr()?.port();
            if port != 13310 {
                break port;
            }
        };
        ensure!(port != 13310, "production port is forbidden");
        let password = data
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let output = File::create(log)?;
        let mut command = Command::new(binary);
        command
            .args(["--bind", "127.0.0.1", "--data-dir"])
            .arg(data.path())
            .args([
                "--mysql-port",
                &if protocol == "mysql" {
                    port.to_string()
                } else {
                    "0".into()
                },
                "--pg-port",
                &if protocol == "postgres" {
                    port.to_string()
                } else {
                    "0".into()
                },
                "--native-port",
                "0",
                "--max-dop",
                &dop.to_string(),
                "--mysql-password",
                &password,
                "--pg-password",
                &password,
                "--cache-size",
                "64M",
                "--query-memory-budget",
                "256M",
                "--memory-budget",
                "512M",
            ])
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output);
        for (key, _) in std::env::vars_os() {
            if key
                .to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("THINDB_")
            {
                command.env_remove(key);
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        let child = command.spawn().context("start disposable thinDB server")?;
        let mut server = Self {
            child,
            _data: data,
            port,
            password,
        };
        let started = Instant::now();
        loop {
            if let Some(status) = server.child.try_wait()? {
                bail!(
                    "server exited during startup: {status}; see {}",
                    log.display()
                );
            }
            // SO_REUSEADDR can admit a second listener. Require this child's
            // startup log and its unique password, not just a successful bind.
            if fs::read_to_string(log)
                .unwrap_or_default()
                .contains(&format!("127.0.0.1:{port}"))
            {
                let ready = async {
                    let mut db =
                        Database::connect(protocol, port, &server.password, "main").await?;
                    db.query("SELECT 1").await
                };
                if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_secs(2), ready).await {
                    return Ok(server);
                }
            }
            ensure!(
                started.elapsed() < Duration::from_secs(30),
                "server readiness timeout; see {}",
                log.display()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
