use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thindb_external_sql::server::Server;

#[derive(Debug, Parser)]
#[command(about = "Bounded SQLancer TLP checks against owned disposable thinDB servers")]
struct Args {
    #[arg(long)]
    server: PathBuf,
    #[arg(long, default_value = "sqlancer/target/thindb-sqlancer.jar")]
    jar: PathBuf,
    #[arg(long, default_value = "java")]
    java: PathBuf,
    #[arg(long, default_value_t = 0, allow_hyphen_values = true)]
    seed: i64,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=1000))]
    seeds: u32,
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=100000))]
    queries: u32,
    #[arg(long, default_value_t = 32, value_parser = clap::value_parser!(u32).range(0..=256))]
    max_rows: u32,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..=300))]
    query_timeout: u32,
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u32).range(1..=3600))]
    seed_timeout: u32,
    #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u32).range(0..=1000))]
    reduce_attempts: u32,
    #[arg(long, default_value_t = 1)]
    max_dop: u32,
    #[arg(long)]
    replay: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
}

struct JavaChild(Child);
impl Drop for JavaChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn hash(path: &Path) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

fn completed_checks(directory: &Path) -> u64 {
    // A hard deadline can end Java before provider.json is written. Flushed checks still count.
    fs::read_to_string(directory.join("checks.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|line| serde_json::from_str::<Value>(line).is_ok())
        .count() as u64
}

fn portable_path(path: &Path) -> Result<PathBuf> {
    let canonical = path.canonicalize()?;
    // Java's JAR class loader cannot load classes through a Windows verbatim path.
    #[cfg(windows)]
    if let Some(path) = canonical.to_str().and_then(|s| s.strip_prefix(r"\\?\")) {
        return Ok(PathBuf::from(
            if let Some(unc) = path.strip_prefix(r"UNC\") {
                format!(r"\\{unc}")
            } else {
                path.to_string()
            },
        ));
    }
    Ok(canonical)
}

async fn run_seed(args: &Args, seed: i64, directory: &Path) -> Result<Value> {
    let mut server = Server::start(
        &args.server,
        "mysql",
        args.max_dop,
        &directory.join("server.log"),
    )
    .await?;
    let log = File::create(directory.join("java.log"))?;
    let mut command = Command::new(&args.java);
    command
        .args(["-ea", "-Xmx256m", "-Dfile.encoding=UTF-8", "-jar"])
        .arg(&args.jar)
        .args([
            "--output",
            &directory.to_string_lossy(),
            "--seed",
            &seed.to_string(),
        ])
        .args([
            "--queries",
            &args.queries.to_string(),
            "--max-rows",
            &args.max_rows.to_string(),
            "--query-timeout",
            &args.query_timeout.to_string(),
            "--reduce-attempts",
            &args.reduce_attempts.to_string(),
        ])
        .env("THINDB_SQLANCER_PORT", server.port.to_string())
        .env("THINDB_SQLANCER_PASSWORD", &server.password)
        .env_remove("JAVA_TOOL_OPTIONS")
        .env_remove("JDK_JAVA_OPTIONS")
        .env_remove("_JAVA_OPTIONS")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    if let Some(replay) = &args.replay {
        command.arg("--replay").arg(replay);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut child = JavaChild(command.spawn().context("start Java 21 SQLancer provider")?);
    let started = Instant::now();
    let status = loop {
        server.check_running()?;
        if let Some(status) = child.0.try_wait()? {
            break status;
        }
        ensure!(
            started.elapsed() < Duration::from_secs(args.seed_timeout.into()),
            "seed deadline exceeded"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let report: Value = serde_json::from_slice(
        &fs::read(directory.join("provider.json"))
            .context("provider exited without a report; see java.log")?,
    )?;
    ensure!(
        status.success() && report["status"] == "passed",
        "SQLancer failed: {}",
        report["error"]
    );
    let requested = if args.replay.is_some() {
        1
    } else {
        args.queries
    };
    ensure!(
        report["completed"].as_u64() == Some(requested.into())
            && completed_checks(directory) == u64::from(requested),
        "provider did not finish every requested check"
    );
    Ok(report)
}

fn xml(text: &str) -> String {
    text.chars()
        .filter(|c| *c >= ' ' || ['\n', '\r', '\t'].contains(c))
        .collect::<String>()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn write_report(directory: &Path, seeds: &[Value], requested: u32) -> Result<()> {
    let failed = seeds.iter().filter(|s| s["status"] != "passed").count();
    let completed: u64 = seeds
        .iter()
        .filter_map(|s| s["completed_checks"].as_u64())
        .sum();
    fs::write(
        directory.join("summary.json"),
        serde_json::to_vec_pretty(&json!({
            "requested_seeds": requested, "attempted_seeds": seeds.len(), "failed_seeds": failed,
            "completed_checks": completed, "seeds": seeds
        }))?,
    )?;
    let mut junit = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"sqlancer-tlp\" tests=\"{}\" failures=\"{failed}\">\n",
        seeds.len()
    );
    for seed in seeds {
        junit.push_str(&format!(
            "<testcase name=\"seed-{}\" time=\"{}\">",
            seed["seed"], seed["seconds"]
        ));
        if seed["status"] != "passed" {
            junit.push_str(&format!(
                "<failure>{}</failure>",
                xml(&seed["error"].to_string())
            ));
        }
        junit.push_str("</testcase>\n");
    }
    junit.push_str("</testsuite>\n");
    fs::write(directory.join("junit.xml"), junit)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    ensure!([1, 4].contains(&args.max_dop), "max-dop must be 1 or 4");
    args.server = portable_path(&args.server).context("server binary")?;
    args.jar = portable_path(&args.jar).context("build sqlancer/pom.xml with Maven first")?;
    if let Some(replay) = &mut args.replay {
        *replay = portable_path(replay).context("replay file")?;
    }
    let count = if args.replay.is_some() { 1 } else { args.seeds };
    ensure!(
        args.seed.checked_add(i64::from(count - 1)).is_some(),
        "seed range overflows i64"
    );
    let root = args
        .output
        .clone()
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts/sqlancer"));
    let output = root.join(format!(
        "run-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    fs::create_dir_all(&output)?;
    let output = portable_path(&output)?;
    println!("SQLancer diagnostics: {}", output.display());
    fs::write(
        output.join("run.json"),
        serde_json::to_vec_pretty(&json!({
            "server": args.server, "server_sha256": hash(&args.server)?,
            "provider_jar_sha256": hash(&args.jar)?, "platform": std::env::consts::OS,
            "seed": args.seed, "seeds": count, "queries": args.queries, "max_rows": args.max_rows,
            "max_dop": args.max_dop, "query_timeout": args.query_timeout, "seed_timeout": args.seed_timeout,
            "reduce_attempts": args.reduce_attempts, "replay": args.replay,
        }))?,
    )?;
    let mut seeds = Vec::new();
    for offset in 0..count {
        let seed = args.seed + i64::from(offset);
        let directory = output.join(format!("seed-{seed}"));
        fs::create_dir(&directory)?;
        if args.replay.is_none() {
            fs::write(
                directory.join("regenerate-command.json"),
                serde_json::to_vec_pretty(&json!([
                    "zig",
                    "build",
                    "test-sqlancer",
                    "-Doptimize=ReleaseSafe",
                    format!("-Dsqlancer-seed={seed}"),
                    "-Dsqlancer-seeds=1",
                    format!("-Dsqlancer-queries={}", args.queries),
                    format!("-Dsqlancer-max-rows={}", args.max_rows),
                    format!("-Dsqlancer-dop={}", args.max_dop),
                    format!("-Dsqlancer-query-timeout={}", args.query_timeout),
                    format!("-Dsqlancer-seed-timeout={}", args.seed_timeout),
                    format!("-Dsqlancer-reduce-attempts={}", args.reduce_attempts),
                ]))?,
            )?;
        }
        fs::write(
            directory.join("replay-command.json"),
            serde_json::to_vec_pretty(&json!([
                "zig",
                "build",
                "test-sqlancer",
                "-Doptimize=ReleaseSafe",
                format!(
                    "-Dsqlancer-replay={}",
                    directory.join("replay.json").display()
                ),
                format!("-Dsqlancer-dop={}", args.max_dop),
                format!("-Dsqlancer-query-timeout={}", args.query_timeout),
                format!("-Dsqlancer-seed-timeout={}", args.seed_timeout),
                format!("-Dsqlancer-reduce-attempts={}", args.reduce_attempts),
            ]))?,
        )?;
        let started = Instant::now();
        let result = tokio::select! {
            result = run_seed(&args, seed, &directory) => result,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                Err(anyhow::anyhow!("interrupted; owned processes terminated"))
            }
        };
        let provider = fs::read(directory.join("provider.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or(Value::Null);
        let error = result.as_ref().err().map(|e| format!("{e:#}"));
        println!(
            "seed {seed}: {}",
            if error.is_none() { "passed" } else { "FAILED" }
        );
        seeds.push(
            json!({"seed": seed, "status": if error.is_none() { "passed" } else { "failed" },
            "seconds": started.elapsed().as_secs_f64(), "error": error, "provider": provider,
            "completed_checks": completed_checks(&directory),
            "directory": directory, "checks_sha256": hash(&directory.join("checks.jsonl")).ok()}),
        );
        write_report(&output, &seeds, count)?;
        if let Some(error) = error {
            bail!("{error}; diagnostics: {}", directory.display());
        }
    }
    println!("All {count} seeds passed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_campaign_and_infrastructure_failure_are_visible() {
        let directory = tempfile::tempdir().unwrap();
        write_report(
            directory.path(),
            &[json!({"seed": 7, "status": "failed", "seconds": 1,
            "error": "deadline < exceeded", "completed_checks": 3, "provider": null})],
            5,
        )
        .unwrap();
        let report: Value =
            serde_json::from_slice(&fs::read(directory.path().join("summary.json")).unwrap())
                .unwrap();
        assert_eq!(report["failed_seeds"], 1);
        assert_eq!(report["requested_seeds"], 5);
        assert_eq!(report["completed_checks"], 3);
        assert!(
            fs::read_to_string(directory.path().join("junit.xml"))
                .unwrap()
                .contains("&lt;")
        );
    }

    #[test]
    fn completed_checks_survive_missing_provider_report_and_partial_last_line() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("checks.jsonl"),
            "{\"index\":0}\n{\"index\":1}\n{\"ind",
        )
        .unwrap();
        assert_eq!(completed_checks(directory.path()), 2);
    }
}
