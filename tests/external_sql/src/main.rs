mod compare;
mod corpus;
mod server;
mod wire;

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use corpus::{Case, Policies, Policy, Source};
use serde::Serialize;
use sqllogictest::{ExpectedError, QueryExpect, Record, Runner, StatementExpect};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use wire::{Database, WireError};

#[derive(Parser, Debug)]
#[command(about = "Pinned upstream SQL correctness suites against disposable thinDB servers")]
struct Args {
    #[arg(long)]
    server: PathBuf,
    #[arg(long, default_value = "all", value_parser = ["all", "sqlite", "duckdb", "starrocks"])]
    suite: String,
    #[arg(long)]
    filter: Option<String>,
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    #[arg(long, default_value_t = 600)]
    file_timeout: u64,
    #[arg(long, default_value = "mysql", value_parser = ["mysql", "postgres"])]
    protocol: String,
    #[arg(long, default_value_t = 1)]
    max_dop: u32,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    list: bool,
    #[arg(long)]
    self_test: bool,
    #[arg(long)]
    compare_run: Option<PathBuf>,
}

#[derive(Serialize, serde::Deserialize)]
struct Check {
    id: String,
    status: String,
    seconds: f64,
    detail: String,
    sql: String,
    expected: String,
    actual: String,
    replay: String,
    comparison: String,
}
#[derive(Serialize, serde::Deserialize)]
struct Report {
    protocol: String,
    max_dop: u32,
    counts: BTreeMap<String, usize>,
    checks: Vec<Check>,
}
impl Report {
    fn push(&mut self, check: Check) {
        *self.counts.entry(check.status.clone()).or_default() += 1;
        self.checks.push(check);
    }
    fn write(&self, directory: &std::path::Path) -> Result<()> {
        fs::write(
            directory.join("results.json"),
            serde_json::to_vec_pretty(self)?,
        )?;
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"external-sql\" tests=\"{}\" failures=\"{}\">\n",
            self.checks.len(),
            self.checks
                .iter()
                .filter(|c| ["failed", "unexpected_pass", "blocked"].contains(&c.status.as_str()))
                .count()
        );
        for c in &self.checks {
            xml.push_str(&format!(
                "<testcase name=\"{}\" time=\"{}\">",
                escape(&c.id),
                c.seconds
            ));
            if ["failed", "unexpected_pass", "blocked"].contains(&c.status.as_str()) {
                xml.push_str(&format!(
                    "<failure message=\"{}\">{}</failure>",
                    escape(&c.detail),
                    escape(&format!(
                        "{}\nExpected: {}\nActual: {}\n{}",
                        c.sql, c.expected, c.actual, c.replay
                    ))
                ));
            } else if c.status != "passed" {
                xml.push_str(&format!(
                    "<skipped message=\"{}: {}\"/>",
                    escape(&c.status),
                    escape(&c.detail)
                ));
            }
            xml.push_str("</testcase>\n");
        }
        xml.push_str("</testsuite>\n");
        fs::write(directory.join("junit.xml"), xml)?;
        Ok(())
    }
}
fn escape(s: &str) -> String {
    s.chars()
        .filter(|c| *c >= ' ' || ['\n', '\r', '\t'].contains(c))
        .collect::<String>()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn sql(case: &Case) -> &str {
    match &case.record {
        Record::Statement { sql, .. } | Record::Query { sql, .. } => sql,
        _ => unreachable!(),
    }
}
fn expected_error(case: &Case) -> Option<&ExpectedError> {
    match &case.record {
        Record::Statement {
            expected: StatementExpect::Error(e),
            ..
        }
        | Record::Query {
            expected: QueryExpect::Error(e),
            ..
        } => Some(e),
        _ => None,
    }
}
fn is_setup(case: &Case) -> bool {
    matches!(
        case.record,
        Record::Statement {
            expected: StatementExpect::Ok | StatementExpect::Count(_),
            ..
        }
    )
}
fn verify_error(error: &WireError, expected: &ExpectedError, policy: &Policy) -> Result<()> {
    let state = error
        .state
        .as_deref()
        .context("connection/internal failure is never an expected SQL error")?;
    if let Some(states) = &policy.error_states {
        ensure!(
            states.iter().any(|s| s == state),
            "unexpected SQLSTATE: {error}"
        );
        return Ok(());
    }
    match expected {
        ExpectedError::Empty => bail!(
            "unqualified expected error requires an explicit SQLSTATE policy; received {error}"
        ),
        ExpectedError::Inline(regex) => ensure!(
            regex.is_match(&error.message),
            "unexpected SQL error: {error}"
        ),
        ExpectedError::Multiline(message) => {
            ensure!(error.message == *message, "unexpected SQL error: {error}")
        }
        ExpectedError::SqlState(expected) => {
            ensure!(state == expected, "unexpected SQL error: {error}")
        }
    }
    Ok(())
}

fn verify_label(
    labels: &mut HashMap<String, String>,
    label: &str,
    fingerprint: &str,
) -> Result<()> {
    if let Some(previous) = labels.get(label) {
        ensure!(previous == fingerprint, "label mismatch: {label}");
    } else {
        labels.insert(label.to_string(), fingerprint.to_string());
    }
    Ok(())
}

async fn run_source(
    args: &Args,
    source: &Source,
    cases: Vec<Case>,
    policies: &Policies,
    directory: &std::path::Path,
    report: &mut Report,
) -> Result<()> {
    let started = Instant::now();
    println!(
        "{}: https://github.com/{}/blob/{}/{}",
        source.id, source.repository, source.revision, source.source_path
    );
    let mut server = server::Server::start(
        &args.server,
        &args.protocol,
        args.max_dop,
        &directory.join(format!("{}.log", source.id.replace('/', "_"))),
    )
    .await?;
    let db = tokio::time::timeout(Duration::from_secs(args.timeout), async {
        let mut db =
            Database::connect(&args.protocol, server.port, &server.password, "main").await?;
        db.execute("CREATE DATABASE external_sql").await?;
        drop(db);
        Database::connect(
            &args.protocol,
            server.port,
            &server.password,
            "external_sql",
        )
        .await
    })
    .await??;
    let last = db.last.clone();
    let describe_result = db.describe_result.clone();
    let single = Arc::new(Mutex::new(Some(db)));
    let mut runner = Runner::new(move || {
        let db = single.lock().unwrap().take();
        async move {
            db.ok_or_else(|| WireError {
                state: None,
                message: "unexpected connection replacement".into(),
            })
        }
    });
    let mut blocked = None;
    let mut labels: HashMap<String, String> = HashMap::new();
    let mut setup_sql = String::new();
    let last_selected = cases
        .iter()
        .rposition(|c| selected(args.filter.as_deref(), &source.id, &c.id))
        .unwrap_or(0);
    for (index, mut case) in cases.into_iter().enumerate() {
        let policy = policies.get(source, &case)?;
        policies.apply(&mut case)?;
        let before = Instant::now();
        let mut check = Check {
            id: case.id.clone(),
            status: "passed".into(),
            seconds: 0.0,
            detail: String::new(),
            sql: sql(&case).into(),
            expected: match &case.record {
                Record::Query { expected, .. } => format!("{expected:?}"),
                Record::Statement { expected, .. } => format!("{expected:?}"),
                _ => unreachable!(),
            },
            actual: String::new(),
            comparison: String::new(),
            replay: format!(
                "zig build test-external-sql -Doptimize=ReleaseSafe -Dsql-suite={} -Dsql-filter={} -Dsql-protocol={} -Dsql-dop={} -Dsql-timeout={} -Dsql-file-timeout={}",
                source.suite, case.id, args.protocol, args.max_dop, args.timeout, args.file_timeout
            ),
        };
        if index > last_selected
            || (!selected(args.filter.as_deref(), &source.id, &case.id) && !is_setup(&case))
        {
            check.status = "filtered".into();
        } else if let Some(reason) = case.condition_exclusion() {
            check.status = "conditional_exclusion".into();
            check.detail = reason;
        } else if policy.kind.as_ref().is_some_and(|k| k != "known_bug") {
            check.status = policy.kind.clone().unwrap();
            check.detail = policy.reason.clone().unwrap();
            if is_setup(&case) && !policy.setup_noop {
                blocked = Some(format!("excluded setup {}: {}", case.id, check.detail));
            }
        } else if let Some(reason) = &blocked {
            check.status = "blocked".into();
            check.detail = reason.clone();
        } else {
            *last.lock().unwrap() = None;
            *describe_result.lock().unwrap() = matches!(case.record, Record::Query { .. });
            let remaining =
                Duration::from_secs(args.file_timeout).saturating_sub(started.elapsed());
            let timeout = Duration::from_secs(args.timeout).min(remaining);
            let applied =
                tokio::time::timeout(timeout, runner.apply_record(case.record.clone())).await;
            let mut fatal = false;
            let outcome = if applied.is_err() {
                fatal = true;
                Err(anyhow::anyhow!("query/file timeout"))
            } else {
                let output = last
                    .lock()
                    .unwrap()
                    .take()
                    .context("record was not executed")?;
                match output {
                    Err(error) => {
                        fatal = error.state.is_none();
                        check.actual = error.to_string();
                        if let Some(expected) = expected_error(&case) {
                            verify_error(&error, expected, &policy)
                        } else {
                            Err(error.into())
                        }
                    }
                    Ok(output) => {
                        check.actual = serde_json::to_string(&output)?;
                        if expected_error(&case).is_some() {
                            Err(anyhow::anyhow!(
                                "statement succeeded but an SQL error was expected"
                            ))
                        } else {
                            match &case.record {
                                Record::Statement { expected, .. } => {
                                    if let StatementExpect::Count(n) = expected {
                                        if *n != output.affected {
                                            Err(anyhow::anyhow!("affected rows mismatch"))
                                        } else {
                                            Ok(())
                                        }
                                    } else {
                                        Ok(())
                                    }
                                }
                                Record::Query {
                                    expected:
                                        QueryExpect::Results {
                                            types,
                                            sort_mode,
                                            results,
                                            label,
                                            ..
                                        },
                                    ..
                                } => {
                                    let comparison = compare::compare(
                                        &output,
                                        types.len(),
                                        results,
                                        *sort_mode,
                                        policy.tolerance.as_ref(),
                                    );
                                    comparison.and_then(|hash| {
                                        check.comparison =
                                            format!("{} columns: {hash}", types.len());
                                        if let Some(label) = label {
                                            verify_label(&mut labels, label, &check.comparison)?;
                                        }
                                        Ok(())
                                    })
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                }
            };
            match outcome {
                Ok(()) if policy.kind.as_deref() == Some("known_bug") => {
                    check.status = "unexpected_pass".into();
                    check.detail = "remove stale known-bug classification".into();
                }
                Ok(()) => {
                    if !check.comparison.is_empty() {
                        check.actual = check.comparison.clone();
                    }
                }
                Err(error) => {
                    check.detail = format!("{error:#}");
                    if !fatal
                        && policy.kind.as_deref() == Some("known_bug")
                        && policy.signature.as_deref() == Some(&check.detail)
                    {
                        check.status = "known_bug".into();
                    } else {
                        check.status = "failed".into();
                    }
                    if fatal || is_setup(&case) {
                        blocked = Some(format!(
                            "failed setup/connection at {}: {}",
                            case.id, check.detail
                        ));
                    }
                    let name = case.id.replace(['/', ':'], "_");
                    fs::write(
                        directory.join(format!("{name}.sql")),
                        format!(
                            "-- {}\n-- {}\n{}\n{};\n",
                            check.id,
                            check.detail.replace('\n', " "),
                            setup_sql,
                            check.sql.trim_end_matches(';')
                        ),
                    )?;
                }
            }
            if is_setup(&case) && check.status == "passed" {
                setup_sql.push_str(&format!("{};\n", check.sql.trim_end_matches(';')));
            }
        }
        check.seconds = before.elapsed().as_secs_f64();
        if check.status == "failed" || check.status == "unexpected_pass" {
            eprintln!("{} {}: {}", check.status, check.id, check.detail);
        }
        report.push(check);
    }
    drop(runner);
    server.check_running()?;
    drop(server);
    println!(
        "{} complete ({:.1}s)",
        source.id,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

fn selected(filter: Option<&str>, source: &str, id: &str) -> bool {
    filter.is_none_or(|f| {
        if f.contains(':') {
            id == f
        } else {
            source.contains(f)
        }
    })
}

async fn live_self_test(args: &Args, directory: &std::path::Path) -> Result<()> {
    let server = server::Server::start(
        &args.server,
        &args.protocol,
        args.max_dop,
        &directory.join("self_test.log"),
    )
    .await?;
    let mut db = Database::connect(&args.protocol, server.port, &server.password, "main").await?;
    db.execute("CREATE TABLE isolation_probe(id INT)").await?;
    db.execute("INSERT INTO isolation_probe VALUES (1)").await?;
    db.execute("INSERT INTO isolation_probe SELECT * FROM isolation_probe")
        .await?;
    compare::compare(
        &db.query("SELECT COUNT(*) FROM isolation_probe").await?,
        1,
        &["2".into()],
        None,
        None,
    )?;
    db.execute("CREATE TEMPORARY TABLE session_probe(id BIGINT, value DECIMAL(30,4), text_value VARCHAR(30))").await?;
    db.execute("INSERT INTO session_probe VALUES (9007199254740993, 123456789012345678.1200, ' a  b '), (1, NULL, ''), (2, 0, NULL)").await?;
    let output = db
        .query("SELECT id, value, text_value FROM session_probe ORDER BY id")
        .await?;
    let expected = [
        "1",
        "NULL",
        "(empty)",
        "2",
        "0",
        "NULL",
        "9007199254740993",
        "123456789012345678.12",
        " a  b ",
    ]
    .map(String::from);
    compare::compare(&output, 3, &expected, None, None)?;
    let empty = db
        .query("SELECT id FROM session_probe WHERE id = 999")
        .await?;
    compare::compare(&empty, 1, &[], None, None)?;
    ensure!(
        compare::compare(&empty, 2, &[], None, None).is_err(),
        "empty result width was ignored"
    );
    ensure!(
        db.query("SELECT nonexistent FROM session_probe")
            .await
            .unwrap_err()
            .state
            .is_some(),
        "SQL error lost its class"
    );
    ensure!(
        tokio::time::timeout(Duration::from_millis(20), db.query("SELECT SLEEP(1)"))
            .await
            .is_err(),
        "timeout was ignored"
    );
    drop(db);
    let mut db = Database::connect(&args.protocol, server.port, &server.password, "main").await?;
    drop(server);
    let error = tokio::time::timeout(Duration::from_secs(5), db.query("SELECT 1"))
        .await?
        .unwrap_err();
    ensure!(
        error.state.is_none(),
        "connection loss treated as SQL error"
    );
    drop(db);
    let fresh = server::Server::start(
        &args.server,
        &args.protocol,
        args.max_dop,
        &directory.join("isolation_test.log"),
    )
    .await?;
    let mut db = Database::connect(&args.protocol, fresh.port, &fresh.password, "main").await?;
    ensure!(
        db.query("SELECT id FROM isolation_probe")
            .await
            .unwrap_err()
            .state
            .is_some(),
        "database leaked between server instances"
    );
    println!(
        "Live adapter self-test passed: session, exact values, empty metadata, SQL error, timeout, disconnect, isolation"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();
    ensure!(
        args.timeout > 0 && args.file_timeout > 0,
        "timeouts must be positive"
    );
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let sources: Vec<Source> =
        serde_json::from_str(&fs::read_to_string(root.join("sources.json"))?)?;
    let policies: Policies =
        serde_json::from_str(&fs::read_to_string(root.join("policies.json"))?)?;
    let mut selected_sources = vec![];
    let mut all_ids = std::collections::HashSet::new();
    for source in sources {
        let cases = corpus::load(&root, &source)?;
        for case in &cases {
            ensure!(all_ids.insert(case.id.clone()), "duplicate case id");
            policies.get(&source, case)?;
            let mut copy = case.clone();
            policies.apply(&mut copy)?;
        }
        if args.suite != "all" && args.suite != source.suite {
            continue;
        }
        if !cases
            .iter()
            .any(|c| selected(args.filter.as_deref(), &source.id, &c.id))
        {
            continue;
        }
        selected_sources.push((source, cases));
    }
    for id in policies.cases.keys().chain(policies.patches.keys()) {
        ensure!(all_ids.contains(id), "stale policy id {id}");
    }
    ensure!(!selected_sources.is_empty(), "no matching cases");
    if args.list {
        for (_, cases) in &selected_sources {
            for case in cases {
                println!("{} {}", case.id, sql(case).lines().next().unwrap_or(""));
            }
        }
        return Ok(());
    }
    args.server =
        fs::canonicalize(&args.server).context("build thinDB first / provide --server")?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = args
        .output
        .clone()
        .unwrap_or_else(|| root.join("artifacts"))
        .join(format!("run-{stamp}"));
    fs::create_dir_all(&directory)?;
    println!("Artifacts: {}", directory.display());
    fs::copy(root.join("sources.json"), directory.join("sources.json"))?;
    fs::copy(root.join("policies.json"), directory.join("policies.json"))?;
    use sha2::Digest;
    let server_sha256 = format!("{:x}", sha2::Sha256::digest(fs::read(&args.server)?));
    fs::write(
        directory.join("run.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "server": args.server, "server_sha256": server_sha256,
            "protocol": args.protocol, "max_dop": args.max_dop, "timeout": args.timeout,
            "file_timeout": args.file_timeout, "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "suite": args.suite, "filter": args.filter,
        }))?,
    )?;
    if args.self_test {
        return tokio::select! {
            result = live_self_test(&args, &directory) => result,
            _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("interrupted")),
        };
    }
    let mut report = Report {
        protocol: args.protocol.clone(),
        max_dop: args.max_dop,
        counts: BTreeMap::new(),
        checks: vec![],
    };
    for (source, cases) in selected_sources {
        let pending = cases.clone();
        let result = tokio::select! {
            result = run_source(&args, &source, cases, &policies, &directory, &mut report) => result,
            _ = tokio::signal::ctrl_c() => Err(anyhow::anyhow!("interrupted")),
        };
        if let Err(error) = result {
            report.push(Check {
                id: format!("{}:infrastructure", source.id),
                status: "failed".into(),
                seconds: 0.0,
                detail: format!("{error:#}"),
                sql: String::new(),
                expected: String::new(),
                actual: String::new(),
                comparison: String::new(),
                replay: format!(
                    "zig build test-external-sql -Dsql-filter={} -Dsql-protocol={} -Dsql-dop={}",
                    source.id, args.protocol, args.max_dop
                ),
            });
            for case in pending {
                if !report.checks.iter().any(|c| c.id == case.id) {
                    report.push(Check {
                        id: case.id.clone(),
                        status: "blocked".into(),
                        seconds: 0.0,
                        detail: format!("infrastructure: {error:#}"),
                        sql: sql(&case).into(),
                        expected: String::new(),
                        actual: String::new(),
                        replay: String::new(),
                        comparison: String::new(),
                    });
                }
            }
            report.write(&directory)?;
            return Err(error);
        }
        report.write(&directory)?;
    }
    println!("{}", serde_json::to_string_pretty(&report.counts)?);
    if let Some(previous) = args.compare_run {
        let previous: Report = serde_json::from_slice(&fs::read(previous)?)?;
        let previous: HashMap<_, _> = previous.checks.iter().map(|c| (&c.id, c)).collect();
        let mismatches: Vec<_> = report
            .checks
            .iter()
            .filter(|c| {
                !previous.get(&c.id).is_some_and(|p| {
                    p.status == c.status && p.comparison == c.comparison && p.detail == c.detail
                })
            })
            .map(|c| c.id.clone())
            .collect();
        fs::write(
            directory.join("comparison.json"),
            serde_json::to_vec_pretty(&mismatches)?,
        )?;
        if previous.len() != report.checks.len() || !mismatches.is_empty() {
            let detail = format!("configuration/repeat mismatch: {mismatches:?}");
            report.push(Check {
                id: "configuration_comparison".into(),
                status: "failed".into(),
                seconds: 0.0,
                detail: detail.clone(),
                sql: String::new(),
                expected: String::new(),
                actual: String::new(),
                comparison: String::new(),
                replay: String::new(),
            });
            report.write(&directory)?;
            bail!("{detail}");
        }
    }
    ensure!(
        !report
            .counts
            .keys()
            .any(|s| ["failed", "unexpected_pass", "blocked"].contains(&s.as_str())),
        "external SQL checks failed; see {}",
        directory.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_labels_preserve_the_first_result_and_shape() {
        let mut labels = HashMap::new();
        verify_label(&mut labels, "same", "2 columns: hash-a").unwrap();
        verify_label(&mut labels, "same", "2 columns: hash-a").unwrap();
        assert!(verify_label(&mut labels, "same", "2 columns: hash-b").is_err());
        assert!(verify_label(&mut labels, "same", "1 columns: hash-a").is_err());
        verify_label(&mut labels, "same", "2 columns: hash-a").unwrap();
    }
    #[test]
    fn transport_failure_cannot_satisfy_expected_error() {
        let policy = Policy {
            error_states: Some(vec!["42000".into()]),
            ..Default::default()
        };
        let error = WireError {
            state: None,
            message: "connection lost".into(),
        };
        assert!(verify_error(&error, &ExpectedError::Empty, &policy).is_err());
        assert!(
            verify_error(
                &WireError {
                    state: Some("42000".into()),
                    message: "syntax".into()
                },
                &ExpectedError::Empty,
                &policy
            )
            .is_ok()
        );
        assert!(
            verify_error(
                &WireError {
                    state: Some("HY000".into()),
                    message: "internal".into()
                },
                &ExpectedError::Empty,
                &policy
            )
            .is_err()
        );
    }
}
