use crate::compare::Tolerance;
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqllogictest::{Condition, Connection, DefaultColumnType, Record, parse_with_name};
use std::{collections::BTreeMap, fs, path::Path};

pub type SqlRecord = Record<DefaultColumnType>;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub id: String,
    pub suite: String,
    pub path: String,
    pub repository: String,
    pub revision: String,
    pub source_path: String,
    pub sha256: String,
    pub format: String,
}
#[derive(Clone, Debug)]
pub struct Case {
    pub id: String,
    pub line: u32,
    pub record: SqlRecord,
}

impl Case {
    pub fn condition_exclusion(&self) -> Option<String> {
        let conditions = match &self.record {
            Record::Statement { conditions, .. } | Record::Query { conditions, .. } => conditions,
            _ => return None,
        };
        conditions
            .iter()
            .find(|c| match c {
                Condition::OnlyIf { label } => label != "thindb",
                Condition::SkipIf { label } => label == "thindb",
            })
            .map(|c| format!("upstream condition: {c:?}"))
    }
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub kind: Option<String>,
    pub reason: Option<String>,
    pub issue: Option<String>,
    pub signature: Option<String>,
    pub tolerance: Option<Tolerance>,
    pub error_states: Option<Vec<String>>,
    #[serde(default)]
    pub setup_noop: bool,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Patch {
    pub before: String,
    pub after: String,
    pub reason: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludedRange {
    pub source: String,
    pub first: u32,
    pub last: u32,
    pub reason: String,
    pub kind: String,
}
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Policies {
    #[serde(default)]
    pub cases: BTreeMap<String, Policy>,
    #[serde(default)]
    pub patches: BTreeMap<String, Patch>,
    #[serde(default)]
    pub excluded_ranges: Vec<ExcludedRange>,
}
impl Policies {
    pub fn get(&self, source: &Source, case: &Case) -> Result<Policy> {
        let policy = self.cases.get(&case.id).cloned().unwrap_or_else(|| {
            self.excluded_ranges
                .iter()
                .find(|r| r.source == source.id && case.line >= r.first && case.line <= r.last)
                .map(|r| Policy {
                    kind: Some(r.kind.clone()),
                    reason: Some(r.reason.clone()),
                    ..Default::default()
                })
                .unwrap_or_default()
        });
        if let Some(kind) = &policy.kind {
            ensure!(
                ["unsupported", "semantic_difference", "known_bug"].contains(&kind.as_str()),
                "unknown classification for {}",
                case.id
            );
            ensure!(
                policy.reason.as_ref().is_some_and(|s| !s.is_empty()),
                "missing exclusion reason: {}",
                case.id
            );
            if kind == "known_bug" {
                ensure!(
                    policy.issue.as_ref().is_some_and(|s| !s.is_empty())
                        && policy.signature.as_ref().is_some_and(|s| !s.is_empty()),
                    "known bug needs issue and signature: {}",
                    case.id
                );
            }
        }
        if let Some(t) = &policy.tolerance {
            ensure!(
                t.absolute.is_finite()
                    && t.relative.is_finite()
                    && t.absolute >= 0.0
                    && t.relative >= 0.0,
                "invalid tolerance"
            );
        }
        if policy.tolerance.is_some() || policy.error_states.is_some() || policy.setup_noop {
            ensure!(
                policy.reason.as_ref().is_some_and(|s| !s.is_empty()),
                "comparison policy requires a reason: {}",
                case.id
            );
        }
        Ok(policy)
    }
    pub fn apply(&self, case: &mut Case) -> Result<()> {
        if let Some(patch) = self.patches.get(&case.id) {
            let sql = match &mut case.record {
                Record::Query { sql, .. } | Record::Statement { sql, .. } => sql,
                _ => bail!("patch is not SQL"),
            };
            ensure!(*sql == patch.before, "stale adaptation for {}", case.id);
            ensure!(!patch.reason.is_empty(), "adaptation requires reason");
            *sql = patch.after.clone();
        }
        Ok(())
    }
}

pub fn load(root: &Path, source: &Source) -> Result<Vec<Case>> {
    ensure!(
        source.revision.len() == 40 && !source.repository.is_empty(),
        "unpinned source"
    );
    let bytes = fs::read(root.join(&source.path))?;
    ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == source.sha256,
        "source checksum mismatch: {}",
        source.id
    );
    let text = String::from_utf8(bytes)?.replace("\r\n", "\n");
    let cases = match source.format.as_str() {
        "starrocks" => starrocks(&text, &source.id)?,
        "slt" => slt(&text, &source.id)?,
        _ => bail!("unknown format"),
    };
    validate_reference_labels(&cases)?;
    Ok(cases)
}

fn validate_reference_labels(cases: &[Case]) -> Result<()> {
    let mut labels = BTreeMap::new();
    for case in cases {
        if let Record::Query {
            expected:
                sqllogictest::QueryExpect::Results {
                    types,
                    results,
                    label: Some(label),
                    ..
                },
            ..
        } = &case.record
        {
            let fingerprint = crate::compare::reference_fingerprint(types.len(), results)?;
            if let Some(previous) = labels.insert(label, fingerprint.clone()) {
                ensure!(
                    previous == fingerprint,
                    "inconsistent upstream expectations for label {label}: {}",
                    case.id
                );
            }
        }
    }
    Ok(())
}

fn slt(text: &str, source: &str) -> Result<Vec<Case>> {
    // DuckDB ends error messages at the next blank line; the Rust dialect
    // requires two. Parse complete records separately to retain upstream lines.
    let mut cases = vec![];
    let mut block = vec![];
    let mut start = 1;
    let mut pending_conditions = vec![];
    for (i, line) in text.lines().chain(std::iter::once("")).enumerate() {
        if line.is_empty() {
            if !block.is_empty() {
                let script = block.join("\n") + "\n\n\n";
                for mut record in parse_with_name::<DefaultColumnType>(&script, source.to_string())
                    .with_context(|| format!("{source}:{start}"))?
                {
                    match &mut record {
                        Record::Statement {
                            loc,
                            conditions,
                            connection,
                            ..
                        }
                        | Record::Query {
                            loc,
                            conditions,
                            connection,
                            ..
                        } => {
                            ensure!(
                                *connection == Connection::Default,
                                "named connections require an explicit import policy"
                            );
                            *conditions = std::mem::take(&mut pending_conditions);
                            let line = start + loc.line() - 1;
                            cases.push(Case {
                                id: format!("{source}:{line}"),
                                line,
                                record,
                            });
                        }
                        Record::Condition(condition) => pending_conditions.push(condition.clone()),
                        Record::Comment(_) | Record::Newline | Record::HashThreshold { .. } => {}
                        _ => bail!(
                            "unrecognized/unsupported directive at {source}:{start}: {record:?}"
                        ),
                    }
                }
                block.clear();
            }
            start = i as u32 + 2;
        } else {
            block.push(line);
        }
    }
    ensure!(
        pending_conditions.is_empty(),
        "condition without a following SQL record"
    );
    Ok(cases)
}

fn starrocks(text: &str, source: &str) -> Result<Vec<Case>> {
    let mut cases = vec![];
    let mut sql = vec![];
    let mut expected = vec![];
    let mut in_result = false;
    let mut start = 0;
    for (i, line) in text.lines().enumerate() {
        if line == "-- result:" {
            ensure!(!sql.is_empty() && !in_result, "invalid result delimiter");
            in_result = true;
            continue;
        }
        if line == "-- !result" {
            ensure!(in_result, "unmatched result end");
            let mut query = sql.join("\n");
            let ordered = query.starts_with("[ORDER]");
            if ordered {
                query = query.trim_start_matches("[ORDER]").into();
            }
            ensure!(
                !query.starts_with('[')
                    && !query.starts_with("shell:")
                    && !query.starts_with("function:"),
                "unsupported StarRocks directive at {source}:{start}"
            );
            let is_query = query
                .trim_start()
                .to_ascii_uppercase()
                .starts_with("SELECT")
                || query.trim_start().to_ascii_uppercase().starts_with("WITH");
            let script = if is_query {
                if expected
                    .first()
                    .is_some_and(|s: &String| s.starts_with("[REGEX]"))
                {
                    format!(
                        "query error {}\n{query}\n\n",
                        expected[0].trim_start_matches("[REGEX]")
                    )
                } else {
                    let width = expected.first().map(|s| s.split('\t').count()).unwrap_or(1);
                    let results: Vec<String> = expected
                        .iter()
                        .map(|s| {
                            s.split('\t')
                                .map(|c| {
                                    if c == "None" {
                                        "NULL"
                                    } else if c.is_empty() {
                                        "(empty)"
                                    } else {
                                        c
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join("\t")
                        })
                        .collect();
                    format!(
                        "query {} {}\n{query}\n----\n{}\n\n",
                        "T".repeat(width),
                        if ordered { "nosort" } else { "rowsort" },
                        results.join("\n")
                    )
                }
            } else {
                ensure!(
                    expected.is_empty(),
                    "unexpected statement output at {source}:{start}"
                );
                format!("statement ok\n{query}\n\n")
            };
            let record = parse_with_name::<DefaultColumnType>(&script, source.to_string())?
                .into_iter()
                .find(|r| matches!(r, Record::Query { .. } | Record::Statement { .. }))
                .context("empty StarRocks case")?;
            cases.push(Case {
                id: format!("{source}:{start}"),
                line: start,
                record,
            });
            sql.clear();
            expected.clear();
            in_result = false;
            start = 0;
        } else if in_result {
            expected.push(line.to_string());
        } else if !line.trim().is_empty() && !line.trim_start().starts_with("--") {
            if start == 0 {
                start = i as u32 + 1;
            }
            sql.push(line);
        }
    }
    ensure!(
        !in_result && sql.is_empty(),
        "unterminated StarRocks record"
    );
    Ok(cases)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_pinned_sources_parse_and_have_unique_ids() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let sources: Vec<Source> =
            serde_json::from_str(&fs::read_to_string(root.join("sources.json")).unwrap()).unwrap();
        let mut queries = 0;
        let mut ids = std::collections::HashSet::new();
        let policies: Policies =
            serde_json::from_str(&fs::read_to_string(root.join("policies.json")).unwrap()).unwrap();
        for source in &sources {
            for mut case in load(root, source).unwrap() {
                assert!(ids.insert(case.id.clone()));
                let policy = policies.get(source, &case).unwrap();
                if let Some(issue) = policy.issue {
                    assert!(
                        root.join(issue.split('#').next().unwrap()).is_file(),
                        "missing local issue record"
                    );
                }
                policies.apply(&mut case).unwrap();
                if source.suite == "sqlite" && matches!(case.record, Record::Query { .. }) {
                    queries += 1;
                }
            }
        }
        assert_eq!(queries, 8152);
        assert_eq!(ids.len(), 9766);
        assert!(
            policies
                .cases
                .keys()
                .chain(policies.patches.keys())
                .all(|id| ids.contains(id))
        );
    }
    #[test]
    fn starrocks_preserves_values_and_rejects_helpers() {
        assert!(starrocks("SELECT 1;\n-- result:\n1\n-- !result\n", "test").is_ok());
        assert!(starrocks("shell: echo bad\n-- result:\n-- !result\n", "test").is_err());
        assert!(starrocks("SELECT 1;\n-- result:\n1\n", "test").is_err());
    }
    #[test]
    fn conditions_whitespace_and_unknown_directives() {
        let cases = slt("onlyif thindb\n\nquery T\nSELECT ' x  y ';\n----\n x  y \n\nskipif thindb\nquery I\nSELECT 1;\n----\n1\n", "test").unwrap();
        assert!(cases[0].condition_exclusion().is_none());
        assert!(cases[1].condition_exclusion().is_some());
        if let Record::Query {
            expected: sqllogictest::QueryExpect::Results { results, .. },
            ..
        } = &cases[0].record
        {
            assert_eq!(results, &[" x  y "]);
        } else {
            panic!("expected query");
        }
        assert!(slt("require imaginary_extension\n", "test").is_err());
        assert!(slt("onlyif thindb\n", "test").is_err());
        assert!(slt("connection secondary\nstatement ok\nSELECT 1\n", "test").is_err());
        let labelled = slt("query I nosort same\nSELECT 1\n----\n1\n\nskipif thindb\nquery I nosort same\nSELECT 2\n----\n2\n", "test").unwrap();
        assert!(validate_reference_labels(&labelled).is_err());
    }
}
