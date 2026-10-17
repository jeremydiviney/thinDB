use anyhow::{Result, bail, ensure};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use sqllogictest::SortMode;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum Cell {
    Null,
    Text(String),
    Number(String),
    Float(String),
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Output {
    pub columns: usize,
    pub rows: Vec<Vec<Cell>>,
    pub affected: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tolerance {
    pub absolute: f64,
    pub relative: f64,
}

// Decimal strings avoid losing BIGINT/LARGEINT bits or decimal scale through f64.
pub fn decimal(s: &str) -> Option<String> {
    let (negative, s) = if let Some(s) = s.strip_prefix('-') {
        (true, s)
    } else {
        (false, s.strip_prefix('+').unwrap_or(s))
    };
    let mut parts = s.split(['e', 'E']);
    let base = parts.next()?;
    let exponent: i32 = parts.next().map(str::parse).transpose().ok()?.unwrap_or(0);
    if parts.next().is_some() || exponent.unsigned_abs() > 10000 {
        return None;
    }
    let mut parts = base.split('.');
    let whole = parts.next()?;
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some()
        || whole.len() + fraction.len() == 0
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{fraction}");
    let mut scale = fraction.len() as i32 - exponent;
    while digits.ends_with('0') && !digits.is_empty() {
        digits.pop();
        scale -= 1;
    }
    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        return Some("0".into());
    }
    Some(format!(
        "{}{digits}e{}",
        if negative { "-" } else { "" },
        -scale
    ))
}

impl Cell {
    pub fn slt(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Text(s) if s.is_empty() => "(empty)".into(),
            Self::Text(s) | Self::Number(s) | Self::Float(s) => s.clone(),
        }
    }
    fn matches(&self, expected: &str, tolerance: Option<&Tolerance>) -> bool {
        match self {
            Self::Null => expected == "NULL",
            Self::Text(s) => {
                if s.is_empty() {
                    expected == "(empty)"
                } else {
                    s == expected
                }
            }
            Self::Number(s) => decimal(s).is_some_and(|v| Some(v) == decimal(expected)),
            Self::Float(s) => {
                if s == expected || decimal(s).is_some_and(|v| Some(v) == decimal(expected)) {
                    return true;
                }
                let Some(t) = tolerance else {
                    return false;
                };
                let (Ok(a), Ok(b)) = (s.parse::<f64>(), expected.parse::<f64>()) else {
                    return false;
                };
                a.is_finite()
                    && b.is_finite()
                    && (a - b).abs() <= t.absolute + t.relative * a.abs().max(b.abs())
            }
        }
    }
}

fn sorted_rows(output: &Output, width: usize, sort: Option<SortMode>) -> Result<Vec<Vec<Cell>>> {
    ensure!(
        output.columns == width,
        "column count: expected {width}, actual {}",
        output.columns
    );
    ensure!(
        output.rows.iter().all(|r| r.len() == width),
        "malformed result row width"
    );
    let mut rows = output.rows.clone();
    match sort {
        Some(SortMode::RowSort) => {
            rows.sort_by_cached_key(|r| r.iter().map(Cell::slt).collect::<Vec<_>>())
        }
        Some(SortMode::ValueSort) => {
            rows = rows.into_iter().flatten().map(|v| vec![v]).collect();
            rows.sort_by_cached_key(|r| r[0].slt());
        }
        _ => {}
    }
    Ok(rows)
}

fn hash_rows(rows: &[Vec<Cell>]) -> String {
    let values: Vec<_> = rows.iter().flatten().collect();
    let mut digest = Md5::new();
    for value in &values {
        digest.update(value.slt().as_bytes());
        digest.update(b"\n");
    }
    format!("{} values hashing to {:x}", values.len(), digest.finalize())
}

pub fn reference_fingerprint(width: usize, expected: &[String]) -> Result<String> {
    if expected.len() == 1 && expected[0].contains(" values hashing to ") {
        return Ok(format!("{width} columns: {}", expected[0]));
    }
    let values: Vec<_> = if width > 1 && expected.iter().any(|s| s.contains('\t')) {
        ensure!(
            expected.iter().all(|s| s.split('\t').count() == width),
            "malformed expected row width"
        );
        expected.iter().flat_map(|s| s.split('\t')).collect()
    } else {
        expected.iter().map(String::as_str).collect()
    };
    ensure!(
        width > 0 && values.len() % width == 0,
        "malformed expected value count"
    );
    let rows: Vec<_> = values
        .chunks(width)
        .map(|row| row.iter().map(|s| Cell::Text((*s).into())).collect())
        .collect();
    Ok(format!("{width} columns: {}", hash_rows(&rows)))
}

pub fn compare(
    output: &Output,
    width: usize,
    expected: &[String],
    sort: Option<SortMode>,
    tolerance: Option<&Tolerance>,
) -> Result<String> {
    let rows = sorted_rows(output, width, sort)?;
    let values: Vec<_> = rows.iter().flatten().collect();
    let fingerprint = hash_rows(&rows);
    if expected.len() == 1 && expected[0].contains(" values hashing to ") {
        ensure!(
            fingerprint == expected[0],
            "hash mismatch: expected {}, actual {fingerprint}",
            expected[0]
        );
        return Ok(fingerprint);
    }
    let expected: Vec<&str> = if width > 1 && expected.iter().any(|line| line.contains('\t')) {
        ensure!(
            expected
                .iter()
                .all(|line| line.split('\t').count() == width),
            "malformed expected row width"
        );
        expected.iter().flat_map(|line| line.split('\t')).collect()
    } else {
        expected.iter().map(String::as_str).collect()
    };
    ensure!(
        values.len() == expected.len(),
        "value count: expected {}, actual {} ({} rows)",
        expected.len(),
        values.len(),
        output.rows.len()
    );
    for (i, (actual, expected)) in values.iter().zip(expected).enumerate() {
        if !actual.matches(expected, tolerance) {
            bail!(
                "value mismatch at row {}, column {}: expected {:?}, actual {:?}",
                i / width.max(1) + 1,
                i % width.max(1) + 1,
                expected,
                actual
            );
        }
    }
    Ok(fingerprint)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn out(rows: Vec<Vec<Cell>>, width: usize) -> Output {
        Output {
            columns: width,
            rows,
            affected: 0,
        }
    }
    fn check(o: &Output, w: usize, e: &[&str], s: Option<SortMode>) -> bool {
        compare(
            o,
            w,
            &e.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            s,
            None,
        )
        .is_ok()
    }
    #[test]
    fn exact_values_and_shape() {
        let o = out(
            vec![vec![
                Cell::Number("9007199254740993".into()),
                Cell::Number("123456789012345678.1200".into()),
            ]],
            2,
        );
        assert!(check(
            &o,
            2,
            &["9007199254740993", "123456789012345678.12"],
            None
        ));
        assert!(!check(
            &o,
            2,
            &["9007199254740992", "123456789012345678.12"],
            None
        ));
        assert!(!check(
            &o,
            1,
            &["9007199254740993", "123456789012345678.12"],
            None
        ));
        assert!(check(&out(vec![], 2), 2, &[], None));
        assert!(!check(&out(vec![], 2), 1, &[], None));
    }
    #[test]
    fn null_empty_and_whitespace_are_distinct() {
        let o = out(
            vec![vec![
                Cell::Null,
                Cell::Text("".into()),
                Cell::Text(" a  b ".into()),
            ]],
            3,
        );
        assert!(check(&o, 3, &["NULL", "(empty)", " a  b "], None));
        assert!(!check(&o, 3, &["(empty)", "NULL", "a b"], None));
        assert!(check(
            &out(vec![vec![Cell::Text(" a\tb ".into())]], 1),
            1,
            &[" a\tb "],
            None
        ));
    }
    #[test]
    fn order_duplicates_and_hashes() {
        let o = out(
            vec![
                vec![Cell::Number("2".into())],
                vec![Cell::Number("1".into())],
                vec![Cell::Number("1".into())],
            ],
            1,
        );
        assert!(check(&o, 1, &["1", "1", "2"], Some(SortMode::RowSort)));
        assert!(!check(&o, 1, &["1", "1", "2"], None));
        assert!(!check(&o, 1, &["1", "2"], Some(SortMode::RowSort)));
        assert!(!check(&o, 1, &["3 values hashing to bad"], None));
        let hash = compare(&o, 1, &["2".into(), "1".into(), "1".into()], None, None).unwrap();
        assert!(check(&o, 1, &[&hash], None));
        let swapped = out(
            vec![vec![Cell::Number("2".into()), Cell::Number("1".into())]],
            2,
        );
        assert!(!check(&swapped, 2, &["1", "2"], Some(SortMode::RowSort)));
        assert!(check(&swapped, 2, &["1", "2"], Some(SortMode::ValueSort)));
        let two_rows = out(
            vec![vec![Cell::Number("1".into()), Cell::Number("2".into())]; 2],
            2,
        );
        assert!(!check(&two_rows, 2, &["1\t2\t1\t2"], None));
        let wrong_shape = out(
            vec![vec![Cell::Text("1\t2".into()), Cell::Text("3\t4".into())]],
            2,
        );
        assert!(!check(&wrong_shape, 2, &["1\t2", "3\t4"], None));
    }
    #[test]
    fn float_tolerance_is_opt_in_and_never_used_for_decimals() {
        let t = Tolerance {
            absolute: 1e-12,
            relative: 1e-9,
        };
        assert!(!Cell::Float("1.0000000001".into()).matches("1", None));
        assert!(Cell::Float("1.0000000001".into()).matches("1", Some(&t)));
        assert!(!Cell::Number("1.0000000001".into()).matches("1", Some(&t)));
        assert_eq!(decimal("-0.000"), decimal("0"));
        assert_eq!(decimal("1.20e2"), decimal("120"));
    }
}
