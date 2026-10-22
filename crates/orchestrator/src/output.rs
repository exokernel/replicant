//! Writes the per-trial and summary records to stdout, as CSV or JSON Lines.
//!
//! [`COLUMNS`] is the only definition of the output schema. The archived CSVs
//! in `data/` were written with it, so its existing columns must keep their
//! names, order, and formatting.

use crate::runner::RunResult;

/// How per-trial and summary records are written.
///
/// Both formats mix per-trial records with the per-scenario summary. A
/// `row_type` column tells them apart, holding either `trial` or `summary`.
#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum OutputFormat {
    /// CSV rows. The header is written once, before the first record.
    Csv,
    /// JSON Lines: one object per line, with no enclosing array.
    Json,
}

/// The output schema, in column order. This is its only definition.
///
/// Both row kinds carry every column. A column a row kind does not fill becomes
/// an empty CSV field, and is left out of the JSON object entirely. [`emit`]
/// checks each row against this list by name and position, so a column cannot
/// be added to one row kind and forgotten in the other.
///
/// **Columns are append-only.** `wiring_ms` and `mean_wiring_ms` sit at the end
/// rather than beside the other timings, so the original twelve keep the
/// positions that the archived CSVs in `data/` were written with.
const COLUMNS: &[&str] = &[
    "row_type",
    "scenario",
    "trial",
    "node_count",
    "op_count",
    "topology_kind",
    "edge_count",
    "diameter",
    "convergence_ms",
    "mean_ms",
    "p50_ms",
    "p95_ms",
    "wiring_ms",
    "mean_wiring_ms",
];

/// One output row: `(column, value)` pairs in [`COLUMNS`] order. `None` marks a
/// column that does not apply to this row kind.
pub type Row = Vec<(&'static str, Option<serde_json::Value>)>;

/// Returns the CSV header line.
pub fn header() -> String {
    COLUMNS.join(",")
}

/// Builds the row for one trial.
pub fn trial_row(
    scenario: &str,
    trial: usize,
    node_count: usize,
    op_count: usize,
    result: &RunResult,
) -> Row {
    vec![
        ("row_type", Some("trial".into())),
        ("scenario", Some(scenario.into())),
        ("trial", Some(trial.into())),
        ("node_count", Some(node_count.into())),
        ("op_count", Some(op_count.into())),
        ("topology_kind", Some(result.topology_kind.into())),
        ("edge_count", Some(result.edge_count.into())),
        ("diameter", Some(result.diameter.into())),
        ("convergence_ms", Some(result.convergence_ms.into())),
        ("mean_ms", None),
        ("p50_ms", None),
        ("p95_ms", None),
        ("wiring_ms", Some(result.wiring_ms.into())),
        ("mean_wiring_ms", None),
    ]
}

/// Builds the summary row for one scenario.
///
/// `shape` supplies the structural fields: topology kind, edge count, and
/// diameter. They are the same in every trial, so any trial's result will do.
///
/// On this row kind the `trial` column holds the trial *count*, not an index.
pub fn summary_row(
    scenario: &str,
    n_trials: usize,
    node_count: usize,
    op_count: usize,
    shape: &RunResult,
    s: &TrialStats,
    mean_wiring_ms: f64,
) -> Row {
    vec![
        ("row_type", Some("summary".into())),
        ("scenario", Some(scenario.into())),
        ("trial", Some(n_trials.into())),
        ("node_count", Some(node_count.into())),
        ("op_count", Some(op_count.into())),
        ("topology_kind", Some(shape.topology_kind.into())),
        ("edge_count", Some(shape.edge_count.into())),
        ("diameter", Some(shape.diameter.into())),
        ("convergence_ms", None),
        ("mean_ms", Some(s.mean.into())),
        ("p50_ms", Some(s.p50.into())),
        ("p95_ms", Some(s.p95.into())),
        ("wiring_ms", None),
        ("mean_wiring_ms", Some(mean_wiring_ms.into())),
    ]
}

/// Prints `row` in the requested format.
pub fn emit(fmt: OutputFormat, row: &Row) {
    debug_assert!(
        row.iter()
            .map(|(name, _)| *name)
            .eq(COLUMNS.iter().copied()),
        "row columns do not match COLUMNS in name or order"
    );
    match fmt {
        OutputFormat::Csv => println!("{}", csv_line(row)),
        OutputFormat::Json => println!("{}", json_line(row)),
    }
}

/// Renders a row as one CSV line.
fn csv_line(row: &Row) -> String {
    row.iter()
        .map(|(_, value)| csv_cell(value.as_ref()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Renders one CSV cell.
///
/// An absent value is empty. A float gets three decimal places, which is the
/// precision every millisecond column has always used. A string is written
/// bare, because no field in this schema can contain a comma. An integer prints
/// as itself.
fn csv_cell(value: Option<&serde_json::Value>) -> String {
    match value {
        None => String::new(),
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) if n.is_f64() => {
            format!("{:.3}", n.as_f64().expect("checked is_f64"))
        }
        Some(other) => other.to_string(),
    }
}

/// Renders a row as one JSON object. An absent column is left out rather than
/// written as `null`.
fn json_line(row: &Row) -> String {
    let obj: serde_json::Map<String, serde_json::Value> = row
        .iter()
        .filter_map(|(name, value)| value.clone().map(|v| ((*name).to_owned(), v)))
        .collect();
    serde_json::Value::Object(obj).to_string()
}

// ── Statistics ───────────────────────────────────────────────────────────────

/// Convergence statistics across one scenario's trials, in milliseconds.
pub struct TrialStats {
    /// Arithmetic mean.
    pub mean: f64,
    /// Nearest-rank median.
    pub p50: f64,
    /// Nearest-rank 95th percentile.
    pub p95: f64,
}

/// Returns the mean, p50, and p95 of `values`, which must not be empty.
pub fn stats(values: &[f64]) -> TrialStats {
    debug_assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    TrialStats {
        mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
    }
}

/// Returns the nearest-rank percentile `p` of an already-sorted slice.
fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((p / 100.0 * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[idx]
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── stats / percentile ─────────────────────────────────────────────────

    #[test]
    fn stats_single_value() {
        let s = stats(&[42.0]);
        assert_eq!(s.mean, 42.0);
        assert_eq!(s.p50, 42.0);
        assert_eq!(s.p95, 42.0);
    }

    #[test]
    fn stats_mean_and_percentiles() {
        // [10, 20, 30, 40, 50] — mean=30, p50=30, p95=50
        let s = stats(&[50.0, 10.0, 40.0, 30.0, 20.0]);
        assert_eq!(s.mean, 30.0);
        assert_eq!(s.p50, 30.0);
        assert_eq!(s.p95, 50.0);
    }

    #[test]
    fn percentile_p100_is_max() {
        let sorted = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(&sorted, 100.0), 5.0);
    }

    #[test]
    fn percentile_p0_is_min() {
        let sorted = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        // ceil(0/100 * 5) = 0, saturating_sub(1) = 0 → first element
        assert_eq!(percentile(&sorted, 0.0), 1.0);
    }

    // ── Output schema ──────────────────────────────────────────────────────

    fn sample_result() -> RunResult {
        RunResult {
            convergence_ms: 12.3456,
            wiring_ms: 1.5,
            total_ops: 10,
            topology_kind: "full_mesh",
            edge_count: 6,
            diameter: 1,
        }
    }

    fn sample_stats() -> TrialStats {
        TrialStats {
            mean: 1.0,
            p50: 2.0,
            p95: 3.0,
        }
    }

    /// The header and both row kinds come from one column list, so they cannot
    /// drift apart.
    #[test]
    fn header_and_both_row_kinds_share_one_column_list() {
        let trial = trial_row("s", 1, 3, 10, &sample_result());
        let summary = summary_row("s", 5, 3, 10, &sample_result(), &sample_stats(), 1.5);

        for row in [&trial, &summary] {
            let names: Vec<&str> = row.iter().map(|(name, _)| *name).collect();
            assert_eq!(names, COLUMNS, "row columns diverged from COLUMNS");
        }
        assert_eq!(header().split(',').count(), COLUMNS.len());
        assert_eq!(csv_line(&trial).split(',').count(), COLUMNS.len());
        assert_eq!(csv_line(&summary).split(',').count(), COLUMNS.len());
    }

    /// Each row kind fills its own columns and leaves the other kind's empty.
    #[test]
    fn row_kinds_populate_complementary_columns() {
        let trial = trial_row("s", 1, 3, 10, &sample_result());
        let summary = summary_row("s", 5, 3, 10, &sample_result(), &sample_stats(), 1.5);
        let filled = |row: &Row, name: &str| {
            row.iter()
                .find(|(n, _)| *n == name)
                .expect("column exists")
                .1
                .is_some()
        };

        for name in ["convergence_ms", "wiring_ms"] {
            assert!(filled(&trial, name), "trial row missing {name}");
            assert!(!filled(&summary, name), "summary row should omit {name}");
        }
        for name in ["mean_ms", "p50_ms", "p95_ms", "mean_wiring_ms"] {
            assert!(filled(&summary, name), "summary row missing {name}");
            assert!(!filled(&trial, name), "trial row should omit {name}");
        }
    }

    /// Millisecond columns keep three decimal places, an absent column is an
    /// empty field, and strings are unquoted. This is the CSV shape the
    /// archived sweeps in `data/` were written with.
    #[test]
    fn csv_cells_preserve_the_historical_formatting() {
        let line = csv_line(&trial_row("full-mesh-n3", 2, 3, 10, &sample_result()));
        let cells: Vec<&str> = line.split(',').collect();
        assert_eq!(cells[0], "trial");
        assert_eq!(cells[1], "full-mesh-n3");
        assert_eq!(cells[3], "3");
        assert_eq!(cells[8], "12.346", "convergence_ms rounds to 3 places");
        assert_eq!(cells[9], "", "mean_ms is summary-only");
        assert_eq!(cells[12], "1.500", "wiring_ms rounds to 3 places");
    }

    /// A JSON Lines row describes itself: every filled column becomes a key,
    /// and a column that does not apply is left out rather than set to `null`.
    #[test]
    fn json_rows_omit_inapplicable_columns() {
        let json: serde_json::Value =
            serde_json::from_str(&json_line(&trial_row("s", 1, 3, 10, &sample_result()))).unwrap();
        assert_eq!(json["row_type"], "trial");
        assert_eq!(json["convergence_ms"], 12.3456);
        assert_eq!(json["wiring_ms"], 1.5);
        assert!(json.get("mean_ms").is_none());

        let json: serde_json::Value = serde_json::from_str(&json_line(&summary_row(
            "s",
            5,
            3,
            10,
            &sample_result(),
            &sample_stats(),
            1.5,
        )))
        .unwrap();
        assert_eq!(json["trial"], 5, "summary `trial` holds the trial count");
        assert_eq!(json["mean_wiring_ms"], 1.5);
        assert!(json.get("convergence_ms").is_none());
    }
}
