//! Tests of the `orchestrator` binary as a user runs it: its flags, the files
//! that it writes, and the errors that must stop a run before its first trial.
//!
//! Every run uses in-process replicas and a small scenario, so each test takes
//! well under a second.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

/// The path of the binary that Cargo built for these tests.
const ORCHESTRATOR: &str = env!("CARGO_BIN_EXE_orchestrator");

/// A two-replica steady-state scenario with three writes.
const TWO_REPLICAS: &str = r#"
name = "two-replicas"

[topology]
node_count = 2
connections = "full_mesh"
write_pattern = "round_robin"
op_count = 3
"#;

/// Returns an empty directory for one test, under Cargo's temp directory for
/// integration tests.
fn scratch(test: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("orchestrator-cli")
        .join(test);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes `toml` to `dir/name.toml` and returns the path.
fn scenario(dir: &Path, name: &str, toml: &str) -> PathBuf {
    let path = dir.join(format!("{name}.toml"));
    std::fs::write(&path, toml).unwrap();
    path
}

/// Runs the orchestrator with `args`, with no OTLP endpoint.
fn run(args: &[&str]) -> Output {
    Command::new(ORCHESTRATOR)
        .args(args)
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .output()
        .unwrap()
}

/// Returns the standard error of `out` as text.
fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Parses each non-empty line of `text` as JSON.
fn json_lines(text: &str) -> Vec<Value> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect()
}

#[test]
fn crdt_with_replicas_is_refused() {
    let out = run(&["--crdt", "yrs", "--replicas", "localhost:1"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("--crdt applies only to in-process replicas"));
}

#[test]
fn zero_trials_are_refused() {
    let out = run(&["--trials", "0"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("--trials must be at least 1"));
}

#[test]
fn an_empty_replicas_entry_is_refused() {
    let out = run(&["--replicas", "localhost:1,=peer:2"]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("empty address"));
}

/// A dry run writes the provenance file and runs no trial, so it prints no
/// result row.
#[test]
fn a_dry_run_writes_provenance_and_no_rows() {
    let dir = scratch("dry-run");
    let file = scenario(&dir, "two", TWO_REPLICAS);
    let provenance = dir.join("provenance.json");
    let out = run(&[
        "--dry-run",
        "--crdt",
        "loro",
        "--trials",
        "4",
        "--provenance",
        provenance.to_str().unwrap(),
        file.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(out.stdout.is_empty());
    let p: Value = serde_json::from_slice(&std::fs::read(&provenance).unwrap()).unwrap();
    assert_eq!(p["run"]["crdt"], "loro");
    assert_eq!(p["run"]["trials"], 4);
    assert_eq!(p["run"]["node_source"], "in_process");
    assert_eq!(p["scenarios"][0]["name"], "two-replicas");
    assert!(p["git"]["commit"].is_string());
}

/// One run with every output: JSON rows, a trace, and a metrics file.
#[test]
fn a_run_writes_rows_a_trace_and_metrics() {
    let dir = scratch("outputs");
    let file = scenario(&dir, "two", TWO_REPLICAS);
    let (trace, metrics) = (dir.join("trace.jsonl"), dir.join("metrics.json"));
    let out = run(&[
        "--crdt",
        "yrs",
        "--trials",
        "2",
        "--output",
        "json",
        "--trace",
        trace.to_str().unwrap(),
        "--metrics-file",
        metrics.to_str().unwrap(),
        file.to_str().unwrap(),
    ]);
    assert!(out.status.success(), "{}", stderr(&out));

    let rows = json_lines(&String::from_utf8_lossy(&out.stdout));
    let kinds: Vec<_> = rows
        .iter()
        .map(|r| r["row_type"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["trial", "trial", "summary"]);
    assert!(rows[0]["convergence_ms"].as_f64().unwrap() > 0.0);

    let events = json_lines(&std::fs::read_to_string(&trace).unwrap());
    for trial in [1, 2] {
        let names: Vec<_> = events
            .iter()
            .filter(|e| e["trial"] == trial)
            .map(|e| e["event"].as_str().unwrap())
            .collect();
        for expected in [
            "wired",
            "clock_start",
            "write",
            "poll",
            "converged",
            "stats",
        ] {
            assert!(
                names.contains(&expected),
                "trial {trial}: no {expected} in {names:?}"
            );
        }
    }

    // The exporter writes one snapshot per export. The last one is complete.
    let snapshots = json_lines(&std::fs::read_to_string(&metrics).unwrap());
    let last = snapshots.last().expect("at least one metrics snapshot");
    let names: Vec<_> = last["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    for expected in [
        "replicant.op.duration",
        "replicant.sync.messages.tx",
        "replicant.sync.messages.rx",
        "replicant.doc.size_bytes",
    ] {
        assert!(names.contains(&expected), "no {expected} in {names:?}");
    }
}

/// A missing baseline trace must stop the run before its first trial, not in
/// the middle of a sweep.
#[test]
fn a_missing_baseline_fails_before_the_first_trial() {
    let dir = scratch("missing-baseline");
    let file = scenario(
        &dir,
        "paper",
        r#"
name = "paper"

[partition_heal]
node_count = 2
write_pattern = "concentrated"
workload = "text_splice"
ops_per_group = 1

[partition_heal.baseline]
trace = "absent.json.gz"
sha256 = "00"
length = 10

[[partition_heal.groups]]
nodes = [0]

[[partition_heal.groups]]
nodes = [1]
"#,
    );
    let out = run(&[
        "--output",
        "json",
        "--data-dir",
        dir.to_str().unwrap(),
        file.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(out.stdout.is_empty(), "a row was written before the check");
    assert!(stderr(&out).contains("absent.json.gz"), "{}", stderr(&out));
}

#[test]
fn a_scenario_with_both_tables_is_refused_by_name() {
    let dir = scratch("both-tables");
    let file = scenario(
        &dir,
        "both",
        &format!(
            "{TWO_REPLICAS}\n[partition_heal]\nnode_count = 2\nwrite_pattern = \"concentrated\"\n\
             ops_per_group = 1\n[[partition_heal.groups]]\nnodes = [0, 1]\n"
        ),
    );
    let out = run(&[file.to_str().unwrap()]);
    assert!(!out.status.success());
    let err = stderr(&out);
    assert!(
        err.contains("two-replicas") && err.contains("only one of"),
        "{err}"
    );
}

#[test]
fn a_trace_file_in_a_missing_directory_is_an_error() {
    let dir = scratch("bad-trace");
    let file = scenario(&dir, "two", TWO_REPLICAS);
    let trace = dir.join("no-such-dir").join("trace.jsonl");
    let out = run(&["--trace", trace.to_str().unwrap(), file.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("creating trace file"));
}
