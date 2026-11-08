//! Tests of the `replay` binary as a user runs it: workload selection, the
//! rows that it writes, and its exit status when a check fails.
//!
//! The tests use the smoke fixture of `workloads/fixtures`, and small traces
//! that each test writes. They need no generated trace files.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

/// The path of the binary that Cargo built for these tests.
const REPLAY: &str = env!("CARGO_BIN_EXE_replay");

/// The SHA-256 of `workloads/fixtures/smoke-text.trace.json`, as the smoke spec
/// pins it.
const SMOKE_SHA: &str = "d49ad91a6c882150cf0d7b020beaf3228b72bb72547da7e299fa355bd028a81e";

/// Two agents insert at position 0 of a shared text, each without the other's
/// insert. With ascending ids, Yrs and Loro give `AABBbase`, and Automerge
/// gives `BBAAbase`.
const FORK: &str = r#"{"kind":"concurrent","numAgents":2,"endContent":"AABBbase","txns":[
    {"agent":0,"parents":[],"patches":[[0,0,"base"]]},
    {"agent":0,"parents":[0],"patches":[[0,0,"AA"]]},
    {"agent":1,"parents":[0],"patches":[[0,0,"BB"]]}]}"#;

/// Returns the `workloads/fixtures` directory of the repository.
fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workloads/fixtures")
}

/// Returns an empty directory for one test, under Cargo's temp directory for
/// integration tests.
fn scratch(test: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("replay-cli")
        .join(test);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes a spec in `dir` with one workload per `(name, heavy)` pair, each on
/// the smoke fixture. Returns its path.
fn smoke_spec(dir: &Path, workloads: &[(&str, bool)]) -> PathBuf {
    let body: String = workloads
        .iter()
        .map(|(name, heavy)| {
            format!(
                "[[workload]]\nname = \"{name}\"\ndescription = \"about {name}\"\n\
                 trace = \"smoke-text.trace.json\"\nsha256 = \"{SMOKE_SHA}\"\nheavy = {heavy}\n\n"
            )
        })
        .collect();
    let path = dir.join("spec.toml");
    std::fs::write(&path, body).unwrap();
    path
}

/// Runs the replay with `args`.
fn run(args: &[&str]) -> Output {
    Command::new(REPLAY).args(args).output().unwrap()
}

/// Returns the result rows that `out` printed, as JSON values.
fn rows(out: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect()
}

/// Returns the rows of one `row_type`.
fn of_type<'a>(rows: &'a [Value], row_type: &str) -> Vec<&'a Value> {
    rows.iter().filter(|r| r["row_type"] == row_type).collect()
}

#[test]
fn list_names_every_workload_and_marks_the_heavy_ones() {
    let dir = scratch("list");
    let spec = smoke_spec(&dir, &[("g.light", false), ("g.big", true)]);
    let out = run(&["--spec", spec.to_str().unwrap(), "--list"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        text.lines().collect::<Vec<_>>(),
        ["g.light: about g.light", "g.big (heavy): about g.big"]
    );
}

/// `--only g.` selects a group by prefix. A heavy workload runs only with
/// `--include-heavy`. Each row of the run names its workload.
#[test]
fn only_selects_a_group_and_heavy_needs_a_flag() {
    let dir = scratch("only");
    let spec = smoke_spec(&dir, &[("g.a", false), ("g.b", true), ("h.c", false)]);
    let fixtures = fixtures();
    let base = [
        "--spec",
        spec.to_str().unwrap(),
        "--data-dir",
        fixtures.to_str().unwrap(),
        "--crdt",
        "loro",
        "--trials",
        "1",
        "--no-memory",
        "--only",
        "g.",
    ];
    let names = |extra: &[&str]| -> Vec<String> {
        let out = run(&[&base[..], extra].concat());
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        of_type(&rows(&out), "workload")
            .iter()
            .map(|r| r["workload"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names(&[]), ["g.a"]);
    assert_eq!(names(&["--include-heavy"]), ["g.a", "g.b"]);
}

#[test]
fn a_selection_that_matches_nothing_is_an_error() {
    let dir = scratch("no-match");
    let spec = smoke_spec(&dir, &[("g.a", false)]);
    let out = run(&["--spec", spec.to_str().unwrap(), "--only", "x."]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no workload matches"));
}

#[test]
fn a_trace_with_the_wrong_hash_is_an_error() {
    let dir = scratch("hash");
    let spec = smoke_spec(&dir, &[("g.a", false)]);
    let text = std::fs::read_to_string(&spec)
        .unwrap()
        .replace(SMOKE_SHA, &"0".repeat(64));
    std::fs::write(&spec, text).unwrap();
    let fixtures = fixtures();
    let out = run(&[
        "--spec",
        spec.to_str().unwrap(),
        "--data-dir",
        fixtures.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("g.a"));
}

/// A trial that fails its check gives a `failed` row, and the binary exits
/// with an error after it has run the other libraries.
#[test]
fn a_failed_check_is_a_row_and_an_error_exit() {
    let dir = scratch("fork");
    let trace = dir.join("fork.trace.json");
    std::fs::write(&trace, FORK).unwrap();
    let out = run(&[
        "--trace",
        trace.to_str().unwrap(),
        "--id-order",
        "ascending",
        "--trials",
        "1",
        "--no-memory",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("at least one trial failed"));
    let rows = rows(&out);
    let failed: Vec<_> = of_type(&rows, "failed")
        .iter()
        .map(|r| r["crdt"].clone())
        .collect();
    let passed: Vec<_> = of_type(&rows, "trial")
        .iter()
        .map(|r| r["crdt"].clone())
        .collect();
    assert_eq!(failed, ["automerge"]);
    assert_eq!(passed, ["yrs", "loro"]);
    assert_eq!(of_type(&rows, "workload")[0]["id_order"], "ascending");
}

/// The Yrs adapter indexes text by bytes, so it skips a trace that is not
/// ASCII. The other libraries run it, and the run is clean.
#[test]
fn yrs_skips_a_trace_that_is_not_ascii() {
    let dir = scratch("non-ascii");
    let trace = dir.join("accent.trace.json");
    std::fs::write(
        &trace,
        r#"{"endContent":"héllo","txns":[{"patches":[[0,0,"héllo"]]}]}"#,
    )
    .unwrap();
    let out = run(&[
        "--trace",
        trace.to_str().unwrap(),
        "--trials",
        "1",
        "--no-memory",
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let rows = rows(&out);
    let skipped = of_type(&rows, "skipped");
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0]["crdt"], "yrs");
    assert!(
        skipped[0]["reason"]
            .as_str()
            .unwrap()
            .contains("ascii_only")
    );
    assert_eq!(of_type(&rows, "trial").len(), 2);
}

/// Without `--no-memory`, each library gets one more trial that reports
/// memory. `--out` creates its folder.
#[test]
fn an_in_process_run_adds_a_memory_row_and_out_creates_its_folder() {
    let dir = scratch("memory");
    let out_file = dir.join("new").join("rows.jsonl");
    let fixtures = fixtures();
    let spec = fixtures.join("../smoke.toml");
    let out = run(&[
        "--spec",
        spec.to_str().unwrap(),
        "--data-dir",
        fixtures.to_str().unwrap(),
        "--only",
        "smoke.single",
        "--crdt",
        "automerge",
        "--trials",
        "2",
        "--out",
        out_file.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stdout.is_empty());
    let text = std::fs::read_to_string(&out_file).unwrap();
    let rows: Vec<Value> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows[0]["row_type"], "run");
    assert_eq!(of_type(&rows, "trial").len(), 2);
    let memory = of_type(&rows, "memory");
    assert_eq!(memory.len(), 1);
    assert!(memory[0]["peak_bytes"].as_u64().unwrap() > 0);
}

#[test]
fn replicas_need_exactly_one_library() {
    let out = run(&["--replicas", "localhost:1", "--crdt", "yrs,loro"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("exactly one --crdt"));
}
