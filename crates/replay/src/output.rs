//! Writes result rows as JSON Lines.
//!
//! The first row has `row_type = "run"`. It records the commit, the host, the
//! build profile, and the library versions, so a results file describes itself.
//! The other rows have `row_type` `trial`, `memory`, `skipped`, or `failed`.

use std::process::Command;

use serde_json::{Value, json};

use crate::driver::TrialResult;

/// The lock file of this build. It gives the exact library versions.
const CARGO_LOCK: &str = include_str!("../../../Cargo.lock");

/// Returns the locked version of `package`, if the lock file has it.
fn locked_version(package: &str) -> Option<String> {
    let marker = format!("name = \"{package}\"\nversion = \"");
    let start = CARGO_LOCK.find(&marker)? + marker.len();
    let end = CARGO_LOCK[start..].find('"')?;
    Some(CARGO_LOCK[start..start + end].to_owned())
}

/// Returns the trimmed output of a command, or `None` if the command fails.
fn capture(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Returns the `run` row.
pub fn run_row(args: Value) -> Value {
    json!({
        "row_type": "run",
        "git_commit": capture("git", &["rev-parse", "HEAD"]),
        "git_dirty": capture("git", &["status", "--porcelain"]).map(|s| !s.is_empty()),
        "host": capture("hostname", &[]),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cpus": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "versions": {
            "automerge": locked_version("automerge"),
            "yrs": locked_version("yrs"),
            "loro": locked_version("loro"),
        },
        "args": args,
    })
}

/// Returns a `trial` row.
pub fn trial_row(workload: &str, crdt: &str, lane: &str, trial: usize, r: &TrialResult) -> Value {
    json!({
        "row_type": "trial",
        "workload": workload,
        "crdt": crdt,
        "lane": lane,
        "trial": trial,
        "time_ms": r.time_ms,
        "read_ms": r.read_ms,
        "encode_ms": r.encode_ms,
        "doc_size": r.doc_size,
        "parse_ms": r.parse_ms,
        "update_bytes": r.update_bytes,
        "update_count": r.update_count,
        "adapter_ms": r.adapter_ms,
    })
}

/// Returns a `memory` row.
pub fn memory_row(workload: &str, crdt: &str, lane: &str, r: &TrialResult) -> Value {
    json!({
        "row_type": "memory",
        "workload": workload,
        "crdt": crdt,
        "lane": lane,
        "peak_bytes": r.peak_bytes,
        "retained_bytes": r.retained_bytes,
    })
}

/// Returns a `skipped` row, for a workload that this lane cannot run.
pub fn skipped_row(workload: &str, crdt: &str, lane: &str, reason: &str) -> Value {
    json!({
        "row_type": "skipped",
        "workload": workload,
        "crdt": crdt,
        "lane": lane,
        "reason": reason,
    })
}

/// Returns a `failed` row, for a trial that failed a check or an operation.
pub fn failed_row(workload: &str, crdt: &str, lane: &str, trial: usize, error: &str) -> Value {
    json!({
        "row_type": "failed",
        "workload": workload,
        "crdt": crdt,
        "lane": lane,
        "trial": trial,
        "error": error,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_run_row_records_each_library_version() {
        let row = run_row(json!({}));
        for lib in ["automerge", "yrs", "loro"] {
            assert!(row["versions"][lib].is_string(), "{lib}");
        }
    }

    #[test]
    fn a_trial_row_keeps_the_adapter_time_empty_in_process() {
        let row = trial_row("b4", "yrs", "in_process", 1, &TrialResult::default());
        assert!(row["adapter_ms"].is_null());
        assert_eq!(row["trial"], 1);
    }
}
