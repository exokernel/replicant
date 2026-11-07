//! The result rows, written as JSON Lines.
//!
//! The first row has `row_type = "run"`. It records the commit, the host, the
//! build profile, and the library versions, so a results file describes itself.

use common::provenance;
use serde::Serialize;
use serde_json::{Value, json};

use crate::driver::{MemoryResult, TrialResult};
use crate::trace::GraphStats;

/// The lock file of this build. It gives the exact library versions.
const CARGO_LOCK: &str = include_str!("../../../Cargo.lock");

/// Names what a row measured.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RowKey<'a> {
    /// The workload name.
    pub workload: &'a str,
    /// The library name.
    pub crdt: &'a str,
    /// The lane name.
    pub lane: &'a str,
}

/// One result row after the `run` row.
#[derive(Debug, Serialize)]
#[serde(tag = "row_type", rename_all = "snake_case")]
pub enum Row<'a> {
    /// The shape of one workload's causal graph. It shows the concurrency.
    Workload {
        /// The workload name.
        workload: &'a str,
        /// The number of agents.
        agents: usize,
        /// The graph.
        #[serde(flatten)]
        stats: GraphStats,
    },
    /// One timed trial.
    Trial {
        /// What the row measured.
        #[serde(flatten)]
        key: RowKey<'a>,
        /// The trial number, from 1.
        trial: usize,
        /// The measurements.
        #[serde(flatten)]
        result: TrialResult,
    },
    /// The memory trial.
    Memory {
        /// What the row measured.
        #[serde(flatten)]
        key: RowKey<'a>,
        /// The measurements.
        #[serde(flatten)]
        result: MemoryResult,
    },
    /// A workload that this lane cannot run.
    Skipped {
        /// What the row measured.
        #[serde(flatten)]
        key: RowKey<'a>,
        /// Why the lane cannot run it.
        reason: &'a str,
    },
    /// A trial that failed a check or an operation.
    Failed {
        /// What the row measured.
        #[serde(flatten)]
        key: RowKey<'a>,
        /// The trial number, or `None` for the memory trial.
        trial: Option<usize>,
        /// The failure.
        error: &'a str,
    },
}

/// Returns the locked version of `package`, if the lock file has it.
fn locked_version(package: &str) -> Option<&'static str> {
    let marker = format!("name = \"{package}\"\nversion = \"");
    let rest = &CARGO_LOCK[CARGO_LOCK.find(&marker)? + marker.len()..];
    rest.split('"').next()
}

/// Returns the `run` row.
#[must_use]
pub fn run_row(args: &Value) -> Value {
    json!({
        "row_type": "run",
        "git_commit": provenance::git_commit(),
        "git_dirty": provenance::git_dirty(),
        "host": provenance::hostname(),
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "cpus": provenance::cpus(),
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "versions": {
            "automerge": locked_version("automerge"),
            "yrs": locked_version("yrs"),
            "loro": locked_version("loro"),
        },
        "args": args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: RowKey = RowKey {
        workload: "b4",
        crdt: "yrs",
        lane: "in_process",
    };

    #[test]
    fn the_run_row_records_each_library_version() {
        let row = run_row(&json!({}));
        for lib in ["automerge", "yrs", "loro"] {
            assert!(row["versions"][lib].is_string(), "{lib}");
        }
    }

    #[test]
    fn a_trial_row_is_flat_and_has_its_type() {
        let row = serde_json::to_value(Row::Trial {
            key: KEY,
            trial: 1,
            result: TrialResult::default(),
        })
        .unwrap();
        assert_eq!(row["row_type"], "trial");
        assert_eq!(row["workload"], "b4");
        assert_eq!(row["trial"], 1);
        assert!(row["adapter_ms"].is_null());
        assert_eq!(row["doc_size"], 0);
    }

    #[test]
    fn the_other_rows_have_their_types() {
        let memory = Row::Memory {
            key: KEY,
            result: MemoryResult::default(),
        };
        let skipped = Row::Skipped {
            key: KEY,
            reason: "r",
        };
        let failed = Row::Failed {
            key: KEY,
            trial: None,
            error: "e",
        };
        for (row, row_type) in [(memory, "memory"), (skipped, "skipped"), (failed, "failed")] {
            assert_eq!(serde_json::to_value(row).unwrap()["row_type"], row_type);
        }
    }
}
