//! The workload spec: how to run a trace.
//!
//! A spec file is TOML, with one `[[workload]]` table per workload. The library
//! is not a spec field, so one spec runs against every library.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Selects the part of a replay that the timer covers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Timing {
    /// The timer covers every transaction after the setup, the delivery to the
    /// checked replicas, and the final reads.
    #[default]
    All,
    /// All writers apply their transactions first. Replica 0 then merges all
    /// updates, as a reference. The timer covers only the merge on the other
    /// checked replicas, and the final reads.
    MergeOnly,
}

/// One workload in a spec file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    /// The name in the result rows and in `--only`.
    pub name: String,
    /// What the workload does, for a person reading the file.
    #[serde(default)]
    pub description: String,
    /// The trace file, relative to the data directory.
    pub trace: PathBuf,
    /// The expected SHA-256 hash of the trace file.
    pub sha256: String,
    /// How many times one agent replays the trace on one document.
    #[serde(default = "one")]
    pub repeat: usize,
    /// Extra replicas that only receive updates.
    #[serde(default)]
    pub receivers: usize,
    /// How many leading transactions run before the timer starts. Every replica
    /// receives them before the timer starts.
    #[serde(default)]
    pub setup_txns: usize,
    /// The part of the replay that the timer covers.
    #[serde(default)]
    pub timing: Timing,
    /// Delivers all updates to a replica in one adapter call.
    #[serde(default)]
    pub merge_batch: bool,
    /// How many replicas to read and check, starting at replica 0. Omit this to
    /// check all replicas.
    pub check_replicas: Option<usize>,
    /// Runs only with `--include-heavy`, because it can take many minutes.
    #[serde(default)]
    pub heavy: bool,
}

/// The default of [`Workload::repeat`].
fn one() -> usize {
    1
}

/// The top level of a spec file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecFile {
    /// The `[[workload]]` tables.
    workload: Vec<Workload>,
}

/// Reads the workloads in the spec file at `path`.
pub fn load(path: &Path) -> Result<Vec<Workload>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let spec: SpecFile =
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(spec.workload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_workload_has_useful_defaults() {
        let spec: SpecFile = toml::from_str(
            r#"
            [[workload]]
            name = "b4"
            trace = "b4.json"
            sha256 = "00"
            "#,
        )
        .unwrap();
        let w = &spec.workload[0];
        assert_eq!(w.repeat, 1);
        assert_eq!(w.timing, Timing::All);
        assert_eq!(w.check_replicas, None);
        assert!(!w.heavy);
    }

    #[test]
    fn an_unknown_field_is_an_error() {
        let bad = r#"
            [[workload]]
            name = "b4"
            trace = "b4.json"
            sha256 = "00"
            recievers = 1
            "#;
        assert!(toml::from_str::<SpecFile>(bad).is_err());
    }
}
