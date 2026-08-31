//! Writes the provenance file that accompanies a benchmark CSV.
//!
//! A result CSV holds measurements and nothing else. Read one months later and
//! it cannot say which commit produced it, on which host, in which build
//! profile, or from which seeds. `results/` is gitignored, so there is no commit
//! history to recover that from either. This module records it as JSON beside
//! the CSV.
//!
//! The file describes the run's *configuration*, not its outcome. It is written
//! before the first trial, so a sweep that dies halfway still leaves a record of
//! what it meant to run. Read it together with the CSV to see how far the run
//! actually got.
//!
//! The pinned toolchain is not recorded separately. `rust-toolchain.toml` is
//! tracked, so the commit already determines it.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::contention::{self, AchievedContention};
use crate::partition_heal::{DIVERGENCE_SEED_BASE, seed_for};
use crate::runner::ReplicaEndpoint;
use crate::topology::{PartitionConfig, ScenarioBody, ScenarioFile, Workload};

/// Version of the emitted JSON shape. Raise it whenever the shape changes in a
/// way an existing reader would misread.
///
/// Version 2 added `run.crdt`. The change is additive, so a reader that ignores
/// unknown fields handles both versions. The bump lets a reader that *needs* the
/// library name tell a missing field apart from an unknown one: below version 2
/// its absence means Automerge.
const SCHEMA_VERSION: u32 = 2;

/// The parts of a run that the result CSV does not record.
pub struct RunMeta<'a> {
    /// Scenarios in execution order.
    pub scenarios: &'a [ScenarioFile],
    /// Source paths, positionally matching `scenarios`. Empty when the
    /// built-in regression scenarios are running (they have no files).
    pub paths: &'a [PathBuf],
    /// Trials per scenario. This is also the number of PRNG repetitions per
    /// cell.
    pub trials: usize,
    /// The external replicas dialled, or empty for the in-process lane.
    ///
    /// Record which lane ran. On Linux, in-process convergence timings are
    /// distorted by TCP delayed-ACK stalls, so only the external lane can be
    /// used for convergence analysis.
    pub replicas: &'a [ReplicaEndpoint],
    /// The CRDT library, when the orchestrator picked it. That means the
    /// in-process lane.
    ///
    /// `None` for an external run, where whoever launched the stack chose. The
    /// bench scripts then fill the same field in, so `run.crdt` is the single
    /// place to read the library from in either lane.
    pub crdt: Option<&'a str>,
}

/// Writes the provenance file for `meta` to `path`, creating parent
/// directories as needed.
pub fn write(path: &Path, meta: &RunMeta<'_>) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating provenance directory {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(&document(meta))
        .expect("serde_json::Value serialization is infallible");
    std::fs::write(path, format!("{body}\n"))
        .with_context(|| format!("writing provenance file {}", path.display()))
}

/// Builds the provenance document.
fn document(meta: &RunMeta<'_>) -> Value {
    let scenarios: Vec<Value> = meta
        .scenarios
        .iter()
        .enumerate()
        .map(|(i, s)| scenario_entry(s, meta.paths.get(i), meta.trials))
        .collect();

    json!({
        "schema_version": SCHEMA_VERSION,
        "generated_at_unix_ms": unix_millis(),
        "git": git_provenance(),
        "host": host_provenance(),
        "build": {
            // The orchestrator always runs through `cargo run`, so the binary
            // matches the working tree that `git` reported above.
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "orchestrator_version": env!("CARGO_PKG_VERSION"),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "run": {
            "trials": meta.trials,
            "node_source": if meta.replicas.is_empty() { "in_process" } else { "external" },
            "crdt": meta.crdt,
            "replicas": meta.replicas.iter()
                .map(|r| json!({"client_addr": r.client_addr, "peer_addr": r.peer_addr}))
                .collect::<Vec<_>>(),
        },
        "scenarios": scenarios,
    })
}

/// Builds one scenario's entry: its parameters, and its seeds when the
/// divergence generator drives it.
fn scenario_entry(scenario: &ScenarioFile, path: Option<&PathBuf>, trials: usize) -> Value {
    let mut entry = json!({
        "name": scenario.name,
        "path": path.map(|p| p.display().to_string()),
        "node_count": scenario.node_count(),
        "op_count": scenario.op_count(),
    });
    let obj = entry.as_object_mut().expect("json! built an object");

    match &scenario.body {
        ScenarioBody::Topology(c) => {
            obj.insert("kind".into(), "topology".into());
            obj.insert(
                "params".into(),
                json!({
                    "connections": c.connections.kind(),
                    "write_pattern": c.write_pattern,
                    "workload": c.workload,
                    "op_count": c.op_count,
                    "op_interval_ms": c.op_interval_ms,
                }),
            );
        }
        ScenarioBody::PartitionHeal(c) => {
            obj.insert("kind".into(), "partition_heal".into());
            obj.insert(
                "params".into(),
                json!({
                    "groups": c.groups.iter().map(|g| g.nodes.clone()).collect::<Vec<_>>(),
                    "ops_per_group": c.ops_per_group,
                    "write_pattern": c.write_pattern,
                    "workload": c.workload,
                    "locality": c.locality,
                    "heal_topology": c.heal_topology,
                }),
            );
            // Seeds only mean something for the text workload — `MapPut` keys
            // are derived from the op index, not from the PRNG.
            if c.workload == Workload::TextSplice {
                let repetitions: Vec<Value> = (1..=trials)
                    .map(|rep| {
                        let by_node: Vec<Value> = c
                            .groups
                            .iter()
                            .flat_map(|g| &g.nodes)
                            .map(|&node| {
                                json!({
                                    "node": node,
                                    "seed": format!("{:#018x}", seed_for(c, node, rep)),
                                })
                            })
                            .collect();
                        json!({"repetition": rep, "by_node": by_node})
                    })
                    .collect();
                obj.insert(
                    "seeds".into(),
                    json!({
                        "base": format!("{DIVERGENCE_SEED_BASE:#018x}"),
                        // A node draws from its stream only if the configured
                        // write_pattern sends it writes; seeds are listed for
                        // every node in the cell regardless.
                        "per_repetition": repetitions,
                    }),
                );
                if let Some(c) = achieved_contention(c, trials) {
                    obj.insert("achieved_contention".into(), c);
                }
            }
        }
    }
    entry
}

/// Summarises the cell's achieved anchor contention over its repetitions.
///
/// The `Locality` axis claims an ordering of contention. This records what the
/// generated stream actually produces, so a cell's merge time can be read
/// against real contention rather than intended contention.
///
/// `None` when the metric does not apply to the cell. See
/// [`crate::contention`].
fn achieved_contention(config: &PartitionConfig, trials: usize) -> Option<Value> {
    let per_rep: Vec<AchievedContention> = (1..=trials)
        .filter_map(|rep| contention::simulate(config, rep))
        .collect();
    let (first, rest) = per_rep.split_first()?;

    let siblings: Vec<usize> = per_rep.iter().map(|c| c.max_concurrent_siblings).collect();
    let total: usize = siblings.iter().sum();
    // Repetitions differ only in their PRNG stream. A contested-anchor count
    // that varied between them would mean the metric is not a property of the
    // cell.
    debug_assert!(
        rest.iter()
            .all(|c| c.contested_anchors == first.contested_anchors),
        "contested_anchors varies across repetitions"
    );

    Some(json!({
        // Name the model, so a later adapter can state whether this anchor
        // model still describes it.
        "model": "rga_anchor_simulation",
        "contested_anchors": first.contested_anchors,
        "max_concurrent_siblings": {
            "min": siblings.iter().min(),
            "max": siblings.iter().max(),
            "mean": total as f64 / siblings.len() as f64,
        },
        "head_children_repetition_1": first.head_children,
    }))
}

/// Returns milliseconds since the Unix epoch, or 0 if the clock reads earlier
/// than that.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Returns the commit and a dirty flag for the working tree.
///
/// Calls `git` rather than `jj`. The repository is jj-colocated, so `git` sees
/// the same history, and `git` is the one present on every measurement host.
///
/// Both fields are `null` if `git` is missing or the run happens outside a
/// repository. A provenance file with no git identity is still better than
/// none, so this never fails the run.
fn git_provenance() -> Value {
    let commit = capture(&["rev-parse", "HEAD"]);
    // `--porcelain` prints one line per modified path, so empty output means a
    // clean tree.
    let dirty = capture(&["status", "--porcelain"]).map(|s| !s.is_empty());
    json!({
        "commit": commit,
        // True when the tree held uncommitted changes. The run is then not
        // reproducible from `commit` alone.
        "dirty": dirty,
    })
}

/// Runs `git` with `args` and returns its trimmed stdout, or `None` if the
/// command fails for any reason.
fn capture(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Returns the hostname and CPU count, which identify the machine a timing was
/// taken on.
fn host_provenance() -> Value {
    // On Linux, the measurement host, /proc is authoritative. HOSTNAME is a
    // fallback and not every shell exports it.
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_owned())
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
        .filter(|s| !s.is_empty());
    json!({
        "hostname": hostname,
        "cpus": std::thread::available_parallelism().map(usize::from).ok(),
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Group, HealTopology, Locality, PartitionConfig, WritePattern};

    fn text_cell(ops: usize, locality: Locality) -> ScenarioFile {
        ScenarioFile {
            name: "divergence-n2-test".to_owned(),
            body: ScenarioBody::PartitionHeal(PartitionConfig {
                node_count: 2,
                groups: vec![Group { nodes: vec![0] }, Group { nodes: vec![1] }],
                ops_per_group: ops,
                write_pattern: WritePattern::Concentrated,
                workload: Workload::TextSplice,
                locality,
                heal_topology: HealTopology::FullMesh,
            }),
        }
    }

    fn meta_for(scenarios: &[ScenarioFile], trials: usize) -> RunMeta<'_> {
        RunMeta {
            scenarios,
            paths: &[],
            trials,
            replicas: &[],
            crdt: Some("automerge"),
        }
    }

    #[test]
    fn records_cell_params_and_schema_version() {
        let scenarios = [text_cell(1000, Locality::SameRegion)];
        let doc = document(&meta_for(&scenarios, 3));
        assert_eq!(doc["schema_version"], SCHEMA_VERSION);
        let cell = &doc["scenarios"][0];
        assert_eq!(cell["kind"], "partition_heal");
        assert_eq!(cell["op_count"], 2000); // two singleton groups
        assert_eq!(cell["params"]["ops_per_group"], 1000);
        assert_eq!(cell["params"]["locality"], "same_region");
        assert_eq!(cell["params"]["workload"], "text_splice");
        assert_eq!(cell["params"]["write_pattern"], "concentrated");
        assert_eq!(cell["params"]["groups"], json!([[0], [1]]));
    }

    /// Every `(repetition, node)` seed the runner will use must appear in the
    /// file. That is what makes the sweep reproducible.
    #[test]
    fn records_one_seed_per_node_per_repetition() {
        let scenarios = [text_cell(100, Locality::Append)];
        let doc = document(&meta_for(&scenarios, 8));
        let seeds = &doc["scenarios"][0]["seeds"];
        let reps = seeds["per_repetition"].as_array().unwrap();
        assert_eq!(reps.len(), 8);
        assert_eq!(reps[0]["repetition"], 1);
        assert_eq!(reps[0]["by_node"].as_array().unwrap().len(), 2);
        assert_eq!(seeds["base"], format!("{DIVERGENCE_SEED_BASE:#018x}"));
    }

    /// A recorded seed must equal the one the runner uses. If it does not, the
    /// file documents a replay that would not reproduce the run.
    #[test]
    fn recorded_seed_matches_runner_seed_fn() {
        let scenarios = [text_cell(100, Locality::RandomPosition)];
        let doc = document(&meta_for(&scenarios, 2));
        let ScenarioBody::PartitionHeal(cfg) = &scenarios[0].body else {
            unreachable!("built as partition_heal");
        };
        let recorded = &doc["scenarios"][0]["seeds"]["per_repetition"][1]["by_node"][0];
        assert_eq!(recorded["node"], 0);
        assert_eq!(recorded["seed"], format!("{:#018x}", seed_for(cfg, 0, 2)));
    }

    /// A map-put scenario never draws from the PRNG, so recording seeds for one
    /// would be misleading.
    #[test]
    fn omits_seeds_for_map_put_scenarios() {
        let scenarios = crate::topology::builtin_scenarios();
        let doc = document(&meta_for(&scenarios, 2));
        for cell in doc["scenarios"].as_array().unwrap() {
            assert!(cell["seeds"].is_null(), "unexpected seeds in {cell}");
        }
    }

    #[test]
    fn distinguishes_in_process_from_external_lane() {
        let scenarios = [text_cell(100, Locality::Append)];
        assert_eq!(
            document(&meta_for(&scenarios, 1))["run"]["node_source"],
            "in_process"
        );

        let replicas = [ReplicaEndpoint {
            client_addr: "localhost:50051".to_owned(),
            peer_addr: "replica-0:50051".to_owned(),
        }];
        let doc = document(&RunMeta {
            scenarios: &scenarios,
            paths: &[],
            trials: 1,
            replicas: &replicas,
            // The orchestrator cannot know an external stack's library; the
            // bench scripts patch `run.crdt` in after the fact.
            crdt: None,
        });
        assert_eq!(doc["run"]["node_source"], "external");
        assert_eq!(doc["run"]["replicas"][0]["peer_addr"], "replica-0:50051");
        assert!(
            doc["run"]["crdt"].is_null(),
            "external runs must leave run.crdt for the bench script to fill"
        );
    }

    /// The in-process lane knows its own library and records it, so its
    /// `--dry-run` provenance file needs nothing added later.
    #[test]
    fn in_process_run_records_its_crdt() {
        let scenarios = vec![text_cell(4, Locality::Append)];
        let doc = document(&meta_for(&scenarios, 1));
        assert_eq!(doc["run"]["node_source"], "in_process");
        assert_eq!(doc["run"]["crdt"], "automerge");
    }

    #[test]
    fn writes_file_and_creates_parent_dir() {
        let dir = std::env::temp_dir().join(format!("replicant-provenance-{}", std::process::id()));
        let path = dir.join("nested").join("meta.json");
        let scenarios = [text_cell(100, Locality::Append)];
        write(&path, &meta_for(&scenarios, 1)).unwrap();

        let parsed: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed["scenarios"][0]["name"], "divergence-n2-test");
        std::fs::remove_dir_all(&dir).ok();
    }
}
