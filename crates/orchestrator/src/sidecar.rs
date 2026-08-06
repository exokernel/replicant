//! Results sidecar — run provenance written alongside a benchmark CSV.
//!
//! The CSV carries measurements and nothing else: reading one months later
//! cannot tell you which commit produced it, on which host, in which build
//! profile, or from which PRNG seeds. `results/` is gitignored, so there is no
//! commit history to fall back on either. This module writes that context to a
//! JSON file next to the CSV at the moment the run starts.
//!
//! Scope: the sidecar describes the run's *configuration*, not its outcome. It
//! is written before the first trial, so a sweep that dies halfway still leaves
//! a record of what was attempted — pair it with the CSV to see how far the run
//! actually got.
//!
//! The pinned toolchain is not recorded separately: `rust-toolchain.toml` is
//! tracked, so `git.commit` already determines it.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::contention::{self, AchievedContention};
use crate::runner::{DIVERGENCE_SEED_BASE, ReplicaEndpoint, seed_for};
use crate::topology::{PartitionConfig, ScenarioBody, ScenarioFile, Workload};

/// Bumped whenever the emitted shape changes incompatibly, so downstream
/// analysis can branch instead of silently misreading an older file.
const SCHEMA_VERSION: u32 = 1;

/// Everything about a run that the result CSV does not record.
pub struct RunMeta<'a> {
    /// Scenarios in execution order.
    pub scenarios: &'a [ScenarioFile],
    /// Source paths, positionally matching `scenarios`. Empty when the
    /// built-in regression scenarios are running (they have no files).
    pub paths: &'a [PathBuf],
    /// Trials per scenario — also the number of PRNG repetitions per cell.
    pub trials: usize,
    /// External replicas dialled, or empty for the in-process lane. Which lane
    /// a run used is load-bearing here: in-process convergence timings on
    /// Linux are polluted by TCP delayed-ACK stalls, so only the external
    /// (docker/k8s) lane is trustworthy for convergence analysis.
    pub replicas: &'a [ReplicaEndpoint],
}

/// Write the sidecar for `meta` to `path`, creating parent directories.
pub fn write(path: &Path, meta: &RunMeta<'_>) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating sidecar directory {}", parent.display()))?;
    }
    let body = serde_json::to_string_pretty(&document(meta))
        .expect("serde_json::Value serialization is infallible");
    std::fs::write(path, format!("{body}\n"))
        .with_context(|| format!("writing sidecar {}", path.display()))
}

/// Build the sidecar document.
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
            // The orchestrator is always run through `cargo run`, so the
            // binary matches the working tree `git` reported above.
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "orchestrator_version": env!("CARGO_PKG_VERSION"),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "run": {
            "trials": meta.trials,
            "node_source": if meta.replicas.is_empty() { "in_process" } else { "external" },
            "replicas": meta.replicas.iter()
                .map(|r| json!({"client_addr": r.client_addr, "peer_addr": r.peer_addr}))
                .collect::<Vec<_>>(),
        },
        "scenarios": scenarios,
    })
}

/// One scenario's parameters, plus its seeds when the divergence generator
/// drives it.
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

/// Summarise the cell's achieved anchor contention across its repetitions.
///
/// The `Locality` axis *claims* a contention ordering; this records what the
/// generated op stream actually produces, so a cell's measured merge time can
/// be read against real contention rather than intended contention. `None` when
/// the metric is undefined for the cell (see [`crate::contention`]).
fn achieved_contention(config: &PartitionConfig, trials: usize) -> Option<Value> {
    let per_rep: Vec<AchievedContention> = (1..=trials)
        .filter_map(|rep| contention::simulate(config, rep))
        .collect();
    let (first, rest) = per_rep.split_first()?;

    let siblings: Vec<usize> = per_rep.iter().map(|c| c.max_concurrent_siblings).collect();
    let total: usize = siblings.iter().sum();
    // Repetitions differ only through the PRNG, so a varying contested-anchor
    // count would mean the metric is not a stable property of the cell.
    debug_assert!(
        rest.iter()
            .all(|c| c.contested_anchors == first.contested_anchors),
        "contested_anchors varies across repetitions"
    );

    Some(json!({
        // Names the simulation so a future adapter (Yrs/YATA, Loro/Fugue) can
        // say whether this anchor model still applies to it.
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

/// Milliseconds since the Unix epoch; 0 if the clock predates it.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Commit and dirty-flag for the working tree, best-effort.
///
/// Shells out to `git` rather than `jj` because the repo is jj-colocated (so
/// `git` sees the same history) and `git` is the one that is present on any
/// measurement host. Both fields are `null` if the command is unavailable or
/// the run happens outside a repository — a sidecar without provenance is
/// still worth more than no sidecar.
fn git_provenance() -> Value {
    let commit = capture(&["rev-parse", "HEAD"]);
    // `--porcelain` prints one line per modified path; empty output == clean.
    let dirty = capture(&["status", "--porcelain"]).map(|s| !s.is_empty());
    json!({
        "commit": commit,
        // True when the tree had uncommitted changes — the run is then not
        // reproducible from `commit` alone.
        "dirty": dirty,
    })
}

/// Run `git` with `args` and return trimmed stdout, or `None` on any failure.
fn capture(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// Hostname and CPU count — the "which machine" half of a timing result.
fn host_provenance() -> Value {
    // /proc is authoritative on Linux (the measurement host); HOSTNAME is a
    // best-effort fallback and is not exported by every shell.
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

    /// The sweep's reproducibility claim: every (repetition, node) seed that
    /// the runner will use is written down.
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

    /// A recorded seed must be the one the runner actually seeds with —
    /// otherwise the sidecar documents a replay that does not reproduce.
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

    /// Map-put scenarios do not consume the PRNG, so claiming seeds for them
    /// would be noise.
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
        });
        assert_eq!(doc["run"]["node_source"], "external");
        assert_eq!(doc["run"]["replicas"][0]["peer_addr"], "replica-0:50051");
    }

    #[test]
    fn writes_file_and_creates_parent_dir() {
        let dir = std::env::temp_dir().join(format!("replicant-sidecar-{}", std::process::id()));
        let path = dir.join("nested").join("meta.json");
        let scenarios = [text_cell(100, Locality::Append)];
        write(&path, &meta_for(&scenarios, 1)).unwrap();

        let parsed: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed["scenarios"][0]["name"], "divergence-n2-test");
        std::fs::remove_dir_all(&dir).ok();
    }
}
