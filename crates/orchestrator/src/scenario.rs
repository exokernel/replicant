//! Scenario files: the shapes a scenario can take, the rules a valid one
//! follows, and the built-in regression scenarios.
//!
//! A scenario file is TOML. It holds either a `[topology]` table for a
//! steady-state run or a `[partition_heal]` table for a divergence cell. See
//! `docs/scenarios.md` for the fields, and `docs/divergence-sweep.md` for what
//! the sweep parameters mean.
//!
//! This module is data and pure functions only. It spawns nothing and talks to
//! no replica.

use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize};

use crate::topology::Connections;
use crate::workload::Locality;

/// One parsed scenario file. Exactly one of `[topology]` or `[partition_heal]`
/// must be present.
#[derive(Debug)]
pub struct ScenarioFile {
    /// Human-readable name used in log output and result reporting.
    pub name: String,
    /// Which kind of scenario this file describes.
    pub body: ScenarioBody,
}

/// The two scenario shapes a file can describe.
///
/// Parsing rejects a file that sets both tables or neither, so code that reads
/// this enum never has to re-check that rule.
#[derive(Debug)]
pub enum ScenarioBody {
    /// A steady-state run on a fixed graph, from a `[topology]` table.
    Topology(TopologyConfig),
    /// One divergence-sweep cell, from a `[partition_heal]` table.
    PartitionHeal(PartitionConfig),
}

// Hand-written Deserialize. It keeps the TOML form as a plain table:
//
//   [topology]    or    [partition_heal]
//
// serde's default for an enum field would instead require
// `body = { topology = {...} }`. This impl also rejects a file that sets both
// tables or neither, with a message naming the scenario.
impl<'de> Deserialize<'de> for ScenarioFile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            name: String,
            topology: Option<TopologyConfig>,
            partition_heal: Option<PartitionConfig>,
        }
        let raw = Raw::deserialize(deserializer)?;
        let body = match (raw.topology, raw.partition_heal) {
            (Some(t), None) => ScenarioBody::Topology(t),
            (None, Some(p)) => ScenarioBody::PartitionHeal(p),
            (Some(_), Some(_)) => {
                return Err(serde::de::Error::custom(format!(
                    "scenario '{}': only one of [topology] or [partition_heal] may be present",
                    raw.name
                )));
            }
            (None, None) => {
                return Err(serde::de::Error::custom(format!(
                    "scenario '{}': one of [topology] or [partition_heal] must be present",
                    raw.name
                )));
            }
        };
        Ok(Self {
            name: raw.name,
            body,
        })
    }
}

/// Chooses which node receives each write.
///
/// `Serialize` is derived as well as `Deserialize`, so the provenance file
/// records the parameter with the same spelling the scenario TOML uses.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePattern {
    /// All writes go to the first node in scope (node 0 or `group.nodes[0]`).
    Concentrated,
    /// Writes cycle through nodes in round-robin order.
    RoundRobin,
}

/// The kind of CRDT operation each write applies.
///
/// `TextSplice` drives the sequence-CRDT path, which is what the divergence
/// sweep measures. `MapPut` is the default, so a scenario TOML that omits the
/// field keeps its original behaviour.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    /// Each write is a `MapPut` on the root map.
    #[default]
    MapPut,
    /// Each write is a `TextSplice` on a named text object under root.
    TextSplice,
}

/// Post-heal wiring for a partition-heal scenario.
///
/// Selects how the partition is repaired in phase 2. `FullMesh` reconnects
/// every pair across groups (original behaviour); `Bridge` connects only
/// `groups[0].nodes[0]` to `groups[1].nodes[0]`, forcing all cross-partition
/// state through one edge. `Bridge` requires exactly 2 groups.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealTopology {
    /// Connect every cross-group pair on heal (post-heal graph is a full mesh).
    #[default]
    FullMesh,
    /// Connect only `groups[0].nodes[0]` to `groups[1].nodes[0]` on heal.
    Bridge,
}

/// Parameters of a steady-state topology run.
#[derive(Debug, Clone, Deserialize)]
pub struct TopologyConfig {
    /// Number of replica nodes to spawn.
    pub node_count: usize,
    /// Which node pairs to connect before writing.
    pub connections: Connections,
    /// How to distribute write ops across nodes.
    pub write_pattern: WritePattern,
    /// Which CRDT operation each write applies. Defaults to `MapPut`.
    #[serde(default)]
    pub workload: Workload,
    /// Total operations to apply.
    pub op_count: usize,
    /// Delay between one operation and the next, in milliseconds.
    ///
    /// `0`, the default, sends them as fast as possible. A positive value
    /// spreads the run out so Grafana's rate panels show a visible curve.
    ///
    /// The delay falls *inside* the measured window. A paced run therefore
    /// reports a larger `convergence_ms` than a burst run on the same topology.
    #[serde(default)]
    pub op_interval_ms: u64,
}

/// One partition group: the nodes that stay connected to each other while the
/// partition holds.
#[derive(Debug, Clone, Deserialize)]
pub struct Group {
    /// Node indices belonging to this partition group.
    pub nodes: Vec<usize>,
}

/// Parameters of a partition-heal scenario, which is one cell of the
/// divergence sweep.
///
/// Each group writes on its own while cut off from the others. The groups are
/// then reconnected, and the time to global agreement is the result. See
/// `docs/divergence-sweep.md`.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionConfig {
    /// Total node count (must equal the sum of all group sizes).
    pub node_count: usize,
    /// Disjoint sets of node indices, one per partition group.
    pub groups: Vec<Group>,
    /// Operations each group applies while the partition holds. This is the
    /// divergence-volume axis of the sweep.
    pub ops_per_group: usize,
    /// Write distribution within each group.
    pub write_pattern: WritePattern,
    /// Which CRDT operation each write applies. Defaults to `MapPut`.
    #[serde(default)]
    pub workload: Workload,
    /// Where each insert lands under the `TextSplice` workload. This is the
    /// anchor-contention axis of the sweep. Ignored by `MapPut`. Defaults to
    /// `Append`.
    #[serde(default)]
    pub locality: Locality,
    /// Which links to reopen at heal time. Defaults to `FullMesh`.
    #[serde(default)]
    pub heal_topology: HealTopology,
}

impl ScenarioFile {
    /// Number of replica nodes this scenario will spawn.
    pub fn node_count(&self) -> usize {
        match &self.body {
            ScenarioBody::Topology(t) => t.node_count,
            ScenarioBody::PartitionHeal(p) => p.node_count,
        }
    }

    /// Total operations one trial of this scenario applies across all nodes.
    ///
    /// Computed from the config, so every trial of a scenario reports the same
    /// value.
    pub fn op_count(&self) -> usize {
        match &self.body {
            ScenarioBody::Topology(t) => t.op_count,
            ScenarioBody::PartitionHeal(p) => p.total_ops(),
        }
    }

    /// Checks the scenario's config. The error names the scenario.
    ///
    /// Call this when the file is loaded, so a bad file late in a sweep fails
    /// before the first trial runs.
    pub fn validate(&self) -> Result<()> {
        match &self.body {
            ScenarioBody::Topology(c) => c.validate(),
            ScenarioBody::PartitionHeal(c) => c.validate(),
        }
        .with_context(|| format!("scenario '{}'", self.name))
    }
}

impl TopologyConfig {
    /// Checks the graph against `node_count`. See [`Connections::validate`].
    pub fn validate(&self) -> Result<()> {
        self.connections.validate(self.node_count)
    }
}

impl PartitionConfig {
    /// Checks that the groups cover every node in `0..node_count` exactly
    /// once, and that `heal_topology = "bridge"` has exactly two groups.
    ///
    /// The groups must also be non-empty. The runner reads `group.nodes[0]`,
    /// and a node outside every group would never be written to or measured.
    ///
    /// `Bridge` is defined only for a two-way partition, because it connects
    /// the first node of one group to the first node of the other.
    pub fn validate(&self) -> Result<()> {
        if self.groups.is_empty() {
            bail!("PartitionConfig: at least one group is required");
        }
        let total: usize = self.groups.iter().map(|g| g.nodes.len()).sum();
        if total != self.node_count {
            bail!(
                "PartitionConfig: node_count={} but groups cover {} nodes",
                self.node_count,
                total
            );
        }
        // With the sizes matching, in range and no repeats means each node is
        // in exactly one group.
        let mut seen = HashSet::with_capacity(self.node_count);
        for (gi, group) in self.groups.iter().enumerate() {
            if group.nodes.is_empty() {
                bail!("PartitionConfig: group {gi} is empty");
            }
            for &node in &group.nodes {
                if node >= self.node_count {
                    bail!(
                        "PartitionConfig: node {node} in group {gi} is out of range for \
                         node_count={}",
                        self.node_count
                    );
                }
                if !seen.insert(node) {
                    bail!("PartitionConfig: node {node} appears more than once (group {gi})");
                }
            }
        }
        if self.heal_topology == HealTopology::Bridge && self.groups.len() != 2 {
            bail!(
                "PartitionConfig: heal_topology=\"bridge\" requires exactly 2 groups, got {}",
                self.groups.len()
            );
        }
        Ok(())
    }

    /// Total operations across all groups in one trial.
    pub fn total_ops(&self) -> usize {
        self.groups.len() * self.ops_per_group
    }
}

#[cfg(test)]
impl PartitionConfig {
    /// A `MapPut` config with one operation per group and every other field at
    /// its default. Override fields with struct-update syntax.
    pub fn for_test(node_count: usize, groups: &[&[usize]]) -> Self {
        Self {
            node_count,
            groups: groups
                .iter()
                .map(|nodes| Group {
                    nodes: nodes.to_vec(),
                })
                .collect(),
            ops_per_group: 1,
            write_pattern: WritePattern::RoundRobin,
            workload: Workload::MapPut,
            locality: Locality::Append,
            heal_topology: HealTopology::FullMesh,
        }
    }
}

/// Sources of the built-in scenarios, compiled in from `scenarios/`. The
/// built-ins and the files of the same name therefore cannot drift apart.
const BUILTIN_SOURCES: [&str; 3] = [
    include_str!("../../../scenarios/full-mesh-n2.toml"),
    include_str!("../../../scenarios/full-mesh-n3.toml"),
    include_str!("../../../scenarios/partition-heal-n4.toml"),
];

/// Returns the scenarios that run when the orchestrator is given no TOML file.
/// They act as a small regression suite.
pub fn builtin_scenarios() -> Vec<ScenarioFile> {
    BUILTIN_SOURCES
        .iter()
        .map(|src| {
            toml::from_str(src).expect("built-in scenarios parse; see builtin_scenarios_are_valid")
        })
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(src: &str) -> ScenarioFile {
        toml::from_str(src).expect("valid scenario TOML")
    }

    fn parse_topology(src: &str) -> TopologyConfig {
        let ScenarioBody::Topology(cfg) = parse(src).body else {
            panic!("expected Topology body");
        };
        cfg
    }

    fn parse_partition(src: &str) -> PartitionConfig {
        let ScenarioBody::PartitionHeal(cfg) = parse(src).body else {
            panic!("expected PartitionHeal body");
        };
        cfg
    }

    // ── PartitionConfig::validate ──────────────────────────────────────────

    #[test]
    fn validate_ok_when_counts_match() {
        assert!(
            PartitionConfig::for_test(4, &[&[0, 1], &[2, 3]])
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_err_when_node_count_too_high() {
        let err = PartitionConfig::for_test(5, &[&[0, 1], &[2, 3]])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("node_count=5"), "{err}");
        assert!(err.to_string().contains("cover 4 nodes"), "{err}");
    }

    #[test]
    fn validate_err_when_node_count_too_low() {
        let err = PartitionConfig::for_test(2, &[&[0, 1, 2]])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("node_count=2"), "{err}");
        assert!(err.to_string().contains("cover 3 nodes"), "{err}");
    }

    #[test]
    fn validate_ok_single_group() {
        assert!(
            PartitionConfig::for_test(3, &[&[0, 1, 2]])
                .validate()
                .is_ok()
        );
    }

    /// The sizes add up, but node 0 is listed twice and node 1 never.
    #[test]
    fn validate_err_when_a_node_repeats() {
        let err = PartitionConfig::for_test(4, &[&[0, 0], &[2, 3]])
            .validate()
            .unwrap_err();
        assert!(
            err.to_string().contains("node 0 appears more than once"),
            "{err}"
        );
    }

    #[test]
    fn validate_err_when_a_node_is_out_of_range() {
        let err = PartitionConfig::for_test(4, &[&[0, 1], &[2, 4]])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("node 4"), "{err}");
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn validate_err_when_a_group_is_empty() {
        let err = PartitionConfig::for_test(2, &[&[0, 1], &[]])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("group 1 is empty"), "{err}");
    }

    #[test]
    fn validate_err_when_there_are_no_groups() {
        let err = PartitionConfig::for_test(0, &[]).validate().unwrap_err();
        assert!(err.to_string().contains("at least one group"), "{err}");
    }

    #[test]
    fn validate_err_when_bridge_with_one_group() {
        let cfg = PartitionConfig {
            heal_topology: HealTopology::Bridge,
            ..PartitionConfig::for_test(3, &[&[0, 1, 2]])
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("requires exactly 2 groups"),
            "{err}"
        );
        assert!(err.to_string().contains("got 1"), "{err}");
    }

    #[test]
    fn validate_err_when_bridge_with_three_groups() {
        let cfg = PartitionConfig {
            heal_topology: HealTopology::Bridge,
            ..PartitionConfig::for_test(6, &[&[0, 1], &[2, 3], &[4, 5]])
        };
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("requires exactly 2 groups"),
            "{err}"
        );
        assert!(err.to_string().contains("got 3"), "{err}");
    }

    #[test]
    fn validate_ok_when_bridge_with_two_groups() {
        let cfg = PartitionConfig {
            heal_topology: HealTopology::Bridge,
            ..PartitionConfig::for_test(4, &[&[0, 1], &[2, 3]])
        };
        assert!(cfg.validate().is_ok());
    }

    /// The two-group rule applies only to `Bridge`. A `FullMesh` heal accepts
    /// any number of groups.
    #[test]
    fn validate_ok_when_full_mesh_with_three_groups() {
        let cfg = PartitionConfig::for_test(6, &[&[0, 1], &[2, 3], &[4, 5]]);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn total_ops_is_groups_times_ops_per_group() {
        let cfg = PartitionConfig {
            ops_per_group: 7,
            ..PartitionConfig::for_test(6, &[&[0, 1], &[2, 3], &[4, 5]])
        };
        assert_eq!(cfg.total_ops(), 21);
    }

    // ── ScenarioFile ───────────────────────────────────────────────────────

    #[test]
    fn node_count_topology_variant() {
        let s = parse(
            r#"
            name = "t"
            [topology]
            node_count = 5
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 1
            "#,
        );
        assert_eq!(s.node_count(), 5);
    }

    #[test]
    fn node_count_partition_heal_variant() {
        let s = ScenarioFile {
            name: "p".into(),
            body: ScenarioBody::PartitionHeal(PartitionConfig::for_test(4, &[&[0, 1], &[2, 3]])),
        };
        assert_eq!(s.node_count(), 4);
    }

    /// A scenario-level error names the scenario, so a bad file in a long
    /// sweep is easy to find.
    #[test]
    fn scenario_validate_names_the_scenario() {
        let s = ScenarioFile {
            name: "broken-cell".into(),
            body: ScenarioBody::PartitionHeal(PartitionConfig::for_test(4, &[&[0, 0], &[2, 3]])),
        };
        let err = s.validate().unwrap_err();
        assert!(
            format!("{err:#}").contains("scenario 'broken-cell'"),
            "{err:#}"
        );
    }

    // ── Parse-time invariants ──────────────────────────────────────────────

    #[test]
    fn scenario_parses_topology_form() {
        let s = parse(
            r#"
            name = "t"
            [topology]
            node_count = 3
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 2
            "#,
        );
        assert_eq!(s.name, "t");
        assert!(matches!(s.body, ScenarioBody::Topology(_)));
    }

    #[test]
    fn scenario_parses_partition_heal_form() {
        let s = parse(
            r#"
            name = "p"
            [partition_heal]
            node_count = 2
            ops_per_group = 1
            write_pattern = "round_robin"
            [[partition_heal.groups]]
            nodes = [0, 1]
            "#,
        );
        assert!(matches!(s.body, ScenarioBody::PartitionHeal(_)));
    }

    #[test]
    fn scenario_rejects_both_bodies_present() {
        let err = toml::from_str::<ScenarioFile>(
            r#"
            name = "x"
            [topology]
            node_count = 2
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 1
            [partition_heal]
            node_count = 2
            ops_per_group = 1
            write_pattern = "round_robin"
            [[partition_heal.groups]]
            nodes = [0, 1]
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("only one of"), "{err}");
    }

    #[test]
    fn scenario_rejects_no_body() {
        let err = toml::from_str::<ScenarioFile>(r#"name = "x""#).unwrap_err();
        assert!(err.to_string().contains("must be present"), "{err}");
    }

    // ── op_interval_ms ─────────────────────────────────────────────────────

    /// A scenario that omits `op_interval_ms` must get 0, which sends
    /// operations as fast as possible.
    #[test]
    fn op_interval_ms_defaults_to_zero_when_absent() {
        let cfg = parse_topology(
            r#"
            name = "t"
            [topology]
            node_count = 3
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 2
            "#,
        );
        assert_eq!(cfg.op_interval_ms, 0);
    }

    #[test]
    fn op_interval_ms_parses_explicit_value() {
        let cfg = parse_topology(
            r#"
            name = "t"
            [topology]
            node_count = 3
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 4
            op_interval_ms = 250
            "#,
        );
        assert_eq!(cfg.op_interval_ms, 250);
    }

    #[test]
    fn topology_config_validate_delegates_to_connections() {
        let bad = parse_topology(
            r#"
            name = "t"
            [topology]
            node_count = 3
            connections = { edges = [[0, 1]] }
            write_pattern = "round_robin"
            op_count = 1
            "#,
        );
        // Node 2 is unreachable — should be caught by connections.validate.
        let err = bad.validate().unwrap_err();
        assert!(err.to_string().contains("disconnected"), "{err}");
    }

    // ── HealTopology ───────────────────────────────────────────────────────

    /// A partition-heal scenario that omits `heal_topology` must get
    /// `FullMesh`.
    #[test]
    fn heal_topology_defaults_to_full_mesh_when_absent() {
        let cfg = parse_partition(
            r#"
            name = "p"
            [partition_heal]
            node_count = 2
            ops_per_group = 1
            write_pattern = "round_robin"
            [[partition_heal.groups]]
            nodes = [0, 1]
            "#,
        );
        assert_eq!(cfg.heal_topology, HealTopology::FullMesh);
    }

    #[test]
    fn heal_topology_parses_bridge_form() {
        let cfg = parse_partition(
            r#"
            name = "p"
            [partition_heal]
            node_count = 4
            ops_per_group = 1
            write_pattern = "round_robin"
            heal_topology = "bridge"
            [[partition_heal.groups]]
            nodes = [0, 1]
            [[partition_heal.groups]]
            nodes = [2, 3]
            "#,
        );
        assert_eq!(cfg.heal_topology, HealTopology::Bridge);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn heal_topology_parses_full_mesh_form() {
        let cfg = parse_partition(
            r#"
            name = "p"
            [partition_heal]
            node_count = 4
            ops_per_group = 1
            write_pattern = "round_robin"
            heal_topology = "full_mesh"
            [[partition_heal.groups]]
            nodes = [0, 1]
            [[partition_heal.groups]]
            nodes = [2, 3]
            "#,
        );
        assert_eq!(cfg.heal_topology, HealTopology::FullMesh);
    }

    #[test]
    fn heal_topology_default_is_full_mesh() {
        assert_eq!(HealTopology::default(), HealTopology::FullMesh);
    }

    // ── Built-in scenarios ─────────────────────────────────────────────────

    /// The built-ins parse, validate, and keep the names and sizes the
    /// regression suite has always run.
    #[test]
    fn builtin_scenarios_are_valid() {
        let scenarios = builtin_scenarios();
        let shape: Vec<(&str, usize, usize)> = scenarios
            .iter()
            .map(|s| (s.name.as_str(), s.node_count(), s.op_count()))
            .collect();
        assert_eq!(
            shape,
            [
                ("full-mesh-n2", 2, 2),
                ("full-mesh-n3", 3, 6),
                ("partition-heal-n4", 4, 8),
            ]
        );
        for s in &scenarios {
            s.validate().unwrap();
        }
    }
}
