use std::collections::{HashSet, VecDeque};

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer};

/// Top-level scenario file — TOML format; exactly one of `[topology]` or
/// `[partition_heal]` must be present.
#[derive(Debug)]
pub struct ScenarioFile {
    /// Human-readable name used in log output and result reporting.
    pub name: String,
    /// Which kind of scenario this file describes.
    pub body: ScenarioBody,
}

/// The two scenario shapes a file may describe. The exactly-one-set invariant
/// from the TOML layer is encoded here so downstream code never has to
/// re-check it.
#[derive(Debug)]
pub enum ScenarioBody {
    Topology(TopologyConfig),
    PartitionHeal(PartitionConfig),
}

// Custom Deserialize so the TOML form stays as:
//   [topology]    | [partition_heal]
// rather than serde's default `body = { topology = {...} }`. Validates that
// exactly one of the two tables is present and surfaces a clear error
// otherwise, so the invariant is established at parse time.
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

/// Write distribution for ops in a scenario run.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePattern {
    /// All writes go to the first node in scope (node 0 or `group.nodes[0]`).
    Concentrated,
    /// Writes cycle through nodes in round-robin order.
    RoundRobin,
}

/// Post-heal wiring for a partition-heal scenario.
///
/// Selects how the partition is repaired in phase 2. `FullMesh` reconnects
/// every pair across groups (original behaviour); `Bridge` connects only
/// `groups[0].nodes[0]` to `groups[1].nodes[0]`, forcing all cross-partition
/// state through one edge. `Bridge` requires exactly 2 groups.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealTopology {
    /// Connect every cross-group pair on heal (post-heal graph is a full mesh).
    #[default]
    FullMesh,
    /// Connect only `groups[0].nodes[0]` to `groups[1].nodes[0]` on heal.
    Bridge,
}

/// Connection topology for a scenario run.
///
/// Named variants (`FullMesh`, `Ring`, `Line`, `Star`) are derived
/// programmatically from `n`. `Custom` carries an explicit undirected edge
/// list for arbitrary graphs. `#[non_exhaustive]` keeps `match` arms
/// forwards-compatible.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Connections {
    /// Connect every pair — n*(n-1)/2 bidi sync streams. Diameter 1.
    FullMesh,
    /// Cycle of length `n`: edges `(i, (i+1) mod n)`. Diameter ⌊n/2⌋.
    /// Degenerate for `n < 3`.
    Ring,
    /// Path: edges `(i, i+1)` for `i in 0..n-1`. Diameter `n-1`.
    Line,
    /// Hub-and-spoke: edges `(0, k)` for `k in 1..n`. Diameter 2.
    Star,
    /// User-supplied undirected edge list. Must satisfy
    /// [`Connections::validate`].
    Custom {
        /// Edges as `(i, j)`. Treated as undirected; duplicates (in either
        /// orientation) are rejected.
        edges: Vec<(usize, usize)>,
    },
}

// Custom Deserialize so TOML accepts the ergonomic forms:
//   connections = "full_mesh" | "ring" | "line" | "star"
//   connections = { edges = [[0,1],[1,2]] }     # Custom
// rather than serde's externally-tagged default for struct variants
// (`connections = { custom = { edges = [...] } }`).
impl<'de> Deserialize<'de> for Connections {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Named {
            FullMesh,
            Ring,
            Line,
            Star,
        }
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Named(Named),
            Custom { edges: Vec<(usize, usize)> },
        }
        Ok(match Repr::deserialize(deserializer)? {
            Repr::Named(Named::FullMesh) => Connections::FullMesh,
            Repr::Named(Named::Ring) => Connections::Ring,
            Repr::Named(Named::Line) => Connections::Line,
            Repr::Named(Named::Star) => Connections::Star,
            Repr::Custom { edges } => Connections::Custom { edges },
        })
    }
}

impl Connections {
    /// Stable identifier for this topology, used in result reporting so
    /// downstream analyses can group/pivot by topology kind.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::FullMesh => "full_mesh",
            Self::Ring => "ring",
            Self::Line => "line",
            Self::Star => "star",
            Self::Custom { .. } => "custom",
        }
    }

    /// Return the (initiator, target) edge list for `n` nodes.
    ///
    /// Mechanical view of the topology; degenerate combinations (e.g. `Ring`
    /// with `n < 3`) yield edges that fail [`Connections::validate`].
    pub fn edges(&self, n: usize) -> Vec<(usize, usize)> {
        match self {
            Self::FullMesh => (0..n)
                .flat_map(|i| (i + 1..n).map(move |j| (i, j)))
                .collect(),
            Self::Ring => (0..n).map(|i| (i, (i + 1) % n)).collect(),
            Self::Line => (0..n.saturating_sub(1)).map(|i| (i, i + 1)).collect(),
            Self::Star => (1..n).map(|k| (0, k)).collect(),
            Self::Custom { edges } => edges.clone(),
        }
    }

    /// Diameter of the (undirected) topology — the longest shortest-path
    /// between any pair of nodes. Predictor of multi-hop convergence latency.
    ///
    /// Computed by BFS from every node (O(n·(n+e))); fine for thesis-scale
    /// `n`. Assumes the graph is connected — call [`Connections::validate`]
    /// first if the input came from a `Custom` user-supplied edge list.
    /// Returns 0 for `n <= 1`. Unreachable nodes are skipped, so on a
    /// disconnected graph this returns the largest within-component diameter.
    pub fn diameter(&self, n: usize) -> usize {
        if n <= 1 {
            return 0;
        }
        let adj = adjacency_list(n, &self.edges(n));
        let mut max_dist = 0;
        for start in 0..n {
            let mut dist = vec![usize::MAX; n];
            let mut queue: VecDeque<usize> = VecDeque::new();
            dist[start] = 0;
            queue.push_back(start);
            while let Some(node) = queue.pop_front() {
                for &neighbor in &adj[node] {
                    if dist[neighbor] == usize::MAX {
                        dist[neighbor] = dist[node] + 1;
                        queue.push_back(neighbor);
                    }
                }
            }
            for &d in &dist {
                if d != usize::MAX && d > max_dist {
                    max_dist = d;
                }
            }
        }
        max_dist
    }

    /// Validate that the resulting edge list is a well-formed connected
    /// undirected simple graph on `n` nodes.
    ///
    /// Catches the four common ways topologies break in practice:
    /// out-of-range indices, self-loops, duplicate undirected edges, and
    /// disconnected components — the last would never converge, so we fail
    /// fast instead of waiting for the convergence timeout.
    pub fn validate(&self, n: usize) -> Result<()> {
        let edges = self.edges(n);

        let mut seen: HashSet<(usize, usize)> = HashSet::with_capacity(edges.len());
        for &(i, j) in &edges {
            if i >= n || j >= n {
                bail!("edge ({i},{j}) out of range for node_count={n}");
            }
            if i == j {
                bail!("self-loop at node {i}");
            }
            let canon = if i < j { (i, j) } else { (j, i) };
            if !seen.insert(canon) {
                bail!("duplicate undirected edge ({},{})", canon.0, canon.1);
            }
        }

        // Connectivity via BFS from node 0; n <= 1 is trivially connected.
        if n > 1 {
            let adj = adjacency_list(n, &edges);
            let mut visited = vec![false; n];
            let mut queue: VecDeque<usize> = VecDeque::new();
            visited[0] = true;
            queue.push_back(0);
            while let Some(node) = queue.pop_front() {
                for &neighbor in &adj[node] {
                    if !visited[neighbor] {
                        visited[neighbor] = true;
                        queue.push_back(neighbor);
                    }
                }
            }
            if !visited.iter().all(|&v| v) {
                let unreached: Vec<usize> = visited
                    .iter()
                    .enumerate()
                    .filter_map(|(i, &v)| (!v).then_some(i))
                    .collect();
                bail!("disconnected topology: nodes {unreached:?} unreachable from node 0");
            }
        }

        Ok(())
    }
}

/// Configuration for a topology run.
#[derive(Debug, Clone, Deserialize)]
pub struct TopologyConfig {
    /// Number of replica nodes to spawn.
    pub node_count: usize,
    /// Which node pairs to connect before writing.
    pub connections: Connections,
    /// How to distribute write ops across nodes.
    pub write_pattern: WritePattern,
    /// Total `MapPut` ops to apply.
    pub op_count: usize,
}

/// A single partition group in a partition-heal scenario.
#[derive(Debug, Clone, Deserialize)]
pub struct Group {
    /// Node indices belonging to this partition group.
    pub nodes: Vec<usize>,
}

/// Configuration for a partition-then-heal scenario.
///
/// Phase 1: nodes in each group connect internally and write independently.
/// Phase 2 (heal): remaining cross-group edges are added; we wait for
/// global convergence and record the time.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionConfig {
    /// Total node count (must equal the sum of all group sizes).
    pub node_count: usize,
    /// Disjoint sets of node indices, one per partition group.
    pub groups: Vec<Group>,
    /// Ops applied to each group independently during the partition phase.
    pub ops_per_group: usize,
    /// Write distribution within each group.
    pub write_pattern: WritePattern,
    /// Wiring added on heal. Defaults to `FullMesh` so pre-existing TOML
    /// scenarios that omit the field keep their original behaviour.
    #[serde(default)]
    pub heal_topology: HealTopology,
}

/// Result of a completed scenario run.
#[derive(Debug, Clone, Copy)]
pub struct RunResult {
    /// Fractional milliseconds from write-start (full-mesh) or heal-start
    /// (partition-heal) until all node fingerprints agree.
    pub convergence_ms: f64,
    /// Total ops applied across all nodes.
    pub total_ops: usize,
    /// Stable identifier for the topology that produced this run, e.g.
    /// `"full_mesh"`, `"ring"`, `"partition_heal"`. Used as a pivot key
    /// in CSV/JSON output.
    pub topology_kind: &'static str,
    /// Number of undirected edges in the final wired topology.
    pub edge_count: usize,
    /// Diameter of the final wired topology (longest shortest-path between
    /// any pair of nodes). The structural predictor of multi-hop convergence
    /// latency. `0` for `n <= 1`.
    pub diameter: usize,
}

/// Build an undirected adjacency list from an edge list.
///
/// Each edge `(i, j)` adds `j` to `adj[i]` and `i` to `adj[j]`. Caller is
/// responsible for ensuring `i, j < n`; out-of-range edges panic on index.
fn adjacency_list(n: usize, edges: &[(usize, usize)]) -> Vec<Vec<usize>> {
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(i, j) in edges {
        adj[i].push(j);
        adj[j].push(i);
    }
    adj
}

impl ScenarioFile {
    /// Number of replica nodes this scenario will spawn.
    pub fn node_count(&self) -> usize {
        match &self.body {
            ScenarioBody::Topology(t) => t.node_count,
            ScenarioBody::PartitionHeal(p) => p.node_count,
        }
    }

    /// Total ops applied across all nodes in one trial of this scenario.
    ///
    /// Deterministic from the config, so every trial of the same scenario
    /// reports the same value.
    pub fn op_count(&self) -> usize {
        match &self.body {
            ScenarioBody::Topology(t) => t.op_count,
            ScenarioBody::PartitionHeal(p) => p.groups.len() * p.ops_per_group,
        }
    }
}

impl TopologyConfig {
    /// Check that the connection topology is well-formed for `node_count`
    /// (see [`Connections::validate`]).
    pub fn validate(&self) -> Result<()> {
        self.connections.validate(self.node_count)
    }
}

impl PartitionConfig {
    /// Check that `node_count` equals the total number of nodes across all
    /// groups, and that `heal_topology = "bridge"` (if set) is paired with
    /// exactly 2 groups — the bridge variant is currently only defined for
    /// two-partition heals.
    pub fn validate(&self) -> Result<()> {
        let total: usize = self.groups.iter().map(|g| g.nodes.len()).sum();
        if total != self.node_count {
            bail!(
                "PartitionConfig: node_count={} but groups cover {} nodes",
                self.node_count,
                total
            );
        }
        if self.heal_topology == HealTopology::Bridge && self.groups.len() != 2 {
            bail!(
                "PartitionConfig: heal_topology=\"bridge\" requires exactly 2 groups, got {}",
                self.groups.len()
            );
        }
        Ok(())
    }
}

/// Built-in scenarios used as a regression suite when no TOML files are given.
pub fn builtin_scenarios() -> Vec<ScenarioFile> {
    vec![
        ScenarioFile {
            name: "full-mesh-n2".to_owned(),
            body: ScenarioBody::Topology(TopologyConfig {
                node_count: 2,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 2,
            }),
        },
        ScenarioFile {
            name: "full-mesh-n3".to_owned(),
            body: ScenarioBody::Topology(TopologyConfig {
                node_count: 3,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 6,
            }),
        },
        ScenarioFile {
            name: "partition-heal-n4".to_owned(),
            body: ScenarioBody::PartitionHeal(PartitionConfig {
                node_count: 4,
                groups: vec![Group { nodes: vec![0, 1] }, Group { nodes: vec![2, 3] }],
                ops_per_group: 4,
                write_pattern: WritePattern::RoundRobin,
                heal_topology: HealTopology::FullMesh,
            }),
        },
    ]
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(node_count: usize, groups: Vec<Vec<usize>>) -> PartitionConfig {
        PartitionConfig {
            node_count,
            groups: groups.into_iter().map(|nodes| Group { nodes }).collect(),
            ops_per_group: 1,
            write_pattern: WritePattern::RoundRobin,
            heal_topology: HealTopology::FullMesh,
        }
    }

    #[test]
    fn validate_ok_when_counts_match() {
        assert!(
            make_config(4, vec![vec![0, 1], vec![2, 3]])
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validate_err_when_node_count_too_high() {
        let err = make_config(5, vec![vec![0, 1], vec![2, 3]])
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("node_count=5"), "{err}");
        assert!(err.to_string().contains("cover 4 nodes"), "{err}");
    }

    #[test]
    fn validate_err_when_node_count_too_low() {
        let err = make_config(2, vec![vec![0, 1, 2]]).validate().unwrap_err();
        assert!(err.to_string().contains("node_count=2"), "{err}");
        assert!(err.to_string().contains("cover 3 nodes"), "{err}");
    }

    #[test]
    fn validate_ok_single_group() {
        assert!(make_config(3, vec![vec![0, 1, 2]]).validate().is_ok());
    }

    // ── ScenarioFile::node_count ───────────────────────────────────────────

    #[test]
    fn node_count_topology_variant() {
        let s = ScenarioFile {
            name: "t".into(),
            body: ScenarioBody::Topology(TopologyConfig {
                node_count: 5,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 1,
            }),
        };
        assert_eq!(s.node_count(), 5);
    }

    #[test]
    fn node_count_partition_heal_variant() {
        let s = ScenarioFile {
            name: "p".into(),
            body: ScenarioBody::PartitionHeal(PartitionConfig {
                node_count: 4,
                groups: vec![Group { nodes: vec![0, 1] }, Group { nodes: vec![2, 3] }],
                ops_per_group: 2,
                write_pattern: WritePattern::RoundRobin,
                heal_topology: HealTopology::FullMesh,
            }),
        };
        assert_eq!(s.node_count(), 4);
    }

    // ── ScenarioFile parse-time invariants ─────────────────────────────────

    #[test]
    fn scenario_parses_topology_form() {
        let s: ScenarioFile = toml::from_str(
            r#"
            name = "t"
            [topology]
            node_count = 3
            connections = "full_mesh"
            write_pattern = "round_robin"
            op_count = 2
            "#,
        )
        .unwrap();
        assert_eq!(s.name, "t");
        assert!(matches!(s.body, ScenarioBody::Topology(_)));
    }

    #[test]
    fn scenario_parses_partition_heal_form() {
        let s: ScenarioFile = toml::from_str(
            r#"
            name = "p"
            [partition_heal]
            node_count = 2
            ops_per_group = 1
            write_pattern = "round_robin"
            [[partition_heal.groups]]
            nodes = [0, 1]
            "#,
        )
        .unwrap();
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

    // ── HealTopology ───────────────────────────────────────────────────────

    /// Pre-existing partition_heal TOML scenarios omit `heal_topology`; the
    /// field must default to `FullMesh` so their behaviour does not change.
    #[test]
    fn heal_topology_defaults_to_full_mesh_when_absent() {
        let s: ScenarioFile = toml::from_str(
            r#"
            name = "p"
            [partition_heal]
            node_count = 2
            ops_per_group = 1
            write_pattern = "round_robin"
            [[partition_heal.groups]]
            nodes = [0, 1]
            "#,
        )
        .unwrap();
        let ScenarioBody::PartitionHeal(cfg) = s.body else {
            panic!("expected PartitionHeal body");
        };
        assert_eq!(cfg.heal_topology, HealTopology::FullMesh);
    }

    #[test]
    fn heal_topology_parses_bridge_form() {
        let s: ScenarioFile = toml::from_str(
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
        )
        .unwrap();
        let ScenarioBody::PartitionHeal(cfg) = s.body else {
            panic!("expected PartitionHeal body");
        };
        assert_eq!(cfg.heal_topology, HealTopology::Bridge);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn heal_topology_parses_full_mesh_form() {
        let s: ScenarioFile = toml::from_str(
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
        )
        .unwrap();
        let ScenarioBody::PartitionHeal(cfg) = s.body else {
            panic!("expected PartitionHeal body");
        };
        assert_eq!(cfg.heal_topology, HealTopology::FullMesh);
    }

    #[test]
    fn heal_topology_default_is_full_mesh() {
        assert_eq!(HealTopology::default(), HealTopology::FullMesh);
    }

    #[test]
    fn validate_err_when_bridge_with_one_group() {
        let mut cfg = make_config(3, vec![vec![0, 1, 2]]);
        cfg.heal_topology = HealTopology::Bridge;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("requires exactly 2 groups"),
            "{err}"
        );
        assert!(err.to_string().contains("got 1"), "{err}");
    }

    #[test]
    fn validate_err_when_bridge_with_three_groups() {
        let mut cfg = make_config(6, vec![vec![0, 1], vec![2, 3], vec![4, 5]]);
        cfg.heal_topology = HealTopology::Bridge;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("requires exactly 2 groups"),
            "{err}"
        );
        assert!(err.to_string().contains("got 3"), "{err}");
    }

    #[test]
    fn validate_ok_when_bridge_with_two_groups() {
        let mut cfg = make_config(4, vec![vec![0, 1], vec![2, 3]]);
        cfg.heal_topology = HealTopology::Bridge;
        assert!(cfg.validate().is_ok());
    }

    /// Many-group partition-heals are still allowed under the default
    /// `FullMesh` heal — the 2-group restriction only kicks in for `Bridge`.
    #[test]
    fn validate_ok_when_full_mesh_with_three_groups() {
        let cfg = make_config(6, vec![vec![0, 1], vec![2, 3], vec![4, 5]]);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn connections_full_mesh_edges_n2() {
        assert_eq!(Connections::FullMesh.edges(2), vec![(0, 1)]);
    }

    #[test]
    fn connections_full_mesh_edges_n3() {
        assert_eq!(Connections::FullMesh.edges(3), vec![(0, 1), (0, 2), (1, 2)]);
    }

    #[test]
    fn connections_full_mesh_edges_count() {
        // n*(n-1)/2 pairs for full mesh
        for n in 0..=6 {
            assert_eq!(
                Connections::FullMesh.edges(n).len(),
                n * n.saturating_sub(1) / 2
            );
        }
    }

    // ── Connections::edges — Ring / Line / Star / Custom ───────────────────

    #[test]
    fn connections_ring_edges_n5() {
        assert_eq!(
            Connections::Ring.edges(5),
            vec![(0, 1), (1, 2), (2, 3), (3, 4), (4, 0)]
        );
    }

    #[test]
    fn connections_ring_edge_count_equals_n() {
        for n in 3..=6 {
            assert_eq!(Connections::Ring.edges(n).len(), n);
        }
    }

    #[test]
    fn connections_line_edges_n5() {
        assert_eq!(
            Connections::Line.edges(5),
            vec![(0, 1), (1, 2), (2, 3), (3, 4)]
        );
    }

    #[test]
    fn connections_line_edge_count_n_minus_1() {
        for n in 2..=6 {
            assert_eq!(Connections::Line.edges(n).len(), n - 1);
        }
        assert!(Connections::Line.edges(0).is_empty());
        assert!(Connections::Line.edges(1).is_empty());
    }

    #[test]
    fn connections_star_edges_n5() {
        assert_eq!(
            Connections::Star.edges(5),
            vec![(0, 1), (0, 2), (0, 3), (0, 4)]
        );
    }

    #[test]
    fn connections_star_all_edges_anchor_at_zero() {
        for &(i, _) in &Connections::Star.edges(8) {
            assert_eq!(i, 0);
        }
    }

    #[test]
    fn connections_custom_edges_passthrough() {
        let e = vec![(0, 2), (2, 1), (1, 3)];
        assert_eq!(Connections::Custom { edges: e.clone() }.edges(4), e);
    }

    // ── Connections::validate ──────────────────────────────────────────────

    #[test]
    fn validate_named_topologies_ok_at_sensible_n() {
        assert!(Connections::FullMesh.validate(4).is_ok());
        assert!(Connections::Ring.validate(4).is_ok());
        assert!(Connections::Line.validate(4).is_ok());
        assert!(Connections::Star.validate(4).is_ok());
    }

    #[test]
    fn validate_ring_n2_rejects_duplicate_edge() {
        // Ring on 2 nodes emits (0,1) then (1,0) — same undirected edge.
        let err = Connections::Ring.validate(2).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn validate_ring_n1_rejects_self_loop() {
        let err = Connections::Ring.validate(1).unwrap_err();
        assert!(err.to_string().contains("self-loop"), "{err}");
    }

    #[test]
    fn validate_custom_rejects_out_of_range() {
        let c = Connections::Custom {
            edges: vec![(0, 1), (1, 5)],
        };
        let err = c.validate(3).unwrap_err();
        assert!(err.to_string().contains("out of range"), "{err}");
    }

    #[test]
    fn validate_custom_rejects_self_loop() {
        let c = Connections::Custom {
            edges: vec![(0, 1), (2, 2)],
        };
        let err = c.validate(3).unwrap_err();
        assert!(err.to_string().contains("self-loop"), "{err}");
    }

    #[test]
    fn validate_custom_rejects_duplicate_undirected_edges() {
        // (0,1) and (1,0) are the same undirected edge.
        let c = Connections::Custom {
            edges: vec![(0, 1), (1, 2), (1, 0)],
        };
        let err = c.validate(3).unwrap_err();
        assert!(err.to_string().contains("duplicate"), "{err}");
    }

    #[test]
    fn validate_custom_rejects_disconnected_graph() {
        // Two components: {0,1} and {2,3}.
        let c = Connections::Custom {
            edges: vec![(0, 1), (2, 3)],
        };
        let err = c.validate(4).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("disconnected"), "{msg}");
        assert!(msg.contains("2") && msg.contains("3"), "{msg}");
    }

    #[test]
    fn validate_custom_accepts_connected_tree() {
        let c = Connections::Custom {
            edges: vec![(0, 1), (1, 2), (2, 3)],
        };
        assert!(c.validate(4).is_ok());
    }

    #[test]
    fn validate_n_zero_is_trivially_ok() {
        assert!(Connections::FullMesh.validate(0).is_ok());
    }

    #[test]
    fn topology_config_validate_delegates_to_connections() {
        let bad = TopologyConfig {
            node_count: 3,
            connections: Connections::Custom {
                edges: vec![(0, 1)],
            },
            write_pattern: WritePattern::RoundRobin,
            op_count: 1,
        };
        // Node 2 is unreachable — should be caught by connections.validate.
        let err = bad.validate().unwrap_err();
        assert!(err.to_string().contains("disconnected"), "{err}");
    }

    // ── TOML deserialization for Connections ───────────────────────────────

    fn parse_connections(s: &str) -> Connections {
        #[derive(Deserialize)]
        struct Wrap {
            connections: Connections,
        }
        toml::from_str::<Wrap>(s).expect("parse").connections
    }

    #[test]
    fn toml_connections_full_mesh() {
        assert!(matches!(
            parse_connections(r#"connections = "full_mesh""#),
            Connections::FullMesh
        ));
    }

    #[test]
    fn toml_connections_ring_line_star() {
        assert!(matches!(
            parse_connections(r#"connections = "ring""#),
            Connections::Ring
        ));
        assert!(matches!(
            parse_connections(r#"connections = "line""#),
            Connections::Line
        ));
        assert!(matches!(
            parse_connections(r#"connections = "star""#),
            Connections::Star
        ));
    }

    #[test]
    fn toml_connections_custom_inline_table() {
        let c = parse_connections(r#"connections = { edges = [[0, 1], [1, 2]] }"#);
        match c {
            Connections::Custom { edges } => assert_eq!(edges, vec![(0, 1), (1, 2)]),
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    #[test]
    fn toml_connections_custom_dotted_form() {
        // Dotted-key TOML form normalizes to the same value as inline-table.
        let c = parse_connections("connections.edges = [[0, 1], [1, 2], [2, 3]]");
        match c {
            Connections::Custom { edges } => assert_eq!(edges, vec![(0, 1), (1, 2), (2, 3)]),
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    // ── Connections::kind ──────────────────────────────────────────────────

    #[test]
    fn kind_strings_are_stable() {
        assert_eq!(Connections::FullMesh.kind(), "full_mesh");
        assert_eq!(Connections::Ring.kind(), "ring");
        assert_eq!(Connections::Line.kind(), "line");
        assert_eq!(Connections::Star.kind(), "star");
        assert_eq!(Connections::Custom { edges: vec![] }.kind(), "custom");
    }

    // ── Connections::diameter ──────────────────────────────────────────────

    #[test]
    fn diameter_zero_and_one_node() {
        assert_eq!(Connections::FullMesh.diameter(0), 0);
        assert_eq!(Connections::FullMesh.diameter(1), 0);
    }

    #[test]
    fn diameter_full_mesh_is_one() {
        for n in 2..=8 {
            assert_eq!(Connections::FullMesh.diameter(n), 1, "n={n}");
        }
    }

    #[test]
    fn diameter_ring_is_floor_n_over_two() {
        for n in 3..=12 {
            assert_eq!(Connections::Ring.diameter(n), n / 2, "n={n}");
        }
    }

    #[test]
    fn diameter_line_is_n_minus_one() {
        for n in 2..=10 {
            assert_eq!(Connections::Line.diameter(n), n - 1, "n={n}");
        }
    }

    #[test]
    fn diameter_star_is_two_for_n_geq_three() {
        // n=2 star is a single edge (diameter 1); n>=3 has leaf→hub→leaf paths.
        assert_eq!(Connections::Star.diameter(2), 1);
        for n in 3..=8 {
            assert_eq!(Connections::Star.diameter(n), 2, "n={n}");
        }
    }

    #[test]
    fn diameter_custom_matches_structure() {
        // Two-armed path: 0-1-2 plus a branch 1-3. Diameter is 2 (e.g. 0→2).
        let c = Connections::Custom {
            edges: vec![(0, 1), (1, 2), (1, 3)],
        };
        assert_eq!(c.diameter(4), 2);
    }

    #[test]
    fn diameter_custom_disconnected_returns_largest_component() {
        // Components: {0,1,2} as a line (diameter 2), {3,4} as one edge.
        // Largest within-component diameter is 2.
        let c = Connections::Custom {
            edges: vec![(0, 1), (1, 2), (3, 4)],
        };
        assert_eq!(c.diameter(5), 2);
    }

    #[test]
    fn toml_connections_unknown_name_rejected() {
        let err = toml::from_str::<std::collections::HashMap<String, Connections>>(
            r#"connections = "mesh""#,
        )
        .unwrap_err();
        // serde's untagged enum error wraps both arms; the message references
        // the candidates rather than a single one — just confirm it failed.
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }
}
