use serde::Deserialize;

/// Write distribution for ops in a scenario run.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WritePattern {
    /// All writes go to the first node in scope (node 0 or `group.nodes[0]`).
    Concentrated,
    /// Writes cycle through nodes in round-robin order.
    RoundRobin,
}

/// Connection topology for a scenario run.
///
/// Note: only `FullMesh` guarantees convergence for all write patterns with
/// the current sync implementation. Ring/star topologies require
/// relay-on-receive in `recv_loop` (not yet implemented).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Connections {
    /// Connect every pair — n*(n-1)/2 bidi sync streams.
    FullMesh,
}

impl Connections {
    /// Return the concrete (initiator, target) edge list for `n` nodes.
    pub fn edges(&self, n: usize) -> Vec<(usize, usize)> {
        match self {
            Self::FullMesh => (0..n)
                .flat_map(|i| (i + 1..n).map(move |j| (i, j)))
                .collect(),
        }
    }
}

/// Configuration for a full-mesh topology run.
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
}

/// Result of a completed scenario run.
pub struct RunResult {
    /// Milliseconds from convergence-wait start until all fingerprints match.
    ///
    /// For [`crate::runner::run`]: measured after all ops are applied.
    /// For [`crate::runner::run_partition_heal`]: measured from heal start.
    pub convergence_ms: u128,
    /// Total ops applied across all nodes.
    pub total_ops: usize,
}

/// Top-level scenario file — TOML format; exactly one of `[topology]` or
/// `[partition_heal]` must be present.
#[derive(Debug, Deserialize)]
pub struct ScenarioFile {
    /// Human-readable name used in log output and result reporting.
    pub name: String,
    /// Present for a topology run.
    pub topology: Option<TopologyConfig>,
    /// Present for a partition-heal run.
    pub partition_heal: Option<PartitionConfig>,
}
