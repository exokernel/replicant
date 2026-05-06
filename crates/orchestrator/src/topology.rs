use anyhow::{Result, bail};
use serde::Deserialize;

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
    /// Fractional milliseconds from write-start (full-mesh) or heal-start
    /// (partition-heal) until all node fingerprints agree.
    pub convergence_ms: f64,
    /// Total ops applied across all nodes.
    pub total_ops: usize,
}

impl PartitionConfig {
    /// Check that `node_count` equals the total number of nodes across all groups.
    pub fn validate(&self) -> Result<()> {
        let total: usize = self.groups.iter().map(|g| g.nodes.len()).sum();
        if total != self.node_count {
            bail!(
                "PartitionConfig: node_count={} but groups cover {} nodes",
                self.node_count,
                total
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
            topology: Some(TopologyConfig {
                node_count: 2,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 2,
            }),
            partition_heal: None,
        },
        ScenarioFile {
            name: "full-mesh-n3".to_owned(),
            topology: Some(TopologyConfig {
                node_count: 3,
                connections: Connections::FullMesh,
                write_pattern: WritePattern::RoundRobin,
                op_count: 6,
            }),
            partition_heal: None,
        },
        ScenarioFile {
            name: "partition-heal-n4".to_owned(),
            topology: None,
            partition_heal: Some(PartitionConfig {
                node_count: 4,
                groups: vec![Group { nodes: vec![0, 1] }, Group { nodes: vec![2, 3] }],
                ops_per_group: 4,
                write_pattern: WritePattern::RoundRobin,
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
}
