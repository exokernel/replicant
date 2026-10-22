//! The graphs a run can be wired into: which node pairs get a sync link, and
//! the graph properties that predict convergence.
//!
//! This module is pure functions only. It spawns nothing and talks to no
//! replica.

use std::collections::{HashSet, VecDeque};

use anyhow::{Result, bail};
use serde::{Deserialize, Deserializer};

/// Which node pairs a run connects.
///
/// The named variants compute their edges from the node count `n`. `Custom`
/// carries an explicit undirected edge list.
///
/// A new variant needs a new arm in `kind` and `edges`. The enum is matched
/// exhaustively so the compiler reports both.
#[derive(Debug, Clone)]
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

// Hand-written Deserialize. It accepts these two TOML forms:
//
//   connections = "full_mesh" | "ring" | "line" | "star"
//   connections = { edges = [[0,1],[1,2]] }
//
// serde's default for a struct variant would instead require
// `connections = { custom = { edges = [...] } }`.
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
    /// Returns the stable name of this topology. Results are grouped by this
    /// value, so it must not change.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::FullMesh => "full_mesh",
            Self::Ring => "ring",
            Self::Line => "line",
            Self::Star => "star",
            Self::Custom { .. } => "custom",
        }
    }

    /// Returns the `(initiator, target)` edges for `n` nodes.
    ///
    /// No checking happens here. A degenerate combination, such as `Ring` with
    /// `n < 3`, produces edges that [`Connections::validate`] then rejects.
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

    /// Returns the diameter of this topology on `n` nodes. See [`diameter`].
    ///
    /// Call [`Connections::validate`] first if the edges came from a `Custom`
    /// list.
    pub fn diameter(&self, n: usize) -> usize {
        diameter(n, &self.edges(n))
    }

    /// Checks that the edges form a connected undirected simple graph on `n`
    /// nodes.
    ///
    /// Rejects four faults: an index outside `0..n`, a self-loop, a duplicate
    /// undirected edge, and a disconnected graph. A disconnected graph would
    /// never converge, so failing here saves waiting for the timeout.
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

        // Connectivity: BFS from node 0. A graph of 0 or 1 nodes is connected.
        if n > 1 {
            let unreached: Vec<usize> = bfs_distances(&adjacency_list(n, &edges), 0)
                .iter()
                .enumerate()
                .filter_map(|(i, d)| d.is_none().then_some(i))
                .collect();
            if !unreached.is_empty() {
                bail!("disconnected topology: nodes {unreached:?} unreachable from node 0");
            }
        }

        Ok(())
    }
}

/// Returns the diameter of the undirected graph with `edges` on `n` nodes: the
/// longest shortest path between any pair of nodes. It predicts how many hops
/// convergence needs.
///
/// Runs a BFS from every node, so it costs `O(n·(n+e))`. Returns 0 for
/// `n <= 1`. Unreachable nodes are skipped, so a disconnected graph yields the
/// largest diameter within any one component. Every edge must be within
/// `0..n`.
pub fn diameter(n: usize, edges: &[(usize, usize)]) -> usize {
    if n <= 1 {
        return 0;
    }
    let adj = adjacency_list(n, edges);
    (0..n)
        .flat_map(|start| bfs_distances(&adj, start))
        .flatten()
        .max()
        .unwrap_or(0)
}

/// Returns every pair within `nodes`, which wires one group as a full mesh.
pub fn intra_group_edges(nodes: &[usize]) -> Vec<(usize, usize)> {
    nodes
        .iter()
        .flat_map(|&i| nodes.iter().filter(move |&&j| j > i).map(move |&j| (i, j)))
        .collect()
}

/// Builds an undirected adjacency list from an edge list.
///
/// Each edge `(i, j)` adds `j` to `adj[i]` and `i` to `adj[j]`. The caller must
/// ensure `i < n` and `j < n`. An out-of-range edge panics.
fn adjacency_list(n: usize, edges: &[(usize, usize)]) -> Vec<Vec<usize>> {
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(i, j) in edges {
        adj[i].push(j);
        adj[j].push(i);
    }
    adj
}

/// Returns the hop count from `start` to every node, or `None` for a node that
/// `start` cannot reach.
fn bfs_distances(adj: &[Vec<usize>], start: usize) -> Vec<Option<usize>> {
    let mut dist = vec![None; adj.len()];
    dist[start] = Some(0);
    let mut queue = VecDeque::from([(start, 0)]);
    while let Some((node, d)) = queue.pop_front() {
        for &neighbor in &adj[node] {
            if dist[neighbor].is_none() {
                dist[neighbor] = Some(d + 1);
                queue.push_back((neighbor, d + 1));
            }
        }
    }
    dist
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Connections::edges ─────────────────────────────────────────────────

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

    // ── intra_group_edges ──────────────────────────────────────────────────

    #[test]
    fn intra_group_edges_two_nodes() {
        assert_eq!(intra_group_edges(&[0, 1]), vec![(0, 1)]);
    }

    #[test]
    fn intra_group_edges_three_nodes() {
        let mut edges = intra_group_edges(&[0, 1, 2]);
        edges.sort_unstable();
        assert_eq!(edges, vec![(0, 1), (0, 2), (1, 2)]);
    }

    #[test]
    fn intra_group_edges_single_node_is_empty() {
        assert!(intra_group_edges(&[3]).is_empty());
    }

    #[test]
    fn intra_group_edges_no_symmetric_duplicates() {
        let edges = intra_group_edges(&[0, 1, 2, 3]);
        // Every pair should appear exactly once, in (smaller, larger) order.
        let n = edges.len();
        let deduped: HashSet<_> = edges.iter().copied().collect();
        assert_eq!(n, deduped.len());
        assert!(edges.iter().all(|(i, j)| i < j));
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
        assert!(msg.contains("[2, 3]"), "{msg}");
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

    // ── Connections::kind ──────────────────────────────────────────────────

    #[test]
    fn kind_strings_are_stable() {
        assert_eq!(Connections::FullMesh.kind(), "full_mesh");
        assert_eq!(Connections::Ring.kind(), "ring");
        assert_eq!(Connections::Line.kind(), "line");
        assert_eq!(Connections::Star.kind(), "star");
        assert_eq!(Connections::Custom { edges: vec![] }.kind(), "custom");
    }

    // ── diameter ───────────────────────────────────────────────────────────

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
        assert_eq!(diameter(4, &[(0, 1), (1, 2), (1, 3)]), 2);
    }

    #[test]
    fn diameter_custom_disconnected_returns_largest_component() {
        // Components: {0,1,2} as a line (diameter 2), {3,4} as one edge.
        // Largest within-component diameter is 2.
        assert_eq!(diameter(5, &[(0, 1), (1, 2), (3, 4)]), 2);
    }
}
