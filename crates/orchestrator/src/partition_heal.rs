//! Runs one cell of the offline-divergence sweep.
//!
//! A cell is one experiment. Two or more groups of replicas start from the same
//! document. The groups are cut off from each other. Each group edits on its
//! own. The groups are then reconnected, and this module measures how long the
//! replicas take to agree again. That merge time is the sweep's main result.
//!
//! `docs/divergence-sweep.md` explains the experiment and defines the terms
//! used here (cell, grid, anchor, contested anchor, divergence phase, heal).
//!
//! # Phases
//!
//! 1. **Setup.** Reset every replica. For text workloads, create the shared
//!    text object on every replica. Wire the *complete* post-heal graph, then
//!    block every cross-group link. A blocked link stays open but carries no
//!    data.
//! 2. **Divergence.** Each group applies `ops_per_group` operations. The blocks
//!    keep the groups invisible to each other.
//! 3. **Heal.** Clear the blocks and start the sync handshake. The clock runs
//!    from the first unblock until every replica reports the same fingerprint.
//!
//! # Why the graph is wired before the clock starts
//!
//! Opening a sync stream costs a TCP connect and an HTTP/2 handshake. That cost
//! grows with the number of links opened. An earlier version opened the links
//! at heal time, so a `FullMesh` heal paid the cost on every cross-group pair
//! and a `Bridge` heal paid it once. Comparing the two compared connection
//! setup as much as merge cost. On the docker lane, setup was up to 70% of the
//! measured window. Setup now happens before the clock starts, so the heal is
//! only a set of flag flips and `convergence_ms` measures the sync protocol and
//! the CRDT merge.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::task::JoinSet;

use crate::runner::{
    NodeSource, TEXT_OBJ, acquire_nodes, check_tasks, connect_edges, ensure_text_all,
    intra_group_edges, kick_sync_edges, map_put, reset_all, set_links_blocked, target_node,
    text_splice, verify_groups_diverged, verify_text_length, wait_for_nodes,
};
use crate::topology::{
    Connections, Group, HealTopology, PartitionConfig, RunResult, SplitMix64, Workload,
};

/// Fixed starting value for every divergence-sweep seed.
///
/// Recorded as this constant so a cell's operation streams can be reproduced
/// later. [`seed_for`] mixes it with the cell parameters, the node index, and
/// the repetition number.
pub(crate) const DIVERGENCE_SEED_BASE: u64 = 0x5EED_D1F5_0FF5_E7A1;

/// Returns the PRNG seed for one replica in one repetition of one cell.
///
/// The seed is a function of the cell, the replica, and the repetition. Two
/// replicas therefore draw different operation streams, and the same three
/// inputs always replay the identical stream. That replay guarantee is what
/// lets different CRDT libraries be compared on the same workload.
///
/// The mixed value passes through one SplitMix64 round so that a change in any
/// single field changes the whole seed.
pub(crate) fn seed_for(config: &PartitionConfig, node: usize, repetition: usize) -> u64 {
    let mixed = DIVERGENCE_SEED_BASE
        ^ (config.ops_per_group as u64).wrapping_mul(0x1_0000_0001)
        ^ ((config.locality as u64) << 3)
        ^ ((node as u64) << 32)
        ^ (repetition as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    SplitMix64::new(mixed).next_u64()
}

/// Runs one partition-heal scenario and returns its measurements.
///
/// See the module documentation for the phase order and for why the graph is
/// wired before the measured window opens.
pub async fn run_partition_heal(
    config: &PartitionConfig,
    source: NodeSource,
    repetition: usize,
) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&clients).await?;

    // Create the shared text object on every replica while the document is
    // still empty and no links exist.
    //
    // This is what makes the heal interleave two sequences. The adapter creates
    // an object on first use. Without this step, replicas that diverge from an
    // empty document each create their own `ROOT["text"]`, and the heal then
    // resolves a map-key conflict that discards one side's text completely.
    if config.workload == Workload::TextSplice {
        ensure_text_all(&mut clients, 0..n).await?;
    }

    // Intra-group edges, plus the cross-group edges the heal will open.
    let intra: Vec<(usize, usize)> = config
        .groups
        .iter()
        .flat_map(|g| intra_group_edges(&g.nodes))
        .collect();
    let intra_set: HashSet<(usize, usize)> = intra.iter().copied().collect();
    let heal_edges = heal_edges(&config.heal_topology, &config.groups, n, &intra_set);

    // Block every cross-group link BEFORE wiring it, so the streams open
    // silently. `connect_to_peer`'s opening handshake is itself skipped on a
    // blocked link. Blocking is by peer ID and does not require the stream to
    // exist yet, which is what makes this order safe.
    set_links_blocked(&clients, &heal_edges, true).await?;

    // Wire the full post-heal topology. All of this is outside every timed
    // window; the heal below only flips flags.
    let mut all_edges = intra.clone();
    all_edges.extend(heal_edges.iter().copied());
    connect_edges(&clients, &endpoints, &all_edges).await?;
    check_tasks(&mut tasks)?;

    // Divergence phase: each group applies its operations independently.
    //
    // `MapPut` uses globally unique keys (`op_idx`). `TextSplice` draws each
    // position against the issuing replica's own simulated text length, using a
    // seeded PRNG per replica (see [`seed_for`]). Drawing against the replica's
    // own length is what keeps every position valid.
    //
    // The length counter is exact for singleton groups, which is what the
    // divergence-n2 family uses. For multi-node groups it can under-count,
    // because a replica also receives its peers' operations during the
    // partition. The drawn position stays within the real length either way, so
    // it remains a valid anchor.
    let mut op_idx = 0;
    // Per physical node: (seeded PRNG, simulated text length).
    let mut gen_state: HashMap<usize, (SplitMix64, usize)> = HashMap::new();
    for group in &config.groups {
        for i in 0..config.ops_per_group {
            let target = target_node(&config.write_pattern, i, &group.nodes);
            match config.workload {
                Workload::MapPut => {
                    map_put(
                        &mut clients[target],
                        &format!("k{op_idx}"),
                        &format!("v{op_idx}"),
                    )
                    .await?;
                }
                Workload::TextSplice => {
                    let state = gen_state.entry(target).or_insert_with(|| {
                        (SplitMix64::new(seed_for(config, target, repetition)), 0)
                    });
                    let pos = config.locality.draw_pos(&mut state.0, state.1);
                    state.1 += 1;
                    text_splice(&mut clients[target], TEXT_OBJ, pos, "x").await?;
                }
            }
            op_idx += 1;
        }
    }

    check_tasks(&mut tasks)?;

    // Wait for each group to reach internal consistency before healing.
    // The start instant is throwaway — this is a gate, not a measurement.
    for group in &config.groups {
        if group.nodes.len() > 1 {
            wait_for_nodes(
                &mut clients,
                &group.nodes,
                Instant::now(),
                Duration::from_secs(5),
            )
            .await?;
        }
    }
    check_tasks(&mut tasks)?;

    // Validity gate: the partition must actually have held. If any cross-group
    // state leaked during the divergence phase, the groups already agree and
    // the heal measures nothing. Checking here is cheaper and more direct than
    // inferring a leak downstream from a suspiciously fast convergence.
    verify_groups_diverged(&mut clients, &config.groups).await?;

    // Heal: clear the blocks on every healed link, then start the handshake.
    // Both steps are inside the measured window, because both are part of the
    // heal. Neither opens a connection, so neither scales the way opening N
    // streams did.
    //
    // Every unblock must finish before any kick is sent. A kick that reached a
    // still-blocked peer would be dropped on arrival, and the sender would then
    // wait for a reply that never comes.
    let heal_start = Instant::now();
    set_links_blocked(&clients, &heal_edges, false).await?;
    let wiring_ms = heal_start.elapsed().as_secs_f64() * 1000.0;
    kick_sync_edges(&clients, &heal_edges).await?;

    let all_nodes: Vec<usize> = (0..n).collect();
    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        heal_start,
        Duration::from_secs(10),
    )
    .await?;
    check_tasks(&mut tasks)?;

    // Validity gate, outside the measured window: the healed text must contain
    // both sides' inserts. Operations are insert-only and one character each,
    // so the final length must equal the total operation count. This is the
    // check that separates a real sequence interleave from a heal that
    // converged by discarding one replica's work.
    if config.workload == Workload::TextSplice {
        verify_text_length(
            &mut clients,
            &all_nodes,
            config.groups.len() * config.ops_per_group,
        )
        .await?;
    }

    // Report structural fields for the actual post-heal graph (intra-group
    // edges plus heal edges). `topology_kind = "partition_heal"` marks the
    // scenario shape, so an analysis can separate heal-driven convergence from
    // steady-state runs. `edge_count` and `diameter` let downstream plots tell
    // the FullMesh-heal and Bridge-heal variants apart.
    let final_edges = all_edges;
    let final_graph = Connections::Custom {
        edges: final_edges.clone(),
    };
    Ok(RunResult {
        convergence_ms,
        wiring_ms,
        total_ops: config.groups.len() * config.ops_per_group,
        topology_kind: "partition_heal",
        edge_count: final_edges.len(),
        diameter: final_graph.diameter(n),
    })
}

/// Returns the cross-group edges to unblock at heal time.
///
/// The caller owns the intra-group edges, which are already wired during the
/// partition phase. This function returns only the edges added at the heal.
///
/// * `FullMesh` — every pair `(i, j)` with `i < j` that is not already in
///   `intra_set`.
/// * `Bridge` — exactly one edge between `groups[0].nodes[0]` and
///   `groups[1].nodes[0]`, ordered `(min, max)`. Assumes two groups, which
///   [`PartitionConfig::validate`] enforces.
fn heal_edges(
    heal: &HealTopology,
    groups: &[Group],
    n: usize,
    intra_set: &HashSet<(usize, usize)>,
) -> Vec<(usize, usize)> {
    match heal {
        HealTopology::FullMesh => Connections::FullMesh
            .edges(n)
            .into_iter()
            .filter(|e| !intra_set.contains(e))
            .collect(),
        HealTopology::Bridge => {
            let a = groups[0].nodes[0];
            let b = groups[1].nodes[0];
            vec![if a < b { (a, b) } else { (b, a) }]
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use replica::adapter::Crdt;

    // ── heal_edges ─────────────────────────────────────────────────────────

    fn intra_for(groups: &[Group]) -> HashSet<(usize, usize)> {
        groups
            .iter()
            .flat_map(|g| intra_group_edges(&g.nodes))
            .collect()
    }

    fn two_groups(a: Vec<usize>, b: Vec<usize>) -> Vec<Group> {
        vec![Group { nodes: a }, Group { nodes: b }]
    }

    #[test]
    fn heal_edges_full_mesh_returns_only_cross_group_pairs() {
        // n=4, groups [0,1] & [2,3]. Intra: (0,1),(2,3). Cross: (0,2),(0,3),(1,2),(1,3).
        let groups = two_groups(vec![0, 1], vec![2, 3]);
        let intra = intra_for(&groups);
        let mut edges = heal_edges(&HealTopology::FullMesh, &groups, 4, &intra);
        edges.sort_unstable();
        assert_eq!(edges, vec![(0, 2), (0, 3), (1, 2), (1, 3)]);
        // Sanity: union of intra + heal == full-mesh edges (i.e., no overlap and complete cover).
        let mut all: Vec<_> = intra.iter().copied().chain(edges.iter().copied()).collect();
        all.sort_unstable();
        assert_eq!(all, Connections::FullMesh.edges(4));
    }

    #[test]
    fn heal_edges_bridge_is_single_edge_between_group_zeros() {
        let groups = two_groups(vec![0, 1], vec![2, 3]);
        let intra = intra_for(&groups);
        let edges = heal_edges(&HealTopology::Bridge, &groups, 4, &intra);
        assert_eq!(edges, vec![(0, 2)]);
    }

    #[test]
    fn heal_edges_bridge_normalizes_edge_ordering() {
        // Reverse group ordering: groups[0].nodes[0]=2, groups[1].nodes[0]=0.
        // The bridge edge must still be (min, max) = (0, 2).
        let groups = two_groups(vec![2, 3], vec![0, 1]);
        let intra = intra_for(&groups);
        let edges = heal_edges(&HealTopology::Bridge, &groups, 4, &intra);
        assert_eq!(edges, vec![(0, 2)]);
    }

    #[test]
    fn heal_edges_bridge_picks_first_node_of_each_group() {
        // groups[0].nodes[0]=1 (not the minimum 5 in group 0!), groups[1].nodes[0]=3.
        let groups = two_groups(vec![1, 5, 7], vec![3, 0, 2]);
        let intra = intra_for(&groups);
        let edges = heal_edges(&HealTopology::Bridge, &groups, 8, &intra);
        assert_eq!(edges, vec![(1, 3)]);
    }

    /// Post-heal graph for Bridge is two cliques joined by one edge — diameter
    /// is `1 (intra-group) + 1 (bridge) + 1 (intra-group) = 3` for full-mesh
    /// groups of size ≥ 2. The `Connections::Custom` diameter pass that the
    /// runner uses must agree with this hand computation.
    #[test]
    fn bridge_post_heal_diameter_two_full_mesh_groups_is_three() {
        let groups = two_groups(vec![0, 1], vec![2, 3]);
        let intra: Vec<_> = groups
            .iter()
            .flat_map(|g| intra_group_edges(&g.nodes))
            .collect();
        let intra_set: HashSet<_> = intra.iter().copied().collect();
        let heal = heal_edges(&HealTopology::Bridge, &groups, 4, &intra_set);

        let mut all = intra;
        all.extend(heal.iter().copied());
        let graph = Connections::Custom { edges: all };
        assert_eq!(graph.diameter(4), 3);
    }

    #[test]
    fn bridge_post_heal_diameter_grows_with_group_size() {
        // n=8, two cliques of size 4. Diameter from any far-node-in-g0 to
        // any far-node-in-g1 is 1 + 1 + 1 = 3 (clique → bridge → clique).
        let groups = two_groups(vec![0, 1, 2, 3], vec![4, 5, 6, 7]);
        let intra: Vec<_> = groups
            .iter()
            .flat_map(|g| intra_group_edges(&g.nodes))
            .collect();
        let intra_set: HashSet<_> = intra.iter().copied().collect();
        let heal = heal_edges(&HealTopology::Bridge, &groups, 8, &intra_set);
        let mut all = intra;
        all.extend(heal.iter().copied());
        assert_eq!(Connections::Custom { edges: all }.diameter(8), 3);
    }

    #[test]
    fn bridge_post_heal_edge_count_matches_intra_plus_one() {
        // n=6, groups of size 3. Intra edges per group = 3 (clique on 3) → 6 total.
        // Bridge adds 1. Final edge count = 7.
        let groups = two_groups(vec![0, 1, 2], vec![3, 4, 5]);
        let intra: Vec<_> = groups
            .iter()
            .flat_map(|g| intra_group_edges(&g.nodes))
            .collect();
        assert_eq!(intra.len(), 6);
        let intra_set: HashSet<_> = intra.iter().copied().collect();
        let heal = heal_edges(&HealTopology::Bridge, &groups, 6, &intra_set);
        assert_eq!(intra.len() + heal.len(), 7);
    }

    // ── seed_for (divergence generator) ────────────────────────────────────

    fn text_cfg(ops: usize, locality: &str) -> PartitionConfig {
        toml::from_str(&format!(
            "node_count = 2\n\
             write_pattern = \"concentrated\"\n\
             workload = \"text_splice\"\n\
             locality = \"{locality}\"\n\
             ops_per_group = {ops}\n\
             groups = [{{ nodes = [0] }}, {{ nodes = [1] }}]"
        ))
        .expect("valid partition config")
    }

    #[test]
    fn seed_for_is_deterministic() {
        let c = text_cfg(100, "random_position");
        assert_eq!(seed_for(&c, 0, 1), seed_for(&c, 0, 1));
    }

    #[test]
    fn seed_for_varies_by_replica_repetition_and_cell() {
        let c = text_cfg(100, "random_position");
        let base = seed_for(&c, 0, 1);
        assert_ne!(base, seed_for(&c, 1, 1), "distinct replica must reseed");
        assert_ne!(base, seed_for(&c, 0, 2), "distinct repetition must reseed");
        assert_ne!(
            base,
            seed_for(&text_cfg(1000, "random_position"), 0, 1),
            "distinct ops-per-side must reseed"
        );
        assert_ne!(
            base,
            seed_for(&text_cfg(100, "same_region"), 0, 1),
            "distinct locality must reseed"
        );
    }

    // ── end-to-end cell ────────────────────────────────────────────────────

    /// Runs a whole small cell through the real gRPC stack: partition into
    /// singleton groups, apply seeded text splices, heal, converge.
    ///
    /// The text-length gate inside `run_partition_heal` must pass. That proves
    /// the heal interleaved both sides' sequences instead of discarding one.
    /// All three localities run, because `same_region` is the shape that
    /// originally exposed the shared-object bug.
    #[tokio::test]
    async fn divergence_heal_preserves_both_sides_text() {
        for locality in ["append", "random_position", "same_region"] {
            let config = text_cfg(25, locality);
            let result = run_partition_heal(&config, NodeSource::InProcess(Crdt::Automerge), 1)
                .await
                .unwrap_or_else(|e| panic!("locality={locality}: {e:#}"));
            assert_eq!(result.total_ops, 50, "locality={locality}");
        }
    }
}
