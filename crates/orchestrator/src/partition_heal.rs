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
//! 1. **Setup.** Reset every replica. If the cell has a baseline, load it on every
//!    replica. For text workloads, create the shared text object on every
//!    replica. Wire the *complete* post-heal graph with every link open, wait
//!    until the sync on every link is idle, then block every cross-group link. A
//!    blocked link stays open but carries no data.
//! 2. **Divergence.** Each group applies `ops_per_group` operations. The blocks
//!    keep the groups invisible to each other.
//! 3. **Heal.** Clear the blocks and kick the sync. An unblock is a reconnect:
//!    each replica keeps only the sync state that its library keeps across a
//!    disconnect (`CrdtAdapter::sync_reconnect`). Automerge keeps the heads that
//!    both sides have, so it sends only what changed. Yrs and Loro exchange
//!    versions again. The clock runs from the first unblock until every replica
//!    reports the same fingerprint.
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

use anyhow::{Context, Result, bail};
use tokio::task::JoinSet;

use crate::baseline::BaselineStore;
use crate::event_log::{Event, EventLog};
use crate::replicas::{
    Client, NodeSource, TEXT_OBJ, acquire_nodes, apply_write, check_tasks, connect_edges,
    ensure_text_all, fingerprint, kick_sync_edges, record_stats, reset_all, set_links_blocked,
    text_splice, verify_text_length, wait_for_nodes, wait_for_sync_idle,
};
use crate::runner::RunResult;
use crate::scenario::{Group, HealTopology, PartitionConfig, WriteKind};
use crate::topology::{self, Connections, intra_group_edges};
use crate::workload::{PositionStream, target_node};

/// The longest time that a trial waits for the sync before the partition.
const SYNC_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// The longest heal that a trial waits for before it fails.
///
/// This only detects a heal that never ends. It does not change a measurement.
/// Some cells take many seconds: on `same_region` at 10^4 operations per side,
/// some Loro peer ids make a heal take more than 10 s on the docker lane, and a
/// cell at 10^5 takes longer.
const HEAL_TIMEOUT: Duration = Duration::from_secs(600);

/// Runs one partition-heal scenario and returns its measurements.
///
/// See the module documentation for the phase order and for why the graph is
/// wired before the measured window opens.
pub async fn run_partition_heal(
    config: &PartitionConfig,
    source: NodeSource,
    repetition: usize,
    baselines: &mut BaselineStore,
    log: &mut EventLog,
) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&clients).await?;

    if let Some(baseline) = &config.baseline {
        baselines.install(baseline, &mut clients).await?;
    }

    // Create the shared text object on every replica while no links exist, and,
    // for a cell with no baseline, while the document is still empty. A replica
    // that loaded a baseline has the object already, so this does not change it.
    //
    // This is what makes the heal interleave two sequences. The adapter creates
    // an object on first use. Without this step, replicas that diverge from an
    // empty document each create their own `ROOT["text"]`, and the heal then
    // resolves a map-key conflict that discards one side's text completely.
    if config.workload == WriteKind::TextSplice {
        ensure_text_all(&mut clients, 0..n).await?;
    }

    let plan = plan_edges(config);

    // Wire the full post-heal topology, with every link open, and let each link
    // sync. The documents are equal here, so this exchanges only versions. The
    // replicas then know each other, as peers do that were in contact before they
    // went offline, and the heal sends only what changed. All of this is outside
    // every timed window; the heal below only flips flags.
    let all_edges = plan.all_edges();
    connect_edges(&clients, &endpoints, &all_edges).await?;
    wait_for_sync_idle(&mut clients, &all_edges, SYNC_IDLE_TIMEOUT).await?;

    // Block the cross-group links only now that their sync is idle. Then every
    // side knows what the other has, and the state that a reconnect keeps is
    // the same in every cell. The links stay open.
    set_links_blocked(&clients, &plan.heal, true).await?;
    check_tasks(&mut tasks)?;
    log.record(Event::Wired {
        edges: all_edges.clone(),
        blocked: plan.heal.clone(),
    });

    // Divergence phase: each group applies its operations independently.
    //
    // `MapPut` uses globally unique keys (`op_idx`). `TextSplice` takes each
    // position from the issuing replica's own seeded stream, created on its
    // first write. See [`PositionStream`] for why every position is valid.
    let mut op_idx = 0;
    let mut streams: HashMap<usize, PositionStream> = HashMap::new();
    for group in &config.groups {
        for i in 0..config.ops_per_group {
            let target = target_node(&config.write_pattern, i, &group.nodes);
            match config.workload {
                WriteKind::MapPut => {
                    apply_write(&mut clients[target], WriteKind::MapPut, op_idx).await?;
                }
                WriteKind::TextSplice => {
                    let pos = streams
                        .entry(target)
                        .or_insert_with(|| PositionStream::new(config, target, repetition))
                        .next_pos();
                    text_splice(&mut clients[target], TEXT_OBJ, pos, "x").await?;
                }
            }
            log.record(Event::Write {
                node: target,
                op: op_idx,
            });
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
                log,
            )
            .await?;
        }
    }
    check_tasks(&mut tasks)?;

    // Equal fingerprints in a group do not mean that its replicas have stopped
    // sending. A replica can still relay a received change to its other peers.
    // If such a relay reached a cross-group link after one end was unblocked and
    // before the other end was, the blocked end would drop it, and the sender
    // would count it as sent. Then the kick can send nothing, and the heal can
    // stall. So wait, outside the measured window, until sync inside the groups
    // is idle. The idle check ignores the blocked peers.
    if !plan.intra.is_empty() {
        wait_for_sync_idle(&mut clients, &plan.intra, SYNC_IDLE_TIMEOUT).await?;
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
    log.record(Event::ClockStart);
    set_links_blocked(&clients, &plan.heal, false).await?;
    let wiring_ms = heal_start.elapsed().as_secs_f64() * 1000.0;
    log.record(Event::Unblocked);
    kick_sync_edges(&clients, &plan.heal).await?;
    log.record(Event::Kicked);

    let all_nodes: Vec<usize> = (0..n).collect();
    let convergence_ms =
        wait_for_nodes(&mut clients, &all_nodes, heal_start, HEAL_TIMEOUT, log).await?;
    check_tasks(&mut tasks)?;
    record_stats(&mut clients, log).await?;

    // Validity gate, outside the measured window: the healed text must contain
    // both sides' inserts. Operations are insert-only and one character each,
    // so the final length must equal the starting length plus the total operation
    // count. This is the
    // check that separates a real sequence interleave from a heal that
    // converged by discarding one replica's work. A baseline adds its length.
    if config.workload == WriteKind::TextSplice {
        let expected = config.baseline_len() + config.total_ops();
        verify_text_length(&mut clients, &all_nodes, expected).await?;
    }

    // Report structural fields for the actual post-heal graph (intra-group
    // edges plus heal edges). `topology_kind = "partition_heal"` marks the
    // scenario shape, so an analysis can separate heal-driven convergence from
    // steady-state runs. `edge_count` and `diameter` let downstream plots tell
    // the FullMesh-heal and Bridge-heal variants apart.
    Ok(RunResult {
        convergence_ms,
        wiring_ms,
        total_ops: config.total_ops(),
        topology_kind: "partition_heal",
        edge_count: all_edges.len(),
        diameter: topology::diameter(n, &all_edges),
    })
}

/// The links of a partition-heal run.
struct EdgePlan {
    /// The full mesh inside each group. These links carry data during the
    /// partition.
    intra: Vec<(usize, usize)>,
    /// The cross-group links. They are blocked during the partition, and the
    /// heal unblocks them.
    heal: Vec<(usize, usize)>,
}

impl EdgePlan {
    /// Every link of the post-heal graph: the intra-group links, then the heal
    /// links.
    fn all_edges(&self) -> Vec<(usize, usize)> {
        self.intra.iter().chain(&self.heal).copied().collect()
    }
}

/// Plans the intra-group and heal links for `config`.
fn plan_edges(config: &PartitionConfig) -> EdgePlan {
    let intra: Vec<(usize, usize)> = config
        .groups
        .iter()
        .flat_map(|g| intra_group_edges(&g.nodes))
        .collect();
    let intra_set: HashSet<(usize, usize)> = intra.iter().copied().collect();
    let heal = heal_edges(
        &config.heal_topology,
        &config.groups,
        config.node_count,
        &intra_set,
    );
    EdgePlan { intra, heal }
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

/// Checks that the partition held. No two groups may share a fingerprint at the
/// end of the divergence phase.
///
/// The graph is fully wired during the partition, and only the blocked-link
/// flags keep the groups apart. If that enforcement fails, the groups converge
/// early and the run becomes "already agreed, then heal". It reports a fast
/// heal, which reads as a result rather than as a broken experiment.
///
/// A group that wrote nothing is skipped. Empty groups match each other, and
/// that is not a leak.
async fn verify_groups_diverged(clients: &mut [Client], groups: &[Group]) -> Result<()> {
    let mut seen: Vec<(usize, Vec<u8>)> = Vec::with_capacity(groups.len());
    for (gi, group) in groups.iter().enumerate() {
        let node = group.nodes[0];
        let fp = fingerprint(&mut clients[node])
            .await
            .with_context(|| format!("GetStateFingerprint on node {node}"))?;
        if fp.is_empty() {
            continue;
        }
        if let Some((other, _)) = seen.iter().find(|(_, other_fp)| *other_fp == fp) {
            bail!(
                "partition leaked: groups {other} and {gi} already agree before the heal — \
                 the divergence phase measured nothing"
            );
        }
        seen.push((gi, fp));
    }
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::{Path, PathBuf};

    use replica::adapter::Crdt;

    use crate::replicas::{map_put, spawn_nodes};
    use crate::scenario::{Baseline, WritePattern};
    use crate::workload::Locality;

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

    /// A two-group config with a `Bridge` heal.
    fn bridge_cell(a: &[usize], b: &[usize]) -> PartitionConfig {
        PartitionConfig {
            heal_topology: HealTopology::Bridge,
            ..PartitionConfig::for_test(a.len() + b.len(), &[a, b])
        }
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
    /// groups of size ≥ 2. The diameter the runner reports must agree with
    /// this hand computation.
    #[test]
    fn bridge_post_heal_diameter_two_full_mesh_groups_is_three() {
        let plan = plan_edges(&bridge_cell(&[0, 1], &[2, 3]));
        assert_eq!(topology::diameter(4, &plan.all_edges()), 3);
    }

    #[test]
    fn bridge_post_heal_diameter_grows_with_group_size() {
        // n=8, two cliques of size 4. Diameter from any far-node-in-g0 to
        // any far-node-in-g1 is 1 + 1 + 1 = 3 (clique → bridge → clique).
        let plan = plan_edges(&bridge_cell(&[0, 1, 2, 3], &[4, 5, 6, 7]));
        assert_eq!(topology::diameter(8, &plan.all_edges()), 3);
    }

    #[test]
    fn bridge_post_heal_edge_count_matches_intra_plus_one() {
        // n=6, groups of size 3. Intra edges per group = 3 (clique on 3) → 6 total.
        // Bridge adds 1. Final edge count = 7.
        let plan = plan_edges(&bridge_cell(&[0, 1, 2], &[3, 4, 5]));
        assert_eq!(plan.intra.len(), 6);
        assert_eq!(plan.all_edges().len(), 7);
    }

    // ── Leak gate ──────────────────────────────────────────────────────────

    /// The leak gate fires when the groups already agree. Two nodes that were
    /// never partitioned stand in for a leaked partition.
    #[tokio::test]
    async fn diverged_gate_rejects_groups_that_already_agree() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        connect_edges(&clients, &endpoints, &[(0, 1)])
            .await
            .unwrap();
        map_put(&mut clients[0], "k", "v").await.unwrap();
        wait_for_nodes(
            &mut clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(5),
            &mut EventLog::off(),
        )
        .await
        .unwrap();

        let groups = two_groups(vec![0], vec![1]);
        let err = verify_groups_diverged(&mut clients, &groups)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("partition leaked"), "{err}");
    }

    /// A sync message larger than gRPC's default 4 MiB limit still arrives. An
    /// Automerge heal of 10^5 ops per side on the B4 paper sends about 13 MB.
    #[tokio::test]
    async fn a_sync_message_above_4_mib_arrives() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        connect_edges(&clients, &endpoints, &[(0, 1)])
            .await
            .unwrap();
        // Text that does not compress, so the sync message stays above 4 MiB.
        let mut rng = common::SplitMix64::new(7);
        let big: String = (0..8 << 20)
            .map(|_| char::from(b'!' + (rng.next_u64() % 94) as u8))
            .collect();
        map_put(&mut clients[0], "big", &big).await.unwrap();
        wait_for_nodes(
            &mut clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(30),
            &mut EventLog::off(),
        )
        .await
        .unwrap();
    }

    /// The leak gate passes when the groups really do differ.
    #[tokio::test]
    async fn diverged_gate_accepts_genuinely_divergent_groups() {
        let mut tasks = JoinSet::new();
        let (_endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        map_put(&mut clients[0], "a", "1").await.unwrap();
        map_put(&mut clients[1], "b", "2").await.unwrap();

        let groups = two_groups(vec![0], vec![1]);
        assert!(verify_groups_diverged(&mut clients, &groups).await.is_ok());
    }

    // ── wiring_ms ──────────────────────────────────────────────────────────

    /// Two equal halves of `n` nodes, `per_group` operations each.
    fn halves(n: usize, per_group: usize) -> PartitionConfig {
        let a: Vec<usize> = (0..n / 2).collect();
        let b: Vec<usize> = (n / 2..n).collect();
        PartitionConfig {
            ops_per_group: per_group,
            ..PartitionConfig::for_test(n, &[&a, &b])
        }
    }

    /// `wiring_ms` must not grow with the number of healed links.
    ///
    /// A full-mesh heal on n=6 reopens 9 cross-group links. Because the graph
    /// is wired up front, the measured cost is only the unblock round.
    #[tokio::test]
    async fn full_mesh_heal_wiring_does_not_scale_with_edge_count() {
        let small = run_partition_heal(
            &halves(2, 4),
            NodeSource::InProcess(Crdt::Automerge),
            1,
            &mut BaselineStore::default(),
            &mut EventLog::off(),
        )
        .await
        .unwrap();
        let large = run_partition_heal(
            &halves(6, 4),
            NodeSource::InProcess(Crdt::Automerge),
            1,
            &mut BaselineStore::default(),
            &mut EventLog::off(),
        )
        .await
        .unwrap();

        // n=6 full-mesh heal opens 9 cross-group links vs n=2's 1. Wiring is
        // now two concurrent RPC rounds either way, so allow a wide factor
        // and still catch a return to per-edge connection setup.
        assert!(
            large.wiring_ms < small.wiring_ms.max(1.0) * 10.0,
            "heal wiring scaled with edge count: n2={:.3}ms n6={:.3}ms",
            small.wiring_ms,
            large.wiring_ms
        );
    }

    /// The heal's unblock round runs inside the measured window and is reported
    /// as `wiring_ms`. It no longer contains connection setup, but it is still
    /// a real part of the window: `wiring_ms <= convergence_ms` must hold, or
    /// an analysis that subtracts it would report a negative merge time.
    #[tokio::test]
    async fn partition_heal_reports_wiring_within_the_convergence_window() {
        let result = run_partition_heal(
            &halves(4, 4),
            NodeSource::InProcess(Crdt::Automerge),
            1,
            &mut BaselineStore::default(),
            &mut EventLog::off(),
        )
        .await
        .unwrap();
        assert!(result.wiring_ms > 0.0, "heal wiring is inside the window");
        assert!(
            result.wiring_ms <= result.convergence_ms,
            "wiring {} exceeds the window it is measured inside ({})",
            result.wiring_ms,
            result.convergence_ms
        );
    }

    // ── End-to-end cell ────────────────────────────────────────────────────

    /// Runs a whole small cell through the real gRPC stack: partition into
    /// singleton groups, apply seeded text splices, heal, converge.
    ///
    /// The text-length gate inside `run_partition_heal` must pass. That proves
    /// the heal interleaved both sides' sequences instead of discarding one.
    /// All three localities run, because `same_region` is the shape that
    /// originally exposed the shared-object bug.
    #[tokio::test]
    async fn divergence_heal_preserves_both_sides_text() {
        for locality in [
            Locality::Append,
            Locality::RandomPosition,
            Locality::SameRegion,
        ] {
            let config = PartitionConfig {
                ops_per_group: 25,
                write_pattern: WritePattern::Concentrated,
                workload: WriteKind::TextSplice,
                locality,
                ..PartitionConfig::for_test(2, &[&[0], &[1]])
            };
            let result = run_partition_heal(
                &config,
                NodeSource::InProcess(Crdt::Automerge),
                1,
                &mut BaselineStore::default(),
                &mut EventLog::off(),
            )
            .await
            .unwrap_or_else(|e| panic!("locality={locality:?}: {e:#}"));
            assert_eq!(result.total_ops, 50, "locality={locality:?}");
        }
    }

    // ── Baseline ───────────────────────────────────────────────────────────

    /// A cell whose start is the committed smoke fixture, whose text is `hippo`.
    fn baseline_cell(sha256: &str, length: usize) -> PartitionConfig {
        PartitionConfig {
            ops_per_group: 25,
            write_pattern: WritePattern::Concentrated,
            workload: WriteKind::TextSplice,
            locality: Locality::RandomPosition,
            baseline: Some(Baseline {
                trace: "smoke-text.trace.json".into(),
                sha256: sha256.to_owned(),
                length,
            }),
            ..PartitionConfig::for_test(2, &[&[0], &[1]])
        }
    }

    /// The directory that holds the smoke fixture.
    fn fixtures() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../workloads/fixtures")
    }

    /// Runs one trial of `config` on `crdt`, with a new store.
    async fn run_on(config: &PartitionConfig, crdt: Crdt, trial: usize) -> Result<RunResult> {
        let mut store = BaselineStore::new(&fixtures());
        run_partition_heal(
            config,
            NodeSource::InProcess(crdt),
            trial,
            &mut store,
            &mut EventLog::off(),
        )
        .await
    }

    /// The hash of the smoke fixture, as `workloads/smoke.toml` pins it.
    const FIXTURE_SHA256: &str = "d49ad91a6c882150cf0d7b020beaf3228b72bb72547da7e299fa355bd028a81e";

    /// Each library merges inserts that attach to shared elements, and keeps the
    /// baseline and both sides. The final gate in `run_partition_heal` fails if a
    /// replica loses text. It also fails if two replicas share an author id,
    /// because their ops then collide.
    #[tokio::test]
    async fn a_baseline_cell_keeps_the_start_and_both_sides_on_each_library() {
        for crdt in Crdt::ALL {
            let config = baseline_cell(FIXTURE_SHA256, 5);
            for trial in 1..=3 {
                let result = run_on(&config, crdt, trial)
                    .await
                    .unwrap_or_else(|e| panic!("{crdt} trial {trial}: {e:#}"));
                assert_eq!(result.total_ops, 50, "{crdt}");
            }
        }
    }

    /// Later trials use the encoded bytes, and do not replay the trace again.
    #[tokio::test]
    async fn one_store_serves_many_trials_on_each_library() {
        for crdt in Crdt::ALL {
            let config = baseline_cell(FIXTURE_SHA256, 5);
            let mut store = BaselineStore::new(&fixtures());
            for trial in 1..=3 {
                run_partition_heal(
                    &config,
                    NodeSource::InProcess(crdt),
                    trial,
                    &mut store,
                    &mut EventLog::off(),
                )
                .await
                .unwrap_or_else(|e| panic!("{crdt} trial {trial}: {e:#}"));
            }
        }
    }

    #[tokio::test]
    async fn a_wrong_baseline_length_fails_before_the_partition() {
        let err = run_on(&baseline_cell(FIXTURE_SHA256, 6), Crdt::Yrs, 1)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("expected 6 chars"), "{err:#}");
    }

    #[test]
    fn a_wrong_baseline_hash_fails_the_check() {
        let mut store = BaselineStore::new(&fixtures());
        let config = baseline_cell(&"0".repeat(64), 5);
        assert!(store.check(config.baseline.as_ref().unwrap()).is_err());
    }

    #[test]
    fn a_missing_baseline_file_names_the_recipe() {
        let mut store = BaselineStore::new(Path::new("no-such-directory"));
        let config = baseline_cell(FIXTURE_SHA256, 5);
        let err = store.check(config.baseline.as_ref().unwrap()).unwrap_err();
        assert!(format!("{err:#}").contains("just gen-traces"), "{err:#}");
    }
}
