use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::{Channel, Server};

use common::proto::{
    Empty, MapPut, OpRequest, PeerRef, ScalarValue, TextSplice, op_request,
    replica_client::ReplicaClient, replica_server::ReplicaServer, scalar_value,
    sync_server::SyncServer,
};
use replica::adapter::AutomergeAdapter;
use replica::server::{ReplicaService, ReplicaState, SyncService};

use crate::topology::{
    Connections, Group, HealTopology, PartitionConfig, RunResult, TopologyConfig, Workload,
    WritePattern,
};

// ── Replica endpoints ──────────────────────────────────────────────────────

/// Network addresses for a single replica.
///
/// Two addresses are kept because the orchestrator and the replicas may sit
/// in different name spaces. On host, they coincide (`127.0.0.1:<port>`).
/// In docker-compose, `client_addr` is `localhost:<host-port>` (orchestrator
/// reaches the replica via the published port) and `peer_addr` is
/// `replica-N:50051` (containers resolve each other via service DNS).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    /// `host:port` the orchestrator uses to dial this replica.
    pub client_addr: String,
    /// `host:port` other replicas use to dial this replica (passed in
    /// `PeerRef.addr` during `ConnectPeer`).
    pub peer_addr: String,
}

impl ReplicaEndpoint {
    /// Endpoint where the orchestrator and peers reach the replica at the
    /// same address — the in-process case.
    pub fn loopback(addr: impl Into<String>) -> Self {
        let s = addr.into();
        Self {
            client_addr: s.clone(),
            peer_addr: s,
        }
    }
}

/// Where the scenario gets its replicas from.
pub enum NodeSource {
    /// Spawn `n` replicas inside this process on ephemeral ports.
    InProcess,
    /// Connect to replicas already running at these endpoints.
    External(Vec<ReplicaEndpoint>),
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Bind a port, start a replica server, and return its endpoint and gRPC client.
///
/// The server task is added to `tasks` so the caller can detect panics; the
/// `JoinSet` must outlive all client usage or the server will be dropped.
async fn spawn_node(
    actor_id: String,
    tasks: &mut JoinSet<()>,
) -> Result<(ReplicaEndpoint, ReplicaClient<Channel>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = ReplicaState::new(actor_id, AutomergeAdapter::new());
    tasks.spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
            .add_service(SyncServer::new(SyncService::new(state)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
        {
            tracing::error!("replica server exited with error: {e:#}");
        }
    });
    let client = ReplicaClient::connect(format!("http://{addr}")).await?;
    Ok((ReplicaEndpoint::loopback(addr.to_string()), client))
}

/// Spawn `n` nodes and return their endpoints and clients.
async fn spawn_nodes(
    n: usize,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<ReplicaClient<Channel>>)> {
    let mut endpoints = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);
    for i in 0..n {
        let (ep, client) = spawn_node(format!("node-{i}"), tasks).await?;
        endpoints.push(ep);
        clients.push(client);
    }
    Ok((endpoints, clients))
}

/// Connect to externally-managed replicas at the supplied endpoints.
///
/// Each endpoint is dialled via `client_addr`; before returning, every replica
/// must answer a `GetStateFingerprint` RPC so we fail fast if the container
/// hasn't started yet (rather than during the first `ConnectPeer`).
async fn connect_external(endpoints: &[ReplicaEndpoint]) -> Result<Vec<ReplicaClient<Channel>>> {
    let mut clients = Vec::with_capacity(endpoints.len());
    for ep in endpoints {
        let mut client = ReplicaClient::connect(format!("http://{}", ep.client_addr))
            .await
            .with_context(|| format!("connecting to external replica at {}", ep.client_addr))?;
        client
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .with_context(|| format!("smoke fingerprint RPC failed for {}", ep.client_addr))?;
        clients.push(client);
    }
    Ok(clients)
}

/// Acquire replicas from the configured source.
///
/// For `InProcess`, spawns `n` replicas on ephemeral ports; for `External`,
/// validates that the endpoint count matches `n` and connects to each. The
/// returned `JoinSet` is non-empty only for the in-process case — `External`
/// runs leave it empty, so subsequent `check_tasks` calls degrade to no-ops.
async fn acquire_nodes(
    source: NodeSource,
    n: usize,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<ReplicaClient<Channel>>)> {
    match source {
        NodeSource::InProcess => spawn_nodes(n, tasks).await,
        NodeSource::External(endpoints) => {
            if endpoints.len() != n {
                bail!(
                    "scenario needs {n} replicas but --replicas supplied {}",
                    endpoints.len()
                );
            }
            let clients = connect_external(&endpoints).await?;
            Ok((endpoints, clients))
        }
    }
}

/// Reset every replica back to an empty document with no peer connections.
///
/// Issued in parallel so the per-trial overhead is one round-trip's worth of
/// latency rather than `n × round-trip`. For in-process replicas this is
/// effectively a no-op; for external (docker/k8s) replicas it replaces what
/// used to require a full container bounce between trials.
async fn reset_all(clients: &mut [ReplicaClient<Channel>]) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for client in clients.iter() {
        let mut c = client.clone();
        set.spawn(async move {
            c.reset(Request::new(Empty {})).await?;
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.context("reset task panicked")??;
    }
    Ok(())
}

/// Call `ConnectPeer` for each edge and wait for all streams to be ready.
///
/// Each `ConnectPeer` RPC blocks until the TCP connection and gRPC stream are
/// open and the peer is registered, so no post-connect sleep is needed. The
/// address passed in `PeerRef.addr` is `peer_addr`, which is what the *target*
/// replica `i` will dial to reach replica `j` (may differ from the
/// orchestrator's `client_addr` for the same replica — see [`ReplicaEndpoint`]).
async fn connect_edges(
    clients: &mut [ReplicaClient<Channel>],
    endpoints: &[ReplicaEndpoint],
    edges: &[(usize, usize)],
) -> Result<()> {
    for &(i, j) in edges {
        clients[i]
            .connect_peer(Request::new(PeerRef {
                peer_id: format!("node-{j}"),
                addr: endpoints[j].peer_addr.clone(),
            }))
            .await?;
    }
    Ok(())
}

/// Apply a single `MapPut` on the root map.
async fn map_put(client: &mut ReplicaClient<Channel>, key: &str, val: &str) -> Result<()> {
    client
        .apply_op(Request::new(OpRequest {
            op: Some(op_request::Op::MapPut(MapPut {
                obj: String::new(),
                key: key.to_owned(),
                value: Some(ScalarValue {
                    value: Some(scalar_value::Value::StrVal(val.to_owned())),
                }),
            })),
        }))
        .await?;
    Ok(())
}

/// Apply a single `TextSplice` on the named text object under root.
///
/// `pos = 0` (prepend) is always a valid splice position regardless of the
/// replica's current text length, so it stays valid under any write pattern
/// without the orchestrator tracking per-node document length. Phase 1's
/// generator replaces this fixed anchor with the swept locality rule.
async fn text_splice(
    client: &mut ReplicaClient<Channel>,
    obj: &str,
    pos: usize,
    insert: &str,
) -> Result<()> {
    client
        .apply_op(Request::new(OpRequest {
            op: Some(op_request::Op::TextSplice(TextSplice {
                obj: obj.to_owned(),
                pos: pos as u64,
                del_count: 0,
                insert: insert.to_owned(),
            })),
        }))
        .await?;
    Ok(())
}

/// Apply write number `seq` to `client` under the configured `workload`.
///
/// `seq` disambiguates `MapPut` keys; the text workload ignores it (each op is
/// a fixed-anchor prepend of one filler character — content is merge-cost
/// irrelevant, only position and op shape matter).
async fn apply_write(
    client: &mut ReplicaClient<Channel>,
    workload: Workload,
    seq: usize,
) -> Result<()> {
    match workload {
        Workload::MapPut => map_put(client, &format!("k{seq}"), &format!("v{seq}")).await,
        Workload::TextSplice => text_splice(client, "text", 0, "x").await,
    }
}

/// Poll nodes at `indices` until all have equal, non-empty fingerprints.
///
/// Returns fractional ms from `start` to convergence.
async fn wait_for_nodes(
    clients: &mut [ReplicaClient<Channel>],
    indices: &[usize],
    start: Instant,
    timeout: Duration,
) -> Result<f64> {
    loop {
        let mut fps: Vec<Vec<u8>> = Vec::with_capacity(indices.len());
        for &i in indices {
            fps.push(
                clients[i]
                    .get_state_fingerprint(Request::new(Empty {}))
                    .await?
                    .into_inner()
                    .fingerprint,
            );
        }

        // Empty fingerprint means the node has received no ops yet; treat as
        // not converged to avoid a spurious match before any writes land.
        let converged = fps.iter().all(|fp| !fp.is_empty()) && fps.windows(2).all(|w| w[0] == w[1]);
        if converged {
            return Ok(start.elapsed().as_secs_f64() * 1000.0);
        }

        if start.elapsed() >= timeout {
            bail!(
                "nodes {:?} did not converge within {}s",
                indices,
                timeout.as_secs()
            );
        }

        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Check whether any server tasks have exited and bail if so.
///
/// Servers are expected to run for the duration of a scenario. Any exit —
/// clean shutdown or panic — means something went wrong. Calling this after
/// each major phase surfaces failures promptly rather than letting them appear
/// as confusing "connection refused" RPC errors.
fn check_tasks(tasks: &mut JoinSet<()>) -> Result<()> {
    if let Some(result) = tasks.try_join_next() {
        match result {
            Err(e) => bail!("server task panicked: {e}"),
            Ok(()) => bail!("server task exited unexpectedly during scenario"),
        }
    }
    Ok(())
}

/// Return the full-mesh intra-group edges for a slice of node indices.
fn intra_group_edges(nodes: &[usize]) -> Vec<(usize, usize)> {
    nodes
        .iter()
        .flat_map(|&i| nodes.iter().filter(move |&&j| j > i).map(move |&j| (i, j)))
        .collect()
}

/// Select the target-node index for op `i` given a write pattern.
///
/// `nodes` is the in-scope slice — `0..n` for a topology run, or a group's
/// `nodes` for one phase of a partition-heal run.
fn target_node(pattern: &WritePattern, i: usize, nodes: &[usize]) -> usize {
    match pattern {
        WritePattern::Concentrated => nodes[0],
        WritePattern::RoundRobin => nodes[i % nodes.len()],
    }
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Spawn nodes, wire the topology, apply writes, and wait for convergence.
pub async fn run(config: &TopologyConfig, source: NodeSource) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&mut clients).await?;

    let edges = config.connections.edges(n);
    let edge_count = edges.len();
    let topology_kind = config.connections.kind();
    let diameter = config.connections.diameter(n);
    connect_edges(&mut clients, &endpoints, &edges).await?;
    check_tasks(&mut tasks)?;

    // Start timing before the first write so the measurement includes write
    // propagation time; on loopback sync completes before wait_for_nodes
    // returns its first poll, so a post-write timer would always read 0.
    let all_nodes: Vec<usize> = (0..n).collect();
    let measure_start = Instant::now();
    for i in 0..config.op_count {
        let target = target_node(&config.write_pattern, i, &all_nodes);
        apply_write(&mut clients[target], config.workload, i).await?;
        // Pace between op submissions only — sleeping after the last op would
        // just delay wait_for_nodes' first poll and inflate convergence_ms
        // beyond what the pacing semantics imply.
        if config.op_interval_ms > 0 && i + 1 < config.op_count {
            tokio::time::sleep(Duration::from_millis(config.op_interval_ms)).await;
        }
    }
    check_tasks(&mut tasks)?;

    // Timeout absorbs the cumulative paced wall time plus a 5s convergence
    // budget; burst runs (op_interval_ms = 0) keep the historical 5s deadline.
    let pacing_budget =
        Duration::from_millis(config.op_interval_ms.saturating_mul(config.op_count as u64));
    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        measure_start,
        Duration::from_secs(5) + pacing_budget,
    )
    .await?;
    check_tasks(&mut tasks)?;

    Ok(RunResult {
        convergence_ms,
        total_ops: config.op_count,
        topology_kind,
        edge_count,
        diameter,
    })
}

/// Run a partition-then-heal scenario.
///
/// Phase 1: each group connects internally and writes independently.
/// Phase 2 (heal): remaining cross-group edges are added; time from heal
/// trigger to global convergence is returned.
pub async fn run_partition_heal(config: &PartitionConfig, source: NodeSource) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&mut clients).await?;

    // Wire each group internally and collect those edges for later subtraction.
    let intra: Vec<(usize, usize)> = config
        .groups
        .iter()
        .flat_map(|g| intra_group_edges(&g.nodes))
        .collect();
    connect_edges(&mut clients, &endpoints, &intra).await?;
    check_tasks(&mut tasks)?;

    // Apply ops to each group; keys are globally unique across groups.
    let mut op_idx = 0;
    for group in &config.groups {
        for i in 0..config.ops_per_group {
            let target = target_node(&config.write_pattern, i, &group.nodes);
            apply_write(&mut clients[target], config.workload, op_idx).await?;
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

    // Heal: pick the cross-group edges per heal_topology.
    //   FullMesh — every cross-group pair; post-heal graph is K_n
    //   Bridge   — only `groups[0].nodes[0] ↔ groups[1].nodes[0]` (validated
    //              to require exactly 2 groups)
    let intra_set: HashSet<(usize, usize)> = intra.iter().copied().collect();
    let heal_edges = heal_edges(&config.heal_topology, &config.groups, n, &intra_set);

    let heal_start = Instant::now();
    connect_edges(&mut clients, &endpoints, &heal_edges).await?;

    let all_nodes: Vec<usize> = (0..n).collect();
    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        heal_start,
        Duration::from_secs(10),
    )
    .await?;
    check_tasks(&mut tasks)?;

    // Report structural fields for the actual post-heal graph (intra-group
    // edges + heal edges). `topology_kind = "partition_heal"` flags the
    // scenario shape so analyses can separate heal-driven convergence from
    // steady-state runs; edge_count and diameter let downstream plots tell
    // the FullMesh-heal and Bridge-heal variants apart on structural axes.
    let mut final_edges = intra;
    final_edges.extend(heal_edges.iter().copied());
    let final_graph = Connections::Custom {
        edges: final_edges.clone(),
    };
    Ok(RunResult {
        convergence_ms,
        total_ops: config.groups.len() * config.ops_per_group,
        topology_kind: "partition_heal",
        edge_count: final_edges.len(),
        diameter: final_graph.diameter(n),
    })
}

/// Compute the heal-phase edges for a partition-heal scenario.
///
/// Caller owns the set of intra-group edges already wired during the partition
/// phase; this function returns *only* the cross-group edges to add at heal.
///
/// * `FullMesh` — every pair (i, j) with `i < j` that isn't already in `intra_set`.
/// * `Bridge` — exactly one edge `(min, max)` between `groups[0].nodes[0]` and
///   `groups[1].nodes[0]`. Assumes `groups.len() == 2` (enforced by
///   [`PartitionConfig::validate`]).
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

    // ── check_tasks ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn check_tasks_ok_when_all_running() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(std::future::pending());
        assert!(check_tasks(&mut tasks).is_ok());
        tasks.abort_all();
    }

    #[tokio::test]
    async fn check_tasks_errors_on_panic() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(async { panic!("boom") });
        // Sleep so tokio schedules the task; the panic is caught as a JoinError
        // and stored in the JoinSet. We must not call join_next here — that
        // would consume the result before check_tasks can see it.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let err = check_tasks(&mut tasks).unwrap_err();
        assert!(err.to_string().contains("panicked"), "{err}");
    }

    #[tokio::test]
    async fn check_tasks_errors_on_unexpected_exit() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        tasks.spawn(std::future::ready(()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let err = check_tasks(&mut tasks).unwrap_err();
        assert!(err.to_string().contains("unexpectedly"), "{err}");
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
        let deduped: std::collections::HashSet<_> = edges.iter().copied().collect();
        assert_eq!(n, deduped.len());
        assert!(edges.iter().all(|(i, j)| i < j));
    }

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

    // ── relay across non-mesh topologies ───────────────────────────────────

    /// `Replica.Reset` over the wire: write, converge, reset, verify the
    /// fingerprint is empty, then re-wire and write again — proves the second
    /// trial starts from a clean slate. This is the exact pattern the
    /// orchestrator runs each time a `--replicas`-mode trial begins.
    #[tokio::test]
    async fn reset_clears_state_and_allows_subsequent_run() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, &mut tasks).await.unwrap();
        let edges = vec![(0, 1)];

        // Trial 1: write something and converge.
        connect_edges(&mut clients, &endpoints, &edges)
            .await
            .unwrap();
        for i in 0..5 {
            map_put(&mut clients[0], &format!("trial1_k{i}"), &format!("v{i}"))
                .await
                .unwrap();
        }
        wait_for_nodes(
            &mut clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let fp_before = clients[0]
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner()
            .fingerprint;
        assert!(
            !fp_before.is_empty(),
            "fingerprint must be non-empty after writes"
        );

        // Reset every replica back to empty.
        reset_all(&mut clients).await.unwrap();

        for (i, client) in clients.iter_mut().enumerate() {
            let fp = client
                .get_state_fingerprint(Request::new(Empty {}))
                .await
                .unwrap()
                .into_inner()
                .fingerprint;
            assert!(
                fp.is_empty(),
                "node {i} fingerprint must be empty after reset"
            );
        }

        // Trial 2: re-wire peers (Reset cleared peer_txs) and write *different*
        // data. Convergence here only succeeds if the previous trial's sync
        // state was fully discarded — a stale per-peer sync::State would leave
        // the new handshake stuck or produce a divergent fingerprint.
        connect_edges(&mut clients, &endpoints, &edges)
            .await
            .unwrap();
        for i in 0..3 {
            map_put(&mut clients[0], &format!("trial2_k{i}"), &format!("w{i}"))
                .await
                .unwrap();
        }
        wait_for_nodes(
            &mut clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        let fp_after_a = clients[0]
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner()
            .fingerprint;
        let fp_after_b = clients[1]
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner()
            .fingerprint;
        assert_eq!(fp_after_a, fp_after_b, "post-reset trial must converge");
        assert_ne!(
            fp_after_a, fp_before,
            "post-reset fingerprint must reflect the new writes, not the prior trial",
        );

        check_tasks(&mut tasks).unwrap();
    }

    /// Reset must be safe to call when there are no peer connections and an
    /// empty document — i.e. the very first trial after `spawn_nodes`. The
    /// orchestrator unconditionally resets at the start of every trial, so
    /// this path needs to not deadlock or panic on the no-op case.
    #[tokio::test]
    async fn reset_is_noop_on_fresh_replica() {
        let mut tasks = JoinSet::new();
        let (_endpoints, mut clients) = spawn_nodes(2, &mut tasks).await.unwrap();
        reset_all(&mut clients).await.unwrap();
        for client in clients.iter_mut() {
            let fp = client
                .get_state_fingerprint(Request::new(Empty {}))
                .await
                .unwrap()
                .into_inner()
                .fingerprint;
            assert!(fp.is_empty());
        }
        check_tasks(&mut tasks).unwrap();
    }

    /// Convergence across a 4-node line (0↔1↔2↔3) with all writes at node 0
    /// only succeeds if `recv_loop` relays received state onward — nodes 2
    /// and 3 are not directly connected to the writer, so the only way for
    /// them to learn about its changes is through node 1 forwarding.
    #[tokio::test]
    async fn line_topology_n4_converges_with_relay() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(4, &mut tasks).await.unwrap();

        let edges = vec![(0, 1), (1, 2), (2, 3)];
        connect_edges(&mut clients, &endpoints, &edges)
            .await
            .unwrap();
        check_tasks(&mut tasks).unwrap();

        let measure_start = Instant::now();
        for i in 0..4 {
            map_put(&mut clients[0], &format!("k{i}"), &format!("v{i}"))
                .await
                .unwrap();
        }
        check_tasks(&mut tasks).unwrap();

        wait_for_nodes(
            &mut clients,
            &[0, 1, 2, 3],
            measure_start,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        check_tasks(&mut tasks).unwrap();
    }
}
