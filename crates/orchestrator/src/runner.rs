//! Spawns replicas, wires them into a topology, drives writes, and waits for
//! convergence.
//!
//! Two scenario shapes use this machinery. [`run`] runs a steady-state topology
//! scenario. [`crate::partition_heal`] runs the divergence sweep and builds on
//! the helpers defined here.
//!
//! A replica is either an in-process Tokio task or an externally managed
//! process reached over gRPC. [`NodeSource`] selects between them, and the rest
//! of the module is written against the gRPC client either way.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::{Channel, Server};

use common::NodeId;
use common::proto::{
    Empty, MapPut, ObjRef, OpRequest, PeerIds, PeerLinkUpdate, PeerRef, ScalarValue, TextSplice,
    op_request, replica_client::ReplicaClient, replica_server::ReplicaServer, scalar_value,
    sync_server::SyncServer,
};
use replica::adapter::Crdt;
use replica::server::{ReplicaService, ReplicaState, SyncService};

use crate::topology::{Group, RunResult, TopologyConfig, Workload, WritePattern};

// ── Replica endpoints ──────────────────────────────────────────────────────

/// The two network addresses of one replica.
///
/// Two are needed because the orchestrator and the replicas can sit in
/// different name spaces. On the host they are the same
/// (`127.0.0.1:<port>`). Under docker-compose, `client_addr` is
/// `localhost:<published-port>` and `peer_addr` is `replica-N:50051`, because
/// containers reach each other by service DNS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEndpoint {
    /// `host:port` the orchestrator uses to dial this replica.
    pub client_addr: String,
    /// `host:port` other replicas use to dial this replica (passed in
    /// `PeerRef.addr` during `ConnectPeer`).
    pub peer_addr: String,
}

impl ReplicaEndpoint {
    /// Builds an endpoint whose two addresses are the same. This is the
    /// in-process case.
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
    /// Spawn `n` replicas inside this process on ephemeral ports, each backed
    /// by the named CRDT library.
    ///
    /// The library is part of this variant because it applies only here. An
    /// external replica chose its library from its own `--crdt` flag at
    /// startup, and the orchestrator cannot change it. Carrying the choice on
    /// the variant makes a CRDT selection for an external run impossible to
    /// express, rather than silently ignored.
    InProcess(Crdt),
    /// Connect to replicas that are already running at these endpoints.
    External(Vec<ReplicaEndpoint>),
}

// ── Private helpers ────────────────────────────────────────────────────────

/// Returns the actor ID for node `i`, which is `node-{i}`.
///
/// This naming scheme is shared with code outside this crate. The Kubernetes
/// StatefulSet is named `node`, so its pod ordinals produce the same names, and
/// scenarios are wired by index on that assumption. Do not change the format.
fn node_id(i: usize) -> Result<NodeId> {
    NodeId::new(format!("node-{i}"))
}

/// Binds a port, starts a replica server, and returns its endpoint and client.
///
/// The server task joins `tasks`, so the caller can detect a panic. `tasks`
/// must outlive every use of the returned client, or the server is dropped.
async fn spawn_node(
    actor_id: NodeId,
    crdt: Crdt,
    tasks: &mut JoinSet<()>,
) -> Result<(ReplicaEndpoint, ReplicaClient<Channel>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = ReplicaState::from_boxed_adapter(actor_id, crdt.build());
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

/// Spawns `n` nodes and returns their endpoints and clients.
async fn spawn_nodes(
    n: usize,
    crdt: Crdt,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<ReplicaClient<Channel>>)> {
    let mut endpoints = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);
    for i in 0..n {
        let (ep, client) = spawn_node(node_id(i)?, crdt, tasks).await?;
        endpoints.push(ep);
        clients.push(client);
    }
    Ok((endpoints, clients))
}

/// Connects to replicas that are already running at `endpoints`.
///
/// Each endpoint is dialled on its `client_addr`. Every replica must then
/// answer a `GetStateFingerprint` call before this returns. That check reports
/// a container that has not started yet, instead of letting the failure appear
/// later during `ConnectPeer`.
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

/// Gets `n` replicas from `source`.
///
/// `InProcess` spawns them on ephemeral ports. `External` checks that the
/// endpoint count matches `n`, then connects to each.
///
/// Only the in-process path adds anything to `tasks`. An external run leaves it
/// empty, which makes every later [`check_tasks`] call a no-op.
pub(crate) async fn acquire_nodes(
    source: NodeSource,
    n: usize,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<ReplicaClient<Channel>>)> {
    match source {
        NodeSource::InProcess(crdt) => spawn_nodes(n, crdt, tasks).await,
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

/// Resets every replica to an empty document with no peer connections.
///
/// The calls run in parallel, so this costs one round trip rather than `n` of
/// them. For an external replica this is what makes repeated trials possible
/// without restarting the container.
pub(crate) async fn reset_all(clients: &[ReplicaClient<Channel>]) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for client in clients {
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

/// Opens a sync stream for every edge and waits until all of them are ready.
///
/// A `ConnectPeer` call returns only once the TCP connection and the gRPC
/// stream are open and the peer is registered, so no sleep is needed
/// afterwards.
///
/// `PeerRef.addr` carries `peer_addr`, the address replica `i` dials to reach
/// replica `j`. It can differ from the `client_addr` the orchestrator dials.
/// See [`ReplicaEndpoint`].
///
/// The edges are wired concurrently, so this costs one connection setup rather
/// than `edges.len()` of them.
pub(crate) async fn connect_edges(
    clients: &[ReplicaClient<Channel>],
    endpoints: &[ReplicaEndpoint],
    edges: &[(usize, usize)],
) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for &(i, j) in edges {
        let mut client = clients[i].clone();
        let peer = PeerRef {
            peer_id: format!("node-{j}"),
            addr: endpoints[j].peer_addr.clone(),
        };
        set.spawn(async move {
            client
                .connect_peer(Request::new(peer))
                .await
                .with_context(|| format!("ConnectPeer node-{i} -> node-{j}"))?;
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.context("ConnectPeer task panicked")??;
    }
    Ok(())
}

/// Blocks or unblocks the sync link for every edge, on both of its endpoints.
///
/// Both endpoints must be set. A link blocked on one side only still carries
/// traffic the other way, which leaks the partition. At heal time, a peer
/// unblocked before its counterpart has its kick dropped on arrival.
///
/// The calls are grouped by node, so this issues one call per replica rather
/// than one per edge, and they run concurrently. The unblock happens inside the
/// measured window, so its cost must not grow with the number of edges.
pub(crate) async fn set_links_blocked(
    clients: &[ReplicaClient<Channel>],
    edges: &[(usize, usize)],
    blocked: bool,
) -> Result<()> {
    let mut by_node: HashMap<usize, Vec<String>> = HashMap::new();
    for &(i, j) in edges {
        by_node.entry(i).or_default().push(format!("node-{j}"));
        by_node.entry(j).or_default().push(format!("node-{i}"));
    }

    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for (node, peer_ids) in by_node {
        let mut client = clients[node].clone();
        set.spawn(async move {
            client
                .set_peer_links(Request::new(PeerLinkUpdate { peer_ids, blocked }))
                .await
                .with_context(|| format!("SetPeerLinks(blocked={blocked}) on node {node}"))?;
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.context("SetPeerLinks task panicked")??;
    }
    Ok(())
}

/// Starts the sync handshake on every edge.
///
/// Only one endpoint of each edge needs a kick, because the sync protocol runs
/// in both directions once a message arrives. This kicks the lower-numbered
/// node of each pair. The calls run concurrently, for the same reason as in
/// [`set_links_blocked`].
///
/// Every endpoint must already be unblocked. A kick to a blocked peer is
/// dropped, and the sender then waits for a reply that never comes.
pub(crate) async fn kick_sync_edges(
    clients: &[ReplicaClient<Channel>],
    edges: &[(usize, usize)],
) -> Result<()> {
    let mut by_node: HashMap<usize, Vec<String>> = HashMap::new();
    for &(i, j) in edges {
        let (from, to) = if i < j { (i, j) } else { (j, i) };
        by_node.entry(from).or_default().push(format!("node-{to}"));
    }

    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for (node, peer_ids) in by_node {
        let mut client = clients[node].clone();
        set.spawn(async move {
            client
                .kick_sync(Request::new(PeerIds { peer_ids }))
                .await
                .with_context(|| format!("KickSync on node {node}"))?;
            Ok(())
        });
    }
    while let Some(joined) = set.join_next().await {
        joined.context("KickSync task panicked")??;
    }
    Ok(())
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
pub(crate) async fn verify_groups_diverged(
    clients: &mut [ReplicaClient<Channel>],
    groups: &[Group],
) -> Result<()> {
    let mut seen: Vec<(usize, Vec<u8>)> = Vec::with_capacity(groups.len());
    for (gi, group) in groups.iter().enumerate() {
        let node = group.nodes[0];
        let fp = clients[node]
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .with_context(|| format!("GetStateFingerprint on node {node}"))?
            .into_inner()
            .fingerprint;
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

/// Applies one `MapPut` to the root map.
pub(crate) async fn map_put(
    client: &mut ReplicaClient<Channel>,
    key: &str,
    val: &str,
) -> Result<()> {
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

/// Applies one `TextSplice` to the named text object under the root.
///
/// Inserts only; `del_count` is always 0. The caller supplies `pos` and must
/// keep it within the replica's current text length. `pos = 0` is valid at any
/// length.
pub(crate) async fn text_splice(
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

/// Name of the shared text object that every `TextSplice` workload writes to.
pub(crate) const TEXT_OBJ: &str = "text";

/// Creates the shared text object on every node in `indices`.
///
/// Each replica writes a bit-identical first change, using a fixed actor and
/// time 0. All replicas therefore end up with one shared object identity
/// without syncing.
///
/// Call this after `reset_all` and **before any edge is wired**. Once sync is
/// running, a peer's operation can arrive first, and the replica's determinism
/// guard then rejects the bootstrap change.
pub(crate) async fn ensure_text_all(
    clients: &mut [ReplicaClient<Channel>],
    indices: impl Iterator<Item = usize>,
) -> Result<()> {
    for i in indices {
        clients[i]
            .ensure_text(Request::new(ObjRef {
                obj: TEXT_OBJ.to_owned(),
            }))
            .await
            .with_context(|| format!("EnsureText on node {i}"))?;
    }
    Ok(())
}

/// Checks that every node's text holds exactly `expected` characters. Valid
/// only for insert-only workloads, where each operation adds one character.
///
/// Matching fingerprints prove the replicas *agree*. This proves they agree on
/// a document that still contains everyone's work. A bug that made both sides
/// converge on one side's text only would pass the fingerprint check and fail
/// here.
pub(crate) async fn verify_text_length(
    clients: &mut [ReplicaClient<Channel>],
    indices: &[usize],
    expected: usize,
) -> Result<()> {
    for &i in indices {
        let got = clients[i]
            .get_text_length(Request::new(ObjRef {
                obj: TEXT_OBJ.to_owned(),
            }))
            .await
            .with_context(|| format!("GetTextLength on node {i}"))?
            .into_inner()
            .length as usize;
        if got != expected {
            bail!(
                "text-length check failed on node {i}: expected {expected} chars \
                 (one per op), found {got} — some replicas' inserts were lost \
                 in the merge"
            );
        }
    }
    Ok(())
}

/// Applies write number `seq` to `client`, using the configured `workload`.
///
/// `seq` keeps `MapPut` keys distinct. The text workload ignores it and
/// prepends one filler character, because merge cost depends on position and
/// operation shape, not on content.
///
/// This is the steady-state path. The divergence sweep draws its positions from
/// the locality rule instead. See [`crate::partition_heal`].
async fn apply_write(
    client: &mut ReplicaClient<Channel>,
    workload: Workload,
    seq: usize,
) -> Result<()> {
    match workload {
        Workload::MapPut => map_put(client, &format!("k{seq}"), &format!("v{seq}")).await,
        Workload::TextSplice => text_splice(client, TEXT_OBJ, 0, "x").await,
    }
}

/// Polls the nodes at `indices` until every fingerprint is equal and non-empty.
///
/// Returns the milliseconds elapsed from `start`. Fails if `timeout` passes
/// first.
pub(crate) async fn wait_for_nodes(
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

        // An empty fingerprint means the node has no operations yet. Treat that
        // as not converged, so a run cannot match before any write lands.
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

/// Fails if any server task has exited.
///
/// A server runs for the whole scenario, so any exit is a fault, whether it
/// panicked or returned cleanly. Call this after each phase. Without it the
/// failure surfaces later as a confusing "connection refused" from an RPC.
pub(crate) fn check_tasks(tasks: &mut JoinSet<()>) -> Result<()> {
    if let Some(result) = tasks.try_join_next() {
        match result {
            Err(e) => bail!("server task panicked: {e}"),
            Ok(()) => bail!("server task exited unexpectedly during scenario"),
        }
    }
    Ok(())
}

/// Returns every pair within `nodes`, which wires one group as a full mesh.
pub(crate) fn intra_group_edges(nodes: &[usize]) -> Vec<(usize, usize)> {
    nodes
        .iter()
        .flat_map(|&i| nodes.iter().filter(move |&&j| j > i).map(move |&j| (i, j)))
        .collect()
}

/// Returns the node that receives operation `i`.
///
/// `nodes` is the set in scope: `0..n` for a topology run, or one group's nodes
/// during a partition.
pub(crate) fn target_node(pattern: &WritePattern, i: usize, nodes: &[usize]) -> usize {
    match pattern {
        WritePattern::Concentrated => nodes[0],
        WritePattern::RoundRobin => nodes[i % nodes.len()],
    }
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Runs one steady-state topology scenario and returns its measurements.
///
/// Acquires the nodes, wires the graph, applies the writes, and waits for every
/// node to agree. All wiring happens before the clock starts, so `wiring_ms` is
/// always `0.0` here.
pub async fn run(config: &TopologyConfig, source: NodeSource) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = acquire_nodes(source, n, &mut tasks).await?;

    // Reset before wiring peers so each trial starts from a clean slate even
    // when the same external stack is reused across many scenarios/trials.
    reset_all(&clients).await?;

    // Create the shared text object on every node before any edge exists. See
    // `ensure_text_all`. Without this each node creates its own object on first
    // write, and those creations then collide as a map-key conflict.
    if config.workload == Workload::TextSplice {
        ensure_text_all(&mut clients, 0..n).await?;
    }

    let edges = config.connections.edges(n);
    let edge_count = edges.len();
    let topology_kind = config.connections.kind();
    let diameter = config.connections.diameter(n);
    connect_edges(&clients, &endpoints, &edges).await?;
    check_tasks(&mut tasks)?;

    // Start the clock before the first write, so the measurement includes
    // propagation. Over loopback, sync finishes before the first poll returns,
    // so a clock started after the writes would always read 0.
    let all_nodes: Vec<usize> = (0..n).collect();
    let measure_start = Instant::now();
    for i in 0..config.op_count {
        let target = target_node(&config.write_pattern, i, &all_nodes);
        apply_write(&mut clients[target], config.workload, i).await?;
        // Pace between operations only. A sleep after the last one would delay
        // the first convergence poll and inflate `convergence_ms`.
        if config.op_interval_ms > 0 && i + 1 < config.op_count {
            tokio::time::sleep(Duration::from_millis(config.op_interval_ms)).await;
        }
    }
    check_tasks(&mut tasks)?;

    // The timeout covers the total pacing delay plus 5s for convergence. An
    // unpaced run therefore gets the plain 5s deadline.
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

    // Validity gate, outside the measured window: the converged text must hold
    // one character per operation, because operations are insert-only.
    if config.workload == Workload::TextSplice {
        verify_text_length(&mut clients, &all_nodes, config.op_count).await?;
    }

    Ok(RunResult {
        convergence_ms,
        // Every edge is wired before `measure_start`, so no setup is inside the
        // reported window.
        wiring_ms: 0.0,
        total_ops: config.op_count,
        topology_kind,
        edge_count,
        diameter,
    })
}
// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_heal::run_partition_heal;
    use crate::topology::PartitionConfig;

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

    // ── relay across non-mesh topologies ───────────────────────────────────

    /// Drives `Replica.Reset` over the wire: write, converge, reset, check the
    /// fingerprint is empty, then re-wire and write again.
    ///
    /// This is the sequence the orchestrator runs at the start of every trial
    /// against external replicas. It shows the second trial starts clean.
    #[tokio::test]
    async fn reset_clears_state_and_allows_subsequent_run() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        let edges = vec![(0, 1)];

        // Trial 1: write something and converge.
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
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
        reset_all(&clients).await.unwrap();

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
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
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

    /// Reset on a fresh replica: no peers, empty document. The orchestrator
    /// resets before every trial, including the first, so this case must not
    /// deadlock or panic.
    #[tokio::test]
    async fn reset_is_noop_on_fresh_replica() {
        let mut tasks = JoinSet::new();
        let (_endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        reset_all(&clients).await.unwrap();
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

    /// Convergence across a 4-node line (0↔1↔2↔3) with every write at node 0.
    ///
    /// Nodes 2 and 3 have no link to the writer. They can only learn its
    /// changes if `recv_loop` forwards received state onward, so this passes
    /// only when relaying works.
    #[tokio::test]
    async fn line_topology_n4_converges_with_relay() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(4, Crdt::Automerge, &mut tasks).await.unwrap();

        let edges = vec![(0, 1), (1, 2), (2, 3)];
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
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

    // ── app-layer partition ────────────────────────────────────────────────

    async fn fingerprint_of(client: &mut ReplicaClient<Channel>) -> Vec<u8> {
        client
            .get_state_fingerprint(Request::new(Empty {}))
            .await
            .unwrap()
            .into_inner()
            .fingerprint
    }

    /// A blocked link carries nothing, even though its stream is open and both
    /// replicas are writing.
    ///
    /// This is what lets the harness wire the whole graph before the partition.
    /// If a blocked link leaked, the groups would merge at once and the heal
    /// would measure nothing.
    #[tokio::test]
    async fn blocked_link_carries_no_state_while_open() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        let edges = vec![(0, 1)];

        // Block first, then wire: the stream opens without a handshake.
        set_links_blocked(&clients, &edges, true).await.unwrap();
        connect_edges(&clients, &endpoints, &edges).await.unwrap();

        map_put(&mut clients[0], "from_a", "1").await.unwrap();
        map_put(&mut clients[1], "from_b", "2").await.unwrap();

        // Give any leak a generous chance to land before asserting.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let fp_a = fingerprint_of(&mut clients[0]).await;
        let fp_b = fingerprint_of(&mut clients[1]).await;
        assert!(
            !fp_a.is_empty() && !fp_b.is_empty(),
            "both must have written"
        );
        assert_ne!(fp_a, fp_b, "blocked link leaked state across the partition");

        check_tasks(&mut tasks).unwrap();
    }

    /// An unblock plus a kick converges an already-wired link.
    ///
    /// This is the heal path through the real gRPC stack. No connection is
    /// opened, so the time here is the sync protocol alone.
    #[tokio::test]
    async fn unblock_and_kick_heals_without_reconnecting() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        let edges = vec![(0, 1)];

        set_links_blocked(&clients, &edges, true).await.unwrap();
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
        map_put(&mut clients[0], "from_a", "1").await.unwrap();
        map_put(&mut clients[1], "from_b", "2").await.unwrap();

        set_links_blocked(&clients, &edges, false).await.unwrap();
        kick_sync_edges(&clients, &edges).await.unwrap();

        wait_for_nodes(
            &mut clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        check_tasks(&mut tasks).unwrap();
    }

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
        )
        .await
        .unwrap();

        let groups = vec![Group { nodes: vec![0] }, Group { nodes: vec![1] }];
        let err = verify_groups_diverged(&mut clients, &groups)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("partition leaked"), "{err}");
    }

    /// The leak gate passes when the groups really do differ.
    #[tokio::test]
    async fn diverged_gate_accepts_genuinely_divergent_groups() {
        let mut tasks = JoinSet::new();
        let (_endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        map_put(&mut clients[0], "a", "1").await.unwrap();
        map_put(&mut clients[1], "b", "2").await.unwrap();

        let groups = vec![Group { nodes: vec![0] }, Group { nodes: vec![1] }];
        assert!(verify_groups_diverged(&mut clients, &groups).await.is_ok());
    }

    /// `wiring_ms` must not grow with the number of healed links.
    ///
    /// A full-mesh heal on n=6 reopens 9 cross-group links. Because the graph
    /// is wired up front, the measured cost is only the unblock round.
    #[tokio::test]
    async fn full_mesh_heal_wiring_does_not_scale_with_edge_count() {
        let cfg = |n: usize, per_group: usize| -> PartitionConfig {
            let half = n / 2;
            let a: Vec<usize> = (0..half).collect();
            let b: Vec<usize> = (half..n).collect();
            toml::from_str(&format!(
                "node_count = {n}\n\
                 write_pattern = \"round_robin\"\n\
                 ops_per_group = {per_group}\n\
                 groups = [{{ nodes = {a:?} }}, {{ nodes = {b:?} }}]"
            ))
            .expect("valid partition config")
        };

        let small = run_partition_heal(&cfg(2, 4), NodeSource::InProcess(Crdt::Automerge), 1)
            .await
            .unwrap();
        let large = run_partition_heal(&cfg(6, 4), NodeSource::InProcess(Crdt::Automerge), 1)
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

    // ── wiring_ms ──────────────────────────────────────────────────────────

    /// A topology run wires every edge before the clock starts, so its
    /// `wiring_ms` is 0.0 and no stream setup is inside `convergence_ms`.
    #[tokio::test]
    async fn topology_run_reports_no_wiring_inside_the_window() {
        let config: TopologyConfig = toml::from_str(
            "node_count = 3\n\
             connections = \"full_mesh\"\n\
             write_pattern = \"round_robin\"\n\
             op_count = 3",
        )
        .expect("valid topology config");
        let result = run(&config, NodeSource::InProcess(Crdt::Automerge))
            .await
            .unwrap();
        assert_eq!(result.wiring_ms, 0.0);
    }

    /// The heal's unblock round runs inside the measured window and is reported
    /// as `wiring_ms`. It no longer contains connection setup, but it is still
    /// a real part of the window: `wiring_ms <= convergence_ms` must hold, or
    /// an analysis that subtracts it would report a negative merge time.
    #[tokio::test]
    async fn partition_heal_reports_wiring_within_the_convergence_window() {
        let config: PartitionConfig = toml::from_str(
            "node_count = 4\n\
             write_pattern = \"round_robin\"\n\
             ops_per_group = 4\n\
             groups = [{ nodes = [0, 1] }, { nodes = [2, 3] }]",
        )
        .expect("valid partition config");
        let result = run_partition_heal(&config, NodeSource::InProcess(Crdt::Automerge), 1)
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
}
