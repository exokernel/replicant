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
    /// Spawn `n` replicas inside this process on ephemeral ports, each
    /// backed by the given CRDT library.
    ///
    /// The library rides on this variant rather than sitting beside it
    /// because it only means anything here: externally-managed replicas were
    /// launched with their own `--crdt` long before the orchestrator
    /// connected, and nothing the orchestrator does can change them. Putting
    /// it on the enum makes "a CRDT choice for an external run" unstateable
    /// rather than merely ignored.
    InProcess(Crdt),
    /// Connect to replicas already running at these endpoints.
    External(Vec<ReplicaEndpoint>),
}

// ── Private helpers ────────────────────────────────────────────────────────

/// The canonical actor id for node `i`.
///
/// The `node-{i}` scheme is load-bearing beyond this crate: the k8s
/// StatefulSet is named `node` so its pod ordinals produce the same names,
/// and the orchestrator wires scenarios by index assuming it. One helper so
/// the format string is not repeated at each site that needs it.
fn node_id(i: usize) -> Result<NodeId> {
    NodeId::new(format!("node-{i}"))
}

/// Bind a port, start a replica server, and return its endpoint and gRPC client.
///
/// The server task is added to `tasks` so the caller can detect panics; the
/// `JoinSet` must outlive all client usage or the server will be dropped.
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

/// Spawn `n` nodes and return their endpoints and clients.
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

/// Reset every replica back to an empty document with no peer connections.
///
/// Issued in parallel so the per-trial overhead is one round-trip's worth of
/// latency rather than `n × round-trip`. For in-process replicas this is
/// effectively a no-op; for external (docker/k8s) replicas it replaces what
/// used to require a full container bounce between trials.
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

/// Call `ConnectPeer` for each edge and wait for all streams to be ready.
///
/// Each `ConnectPeer` RPC blocks until the TCP connection and gRPC stream are
/// open and the peer is registered, so no post-connect sleep is needed. The
/// address passed in `PeerRef.addr` is `peer_addr`, which is what the *target*
/// replica `i` will dial to reach replica `j` (may differ from the
/// orchestrator's `client_addr` for the same replica — see [`ReplicaEndpoint`]).
///
/// Edges are wired concurrently, so the wall time is one connection setup
/// rather than `edges.len()` of them. Every caller now wires outside a timed
/// window — [`crate::partition_heal::run_partition_heal`] pre-wires its whole post-heal graph — so
/// this is about keeping setup cheap, not about measurement validity.
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

/// Block or unblock the sync links for `edges`, on both endpoints of each.
///
/// Both endpoints matter. A link blocked on one side only still carries
/// traffic the other way, so the partition would leak; and on heal, a peer
/// unblocked before its counterpart would have its kick dropped as
/// still-blocked inbound. Grouping by node means one RPC per replica rather
/// than one per edge, so the heal-side cost stays flat in edge count.
///
/// Issued concurrently — for the unblock this is inside the measured heal
/// window, and serial RPCs would reintroduce exactly the edge-count-scaled
/// cost this design removes.
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

/// Start the post-heal sync handshake across `edges`.
///
/// Only one endpoint of each edge needs kicking — the sync protocol is
/// bidirectional once a message lands — so this kicks the lower-numbered node
/// of each pair. Concurrent for the same reason as [`set_links_blocked`].
///
/// Must run only after every endpoint is unblocked.
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

/// Assert that the partition actually held: no two groups may share a
/// fingerprint at the end of the divergence phase.
///
/// With the topology fully wired and the partition enforced only by the
/// blocked-link flags, a bug in that enforcement would quietly turn the
/// scenario into "already converged, then heal" — which reports a fast heal
/// and looks like a result rather than a broken experiment. The same class of
/// failure as the shared-object bug the text-length gate catches, so it gets
/// the same treatment: check it, in-runner, every trial.
///
/// Groups that wrote no ops are skipped — they are legitimately empty and
/// would match each other.
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

/// Apply a single `MapPut` on the root map.
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

/// Apply a single `TextSplice` on the named text object under root.
///
/// `pos = 0` (prepend) is always a valid splice position regardless of the
/// replica's current text length, so it stays valid under any write pattern
/// without the orchestrator tracking per-node document length. Phase 1's
/// generator replaces this fixed anchor with the swept locality rule.
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

/// Name of the shared text object every `TextSplice` workload writes to.
pub(crate) const TEXT_OBJ: &str = "text";

/// Bootstrap the shared text object on every node in `indices`.
///
/// Each replica authors the bit-identical bootstrap change (fixed actor,
/// time 0) as its first change, so all replicas share one object identity
/// with no sync required. Must run after `reset_all` and **before any edges
/// are wired**: once sync is flowing, a peer's ops could land first and the
/// replica-side determinism guard would reject the bootstrap.
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

/// Post-convergence validity check for insert-only text workloads: every
/// node's text must hold exactly `expected` characters (one per op).
///
/// Fingerprint equality proves the replicas *agree*; this proves they agree
/// on a document that contains **all** the work. The distinction is not
/// theoretical — a shared-object bug once made every heal converge cleanly
/// while silently discarding one side's entire text, and only a length check
/// like this one could have caught it.
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
        Workload::TextSplice => text_splice(client, TEXT_OBJ, 0, "x").await,
    }
}

/// Poll nodes at `indices` until all have equal, non-empty fingerprints.
///
/// Returns fractional ms from `start` to convergence.
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
pub(crate) fn check_tasks(tasks: &mut JoinSet<()>) -> Result<()> {
    if let Some(result) = tasks.try_join_next() {
        match result {
            Err(e) => bail!("server task panicked: {e}"),
            Ok(()) => bail!("server task exited unexpectedly during scenario"),
        }
    }
    Ok(())
}

/// Return the full-mesh intra-group edges for a slice of node indices.
pub(crate) fn intra_group_edges(nodes: &[usize]) -> Vec<(usize, usize)> {
    nodes
        .iter()
        .flat_map(|&i| nodes.iter().filter(move |&&j| j > i).map(move |&j| (i, j)))
        .collect()
}

/// Select the target-node index for op `i` given a write pattern.
///
/// `nodes` is the in-scope slice — `0..n` for a topology run, or a group's
/// `nodes` for one phase of a partition-heal run.
pub(crate) fn target_node(pattern: &WritePattern, i: usize, nodes: &[usize]) -> usize {
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
    reset_all(&clients).await?;

    // Text workloads need the shared text object bootstrapped on every node
    // before any edges exist (see `ensure_text_all`); otherwise each node
    // lazily creates its own object on first write and concurrent creations
    // collide as map-key conflicts.
    if config.workload == Workload::TextSplice {
        ensure_text_all(&mut clients, 0..n).await?;
    }

    let edges = config.connections.edges(n);
    let edge_count = edges.len();
    let topology_kind = config.connections.kind();
    let diameter = config.connections.diameter(n);
    connect_edges(&clients, &endpoints, &edges).await?;
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

    // Validity gate, outside the measurement window: the converged text must
    // contain every op's insert (ops are insert-only, one char each).
    if config.workload == Workload::TextSplice {
        verify_text_length(&mut clients, &all_nodes, config.op_count).await?;
    }

    Ok(RunResult {
        convergence_ms,
        // All wiring happens before `measure_start`, so none of it is inside
        // the reported window.
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

    /// `Replica.Reset` over the wire: write, converge, reset, verify the
    /// fingerprint is empty, then re-wire and write again — proves the second
    /// trial starts from a clean slate. This is the exact pattern the
    /// orchestrator runs each time a `--replicas`-mode trial begins.
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

    /// Reset must be safe to call when there are no peer connections and an
    /// empty document — i.e. the very first trial after `spawn_nodes`. The
    /// orchestrator unconditionally resets at the start of every trial, so
    /// this path needs to not deadlock or panic on the no-op case.
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

    /// Convergence across a 4-node line (0↔1↔2↔3) with all writes at node 0
    /// only succeeds if `recv_loop` relays received state onward — nodes 2
    /// and 3 are not directly connected to the writer, so the only way for
    /// them to learn about its changes is through node 1 forwarding.
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

    /// The core guarantee of the app-layer partition: a blocked link carries
    /// nothing, even though the stream is fully open and both replicas are
    /// writing. Without this, wiring the heal edges up front would simply
    /// merge the groups immediately and the scenario would measure nothing.
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

    /// Unblocking plus a kick must converge an already-wired link. This is the
    /// heal path end-to-end through the real gRPC stack: no new connection is
    /// made, so convergence here is the sync protocol alone.
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

    /// The leak gate must fire when the groups already agree. Simulated by
    /// running the check on two nodes that were never partitioned at all.
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

    /// ...and must pass when they genuinely differ.
    #[tokio::test]
    async fn diverged_gate_accepts_genuinely_divergent_groups() {
        let mut tasks = JoinSet::new();
        let (_endpoints, mut clients) = spawn_nodes(2, Crdt::Automerge, &mut tasks).await.unwrap();
        map_put(&mut clients[0], "a", "1").await.unwrap();
        map_put(&mut clients[1], "b", "2").await.unwrap();

        let groups = vec![Group { nodes: vec![0] }, Group { nodes: vec![1] }];
        assert!(verify_groups_diverged(&mut clients, &groups).await.is_ok());
    }

    /// A heal that opens no new connections should cost far less than one
    /// that does, and — the point of the change — should not scale with the
    /// number of healed edges. Full-mesh heal on n=6 opens 9 cross-group
    /// links; the reported `wiring_ms` is now flag flips only.
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

    /// A topology run wires every edge before starting the clock, so none of
    /// its `convergence_ms` is stream setup.
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

    /// The heal's unblock round is inside the window, so it is still reported
    /// as `wiring_ms` — it just no longer contains connection setup. It must
    /// remain a real part of the measured window, or an analysis subtracting
    /// it would produce a negative merge time.
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
