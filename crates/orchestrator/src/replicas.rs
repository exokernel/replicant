//! Talks to replicas over gRPC: gets them, wires them, resets them, drives
//! writes, and polls them for convergence.
//!
//! The two scenario runners, [`crate::runner`] and [`crate::partition_heal`],
//! both build on these helpers.
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
use tonic::transport::Channel;

use common::NodeId;
use common::proto::{
    Empty, MapPut, ObjRef, OpRequest, PeerIds, PeerLinkUpdate, PeerRef, ScalarValue, TextSplice,
    op_request, replica_client::ReplicaClient, scalar_value,
};
use replica::adapter::Crdt;
use replica::server::{self, ReplicaState};

use crate::scenario::Workload;
use crate::trace::{Event, Trace};

/// The gRPC client the orchestrator uses for one replica.
pub type Client = ReplicaClient<Channel>;

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

// ── Node identity ──────────────────────────────────────────────────────────

/// Returns the name of node `i`, which is `node-{i}`. It is both the actor ID
/// and the peer ID other replicas use for it.
///
/// This naming scheme is shared with code outside this crate. The Kubernetes
/// StatefulSet is named `node`, so its pod ordinals produce the same names, and
/// scenarios are wired by index on that assumption. Do not change the format.
pub fn node_name(i: usize) -> String {
    format!("node-{i}")
}

/// Returns the actor ID for node `i`. See [`node_name`].
fn node_id(i: usize) -> Result<NodeId> {
    NodeId::new(node_name(i))
}

// ── Getting replicas ───────────────────────────────────────────────────────

/// Binds a port, starts a replica server, and returns its endpoint and client.
///
/// The server task joins `tasks`, so the caller can detect a panic. `tasks`
/// must outlive every use of the returned client, or the server is dropped.
async fn spawn_node(
    actor_id: NodeId,
    crdt: Crdt,
    tasks: &mut JoinSet<()>,
) -> Result<(ReplicaEndpoint, Client)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = ReplicaState::new(actor_id, crdt.build());
    tasks.spawn(async move {
        if let Err(e) = server::router(state)
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
pub async fn spawn_nodes(
    n: usize,
    crdt: Crdt,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<Client>)> {
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
async fn connect_external(endpoints: &[ReplicaEndpoint]) -> Result<Vec<Client>> {
    let mut clients = Vec::with_capacity(endpoints.len());
    for ep in endpoints {
        let mut client = ReplicaClient::connect(format!("http://{}", ep.client_addr))
            .await
            .with_context(|| format!("connecting to external replica at {}", ep.client_addr))?;
        fingerprint(&mut client)
            .await
            .with_context(|| format!("smoke fingerprint RPC failed for {}", ep.client_addr))?;
        clients.push(client);
    }
    Ok(clients)
}

/// Gets `n` replicas from `source`.
///
/// `InProcess` spawns them on ephemeral ports. `External` uses the first `n`
/// endpoints, and fails if there are fewer. It resets the endpoints it does not
/// use, so no replica keeps a sync stream or state from an earlier run. One
/// stack can then serve scenarios of every size up to its own.
///
/// Only the in-process path adds anything to `tasks`. An external run leaves it
/// empty, which makes every later [`check_tasks`] call a no-op.
pub async fn acquire_nodes(
    source: NodeSource,
    n: usize,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<ReplicaEndpoint>, Vec<Client>)> {
    match source {
        NodeSource::InProcess(crdt) => spawn_nodes(n, crdt, tasks).await,
        NodeSource::External(mut endpoints) => {
            if endpoints.len() < n {
                bail!(
                    "scenario needs {n} replicas but --replicas supplied only {}",
                    endpoints.len()
                );
            }
            let unused = endpoints.split_off(n);
            if !unused.is_empty() {
                reset_all(&connect_external(&unused).await?).await?;
            }
            let clients = connect_external(&endpoints).await?;
            Ok((endpoints, clients))
        }
    }
}

/// Fails if any server task has exited.
///
/// A server runs for the whole scenario, so any exit is a fault, whether it
/// panicked or returned cleanly. Call this after each phase. Without it the
/// failure surfaces later as a confusing "connection refused" from an RPC.
pub fn check_tasks(tasks: &mut JoinSet<()>) -> Result<()> {
    if let Some(result) = tasks.try_join_next() {
        match result {
            Err(e) => bail!("server task panicked: {e}"),
            Ok(()) => bail!("server task exited unexpectedly during scenario"),
        }
    }
    Ok(())
}

// ── Wiring ─────────────────────────────────────────────────────────────────

/// Waits for every task in `set` and returns the first error.
///
/// `what` names the RPC in the message for a task that panicked.
async fn join_all(mut set: JoinSet<Result<()>>, what: &str) -> Result<()> {
    while let Some(joined) = set.join_next().await {
        joined.with_context(|| format!("{what} task panicked"))??;
    }
    Ok(())
}

/// Resets every replica to an empty document with no peer connections.
///
/// The calls run in parallel, so this costs one round trip rather than `n` of
/// them. For an external replica this is what makes repeated trials possible
/// without restarting the container.
pub async fn reset_all(clients: &[Client]) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for client in clients {
        let mut c = client.clone();
        set.spawn(async move {
            c.reset(Request::new(Empty {})).await?;
            Ok(())
        });
    }
    join_all(set, "reset").await
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
pub async fn connect_edges(
    clients: &[Client],
    endpoints: &[ReplicaEndpoint],
    edges: &[(usize, usize)],
) -> Result<()> {
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for &(i, j) in edges {
        let mut client = clients[i].clone();
        let peer = PeerRef {
            peer_id: node_name(j),
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
    join_all(set, "ConnectPeer").await
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
pub async fn set_links_blocked(
    clients: &[Client],
    edges: &[(usize, usize)],
    blocked: bool,
) -> Result<()> {
    let mut by_node: HashMap<usize, Vec<String>> = HashMap::new();
    for &(i, j) in edges {
        by_node.entry(i).or_default().push(node_name(j));
        by_node.entry(j).or_default().push(node_name(i));
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
    join_all(set, "SetPeerLinks").await
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
pub async fn kick_sync_edges(clients: &[Client], edges: &[(usize, usize)]) -> Result<()> {
    let mut by_node: HashMap<usize, Vec<String>> = HashMap::new();
    for &(i, j) in edges {
        let (from, to) = if i < j { (i, j) } else { (j, i) };
        by_node.entry(from).or_default().push(node_name(to));
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
    join_all(set, "KickSync").await
}

// ── Writes ─────────────────────────────────────────────────────────────────

/// Name of the shared text object that every `TextSplice` workload writes to.
pub const TEXT_OBJ: &str = "text";

/// Applies one `MapPut` to the root map.
pub async fn map_put(client: &mut Client, key: &str, val: &str) -> Result<()> {
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
pub async fn text_splice(client: &mut Client, obj: &str, pos: usize, insert: &str) -> Result<()> {
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

/// Applies write number `seq` to `client`, using the configured `workload`.
///
/// `seq` keeps `MapPut` keys distinct. The text workload ignores it and
/// prepends one filler character, because merge cost depends on position and
/// operation shape, not on content.
///
/// The divergence sweep uses this for `MapPut` only. It draws its text
/// positions from the locality rule instead. See [`crate::partition_heal`].
pub async fn apply_write(client: &mut Client, workload: Workload, seq: usize) -> Result<()> {
    match workload {
        Workload::MapPut => map_put(client, &format!("k{seq}"), &format!("v{seq}")).await,
        Workload::TextSplice => text_splice(client, TEXT_OBJ, 0, "x").await,
    }
}

/// Creates the shared text object on every node in `indices`.
///
/// Each replica writes a bit-identical first change, using a fixed actor and
/// time 0. All replicas therefore end up with one shared object identity
/// without syncing.
///
/// Call this after `reset_all` and **before any edge is wired**. Once sync is
/// running, a peer's operation can arrive first, and the replica's determinism
/// guard then rejects the bootstrap change.
pub async fn ensure_text_all(
    clients: &mut [Client],
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

// ── Reads and convergence ──────────────────────────────────────────────────

/// Returns the replica's state fingerprint. An empty fingerprint means the
/// document has no operations yet.
pub async fn fingerprint(client: &mut Client) -> Result<Vec<u8>> {
    Ok(client
        .get_state_fingerprint(Request::new(Empty {}))
        .await?
        .into_inner()
        .fingerprint)
}

/// Polls the nodes at `indices` until every fingerprint is equal and non-empty.
///
/// Returns the milliseconds elapsed from `start`. Fails if `timeout` passes
/// first. Records each poll, and the convergence, in `trace`.
pub async fn wait_for_nodes(
    clients: &mut [Client],
    indices: &[usize],
    start: Instant,
    timeout: Duration,
    trace: &mut Trace,
) -> Result<f64> {
    loop {
        let mut fps: Vec<Vec<u8>> = Vec::with_capacity(indices.len());
        for &i in indices {
            fps.push(fingerprint(&mut clients[i]).await?);
        }
        trace.poll(indices, &fps);

        // An empty fingerprint means the node has no operations yet. Treat that
        // as not converged, so a run cannot match before any write lands.
        let converged = fps.iter().all(|fp| !fp.is_empty()) && fps.windows(2).all(|w| w[0] == w[1]);
        if converged {
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            trace.record(Event::Converged {
                nodes: indices.to_vec(),
            });
            return Ok(elapsed_ms);
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

/// Checks that every node's text holds exactly `expected` characters. Valid
/// only for insert-only workloads, where each operation adds one character.
///
/// Matching fingerprints prove the replicas *agree*. This proves they agree on
/// a document that still contains everyone's work. A bug that made both sides
/// converge on one side's text only would pass the fingerprint check and fail
/// here.
pub async fn verify_text_length(
    clients: &mut [Client],
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

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Spawns two in-process Automerge replicas.
    async fn two_nodes(tasks: &mut JoinSet<()>) -> (Vec<ReplicaEndpoint>, Vec<Client>) {
        spawn_nodes(2, Crdt::Automerge, tasks).await.unwrap()
    }

    /// Waits for nodes 0 and 1 to agree, with a fresh 5 s deadline.
    async fn wait_for_pair(clients: &mut [Client]) {
        wait_for_nodes(
            clients,
            &[0, 1],
            Instant::now(),
            Duration::from_secs(5),
            &mut Trace::off(),
        )
        .await
        .unwrap();
    }

    // ── acquire_nodes ──────────────────────────────────────────────────────

    /// An external stack larger than the scenario serves it: the scenario gets
    /// the first `n` replicas, and the rest are reset.
    #[tokio::test]
    async fn external_source_uses_the_first_n_and_resets_the_rest() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = spawn_nodes(3, Crdt::Automerge, &mut tasks).await.unwrap();
        map_put(&mut clients[2], "k", "v").await.unwrap();
        assert!(!fingerprint(&mut clients[2]).await.unwrap().is_empty());

        let mut unused_tasks = JoinSet::new();
        let (got, _) = acquire_nodes(
            NodeSource::External(endpoints.clone()),
            2,
            &mut unused_tasks,
        )
        .await
        .unwrap();

        assert_eq!(got, endpoints[..2]);
        assert!(
            fingerprint(&mut clients[2]).await.unwrap().is_empty(),
            "the unused replica was not reset"
        );
    }

    #[tokio::test]
    async fn external_source_with_too_few_replicas_fails() {
        let mut tasks = JoinSet::new();
        let (endpoints, _) = two_nodes(&mut tasks).await;
        let err = acquire_nodes(NodeSource::External(endpoints), 3, &mut JoinSet::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("only 2"), "{err}");
    }

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

    #[test]
    fn node_name_matches_the_statefulset_pod_names() {
        assert_eq!(node_name(0), "node-0");
        assert_eq!(node_name(12), "node-12");
    }

    // ── Reset ──────────────────────────────────────────────────────────────

    /// Drives `Replica.Reset` over the wire: write, converge, reset, check the
    /// fingerprint is empty, then re-wire and write again.
    ///
    /// This is the sequence the orchestrator runs at the start of every trial
    /// against external replicas. It shows the second trial starts clean.
    #[tokio::test]
    async fn reset_clears_state_and_allows_subsequent_run() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = two_nodes(&mut tasks).await;
        let edges = vec![(0, 1)];

        // Trial 1: write something and converge.
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
        for i in 0..5 {
            map_put(&mut clients[0], &format!("trial1_k{i}"), &format!("v{i}"))
                .await
                .unwrap();
        }
        wait_for_pair(&mut clients).await;

        let fp_before = fingerprint(&mut clients[0]).await.unwrap();
        assert!(
            !fp_before.is_empty(),
            "fingerprint must be non-empty after writes"
        );

        // Reset every replica back to empty.
        reset_all(&clients).await.unwrap();

        for (i, client) in clients.iter_mut().enumerate() {
            let fp = fingerprint(client).await.unwrap();
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
        wait_for_pair(&mut clients).await;

        let fp_after_a = fingerprint(&mut clients[0]).await.unwrap();
        let fp_after_b = fingerprint(&mut clients[1]).await.unwrap();
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
        let (_endpoints, mut clients) = two_nodes(&mut tasks).await;
        reset_all(&clients).await.unwrap();
        for client in clients.iter_mut() {
            assert!(fingerprint(client).await.unwrap().is_empty());
        }
        check_tasks(&mut tasks).unwrap();
    }

    // ── Relay across non-mesh topologies ───────────────────────────────────

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
            &mut Trace::off(),
        )
        .await
        .unwrap();
        check_tasks(&mut tasks).unwrap();
    }

    // ── App-layer partition ────────────────────────────────────────────────

    /// A blocked link carries nothing, even though its stream is open and both
    /// replicas are writing.
    ///
    /// This is what lets the harness wire the whole graph before the partition.
    /// If a blocked link leaked, the groups would merge at once and the heal
    /// would measure nothing.
    #[tokio::test]
    async fn blocked_link_carries_no_state_while_open() {
        let mut tasks = JoinSet::new();
        let (endpoints, mut clients) = two_nodes(&mut tasks).await;
        let edges = vec![(0, 1)];

        // Block first, then wire: the stream opens without a handshake.
        set_links_blocked(&clients, &edges, true).await.unwrap();
        connect_edges(&clients, &endpoints, &edges).await.unwrap();

        map_put(&mut clients[0], "from_a", "1").await.unwrap();
        map_put(&mut clients[1], "from_b", "2").await.unwrap();

        // Give any leak a generous chance to land before asserting.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let fp_a = fingerprint(&mut clients[0]).await.unwrap();
        let fp_b = fingerprint(&mut clients[1]).await.unwrap();
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
        let (endpoints, mut clients) = two_nodes(&mut tasks).await;
        let edges = vec![(0, 1)];

        set_links_blocked(&clients, &edges, true).await.unwrap();
        connect_edges(&clients, &endpoints, &edges).await.unwrap();
        map_put(&mut clients[0], "from_a", "1").await.unwrap();
        map_put(&mut clients[1], "from_b", "2").await.unwrap();

        set_links_blocked(&clients, &edges, false).await.unwrap();
        kick_sync_edges(&clients, &edges).await.unwrap();

        wait_for_pair(&mut clients).await;
        check_tasks(&mut tasks).unwrap();
    }
}
