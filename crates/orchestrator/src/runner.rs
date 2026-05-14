use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::{Channel, Server};

use common::proto::{
    Empty, MapPut, OpRequest, PeerRef, ScalarValue, op_request, replica_client::ReplicaClient,
    replica_server::ReplicaServer, scalar_value, sync_server::SyncServer,
};
use replica::adapter::AutomergeAdapter;
use replica::server::{ReplicaService, ReplicaState, SyncService};

use crate::topology::{Connections, PartitionConfig, RunResult, TopologyConfig, WritePattern};

// ── Private helpers ────────────────────────────────────────────────────────

/// Bind a port, start a replica server, and return its address and gRPC client.
///
/// The server task is added to `tasks` so the caller can detect panics; the
/// `JoinSet` must outlive all client usage or the server will be dropped.
async fn spawn_node(
    actor_id: String,
    tasks: &mut JoinSet<()>,
) -> Result<(SocketAddr, ReplicaClient<Channel>)> {
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
    Ok((addr, client))
}

/// Spawn `n` nodes and return their addresses and clients.
async fn spawn_nodes(
    n: usize,
    tasks: &mut JoinSet<()>,
) -> Result<(Vec<SocketAddr>, Vec<ReplicaClient<Channel>>)> {
    let mut addrs = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);
    for i in 0..n {
        let (addr, client) = spawn_node(format!("node-{i}"), tasks).await?;
        addrs.push(addr);
        clients.push(client);
    }
    Ok((addrs, clients))
}

/// Call `ConnectPeer` for each edge and wait for all streams to be ready.
///
/// Each `ConnectPeer` RPC blocks until the TCP connection and gRPC stream are
/// open and the peer is registered, so no post-connect sleep is needed.
async fn connect_edges(
    clients: &mut [ReplicaClient<Channel>],
    addrs: &[SocketAddr],
    edges: &[(usize, usize)],
) -> Result<()> {
    for &(i, j) in edges {
        clients[i]
            .connect_peer(Request::new(PeerRef {
                peer_id: format!("node-{j}"),
                addr: addrs[j].to_string(),
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
pub async fn run(config: &TopologyConfig) -> Result<RunResult> {
    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (addrs, mut clients) = spawn_nodes(n, &mut tasks).await?;

    let edges = config.connections.edges(n);
    connect_edges(&mut clients, &addrs, &edges).await?;
    check_tasks(&mut tasks)?;

    // Start timing before the first write so the measurement includes write
    // propagation time; on loopback sync completes before wait_for_nodes
    // returns its first poll, so a post-write timer would always read 0.
    let all_nodes: Vec<usize> = (0..n).collect();
    let measure_start = Instant::now();
    for i in 0..config.op_count {
        let target = target_node(&config.write_pattern, i, &all_nodes);
        map_put(&mut clients[target], &format!("k{i}"), &format!("v{i}")).await?;
    }
    check_tasks(&mut tasks)?;

    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        measure_start,
        Duration::from_secs(5),
    )
    .await?;
    check_tasks(&mut tasks)?;

    Ok(RunResult {
        convergence_ms,
        total_ops: config.op_count,
    })
}

/// Run a partition-then-heal scenario.
///
/// Phase 1: each group connects internally and writes independently.
/// Phase 2 (heal): remaining cross-group edges are added; time from heal
/// trigger to global convergence is returned.
pub async fn run_partition_heal(config: &PartitionConfig) -> Result<RunResult> {
    config.validate()?;

    let n = config.node_count;
    let mut tasks = JoinSet::new();
    let (addrs, mut clients) = spawn_nodes(n, &mut tasks).await?;

    // Wire each group internally and collect those edges for later subtraction.
    let intra: Vec<(usize, usize)> = config
        .groups
        .iter()
        .flat_map(|g| intra_group_edges(&g.nodes))
        .collect();
    connect_edges(&mut clients, &addrs, &intra).await?;
    check_tasks(&mut tasks)?;

    // Apply ops to each group; keys are globally unique across groups.
    let mut op_idx = 0;
    for group in &config.groups {
        for i in 0..config.ops_per_group {
            let target = target_node(&config.write_pattern, i, &group.nodes);
            map_put(
                &mut clients[target],
                &format!("k{op_idx}"),
                &format!("v{op_idx}"),
            )
            .await?;
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

    // Heal: add the edges that cross group boundaries. `intra` is the set of
    // edges already wired during the partition phase; subtracting them from
    // the full mesh gives exactly the cross-group connections needed.
    let intra_set: HashSet<(usize, usize)> = intra.into_iter().collect();
    let heal_edges: Vec<(usize, usize)> = Connections::FullMesh
        .edges(n)
        .into_iter()
        .filter(|e| !intra_set.contains(e))
        .collect();

    let heal_start = Instant::now();
    connect_edges(&mut clients, &addrs, &heal_edges).await?;

    let all_nodes: Vec<usize> = (0..n).collect();
    let convergence_ms = wait_for_nodes(
        &mut clients,
        &all_nodes,
        heal_start,
        Duration::from_secs(10),
    )
    .await?;
    check_tasks(&mut tasks)?;

    Ok(RunResult {
        convergence_ms,
        total_ops: config.groups.len() * config.ops_per_group,
    })
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

    // ── relay across non-mesh topologies ───────────────────────────────────

    /// Convergence across a 4-node line (0↔1↔2↔3) with all writes at node 0
    /// only succeeds if `recv_loop` relays received state onward — nodes 2
    /// and 3 are not directly connected to the writer, so the only way for
    /// them to learn about its changes is through node 1 forwarding.
    #[tokio::test]
    async fn line_topology_n4_converges_with_relay() {
        let mut tasks = JoinSet::new();
        let (addrs, mut clients) = spawn_nodes(4, &mut tasks).await.unwrap();

        let edges = vec![(0, 1), (1, 2), (2, 3)];
        connect_edges(&mut clients, &addrs, &edges).await.unwrap();
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
