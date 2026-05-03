use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use tokio::net::TcpListener;
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
async fn spawn_node(actor_id: String) -> Result<(SocketAddr, ReplicaClient<Channel>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let state = ReplicaState::new(actor_id, AutomergeAdapter::new());
    tokio::spawn(
        Server::builder()
            .add_service(ReplicaServer::new(ReplicaService::new(state.clone())))
            .add_service(SyncServer::new(SyncService::new(state)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let client = ReplicaClient::connect(format!("http://{addr}")).await?;
    Ok((addr, client))
}

/// Call `ConnectPeer` for each edge and wait 50 ms for handshakes to settle.
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
    tokio::time::sleep(Duration::from_millis(50)).await;
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
/// Returns elapsed ms from call time to convergence.
async fn wait_for_nodes(
    clients: &mut [ReplicaClient<Channel>],
    indices: &[usize],
    timeout: Duration,
) -> Result<u128> {
    let start = Instant::now();
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

        let converged = fps.iter().all(|fp| !fp.is_empty()) && fps.windows(2).all(|w| w[0] == w[1]);
        if converged {
            return Ok(start.elapsed().as_millis());
        }

        if start.elapsed() >= timeout {
            bail!(
                "nodes {:?} did not converge within {}s",
                indices,
                timeout.as_secs()
            );
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Spawn nodes, wire the topology, apply writes, and wait for convergence.
pub async fn run(config: &TopologyConfig) -> Result<RunResult> {
    let n = config.node_count;
    let mut addrs = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);
    for i in 0..n {
        let (addr, client) = spawn_node(format!("node-{i}")).await?;
        addrs.push(addr);
        clients.push(client);
    }

    let edges = config.connections.edges(n);
    connect_edges(&mut clients, &addrs, &edges).await?;

    for i in 0..config.op_count {
        let target = match &config.write_pattern {
            WritePattern::Concentrated => 0,
            WritePattern::RoundRobin => i % n,
        };
        map_put(&mut clients[target], &format!("k{i}"), &format!("v{i}")).await?;
    }

    let all: Vec<usize> = (0..n).collect();
    let convergence_ms = wait_for_nodes(&mut clients, &all, Duration::from_secs(5)).await?;

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
    let n = config.node_count;
    let mut addrs = Vec::with_capacity(n);
    let mut clients = Vec::with_capacity(n);
    for i in 0..n {
        let (addr, client) = spawn_node(format!("node-{i}")).await?;
        addrs.push(addr);
        clients.push(client);
    }

    // Wire each group internally.
    for group in &config.groups {
        let nodes = &group.nodes;
        let edges: Vec<(usize, usize)> = nodes
            .iter()
            .flat_map(|&i| nodes.iter().filter(move |&&j| j > i).map(move |&j| (i, j)))
            .collect();
        connect_edges(&mut clients, &addrs, &edges).await?;
    }

    // Apply ops to each group; keys are globally unique across groups.
    let mut op_idx = 0usize;
    for group in &config.groups {
        let nodes = &group.nodes;
        let m = nodes.len();
        for i in 0..config.ops_per_group {
            let target = nodes[match &config.write_pattern {
                WritePattern::Concentrated => 0,
                WritePattern::RoundRobin => i % m,
            }];
            map_put(
                &mut clients[target],
                &format!("k{op_idx}"),
                &format!("v{op_idx}"),
            )
            .await?;
            op_idx += 1;
        }
    }

    // Wait for each group to reach internal consistency before healing.
    for group in &config.groups {
        if group.nodes.len() > 1 {
            wait_for_nodes(&mut clients, &group.nodes, Duration::from_secs(5)).await?;
        }
    }

    // Heal: connect remaining cross-group edges to complete a full mesh.
    let intra: Vec<(usize, usize)> = config
        .groups
        .iter()
        .flat_map(|g| {
            g.nodes.iter().flat_map(|&i| {
                g.nodes
                    .iter()
                    .filter(move |&&j| j > i)
                    .map(move |&j| (i, j))
            })
        })
        .collect();
    let heal_edges: Vec<(usize, usize)> = Connections::FullMesh
        .edges(n)
        .into_iter()
        .filter(|e| !intra.contains(e))
        .collect();

    let heal_start = Instant::now();
    connect_edges(&mut clients, &addrs, &heal_edges).await?;

    let all: Vec<usize> = (0..n).collect();
    wait_for_nodes(&mut clients, &all, Duration::from_secs(10)).await?;
    let convergence_ms = heal_start.elapsed().as_millis();

    Ok(RunResult {
        convergence_ms,
        total_ops: config.groups.len() * config.ops_per_group,
    })
}
