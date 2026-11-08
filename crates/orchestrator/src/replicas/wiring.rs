//! Wires replicas together: connects, blocks, and kicks peer links, and resets
//! replicas between trials.

use std::collections::HashMap;

use anyhow::{Context, Result};
use tokio::task::JoinSet;
use tonic::Request;

use common::proto::{Empty, PeerIds, PeerLinkUpdate, PeerRef};

use super::Client;
use super::nodes::{ReplicaEndpoint, node_name};

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

/// Sets the library id of each replica: `ids[i]` goes to `clients[i]`.
///
/// Call this after [`reset_all`] and before anything else, while each document is
/// empty. A later `Load` keeps the id. The calls run in parallel.
pub async fn set_instance_ids(clients: &[Client], ids: &[u128]) -> Result<()> {
    anyhow::ensure!(
        clients.len() == ids.len(),
        "{} ids for {} replicas",
        ids.len(),
        clients.len()
    );
    let mut set: JoinSet<Result<()>> = JoinSet::new();
    for (client, &id) in clients.iter().zip(ids) {
        let mut c = client.clone();
        set.spawn(async move {
            c.set_instance_id(Request::new(common::proto::InstanceId {
                high: (id >> 64) as u64,
                low: id as u64,
            }))
            .await?;
            Ok(())
        });
    }
    join_all(set, "set_instance_id").await
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
