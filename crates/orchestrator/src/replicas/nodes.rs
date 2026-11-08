//! Gets replicas: names them, spawns them in this process or connects to external
//! ones, and watches the in-process tasks.

use anyhow::{Context, Result, bail};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;

use common::NodeId;
use replica::adapter::Crdt;
use replica::server::{self, ReplicaState};

use super::Client;
use super::convergence::fingerprint;
use super::wiring::reset_all;

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

/// Returns the name of node `i`, which is `node-{i}`. It is both the actor ID
/// and the peer ID other replicas use for it.
///
/// This naming scheme is shared with code outside this crate. The Kubernetes
/// StatefulSet is named `node`, so its pod ordinals produce the same names, and
/// scenarios are wired by index on that assumption. Do not change the format.
pub fn node_name(i: usize) -> String {
    format!("node-{i}")
}

/// Returns `i` for the node name `node-{i}`. See [`node_name`].
pub fn node_index(name: &str) -> Option<usize> {
    name.strip_prefix("node-")?.parse().ok()
}

/// Returns the actor ID for node `i`. See [`node_name`].
fn node_id(i: usize) -> Result<NodeId> {
    NodeId::new(node_name(i))
}

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
    let client = common::connect_replica(&addr.to_string()).await?;
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
        let mut client = common::connect_replica(&ep.client_addr)
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
