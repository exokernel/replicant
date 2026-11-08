//! A target is one replica that the driver can use.
//!
//! Two kinds exist. [`Local`] calls an adapter in this process, so nothing but
//! the library is in the timed path. [`Remote`] calls a replica over gRPC. That
//! replica can be in a container. A remote call adds transport time, so a remote
//! target also reports the time inside the adapter.

use std::rc::Rc;

use anyhow::{Context, Result, ensure};
use common::proto::{self, replica_client::ReplicaClient};
use common::{CrdtAdapter, Op, ScalarVal};
use replica::adapter::Crdt;
use tokio::runtime::Runtime;
use tonic::Request;
use tonic::transport::Channel;

use crate::oracle::Content;
use crate::trace::ContentKind;

/// One replica. Each method matches the [`CrdtAdapter`] method of the same name.
pub trait Target {
    /// Empties the document.
    fn reset(&mut self) -> Result<()>;
    /// Sets the library's id for this replica. Call this on an empty document.
    fn set_instance_id(&mut self, id: u128) -> Result<()>;
    /// Creates the object that a trace of this `kind` edits.
    fn ensure(&mut self, kind: ContentKind) -> Result<()>;
    /// Applies `ops` in one transaction and returns the update.
    fn apply_txn(&mut self, ops: &[Op]) -> Result<Vec<u8>>;
    /// Applies updates in causal order, in one call.
    fn apply_updates(&mut self, updates: &[&[u8]]) -> Result<()>;
    /// Reads the object that a trace of this `kind` edits.
    fn read(&mut self, kind: ContentKind) -> Result<Content>;
    /// Returns the full encoded document.
    fn encode(&mut self) -> Result<Vec<u8>>;
    /// Replaces the document with an encoded one.
    fn load(&mut self, bytes: &[u8]) -> Result<()>;
    /// Returns the adapter time since the last call, in nanoseconds. Returns
    /// `None` if this target has no transport to separate from the adapter.
    fn take_adapter_ns(&mut self) -> Option<u64>;
}

/// An adapter in this process.
pub struct Local(Box<dyn CrdtAdapter>);

impl Target for Local {
    fn reset(&mut self) -> Result<()> {
        self.0.reset();
        Ok(())
    }

    fn set_instance_id(&mut self, id: u128) -> Result<()> {
        self.0.set_instance_id(id)
    }

    fn ensure(&mut self, kind: ContentKind) -> Result<()> {
        match kind {
            ContentKind::Text => self.0.ensure_text(kind.obj()),
            ContentKind::List => self.0.ensure_list(kind.obj()),
            ContentKind::Map => Ok(()),
        }
    }

    fn apply_txn(&mut self, ops: &[Op]) -> Result<Vec<u8>> {
        self.0.apply_txn(ops)
    }

    fn apply_updates(&mut self, updates: &[&[u8]]) -> Result<()> {
        self.0.apply_updates(updates)
    }

    fn read(&mut self, kind: ContentKind) -> Result<Content> {
        Ok(match kind {
            ContentKind::Text => Content::Text(self.0.read_text(kind.obj())?),
            ContentKind::List => Content::List(self.0.read_list(kind.obj())?),
            ContentKind::Map => Content::Map(self.0.read_map(kind.obj())?),
        })
    }

    fn encode(&mut self) -> Result<Vec<u8>> {
        Ok(self.0.encode())
    }

    fn load(&mut self, bytes: &[u8]) -> Result<()> {
        self.0.load(bytes)
    }

    fn take_adapter_ns(&mut self) -> Option<u64> {
        None
    }
}

/// A replica behind gRPC.
///
/// The driver is synchronous, so each call blocks on a shared single-thread
/// runtime.
pub struct Remote {
    /// The gRPC client of this replica.
    client: ReplicaClient<Channel>,
    /// The runtime that runs the client calls.
    runtime: Rc<Runtime>,
    /// The adapter time since the last [`Target::take_adapter_ns`].
    adapter_ns: u64,
}

/// Blocks on one RPC of a [`Remote`], and returns the reply body.
macro_rules! rpc {
    ($remote:ident . $method:ident ( $request:expr )) => {
        $remote
            .runtime
            .block_on($remote.client.$method(Request::new($request)))?
            .into_inner()
    };
}

/// Does what [`rpc!`] does, and adds the reply's adapter time to the total.
macro_rules! timed_rpc {
    ($remote:ident . $method:ident ( $request:expr )) => {{
        let reply = rpc!($remote.$method($request));
        $remote.adapter_ns += reply.adapter_ns;
        reply
    }};
}

/// Returns the request that names the object of a trace of this `kind`.
fn obj_ref(kind: ContentKind) -> proto::ObjRef {
    proto::ObjRef {
        obj: kind.obj().to_owned(),
    }
}

impl Target for Remote {
    fn reset(&mut self) -> Result<()> {
        rpc!(self.reset(proto::Empty {}));
        Ok(())
    }

    fn set_instance_id(&mut self, id: u128) -> Result<()> {
        let (high, low) = ((id >> 64) as u64, id as u64);
        rpc!(self.set_instance_id(proto::InstanceId { high, low }));
        Ok(())
    }

    fn ensure(&mut self, kind: ContentKind) -> Result<()> {
        match kind {
            ContentKind::Text => {
                rpc!(self.ensure_text(obj_ref(kind)));
            }
            ContentKind::List => {
                rpc!(self.ensure_list(obj_ref(kind)));
            }
            ContentKind::Map => {}
        }
        Ok(())
    }

    fn apply_txn(&mut self, ops: &[Op]) -> Result<Vec<u8>> {
        let ops = ops
            .iter()
            .map(proto::OpRequest::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(timed_rpc!(self.apply_txn(proto::TxnRequest { ops })).update)
    }

    fn apply_updates(&mut self, updates: &[&[u8]]) -> Result<()> {
        let updates = updates.iter().map(|update| update.to_vec()).collect();
        timed_rpc!(self.apply_updates(proto::UpdatesRequest { updates }));
        Ok(())
    }

    fn read(&mut self, kind: ContentKind) -> Result<Content> {
        Ok(match kind {
            ContentKind::Text => Content::Text(timed_rpc!(self.read_text(obj_ref(kind))).text),
            ContentKind::List => Content::List(
                timed_rpc!(self.read_list(obj_ref(kind)))
                    .values
                    .into_iter()
                    .map(ScalarVal::try_from)
                    .collect::<Result<_>>()?,
            ),
            ContentKind::Map => Content::Map(
                timed_rpc!(self.read_map(obj_ref(kind)))
                    .values
                    .into_iter()
                    .map(|(k, v)| Ok((k, ScalarVal::try_from(v)?)))
                    .collect::<Result<_>>()?,
            ),
        })
    }

    fn encode(&mut self) -> Result<Vec<u8>> {
        Ok(timed_rpc!(self.encode(proto::Empty {})).state)
    }

    fn load(&mut self, bytes: &[u8]) -> Result<()> {
        let state = bytes.to_vec();
        timed_rpc!(self.load(proto::LoadRequest { state }));
        Ok(())
    }

    fn take_adapter_ns(&mut self) -> Option<u64> {
        Some(std::mem::take(&mut self.adapter_ns))
    }
}

/// Where the replicas of a run come from.
pub enum Lane {
    /// New adapters in this process, for one library.
    InProcess(Crdt),
    /// Replicas that already run.
    Remote {
        /// The library that the replicas use. The caller states it, because a
        /// replica does not report it.
        crdt: Crdt,
        /// One connected client per replica.
        clients: Vec<ReplicaClient<Channel>>,
        /// The runtime that the clients use.
        runtime: Rc<Runtime>,
    },
}

impl Lane {
    /// Connects to the replicas at `addrs`, each in `host:port` form.
    pub fn connect(crdt: Crdt, addrs: &[String]) -> Result<Self> {
        ensure!(
            !addrs.is_empty(),
            "the remote lane needs at least one replica"
        );
        let runtime = Rc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("building the client runtime")?,
        );
        let mut clients = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let client = runtime
                .block_on(common::connect_replica(addr))
                .with_context(|| format!("connecting to the replica at {addr}"))?;
            clients.push(client);
        }
        Ok(Self::Remote {
            crdt,
            clients,
            runtime,
        })
    }

    /// Returns the library that this lane runs.
    #[must_use]
    pub fn crdt(&self) -> Crdt {
        match self {
            Self::InProcess(crdt) | Self::Remote { crdt, .. } => *crdt,
        }
    }

    /// Returns the lane name for the result rows.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::InProcess(_) => "in_process",
            Self::Remote { .. } => "remote",
        }
    }

    /// Returns the largest number of replicas this lane can give, if it has a
    /// limit.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        match self {
            Self::InProcess(_) => None,
            Self::Remote { clients, .. } => Some(clients.len()),
        }
    }

    /// Returns `n` empty replicas.
    ///
    /// A remote lane also resets the replicas that it does not use.
    pub fn acquire(&self, n: usize) -> Result<Vec<Box<dyn Target>>> {
        match self {
            Self::InProcess(crdt) => Ok((0..n)
                .map(|_| Box::new(Local(crdt.build())) as Box<dyn Target>)
                .collect()),
            Self::Remote {
                clients, runtime, ..
            } => {
                ensure!(
                    n <= clients.len(),
                    "needs {n} replicas, the lane has {}",
                    clients.len()
                );
                let mut targets: Vec<Box<dyn Target>> = Vec::with_capacity(n);
                for (i, client) in clients.iter().enumerate() {
                    let mut target = Remote {
                        client: client.clone(),
                        runtime: runtime.clone(),
                        adapter_ns: 0,
                    };
                    target.reset()?;
                    if i < n {
                        targets.push(Box::new(target));
                    }
                }
                Ok(targets)
            }
        }
    }
}
