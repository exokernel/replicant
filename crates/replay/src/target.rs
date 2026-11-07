//! A target is one replica that the driver can use.
//!
//! Two kinds exist. [`Local`] calls an adapter in this process, so nothing but
//! the library is in the timed path. [`Remote`] calls a replica over gRPC. That
//! replica can be in a container. A remote call adds transport time, so a remote
//! target also reports the time inside the adapter.

use std::rc::Rc;

use anyhow::{Context, Result, ensure};
use common::proto::{self, replica_client::ReplicaClient};
use common::{CrdtAdapter, MAX_RPC_BYTES, Op, ScalarVal};
use replica::adapter::Crdt;
use tokio::runtime::Runtime;
use tonic::Request;
use tonic::transport::Channel;

use crate::oracle::Content;
use crate::trace::{ContentKind, LIST_OBJ, MAP_OBJ, TEXT_OBJ};

/// One replica. Each method matches the [`CrdtAdapter`] method of the same name.
pub trait Target {
    /// Empties the document.
    fn reset(&mut self) -> Result<()>;
    /// Sets the library's id for this replica. Call this on an empty document.
    fn set_instance_id(&mut self, id: u32) -> Result<()>;
    /// Creates the object that a trace of this `kind` edits.
    fn ensure(&mut self, kind: ContentKind) -> Result<()>;
    /// Applies `ops` in one transaction and returns the update.
    fn apply_txn(&mut self, ops: &[Op]) -> Result<Vec<u8>>;
    /// Applies updates in causal order, in one call.
    fn apply_updates(&mut self, updates: &[Vec<u8>]) -> Result<()>;
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

    fn set_instance_id(&mut self, id: u32) -> Result<()> {
        self.0.set_instance_id(id)
    }

    fn ensure(&mut self, kind: ContentKind) -> Result<()> {
        match kind {
            ContentKind::Text => self.0.ensure_text(TEXT_OBJ),
            ContentKind::List => self.0.ensure_list(LIST_OBJ),
            ContentKind::Map => Ok(()),
        }
    }

    fn apply_txn(&mut self, ops: &[Op]) -> Result<Vec<u8>> {
        self.0.apply_txn(ops)
    }

    fn apply_updates(&mut self, updates: &[Vec<u8>]) -> Result<()> {
        self.0.apply_updates(updates)
    }

    fn read(&mut self, kind: ContentKind) -> Result<Content> {
        Ok(match kind {
            ContentKind::Text => Content::Text(self.0.read_text(TEXT_OBJ)?),
            ContentKind::List => Content::List(self.0.read_list(LIST_OBJ)?),
            ContentKind::Map => Content::Map(self.0.read_map(MAP_OBJ)?),
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

impl Remote {
    /// Returns a request that names the object of a trace of this `kind`.
    fn obj(kind: ContentKind) -> Request<proto::ObjRef> {
        let obj = match kind {
            ContentKind::Text => TEXT_OBJ,
            ContentKind::List => LIST_OBJ,
            ContentKind::Map => MAP_OBJ,
        };
        Request::new(proto::ObjRef {
            obj: obj.to_owned(),
        })
    }
}

impl Target for Remote {
    fn reset(&mut self) -> Result<()> {
        self.runtime
            .block_on(self.client.reset(Request::new(proto::Empty {})))?;
        Ok(())
    }

    fn set_instance_id(&mut self, id: u32) -> Result<()> {
        let request = Request::new(proto::InstanceId { id });
        self.runtime
            .block_on(self.client.set_instance_id(request))?;
        Ok(())
    }

    fn ensure(&mut self, kind: ContentKind) -> Result<()> {
        match kind {
            ContentKind::Text => {
                self.runtime
                    .block_on(self.client.ensure_text(Self::obj(kind)))?;
            }
            ContentKind::List => {
                self.runtime
                    .block_on(self.client.ensure_list(Self::obj(kind)))?;
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
        let reply = self
            .runtime
            .block_on(
                self.client
                    .apply_txn(Request::new(proto::TxnRequest { ops })),
            )?
            .into_inner();
        self.adapter_ns += reply.adapter_ns;
        Ok(reply.update)
    }

    fn apply_updates(&mut self, updates: &[Vec<u8>]) -> Result<()> {
        let request = Request::new(proto::UpdatesRequest {
            updates: updates.to_vec(),
        });
        let reply = self
            .runtime
            .block_on(self.client.apply_updates(request))?
            .into_inner();
        self.adapter_ns += reply.adapter_ns;
        Ok(())
    }

    fn read(&mut self, kind: ContentKind) -> Result<Content> {
        Ok(match kind {
            ContentKind::Text => {
                let reply = self
                    .runtime
                    .block_on(self.client.read_text(Self::obj(kind)))?
                    .into_inner();
                self.adapter_ns += reply.adapter_ns;
                Content::Text(reply.text)
            }
            ContentKind::List => {
                let reply = self
                    .runtime
                    .block_on(self.client.read_list(Self::obj(kind)))?
                    .into_inner();
                self.adapter_ns += reply.adapter_ns;
                Content::List(
                    reply
                        .values
                        .into_iter()
                        .map(ScalarVal::try_from)
                        .collect::<Result<_>>()?,
                )
            }
            ContentKind::Map => {
                let reply = self
                    .runtime
                    .block_on(self.client.read_map(Self::obj(kind)))?
                    .into_inner();
                self.adapter_ns += reply.adapter_ns;
                Content::Map(
                    reply
                        .values
                        .into_iter()
                        .map(|(k, v)| Ok((k, ScalarVal::try_from(v)?)))
                        .collect::<Result<_>>()?,
                )
            }
        })
    }

    fn encode(&mut self) -> Result<Vec<u8>> {
        let reply = self
            .runtime
            .block_on(self.client.encode(Request::new(proto::Empty {})))?
            .into_inner();
        self.adapter_ns += reply.adapter_ns;
        Ok(reply.state)
    }

    fn load(&mut self, bytes: &[u8]) -> Result<()> {
        let request = Request::new(proto::LoadRequest {
            state: bytes.to_vec(),
        });
        let reply = self
            .runtime
            .block_on(self.client.load(request))?
            .into_inner();
        self.adapter_ns += reply.adapter_ns;
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
                .block_on(ReplicaClient::connect(format!("http://{addr}")))
                .with_context(|| format!("connecting to the replica at {addr}"))?
                .max_decoding_message_size(MAX_RPC_BYTES)
                .max_encoding_message_size(MAX_RPC_BYTES);
            clients.push(client);
        }
        Ok(Self::Remote {
            crdt,
            clients,
            runtime,
        })
    }

    /// Returns the library that this lane runs.
    pub fn crdt(&self) -> Crdt {
        match self {
            Self::InProcess(crdt) | Self::Remote { crdt, .. } => *crdt,
        }
    }

    /// Returns the lane name for the result rows.
    pub fn name(&self) -> &'static str {
        match self {
            Self::InProcess(_) => "in_process",
            Self::Remote { .. } => "remote",
        }
    }

    /// Returns the largest number of replicas this lane can give, if it has a
    /// limit.
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
