//! [`CrdtAdapter`], the interface that each CRDT backend implements.

use std::collections::BTreeMap;

use crate::{Op, ScalarVal};

/// The number of low bits of an instance id that every adapter keeps. Yrs keeps
/// the fewest: its client ids have 53 bits, and the adapter always sets the top bit.
pub const INSTANCE_ID_BITS: u32 = 52;

/// The library-agnostic interface between the replica scaffolding and a CRDT
/// library.
///
/// The scaffolding times each call and emits the comparable `replicant.*`
/// metrics. Adapters must not emit these metrics.
///
/// All methods take `&mut self`, including the reads. Automerge needs this:
/// its `AutoCommit` commits any pending transaction before it answers a read.
/// Yrs and Loro need only `&self`.
///
/// `Send + 'static` lets the scaffolding keep the adapter in a
/// `Box<dyn CrdtAdapter>` and move it into a Tokio task.
pub trait CrdtAdapter: Send + 'static {
    /// Applies one document operation.
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()>;

    /// Returns the current document heads as sorted, opaque byte vectors.
    ///
    /// The content of an entry depends on the library. The trait makes no
    /// claim about it:
    ///
    /// - Automerge: a 32-byte `ChangeHash`.
    /// - Loro: a (peer, counter) pair from the causal frontier.
    /// - Yrs: a (client, clock) pair from the state vector. This is not a DAG
    ///   frontier.
    ///
    /// Every implementation must sort the result. Then two results compare
    /// equal regardless of the iteration order of the underlying collection.
    fn get_heads(&mut self) -> Vec<Vec<u8>>;

    /// Returns an opaque fingerprint of the current document state.
    ///
    /// The orchestrator compares fingerprints byte for byte to detect
    /// convergence. It does not interpret their content.
    fn state_fingerprint(&mut self) -> Vec<u8>;

    /// Returns the serialized document size in bytes.
    fn doc_size_bytes(&mut self) -> usize;

    /// Generates the next outbound sync message for `peer`, if there is one.
    ///
    /// Returns `None` when this replica has nothing more to send to `peer`,
    /// given what it already sent. This `None` must actually occur.
    ///
    /// The replica calls this method once for each peer after each local op.
    /// It calls it again after each inbound sync message, to reply to the
    /// sender and to relay the change to the other peers. Each message that
    /// a peer receives thus causes more calls on that peer. This exchange
    /// stops only when every call returns `None`. If `None` never comes, the
    /// replicas send messages to each other without end, and the sync message
    /// and byte metrics are wrong.
    ///
    /// Each adapter decides how to track what it sent to `peer`. Automerge
    /// keeps a `sync::State` per peer. Other adapters can keep the peer's last
    /// known state vector, or another record that suits their sync model.
    ///
    /// The returned bytes are opaque. Only the same adapter's
    /// [`Self::sync_receive`] decodes them. The caller cannot tell whether a
    /// message is a handshake or a data delta.
    ///
    /// This method does not detect convergence. The orchestrator compares
    /// [`Self::state_fingerprint`] across replicas for that. `None` only stops
    /// the exchange.
    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>>;

    /// Processes an inbound sync message from `peer`.
    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()>;

    /// Returns true if [`Self::sync_generate`] for `peer` would return a message
    /// now. Changes no state.
    ///
    /// The orchestrator uses this to see that sync between two replicas is
    /// complete: no replica has a message to send, and every sent message has
    /// arrived.
    fn sync_pending(&mut self, peer: &str) -> bool;

    /// Discards the per-peer sync state for `peer`. Does not change the
    /// document.
    ///
    /// After this call, the next [`Self::sync_generate`] starts sync with
    /// `peer` again from nothing. For a request/response protocol, this is a
    /// new handshake. For an adapter that caches the peer's state vector, this
    /// is a diff from the empty state.
    ///
    /// The replica calls this when a new sync stream to `peer` opens. The old
    /// state can record messages that the old stream did not deliver.
    /// Discarding the state is always safe. The worst result is that the
    /// adapter sends again some data that the peer already has.
    ///
    /// A blocked link keeps its stream, so a partition does not call this. The
    /// state that the replicas had before the partition stays, and the heal
    /// sends only what changed.
    ///
    /// Must do nothing if there is no state for `peer`.
    fn sync_reset(&mut self, peer: &str);

    /// Discards all document state and all per-peer sync state. The adapter
    /// is then equal to a new, empty adapter.
    ///
    /// The replica's `Reset` RPC calls this. It lets an externally managed
    /// replica run many trials without a container restart.
    fn reset(&mut self);

    /// Creates the named text object under ROOT, so that every replica has
    /// the same object identity without any sync.
    ///
    /// Call this on an empty document, before any writes. Each replica that
    /// does so gets the same object. Text workloads on partitioned replicas
    /// need this. If each side instead creates the object on its first write,
    /// the two creations are concurrent puts of the same map key. The merge
    /// keeps only one of them, and all text on the other side is lost.
    ///
    /// Constraints:
    ///
    /// - Must succeed and do nothing if the object already exists, whether it
    ///   was created locally or received through sync.
    /// - Must fail if the document has changes but does not have this object.
    ///   Do not try to create a shared identity from a later state.
    ///
    /// If the library names root objects globally, this can be a lookup that
    /// does nothing. Yrs and Loro both work this way. Yrs's
    /// `get_or_insert_text` gives the same identity for the same name. Loro
    /// derives a root container's id from `(name, type)`, and the container
    /// always exists. In both cases there is no creation op, so no concurrent
    /// creation can occur.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()>;

    /// Returns the length in characters of the named text object.
    ///
    /// The orchestrator uses this to check results after convergence. For an
    /// insert-only workload, the final length must equal the total op count.
    /// This proves that the merge discarded no replica's inserts. Equal
    /// fingerprints alone cannot prove this.
    ///
    /// Returns an error if the text object does not exist, where the library
    /// can express that. Loro cannot: every `(name, Text)` root container
    /// exists, so an unused name returns `Ok(0)`. Thus callers must use this
    /// as a length query, not as an existence check. The orchestrator's
    /// `verify_text_length` does this. With a wrong object name, it fails
    /// on a length mismatch instead of an error.
    fn text_length(&mut self, obj: &str) -> anyhow::Result<usize>;

    /// Sets the id that the library uses for this replica's ops. Call this on
    /// an empty document.
    ///
    /// The library breaks ties between concurrent ops with this id. It is random
    /// by default, so a replay can have a different result on each run. A replay
    /// sets an id for each agent. The adapter keeps as many low bits of `id` as
    /// its library can hold, and keeps the encoded width of a random id. Ids must
    /// differ in their low [`INSTANCE_ID_BITS`] bits.
    fn set_instance_id(&mut self, id: u128) -> anyhow::Result<()>;

    /// Creates the named list object under ROOT, so that every replica has the
    /// same object identity without any sync.
    ///
    /// The contract is the same as [`Self::ensure_text`]. Call this before
    /// concurrent list writes on replicas that have not synced.
    fn ensure_list(&mut self, obj: &str) -> anyhow::Result<()>;

    /// Returns the content of the named text object.
    ///
    /// The replay driver compares this with the expected content of a
    /// workload. Returns an error if the object does not exist, where the
    /// library can express that. See [`Self::text_length`].
    fn read_text(&mut self, obj: &str) -> anyhow::Result<String>;

    /// Returns the content of the named list object. See [`Self::read_text`].
    fn read_list(&mut self, obj: &str) -> anyhow::Result<Vec<ScalarVal>>;

    /// Returns the content of the named map object. An empty `obj` names ROOT.
    ///
    /// An adapter stores [`ScalarVal::Uint`] as a signed integer if the library
    /// has no unsigned type. So a replay workload must use
    /// [`ScalarVal::Int`] to compare the same value on every adapter.
    fn read_map(&mut self, obj: &str) -> anyhow::Result<BTreeMap<String, ScalarVal>>;

    /// Applies `ops` in one library transaction. Returns the update that holds
    /// these ops and no others.
    ///
    /// [`Self::apply_updates`] on another replica accepts the update. The
    /// update contains the deletes of this transaction. Returns an empty vector
    /// if `ops` is empty.
    ///
    /// If this returns an error, the document state is not specified. Call
    /// [`Self::reset`] before the next use.
    fn apply_txn(&mut self, ops: &[Op]) -> anyhow::Result<Vec<u8>>;

    /// Applies updates that [`Self::apply_txn`] returned on another replica.
    ///
    /// The caller must give the updates in causal order. An adapter can use a
    /// batch path for all `updates` in one call. Each adapter uses the same path
    /// as its crdt-benchmarks factory, so that the two can be compared: Yrs uses
    /// one transaction, Loro uses a batch import, and Automerge applies one
    /// update at a time.
    fn apply_updates(&mut self, updates: &[&[u8]]) -> anyhow::Result<()>;

    /// Returns the full encoded document. [`Self::load`] accepts it.
    ///
    /// [`Self::doc_size_bytes`] returns the length of this encoding.
    fn encode(&mut self) -> Vec<u8>;

    /// Replaces the document with one that [`Self::encode`] returned.
    ///
    /// This discards all per-peer sync state. The adapter takes a new instance
    /// id, so it is a different replica from the one that encoded the document.
    fn load(&mut self, bytes: &[u8]) -> anyhow::Result<()>;
}
