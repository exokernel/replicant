//! Shared types for the replicant project.
//!
//! - [`proto`]: generated tonic/prost code for the `replicant.v1` gRPC API.
//! - [`NodeId`]: the identity of a replica.
//! - [`ScalarVal`] and [`Op`]: library-agnostic values and operations. Each
//!   has a `TryFrom` impl from its proto type.
//! - [`CrdtAdapter`]: the trait that each CRDT backend (Automerge, Yrs, Loro)
//!   implements. The replica scaffolding calls it.

use anyhow::Context as _;

/// Generated protobuf types for the `replicant.v1` package.
pub mod proto {
    tonic::include_proto!("replicant.v1");
}

// ── Node identity ──────────────────────────────────────────────────────────

/// The stable identity of a replica, for example `node-0` or `node-1`.
///
/// The same type names a replica's own `actor_id` and the peers it connects
/// to. Both are in one namespace.
///
/// # Invariant
///
/// A `NodeId` is always a legal HTTP header value. The replica sends it as the
/// `x-peer-id` gRPC metadata header on each outbound sync stream.
/// [`NodeId::new`] is the only constructor, and it checks this.
///
/// # Lookup by `&str`
///
/// `NodeId` implements `Borrow<str>`. You can query a `HashMap<NodeId, _>`
/// with a plain `&str`. This lets [`CrdtAdapter`] keep its `peer: &str`
/// parameters. Adapters use the id only as an opaque map key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(String);

impl NodeId {
    /// Validates and wraps a node identifier.
    ///
    /// Rejects the empty string and any value that is not a legal HTTP header
    /// value. A bad id thus fails where it enters the system, not at the first
    /// peer connection.
    pub fn new(id: impl Into<String>) -> anyhow::Result<Self> {
        let id = id.into();
        if id.is_empty() {
            anyhow::bail!("node id must not be empty");
        }
        id.parse::<tonic::metadata::AsciiMetadataValue>()
            .with_context(|| format!("node id is not a valid HTTP-header value: {id:?}"))?;
        Ok(Self(id))
    }

    /// Returns the id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the id as a gRPC metadata value, for the `x-peer-id` header.
    ///
    /// Cannot fail: [`NodeId::new`] already parsed the id as a header value.
    pub fn header_value(&self) -> tonic::metadata::AsciiMetadataValue {
        self.0
            .parse()
            .expect("NodeId validated as a header value at construction")
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Lets you query `HashMap<NodeId, V>` and `HashSet<NodeId>` with `&str`.
///
/// This is correct only because `NodeId`'s `Hash` and `Eq` delegate to the
/// inner `String`. Do not give `NodeId` a custom `Hash` or `Eq`.
impl std::borrow::Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl From<NodeId> for String {
    fn from(id: NodeId) -> String {
        id.0
    }
}

impl TryFrom<String> for NodeId {
    type Error = anyhow::Error;
    fn try_from(s: String) -> anyhow::Result<Self> {
        Self::new(s)
    }
}

// ── Scalar values ──────────────────────────────────────────────────────────

/// A scalar value that can be stored in a CRDT document.
///
/// There is no float variant, so `ScalarVal` implements `Eq`. Adding a float
/// would remove `Eq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarVal {
    /// A UTF-8 string.
    Str(String),
    /// An unsigned integer. Yrs and Loro store it as `i64`, so values above
    /// `i64::MAX` do not round-trip there.
    Uint(u64),
    /// A signed integer.
    Int(i64),
    /// A boolean.
    Bool(bool),
    /// An opaque byte string.
    Bytes(Vec<u8>),
}

impl From<&str> for ScalarVal {
    fn from(s: &str) -> Self {
        Self::Str(s.to_owned())
    }
}
impl From<String> for ScalarVal {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}
impl From<u64> for ScalarVal {
    fn from(n: u64) -> Self {
        Self::Uint(n)
    }
}
impl From<i64> for ScalarVal {
    fn from(n: i64) -> Self {
        Self::Int(n)
    }
}
impl From<bool> for ScalarVal {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}
impl From<Vec<u8>> for ScalarVal {
    fn from(b: Vec<u8>) -> Self {
        Self::Bytes(b)
    }
}

impl TryFrom<proto::ScalarValue> for ScalarVal {
    type Error = anyhow::Error;

    fn try_from(v: proto::ScalarValue) -> anyhow::Result<Self> {
        use proto::scalar_value::Value;
        match v.value.context("ScalarValue missing value field")? {
            Value::StrVal(s) => Ok(Self::Str(s)),
            Value::UintVal(n) => Ok(Self::Uint(n)),
            Value::IntVal(n) => Ok(Self::Int(n)),
            Value::BoolVal(b) => Ok(Self::Bool(b)),
            Value::BytesVal(b) => Ok(Self::Bytes(b)),
        }
    }
}

// ── Op model ───────────────────────────────────────────────────────────────

/// A CRDT document operation.
///
/// `obj` names a top-level object under ROOT. The empty string refers to ROOT
/// itself, which is a map. Map ops accept the empty name. List and text ops
/// must name an object. The adapter creates a named list or text object on
/// first access.
///
/// Indices are `usize`, as in the Automerge API. The conversion from proto
/// rejects a `u64` that does not fit in `usize`. It does not truncate.
#[derive(Debug, Clone)]
pub enum Op {
    /// Sets `key` to `value` in a map.
    MapPut {
        /// The map. Empty means ROOT.
        obj: String,
        /// The key to set.
        key: String,
        /// The new value.
        value: ScalarVal,
    },
    /// Removes `key` from a map.
    MapDelete {
        /// The map. Empty means ROOT.
        obj: String,
        /// The key to remove.
        key: String,
    },
    /// Inserts one value into a list.
    ListInsert {
        /// The list. Must not be empty.
        obj: String,
        /// The position of the new element. The element at `index` and all
        /// later elements move one position right.
        index: usize,
        /// The value to insert.
        value: ScalarVal,
    },
    /// Removes one element from a list.
    ListDelete {
        /// The list. Must not be empty.
        obj: String,
        /// The position of the element to remove.
        index: usize,
    },
    /// Deletes and inserts list elements in one operation. Matches
    /// Automerge's `Transactable::splice`.
    ListSplice {
        /// The list. Must not be empty.
        obj: String,
        /// The position where the deletion starts and the insertion goes.
        pos: usize,
        /// The number of elements to delete, starting at `pos`.
        del_count: usize,
        /// The values to insert at `pos`, after the deletion.
        values: Vec<ScalarVal>,
    },
    /// Deletes and inserts text in one operation. Matches Automerge's
    /// `Transactable::splice_text`.
    TextSplice {
        /// The text object. Must not be empty.
        obj: String,
        /// The character position where the deletion starts and the insertion
        /// goes.
        pos: usize,
        /// The number of characters to delete, starting at `pos`.
        del_count: usize,
        /// The text to insert at `pos`, after the deletion.
        insert: String,
    },
}

impl Op {
    /// Returns the operation name. The replica uses it as the `op` metric
    /// attribute.
    pub fn name(&self) -> &'static str {
        match self {
            Op::MapPut { .. } => "map_put",
            Op::MapDelete { .. } => "map_delete",
            Op::ListInsert { .. } => "list_insert",
            Op::ListDelete { .. } => "list_delete",
            Op::ListSplice { .. } => "list_splice",
            Op::TextSplice { .. } => "text_splice",
        }
    }
}

/// Converts a proto `u64` index to `usize`.
///
/// Fails if the value does not fit, on targets where `usize` is narrower than
/// 64 bits. A truncated index would address the wrong element.
fn to_index(v: u64, field: &str) -> anyhow::Result<usize> {
    usize::try_from(v).with_context(|| format!("{field} ({v}) does not fit in usize"))
}

impl TryFrom<proto::OpRequest> for Op {
    type Error = anyhow::Error;

    fn try_from(req: proto::OpRequest) -> anyhow::Result<Self> {
        use proto::op_request::Op as P;

        match req.op.context("OpRequest missing op field")? {
            P::MapPut(p) => Ok(Op::MapPut {
                obj: p.obj,
                key: p.key,
                value: p.value.context("MapPut missing value")?.try_into()?,
            }),
            P::MapDelete(p) => Ok(Op::MapDelete {
                obj: p.obj,
                key: p.key,
            }),
            P::ListInsert(p) => Ok(Op::ListInsert {
                obj: p.obj,
                index: to_index(p.index, "ListInsert.index")?,
                value: p.value.context("ListInsert missing value")?.try_into()?,
            }),
            P::ListDelete(p) => Ok(Op::ListDelete {
                obj: p.obj,
                index: to_index(p.index, "ListDelete.index")?,
            }),
            P::ListSplice(p) => {
                let values = p
                    .values
                    .into_iter()
                    .map(ScalarVal::try_from)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(Op::ListSplice {
                    obj: p.obj,
                    pos: to_index(p.pos, "ListSplice.pos")?,
                    // `del_count` is a proto uint32. Widen it so that one
                    // helper converts every index-like field.
                    del_count: to_index(p.del_count.into(), "ListSplice.del_count")?,
                    values,
                })
            }
            P::TextSplice(p) => Ok(Op::TextSplice {
                obj: p.obj,
                pos: to_index(p.pos, "TextSplice.pos")?,
                del_count: to_index(p.del_count.into(), "TextSplice.del_count")?,
                insert: p.insert,
            }),
        }
    }
}

// ── CrdtAdapter trait ──────────────────────────────────────────────────────

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
    /// given what it already sent. This `None` must actually occur. The
    /// replica's `flush_to_peers` loop calls this method until it returns
    /// `None`. If `None` never comes, the loop never stops and the sync
    /// message and byte metrics are wrong.
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
    /// the push loop.
    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>>;

    /// Processes an inbound sync message from `peer`.
    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()>;

    /// Discards the per-peer sync state for `peer`. Does not change the
    /// document.
    ///
    /// After this call, the next [`Self::sync_generate`] starts sync with
    /// `peer` again from nothing. For a request/response protocol, this is a
    /// new handshake. For an adapter that caches the peer's state vector, this
    /// is a diff from the empty state.
    ///
    /// The orchestrator calls this when it heals a simulated partition. While
    /// a link is blocked, the replica can generate a message that is then
    /// dropped. The per-peer state then records data that the peer never
    /// received, and sync can stop. Discarding the state is always safe. The
    /// worst result is that the adapter sends again some data that the peer
    /// already has.
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
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{
        ListDelete, ListInsert, ListSplice, MapDelete, MapPut, OpRequest, ScalarValue, TextSplice,
        op_request, scalar_value,
    };

    // ── NodeId ─────────────────────────────────────────────────────────────

    #[test]
    fn node_id_accepts_the_scheme_the_system_actually_uses() {
        for name in ["node-0", "node-11", "replica-0"] {
            let id = NodeId::new(name).unwrap();
            assert_eq!(id.as_str(), name);
            assert_eq!(id.to_string(), name);
            // The header conversion must not panic for any id that `new`
            // accepts.
            let _ = id.header_value();
        }
    }

    #[test]
    fn node_id_rejects_what_would_break_the_peer_header() {
        // The empty string, and control characters that are illegal in an
        // HTTP header value. A newline is the important case: it could inject
        // a second header. Horizontal tab and bytes 0x80-0xFF are legal in a
        // header value, so they are not in this list. See the next test.
        for bad in ["", "node\n0", "node\r\n0", "node\u{0}0"] {
            assert!(
                NodeId::new(bad).is_err(),
                "should have rejected {bad:?} as a node id"
            );
        }
    }

    /// `NodeId` accepts non-ASCII ids. Bytes 0x80-0xFF are legal obs-text in
    /// an HTTP header value, so the id can still go in `x-peer-id`.
    ///
    /// The name `AsciiMetadataValue` suggests a stricter rule. This test
    /// records the actual rule.
    #[test]
    fn node_id_allows_non_ascii_because_the_header_does() {
        let id = NodeId::new("nodé-0").unwrap();
        let _ = id.header_value();
    }

    #[test]
    fn node_id_maps_can_be_probed_with_a_str() {
        // The `Borrow<str>` impl makes this work. Without it, each lookup
        // would allocate a `NodeId`.
        let mut m = std::collections::HashMap::new();
        m.insert(NodeId::new("node-1").unwrap(), 7);
        assert_eq!(m.get("node-1"), Some(&7));
        assert_eq!(m.get("node-2"), None);
    }

    // ── ScalarVal conversions ──────────────────────────────────────────────

    fn scalar(v: scalar_value::Value) -> ScalarValue {
        ScalarValue { value: Some(v) }
    }

    #[test]
    fn scalar_val_roundtrip_each_variant() {
        let cases: &[(scalar_value::Value, ScalarVal)] = &[
            (
                scalar_value::Value::StrVal("hi".into()),
                ScalarVal::Str("hi".into()),
            ),
            (scalar_value::Value::UintVal(42), ScalarVal::Uint(42)),
            (scalar_value::Value::IntVal(-7), ScalarVal::Int(-7)),
            (scalar_value::Value::BoolVal(true), ScalarVal::Bool(true)),
            (
                scalar_value::Value::BytesVal(vec![1, 2, 3]),
                ScalarVal::Bytes(vec![1, 2, 3]),
            ),
        ];
        for (wire, expected) in cases {
            let got = ScalarVal::try_from(scalar(wire.clone())).unwrap();
            assert_eq!(got, *expected);
        }
    }

    #[test]
    fn scalar_val_missing_value_field_is_error() {
        let err = ScalarVal::try_from(ScalarValue { value: None }).unwrap_err();
        assert!(err.to_string().contains("missing value field"), "{err}");
    }

    // ── Op conversions ─────────────────────────────────────────────────────

    fn op_req(op: op_request::Op) -> OpRequest {
        OpRequest { op: Some(op) }
    }

    #[test]
    fn op_map_put_ok() {
        let req = op_req(op_request::Op::MapPut(MapPut {
            obj: "doc".into(),
            key: "k".into(),
            value: Some(scalar(scalar_value::Value::UintVal(1))),
        }));
        let op = Op::try_from(req).unwrap();
        assert!(
            matches!(op, Op::MapPut { obj, key, value: ScalarVal::Uint(1) }
            if obj == "doc" && key == "k")
        );
    }

    #[test]
    fn op_map_put_missing_value_is_error() {
        let req = op_req(op_request::Op::MapPut(MapPut {
            obj: String::new(),
            key: "k".into(),
            value: None,
        }));
        let err = Op::try_from(req).unwrap_err();
        assert!(err.to_string().contains("MapPut missing value"), "{err}");
    }

    #[test]
    fn op_map_delete_ok() {
        let req = op_req(op_request::Op::MapDelete(MapDelete {
            obj: String::new(),
            key: "k".into(),
        }));
        assert!(matches!(Op::try_from(req).unwrap(), Op::MapDelete { key, .. } if key == "k"));
    }

    #[test]
    fn op_list_insert_ok() {
        let req = op_req(op_request::Op::ListInsert(ListInsert {
            obj: "list".into(),
            index: 0,
            value: Some(scalar(scalar_value::Value::BoolVal(false))),
        }));
        assert!(matches!(
            Op::try_from(req).unwrap(),
            Op::ListInsert {
                index: 0,
                value: ScalarVal::Bool(false),
                ..
            }
        ));
    }

    #[test]
    fn op_list_insert_missing_value_is_error() {
        let req = op_req(op_request::Op::ListInsert(ListInsert {
            obj: String::new(),
            index: 0,
            value: None,
        }));
        let err = Op::try_from(req).unwrap_err();
        assert!(
            err.to_string().contains("ListInsert missing value"),
            "{err}"
        );
    }

    #[test]
    fn op_list_delete_ok() {
        let req = op_req(op_request::Op::ListDelete(ListDelete {
            obj: "list".into(),
            index: 3,
        }));
        assert!(matches!(
            Op::try_from(req).unwrap(),
            Op::ListDelete { index: 3, .. }
        ));
    }

    #[test]
    fn op_list_splice_ok() {
        let req = op_req(op_request::Op::ListSplice(ListSplice {
            obj: "list".into(),
            pos: 1,
            del_count: 2,
            values: vec![scalar(scalar_value::Value::IntVal(99))],
        }));
        let op = Op::try_from(req).unwrap();
        assert!(matches!(
            op,
            Op::ListSplice {
                pos: 1,
                del_count: 2,
                ..
            }
        ));
        if let Op::ListSplice { values, .. } = op {
            assert_eq!(values, vec![ScalarVal::Int(99)]);
        }
    }

    #[test]
    fn op_text_splice_ok() {
        let req = op_req(op_request::Op::TextSplice(TextSplice {
            obj: "body".into(),
            pos: 5,
            del_count: 0,
            insert: "hello".into(),
        }));
        assert!(matches!(
            Op::try_from(req).unwrap(),
            Op::TextSplice { pos: 5, del_count: 0, insert, .. } if insert == "hello"
        ));
    }

    #[test]
    fn op_missing_op_field_is_error() {
        let err = Op::try_from(OpRequest { op: None }).unwrap_err();
        assert!(err.to_string().contains("missing op field"), "{err}");
    }
}
