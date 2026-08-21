//! Shared types for the replicant project.
//!
//! - [`proto`] — generated tonic/prost code for the `replicant.v1` gRPC API.
//! - [`ScalarVal`] / [`Op`] — library-agnostic value and operation models,
//!   with `TryFrom` impls from their proto counterparts.
//! - [`CrdtAdapter`] — the trait every CRDT backend (Automerge, Yrs, Loro)
//!   implements, called by the replica scaffolding.

use anyhow::Context as _;

/// Generated protobuf types for the `replicant.v1` package.
pub mod proto {
    tonic::include_proto!("replicant.v1");
}

// ── Scalar values ──────────────────────────────────────────────────────────

/// A scalar value that can be stored in a CRDT document.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarVal {
    Str(String),
    Uint(u64),
    Int(i64),
    Bool(bool),
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
/// itself. List and text objects are created by the adapter lazily on first
/// access.
///
/// Indices use `usize` to match the Automerge API; the proto layer converts
/// from `u64` on ingress, rejecting values that do not fit rather than
/// truncating them.
#[derive(Debug, Clone)]
pub enum Op {
    MapPut {
        obj: String,
        key: String,
        value: ScalarVal,
    },
    MapDelete {
        obj: String,
        key: String,
    },
    ListInsert {
        obj: String,
        index: usize,
        value: ScalarVal,
    },
    ListDelete {
        obj: String,
        index: usize,
    },
    /// Matches `Transactable::splice`: insert/delete in one operation.
    ListSplice {
        obj: String,
        pos: usize,
        del_count: usize,
        values: Vec<ScalarVal>,
    },
    /// Matches `Transactable::splice_text`.
    TextSplice {
        obj: String,
        pos: usize,
        del_count: usize,
        insert: String,
    },
}

impl Op {
    /// The operation name used as the `op` metric attribute.
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

/// Convert a proto `u64` index to `usize`.
///
/// Fails rather than truncating on targets where `usize` is narrower than 64
/// bits: a silently wrapped index would address the wrong element instead of
/// reporting a bad request.
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
                    // `del_count` is a proto uint32; widen before the check so
                    // one conversion helper covers every index-like field.
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

/// Library-agnostic interface between the replica scaffolding and a CRDT
/// implementation.
///
/// All timing and metric emission for comparable metrics (`replicant.*`) live
/// in the scaffolding layer, *outside* calls to this trait. Adapter impls
/// must not emit those metrics directly.
///
/// All methods take `&mut self`, including the reads. The requirement comes
/// from Automerge, whose `AutoCommit` commits any pending transaction before
/// answering a read; Yrs and Loro would both be satisfied by `&self`. Kept
/// as the wider bound so the trait admits either shape.
///
/// `Send + 'static` allows the adapter to be held behind `Box<dyn
/// CrdtAdapter>` and moved into a Tokio task.
pub trait CrdtAdapter: Send + 'static {
    /// Apply a single document operation.
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()>;

    /// Return the current document heads as sorted opaque byte vectors.
    ///
    /// What an entry *is* varies by library and the trait makes no claim
    /// about it: a 32-byte `ChangeHash` for Automerge, a (peer, counter)
    /// pair from the causal frontier for Loro, a (client, clock) pair from
    /// the state vector for Yrs — which, note, is not a DAG frontier at all.
    /// Sorting is required of every implementation, so that equality
    /// comparison is order-independent even where the underlying collection
    /// is a hash map with unspecified iteration order.
    fn get_heads(&mut self) -> Vec<Vec<u8>>;

    /// Return an opaque fingerprint of the current document state.
    ///
    /// The orchestrator compares fingerprints for byte equality to check
    /// convergence without interpreting their content.
    fn state_fingerprint(&mut self) -> Vec<u8>;

    /// Return the serialized document size in bytes.
    fn doc_size_bytes(&mut self) -> usize;

    /// Generate the next outbound sync message for `peer`, if any.
    ///
    /// Returns `None` once this replica has nothing further to tell `peer`
    /// given what it has sent so far — a fixed point that must actually be
    /// reachable, not merely converged toward, or the scaffolding's
    /// push-after-every-op flush loop (the replica's `flush_to_peers`) spins
    /// forever and pollutes the sync message-count/byte telemetry. How an
    /// adapter tracks "what I've told `peer`" to answer this is
    /// implementation-defined: Automerge threads a `sync::State` per peer;
    /// other libraries may track a remembered peer state vector, a per-peer
    /// drain cursor over locally-produced update bytes, or something else
    /// native to their own sync model. This method makes no claim about which
    /// message a given call represents (handshake-initiating vs. a data
    /// delta) — that is opaque protocol framing internal to the returned
    /// bytes, decoded only by the adapter's own `sync_receive`.
    ///
    /// Note that convergence itself is *not* detected through this method —
    /// the orchestrator polls [`Self::state_fingerprint`] for byte equality
    /// across replicas independently of what any adapter believes about its
    /// peers. `None` here only starves the push loop; it is not the
    /// convergence oracle.
    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>>;

    /// Process an inbound sync message from `peer`.
    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()>;

    /// Discard the per-peer sync protocol state for `peer`, so the next
    /// [`Self::sync_generate`] restarts whatever this adapter's native
    /// protocol does to (re)establish sync with a peer it has no bookkeeping
    /// for — a fresh handshake for a request/response protocol, a
    /// from-scratch diff for a state-vector-cache design, etc. Document state
    /// is untouched.
    ///
    /// Used when healing a simulated partition: while a link is blocked,
    /// messages may have been generated and then dropped (e.g. one racing the
    /// block's onset), leaving this side's per-peer bookkeeping believing the
    /// peer received data it never saw. Whatever an adapter caches about a
    /// peer's progress, a stale cache of that kind can stall the exchange
    /// indefinitely, and discarding it must always be safe: re-deriving from
    /// scratch costs at most some re-sent data the peer already holds, never
    /// incorrectness.
    ///
    /// Must be a no-op if no state exists for `peer`.
    fn sync_reset(&mut self, peer: &str);

    /// Discard all document and per-peer sync state, returning the adapter
    /// to its initial empty state.
    ///
    /// Used by the replica's `Reset` RPC so externally-managed replicas can be
    /// recycled between trials without bouncing the container.
    fn reset(&mut self);

    /// Deterministically create the named text object under ROOT so that every
    /// replica calling this on an *empty* document ends up with the same
    /// object identity, without any sync having happened.
    ///
    /// This is the precondition for text workloads on partitioned replicas:
    /// if each side instead creates the object lazily on first write, the two
    /// creations are concurrent map-key puts and the eventual merge keeps only
    /// one — silently discarding the other side's entire text rather than
    /// interleaving it.
    ///
    /// Must be idempotent when the object already exists (whether created
    /// locally or received via sync). Must fail rather than guess when the
    /// document has prior changes but no such object — how an implementation
    /// achieves cross-replica identity from a later state is adapter-specific
    /// and generally unsafe.
    ///
    /// Adapters whose library names root objects globally may implement this
    /// as a no-op lookup — both non-Automerge adapters written so far do,
    /// by different mechanisms: Yrs's `get_or_insert_text` is identity-stable
    /// by name, and a Loro root container's id is *derived* from `(name,
    /// type)` and always exists. Where that holds there is no creation op,
    /// hence no concurrent creation and no losing side.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()>;

    /// Character length of the named text object.
    ///
    /// The orchestrator uses this as a post-convergence validity check: for
    /// insert-only workloads the final length must equal the total op count,
    /// proving no replica's inserts were discarded on merge. Fingerprint
    /// equality alone cannot distinguish "converged on everything" from
    /// "converged after throwing half the work away".
    ///
    /// Errors if no such text object exists — where the backing library can
    /// represent that. Loro cannot: every `(name, Text)` root id exists by
    /// construction, so an untouched name reads as an empty text and returns
    /// `Ok(0)`. Callers must therefore treat this as a length query and not
    /// as an existence check. The orchestrator's `verify_text_length` already
    /// does: it compares the length against the scenario's op count, so a
    /// wrong object name still fails, as a mismatch rather than an error.
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
