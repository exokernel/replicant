//! Shared types for the replicant project.
//!
//! - [`proto`] — generated tonic/prost code for the `replicant.v1` gRPC API.
//! - [`ScalarVal`] / [`Op`] — library-agnostic value and operation models,
//!   with `TryFrom` impls from their proto counterparts.
//! - [`CrdtAdapter`] — the trait every CRDT backend (currently just Automerge)
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
/// from `u64` on ingress.
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
                index: p.index as usize,
                value: p.value.context("ListInsert missing value")?.try_into()?,
            }),
            P::ListDelete(p) => Ok(Op::ListDelete {
                obj: p.obj,
                index: p.index as usize,
            }),
            P::ListSplice(p) => {
                let values = p
                    .values
                    .into_iter()
                    .map(ScalarVal::try_from)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                Ok(Op::ListSplice {
                    obj: p.obj,
                    pos: p.pos as usize,
                    del_count: p.del_count as usize,
                    values,
                })
            }
            P::TextSplice(p) => Ok(Op::TextSplice {
                obj: p.obj,
                pos: p.pos as usize,
                del_count: p.del_count as usize,
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
/// All methods take `&mut self` because `AutoCommit` requires mutable access
/// even for reads — it commits any pending transaction first.
///
/// `Send + 'static` allows the adapter to be held behind `Box<dyn
/// CrdtAdapter>` and moved into a Tokio task.
pub trait CrdtAdapter: Send + 'static {
    /// Apply a single document operation.
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()>;

    /// Return the current document heads as sorted opaque byte vectors.
    ///
    /// For Automerge each entry is a 32-byte `ChangeHash`. Sorting ensures
    /// that equality comparison is order-independent.
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
    /// Returns `None` when this replica believes it is already in sync with
    /// `peer`. The adapter owns one `sync::State` per peer keyed by `peer`.
    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>>;

    /// Process an inbound sync message from `peer`.
    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()>;
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
