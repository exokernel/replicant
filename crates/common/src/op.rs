//! [`ScalarVal`] and [`Op`]: library-agnostic values and operations, and
//! their conversions from the proto types.

use anyhow::Context as _;

use crate::proto;

// ── Scalar values ──────────────────────────────────────────────────────────

/// A scalar value that can be stored in a CRDT document.
///
/// There is no float variant, so `ScalarVal` implements `Eq`. Adding a float
/// would remove `Eq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarVal {
    /// A UTF-8 string.
    Str(String),
    /// An unsigned integer. Yrs and Loro store integers as `i64`, so their
    /// adapters reject a value above `i64::MAX`.
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
