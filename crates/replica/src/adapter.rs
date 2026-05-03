use std::collections::HashMap;

use anyhow::Context;
use automerge::{
    AutoCommit, ObjId, ObjType, ROOT, ScalarValue as AmScalarValue,
    sync::{self, SyncDoc},
    transaction::Transactable,
};
use common::{CrdtAdapter, Op, ScalarVal};

/// [`common::CrdtAdapter`] implementation backed by `automerge::AutoCommit`.
///
/// Each instance owns a single document and one `sync::State` per peer.
/// Objects under ROOT are created lazily on first access and cached in
/// `objects` to avoid repeated lookups through the Automerge `get` API.
pub struct AutomergeAdapter {
    doc: AutoCommit,
    /// Per-peer sync state. Keyed by stable peer ID.
    sync_states: HashMap<String, sync::State>,
    /// Cache of named top-level objects created under ROOT.
    objects: HashMap<String, ObjId>,
}

impl Default for AutomergeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl AutomergeAdapter {
    /// Create a new, empty Automerge document with no peer state.
    pub fn new() -> Self {
        Self {
            doc: AutoCommit::new(),
            sync_states: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    /// Convert a [`ScalarVal`] to the Automerge scalar type.
    fn to_am_scalar(v: ScalarVal) -> AmScalarValue {
        match v {
            ScalarVal::Str(s) => AmScalarValue::Str(s.into()),
            ScalarVal::Uint(n) => AmScalarValue::Uint(n),
            ScalarVal::Int(n) => AmScalarValue::Int(n),
            ScalarVal::Bool(b) => AmScalarValue::Boolean(b),
            ScalarVal::Bytes(b) => AmScalarValue::Bytes(b),
        }
    }

    /// Return the `ObjId` for a named top-level object, creating it if needed.
    ///
    /// Empty `obj` resolves to ROOT. `obj_type` is only consulted on first
    /// access; subsequent calls return the cached id regardless of type.
    fn resolve_obj(&mut self, obj: &str, obj_type: ObjType) -> anyhow::Result<ObjId> {
        if obj.is_empty() {
            return Ok(ROOT);
        }
        if let Some(id) = self.objects.get(obj) {
            return Ok(id.clone());
        }
        let id = self
            .doc
            .put_object(ROOT, obj, obj_type)
            .with_context(|| format!("creating object '{obj}'"))?;
        self.objects.insert(obj.to_owned(), id.clone());
        Ok(id)
    }
}

impl CrdtAdapter for AutomergeAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        match op {
            Op::MapPut { obj, key, value } => {
                let id = self.resolve_obj(obj, ObjType::Map)?;
                self.doc
                    .put(id, key.as_str(), Self::to_am_scalar(value.clone()))?;
            }
            Op::MapDelete { obj, key } => {
                let id = self.resolve_obj(obj, ObjType::Map)?;
                self.doc.delete(id, key.as_str())?;
            }
            Op::ListInsert { obj, index, value } => {
                let id = self.resolve_obj(obj, ObjType::List)?;
                self.doc
                    .insert(id, *index, Self::to_am_scalar(value.clone()))?;
            }
            Op::ListDelete { obj, index } => {
                let id = self.resolve_obj(obj, ObjType::List)?;
                self.doc.delete(id, *index)?;
            }
            Op::ListSplice {
                obj,
                pos,
                del_count,
                values,
            } => {
                let id = self.resolve_obj(obj, ObjType::List)?;
                let scalars = values.iter().cloned().map(Self::to_am_scalar);
                self.doc.splice(id, *pos, *del_count as isize, scalars)?;
            }
            Op::TextSplice {
                obj,
                pos,
                del_count,
                insert,
            } => {
                let id = self.resolve_obj(obj, ObjType::Text)?;
                self.doc
                    .splice_text(id, *pos, *del_count as isize, insert)?;
            }
        }
        // Commit the open transaction so op_duration_ms captures the full
        // apply+commit cost, and sync_generate sees exactly this change.
        self.doc.commit();
        Ok(())
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        let mut heads: Vec<Vec<u8>> = self
            .doc
            .get_heads()
            .into_iter()
            .map(|h| h.0.to_vec())
            .collect();
        heads.sort_unstable();
        heads
    }

    fn state_fingerprint(&mut self) -> Vec<u8> {
        // Sorted concatenation of all head hashes. Equal on two replicas iff
        // they have the same DAG frontier.
        self.get_heads().into_iter().flatten().collect()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // save() produces the full binary encoding — there is no cheaper
        // size query in Automerge. Acceptable here because this is called
        // once per op for the benchmark size gauge.
        self.doc.save().len()
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let state = self.sync_states.entry(peer.to_owned()).or_default();
        self.doc
            .sync()
            .generate_sync_message(state)
            .map(|msg| msg.encode())
    }

    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        let decoded = sync::Message::decode(&msg)
            .with_context(|| format!("decoding sync message from '{peer}'"))?;
        let state = self.sync_states.entry(peer.to_owned()).or_default();
        self.doc.sync().receive_sync_message(state, decoded)?;
        Ok(())
    }
}
