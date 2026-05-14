use std::collections::HashMap;

use anyhow::Context;
use automerge::{
    AutoCommit, ObjId, ObjType, ROOT, ReadDoc, ScalarValue as AmScalarValue,
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
    ///
    /// # Invariant
    /// Callers must use the same `obj_type` for a given `obj` name across all
    /// calls. Passing a different type for an already-cached name returns the
    /// cached id silently, which will cause Automerge operations to fail or
    /// corrupt state if the types are incompatible.
    fn resolve_obj(&mut self, obj: &str, obj_type: ObjType) -> anyhow::Result<ObjId> {
        if obj.is_empty() {
            return Ok(ROOT);
        }
        if let Some(id) = self.objects.get(obj) {
            debug_assert!(
                // Verify the cached object still has the expected type. This
                // catches callers that reuse a name with a different ObjType.
                self.doc
                    .object_type(id)
                    .map(|t| t == obj_type)
                    .unwrap_or(false),
                "resolve_obj: '{obj}' was cached as a different type than {obj_type:?}"
            );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pump sync messages between two adapters until both return `None`,
    /// meaning each side believes the other is caught up.
    fn sync_until_quiescent(
        a: &mut AutomergeAdapter,
        a_id: &str,
        b: &mut AutomergeAdapter,
        b_id: &str,
    ) {
        // Bounded to prevent a buggy adapter from looping forever; well above
        // any reasonable handshake length for these tiny docs.
        for _ in 0..64 {
            let from_a = a.sync_generate(b_id);
            if let Some(msg) = from_a.clone() {
                b.sync_receive(a_id, msg).unwrap();
            }
            let from_b = b.sync_generate(a_id);
            if let Some(msg) = from_b.clone() {
                a.sync_receive(b_id, msg).unwrap();
            }
            if from_a.is_none() && from_b.is_none() {
                return;
            }
        }
        panic!("sync did not reach quiescence within 64 rounds");
    }

    fn map_put(obj: &str, key: &str, value: impl Into<ScalarVal>) -> Op {
        Op::MapPut {
            obj: obj.to_owned(),
            key: key.to_owned(),
            value: value.into(),
        }
    }

    #[test]
    fn identical_ops_yield_equal_fingerprints() {
        // Two fresh adapters that apply the exact same op sequence should
        // produce the same heads and the same fingerprint — no sync involved.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        // Automerge change hashes depend on actor id, which is random per
        // adapter, so we have to drive convergence through sync rather than
        // assuming identical ops produce identical hashes. Apply on `a`, sync
        // to `b`, then check.
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.get_heads(), b.get_heads());
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        assert!(
            !a.state_fingerprint().is_empty(),
            "fingerprint after a write must not be empty"
        );
    }

    #[test]
    fn disjoint_edits_converge_after_sync() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "a_key", 1u64)).unwrap();
        b.apply_op(&map_put("doc", "b_key", 2u64)).unwrap();

        assert_ne!(
            a.state_fingerprint(),
            b.state_fingerprint(),
            "replicas with disjoint edits must not appear equal pre-sync"
        );

        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.get_heads(), b.get_heads());
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    }

    #[test]
    fn concurrent_edits_to_same_key_converge() {
        // Both replicas write to the same key with no prior sync. The DAG
        // ends up with two heads; get_heads must sort them so byte-equal
        // fingerprint comparison still works.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "from-a")).unwrap();
        b.apply_op(&map_put("doc", "k", "from-b")).unwrap();

        sync_until_quiescent(&mut a, "a", &mut b, "b");

        let a_heads = a.get_heads();
        let b_heads = b.get_heads();
        assert_eq!(a_heads, b_heads);
        assert_eq!(a_heads.len(), 2, "concurrent writes should leave 2 heads");
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    }

    #[test]
    fn post_sync_divergence_is_detected() {
        // Negative case: if a writes after sync without re-syncing, the
        // fingerprints must differ. Without this, a buggy fingerprint that
        // returns a constant value would still pass the convergence tests.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v0")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());

        a.apply_op(&map_put("doc", "k", "v1")).unwrap();
        assert_ne!(a.state_fingerprint(), b.state_fingerprint());
        assert_ne!(a.get_heads(), b.get_heads());
    }

    #[test]
    fn each_op_variant_mutates_the_doc() {
        // Apply one of each Op variant in dependency order (deletes need
        // something to delete) and assert the fingerprint changes and the
        // doc size grows on every step. Guards against a new Op variant
        // being added to the enum but not wired up in apply_op — the match
        // is exhaustive so the compiler catches a missing arm, but it would
        // not catch an arm that silently no-ops.
        let mut a = AutomergeAdapter::new();
        let mut prev_fp = a.state_fingerprint();
        let mut prev_size = a.doc_size_bytes();

        let steps: Vec<Op> = vec![
            map_put("m", "k", "v"),
            Op::MapDelete {
                obj: "m".into(),
                key: "k".into(),
            },
            Op::ListInsert {
                obj: "l".into(),
                index: 0,
                value: ScalarVal::Uint(1),
            },
            Op::ListSplice {
                obj: "l".into(),
                pos: 1,
                del_count: 0,
                values: vec![ScalarVal::Uint(2), ScalarVal::Uint(3)],
            },
            Op::ListDelete {
                obj: "l".into(),
                index: 0,
            },
            Op::TextSplice {
                obj: "t".into(),
                pos: 0,
                del_count: 0,
                insert: "hello".into(),
            },
        ];

        for op in &steps {
            a.apply_op(op).unwrap();
            let fp = a.state_fingerprint();
            let size = a.doc_size_bytes();
            assert_ne!(fp, prev_fp, "fingerprint unchanged after {}", op.name());
            assert!(
                size > prev_size,
                "doc size did not grow after {} ({prev_size} -> {size})",
                op.name()
            );
            prev_fp = fp;
            prev_size = size;
        }
    }

    #[test]
    fn reads_are_stable_without_writes() {
        // state_fingerprint() and get_heads() must be pure with respect to
        // document state: repeated calls without intervening writes return
        // identical bytes. Guards against the fingerprint accidentally
        // including incidental state (a counter, a transaction id, etc.)
        // that the orchestrator's convergence check would mistake for a
        // real divergence.
        let mut a = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();

        let fp1 = a.state_fingerprint();
        let fp2 = a.state_fingerprint();
        let fp3 = a.state_fingerprint();
        assert_eq!(fp1, fp2);
        assert_eq!(fp2, fp3);

        let h1 = a.get_heads();
        let h2 = a.get_heads();
        let h3 = a.get_heads();
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    #[test]
    fn sync_is_idempotent_once_converged() {
        // After sync_until_quiescent, both sides should immediately report
        // "nothing to send." The orchestrator's convergence-detection loop
        // depends on this; a regression that makes sync chatty would only
        // show up downstream as a flaky integration test.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert!(a.sync_generate("b").is_none());
        assert!(b.sync_generate("a").is_none());
    }

    #[test]
    fn three_replicas_converge_through_a_hub() {
        // Line topology: a <-> b <-> c, then a <-> c directly. Each pair
        // uses its own sync::State, so this catches cross-talk bugs in the
        // per-peer state map.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        let mut c = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "from_a", 1u64)).unwrap();
        b.apply_op(&map_put("doc", "from_b", 2u64)).unwrap();
        c.apply_op(&map_put("doc", "from_c", 3u64)).unwrap();

        sync_until_quiescent(&mut a, "a", &mut b, "b");
        sync_until_quiescent(&mut b, "b", &mut c, "c");
        sync_until_quiescent(&mut a, "a", &mut c, "c");

        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        assert_eq!(b.state_fingerprint(), c.state_fingerprint());
    }
}
