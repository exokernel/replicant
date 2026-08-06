use std::collections::HashMap;

use anyhow::{Context, bail};
use automerge::{
    ActorId, AutoCommit, ObjId, ObjType, ROOT, ReadDoc, ScalarValue as AmScalarValue, Value,
    sync::{self, SyncDoc},
    transaction::{CommitOptions, Transactable},
};
use common::{CrdtAdapter, Op, ScalarVal};

/// Fixed actor id under which [`CrdtAdapter::ensure_text`] authors the
/// bootstrap change. Every replica uses the same actor, the same single op,
/// empty deps (the document must be empty), and commit time 0 — so the change
/// bytes, its hash, and the created object's identity (`OpId(1, this actor)`)
/// are identical everywhere without any sync. The adapter's own actor is
/// restored immediately after, so all real ops remain attributed per replica.
const BOOTSTRAP_ACTOR: &[u8] = b"replicant-bootstrap";

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
    ///
    /// Primitives are copied; `Str` and `Bytes` clone their backing buffer.
    fn to_am_scalar(v: &ScalarVal) -> AmScalarValue {
        match v {
            ScalarVal::Str(s) => AmScalarValue::Str(s.as_str().into()),
            ScalarVal::Uint(n) => AmScalarValue::Uint(*n),
            ScalarVal::Int(n) => AmScalarValue::Int(*n),
            ScalarVal::Bool(b) => AmScalarValue::Boolean(*b),
            ScalarVal::Bytes(b) => AmScalarValue::Bytes(b.clone()),
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
        // An object under this name may already exist without being cached:
        // created by a peer and received via sync, or by `ensure_text`.
        // Creating a new one here would put a *concurrent* object at the same
        // key, and the eventual merge would keep only one — silently dropping
        // every op applied to the loser. Reuse before create.
        let id = match self.lookup_obj(obj) {
            Some((id, t)) if t == obj_type => id,
            Some((_, t)) => {
                bail!("object '{obj}' already exists with type {t:?}, not {obj_type:?}")
            }
            None => self
                .doc
                .put_object(ROOT, obj, obj_type)
                .with_context(|| format!("creating object '{obj}'"))?,
        };
        self.objects.insert(obj.to_owned(), id.clone());
        Ok(id)
    }

    /// Look up an existing object under `ROOT[obj]`, returning its id and type.
    fn lookup_obj(&self, obj: &str) -> Option<(ObjId, ObjType)> {
        match self.doc.get(ROOT, obj) {
            Ok(Some((Value::Object(t), id))) => Some((id, t)),
            _ => None,
        }
    }
}

impl CrdtAdapter for AutomergeAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        match op {
            Op::MapPut { obj, key, value } => {
                let id = self.resolve_obj(obj, ObjType::Map)?;
                self.doc.put(id, key.as_str(), Self::to_am_scalar(value))?;
            }
            Op::MapDelete { obj, key } => {
                let id = self.resolve_obj(obj, ObjType::Map)?;
                self.doc.delete(id, key.as_str())?;
            }
            Op::ListInsert { obj, index, value } => {
                let id = self.resolve_obj(obj, ObjType::List)?;
                self.doc.insert(id, *index, Self::to_am_scalar(value))?;
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
                let scalars = values.iter().map(Self::to_am_scalar);
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

    fn sync_reset(&mut self, peer: &str) {
        // Dropping the entry is the whole reset: the next generate/receive
        // lazily creates a fresh `sync::State`, which starts the protocol
        // from the full handshake.
        self.sync_states.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        if obj.is_empty() {
            bail!("ensure_text: object name must not be empty (ROOT is a map)");
        }
        match self.lookup_obj(obj) {
            Some((id, ObjType::Text)) => {
                // Already present (bootstrapped earlier, or received via sync).
                self.objects.insert(obj.to_owned(), id);
                return Ok(());
            }
            Some((_, t)) => bail!("ensure_text: '{obj}' exists with type {t:?}"),
            None => {}
        }
        // Authoring the bootstrap change is only deterministic from the empty
        // document: any prior change would land in `deps`, and two replicas
        // authoring different deps under the same (actor, seq) would corrupt
        // the DAG rather than deduplicate.
        if !self.doc.get_heads().is_empty() {
            bail!("ensure_text: document already has changes; bootstrap must be the first change");
        }
        let own_actor = self.doc.get_actor().clone();
        self.doc.set_actor(ActorId::from(BOOTSTRAP_ACTOR));
        let id = self
            .doc
            .put_object(ROOT, obj, ObjType::Text)
            .with_context(|| format!("bootstrapping text object '{obj}'"))?;
        // Time 0 keeps the change bytes identical across replicas and runs;
        // the default commit would also use 0, but the determinism guarantee
        // is the whole point here, so pin it explicitly.
        self.doc.commit_with(CommitOptions::default().with_time(0));
        self.doc.set_actor(own_actor);
        self.objects.insert(obj.to_owned(), id);
        Ok(())
    }

    fn text_length(&mut self, obj: &str) -> anyhow::Result<usize> {
        match self.lookup_obj(obj) {
            Some((id, ObjType::Text)) => Ok(self
                .doc
                .text(&id)
                .with_context(|| format!("reading text object '{obj}'"))?
                .chars()
                .count()),
            Some((_, t)) => bail!("text_length: '{obj}' exists with type {t:?}, not Text"),
            None => bail!("text_length: no object named '{obj}'"),
        }
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

    #[test]
    fn reset_clears_doc_and_sync_state() {
        // Reset returns the adapter to its initial empty state: heads/
        // fingerprint go back to empty, doc_size matches a fresh adapter,
        // and any per-peer sync::State entries are dropped (so sync_generate
        // produces a fresh handshake message rather than continuing an
        // already-quiesced conversation).
        let mut a = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        // Populate sync_states for a peer so the reset has something to clear.
        let initial_msg = a.sync_generate("peer-x");
        assert!(
            initial_msg.is_some(),
            "fresh adapter should send a handshake"
        );
        let fresh_size = AutomergeAdapter::new().doc_size_bytes();
        assert!(!a.get_heads().is_empty());
        assert!(
            a.doc_size_bytes() > fresh_size,
            "doc must have grown after a write"
        );

        a.reset();

        assert!(a.get_heads().is_empty(), "heads not cleared by reset");
        assert!(
            a.state_fingerprint().is_empty(),
            "fingerprint not cleared by reset"
        );
        assert_eq!(
            a.doc_size_bytes(),
            fresh_size,
            "doc not reset to empty size"
        );
        // A new sync conversation against the same peer-id should start from
        // scratch — if sync_states leaked across reset, the second call would
        // observe quiescence and return None.
        assert!(
            a.sync_generate("peer-x").is_some(),
            "reset must drop per-peer sync state"
        );
    }

    #[test]
    fn reset_allows_clean_re_sync_to_another_replica() {
        // End-to-end at the adapter layer: a writes, syncs with b, reset both,
        // a writes different data, sync, and both converge on the new state
        // alone — proving the old DAG is gone, not merely hidden.
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "before", "old")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());

        a.reset();
        b.reset();
        assert!(a.state_fingerprint().is_empty());
        assert!(b.state_fingerprint().is_empty());

        a.apply_op(&map_put("doc", "after", "new")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        // The fresh post-reset doc must match a from-scratch baseline that
        // only ever saw the "after" write — i.e. the size profile is that
        // of a one-write document, not a two-write one.
        let mut baseline = AutomergeAdapter::new();
        baseline.apply_op(&map_put("doc", "after", "new")).unwrap();
        assert_eq!(
            a.doc_size_bytes(),
            baseline.doc_size_bytes(),
            "post-reset doc size differs from a from-scratch single-write doc"
        );
    }

    // ── ensure_text / text_length (shared-object bootstrap) ────────────────

    /// The core determinism guarantee: two replicas that bootstrap
    /// independently — no sync — produce the bit-identical change and
    /// therefore the same heads. Everything else about the divergence
    /// workload rests on this.
    #[test]
    fn ensure_text_is_deterministic_across_replicas() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.ensure_text("text").unwrap();
        b.ensure_text("text").unwrap();

        assert!(!a.get_heads().is_empty());
        assert_eq!(
            a.get_heads(),
            b.get_heads(),
            "independent bootstraps must produce the identical change"
        );
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    }

    #[test]
    fn ensure_text_is_idempotent() {
        let mut a = AutomergeAdapter::new();
        a.ensure_text("text").unwrap();
        let heads = a.get_heads();
        a.ensure_text("text").unwrap();
        assert_eq!(a.get_heads(), heads, "second call must not author a change");
    }

    /// Regression for the divergence-sweep bug: two replicas that diverge
    /// while partitioned must merge into ONE text containing both sides'
    /// inserts. Without the shared bootstrap, each side lazily created its
    /// own object, the merge kept one, and half the workload vanished —
    /// while fingerprints happily converged.
    #[test]
    fn partitioned_text_edits_interleave_after_bootstrap() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.ensure_text("text").unwrap();
        b.ensure_text("text").unwrap();

        let splice = |pos: usize| Op::TextSplice {
            obj: "text".into(),
            pos,
            del_count: 0,
            insert: "x".into(),
        };
        // Simulate divergence: 10 prepends each side, no sync in between
        // (the same_region shape — every op contests the shared HEAD anchor).
        for _ in 0..10 {
            a.apply_op(&splice(0)).unwrap();
            b.apply_op(&splice(0)).unwrap();
        }
        assert_eq!(a.text_length("text").unwrap(), 10);

        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        assert_eq!(
            a.text_length("text").unwrap(),
            20,
            "merge must interleave both sides, not discard one"
        );
        assert_eq!(b.text_length("text").unwrap(), 20);
    }

    /// The counterpart guard: without bootstrap the lazy-creation collision
    /// still exists, and text_length is exactly the check that exposes it.
    /// Locks in WHY ensure_text is mandatory for partitioned text workloads —
    /// if a future Automerge changes this behaviour, we want to know.
    #[test]
    fn without_bootstrap_partitioned_text_loses_a_side() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        let splice = Op::TextSplice {
            obj: "text".into(),
            pos: 0,
            del_count: 0,
            insert: "x".into(),
        };
        for _ in 0..10 {
            a.apply_op(&splice).unwrap();
            b.apply_op(&splice).unwrap();
        }
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        assert_eq!(
            a.text_length("text").unwrap(),
            10,
            "lazy creation collides: converged doc keeps only the winning object"
        );
    }

    #[test]
    fn ensure_text_rejects_non_empty_doc_without_the_object() {
        let mut a = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        let err = a.ensure_text("text").unwrap_err();
        assert!(err.to_string().contains("first change"), "{err}");
    }

    /// A synced-in object satisfies ensure_text — the late replica adopts it
    /// rather than authoring a bootstrap of its own.
    #[test]
    fn ensure_text_adopts_synced_in_object() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.ensure_text("text").unwrap();
        a.apply_op(&Op::TextSplice {
            obj: "text".into(),
            pos: 0,
            del_count: 0,
            insert: "hi".into(),
        })
        .unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        b.ensure_text("text").unwrap();
        assert_eq!(b.text_length("text").unwrap(), 2);
        assert_eq!(a.get_heads(), b.get_heads(), "no extra change authored");
    }

    /// resolve_obj must reuse an object that arrived via sync instead of
    /// creating a concurrent one — the connected-topology flavour of the
    /// same collision (a round-robin writer's first op racing the sync of
    /// another node's creation).
    #[test]
    fn first_local_write_reuses_synced_in_object() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        let splice = |insert: &str| Op::TextSplice {
            obj: "text".into(),
            pos: 0,
            del_count: 0,
            insert: insert.into(),
        };
        a.ensure_text("text").unwrap();
        a.apply_op(&splice("aa")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        // b's first local write: the object exists in its doc but not its
        // cache. It must splice into that object, not create a rival.
        b.apply_op(&splice("bb")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.text_length("text").unwrap(), 4, "all four chars survive");
        assert_eq!(b.text_length("text").unwrap(), 4);
    }

    // ── sync_reset (partition-heal support) ────────────────────────────────

    /// After quiescence, generate returns `None` — the state believes the
    /// peer is caught up. `sync_reset` must forget that, so the next generate
    /// restarts the handshake. This is what lets a healed link re-establish
    /// sync without reconnecting the stream.
    #[test]
    fn sync_reset_forgets_quiescence() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        sync_until_quiescent(&mut a, "a", &mut b, "b");
        assert!(a.sync_generate("b").is_none(), "quiesced before reset");

        a.sync_reset("b");
        assert!(
            a.sync_generate("b").is_some(),
            "reset must restart the handshake"
        );
        // Document state is untouched by the protocol reset.
        assert!(!a.get_heads().is_empty());
    }

    /// The heal scenario end-to-end at the adapter layer: a message is
    /// generated and then lost (the block races the flush), leaving `a`'s
    /// protocol state believing `b` received data it never saw. Resetting
    /// both sides' states — what unblocking does — must let a fresh exchange
    /// converge anyway.
    #[test]
    fn sync_reset_recovers_from_a_lost_message() {
        let mut a = AutomergeAdapter::new();
        let mut b = AutomergeAdapter::new();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();

        // Generated but never delivered: a's sync::State records these heads
        // as sent.
        let lost = a.sync_generate("b");
        assert!(lost.is_some(), "there was a change to send");
        drop(lost);

        // Heal: both sides discard protocol state, then sync normally.
        a.sync_reset("b");
        b.sync_reset("a");
        sync_until_quiescent(&mut a, "a", &mut b, "b");

        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
        assert_eq!(a.get_heads(), b.get_heads());
        assert!(!b.get_heads().is_empty(), "b must have received the change");
    }

    /// Resetting a peer that has no state must be a no-op, not a panic —
    /// unblock fires for peers that never exchanged a message.
    #[test]
    fn sync_reset_unknown_peer_is_noop() {
        let mut a = AutomergeAdapter::new();
        a.sync_reset("never-seen");
        assert!(a.get_heads().is_empty());
    }

    #[test]
    fn text_length_errors_on_missing_or_wrong_type() {
        let mut a = AutomergeAdapter::new();
        assert!(a.text_length("nope").is_err());
        a.apply_op(&Op::ListInsert {
            obj: "l".into(),
            index: 0,
            value: ScalarVal::Uint(1),
        })
        .unwrap();
        let err = a.text_length("l").unwrap_err();
        assert!(err.to_string().contains("not Text"), "{err}");
    }

    #[test]
    fn save_bytes_not_canonical_across_converged_replicas() {
        // Locks in a known Automerge property: two replicas with identical
        // logical state (same heads, same fingerprint, same readable values)
        // can produce *different* save() byte streams. The encoding preserves
        // change-list storage order, which depends on integration order, and
        // that order varies by graph position in non-mesh topologies with
        // distributed writes.
        //
        // The notebook's per-scenario doc-size table treats this as expected
        // and presents the spread as informational rather than an assertion.
        // If a future Automerge release ever canonicalizes save(), this test
        // will start failing and the table can be tightened to strict
        // equality.
        let n = 5;
        let op_count = 10;
        let mut replicas: Vec<AutomergeAdapter> = (0..n).map(|_| AutomergeAdapter::new()).collect();
        let id = |i: usize| format!("node-{i}");

        for i in 0..op_count {
            let writer = i % n;
            replicas[writer]
                .apply_op(&map_put("doc", &format!("k{i}"), i as u64))
                .unwrap();
            // Inline one round of bidirectional sync over each line edge —
            // approximates the server's post-apply_op flush_to_peers.
            for j in 0..n - 1 {
                let (left, right) = replicas.split_at_mut(j + 1);
                if let Some(msg) = left[j].sync_generate(&id(j + 1)) {
                    right[0].sync_receive(&id(j), msg).unwrap();
                }
                if let Some(msg) = right[0].sync_generate(&id(j)) {
                    left[j].sync_receive(&id(j + 1), msg).unwrap();
                }
            }
        }
        // Drain any in-flight messages until both directions of every edge
        // report quiescence.
        loop {
            let mut any = false;
            for j in 0..n - 1 {
                let (left, right) = replicas.split_at_mut(j + 1);
                if let Some(msg) = left[j].sync_generate(&id(j + 1)) {
                    right[0].sync_receive(&id(j), msg).unwrap();
                    any = true;
                }
                if let Some(msg) = right[0].sync_generate(&id(j)) {
                    left[j].sync_receive(&id(j + 1), msg).unwrap();
                    any = true;
                }
            }
            if !any {
                break;
            }
        }

        // Fingerprints (the CRDT convergence invariant) must agree.
        let fp = replicas[0].state_fingerprint();
        for replica in replicas.iter_mut().skip(1) {
            assert_eq!(replica.state_fingerprint(), fp);
        }

        // save() bytes are allowed to differ — and empirically they do.
        // Asserting they vary documents the current Automerge behavior so a
        // future-upgrade regression toward canonical save() is loud rather
        // than silent.
        let sizes: Vec<usize> = (0..n).map(|i| replicas[i].doc_size_bytes()).collect();
        assert!(
            sizes.iter().min() != sizes.iter().max(),
            "save() became canonical across line replicas — sizes: {sizes:?}. \
             Tighten the notebook doc-size table to strict equality.",
        );
    }
}
