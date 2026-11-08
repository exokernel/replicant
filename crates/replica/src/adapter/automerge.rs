use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, bail};
use automerge::{
    ActorId, AutoCommit, ObjId, ObjType, ROOT, ReadDoc, ScalarValue as AmScalarValue,
    ScalarValueRef, Value, ValueRef,
    sync::{self, SyncDoc},
    transaction::{CommitOptions, Transactable},
};
use common::{CrdtAdapter, Op, ScalarVal};

/// Fixed actor id for the bootstrap change that [`CrdtAdapter::ensure_text`]
/// authors.
///
/// Every replica authors that change with this actor, the same single op,
/// empty deps (the document must be empty), and commit time 0. So the change
/// bytes, its hash, and the new object's id (`OpId(1, this actor)`) are the
/// same on every replica, with no sync. The adapter restores its own actor
/// right after, so every real op stays attributed to its replica.
const BOOTSTRAP_ACTOR: &[u8] = b"replicant-bootstrap";

/// Converts an `Op` delete count to the `isize` that Automerge's splice
/// uses.
///
/// Call it before `resolve_obj`. `resolve_obj` can create the object, so an
/// error after it would leave half an op in the document.
fn to_isize(n: usize) -> anyhow::Result<isize> {
    isize::try_from(n).with_context(|| format!("delete count {n} exceeds isize::MAX"))
}

/// [`common::CrdtAdapter`] implementation backed by `automerge::AutoCommit`.
///
/// Each instance owns one document and one `sync::State` per peer. Objects
/// under ROOT are created lazily on first access. `objects` caches their ids,
/// so later ops skip the lookup through Automerge's `get` API.
pub struct AutomergeAdapter {
    /// The document.
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
    /// Creates an empty Automerge document with no peer state.
    pub fn new() -> Self {
        Self {
            doc: AutoCommit::new(),
            sync_states: HashMap::new(),
            objects: HashMap::new(),
        }
    }

    /// Converts a [`ScalarVal`] to an Automerge scalar.
    ///
    /// Primitives are copied. `Str` and `Bytes` clone their buffer.
    fn to_am_scalar(v: &ScalarVal) -> AmScalarValue {
        match v {
            ScalarVal::Str(s) => AmScalarValue::Str(s.as_str().into()),
            ScalarVal::Uint(n) => AmScalarValue::Uint(*n),
            ScalarVal::Int(n) => AmScalarValue::Int(*n),
            ScalarVal::Bool(b) => AmScalarValue::Boolean(*b),
            ScalarVal::Bytes(b) => AmScalarValue::Bytes(b.clone()),
        }
    }

    /// Returns the `ObjId` of a named top-level object, and creates the object
    /// if it does not exist.
    ///
    /// An empty `obj` resolves to ROOT. `obj_type` is used only on first
    /// access. Later calls return the cached id whatever the type.
    ///
    /// # Invariant
    ///
    /// Callers must pass the same `obj_type` for a given name on every call.
    /// If the type differs for a cached name, this returns the cached id with
    /// no error, and Automerge ops on it then fail or corrupt state. A
    /// `debug_assert!` catches this in debug builds.
    fn resolve_obj(&mut self, obj: &str, obj_type: ObjType) -> anyhow::Result<ObjId> {
        if obj.is_empty() {
            return Ok(ROOT);
        }
        if let Some(id) = self.objects.get(obj) {
            debug_assert!(
                // Check that the cached object still has the expected type.
                // This catches a caller that reuses a name with another type.
                self.doc.object_type(id).is_ok_and(|t| t == obj_type),
                "resolve_obj: '{obj}' was cached as a different type than {obj_type:?}"
            );
            return Ok(id.clone());
        }
        // The object can exist without being cached: a peer created it and
        // sync delivered it, or `ensure_text` created it. Creating a new one
        // here would put a concurrent object at the same key. The merge keeps
        // only one of the two and silently drops every op applied to the
        // other. So reuse before create.
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

    /// Looks up the object at `ROOT[obj]` and returns its id and type.
    fn lookup_obj(&self, obj: &str) -> Option<(ObjId, ObjType)> {
        match self.doc.get(ROOT, obj) {
            Ok(Some((Value::Object(t), id))) => Some((id, t)),
            _ => None,
        }
    }

    /// Applies `op` in the open transaction. Does not commit.
    fn apply_uncommitted(&mut self, op: &Op) -> anyhow::Result<()> {
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
                let del_count = to_isize(*del_count)?;
                let id = self.resolve_obj(obj, ObjType::List)?;
                let scalars = values.iter().map(Self::to_am_scalar);
                self.doc.splice(id, *pos, del_count, scalars)?;
            }
            Op::TextSplice {
                obj,
                pos,
                del_count,
                insert,
            } => {
                let del_count = to_isize(*del_count)?;
                let id = self.resolve_obj(obj, ObjType::Text)?;
                self.doc.splice_text(id, *pos, del_count, insert)?;
            }
        }
        Ok(())
    }

    /// Creates the object at `ROOT[obj]` with a deterministic first change.
    /// See [`CrdtAdapter::ensure_text`].
    fn ensure_object(&mut self, obj: &str, obj_type: ObjType) -> anyhow::Result<()> {
        if obj.is_empty() {
            bail!("ensure_object: object name must not be empty (ROOT is a map)");
        }
        match self.lookup_obj(obj) {
            Some((id, t)) if t == obj_type => {
                // Already present: bootstrapped earlier, or received via sync.
                self.objects.insert(obj.to_owned(), id);
                return Ok(());
            }
            Some((_, t)) => bail!("ensure_object: '{obj}' exists with type {t:?}"),
            None => {}
        }
        // The bootstrap change is deterministic only on an empty document. A
        // prior change would go into `deps`. Two replicas that author
        // different deps under the same (actor, seq) corrupt the DAG instead
        // of deduplicating.
        if !self.doc.get_heads().is_empty() {
            bail!(
                "ensure_object: document already has changes; bootstrap must be the first change"
            );
        }
        let own_actor = self.doc.get_actor().clone();
        self.doc.set_actor(ActorId::from(BOOTSTRAP_ACTOR));
        let id = self
            .doc
            .put_object(ROOT, obj, obj_type)
            .with_context(|| format!("bootstrapping object '{obj}'"))?;
        // Time 0 keeps the change bytes identical across replicas and runs.
        // The default commit also uses 0, but determinism is the purpose of
        // this change, so set it explicitly.
        self.doc.commit_with(CommitOptions::default().with_time(0));
        self.doc.set_actor(own_actor);
        self.objects.insert(obj.to_owned(), id);
        Ok(())
    }
}

/// Converts one Automerge value to a [`ScalarVal`]. Fails on an object or on a
/// scalar type that the op model does not have.
fn from_am_value(value: ValueRef<'_>) -> anyhow::Result<ScalarVal> {
    match value {
        ValueRef::Scalar(ScalarValueRef::Str(s)) => Ok(ScalarVal::Str(s.to_string())),
        ValueRef::Scalar(ScalarValueRef::Int(n)) => Ok(ScalarVal::Int(n)),
        ValueRef::Scalar(ScalarValueRef::Uint(n)) => Ok(ScalarVal::Uint(n)),
        ValueRef::Scalar(ScalarValueRef::Boolean(b)) => Ok(ScalarVal::Bool(b)),
        ValueRef::Scalar(ScalarValueRef::Bytes(b)) => Ok(ScalarVal::Bytes(b.to_vec())),
        ValueRef::Scalar(other) => bail!("value {other:?} has no ScalarVal form"),
        ValueRef::Object(t) => bail!("value is a nested {t:?}, not a scalar"),
    }
}

impl CrdtAdapter for AutomergeAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        self.apply_uncommitted(op)?;
        // Commit the open transaction now. Then `op_duration_ms` includes the
        // commit cost, and the next `sync_generate` sees exactly this change.
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
        // The sorted, concatenated head hashes. Two replicas have equal
        // fingerprints if and only if they have the same DAG frontier.
        self.get_heads().into_iter().flatten().collect()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // `save()` produces the full binary encoding. Automerge has no cheaper
        // size query. The cost is acceptable because this runs once per op,
        // for the size gauge.
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
        // Removing the entry is the whole reset. The next generate or receive
        // creates a fresh `sync::State`, which starts the protocol from the
        // full handshake.
        self.sync_states.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        self.ensure_object(obj, ObjType::Text)
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

    /// Uses 16 bytes, the width of a random actor id.
    fn set_instance_id(&mut self, id: u32) -> anyhow::Result<()> {
        let mut actor = [0xa5u8; 16];
        actor[12..].copy_from_slice(&id.to_be_bytes());
        self.doc.set_actor(ActorId::from(actor.as_slice()));
        Ok(())
    }

    fn ensure_list(&mut self, obj: &str) -> anyhow::Result<()> {
        self.ensure_object(obj, ObjType::List)
    }

    fn read_text(&mut self, obj: &str) -> anyhow::Result<String> {
        match self.lookup_obj(obj) {
            Some((id, ObjType::Text)) => self
                .doc
                .text(&id)
                .with_context(|| format!("reading text object '{obj}'")),
            Some((_, t)) => bail!("read_text: '{obj}' exists with type {t:?}, not Text"),
            None => bail!("read_text: no object named '{obj}'"),
        }
    }

    fn read_list(&mut self, obj: &str) -> anyhow::Result<Vec<ScalarVal>> {
        match self.lookup_obj(obj) {
            Some((id, ObjType::List)) => self
                .doc
                .list_range(&id, ..)
                .map(|item| from_am_value(item.value))
                .collect(),
            Some((_, t)) => bail!("read_list: '{obj}' exists with type {t:?}, not List"),
            None => bail!("read_list: no object named '{obj}'"),
        }
    }

    fn read_map(&mut self, obj: &str) -> anyhow::Result<BTreeMap<String, ScalarVal>> {
        let id = if obj.is_empty() {
            ROOT
        } else {
            match self.lookup_obj(obj) {
                Some((id, ObjType::Map)) => id,
                Some((_, t)) => bail!("read_map: '{obj}' exists with type {t:?}, not Map"),
                None => bail!("read_map: no object named '{obj}'"),
            }
        };
        self.doc
            .map_range(&id, ..)
            .map(|item| Ok((item.key.to_string(), from_am_value(item.value)?)))
            .collect()
    }

    fn apply_txn(&mut self, ops: &[Op]) -> anyhow::Result<Vec<u8>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        for op in ops {
            self.apply_uncommitted(op)?;
        }
        // The commit gives no hash when the ops changed nothing, for example a
        // delete of a missing key. Then the update is empty. Otherwise, the new
        // change holds these ops and no others, so the update does not repeat
        // changes that this replica received from a peer.
        let Some(hash) = self.doc.commit() else {
            return Ok(Vec::new());
        };
        self.doc
            .get_change_by_hash(&hash)
            .map(|change| change.raw_bytes().to_vec())
            .context("apply_txn: no change for the new commit hash")
    }

    fn apply_updates(&mut self, updates: &[&[u8]]) -> anyhow::Result<()> {
        for update in updates {
            self.doc
                .load_incremental(update)
                .context("applying an Automerge update")?;
        }
        Ok(())
    }

    fn encode(&mut self) -> Vec<u8> {
        self.doc.save()
    }

    fn load(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.doc = AutoCommit::load(bytes).context("loading an Automerge document")?;
        self.objects.clear();
        self.sync_states.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::AutomergeAdapter;
    use crate::adapter::conformance::*;
    use common::{CrdtAdapter, Op};

    conformance_tests!(
        AutomergeAdapter:
        reads_return_the_written_content,
        txn_updates_carry_inserts_and_deletes,
        txn_applies_ops_in_sequence,
        txn_update_excludes_received_ops,
        update_batch_equals_one_by_one,
        encode_and_load_round_trip,
        partitioned_list_inserts_merge_after_ensure_list,
        set_instance_id_gives_distinct_replicas,
        fixed_ids_make_a_merge_repeatable,
        identical_ops_yield_equal_fingerprints,
        disjoint_edits_converge_after_sync,
        concurrent_edits_to_same_key_converge,
        post_sync_divergence_is_detected,
        root_map_ops_are_supported,
        each_op_variant_mutates_the_doc,
        reads_are_stable_without_writes,
        sync_is_idempotent_once_converged,
        three_replicas_converge_through_a_hub,
        doc_size_grows_with_every_op,
        reset_returns_adapter_to_a_fresh_state,
        reset_drops_stale_peer_state,
        reset_allows_clean_re_sync_with_equal_fingerprint,
        reset_clears_doc_and_sync_state,
        reset_allows_clean_re_sync_to_another_replica,
        ensure_text_is_deterministic_across_replicas,
        ensure_text_is_idempotent,
        partitioned_text_edits_interleave_after_bootstrap,
        without_bootstrap_partitioned_text_loses_a_side,
        ensure_text_rejects_non_empty_doc_without_the_object,
        ensure_text_adopts_synced_in_object,
        first_local_write_reuses_synced_in_object,
        sync_reset_forgets_quiescence,
        sync_reset_recovers_from_a_lost_message,
        sync_reset_unknown_peer_is_noop,
        text_length_errors_on_missing_or_wrong_type,
        save_bytes_not_canonical_across_converged_replicas,
    );

    // Not wrapped here: uint_above_i64_max_is_rejected and the two Yrs and
    // Loro mirrors of the bootstrap tests. The test below asserts what
    // Automerge does instead of the first.

    /// A transaction that changes nothing gives an empty update. It must not
    /// give the change of an earlier transaction again.
    #[test]
    fn a_txn_that_changes_nothing_gives_an_empty_update() {
        let mut a = AutomergeAdapter::default();
        let first = a.apply_txn(&[map_put("", "k", 1i64)]).unwrap();
        assert!(!first.is_empty());
        let missing = Op::MapDelete {
            obj: String::new(),
            key: "missing".into(),
        };
        assert_eq!(a.apply_txn(&[missing]).unwrap(), Vec::<u8>::new());
    }

    /// Automerge version of `uint_above_i64_max_is_rejected` (Yrs and Loro).
    ///
    /// Automerge has a native `u64` scalar, so it stores the value.
    #[test]
    fn automerge_stores_uint_above_i64_max() {
        let mut a = AutomergeAdapter::default();
        let before = a.state_fingerprint();
        a.apply_op(&map_put("m", "k", u64::MAX)).unwrap();
        assert_ne!(a.state_fingerprint(), before);
    }
}
