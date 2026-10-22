use std::collections::HashMap;

use anyhow::{Context, bail};
use automerge::{
    ActorId, AutoCommit, ObjId, ObjType, ROOT, ReadDoc, ScalarValue as AmScalarValue, Value,
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

/// [`common::CrdtAdapter`] implementation backed by `automerge::AutoCommit`.
///
/// Each instance owns one document and one `sync::State` per peer. Objects
/// under ROOT are created lazily on first access. `objects` caches their ids,
/// so later ops skip the lookup through Automerge's `get` API.
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
        if obj.is_empty() {
            bail!("ensure_text: object name must not be empty (ROOT is a map)");
        }
        match self.lookup_obj(obj) {
            Some((id, ObjType::Text)) => {
                // Already present: bootstrapped earlier, or received via sync.
                self.objects.insert(obj.to_owned(), id);
                return Ok(());
            }
            Some((_, t)) => bail!("ensure_text: '{obj}' exists with type {t:?}"),
            None => {}
        }
        // The bootstrap change is deterministic only on an empty document. A
        // prior change would go into `deps`. Two replicas that author
        // different deps under the same (actor, seq) corrupt the DAG instead
        // of deduplicating.
        if !self.doc.get_heads().is_empty() {
            bail!("ensure_text: document already has changes; bootstrap must be the first change");
        }
        let own_actor = self.doc.get_actor().clone();
        self.doc.set_actor(ActorId::from(BOOTSTRAP_ACTOR));
        let id = self
            .doc
            .put_object(ROOT, obj, ObjType::Text)
            .with_context(|| format!("bootstrapping text object '{obj}'"))?;
        // Time 0 keeps the change bytes identical across replicas and runs.
        // The default commit also uses 0, but determinism is the purpose of
        // this change, so set it explicitly.
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
    use super::AutomergeAdapter;
    use crate::adapter::conformance::*;

    #[test]
    fn automerge_identical_ops_yield_equal_fingerprints() {
        identical_ops_yield_equal_fingerprints::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_disjoint_edits_converge_after_sync() {
        disjoint_edits_converge_after_sync::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_concurrent_edits_to_same_key_converge() {
        concurrent_edits_to_same_key_converge::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_post_sync_divergence_is_detected() {
        post_sync_divergence_is_detected::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_root_map_ops_are_supported() {
        root_map_ops_are_supported::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_each_op_variant_mutates_the_doc() {
        each_op_variant_mutates_the_doc::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reads_are_stable_without_writes() {
        reads_are_stable_without_writes::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_sync_is_idempotent_once_converged() {
        sync_is_idempotent_once_converged::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_three_replicas_converge_through_a_hub() {
        three_replicas_converge_through_a_hub::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_doc_size_grows_with_every_op() {
        doc_size_grows_with_every_op::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reset_returns_adapter_to_a_fresh_state() {
        reset_returns_adapter_to_a_fresh_state::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reset_drops_stale_peer_state() {
        reset_drops_stale_peer_state::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reset_allows_clean_re_sync_with_equal_fingerprint() {
        reset_allows_clean_re_sync_with_equal_fingerprint::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reset_clears_doc_and_sync_state() {
        reset_clears_doc_and_sync_state::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_reset_allows_clean_re_sync_to_another_replica() {
        reset_allows_clean_re_sync_to_another_replica::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_ensure_text_is_deterministic_across_replicas() {
        ensure_text_is_deterministic_across_replicas::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_ensure_text_is_idempotent() {
        ensure_text_is_idempotent::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_partitioned_text_edits_interleave_after_bootstrap() {
        partitioned_text_edits_interleave_after_bootstrap::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_without_bootstrap_partitioned_text_loses_a_side() {
        without_bootstrap_partitioned_text_loses_a_side::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_ensure_text_rejects_non_empty_doc_without_the_object() {
        ensure_text_rejects_non_empty_doc_without_the_object::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_ensure_text_adopts_synced_in_object() {
        ensure_text_adopts_synced_in_object::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_first_local_write_reuses_synced_in_object() {
        first_local_write_reuses_synced_in_object::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_sync_reset_forgets_quiescence() {
        sync_reset_forgets_quiescence::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_sync_reset_recovers_from_a_lost_message() {
        sync_reset_recovers_from_a_lost_message::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_sync_reset_unknown_peer_is_noop() {
        sync_reset_unknown_peer_is_noop::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_text_length_errors_on_missing_or_wrong_type() {
        text_length_errors_on_missing_or_wrong_type::<AutomergeAdapter>();
    }

    #[test]
    fn automerge_save_bytes_not_canonical_across_converged_replicas() {
        save_bytes_not_canonical_across_converged_replicas::<AutomergeAdapter>();
    }
}
