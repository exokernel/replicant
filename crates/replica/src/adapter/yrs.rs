use std::collections::HashMap;

use anyhow::{Context, bail};
use common::{CrdtAdapter, Op, ScalarVal};
use yrs::{
    Any, Array, ArrayRef, Doc, GetString, In, Map, MapRef, Number, ReadTxn, Root, SharedRef,
    StateVector, Text, TextRef, Transact, TransactionMut, Update, branch::Branch, types::TypeRef,
    updates::decoder::Decode,
};

/// Returns the type of a root shared type.
///
/// `TextRef`, `MapRef`, and `ArrayRef` have more than one `AsRef` impl, so a
/// bare `.as_ref()` is ambiguous. This function names `Branch` as the target.
fn type_ref_of<S: SharedRef>(s: &S) -> &TypeRef {
    AsRef::<Branch>::as_ref(s).type_ref()
}

/// Name of the root map that stands in for ROOT (the empty object name).
///
/// Yrs has no unnamed root map, so the adapter maps `obj == ""` to this name.
/// Every replica resolves the name the same way, and no op creates the map,
/// so two replicas cannot create it concurrently.
///
/// Do not use this name for a workload object. It would alias ROOT.
const ROOT_MAP_NAME: &str = "_root";

/// [`common::CrdtAdapter`] backed by `yrs::Doc`, the Rust port of Yjs. Its
/// list and text CRDT is YATA.
///
/// Yrs addresses a root shared type (`Text`, `Map`, or `Array`) by its name
/// only. Creating one is a local lookup in a registry that the CRDT does not
/// track. It authors no op and has no ID. Two replicas that create the same
/// root type therefore cannot collide, as two Automerge `put_object` calls
/// can. So, unlike [`super::automerge::AutomergeAdapter`], this adapter needs
/// no object-ID cache and no bootstrap actor. See `ensure_text`.
pub struct YrsAdapter {
    doc: Doc,
    /// What this adapter has already sent to each peer.
    ///
    /// Yrs keeps no per-peer sync state, so the adapter keeps it. Without this
    /// map, `sync_generate` would send the full document on every call. That
    /// is correct, because Yrs updates are idempotent. But it never returns
    /// `None`, and the replica's push loop and the
    /// `sync_is_idempotent_once_converged` test both need `None` once the
    /// peers agree.
    peer_sent: HashMap<String, PeerSent>,
}

/// What [`YrsAdapter`] has already sent to one peer.
///
/// Both fields are necessary. **A state vector alone cannot detect a
/// deletion.** A Yrs delete marks an existing block and adds the range to a
/// separate delete set. It authors no block and advances no clock. If the
/// "nothing new" check used only the state vector, a delete made after the
/// replicas went quiet would never be sent.
///
/// Rule: any Yrs bookkeeping that counts what each client authored cannot see
/// deletes. `state_fingerprint` has the same constraint (see `get_heads`).
struct PeerSent {
    sv: StateVector,
    /// The encoded delete set at the last send. It is compared by value, not
    /// by hash, so a hash collision cannot hide a deletion.
    ds: Vec<u8>,
}

impl Default for YrsAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl YrsAdapter {
    /// Creates an empty Yrs document with no peer state.
    ///
    /// `Doc::new()` picks a random `ClientID`, so two calls give two distinct
    /// replicas. The conformance suite's `A: CrdtAdapter + Default` bound
    /// needs this.
    pub fn new() -> Self {
        Self {
            doc: Doc::new(),
            peer_sent: HashMap::new(),
        }
    }

    /// Converts a [`ScalarVal`] to a Yrs `Any`.
    ///
    /// `Uint` becomes `i64`, because `Any` has no unsigned variant. Values
    /// above `i64::MAX` wrap. Benchmark values are small counters and indices,
    /// so this does not occur today.
    fn to_any(v: &ScalarVal) -> Any {
        match v {
            ScalarVal::Str(s) => Any::String(s.as_str().into()),
            ScalarVal::Uint(n) => Any::Number(Number::Int(*n as i64)),
            ScalarVal::Int(n) => Any::Number(Number::Int(*n)),
            ScalarVal::Bool(b) => Any::Bool(*b),
            ScalarVal::Bytes(b) => Any::Buffer(b.as_slice().into()),
        }
    }

    /// Encodes the full document: every block and the delete set.
    ///
    /// `state_fingerprint` and `doc_size_bytes` both use this encoding.
    fn full_update_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }

    /// Encodes the document's delete set.
    ///
    /// A diff against the current state vector contains no blocks. A Yrs
    /// update always carries the full delete set, so the result is the delete
    /// set plus a few bytes of framing. Yrs has no delete-set version or
    /// counter, so this is the cheapest way to see whether a delete occurred.
    fn delete_set_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        let sv = txn.state_vector();
        txn.encode_diff_v1(&sv)
    }

    /// Gets or creates the root `Map` named `obj`.
    ///
    /// An empty `obj` resolves to [`ROOT_MAP_NAME`]. Fails if `obj` already
    /// names a different type.
    fn get_or_create_map(txn: &mut TransactionMut, obj: &str) -> anyhow::Result<MapRef> {
        let obj = if obj.is_empty() { ROOT_MAP_NAME } else { obj };
        let map = Root::<MapRef>::new(obj).get_or_create(txn);
        let actual = type_ref_of(&map);
        if actual != &TypeRef::Map {
            bail!("object '{obj}' already exists with type {actual:?}, not Map");
        }
        Ok(map)
    }

    /// Gets or creates the root `Array` named `obj`.
    ///
    /// Fails if `obj` is empty, because ROOT is a map, not a list. Fails if
    /// `obj` already names a different type.
    fn get_or_create_array(txn: &mut TransactionMut, obj: &str) -> anyhow::Result<ArrayRef> {
        if obj.is_empty() {
            bail!("object name must not be empty: Yrs has no implicit root array");
        }
        let arr = Root::<ArrayRef>::new(obj).get_or_create(txn);
        let actual = type_ref_of(&arr);
        if actual != &TypeRef::Array {
            bail!("object '{obj}' already exists with type {actual:?}, not Array");
        }
        Ok(arr)
    }

    /// Gets or creates the root `Text` named `obj`.
    ///
    /// Fails if `obj` is empty or already names a different type. This is
    /// also the full implementation of [`CrdtAdapter::ensure_text`].
    fn get_or_create_text(txn: &mut TransactionMut, obj: &str) -> anyhow::Result<TextRef> {
        if obj.is_empty() {
            bail!("object name must not be empty: Yrs has no implicit root text");
        }
        let text = Root::<TextRef>::new(obj).get_or_create(txn);
        let actual = type_ref_of(&text);
        if actual != &TypeRef::Text {
            bail!("object '{obj}' already exists with type {actual:?}, not Text");
        }
        Ok(text)
    }
}

impl CrdtAdapter for YrsAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        match op {
            Op::MapPut { obj, key, value } => {
                let map = Self::get_or_create_map(&mut txn, obj)?;
                map.insert(&mut txn, key.as_str(), In::Any(Self::to_any(value)));
            }
            Op::MapDelete { obj, key } => {
                let map = Self::get_or_create_map(&mut txn, obj)?;
                map.remove(&mut txn, key);
            }
            Op::ListInsert { obj, index, value } => {
                let arr = Self::get_or_create_array(&mut txn, obj)?;
                arr.insert(&mut txn, *index as u32, In::Any(Self::to_any(value)));
            }
            Op::ListDelete { obj, index } => {
                let arr = Self::get_or_create_array(&mut txn, obj)?;
                arr.remove(&mut txn, *index as u32);
            }
            Op::ListSplice {
                obj,
                pos,
                del_count,
                values,
            } => {
                let arr = Self::get_or_create_array(&mut txn, obj)?;
                if *del_count > 0 {
                    arr.remove_range(&mut txn, *pos as u32, *del_count as u32);
                }
                for (i, v) in values.iter().enumerate() {
                    arr.insert(&mut txn, (*pos + i) as u32, In::Any(Self::to_any(v)));
                }
            }
            Op::TextSplice {
                obj,
                pos,
                del_count,
                insert,
            } => {
                // CAUTION: Yrs indexes text by UTF-8 byte offset.
                // `Op::TextSplice` uses char offsets, as Automerge does. The
                // two are equal only for ASCII text. All current workloads
                // are ASCII. A non-ASCII workload needs a char-to-byte
                // translation here.
                let text = Self::get_or_create_text(&mut txn, obj)?;
                if *del_count > 0 {
                    text.remove_range(&mut txn, *pos as u32, *del_count as u32);
                }
                if !insert.is_empty() {
                    text.insert(&mut txn, *pos as u32, insert);
                }
            }
        }
        // `txn` commits when it drops. There is no separate commit call.
        Ok(())
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        // Yrs has no hash DAG of changes. Its version is the state vector:
        // the highest clock seen from each client. Each head is one
        // (client_id: 8 bytes BE, clock: 4 bytes BE) pair. The pairs are
        // sorted here, because the order of `StateVector`'s own encoder is
        // not documented as canonical.
        //
        // CAUTION: the heads are not a convergence fingerprint. A delete does
        // not change the state vector (see `PeerSent`). Two documents can
        // have the same heads and different content. `state_fingerprint`
        // therefore does not use this method.
        let txn = self.doc.transact();
        let sv = txn.state_vector();
        let mut heads: Vec<Vec<u8>> = sv
            .iter()
            .map(|(client, clock)| {
                let mut buf = Vec::with_capacity(12);
                buf.extend_from_slice(&client.get().to_be_bytes());
                buf.extend_from_slice(&clock.to_be_bytes());
                buf
            })
            .collect();
        heads.sort_unstable();
        heads
    }

    fn state_fingerprint(&mut self) -> Vec<u8> {
        // Do not use `get_heads()` here, as AutomergeAdapter does. The heads
        // do not change on a delete (see `get_heads`). The full encoding
        // includes the delete set, so it changes on every op.
        // `each_op_variant_mutates_the_doc` checks this.
        self.full_update_bytes()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // Yrs has no separate save format. The full document encoding is an
        // update from the empty state vector. These are the same bytes as
        // `state_fingerprint`.
        self.full_update_bytes().len()
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let my_ds = self.delete_set_bytes();
        let txn = self.doc.transact();
        let my_sv = txn.state_vector();
        let sent = self.peer_sent.get(peer);
        let known = sent.map(|s| s.sv.clone()).unwrap_or_default();
        // Send nothing only if both conditions are true:
        // 1. The peer already has every clock in my state vector.
        // 2. My delete set has not changed since the last send.
        // Condition 2 is necessary. See `PeerSent`.
        let blocks_covered = my_sv
            .iter()
            .all(|(client, clock)| known.get(client) >= *clock);
        let deletes_covered = sent.is_some_and(|s| s.ds == my_ds);
        if blocks_covered && deletes_covered {
            return None;
        }
        let bytes = txn.encode_diff_v1(&known);
        drop(txn);
        // Assume that this message arrives, so the next call does not send
        // the same bytes again. Automerge's `sync::State` does the same. If
        // the message is lost, `sync_reset` recovers.
        self.peer_sent.insert(
            peer.to_owned(),
            PeerSent {
                sv: my_sv,
                ds: my_ds,
            },
        );
        Some(bytes)
    }

    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        let update = Update::decode_v1(&msg)
            .with_context(|| format!("decoding sync message from '{peer}'"))?;
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update)
            .with_context(|| format!("applying update from '{peer}'"))?;
        Ok(())
    }

    fn sync_reset(&mut self, peer: &str) {
        // Removing the entry is the full reset. With no entry, the next
        // `sync_generate` diffs from an empty state vector, and its delete-set
        // check fails. So it sends the full document.
        self.peer_sent.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    /// Does a local lookup. Authors no change.
    ///
    /// Yrs root types cannot collide (see [`YrsAdapter`]). So there is no
    /// bootstrap actor and no "must be the first change" precondition.
    /// `apply_op` calls the same `get_or_create_text` before every
    /// `TextSplice`. The Yrs-specific tests below check this behaviour.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        Self::get_or_create_text(&mut txn, obj)?;
        Ok(())
    }

    fn text_length(&mut self, obj: &str) -> anyhow::Result<usize> {
        let txn = self.doc.transact();
        match Root::<TextRef>::new(obj).get(&txn) {
            Some(text) => {
                let actual = type_ref_of(&text);
                if actual != &TypeRef::Text {
                    bail!("text_length: '{obj}' exists with type {actual:?}, not Text");
                }
                Ok(text.get_string(&txn).chars().count())
            }
            None => bail!("text_length: no object named '{obj}'"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::YrsAdapter;
    use crate::adapter::conformance::*;
    use common::{CrdtAdapter, Op};

    // ── Universal conformance (see adapter::conformance) ───────────────────

    #[test]
    fn yrs_identical_ops_yield_equal_fingerprints() {
        identical_ops_yield_equal_fingerprints::<YrsAdapter>();
    }

    #[test]
    fn yrs_disjoint_edits_converge_after_sync() {
        disjoint_edits_converge_after_sync::<YrsAdapter>();
    }

    #[test]
    fn yrs_concurrent_edits_to_same_key_converge() {
        concurrent_edits_to_same_key_converge::<YrsAdapter>();
    }

    #[test]
    fn yrs_post_sync_divergence_is_detected() {
        post_sync_divergence_is_detected::<YrsAdapter>();
    }

    #[test]
    fn yrs_root_map_ops_are_supported() {
        root_map_ops_are_supported::<YrsAdapter>();
    }

    #[test]
    fn yrs_each_op_variant_mutates_the_doc() {
        each_op_variant_mutates_the_doc::<YrsAdapter>();
    }

    #[test]
    fn yrs_reads_are_stable_without_writes() {
        reads_are_stable_without_writes::<YrsAdapter>();
    }

    #[test]
    fn yrs_sync_is_idempotent_once_converged() {
        sync_is_idempotent_once_converged::<YrsAdapter>();
    }

    #[test]
    fn yrs_three_replicas_converge_through_a_hub() {
        three_replicas_converge_through_a_hub::<YrsAdapter>();
    }

    #[test]
    fn yrs_doc_size_grows_with_every_op() {
        doc_size_grows_with_every_op::<YrsAdapter>();
    }

    #[test]
    fn yrs_reset_returns_adapter_to_a_fresh_state() {
        reset_returns_adapter_to_a_fresh_state::<YrsAdapter>();
    }

    #[test]
    fn yrs_reset_drops_stale_peer_state() {
        reset_drops_stale_peer_state::<YrsAdapter>();
    }

    #[test]
    fn yrs_reset_allows_clean_re_sync_with_equal_fingerprint() {
        reset_allows_clean_re_sync_with_equal_fingerprint::<YrsAdapter>();
    }

    #[test]
    fn yrs_ensure_text_is_idempotent() {
        ensure_text_is_idempotent::<YrsAdapter>();
    }

    #[test]
    fn yrs_partitioned_text_edits_interleave_after_bootstrap() {
        partitioned_text_edits_interleave_after_bootstrap::<YrsAdapter>();
    }

    #[test]
    fn yrs_ensure_text_adopts_synced_in_object() {
        ensure_text_adopts_synced_in_object::<YrsAdapter>();
    }

    #[test]
    fn yrs_first_local_write_reuses_synced_in_object() {
        first_local_write_reuses_synced_in_object::<YrsAdapter>();
    }

    #[test]
    fn yrs_sync_reset_forgets_quiescence() {
        sync_reset_forgets_quiescence::<YrsAdapter>();
    }

    #[test]
    fn yrs_sync_reset_recovers_from_a_lost_message() {
        sync_reset_recovers_from_a_lost_message::<YrsAdapter>();
    }

    #[test]
    fn yrs_sync_reset_unknown_peer_is_noop() {
        sync_reset_unknown_peer_is_noop::<YrsAdapter>();
    }

    #[test]
    fn yrs_text_length_errors_on_missing_or_wrong_type() {
        text_length_errors_on_missing_or_wrong_type::<YrsAdapter>();
    }

    // Not wrapped here, because they are Automerge-only (see
    // adapter::conformance):
    //
    // - ensure_text_is_deterministic_across_replicas
    // - without_bootstrap_partitioned_text_loses_a_side
    // - ensure_text_rejects_non_empty_doc_without_the_object
    // - save_bytes_not_canonical_across_converged_replicas. Yrs gives the
    //   opposite result; see yrs_save_bytes_are_canonical_across_converged_replicas.
    // - reset_clears_doc_and_sync_state. It assumes that a fresh adapter has
    //   a handshake to send and that its fingerprint is empty. Neither is
    //   true for Yrs.
    // - reset_allows_clean_re_sync_to_another_replica.
    //
    // The generic reset_* tests wrapped above cover the same properties.

    // ── Yrs-specific characterization ───────────────────────────────────
    //
    // Each test is the Yrs counterpart of an Automerge-only test above. It
    // shows why that test does not apply to Yrs.

    /// Counterpart to `ensure_text_is_deterministic_across_replicas`.
    ///
    /// In Yrs, `ensure_text` authors no change, because creating a root type
    /// is a local lookup. The heads stay empty, and that is the expected
    /// result.
    #[test]
    fn yrs_ensure_text_is_a_pure_lookup_no_change_authored() {
        let mut a = YrsAdapter::default();
        let mut b = YrsAdapter::default();
        a.ensure_text("text").unwrap();
        b.ensure_text("text").unwrap();

        assert!(
            a.get_heads().is_empty(),
            "ensure_text must not author a change for Yrs"
        );
        assert_eq!(a.get_heads(), b.get_heads());
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    }

    /// Counterpart to `without_bootstrap_partitioned_text_loses_a_side`.
    ///
    /// In Automerge, two replicas that create the same text object
    /// concurrently collide, and the merge keeps only one side. In Yrs, a root
    /// object has no creation ID, so it cannot collide. This test skips
    /// `ensure_text` and lets the first `TextSplice` create the object. Both
    /// sides' edits survive the merge.
    #[test]
    fn yrs_partitioned_text_edits_interleave_even_without_ensure_text() {
        let mut a = YrsAdapter::default();
        let mut b = YrsAdapter::default();
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
            20,
            "no lazy-creation collision to lose a side to"
        );
        assert_eq!(b.text_length("text").unwrap(), 20);
    }

    /// Counterpart to `save_bytes_not_canonical_across_converged_replicas`.
    /// Yrs gives the opposite result.
    ///
    /// Automerge's `save()` keeps changes in the order the replica received
    /// them. That order depends on the replica's position in the graph. Yrs
    /// sorts blocks by client ID before it encodes them. Converged Yrs
    /// replicas therefore produce identical bytes, whatever their position.
    #[test]
    fn yrs_save_bytes_are_canonical_across_converged_replicas() {
        let mut replicas: Vec<YrsAdapter> = (0..5).map(|_| YrsAdapter::default()).collect();
        converged_line(&mut replicas, 10);

        let sizes: Vec<usize> = replicas
            .iter_mut()
            .map(YrsAdapter::doc_size_bytes)
            .collect();
        assert!(
            sizes.iter().min() == sizes.iter().max(),
            "save() became non-canonical across line replicas — sizes: {sizes:?}. \
             If this starts failing, without_bootstrap_partitioned_text_loses_a_side-style \
             analysis is needed to find what changed in yrs's encoder.",
        );
    }
}
