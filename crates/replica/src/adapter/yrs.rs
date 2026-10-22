use std::collections::HashMap;

use anyhow::{Context, bail};
use common::{CrdtAdapter, Op, ScalarVal};
use yrs::{
    Any, Array, ArrayRef, Doc, GetString, In, Map, MapRef, Number, ReadTxn, Root, RootRef,
    StateVector, Text, TextRef, Transact, TransactionMut, Update, branch::Branch, types::TypeRef,
    updates::decoder::Decode,
};

use super::{map_name, require_name};

/// Returns the actual type of a root shared type if it is not `S`.
///
/// `Root::<S>::get_or_create` returns the existing branch when the name is
/// already in use, whatever its type. So the caller must check the type.
/// `TextRef`, `MapRef`, and `ArrayRef` have more than one `AsRef` impl, so a
/// bare `.as_ref()` is ambiguous. This function names `Branch` as the target.
fn wrong_type<S: RootRef>(s: &S) -> Option<&TypeRef> {
    let actual = AsRef::<Branch>::as_ref(s).type_ref();
    (actual != &S::type_ref()).then_some(actual)
}

/// Gets or creates the root shared type `S` named `name`.
///
/// Fails if `name` already names a different type.
fn get_or_create<S: RootRef>(txn: &mut TransactionMut, name: &str) -> anyhow::Result<S> {
    let root = Root::<S>::new(name).get_or_create(txn);
    if let Some(actual) = wrong_type(&root) {
        bail!(
            "object '{name}' already exists with type {actual:?}, not {:?}",
            S::type_ref()
        );
    }
    Ok(root)
}

/// Converts an `Op` index or count to the `u32` that Yrs uses.
fn to_u32(n: usize) -> anyhow::Result<u32> {
    u32::try_from(n).with_context(|| format!("index or count {n} does not fit in a Yrs u32"))
}

/// [`common::CrdtAdapter`] backed by `yrs::Doc`, the Rust port of Yjs. Its
/// list and text CRDT is YATA.
///
/// Yrs addresses a root shared type (`Text`, `Map`, or `Array`) by its name
/// only. Creating one is a local lookup in a registry that the CRDT does not
/// track. It authors no op and has no ID. Two replicas that create the same
/// root type therefore cannot collide, as two Automerge `put_object` calls
/// can. So, unlike [`super::AutomergeAdapter`], this adapter needs no
/// object-ID cache and no bootstrap actor. See `ensure_text`.
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
    /// `Uint` becomes `i64`, because `Any` has no unsigned variant. A value
    /// above `i64::MAX` is an error, not a wrapped number.
    fn to_any(v: &ScalarVal) -> anyhow::Result<Any> {
        Ok(match v {
            ScalarVal::Str(s) => Any::String(s.as_str().into()),
            ScalarVal::Uint(n) => Any::Number(Number::Int(
                i64::try_from(*n).with_context(|| format!("Uint {n} exceeds i64::MAX"))?,
            )),
            ScalarVal::Int(n) => Any::Number(Number::Int(*n)),
            ScalarVal::Bool(b) => Any::Bool(*b),
            ScalarVal::Bytes(b) => Any::Buffer(b.as_slice().into()),
        })
    }

    /// Encodes the full document: every block and the delete set.
    ///
    /// `state_fingerprint` and `doc_size_bytes` both use this encoding.
    fn full_update_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }
}

impl CrdtAdapter for YrsAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        // Convert and check every input before the first edit. `txn` commits
        // when it drops, also on an error return, so an error after an edit
        // would leave half an op in the document.
        let mut txn = self.doc.transact_mut();
        match op {
            Op::MapPut { obj, key, value } => {
                let value = Self::to_any(value)?;
                let map: MapRef = get_or_create(&mut txn, map_name(obj))?;
                map.insert(&mut txn, key.as_str(), In::Any(value));
            }
            Op::MapDelete { obj, key } => {
                let map: MapRef = get_or_create(&mut txn, map_name(obj))?;
                map.remove(&mut txn, key);
            }
            Op::ListInsert { obj, index, value } => {
                require_name(obj)?;
                let index = to_u32(*index)?;
                let value = Self::to_any(value)?;
                let arr: ArrayRef = get_or_create(&mut txn, obj)?;
                arr.insert(&mut txn, index, In::Any(value));
            }
            Op::ListDelete { obj, index } => {
                require_name(obj)?;
                let index = to_u32(*index)?;
                let arr: ArrayRef = get_or_create(&mut txn, obj)?;
                arr.remove(&mut txn, index);
            }
            Op::ListSplice {
                obj,
                pos,
                del_count,
                values,
            } => {
                require_name(obj)?;
                let del_count = to_u32(*del_count)?;
                let inserts = values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| Ok((to_u32(*pos + i)?, Self::to_any(v)?)))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let pos = to_u32(*pos)?;
                let arr: ArrayRef = get_or_create(&mut txn, obj)?;
                if del_count > 0 {
                    arr.remove_range(&mut txn, pos, del_count);
                }
                for (index, value) in inserts {
                    arr.insert(&mut txn, index, In::Any(value));
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
                require_name(obj)?;
                let pos = to_u32(*pos)?;
                let del_count = to_u32(*del_count)?;
                let text: TextRef = get_or_create(&mut txn, obj)?;
                if del_count > 0 {
                    text.remove_range(&mut txn, pos, del_count);
                }
                if !insert.is_empty() {
                    text.insert(&mut txn, pos, insert);
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
        let txn = self.doc.transact();
        let my_sv = txn.state_vector();
        // A diff against my own state vector contains no blocks. A Yrs update
        // always carries the full delete set, so this is the delete set plus
        // a few bytes of framing. Yrs has no delete-set version or counter,
        // so this is the cheapest way to see whether a delete occurred.
        let my_ds = txn.encode_diff_v1(&my_sv);
        let sent = self.peer_sent.get(peer);
        let empty = StateVector::default();
        let known = sent.map_or(&empty, |s| &s.sv);
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
        let bytes = txn.encode_diff_v1(known);
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
    /// `apply_op` does the same lookup before every `TextSplice`.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        require_name(obj)?;
        let mut txn = self.doc.transact_mut();
        get_or_create::<TextRef>(&mut txn, obj)?;
        Ok(())
    }

    fn text_length(&mut self, obj: &str) -> anyhow::Result<usize> {
        let txn = self.doc.transact();
        let Some(text) = Root::<TextRef>::new(obj).get(&txn) else {
            bail!("text_length: no object named '{obj}'");
        };
        if let Some(actual) = wrong_type(&text) {
            bail!("text_length: '{obj}' exists with type {actual:?}, not Text");
        }
        Ok(text.get_string(&txn).chars().count())
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

    // ── Characterization shared with Loro (see adapter::conformance) ───────

    #[test]
    fn yrs_ensure_text_is_a_pure_lookup_no_change_authored() {
        ensure_text_is_a_pure_lookup_no_change_authored::<YrsAdapter>();
    }

    #[test]
    fn yrs_partitioned_text_edits_interleave_even_without_ensure_text() {
        partitioned_text_edits_interleave_even_without_ensure_text::<YrsAdapter>();
    }

    #[test]
    fn yrs_uint_above_i64_max_is_rejected() {
        uint_above_i64_max_is_rejected::<YrsAdapter>();
    }

    // Not wrapped here, because they are Automerge-only (see
    // adapter::conformance):
    //
    // - ensure_text_is_deterministic_across_replicas. The mirror is
    //   ensure_text_is_a_pure_lookup_no_change_authored, wrapped above.
    // - without_bootstrap_partitioned_text_loses_a_side. The mirror is
    //   partitioned_text_edits_interleave_even_without_ensure_text, wrapped
    //   above.
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

    /// Yrs-only. Yrs indexes lists and text with `u32`. An index that does
    /// not fit is an error, and the document does not change.
    #[test]
    fn yrs_index_above_u32_max_is_rejected() {
        let mut a = YrsAdapter::default();
        let before = a.state_fingerprint();
        let err = a
            .apply_op(&Op::ListInsert {
                obj: "l".into(),
                index: 1 << 32,
                value: 1u64.into(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("does not fit"), "{err}");
        assert_eq!(a.state_fingerprint(), before);
    }
}
