use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, bail};
use common::{CrdtAdapter, INSTANCE_ID_BITS, Op, ScalarVal};
use yrs::{
    Any, Array, ArrayRef, Doc, GetString, In, Map, MapRef, Number, ReadTxn, Root, RootRef,
    StateVector, Text, TextRef, Transact, TransactionMut, Update,
    branch::Branch,
    types::{ToJson, TypeRef},
    updates::{decoder::Decode, encoder::Encode},
};

use super::frame::Frame;
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

/// Returns the root shared type `S` named `name`. Fails if it does not exist or
/// has a different type.
///
/// A root type that arrives only through an update has no known type until a
/// local call names one. `get_or_create` sets the type. Thus a read needs a
/// write transaction, also on a document that came from `load`.
fn existing<S: RootRef>(txn: &mut TransactionMut, name: &str, what: &str) -> anyhow::Result<S> {
    if Root::<S>::new(name).get(txn).is_none() {
        bail!("{what}: no object named '{name}'");
    }
    get_or_create(txn, name)
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
    /// The document.
    doc: Doc,
    /// The sync state of each peer. Yrs keeps none, so the adapter keeps it.
    /// See [`PeerSync`].
    peers: HashMap<String, PeerSync>,
}

/// The sync state of [`YrsAdapter`] for one peer.
///
/// The protocol follows the Yjs sync protocol. Each side sends its state vector.
/// The other side answers with the blocks that this state vector lacks, and its
/// delete set. After that, each side sends what it adds. See
/// [`super::frame`].
///
/// **A state vector alone cannot detect a deletion.** A Yrs delete marks an
/// existing block and adds the range to a separate delete set. It authors no
/// block and advances no clock. So the state also records the delete set at the
/// last send, and a changed delete set is sent again.
///
/// Rule: any Yrs bookkeeping that counts what each client authored cannot see
/// deletes. `state_fingerprint` has the same constraint (see `get_heads`).
#[derive(Clone)]
struct PeerSync {
    /// The blocks that the peer has, as far as this adapter knows: its state
    /// vector, what this adapter sent, and what the peer sent. `None` until the
    /// peer sends its state vector.
    known: Option<StateVector>,
    /// The encoded delete set at the last send. `None` if this adapter has not
    /// sent one since it learned the peer's state vector. It is compared by
    /// value, not by hash, so a hash collision cannot hide a deletion.
    sent_ds: Option<Vec<u8>>,
    /// True if this adapter must send its own state vector.
    version_due: bool,
}

impl PeerSync {
    /// Returns the state of a peer that has sent nothing yet.
    fn new() -> Self {
        Self {
            known: None,
            sent_ds: None,
            version_due: true,
        }
    }
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
            peers: HashMap::new(),
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

    /// Returns the next sync message for `peer` and the peer state after it, or
    /// `None` if there is nothing to send. Changes no state.
    fn next_message(&self, peer: &str) -> Option<(Frame, PeerSync)> {
        let state = self.peers.get(peer).cloned().unwrap_or_else(PeerSync::new);
        let txn = self.doc.transact();
        let my_sv = txn.state_vector();
        // A diff against my own state vector contains no blocks. A Yrs update
        // always carries the full delete set, so this is the delete set plus
        // a few bytes of framing. Yrs has no delete-set version or counter,
        // so this is the cheapest way to see whether a delete occurred.
        let my_ds = txn.encode_diff_v1(&my_sv);
        // Send data only to a peer whose state vector is known. Send nothing
        // only if both conditions are true:
        // 1. The peer already has every clock in my state vector.
        // 2. My delete set has not changed since the last send.
        // Condition 2 is necessary. See `PeerSync`.
        let update = state.known.as_ref().and_then(|known| {
            let blocks_covered = my_sv
                .iter()
                .all(|(client, clock)| known.get(client) >= *clock);
            let deletes_covered = state.sent_ds.as_ref() == Some(&my_ds);
            (!(blocks_covered && deletes_covered)).then(|| txn.encode_diff_v1(known))
        });
        if !state.version_due && update.is_none() {
            return None;
        }
        let frame = Frame {
            version: state.version_due.then(|| my_sv.encode_v1()),
            asks_version: state.known.is_none(),
            update,
        };
        let mut next = state;
        next.version_due = false;
        if frame.update.is_some() {
            // Assume that this message arrives, so the next call does not send
            // the same bytes again. Automerge's `sync::State` does the same.
            // If the message is lost, `sync_reset` recovers.
            let mut known = next.known.take().unwrap_or_default();
            known.merge(my_sv);
            next.known = Some(known);
            next.sent_ds = Some(my_ds);
        }
        Some((frame, next))
    }

    /// Encodes the full document in the v1 format: every block and the delete
    /// set. `state_fingerprint` uses this encoding.
    fn full_update_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }

    /// Applies `op` in `txn`.
    ///
    /// Convert and check every input before the first edit. `txn` commits when
    /// it drops, also on an error return, so an error after an edit would leave
    /// half an op in the document.
    fn apply_in(txn: &mut TransactionMut, op: &Op) -> anyhow::Result<()> {
        match op {
            Op::MapPut { obj, key, value } => {
                let value = Self::to_any(value)?;
                let map: MapRef = get_or_create(txn, map_name(obj))?;
                map.insert(txn, key.as_str(), In::Any(value));
            }
            Op::MapDelete { obj, key } => {
                let map: MapRef = get_or_create(txn, map_name(obj))?;
                map.remove(txn, key);
            }
            Op::ListInsert { obj, index, value } => {
                require_name(obj)?;
                let index = to_u32(*index)?;
                let value = Self::to_any(value)?;
                let arr: ArrayRef = get_or_create(txn, obj)?;
                arr.insert(txn, index, In::Any(value));
            }
            Op::ListDelete { obj, index } => {
                require_name(obj)?;
                let index = to_u32(*index)?;
                let arr: ArrayRef = get_or_create(txn, obj)?;
                arr.remove(txn, index);
            }
            Op::ListSplice {
                obj,
                pos,
                del_count,
                values,
            } => {
                require_name(obj)?;
                let del_count = to_u32(*del_count)?;
                let values = values
                    .iter()
                    .map(Self::to_any)
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let pos = to_u32(*pos)?;
                let arr: ArrayRef = get_or_create(txn, obj)?;
                if del_count > 0 {
                    arr.remove_range(txn, pos, del_count);
                }
                // One range insert, as Yjs does. One insert per value costs
                // more time for the same document.
                arr.insert_range(txn, pos, values);
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
                let text: TextRef = get_or_create(txn, obj)?;
                if del_count > 0 {
                    text.remove_range(txn, pos, del_count);
                }
                if !insert.is_empty() {
                    text.insert(txn, pos, insert);
                }
            }
        }
        Ok(())
    }
}

/// Converts one Yrs value to a [`ScalarVal`]. Fails on a value that the op
/// model does not have.
fn from_any(value: &Any) -> anyhow::Result<ScalarVal> {
    Ok(match value {
        Any::String(s) => ScalarVal::Str(s.to_string()),
        Any::Bool(b) => ScalarVal::Bool(*b),
        Any::Number(Number::Int(n)) => ScalarVal::Int(*n),
        Any::Buffer(b) => ScalarVal::Bytes(b.to_vec()),
        other => bail!("value {other:?} has no ScalarVal form"),
    })
}

impl CrdtAdapter for YrsAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        Self::apply_in(&mut txn, op)
        // `txn` commits when it drops. There is no separate commit call.
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        // Yrs has no hash DAG of changes. Its version is the state vector:
        // the highest clock seen from each client. Each head is one
        // (client_id: 8 bytes BE, clock: 4 bytes BE) pair. The pairs are
        // sorted here, because the order of `StateVector`'s own encoder is
        // not documented as canonical.
        //
        // CAUTION: the heads are not a convergence fingerprint. A delete does
        // not change the state vector (see `PeerSync`). Two documents can
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
        // update from the empty state vector.
        self.encode().len()
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let (frame, next) = self.next_message(peer)?;
        self.peers.insert(peer.to_owned(), next);
        Some(frame.encode())
    }

    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        let frame =
            Frame::decode(&msg).with_context(|| format!("decoding sync message from '{peer}'"))?;
        let mut state = self.peers.get(peer).cloned().unwrap_or_else(PeerSync::new);
        if let Some(version) = &frame.version {
            let sv = StateVector::decode_v1(version)
                .with_context(|| format!("decoding the state vector from '{peer}'"))?;
            let mut known = state.known.take().unwrap_or_default();
            known.merge(sv);
            state.known = Some(known);
            // The peer may not have this adapter's deletes. Send them again.
            state.sent_ds = None;
        }
        if frame.asks_version {
            state.version_due = true;
        }
        if let Some(bytes) = &frame.update {
            let update = Update::decode_v1(bytes)
                .with_context(|| format!("decoding sync message from '{peer}'"))?;
            // The peer has what it sent, so do not send it back.
            let sent = update.state_vector();
            let mut txn = self.doc.transact_mut();
            txn.apply_update(update)
                .with_context(|| format!("applying update from '{peer}'"))?;
            if let Some(known) = &mut state.known {
                known.merge(sent);
            }
        }
        self.peers.insert(peer.to_owned(), state);
        Ok(())
    }

    fn sync_pending(&mut self, peer: &str) -> bool {
        self.next_message(peer).is_some()
    }

    fn sync_reset(&mut self, peer: &str) {
        // Removing the entry is the full reset. The next `sync_generate` sends
        // the state vector again, and waits for the peer's.
        self.peers.remove(peer);
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

    /// Replaces the document with an empty one that has this client id. It keeps
    /// the low 52 bits of `id`. Bit 52 keeps the varint width of a random id,
    /// which has 53 bits.
    fn set_instance_id(&mut self, id: u128) -> anyhow::Result<()> {
        let low = (id as u64) & ((1 << INSTANCE_ID_BITS) - 1);
        self.doc = Doc::with_client_id(low | (1 << INSTANCE_ID_BITS));
        self.peers.clear();
        Ok(())
    }

    /// Does a local lookup. Authors no change. See `ensure_text`.
    fn ensure_list(&mut self, obj: &str) -> anyhow::Result<()> {
        require_name(obj)?;
        let mut txn = self.doc.transact_mut();
        get_or_create::<ArrayRef>(&mut txn, obj)?;
        Ok(())
    }

    fn read_text(&mut self, obj: &str) -> anyhow::Result<String> {
        let mut txn = self.doc.transact_mut();
        let text: TextRef = existing(&mut txn, obj, "read_text")?;
        Ok(text.get_string(&txn))
    }

    fn read_list(&mut self, obj: &str) -> anyhow::Result<Vec<ScalarVal>> {
        let mut txn = self.doc.transact_mut();
        let arr: ArrayRef = existing(&mut txn, obj, "read_list")?;
        match arr.to_json(&txn) {
            Any::Array(values) => values.iter().map(from_any).collect(),
            other => bail!("read_list: unexpected JSON form {other:?}"),
        }
    }

    fn read_map(&mut self, obj: &str) -> anyhow::Result<BTreeMap<String, ScalarVal>> {
        let mut txn = self.doc.transact_mut();
        let map: MapRef = existing(&mut txn, map_name(obj), "read_map")?;
        match map.to_json(&txn) {
            Any::Map(values) => values
                .iter()
                .map(|(k, v)| Ok((k.clone(), from_any(v)?)))
                .collect(),
            other => bail!("read_map: unexpected JSON form {other:?}"),
        }
    }

    fn apply_txn(&mut self, ops: &[Op]) -> anyhow::Result<Vec<u8>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let mut txn = self.doc.transact_mut();
        for op in ops {
            Self::apply_in(&mut txn, op)?;
        }
        // Commit first. The commit squashes the delete set, and the update
        // must hold the same bytes that an update observer would see.
        txn.commit();
        Ok(txn.encode_update_v2())
    }

    fn apply_updates(&mut self, updates: &[&[u8]]) -> anyhow::Result<()> {
        let mut txn = self.doc.transact_mut();
        for update in updates {
            let update = Update::decode_v2(update).context("decoding a Yrs update")?;
            txn.apply_update(update).context("applying a Yrs update")?;
        }
        Ok(())
    }

    /// Returns the v2 encoding. Yjs and ywasm benchmarks use this encoding too.
    /// The v1 encoding of the same B4 document is 2.4 times larger.
    fn encode(&mut self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v2(&StateVector::default())
    }

    fn load(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        *self = Self::new();
        self.apply_updates(&[bytes])
    }
}

#[cfg(test)]
mod tests {
    use super::YrsAdapter;
    use crate::adapter::conformance::*;
    use common::{CrdtAdapter, Op};

    conformance_tests!(
        YrsAdapter:
        // Universal conformance.
        reads_return_the_written_content,
        txn_updates_carry_inserts_and_deletes,
        txn_applies_ops_in_sequence,
        txn_update_excludes_received_ops,
        update_batch_equals_one_by_one,
        encode_and_load_round_trip,
        partitioned_list_inserts_merge_after_ensure_list,
        set_instance_id_gives_distinct_replicas,
        wide_ids_stay_distinct,
        fixed_ids_make_a_merge_repeatable,
        identical_ops_yield_equal_fingerprints,
        disjoint_edits_converge_after_sync,
        concurrent_edits_to_same_key_converge,
        post_sync_divergence_is_detected,
        root_map_ops_are_supported,
        each_op_variant_mutates_the_doc,
        reads_are_stable_without_writes,
        sync_is_idempotent_once_converged,
        sync_pending_matches_generate,
        a_heal_after_a_quiet_partition_sends_little,
        first_sync_of_a_shared_document_sends_little,
        three_replicas_converge_through_a_hub,
        doc_size_grows_with_every_op,
        reset_returns_adapter_to_a_fresh_state,
        reset_drops_stale_peer_state,
        reset_allows_clean_re_sync_with_equal_fingerprint,
        ensure_text_is_idempotent,
        partitioned_text_edits_interleave_after_bootstrap,
        ensure_text_adopts_synced_in_object,
        first_local_write_reuses_synced_in_object,
        sync_reset_forgets_quiescence,
        sync_reset_recovers_from_a_lost_message,
        sync_reset_unknown_peer_is_noop,
        text_length_errors_on_missing_or_wrong_type,
        // Characterization shared with Loro.
        ensure_text_is_a_pure_lookup_no_change_authored,
        partitioned_text_edits_interleave_even_without_ensure_text,
        uint_above_i64_max_is_rejected,
    );

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
