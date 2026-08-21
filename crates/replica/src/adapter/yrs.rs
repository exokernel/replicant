use std::collections::HashMap;

use anyhow::{Context, bail};
use common::{CrdtAdapter, Op, ScalarVal};
use yrs::{
    Any, Array, ArrayRef, Doc, GetString, In, Map, MapRef, ReadTxn, Root, SharedRef, StateVector,
    Text, TextRef, Transact, TransactionMut, Update, branch::Branch, types::TypeRef,
    updates::decoder::Decode,
};

/// `TextRef`/`MapRef`/`ArrayRef` each implement `AsRef<Branch>` (via the
/// `SharedRef` trait) among other `AsRef` impls, so a bare `.as_ref()` call
/// is ambiguous to type inference; this pins the target explicitly.
fn type_ref_of<S: SharedRef>(s: &S) -> &TypeRef {
    AsRef::<Branch>::as_ref(s).type_ref()
}

/// Root-map name standing in for the empty object name.
///
/// [`common::Op`]'s model says the empty `obj` string refers to ROOT itself —
/// an unnamed top-level map, which Automerge has natively and Yrs does not.
/// Mapping it to one reserved root name reproduces ROOT's semantics exactly
/// where it matters: a single well-known top-level map that every replica
/// resolves identically, with no creation op and therefore no concurrent-
/// creation hazard (see this module's doc comment).
///
/// The alternative — having the orchestrator name the map explicitly — was
/// rejected because it would make Automerge create that map lazily on first
/// write, which is exactly the collision `ensure_text` exists to prevent, and
/// would change measured Automerge behaviour on every scenario already swept.
///
/// A workload that used this literal name for its own object would alias
/// ROOT. Nothing generates it today.
const ROOT_MAP_NAME: &str = "_root";

/// [`common::CrdtAdapter`] implementation backed by `yrs::Doc` (the Rust port
/// of Yjs, using the YATA algorithm rather than Automerge's RGA-descended
/// list CRDT).
///
/// Unlike [`super::automerge::AutomergeAdapter`], this adapter needs no
/// object-id cache and no bootstrap-actor trick. Yrs root-level shared types
/// (`Text`/`Map`/`Array` attached directly to the `Doc`) are addressed
/// purely by string name, materialized in a local, non-CRDT-tracked `types`
/// registry (verified against `yrs` 0.27.3 source: `Store::get_or_create_type`
/// only touches a `HashMap`, never a block/clock). Two replicas independently
/// calling `get_or_insert_text("text")` cannot race the way two Automerge
/// `put_object(ROOT, "text", ObjType::Text)` calls can — there is no op, no
/// id, and therefore no losing side to merge away. See `ensure_text` below
/// for what this means for the shared-object bootstrap problem.
pub struct YrsAdapter {
    doc: Doc,
    /// This adapter's own belief of what each peer has already been sent.
    /// Yrs's own reference sync protocol (`y-sync`) is a stateless
    /// request/response handshake with no persistent per-peer cache of this
    /// kind — see `common::CrdtAdapter::sync_generate`'s doc comment.
    /// Without this map, every `sync_generate` call would have to re-send
    /// the full document, which is correct (Yrs updates are idempotent to
    /// re-application) but would never reach the `None` fixed point the push
    /// loop and the `sync_is_idempotent_once_converged` conformance test
    /// require.
    peer_sent: HashMap<String, PeerSent>,
}

/// What [`YrsAdapter`] believes one peer has already received.
///
/// Two fields, not one, and the second is the whole point: **a state vector
/// alone cannot detect a deletion.** Yrs deletion flags an existing block and
/// records the range in a separate delete set; it authors no block and
/// advances no client's clock. A state-vector-only "nothing new" check is
/// therefore blind to a pure delete, and after two replicas quiesce, any
/// subsequent delete would never be offered to the peer — it would sit
/// locally forever while both sides believed they were caught up.
///
/// This is the same blind spot that made a state-vector-derived
/// `state_fingerprint` wrong (see `get_heads`), reappearing in a different
/// method. Both were found by the conformance suite rather than by review,
/// and it is worth assuming there is a third: **any** Yrs bookkeeping keyed
/// on "how much has each client authored" is delete-blind by construction.
struct PeerSent {
    sv: StateVector,
    /// The delete set as of the last send, encoded. Compared by value rather
    /// than hashed so a collision cannot silently drop a deletion.
    ds: Vec<u8>,
}

impl Default for YrsAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl YrsAdapter {
    /// Create a new, empty Yrs document with no peer state.
    ///
    /// `Doc::new()` generates a random `ClientID` (verified against source:
    /// `Options::default()` calls `ClientID::random()`), so two `default()`
    /// calls model two distinct replicas the same way `AutoCommit::new()`
    /// does for Automerge — required by the `A: CrdtAdapter + Default` bound
    /// the conformance suite relies on.
    pub fn new() -> Self {
        Self {
            doc: Doc::new(),
            peer_sent: HashMap::new(),
        }
    }

    /// Convert a [`ScalarVal`] to the Yrs `Any` scalar type.
    ///
    /// `Uint` narrows through `i64`: Yrs's `Any` has no unsigned integer
    /// variant. Benchmark scalar values are small counters/indices, so this
    /// is not expected to lose precision in practice, but it is a real
    /// narrowing, unlike the other variants.
    fn to_any(v: &ScalarVal) -> Any {
        match v {
            ScalarVal::Str(s) => Any::String(s.as_str().into()),
            ScalarVal::Uint(n) => Any::BigInt(*n as i64),
            ScalarVal::Int(n) => Any::BigInt(*n),
            ScalarVal::Bool(b) => Any::Bool(*b),
            ScalarVal::Bytes(b) => Any::Buffer(b.as_slice().into()),
        }
    }

    /// Full document encoding: every block plus the delete set, relative to
    /// the empty state vector. The one piece of shared logic behind both
    /// `state_fingerprint` and `doc_size_bytes` — see `state_fingerprint`
    /// for why a state vector alone is not enough here.
    fn full_update_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    }

    /// The document's delete set alone, encoded.
    ///
    /// Diffing against the *current* state vector means no blocks are
    /// written — a Yrs update always carries the full delete set regardless
    /// of the target vector, so what comes back is the delete set plus a few
    /// bytes of framing. This is the cheapest handle the public API offers
    /// on "has anything been deleted since last time"; Yrs exposes no delete
    /// set version or counter.
    fn delete_set_bytes(&self) -> Vec<u8> {
        let txn = self.doc.transact();
        let sv = txn.state_vector();
        txn.encode_diff_v1(&sv)
    }

    /// Materialize (or look up) a root-level `Map`, failing if `obj` already
    /// names a different type. An empty `obj` resolves to [`ROOT_MAP_NAME`];
    /// see that constant for why. Unlike the list and text accessors below,
    /// this one has a faithful stand-in for Automerge's ROOT available,
    /// because ROOT is itself a map.
    fn get_or_create_map(txn: &mut TransactionMut, obj: &str) -> anyhow::Result<MapRef> {
        let obj = if obj.is_empty() { ROOT_MAP_NAME } else { obj };
        let map = Root::<MapRef>::new(obj).get_or_create(txn);
        let actual = type_ref_of(&map);
        if actual != &TypeRef::Map {
            bail!("object '{obj}' already exists with type {actual:?}, not Map");
        }
        Ok(map)
    }

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

    /// Materialize (or look up) a root-level `Text`, failing if `obj` already
    /// names a different type. This is also the entire implementation of
    /// [`CrdtAdapter::ensure_text`] — see that method for why no bootstrap
    /// trick is needed here.
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
                // NOTE: Yrs indexes Text by UTF-8 byte offset by default (the
                // only alternative is UTF-16 code units; there is no
                // char/Unicode-scalar `OffsetKind`). `Op::TextSplice` matches
                // Automerge's `splice_text`, which is char-indexed. The two
                // are numerically identical for ASCII content (all current
                // benchmark text workloads), but this adapter has NOT been
                // validated against multi-byte UTF-8 insert content — a
                // future non-ASCII text workload would need an explicit
                // byte<->char index translation here, not just a hopeful
                // pos/del_count pass-through.
                let text = Self::get_or_create_text(&mut txn, obj)?;
                if *del_count > 0 {
                    text.remove_range(&mut txn, *pos as u32, *del_count as u32);
                }
                if !insert.is_empty() {
                    text.insert(&mut txn, *pos as u32, insert);
                }
            }
        }
        // txn commits on drop (`impl Drop for TransactionMut` calls
        // `self.commit()` — verified against source); no explicit commit
        // call needed or available to time separately the way
        // AutomergeAdapter's `self.doc.commit()` is.
        Ok(())
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        // Yrs has no hash-linked DAG frontier the way Automerge does; a
        // state vector (highest known clock per client) is the native
        // notion of "current version" here — "how many items has each
        // client authored," full stop. Encoded as sorted (client_id: 8
        // bytes BE, clock: 4 bytes BE) pairs rather than trusting
        // `StateVector`'s own encoder's iteration order, which is not
        // documented as canonical and must not be assumed so for a
        // byte-equality comparison.
        //
        // NOTE: this is NOT a convergence fingerprint — see
        // `state_fingerprint`, which deliberately does not derive from this.
        // A state vector only counts authored items; it says nothing about
        // which of them are subsequently deleted, because Yrs deletion
        // flags an existing block rather than authoring a new one (verified
        // against source: deletions live in a separate `IdSet` "delete set",
        // never touching any client's clock). Two documents can therefore
        // share a state vector while disagreeing about live content.
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
        // Deliberately NOT `get_heads().flatten()` (unlike AutomergeAdapter):
        // a first attempt did exactly that and the generic
        // `each_op_variant_mutates_the_doc` conformance test caught it
        // immediately — the fingerprint didn't change after a MapDelete,
        // because deletion doesn't advance any client's clock (see
        // `get_heads`'s note). The full update encoding includes the delete
        // set, so it — not the state vector — is the thing that actually
        // captures live document content.
        self.full_update_bytes()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // Yrs has no separate "snapshot save" format the way Automerge's
        // save() is distinct from a sync update; the full document encoding
        // *is* an update relative to the empty state vector. Same bytes as
        // `state_fingerprint` — kept as two methods only because the trait
        // separates the two concerns, not because they need different data.
        self.full_update_bytes().len()
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let my_ds = self.delete_set_bytes();
        let txn = self.doc.transact();
        let my_sv = txn.state_vector();
        let sent = self.peer_sent.get(peer);
        let known = sent.map(|s| s.sv.clone()).unwrap_or_default();
        // Nothing to send iff BOTH: every client clock I have is already
        // covered by what I believe this peer has, AND my delete set is
        // unchanged since the last send. See `PeerSent` for why the second
        // condition is not redundant — without it a post-quiescence delete
        // is never offered and the two replicas silently stay diverged.
        let blocks_covered = my_sv
            .iter()
            .all(|(client, clock)| known.get(client) >= *clock);
        let deletes_covered = sent.is_some_and(|s| s.ds == my_ds);
        if blocks_covered && deletes_covered {
            return None;
        }
        let bytes = txn.encode_diff_v1(&known);
        drop(txn);
        // Optimistic advance, mirroring what Automerge's own `sync::State`
        // does with sent heads: assume this send reaches the peer, so the
        // next call doesn't re-offer the same bytes. `sync_reset` is what
        // lets a lost message be recovered from — see its doc comment.
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
        // Dropping the entry is the whole reset: the next sync_generate
        // compares against `StateVector::default()` and against a `None`
        // last-sent delete set, both of which fail the "already covered"
        // test for any non-empty document, so it resends everything this
        // peer might have missed rather than trusting a possibly-stale
        // belief that they already have it.
        self.peer_sent.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    /// A pure local lookup, not an authored change — see this module's doc
    /// comment for why Yrs root types cannot collide the way Automerge
    /// objects can. There is no bootstrap-actor trick, no "must be the
    /// first change" precondition, and no losing side to guard against:
    /// `get_or_create_text` already *is* the shared-object bootstrap, and it
    /// is also exactly what every `apply_op` `TextSplice` call does before
    /// writing. This is a genuine difference from Automerge worth its own
    /// characterization tests (see this module's test suite) rather than a
    /// simplification of the same problem.
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

    // Deliberately NOT wrapped here (Automerge-only characterization, see
    // adapter::conformance for why): ensure_text_is_deterministic_across_replicas,
    // without_bootstrap_partitioned_text_loses_a_side,
    // ensure_text_rejects_non_empty_doc_without_the_object,
    // save_bytes_not_canonical_across_converged_replicas (see
    // yrs_save_bytes_are_canonical_across_converged_replicas below — tried
    // against YrsAdapter and it asserts the opposite outcome).
    //
    // reset_clears_doc_and_sync_state and
    // reset_allows_clean_re_sync_to_another_replica were tried and both
    // failed — not from an adapter bug, but because each bakes in an
    // Automerge-specific assumption this design deliberately doesn't share.
    // reset_clears_doc_and_sync_state fails on two separate Automerge-shaped
    // assumptions, not one: "a fresh adapter always has a handshake to send"
    // AND "a fresh adapter's fingerprint is an empty byte vector" (true for
    // Automerge's flattened empty head list, false for Yrs's fixed-size
    // empty-update marker — this one first surfaced while writing the
    // replacement test, not the original).
    //
    // Both replacements originally lived here as Yrs-local tests. When
    // LoroAdapter needed the character-for-character same two replacements,
    // that stopped being a Yrs characterization and became the universal
    // form of the property, so they moved into the generic suite as
    // reset_returns_adapter_to_a_fresh_state / reset_drops_stale_peer_state
    // / reset_allows_clean_re_sync_with_equal_fingerprint (wrapped above).
    // The Automerge-only assertions they dropped still run, under the
    // original two names.

    // ── Yrs-specific characterization ───────────────────────────────────
    //
    // These are the mirror image of the two Automerge-only tests skipped
    // above: they lock in *why* those tests don't generalize, as executable
    // claims about this library rather than prose in a comment.

    /// Counterpart to `ensure_text_is_deterministic_across_replicas`
    /// (Automerge-only): independent bootstraps author NO change at all for
    /// Yrs, because root-type materialization is a local lookup, not an
    /// operation. Heads staying empty is not a degenerate case to work
    /// around — it is the whole point.
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

    /// Counterpart to `without_bootstrap_partitioned_text_loses_a_side`
    /// (Automerge-only): the hazard that test guards against — lazy
    /// concurrent creation of the same root object colliding, with the
    /// merge keeping only one side — cannot happen for Yrs, because root
    /// objects have no creation-time identity to collide on. Skipping
    /// `ensure_text` entirely and letting the first `TextSplice` create the
    /// object lazily (exactly what `without_bootstrap_partitioned_text_loses_a_side`
    /// shows losing a side under Automerge) still interleaves both sides
    /// correctly here.
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

    /// Counterpart to `save_bytes_not_canonical_across_converged_replicas`
    /// (Automerge-only): tried against `YrsAdapter` directly (not just
    /// asserted from source-reading) and it asserts the *opposite* outcome.
    /// Automerge's `save()` preserves change-list storage order, which
    /// depends on integration order and therefore differs by graph position
    /// in non-mesh topologies. Yrs's full-update encoding sorts blocks by
    /// client id before writing (verified against source:
    /// `Store::write_blocks_from`, `diff.sort_by(|a, b| b.0.cmp(&a.0))`), so
    /// converged replicas produce byte-identical encodings regardless of
    /// which graph position received which write. This is a genuine
    /// library-level difference, not an untested gap — see
    /// `save_bytes_not_canonical_across_converged_replicas`'s doc comment,
    /// which now reflects this rather than leaving it open.
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
