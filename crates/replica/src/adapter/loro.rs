use std::collections::HashMap;

use anyhow::{Context, bail};
use common::{CrdtAdapter, Op, ScalarVal};
use loro::{ExportMode, LoroDoc, LoroValue, VersionVector};

/// Root-map name standing in for the empty object name.
///
/// [`common::Op`]'s model says the empty `obj` string refers to ROOT itself —
/// an unnamed top-level map, which Automerge has natively and Loro does not.
/// Mapping it to one reserved root name reproduces ROOT's semantics exactly
/// where it matters: a single well-known top-level map that every replica
/// resolves identically, with no creation op and therefore no concurrent-
/// creation hazard (see fact 1 in this module's doc comment).
///
/// The alternative — having the orchestrator name the map explicitly — was
/// rejected because it would make Automerge create that map lazily on first
/// write, which is exactly the collision `ensure_text` exists to prevent, and
/// would change measured Automerge behaviour on every scenario already swept.
///
/// A workload that used this literal name for its own object would alias
/// ROOT. Nothing generates it today.
const ROOT_MAP_NAME: &str = "_root";

/// [`common::CrdtAdapter`] implementation backed by `loro::LoroDoc` (Loro,
/// whose list/text CRDT is Fugue-descended rather than Automerge's RGA or
/// Yjs/Yrs's YATA).
///
/// Two structural facts about Loro drive most of what follows, both verified
/// against `loro` / `loro-internal` 1.13.9 source rather than the docs:
///
/// 1. **Root containers are keyed by `(name, type)`, and always exist.**
///    `IntoContainerId for &str` builds `ContainerID::Root { name,
///    container_type }` from the name *and* the accessor that was used, and
///    `LoroDoc::has_container` returns `true` unconditionally for a
///    non-mergeable root id. So `get_text("x")` and `get_list("x")` name two
///    different containers, and neither can fail to exist. There is no
///    creation op, no wrong-type error, and — like [`super::yrs::YrsAdapter`]
///    but unlike [`super::automerge::AutomergeAdapter`] — no bootstrap
///    problem: see [`CrdtAdapter::ensure_text`] and
///    [`CrdtAdapter::text_length`] below for the two places this leaks into
///    the trait contract.
///
/// 2. **Loro ships no sync protocol.** `loro::sync` is a re-export of the
///    crate's own `Mutex`/`RwLock` wrappers, not a peer protocol. The
///    library's sync primitive is version-vector diffing:
///    `export(ExportMode::updates(&their_vv))`. Like Yrs, that leaves the
///    per-peer bookkeeping to the caller — see `peer_vv`.
pub struct LoroAdapter {
    doc: LoroDoc,
    /// This adapter's own belief of what each peer has already been sent, as
    /// a version vector. Loro has no native per-peer state to thread here
    /// (fact 2 above), so — exactly as in [`super::yrs::YrsAdapter`] — this
    /// cache is introduced by the benchmark harness, not by the library.
    /// Two of the three libraries needing the harness to supply this is an
    /// RQ-3 measurement-boundary point, not an implementation detail.
    ///
    /// Without it, `sync_generate` would have to export from the empty
    /// version vector every time: still *correct* (Loro imports are
    /// idempotent) but never reaching the `None` fixed point that the
    /// replica's `flush_to_peers` loop and
    /// `sync_is_idempotent_once_converged` both require.
    peer_vv: HashMap<String, VersionVector>,
}

impl Default for LoroAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl LoroAdapter {
    /// Create a new, empty Loro document with no peer state.
    ///
    /// `LoroDoc::new()` assigns a random `PeerID` (a `u64`) and calls
    /// `start_auto_commit()`, so ops accumulate in an implicit transaction
    /// until something commits it. Two `new()` calls model two distinct
    /// replicas, as the conformance suite's `A: CrdtAdapter + Default` bound
    /// requires.
    pub fn new() -> Self {
        Self {
            doc: LoroDoc::new(),
            peer_vv: HashMap::new(),
        }
    }

    /// Create an adapter whose document uses a caller-chosen `PeerID`
    /// instead of a random one.
    ///
    /// Not used by the replica binary — Loro's own docs warn against
    /// assigning fixed peer ids to concurrent writers, and the benchmark
    /// deliberately keeps all three adapters on the same footing of a random
    /// per-instance identifier. It exists because peer-id *width* is the
    /// only thing that makes two converged replicas' snapshots differ (see
    /// `loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids`),
    /// so pinning ids is what turns that into a claim worth asserting.
    pub fn with_peer_id(peer: u64) -> anyhow::Result<Self> {
        let me = Self::new();
        me.doc
            .set_peer_id(peer)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("setting peer id {peer}"))?;
        Ok(me)
    }

    /// Read the named text container's current contents. Test-only: the
    /// trait exposes length rather than content, because the orchestrator's
    /// validity check only needs the length.
    #[cfg(test)]
    fn read_text(&self, obj: &str) -> String {
        self.doc.get_text(obj).to_string()
    }

    /// Convert a [`ScalarVal`] to Loro's `LoroValue`.
    ///
    /// `Uint` narrows through `i64`: `LoroValue` has no unsigned integer
    /// variant (only `I64` and `Double`). Same real-but-not-expected-in-
    /// practice narrowing as [`super::yrs::YrsAdapter::to_any`] — benchmark
    /// scalars are small counters and indices.
    fn to_loro_value(v: &ScalarVal) -> LoroValue {
        match v {
            ScalarVal::Str(s) => LoroValue::String(s.as_str().into()),
            ScalarVal::Uint(n) => LoroValue::I64(*n as i64),
            ScalarVal::Int(n) => LoroValue::I64(*n),
            ScalarVal::Bool(b) => LoroValue::Bool(*b),
            ScalarVal::Bytes(b) => LoroValue::Binary(b.clone().into()),
        }
    }

    /// Resolve a map object name, translating the empty name to
    /// [`ROOT_MAP_NAME`]. See that constant for why.
    fn map_name(obj: &str) -> &str {
        if obj.is_empty() { ROOT_MAP_NAME } else { obj }
    }

    /// Reject an empty object name for the non-map container kinds.
    ///
    /// Automerge's ROOT is a map, so an empty name has no meaning for a list
    /// or text op there either; erroring is more useful than silently
    /// inventing a root sequence. Unlike [`super::yrs::YrsAdapter`], this is
    /// the *only* validation a Loro container accessor needs: a wrong-type
    /// collision is unrepresentable (fact 1 in this module's doc comment).
    fn check_name(obj: &str) -> anyhow::Result<()> {
        if obj.is_empty() {
            bail!("object name must not be empty: Loro has no implicit root container");
        }
        Ok(())
    }
}

impl CrdtAdapter for LoroAdapter {
    fn apply_op(&mut self, op: &Op) -> anyhow::Result<()> {
        match op {
            Op::MapPut { obj, key, value } => {
                self.doc
                    .get_map(Self::map_name(obj))
                    .insert(key.as_str(), Self::to_loro_value(value))?;
            }
            Op::MapDelete { obj, key } => {
                self.doc.get_map(Self::map_name(obj)).delete(key.as_str())?;
            }
            Op::ListInsert { obj, index, value } => {
                Self::check_name(obj)?;
                self.doc
                    .get_list(obj.as_str())
                    .insert(*index, Self::to_loro_value(value))?;
            }
            Op::ListDelete { obj, index } => {
                Self::check_name(obj)?;
                self.doc.get_list(obj.as_str()).delete(*index, 1)?;
            }
            Op::ListSplice {
                obj,
                pos,
                del_count,
                values,
            } => {
                Self::check_name(obj)?;
                let list = self.doc.get_list(obj.as_str());
                // Loro's list has no single splice primitive (unlike its
                // text handler), so delete-then-insert, matching the order
                // Automerge's `splice` documents.
                if *del_count > 0 {
                    list.delete(*pos, *del_count)?;
                }
                for (i, v) in values.iter().enumerate() {
                    list.insert(*pos + i, Self::to_loro_value(v))?;
                }
            }
            Op::TextSplice {
                obj,
                pos,
                del_count,
                insert,
            } => {
                Self::check_name(obj)?;
                // `LoroText::splice` is Unicode-scalar indexed (it passes
                // `PosType::Unicode` through to the handler), which is
                // exactly `Op::TextSplice`'s contract — inherited from
                // Automerge's char-indexed `splice_text`. Loro therefore
                // needs none of the byte-vs-char caveat that
                // `super::yrs::YrsAdapter` carries: non-ASCII insert
                // content would index correctly here today. `splice`
                // returns the deleted string, which we drop.
                self.doc
                    .get_text(obj.as_str())
                    .splice(*pos, *del_count, insert)?;
            }
        }
        // Close the implicit auto-commit transaction, so op_duration_ms
        // captures the full apply+commit cost and the following
        // sync_generate sees exactly this change — same reason
        // AutomergeAdapter calls `self.doc.commit()`. Without it Loro would
        // commit lazily at the next export/import instead, moving that cost
        // into whichever call happened to trigger it.
        self.doc.commit();
        Ok(())
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        // Loro's `Frontiers` is a true DAG frontier — the set of maximal op
        // ids in the causal graph — so this is the direct analogue of
        // Automerge's `get_heads`, not of Yrs's state vector. Crucially,
        // deletion in Loro *is* an op that advances the deleting peer's
        // counter (unlike Yrs, where it only flags an existing block), so
        // the frontier does move on a delete and can serve as a fingerprint.
        //
        // Encoded as sorted (peer: 8 bytes BE, counter: 4 bytes BE) pairs.
        // The sort is required, not cosmetic: `Frontiers::Map` is backed by
        // an `FxHashMap`, whose iteration order is unspecified — the same
        // trap `YrsAdapter::get_heads` documents for `StateVector`.
        let mut heads: Vec<Vec<u8>> = self
            .doc
            .oplog_frontiers()
            .iter()
            .map(|id| {
                let mut buf = Vec::with_capacity(12);
                buf.extend_from_slice(&id.peer.to_be_bytes());
                buf.extend_from_slice(&id.counter.to_be_bytes());
                buf
            })
            .collect();
        heads.sort_unstable();
        heads
    }

    fn state_fingerprint(&mut self) -> Vec<u8> {
        // Sorted concatenation of the frontier, exactly as
        // AutomergeAdapter does with its head hashes: equal iff the two
        // replicas have the same causal frontier, hence the same history,
        // hence the same state.
        //
        // Deliberately NOT the snapshot bytes — measured, not assumed: five
        // line-topology replicas that converge to an identical frontier
        // produce snapshots of 654/667/654/667/654 bytes. Snapshot
        // encoding is integration-order dependent here, the same way
        // Automerge's `save()` is (and unlike Yrs's canonical full update).
        // See `doc_size_bytes` and the characterization test
        // `loro_save_bytes_not_canonical_across_converged_replicas`.
        self.get_heads().into_iter().flatten().collect()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // `ExportMode::Snapshot` is Loro's "the whole document" encoding —
        // full history plus materialized state — and so the closest
        // analogue of Automerge's `save()`.
        //
        // NOTE: unlike Automerge's and Yrs's append-only encodings, this can
        // *shrink*. The snapshot carries a state section as well as a
        // history section, so deleting a map key can remove more state bytes
        // than the deletion op adds history bytes (measured: 240 -> 238 for
        // a MapPut followed by a MapDelete). That is why this adapter does
        // not claim the `doc_size_grows_with_every_op` characterization —
        // see `adapter::conformance` and the mirror test
        // `loro_doc_size_can_shrink_on_delete`.
        self.doc
            .export(ExportMode::Snapshot)
            .map(|b| b.len())
            // The trait's signature has no error channel here, and Loro's
            // only documented `LoroEncodeError` cases are shallow-snapshot
            // depth errors that a plain `Snapshot` export cannot hit. Report
            // 0 rather than panicking a benchmark run on an impossible arm.
            .unwrap_or(0)
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let my_vv = self.doc.oplog_vv();
        let known = self.peer_vv.get(peer).cloned().unwrap_or_default();
        // The `None` fixed point has to come from this comparison, not from
        // an empty export: a Loro update carrying no ops is still 22 bytes
        // of magic + checksum + header (measured), so `bytes.is_empty()`
        // would never be true and the push loop would spin forever.
        if known.includes_vv(&my_vv) {
            return None;
        }
        let bytes = self.doc.export(ExportMode::updates(&known)).ok()?;
        // Optimistic advance, mirroring Automerge's `sync::State` recording
        // the heads it has sent: assume this send lands, so the next call
        // does not re-offer the same ops. `sync_reset` is the recovery path
        // when it does not — see its doc comment.
        self.peer_vv.insert(peer.to_owned(), my_vv);
        Some(bytes)
    }

    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        // Loro tolerates causally-incomplete updates: ops whose
        // dependencies have not arrived are buffered and reported in
        // `ImportStatus::pending` rather than rejected, and applied later
        // once the gap is filled. That is deliberately *not* treated as an
        // error here — it is the normal shape of an out-of-order delivery —
        // and it is safe for the convergence oracle because pending ops are
        // not in the oplog and so do not move `get_heads`. A replica sitting
        // on a permanent gap therefore reads as un-converged, which is the
        // truth.
        self.doc
            .import(&msg)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("applying update from '{peer}'"))?;
        Ok(())
    }

    fn sync_reset(&mut self, peer: &str) {
        // Dropping the entry is the whole reset: the next sync_generate
        // compares against `VersionVector::default()`, which includes
        // nothing, so it re-exports everything this peer might have missed
        // rather than trusting a belief that a dropped message landed.
        self.peer_vv.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    /// A pure local lookup that authors no change, for the same reason as
    /// [`super::yrs::YrsAdapter::ensure_text`] but by a different mechanism:
    /// a Loro root container's identity is `(name, type)`, decided entirely
    /// by the accessor call, so two replicas independently reaching for
    /// `get_text("text")` are naming the same container by construction.
    /// There is no creation op to be concurrent with, hence no losing side.
    ///
    /// The only thing left to check is the name itself.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        Self::check_name(obj)?;
        Ok(())
    }

    /// # Loro deviation from the trait's "errors if no such text object"
    ///
    /// Loro cannot represent that condition: every `(name, Text)` root id
    /// exists by definition, so an untouched name reads as a legitimately
    /// empty text and this returns `Ok(0)`. Erroring would mean inventing an
    /// existence predicate the library does not have.
    ///
    /// The orchestrator's use of this method is unaffected:
    /// `verify_text_length` compares the returned length against the
    /// scenario's op count, so a mistyped object name still fails loudly as
    /// `0 != op_count` rather than passing silently. Only the *shape* of the
    /// failure changes, from an error to a mismatch. Locked in by
    /// `loro_text_length_of_an_untouched_name_is_zero_not_an_error`.
    fn text_length(&mut self, obj: &str) -> anyhow::Result<usize> {
        Self::check_name(obj)?;
        Ok(self.doc.get_text(obj).len_unicode())
    }
}

#[cfg(test)]
mod tests {
    use super::LoroAdapter;
    use crate::adapter::conformance::*;
    use common::{CrdtAdapter, Op};

    // ── Universal conformance (see adapter::conformance) ───────────────────

    #[test]
    fn loro_identical_ops_yield_equal_fingerprints() {
        identical_ops_yield_equal_fingerprints::<LoroAdapter>();
    }

    #[test]
    fn loro_disjoint_edits_converge_after_sync() {
        disjoint_edits_converge_after_sync::<LoroAdapter>();
    }

    #[test]
    fn loro_concurrent_edits_to_same_key_converge() {
        concurrent_edits_to_same_key_converge::<LoroAdapter>();
    }

    #[test]
    fn loro_post_sync_divergence_is_detected() {
        post_sync_divergence_is_detected::<LoroAdapter>();
    }

    #[test]
    fn loro_root_map_ops_are_supported() {
        root_map_ops_are_supported::<LoroAdapter>();
    }

    #[test]
    fn loro_each_op_variant_mutates_the_doc() {
        each_op_variant_mutates_the_doc::<LoroAdapter>();
    }

    #[test]
    fn loro_reads_are_stable_without_writes() {
        reads_are_stable_without_writes::<LoroAdapter>();
    }

    #[test]
    fn loro_sync_is_idempotent_once_converged() {
        sync_is_idempotent_once_converged::<LoroAdapter>();
    }

    #[test]
    fn loro_three_replicas_converge_through_a_hub() {
        three_replicas_converge_through_a_hub::<LoroAdapter>();
    }

    #[test]
    fn loro_reset_returns_adapter_to_a_fresh_state() {
        reset_returns_adapter_to_a_fresh_state::<LoroAdapter>();
    }

    #[test]
    fn loro_reset_drops_stale_peer_state() {
        reset_drops_stale_peer_state::<LoroAdapter>();
    }

    #[test]
    fn loro_reset_allows_clean_re_sync_with_equal_fingerprint() {
        reset_allows_clean_re_sync_with_equal_fingerprint::<LoroAdapter>();
    }

    #[test]
    fn loro_ensure_text_is_idempotent() {
        ensure_text_is_idempotent::<LoroAdapter>();
    }

    #[test]
    fn loro_partitioned_text_edits_interleave_after_bootstrap() {
        partitioned_text_edits_interleave_after_bootstrap::<LoroAdapter>();
    }

    #[test]
    fn loro_ensure_text_adopts_synced_in_object() {
        ensure_text_adopts_synced_in_object::<LoroAdapter>();
    }

    #[test]
    fn loro_first_local_write_reuses_synced_in_object() {
        first_local_write_reuses_synced_in_object::<LoroAdapter>();
    }

    #[test]
    fn loro_sync_reset_forgets_quiescence() {
        sync_reset_forgets_quiescence::<LoroAdapter>();
    }

    #[test]
    fn loro_sync_reset_recovers_from_a_lost_message() {
        sync_reset_recovers_from_a_lost_message::<LoroAdapter>();
    }

    #[test]
    fn loro_sync_reset_unknown_peer_is_noop() {
        sync_reset_unknown_peer_is_noop::<LoroAdapter>();
    }

    // Deliberately NOT wrapped here. Each has a mirror test below asserting
    // what Loro does instead, rather than a bare skip:
    //
    // - ensure_text_is_deterministic_across_replicas and
    //   without_bootstrap_partitioned_text_loses_a_side: both require root
    //   object creation to be a mergeable op with a losing side. Loro root
    //   ids are `(name, type)` — nothing to collide.
    // - ensure_text_rejects_non_empty_doc_without_the_object: no such
    //   precondition exists to reject.
    // - doc_size_grows_with_every_op: Loro's snapshot has a state section
    //   that can shrink.
    // - text_length_errors_on_missing_or_wrong_type: neither failure mode is
    //   representable.
    // - save_bytes_not_canonical_across_converged_replicas: Loro is neither
    //   Automerge's "reliably differs" nor Yrs's "always identical" — see
    //   the mirror test for the measured third answer.
    // - reset_clears_doc_and_sync_state and
    //   reset_allows_clean_re_sync_to_another_replica: Automerge-only for
    //   the reasons documented on those functions; the universal halves are
    //   wrapped above.

    // ── Loro-specific characterization ─────────────────────────────────
    //
    // Executable claims about what this library does instead, so a future
    // Loro release that changes any of them fails loudly rather than
    // silently invalidating a comment.

    /// Counterpart to `ensure_text_is_deterministic_across_replicas`
    /// (Automerge-only). Like Yrs, Loro authors no change here — but for a
    /// different reason worth keeping distinct. Yrs materializes root types
    /// in a local registry; Loro does not materialize anything at all,
    /// because a root `ContainerID` is *derived* from the name and the
    /// accessor's type. Two replicas reaching for `get_text("text")` are
    /// naming one container by construction, so an empty frontier here is
    /// the point, not a degenerate case.
    #[test]
    fn loro_ensure_text_is_a_pure_lookup_no_change_authored() {
        let mut a = LoroAdapter::default();
        let mut b = LoroAdapter::default();
        a.ensure_text("text").unwrap();
        b.ensure_text("text").unwrap();

        assert!(
            a.get_heads().is_empty(),
            "ensure_text must not author a change for Loro"
        );
        assert_eq!(a.get_heads(), b.get_heads());
        assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    }

    /// Counterpart to `ensure_text_rejects_non_empty_doc_without_the
    /// _object` (Automerge-only): the "must be the doc's first change"
    /// precondition has no Loro analogue, so bootstrapping a text object
    /// into a document that already has unrelated history simply succeeds.
    /// The empty name is the only rejection this adapter has.
    #[test]
    fn loro_ensure_text_succeeds_on_a_non_empty_doc() {
        let mut a = LoroAdapter::default();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        a.ensure_text("text").unwrap();

        let err = a.ensure_text("").unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    /// Counterpart to `without_bootstrap_partitioned_text_loses_a_side`
    /// (Automerge-only): the lazy-creation collision that test guards
    /// against cannot occur here, so skipping `ensure_text` entirely and
    /// letting the first `TextSplice` reach the container still interleaves
    /// both partitioned sides.
    #[test]
    fn loro_partitioned_text_edits_interleave_even_without_ensure_text() {
        let mut a = LoroAdapter::default();
        let mut b = LoroAdapter::default();
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

    /// Counterpart to `doc_size_grows_with_every_op`: Loro's snapshot
    /// carries a materialized state section as well as history, so deleting
    /// a map key can free more state bytes than the deletion op costs in
    /// history bytes. Asserted as a strict shrink rather than "not
    /// necessarily growing", because a merely-flat size would be the
    /// signature of a `MapDelete` arm that silently did nothing — the exact
    /// bug the original combined test was written to catch.
    #[test]
    fn loro_doc_size_can_shrink_on_delete() {
        let mut a = LoroAdapter::default();
        a.apply_op(&map_put("m", "k", "some longer value")).unwrap();
        let after_put = a.doc_size_bytes();
        let fp_after_put = a.state_fingerprint();

        a.apply_op(&Op::MapDelete {
            obj: "m".into(),
            key: "k".into(),
        })
        .unwrap();

        assert!(
            a.doc_size_bytes() < after_put,
            "expected the snapshot to shrink on delete ({after_put} -> {})",
            a.doc_size_bytes()
        );
        assert_ne!(
            a.state_fingerprint(),
            fp_after_put,
            "the delete must still move the frontier — that, not size, is \
             what proves the op landed"
        );
    }

    /// Counterpart to `text_length_errors_on_missing_or_wrong_type`
    /// (Automerge/Yrs): *neither* of that test's two failure modes is
    /// representable in Loro.
    ///
    /// A root `ContainerID` is `(name, type)` and
    /// `LoroDoc::has_container` returns true unconditionally for one, so an
    /// untouched name is an empty text rather than a missing object, and a
    /// name used as a list is a different container entirely rather than a
    /// type conflict. See `LoroAdapter::text_length` for why this is safe
    /// for the orchestrator's post-convergence validity check.
    #[test]
    fn loro_text_length_of_an_untouched_name_is_zero_not_an_error() {
        let mut a = LoroAdapter::default();
        assert_eq!(a.text_length("never-touched").unwrap(), 0);

        // Same name, used as a list: a distinct container, not a conflict.
        a.apply_op(&Op::ListInsert {
            obj: "l".into(),
            index: 0,
            value: 1u64.into(),
        })
        .unwrap();
        assert_eq!(
            a.text_length("l").unwrap(),
            0,
            "the (name, type) key means text 'l' and list 'l' coexist"
        );

        // The empty name is the one thing that does fail.
        assert!(a.text_length("").is_err());
    }

    /// Counterpart to `save_bytes_not_canonical_across_converged_replicas`
    /// (Automerge-only) and to
    /// `yrs_save_bytes_are_canonical_across_converged_replicas`. Loro is
    /// neither: it is *structurally* canonical, and the only thing that
    /// makes two converged replicas' snapshots differ is the encoded width
    /// of their random `PeerID`s.
    ///
    /// Measured before writing this test, over the same 5-replica line
    /// topology the Automerge version uses: with random peer ids the five
    /// snapshots differ in 16/100 trials at 10 ops and 12/100 at 40 ops —
    /// a rate that does *not* grow with history, which is what rules out
    /// Automerge's integration-order explanation. Pinning the peer ids to
    /// 1..=5 drops it to 0/100 at both sizes.
    ///
    /// So the assertion is made with pinned ids, which is the deterministic
    /// form of the claim. Asserting the random-id case either way would be
    /// a coin flip — and a flaky test is worse than an unmade claim.
    ///
    /// Consequence for the thesis's per-scenario doc-size table: a Loro
    /// spread across replicas is peer-id noise, not the graph-position
    /// effect Automerge's spread measures. The two must not be read the
    /// same way.
    #[test]
    fn loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids() {
        let n = 5;
        let op_count = 10;
        let mut replicas: Vec<LoroAdapter> = (1..=n as u64)
            .map(LoroAdapter::with_peer_id)
            .collect::<Result<_, _>>()
            .unwrap();
        let id = |i: usize| format!("node-{i}");

        for i in 0..op_count {
            let writer = i % n;
            replicas[writer]
                .apply_op(&map_put("doc", &format!("k{i}"), i as u64))
                .unwrap();
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

        let fp = replicas[0].state_fingerprint();
        for replica in replicas.iter_mut().skip(1) {
            assert_eq!(replica.state_fingerprint(), fp);
        }

        let sizes: Vec<usize> = (0..n).map(|i| replicas[i].doc_size_bytes()).collect();
        assert!(
            sizes.iter().min() == sizes.iter().max(),
            "snapshot stopped being canonical for equal-width peer ids — \
             sizes: {sizes:?}. If this starts failing, Loro's encoder gained \
             an integration-order dependence and the doc-size table's \
             interpretation above needs revisiting.",
        );
    }

    /// Loro-only, with no counterpart in the generic suite because no other
    /// adapter can currently claim it: `Op::TextSplice`'s positions are
    /// character (Unicode scalar) offsets — inherited from Automerge's
    /// `splice_text` — and `LoroText::splice` is natively Unicode-indexed,
    /// so multi-byte content needs no translation layer.
    ///
    /// This is the gap `super::super::yrs::YrsAdapter` carries as an open
    /// caveat: Yrs offers only UTF-8 and UTF-16 `OffsetKind`s, so its
    /// `TextSplice` is byte-indexed and correct only for ASCII. Written
    /// here to establish that the *trait's* contract is satisfiable, so
    /// that gap reads as a Yrs-adapter limitation and not an under-specified
    /// `Op` model.
    #[test]
    fn loro_text_splice_is_unicode_indexed_not_byte_indexed() {
        let mut a = LoroAdapter::default();
        a.ensure_text("text").unwrap();
        // 5 characters, 7 UTF-8 bytes: é and ü are two bytes each.
        a.apply_op(&Op::TextSplice {
            obj: "text".into(),
            pos: 0,
            del_count: 0,
            insert: "héllü".into(),
        })
        .unwrap();
        assert_eq!(a.text_length("text").unwrap(), 5, "length is in chars");

        // Delete the single character at char index 1 (é). A byte-indexed
        // implementation would cut the string mid-codepoint or address 'l'.
        a.apply_op(&Op::TextSplice {
            obj: "text".into(),
            pos: 1,
            del_count: 1,
            insert: "e".into(),
        })
        .unwrap();
        assert_eq!(a.text_length("text").unwrap(), 5);
        assert_eq!(a.read_text("text"), "hellü");
    }
}
