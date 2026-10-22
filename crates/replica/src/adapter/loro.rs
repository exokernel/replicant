use std::collections::HashMap;

use anyhow::{Context, bail};
use common::{CrdtAdapter, Op, ScalarVal};
use loro::{ExportMode, LoroDoc, LoroValue, VersionVector};

/// Name of the root map that stands in for ROOT (the empty object name).
///
/// In the [`common::Op`] model, an empty `obj` names ROOT: one unnamed
/// top-level map. Automerge has this map. Loro does not, so the adapter maps
/// the empty name to this name. Every replica resolves the name the same way,
/// and no op creates the map, so two replicas cannot create it concurrently.
///
/// Do not use this name for a workload object. It would alias ROOT.
const ROOT_MAP_NAME: &str = "_root";

/// [`common::CrdtAdapter`] implementation backed by `loro::LoroDoc`.
///
/// Loro's list and text CRDT descends from Fugue. Automerge uses RGA, and Yrs
/// uses YATA.
///
/// Two facts about Loro shape this adapter:
///
/// 1. **A root container is keyed by `(name, type)` and always exists.** The
///    accessor sets the type, so `get_text("x")` and `get_list("x")` are two
///    different containers. `LoroDoc::has_container` returns `true` for every
///    root id. There is no creation op and no wrong-type error. So, like
///    [`super::yrs::YrsAdapter`] and unlike
///    [`super::automerge::AutomergeAdapter`], Loro has no text bootstrap
///    problem. This changes the trait contract in two places:
///    [`CrdtAdapter::ensure_text`] and [`CrdtAdapter::text_length`].
///
/// 2. **Loro has no sync protocol.** (`loro::sync` holds lock wrappers, not a
///    peer protocol.) The sync primitive is a version-vector diff:
///    `export(ExportMode::updates(&their_vv))`. As with Yrs, the caller must
///    track what each peer has. See `peer_vv`.
pub struct LoroAdapter {
    doc: LoroDoc,
    /// The version vector this adapter believes each peer has, based on what
    /// it has sent. Loro keeps no per-peer state (fact 2), so the adapter must.
    /// [`super::yrs::YrsAdapter`] does the same.
    ///
    /// Without this map, `sync_generate` would export from the empty version
    /// vector every time. The result would still be correct, because Loro
    /// imports are idempotent. But it would never return `None`, and both the
    /// replica's `flush_to_peers` loop and `sync_is_idempotent_once_converged`
    /// need that `None`.
    peer_vv: HashMap<String, VersionVector>,
}

impl Default for LoroAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl LoroAdapter {
    /// Creates an empty Loro document with no peer state.
    ///
    /// `LoroDoc::new()` assigns a random `PeerID` (a `u64`) and turns on
    /// auto-commit: ops collect in an implicit transaction until something
    /// commits it. Each call gives a distinct replica, as the conformance
    /// suite's `A: CrdtAdapter + Default` bound requires.
    pub fn new() -> Self {
        Self {
            doc: LoroDoc::new(),
            peer_vv: HashMap::new(),
        }
    }

    /// Creates an adapter whose document uses the given `PeerID` instead of a
    /// random one.
    ///
    /// The replica binary does not use this. It keeps a random id per
    /// instance, like the other two adapters, and Loro's docs warn against
    /// fixed ids for concurrent writers. Tests need pinned ids because peer-id
    /// width is the only thing that makes the snapshots of two converged
    /// replicas differ. See
    /// `loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids`.
    pub fn with_peer_id(peer: u64) -> anyhow::Result<Self> {
        let me = Self::new();
        me.doc
            .set_peer_id(peer)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("setting peer id {peer}"))?;
        Ok(me)
    }

    /// Returns the contents of the named text container. Test-only. The trait
    /// exposes only the length, because the orchestrator's validity check
    /// needs only the length.
    #[cfg(test)]
    fn read_text(&self, obj: &str) -> String {
        self.doc.get_text(obj).to_string()
    }

    /// Converts a [`ScalarVal`] to a `LoroValue`.
    ///
    /// `Uint` is cast to `i64`, because `LoroValue` has no unsigned integer
    /// variant. A value above `i64::MAX` wraps. Benchmark scalars are small
    /// counters and indices, so this does not occur in practice.
    /// [`super::yrs::YrsAdapter::to_any`] does the same cast.
    fn to_loro_value(v: &ScalarVal) -> LoroValue {
        match v {
            ScalarVal::Str(s) => LoroValue::String(s.as_str().into()),
            ScalarVal::Uint(n) => LoroValue::I64(*n as i64),
            ScalarVal::Int(n) => LoroValue::I64(*n),
            ScalarVal::Bool(b) => LoroValue::Bool(*b),
            ScalarVal::Bytes(b) => LoroValue::Binary(b.clone().into()),
        }
    }

    /// Returns the Loro map name for `obj`. The empty name becomes
    /// [`ROOT_MAP_NAME`].
    fn map_name(obj: &str) -> &str {
        if obj.is_empty() { ROOT_MAP_NAME } else { obj }
    }

    /// Rejects an empty object name for list and text ops.
    ///
    /// Automerge's ROOT is a map, so an empty name has no meaning for a list
    /// or text op there either. An error is better than a silently invented
    /// root sequence. This is the only check a Loro accessor needs, because a
    /// wrong-type collision cannot occur (fact 1 on [`LoroAdapter`]).
    /// [`super::yrs::YrsAdapter`] must also check the type.
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
                // Loro's list has no splice primitive (its text does). Delete,
                // then insert, in the order Automerge's `splice` documents.
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
                // `LoroText::splice` uses Unicode-scalar offsets. That matches
                // the `Op::TextSplice` contract, which comes from Automerge's
                // char-indexed `splice_text`. So, unlike
                // `super::yrs::YrsAdapter`, non-ASCII text indexes correctly
                // here. The returned deleted string is dropped.
                self.doc
                    .get_text(obj.as_str())
                    .splice(*pos, *del_count, insert)?;
            }
        }
        // Commit the implicit transaction now. Then `op_duration_ms` includes
        // the commit cost, and the next `sync_generate` sees exactly this
        // change. Without this, Loro commits lazily at the next export or
        // import, and that call pays the cost. AutomergeAdapter commits here
        // for the same reason.
        self.doc.commit();
        Ok(())
    }

    fn get_heads(&mut self) -> Vec<Vec<u8>> {
        // Loro's `Frontiers` is a DAG frontier: the set of maximal op ids in
        // the causal graph. It is the direct analogue of Automerge's heads,
        // not of Yrs's state vector. A delete in Loro is an op that advances
        // the peer's counter. (In Yrs, a delete only flags an existing block.)
        // So the frontier moves on a delete and can serve as a fingerprint.
        //
        // Each head is encoded as peer (8 bytes BE) then counter (4 bytes BE).
        // The sort is required. `Frontiers::Map` uses an `FxHashMap`, whose
        // iteration order is unspecified. `YrsAdapter::get_heads` sorts its
        // `StateVector` for the same reason.
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
        // The sorted, concatenated frontier, as in AutomergeAdapter. Two
        // replicas with the same frontier have the same history, and so the
        // same state.
        //
        // Do not use the snapshot bytes. Converged replicas can produce
        // different snapshots: five line-topology replicas with an identical
        // frontier produced snapshots of 654/667/654/667/654 bytes. The cause
        // is the encoded width of random peer ids. See
        // `loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids`.
        self.get_heads().into_iter().flatten().collect()
    }

    fn doc_size_bytes(&mut self) -> usize {
        // `ExportMode::Snapshot` encodes the whole document: full history
        // plus materialized state. It is the closest analogue of Automerge's
        // `save()`.
        //
        // Unlike the Automerge and Yrs encodings, this size can shrink. The
        // snapshot has a state section, so a map-key delete can remove more
        // state bytes than its op adds to history (measured: 240 -> 238 for a
        // MapPut then a MapDelete). So this adapter does not claim
        // `doc_size_grows_with_every_op`. See
        // `loro_doc_size_can_shrink_on_delete`.
        //
        // The trait gives no error channel here. Loro's only documented
        // `LoroEncodeError` cases are for shallow snapshots, which a plain
        // `Snapshot` export cannot hit. Return 0 instead of panicking a run.
        self.doc.export(ExportMode::Snapshot).map_or(0, |b| b.len())
    }

    fn sync_generate(&mut self, peer: &str) -> Option<Vec<u8>> {
        let my_vv = self.doc.oplog_vv();
        let known = self.peer_vv.get(peer).cloned().unwrap_or_default();
        // `None` must come from this comparison, not from an empty export.
        // An update with no ops is still 22 bytes (magic, checksum, header),
        // so the bytes are never empty and the push loop would never stop.
        if known.includes_vv(&my_vv) {
            return None;
        }
        let bytes = self.doc.export(ExportMode::updates(&known)).ok()?;
        // Assume this message arrives, and record that the peer now has
        // `my_vv`. Then the next call does not resend the same ops.
        // Automerge's `sync::State` does the same with the heads it sends. If
        // the message is lost, `sync_reset` recovers.
        self.peer_vv.insert(peer.to_owned(), my_vv);
        Some(bytes)
    }

    fn sync_receive(&mut self, peer: &str, msg: Vec<u8>) -> anyhow::Result<()> {
        // Loro accepts causally incomplete updates. Ops whose dependencies
        // have not arrived are buffered, reported in `ImportStatus::pending`,
        // and applied when the gap fills. This is normal for out-of-order
        // delivery, so it is not an error. It is also safe for the
        // convergence check: pending ops are not in the oplog, so they do not
        // move `get_heads`. A replica with a permanent gap correctly reads as
        // not converged.
        self.doc
            .import(&msg)
            .map_err(|e| anyhow::anyhow!("{e}"))
            .with_context(|| format!("applying update from '{peer}'"))?;
        Ok(())
    }

    fn sync_reset(&mut self, peer: &str) {
        // Removing the entry is the whole reset. The next `sync_generate`
        // compares against an empty version vector, so it re-exports
        // everything this peer might have missed.
        self.peer_vv.remove(peer);
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    /// Checks the name and authors no change.
    ///
    /// A Loro root container's identity is `(name, type)`, set by the
    /// accessor. Two replicas that call `get_text("text")` name the same
    /// container. There is no creation op, so there is nothing to conflict
    /// with. [`super::yrs::YrsAdapter::ensure_text`] also authors no change,
    /// for a different reason.
    fn ensure_text(&mut self, obj: &str) -> anyhow::Result<()> {
        Self::check_name(obj)?;
        Ok(())
    }

    /// Returns the text length in Unicode scalars.
    ///
    /// # Deviation from the trait contract
    ///
    /// The trait says this errors if the text object does not exist. In Loro,
    /// every `(name, Text)` root id exists, so an unused name is an empty text
    /// and this returns `Ok(0)`.
    ///
    /// The orchestrator still catches a wrong name. `verify_text_length`
    /// compares this length with the scenario's op count, so a wrong name
    /// fails as `0 != op_count` instead of as an error.
    /// `loro_text_length_of_an_untouched_name_is_zero_not_an_error` locks this
    /// in.
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

    // Not wrapped here. Each has a Loro test below that asserts what Loro
    // does instead:
    //
    // - ensure_text_is_deterministic_across_replicas and
    //   without_bootstrap_partitioned_text_loses_a_side: both need root
    //   object creation to be a mergeable op with a losing side. A Loro root
    //   id is `(name, type)`, so nothing can collide.
    // - ensure_text_rejects_non_empty_doc_without_the_object: Loro has no
    //   such precondition.
    // - doc_size_grows_with_every_op: Loro's snapshot has a state section,
    //   which can shrink.
    // - text_length_errors_on_missing_or_wrong_type: Loro cannot represent
    //   either failure.
    // - save_bytes_not_canonical_across_converged_replicas: Loro neither
    //   "reliably differs" like Automerge nor is "always identical" like Yrs.
    //   See the Loro test for its behaviour.
    // - reset_clears_doc_and_sync_state and
    //   reset_allows_clean_re_sync_to_another_replica: Automerge-only, for
    //   the reasons given on those functions. Their universal halves are
    //   wrapped above.

    // ── Loro-specific characterization ─────────────────────────────────
    //
    // These tests state what Loro does instead. If a Loro release changes
    // one of these behaviours, a test fails, instead of a comment going
    // silently stale.

    /// Loro version of `ensure_text_is_deterministic_across_replicas`
    /// (Automerge-only).
    ///
    /// Loro authors no change, as Yrs does, but for a different reason. Yrs
    /// materializes root types in a local registry. Loro materializes
    /// nothing: a root `ContainerID` is derived from the name and the
    /// accessor's type. Two replicas that call `get_text("text")` name one
    /// container. The empty frontier is the expected result.
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

    /// Loro version of `ensure_text_rejects_non_empty_doc_without_the_object`
    /// (Automerge-only).
    ///
    /// Loro has no "must be the document's first change" precondition, so
    /// `ensure_text` succeeds on a document that already has history. The
    /// only name this adapter rejects is the empty one.
    #[test]
    fn loro_ensure_text_succeeds_on_a_non_empty_doc() {
        let mut a = LoroAdapter::default();
        a.apply_op(&map_put("doc", "k", "v")).unwrap();
        a.ensure_text("text").unwrap();

        let err = a.ensure_text("").unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    /// Loro version of `without_bootstrap_partitioned_text_loses_a_side`
    /// (Automerge-only).
    ///
    /// The lazy-creation collision cannot occur in Loro. Even with no
    /// `ensure_text`, the edits of both partitioned sides interleave.
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

    /// Loro version of `doc_size_grows_with_every_op`.
    ///
    /// Loro's snapshot holds materialized state as well as history. Deleting
    /// a map key can free more state bytes than the delete op adds to
    /// history. The test asserts a strict shrink, not just "no growth". A
    /// size that stays flat would also match a `MapDelete` arm that does
    /// nothing, which is the bug the generic test was written to catch.
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

    /// Loro version of `text_length_errors_on_missing_or_wrong_type`
    /// (Automerge and Yrs).
    ///
    /// Loro cannot represent either failure. A root `ContainerID` is
    /// `(name, type)`, and `LoroDoc::has_container` is always true for it.
    /// An unused name is an empty text, not a missing object. A name used as
    /// a list is a different container, not a type conflict. See
    /// `LoroAdapter::text_length` for why the orchestrator's validity check
    /// still works.
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

    /// Loro version of `save_bytes_not_canonical_across_converged_replicas`
    /// (Automerge-only) and of
    /// `yrs_save_bytes_are_canonical_across_converged_replicas`.
    ///
    /// Loro's snapshot is canonical in structure. Only the encoded width of
    /// the random `PeerID`s makes the snapshots of two converged replicas
    /// differ.
    ///
    /// Measured on the same 5-replica line topology as the Automerge test:
    /// with random peer ids, the snapshots differed in 16/100 trials at 10
    /// ops and 12/100 at 40 ops. The rate does not grow with history, so the
    /// cause is not integration order, as it is for Automerge. With peer ids
    /// pinned to 1..=5, the rate is 0/100 at both sizes.
    ///
    /// The test therefore pins the peer ids. With random ids the outcome
    /// would be random, and a flaky test is worse than none.
    ///
    /// Consequence for the doc-size results: a Loro size spread across
    /// replicas is peer-id noise. It is not the graph-position effect that
    /// Automerge's spread shows. Do not read the two the same way.
    #[test]
    fn loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids() {
        let mut replicas: Vec<LoroAdapter> = (1..=5)
            .map(LoroAdapter::with_peer_id)
            .collect::<Result<_, _>>()
            .unwrap();
        converged_line(&mut replicas, 10);

        let sizes: Vec<usize> = replicas
            .iter_mut()
            .map(LoroAdapter::doc_size_bytes)
            .collect();
        assert!(
            sizes.iter().min() == sizes.iter().max(),
            "snapshot stopped being canonical for equal-width peer ids — \
             sizes: {sizes:?}. If this starts failing, Loro's encoder gained \
             an integration-order dependence and the doc-size table's \
             interpretation above needs revisiting.",
        );
    }

    /// Loro-only. `Op::TextSplice` positions are Unicode-scalar offsets,
    /// taken from Automerge's `splice_text`. `LoroText::splice` uses the same
    /// offsets, so multi-byte text needs no translation.
    ///
    /// `super::super::yrs::YrsAdapter` cannot claim this yet. Yrs offers only
    /// UTF-8 and UTF-16 offsets, so its `TextSplice` is byte-indexed and is
    /// correct only for ASCII. This test shows that the trait contract can be
    /// met, so the Yrs gap is a limit of that adapter, not of the `Op` model.
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
