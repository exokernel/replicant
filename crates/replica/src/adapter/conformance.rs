//! Generic conformance suite for any [`CrdtAdapter`] implementation.
//!
//! Every function here is generic over the adapter. Each adapter module
//! (`automerge.rs`, `yrs.rs`, `loro.rs`) wraps the ones it claims with a
//! `#[test]`. A body deliberately left unwrapped for a given adapter is a
//! documented characterization difference, not an oversight — see that
//! adapter's test module for the mirror test asserting what it does instead.
//!
//! A new adapter is done when every generic function here that it wraps
//! passes.
//!
//! # Two kinds of function live here
//!
//! - **Universal**: properties any adapter must satisfy regardless of which
//!   library backs it. Every adapter wraps these.
//! - **Characterization** (marked `ACCOMMODATION` in their doc comments):
//!   properties true of *some* libraries, kept generic rather than moved
//!   into one adapter module because the body has no library-specific
//!   imports and a future adapter may share the property. That bet has paid
//!   out once already — `save_bytes_not_canonical_across_converged_replicas`
//!   was written as Automerge-only and Loro turned out to need it as the
//!   thing it is *not*.
//!
//! # Keeping the line in the right place
//!
//! The split is empirical, not predicted: a function is universal until an
//! adapter demonstrates otherwise. It moves in both directions.
//!
//! Adding Loro as the third adapter moved three properties *toward*
//! universal and one *away* from it, and the deciding rule in each case was
//! the same — when a second adapter needed the identical mirror test, the
//! mirror was the universal form and the original was the characterization:
//!
//! - `reset_returns_adapter_to_a_fresh_state`,
//!   `reset_drops_stale_peer_state` and
//!   `reset_allows_clean_re_sync_with_equal_fingerprint` began as
//!   hand-written Yrs-local replacements for two Automerge-shaped tests.
//!   Loro needed the same replacements verbatim, so they were lifted here
//!   and the Automerge-only assertions they dropped stayed behind under the
//!   original names.
//! - `each_op_variant_mutates_the_doc` used to assert both "the fingerprint
//!   moved" and "the document grew". Loro's snapshot can shrink on a
//!   delete, so the size half split off into
//!   `doc_size_grows_with_every_op`.

use common::{CrdtAdapter, Op, ScalarVal};

/// Pump sync messages between two adapters until both return `None`,
/// meaning each side believes the other is caught up.
pub(crate) fn sync_until_quiescent<A: CrdtAdapter>(a: &mut A, a_id: &str, b: &mut A, b_id: &str) {
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

pub(crate) fn map_put(obj: &str, key: &str, value: impl Into<ScalarVal>) -> Op {
    Op::MapPut {
        obj: obj.to_owned(),
        key: key.to_owned(),
        value: value.into(),
    }
}

pub(crate) fn identical_ops_yield_equal_fingerprints<A: CrdtAdapter + Default>() {
    // Two fresh adapters that apply the exact same op sequence should
    // produce the same heads and the same fingerprint — no sync involved.
    let mut a = A::default();
    let mut b = A::default();
    // ACCOMODATION: Automerge
    // Change identifiers may depend on per-instance actor id, which is random per
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

pub(crate) fn disjoint_edits_converge_after_sync<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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

pub(crate) fn concurrent_edits_to_same_key_converge<A: CrdtAdapter + Default>() {
    // Both replicas write to the same key with no prior sync. The DAG
    // ends up with two heads; get_heads must sort them so byte-equal
    // fingerprint comparison still works.
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "from-a")).unwrap();
    b.apply_op(&map_put("doc", "k", "from-b")).unwrap();

    sync_until_quiescent(&mut a, "a", &mut b, "b");

    let a_heads = a.get_heads();
    let b_heads = b.get_heads();
    assert_eq!(a_heads, b_heads);
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());
}

pub(crate) fn post_sync_divergence_is_detected<A: CrdtAdapter + Default>() {
    // Negative case: if a writes after sync without re-syncing, the
    // fingerprints must differ. Without this, a buggy fingerprint that
    // returns a constant value would still pass the convergence tests.
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v0")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());

    a.apply_op(&map_put("doc", "k", "v1")).unwrap();
    assert_ne!(a.state_fingerprint(), b.state_fingerprint());
    assert_ne!(a.get_heads(), b.get_heads());
}

/// One of each [`Op`] variant, in dependency order (deletes need something
/// to delete). Shared by the two functions below so they exercise exactly
/// the same sequence.
fn op_variants() -> Vec<Op> {
    vec![
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
    ]
}

/// Every `Op` variant must move the fingerprint. Guards against a new
/// variant being added to the enum but not wired up in `apply_op` — the
/// match is exhaustive so the compiler catches a *missing* arm, but it
/// would not catch an arm that silently no-ops.
///
/// This is the universal half of what used to be one test; the doc-size
/// half moved to `doc_size_grows_with_every_op` when `LoroAdapter` showed
/// it is not universal.
pub(crate) fn each_op_variant_mutates_the_doc<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut prev_fp = a.state_fingerprint();

    for op in &op_variants() {
        a.apply_op(op).unwrap();
        let fp = a.state_fingerprint();
        assert_ne!(fp, prev_fp, "fingerprint unchanged after {}", op.name());
        prev_fp = fp;
    }
}

/// ACCOMMODATION: adapters whose `doc_size_bytes` reads an append-only
/// encoding (Automerge's `save()`, Yrs's full update — both of which encode
/// history, with deletions represented as *additional* data). For those,
/// document size is monotonically non-decreasing, and a delete that failed
/// to record anything would show up here as a flat size.
///
/// It is not universal. Loro's `ExportMode::Snapshot` carries a state
/// section alongside the history section, so removing a map key can free
/// more state bytes than the deletion op adds history bytes: measured
/// 252 -> 250 across a `MapPut` then `MapDelete`. See
/// `loro_doc_size_can_shrink_on_delete` for the mirror assertion. That is
/// a property of the encoding, not evidence the op was dropped — which is
/// exactly why the fingerprint check above, not this one, is the universal
/// guard.
pub(crate) fn doc_size_grows_with_every_op<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut prev_size = a.doc_size_bytes();

    for op in &op_variants() {
        a.apply_op(op).unwrap();
        let size = a.doc_size_bytes();
        assert!(
            size > prev_size,
            "doc size did not grow after {} ({prev_size} -> {size})",
            op.name()
        );
        prev_size = size;
    }
}

pub(crate) fn reads_are_stable_without_writes<A: CrdtAdapter + Default>() {
    // state_fingerprint() and get_heads() must be pure with respect to
    // document state: repeated calls without intervening writes return
    // identical bytes. Guards against the fingerprint accidentally
    // including incidental state (a counter, a transaction id, etc.)
    // that the orchestrator's convergence check would mistake for a
    // real divergence.
    let mut a = A::default();
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

pub(crate) fn sync_is_idempotent_once_converged<A: CrdtAdapter + Default>() {
    // After sync_until_quiescent, both sides should immediately report
    // "nothing to send." The orchestrator's convergence-detection loop
    // depends on this; a regression that makes sync chatty would only
    // show up downstream as a flaky integration test.
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");

    assert!(a.sync_generate("b").is_none());
    assert!(b.sync_generate("a").is_none());
}

pub(crate) fn three_replicas_converge_through_a_hub<A: CrdtAdapter + Default>() {
    // Line topology: a <-> b <-> c, then a <-> c directly. Each pair
    // uses its own per-peer sync state, so this catches cross-talk bugs in
    // the per-peer state map.
    let mut a = A::default();
    let mut b = A::default();
    let mut c = A::default();
    a.apply_op(&map_put("doc", "from_a", 1u64)).unwrap();
    b.apply_op(&map_put("doc", "from_b", 2u64)).unwrap();
    c.apply_op(&map_put("doc", "from_c", 3u64)).unwrap();

    sync_until_quiescent(&mut a, "a", &mut b, "b");
    sync_until_quiescent(&mut b, "b", &mut c, "c");
    sync_until_quiescent(&mut a, "a", &mut c, "c");

    assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    assert_eq!(b.state_fingerprint(), c.state_fingerprint());
}

/// Reset returns the adapter to its initial empty state: no heads, a
/// fingerprint and doc size indistinguishable from a never-used adapter.
///
/// Compared against a fresh `A::default()` rather than against emptiness:
/// "the empty document encodes to zero bytes" is an Automerge/Loro
/// coincidence (both flatten an empty head list), not a contract — Yrs's
/// empty-document update is a fixed 2-byte marker.
pub(crate) fn reset_returns_adapter_to_a_fresh_state<A: CrdtAdapter + Default>() {
    let mut fresh = A::default();
    let fresh_fp = fresh.state_fingerprint();
    let fresh_size = fresh.doc_size_bytes();

    let mut a = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    assert!(!a.get_heads().is_empty());
    assert_ne!(
        a.state_fingerprint(),
        fresh_fp,
        "fingerprint must have moved after a write"
    );

    a.reset();

    assert!(a.get_heads().is_empty(), "heads not cleared by reset");
    assert_eq!(
        a.state_fingerprint(),
        fresh_fp,
        "fingerprint not returned to a fresh-adapter value by reset"
    );
    assert_eq!(
        a.doc_size_bytes(),
        fresh_size,
        "doc not reset to empty size"
    );
}

/// Reset must *drop* per-peer bookkeeping, not merely hide it behind an
/// empty document.
///
/// Stated as "after reset, a new write is still offered to a peer this
/// adapter had already caught up with pre-reset". If the stale entry
/// leaked, the post-reset write would be compared against the pre-reset
/// progress and could be wrongly suppressed.
///
/// Deliberately does *not* assert that a freshly-reset adapter has
/// something to send with no content to report — that holds only for a
/// protocol that opens with a content-free handshake (Automerge's
/// `sync::State`), and both vector-diffing adapters legitimately stay
/// silent. See `reset_clears_doc_and_sync_state` for the Automerge-only
/// version that does assert it.
pub(crate) fn reset_drops_stale_peer_state<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    assert!(
        a.sync_generate("peer-x").is_some(),
        "there was a write to report"
    );

    a.reset();

    a.apply_op(&map_put("doc", "k2", "v2")).unwrap();
    assert!(
        a.sync_generate("peer-x").is_some(),
        "reset must have dropped peer-x's pre-reset progress"
    );
}

/// End-to-end at the adapter layer: a writes, syncs with b, both reset, a
/// writes different data, sync, and both converge again — proving the old
/// per-peer state is gone rather than merely stale.
///
/// Convergence is asserted on fingerprints, not sizes. Sizes do not
/// generalize: two adapters carry a random per-instance id (Yrs's var-int
/// `ClientID`, Loro's `PeerID`) whose encoded width can differ for
/// logically identical content. See
/// `reset_allows_clean_re_sync_to_another_replica` for the Automerge-only
/// size-equality version.
pub(crate) fn reset_allows_clean_re_sync_with_equal_fingerprint<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "before", "old")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());

    a.reset();
    b.reset();
    let fresh = A::default().state_fingerprint();
    assert_eq!(
        a.state_fingerprint(),
        fresh,
        "reset must return to a fresh-doc fingerprint"
    );
    assert_eq!(b.state_fingerprint(), fresh);

    a.apply_op(&map_put("doc", "after", "new")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(
        a.state_fingerprint(),
        b.state_fingerprint(),
        "post-reset replicas must still converge with each other"
    );
}

/// ACCOMMODATION: Automerge-only. Retains the two assertions that the
/// generic `reset_returns_adapter_to_a_fresh_state` and
/// `reset_drops_stale_peer_state` deliberately dropped, because both are
/// true of a stateful handshake protocol and of no other adapter written
/// so far:
///
/// - a freshly-reset adapter's fingerprint is literally empty (Automerge
///   flattens an empty head list; Yrs's is a 2-byte marker);
/// - `sync_generate` for a never-seen peer always has *something* to say,
///   even with an empty document, because `sync::State` opens with a
///   content-free handshake. Both vector-diffing adapters stay silent
///   there by design — always greeting every idle peer is exactly the
///   unforced per-link overhead the sync-protocol design discussion ruled
///   out.
pub(crate) fn reset_clears_doc_and_sync_state<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    // Populate per-peer sync state so the reset has something to clear.
    let initial_msg = a.sync_generate("peer-x");
    assert!(
        initial_msg.is_some(),
        "fresh adapter should send a handshake"
    );
    let fresh_size = A::default().doc_size_bytes();
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
    // scratch — if per-peer sync state leaked across reset, the second call
    // would observe quiescence and return None.
    assert!(
        a.sync_generate("peer-x").is_some(),
        "reset must drop per-peer sync state"
    );
}

/// ACCOMMODATION: Automerge-only. The generic
/// `reset_allows_clean_re_sync_with_equal_fingerprint` covers the
/// convergence property; what this adds is the stricter size claim — a
/// post-reset document must be byte-for-byte the size of a from-scratch
/// one-write document. That holds only where the per-instance identifier
/// is fixed-width (Automerge's 16-byte UUID `ActorId`). Yrs's `ClientID`
/// and Loro's `PeerID` are both var-int encoded, so two independently
/// random ids can legitimately need a different number of bytes for the
/// same logical content — observed as a 29-vs-30-byte failure for Yrs.
pub(crate) fn reset_allows_clean_re_sync_to_another_replica<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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
    let mut baseline = A::default();
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
///
/// ACCOMMODATION: Automerge-only. This asserts `ensure_text` authors a
/// change (heads non-empty). It is only true of adapters whose root-level
/// object creation is itself an operation subject to the same merge
/// semantics as any other op — which is why Automerge needs the
/// BOOTSTRAP_ACTOR trick in the first place. Libraries whose root-level
/// containers are identity-stable by name with no possible creation
/// collision (Yrs: see `adapter::yrs`) legitimately fail this — see that
/// adapter's own characterization test for the mirror-image assertion.
pub(crate) fn ensure_text_is_deterministic_across_replicas<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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

pub(crate) fn ensure_text_is_idempotent<A: CrdtAdapter + Default>() {
    let mut a = A::default();
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
pub(crate) fn partitioned_text_edits_interleave_after_bootstrap<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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
///
/// ACCOMMODATION: Automerge-only, for the same reason as
/// `ensure_text_is_deterministic_across_replicas` — this hazard requires
/// root-object creation to be a mergeable op with a losing side, which is
/// not true of every adapter.
pub(crate) fn without_bootstrap_partitioned_text_loses_a_side<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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

/// ACCOMMODATION: the trait does not specify `ensure_text`'s error text,
/// only Automerge's "document already has changes" precondition happens to
/// say "first change". Adapters without that precondition (Yrs: root types
/// have no such ordering constraint) have no analogous error to test.
pub(crate) fn ensure_text_rejects_non_empty_doc_without_the_object<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    let err = a.ensure_text("text").unwrap_err();
    assert!(err.to_string().contains("first change"), "{err}");
}

/// A synced-in object satisfies ensure_text — the late replica adopts it
/// rather than authoring a bootstrap of its own.
pub(crate) fn ensure_text_adopts_synced_in_object<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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

/// The adapter's object resolution must reuse an object that arrived via
/// sync instead of creating a concurrent one — the connected-topology
/// flavour of the same collision (a round-robin writer's first op racing
/// the sync of another node's creation).
pub(crate) fn first_local_write_reuses_synced_in_object<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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
///
/// This is a contract-level assertion, not a mechanism-level one: it
/// exercises "quiesced -> reset -> generate has something to say again"
/// without assuming *how* an adapter tracks quiescence. Automerge threads
/// a `sync::State`; other adapters may cache a remembered peer state
/// vector or something else native to their protocol — either satisfies
/// this test.
pub(crate) fn sync_reset_forgets_quiescence<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
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
pub(crate) fn sync_reset_recovers_from_a_lost_message<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();

    // Generated but never delivered: a's per-peer sync state records these
    // heads as sent.
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
pub(crate) fn sync_reset_unknown_peer_is_noop<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.sync_reset("never-seen");
    assert!(a.get_heads().is_empty());
}

/// ACCOMMODATION: adapters in which an object name can fail to resolve.
///
/// Two separate assumptions, both Automerge- and Yrs-true and both
/// Loro-false: that a never-written name is a *missing* object rather than
/// an empty one, and that one name can be claimed by one type at a time.
/// Loro derives a root container id from `(name, type)` and treats every
/// such id as existing, so neither failure is representable — see
/// `loro_text_length_of_an_untouched_name_is_zero_not_an_error`.
///
/// The error *text* is a weaker accommodation on top: the trait does not
/// specify it, only that the call must fail. "not Text" happens to be
/// worded the same way by convention in the two adapters that can fail
/// this way; nothing requires that to stay true.
pub(crate) fn text_length_errors_on_missing_or_wrong_type<A: CrdtAdapter + Default>() {
    let mut a = A::default();
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

/// ACCOMMODATION: Automerge-only — confirmed, not merely untested elsewhere.
/// Locks in a known Automerge property: two replicas with identical
/// logical state (same heads, same fingerprint, same readable values)
/// can produce *different* save() byte streams. The encoding preserves
/// change-list storage order, which depends on integration order, and
/// that order varies by graph position in non-mesh topologies with
/// distributed writes.
///
/// The notebook's per-scenario doc-size table treats this as expected
/// and presents the spread as informational rather than an assertion.
/// If a future Automerge release ever canonicalizes save(), this test
/// will start failing and the table can be tightened to strict
/// equality. Tried against `YrsAdapter` directly
/// (`yrs_save_bytes_are_canonical_across_converged_replicas`) and it
/// asserts the opposite: Yrs's full-update encoding sorts blocks by
/// client id before writing, so it is canonical across converged
/// replicas regardless of graph position. Kept generic here rather than
/// moved into `automerge.rs` for the same reason as the other
/// Automerge-only functions above: the body has no Automerge-specific
/// imports, so a future adapter with the same non-canonical-save
/// property (plausible for a from-scratch RGA implementation) can wrap
/// it directly without extraction work.
///
/// Loro is a third answer rather than a vote for either: its snapshot is
/// structurally canonical, and converged replicas differ only when their
/// random `PeerID`s encode to different widths (measured at 12-16% of
/// trials, flat in history length). Asserting this function for Loro would
/// be a coin flip, so it wraps the pinned-peer-id form instead — see
/// `loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids`.
/// Three adapters, three different behaviours here: worth remembering
/// before treating any per-replica doc-size spread as one phenomenon.
pub(crate) fn save_bytes_not_canonical_across_converged_replicas<A: CrdtAdapter + Default>() {
    let n = 5;
    let op_count = 10;
    let mut replicas: Vec<A> = (0..n).map(|_| A::default()).collect();
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
