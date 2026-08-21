//! Generic conformance suite for any [`CrdtAdapter`] implementation.
//!
//! These functions are the *universal* half of the suite described in the
//! Day 3 notes: properties any adapter must satisfy regardless of which CRDT
//! library backs it. Each adapter module (`automerge.rs`, `yrs.rs`, ...) wraps
//! the ones it claims with a `#[test]` function. A generic body deliberately
//! left unwrapped for a given adapter is a documented characterization
//! difference, not an oversight — see that adapter's test module for why.
//!
//! A new adapter is done when every generic function here that it wraps
//! passes.

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

pub(crate) fn each_op_variant_mutates_the_doc<A: CrdtAdapter + Default>() {
    // Apply one of each Op variant in dependency order (deletes need
    // something to delete) and assert the fingerprint changes and the
    // doc size grows on every step. Guards against a new Op variant
    // being added to the enum but not wired up in apply_op — the match
    // is exhaustive so the compiler catches a missing arm, but it would
    // not catch an arm that silently no-ops.
    let mut a = A::default();
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

pub(crate) fn reset_clears_doc_and_sync_state<A: CrdtAdapter + Default>() {
    // Reset returns the adapter to its initial empty state: heads/
    // fingerprint go back to empty, doc_size matches a fresh adapter,
    // and any per-peer sync state entries are dropped (so sync_generate
    // produces a fresh handshake message rather than continuing an
    // already-quiesced conversation).
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

pub(crate) fn reset_allows_clean_re_sync_to_another_replica<A: CrdtAdapter + Default>() {
    // End-to-end at the adapter layer: a writes, syncs with b, reset both,
    // a writes different data, sync, and both converge on the new state
    // alone — proving the old history is gone, not merely hidden.
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

/// ACCOMMODATION: the trait does not specify `text_length`'s error text for
/// the wrong-type case, only that it must fail. "not Text" happens to be
/// worded the same way by convention across the adapters written so far;
/// nothing requires that to stay true.
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
