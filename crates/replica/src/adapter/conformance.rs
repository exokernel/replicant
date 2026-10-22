//! Generic conformance suite for any [`CrdtAdapter`] implementation.
//!
//! Every function here is generic over the adapter. Each adapter module
//! (`automerge.rs`, `yrs.rs`, `loro.rs`) wraps the functions it supports in a
//! `#[test]`. If an adapter does not wrap a function, that is a known
//! difference, not an omission. A mirror test then asserts what the adapter
//! does instead. The mirror is in the adapter's own test module, or here if
//! more than one adapter needs it.
//!
//! A new adapter is complete when every function that it wraps passes.
//!
//! # Two kinds of function
//!
//! - **Universal**: a property that every adapter must have. Every adapter
//!   wraps these.
//! - **Characterization** (marked `ACCOMMODATION` in the doc comment): a
//!   property that only some libraries have. These stay here, not in one
//!   adapter module, because the body uses no library-specific code and a
//!   future adapter can have the same property.
//!
//! # Moving a function between the two kinds
//!
//! A function is universal until an adapter fails it for a valid reason.
//! When a second adapter needs the same mirror test as the first, move the
//! mirror here. The mirror is universal if every adapter passes it.
//! Otherwise mark it `ACCOMMODATION` too, as
//! `ensure_text_is_a_pure_lookup_no_change_authored` is.

use common::{CrdtAdapter, Op, ScalarVal};

/// Sends sync messages between two adapters until both return `None`.
/// `None` means that each side believes the other is up to date.
pub(crate) fn sync_until_quiescent<A: CrdtAdapter>(a: &mut A, a_id: &str, b: &mut A, b_id: &str) {
    // The bound stops a faulty adapter from looping forever. A handshake on
    // these small documents takes far fewer rounds.
    for _ in 0..64 {
        if !sync_once(a, a_id, b, b_id) {
            return;
        }
    }
    panic!("sync did not reach quiescence within 64 rounds");
}

/// Sends at most one message in each direction between two adapters.
///
/// Returns `true` if any message was sent. Callers use this to decide when to
/// stop. Each message moves into the receiver and is not cloned, because the
/// 1e4 divergence cells make large messages.
pub(crate) fn sync_once<A: CrdtAdapter>(a: &mut A, a_id: &str, b: &mut A, b_id: &str) -> bool {
    let mut moved = false;
    if let Some(msg) = a.sync_generate(b_id) {
        b.sync_receive(a_id, msg).unwrap();
        moved = true;
    }
    if let Some(msg) = b.sync_generate(a_id) {
        a.sync_receive(b_id, msg).unwrap();
        moved = true;
    }
    moved
}

/// Returns the peer id for replica `i` in a line topology.
fn line_id(i: usize) -> String {
    format!("node-{i}")
}

/// Sends one message in each direction on every edge of the line topology
/// `0 <-> 1 <-> .. <-> n-1`. Returns `true` if any message was sent.
///
/// This approximates what the server's `flush_to_peers` does after
/// `apply_op`. In a line, unlike a full mesh, replicas integrate changes in
/// different orders. A library encoder that depends on that order then
/// produces different bytes.
pub(crate) fn sync_line_once<A: CrdtAdapter>(replicas: &mut [A]) -> bool {
    let mut moved = false;
    for j in 0..replicas.len().saturating_sub(1) {
        let (left, right) = replicas.split_at_mut(j + 1);
        moved |= sync_once(&mut left[j], &line_id(j), &mut right[0], &line_id(j + 1));
    }
    moved
}

/// Runs `sync_line_once` until no message is sent.
pub(crate) fn sync_line_until_quiescent<A: CrdtAdapter>(replicas: &mut [A]) {
    for _ in 0..64 {
        if !sync_line_once(replicas) {
            return;
        }
    }
    panic!("line topology did not reach quiescence within 64 rounds");
}

/// Applies `op_count` map writes round-robin across `replicas` in a line
/// topology, and asserts that all replicas converge.
///
/// The line syncs one round after each write, then syncs until quiescent.
///
/// Every save-byte canonicity test uses this setup. The tests differ only in
/// what they assert about the encodings, so their results are comparable.
pub(crate) fn converged_line<A: CrdtAdapter>(replicas: &mut [A], op_count: usize) {
    let n = replicas.len();
    for i in 0..op_count {
        replicas[i % n]
            .apply_op(&map_put("doc", &format!("k{i}"), i as u64))
            .unwrap();
        sync_line_once(replicas);
    }
    sync_line_until_quiescent(replicas);

    let fp = replicas[0].state_fingerprint();
    for (i, replica) in replicas.iter_mut().enumerate().skip(1) {
        assert_eq!(
            replica.state_fingerprint(),
            fp,
            "replica {i} did not converge with replica 0"
        );
    }
}

/// Builds an `Op::MapPut`.
pub(crate) fn map_put(obj: &str, key: &str, value: impl Into<ScalarVal>) -> Op {
    Op::MapPut {
        obj: obj.to_owned(),
        key: key.to_owned(),
        value: value.into(),
    }
}

/// A write that syncs from one replica to another gives equal heads and an
/// equal, non-empty fingerprint on both.
pub(crate) fn identical_ops_yield_equal_fingerprints<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    // The test does not apply the same op on both replicas. A change id can
    // include the per-instance actor id, which is random, so the same op on
    // two replicas can give two different changes. Apply on `a`, then sync
    // to `b`.
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");

    assert_eq!(a.get_heads(), b.get_heads());
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    assert!(
        !a.state_fingerprint().is_empty(),
        "fingerprint after a write must not be empty"
    );
}

/// Two replicas that write different keys differ before sync and are equal
/// after sync.
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

/// Two replicas that write the same key concurrently converge after sync.
///
/// The merged history has two heads. `get_heads` must return them in a fixed
/// order, so that a byte comparison of heads and fingerprints still works.
pub(crate) fn concurrent_edits_to_same_key_converge<A: CrdtAdapter + Default>() {
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

/// A write after sync, with no second sync, makes the fingerprints and heads
/// differ.
///
/// This is the negative case. Without it, a fingerprint that always returns
/// the same value would pass every convergence test.
pub(crate) fn post_sync_divergence_is_detected<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v0")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());

    a.apply_op(&map_put("doc", "k", "v1")).unwrap();
    assert_ne!(a.state_fingerprint(), b.state_fingerprint());
    assert_ne!(a.get_heads(), b.get_heads());
}

/// Map ops on ROOT (the empty `obj` name) work and converge.
///
/// The orchestrator's `map_put` sends `obj: ""`. All other functions in this
/// suite name their map, so this function is the only one that tests the
/// shape the orchestrator really sends.
///
/// When you add a test here, check it against what the callers send, not
/// only against the trait.
pub(crate) fn root_map_ops_are_supported<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();

    a.apply_op(&map_put("", "k", "v")).unwrap();
    let after_put = a.state_fingerprint();
    assert_ne!(
        after_put,
        A::default().state_fingerprint(),
        "a ROOT map put must change the document"
    );

    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());

    // ROOT must be one object on both replicas, not one object per replica.
    // So when `b` deletes the key that `a` wrote, the delete must reach
    // `a`'s entry.
    b.apply_op(&Op::MapDelete {
        obj: String::new(),
        key: "k".into(),
    })
    .unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());
    assert_ne!(
        a.state_fingerprint(),
        after_put,
        "the ROOT delete must be visible on the writer's side too — if each \
         replica had its own ROOT, this would converge without the delete \
         ever reaching a's entry"
    );
}

/// Returns one op of each [`Op`] variant.
///
/// Each delete comes after the insert it removes. The two functions below
/// both use this list, so they test the same sequence.
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

/// Every `Op` variant changes the fingerprint.
///
/// The compiler finds a missing match arm in `apply_op`. It does not find an
/// arm that does nothing. This test finds that.
///
/// `doc_size_grows_with_every_op` is the size check. It is a separate
/// function because it is not universal.
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

/// Every `Op` variant makes the document larger.
///
/// ACCOMMODATION: Automerge and Yrs. Their `doc_size_bytes` reads an
/// append-only encoding of history (`save()` and the full update). A delete
/// adds data, so the size never goes down. A delete that records nothing
/// shows here as a size that does not change.
///
/// Loro fails this test, and that is correct. Its snapshot also holds the
/// current state, so a map delete can free more state bytes than it adds in
/// history (252 to 250 bytes for a `MapPut` and a `MapDelete`). See
/// `loro_doc_size_can_shrink_on_delete`. Use
/// `each_op_variant_mutates_the_doc` as the universal check that an op had an
/// effect.
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

/// A `Uint` above `i64::MAX` is an error, and the document does not change.
///
/// ACCOMMODATION: Yrs and Loro. Both store integers as `i64` and have no
/// unsigned type, so the adapter must reject the value instead of wrapping
/// it. Automerge has a native `u64` and stores the value. See
/// `automerge_stores_uint_above_i64_max`.
///
/// The splice puts a valid value before the bad one. The adapter must check
/// every value before its first edit, so the valid value must not land.
pub(crate) fn uint_above_i64_max_is_rejected<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let before = a.state_fingerprint();
    let big = ScalarVal::Uint(u64::MAX);
    let ops = [
        map_put("m", "k", big.clone()),
        Op::ListInsert {
            obj: "l".into(),
            index: 0,
            value: big.clone(),
        },
        Op::ListSplice {
            obj: "l".into(),
            pos: 0,
            del_count: 0,
            values: vec![ScalarVal::Uint(1), big],
        },
    ];
    for op in &ops {
        let err = a.apply_op(op).unwrap_err();
        assert!(err.to_string().contains("exceeds i64::MAX"), "{err}");
        // Read the size first. A library with an implicit transaction (Loro)
        // commits on export, so a partial edit then shows in the fingerprint.
        a.doc_size_bytes();
        assert_eq!(
            a.state_fingerprint(),
            before,
            "rejected {} changed the doc",
            op.name()
        );
    }
}

/// Repeated calls to `state_fingerprint` and `get_heads`, with no write
/// between them, return the same bytes.
///
/// If the fingerprint included other state, such as a counter or a
/// transaction id, the orchestrator's convergence check would see a
/// divergence that is not real.
pub(crate) fn reads_are_stable_without_writes<A: CrdtAdapter + Default>() {
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

/// After two replicas converge, `sync_generate` returns `None` on both sides.
///
/// The orchestrator's convergence loop needs this. If sync keeps sending
/// messages after convergence, this test fails here. Otherwise the fault
/// shows only as a flaky integration test.
pub(crate) fn sync_is_idempotent_once_converged<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");

    assert!(a.sync_generate("b").is_none());
    assert!(b.sync_generate("a").is_none());
}

/// Three replicas converge when they sync pair by pair: `a <-> b`,
/// `b <-> c`, then `a <-> c`.
///
/// Each pair uses its own per-peer sync state. A fault that mixes up two
/// peers' entries in the per-peer map makes this test fail.
pub(crate) fn three_replicas_converge_through_a_hub<A: CrdtAdapter + Default>() {
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

/// After `reset`, the adapter has no heads, and its fingerprint and doc size
/// are equal to those of a new adapter.
///
/// The test compares against a new `A::default()`, not against empty bytes.
/// The trait does not say that an empty document has an empty fingerprint.
/// For Yrs it does not: the empty document encodes to a fixed 2-byte marker.
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

/// `reset` deletes per-peer sync state.
///
/// After `reset`, a new write must go to a peer that was up to date before
/// the reset. If the old per-peer entry stayed, the adapter could compare
/// the new write against old progress and not send it.
///
/// This test does not assert that a reset adapter with no writes has a
/// message to send. Only a protocol that starts with a handshake does that
/// (Automerge). The version-vector adapters (Yrs, Loro) correctly send
/// nothing. `reset_clears_doc_and_sync_state` is the Automerge-only check.
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

/// Two replicas that sync, reset, and write again converge again.
///
/// The sequence is: `a` writes and syncs with `b`. Both reset. `a` writes
/// different data, and the two sync. Convergence shows that the old per-peer
/// state is gone.
///
/// The test compares fingerprints, not sizes. Yrs and Loro use a random
/// per-instance id with a variable-length encoding, so two equal documents
/// can have different sizes.
/// `reset_allows_clean_re_sync_to_another_replica` is the Automerge-only
/// size check.
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

/// `reset` clears the document and the per-peer sync state.
///
/// ACCOMMODATION: Automerge only. This test keeps two assertions that the
/// universal `reset_returns_adapter_to_a_fresh_state` and
/// `reset_drops_stale_peer_state` do not make:
///
/// - The fingerprint of a reset adapter is empty. Automerge's fingerprint is
///   the flattened head list, which is empty. Yrs's is a 2-byte marker.
/// - `sync_generate` for an unknown peer always returns a message, even for
///   an empty document. Automerge's `sync::State` starts with a handshake.
///   The version-vector adapters (Yrs, Loro) send nothing when there is
///   nothing to send.
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
    // Sync with the same peer id must start again from the beginning. If
    // the per-peer state stayed after reset, this call would return None.
    assert!(
        a.sync_generate("peer-x").is_some(),
        "reset must drop per-peer sync state"
    );
}

/// After sync, reset, and one new write, the document has the same size as
/// a new document with only that write.
///
/// ACCOMMODATION: Automerge only. The universal
/// `reset_allows_clean_re_sync_with_equal_fingerprint` checks convergence.
/// This test adds the size check. The size check needs a fixed-width
/// per-instance id, such as Automerge's 16-byte `ActorId`. Yrs's `ClientID`
/// and Loro's `PeerID` have a variable-length encoding, so two random ids can
/// give different sizes for the same content (29 and 30 bytes for Yrs).
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
    // The reset document must have the size of a new document with only
    // the "after" write, not the size of a two-write document.
    let mut baseline = A::default();
    baseline.apply_op(&map_put("doc", "after", "new")).unwrap();
    assert_eq!(
        a.doc_size_bytes(),
        baseline.doc_size_bytes(),
        "post-reset doc size differs from a from-scratch single-write doc"
    );
}

// ── ensure_text / text_length (shared-object bootstrap) ────────────────

/// Two replicas that call `ensure_text` without sync make the same change
/// and so have the same heads.
///
/// The divergence workload depends on this. It makes all replicas share one
/// text object.
///
/// ACCOMMODATION: Automerge only. The test asserts that `ensure_text` makes
/// a change (the heads are not empty). That is true only where creating a
/// root object is an op that merges like any other op. This is why Automerge
/// needs `BOOTSTRAP_ACTOR`. In Yrs and Loro, a root object is identified by
/// its name, so creation cannot collide and makes no change. Their mirror is
/// `ensure_text_is_a_pure_lookup_no_change_authored`.
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

/// A second `ensure_text` call on the same name makes no change.
pub(crate) fn ensure_text_is_idempotent<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.ensure_text("text").unwrap();
    let heads = a.get_heads();
    a.ensure_text("text").unwrap();
    assert_eq!(a.get_heads(), heads, "second call must not author a change");
}

/// Two replicas that call `ensure_text` and then edit while partitioned
/// merge into one text that holds the inserts from both sides.
///
/// Without `ensure_text`, each replica can create its own text object. The
/// merge then keeps one object and loses the other side's inserts, but the
/// fingerprints still converge. Only the text length shows the loss.
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
    // Divergence: each side inserts at position 0 ten times, with no sync.
    // This is the same_region shape, where every op contests the same
    // anchor.
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

/// Without `ensure_text`, two partitioned replicas each create a text
/// object, and the merge keeps only one.
///
/// This shows why partitioned text workloads must call `ensure_text`.
/// `text_length` is the check that finds the loss. If a future Automerge
/// release removes the collision, this test fails.
///
/// ACCOMMODATION: Automerge only, for the same reason as
/// `ensure_text_is_deterministic_across_replicas`. The collision needs root
/// object creation to be an op that can lose a merge. The Yrs and Loro
/// mirror is `partitioned_text_edits_interleave_even_without_ensure_text`.
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

/// `ensure_text` makes no change. The heads stay empty, and two replicas
/// that call it without sync have equal fingerprints.
///
/// ACCOMMODATION: Yrs and Loro. This is the mirror of
/// `ensure_text_is_deterministic_across_replicas`. In both libraries a root
/// object is identified by its name, so creating it is not an op. Yrs looks
/// the name up in a local registry. Loro derives the container id from the
/// name and the accessor's type, and stores nothing.
pub(crate) fn ensure_text_is_a_pure_lookup_no_change_authored<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.ensure_text("text").unwrap();
    b.ensure_text("text").unwrap();

    assert!(
        a.get_heads().is_empty(),
        "ensure_text must not author a change"
    );
    assert_eq!(a.get_heads(), b.get_heads());
    assert_eq!(a.state_fingerprint(), b.state_fingerprint());
}

/// Without `ensure_text`, two partitioned replicas still merge into one text
/// that holds the inserts from both sides.
///
/// ACCOMMODATION: Yrs and Loro. This is the mirror of
/// `without_bootstrap_partitioned_text_loses_a_side`. A root object has no
/// creation op, so the first `TextSplice` on each side names the same object,
/// and no side can lose the merge.
pub(crate) fn partitioned_text_edits_interleave_even_without_ensure_text<
    A: CrdtAdapter + Default,
>() {
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
        20,
        "no lazy-creation collision to lose a side to"
    );
    assert_eq!(b.text_length("text").unwrap(), 20);
}

/// `ensure_text` fails on a document that already has changes but does not
/// have the object.
///
/// ACCOMMODATION: Automerge only. Automerge's bootstrap must be the first
/// change in the document, and its error says "first change". The trait
/// does not specify this error. Yrs and Loro have no such precondition, so
/// they have no error to test.
pub(crate) fn ensure_text_rejects_non_empty_doc_without_the_object<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();
    let err = a.ensure_text("text").unwrap_err();
    assert!(err.to_string().contains("first change"), "{err}");
}

/// `ensure_text` on a replica that already received the object by sync
/// uses that object and makes no change.
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

/// A replica's first local write to an object that arrived by sync uses
/// that object and does not create a second one.
///
/// This is the same collision in a connected topology. A round-robin
/// writer's first op can arrive just after another node's creation of the
/// object.
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

    // b's first local write. The object is in b's document but not in b's
    // cache. The write must go into that object and not create a new one.
    b.apply_op(&splice("bb")).unwrap();
    sync_until_quiescent(&mut a, "a", &mut b, "b");

    assert_eq!(a.text_length("text").unwrap(), 4, "all four chars survive");
    assert_eq!(b.text_length("text").unwrap(), 4);
}

// ── sync_reset (partition-heal support) ────────────────────────────────

/// After `sync_reset`, `sync_generate` returns a message again for a peer
/// that was up to date.
///
/// When the replicas are quiescent, `sync_generate` returns `None`.
/// `sync_reset` makes the adapter forget the peer's progress. This lets a
/// healed link start sync again without a new stream.
///
/// The test checks the trait contract only. It does not assume how the
/// adapter tracks the peer. Automerge keeps a `sync::State`. Yrs and Loro
/// keep the peer's last known version vector.
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
    // The protocol reset does not change the document.
    assert!(!a.get_heads().is_empty());
}

/// Two replicas converge after a lost message if both call `sync_reset`.
///
/// This is the heal scenario at the adapter layer. A block can occur while a
/// flush is in progress, so a message can be generated and then lost. `a`
/// then believes that `b` has data that `b` never received. Unblocking
/// calls `sync_reset` on both sides, and a new exchange must converge.
pub(crate) fn sync_reset_recovers_from_a_lost_message<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    let mut b = A::default();
    a.apply_op(&map_put("doc", "k", "v")).unwrap();

    // Generated but not delivered. a's per-peer sync state now records
    // these heads as sent.
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

/// `sync_reset` for a peer with no state does nothing and does not panic.
///
/// Unblocking calls it for peers that never sent a message.
pub(crate) fn sync_reset_unknown_peer_is_noop<A: CrdtAdapter + Default>() {
    let mut a = A::default();
    a.sync_reset("never-seen");
    assert!(a.get_heads().is_empty());
}

/// `text_length` fails for a name that was never written and for a name
/// that holds a list.
///
/// ACCOMMODATION: Automerge and Yrs. The test makes two assumptions:
///
/// - A name that was never written is a missing object, not an empty one.
/// - One name holds one type at a time.
///
/// Neither is true for Loro. Loro identifies a root container by
/// `(name, type)`, and every such container exists. See
/// `loro_text_length_of_an_untouched_name_is_zero_not_an_error`.
///
/// The trait requires only that the call fails, not the error text. The
/// check for "not Text" works because both adapters use that wording.
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

/// Converged replicas in a line can have different `save()` bytes.
///
/// ACCOMMODATION: Automerge only. Two replicas with the same heads,
/// fingerprint, and values can produce different `save()` bytes. The
/// encoding keeps changes in the order the replica integrated them. In a
/// topology that is not a full mesh, that order depends on the replica's
/// position in the graph.
///
/// The analysis notebook's doc-size table shows this spread as information,
/// not as an error. If a future Automerge release makes `save()` canonical,
/// this test fails. Then change the table to require equal sizes.
///
/// The other adapters behave differently:
///
/// - Yrs sorts blocks by client id before it writes, so its encoding is
///   canonical. See `yrs_save_bytes_are_canonical_across_converged_replicas`.
/// - Loro's snapshot is canonical except for the encoded width of the random
///   `PeerID`. Sizes differ in 12-16% of trials, so this test would pass or
///   fail at random. See
///   `loro_snapshot_is_canonical_across_converged_replicas_with_pinned_peer_ids`.
///
/// So a spread of doc sizes across replicas has a different cause for each
/// library.
pub(crate) fn save_bytes_not_canonical_across_converged_replicas<A: CrdtAdapter + Default>() {
    let n = 5;
    let mut replicas: Vec<A> = (0..n).map(|_| A::default()).collect();
    converged_line(&mut replicas, 10);

    // The sizes are permitted to differ, and they do. The assertion makes a
    // future change to canonical save() fail this test.
    let sizes: Vec<usize> = replicas.iter_mut().map(A::doc_size_bytes).collect();
    assert!(
        sizes.iter().min() != sizes.iter().max(),
        "save() became canonical across line replicas — sizes: {sizes:?}. \
         Tighten the notebook doc-size table to strict equality.",
    );
}
