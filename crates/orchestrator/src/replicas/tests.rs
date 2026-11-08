//! Tests for the replica helpers. They run real in-process replicas over gRPC.

use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use replica::adapter::Crdt;

use crate::event_log::EventLog;

use super::*;

/// Spawns two in-process Automerge replicas.
async fn two_nodes(tasks: &mut JoinSet<()>) -> (Vec<ReplicaEndpoint>, Vec<Client>) {
    spawn_nodes(2, Crdt::Automerge, tasks).await.unwrap()
}

/// Waits for nodes 0 and 1 to agree, with a fresh 5 s deadline.
async fn wait_for_pair(clients: &mut [Client]) {
    wait_for_nodes(
        clients,
        &[0, 1],
        Instant::now(),
        Duration::from_secs(5),
        &mut EventLog::off(),
    )
    .await
    .unwrap();
}

// ── node names ─────────────────────────────────────────────────────────

#[test]
fn node_index_inverts_node_name() {
    for i in [0, 7, 12] {
        assert_eq!(node_index(&node_name(i)), Some(i));
    }
    assert_eq!(node_index("replica-1"), None);
    assert_eq!(node_index("node-x"), None);
}

// ── acquire_nodes ──────────────────────────────────────────────────────

/// An external stack larger than the scenario serves it: the scenario gets
/// the first `n` replicas, and the rest are reset.
#[tokio::test]
async fn external_source_uses_the_first_n_and_resets_the_rest() {
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = spawn_nodes(3, Crdt::Automerge, &mut tasks).await.unwrap();
    map_put(&mut clients[2], "k", "v").await.unwrap();
    assert!(!fingerprint(&mut clients[2]).await.unwrap().is_empty());

    let mut unused_tasks = JoinSet::new();
    let (got, _) = acquire_nodes(
        NodeSource::External(endpoints.clone()),
        2,
        &mut unused_tasks,
    )
    .await
    .unwrap();

    assert_eq!(got, endpoints[..2]);
    assert!(
        fingerprint(&mut clients[2]).await.unwrap().is_empty(),
        "the unused replica was not reset"
    );
}

#[tokio::test]
async fn external_source_with_too_few_replicas_fails() {
    let mut tasks = JoinSet::new();
    let (endpoints, _) = two_nodes(&mut tasks).await;
    let err = acquire_nodes(NodeSource::External(endpoints), 3, &mut JoinSet::new())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("only 2"), "{err}");
}

// ── check_tasks ────────────────────────────────────────────────────────

#[tokio::test]
async fn check_tasks_ok_when_all_running() {
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn(std::future::pending());
    assert!(check_tasks(&mut tasks).is_ok());
    tasks.abort_all();
}

#[tokio::test]
async fn check_tasks_errors_on_panic() {
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn(async { panic!("boom") });
    // Sleep so tokio schedules the task; the panic is caught as a JoinError
    // and stored in the JoinSet. We must not call join_next here — that
    // would consume the result before check_tasks can see it.
    tokio::time::sleep(Duration::from_millis(10)).await;
    let err = check_tasks(&mut tasks).unwrap_err();
    assert!(err.to_string().contains("panicked"), "{err}");
}

#[tokio::test]
async fn check_tasks_errors_on_unexpected_exit() {
    let mut tasks: JoinSet<()> = JoinSet::new();
    tasks.spawn(std::future::ready(()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    let err = check_tasks(&mut tasks).unwrap_err();
    assert!(err.to_string().contains("unexpectedly"), "{err}");
}

#[test]
fn node_name_matches_the_statefulset_pod_names() {
    assert_eq!(node_name(0), "node-0");
    assert_eq!(node_name(12), "node-12");
}

// ── Reset ──────────────────────────────────────────────────────────────

/// Drives `Replica.Reset` over the wire: write, converge, reset, check the
/// fingerprint is empty, then re-wire and write again.
///
/// This is the sequence the orchestrator runs at the start of every trial
/// against external replicas. It shows the second trial starts clean.
#[tokio::test]
async fn reset_clears_state_and_allows_subsequent_run() {
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = two_nodes(&mut tasks).await;
    let edges = vec![(0, 1)];

    // Trial 1: write something and converge.
    connect_edges(&clients, &endpoints, &edges).await.unwrap();
    for i in 0..5 {
        map_put(&mut clients[0], &format!("trial1_k{i}"), &format!("v{i}"))
            .await
            .unwrap();
    }
    wait_for_pair(&mut clients).await;

    let fp_before = fingerprint(&mut clients[0]).await.unwrap();
    assert!(
        !fp_before.is_empty(),
        "fingerprint must be non-empty after writes"
    );

    // Reset every replica back to empty.
    reset_all(&clients).await.unwrap();

    for (i, client) in clients.iter_mut().enumerate() {
        let fp = fingerprint(client).await.unwrap();
        assert!(
            fp.is_empty(),
            "node {i} fingerprint must be empty after reset"
        );
    }

    // Trial 2: re-wire peers (Reset cleared peer_txs) and write *different*
    // data. Convergence here only succeeds if the previous trial's sync
    // state was fully discarded — a stale per-peer sync::State would leave
    // the new handshake stuck or produce a divergent fingerprint.
    connect_edges(&clients, &endpoints, &edges).await.unwrap();
    for i in 0..3 {
        map_put(&mut clients[0], &format!("trial2_k{i}"), &format!("w{i}"))
            .await
            .unwrap();
    }
    wait_for_pair(&mut clients).await;

    let fp_after_a = fingerprint(&mut clients[0]).await.unwrap();
    let fp_after_b = fingerprint(&mut clients[1]).await.unwrap();
    assert_eq!(fp_after_a, fp_after_b, "post-reset trial must converge");
    assert_ne!(
        fp_after_a, fp_before,
        "post-reset fingerprint must reflect the new writes, not the prior trial",
    );

    check_tasks(&mut tasks).unwrap();
}

/// Reset on a fresh replica: no peers, empty document. The orchestrator
/// resets before every trial, including the first, so this case must not
/// deadlock or panic.
#[tokio::test]
async fn reset_is_noop_on_fresh_replica() {
    let mut tasks = JoinSet::new();
    let (_endpoints, mut clients) = two_nodes(&mut tasks).await;
    reset_all(&clients).await.unwrap();
    for client in clients.iter_mut() {
        assert!(fingerprint(client).await.unwrap().is_empty());
    }
    check_tasks(&mut tasks).unwrap();
}

// ── Relay across non-mesh topologies ───────────────────────────────────

/// Convergence across a 4-node line (0↔1↔2↔3) with every write at node 0.
///
/// Nodes 2 and 3 have no link to the writer. They can only learn its
/// changes if `recv_loop` forwards received state onward, so this passes
/// only when relaying works.
#[tokio::test]
async fn line_topology_n4_converges_with_relay() {
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = spawn_nodes(4, Crdt::Automerge, &mut tasks).await.unwrap();

    let edges = vec![(0, 1), (1, 2), (2, 3)];
    connect_edges(&clients, &endpoints, &edges).await.unwrap();
    check_tasks(&mut tasks).unwrap();

    let measure_start = Instant::now();
    for i in 0..4 {
        map_put(&mut clients[0], &format!("k{i}"), &format!("v{i}"))
            .await
            .unwrap();
    }
    check_tasks(&mut tasks).unwrap();

    wait_for_nodes(
        &mut clients,
        &[0, 1, 2, 3],
        measure_start,
        Duration::from_secs(5),
        &mut EventLog::off(),
    )
    .await
    .unwrap();
    check_tasks(&mut tasks).unwrap();
}

// ── App-layer partition ────────────────────────────────────────────────

/// A blocked link carries nothing, even though its stream is open and both
/// replicas are writing.
///
/// This is what lets the harness wire the whole graph before the partition.
/// If a blocked link leaked, the groups would merge at once and the heal
/// would measure nothing.
#[tokio::test]
async fn blocked_link_carries_no_state_while_open() {
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = two_nodes(&mut tasks).await;
    let edges = vec![(0, 1)];

    // Block first, then wire: the stream opens without a handshake.
    set_links_blocked(&clients, &edges, true).await.unwrap();
    connect_edges(&clients, &endpoints, &edges).await.unwrap();

    map_put(&mut clients[0], "from_a", "1").await.unwrap();
    map_put(&mut clients[1], "from_b", "2").await.unwrap();

    // Give any leak a generous chance to land before asserting.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let fp_a = fingerprint(&mut clients[0]).await.unwrap();
    let fp_b = fingerprint(&mut clients[1]).await.unwrap();
    assert!(
        !fp_a.is_empty() && !fp_b.is_empty(),
        "both must have written"
    );
    assert_ne!(fp_a, fp_b, "blocked link leaked state across the partition");

    check_tasks(&mut tasks).unwrap();
}

/// An unblock plus a kick converges an already-wired link.
///
/// This is the heal path through the real gRPC stack. No connection is
/// opened, so the time here is the sync protocol alone.
#[tokio::test]
async fn unblock_and_kick_heals_without_reconnecting() {
    let mut tasks = JoinSet::new();
    let (endpoints, mut clients) = two_nodes(&mut tasks).await;
    let edges = vec![(0, 1)];

    set_links_blocked(&clients, &edges, true).await.unwrap();
    connect_edges(&clients, &endpoints, &edges).await.unwrap();
    map_put(&mut clients[0], "from_a", "1").await.unwrap();
    map_put(&mut clients[1], "from_b", "2").await.unwrap();

    set_links_blocked(&clients, &edges, false).await.unwrap();
    kick_sync_edges(&clients, &edges).await.unwrap();

    wait_for_pair(&mut clients).await;
    check_tasks(&mut tasks).unwrap();
}
