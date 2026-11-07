//! Tests for the driver: every workload shape, on every library and lane.

use super::*;
use replica::adapter::Crdt;

/// Runs `json` with `workload` on every library, in process.
fn run_all(json: &str, workload: Workload) -> Vec<TrialResult> {
    Crdt::ALL
        .iter()
        .map(|&crdt| {
            let trace = Trace::parse(json.as_bytes()).unwrap();
            let plan = Plan::new(workload.clone(), trace).unwrap();
            run_trial(&plan, &Lane::InProcess(crdt), Ids::ASCENDING)
                .unwrap_or_else(|e| panic!("{crdt}: {e:#}"))
        })
        .collect()
}

/// Returns a workload with default settings.
fn workload() -> Workload {
    Workload {
        name: "t".into(),
        description: String::new(),
        trace: "t".into(),
        sha256: String::new(),
        repeat: 1,
        receivers: 0,
        setup_txns: 0,
        timing: Timing::All,
        merge_batch: false,
        check_replicas: None,
        heavy: false,
        id_order: None,
    }
}

#[test]
fn a_single_writer_replays_with_deletes() {
    let results = run_all(
        r#"{"endContent":"hippo","txns":[
            {"patches":[[0,0,"hello"]]},{"patches":[[1,3,"ipp"]]}]}"#,
        workload(),
    );
    for r in results {
        assert!(r.doc_size > 0);
        assert_eq!(r.update_count, 2);
    }
}

#[test]
fn a_receiver_gets_every_update() {
    run_all(
        r#"{"endContent":"abc","txns":[
            {"patches":[[0,0,"a"]]},{"patches":[[1,0,"x"]]},
            {"patches":[[1,1,"bc"]]}]}"#,
        Workload {
            receivers: 1,
            ..workload()
        },
    );
}

#[test]
fn the_example_in_the_editing_traces_readme_replays() {
    run_all(
        r#"{"kind":"concurrent","endContent":"yoooo ho ho\n","numAgents":2,"txns":[
            {"parents":[],"numChildren":1,"agent":0,"time":"t",
             "patches":[[0,0,"hi there\n"]]},
            {"parents":[0],"numChildren":1,"agent":0,"time":"t",
             "patches":[[0,8,""],[0,0,"yoooo"]]},
            {"parents":[1],"numChildren":0,"agent":1,"time":"t",
             "patches":[[5,0," ho ho"]]}]}"#,
        workload(),
    );
}

/// A merge with no same-position inserts, as the upstream rules require.
#[test]
fn an_upstream_style_merge_replays_to_the_exact_text() {
    run_all(
        r#"{"kind":"concurrent","endContent":"AxyzB!","numAgents":2,"txns":[
            {"agent":0,"parents":[],"patches":[[0,0,"xyz"]]},
            {"agent":0,"parents":[0],"patches":[[0,0,"A"]]},
            {"agent":1,"parents":[0],"patches":[[3,0,"B"]]},
            {"agent":0,"parents":[1,2],"patches":[[5,0,"!"]]}]}"#,
        workload(),
    );
}

/// B2 as crdt-benchmarks runs it. The second batch follows the first.
#[test]
fn sequential_batches_give_one_exact_text() {
    run_all(
        r#"{"kind":"concurrent","endContent":"BBAAbase","numAgents":2,"txns":[
            {"agent":0,"parents":[],"patches":[[0,0,"base"]]},
            {"agent":0,"parents":[0],"patches":[[0,0,"A"],[1,0,"A"]]},
            {"agent":1,"parents":[1],"patches":[[0,0,"B"],[1,0,"B"]]}]}"#,
        Workload {
            setup_txns: 1,
            ..workload()
        },
    );
}

/// The same ops with real concurrency. The two batches fork from the setup.
#[test]
fn concurrent_batches_pass_the_tree_check() {
    run_all(
        r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,"txns":[
            {"agent":0,"parents":[],"patches":[[0,0,"base"]]},
            {"agent":0,"parents":[0],"patches":[[0,0,"A"],[1,0,"A"]]},
            {"agent":1,"parents":[0],"patches":[[0,0,"B"],[1,0,"B"],[2,1,""]]}]}"#,
        Workload {
            setup_txns: 1,
            ..workload()
        },
    );
}

/// B3's shape: many writers, one op each, merged on two replicas only.
#[test]
fn a_fan_in_of_concurrent_inserts_merges_for_text_and_list() {
    let text: Vec<String> = (0..8)
        .map(|i| format!(r#"{{"agent":{i},"parents":[],"patches":[[0,0,"{i}"]]}}"#))
        .collect();
    let list: Vec<String> = (0..8)
        .map(|i| format!(r#"{{"agent":{i},"parents":[],"listOps":[[0,0,["{i}"]]]}}"#))
        .collect();
    for txns in [text, list] {
        run_all(
            &format!(
                r#"{{"kind":"concurrent","oracle":{{"kind":"tree_invariants"}},
                "numAgents":8,"txns":[{}]}}"#,
                txns.join(",")
            ),
            Workload {
                timing: Timing::MergeOnly,
                merge_batch: true,
                check_replicas: Some(2),
                ..workload()
            },
        );
    }
}

#[test]
fn concurrent_map_writes_leave_one_written_value() {
    run_all(
        r#"{"kind":"concurrent","oracle":{"kind":"map_winner"},"numAgents":3,"txns":[
            {"agent":0,"parents":[],"mapOps":[["v",0]]},
            {"agent":1,"parents":[],"mapOps":[["v",1]]},
            {"agent":2,"parents":[],"mapOps":[["v","two"]]}]}"#,
        Workload {
            timing: Timing::MergeOnly,
            merge_batch: true,
            check_replicas: Some(2),
            ..workload()
        },
    );
}

/// The shape of the future conflict sweep: N replicas, a shared history,
/// offline divergence with tunable same-position inserts, then one merge.
/// This test shows that the format expresses it on every library.
#[test]
fn the_conflict_sweep_shape_is_expressible() {
    let mut txns =
        vec![r#"{"agent":0,"parents":[],"patches":[[0,0,"shared history"]]}"#.to_owned()];
    for agent in 0..3 {
        let contested = r#"[0,0,"s"],[0,0,"s"]"#;
        let own = r#"[5,0,"o"],[6,1,""]"#;
        let parent = 0;
        txns.push(format!(
            r#"{{"agent":{agent},"parents":[{parent}],"patches":[{contested},{own}]}}"#
        ));
    }
    // Agent 0's first transaction is the shared root, so the root's own
    // agent continues from it. The other agents fork from the root.
    run_all(
        &format!(
            r#"{{"kind":"concurrent","oracle":{{"kind":"tree_invariants"}},
            "numAgents":3,"txns":[{}]}}"#,
            txns.join(",")
        ),
        Workload {
            setup_txns: 1,
            ..workload()
        },
    );
}

#[test]
fn repeat_replays_on_top_of_the_last_pass() {
    run_all(
        r#"{"endContent":"ab","txns":[{"patches":[[0,0,"a"]]},{"patches":[[1,0,"b"]]}]}"#,
        Workload {
            repeat: 3,
            ..workload()
        },
    );
}

/// A wrong final text must fail the trial, on every library.
#[test]
fn a_wrong_final_text_fails_the_trial() {
    for crdt in Crdt::ALL {
        let trace =
            Trace::parse(br#"{"endContent":"not this","txns":[{"patches":[[0,0,"abc"]]}]}"#)
                .unwrap();
        let plan = Plan::new(workload(), trace).unwrap();
        let e = run_trial(&plan, &Lane::InProcess(crdt), Ids::ASCENDING).unwrap_err();
        assert!(format!("{e:#}").contains("final content is wrong"), "{e:#}");
    }
}

#[test]
fn a_memory_trial_runs_without_the_allocator_installed() {
    let trace = Trace::parse(br#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#).unwrap();
    let plan = Plan::new(workload(), trace).unwrap();
    run_memory_trial(&plan, &Lane::InProcess(Crdt::Yrs), Ids::ASCENDING).unwrap();
}

/// Each id order gives the same encoded size on each run. Unseeded random ids
/// can change the size, because the encoded id width can change.
#[test]
fn each_id_order_repeats_the_document_size() {
    let json = br#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
        "txns":[
        {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
        {"agent":1,"parents":[],"patches":[[0,0,"b"]]}]}"#;
    for crdt in Crdt::ALL {
        let size = |ids| {
            let plan = Plan::new(workload(), Trace::parse(json).unwrap()).unwrap();
            run_trial(&plan, &Lane::InProcess(crdt), ids)
                .unwrap()
                .doc_size
        };
        assert_eq!(size(Ids::ASCENDING), size(Ids::ASCENDING), "{crdt}");
        assert_eq!(size(Ids::SEEDED), size(Ids::SEEDED), "{crdt}");
    }
}

#[test]
fn fixed_orders_give_ids_in_order() {
    let positions = |order| {
        let ids = Ids { order, seed: 1 }.all(3).unwrap();
        ids.iter().map(|id| id - FIXED_ID_BASE).collect::<Vec<_>>()
    };
    assert_eq!(positions(IdOrder::Ascending), [1, 2, 3]);
    assert_eq!(positions(IdOrder::Descending), [3, 2, 1]);
}

/// B3 has 1540 clients, so the seeded ids must stay distinct at that scale, in
/// the bits that every adapter keeps.
#[test]
fn seeded_ids_repeat_for_a_seed_and_change_with_the_seed() {
    let ids = |seed| {
        Ids {
            order: IdOrder::Seeded,
            seed,
        }
        .all(1540)
        .unwrap()
    };
    assert_eq!(ids(1), ids(1));
    assert_ne!(ids(1), ids(2));
    assert!(
        ids(1).iter().all(|id| id >> 64 != 0),
        "ids use the high bits"
    );
}

/// The default order is seeded. A workload can pin another order, and a request
/// overrides both.
#[test]
fn a_requested_id_order_overrides_the_workload() {
    let plan = |pinned| {
        let trace = Trace::parse(br#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#);
        Plan::new(
            Workload {
                id_order: pinned,
                ..workload()
            },
            trace.unwrap(),
        )
        .unwrap()
    };
    assert_eq!(plan(None).id_order(None), IdOrder::Seeded);
    let pinned = plan(Some(IdOrder::Ascending));
    assert_eq!(pinned.id_order(None), IdOrder::Ascending);
    assert_eq!(
        pinned.id_order(Some(IdOrder::Descending)),
        IdOrder::Descending
    );
}

/// Setup transactions on a lone replica need no delivery.
#[test]
fn setup_works_with_one_replica() {
    run_all(
        r#"{"endContent":"ab","txns":[{"patches":[[0,0,"a"]]},{"patches":[[1,0,"b"]]}]}"#,
        Workload {
            setup_txns: 1,
            ..workload()
        },
    );
}

#[test]
fn plan_rejects_repeat_with_a_receiver() {
    let trace = Trace::parse(br#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#).unwrap();
    let bad = Workload {
        repeat: 2,
        receivers: 1,
        ..workload()
    };
    assert!(Plan::new(bad, trace).is_err());
}

/// Two root inserts are concurrent. The setup broadcast would give the second
/// writer the first insert before its own, so the plan refuses the workload.
#[test]
fn plan_rejects_a_txn_that_does_not_follow_the_setup() {
    let trace = Trace::parse(
        br#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,"txns":[
            {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
            {"agent":1,"parents":[],"patches":[[0,0,"b"]]}]}"#,
    )
    .unwrap();
    let setup = Workload {
        setup_txns: 1,
        ..workload()
    };
    let err = Plan::new(setup, trace).err().expect("the plan must fail");
    assert!(
        format!("{err:#}").contains("txn 1 does not follow"),
        "{err:#}"
    );
}

/// A later txn must follow every setup txn, not only the last one. Txn 2 follows
/// txn 1 but not txn 0, so a setup of two is refused, and a setup of one is not.
#[test]
fn every_setup_txn_must_be_an_ancestor() {
    let json = br#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,"txns":[
        {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
        {"agent":1,"parents":[],"patches":[[0,0,"b"]]},
        {"agent":1,"parents":[1],"patches":[[0,0,"c"]]}]}"#;
    let plan = |setup_txns| {
        Plan::new(
            Workload {
                setup_txns,
                ..workload()
            },
            Trace::parse(json).unwrap(),
        )
    };
    assert!(plan(2).is_err());
    assert_eq!(
        first_txn_outside_setup(&Trace::parse(json).unwrap(), 1),
        Some(1)
    );
    assert!(plan(0).is_ok());
}

/// The check uses one bit for each setup txn, so it must work past 64 of them.
#[test]
fn the_setup_check_works_past_64_setup_txns() {
    // A chain of 70 txns by agent 0. Txn 70 is by agent 1 and follows txn 63 only.
    let mut txns: Vec<String> = (0..70)
        .map(|i| {
            let parents = if i == 0 {
                String::new()
            } else {
                (i - 1).to_string()
            };
            format!(r#"{{"agent":0,"parents":[{parents}],"patches":[[0,0,"x"]]}}"#)
        })
        .collect();
    txns.push(r#"{"agent":1,"parents":[63],"patches":[[0,0,"y"]]}"#.into());
    let json = format!(
        r#"{{"kind":"concurrent","oracle":{{"kind":"tree_invariants"}},"numAgents":2,"txns":[{}]}}"#,
        txns.join(",")
    );
    let trace = Trace::parse(json.as_bytes()).unwrap();
    assert_eq!(first_txn_outside_setup(&trace, 64), None);
    assert_eq!(first_txn_outside_setup(&trace, 65), Some(70));
    assert_eq!(first_txn_outside_setup(&trace, 70), Some(70));
}

/// The remote lane: a real replica behind gRPC runs the same plan, and the
/// result row also gives the adapter time.
#[test]
fn the_remote_lane_runs_the_same_plan() {
    for crdt in Crdt::ALL {
        let addr = spawn_replica(crdt);
        let lane = Lane::connect(crdt, std::slice::from_ref(&addr)).unwrap();
        let trace = Trace::parse(
            br#"{"endContent":"hippo","txns":[
                {"patches":[[0,0,"hello"]]},{"patches":[[1,3,"ipp"]]}]}"#,
        )
        .unwrap();
        let plan = Plan::new(workload(), trace).unwrap();
        let r = run_trial(&plan, &lane, Ids::ASCENDING).unwrap();
        assert!(r.adapter_ms.is_some_and(|a| a > 0.0 && a <= r.time_ms));
        assert!(r.doc_size > 0);
    }
}

#[test]
fn the_remote_lane_moves_updates_between_replicas() {
    let crdt = Crdt::Loro;
    let addrs = [spawn_replica(crdt), spawn_replica(crdt)];
    let lane = Lane::connect(crdt, &addrs).unwrap();
    let trace = Trace::parse(
        br#"{"endContent":"ab","txns":[{"patches":[[0,0,"a"]]},{"patches":[[1,0,"b"]]}]}"#,
    )
    .unwrap();
    let plan = Plan::new(
        Workload {
            receivers: 1,
            ..workload()
        },
        trace,
    )
    .unwrap();
    run_trial(&plan, &lane, Ids::ASCENDING).unwrap();
}

#[test]
fn the_remote_lane_refuses_a_plan_with_too_many_replicas() {
    let crdt = Crdt::Yrs;
    let addr = spawn_replica(crdt);
    let lane = Lane::connect(crdt, std::slice::from_ref(&addr)).unwrap();
    assert!(lane.acquire(2).is_err());
    assert_eq!(lane.capacity(), Some(1));
}

/// Starts a replica on a loopback port, on its own thread and runtime, for
/// the rest of the test process.
fn spawn_replica(crdt: Crdt) -> String {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            tx.send(listener.local_addr().unwrap().to_string()).unwrap();
            let state = replica::server::ReplicaState::new(
                common::NodeId::new("node-0").unwrap(),
                crdt.build(),
            );
            replica::server::router(state)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
    });
    rx.recv().unwrap()
}
