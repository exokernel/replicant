//! Runs a trace on targets, times it, and checks the result.
//!
//! # Delivery
//!
//! The driver moves updates between replicas itself. Before an agent applies a
//! transaction, the driver gives the agent the updates of that transaction's
//! ancestors, and no others. A replica thus has the exact state that the trace
//! names. No sync protocol runs. A sync protocol sends all that a peer has, so
//! it cannot give one exact version of a concurrent trace.
//!
//! # Order of one trial
//!
//! 1. Create the replicas and their shared object.
//! 2. Run the setup transactions, and give them to every replica.
//! 3. Run the timed part. See [`Timing`].
//! 4. Encode replica 0, then load the bytes, each with its own timer.
//! 5. Check all content, outside every timer.
//!
//! A failed check is an error. The trial then reports no timing.

use std::time::Instant;

use anyhow::{Context, Result, ensure};

use crate::alloc;
use crate::oracle::{self, CheckPlan, Content};
use crate::spec::{Timing, Workload};
use crate::target::{Lane, Target};
use crate::trace::Trace;

/// The order of the fixed library ids across agents.
///
/// A library breaks ties between concurrent ops with these ids. So this order
/// can change the final text of a trace that has concurrent inserts at one
/// position. Under Yjs and Yrs, the upstream `friendsforever` trace reaches its
/// `endContent` only with ascending ids.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum IdOrder {
    /// Agent 0 has the lowest id.
    #[default]
    Ascending,
    /// Agent 0 has the highest id.
    Descending,
}

impl IdOrder {
    /// Returns the name for the result file.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ascending => "ascending",
            Self::Descending => "descending",
        }
    }

    /// Returns the library id of `replica`, out of `replicas`. All ids differ.
    fn id(self, replica: usize, replicas: usize) -> u32 {
        let position = match self {
            Self::Ascending => replica,
            Self::Descending => replicas - 1 - replica,
        };
        position as u32 + 1
    }
}

/// A trace with its run settings. Build it once, then run many trials.
pub struct Plan {
    /// The workload name.
    pub name: String,
    /// The trace.
    pub trace: Trace,
    /// How many times one agent replays the trace.
    pub repeat: usize,
    /// Replicas that only receive.
    pub receivers: usize,
    /// Leading transactions outside the timer.
    pub setup_txns: usize,
    /// The part of the replay under the timer.
    pub timing: Timing,
    /// Delivers all updates to a replica in one adapter call.
    pub merge_batch: bool,
    /// How many replicas to read and check.
    pub check_replicas: usize,
    /// The final-content check.
    pub check: CheckPlan,
}

impl Plan {
    /// Builds a plan for `trace` with the settings of `workload`.
    pub fn new(workload: &Workload, trace: Trace) -> Result<Self> {
        let replicas = trace.num_agents + workload.receivers;
        let check_replicas = workload.check_replicas.unwrap_or(replicas);
        let check = oracle::plan(&trace, workload.repeat)?;
        ensure!(
            workload.repeat == 1 || (trace.is_single_chain() && workload.receivers == 0),
            "repeat needs one chain by one agent, and no receivers"
        );
        ensure!(
            workload.setup_txns <= trace.txns.len(),
            "setup_txns is larger than the trace"
        );
        ensure!(
            workload.setup_txns == 0 || workload.repeat == 1,
            "repeat cannot have setup transactions"
        );
        ensure!(
            (1..=replicas).contains(&check_replicas),
            "check_replicas must be in 1..={replicas}"
        );
        ensure!(
            workload.timing == Timing::All || check_replicas >= 2,
            "merge_only needs a reference replica and a timed replica"
        );
        Ok(Self {
            name: workload.name.clone(),
            trace,
            repeat: workload.repeat,
            receivers: workload.receivers,
            setup_txns: workload.setup_txns,
            timing: workload.timing,
            merge_batch: workload.merge_batch,
            check_replicas,
            check,
        })
    }

    /// Returns the number of replicas that a trial needs.
    pub fn replica_count(&self) -> usize {
        self.trace.num_agents + self.receivers
    }

    /// Returns the plan for a file with no spec: all agents, all timed, and all
    /// replicas checked.
    pub fn ad_hoc(name: &str, trace: Trace) -> Result<Self> {
        let check = oracle::plan(&trace, 1)?;
        let check_replicas = trace.num_agents;
        Ok(Self {
            name: name.to_owned(),
            trace,
            repeat: 1,
            receivers: 0,
            setup_txns: 0,
            timing: Timing::All,
            merge_batch: false,
            check_replicas,
            check,
        })
    }
}

/// What one trial measured.
#[derive(Debug, Clone, Copy, Default)]
pub struct TrialResult {
    /// The timed part, in milliseconds. It includes `read_ms`.
    pub time_ms: f64,
    /// The final reads inside the timed part, in milliseconds.
    pub read_ms: f64,
    /// The time to encode replica 0, in milliseconds.
    pub encode_ms: f64,
    /// The length of the encoded document, in bytes.
    pub doc_size: usize,
    /// The time to load the encoded document and read it, in milliseconds.
    pub parse_ms: f64,
    /// The bytes of all updates that the trial made.
    pub update_bytes: usize,
    /// The number of non-empty updates that the trial made.
    pub update_count: usize,
    /// The adapter time in the timed part, in milliseconds. `None` if the
    /// target has no transport to separate from the adapter.
    pub adapter_ms: Option<f64>,
    /// The peak of live bytes during the replay. Only a memory trial sets this.
    pub peak_bytes: Option<i64>,
    /// The live bytes that the loaded document holds. Only a memory trial sets
    /// this.
    pub retained_bytes: Option<i64>,
}

/// The replicas of one trial, and what each one has seen.
struct Trial<'a> {
    /// The plan that this trial runs.
    plan: &'a Plan,
    /// The replicas.
    targets: Vec<Box<dyn Target>>,
    /// For each replica, which transactions it has.
    seen: Vec<Vec<bool>>,
    /// The update of each transaction. Empty if nobody needs it.
    updates: Vec<Vec<u8>>,
    /// The bytes of all updates so far.
    update_bytes: usize,
    /// The number of non-empty updates so far.
    update_count: usize,
}

impl Trial<'_> {
    /// Applies transaction `i` on its agent, after the agent has its ancestors.
    fn apply(&mut self, i: usize, retain: bool) -> Result<()> {
        let txn = &self.plan.trace.txns[i];
        if retain {
            self.deliver_ancestors(txn.agent, i)?;
        }
        let update = self.targets[txn.agent]
            .apply_txn(&txn.ops)
            .with_context(|| format!("txn {i}"))?;
        self.update_bytes += update.len();
        self.update_count += usize::from(!update.is_empty());
        if retain {
            self.seen[txn.agent][i] = true;
            self.updates[i] = update;
        }
        Ok(())
    }

    /// Gives `replica` the ancestors of transaction `i` that it does not have,
    /// in index order. A parent always has a lower index than its child, so
    /// index order is causal order.
    fn deliver_ancestors(&mut self, replica: usize, i: usize) -> Result<()> {
        let mut missing = Vec::new();
        let mut stack: Vec<usize> = self.plan.trace.txns[i].parents.clone();
        let mut queued = std::collections::HashSet::new();
        while let Some(t) = stack.pop() {
            if self.seen[replica][t] || !queued.insert(t) {
                continue;
            }
            missing.push(t);
            stack.extend(&self.plan.trace.txns[t].parents);
        }
        missing.sort_unstable();
        self.deliver(replica, &missing, false)
    }

    /// Gives `replica` all transactions that it does not have.
    fn settle(&mut self, replica: usize) -> Result<()> {
        let missing: Vec<usize> = (0..self.plan.trace.txns.len())
            .filter(|&t| !self.seen[replica][t])
            .collect();
        self.deliver(replica, &missing, self.plan.merge_batch)
    }

    /// Gives `replica` the updates of `txns`, in order. With `batch`, they go in
    /// one adapter call. Marks each transaction as seen.
    fn deliver(&mut self, replica: usize, txns: &[usize], batch: bool) -> Result<()> {
        let updates: Vec<Vec<u8>> = txns
            .iter()
            .filter(|&&t| !self.updates[t].is_empty())
            .map(|&t| self.updates[t].clone())
            .collect();
        if batch {
            if !updates.is_empty() {
                self.targets[replica].apply_updates(&updates)?;
            }
        } else {
            for update in &updates {
                self.targets[replica].apply_updates(std::slice::from_ref(update))?;
            }
        }
        for &t in txns {
            self.seen[replica][t] = true;
        }
        Ok(())
    }

    /// Reads the replicas to check. Returns their content.
    fn read_checked(&mut self) -> Result<Vec<Content>> {
        let kind = self.plan.trace.kind;
        self.targets[..self.plan.check_replicas]
            .iter_mut()
            .map(|t| t.read(kind))
            .collect()
    }
}

/// Runs one trial on `lane`. Returns an error if any check fails.
///
/// `memory` turns on the allocation count. The times of such a trial are not
/// valid measurements.
pub fn run_trial(plan: &Plan, lane: &Lane, ids: IdOrder, memory: bool) -> Result<TrialResult> {
    let replicas = plan.replica_count();
    let trace = &plan.trace;
    let mut targets = lane.acquire(replicas)?;
    for (r, t) in targets.iter_mut().enumerate() {
        t.set_instance_id(ids.id(r, replicas))?;
        t.ensure(trace.kind)?;
    }
    let n = trace.txns.len();
    // A trace with one replica needs no delivery. Then it keeps no updates, and
    // memory stays at the document's own use.
    let retain = replicas > 1;
    let mut trial = Trial {
        plan,
        targets,
        seen: vec![vec![false; if retain { n } else { 0 }]; replicas],
        updates: vec![Vec::new(); if retain { n } else { 0 }],
        update_bytes: 0,
        update_count: 0,
    };

    if memory {
        alloc::set_counting(true);
        alloc::reset_peak();
    }

    // Setup: outside the timer. Every replica gets these transactions.
    for i in 0..plan.setup_txns {
        trial.apply(i, retain)?;
    }
    for r in 0..replicas {
        let setup: Vec<usize> = (0..plan.setup_txns)
            .filter(|&t| !trial.seen[r][t])
            .collect();
        trial.deliver(r, &setup, false)?;
    }
    for t in &mut trial.targets {
        t.take_adapter_ns();
    }

    let (time_ms, read_ms, contents) = match plan.timing {
        Timing::All => {
            let t0 = Instant::now();
            for _ in 0..plan.repeat {
                for i in plan.setup_txns..n {
                    trial.apply(i, retain)?;
                }
            }
            if retain {
                for r in 0..plan.check_replicas {
                    trial.settle(r)?;
                }
            }
            let t_read = Instant::now();
            let contents = trial.read_checked()?;
            (ms(t0.elapsed()), ms(t_read.elapsed()), contents)
        }
        Timing::MergeOnly => {
            for i in plan.setup_txns..n {
                trial.apply(i, retain)?;
            }
            trial.settle(0)?;
            for t in &mut trial.targets {
                t.take_adapter_ns();
            }
            let t0 = Instant::now();
            for r in 1..plan.check_replicas {
                trial.settle(r)?;
            }
            let t_read = Instant::now();
            let contents = trial.read_checked()?;
            (ms(t0.elapsed()), ms(t_read.elapsed()), contents)
        }
    };
    let adapter_ns: Option<u64> = trial.targets.iter_mut().map(|t| t.take_adapter_ns()).sum();
    let peak_bytes = memory.then(alloc::peak);

    // Encode and load. A trial keeps the update list until here, as the
    // crdt-benchmarks harness does. Free it before the memory baseline.
    let (update_bytes, update_count) = (trial.update_bytes, trial.update_count);
    trial.updates = Vec::new();
    trial.seen = Vec::new();

    let t_encode = Instant::now();
    let encoded = trial.targets[0].encode()?;
    let encode_ms = ms(t_encode.elapsed());
    let doc_size = encoded.len();

    // Free the writers. A target resets outside the timer, so the parse time
    // has no cost from dropping the old document.
    for t in &mut trial.targets {
        t.reset()?;
    }
    let baseline = memory.then(alloc::current);
    let t_parse = Instant::now();
    trial.targets[0].load(&encoded)?;
    let loaded = trial.targets[0].read(trace.kind)?;
    let parse_ms = ms(t_parse.elapsed());
    let retained_bytes = baseline.map(|base| {
        let bytes = content_bytes(&loaded);
        alloc::current() - base - bytes
    });
    if memory {
        alloc::set_counting(false);
    }

    // Checks: outside every timer.
    for (r, content) in contents.iter().enumerate() {
        plan.check
            .check(content)
            .with_context(|| format!("replica {r}"))?;
        ensure!(
            content == &contents[0],
            "replica {r} differs from replica 0"
        );
    }
    ensure!(
        loaded == contents[0],
        "the loaded document differs from replica 0"
    );

    Ok(TrialResult {
        time_ms,
        read_ms,
        encode_ms,
        doc_size,
        parse_ms,
        update_bytes,
        update_count,
        adapter_ms: adapter_ns.map(|ns| ns as f64 / 1e6),
        peak_bytes,
        retained_bytes,
    })
}

/// Converts a duration to milliseconds.
fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Returns an estimate of the heap bytes in `content`, so that a memory figure
/// excludes the read-back copy.
fn content_bytes(content: &Content) -> i64 {
    match content {
        Content::Text(s) => s.capacity() as i64,
        Content::List(v) => (v.capacity() * std::mem::size_of::<common::ScalarVal>()) as i64,
        Content::Map(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use replica::adapter::Crdt;

    /// Runs `json` with `workload` on every library, in process.
    fn run_all(json: &str, workload: Workload) -> Vec<TrialResult> {
        Crdt::ALL
            .iter()
            .map(|&crdt| {
                let trace = Trace::parse(json.as_bytes()).unwrap();
                let plan = Plan::new(&workload, trace).unwrap();
                run_trial(&plan, &Lane::InProcess(crdt), IdOrder::Ascending, false)
                    .unwrap_or_else(|e| panic!("{crdt}: {e:#}"))
            })
            .collect()
    }

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
            let plan = Plan::new(&workload(), trace).unwrap();
            let e =
                run_trial(&plan, &Lane::InProcess(crdt), IdOrder::Ascending, false).unwrap_err();
            assert!(format!("{e:#}").contains("final content is wrong"), "{e:#}");
        }
    }

    #[test]
    fn a_memory_trial_runs_without_the_allocator_installed() {
        let trace =
            Trace::parse(br#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#).unwrap();
        let plan = Plan::new(&workload(), trace).unwrap();
        let r = run_trial(&plan, &Lane::InProcess(Crdt::Yrs), IdOrder::Ascending, true).unwrap();
        assert!(r.peak_bytes.is_some());
        assert!(r.retained_bytes.is_some());
    }

    /// A fixed id order gives the same encoded size on each run. Random ids
    /// change the size, because the encoded id width can change.
    #[test]
    fn a_fixed_id_order_repeats_the_document_size() {
        let json = br#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
            "txns":[
            {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
            {"agent":1,"parents":[],"patches":[[0,0,"b"]]}]}"#;
        for crdt in Crdt::ALL {
            let size = |ids| {
                let plan = Plan::new(&workload(), Trace::parse(json).unwrap()).unwrap();
                run_trial(&plan, &Lane::InProcess(crdt), ids, false)
                    .unwrap()
                    .doc_size
            };
            assert_eq!(size(IdOrder::Ascending), size(IdOrder::Ascending), "{crdt}");
        }
    }

    #[test]
    fn plan_rejects_repeat_with_a_receiver() {
        let trace =
            Trace::parse(br#"{"endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#).unwrap();
        let bad = Workload {
            repeat: 2,
            receivers: 1,
            ..workload()
        };
        assert!(Plan::new(&bad, trace).is_err());
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
            let plan = Plan::new(&workload(), trace).unwrap();
            let r = run_trial(&plan, &lane, IdOrder::Ascending, false).unwrap();
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
            &Workload {
                receivers: 1,
                ..workload()
            },
            trace,
        )
        .unwrap();
        run_trial(&plan, &lane, IdOrder::Ascending, false).unwrap();
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
}
