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

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;

use crate::alloc;
use crate::oracle::{CheckPlan, Content};
use crate::spec::{Timing, Workload};
use crate::target::{Lane, Target};
use crate::trace::Trace;

/// The order of the fixed library ids across agents.
///
/// A library breaks ties between concurrent ops with these ids. So this order
/// can change the final text of a trace that has concurrent inserts at one
/// position. Under Yjs and Yrs, the upstream `friendsforever` trace reaches its
/// `endContent` only with ascending ids.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum IdOrder {
    /// Agent 0 has the lowest id.
    #[default]
    Ascending,
    /// Agent 0 has the highest id.
    Descending,
}

impl IdOrder {
    /// Returns the library id of `replica`, out of `replicas`. All ids differ.
    fn id(self, replica: usize, replicas: usize) -> Result<u32> {
        let position = match self {
            Self::Ascending => replica,
            Self::Descending => replicas - 1 - replica,
        };
        u32::try_from(position + 1).context("too many replicas for a 32-bit id")
    }
}

/// Returns the first transaction after the setup that does not have every setup
/// transaction in its ancestry. Returns `None` when there is no such transaction.
///
/// The driver gives the setup to every replica before the timed part. So a later
/// transaction that does not follow the whole setup would see edits that are
/// concurrent with it in the trace.
fn first_txn_outside_setup(trace: &Trace, setup: usize) -> Option<usize> {
    if setup == 0 {
        return None;
    }
    // One bit for each setup transaction in the ancestry of each transaction.
    let words = setup.div_ceil(64);
    let mut ancestry = vec![0u64; trace.txns.len() * words];
    let full: Vec<u64> = (0..words)
        .map(|w| match setup - w * 64 {
            n if n >= 64 => u64::MAX,
            n => (1 << n) - 1,
        })
        .collect();
    for (i, txn) in trace.txns.iter().enumerate() {
        let (done, rest) = ancestry.split_at_mut(i * words);
        let bits = &mut rest[..words];
        for &p in &txn.parents {
            for (bit, parent) in bits.iter_mut().zip(&done[p * words..(p + 1) * words]) {
                *bit |= parent;
            }
        }
        if i < setup {
            bits[i / 64] |= 1 << (i % 64);
        } else if bits != full.as_slice() {
            return Some(i);
        }
    }
    None
}

/// A trace with its run settings. Build it once, then run many trials.
pub struct Plan {
    /// The run settings.
    pub workload: Workload,
    /// The trace.
    pub trace: Trace,
    /// How many replicas to read and check.
    check_replicas: usize,
    /// The final-content check.
    check: CheckPlan,
}

impl Plan {
    /// Builds a plan for `trace` with the settings of `workload`.
    pub fn new(workload: Workload, trace: Trace) -> Result<Self> {
        let replicas = trace.num_agents + workload.receivers;
        let check_replicas = workload.check_replicas.unwrap_or(replicas);
        let check = CheckPlan::new(&trace, workload.repeat)?;
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
        if let Some(i) = first_txn_outside_setup(&trace, workload.setup_txns) {
            bail!(
                "txn {i} does not follow all {} setup transactions. Every replica \
                 gets the setup first, so the replay would make concurrent edits \
                 causal",
                workload.setup_txns
            );
        }
        ensure!(
            (1..=replicas).contains(&check_replicas),
            "check_replicas must be in 1..={replicas}"
        );
        ensure!(
            workload.timing == Timing::All || check_replicas >= 2,
            "merge_only needs a reference replica and a timed replica"
        );
        Ok(Self {
            workload,
            trace,
            check_replicas,
            check,
        })
    }

    /// Returns the workload name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.workload.name
    }

    /// Returns the number of replicas that a trial needs.
    #[must_use]
    pub fn replica_count(&self) -> usize {
        self.trace.num_agents + self.workload.receivers
    }

    /// Runs the final-content check on what the replicas report. This is the
    /// check that makes a timing valid.
    fn verify(&self, contents: &[Content], loaded: &Content) -> Result<()> {
        for (r, content) in contents.iter().enumerate() {
            self.check
                .check(content)
                .with_context(|| format!("replica {r}"))?;
            ensure!(
                content == &contents[0],
                "replica {r} differs from replica 0"
            );
        }
        ensure!(
            loaded == &contents[0],
            "the loaded document differs from replica 0"
        );
        Ok(())
    }
}

/// What one trial measured.
#[derive(Debug, Clone, Copy, Default, Serialize)]
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
}

/// What one memory trial measured.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct MemoryResult {
    /// The highest live bytes during the replay, above the live bytes of the
    /// trial before its first transaction. The count starts after
    /// `Trial::new`, so the empty replicas and the replay buffers are not in
    /// it. So this is the growth that the replay causes, not the total memory.
    pub peak_bytes: i64,
    /// The live bytes that the loaded document holds.
    pub retained_bytes: i64,
}

/// The timed part of a trial.
struct Timed {
    /// The whole timed part.
    total: Duration,
    /// The reads at its end.
    read: Duration,
    /// What each checked replica holds.
    contents: Vec<Content>,
}

/// The encode and load steps of a trial.
struct Reloaded {
    /// The length of the encoded document, in bytes.
    doc_size: usize,
    /// The time to encode.
    encode: Duration,
    /// The time to load and read.
    parse: Duration,
    /// What the loaded document holds.
    content: Content,
}

/// The replicas of one trial, and what each one has seen.
struct Trial<'a> {
    /// The plan that this trial runs.
    plan: &'a Plan,
    /// The replicas.
    targets: Vec<Box<dyn Target>>,
    /// True if more than one replica exists. Otherwise the trial needs no
    /// delivery and keeps no updates, so memory stays at the document's own use.
    delivers: bool,
    /// For each replica, which transactions it has.
    seen: Vec<Vec<bool>>,
    /// The update of each transaction. Empty if nobody needs it.
    updates: Vec<Vec<u8>>,
    /// The bytes of all updates so far.
    update_bytes: usize,
    /// The number of non-empty updates so far.
    update_count: usize,
}

impl<'a> Trial<'a> {
    /// Creates the replicas and their shared object.
    fn new(plan: &'a Plan, lane: &Lane, ids: IdOrder) -> Result<Self> {
        let replicas = plan.replica_count();
        let mut targets = lane.acquire(replicas)?;
        for (r, target) in targets.iter_mut().enumerate() {
            target.set_instance_id(ids.id(r, replicas)?)?;
            target.ensure(plan.trace.kind)?;
        }
        let delivers = replicas > 1;
        let txns = if delivers { plan.trace.txns.len() } else { 0 };
        Ok(Self {
            plan,
            targets,
            delivers,
            seen: vec![vec![false; txns]; replicas],
            updates: vec![Vec::new(); txns],
            update_bytes: 0,
            update_count: 0,
        })
    }

    /// Applies transaction `i` on its agent, after the agent has its ancestors.
    fn apply(&mut self, i: usize) -> Result<()> {
        let txn = &self.plan.trace.txns[i];
        if self.delivers {
            self.deliver_ancestors(txn.agent, i)?;
        }
        let update = self.targets[txn.agent]
            .apply_txn(&txn.ops)
            .with_context(|| format!("txn {i}"))?;
        self.update_bytes += update.len();
        self.update_count += usize::from(!update.is_empty());
        if self.delivers {
            self.seen[txn.agent][i] = true;
            self.updates[i] = update;
        }
        Ok(())
    }

    /// Gives `replica` the ancestors of transaction `i` that it does not have,
    /// in index order. A parent always has a lower index than its child, so
    /// index order is causal order.
    fn deliver_ancestors(&mut self, replica: usize, i: usize) -> Result<()> {
        let txns = &self.plan.trace.txns;
        let mut missing = Vec::new();
        let mut stack = txns[i].parents.clone();
        let mut queued = HashSet::new();
        while let Some(t) = stack.pop() {
            if !self.seen[replica][t] && queued.insert(t) {
                missing.push(t);
                stack.extend(&txns[t].parents);
            }
        }
        missing.sort_unstable();
        self.deliver(replica, &missing, false)
    }

    /// Gives `replica` all transactions that it does not have.
    fn settle(&mut self, replica: usize) -> Result<()> {
        if !self.delivers {
            return Ok(());
        }
        let missing: Vec<usize> = (0..self.updates.len())
            .filter(|&t| !self.seen[replica][t])
            .collect();
        self.deliver(replica, &missing, self.plan.workload.merge_batch)
    }

    /// Gives `replica` the updates of `txns`, in order. With `batch`, they go in
    /// one adapter call. Marks each transaction as seen.
    fn deliver(&mut self, replica: usize, txns: &[usize], batch: bool) -> Result<()> {
        let updates: Vec<&[u8]> = txns
            .iter()
            .map(|&t| self.updates[t].as_slice())
            .filter(|update| !update.is_empty())
            .collect();
        let target = &mut self.targets[replica];
        if batch && !updates.is_empty() {
            target.apply_updates(&updates)?;
        } else if !batch {
            for update in updates {
                target.apply_updates(&[update])?;
            }
        }
        for &t in txns {
            self.seen[replica][t] = true;
        }
        Ok(())
    }

    /// Runs the setup transactions, and gives them to every replica.
    fn run_setup(&mut self) -> Result<()> {
        let setup = 0..self.plan.workload.setup_txns;
        for i in setup.clone() {
            self.apply(i)?;
        }
        if self.delivers {
            let txns: Vec<usize> = setup.collect();
            for r in 0..self.targets.len() {
                let missing: Vec<usize> =
                    txns.iter().copied().filter(|&t| !self.seen[r][t]).collect();
                self.deliver(r, &missing, false)?;
            }
        }
        Ok(())
    }

    /// Discards the adapter time so far, so the next total covers the timed part.
    fn restart_adapter_time(&mut self) {
        for target in &mut self.targets {
            target.take_adapter_ns();
        }
    }

    /// Reads the replicas to check. `started` is the start of the timed part.
    fn finish_timed(&mut self, started: Instant) -> Result<Timed> {
        let kind = self.plan.trace.kind;
        let reading = Instant::now();
        let contents = self.targets[..self.plan.check_replicas]
            .iter_mut()
            .map(|target| target.read(kind))
            .collect::<Result<_>>()?;
        Ok(Timed {
            total: started.elapsed(),
            read: reading.elapsed(),
            contents,
        })
    }

    /// Runs the part that [`Timing`] selects.
    fn run_timed(&mut self) -> Result<Timed> {
        let workload = &self.plan.workload;
        let setup = workload.setup_txns;
        let txns = self.plan.trace.txns.len();
        match workload.timing {
            Timing::All => {
                self.restart_adapter_time();
                let started = Instant::now();
                for _ in 0..workload.repeat {
                    for i in setup..txns {
                        self.apply(i)?;
                    }
                }
                for r in 0..self.plan.check_replicas {
                    self.settle(r)?;
                }
                self.finish_timed(started)
            }
            Timing::MergeOnly => {
                for i in setup..txns {
                    self.apply(i)?;
                }
                self.settle(0)?;
                self.restart_adapter_time();
                let started = Instant::now();
                for r in 1..self.plan.check_replicas {
                    self.settle(r)?;
                }
                self.finish_timed(started)
            }
        }
    }

    /// Frees what only the replay needed. A replay holds its updates, as the
    /// crdt-benchmarks harness does.
    fn release_replay_state(&mut self) {
        self.updates = Vec::new();
        self.seen = Vec::new();
    }

    /// Encodes replica 0.
    fn encode(&mut self) -> Result<(Vec<u8>, Duration)> {
        let started = Instant::now();
        let encoded = self.targets[0].encode()?;
        Ok((encoded, started.elapsed()))
    }

    /// Empties every replica. This is outside the parse timer, so the parse time
    /// has no cost from dropping the old documents.
    fn reset_all(&mut self) -> Result<()> {
        self.targets.iter_mut().try_for_each(|t| t.reset())
    }

    /// Loads `encoded` into replica 0 and reads it.
    fn load(&mut self, encoded: &[u8]) -> Result<(Content, Duration)> {
        let started = Instant::now();
        self.targets[0].load(encoded)?;
        let content = self.targets[0].read(self.plan.trace.kind)?;
        Ok((content, started.elapsed()))
    }

    /// Encodes replica 0, empties all replicas, and loads the bytes.
    fn reload(&mut self) -> Result<Reloaded> {
        self.release_replay_state();
        let (encoded, encode) = self.encode()?;
        self.reset_all()?;
        let (content, parse) = self.load(&encoded)?;
        Ok(Reloaded {
            doc_size: encoded.len(),
            encode,
            parse,
            content,
        })
    }
}

/// Runs one timed trial on `lane`. Returns an error if any check fails.
pub fn run_trial(plan: &Plan, lane: &Lane, ids: IdOrder) -> Result<TrialResult> {
    let mut trial = Trial::new(plan, lane, ids)?;
    trial.run_setup()?;
    let timed = trial.run_timed()?;
    let adapter_ns: Option<u64> = trial.targets.iter_mut().map(|t| t.take_adapter_ns()).sum();
    let (update_bytes, update_count) = (trial.update_bytes, trial.update_count);
    let reloaded = trial.reload()?;
    plan.verify(&timed.contents, &reloaded.content)?;
    Ok(TrialResult {
        time_ms: ms(timed.total),
        read_ms: ms(timed.read),
        encode_ms: ms(reloaded.encode),
        doc_size: reloaded.doc_size,
        parse_ms: ms(reloaded.parse),
        update_bytes,
        update_count,
        adapter_ms: adapter_ns.map(|ns| ns as f64 / 1e6),
    })
}

/// Runs one trial with the allocation count on. Returns an error if any check
/// fails. This trial gives no timing, because the count adds work.
pub fn run_memory_trial(plan: &Plan, lane: &Lane, ids: IdOrder) -> Result<MemoryResult> {
    let mut trial = Trial::new(plan, lane, ids)?;
    let count = alloc::Counting::start();
    trial.run_setup()?;
    let timed = trial.run_timed()?;
    let peak_bytes = count.peak();

    trial.release_replay_state();
    let (encoded, _) = trial.encode()?;
    trial.reset_all()?;
    let baseline = count.current();
    let (loaded, _) = trial.load(&encoded)?;
    plan.verify(&timed.contents, &loaded)?;
    drop(loaded);
    let retained_bytes = count.current() - baseline;
    Ok(MemoryResult {
        peak_bytes,
        retained_bytes,
    })
}

/// Converts a duration to milliseconds.
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests;
