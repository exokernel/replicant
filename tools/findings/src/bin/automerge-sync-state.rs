//! Finding: the sync state that Automerge keeps across a disconnect sets the
//! cost of the heal.
//!
//! Two documents load the B4 paper and sync until both are idle. Each then
//! inserts one character, and the two sync again. Before the second sync, the
//! probe treats the sync state in one of three ways:
//!
//! - `kept`: keep the state in memory, as it was at the end of the first sync.
//! - `persisted`: `sync::State::decode(&state.encode())`, Automerge's
//!   documented persistence. It keeps only the heads that both peers have.
//! - `fresh`: a new `sync::State`, as at a first contact.
//!
//! Both sides start each sync at the same time, and each side answers each
//! message with a reply and one more generate, as Replicant's replica does.
//!
//! Output: CSV with one row for each trial.

use std::collections::VecDeque;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use automerge::sync::{self, SyncDoc};
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ROOT, ReadDoc};
use findings::{Trace, automerge_doc, ms_since};

/// How the probe treats the sync state before the heal.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Keep the state in memory.
    Kept,
    /// Keep only what `sync::State::encode` keeps.
    Persisted,
    /// Use a new state.
    Fresh,
}

/// The time of one sync, split into the generate calls and the total.
struct SyncTime {
    /// The time in all generate and receive calls.
    total_ms: f64,
    /// The time in the generate calls only.
    generate_ms: f64,
}

/// One side of a sync: a document and its state for the other side.
struct Side {
    /// The document.
    doc: AutoCommit,
    /// The sync state for the other side.
    state: sync::State,
}

impl Side {
    /// Generates one message, and adds the time to `time`.
    fn generate(&mut self, time: &mut SyncTime) -> Option<sync::Message> {
        let start = Instant::now();
        let msg = self.doc.sync().generate_sync_message(&mut self.state);
        let elapsed = ms_since(start);
        time.generate_ms += elapsed;
        time.total_ms += elapsed;
        msg
    }

    /// Receives one message, and adds the time to `time`.
    fn receive(&mut self, msg: sync::Message, time: &mut SyncTime) -> Result<()> {
        let start = Instant::now();
        self.doc.sync().receive_sync_message(&mut self.state, msg)?;
        time.total_ms += ms_since(start);
        Ok(())
    }
}

/// Syncs `a` and `b` until no message is in flight, in the replica's pattern.
fn sync_pair(a: &mut Side, b: &mut Side) -> Result<SyncTime> {
    let mut time = SyncTime {
        total_ms: 0.0,
        generate_ms: 0.0,
    };
    let mut to_a = VecDeque::new();
    let mut to_b = VecDeque::new();
    to_b.extend(a.generate(&mut time));
    to_a.extend(b.generate(&mut time));
    while !(to_a.is_empty() && to_b.is_empty()) {
        if let Some(msg) = to_b.pop_front() {
            b.receive(msg, &mut time)?;
            for _ in 0..2 {
                to_a.extend(b.generate(&mut time));
            }
        }
        if let Some(msg) = to_a.pop_front() {
            a.receive(msg, &mut time)?;
            for _ in 0..2 {
                to_b.extend(a.generate(&mut time));
            }
        }
    }
    Ok(time)
}

fn main() -> Result<()> {
    let trace = Trace::paper_from_args()?;
    let (mut paper, _) = automerge_doc(&trace)?;
    let saved = paper.save();
    println!("finding,mode,trial,heal_ms,generate_ms");
    for mode in [Mode::Kept, Mode::Persisted, Mode::Fresh] {
        for trial in 0..3 {
            let mut a = Side {
                doc: AutoCommit::load(&saved)?,
                state: sync::State::new(),
            };
            let mut b = Side {
                doc: AutoCommit::load(&saved)?,
                state: sync::State::new(),
            };
            sync_pair(&mut a, &mut b)?;
            for (side, pos, ins) in [(&mut a, 1_000, "x"), (&mut b, 50_000, "y")] {
                let (_, text) = side.doc.get(ROOT, "text")?.context("no text object")?;
                side.doc.splice_text(&text, pos, 0, ins)?;
                side.doc.commit();
            }
            for side in [&mut a, &mut b] {
                match mode {
                    Mode::Kept => {}
                    Mode::Persisted => side.state = sync::State::decode(&side.state.encode())?,
                    Mode::Fresh => side.state = sync::State::new(),
                }
            }
            let heal = sync_pair(&mut a, &mut b)?;
            ensure!(
                a.doc.get_heads() == b.doc.get_heads(),
                "the heal did not converge"
            );
            println!(
                "automerge-sync-state,{mode:?},{trial},{:.1},{:.1}",
                heal.total_ms, heal.generate_ms
            );
        }
    }
    Ok(())
}
