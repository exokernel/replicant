//! A per-trial event log, written by `--trace`.
//!
//! The CSV row says how long a trial took to converge. The trace shows how it
//! got there: which links exist, when each write lands, and, at each poll,
//! which replicas already agree. The guided notebook in `analysis/` uses it to
//! draw a trial on a timeline.
//!
//! A disabled [`Trace`] records nothing, and each call returns at once. An
//! enabled one does a little work inside the timed window: it reads the clock,
//! and it compares fingerprints at each poll. Do not use traced runs for
//! measurements.

use std::io::Write;
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

/// The events of one trial.
pub struct Trace {
    /// When the trial started. Every event time is relative to it.
    origin: Instant,
    /// `false` makes every recording method a no-op.
    enabled: bool,
    events: Vec<(f64, Event)>,
    /// Each distinct non-empty fingerprint seen so far, in order of first
    /// appearance. A fingerprint's index here is its class.
    classes: Vec<Vec<u8>>,
}

/// One thing that happened during a trial.
#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// Every link is open. `blocked` lists the links that drop all traffic
    /// until the heal.
    Wired {
        edges: Vec<(usize, usize)>,
        blocked: Vec<(usize, usize)>,
    },
    /// `convergence_ms` counts from here.
    ClockStart,
    /// Write number `op` was applied at `node`.
    Write { node: usize, op: usize },
    /// The blocked links were unblocked. The heal starts.
    Unblocked,
    /// A sync handshake was sent on each healed link.
    Kicked,
    /// One poll of the fingerprints of `nodes`. `classes[i]` is the class of
    /// `nodes[i]`: replicas in the same class have the same state. `None`
    /// means the replica has no operations yet.
    Poll {
        nodes: Vec<usize>,
        classes: Vec<Option<usize>>,
    },
    /// Every node in `nodes` has the same fingerprint.
    Converged { nodes: Vec<usize> },
}

/// One line of the trace file.
#[derive(Serialize)]
struct Line<'a> {
    scenario: &'a str,
    trial: usize,
    /// Milliseconds since the trial started.
    t_ms: f64,
    #[serde(flatten)]
    event: &'a Event,
}

impl Trace {
    /// Starts a trace whose clock starts now. With `enabled` false, the trace
    /// records nothing.
    pub fn new(enabled: bool) -> Self {
        Self {
            origin: Instant::now(),
            enabled,
            events: Vec::new(),
            classes: Vec::new(),
        }
    }

    /// A trace that records nothing.
    #[cfg(test)]
    pub fn off() -> Self {
        Self::new(false)
    }

    /// Records `event` at the current time.
    pub fn record(&mut self, event: Event) {
        if self.enabled {
            let t_ms = self.origin.elapsed().as_secs_f64() * 1000.0;
            self.events.push((t_ms, event));
        }
    }

    /// Records one poll. `fingerprints[i]` belongs to `nodes[i]`.
    pub fn poll(&mut self, nodes: &[usize], fingerprints: &[Vec<u8>]) {
        if !self.enabled {
            return;
        }
        let classes = fingerprints.iter().map(|fp| self.class_of(fp)).collect();
        self.record(Event::Poll {
            nodes: nodes.to_vec(),
            classes,
        });
    }

    /// Returns the class of `fp`, and adds a new class if this is the first
    /// time `fp` was seen.
    fn class_of(&mut self, fp: &[u8]) -> Option<usize> {
        if fp.is_empty() {
            return None;
        }
        Some(match self.classes.iter().position(|c| c == fp) {
            Some(i) => i,
            None => {
                self.classes.push(fp.to_vec());
                self.classes.len() - 1
            }
        })
    }

    /// Writes the events as JSON Lines, one object per event, each tagged with
    /// `scenario` and `trial`.
    pub fn write_to(&self, out: &mut impl Write, scenario: &str, trial: usize) -> Result<()> {
        for (t_ms, event) in &self.events {
            let line = Line {
                scenario,
                trial,
                t_ms: *t_ms,
                event,
            };
            serde_json::to_writer(&mut *out, &line)?;
            out.write_all(b"\n")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_trace_records_nothing() {
        let mut trace = Trace::off();
        trace.record(Event::ClockStart);
        trace.poll(&[0], &[vec![1]]);
        let mut out = Vec::new();
        trace.write_to(&mut out, "s", 1).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn classes_are_stable_across_polls() {
        let mut trace = Trace::new(true);
        trace.poll(&[0, 1, 2], &[vec![], vec![7], vec![9]]);
        trace.poll(&[0, 1, 2], &[vec![9], vec![9], vec![9]]);
        let mut out = Vec::new();
        trace.write_to(&mut out, "s", 3).unwrap();
        let lines: Vec<serde_json::Value> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines[0]["classes"], serde_json::json!([null, 0, 1]));
        assert_eq!(lines[1]["classes"], serde_json::json!([1, 1, 1]));
        assert_eq!(lines[1]["event"], "poll");
        assert_eq!(lines[1]["scenario"], "s");
        assert_eq!(lines[1]["trial"], 3);
    }
}
