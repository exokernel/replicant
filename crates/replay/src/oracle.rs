//! Checks the final content of a replay.
//!
//! Equal replicas prove only that the replicas agree. A merge can lose one
//! side's work and still give equal replicas. So every workload must also pass
//! a check against what the ops should produce.
//!
//! Concurrent inserts at one position have no single expected order, because
//! each library has its own rule. For these workloads, [`CheckPlan`] checks the
//! properties that every library must keep.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use common::{Op, ScalarVal};
use serde::Deserialize;

use crate::trace::{JsonScalar, Trace};

/// The check that a trace file asks for.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OracleSpec {
    /// The final text is exactly this string.
    ExactText {
        /// The expected text.
        text: String,
    },
    /// The final list is exactly these values.
    ExactList {
        /// The expected values.
        values: Vec<JsonScalar>,
    },
    /// Each key holds one of the values that some agent wrote to that key.
    MapWinner,
    /// For a trace that only forks. See [`CheckPlan::Tree`].
    TreeInvariants,
}

/// The content that one replica reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Content {
    /// The text of a text object.
    Text(String),
    /// The values of a list object.
    List(Vec<ScalarVal>),
    /// The entries of a map object.
    Map(BTreeMap<String, ScalarVal>),
}

/// A check, ready to run. Build it once per trace, outside any timed window.
#[derive(Debug)]
pub enum CheckPlan {
    /// The content equals this exactly.
    Exact(Content),
    /// Each key holds one of its candidate values, and no other key exists.
    MapWinner(BTreeMap<String, Vec<ScalarVal>>),
    /// The trace only forks, so the elements that survive do not depend on the
    /// library. The merged sequence must:
    ///
    /// - hold exactly the elements that no agent deleted, and
    /// - keep the local order of each branch.
    Tree {
        /// The expected elements, sorted.
        survivors: Vec<String>,
        /// The surviving elements of each branch tip, in local order.
        tips: Vec<Vec<String>>,
    },
}

/// Returns the comparison key of one element.
fn key(value: &ScalarVal) -> String {
    format!("{value:?}")
}

/// Splits `content` into one comparison key per element.
fn elements(content: &Content) -> Result<Vec<String>> {
    Ok(match content {
        Content::Text(text) => text.chars().map(String::from).collect(),
        Content::List(values) => values.iter().map(key).collect(),
        Content::Map(_) => bail!("a map has no element order"),
    })
}

/// Replays the trace on identity lists, one per branch.
///
/// An element's identity is its index in `arena`. The result depends on no
/// library, because no position in the trace follows a merge.
fn tree_plan(trace: &Trace) -> Result<CheckPlan> {
    let mut children = vec![0usize; trace.txns.len()];
    for t in &trace.txns {
        for &p in &t.parents {
            children[p] += 1;
        }
    }
    let mut arena: Vec<String> = Vec::new();
    let mut dead: Vec<bool> = Vec::new();
    // The identity list after each txn. A list moves to its last child.
    let mut states: Vec<Vec<u32>> = vec![Vec::new(); trace.txns.len()];
    let mut remaining = children.clone();
    let mut tips: Vec<Vec<u32>> = Vec::new();
    for (i, t) in trace.txns.iter().enumerate() {
        // Parents have lower indexes, and a parent keeps its state until its
        // last child takes it. So the state is always present here.
        let mut doc = match t.parents.first() {
            None => Vec::new(),
            Some(&p) => {
                remaining[p] -= 1;
                if remaining[p] == 0 {
                    std::mem::take(&mut states[p])
                } else {
                    states[p].clone()
                }
            }
        };
        for op in &t.ops {
            let (pos, del, new): (usize, usize, Vec<String>) = match op {
                Op::TextSplice {
                    pos,
                    del_count,
                    insert,
                    ..
                } => (*pos, *del_count, insert.chars().map(String::from).collect()),
                Op::ListSplice {
                    pos,
                    del_count,
                    values,
                    ..
                } => (*pos, *del_count, values.iter().map(key).collect()),
                other => bail!("txn {i}: {} has no place in a sequence trace", other.name()),
            };
            ensure!(
                pos + del <= doc.len(),
                "txn {i}: position {pos} + {del} is past the end of length {}",
                doc.len()
            );
            for &id in &doc[pos..pos + del] {
                dead[id as usize] = true;
            }
            let first = u32::try_from(arena.len()).context("too many elements for a 32-bit id")?;
            let end = u32::try_from(arena.len() + new.len())
                .context("too many elements for a 32-bit id")?;
            let ids: Vec<u32> = (first..end).collect();
            dead.extend(std::iter::repeat_n(false, new.len()));
            arena.extend(new);
            doc.splice(pos..pos + del, ids);
        }
        if children[i] == 0 {
            tips.push(doc);
        } else {
            states[i] = doc;
        }
    }
    let mut survivors: Vec<String> = arena
        .iter()
        .zip(&dead)
        .filter(|(_, d)| !**d)
        .map(|(e, _)| e.clone())
        .collect();
    survivors.sort_unstable();
    let tips = tips
        .iter()
        .map(|tip| {
            tip.iter()
                .filter(|&&id| !dead[id as usize])
                .map(|&id| arena[id as usize].clone())
                .collect()
        })
        .collect();
    Ok(CheckPlan::Tree { survivors, tips })
}

/// Returns true if `needle` is a subsequence of `hay`.
fn is_subsequence(needle: &[String], hay: &[String]) -> bool {
    let mut hay = hay.iter();
    needle.iter().all(|n| hay.any(|h| h == n))
}

/// Describes the first difference between two values, in short form.
fn first_difference(expected: &Content, actual: &Content) -> String {
    match (expected, actual) {
        (Content::Text(e), Content::Text(a)) => {
            let at = e.chars().zip(a.chars()).take_while(|(x, y)| x == y).count();
            format!(
                "expected {} chars, found {} chars, first difference at char {at}",
                e.chars().count(),
                a.chars().count()
            )
        }
        (Content::List(e), Content::List(a)) => {
            let at = e.iter().zip(a).take_while(|(x, y)| x == y).count();
            format!(
                "expected {} values, found {} values, first difference at index {at}",
                e.len(),
                a.len()
            )
        }
        _ => "the content has a different kind".to_owned(),
    }
}

impl CheckPlan {
    /// Returns the check for `trace`, replayed `repeat` times.
    pub fn new(trace: &Trace, repeat: usize) -> Result<Self> {
        ensure!(
            repeat == 1 || matches!(trace.oracle, OracleSpec::ExactText { .. }),
            "repeat needs an exact text oracle"
        );
        Ok(match &trace.oracle {
            OracleSpec::ExactText { text } => Self::Exact(Content::Text(text.repeat(repeat))),
            OracleSpec::ExactList { values } => Self::Exact(Content::List(
                values.iter().cloned().map(Into::into).collect(),
            )),
            OracleSpec::MapWinner => {
                let mut candidates: BTreeMap<String, Vec<ScalarVal>> = BTreeMap::new();
                for op in trace.txns.iter().flat_map(|t| &t.ops) {
                    if let Op::MapPut { key, value, .. } = op {
                        candidates
                            .entry(key.clone())
                            .or_default()
                            .push(value.clone());
                    }
                }
                Self::MapWinner(candidates)
            }
            OracleSpec::TreeInvariants => tree_plan(trace)?,
        })
    }

    /// Returns an error if `content` fails this check.
    pub fn check(&self, content: &Content) -> Result<()> {
        match self {
            CheckPlan::Exact(expected) => {
                ensure!(
                    expected == content,
                    "final content is wrong: {}",
                    first_difference(expected, content)
                );
            }
            CheckPlan::MapWinner(candidates) => {
                let Content::Map(map) = content else {
                    bail!("expected a map");
                };
                ensure!(
                    map.len() == candidates.len(),
                    "expected {} keys, found {}",
                    candidates.len(),
                    map.len()
                );
                for (k, options) in candidates {
                    let value = map
                        .get(k)
                        .with_context(|| format!("key {k:?} is missing"))?;
                    ensure!(
                        options.contains(value),
                        "key {k:?} holds {value:?}, and no agent wrote that value"
                    );
                }
            }
            CheckPlan::Tree { survivors, tips } => {
                let merged = elements(content)?;
                let mut sorted = merged.clone();
                sorted.sort_unstable();
                ensure!(
                    &sorted == survivors,
                    "expected {} surviving elements, found {}. A merge lost or invented work.",
                    survivors.len(),
                    sorted.len()
                );
                for (i, tip) in tips.iter().enumerate() {
                    ensure!(
                        is_subsequence(tip, &merged),
                        "the merged sequence does not keep the local order of branch {i}"
                    );
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace(json: &str) -> Trace {
        Trace::parse(json.as_bytes()).unwrap()
    }

    fn text(s: &str) -> Content {
        Content::Text(s.to_owned())
    }

    /// Two branches from a shared base. Each prepends, and one deletes a base
    /// character.
    const FORK: &str = r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
        "txns":[
        {"agent":0,"parents":[],"patches":[[0,0,"base"]]},
        {"agent":0,"parents":[0],"patches":[[0,0,"AB"]]},
        {"agent":1,"parents":[0],"patches":[[0,0,"CD"],[2,1,""]]}]}"#;

    #[test]
    fn tree_check_accepts_any_valid_interleaving() {
        let plan = CheckPlan::new(&trace(FORK), 1).unwrap();
        for merged in ["ABCDase", "CDABase", "ACBDase"] {
            plan.check(&text(merged)).unwrap();
        }
    }

    /// The bug that this crate exists to catch: the replicas agree, and the
    /// agreed document holds only half of the work.
    #[test]
    fn tree_check_rejects_a_merge_that_lost_a_side() {
        let plan = CheckPlan::new(&trace(FORK), 1).unwrap();
        let e = plan.check(&text("ABbase")).unwrap_err();
        assert!(e.to_string().contains("lost or invented"), "{e}");
    }

    #[test]
    fn tree_check_rejects_a_broken_local_order() {
        let plan = CheckPlan::new(&trace(FORK), 1).unwrap();
        let e = plan.check(&text("BACDase")).unwrap_err();
        assert!(e.to_string().contains("local order"), "{e}");
    }

    #[test]
    fn exact_check_reports_the_first_difference() {
        let t = trace(r#"{"endContent":"abc","txns":[{"patches":[[0,0,"abc"]]}]}"#);
        let plan = CheckPlan::new(&t, 1).unwrap();
        plan.check(&text("abc")).unwrap();
        let e = plan.check(&text("abd")).unwrap_err();
        assert!(e.to_string().contains("at char 2"), "{e}");
    }

    #[test]
    fn repeat_multiplies_the_expected_text() {
        let t = trace(r#"{"endContent":"ab","txns":[{"patches":[[0,0,"ab"]]}]}"#);
        CheckPlan::new(&t, 3)
            .unwrap()
            .check(&text("ababab"))
            .unwrap();
    }

    #[test]
    fn map_winner_needs_a_value_that_an_agent_wrote() {
        let t = trace(
            r#"{"kind":"concurrent","oracle":{"kind":"map_winner"},"numAgents":2,"txns":[
                {"agent":0,"parents":[],"mapOps":[["v",1]]},
                {"agent":1,"parents":[],"mapOps":[["v",2]]}]}"#,
        );
        let plan = CheckPlan::new(&t, 1).unwrap();
        let map = |n| Content::Map(BTreeMap::from([("v".to_owned(), ScalarVal::Int(n))]));
        plan.check(&map(2)).unwrap();
        assert!(plan.check(&map(3)).is_err());
        assert!(plan.check(&Content::Map(BTreeMap::new())).is_err());
    }

    #[test]
    fn a_position_past_the_end_of_the_branch_is_rejected() {
        let t = trace(
            r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":1,"txns":[
                {"agent":0,"parents":[],"patches":[[5,0,"x"]]}]}"#,
        );
        let e = CheckPlan::new(&t, 1).unwrap_err();
        assert!(e.to_string().contains("past the end"), "{e}");
    }
}
