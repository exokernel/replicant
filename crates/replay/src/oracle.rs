//! Checks the final content of a replay.
//!
//! Equal replicas prove only that the replicas agree. A merge can lose one
//! side's work and still give equal replicas. So every workload must also pass
//! a check against what the ops should produce.
//!
//! Concurrent inserts at one position have no single expected order, because
//! each library has its own rule. For these workloads, [`CheckPlan`] checks the
//! properties that every library must keep.

use std::collections::{BTreeMap, HashMap, HashSet};

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
    /// - be an interleaving of the branches that keeps the local order of each
    ///   branch. An element that two branches share appears once.
    ///
    /// The second rule is a joint check when at most [`MAX_JOINT_CHAINS`]
    /// branches have two or more survivors. A separate subsequence check for
    /// each branch is not enough, because two branches can use the same copy
    /// of a repeated value. With more branches, the plan checks each branch on
    /// its own: to find if a sequence is an interleaving of many sequences is
    /// NP-complete, and B3.5 has 1,530 such branches over ten digits.
    Tree(TreePlan),
}

/// The data of [`CheckPlan::Tree`].
#[derive(Debug)]
pub struct TreePlan {
    /// The expected elements, sorted.
    survivors: Vec<String>,
    /// The surviving elements of each branch tip, in local order.
    tips: Vec<Vec<String>>,
    /// The label of each element in `chains`.
    labels: Vec<u32>,
    /// The label of each value.
    label_of: HashMap<String, u32>,
    /// True for a label that some element outside `chains` has. Such an
    /// element has no order rule.
    free: Vec<bool>,
    /// The tips with two or more survivors, as element ids in local order.
    chains: Vec<Vec<u32>>,
    /// For each element id, each chain that holds it and its index there.
    owners: Vec<Vec<(usize, u32)>>,
}

/// The most chains for which [`TreePlan`] does the joint check. With two chains,
/// the search has at most one state for each position in a chain.
pub const MAX_JOINT_CHAINS: usize = 2;

/// The largest number of search states that the joint branch check keeps.
const MAX_TREE_STATES: usize = 100_000;

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
    let tips: Vec<Vec<u32>> = tips
        .into_iter()
        .map(|tip| tip.into_iter().filter(|&id| !dead[id as usize]).collect())
        .collect();
    Ok(CheckPlan::Tree(TreePlan::new(survivors, &tips, &arena)))
}

impl TreePlan {
    /// Builds the plan from the surviving arena ids of each tip.
    fn new(survivors: Vec<String>, tips: &[Vec<u32>], arena: &[String]) -> Self {
        let mut label_of: HashMap<String, u32> = HashMap::new();
        for value in &survivors {
            let next = label_of.len() as u32;
            label_of.entry(value.clone()).or_insert(next);
        }
        // A tip with one survivor has no order rule, so only longer tips are
        // chains. The elements of the chains get compact ids.
        let mut compact: HashMap<u32, u32> = HashMap::new();
        let mut labels = Vec::new();
        let mut owners: Vec<Vec<(usize, u32)>> = Vec::new();
        let mut chains = Vec::new();
        for tip in tips.iter().filter(|tip| tip.len() >= 2) {
            let c = chains.len();
            let chain: Vec<u32> = tip
                .iter()
                .enumerate()
                .map(|(index, &arena_id)| {
                    let id = *compact.entry(arena_id).or_insert_with(|| {
                        labels.push(label_of[&arena[arena_id as usize]]);
                        owners.push(Vec::new());
                        labels.len() as u32 - 1
                    });
                    owners[id as usize].push((c, index as u32));
                    id
                })
                .collect();
            chains.push(chain);
        }
        // The survivors outside the chains have no order rule.
        let mut in_chains = vec![0usize; label_of.len()];
        for &label in &labels {
            in_chains[label as usize] += 1;
        }
        let mut free = vec![false; label_of.len()];
        let mut total = vec![0usize; label_of.len()];
        for value in &survivors {
            total[label_of[value] as usize] += 1;
        }
        for (label, (&all, &chained)) in total.iter().zip(&in_chains).enumerate() {
            free[label] = all > chained;
        }
        let tips = tips
            .iter()
            .map(|tip| tip.iter().map(|&id| arena[id as usize].clone()).collect())
            .collect();
        Self {
            survivors,
            tips,
            labels,
            label_of,
            free,
            chains,
            owners,
        }
    }

    /// Returns an error if `merged` fails the check.
    fn check(&self, merged: &[String]) -> Result<()> {
        let mut sorted = merged.to_vec();
        sorted.sort_unstable();
        ensure!(
            sorted == self.survivors,
            "expected {} surviving elements, found {}. A merge lost or invented work.",
            self.survivors.len(),
            sorted.len()
        );
        for (i, tip) in self.tips.iter().enumerate() {
            ensure!(
                is_subsequence(tip, merged),
                "the merged sequence does not keep the local order of branch {i}"
            );
        }
        ensure!(
            !self.is_joint() || self.interleaves(merged)?,
            "the merged sequence is not an interleaving of the branches. Each branch \
             keeps its order only if two branches use the same element."
        );
        Ok(())
    }

    /// Returns true if the check is joint. See [`CheckPlan::Tree`].
    #[must_use]
    pub fn is_joint(&self) -> bool {
        self.chains.len() <= MAX_JOINT_CHAINS
    }

    /// Returns true if `merged` is an interleaving of the chains, with each
    /// shared element used once. Call this only when `merged` holds exactly the
    /// survivors.
    ///
    /// The search reads `merged` one element at a time. A state is the position
    /// in each chain. An element can come next if it is the next element of
    /// every chain that holds it. A survivor outside the chains can match any
    /// position with its label. Because the counts are equal, the search does
    /// not need to count these: at the end, the positions that no chain used
    /// are exactly the free survivors.
    fn interleaves(&self, merged: &[String]) -> Result<bool> {
        let ends: Vec<u32> = self.chains.iter().map(|c| c.len() as u32).collect();
        let mut frontier: HashSet<Vec<u32>> = HashSet::from([vec![0; self.chains.len()]]);
        for value in merged {
            let label = self.label_of[value];
            let mut next = HashSet::new();
            for state in &frontier {
                if self.free[label as usize] {
                    next.insert(state.clone());
                }
                for (c, chain) in self.chains.iter().enumerate() {
                    let Some(&id) = chain.get(state[c] as usize) else {
                        continue;
                    };
                    let owners = &self.owners[id as usize];
                    if self.labels[id as usize] != label
                        || owners.iter().any(|&(o, index)| state[o] != index)
                    {
                        continue;
                    }
                    let mut moved = state.clone();
                    for &(o, _) in owners {
                        moved[o] += 1;
                    }
                    next.insert(moved);
                }
            }
            ensure!(
                next.len() <= MAX_TREE_STATES,
                "the joint branch check needs more than {MAX_TREE_STATES} states"
            );
            if next.is_empty() {
                return Ok(false);
            }
            frontier = next;
        }
        Ok(frontier.contains(&ends))
    }
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
            CheckPlan::Tree(plan) => plan.check(&elements(content)?)?,
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

    /// Two concurrent roots with repeated values. In `aabcaa`, each branch is a
    /// subsequence on its own, but `c` needs two `a`s of its own branch before
    /// it, and only the `a`s of the other branch are there.
    #[test]
    fn tree_check_rejects_two_branches_that_share_one_copy_of_a_value() {
        let plan = CheckPlan::new(
            &trace(
                r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
                "txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"aab"]]},
                {"agent":1,"parents":[],"patches":[[0,0,"aac"]]}]}"#,
            ),
            1,
        )
        .unwrap();
        for merged in ["aabaac", "aacaab", "aaabac", "aaaabc"] {
            plan.check(&text(merged)).unwrap();
        }
        let e = plan.check(&text("aabcaa")).unwrap_err();
        assert!(e.to_string().contains("not an interleaving"), "{e}");
    }

    /// A shared base element appears once, so both branches must agree on its
    /// place. Branch 0 gives `bab` (b, base a, base b), and branch 1 gives
    /// `aba` (base a, base b, a). In `abab`, each branch is a subsequence, but
    /// no single place for the base `a` and `b` works for both branches.
    #[test]
    fn tree_check_uses_a_shared_element_once_for_both_branches() {
        let plan = CheckPlan::new(
            &trace(
                r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
                "txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"ab"]]},
                {"agent":0,"parents":[0],"patches":[[0,0,"b"]]},
                {"agent":1,"parents":[0],"patches":[[2,0,"a"]]}]}"#,
            ),
            1,
        )
        .unwrap();
        plan.check(&text("baba")).unwrap();
        let e = plan.check(&text("abab")).unwrap_err();
        assert!(e.to_string().contains("not an interleaving"), "{e}");
    }

    /// A branch with one element has no order rule, and its value can match
    /// any position with that value.
    #[test]
    fn tree_check_places_a_one_element_branch_anywhere() {
        let plan = CheckPlan::new(
            &trace(
                r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,
                "txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"ab"]]},
                {"agent":1,"parents":[],"patches":[[0,0,"a"]]}]}"#,
            ),
            1,
        )
        .unwrap();
        for merged in ["aab", "aba"] {
            plan.check(&text(merged)).unwrap();
        }
        assert!(plan.check(&text("baa")).is_err());
    }

    /// Many branches with two or more survivors: the check is per branch, and
    /// it still refuses a broken local order.
    #[test]
    fn tree_check_with_many_branches_checks_each_branch() {
        let txns: Vec<String> = (10..13)
            .map(|i| {
                format!(
                    r#"{{"agent":{},"parents":[],"patches":[[0,0,"{i}"]]}}"#,
                    i - 10
                )
            })
            .collect();
        let plan = CheckPlan::new(
            &trace(&format!(
                r#"{{"kind":"concurrent","oracle":{{"kind":"tree_invariants"}},"numAgents":3,"txns":[{}]}}"#,
                txns.join(",")
            )),
            1,
        )
        .unwrap();
        let CheckPlan::Tree(tree) = &plan else {
            panic!("expected a tree plan");
        };
        assert!(!tree.is_joint());
        plan.check(&text("121110")).unwrap();
        assert!(
            plan.check(&text("011112"))
                .unwrap_err()
                .to_string()
                .contains("local order")
        );
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

    #[test]
    fn exact_list_check_reports_the_first_difference() {
        let t = trace(
            r#"{"oracle":{"kind":"exact_list","values":["a","b"]},
                "txns":[{"listOps":[[0,0,["a","b"]]]}]}"#,
        );
        let plan = CheckPlan::new(&t, 1).unwrap();
        let list =
            |v: &[&str]| Content::List(v.iter().map(|s| ScalarVal::Str((*s).into())).collect());
        plan.check(&list(&["a", "b"])).unwrap();
        let e = plan.check(&list(&["a", "c", "d"])).unwrap_err();
        assert!(
            e.to_string()
                .contains("expected 2 values, found 3 values, first difference at index 1"),
            "{e}"
        );
        let e = plan.check(&text("ab")).unwrap_err();
        assert!(e.to_string().contains("different kind"), "{e}");
    }

    /// Only an exact text can be repeated: the text of N passes is the text of
    /// one pass N times. No other check has a known result for N passes.
    #[test]
    fn repeat_needs_an_exact_text_oracle() {
        let t = trace(
            r#"{"kind":"concurrent","oracle":{"kind":"map_winner"},"numAgents":1,
                "txns":[{"agent":0,"parents":[],"mapOps":[["k",1]]}]}"#,
        );
        CheckPlan::new(&t, 1).unwrap();
        let e = CheckPlan::new(&t, 2).unwrap_err();
        assert!(
            e.to_string().contains("repeat needs an exact text oracle"),
            "{e}"
        );
    }
}
