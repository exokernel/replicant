//! The trace format: a causal graph of transactions.
//!
//! A trace is a superset of the [editing-traces] format. A sequential or a
//! concurrent upstream file loads unchanged. A trace adds these items:
//!
//! - `listOps` and `mapOps`, next to `patches`, for list and map workloads.
//! - An `oracle` field, for workloads that have no single final text.
//! - More than one root transaction.
//! - Concurrent inserts at the same position.
//!
//! [editing-traces]: https://github.com/josephg/editing-traces

use std::fmt::Write as _;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use common::{Op, ScalarVal, TEXT_OBJ};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::oracle::OracleSpec;

/// The object that list ops edit.
pub const LIST_OBJ: &str = "list";
/// The object that map ops edit. The empty name means ROOT.
pub const MAP_OBJ: &str = "";

/// The kind of object that a trace edits. One trace edits one kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentKind {
    /// A text object, named [`TEXT_OBJ`].
    Text,
    /// A list object, named [`LIST_OBJ`].
    List,
    /// The root map, named [`MAP_OBJ`].
    Map,
}

impl ContentKind {
    /// Returns the name of the object of this kind.
    #[must_use]
    pub fn obj(self) -> &'static str {
        match self {
            Self::Text => TEXT_OBJ,
            Self::List => LIST_OBJ,
            Self::Map => MAP_OBJ,
        }
    }
}

/// One transaction: ops that one agent applies together.
#[derive(Debug, Clone)]
pub struct Txn {
    /// The agent that applies this transaction.
    pub agent: usize,
    /// Indexes of earlier transactions. The agent must have seen these, and
    /// their ancestors, before it applies this transaction. It must not have
    /// seen any others.
    pub parents: Vec<usize>,
    /// The ops, in order. Each op sees the effect of the ops before it.
    pub ops: Vec<Op>,
}

/// Statistics about a trace's causal graph.
///
/// The graph makes the concurrency of a workload visible in its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GraphStats {
    /// All transactions.
    pub txns: usize,
    /// Transactions with no parents.
    pub roots: usize,
    /// Transactions with more than one parent.
    pub merges: usize,
    /// Transactions with more than one child. A fork starts concurrent work.
    pub forks: usize,
}

/// A loaded and validated trace.
#[derive(Debug, Clone)]
pub struct Trace {
    /// The number of agents. Agent ids are `0..num_agents`.
    pub num_agents: usize,
    /// The transactions. Each parent has a lower index than its child.
    pub txns: Vec<Txn>,
    /// What to check at the end of a replay.
    pub oracle: OracleSpec,
    /// The kind of object that the ops edit.
    pub kind: ContentKind,
    /// True if any inserted text is not ASCII.
    pub non_ascii: bool,
}

/// A trace file as JSON. Unknown fields are ignored, because upstream files have
/// fields that a replay does not use.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTrace {
    /// `"concurrent"` for a causal graph. Absent for a sequential upstream file.
    kind: Option<String>,
    /// The initial text. Must be empty if it is present.
    start_content: Option<String>,
    /// The expected final text.
    end_content: Option<String>,
    /// The check to run. It replaces `end_content`.
    oracle: Option<OracleSpec>,
    /// The number of agents. The default is the highest agent id plus one.
    num_agents: Option<usize>,
    /// The transactions.
    txns: Vec<RawTxn>,
}

/// One transaction as JSON.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTxn {
    /// The agent. Required in a concurrent trace.
    agent: Option<usize>,
    /// The parent indexes. Required in a concurrent trace.
    parents: Option<Vec<usize>>,
    /// Text ops, as `[position, deleted chars, inserted text]`.
    #[serde(default)]
    patches: Vec<(usize, usize, String)>,
    /// List ops, as `[position, deleted values, inserted values]`.
    #[serde(default)]
    list_ops: Vec<(usize, usize, Vec<JsonScalar>)>,
    /// Map ops, as `[key, value]`.
    #[serde(default)]
    map_ops: Vec<(String, JsonScalar)>,
}

/// A scalar in a trace file.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum JsonScalar {
    /// An integer. It becomes [`ScalarVal::Int`], which reads back the same on
    /// every adapter.
    Int(i64),
    /// A string.
    Str(String),
    /// A boolean.
    Bool(bool),
}

impl From<JsonScalar> for ScalarVal {
    fn from(v: JsonScalar) -> Self {
        match v {
            JsonScalar::Int(n) => ScalarVal::Int(n),
            JsonScalar::Str(s) => ScalarVal::Str(s),
            JsonScalar::Bool(b) => ScalarVal::Bool(b),
        }
    }
}

/// Returns the bytes of `path`. Fails if the SHA-256 hash is not `sha256`, when
/// `sha256` is given.
pub fn read_verified(path: &Path, sha256: Option<&str>) -> Result<Vec<u8>> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if let Some(expected) = sha256 {
        let actual = hex(&Sha256::digest(&bytes));
        ensure!(
            actual == expected,
            "{}: SHA-256 is {actual}, the spec expects {expected}. Run `just gen-traces <crdt-benchmarks clone>`.",
            path.display()
        );
    }
    Ok(bytes)
}

/// Returns the lowercase hex form of `bytes`.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, b| {
            // Writing to a String cannot fail.
            let _ = write!(out, "{b:02x}");
            out
        })
}

impl Trace {
    /// Parses a trace from JSON or gzipped JSON, and validates it.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let raw: RawTrace = if bytes.starts_with(&[0x1f, 0x8b]) {
            let mut text = String::new();
            GzDecoder::new(bytes)
                .read_to_string(&mut text)
                .context("decompressing a trace")?;
            serde_json::from_str(&text)
        } else {
            serde_json::from_slice(bytes)
        }
        .context("parsing a trace")?;
        let trace = Self::from_raw(raw)?;
        trace.validate()?;
        Ok(trace)
    }

    /// Converts a parsed file to a trace. Does not validate it.
    fn from_raw(raw: RawTrace) -> Result<Self> {
        if let Some(start) = &raw.start_content {
            ensure!(start.is_empty(), "startContent must be empty");
        }
        // An upstream sequential file has no `kind`, and no agents or parents.
        // Its transactions form one chain by one agent.
        let sequential = raw.kind.is_none();
        let mut non_ascii = false;
        let mut kind = None;
        let mut set_kind = |k: ContentKind| -> Result<()> {
            ensure!(
                kind.is_none_or(|old| old == k),
                "a trace must edit one kind of object"
            );
            kind = Some(k);
            Ok(())
        };
        let mut txns = Vec::with_capacity(raw.txns.len());
        for (i, t) in raw.txns.into_iter().enumerate() {
            let (agent, parents) = if sequential {
                (0, if i == 0 { Vec::new() } else { vec![i - 1] })
            } else {
                (
                    t.agent.with_context(|| format!("txn {i} has no agent"))?,
                    t.parents
                        .with_context(|| format!("txn {i} has no parents"))?,
                )
            };
            let mut ops = Vec::with_capacity(t.patches.len() + t.list_ops.len() + t.map_ops.len());
            for (pos, del_count, insert) in t.patches {
                set_kind(ContentKind::Text)?;
                non_ascii |= !insert.is_ascii();
                ops.push(Op::TextSplice {
                    obj: TEXT_OBJ.into(),
                    pos,
                    del_count,
                    insert,
                });
            }
            for (pos, del_count, values) in t.list_ops {
                set_kind(ContentKind::List)?;
                ops.push(Op::ListSplice {
                    obj: LIST_OBJ.into(),
                    pos,
                    del_count,
                    values: values.into_iter().map(Into::into).collect(),
                });
            }
            for (key, value) in t.map_ops {
                set_kind(ContentKind::Map)?;
                ops.push(Op::MapPut {
                    obj: MAP_OBJ.into(),
                    key,
                    value: value.into(),
                });
            }
            txns.push(Txn {
                agent,
                parents,
                ops,
            });
        }
        let num_agents = raw
            .num_agents
            .unwrap_or_else(|| txns.iter().map(|t| t.agent + 1).max().unwrap_or(0));
        let oracle = match (raw.oracle, raw.end_content) {
            (Some(oracle), _) => oracle,
            (None, Some(text)) => OracleSpec::ExactText { text },
            (None, None) => bail!(
                "a trace needs a final-content check: set endContent or oracle. \
                 A replay without a check can agree on a document that lost work."
            ),
        };
        Ok(Self {
            num_agents,
            txns,
            oracle,
            kind: kind.context("a trace must have at least one op")?,
            non_ascii,
        })
    }

    /// Checks the rules that make a replay well defined.
    ///
    /// The main rule is about positions. A position names an element in the
    /// document that its agent sees. Two libraries can order concurrent inserts
    /// at one position differently. After a merge of such inserts, one position
    /// can name different elements in different libraries, and the libraries
    /// then run different workloads. So:
    ///
    /// - A trace with a merge must use an exact oracle. That oracle holds only
    ///   if no merge has this ambiguity. The final check enforces it.
    /// - A trace with an order-free oracle must have no merge. It can fork, and
    ///   all of its branches merge only at the end of the replay.
    pub fn validate(&self) -> Result<()> {
        let mut clocks: Vec<Vec<u32>> = Vec::with_capacity(self.txns.len());
        let mut authored = vec![0u32; self.num_agents];
        for (i, t) in self.txns.iter().enumerate() {
            ensure!(
                t.agent < self.num_agents,
                "txn {i}: agent {} is out of range for numAgents {}",
                t.agent,
                self.num_agents
            );
            let mut clock = vec![0u32; self.num_agents];
            for &p in &t.parents {
                ensure!(p < i, "txn {i}: parent {p} is not an earlier txn");
                for (c, &pc) in clock.iter_mut().zip(&clocks[p]) {
                    *c = (*c).max(pc);
                }
            }
            // All changes by one agent are in one order. If the agent's last
            // txn is not an ancestor, the agent would be concurrent with itself.
            ensure!(
                clock[t.agent] == authored[t.agent],
                "txn {i}: agent {} has not seen its own earlier txns",
                t.agent
            );
            authored[t.agent] += 1;
            clock[t.agent] = authored[t.agent];
            clocks.push(clock);
        }
        let has_merge = self.txns.iter().any(|t| t.parents.len() > 1);
        match &self.oracle {
            OracleSpec::ExactText { .. } => {
                ensure!(self.kind == ContentKind::Text, "exact text needs text ops");
            }
            OracleSpec::ExactList { .. } => {
                ensure!(self.kind == ContentKind::List, "exact list needs list ops");
            }
            OracleSpec::MapWinner => {
                ensure!(self.kind == ContentKind::Map, "map_winner needs map ops");
            }
            OracleSpec::TreeInvariants => {
                ensure!(
                    self.kind != ContentKind::Map,
                    "tree_invariants is for sequences"
                );
                ensure!(
                    !has_merge,
                    "tree_invariants forbids a merge before the end of the trace: a position \
                     after a merge can name different elements in different libraries"
                );
            }
        }
        Ok(())
    }

    /// True if the transactions form one chain by one agent.
    #[must_use]
    pub fn is_single_chain(&self) -> bool {
        self.num_agents == 1
            && self
                .txns
                .iter()
                .enumerate()
                .all(|(i, t)| t.parents == if i == 0 { Vec::new() } else { vec![i - 1] })
    }

    /// Returns the [`GraphStats`] of this trace.
    #[must_use]
    pub fn graph_stats(&self) -> GraphStats {
        let mut children = vec![0usize; self.txns.len()];
        for t in &self.txns {
            for &p in &t.parents {
                children[p] += 1;
            }
        }
        GraphStats {
            txns: self.txns.len(),
            roots: self.txns.iter().filter(|t| t.parents.is_empty()).count(),
            merges: self.txns.iter().filter(|t| t.parents.len() > 1).count(),
            forks: children.iter().filter(|&&c| c > 1).count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> Result<Trace> {
        Trace::parse(json.as_bytes())
    }

    #[test]
    fn upstream_sequential_file_is_one_chain() {
        let t = parse(
            r#"{"startContent":"","endContent":"ab","txns":[
                {"timestamp":"x","patches":[[0,0,"a"]]},
                {"timestamp":"x","patches":[[1,0,"b"]]}]}"#,
        )
        .unwrap();
        assert!(t.is_single_chain());
        assert_eq!(t.txns[1].parents, vec![0]);
        assert_eq!(t.kind, ContentKind::Text);
    }

    #[test]
    fn upstream_concurrent_file_keeps_its_graph() {
        let t = parse(
            r#"{"kind":"concurrent","endContent":"ab","numAgents":2,"txns":[
                {"parents":[],"numChildren":2,"agent":0,"time":"x","patches":[[0,0,"a"]]},
                {"parents":[0],"numChildren":1,"agent":1,"time":"x","patches":[[1,0,"b"]]},
                {"parents":[1],"numChildren":0,"agent":0,"time":"x","patches":[]}]}"#,
        )
        .unwrap();
        assert_eq!(t.num_agents, 2);
        assert_eq!(t.txns[1].agent, 1);
        assert!(!t.is_single_chain());
    }

    #[test]
    fn a_trace_without_a_final_check_is_rejected() {
        let e = parse(r#"{"txns":[{"patches":[[0,0,"a"]]}]}"#).unwrap_err();
        assert!(format!("{e:#}").contains("final-content check"), "{e:#}");
    }

    #[test]
    fn a_position_after_a_merge_is_rejected_without_an_exact_oracle() {
        let e = parse(
            r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":2,"txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
                {"agent":1,"parents":[],"patches":[[0,0,"b"]]},
                {"agent":0,"parents":[0,1],"patches":[[1,0,"c"]]}]}"#,
        )
        .unwrap_err();
        assert!(format!("{e:#}").contains("forbids a merge"), "{e:#}");
    }

    #[test]
    fn a_forward_parent_is_rejected() {
        let e = parse(
            r#"{"kind":"concurrent","endContent":"a","numAgents":1,"txns":[
                {"agent":0,"parents":[1],"patches":[[0,0,"a"]]}]}"#,
        )
        .unwrap_err();
        assert!(format!("{e:#}").contains("not an earlier txn"), "{e:#}");
    }

    #[test]
    fn an_agent_cannot_be_concurrent_with_itself() {
        let e = parse(
            r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":1,"txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
                {"agent":0,"parents":[],"patches":[[0,0,"b"]]}]}"#,
        )
        .unwrap_err();
        assert!(format!("{e:#}").contains("own earlier txns"), "{e:#}");
    }

    #[test]
    fn a_trace_edits_one_kind_of_object() {
        let e = parse(
            r#"{"endContent":"a","txns":[
                {"patches":[[0,0,"a"]],"listOps":[[0,0,[1]]]}]}"#,
        )
        .unwrap_err();
        assert!(format!("{e:#}").contains("one kind of object"), "{e:#}");
    }

    #[test]
    fn graph_stats_show_the_concurrency() {
        let sequential = parse(
            r#"{"kind":"concurrent","endContent":"ab","numAgents":2,"txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
                {"agent":1,"parents":[0],"patches":[[1,0,"b"]]}]}"#,
        )
        .unwrap();
        assert_eq!(
            sequential.graph_stats(),
            GraphStats {
                txns: 2,
                roots: 1,
                merges: 0,
                forks: 0
            }
        );
        let forked = parse(
            r#"{"kind":"concurrent","oracle":{"kind":"tree_invariants"},"numAgents":3,"txns":[
                {"agent":0,"parents":[],"patches":[[0,0,"a"]]},
                {"agent":1,"parents":[0],"patches":[[1,0,"b"]]},
                {"agent":2,"parents":[0],"patches":[[1,0,"c"]]}]}"#,
        )
        .unwrap();
        assert_eq!(forked.graph_stats().forks, 1);
    }

    #[test]
    fn a_hash_mismatch_is_an_error() {
        let dir = std::env::temp_dir().join(format!("replay-hash-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.json");
        std::fs::write(&path, b"abc").unwrap();
        // The SHA-256 of "abc".
        let good = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        assert!(read_verified(&path, Some(good)).is_ok());
        assert!(read_verified(&path, Some("00")).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
