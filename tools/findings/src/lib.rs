//! Shared code for the findings probes: the B4 paper trace, a document built
//! from it in each library, a timer, and a small random number generator.
//!
//! Each probe is a binary in `src/bin/`. See `README.md` for the claim of each
//! probe and its expected result.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use automerge::{AutoCommit, ObjId, ObjType, ROOT, ReadDoc, transaction::Transactable};
use flate2::read::GzDecoder;
use serde::Deserialize;
use yrs::{Doc, GetString, Text, TextRef, Transact};

/// A sequential editing trace in the crdt-benchmarks format (B4).
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trace {
    /// The text before the first transaction.
    pub start_content: String,
    /// The text after the last transaction.
    pub end_content: String,
    /// The transactions, in order.
    pub txns: Vec<Txn>,
}

/// One transaction of a [`Trace`].
#[derive(Deserialize)]
pub struct Txn {
    /// The edits, in order. Each edit sees the effect of the edits before it.
    pub patches: Vec<Patch>,
}

/// One edit: at the position, delete the count of characters, then insert the
/// text.
#[derive(Deserialize)]
pub struct Patch(pub usize, pub usize, pub String);

impl Trace {
    /// Reads a gzipped trace.
    ///
    /// The probes use a character position as a byte position. So the trace
    /// must be ASCII, as the B4 paper is.
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let trace: Self = serde_json::from_reader(BufReader::new(GzDecoder::new(file)))
            .with_context(|| format!("parsing {}", path.display()))?;
        ensure!(trace.start_content.is_empty(), "the trace must start empty");
        // Check every insert, not only the final text: a trace can insert text
        // that is not ASCII and delete it later.
        let inserts = trace.txns.iter().flat_map(|t| &t.patches).map(|p| &p.2);
        ensure!(
            trace.end_content.is_ascii() && inserts.clone().all(|text| text.is_ascii()),
            "the trace must be ASCII"
        );
        Ok(trace)
    }

    /// Reads the B4 paper. The path is the first argument, or Replicant's
    /// `traces/editing-traces/automerge-paper.json.gz`.
    pub fn paper_from_args() -> Result<Self> {
        let path = std::env::args().nth(1).map_or_else(
            || {
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../traces/editing-traces/automerge-paper.json.gz")
            },
            PathBuf::from,
        );
        Self::load(&path)
    }
}

/// Builds the trace in Automerge, with one change for each transaction.
///
/// Returns the document and the id of its text object, `text` under ROOT.
pub fn automerge_doc(trace: &Trace) -> Result<(AutoCommit, ObjId)> {
    let mut doc = AutoCommit::new();
    let text = doc.put_object(ROOT, "text", ObjType::Text)?;
    doc.commit();
    for txn in &trace.txns {
        for Patch(pos, del, ins) in &txn.patches {
            doc.splice_text(&text, *pos, isize::try_from(*del)?, ins)?;
        }
        doc.commit();
    }
    ensure!(
        doc.text(&text)? == trace.end_content,
        "Automerge text does not match the trace"
    );
    Ok((doc, text))
}

/// Builds the trace in Yrs, with one transaction for each transaction of the
/// trace.
///
/// Returns the document and its text object, `text`.
pub fn yrs_doc(trace: &Trace) -> Result<(Doc, TextRef)> {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("text");
    for txn in &trace.txns {
        let mut t = doc.transact_mut();
        for Patch(pos, del, ins) in &txn.patches {
            let pos = u32::try_from(*pos)?;
            if *del > 0 {
                text.remove_range(&mut t, pos, u32::try_from(*del)?);
            }
            if !ins.is_empty() {
                text.insert(&mut t, pos, ins);
            }
        }
    }
    ensure!(
        text.get_string(&doc.transact()) == trace.end_content,
        "Yrs text does not match the trace"
    );
    Ok((doc, text))
}

/// Returns the milliseconds since `start`.
pub fn ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e3
}

/// A small linear congruential generator. A seed gives the same positions on
/// every host, so a probe needs no crate for random numbers.
pub struct Lcg(pub u64);

impl Lcg {
    /// Returns a number in `0..n`. `n` must not be zero.
    pub fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let n = u64::try_from(n).expect("usize fits in u64");
        usize::try_from((self.0 >> 33) % n).expect("the result is below n")
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::Trace;

    /// Writes `json` gzipped to a temporary file and loads it.
    fn load(name: &str, json: &str) -> anyhow::Result<Trace> {
        let path =
            std::env::temp_dir().join(format!("findings-{}-{name}.json.gz", std::process::id()));
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(json.as_bytes()).unwrap();
        std::fs::write(&path, gz.finish().unwrap()).unwrap();
        let trace = Trace::load(&path);
        std::fs::remove_file(&path).unwrap();
        trace
    }

    /// A trace whose final text is ASCII, but that inserted text that is not,
    /// is refused.
    #[test]
    fn a_deleted_non_ascii_insert_is_refused() {
        let json = r#"{"startContent":"","endContent":"a","txns":[
            {"patches":[[0,0,"é"]]},{"patches":[[0,1,"a"]]}]}"#;
        let err = load("deleted", json)
            .err()
            .expect("the trace must be refused");
        assert!(err.to_string().contains("ASCII"), "{err}");
        let ok = r#"{"startContent":"","endContent":"a","txns":[{"patches":[[0,0,"a"]]}]}"#;
        load("ok", ok).unwrap();
    }
}
