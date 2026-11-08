//! Finding: the time for Automerge to apply one small change grows with the
//! history of the document, and with the position of the change in the text.
//!
//! A source and a target document start equal. The source inserts one
//! character and commits. The target then applies that one change with
//! `load_incremental`. The probe does this 20 times at one position, and
//! reports the mean time of one apply. The position is a fraction of the text
//! length: 0 is the start and 1 is the end. It does this for two documents:
//!
//! - `paper`: the B4 paper, with one change for each of its 259,778
//!   transactions.
//! - `small`: a text of 100 characters, with a history of one change.
//!
//! Output: CSV with one row for each document and position.

use std::time::Instant;

use anyhow::{Context, Result, ensure};
use automerge::transaction::Transactable;
use automerge::{AutoCommit, ObjId, ObjType, ROOT, ReadDoc};
use findings::{Trace, automerge_doc, ms_since};

/// The number of changes that the probe applies at each position.
const CHANGES: usize = 20;

/// The insert positions, as fractions of the text length.
const POSITIONS: [f64; 5] = [0.0, 0.25, 0.5, 0.75, 1.0];

/// Returns the id of the text object `text` under ROOT.
fn text_obj(doc: &AutoCommit) -> Result<ObjId> {
    Ok(doc
        .get(ROOT, "text")?
        .context("the document has no text object")?
        .1)
}

/// Applies `CHANGES` one-character changes at `fraction` of the text, from a
/// copy of `base` to another copy of `base`. Returns the mean time of one
/// `load_incremental`, in ms.
fn mean_apply_ms(base: &[u8], fraction: f64) -> Result<f64> {
    let mut source = AutoCommit::load(base)?;
    let mut target = AutoCommit::load(base)?;
    let text = text_obj(&source)?;
    let len = source.length(&text);
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let pos = (len as f64 * fraction) as usize;
    let mut total_ms = 0.0;
    for _ in 0..CHANGES {
        let heads = source.get_heads();
        source.splice_text(&text, pos, 0, "x")?;
        source.commit();
        let change = source.save_after(&heads);
        let start = Instant::now();
        target.load_incremental(&change)?;
        total_ms += ms_since(start);
    }
    ensure!(
        source.get_heads() == target.get_heads(),
        "the target did not apply every change"
    );
    Ok(total_ms / CHANGES as f64)
}

fn main() -> Result<()> {
    let trace = Trace::paper_from_args()?;
    let (mut paper, _) = automerge_doc(&trace)?;

    let mut small = AutoCommit::new();
    let small_text = small.put_object(ROOT, "text", ObjType::Text)?;
    small.splice_text(&small_text, 0, 0, &"a".repeat(100))?;
    small.commit();

    println!("finding,document,changes_in_history,position,mean_apply_ms");
    for (name, doc) in [("paper", &mut paper), ("small", &mut small)] {
        let history = doc.get_changes(&[]).len();
        let base = doc.save();
        for fraction in POSITIONS {
            let mean = mean_apply_ms(&base, fraction)?;
            println!("automerge-apply-cost,{name},{history},{fraction},{mean:.3}");
        }
    }
    Ok(())
}
