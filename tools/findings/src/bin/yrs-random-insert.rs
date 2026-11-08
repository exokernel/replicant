//! Finding: in Yrs, an insert at a random position gets slower as more random
//! inserts are made.
//!
//! The probe makes 10,000 one-character inserts at random positions, and
//! reports the mean time of one insert for each block of 2,000. It does this
//! for two documents with the same text:
//!
//! - `paper`: the B4 paper, built from its full history.
//! - `one-insert`: the final text of the paper, inserted at once.
//!
//! If the time grows in both documents, the cause is the random inserts, not
//! the history. A random insert splits a block, so the count of blocks grows.
//!
//! Output: CSV with one row for each block of inserts.

use std::time::Instant;

use anyhow::Result;
use findings::{Lcg, Trace, ms_since, yrs_doc};
use yrs::{Doc, GetString, Text, TextRef, Transact};

/// The number of inserts in one block.
const BLOCK: usize = 2_000;
/// The number of blocks.
const BLOCKS: usize = 5;

/// Makes the random inserts in `text`, and prints one CSV row for each block.
fn run(name: &str, doc: &Doc, text: &TextRef) {
    let mut len = text.get_string(&doc.transact()).len();
    let mut rng = Lcg(42);
    for block in 0..BLOCKS {
        let start = Instant::now();
        for _ in 0..BLOCK {
            let pos = u32::try_from(rng.below(len + 1)).expect("the text is shorter than 2^32");
            text.insert(&mut doc.transact_mut(), pos, "x");
            len += 1;
        }
        let inserts_before = block * BLOCK;
        let mean = ms_since(start) / BLOCK as f64;
        println!("yrs-random-insert,{name},{inserts_before},{mean:.4}");
    }
}

fn main() -> Result<()> {
    let trace = Trace::paper_from_args()?;
    let (paper, paper_text) = yrs_doc(&trace)?;

    let flat = Doc::new();
    let flat_text = flat.get_or_insert_text("text");
    flat_text.insert(&mut flat.transact_mut(), 0, &trace.end_content);

    println!("finding,document,inserts_before_block,mean_insert_ms");
    run("paper", &paper, &paper_text);
    run("one-insert", &flat, &flat_text);
    Ok(())
}
