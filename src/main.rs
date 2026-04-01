mod doc;

use anyhow::Result;
use automerge::{ROOT, sync};

use crate::doc::InstrumentedDoc;

fn sync_docs(
    a: &mut InstrumentedDoc,
    a_state: &mut sync::State,
    b: &mut InstrumentedDoc,
    b_state: &mut sync::State,
) -> Result<()> {
    loop {
        let a_to_b = a.generate_sync_message(a_state);
        let b_to_a = b.generate_sync_message(b_state);

        if a_to_b.is_none() && b_to_a.is_none() {
            break;
        }

        if let Some(msg) = a_to_b {
            b.receive_sync_message(b_state, msg)?;
        }
        if let Some(msg) = b_to_a {
            a.receive_sync_message(a_state, msg)?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut doc_a = InstrumentedDoc::new();
    let mut doc_b = InstrumentedDoc::new();

    doc_a.put(&ROOT, "written_by", "doc_a")?;
    doc_a.put(&ROOT, "counter", 1_u64)?;

    doc_b.put(&ROOT, "written_by", "doc_b")?;
    doc_b.put(&ROOT, "status", "active")?;

    let mut state_a = sync::State::new();
    let mut state_b = sync::State::new();

    sync_docs(&mut doc_a, &mut state_a, &mut doc_b, &mut state_b)?;

    Ok(())
}
