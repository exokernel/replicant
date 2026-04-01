mod doc;

use anyhow::Result;
use automerge::ROOT;

use crate::doc::InstrumentedDoc;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let mut doc = InstrumentedDoc::new();

    doc.put(&ROOT, "title", "replicant")?;
    doc.put(&ROOT, "version", 1_u64)?;
    doc.put(&ROOT, "active", true)?;

    let title = doc.get(&ROOT, "title")?.unwrap().0;
    let version = doc.get(&ROOT, "version")?.unwrap().0;
    let active = doc.get(&ROOT, "active")?.unwrap().0;

    println!("title:   {title}");
    println!("version: {version}");
    println!("active:  {active}");

    doc.save();

    Ok(())
}
