use anyhow::Result;
use automerge::{AutoCommit, ReadDoc, ROOT, transaction::Transactable};

fn main() -> Result<()> {
    let mut doc = AutoCommit::new();

    doc.put(&ROOT, "title", "replicant")?;
    doc.put(&ROOT, "version", 1_u64)?;
    doc.put(&ROOT, "active", true)?;

    let title = doc.get(&ROOT, "title")?.unwrap().0;
    let version = doc.get(&ROOT, "version")?.unwrap().0;
    let active = doc.get(&ROOT, "active")?.unwrap().0;

    println!("title:   {title}");
    println!("version: {version}");
    println!("active:  {active}");

    Ok(())
}
