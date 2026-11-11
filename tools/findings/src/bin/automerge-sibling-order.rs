//! Finding: Automerge 0.12.0 orders concurrent inserts that have the same
//! reference element by op ID, greatest first. An op ID compares by counter
//! first, and then by actor ID bytes. This is the rule of the Lamport-timestamp
//! RGA in Kleppmann and Beresford's JSON CRDT paper. In all cases the rule held.
//! See `tools/findings/README.md`.
//!
//! A base document holds the text "The cat". Each replica forks the base, sets
//! an explicit actor ID, and puts values on a map key to make its counter
//! larger. Then it inserts one character at index 4. Index 4 is after the
//! space, so each insert has the space as its reference element. The probe then
//! gets the merged text in these ways:
//!
//! - Merges in each order (`A.merge(B)` and `B.merge(A)`, and all six orders
//!   for three replicas).
//! - An observer document that receives the changes of the replicas in each
//!   order, with one `apply_changes` call for each replica, and with one call
//!   for all of them. The actors arrive in a different order each time. If the
//!   order of the siblings followed the index of an actor in the actor table,
//!   and not the actor bytes, the text would change.
//! - A save and a load of the first merge.
//!
//! The base actor (`0x80` bytes) sorts after each replica actor (`0x01` to
//! `0x03` bytes). So when a replica actor arrives, Automerge must put it before
//! the base actor in its sorted actor table, and change the actor index of each
//! op that it has.
//!
//! Output: CSV with one row for each case and each way to get the text. The
//! probe fails if a text is not the text that the rule predicts.

use std::cmp::Reverse;

use anyhow::{Context, Result, ensure};
use automerge::transaction::Transactable;
use automerge::{ActorId, AutoCommit, ObjId, ObjType, ROOT, ReadDoc};

/// The bytes of the base actor. It sorts after each replica actor.
const BASE_ACTOR: u8 = 0x80;

/// The bytes of the observer actor. The observer does not write, so this
/// actor does not go into its actor table.
const OBSERVER_ACTOR: u8 = 0xFE;

/// One replica of a case: its name, the bytes of its actor, the number of
/// padding ops before its insert, and the character that it inserts.
type Spec = (&'static str, u8, usize, &'static str);

/// The cases: a name, a description, and the replicas.
const CASES: &[(&str, &str, &[Spec])] = &[
    (
        "1",
        "larger counter A, larger actor A",
        &[("A", 0x02, 1, "q"), ("B", 0x01, 0, "l")],
    ),
    (
        "2",
        "larger counter A, larger actor B",
        &[("A", 0x01, 1, "q"), ("B", 0x02, 0, "l")],
    ),
    (
        "2b",
        "counter A larger by 5, larger actor B",
        &[("A", 0x01, 5, "q"), ("B", 0x02, 0, "l")],
    ),
    (
        "3",
        "larger counter B, larger actor A",
        &[("A", 0x02, 0, "q"), ("B", 0x01, 1, "l")],
    ),
    (
        "4",
        "equal counters, larger actor A",
        &[("A", 0x02, 0, "q"), ("B", 0x01, 0, "l")],
    ),
    (
        "4b",
        "equal counters after 3 padding ops each, larger actor A",
        &[("A", 0x02, 3, "q"), ("B", 0x01, 3, "l")],
    ),
    (
        "5",
        "equal counters, larger actor B",
        &[("A", 0x01, 0, "q"), ("B", 0x02, 0, "l")],
    ),
    (
        "6",
        "three siblings",
        &[("A", 0x01, 0, "q"), ("B", 0x02, 1, "l"), ("C", 0x03, 0, "x")],
    ),
];

/// Returns an actor ID of 16 bytes, each equal to `byte`.
fn actor(byte: u8) -> ActorId {
    ActorId::from(vec![byte; 16])
}

/// The base document and its text object.
struct Base {
    /// The document, with the text "The cat".
    doc: AutoCommit,
    /// The text object, under ROOT key "text".
    text: ObjId,
}

impl Base {
    /// Makes the base document.
    fn new() -> Result<Self> {
        let mut doc = AutoCommit::new();
        doc.set_actor(actor(BASE_ACTOR));
        let text = doc.put_object(ROOT, "text", ObjType::Text)?;
        doc.splice_text(&text, 0, 0, "The cat")?;
        doc.commit();
        Ok(Self { doc, text })
    }

    /// Returns the text of `doc`.
    fn text_of(&self, doc: &AutoCommit) -> Result<String> {
        Ok(doc.text(&self.text)?)
    }
}

/// A replica after its insert.
struct Replica {
    /// The name of the replica, for example "A".
    name: &'static str,
    /// The document of the replica.
    doc: AutoCommit,
    /// The character that the replica inserted.
    ch: &'static str,
    /// The byte of the actor of the replica.
    actor_byte: u8,
    /// The counter of the op ID of the insert.
    counter: u64,
    /// The op ID of the insert, as `counter@actor` with the actor in hex.
    id: String,
}

impl Replica {
    /// Forks `base`, puts `padding` values on ROOT key "pad", and then inserts
    /// `ch` at index 4. Reads the op ID of the insert from the document.
    fn new(base: &mut Base, (name, actor_byte, padding, ch): Spec) -> Result<Self> {
        let mut doc = base.doc.fork();
        doc.set_actor(actor(actor_byte));
        for i in 0..padding {
            doc.put(ROOT, "pad", i64::try_from(i)?)?;
        }
        doc.splice_text(&base.text, 4, 0, ch)?;
        doc.commit();
        let (value, id) = doc
            .get(&base.text, 4)?
            .context("no element at index 4")?;
        ensure!(value.as_str() == Some(ch), "index 4 does not hold {ch:?}");
        let id = id.to_string();
        let counter = id
            .split('@')
            .next()
            .context("op ID has no counter")?
            .parse()?;
        Ok(Self {
            name,
            doc,
            ch,
            actor_byte,
            counter,
            id,
        })
    }

    /// Returns the op ID with one byte of the actor, for example "10@02". All
    /// 16 bytes of each actor in this probe are equal.
    fn short_id(&self) -> String {
        match self.id.split_once('@') {
            Some((counter, actor)) => format!("{counter}@{}", &actor[..2.min(actor.len())]),
            None => self.id.clone(),
        }
    }
}

/// Returns the text that the rule predicts: "The ", then the inserted
/// characters sorted by (counter, actor bytes), greatest first, then "cat".
fn predicted(replicas: &[Replica]) -> String {
    let mut sorted: Vec<&Replica> = replicas.iter().collect();
    sorted.sort_by_key(|r| Reverse((r.counter, r.actor_byte)));
    let middle: String = sorted.iter().map(|r| r.ch).collect();
    format!("The {middle}cat")
}

/// Returns all orders of `items`.
fn permutations<T: Copy>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut out = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let head = rest.remove(i);
        for mut order in permutations(&rest) {
            order.insert(0, head);
            out.push(order);
        }
    }
    out
}

/// Merges the documents in `order` into a copy of the first one.
fn merge_in_order(order: &[&Replica]) -> Result<AutoCommit> {
    let mut doc = order[0].doc.clone();
    for r in &order[1..] {
        doc.merge(&mut r.doc.clone())?;
    }
    Ok(doc)
}

/// Gives the changes of the replicas in `order` to a fork of the base. With
/// `one_batch`, all changes go in one `apply_changes` call. Without it, each
/// replica has its own call.
fn observe(base: &mut Base, order: &[&Replica], one_batch: bool) -> Result<AutoCommit> {
    let base_heads = base.doc.get_heads();
    let mut observer = base.doc.fork();
    observer.set_actor(actor(OBSERVER_ACTOR));
    let mut batch = Vec::new();
    for r in order {
        let changes = r.doc.clone().get_changes(&base_heads);
        if one_batch {
            batch.extend(changes);
        } else {
            observer.apply_changes(changes)?;
        }
    }
    if one_batch {
        observer.apply_changes(batch)?;
    }
    Ok(observer)
}

/// One way to get the merged text of a case, and the text.
struct Row {
    /// How the probe got the text, for example "A.merge(B)".
    how: String,
    /// The merged text.
    text: String,
}

/// Gets the merged text of `replicas` in each way.
fn rows(base: &mut Base, replicas: &[Replica]) -> Result<Vec<Row>> {
    let refs: Vec<&Replica> = replicas.iter().collect();
    let mut rows = Vec::new();
    for order in permutations(&refs) {
        let names: Vec<&str> = order.iter().map(|r| r.name).collect();
        let doc = merge_in_order(&order)?;
        rows.push(Row {
            how: format!("merge into {}", names.join(" <- ")),
            text: base.text_of(&doc)?,
        });
    }
    for order in permutations(&refs) {
        let names = order.iter().map(|r| r.name).collect::<Vec<_>>().join(" ");
        for one_batch in [false, true] {
            let doc = observe(base, &order, one_batch)?;
            let kind = if one_batch { "one batch" } else { "one batch per replica" };
            rows.push(Row {
                how: format!("observer, {kind}, order {names}"),
                text: base.text_of(&doc)?,
            });
        }
    }
    let mut first = merge_in_order(&refs)?;
    let reloaded = AutoCommit::load(&first.save())?;
    rows.push(Row {
        how: "save and load of the first merge".into(),
        text: base.text_of(&reloaded)?,
    });
    Ok(rows)
}

/// The result of one case.
struct CaseResult {
    /// The name of the case.
    name: &'static str,
    /// The description of the case.
    description: &'static str,
    /// The inserts, for example "A q 10@02; B l 9@01".
    inserts: String,
    /// The text that the rule predicts.
    predicted: String,
    /// Each way to get the text, and the text.
    rows: Vec<Row>,
}

/// Runs all cases.
fn run() -> Result<Vec<CaseResult>> {
    let mut base = Base::new()?;
    ensure!(base.text_of(&base.doc)? == "The cat", "wrong base text");
    let mut results = Vec::new();
    for &(name, description, specs) in CASES {
        let replicas = specs
            .iter()
            .map(|&spec| Replica::new(&mut base, spec))
            .collect::<Result<Vec<_>>>()?;
        let inserts = replicas
            .iter()
            .map(|r| format!("{} {} {}", r.name, r.ch, r.short_id()))
            .collect::<Vec<_>>()
            .join("; ");
        results.push(CaseResult {
            name,
            description,
            inserts,
            predicted: predicted(&replicas),
            rows: rows(&mut base, &replicas)?,
        });
    }
    Ok(results)
}

/// Returns `field` as a CSV field: in double quotes, with each double quote
/// written two times.
fn csv(field: &str) -> String {
    format!("\"{}\"", field.replace('"', "\"\""))
}

fn main() -> Result<()> {
    let results = run()?;
    println!("case,description,inserts,how,text,predicted,agrees");
    let mut failures = 0;
    for case in &results {
        for row in &case.rows {
            let agrees = row.text == case.predicted;
            if !agrees {
                failures += 1;
            }
            let fields = [
                case.name,
                case.description,
                &case.inserts,
                &row.how,
                &row.text,
                &case.predicted,
            ];
            let fields: Vec<String> = fields.iter().map(|f| csv(f)).collect();
            println!("{},{agrees}", fields.join(","));
        }
    }
    ensure!(
        failures == 0,
        "{failures} texts do not agree with greatest (counter, actor bytes) first"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every way to get the text gives the text that the rule predicts.
    #[test]
    fn greatest_counter_then_actor_bytes_comes_first() {
        for case in run().unwrap() {
            for row in &case.rows {
                assert_eq!(
                    row.text, case.predicted,
                    "case {} ({}), {}: {}",
                    case.name, case.description, row.how, case.inserts
                );
            }
        }
    }

    /// The probe controls the counters: a sanity check of case 1.
    #[test]
    fn padding_makes_the_counter_larger() {
        let mut base = Base::new().unwrap();
        let a = Replica::new(&mut base, ("A", 0x02, 1, "q")).unwrap();
        let b = Replica::new(&mut base, ("B", 0x01, 0, "l")).unwrap();
        // "The cat" is ops 1-8 (make text, then 7 characters).
        assert_eq!(a.short_id(), "10@02");
        assert_eq!(b.short_id(), "9@01");
    }
}
