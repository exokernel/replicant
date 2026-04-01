use automerge::AutoCommit;

fn main() {
    println!("replicant starting");
    let _doc = AutoCommit::new();
    println!("automerge doc created");
}
