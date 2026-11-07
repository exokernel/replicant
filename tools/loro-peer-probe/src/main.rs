//! Which Loro import is slow, for which peer ids? Two documents each insert N chars at
//! position 0 (the same_region cell) with no sync, then each imports the other's update.
use loro::{ExportMode, LoroDoc};
use std::time::Instant;
fn side(peer: u64, n: usize) -> LoroDoc {
    let d = LoroDoc::new();
    d.set_peer_id(peer).unwrap();
    let t = d.get_text("text");
    for _ in 0..n { t.insert(0, "x").unwrap(); d.commit(); }
    d
}
fn main() {
    let n: usize = std::env::args().nth(1).map_or(10_000, |s| s.parse().unwrap());
    let pairs: Vec<(u64, u64, &str)> = vec![
        (0x8000_0000_0000_0001, 0x8000_0000_0000_0002, "adapter asc (low|1<<63)"),
        (0xa5a5_a5a5_0000_0001, 0xa5a5_a5a5_0000_0002, "a5 base asc"),
        (1, 2, "1,2"), (2, 1, "2,1"), (1, u64::MAX - 1, "1,max-1"),
        (0x1234_5678_9abc_def0, 0x0fed_cba9_8765_4321, "random-ish A"),
        (0x0fed_cba9_8765_4321, 0x1234_5678_9abc_def0, "random-ish A swapped"),
        (0x7000_0000_0000_0000, 0x7000_0000_0000_0001, "close, high bit clear"),
        (0x0000_0000_0000_1000, 0xffff_0000_0000_0000, "far apart"),
    ];
    let mut s = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || { s = s.wrapping_add(0x9e37_79b9_7f4a_7c15); let mut z = s; z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9); z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb); z ^ (z >> 31) };
    let mut all = pairs.into_iter().map(|(a, b, l)| (a, b, l.to_string())).collect::<Vec<_>>();
    for i in 0..12 { let (a, b) = (next(), next()); all.push((a, b, format!("random {i}"))); }
    println!("label,peer_a,peer_b,a_imports_b_ms,b_imports_a_ms,a_lt_b");
    for (pa, pb, label) in all {
        let (a, b) = (side(pa, n), side(pb, n));
        let ua = a.export(ExportMode::all_updates()).unwrap();
        let ub = b.export(ExportMode::all_updates()).unwrap();
        let t = Instant::now(); a.import(&ub).unwrap(); let ta = t.elapsed().as_secs_f64() * 1e3;
        let t = Instant::now(); b.import(&ua).unwrap(); let tb = t.elapsed().as_secs_f64() * 1e3;
        assert_eq!(a.get_text("text").to_string(), b.get_text("text").to_string());
        println!("{label},{pa:#018x},{pb:#018x},{ta:.1},{tb:.1},{}", pa < pb);
    }
}
