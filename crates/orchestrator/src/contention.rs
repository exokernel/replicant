//! Measures the anchor contention that a cell's workload actually produces.
//!
//! The [`Locality`](crate::workload::Locality) axis is meant to control anchor
//! contention. `Append` should contest one anchor. `SameRegion` should put
//! every operation on one shared anchor. `RandomPosition` should sit between
//! them. Those are claims about the generated operation stream, so this module
//! checks them.
//!
//! No CRDT runs here. This module reads the same [`PositionStream`] that the
//! runner sends to the replicas, and counts the outcome. The numbers describe
//! the workload of one cell and repetition. They say nothing about any
//! library's behaviour.
//!
//! `docs/divergence-sweep.md` defines anchor, contested anchor, and concurrent
//! sibling.
//!
//! # Model
//!
//! A sequence CRDT attaches each insert to the identity of an existing element.
//! An insert at position `p` attaches to the identity at `p - 1`. An insert at
//! position 0 attaches to a HEAD sentinel. Two operations are *concurrent
//! siblings* when they attach to the same identity and were issued by replicas
//! that had not synced. Ordering those siblings is the merge work the sweep
//! measures.
//!
//! # Why counting `pos == 0` is enough
//!
//! Both replicas of a singleton-group partition start from an empty document
//! and never sync during the divergence phase. Every element identity is
//! therefore created by one replica and visible only to that replica. No
//! identity from replica A can ever be an anchor for replica B. HEAD is the
//! only identity the two replicas share, so HEAD is the only anchor that can be
//! contested. Counting the operations drawn at `pos == 0` is the whole
//! simulation.
//!
//! That argument is load-bearing, so it is tested rather than asserted.
//! `simulate_general` builds the sequences and finds contested anchors
//! without the shortcut, and a test requires the two to agree.
//!
//! # With a baseline
//!
//! A cell with a baseline starts with shared elements, so the argument above does
//! not hold. Each baseline character and HEAD can be contested. An element that a
//! replica inserted during the partition still cannot be, because the other replica
//! does not see it. [`simulate_with_baseline`] tracks which element each position
//! names, and counts the inserts on each shared element. A test requires it to agree
//! with `simulate_general`.
//!
//! # Scope
//!
//! All-singleton groups only, which is the divergence-n2 family. In a
//! multi-node group the replicas sync with each other during the partition, so
//! identities do cross replicas, and a correct simulation would need the real
//! sync schedule. [`simulate`] returns `None` instead of guessing.
//! [`PositionStream`]'s length counter has the same limit.

use crate::scenario::{PartitionConfig, WriteKind};
use crate::workload::PositionStream;

/// The contention one cell's operation stream actually produces.
///
/// Compare these numbers against what the cell's
/// [`Locality`](crate::workload::Locality) setting intends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AchievedContention {
    /// Number of anchors that received inserts from more than one replica.
    /// Every other anchor merges without interleaving.
    pub contested_anchors: usize,
    /// Inserts on all contested anchors, added up across replicas. These are the
    /// operations that the merge must order against another replica's operations.
    pub contested_ops: usize,
    /// Inserts at the most contested anchor, added up across replicas. `0` when
    /// no anchor is contested. This is the main number the `Locality` variants
    /// disagree about.
    pub max_concurrent_siblings: usize,
    /// Operations anchored at HEAD, one entry per replica, in group order. This
    /// is the per-replica breakdown of `max_concurrent_siblings`.
    pub head_children: Vec<usize>,
}

/// Returns the contention one repetition of `config` will produce.
///
/// Returns `None` when the metric has no meaning for the cell. That happens for
/// non-text workloads, which have no anchors, and for non-singleton groups. See
/// the Scope section of the module documentation.
pub fn simulate(config: &PartitionConfig, repetition: usize) -> Option<AchievedContention> {
    if config.workload != WriteKind::TextSplice || !all_singleton(config) {
        return None;
    }
    if config.baseline_len() > 0 {
        return Some(simulate_with_baseline(config, repetition));
    }

    // Only HEAD can be contested (see module docs), so counting `pos == 0`
    // draws per replica is the whole simulation.
    let head_children: Vec<usize> = config
        .groups
        .iter()
        .map(|g| head_children(config, g.nodes[0], repetition))
        .collect();

    Some(summarise(&head_children))
}

/// Counts the inserts that `node` draws at position 0, which anchor at HEAD.
fn head_children(config: &PartitionConfig, node: usize, repetition: usize) -> usize {
    let mut stream = PositionStream::new(config, node, repetition);
    (0..config.ops_per_group)
        .filter(|_| stream.next_pos() == 0)
        .count()
}

/// Turns per-replica HEAD-child counts into the reported summary.
fn summarise(head_children: &[usize]) -> AchievedContention {
    let contributors = head_children.iter().filter(|&&c| c > 0).count();
    let contested = contributors >= 2;
    let siblings = if contested {
        head_children.iter().sum()
    } else {
        0
    };
    AchievedContention {
        contested_anchors: usize::from(contested),
        contested_ops: siblings,
        max_concurrent_siblings: siblings,
        head_children: head_children.to_vec(),
    }
}

/// Returns the contention of a cell with a baseline. See the module documentation.
fn simulate_with_baseline(config: &PartitionConfig, repetition: usize) -> AchievedContention {
    let children: Vec<Vec<u32>> = config
        .groups
        .iter()
        .map(|g| shared_anchor_children(config, g.nodes[0], repetition))
        .collect();

    let mut contested_anchors = 0;
    let mut contested_ops = 0;
    let mut max_concurrent_siblings = 0;
    for anchor in 0..=config.baseline_len() {
        let contributors = children.iter().filter(|c| c[anchor] > 0).count();
        if contributors >= 2 {
            let siblings: usize = children.iter().map(|c| c[anchor] as usize).sum();
            contested_anchors += 1;
            contested_ops += siblings;
            max_concurrent_siblings = max_concurrent_siblings.max(siblings);
        }
    }
    AchievedContention {
        contested_anchors,
        contested_ops,
        max_concurrent_siblings,
        head_children: children.iter().map(|c| c[0] as usize).collect(),
    }
}

/// Returns how many inserts `node` attaches to each shared element. Index 0 is HEAD,
/// and index `i` is baseline character `i - 1`.
fn shared_anchor_children(config: &PartitionConfig, node: usize, repetition: usize) -> Vec<u32> {
    let len = config.baseline_len();
    let mut sequence = Sequence::with_baseline(len);
    let mut stream = PositionStream::new(config, node, repetition);
    let mut children = vec![0u32; len + 1];
    for _ in 0..config.ops_per_group {
        let pos = stream.next_pos();
        let anchor = if pos == 0 { 0 } else { sequence.get(pos - 1) };
        if anchor != OWN_INSERT {
            children[anchor as usize] += 1;
        }
        sequence.insert(pos, OWN_INSERT);
    }
    children
}

/// The value in a [`Sequence`] of an element that the replica itself inserted.
const OWN_INSERT: u32 = u32::MAX;

/// The largest block of a [`Sequence`], before it splits.
const MAX_BLOCK: usize = 2048;

/// One replica's elements in document order, in blocks.
///
/// An insert into one `Vec` costs too much for a cell with 10^5 operations on a
/// baseline of 10^5 characters. This is run at each repetition of a real sweep.
struct Sequence {
    /// The blocks, in document order. No block is empty.
    blocks: Vec<Vec<u32>>,
}

impl Sequence {
    /// Creates a sequence of `len` baseline characters. Character `i` has the value
    /// `i + 1`.
    fn with_baseline(len: usize) -> Self {
        let values: Vec<u32> = (1..=len)
            .map(|i| u32::try_from(i).expect("baseline length fits u32"))
            .collect();
        Self {
            blocks: values.chunks(MAX_BLOCK / 2).map(<[u32]>::to_vec).collect(),
        }
    }

    /// Returns the block that holds `pos`, and the offset of `pos` in that block.
    /// A position after the last element gives the end of the last block.
    fn locate(&self, mut pos: usize) -> (usize, usize) {
        for (b, block) in self.blocks.iter().enumerate() {
            if pos < block.len() {
                return (b, pos);
            }
            pos -= block.len();
        }
        let last = self.blocks.len() - 1;
        (last, self.blocks[last].len() + pos)
    }

    /// Returns the element at `pos`.
    fn get(&self, pos: usize) -> u32 {
        let (b, offset) = self.locate(pos);
        self.blocks[b][offset]
    }

    /// Inserts `value` so that it becomes the element at `pos`.
    fn insert(&mut self, pos: usize, value: u32) {
        let (b, offset) = self.locate(pos);
        let block = &mut self.blocks[b];
        block.insert(offset, value);
        if block.len() > MAX_BLOCK {
            let tail = block.split_off(MAX_BLOCK / 2);
            self.blocks.insert(b + 1, tail);
        }
    }
}

/// True when every group holds exactly one node.
fn all_singleton(config: &PartitionConfig) -> bool {
    !config.groups.is_empty() && config.groups.iter().all(|g| g.nodes.len() == 1)
}

/// Reference implementation. Builds each replica's sequence and finds the
/// contested anchors directly, without the HEAD-only shortcut.
///
/// This exists to keep [`simulate`] honest. It costs `O(ops^2)` for random
/// positions, because it inserts into a `Vec`, so use it only in tests and only
/// on small cells.
#[cfg(test)]
fn simulate_general(config: &PartitionConfig, repetition: usize) -> Option<AchievedContention> {
    if config.workload != WriteKind::TextSplice || !all_singleton(config) {
        return None;
    }

    use std::collections::HashMap;

    /// Element identity: HEAD, a baseline character, or the `seq`-th insert made by
    /// `node`.
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum Anchor {
        Head,
        Baseline(usize),
        Element { node: usize, seq: usize },
    }

    // anchor -> (replicas that anchored here, total children)
    let mut anchors: HashMap<Anchor, (Vec<usize>, usize)> = HashMap::new();

    for group in &config.groups {
        let node = group.nodes[0];
        let mut stream = PositionStream::new(config, node, repetition);
        let mut seq: Vec<Anchor> = (0..config.baseline_len()).map(Anchor::Baseline).collect();

        for i in 0..config.ops_per_group {
            let pos = stream.next_pos();
            let anchor = if pos == 0 { Anchor::Head } else { seq[pos - 1] };
            let entry = anchors.entry(anchor).or_insert_with(|| (Vec::new(), 0));
            if !entry.0.contains(&node) {
                entry.0.push(node);
            }
            entry.1 += 1;
            seq.insert(pos, Anchor::Element { node, seq: i });
        }
    }

    let contested: Vec<usize> = anchors
        .values()
        .filter(|(replicas, _)| replicas.len() >= 2)
        .map(|(_, children)| *children)
        .collect();

    let head_children = config
        .groups
        .iter()
        .map(|g| head_children(config, g.nodes[0], repetition))
        .collect();

    Some(AchievedContention {
        contested_anchors: contested.len(),
        contested_ops: contested.iter().sum(),
        max_concurrent_siblings: contested.into_iter().max().unwrap_or(0),
        head_children,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::WritePattern;
    use crate::workload::Locality;

    fn cell(ops: usize, locality: Locality) -> PartitionConfig {
        PartitionConfig {
            ops_per_group: ops,
            write_pattern: WritePattern::Concentrated,
            workload: WriteKind::TextSplice,
            locality,
            ..PartitionConfig::for_test(2, &[&[0], &[1]])
        }
    }

    /// A cell with a shared start of `length` characters.
    fn baseline_cell(ops: usize, length: usize) -> PartitionConfig {
        PartitionConfig {
            baseline: Some(crate::scenario::Baseline {
                trace: "unused".into(),
                sha256: String::new(),
                length,
            }),
            ..cell(ops, Locality::RandomPosition)
        }
    }

    /// The block sequence must find the same elements as a plain `Vec`. Small
    /// baselines make shared anchors common, and a large cell splits blocks.
    #[test]
    fn baseline_simulation_agrees_with_reference_simulation() {
        for (ops, length) in [
            (1, 1),
            (5, 3),
            (50, 10),
            (200, 50),
            (3000, 100),
            (3000, 5000),
        ] {
            let config = baseline_cell(ops, length);
            for repetition in 1..=3 {
                assert_eq!(
                    simulate(&config, repetition),
                    simulate_general(&config, repetition),
                    "ops={ops} length={length} repetition={repetition}"
                );
            }
        }
    }

    /// This is the point of a baseline: random inserts collide on shared characters,
    /// and more operations give more conflict.
    #[test]
    fn baseline_contention_grows_with_the_operations() {
        let contested = |ops| {
            simulate(&baseline_cell(ops, 10_000), 1)
                .unwrap()
                .contested_ops
        };
        let (few, some, many) = (contested(100), contested(1_000), contested(10_000));
        assert!(few < some && some < many, "{few} {some} {many}");
        assert!(many > 2_000, "{many}");
    }

    /// With no baseline, `random_position` can contest only HEAD.
    #[test]
    fn no_baseline_contests_at_most_head() {
        let c = simulate(&cell(1_000, Locality::RandomPosition), 1).unwrap();
        assert!(c.contested_anchors <= 1);
        assert_eq!(c.contested_ops, c.max_concurrent_siblings);
    }

    /// The shortcut in `simulate` rests on "HEAD is the only shared identity".
    /// The reference implementation checks that by construction, so agreement
    /// across all three localities is what licenses the fast path.
    #[test]
    fn fast_path_agrees_with_reference_simulation() {
        for locality in [
            Locality::Append,
            Locality::RandomPosition,
            Locality::SameRegion,
        ] {
            for ops in [1usize, 2, 10, 200] {
                let c = cell(ops, locality);
                for rep in 1..=3 {
                    assert_eq!(
                        simulate(&c, rep),
                        simulate_general(&c, rep),
                        "locality={locality:?} ops={ops} rep={rep}"
                    );
                }
            }
        }
    }

    /// Never more than one contested anchor, whatever the locality — the
    /// structural consequence of two replicas diverging from an empty document.
    #[test]
    fn at_most_one_anchor_is_ever_contested() {
        for locality in [
            Locality::Append,
            Locality::RandomPosition,
            Locality::SameRegion,
        ] {
            let got = simulate_general(&cell(300, locality), 1).unwrap();
            assert!(
                got.contested_anchors <= 1,
                "locality={locality:?} gave {got:?}"
            );
        }
    }

    /// `Append` contests only the base seam: each replica's very first op is
    /// drawn against an empty document, so it lands at HEAD and nothing else
    /// does. Two siblings total, regardless of cell size.
    #[test]
    fn append_contests_exactly_two_siblings_at_any_size() {
        for ops in [1usize, 100, 10_000] {
            let got = simulate(&cell(ops, Locality::Append), 1).unwrap();
            assert_eq!(got.max_concurrent_siblings, 2, "ops={ops}");
            assert_eq!(got.head_children, vec![1, 1]);
        }
    }

    /// `SameRegion` puts every op at HEAD, so contention is the full op count —
    /// the maximal-contention corner the axis was designed around.
    #[test]
    fn same_region_contests_every_op() {
        for ops in [1usize, 100, 10_000] {
            let got = simulate(&cell(ops, Locality::SameRegion), 1).unwrap();
            assert_eq!(got.max_concurrent_siblings, 2 * ops, "ops={ops}");
            assert_eq!(got.head_children, vec![ops, ops]);
        }
    }

    /// `RandomPosition` hits `pos == 0` with probability `1/(len+1)`, so HEAD
    /// children accumulate like the harmonic series — logarithmic in cell size,
    /// not linear. Bounds are loose enough to be seed-independent.
    #[test]
    fn random_position_contention_grows_logarithmically() {
        let ops = 10_000;
        let got = simulate(&cell(ops, Locality::RandomPosition), 1).unwrap();
        let harmonic: f64 = (1..=ops).map(|i| 1.0 / i as f64).sum();
        let expected_per_replica = harmonic; // ~9.8 at 1e4
        for &c in &got.head_children {
            let c = c as f64;
            assert!(
                c > expected_per_replica * 0.4 && c < expected_per_replica * 2.5,
                "head children {c} far from harmonic expectation {expected_per_replica:.1}"
            );
        }
        assert!(got.max_concurrent_siblings < ops / 100, "{got:?}");
    }

    #[test]
    fn none_for_map_put_and_multi_node_groups() {
        let mut c = cell(100, Locality::Append);
        c.workload = WriteKind::MapPut;
        assert!(simulate(&c, 1).is_none(), "map_put has no anchors");

        let multi = PartitionConfig {
            ops_per_group: 100,
            write_pattern: WritePattern::Concentrated,
            workload: WriteKind::TextSplice,
            ..PartitionConfig::for_test(4, &[&[0, 1], &[2, 3]])
        };
        assert!(
            simulate(&multi, 1).is_none(),
            "intra-group sync is not modelled"
        );
    }

    /// Same cell and repetition must always give the same answer — the metric
    /// is recorded as cell metadata, so it has to be reproducible.
    #[test]
    fn is_deterministic_per_cell_and_repetition() {
        let c = cell(500, Locality::RandomPosition);
        assert_eq!(simulate(&c, 4), simulate(&c, 4));
        assert_ne!(
            simulate(&c, 4).unwrap().head_children,
            simulate(&c, 5).unwrap().head_children,
            "distinct repetitions must draw distinct streams"
        );
    }
}
