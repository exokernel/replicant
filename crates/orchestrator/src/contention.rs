//! Achieved-contention metric for the divergence sweep.
//!
//! The [`Locality`](crate::topology::Locality) axis is *intended* to sweep
//! anchor contention: `Append` contests one anchor, `SameRegion` is supposed to
//! pile every op onto a single shared anchor, `RandomPosition` sits between.
//! That is a claim about the generated op stream, and nothing checked it — so
//! this module measures what the stream actually achieves.
//!
//! # Model
//!
//! Sequence CRDTs anchor each insert to an existing element identity. An insert
//! at position `p` anchors to the identity at `p - 1`, or to a HEAD sentinel
//! when `p == 0`. Two ops are *concurrent siblings* when they anchor to the same
//! identity but were issued by replicas that had not synced — that is the
//! interleaving work a merge has to resolve, and the quantity the `Locality`
//! docs make claims about.
//!
//! This replays the same seeded position draws the runner uses (via
//! [`seed_for`]), so the numbers describe the exact op stream a given cell and
//! repetition will produce. It is a property of the workload, not a measurement
//! of any CRDT implementation.
//!
//! # Why counting `pos == 0` is sufficient
//!
//! Both replicas of a singleton-group partition start from an empty document and
//! never sync during the divergence phase. So every element identity is created
//! by, and visible only to, the replica that made it: no identity from replica A
//! can ever be an anchor for replica B. HEAD is the only identity they share,
//! and therefore the only anchor that can possibly be contested — which reduces
//! the whole simulation to counting ops drawn at `pos == 0`.
//!
//! That is a load-bearing argument, so it is not merely asserted:
//! [`simulate_general`] materialises the sequences and finds contested anchors
//! the slow way, and the tests assert the two agree.
//!
//! # Scope
//!
//! All-singleton groups only (the divergence-n2 family). With multi-node groups
//! the replicas inside a group sync during the partition, so identities do cross
//! replicas and a faithful simulation would need the real sync schedule — the
//! same limitation `run_partition_heal` documents for its own length counter.
//! [`simulate`] returns `None` rather than guess.

use crate::runner::seed_for;
use crate::topology::{PartitionConfig, SplitMix64, Workload};

/// What a cell's op stream actually achieves, as opposed to intends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AchievedContention {
    /// Anchors receiving children from more than one replica. Everything else
    /// merges without interleaving.
    pub contested_anchors: usize,
    /// Children at the most-contested anchor, summed across replicas. `0` when
    /// no anchor is contested. This is the headline number the `Locality`
    /// variants make competing claims about.
    pub max_concurrent_siblings: usize,
    /// Ops anchored at HEAD per replica, in group order — the per-side
    /// breakdown behind `max_concurrent_siblings`.
    pub head_children: Vec<usize>,
}

/// Simulate achieved contention for one repetition of `config`.
///
/// `None` when the metric is not defined for the cell: non-text workloads (no
/// anchors) or non-singleton groups (see the module docs on scope).
pub fn simulate(config: &PartitionConfig, repetition: usize) -> Option<AchievedContention> {
    if config.workload != Workload::TextSplice || !all_singleton(config) {
        return None;
    }

    // Only HEAD can be contested (see module docs), so counting `pos == 0`
    // draws per replica is the whole simulation.
    let head_children: Vec<usize> = config
        .groups
        .iter()
        .map(|g| {
            let node = g.nodes[0];
            let mut rng = SplitMix64::new(seed_for(config, node, repetition));
            (0..config.ops_per_group)
                .filter(|&len| config.locality.draw_pos(&mut rng, len) == 0)
                .count()
        })
        .collect();

    Some(summarise(&head_children))
}

/// Roll per-replica HEAD-child counts into the reported summary.
fn summarise(head_children: &[usize]) -> AchievedContention {
    let contributors = head_children.iter().filter(|&&c| c > 0).count();
    let contested = contributors >= 2;
    AchievedContention {
        contested_anchors: usize::from(contested),
        max_concurrent_siblings: if contested {
            head_children.iter().sum()
        } else {
            0
        },
        head_children: head_children.to_vec(),
    }
}

/// True when every group holds exactly one node.
fn all_singleton(config: &PartitionConfig) -> bool {
    !config.groups.is_empty() && config.groups.iter().all(|g| g.nodes.len() == 1)
}

/// Reference implementation: materialise each replica's sequence and find
/// contested anchors directly, without the HEAD-only shortcut.
///
/// `O(ops^2)` for random positions because it inserts into a `Vec`, so this is
/// for tests and small cells only — its job is to keep [`simulate`] honest.
#[cfg(test)]
fn simulate_general(config: &PartitionConfig, repetition: usize) -> Option<AchievedContention> {
    if config.workload != Workload::TextSplice || !all_singleton(config) {
        return None;
    }

    use std::collections::HashMap;

    /// Element identity: HEAD, or the `seq`-th insert made by `node`.
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum Anchor {
        Head,
        Element { node: usize, seq: usize },
    }

    // anchor -> (replicas that anchored here, total children)
    let mut anchors: HashMap<Anchor, (Vec<usize>, usize)> = HashMap::new();

    for group in &config.groups {
        let node = group.nodes[0];
        let mut rng = SplitMix64::new(seed_for(config, node, repetition));
        let mut seq: Vec<Anchor> = Vec::new();

        for i in 0..config.ops_per_group {
            let pos = config.locality.draw_pos(&mut rng, seq.len());
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
        .map(|g| {
            let node = g.nodes[0];
            let mut rng = SplitMix64::new(seed_for(config, node, repetition));
            (0..config.ops_per_group)
                .filter(|&len| config.locality.draw_pos(&mut rng, len) == 0)
                .count()
        })
        .collect();

    Some(AchievedContention {
        contested_anchors: contested.len(),
        max_concurrent_siblings: contested.into_iter().max().unwrap_or(0),
        head_children,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Group, HealTopology, Locality, WritePattern};

    fn cell(ops: usize, locality: Locality) -> PartitionConfig {
        PartitionConfig {
            node_count: 2,
            groups: vec![Group { nodes: vec![0] }, Group { nodes: vec![1] }],
            ops_per_group: ops,
            write_pattern: WritePattern::Concentrated,
            workload: Workload::TextSplice,
            locality,
            heal_topology: HealTopology::FullMesh,
        }
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
        c.workload = Workload::MapPut;
        assert!(simulate(&c, 1).is_none(), "map_put has no anchors");

        let multi = PartitionConfig {
            node_count: 4,
            groups: vec![Group { nodes: vec![0, 1] }, Group { nodes: vec![2, 3] }],
            ops_per_group: 100,
            write_pattern: WritePattern::Concentrated,
            workload: Workload::TextSplice,
            locality: Locality::Append,
            heal_topology: HealTopology::FullMesh,
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
