//! Generates a scenario's write stream: which node receives each write, and,
//! for text workloads, where each insert lands.
//!
//! Every random choice comes from a seeded [`SplitMix64`], so the same inputs
//! replay the same stream against any CRDT library.

use serde::{Deserialize, Serialize};

use crate::scenario::{PartitionConfig, WritePattern};

/// Chooses the position of each insert in a `TextSplice` workload.
///
/// The position is drawn against the issuing replica's own text length, so it
/// is always valid. The `MapPut` workload ignores this setting.
///
/// This is the anchor-contention axis of the sweep. `Append` contests only the
/// seam between the two replicas' runs. `SameRegion` inserts at a fixed shared
/// anchor, so every operation contests the same anchor. `RandomPosition` falls
/// between the two. [`crate::contention`] measures what a cell's stream
/// actually achieves. `docs/divergence-sweep.md` explains the axis and reports
/// the measured contention per variant.
///
/// The discriminants feed [`seed_for`]. Do not renumber or reorder them, or
/// every seed in every recorded provenance file stops matching its run.
///
/// # Precondition: the text object must be shared before the partition
///
/// This axis only reaches the CRDT if both replicas splice into the *same* text
/// object. The replica adapter creates an object on first use. Replicas that
/// diverge from an empty document therefore each create their own
/// `ROOT["text"]`. The heal then resolves a map-key conflict and discards one
/// side's text completely, instead of interleaving two sequences, and every
/// locality measures the same thing.
///
/// A scenario must therefore create the text object on every replica *before*
/// partitioning. A post-heal check must confirm that both sides' inserts
/// survived. For insert-only workloads that check is `final length == total
/// operations`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Locality {
    /// Insert at the current end (`pos = len`). This is the two-authors-typing-
    /// at-the-end case and the low-contention floor of the sweep.
    #[default]
    Append = 0,
    /// Insert at a uniformly random position in `[0, len]`.
    RandomPosition = 1,
    /// Insert at a fixed shared anchor (`pos = 0`, always prepend). This is the
    /// maximum-contention corner of the sweep.
    SameRegion = 2,
}

impl Locality {
    /// Draw the splice position for the next op given the replica's current
    /// text `len`, using `rng` for the random case. Always returns `<= len`,
    /// so the position is a valid `splice_text` anchor.
    pub fn draw_pos(self, rng: &mut SplitMix64, len: usize) -> usize {
        match self {
            Locality::Append => len,
            Locality::SameRegion => 0,
            Locality::RandomPosition => (rng.next_u64() % (len as u64 + 1)) as usize,
        }
    }
}

/// Minimal deterministic PRNG (SplitMix64) — hand-rolled so the workload is
/// bit-reproducible without pinning an external RNG crate's algorithm across
/// versions. Seeds are recorded in results metadata; the same seed replays the
/// identical op stream against any adapter (Automerge, Yrs, …).
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seed the generator. Distinct seeds give independent streams.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next 64-bit output (advances the state).
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Fixed starting value for every divergence-sweep seed.
///
/// Recorded as this constant so a cell's operation streams can be reproduced
/// later. [`seed_for`] mixes it with the cell parameters, the node index, and
/// the repetition number.
pub const DIVERGENCE_SEED_BASE: u64 = 0x5EED_D1F5_0FF5_E7A1;

/// Returns the PRNG seed for one replica in one repetition of one cell.
///
/// The seed is a function of the cell, the replica, and the repetition. Two
/// replicas therefore draw different operation streams, and the same three
/// inputs always replay the identical stream. That replay guarantee is what
/// lets different CRDT libraries be compared on the same workload.
///
/// The mixed value passes through one SplitMix64 round so that a change in any
/// single field changes the whole seed.
pub fn seed_for(config: &PartitionConfig, node: usize, repetition: usize) -> u64 {
    let mixed = DIVERGENCE_SEED_BASE
        ^ (config.ops_per_group as u64).wrapping_mul(0x1_0000_0001)
        ^ ((config.locality as u64) << 3)
        ^ ((node as u64) << 32)
        ^ (repetition as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    SplitMix64::new(mixed).next_u64()
}

/// The insert positions that one replica draws in one repetition of a cell.
///
/// Each position is drawn against the replica's simulated text length. That
/// length starts at 0 and grows by one per insert, so every position is a
/// valid anchor.
///
/// The simulated length is exact for singleton groups. In a multi-node group
/// a replica also receives its peers' inserts during the partition, so its
/// real text is longer. A drawn position is still within the real length.
///
/// [`crate::partition_heal`] sends these positions to the replicas, and
/// [`crate::contention`] reads the same stream to measure it.
pub struct PositionStream {
    locality: Locality,
    rng: SplitMix64,
    len: usize,
}

impl PositionStream {
    /// Starts the stream for `node` in repetition `repetition` of `config`.
    pub fn new(config: &PartitionConfig, node: usize, repetition: usize) -> Self {
        Self {
            locality: config.locality,
            rng: SplitMix64::new(seed_for(config, node, repetition)),
            len: 0,
        }
    }

    /// Returns the next insert position and counts the insert.
    pub fn next_pos(&mut self) -> usize {
        let pos = self.locality.draw_pos(&mut self.rng, self.len);
        self.len += 1;
        pos
    }
}

/// Returns the node that receives operation `i`.
///
/// `nodes` is the set in scope: `0..n` for a topology run, or one group's nodes
/// during a partition.
pub fn target_node(pattern: &WritePattern, i: usize, nodes: &[usize]) -> usize {
    match pattern {
        WritePattern::Concentrated => nodes[0],
        WritePattern::RoundRobin => nodes[i % nodes.len()],
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn text_cell(ops: usize, locality: Locality) -> PartitionConfig {
        PartitionConfig {
            ops_per_group: ops,
            write_pattern: WritePattern::Concentrated,
            workload: crate::scenario::Workload::TextSplice,
            locality,
            ..PartitionConfig::for_test(2, &[&[0], &[1]])
        }
    }

    #[test]
    fn splitmix64_deterministic_and_seed_sensitive() {
        let seq = |seed| {
            let mut r = SplitMix64::new(seed);
            [r.next_u64(), r.next_u64(), r.next_u64()]
        };
        assert_eq!(seq(42), seq(42), "same seed reproduces the stream");
        assert_ne!(seq(42), seq(43), "different seed diverges");
    }

    #[test]
    fn draw_pos_append_and_same_region_are_fixed() {
        let mut r = SplitMix64::new(1);
        assert_eq!(Locality::Append.draw_pos(&mut r, 0), 0);
        assert_eq!(Locality::Append.draw_pos(&mut r, 57), 57);
        assert_eq!(Locality::SameRegion.draw_pos(&mut r, 57), 0);
    }

    #[test]
    fn draw_pos_random_is_valid_anchor_and_deterministic() {
        // A drawn position must always be a valid splice_text anchor (<= len),
        // including the empty-document case (len == 0 => only 0 is valid).
        let mut r = SplitMix64::new(7);
        for len in [0usize, 1, 2, 100, 100_000] {
            for _ in 0..1000 {
                assert!(Locality::RandomPosition.draw_pos(&mut r, len) <= len);
            }
        }
        // Same seed reproduces the identical draw sequence (bit-identical replay).
        let draws = |seed| {
            let mut r = SplitMix64::new(seed);
            (0..8)
                .map(|_| Locality::RandomPosition.draw_pos(&mut r, 50))
                .collect::<Vec<_>>()
        };
        assert_eq!(draws(9), draws(9));
    }

    #[test]
    fn seed_for_is_deterministic() {
        let c = text_cell(100, Locality::RandomPosition);
        assert_eq!(seed_for(&c, 0, 1), seed_for(&c, 0, 1));
    }

    #[test]
    fn seed_for_varies_by_replica_repetition_and_cell() {
        let c = text_cell(100, Locality::RandomPosition);
        let base = seed_for(&c, 0, 1);
        assert_ne!(base, seed_for(&c, 1, 1), "distinct replica must reseed");
        assert_ne!(base, seed_for(&c, 0, 2), "distinct repetition must reseed");
        assert_ne!(
            base,
            seed_for(&text_cell(1000, Locality::RandomPosition), 0, 1),
            "distinct ops-per-side must reseed"
        );
        assert_ne!(
            base,
            seed_for(&text_cell(100, Locality::SameRegion), 0, 1),
            "distinct locality must reseed"
        );
    }

    /// The `Locality` discriminants are part of every seed. These are the
    /// values that all recorded sweeps were run with.
    #[test]
    fn locality_discriminants_are_pinned() {
        assert_eq!(Locality::Append as u64, 0);
        assert_eq!(Locality::RandomPosition as u64, 1);
        assert_eq!(Locality::SameRegion as u64, 2);
    }

    /// Pins the exact seeds and position streams of a 1000-op cell.
    ///
    /// The values were recorded before the stream moved into
    /// [`PositionStream`]. If this test fails, a refactor changed the workload
    /// that recorded sweeps ran, and their results no longer reproduce.
    #[test]
    fn position_streams_match_the_recorded_golden_values() {
        /// FNV-1a over the positions, so 1000 draws fit in one constant.
        fn fnv(positions: &[usize]) -> u64 {
            positions.iter().fold(0xcbf2_9ce4_8422_2325, |h, &p| {
                (h ^ p as u64).wrapping_mul(0x0100_0000_01b3)
            })
        }
        #[rustfmt::skip]
        let golden: [(Locality, usize, usize, u64, [usize; 10], u64); 12] = [
            (Locality::Append, 0, 1, 0x635097f418cda2a0, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 0x988cc5afd8cb65dd),
            (Locality::Append, 0, 2, 0xf145ace2d95c8243, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 0x988cc5afd8cb65dd),
            (Locality::Append, 1, 1, 0x82e557364542cc05, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 0x988cc5afd8cb65dd),
            (Locality::Append, 1, 2, 0x5faf7393b72d0e78, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], 0x988cc5afd8cb65dd),
            (Locality::RandomPosition, 0, 1, 0x74313c1713291981, [0, 1, 0, 2, 3, 4, 1, 2, 7, 0], 0x22a224c3a84f4004),
            (Locality::RandomPosition, 0, 2, 0x45b6574b5ff0c763, [0, 1, 1, 1, 4, 3, 6, 4, 0, 9], 0x6cb5ef6b4f038350),
            (Locality::RandomPosition, 1, 1, 0x00e269ccb8cb0187, [0, 0, 0, 3, 4, 0, 4, 3, 0, 2], 0x85ab068a7e594806),
            (Locality::RandomPosition, 1, 2, 0x52a900e2e70ffb8a, [0, 0, 1, 2, 1, 3, 6, 4, 5, 1], 0x4044ee328e955d03),
            (Locality::SameRegion, 0, 1, 0xe9968847230b8b06, [0; 10], 0x12633b178b17a745),
            (Locality::SameRegion, 0, 2, 0xd1e2630f0efa8674, [0; 10], 0x12633b178b17a745),
            (Locality::SameRegion, 1, 1, 0x72217af0451e0d34, [0; 10], 0x12633b178b17a745),
            (Locality::SameRegion, 1, 2, 0xe1f93da3a8a9bfff, [0; 10], 0x12633b178b17a745),
        ];
        for (locality, node, rep, seed, first, hash) in golden {
            let cell = text_cell(1000, locality);
            assert_eq!(
                seed_for(&cell, node, rep),
                seed,
                "{locality:?} node={node} rep={rep}"
            );
            let mut stream = PositionStream::new(&cell, node, rep);
            let positions: Vec<usize> = (0..1000).map(|_| stream.next_pos()).collect();
            assert_eq!(positions[..10], first, "{locality:?} node={node} rep={rep}");
            assert_eq!(fnv(&positions), hash, "{locality:?} node={node} rep={rep}");
        }
    }
}
