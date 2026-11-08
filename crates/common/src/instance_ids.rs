//! Seeded library ids for replicas.
//!
//! A CRDT library breaks ties between concurrent ops with the id of the replica
//! that made them. The id can also change the cost of a merge: some pairs of Loro
//! peer ids make a merge about 400 times slower. A random id therefore makes a
//! run differ from the next run. A seeded id makes each run repeatable.

use std::collections::HashSet;

use anyhow::{Result, ensure};

use crate::{INSTANCE_ID_BITS, SplitMix64};

/// Returns `count` ids drawn from `seed`.
///
/// Each id is two draws from a [`SplitMix64`] stream: the first draw is the high
/// 64 bits, and the second is the low 64 bits. The same seed always gives the
/// same ids. The replay tool and the orchestrator both use this function, so
/// both give a replica the same id for the same seed.
#[must_use]
pub fn seeded_instance_ids(seed: u64, count: usize) -> Vec<u128> {
    let mut rng = SplitMix64::new(seed);
    let mut draw = || u128::from(rng.next_u64());
    (0..count).map(|_| (draw() << 64) | draw()).collect()
}

/// Fails if two of `ids` are the same in their low [`INSTANCE_ID_BITS`] bits.
///
/// Each adapter keeps only those bits, so two such ids would give two replicas
/// the same library id.
pub fn ensure_distinct_instance_ids(ids: &[u128]) -> Result<()> {
    let kept: HashSet<u128> = ids
        .iter()
        .map(|id| id & ((1 << INSTANCE_ID_BITS) - 1))
        .collect();
    ensure!(
        kept.len() == ids.len(),
        "two replicas have the same id in the low {INSTANCE_ID_BITS} bits"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ids of seed 1 must not change. The replay tool's seeded ids, and
    /// the results recorded with them, depend on these values. The expected
    /// values were computed outside Rust, with the SplitMix64 reference steps.
    #[test]
    fn seed_one_gives_the_recorded_ids() {
        assert_eq!(
            seeded_instance_ids(1, 3),
            [
                0x910a_2dec_8902_5cc1_beeb_8da1_658e_ec67,
                0xf893_a2ee_fb32_555e_71c1_8690_ee42_c90b,
                0x71bb_54d8_d101_b5b9_c34d_0bff_9015_0280,
            ]
        );
    }

    #[test]
    fn the_same_seed_gives_the_same_ids() {
        assert_eq!(seeded_instance_ids(7, 4), seeded_instance_ids(7, 4));
        assert_ne!(seeded_instance_ids(7, 4), seeded_instance_ids(8, 4));
    }

    #[test]
    fn ids_that_share_their_low_bits_are_rejected() {
        let a = 5u128;
        let b = a | (1 << 100);
        assert!(ensure_distinct_instance_ids(&[a, b]).is_err());
        assert!(ensure_distinct_instance_ids(&[a, a + 1]).is_ok());
    }
}
