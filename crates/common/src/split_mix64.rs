//! A small deterministic random number generator.

/// The `SplitMix64` generator.
///
/// This crate holds its own copy, so that a new version of a random number crate
/// cannot change the algorithm. The same seed always gives the same values, on
/// all hosts and for all libraries.
pub struct SplitMix64 {
    /// The counter that each draw advances.
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator. Different seeds give different streams.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next value, and advances the state.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    #[test]
    fn the_same_seed_gives_the_same_stream() {
        let seq = |seed| {
            let mut r = SplitMix64::new(seed);
            [r.next_u64(), r.next_u64(), r.next_u64()]
        };
        assert_eq!(seq(42), seq(42), "same seed reproduces the stream");
        assert_ne!(seq(42), seq(43), "different seed diverges");
    }
}
