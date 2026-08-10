//! A tiny deterministic PRNG (SplitMix64) so index construction, tests, and
//! benches are fully reproducible without pulling in the `rand` crate —
//! matches the fixed-seed-generator convention already used by
//! `kosha-query`'s `segment_memory` bench.

#[derive(Debug, Clone)]
pub struct DeterministicRng(u64);

impl DeterministicRng {
    pub fn new(seed: u64) -> Self {
        // Avoid the degenerate all-zero state.
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// Uniform integer in `[0, bound)`. Returns `0` for `bound == 0`.
    pub fn next_usize(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % bound as u64) as usize
    }

    /// Uniform in `[lo, hi)`.
    pub fn next_f32_range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next_f64() as f32
    }
}
