//! Mergeable quantile state for the `percentiles` aggregation.
//!
//! Small inputs are kept exactly: up to [`EXACT_LIMIT`] values are stored
//! and a percentile is read off the sorted values with linear
//! interpolation. Past that the values move into a DDSketch (Masson, Rim
//! and Lee, VLDB 2019): counts in logarithmic buckets, which bounds the
//! relative error of any reported value by [`RELATIVE_ACCURACY`].
//!
//! Both forms are order-independent. A multiset of values produces the same
//! state, and so the same percentiles, whatever order the values or the
//! per-segment partials arrive in: the exact form is sorted before use, and
//! DDSketch buckets are plain counters. Whether the state ends up exact or
//! sketched depends only on the total number of values. That is the reason
//! for DDSketch over t-digest, whose centroids depend on merge order.

use std::collections::BTreeMap;

/// Values kept exactly before switching to the sketch.
pub(crate) const EXACT_LIMIT: usize = 4096;

/// Relative error bound of a sketched percentile: the reported value `v'`
/// for a true value `v` satisfies `|v' - v| <= RELATIVE_ACCURACY * |v|`.
pub(crate) const RELATIVE_ACCURACY: f64 = 0.01;

#[derive(Debug, Clone, Default)]
pub(crate) struct QuantileState {
    /// Every value seen, while there are at most [`EXACT_LIMIT`] of them.
    exact: Vec<f64>,
    /// Set once the count passes [`EXACT_LIMIT`]; `exact` is then empty.
    sketch: Option<DdSketch>,
}

impl QuantileState {
    pub(crate) fn add(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        match &mut self.sketch {
            Some(sketch) => sketch.add(value, 1),
            None => {
                self.exact.push(value);
                self.spill_if_large();
            }
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        match (&mut self.sketch, &other.sketch) {
            (Some(mine), Some(theirs)) => mine.merge(theirs),
            (Some(mine), None) => other.exact.iter().for_each(|v| mine.add(*v, 1)),
            (None, Some(theirs)) => {
                let mut sketch = theirs.clone();
                self.exact.drain(..).for_each(|v| sketch.add(v, 1));
                self.sketch = Some(sketch);
            }
            (None, None) => {
                self.exact.extend_from_slice(&other.exact);
                self.spill_if_large();
            }
        }
    }

    fn spill_if_large(&mut self) {
        if self.exact.len() > EXACT_LIMIT {
            let mut sketch = DdSketch::default();
            self.exact.drain(..).for_each(|v| sketch.add(v, 1));
            self.exact.shrink_to_fit();
            self.sketch = Some(sketch);
        }
    }

    pub(crate) fn count(&self) -> u64 {
        match &self.sketch {
            Some(sketch) => sketch.count,
            None => self.exact.len() as u64,
        }
    }

    /// Whether percentiles are estimates (the sketch) rather than exact.
    #[cfg(test)]
    pub(crate) fn is_sketched(&self) -> bool {
        self.sketch.is_some()
    }

    /// The values at `percents` (each in `[0, 100]`), `None` when no value
    /// was added.
    pub(crate) fn percentiles(&self, percents: &[f64]) -> Vec<Option<f64>> {
        if self.count() == 0 {
            return vec![None; percents.len()];
        }
        match &self.sketch {
            Some(sketch) => percents
                .iter()
                .map(|p| Some(sketch.quantile(p / 100.0)))
                .collect(),
            None => {
                let mut sorted = self.exact.clone();
                sorted.sort_by(f64::total_cmp);
                percents
                    .iter()
                    .map(|p| Some(exact_quantile(&sorted, p / 100.0)))
                    .collect()
            }
        }
    }
}

/// Quantile `q` in `[0, 1]` of non-empty `sorted`, interpolating linearly
/// between the two closest ranks (rank `q * (n - 1)`).
fn exact_quantile(sorted: &[f64], q: f64) -> f64 {
    let rank = q.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let (a, b) = (sorted[lo], sorted[hi]);
    a + (b - a) * (rank - lo as f64)
}

/// DDSketch with unbounded bucket stores. A positive value `v` lands in
/// bucket `ceil(log_gamma(v))`; negative values use a mirrored store and
/// zero has its own counter. The number of buckets is bounded by the range
/// of `f64` (about 71,000 per sign at 1% accuracy) and in practice by the
/// spread of the data (about 115 buckets per decade).
#[derive(Debug, Clone)]
struct DdSketch {
    positive: BTreeMap<i32, u64>,
    negative: BTreeMap<i32, u64>,
    zero: u64,
    count: u64,
    min: f64,
    max: f64,
}

impl Default for DdSketch {
    fn default() -> Self {
        Self {
            positive: BTreeMap::new(),
            negative: BTreeMap::new(),
            zero: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

impl DdSketch {
    fn gamma() -> f64 {
        (1.0 + RELATIVE_ACCURACY) / (1.0 - RELATIVE_ACCURACY)
    }

    fn bucket(magnitude: f64) -> i32 {
        (magnitude.ln() / Self::gamma().ln()).ceil() as i32
    }

    /// The value reported for a bucket: the point whose relative distance
    /// to both bucket edges is equal.
    fn representative(index: i32) -> f64 {
        let gamma = Self::gamma();
        2.0 * gamma.powi(index) / (gamma + 1.0)
    }

    fn add(&mut self, value: f64, n: u64) {
        if value > 0.0 {
            *self.positive.entry(Self::bucket(value)).or_default() += n;
        } else if value < 0.0 {
            *self.negative.entry(Self::bucket(-value)).or_default() += n;
        } else {
            self.zero += n;
        }
        self.count += n;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn merge(&mut self, other: &Self) {
        for (index, n) in &other.positive {
            *self.positive.entry(*index).or_default() += n;
        }
        for (index, n) in &other.negative {
            *self.negative.entry(*index).or_default() += n;
        }
        self.zero += other.zero;
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Quantile `q` in `[0, 1]` of a non-empty sketch: the representative
    /// of the bucket holding rank `floor(q * (count - 1))`, clamped to the
    /// observed range so the 0th and 100th percentiles are the exact
    /// minimum and maximum.
    fn quantile(&self, q: f64) -> f64 {
        let q = q.clamp(0.0, 1.0);
        if q == 0.0 {
            return self.min;
        }
        if q == 1.0 {
            return self.max;
        }
        let rank = (q * (self.count - 1) as f64).floor() as u64;
        let mut seen = 0u64;
        // Ascending order: most negative first (largest magnitude bucket),
        // then zero, then positives.
        for (index, n) in self.negative.iter().rev() {
            seen += n;
            if seen > rank {
                return (-Self::representative(*index)).clamp(self.min, self.max);
            }
        }
        seen += self.zero;
        if seen > rank {
            return 0.0;
        }
        for (index, n) in &self.positive {
            seen += n;
            if seen > rank {
                return Self::representative(*index).clamp(self.min, self.max);
            }
        }
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// xorshift64*, so the tests need no RNG dependency.
    fn rng(seed: u64) -> impl FnMut() -> f64 {
        let mut state = seed.max(1);
        move || {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            (state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
        }
    }

    fn state_of(values: &[f64]) -> QuantileState {
        let mut state = QuantileState::default();
        values.iter().for_each(|v| state.add(*v));
        state
    }

    fn brute(values: &[f64], percent: f64) -> f64 {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        exact_quantile(&sorted, percent / 100.0)
    }

    #[test]
    fn empty_state_has_no_percentiles() {
        let state = QuantileState::default();
        assert_eq!(state.percentiles(&[50.0, 99.0]), vec![None, None]);
    }

    #[test]
    fn small_inputs_are_exact_with_linear_interpolation() {
        let state = state_of(&[10.0, 20.0, 30.0, 40.0]);
        assert!(!state.is_sketched());
        let got = state.percentiles(&[0.0, 25.0, 50.0, 75.0, 100.0]);
        assert_eq!(
            got,
            vec![Some(10.0), Some(17.5), Some(25.0), Some(32.5), Some(40.0)]
        );
        assert_eq!(
            state_of(&[7.0]).percentiles(&[1.0, 99.0]),
            vec![Some(7.0); 2]
        );
    }

    #[test]
    fn non_finite_values_are_ignored() {
        let state = state_of(&[1.0, f64::NAN, f64::INFINITY, 3.0]);
        assert_eq!(state.count(), 2);
        assert_eq!(state.percentiles(&[50.0]), vec![Some(2.0)]);
    }

    #[test]
    fn large_inputs_stay_within_the_relative_error_bound() {
        let mut next = rng(7);
        // Log-uniform over nine decades, mixed sign, with exact zeros.
        let values: Vec<f64> = (0..50_000)
            .map(|i| {
                if i % 97 == 0 {
                    return 0.0;
                }
                let magnitude = 10f64.powf(next() * 9.0 - 3.0);
                if i % 5 == 0 {
                    -magnitude
                } else {
                    magnitude
                }
            })
            .collect();
        let state = state_of(&values);
        assert!(state.is_sketched());
        assert_eq!(state.count(), values.len() as u64);
        let mut sorted = values.clone();
        sorted.sort_by(f64::total_cmp);
        for percent in [
            0.0, 0.1, 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0, 99.9, 100.0,
        ] {
            let got = state.percentiles(&[percent])[0].unwrap();
            // The sketch reports the value at rank floor(q * (n - 1)).
            let rank = (percent / 100.0 * (sorted.len() - 1) as f64).floor() as usize;
            let want = sorted[rank];
            let bound = RELATIVE_ACCURACY * want.abs() + 1e-12;
            assert!(
                (got - want).abs() <= bound,
                "p{percent}: got {got}, want {want} (bound {bound})"
            );
        }
        assert_eq!(state.percentiles(&[0.0])[0], Some(sorted[0]));
        assert_eq!(state.percentiles(&[100.0])[0], sorted.last().copied());
    }

    #[test]
    fn merging_partials_in_any_order_gives_identical_results() {
        let mut next = rng(11);
        let values: Vec<f64> = (0..20_000).map(|_| next() * 1000.0 - 100.0).collect();
        let percents = [1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0];
        let whole = state_of(&values).percentiles(&percents);

        // Uneven chunks: some stay exact, some spill on their own, and the
        // exact-into-sketch and sketch-into-exact merge arms both run.
        let chunks: Vec<QuantileState> = [100, 5_000, 3, 9_000, 4_096, 1_801]
            .iter()
            .scan(0usize, |start, len| {
                let chunk = state_of(&values[*start..*start + len]);
                *start += len;
                Some(chunk)
            })
            .collect();
        for order in [[0, 1, 2, 3, 4, 5], [5, 4, 3, 2, 1, 0], [2, 0, 4, 1, 5, 3]] {
            let mut merged = QuantileState::default();
            for i in order {
                merged.merge(&chunks[i]);
            }
            assert_eq!(merged.count(), values.len() as u64);
            assert_eq!(merged.percentiles(&percents), whole, "order {order:?}");
        }
    }

    #[test]
    fn merged_small_partials_stay_exact() {
        let values: Vec<f64> = (1..=100).map(f64::from).collect();
        let mut merged = QuantileState::default();
        for chunk in values.chunks(7) {
            merged.merge(&state_of(chunk));
        }
        assert!(!merged.is_sketched());
        for percent in [1.0, 50.0, 99.0] {
            assert_eq!(
                merged.percentiles(&[percent])[0],
                Some(brute(&values, percent))
            );
        }
    }

    #[test]
    fn the_exact_limit_is_the_only_thing_that_decides_the_form() {
        let at_limit: Vec<f64> = (0..EXACT_LIMIT).map(|i| i as f64).collect();
        assert!(!state_of(&at_limit).is_sketched());
        let mut over = state_of(&at_limit);
        over.add(1.0);
        assert!(over.is_sketched());
        let mut merged = state_of(&at_limit[..EXACT_LIMIT / 2]);
        merged.merge(&state_of(&at_limit[EXACT_LIMIT / 2..]));
        assert!(!merged.is_sketched());
        merged.merge(&state_of(&[1.0]));
        assert!(merged.is_sketched());
    }
}
