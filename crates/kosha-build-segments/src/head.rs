//! Namespace centroid head for cluster-aligned segments (#380, research
//! prototype): `--cluster-layout namespace --cluster-method head`, the
//! default method.
//!
//! A two-level spherical k-means head is trained on a sample of the
//! corpus: `T ≈ √K` top centroids, then each top's members are split into
//! sub-centroids in proportion to its share of the sample, `K` in total
//! (default 16 per segment). The `K` head clusters are laid out in a
//! spatially coherent order (a nearest-neighbour chain over tops, and over
//! each top's subs starting next to the previous top) and packed, whole,
//! into `leaves` segments by estimated size: a head cluster goes to the
//! segment its midpoint falls in. Segments are therefore unions of whole
//! namespace clusters that sit next to each other, never a cut through
//! one, at the price of sizes varying by about one head cluster
//! (~1/16 of a segment by default).
//!
//! Routing a document is `T + K/T` dot products (~120 for K = 3,200).
//!
//! Compared with the hyperplane tree in [`crate::cluster`], which forces
//! exactly equal halves at every level and so can cut a dense region in
//! two, packing whole clusters keeps natural clusters together.

use rayon::prelude::*;

use crate::cluster::normalize;

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Index of the centroid with the largest dot product (nearest by cosine
/// for unit-norm centroids).
fn nearest(v: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best = (f32::NEG_INFINITY, 0);
    for (i, c) in centroids.iter().enumerate() {
        let s = dot(v, c);
        if s > best.0 {
            best = (s, i);
        }
    }
    best.1
}

/// Spherical k-means over `samples[idx]`: `k` unit centroids and each
/// member's assignment (parallel to `idx`). Seeds are distinct random
/// members; an emptied centroid is re-seeded from a random member.
fn spherical_kmeans(
    samples: &[Vec<f32>],
    idx: &[usize],
    k: usize,
    iters: usize,
    rng: &mut u64,
) -> (Vec<Vec<f32>>, Vec<usize>) {
    let dim = samples[idx[0]].len();
    let k = k.clamp(1, idx.len());
    let mut order: Vec<usize> = idx.to_vec();
    for i in (1..order.len()).rev() {
        order.swap(i, (splitmix(rng) % (i as u64 + 1)) as usize);
    }
    let mut cents: Vec<Vec<f32>> = order[..k].iter().map(|&i| samples[i].clone()).collect();
    let mut assign = vec![0usize; idx.len()];
    for it in 0..=iters {
        assign = idx
            .par_iter()
            .map(|&i| nearest(&samples[i], &cents))
            .collect();
        if it == iters {
            break;
        }
        let mut sums = vec![vec![0f32; dim]; k];
        let mut counts = vec![0usize; k];
        for (&i, &a) in idx.iter().zip(&assign) {
            counts[a] += 1;
            for (s, x) in sums[a].iter_mut().zip(&samples[i]) {
                *s += x;
            }
        }
        for (c, (mut s, n)) in cents.iter_mut().zip(sums.into_iter().zip(counts)) {
            if n == 0 {
                s = samples[idx[(splitmix(rng) % idx.len() as u64) as usize]].clone();
            }
            normalize(&mut s);
            *c = s;
        }
    }
    (cents, assign)
}

/// Visiting order over `cents` by a greedy nearest-neighbour chain from
/// `start`.
fn chain_order(cents: &[Vec<f32>], start: usize) -> Vec<usize> {
    let mut left: Vec<usize> = (0..cents.len()).filter(|&i| i != start).collect();
    let mut order = vec![start];
    let mut cur = start;
    while !left.is_empty() {
        let (pos, _) = left
            .iter()
            .enumerate()
            .map(|(p, &i)| (p, dot(&cents[cur], &cents[i])))
            .fold((0, f32::NEG_INFINITY), |b, x| if x.1 > b.1 { x } else { b });
        cur = left.swap_remove(pos);
        order.push(cur);
    }
    order
}

#[derive(Debug, Clone, Copy)]
pub struct HeadParams {
    pub leaves: usize,
    /// Head clusters in total (`K`).
    pub clusters: usize,
    pub iters: usize,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct Head {
    pub dim: usize,
    pub leaves: usize,
    pub tops: Vec<Vec<f32>>,
    /// `subs[t]`: sub-centroids of top `t`.
    pub subs: Vec<Vec<Vec<f32>>>,
    /// `sub_leaf[t][s]`: the segment (leaf) head cluster `(t, s)` packs into.
    pub sub_leaf: Vec<Vec<usize>>,
    /// Sample members per head cluster, same shape as `subs`.
    pub sub_sample: Vec<Vec<usize>>,
}

impl Head {
    /// Trains the head on unit-normalized `samples` and packs its clusters
    /// into `params.leaves` segments.
    pub fn build(samples: &[Vec<f32>], dim: usize, params: HeadParams) -> Self {
        assert!(params.leaves >= 1, "need at least one leaf");
        assert!(
            samples.len() >= 2 * params.clusters.max(params.leaves),
            "cluster sample of {} is too small for {} head clusters / {} leaves (need >= 2 per cluster)",
            samples.len(),
            params.clusters,
            params.leaves
        );
        let mut rng = params.seed | 1;
        let all: Vec<usize> = (0..samples.len()).collect();
        let k = params.clusters.max(params.leaves);
        let t = ((k as f64).sqrt().round() as usize).max(1);
        let (tops, top_assign) = spherical_kmeans(samples, &all, t, params.iters, &mut rng);
        let mut members: Vec<Vec<usize>> = vec![Vec::new(); tops.len()];
        for (&i, &a) in all.iter().zip(&top_assign) {
            members[a].push(i);
        }
        let n = samples.len();
        let mut subs = Vec::with_capacity(tops.len());
        let mut sub_sample = Vec::with_capacity(tops.len());
        for m in &members {
            if m.is_empty() {
                subs.push(Vec::new());
                sub_sample.push(Vec::new());
                continue;
            }
            let kt = ((k as f64 * m.len() as f64 / n as f64).round() as usize).clamp(1, m.len());
            let (c, a) = spherical_kmeans(samples, m, kt, params.iters, &mut rng);
            let mut counts = vec![0usize; c.len()];
            a.iter().for_each(|&x| counts[x] += 1);
            subs.push(c);
            sub_sample.push(counts);
        }

        // Coherent order: chain the tops, and each top's subs starting from
        // the sub nearest the previous top's last sub.
        let top_order = chain_order(&tops, 0);
        let mut sequence: Vec<(usize, usize)> = Vec::with_capacity(k);
        let mut prev: Option<Vec<f32>> = None;
        for &ti in &top_order {
            if subs[ti].is_empty() {
                continue;
            }
            let start = prev.as_ref().map_or(0, |p| nearest(p, &subs[ti]));
            for s in chain_order(&subs[ti], start) {
                sequence.push((ti, s));
            }
            prev = sequence.last().map(|&(a, b)| subs[a][b].clone());
        }
        // Pack by estimated size: a cluster joins the leaf its midpoint
        // falls in.
        let target = n as f64 / params.leaves as f64;
        let mut sub_leaf: Vec<Vec<usize>> = subs.iter().map(|s| vec![0; s.len()]).collect();
        let mut cum = 0f64;
        for &(ti, s) in &sequence {
            let size = sub_sample[ti][s] as f64;
            let mid = cum + size / 2.0;
            sub_leaf[ti][s] = ((mid / target) as usize).min(params.leaves - 1);
            cum += size;
        }
        Head {
            dim,
            leaves: params.leaves,
            tops,
            subs,
            sub_leaf,
            sub_sample,
        }
    }

    /// Head cluster `(top, sub)` of `v` (need not be normalized).
    pub fn route_cluster(&self, v: &[f32]) -> (usize, usize) {
        let mut t = nearest(v, &self.tops);
        if self.subs[t].is_empty() {
            // A top that lost every sample member during training: fall
            // back to the nearest top that has subs.
            t = (0..self.tops.len())
                .filter(|&i| !self.subs[i].is_empty())
                .max_by(|&a, &b| {
                    dot(v, &self.tops[a])
                        .partial_cmp(&dot(v, &self.tops[b]))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .expect("head has clusters");
        }
        (t, nearest(v, &self.subs[t]))
    }

    pub fn route(&self, v: &[f32]) -> usize {
        let (t, s) = self.route_cluster(v);
        self.sub_leaf[t][s]
    }

    pub fn clusters(&self) -> usize {
        self.subs.iter().map(Vec::len).sum()
    }

    /// Writes the head (centroids and packing) as JSON.
    pub fn write_json(
        &self,
        path: &std::path::Path,
        extra: serde_json::Value,
    ) -> std::io::Result<()> {
        let doc = serde_json::json!({
            "format": "kosha-cluster-head",
            "version": 1,
            "dim": self.dim,
            "leaves": self.leaves,
            "clusters": self.clusters(),
            "tops": self.tops,
            "subs": self.subs,
            "sub_leaf": self.sub_leaf,
            "sub_sample": self.sub_sample,
            "build": extra,
        });
        let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
        serde_json::to_writer(&mut f, &doc)?;
        std::io::Write::flush(&mut f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blobs(n: usize, dim: usize, centers: usize, noise: f32, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed;
        let mut normal = || {
            let u = ((splitmix(&mut s) >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
            let v = ((splitmix(&mut s) >> 11) as f64 + 0.5) / (1u64 << 53) as f64;
            ((-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()) as f32
        };
        let cs: Vec<Vec<f32>> = (0..centers)
            .map(|_| {
                let mut c: Vec<f32> = (0..dim).map(|_| normal()).collect();
                normalize(&mut c);
                c
            })
            .collect();
        (0..n)
            .map(|i| {
                let mut v: Vec<f32> = cs[i % centers]
                    .iter()
                    .map(|x| x + noise * normal() / (dim as f32).sqrt())
                    .collect();
                normalize(&mut v);
                v
            })
            .collect()
    }

    fn params(leaves: usize, clusters: usize) -> HeadParams {
        HeadParams {
            leaves,
            clusters,
            iters: 8,
            seed: 11,
        }
    }

    #[test]
    fn every_leaf_is_used_and_sizes_are_near_target() {
        let data = blobs(20_000, 16, 64, 1.0, 1);
        let head = Head::build(&data, 16, params(8, 128));
        let mut counts = [0usize; 8];
        for v in &data {
            counts[head.route(v)] += 1;
        }
        let target = data.len() / 8;
        for (leaf, &c) in counts.iter().enumerate() {
            assert!(
                c > target / 2 && c < target * 3 / 2,
                "leaf {leaf}: {c} docs, target {target}: {counts:?}"
            );
        }
    }

    #[test]
    fn whole_blobs_stay_in_one_leaf() {
        // 16 tight blobs into 4 leaves: packing whole head clusters keeps
        // nearly every blob in a single segment, where input order spreads
        // every blob over all 4.
        let data = blobs(16_000, 32, 16, 0.3, 2);
        let head = Head::build(&data, 32, params(4, 64));
        let mut kept = 0usize;
        for b in 0..16 {
            let mut per_leaf = [0usize; 4];
            for v in data.iter().skip(b).step_by(16) {
                per_leaf[head.route(v)] += 1;
            }
            kept += per_leaf.iter().max().unwrap();
        }
        assert!(
            kept as f64 > 0.9 * data.len() as f64,
            "only {kept} of {} documents share their blob's majority leaf",
            data.len()
        );
    }

    #[test]
    fn route_is_deterministic_and_scale_invariant() {
        let data = blobs(4_000, 8, 10, 1.0, 3);
        let a = Head::build(&data, 8, params(5, 40));
        let b = Head::build(&data, 8, params(5, 40));
        for v in data.iter().take(200) {
            let scaled: Vec<f32> = v.iter().map(|x| x * 7.0).collect();
            assert_eq!(a.route(v), b.route(v));
            assert_eq!(a.route(v), a.route(&scaled));
        }
    }

    #[test]
    fn head_has_about_the_requested_cluster_count() {
        let data = blobs(10_000, 16, 30, 1.0, 4);
        let head = Head::build(&data, 16, params(10, 160));
        let k = head.clusters();
        assert!((120..=200).contains(&k), "got {k} head clusters for 160");
        let leaves: std::collections::BTreeSet<usize> =
            head.sub_leaf.iter().flatten().copied().collect();
        assert_eq!(leaves, (0..10).collect());
    }
}
