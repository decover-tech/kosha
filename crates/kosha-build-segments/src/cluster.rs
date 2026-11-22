//! Namespace-level clustering for cluster-aligned segments (#380,
//! research prototype, opt-in via `--cluster-layout namespace`).
//!
//! The default builder cuts the corpus into segments in input order, so
//! every segment is a random sample of the whole vector space and every
//! segment's SPFresh postings have centroids near every query. This module
//! builds a *balanced partition tree* over a sample of the corpus instead:
//! each internal node is a hyperplane found by balanced spherical 2-means,
//! split at the quantile that gives each side its share of the leaves, so
//! the tree has exactly `leaves` leaves of near-equal size. Every document
//! is routed root-to-leaf (one dot product per level, ~8 for 200 leaves)
//! and each leaf becomes one segment. Leaves are numbered in tree order, so
//! neighbouring segment ids hold neighbouring regions of the space.
//!
//! The segment format is unchanged: each leaf is built by the ordinary
//! `SegmentWriter`, whose per-segment SPFresh clustering then splits a
//! spatially coherent region into postings. The union of those posting
//! centroids is the namespace's fine-grained head, and the query path's
//! global probe budget (`knn_plan`) already ranks them namespace-wide.

use std::io::Write;

/// One node of the partition tree, stored flat; children are indices into
/// [`ClusterTree::nodes`].
#[derive(Debug, Clone)]
pub enum Node {
    Leaf {
        leaf: usize,
    },
    Split {
        /// Hyperplane normal (difference of the two unit-norm 2-means
        /// centroids). A document goes left when `cos(x, w)·|w| >= threshold`,
        /// i.e. `x̂·w >= threshold` for the unit-normalized document `x̂`.
        w: Vec<f32>,
        threshold: f32,
        left: usize,
        right: usize,
    },
}

#[derive(Debug, Clone)]
pub struct ClusterTree {
    pub dim: usize,
    pub leaves: usize,
    pub nodes: Vec<Node>,
}

/// Tunables for [`ClusterTree::build`].
#[derive(Debug, Clone, Copy)]
pub struct TreeParams {
    pub leaves: usize,
    pub iters: usize,
    pub seed: u64,
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// Unit-normalize in place; a zero vector stays zero.
pub fn normalize(v: &mut [f32]) {
    let n = dot(v, v).sqrt();
    if n > 0.0 {
        v.iter_mut().for_each(|x| *x /= n);
    }
}

/// splitmix64 — deterministic, dependency-free.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn mean_unit(samples: &[Vec<f32>], idx: &[usize], dim: usize) -> Vec<f32> {
    let mut c = vec![0f32; dim];
    for &i in idx {
        for (a, b) in c.iter_mut().zip(&samples[i]) {
            *a += b;
        }
    }
    normalize(&mut c);
    c
}

/// Threshold that puts the top `left_count` of `scores` (descending) on the
/// left: the midpoint between the last left and first right score.
fn split_threshold(scores: &mut [f32], left_count: usize) -> f32 {
    debug_assert!(left_count > 0 && left_count < scores.len());
    scores.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    0.5 * (scores[left_count - 1] + scores[left_count])
}

impl ClusterTree {
    /// Builds a tree with exactly `params.leaves` leaves from unit-normalized
    /// `samples`. Needs at least two samples per leaf to place every split;
    /// callers size the sample far above that (hundreds per leaf).
    pub fn build(samples: &[Vec<f32>], dim: usize, params: TreeParams) -> Self {
        assert!(params.leaves >= 1, "cluster tree needs at least one leaf");
        assert!(
            samples.len() >= 2 * params.leaves,
            "cluster sample of {} is too small for {} leaves (need >= 2 per leaf)",
            samples.len(),
            params.leaves
        );
        let mut tree = ClusterTree {
            dim,
            leaves: params.leaves,
            nodes: Vec::new(),
        };
        let mut rng = params.seed | 1;
        let all: Vec<usize> = (0..samples.len()).collect();
        tree.build_node(samples, all, params.leaves, 0, params.iters, &mut rng);
        tree
    }

    fn build_node(
        &mut self,
        samples: &[Vec<f32>],
        idx: Vec<usize>,
        leaves: usize,
        first_leaf: usize,
        iters: usize,
        rng: &mut u64,
    ) -> usize {
        let me = self.nodes.len();
        if leaves == 1 {
            self.nodes.push(Node::Leaf { leaf: first_leaf });
            return me;
        }
        self.nodes.push(Node::Leaf { leaf: usize::MAX }); // placeholder
        let left_leaves = leaves / 2;
        let n = idx.len();
        let left_count = ((n as u128 * left_leaves as u128) / leaves as u128) as usize;
        let left_count = left_count.clamp(1, n - 1);

        // Balanced spherical 2-means: seed with a random sample and the
        // sample least similar to it, then alternate (split at the target
        // quantile of the hyperplane score, recompute both centroids).
        let a = idx[(splitmix(rng) % n as u64) as usize];
        let b = *idx
            .iter()
            .min_by(|&&i, &&j| {
                dot(&samples[i], &samples[a])
                    .partial_cmp(&dot(&samples[j], &samples[a]))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap();
        let (mut c1, mut c2) = (samples[a].clone(), samples[b].clone());
        let mut w: Vec<f32> = c1.iter().zip(&c2).map(|(x, y)| x - y).collect();
        let mut threshold = 0.0;
        for it in 0..=iters {
            let mut scored: Vec<(f32, usize)> =
                idx.iter().map(|&i| (dot(&samples[i], &w), i)).collect();
            let mut scores: Vec<f32> = scored.iter().map(|s| s.0).collect();
            threshold = split_threshold(&mut scores, left_count);
            if it == iters {
                break;
            }
            scored.sort_unstable_by(|x, y| {
                y.0.partial_cmp(&x.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            let left: Vec<usize> = scored[..left_count].iter().map(|s| s.1).collect();
            let right: Vec<usize> = scored[left_count..].iter().map(|s| s.1).collect();
            c1 = mean_unit(samples, &left, self.dim);
            c2 = mean_unit(samples, &right, self.dim);
            w = c1.iter().zip(&c2).map(|(x, y)| x - y).collect();
        }
        // Partition by the final hyperplane, the same rule `route` applies.
        let (left_idx, right_idx): (Vec<usize>, Vec<usize>) = {
            let mut scored: Vec<(f32, usize)> =
                idx.iter().map(|&i| (dot(&samples[i], &w), i)).collect();
            scored.sort_unstable_by(|x, y| {
                y.0.partial_cmp(&x.0).unwrap_or(std::cmp::Ordering::Equal)
            });
            (
                scored[..left_count].iter().map(|s| s.1).collect(),
                scored[left_count..].iter().map(|s| s.1).collect(),
            )
        };
        drop(idx);
        let right_leaves = leaves - left_leaves;
        // A degenerate side (fewer samples than leaves, e.g. duplicate
        // vectors) still gets a subtree: borrow from the other side so
        // every leaf exists; its segment may simply end up small.
        let left_idx = ensure_min(left_idx, &right_idx, 2 * left_leaves);
        let right_idx = ensure_min(right_idx, &left_idx, 2 * right_leaves);
        let left = self.build_node(samples, left_idx, left_leaves, first_leaf, iters, rng);
        let right = self.build_node(
            samples,
            right_idx,
            right_leaves,
            first_leaf + left_leaves,
            iters,
            rng,
        );
        self.nodes[me] = Node::Split {
            w,
            threshold,
            left,
            right,
        };
        me
    }

    /// Leaf for `v` (need not be normalized; a zero vector goes to the
    /// leftmost-scoring path deterministically).
    pub fn route(&self, v: &[f32]) -> usize {
        let norm = dot(v, v).sqrt();
        let inv = if norm > 0.0 { 1.0 / norm } else { 0.0 };
        let mut node = 0;
        loop {
            match &self.nodes[node] {
                Node::Leaf { leaf } => return *leaf,
                Node::Split {
                    w,
                    threshold,
                    left,
                    right,
                } => {
                    node = if dot(v, w) * inv >= *threshold {
                        *left
                    } else {
                        *right
                    };
                }
            }
        }
    }

    /// Writes the tree as JSON (hyperplanes included) so a build can be
    /// reproduced and a writer could route new documents the same way.
    pub fn write_json(
        &self,
        path: &std::path::Path,
        extra: serde_json::Value,
    ) -> std::io::Result<()> {
        let nodes: Vec<serde_json::Value> = self
            .nodes
            .iter()
            .map(|n| match n {
                Node::Leaf { leaf } => serde_json::json!({ "leaf": leaf }),
                Node::Split {
                    w,
                    threshold,
                    left,
                    right,
                } => serde_json::json!({
                    "w": w, "threshold": threshold, "left": left, "right": right
                }),
            })
            .collect();
        let doc = serde_json::json!({
            "format": "kosha-cluster-tree",
            "version": 1,
            "dim": self.dim,
            "leaves": self.leaves,
            "nodes": nodes,
            "build": extra,
        });
        let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
        serde_json::to_writer(&mut f, &doc)?;
        f.flush()
    }
}

fn ensure_min(mut side: Vec<usize>, other: &[usize], min: usize) -> Vec<usize> {
    let mut k = 0;
    while side.len() < min && k < other.len() {
        side.push(other[k]);
        k += 1;
    }
    side
}

/// Spill record codec: `u32 id_len | id | u32 text_len | text | dim × f32`,
/// all little-endian. One file per leaf.
pub fn write_record(
    w: &mut impl Write,
    id: &str,
    text: &str,
    vector: &[f32],
) -> std::io::Result<()> {
    w.write_all(&(id.len() as u32).to_le_bytes())?;
    w.write_all(id.as_bytes())?;
    w.write_all(&(text.len() as u32).to_le_bytes())?;
    w.write_all(text.as_bytes())?;
    for x in vector {
        w.write_all(&x.to_le_bytes())?;
    }
    Ok(())
}

/// Decodes every record of a spill file written by [`write_record`].
pub fn read_records(bytes: &[u8], dim: usize) -> Vec<(String, String, Vec<f32>)> {
    let mut out = Vec::new();
    let mut pos = 0;
    let take_u32 = |pos: &mut usize| {
        let v = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        v
    };
    while pos < bytes.len() {
        let n = take_u32(&mut pos);
        let id = String::from_utf8(bytes[pos..pos + n].to_vec()).expect("spill id utf8");
        pos += n;
        let n = take_u32(&mut pos);
        let text = String::from_utf8(bytes[pos..pos + n].to_vec()).expect("spill text utf8");
        pos += n;
        let vec: Vec<f32> = bytes[pos..pos + dim * 4]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|b| f32::from_le_bytes(*b))
            .collect();
        pos += dim * 4;
        out.push((id, text, vec));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gaussian_corpus(n: usize, dim: usize, centers: usize, seed: u64) -> Vec<Vec<f32>> {
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
                let c = &cs[i % centers];
                let mut v: Vec<f32> = c
                    .iter()
                    .map(|x| x + normal() / (dim as f32).sqrt())
                    .collect();
                normalize(&mut v);
                v
            })
            .collect()
    }

    fn params(leaves: usize) -> TreeParams {
        TreeParams {
            leaves,
            iters: 8,
            seed: 7,
        }
    }

    #[test]
    fn tree_has_exactly_the_requested_leaves_in_order() {
        let data = gaussian_corpus(2_000, 16, 20, 1);
        for leaves in [1, 2, 3, 7, 16, 25] {
            let tree = ClusterTree::build(&data, 16, params(leaves));
            let mut seen: Vec<usize> = tree
                .nodes
                .iter()
                .filter_map(|n| match n {
                    Node::Leaf { leaf } => Some(*leaf),
                    _ => None,
                })
                .collect();
            seen.sort();
            assert_eq!(seen, (0..leaves).collect::<Vec<_>>(), "leaves={leaves}");
        }
    }

    #[test]
    fn routing_is_balanced_on_the_training_distribution() {
        let train = gaussian_corpus(8_000, 16, 40, 2);
        let tree = ClusterTree::build(&train, 16, params(10));
        let fresh = gaussian_corpus(20_000, 16, 40, 2);
        let mut counts = vec![0usize; 10];
        for v in &fresh {
            counts[tree.route(v)] += 1;
        }
        let expect = fresh.len() / 10;
        for (leaf, &c) in counts.iter().enumerate() {
            assert!(
                c > expect / 2 && c < expect * 2,
                "leaf {leaf} got {c} of {} (expected ~{expect}): {counts:?}",
                fresh.len()
            );
        }
    }

    #[test]
    fn routing_keeps_a_gaussian_cluster_mostly_in_one_leaf() {
        // 8 well-separated clusters into 8 leaves: most members of each
        // cluster share a leaf (the whole point of cluster alignment),
        // where input-order cutting would spread each over all 8.
        let data = gaussian_corpus(4_000, 32, 8, 3);
        let tree = ClusterTree::build(&data, 32, params(8));
        let mut purity = 0usize;
        for c in 0..8 {
            let mut per_leaf = [0usize; 8];
            for v in data.iter().skip(c).step_by(8) {
                per_leaf[tree.route(v)] += 1;
            }
            purity += per_leaf.iter().max().unwrap();
        }
        assert!(
            purity as f64 > 0.8 * data.len() as f64,
            "only {purity} of {} documents share their cluster's majority leaf",
            data.len()
        );
    }

    #[test]
    fn route_is_scale_invariant() {
        let data = gaussian_corpus(1_000, 8, 10, 4);
        let tree = ClusterTree::build(&data, 8, params(6));
        for v in data.iter().take(100) {
            let scaled: Vec<f32> = v.iter().map(|x| x * 3.5).collect();
            assert_eq!(tree.route(v), tree.route(&scaled));
        }
    }

    #[test]
    fn spill_records_round_trip() {
        let mut buf = Vec::new();
        write_record(&mut buf, "d1", "hello world", &[1.0, -2.5, 3.25]).unwrap();
        write_record(&mut buf, "", "", &[0.0, 0.0, 0.0]).unwrap();
        write_record(&mut buf, "dé", "ünï", &[f32::MIN, f32::MAX, 0.5]).unwrap();
        let back = read_records(&buf, 3);
        assert_eq!(back.len(), 3);
        assert_eq!(back[0].0, "d1");
        assert_eq!(back[0].1, "hello world");
        assert_eq!(back[0].2, vec![1.0, -2.5, 3.25]);
        assert_eq!(back[1].0, "");
        assert_eq!(back[2].0, "dé");
        assert_eq!(back[2].2, vec![f32::MIN, f32::MAX, 0.5]);
    }

    #[test]
    fn tiny_sample_is_rejected() {
        let data = gaussian_corpus(10, 4, 2, 5);
        let r = std::panic::catch_unwind(|| ClusterTree::build(&data, 4, params(8)));
        assert!(r.is_err());
    }
}
