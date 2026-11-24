//! Read-only boundary replication for offline experiments. Work from a
//! live snapshot after build or rebalance; never feed replicas back to LIRE.

use crate::{dot, normalize_in_place, PostingSnapshot};

#[derive(Debug, Clone, Copy)]
pub enum Assignment {
    /// Relative Euclidean distance cutoff and relative-neighborhood filter.
    /// `max_copies` includes the original posting.
    Spann { epsilon: f32, max_copies: usize },
    /// At most one additional posting, penalizing correlated residuals.
    Soar { lambda: f32 },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Replica {
    pub doc_seq: u32,
    pub posting: usize,
    /// Lower is better when an experiment caps the total number of spills.
    pub priority: f32,
}

/// Select secondary postings for the members of one segment. Uses its
/// existing owner as the primary (even if balancing moved it away from the
/// exact nearest centroid). Vectors must be unit-normalized. Returned
/// assignments reference snapshot ordinals and contain no vector payloads.
pub fn boundary_replicas(postings: &[PostingSnapshot], assignment: Assignment) -> Vec<Replica> {
    match assignment {
        Assignment::Spann {
            epsilon,
            max_copies,
        } => {
            assert!(epsilon.is_finite() && epsilon >= 0.0);
            if max_copies <= 1 {
                return Vec::new();
            }
        }
        Assignment::Soar { lambda } => assert!(lambda.is_finite() && lambda >= 0.0),
    }
    if postings.len() < 2 {
        return Vec::new();
    }

    let centroids: Vec<_> = postings
        .iter()
        .map(|p| {
            let mut centroid = p.centroid.clone();
            normalize_in_place(&mut centroid);
            centroid
        })
        .collect();
    let center_norms: Vec<_> = centroids.iter().map(|c| dot(c, c)).collect();
    let center_distances: Vec<Vec<_>> = centroids
        .iter()
        .enumerate()
        .map(|(i, c)| {
            centroids
                .iter()
                .zip(&center_norms)
                .map(|(other, &norm)| (center_norms[i] + norm - 2.0 * dot(c, other)).max(0.0))
                .collect()
        })
        .collect();
    let mut distances = vec![0.0; postings.len()];
    let mut candidates = Vec::with_capacity(postings.len());
    let mut accepted = Vec::new();
    let mut replicas = Vec::new();
    for (owner, posting) in postings.iter().enumerate() {
        for (doc_seq, vector) in &posting.entries {
            let norm = dot(vector, vector);
            for ((distance, center), &center_norm) in
                distances.iter_mut().zip(&centroids).zip(&center_norms)
            {
                *distance = (norm + center_norm - 2.0 * dot(vector, center)).max(0.0);
            }
            let primary = distances[owner];
            match assignment {
                Assignment::Spann {
                    epsilon,
                    max_copies,
                } => {
                    let limit = (1.0 + epsilon).powi(2) * primary;
                    candidates.clear();
                    candidates.extend(
                        (0..postings.len()).filter(|&j| j != owner && distances[j] <= limit),
                    );
                    candidates.sort_unstable_by(|&a, &b| {
                        distances[a].total_cmp(&distances[b]).then(a.cmp(&b))
                    });
                    accepted.clear();
                    accepted.push(owner);
                    for &j in &candidates {
                        if accepted
                            .iter()
                            .any(|&i| center_distances[i][j] < distances[j])
                        {
                            continue;
                        }
                        accepted.push(j);
                        replicas.push(Replica {
                            doc_seq: *doc_seq,
                            posting: j,
                            priority: distances[j] / primary.max(f32::EPSILON),
                        });
                        if accepted.len() == max_copies {
                            break;
                        }
                    }
                }
                Assignment::Soar { lambda } => {
                    let (posting, cost) = distances
                        .iter()
                        .enumerate()
                        .filter(|&(j, _)| j != owner)
                        .map(|(j, &secondary)| {
                            // Polarization identity: r - r' = c' - c.
                            let ip = 0.5 * (primary + secondary - center_distances[owner][j]);
                            let projection = if primary > f32::EPSILON {
                                (ip * ip / primary).min(secondary)
                            } else {
                                0.0
                            };
                            (j, secondary + lambda * projection)
                        })
                        .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
                        .expect("at least one secondary posting");
                    replicas.push(Replica {
                        doc_seq: *doc_seq,
                        posting,
                        priority: cost - primary,
                    });
                }
            }
        }
    }
    replicas
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterIndex, ClusterIndexConfig};

    fn posting(centroid: &[f32], document: Option<&[f32]>) -> PostingSnapshot {
        PostingSnapshot {
            centroid: centroid.to_vec(),
            entries: document.into_iter().map(|v| (8, v.to_vec())).collect(),
            radius: 0.0,
        }
    }

    #[test]
    fn spann_uses_relative_cutoff_and_filters_close_centroids() {
        let at = |theta: f32| [theta.cos(), theta.sin()];
        let postings = [
            posting(&at(-0.2), Some(&at(0.0))),
            posting(&at(0.22), None),
            posting(&at(0.23), None),
            posting(&at(-0.42), None),
        ];
        let none = boundary_replicas(
            &postings,
            Assignment::Spann {
                epsilon: 0.05,
                max_copies: 4,
            },
        );
        assert!(none.is_empty());
        let spills = boundary_replicas(
            &postings,
            Assignment::Spann {
                epsilon: 0.2,
                max_copies: 4,
            },
        );
        assert_eq!(
            spills
                .iter()
                .map(|r| (r.doc_seq, r.posting))
                .collect::<Vec<_>>(),
            [(8, 1)]
        );
    }

    #[test]
    fn soar_can_prefer_an_orthogonal_residual_to_the_nearer_centroid() {
        let postings = [
            posting(&[0.8, 0.6, 0.0], Some(&[1.0, 0.0, 0.0])),
            posting(&[0.7, 0.71414286, 0.0], None),
            posting(&[0.6, 0.0, 0.8], None),
        ];
        assert_eq!(
            boundary_replicas(&postings, Assignment::Soar { lambda: 0.0 })[0].posting,
            1
        );
        assert_eq!(
            boundary_replicas(&postings, Assignment::Soar { lambda: 1.0 })[0].posting,
            2
        );
    }

    #[test]
    fn rebuilt_plan_excludes_tombstones_and_preserves_single_ownership() {
        let postings = vec![
            posting(&[1.0, 0.0], Some(&[1.0, 0.0])),
            PostingSnapshot {
                centroid: vec![0.0, 1.0],
                entries: vec![(9, vec![0.0, 1.0]), (10, vec![0.0, 1.0])],
                radius: 0.0,
            },
        ];
        let mut cfg = ClusterIndexConfig::new(2);
        cfg.min_posting_size = 1;
        let mut index = ClusterIndex::from_postings(postings, cfg).unwrap();
        assert!(index.delete(9));
        index.rebalance();
        let snapshot = index.snapshot();
        let spills = boundary_replicas(&snapshot, Assignment::Soar { lambda: 1.0 });
        assert_eq!(spills.len(), 2);
        for spill in spills {
            assert!(matches!(spill.doc_seq, 8 | 10));
            assert!(!snapshot[spill.posting]
                .entries
                .iter()
                .any(|(id, _)| *id == spill.doc_seq));
        }
        assert_eq!(index.snapshot(), snapshot);
    }
}
