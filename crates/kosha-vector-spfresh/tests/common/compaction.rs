use std::collections::HashSet;

use kosha_vector_spfresh::{mean_vector, ClusterIndex, ClusterIndexConfig, PostingSnapshot};

/// Inputs with fewer vectors than this have their vectors inserted one by
/// one instead of their postings adopted (mirrors kosha-segment's
/// `LIRE_ADOPT_MIN_VECTORS`).
const ADOPT_MIN: usize = 1024;

/// One compaction: `inputs` are the input segments' postings (a big input
/// arrives as its snapshot, a flush-sized one as plain vectors).
pub enum Input {
    Postings(Vec<PostingSnapshot>),
    Vectors(Vec<(u32, Vec<f32>)>),
}

pub fn input_of(vectors: Vec<(u32, Vec<f32>)>, cfg: &ClusterIndexConfig) -> Input {
    if vectors.len() < ADOPT_MIN {
        Input::Vectors(vectors)
    } else {
        Input::Postings(
            ClusterIndex::build(&vectors, cfg.clone())
                .unwrap()
                .snapshot(),
        )
    }
}

/// Candidate postings per absorbed posting (mirrors kosha-segment).
pub const ABSORB_CANDIDATES: usize = 8;

/// What kosha-segment's compaction writer does with the inputs: the
/// largest input's postings (minus deleted members, centroid recomputed
/// where something was dropped) seed the index, every other input is
/// absorbed into them — posting by posting, or vector by vector for an
/// input too small to have adopted postings — and LIRE rebalances.
pub fn lire_merge(
    inputs: Vec<Input>,
    deleted: &HashSet<u32>,
    cfg: &ClusterIndexConfig,
) -> ClusterIndex {
    let mut groups: Vec<Vec<PostingSnapshot>> = Vec::new();
    let mut loose = Vec::new();
    for input in inputs {
        match input {
            Input::Vectors(vs) => {
                loose.extend(vs.into_iter().filter(|(id, _)| !deleted.contains(id)).map(
                    |(id, v)| PostingSnapshot {
                        centroid: v.clone(),
                        entries: vec![(id, v)],
                        radius: 0.0,
                    },
                ))
            }
            Input::Postings(ps) => groups.push(
                ps.into_iter()
                    .map(|p| {
                        let before = p.entries.len();
                        let entries: Vec<(u32, Vec<f32>)> = p
                            .entries
                            .into_iter()
                            .filter(|(id, _)| !deleted.contains(id))
                            .collect();
                        let centroid = if entries.len() == before {
                            p.centroid
                        } else {
                            mean_vector(entries.iter().map(|(_, v)| v.as_slice()), cfg.dim)
                        };
                        PostingSnapshot {
                            centroid,
                            entries,
                            radius: 0.0,
                        }
                    })
                    .collect(),
            ),
        }
    }
    let size = |g: &Vec<PostingSnapshot>| g.iter().map(|p| p.entries.len()).sum::<usize>();
    let base = (0..groups.len()).max_by_key(|&i| size(&groups[i])).unwrap();
    let mut idx = ClusterIndex::from_postings(groups.swap_remove(base), cfg.clone()).unwrap();
    for g in groups {
        idx.absorb_postings(g, ABSORB_CANDIDATES).unwrap();
    }
    idx.absorb_postings(loose, ABSORB_CANDIDATES).unwrap();
    idx.rebalance();
    idx
}
