use std::collections::HashMap;

use kosha_core::KoshaError;

use super::index::SpFreshIndex;
use super::pq::ProductQuantizer;
use super::types::{
    normalize_options, SpFreshEntry, SpFreshOptions, SpFreshPosting, SpFreshVersion,
};

const MAGIC: &[u8; 8] = b"KSPFRS1\0";
type PqSnapshot = (Option<ProductQuantizer>, HashMap<u32, Vec<u8>>);

impl SpFreshIndex {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        put_u32(&mut buf, self.dimensions as u32);
        put_u32(&mut buf, self.next_posting_id);
        put_u32(&mut buf, self.options.max_posting_len as u32);
        put_u32(&mut buf, self.options.min_posting_len as u32);
        put_u32(&mut buf, self.options.split_neighbor_count as u32);
        put_u32(&mut buf, self.options.boundary_replica_count as u32);
        put_u32(&mut buf, self.options.pq_subvector_count as u32);
        put_u32(&mut buf, self.options.pq_centroids as u32);

        let mut versions: Vec<(u32, SpFreshVersion)> = self
            .version_map
            .iter()
            .map(|(doc_seq, state)| (*doc_seq, *state))
            .collect();
        versions.sort_by_key(|(doc_seq, _)| *doc_seq);
        put_u32(&mut buf, versions.len() as u32);
        for (doc_seq, state) in versions {
            put_u32(&mut buf, doc_seq);
            buf.push(state.version & 0x7f);
            buf.push(u8::from(state.deleted));
            buf.extend_from_slice(&[0, 0]);
        }

        put_u32(&mut buf, self.postings.len() as u32);
        for posting in &self.postings {
            put_u32(&mut buf, posting.id);
            for &value in &posting.centroid {
                put_f32(&mut buf, value);
            }
            put_u32(&mut buf, posting.entries.len() as u32);
            for entry in &posting.entries {
                put_u32(&mut buf, entry.doc_seq);
                buf.push(entry.version & 0x7f);
                buf.push(u8::from(entry.is_replica));
                buf.extend_from_slice(&[0, 0]);
                for &value in &entry.vector {
                    put_f32(&mut buf, value);
                }
            }
        }
        write_pq_snapshot(&mut buf, self.pq.as_ref(), &self.pq_codes);
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Result<Option<Self>, KoshaError> {
        if !data.starts_with(MAGIC) {
            return Ok(None);
        }
        let mut cursor = &data[MAGIC.len()..];
        let dimensions = get_u32(&mut cursor)? as usize;
        let next_posting_id = get_u32(&mut cursor)?;
        let options = normalize_options(SpFreshOptions {
            max_posting_len: get_u32(&mut cursor)? as usize,
            min_posting_len: get_u32(&mut cursor)? as usize,
            split_neighbor_count: get_u32(&mut cursor)? as usize,
            boundary_replica_count: get_u32(&mut cursor)? as usize,
            pq_subvector_count: get_u32(&mut cursor)? as usize,
            pq_centroids: get_u32(&mut cursor)? as usize,
        });

        let version_count = get_u32(&mut cursor)? as usize;
        let mut version_map = HashMap::with_capacity(version_count);
        for _ in 0..version_count {
            let doc_seq = get_u32(&mut cursor)?;
            let version = get_u8(&mut cursor)? & 0x7f;
            let deleted = get_u8(&mut cursor)? != 0;
            skip(&mut cursor, 2)?;
            version_map.insert(doc_seq, SpFreshVersion { version, deleted });
        }

        let posting_count = get_u32(&mut cursor)? as usize;
        let mut postings = Vec::with_capacity(posting_count);
        for _ in 0..posting_count {
            let id = get_u32(&mut cursor)?;
            let centroid = get_f32_vec(&mut cursor, dimensions)?;
            let entry_count = get_u32(&mut cursor)? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let doc_seq = get_u32(&mut cursor)?;
                let version = get_u8(&mut cursor)? & 0x7f;
                let is_replica = get_u8(&mut cursor)? != 0;
                skip(&mut cursor, 2)?;
                let vector = get_f32_vec(&mut cursor, dimensions)?;
                entries.push(SpFreshEntry {
                    doc_seq,
                    version,
                    vector,
                    is_replica,
                });
            }
            postings.push(SpFreshPosting {
                id,
                centroid,
                entries,
            });
        }
        let (pq, pq_codes) = read_pq_snapshot(&mut cursor, dimensions)?;

        Ok(Some(Self {
            options,
            dimensions,
            postings,
            version_map,
            next_posting_id,
            pq,
            pq_codes,
        }))
    }
}

pub fn is_spfresh_vector_index(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

fn write_pq_snapshot(
    buf: &mut Vec<u8>,
    pq: Option<&ProductQuantizer>,
    pq_codes: &HashMap<u32, Vec<u8>>,
) {
    let Some(pq) = pq else {
        put_u32(buf, 0);
        return;
    };
    put_u32(buf, 1);
    put_u32(buf, pq.dimensions as u32);
    put_u32(buf, pq.subvector_count as u32);
    put_u32(buf, pq.centroids_per_subvector as u32);
    put_u32(buf, pq.codebooks.len() as u32);
    for codebook in &pq.codebooks {
        put_u32(buf, codebook.len() as u32);
        for centroid in codebook {
            put_u32(buf, centroid.len() as u32);
            for &value in centroid {
                put_f32(buf, value);
            }
        }
    }
    let mut codes: Vec<_> = pq_codes.iter().collect();
    codes.sort_by_key(|(doc_seq, _)| **doc_seq);
    put_u32(buf, codes.len() as u32);
    for (doc_seq, code) in codes {
        put_u32(buf, *doc_seq);
        put_u32(buf, code.len() as u32);
        buf.extend_from_slice(code);
    }
}

fn read_pq_snapshot(
    cursor: &mut &[u8],
    expected_dimensions: usize,
) -> Result<PqSnapshot, KoshaError> {
    if cursor.is_empty() {
        return Ok((None, HashMap::new()));
    }
    let present = get_u32(cursor)? != 0;
    if !present {
        return Ok((None, HashMap::new()));
    }
    let dimensions = get_u32(cursor)? as usize;
    if dimensions != expected_dimensions {
        return Err(KoshaError::CorruptSegment(format!(
            "spfresh PQ dimensions mismatch: expected {expected_dimensions}, got {dimensions}"
        )));
    }
    let subvector_count = get_u32(cursor)? as usize;
    let centroids_per_subvector = get_u32(cursor)? as usize;
    let codebook_count = get_u32(cursor)? as usize;
    let mut codebooks = Vec::with_capacity(codebook_count);
    for _ in 0..codebook_count {
        let centroid_count = get_u32(cursor)? as usize;
        let mut codebook = Vec::with_capacity(centroid_count);
        for _ in 0..centroid_count {
            let len = get_u32(cursor)? as usize;
            codebook.push(get_f32_vec(cursor, len)?);
        }
        codebooks.push(codebook);
    }
    let code_count = get_u32(cursor)? as usize;
    let mut codes = HashMap::with_capacity(code_count);
    for _ in 0..code_count {
        let doc_seq = get_u32(cursor)?;
        let code_len = get_u32(cursor)? as usize;
        if cursor.len() < code_len {
            return Err(KoshaError::CorruptSegment(
                "truncated spfresh PQ code".into(),
            ));
        }
        let (code, rest) = cursor.split_at(code_len);
        *cursor = rest;
        codes.insert(doc_seq, code.to_vec());
    }
    Ok((
        Some(ProductQuantizer {
            dimensions,
            subvector_count,
            centroids_per_subvector,
            codebooks,
        }),
        codes,
    ))
}

fn put_u32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn put_f32(buf: &mut Vec<u8>, value: f32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn get_u8(cursor: &mut &[u8]) -> Result<u8, KoshaError> {
    if cursor.is_empty() {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let value = cursor[0];
    *cursor = &cursor[1..];
    Ok(value)
}

fn get_u32(cursor: &mut &[u8]) -> Result<u32, KoshaError> {
    if cursor.len() < 4 {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let (bytes, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn get_f32(cursor: &mut &[u8]) -> Result<f32, KoshaError> {
    if cursor.len() < 4 {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    let (bytes, rest) = cursor.split_at(4);
    *cursor = rest;
    Ok(f32::from_le_bytes(bytes.try_into().unwrap()))
}

fn get_f32_vec(cursor: &mut &[u8], dimensions: usize) -> Result<Vec<f32>, KoshaError> {
    let mut values = Vec::with_capacity(dimensions);
    for _ in 0..dimensions {
        values.push(get_f32(cursor)?);
    }
    Ok(values)
}

fn skip(cursor: &mut &[u8], len: usize) -> Result<(), KoshaError> {
    if cursor.len() < len {
        return Err(KoshaError::CorruptSegment(
            "truncated spfresh vector index".into(),
        ));
    }
    *cursor = &cursor[len..];
    Ok(())
}
