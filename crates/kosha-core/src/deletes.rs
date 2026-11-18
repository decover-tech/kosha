//! Immutable, content-addressed per-segment deletion bitmaps.
use crate::{KoshaError, Manifest, NamespaceId, SegmentId};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionVector {
    pub generation: String,
    pub count: u64,
}
impl DeletionVector {
    pub fn path(&self, ns: &NamespaceId, segment: &SegmentId) -> Result<String, KoshaError> {
        if self.generation.len() != 64 || !self.generation.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid("invalid deletion generation"));
        }
        Ok(format!(
            "{}/{}/deletes-{}.roaring",
            ns.0, segment.0, self.generation
        ))
    }
    pub fn encode(ids: &HashSet<u32>) -> Result<(Self, Vec<u8>), KoshaError> {
        let bitmap: RoaringBitmap = ids.iter().copied().collect();
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes)?;
        Ok((
            Self {
                generation: format!("{:x}", Sha256::digest(&bytes)),
                count: bitmap.len(),
            },
            bytes,
        ))
    }
    pub fn decode(&self, bytes: &[u8], doc_count: u32) -> Result<HashSet<u32>, KoshaError> {
        if format!("{:x}", Sha256::digest(bytes)) != self.generation {
            return Err(invalid("deletion bitmap checksum mismatch"));
        }
        let mut input = bytes;
        let bitmap = RoaringBitmap::deserialize_from(&mut input)?;
        if !input.is_empty()
            || bitmap.len() != self.count
            || bitmap.max().is_some_and(|id| id >= doc_count)
        {
            return Err(invalid("invalid deletion bitmap contents"));
        }
        Ok(bitmap.iter().collect())
    }
}
fn invalid(message: &str) -> KoshaError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

/// Resolve all generations before swapping a reader's manifest. Failure must
/// not expose a snapshot with its deletions missing.
pub fn hydrate(
    manifest: &mut Manifest,
    mut read: impl FnMut(&SegmentId, &DeletionVector) -> Result<Vec<u8>, KoshaError>,
) -> Result<(), KoshaError> {
    let mut tombstones = manifest.tombstones.clone();
    for (segment, vector) in &manifest.deletion_vectors {
        let entry = manifest
            .segments
            .iter()
            .find(|e| &e.segment_id == segment)
            .ok_or_else(|| invalid("deletion bitmap references an absent segment"))?;
        let ids = vector.decode(&read(segment, vector)?, entry.doc_count)?;
        tombstones.insert(segment.clone(), ids);
    }
    manifest.tombstones = tombstones;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generations_are_immutable_and_validate_content_and_bounds() {
        let ids = HashSet::from([1, 128, 65536]);
        let (old, bytes) = DeletionVector::encode(&ids).unwrap();
        assert_eq!(old.decode(&bytes, 70000).unwrap(), ids);
        let mut more = ids.clone();
        more.insert(42);
        let (new, newer) = DeletionVector::encode(&more).unwrap();
        assert_ne!(old.generation, new.generation);
        assert!(old.decode(&newer, 70000).is_err());
        assert!(old.decode(&bytes, 100).is_err());
        assert_eq!(old.decode(&bytes, 70000).unwrap(), ids);
        let (_, again) = DeletionVector::encode(&ids).unwrap();
        assert_eq!(bytes, again);
    }
}
