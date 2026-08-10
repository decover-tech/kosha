//! Crate-local error type. Deliberately not `kosha_core::KoshaError` — this
//! crate is a standalone prototype with no dependency on the rest of kosha
//! (see README.md).

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorIndexError {
    DimensionMismatch { expected: usize, got: usize },
    InvalidConfig(&'static str),
    DuplicateId(u32),
}

impl fmt::Display for VectorIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorIndexError::DimensionMismatch { expected, got } => {
                write!(
                    f,
                    "vector dimension mismatch: expected {expected}, got {got}"
                )
            }
            VectorIndexError::InvalidConfig(msg) => write!(f, "invalid ClusterIndexConfig: {msg}"),
            VectorIndexError::DuplicateId(id) => write!(f, "id {id} already exists in the index"),
        }
    }
}

impl std::error::Error for VectorIndexError {}
