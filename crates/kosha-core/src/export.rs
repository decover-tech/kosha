//! Namespace exports (#347): a customer's copy of everything a namespace
//! stores, as gzip-compressed JSON Lines.
//!
//! An [`ExportRecord`] is the durable job: created `pending` by
//! `POST /v1/namespaces/{ns}/exports`, claimed and run by a writer pod
//! (`running`), and `completed` (or `failed`) once its parts are in object
//! storage. It pins the manifests it reads ([`ExportRecord::pins`]): segment
//! GC and the local GC keep every object a pinned manifest references while
//! the export is pending or running, so compaction cannot take a segment
//! away mid-export and the export is consistent to one manifest version per
//! namespace (one per shard for a sharded namespace).
//!
//! A job whose pod died is restartable: another writer claims it once its
//! heartbeat is older than the claim timeout and runs it again from the
//! start, against the same pins. Completed exports expire
//! (`expires_at_ms`); expired and failed exports have their objects
//! deleted. Deleting the namespace or its tenant deletes its exports, and
//! the deletion record counts them.
//!
//! A record holds identifiers, counters and the pinned manifests (segment
//! ids, the namespace schema), never document content. The customer's
//! presigned upload URL is kept only until the delivery finishes and is
//! never returned by the API (only its host is).

use serde::{Deserialize, Serialize};

use crate::{Manifest, NamespaceId};

/// Where an export stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportState {
    /// Created; no pod has claimed it yet (or its pod died).
    Pending,
    /// A pod is writing it.
    Running,
    /// Every part is in object storage.
    Completed,
    /// Gave up; `error` says why. Its objects are deleted.
    Failed,
    /// Past `expires_at_ms`, or deleted by its owner; its objects are gone.
    Expired,
}

impl ExportState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    /// Pending or running: its pins hold.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}

/// Where the optional delivery to a customer-supplied URL stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Delivered,
    Failed,
}

impl DeliveryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "delivered" => Some(Self::Delivered),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One part of a finished export: a complete gzip member holding whole
/// JSON lines. Parts concatenated in order are one valid `.jsonl.gz`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExportPart {
    /// Object-storage path (logical, unprefixed).
    pub path: String,
    pub bytes: u64,
    pub documents: u64,
}

/// An export about to be recorded.
#[derive(Debug, Clone)]
pub struct NewExport {
    pub tenant_id: String,
    /// Internal namespace id (`<tenant>/<name>`).
    pub namespace: String,
    pub include_vectors: bool,
    /// The manifests the export reads, one per namespace (the namespace
    /// itself, or each shard of a sharded one).
    pub pins: Vec<(NamespaceId, Manifest)>,
    /// The customer's presigned PUT URL, already validated.
    pub upload_url: Option<String>,
    /// When a completed export expires (unix ms), counted from completion
    /// by the worker; stored here as the lifetime in milliseconds.
    pub ttl_ms: u64,
}

/// One export, as the control store holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRecord {
    pub id: String,
    pub tenant_id: String,
    pub namespace: String,
    pub state: ExportState,
    pub format: String,
    pub include_vectors: bool,
    /// The pinned manifest version of each exported namespace.
    pub versions: Vec<(String, u64)>,
    pub parts: Vec<ExportPart>,
    pub documents: u64,
    pub bytes: u64,
    pub error: Option<String>,
    /// Host of the customer's upload URL, if one was given.
    pub delivery_host: Option<String>,
    pub delivery_state: Option<DeliveryState>,
    pub delivery_error: Option<String>,
    pub attempts: u32,
    pub claimed_by: Option<String>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub completed_at_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
    pub ttl_ms: u64,
    /// The manifests read; dropped once the export is no longer active.
    #[serde(skip)]
    pub pins: Vec<(NamespaceId, Manifest)>,
    /// Never serialized: a presigned URL is a credential.
    #[serde(skip)]
    pub upload_url: Option<String>,
}

/// A change to an export (only the `Some` fields change).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExportUpdate {
    pub state: Option<ExportState>,
    pub parts: Option<Vec<ExportPart>>,
    pub documents: Option<u64>,
    pub bytes: Option<u64>,
    /// `Some(None)` clears it.
    pub error: Option<Option<String>>,
    pub delivery_state: Option<DeliveryState>,
    pub delivery_error: Option<Option<String>>,
    pub completed_at_ms: Option<u64>,
    pub expires_at_ms: Option<u64>,
    /// Drop the customer's URL (delivery finished, or the export ended).
    pub clear_upload_url: bool,
    /// Drop the pins (the export is no longer active).
    pub clear_pins: bool,
    /// Only apply while the export is claimed by this pod; `NotFound`-like
    /// `Ok(None)` otherwise.
    pub holder: Option<String>,
}

/// Which exports [`crate::ControlStore::exports`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExportFilter {
    pub tenant_id: Option<String>,
    pub namespace: Option<String>,
    pub states: Vec<ExportState>,
    /// At most this many, oldest first (newest first with `newest_first`);
    /// `None` = 1000.
    pub limit: Option<u32>,
    /// Order by `(created_at_ms, id)` descending instead of ascending.
    pub newest_first: bool,
    /// Only exports strictly before this `(created_at_ms, id)` in the
    /// newest-first order (the next page after that export).
    pub before: Option<(u64, String)>,
}

impl ExportFilter {
    pub fn matches(&self, record: &ExportRecord) -> bool {
        self.tenant_id
            .as_ref()
            .is_none_or(|tenant| &record.tenant_id == tenant)
            && self
                .namespace
                .as_ref()
                .is_none_or(|ns| &record.namespace == ns)
            && (self.states.is_empty() || self.states.contains(&record.state))
            && self
                .before
                .as_ref()
                .is_none_or(|(ms, id)| (record.created_at_ms, &record.id) < (*ms, id))
    }

    /// Where `record` sits in the listing order.
    pub fn position(record: &ExportRecord) -> (u64, String) {
        (record.created_at_ms, record.id.clone())
    }

    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(1000).max(1)
    }

    /// Every pending and running export: the ones whose pins hold. No cap:
    /// a pin GC did not read would let it delete what an export reads.
    pub fn active() -> Self {
        Self {
            states: vec![ExportState::Pending, ExportState::Running],
            limit: Some(u32::MAX),
            ..Self::default()
        }
    }
}

/// The logical object-storage directory holding export `id` of `tenant`.
///
/// Exports live under the reserved top-level `_exports/` prefix (like
/// `_wal/`), not under the tenant's own prefix: a tenant may own a
/// namespace called `_exports`, and everything under a namespace's prefix
/// is segment GC's and the deletion purge's to judge.
pub fn export_dir(tenant: &str, id: &str) -> String {
    format!("{EXPORTS_PREFIX}/{tenant}/{id}")
}

/// The logical directory of every export of `tenant`.
pub fn tenant_exports_dir(tenant: &str) -> String {
    format!("{EXPORTS_PREFIX}/{tenant}")
}

/// Top-level prefix of export objects.
pub const EXPORTS_PREFIX: &str = "_exports";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip_and_filter() {
        for state in [
            ExportState::Pending,
            ExportState::Running,
            ExportState::Completed,
            ExportState::Failed,
            ExportState::Expired,
        ] {
            assert_eq!(ExportState::parse(state.as_str()), Some(state));
        }
        assert!(ExportState::Running.is_active());
        assert!(!ExportState::Completed.is_active());
        assert_eq!(export_dir("acme", "exp-1"), "_exports/acme/exp-1");
    }
}
