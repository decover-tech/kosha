//! Deletion records (#347): the durable trail of a hard delete.
//!
//! Deleting a namespace or a tenant is an erasure commitment: once it is
//! acknowledged, the control store must be able to say whether every stored
//! object is gone. A [`DeletionRecord`] is written to the control store in
//! the same step that removes the namespace's rows, in state
//! [`DeletionState::Pending`]. It moves to [`DeletionState::Completed`] only
//! after the object storage under the subject's prefix was deleted and a
//! fresh listing of that prefix came back empty. A delete whose storage
//! reclaim failed (or whose process died half way) therefore stays visible
//! as pending, with the last error, until a retry finishes it.
//!
//! A record holds identifiers only (namespace id, tenant id, versions,
//! counters), never document content.

use serde::{Deserialize, Serialize};

/// What a deletion record covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionScope {
    /// One namespace: `subject` is its internal id (`<tenant>/<name>`).
    Namespace,
    /// A whole tenant: `subject` is the tenant id.
    Tenant,
}

impl DeletionScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Namespace => "namespace",
            Self::Tenant => "tenant",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "namespace" => Some(Self::Namespace),
            "tenant" => Some(Self::Tenant),
            _ => None,
        }
    }
}

/// Where a deletion stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionState {
    /// The control-plane rows are gone; stored objects may remain.
    Pending,
    /// Storage was deleted and verified empty.
    Completed,
}

impl DeletionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Completed => "completed",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "completed" => Some(Self::Completed),
            _ => None,
        }
    }
}

/// A deletion about to be recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDeletion {
    pub scope: DeletionScope,
    /// Namespace id or tenant id, per `scope`.
    pub subject: String,
    /// The tenant the subject belongs to (`None` for a pre-#262 bare
    /// namespace).
    pub tenant_id: Option<String>,
    /// The tenant deletion this namespace deletion is part of, if any.
    pub parent_id: Option<String>,
}

impl NewDeletion {
    pub fn namespace(id: &str, tenant: Option<&str>, parent: Option<&str>) -> Self {
        Self {
            scope: DeletionScope::Namespace,
            subject: id.to_string(),
            tenant_id: tenant.map(str::to_string),
            parent_id: parent.map(str::to_string),
        }
    }

    pub fn tenant(tenant: &str) -> Self {
        Self {
            scope: DeletionScope::Tenant,
            subject: tenant.to_string(),
            tenant_id: Some(tenant.to_string()),
            parent_id: None,
        }
    }
}

/// One deletion, as the control store holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletionRecord {
    pub id: String,
    pub scope: DeletionScope,
    pub subject: String,
    pub tenant_id: Option<String>,
    pub parent_id: Option<String>,
    /// Namespace scope: the version the namespace retired at (see
    /// `ControlStore::delete_namespace`). 0 for a tenant.
    pub last_version: u64,
    pub state: DeletionState,
    /// How many times storage reclaim ran for this record.
    pub attempts: u32,
    /// Objects deleted from object storage, summed over the attempts.
    pub objects_deleted: u64,
    /// Why the last attempt did not complete; `None` once it did.
    pub last_error: Option<String>,
    /// Unix milliseconds.
    pub requested_at_ms: u64,
    /// Unix milliseconds; set when the record completed.
    pub completed_at_ms: Option<u64>,
}

impl DeletionRecord {
    pub fn is_completed(&self) -> bool {
        self.state == DeletionState::Completed
    }
}

impl DeletionRecord {
    /// Where this record sits in the order
    /// [`crate::ControlStore::deletions_after`] lists records in.
    pub fn position(&self) -> DeletionPosition {
        DeletionPosition {
            requested_at_ms: self.requested_at_ms,
            id: self.id.clone(),
        }
    }
}

/// A place in the `(requested_at_ms, id)` order of deletion records, for
/// reading them a page at a time
/// ([`crate::ControlStore::deletions_after`]).
///
/// The order is not the order records became visible in: a record is
/// stamped when its transaction starts, so one committed later can sort
/// before one a reader already saw. A reader that follows new records
/// re-reads a window behind the newest position it has seen and skips the
/// ids it already handled.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeletionPosition {
    pub requested_at_ms: u64,
    pub id: String,
}

impl DeletionPosition {
    /// Just before every record requested at or after `requested_at_ms`.
    pub fn at_ms(requested_at_ms: u64) -> Self {
        Self {
            requested_at_ms,
            id: String::new(),
        }
    }
}

/// Which records [`crate::ControlStore::deletions`] returns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeletionFilter {
    pub state: Option<DeletionState>,
    pub tenant_id: Option<String>,
    /// At most this many, oldest first; `None` for the store's default
    /// ([`DEFAULT_DELETION_LIMIT`]).
    pub limit: Option<u32>,
}

/// How many records a listing returns when the filter sets no limit.
pub const DEFAULT_DELETION_LIMIT: u32 = 1000;

impl DeletionFilter {
    pub fn pending() -> Self {
        Self {
            state: Some(DeletionState::Pending),
            ..Self::default()
        }
    }

    pub fn matches(&self, record: &DeletionRecord) -> bool {
        self.state.is_none_or(|state| state == record.state)
            && self
                .tenant_id
                .as_ref()
                .is_none_or(|tenant| record.tenant_id.as_ref() == Some(tenant))
    }

    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_DELETION_LIMIT).max(1)
    }
}
