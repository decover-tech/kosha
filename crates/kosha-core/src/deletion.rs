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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionScope {
    /// One namespace: `subject` is its internal id (`<tenant>/<name>`).
    Namespace,
    /// A whole tenant: `subject` is the tenant id.
    Tenant,
    /// A copy-on-write branch deleted on its own
    /// (`DELETE /v1/namespaces/{ns}/branch`, #208): `subject` is the
    /// branch's internal id. Unlike a namespace, its prefix can hold
    /// objects a remaining branch still lists, so storage reclaim removes
    /// only what no manifest references.
    Branch,
    /// The bare (pre-#262) source id of `kosha-server migrate-namespace`
    /// (#344), removed once its segments were copied to `<tenant>/<name>`:
    /// `subject` is the bare id, `tenant_id` the destination's tenant.
    /// The bare prefix can hold another tenant's namespaces, so nothing
    /// deletes it wholesale; the migration tool deletes the copied objects
    /// and completes the record.
    Migrated,
}

impl DeletionScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Namespace => "namespace",
            Self::Tenant => "tenant",
            Self::Branch => "branch",
            Self::Migrated => "migrated",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "namespace" => Some(Self::Namespace),
            "tenant" => Some(Self::Tenant),
            "branch" => Some(Self::Branch),
            "migrated" => Some(Self::Migrated),
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
    /// Whether the record completes only once every live pod acknowledged
    /// that it purged its cached copies (`KOSHA_DELETION_REQUIRE_ACKS`).
    pub acks_required: bool,
}

impl NewDeletion {
    pub fn namespace(id: &str, tenant: Option<&str>, parent: Option<&str>) -> Self {
        Self {
            scope: DeletionScope::Namespace,
            subject: id.to_string(),
            tenant_id: tenant.map(str::to_string),
            parent_id: parent.map(str::to_string),
            acks_required: false,
        }
    }

    pub fn tenant(tenant: &str) -> Self {
        Self {
            scope: DeletionScope::Tenant,
            subject: tenant.to_string(),
            tenant_id: Some(tenant.to_string()),
            parent_id: None,
            acks_required: false,
        }
    }

    /// A branch deleted on its own, or as part of tenant `parent`.
    pub fn branch(id: &str, tenant: Option<&str>, parent: Option<&str>) -> Self {
        Self {
            scope: DeletionScope::Branch,
            ..Self::namespace(id, tenant, parent)
        }
    }

    /// The bare source id `from` of a namespace migrated to tenant
    /// `tenant` (#344).
    pub fn migrated(from: &str, tenant: &str) -> Self {
        Self {
            scope: DeletionScope::Migrated,
            subject: from.to_string(),
            tenant_id: Some(tenant.to_string()),
            parent_id: None,
            acks_required: false,
        }
    }

    /// Require per-pod acknowledgements before the record completes.
    pub fn with_acks(mut self, required: bool) -> Self {
        self.acks_required = required;
        self
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
    /// Whether the record waits for every live pod's acknowledgement
    /// before it completes (see [`AckStatus`]).
    #[serde(default)]
    pub acks_required: bool,
    /// Storage reclaim finished and was verified. A record with
    /// `acks_required` stays pending after that until the acknowledgements
    /// are in; a completed record always has it (older rows say `false`,
    /// [`Self::storage_done`] reads both).
    #[serde(default)]
    pub storage_verified: bool,
    /// Exports (#347) of the subject deleted with it.
    #[serde(default)]
    pub exports_deleted: u64,
}

impl DeletionRecord {
    pub fn is_completed(&self) -> bool {
        self.state == DeletionState::Completed
    }

    /// Storage reclaim is over: verified, or the record completed.
    pub fn storage_done(&self) -> bool {
        self.storage_verified || self.is_completed()
    }

    /// Pending only for acknowledgements: storage is verified.
    pub fn awaiting_acks(&self) -> bool {
        !self.is_completed() && self.storage_verified && self.acks_required
    }
}

/// A pod's heartbeat in the pod registry (#347): the pods whose
/// acknowledgement a deletion record with `acks_required` waits for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodHeartbeat {
    /// Stable for the pod's life (`KOSHA_POD_ID`, else `HOSTNAME`).
    pub pod_id: String,
    /// `writer`, `query`, `indexer`.
    pub role: String,
    /// Whether the pod runs the deletion purge. Only such pods are waited
    /// for: a pod without it never acknowledges anything.
    pub purge_enabled: bool,
}

/// A registered pod, as the control store last heard from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodInfo {
    pub pod_id: String,
    pub role: String,
    pub purge_enabled: bool,
    /// Unix milliseconds of the last heartbeat.
    pub last_heartbeat_ms: u64,
}

/// Who acknowledged a deletion record, against the pods alive now.
///
/// A pod is *live* when it runs the purge and its last heartbeat is newer
/// than the liveness window. A pod that stopped heartbeating (it died, or
/// was scaled away) drops out of `live` once the window passes, and is
/// listed in `expired` if it never acknowledged: that is the bound on how
/// long a dead pod can hold a record pending. A pod that restarts with the
/// same id handles every record again in its first purge pass after boot.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckStatus {
    /// Pods that acknowledged, with when (unix ms), live or not.
    pub acked: Vec<(String, u64)>,
    /// Live pods that have not acknowledged.
    pub missing: Vec<String>,
    /// Every live pod.
    pub live: Vec<String>,
    /// Registered purge pods that did not acknowledge and are no longer
    /// live, so are not waited for.
    pub expired: Vec<String>,
}

impl AckStatus {
    /// Every live pod acknowledged.
    pub fn complete(&self) -> bool {
        self.missing.is_empty()
    }

    /// Work out the status from the registry and the acknowledgements.
    pub fn compute(pods: &[PodInfo], acks: &[(String, u64)], live_after_ms: u64) -> Self {
        let acked_ids: std::collections::HashSet<&str> =
            acks.iter().map(|(pod, _)| pod.as_str()).collect();
        let mut status = Self {
            acked: acks.to_vec(),
            ..Self::default()
        };
        for pod in pods.iter().filter(|pod| pod.purge_enabled) {
            let live = pod.last_heartbeat_ms > live_after_ms;
            let acked = acked_ids.contains(pod.pod_id.as_str());
            if live {
                status.live.push(pod.pod_id.clone());
                if !acked {
                    status.missing.push(pod.pod_id.clone());
                }
            } else if !acked {
                status.expired.push(pod.pod_id.clone());
            }
        }
        status.acked.sort();
        status.live.sort();
        status.missing.sort();
        status.expired.sort();
        status
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
