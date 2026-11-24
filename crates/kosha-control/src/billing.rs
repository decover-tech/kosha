//! Billing (#349): plans, rating a month of usage into an invoice, the
//! free tier, and what happens to an account that does not pay.
//!
//! # Plans
//!
//! A [`Catalog`] is the operator's price list: named [`Plan`]s, each of a
//! [`PlanKind`] (free, usage-based, enterprise), with per-meter terms
//! ([`MeterTerms`]: how much is included and what the rest costs), an
//! optional monthly base fee, and the tenant quota defaults of the plan
//! ([`PlanQuotas`]). No prices are built in.
//!
//! # Rating
//!
//! [`rate`] turns a tenant's usage over a calendar month (UTC, a
//! [`BillingPeriod`]) into invoice line items. Money is integer cents; each
//! line is rounded half up, and an invoice's total is the sum of its lines,
//! so the total a payment provider computes from the lines is the same
//! number. The four billable meters ([`billable`]) come from the usage
//! summary of #348: `queries`, `bytes_queried`, `bytes_written`, and
//! `stored_bytes` (the average bytes stored over the month, i.e. byte-hours
//! divided by the hours in the month).
//!
//! # Free tier and non-payment
//!
//! A tenant's [`TenantStanding`] says what it may do right now:
//!
//! - On a free plan, a meter whose month-to-date use has reached its
//!   included amount blocks further use of that kind: searches for the query
//!   meters, writes for the write and storage meters. Nothing is charged.
//!   Moving the tenant to a paid plan lifts it.
//! - An invoice unpaid past its due date makes the account `past_due`
//!   (no restriction), then `read_only` (writes refused), then `suspended`
//!   (everything but billing and usage reads refused), at the delays of the
//!   [`DunningPolicy`]. The state is computed from the oldest unpaid
//!   invoice each time, never stored, so paying restores `active` with
//!   nothing to reset. Enterprise accounts are never restricted
//!   automatically.
//!
//! Deletes stay allowed until suspension, so a read-only or over-quota
//! tenant can still remove data.
//!
//! # The monthly job
//!
//! [`Biller::close_period`] invoices every tenant for a finished month. An
//! invoice's id is `<tenant>/<YYYY-MM>` and inserting it is idempotent, so
//! the job can be run again (after a failure, or from two pods) without
//! double-billing; a re-run also submits invoices an earlier run could not
//! hand to the [`BillingProvider`]. [`Biller::sync_payments`] copies payment
//! results back from the provider.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;

use kosha_core::KoshaError;
use serde::{Deserialize, Serialize};

use crate::usage::{usage_summary, NamespaceUsage, UsageStore, HOUR_MS};

/// Names of the billable meters, as they appear in a plan's `meters` and on
/// invoice lines.
pub mod billable {
    /// Successful searches.
    pub const QUERIES: &str = "queries";
    /// For each search, the bytes its namespace stored at the time.
    pub const BYTES_QUERIED: &str = "bytes_queried";
    /// Logical bytes of accepted document writes.
    pub const BYTES_WRITTEN: &str = "bytes_written";
    /// Average bytes stored over the period.
    pub const STORED_BYTES: &str = "stored_bytes";
    /// Every meter a plan may price.
    pub const ALL: [&str; 4] = [QUERIES, BYTES_QUERIED, BYTES_WRITTEN, STORED_BYTES];
    /// The invoice line of a plan's monthly base fee (not a meter).
    pub const BASE_FEE: &str = "base_fee";
}

const DAY_MS: u64 = 86_400_000;

// ─── Billing period ─────────────────────────────────────────────────────────

/// One calendar month, UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BillingPeriod {
    pub year: i32,
    /// 1 to 12.
    pub month: u32,
}

/// Days from 1970-01-01 to `year-month-day` (proleptic Gregorian).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `(year, month)` of the day `days` after 1970-01-01.
fn civil_from_days(days: i64) -> (i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

impl BillingPeriod {
    /// Parse `YYYY-MM` (1970 to 9999).
    pub fn parse(text: &str) -> Result<Self, String> {
        let invalid = || format!("invalid billing period {text:?}: expected YYYY-MM");
        let (year, month) = text.split_once('-').ok_or_else(invalid)?;
        if year.len() != 4 || month.len() != 2 {
            return Err(invalid());
        }
        let year: i32 = year.parse().map_err(|_| invalid())?;
        let month: u32 = month.parse().map_err(|_| invalid())?;
        if !(1970..=9999).contains(&year) || !(1..=12).contains(&month) {
            return Err(invalid());
        }
        Ok(Self { year, month })
    }

    /// The month `at_ms` (Unix milliseconds) falls in.
    pub fn containing(at_ms: u64) -> Self {
        let days = i64::try_from(at_ms / DAY_MS).unwrap_or(i64::MAX);
        let (year, month) = civil_from_days(days);
        Self {
            year: i32::try_from(year).unwrap_or(i32::MAX),
            month: month as u32,
        }
    }

    /// First millisecond of the month.
    pub fn start_ms(&self) -> u64 {
        let days = days_from_civil(i64::from(self.year), i64::from(self.month), 1);
        u64::try_from(days).unwrap_or(0).saturating_mul(DAY_MS)
    }

    /// First millisecond of the next month (exclusive end).
    pub fn end_ms(&self) -> u64 {
        self.next().start_ms()
    }

    pub fn next(&self) -> Self {
        if self.month == 12 {
            Self {
                year: self.year.saturating_add(1),
                month: 1,
            }
        } else {
            Self {
                year: self.year,
                month: self.month + 1,
            }
        }
    }

    /// The month before; 1970-01 is its own predecessor.
    pub fn previous(&self) -> Self {
        if self.year <= 1970 && self.month == 1 {
            *self
        } else if self.month == 1 {
            Self {
                year: self.year - 1,
                month: 12,
            }
        } else {
            Self {
                year: self.year,
                month: self.month - 1,
            }
        }
    }

    /// Hours in the month (672 to 744).
    pub fn hours(&self) -> u64 {
        (self.end_ms() - self.start_ms()) / HOUR_MS
    }
}

impl fmt::Display for BillingPeriod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

// ─── Plans ──────────────────────────────────────────────────────────────────

/// What kind of plan it is. The kind decides behaviour, the plan's terms
/// decide amounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanKind {
    /// Included amounts are hard limits; nothing is ever charged.
    Free,
    /// Use above the included amounts is charged.
    Usage,
    /// Rated like `Usage`, but never restricted automatically for
    /// non-payment.
    Enterprise,
}

fn one() -> u64 {
    1
}

/// One meter's terms on a plan: the first `included` are free, the rest
/// cost `unit_price_cents` per `unit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeterTerms {
    #[serde(default)]
    pub included: u64,
    /// The quantity a price is quoted for (for example 1,000,000 queries
    /// or 1,073,741,824 bytes). At least 1.
    #[serde(default = "one")]
    pub unit: u64,
    #[serde(default)]
    pub unit_price_cents: u64,
}

/// A plan's tenant quota defaults: the limits of the quotas issue (#341)
/// that apply to a tenant on this plan unless the operator's limits file
/// says otherwise. Absent = not limited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanQuotas {
    pub qps: Option<f64>,
    pub concurrent_queries: Option<u32>,
    pub write_docs_per_sec: Option<f64>,
    pub write_bytes_per_sec: Option<f64>,
    pub namespaces: Option<u64>,
    pub docs_per_namespace: Option<u64>,
    pub bytes_per_namespace: Option<u64>,
    pub vector_dims: Option<u32>,
    pub max_body_bytes: Option<u64>,
    pub weight: Option<u32>,
}

/// One plan of the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub kind: PlanKind,
    /// Charged once per period, whatever the usage.
    #[serde(default)]
    pub base_fee_cents: u64,
    /// Terms per billable meter ([`billable::ALL`]). A meter not listed is
    /// neither charged nor limited.
    #[serde(default)]
    pub meters: BTreeMap<String, MeterTerms>,
    #[serde(default)]
    pub limits: PlanQuotas,
}

fn default_payment_terms_days() -> u64 {
    14
}
fn default_read_only_after_days() -> u64 {
    7
}
fn default_suspend_after_days() -> u64 {
    21
}

/// When an unpaid invoice restricts an account.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DunningPolicy {
    /// An invoice is due this many days after it is issued.
    #[serde(default = "default_payment_terms_days")]
    pub payment_terms_days: u64,
    /// Days past due before writes are refused.
    #[serde(default = "default_read_only_after_days")]
    pub read_only_after_days: u64,
    /// Days past due before everything is refused.
    #[serde(default = "default_suspend_after_days")]
    pub suspend_after_days: u64,
}

impl Default for DunningPolicy {
    fn default() -> Self {
        Self {
            payment_terms_days: default_payment_terms_days(),
            read_only_after_days: default_read_only_after_days(),
            suspend_after_days: default_suspend_after_days(),
        }
    }
}

/// The price list: plans, the plan of tenants without an account, and the
/// dunning policy. Parsed from JSON:
///
/// ```json
/// {
///   "default_plan": "free",
///   "dunning": {"payment_terms_days": 14, "read_only_after_days": 7, "suspend_after_days": 21},
///   "plans": {
///     "free": {"kind": "free",
///              "meters": {"queries": {"included": 100000},
///                         "stored_bytes": {"included": 1073741824}},
///              "limits": {"qps": 5, "namespaces": 3}},
///     "payg": {"kind": "usage", "base_fee_cents": 2500,
///              "meters": {"queries": {"included": 1000000, "unit": 1000000,
///                                     "unit_price_cents": 400}}}
///   }
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    #[serde(default)]
    pub plans: BTreeMap<String, Plan>,
    /// The plan of a tenant with no billing account. `None`: such a tenant
    /// is neither billed nor restricted.
    #[serde(default)]
    pub default_plan: Option<String>,
    #[serde(default)]
    pub dunning: DunningPolicy,
}

impl Catalog {
    /// Parse and validate.
    pub fn parse(text: &str) -> Result<Self, String> {
        let catalog: Catalog =
            serde_json::from_str(text).map_err(|e| format!("invalid billing plans JSON: {e}"))?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(plan) = &self.default_plan {
            if !self.plans.contains_key(plan) {
                return Err(format!("default plan {plan:?} is not defined in `plans`"));
            }
        }
        if self.dunning.read_only_after_days > self.dunning.suspend_after_days {
            return Err(
                "dunning: read_only_after_days must not be greater than suspend_after_days".into(),
            );
        }
        for (name, plan) in &self.plans {
            if name.is_empty() {
                return Err("a plan name must not be empty".into());
            }
            for (meter, terms) in &plan.meters {
                if !billable::ALL.contains(&meter.as_str()) {
                    return Err(format!(
                        "plan {name:?}: unknown meter {meter:?} (known: {})",
                        billable::ALL.join(", ")
                    ));
                }
                if terms.unit == 0 {
                    return Err(format!("plan {name:?}: meter {meter:?}: unit must be >= 1"));
                }
                if plan.kind == PlanKind::Free && terms.unit_price_cents != 0 {
                    return Err(format!(
                        "plan {name:?}: a free plan cannot price meter {meter:?}; \
                         its included amounts are hard limits"
                    ));
                }
            }
            if plan.kind == PlanKind::Free && plan.base_fee_cents != 0 {
                return Err(format!("plan {name:?}: a free plan cannot have a base fee"));
            }
            let q = &plan.limits;
            for (field, rate) in [
                ("qps", q.qps),
                ("write_docs_per_sec", q.write_docs_per_sec),
                ("write_bytes_per_sec", q.write_bytes_per_sec),
            ] {
                if rate.is_some_and(|r| !(r.is_finite() && r > 0.0)) {
                    return Err(format!(
                        "plan {name:?}: limits.{field} must be a positive number"
                    ));
                }
            }
            if q.weight == Some(0) {
                return Err(format!("plan {name:?}: limits.weight must be at least 1"));
            }
        }
        Ok(())
    }

    pub fn plan(&self, name: &str) -> Option<&Plan> {
        self.plans.get(name)
    }

    /// The plan name that applies to a tenant: its account's, else the
    /// default.
    pub fn plan_name_for<'a>(&'a self, account: Option<&'a BillingAccount>) -> Option<&'a str> {
        account
            .map(|a| a.plan.as_str())
            .or(self.default_plan.as_deref())
    }
}

// ─── Rating ─────────────────────────────────────────────────────────────────

/// A tenant's usage over a window, summed over its namespaces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct UsageTotals {
    pub queries: u64,
    pub bytes_queried: u64,
    pub bytes_written: u64,
    pub stored_byte_hours: u64,
    /// Bytes stored now: the sum of each namespace's latest snapshot.
    pub stored_bytes_latest: u64,
}

impl UsageTotals {
    pub fn of(namespaces: &[NamespaceUsage]) -> Self {
        let mut total = NamespaceUsage::default();
        for usage in namespaces {
            total.add(usage);
        }
        Self {
            queries: total.queries,
            bytes_queried: total.bytes_queried,
            bytes_written: total.bytes_written,
            stored_byte_hours: total.stored_byte_hours,
            stored_bytes_latest: total.stored_bytes_latest.unwrap_or(0),
        }
    }

    /// The quantity an invoice bills for `meter` over a period of
    /// `period_hours` hours.
    fn billed(&self, meter: &str, period_hours: u64) -> u64 {
        match meter {
            billable::QUERIES => self.queries,
            billable::BYTES_QUERIED => self.bytes_queried,
            billable::BYTES_WRITTEN => self.bytes_written,
            billable::STORED_BYTES => self.stored_byte_hours / period_hours.max(1),
            _ => 0,
        }
    }

    /// The quantity the free tier compares with a meter's included amount.
    /// Storage is what is stored now, not the month's average: an average
    /// would let a tenant store any amount late in the month.
    fn metered(&self, meter: &str) -> u64 {
        match meter {
            billable::STORED_BYTES => self.stored_bytes_latest,
            other => self.billed(other, 1),
        }
    }
}

/// One line of an invoice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineItem {
    /// A billable meter, or [`billable::BASE_FEE`].
    pub meter: String,
    pub quantity: u64,
    pub included: u64,
    /// `quantity` above `included`.
    pub billable: u64,
    pub unit: u64,
    pub unit_price_cents: u64,
    pub amount_cents: u64,
}

/// `quantity * price / unit`, rounded half up, saturating at `i64::MAX`
/// (amounts are stored as `BIGINT`).
fn charge_cents(quantity: u64, unit: u64, unit_price_cents: u64) -> u64 {
    let unit = u128::from(unit.max(1));
    let cents = (u128::from(quantity) * u128::from(unit_price_cents) + unit / 2) / unit;
    u64::try_from(cents)
        .unwrap_or(u64::MAX)
        .min(i64::MAX as u64)
}

/// Rate `totals` on `plan` for a period of `period_hours` hours: the base
/// fee (when the plan has one) and one line per meter the plan lists, in
/// meter-name order.
pub fn rate(plan: &Plan, totals: &UsageTotals, period_hours: u64) -> Vec<LineItem> {
    let mut lines = Vec::new();
    if plan.base_fee_cents > 0 {
        lines.push(LineItem {
            meter: billable::BASE_FEE.to_string(),
            quantity: 1,
            included: 0,
            billable: 1,
            unit: 1,
            unit_price_cents: plan.base_fee_cents,
            amount_cents: charge_cents(1, 1, plan.base_fee_cents),
        });
    }
    for (meter, terms) in &plan.meters {
        let quantity = totals.billed(meter, period_hours);
        let billable = quantity.saturating_sub(terms.included);
        lines.push(LineItem {
            meter: meter.clone(),
            quantity,
            included: terms.included,
            billable,
            unit: terms.unit,
            unit_price_cents: terms.unit_price_cents,
            amount_cents: charge_cents(billable, terms.unit, terms.unit_price_cents),
        });
    }
    lines
}

/// The sum of `lines`, saturating at `i64::MAX`.
pub fn total_cents(lines: &[LineItem]) -> u64 {
    lines
        .iter()
        .fold(0u64, |sum, line| sum.saturating_add(line.amount_cents))
        .min(i64::MAX as u64)
}

// ─── Standing: free tier and non-payment ────────────────────────────────────

/// A free-tier meter whose included amount is used up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Excess {
    pub meter: String,
    pub used: u64,
    pub included: u64,
}

/// The meters of a free `plan` whose included amount `totals` has reached.
/// Empty for any other kind of plan.
pub fn free_tier_excess(plan: &Plan, totals: &UsageTotals) -> Vec<Excess> {
    if plan.kind != PlanKind::Free {
        return Vec::new();
    }
    plan.meters
        .iter()
        .filter_map(|(meter, terms)| {
            let used = totals.metered(meter);
            (used >= terms.included).then(|| Excess {
                meter: meter.clone(),
                used,
                included: terms.included,
            })
        })
        .collect()
}

/// Where an account is on the non-payment path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountState {
    Active,
    /// An invoice is past due; nothing is restricted yet.
    PastDue,
    /// Writes are refused.
    ReadOnly,
    /// Everything but billing and usage reads is refused.
    Suspended,
}

/// The state of an account whose oldest unpaid invoice was due at
/// `oldest_unpaid_due_ms` (`None`: nothing unpaid).
pub fn account_state(
    kind: PlanKind,
    oldest_unpaid_due_ms: Option<u64>,
    now_ms: u64,
    policy: &DunningPolicy,
) -> AccountState {
    let Some(due_ms) = oldest_unpaid_due_ms else {
        return AccountState::Active;
    };
    if now_ms < due_ms {
        return AccountState::Active;
    }
    if kind == PlanKind::Enterprise {
        return AccountState::PastDue;
    }
    let overdue_ms = now_ms - due_ms;
    if overdue_ms >= policy.suspend_after_days.saturating_mul(DAY_MS) {
        AccountState::Suspended
    } else if overdue_ms >= policy.read_only_after_days.saturating_mul(DAY_MS) {
        AccountState::ReadOnly
    } else {
        AccountState::PastDue
    }
}

/// What a request does, as far as billing cares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// A search.
    Query,
    /// A write that adds or changes data.
    Write,
    /// A delete of documents, a branch or a namespace.
    Delete,
    /// Reading the tenant's own billing or usage.
    Account,
    /// Any other read.
    Other,
}

/// Why a request is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    Suspended,
    ReadOnly,
    FreeTier(Excess),
}

impl Block {
    /// The machine-readable `reason` of the refusal.
    pub fn reason(&self) -> &'static str {
        match self {
            Block::Suspended => "billing_suspended",
            Block::ReadOnly => "billing_read_only",
            Block::FreeTier(_) => "free_tier_exceeded",
        }
    }

    pub fn message(&self) -> String {
        match self {
            Block::Suspended => {
                "account suspended: an invoice is long overdue; pay it to restore access".into()
            }
            Block::ReadOnly => {
                "account is read-only: an invoice is overdue; pay it to restore writes".into()
            }
            Block::FreeTier(excess) => format!(
                "free tier used up for this month: {} is at {} of {} included; \
                 upgrade the plan to continue",
                excess.meter, excess.used, excess.included
            ),
        }
    }
}

/// What a tenant may do right now, and why not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TenantStanding {
    pub plan: String,
    pub kind: PlanKind,
    pub state: AccountState,
    /// Free-tier meters that are used up.
    pub free_tier_exceeded: Vec<Excess>,
    /// Due date of the oldest unpaid invoice, if any.
    pub oldest_unpaid_due_ms: Option<u64>,
}

impl TenantStanding {
    /// Why `operation` is refused, or `None` when it is allowed.
    pub fn block(&self, operation: Operation) -> Option<Block> {
        if operation == Operation::Account {
            return None;
        }
        match self.state {
            AccountState::Suspended => return Some(Block::Suspended),
            AccountState::ReadOnly if operation == Operation::Write => {
                return Some(Block::ReadOnly)
            }
            _ => {}
        }
        let restricted: &[&str] = match operation {
            Operation::Query => &[billable::QUERIES, billable::BYTES_QUERIED],
            Operation::Write => &[billable::BYTES_WRITTEN, billable::STORED_BYTES],
            _ => &[],
        };
        self.free_tier_exceeded
            .iter()
            .find(|excess| restricted.contains(&excess.meter.as_str()))
            .map(|excess| Block::FreeTier(excess.clone()))
    }
}

// ─── Accounts and invoices ──────────────────────────────────────────────────

/// A tenant's billing account: the plan it is on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BillingAccount {
    pub tenant: String,
    pub plan: String,
    /// The tenant's customer id at the billing provider, once it has one.
    pub provider_customer_id: Option<String>,
    /// When the account was first created. Kept by the store across
    /// updates.
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvoiceStatus {
    /// Issued and not paid.
    Open,
    Paid,
    /// Cancelled; nothing is owed.
    Void,
}

impl InvoiceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            InvoiceStatus::Open => "open",
            InvoiceStatus::Paid => "paid",
            InvoiceStatus::Void => "void",
        }
    }

    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "open" => Some(InvoiceStatus::Open),
            "paid" => Some(InvoiceStatus::Paid),
            "void" => Some(InvoiceStatus::Void),
            _ => None,
        }
    }
}

/// One tenant's invoice for one period.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Invoice {
    /// `<tenant>/<YYYY-MM>`: one invoice per tenant per period.
    pub id: String,
    pub tenant: String,
    /// `YYYY-MM`.
    pub period: String,
    pub period_start_ms: u64,
    pub period_end_ms: u64,
    /// The plan it was rated on.
    pub plan: String,
    pub lines: Vec<LineItem>,
    /// Sum of the lines.
    pub total_cents: u64,
    pub issued_at_ms: u64,
    pub due_at_ms: u64,
    pub status: InvoiceStatus,
    pub paid_at_ms: Option<u64>,
    /// The invoice's id at the billing provider, once submitted.
    pub provider_invoice_id: Option<String>,
}

/// The id of `tenant`'s invoice for `period`.
pub fn invoice_id(tenant: &str, period: BillingPeriod) -> String {
    format!("{tenant}/{period}")
}

/// Where billing accounts and invoices are kept.
pub trait BillingStore: Send + Sync {
    fn account(&self, tenant: &str) -> Result<Option<BillingAccount>, KoshaError>;

    /// Every account, ordered by tenant.
    fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError>;

    /// Create the account, or replace an existing one's plan. An existing
    /// account keeps its `created_at_ms`, and keeps its provider customer
    /// id when `account.provider_customer_id` is `None`: a plan change must
    /// not forget the customer the monthly job created in the meantime.
    fn put_account(&self, account: &BillingAccount) -> Result<(), KoshaError>;

    /// Record `tenant`'s customer id at the provider. An existing account
    /// keeps its plan (the caller's copy of it may be stale: an operator
    /// can change the plan while the monthly job runs); a tenant without
    /// an account gets one on `plan_if_new`, created at `now_ms`.
    fn set_provider_customer(
        &self,
        tenant: &str,
        plan_if_new: &str,
        provider_customer_id: &str,
        now_ms: u64,
    ) -> Result<(), KoshaError>;

    /// Store a new invoice. `false`, and nothing changes, when an invoice
    /// with this id exists.
    fn insert_invoice(&self, invoice: &Invoice) -> Result<bool, KoshaError>;

    fn invoice(&self, id: &str) -> Result<Option<Invoice>, KoshaError>;

    /// Invoices of one tenant or (`None`) all, ordered by tenant then
    /// period.
    fn invoices(&self, tenant: Option<&str>) -> Result<Vec<Invoice>, KoshaError>;

    /// Record the provider's id for an invoice. `false` when there is no
    /// such invoice.
    fn set_provider_invoice(&self, id: &str, provider_invoice_id: &str)
        -> Result<bool, KoshaError>;

    /// Set an invoice's status; `paid_at_ms` is stored with `Paid` and
    /// cleared otherwise. `false` when there is no such invoice.
    fn set_invoice_status(
        &self,
        id: &str,
        status: InvoiceStatus,
        paid_at_ms: Option<u64>,
    ) -> Result<bool, KoshaError>;
}

#[derive(Default)]
struct MemoryBilling {
    accounts: BTreeMap<String, BillingAccount>,
    invoices: BTreeMap<String, Invoice>,
}

/// In-memory [`BillingStore`]: the default without Postgres, and what tests
/// use. Not durable and not shared between pods.
#[derive(Default)]
pub struct MemoryBillingStore {
    inner: Mutex<MemoryBilling>,
}

impl MemoryBillingStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BillingStore for MemoryBillingStore {
    fn account(&self, tenant: &str) -> Result<Option<BillingAccount>, KoshaError> {
        Ok(self.inner.lock().unwrap().accounts.get(tenant).cloned())
    }

    fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .accounts
            .values()
            .cloned()
            .collect())
    }

    fn put_account(&self, account: &BillingAccount) -> Result<(), KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        let mut stored = account.clone();
        if let Some(existing) = inner.accounts.get(&account.tenant) {
            stored.created_at_ms = existing.created_at_ms;
            if stored.provider_customer_id.is_none() {
                stored.provider_customer_id = existing.provider_customer_id.clone();
            }
        }
        inner.accounts.insert(stored.tenant.clone(), stored);
        Ok(())
    }

    fn set_provider_customer(
        &self,
        tenant: &str,
        plan_if_new: &str,
        provider_customer_id: &str,
        now_ms: u64,
    ) -> Result<(), KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .accounts
            .entry(tenant.to_string())
            .or_insert_with(|| BillingAccount {
                tenant: tenant.to_string(),
                plan: plan_if_new.to_string(),
                provider_customer_id: None,
                created_at_ms: now_ms,
            })
            .provider_customer_id = Some(provider_customer_id.to_string());
        Ok(())
    }

    fn insert_invoice(&self, invoice: &Invoice) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.invoices.contains_key(&invoice.id) {
            return Ok(false);
        }
        inner.invoices.insert(invoice.id.clone(), invoice.clone());
        Ok(true)
    }

    fn invoice(&self, id: &str) -> Result<Option<Invoice>, KoshaError> {
        Ok(self.inner.lock().unwrap().invoices.get(id).cloned())
    }

    fn invoices(&self, tenant: Option<&str>) -> Result<Vec<Invoice>, KoshaError> {
        let inner = self.inner.lock().unwrap();
        let mut invoices: Vec<Invoice> = inner
            .invoices
            .values()
            .filter(|i| tenant.is_none_or(|t| t == i.tenant))
            .cloned()
            .collect();
        invoices.sort_by(|a, b| (&a.tenant, &a.period).cmp(&(&b.tenant, &b.period)));
        Ok(invoices)
    }

    fn set_provider_invoice(
        &self,
        id: &str,
        provider_invoice_id: &str,
    ) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        Ok(inner
            .invoices
            .get_mut(id)
            .map(|invoice| invoice.provider_invoice_id = Some(provider_invoice_id.to_string()))
            .is_some())
    }

    fn set_invoice_status(
        &self,
        id: &str,
        status: InvoiceStatus,
        paid_at_ms: Option<u64>,
    ) -> Result<bool, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        Ok(inner
            .invoices
            .get_mut(id)
            .map(|invoice| {
                invoice.status = status;
                invoice.paid_at_ms = paid_at_ms.filter(|_| status == InvoiceStatus::Paid);
            })
            .is_some())
    }
}

// ─── Provider ───────────────────────────────────────────────────────────────

/// An invoice's state at the provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderInvoiceStatus {
    /// Not paid (yet): draft, open, or a failed collection.
    Open,
    Paid,
    Void,
}

/// The payment provider: it holds customers and payment methods, collects
/// invoices, and says whether they were paid.
pub trait BillingProvider: Send + Sync {
    /// A short name for logs and API answers.
    fn name(&self) -> &str;

    /// Create (or find) the customer of `tenant`; returns its id.
    fn create_customer(&self, tenant: &str) -> Result<String, KoshaError>;

    /// Hand over `invoice` for collection from `customer`; returns the
    /// provider's invoice id. Must be idempotent on `invoice.id`: a second
    /// call for the same invoice returns the first call's id and charges
    /// nothing more.
    fn create_invoice(&self, customer: &str, invoice: &Invoice) -> Result<String, KoshaError>;

    fn invoice_status(
        &self,
        provider_invoice_id: &str,
    ) -> Result<ProviderInvoiceStatus, KoshaError>;
}

/// What a [`FakeBillingProvider`] was asked to collect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeInvoice {
    pub provider_invoice_id: String,
    pub customer: String,
    pub invoice: Invoice,
    pub status: ProviderInvoiceStatus,
}

#[derive(Default)]
struct FakeProviderInner {
    customers: BTreeMap<String, String>,
    /// Keyed by our invoice id.
    invoices: BTreeMap<String, FakeInvoice>,
    fail: bool,
    create_invoice_calls: u64,
}

/// An in-memory [`BillingProvider`] for tests and local runs: it records
/// what it is given, and a test settles invoices with [`Self::pay`].
#[derive(Default)]
pub struct FakeBillingProvider {
    inner: Mutex<FakeProviderInner>,
}

impl FakeBillingProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make every call fail (the provider is unreachable) or work again.
    pub fn set_failing(&self, failing: bool) {
        self.inner.lock().unwrap().fail = failing;
    }

    /// Settle an invoice by its provider id. `false` when there is none.
    pub fn pay(&self, provider_invoice_id: &str) -> bool {
        self.set_status(provider_invoice_id, ProviderInvoiceStatus::Paid)
    }

    /// Cancel an invoice by its provider id. `false` when there is none.
    pub fn void(&self, provider_invoice_id: &str) -> bool {
        self.set_status(provider_invoice_id, ProviderInvoiceStatus::Void)
    }

    fn set_status(&self, provider_invoice_id: &str, status: ProviderInvoiceStatus) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner
            .invoices
            .values_mut()
            .find(|i| i.provider_invoice_id == provider_invoice_id)
            .map(|i| i.status = status)
            .is_some()
    }

    /// Everything submitted, ordered by our invoice id.
    pub fn submitted(&self) -> Vec<FakeInvoice> {
        self.inner
            .lock()
            .unwrap()
            .invoices
            .values()
            .cloned()
            .collect()
    }

    /// How many times `create_invoice` was called, repeats included.
    pub fn create_invoice_calls(&self) -> u64 {
        self.inner.lock().unwrap().create_invoice_calls
    }

    fn check(inner: &FakeProviderInner) -> Result<(), KoshaError> {
        if inner.fail {
            Err(KoshaError::ControlPlane(
                "fake billing provider: unreachable".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl BillingProvider for FakeBillingProvider {
    fn name(&self) -> &str {
        "fake"
    }

    fn create_customer(&self, tenant: &str) -> Result<String, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        Self::check(&inner)?;
        Ok(inner
            .customers
            .entry(tenant.to_string())
            .or_insert_with(|| format!("cus_fake_{tenant}"))
            .clone())
    }

    fn create_invoice(&self, customer: &str, invoice: &Invoice) -> Result<String, KoshaError> {
        let mut inner = self.inner.lock().unwrap();
        Self::check(&inner)?;
        inner.create_invoice_calls += 1;
        let next = inner.invoices.len() + 1;
        Ok(inner
            .invoices
            .entry(invoice.id.clone())
            .or_insert_with(|| FakeInvoice {
                provider_invoice_id: format!("in_fake_{next}"),
                customer: customer.to_string(),
                invoice: invoice.clone(),
                status: ProviderInvoiceStatus::Open,
            })
            .provider_invoice_id
            .clone())
    }

    fn invoice_status(
        &self,
        provider_invoice_id: &str,
    ) -> Result<ProviderInvoiceStatus, KoshaError> {
        let inner = self.inner.lock().unwrap();
        Self::check(&inner)?;
        inner
            .invoices
            .values()
            .find(|i| i.provider_invoice_id == provider_invoice_id)
            .map(|i| i.status)
            .ok_or_else(|| {
                KoshaError::NotFound(format!("fake billing provider: {provider_invoice_id}"))
            })
    }
}

// ─── The jobs ───────────────────────────────────────────────────────────────

/// What one [`Biller::close_period`] run did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CloseReport {
    pub period: String,
    /// Ids of invoices this run created.
    pub created: Vec<String>,
    /// Invoices that already existed (an earlier run).
    pub existing: usize,
    /// Invoices this run handed to the provider.
    pub submitted: usize,
    /// `(tenant, what went wrong)`. The tenant's invoice is created or
    /// submitted by the next run.
    pub errors: Vec<(String, String)>,
}

/// What one [`Biller::sync_payments`] run did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SyncReport {
    /// Open invoices checked at the provider.
    pub checked: usize,
    pub paid: Vec<String>,
    pub voided: Vec<String>,
    /// `(invoice id, what went wrong)`.
    pub errors: Vec<(String, String)>,
}

/// The billing jobs over a catalog, a billing store, the usage store and
/// (optionally) a payment provider. Without a provider invoices are only
/// recorded, to be collected and marked paid by hand.
pub struct Biller<'a> {
    pub catalog: &'a Catalog,
    pub store: &'a dyn BillingStore,
    pub usage: &'a dyn UsageStore,
    pub provider: Option<&'a dyn BillingProvider>,
}

fn by_tenant(usage: Vec<NamespaceUsage>) -> BTreeMap<String, Vec<NamespaceUsage>> {
    let mut map: BTreeMap<String, Vec<NamespaceUsage>> = BTreeMap::new();
    for namespace in usage {
        map.entry(namespace.tenant.clone())
            .or_default()
            .push(namespace);
    }
    map
}

impl Biller<'_> {
    /// Invoice every tenant for `period`, which must be over at `now_ms`.
    ///
    /// Invoiced: each account that existed before the period ended, and,
    /// when the catalog has a default plan, each other tenant with usage in
    /// the period. Usage recorded under no tenant (pre-tenancy namespaces)
    /// is not invoiced. An invoice that totals zero is stored as paid and
    /// never submitted. A tenant on the default plan gets an account when
    /// its first non-zero invoice is submitted, which fixes it to that
    /// plan.
    ///
    /// Safe to run again: existing invoices are kept as they are, and ones
    /// not yet submitted are submitted.
    pub fn close_period(
        &self,
        period: BillingPeriod,
        now_ms: u64,
    ) -> Result<CloseReport, KoshaError> {
        let (start_ms, end_ms) = (period.start_ms(), period.end_ms());
        if end_ms > now_ms {
            return Err(KoshaError::InvalidQuery(format!(
                "billing period {period} is not over yet"
            )));
        }
        let usage = by_tenant(usage_summary(self.usage, None, start_ms, end_ms)?);
        let accounts: BTreeMap<String, BillingAccount> = self
            .store
            .accounts()?
            .into_iter()
            .map(|a| (a.tenant.clone(), a))
            .collect();
        let mut tenants: BTreeSet<&str> = accounts
            .values()
            .filter(|a| a.created_at_ms < end_ms)
            .map(|a| a.tenant.as_str())
            .collect();
        if self.catalog.default_plan.is_some() {
            tenants.extend(
                usage
                    .keys()
                    .filter(|t| !accounts.contains_key(*t))
                    .map(String::as_str),
            );
        }
        tenants.remove("");

        let mut report = CloseReport {
            period: period.to_string(),
            ..Default::default()
        };
        for tenant in tenants {
            if let Err(e) = self.invoice_tenant(
                tenant,
                accounts.get(tenant),
                usage.get(tenant).map_or(&[][..], Vec::as_slice),
                period,
                now_ms,
                &mut report,
            ) {
                report.errors.push((tenant.to_string(), e.to_string()));
            }
        }
        Ok(report)
    }

    fn invoice_tenant(
        &self,
        tenant: &str,
        account: Option<&BillingAccount>,
        usage: &[NamespaceUsage],
        period: BillingPeriod,
        now_ms: u64,
        report: &mut CloseReport,
    ) -> Result<(), KoshaError> {
        let id = invoice_id(tenant, period);
        let invoice = match self.store.invoice(&id)? {
            Some(existing) => {
                report.existing += 1;
                existing
            }
            None => {
                let plan_name = self
                    .catalog
                    .plan_name_for(account)
                    .ok_or_else(|| KoshaError::NotFound("no plan".into()))?;
                let plan = self.catalog.plan(plan_name).ok_or_else(|| {
                    KoshaError::NotFound(format!("plan {plan_name:?} is not in the catalog"))
                })?;
                let lines = rate(plan, &UsageTotals::of(usage), period.hours());
                let total = total_cents(&lines);
                let invoice = Invoice {
                    id: id.clone(),
                    tenant: tenant.to_string(),
                    period: period.to_string(),
                    period_start_ms: period.start_ms(),
                    period_end_ms: period.end_ms(),
                    plan: plan_name.to_string(),
                    lines,
                    total_cents: total,
                    issued_at_ms: now_ms,
                    due_at_ms: now_ms.saturating_add(
                        self.catalog
                            .dunning
                            .payment_terms_days
                            .saturating_mul(DAY_MS),
                    ),
                    status: if total == 0 {
                        InvoiceStatus::Paid
                    } else {
                        InvoiceStatus::Open
                    },
                    paid_at_ms: (total == 0).then_some(now_ms),
                    provider_invoice_id: None,
                };
                if self.store.insert_invoice(&invoice)? {
                    report.created.push(id.clone());
                    invoice
                } else {
                    // Another run inserted it between the read and the
                    // insert; theirs is the invoice.
                    report.existing += 1;
                    self.store.invoice(&id)?.unwrap_or(invoice)
                }
            }
        };
        let Some(provider) = self.provider else {
            return Ok(());
        };
        if invoice.status != InvoiceStatus::Open
            || invoice.total_cents == 0
            || invoice.provider_invoice_id.is_some()
        {
            return Ok(());
        }
        let customer = match account.and_then(|a| a.provider_customer_id.clone()) {
            Some(customer) => customer,
            None => {
                let customer = provider.create_customer(tenant)?;
                // Only the customer id: `account` was read when the run
                // started, and writing its plan back would undo a plan
                // change made since.
                self.store
                    .set_provider_customer(tenant, &invoice.plan, &customer, now_ms)?;
                customer
            }
        };
        let provider_invoice_id = provider.create_invoice(&customer, &invoice)?;
        self.store.set_provider_invoice(&id, &provider_invoice_id)?;
        report.submitted += 1;
        Ok(())
    }

    /// Ask the provider about every open, submitted invoice and record the
    /// ones it reports paid or void. Nothing to do without a provider.
    pub fn sync_payments(&self, now_ms: u64) -> Result<SyncReport, KoshaError> {
        let mut report = SyncReport::default();
        let Some(provider) = self.provider else {
            return Ok(report);
        };
        for invoice in self.store.invoices(None)? {
            let (InvoiceStatus::Open, Some(provider_id)) =
                (invoice.status, invoice.provider_invoice_id.as_deref())
            else {
                continue;
            };
            report.checked += 1;
            let result = provider.invoice_status(provider_id).and_then(|status| {
                match status {
                    ProviderInvoiceStatus::Open => {}
                    ProviderInvoiceStatus::Paid => {
                        self.store.set_invoice_status(
                            &invoice.id,
                            InvoiceStatus::Paid,
                            Some(now_ms),
                        )?;
                        report.paid.push(invoice.id.clone());
                    }
                    ProviderInvoiceStatus::Void => {
                        self.store
                            .set_invoice_status(&invoice.id, InvoiceStatus::Void, None)?;
                        report.voided.push(invoice.id.clone());
                    }
                }
                Ok(())
            });
            if let Err(e) = result {
                report.errors.push((invoice.id.clone(), e.to_string()));
            }
        }
        Ok(report)
    }

    /// Month-to-date usage of one tenant at `now_ms`.
    pub fn month_to_date(&self, tenant: &str, now_ms: u64) -> Result<UsageTotals, KoshaError> {
        let period = BillingPeriod::containing(now_ms);
        let usage = usage_summary(
            self.usage,
            Some(tenant),
            period.start_ms(),
            now_ms.saturating_add(1),
        )?;
        Ok(UsageTotals::of(&usage))
    }

    /// The standing of every tenant billing knows about at `now_ms`:
    /// tenants with an account or an unpaid invoice and, when the catalog
    /// has a default plan, tenants with usage this month. A tenant whose
    /// plan is not in the catalog is left out (and so not restricted).
    ///
    /// Reads the month's usage only when a free plan with limits is in
    /// use.
    pub fn standings(&self, now_ms: u64) -> Result<BTreeMap<String, TenantStanding>, KoshaError> {
        let accounts: BTreeMap<String, BillingAccount> = self
            .store
            .accounts()?
            .into_iter()
            .map(|a| (a.tenant.clone(), a))
            .collect();
        let mut unpaid_due: BTreeMap<String, u64> = BTreeMap::new();
        for invoice in self.store.invoices(None)? {
            if invoice.status == InvoiceStatus::Open {
                let due = unpaid_due
                    .entry(invoice.tenant.clone())
                    .or_insert(invoice.due_at_ms);
                *due = (*due).min(invoice.due_at_ms);
            }
        }
        let limited_free = |name: &str| {
            self.catalog
                .plan(name)
                .is_some_and(|p| p.kind == PlanKind::Free && !p.meters.is_empty())
        };
        let needs_usage = accounts.values().any(|a| limited_free(&a.plan))
            || self
                .catalog
                .default_plan
                .as_deref()
                .is_some_and(limited_free);
        let usage = if needs_usage {
            let period = BillingPeriod::containing(now_ms);
            by_tenant(usage_summary(
                self.usage,
                None,
                period.start_ms(),
                now_ms.saturating_add(1),
            )?)
        } else {
            BTreeMap::new()
        };

        let mut tenants: BTreeSet<&str> = accounts.keys().map(String::as_str).collect();
        tenants.extend(unpaid_due.keys().map(String::as_str));
        if self.catalog.default_plan.is_some() {
            tenants.extend(usage.keys().map(String::as_str));
        }
        tenants.remove("");

        let mut standings = BTreeMap::new();
        for tenant in tenants {
            let Some(plan_name) = self.catalog.plan_name_for(accounts.get(tenant)) else {
                continue;
            };
            let Some(plan) = self.catalog.plan(plan_name) else {
                continue;
            };
            let oldest_unpaid_due_ms = unpaid_due.get(tenant).copied();
            let totals = UsageTotals::of(usage.get(tenant).map_or(&[][..], Vec::as_slice));
            standings.insert(
                tenant.to_string(),
                TenantStanding {
                    plan: plan_name.to_string(),
                    kind: plan.kind,
                    state: account_state(
                        plan.kind,
                        oldest_unpaid_due_ms,
                        now_ms,
                        &self.catalog.dunning,
                    ),
                    free_tier_exceeded: free_tier_excess(plan, &totals),
                    oldest_unpaid_due_ms,
                },
            );
        }
        Ok(standings)
    }
}

#[cfg(feature = "postgres")]
pub use pg::PgBillingStore;

#[cfg(feature = "postgres")]
mod pg {
    use super::{BillingAccount, BillingStore, Invoice, InvoiceStatus, LineItem};
    use kosha_core::KoshaError;
    use std::time::Duration;

    fn control_plane_error(e: sqlx::Error) -> KoshaError {
        KoshaError::ControlPlane(e.to_string())
    }

    /// Postgres clamps: amounts and times are `BIGINT`.
    fn to_i64(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    fn to_u64(value: i64) -> u64 {
        u64::try_from(value).unwrap_or(0)
    }

    type AccountRow = (String, String, Option<String>, i64);

    fn account_from(row: AccountRow) -> BillingAccount {
        let (tenant, plan, provider_customer_id, created_at_ms) = row;
        BillingAccount {
            tenant,
            plan,
            provider_customer_id,
            created_at_ms: to_u64(created_at_ms),
        }
    }

    #[derive(sqlx::FromRow)]
    struct InvoiceRow {
        invoice_id: String,
        tenant_id: String,
        period: String,
        period_start_ms: i64,
        period_end_ms: i64,
        plan: String,
        line_items: String,
        total_cents: i64,
        issued_at_ms: i64,
        due_at_ms: i64,
        status: String,
        paid_at_ms: Option<i64>,
        provider_invoice_id: Option<String>,
    }

    fn invoice_from(row: InvoiceRow) -> Result<Invoice, KoshaError> {
        let corrupt = |what: &str| {
            KoshaError::ControlPlane(format!(
                "kosha.billing_invoices {:?}: {what}",
                row.invoice_id
            ))
        };
        let lines: Vec<LineItem> =
            serde_json::from_str(&row.line_items).map_err(|e| corrupt(&e.to_string()))?;
        let status = InvoiceStatus::parse(&row.status).ok_or_else(|| corrupt("unknown status"))?;
        Ok(Invoice {
            id: row.invoice_id,
            tenant: row.tenant_id,
            period: row.period,
            period_start_ms: to_u64(row.period_start_ms),
            period_end_ms: to_u64(row.period_end_ms),
            plan: row.plan,
            lines,
            total_cents: to_u64(row.total_cents),
            issued_at_ms: to_u64(row.issued_at_ms),
            due_at_ms: to_u64(row.due_at_ms),
            status,
            paid_at_ms: row.paid_at_ms.map(to_u64),
            provider_invoice_id: row.provider_invoice_id,
        })
    }

    const INVOICE_COLUMNS: &str = "invoice_id, tenant_id, period, period_start_ms, \
         period_end_ms, plan, line_items, total_cents, issued_at_ms, due_at_ms, status, \
         paid_at_ms, provider_invoice_id";

    /// `kosha.billing_accounts` and `kosha.billing_invoices` (migration
    /// 007) behind their own small pool, like the usage store.
    pub struct PgBillingStore {
        pool: sqlx::PgPool,
        rt: tokio::runtime::Runtime,
    }

    impl PgBillingStore {
        /// A store that connects on first use. Assumes the tables exist:
        /// `PgStore::new` applies migration 007 at startup.
        pub fn new(database_url: &str) -> Result<Self, String> {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
            let pool = {
                let _guard = rt.enter();
                crate::postgres::writer_pool_options()
                    .max_connections(2)
                    .acquire_timeout(Duration::from_secs(5))
                    .connect_lazy(database_url)
                    .map_err(|e| format!("invalid postgres url: {e}"))?
            };
            Ok(Self { pool, rt })
        }
    }

    impl BillingStore for PgBillingStore {
        fn account(&self, tenant: &str) -> Result<Option<BillingAccount>, KoshaError> {
            self.rt.block_on(async {
                let row: Option<AccountRow> = sqlx::query_as(
                    "SELECT tenant_id, plan, provider_customer_id, created_at_ms \
                     FROM kosha.billing_accounts WHERE tenant_id = $1",
                )
                .bind(tenant)
                .fetch_optional(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(row.map(account_from))
            })
        }

        fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<AccountRow> = sqlx::query_as(
                    "SELECT tenant_id, plan, provider_customer_id, created_at_ms \
                     FROM kosha.billing_accounts ORDER BY tenant_id",
                )
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(rows.into_iter().map(account_from).collect())
            })
        }

        fn put_account(&self, account: &BillingAccount) -> Result<(), KoshaError> {
            self.rt.block_on(async {
                sqlx::query(
                    "INSERT INTO kosha.billing_accounts \
                     (tenant_id, plan, provider_customer_id, created_at_ms) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (tenant_id) DO UPDATE SET \
                         plan = EXCLUDED.plan, \
                         provider_customer_id = COALESCE( \
                             EXCLUDED.provider_customer_id, \
                             kosha.billing_accounts.provider_customer_id), \
                         updated_at = now()",
                )
                .bind(&account.tenant)
                .bind(&account.plan)
                .bind(&account.provider_customer_id)
                .bind(to_i64(account.created_at_ms))
                .execute(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(())
            })
        }

        fn set_provider_customer(
            &self,
            tenant: &str,
            plan_if_new: &str,
            provider_customer_id: &str,
            now_ms: u64,
        ) -> Result<(), KoshaError> {
            self.rt.block_on(async {
                sqlx::query(
                    "INSERT INTO kosha.billing_accounts \
                     (tenant_id, plan, provider_customer_id, created_at_ms) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (tenant_id) DO UPDATE SET \
                         provider_customer_id = EXCLUDED.provider_customer_id, \
                         updated_at = now()",
                )
                .bind(tenant)
                .bind(plan_if_new)
                .bind(provider_customer_id)
                .bind(to_i64(now_ms))
                .execute(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(())
            })
        }

        fn insert_invoice(&self, invoice: &Invoice) -> Result<bool, KoshaError> {
            let lines = serde_json::to_string(&invoice.lines).map_err(KoshaError::Serde)?;
            self.rt.block_on(async {
                let result = sqlx::query(
                    "INSERT INTO kosha.billing_invoices \
                     (invoice_id, tenant_id, period, period_start_ms, period_end_ms, plan, \
                      line_items, total_cents, issued_at_ms, due_at_ms, status, paid_at_ms, \
                      provider_invoice_id) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
                     ON CONFLICT DO NOTHING",
                )
                .bind(&invoice.id)
                .bind(&invoice.tenant)
                .bind(&invoice.period)
                .bind(to_i64(invoice.period_start_ms))
                .bind(to_i64(invoice.period_end_ms))
                .bind(&invoice.plan)
                .bind(lines)
                .bind(to_i64(invoice.total_cents))
                .bind(to_i64(invoice.issued_at_ms))
                .bind(to_i64(invoice.due_at_ms))
                .bind(invoice.status.as_str())
                .bind(invoice.paid_at_ms.map(to_i64))
                .bind(&invoice.provider_invoice_id)
                .execute(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(result.rows_affected() == 1)
            })
        }

        fn invoice(&self, id: &str) -> Result<Option<Invoice>, KoshaError> {
            self.rt.block_on(async {
                let row: Option<InvoiceRow> = sqlx::query_as(&format!(
                    "SELECT {INVOICE_COLUMNS} FROM kosha.billing_invoices WHERE invoice_id = $1"
                ))
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(control_plane_error)?;
                row.map(invoice_from).transpose()
            })
        }

        fn invoices(&self, tenant: Option<&str>) -> Result<Vec<Invoice>, KoshaError> {
            self.rt.block_on(async {
                let rows: Vec<InvoiceRow> = sqlx::query_as(&format!(
                    "SELECT {INVOICE_COLUMNS} FROM kosha.billing_invoices \
                     WHERE ($1::text IS NULL OR tenant_id = $1) \
                     ORDER BY tenant_id, period"
                ))
                .bind(tenant)
                .fetch_all(&self.pool)
                .await
                .map_err(control_plane_error)?;
                rows.into_iter().map(invoice_from).collect()
            })
        }

        fn set_provider_invoice(
            &self,
            id: &str,
            provider_invoice_id: &str,
        ) -> Result<bool, KoshaError> {
            self.rt.block_on(async {
                let result = sqlx::query(
                    "UPDATE kosha.billing_invoices \
                     SET provider_invoice_id = $2, updated_at = now() WHERE invoice_id = $1",
                )
                .bind(id)
                .bind(provider_invoice_id)
                .execute(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(result.rows_affected() == 1)
            })
        }

        fn set_invoice_status(
            &self,
            id: &str,
            status: InvoiceStatus,
            paid_at_ms: Option<u64>,
        ) -> Result<bool, KoshaError> {
            let paid_at_ms = paid_at_ms
                .filter(|_| status == InvoiceStatus::Paid)
                .map(to_i64);
            self.rt.block_on(async {
                let result = sqlx::query(
                    "UPDATE kosha.billing_invoices \
                     SET status = $2, paid_at_ms = $3, updated_at = now() WHERE invoice_id = $1",
                )
                .bind(id)
                .bind(status.as_str())
                .bind(paid_at_ms)
                .execute(&self.pool)
                .await
                .map_err(control_plane_error)?;
                Ok(result.rows_affected() == 1)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::usage::{meter, MemoryUsageStore, UsageEvent};

    const GIB: u64 = 1 << 30;

    fn catalog() -> Catalog {
        Catalog::parse(
            r#"{
              "default_plan": "free",
              "plans": {
                "free": {"kind": "free",
                         "meters": {"queries": {"included": 100},
                                    "bytes_written": {"included": 1000},
                                    "stored_bytes": {"included": 5000}},
                         "limits": {"qps": 5, "namespaces": 3}},
                "payg": {"kind": "usage", "base_fee_cents": 2500,
                         "meters": {
                           "queries": {"included": 1000000, "unit": 1000000, "unit_price_cents": 400},
                           "bytes_queried": {"unit": 1099511627776, "unit_price_cents": 500},
                           "bytes_written": {"unit": 1073741824, "unit_price_cents": 200},
                           "stored_bytes": {"included": 1073741824, "unit": 1073741824,
                                            "unit_price_cents": 33}}},
                "ent": {"kind": "enterprise", "base_fee_cents": 500000}
              }
            }"#,
        )
        .unwrap()
    }

    fn event(tenant: &str, id: &str, ns: &str, at_ms: u64, meters: &[(&str, u64)]) -> UsageEvent {
        UsageEvent {
            id: format!("{tenant}/{id}"),
            tenant: tenant.into(),
            namespace: ns.into(),
            at_ms,
            meters: meters.iter().map(|(m, q)| (m.to_string(), *q)).collect(),
        }
    }

    fn account(tenant: &str, plan: &str, created_at_ms: u64) -> BillingAccount {
        BillingAccount {
            tenant: tenant.into(),
            plan: plan.into(),
            provider_customer_id: None,
            created_at_ms,
        }
    }

    /// A store in which an operator's plan change lands between the
    /// monthly job's read of the accounts and its write of the provider
    /// customer id.
    struct UpgradeDuringClose {
        inner: MemoryBillingStore,
        upgraded: Mutex<bool>,
    }

    impl BillingStore for UpgradeDuringClose {
        fn account(&self, tenant: &str) -> Result<Option<BillingAccount>, KoshaError> {
            self.inner.account(tenant)
        }
        fn accounts(&self) -> Result<Vec<BillingAccount>, KoshaError> {
            self.inner.accounts()
        }
        fn put_account(&self, account: &BillingAccount) -> Result<(), KoshaError> {
            self.inner.put_account(account)
        }
        fn set_provider_customer(
            &self,
            tenant: &str,
            plan_if_new: &str,
            provider_customer_id: &str,
            now_ms: u64,
        ) -> Result<(), KoshaError> {
            self.inner
                .set_provider_customer(tenant, plan_if_new, provider_customer_id, now_ms)
        }
        fn insert_invoice(&self, invoice: &Invoice) -> Result<bool, KoshaError> {
            let mut upgraded = self.upgraded.lock().unwrap();
            if !*upgraded {
                *upgraded = true;
                self.inner
                    .put_account(&account(&invoice.tenant, "ent", 0))?;
            }
            self.inner.insert_invoice(invoice)
        }
        fn invoice(&self, id: &str) -> Result<Option<Invoice>, KoshaError> {
            self.inner.invoice(id)
        }
        fn invoices(&self, tenant: Option<&str>) -> Result<Vec<Invoice>, KoshaError> {
            self.inner.invoices(tenant)
        }
        fn set_provider_invoice(&self, id: &str, provider_id: &str) -> Result<bool, KoshaError> {
            self.inner.set_provider_invoice(id, provider_id)
        }
        fn set_invoice_status(
            &self,
            id: &str,
            status: InvoiceStatus,
            paid_at_ms: Option<u64>,
        ) -> Result<bool, KoshaError> {
            self.inner.set_invoice_status(id, status, paid_at_ms)
        }
    }

    #[test]
    fn the_monthly_job_and_a_plan_change_do_not_undo_each_other() {
        let catalog = catalog();
        let oct = BillingPeriod::parse("2026-10").unwrap();
        let store = UpgradeDuringClose {
            inner: MemoryBillingStore::new(),
            upgraded: Mutex::new(false),
        };
        let usage = MemoryUsageStore::new();
        let provider = FakeBillingProvider::new();
        store.put_account(&account("acme", "payg", 5)).unwrap();
        let biller = Biller {
            catalog: &catalog,
            store: &store,
            usage: &usage,
            provider: Some(&provider),
        };
        let report = biller.close_period(oct, oct.end_ms()).unwrap();
        assert_eq!((report.submitted, report.errors.len()), (1, 0));
        // The plan change made during the run survives it, together with
        // the customer the run created.
        assert_eq!(
            store.account("acme").unwrap(),
            Some(BillingAccount {
                tenant: "acme".into(),
                plan: "ent".into(),
                provider_customer_id: Some("cus_fake_acme".into()),
                created_at_ms: 5,
            })
        );
        // A later plan change that does not name the customer keeps it.
        store.put_account(&account("acme", "payg", 99)).unwrap();
        let acme = store.account("acme").unwrap().unwrap();
        assert_eq!(
            (acme.plan.as_str(), acme.provider_customer_id.as_deref()),
            ("payg", Some("cus_fake_acme"))
        );
        // A tenant without an account gets one on the given plan.
        store
            .set_provider_customer("newco", "payg", "cus_9", 77)
            .unwrap();
        assert_eq!(
            store.account("newco").unwrap(),
            Some(BillingAccount {
                tenant: "newco".into(),
                plan: "payg".into(),
                provider_customer_id: Some("cus_9".into()),
                created_at_ms: 77,
            })
        );
    }

    #[test]
    fn periods_are_utc_calendar_months() {
        let oct = BillingPeriod::parse("2026-10").unwrap();
        assert_eq!(oct.start_ms(), 1_790_812_800_000); // 2026-10-01T00:00:00Z
        assert_eq!(oct.end_ms(), 1_793_491_200_000); // 2026-11-01T00:00:00Z
        assert_eq!(oct.hours(), 31 * 24);
        assert_eq!(oct.to_string(), "2026-10");
        assert_eq!(BillingPeriod::containing(oct.start_ms()), oct);
        assert_eq!(BillingPeriod::containing(oct.end_ms() - 1), oct);
        assert_eq!(BillingPeriod::containing(oct.end_ms()), oct.next());
        assert_eq!(oct.previous().to_string(), "2026-09");

        let dec = BillingPeriod::parse("2027-12").unwrap();
        assert_eq!(dec.next().to_string(), "2028-01");
        assert_eq!(dec.next().previous(), dec);
        // Leap and non-leap February.
        assert_eq!(BillingPeriod::parse("2028-02").unwrap().hours(), 29 * 24);
        assert_eq!(BillingPeriod::parse("2027-02").unwrap().hours(), 28 * 24);
        assert_eq!(BillingPeriod::parse("2100-02").unwrap().hours(), 28 * 24);
        assert_eq!(BillingPeriod::containing(0).to_string(), "1970-01");
        assert_eq!(
            BillingPeriod::containing(0).previous().to_string(),
            "1970-01"
        );

        // Every month boundary for 80 years round-trips.
        let mut period = BillingPeriod::parse("1970-01").unwrap();
        let mut start = 0;
        for _ in 0..(80 * 12) {
            assert_eq!(period.start_ms(), start);
            assert_eq!(BillingPeriod::containing(start), period);
            assert!((28 * 24..=31 * 24).contains(&period.hours()));
            start = period.end_ms();
            period = period.next();
        }

        for bad in [
            "2026", "2026-13", "2026-00", "26-10", "2026-1", "1969-12", "x-10", "",
        ] {
            assert!(BillingPeriod::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn catalog_rejects_what_it_cannot_honour() {
        let parse = |plans: &str| Catalog::parse(&format!(r#"{{"plans": {plans}}}"#));
        assert!(parse(r#"{"p": {"kind": "usage"}}"#).is_ok());
        for (bad, why) in [
            (
                r#"{"p": {"kind": "usage", "meters": {"cpu": {}}}}"#,
                "unknown meter",
            ),
            (
                r#"{"p": {"kind": "usage", "meters": {"queries": {"unit": 0}}}}"#,
                "unit must be",
            ),
            (
                r#"{"p": {"kind": "free", "meters": {"queries": {"unit_price_cents": 1}}}}"#,
                "free plan cannot price",
            ),
            (
                r#"{"p": {"kind": "free", "base_fee_cents": 1}}"#,
                "base fee",
            ),
            (
                r#"{"p": {"kind": "usage", "limits": {"qps": 0}}}"#,
                "positive",
            ),
            (
                r#"{"p": {"kind": "usage", "limits": {"weight": 0}}}"#,
                "weight",
            ),
            (r#"{"p": {"kind": "gold"}}"#, "invalid billing plans JSON"),
            (
                r#"{"p": {"kind": "usage", "price": 1}}"#,
                "invalid billing plans JSON",
            ),
        ] {
            let err = parse(bad).unwrap_err();
            assert!(err.contains(why), "{bad}: {err}");
        }
        let err = Catalog::parse(r#"{"default_plan": "nope", "plans": {}}"#).unwrap_err();
        assert!(err.contains("not defined"), "{err}");
        let err = Catalog::parse(
            r#"{"dunning": {"read_only_after_days": 9, "suspend_after_days": 3}, "plans": {}}"#,
        )
        .unwrap_err();
        assert!(err.contains("read_only_after_days"), "{err}");
        assert_eq!(catalog().dunning, DunningPolicy::default());
    }

    #[test]
    fn rating_charges_only_above_the_included_amount_and_rounds_half_up() {
        let catalog = catalog();
        let payg = catalog.plan("payg").unwrap();
        let totals = UsageTotals {
            queries: 3_500_000,                // 2.5M billable at $4/M  = 1000
            bytes_queried: 3 * (1u64 << 40),   // 3 TiB at $5/TiB        = 1500
            bytes_written: 10 * GIB + GIB / 4, // 10.25 GiB at $2/GiB  = 2050
            stored_byte_hours: 51 * GIB * 744, // avg 51 GiB, 50 billable at 33c = 1650
            stored_bytes_latest: 0,
        };
        let lines = rate(payg, &totals, 744);
        let amounts: Vec<(&str, u64, u64)> = lines
            .iter()
            .map(|l| (l.meter.as_str(), l.billable, l.amount_cents))
            .collect();
        assert_eq!(
            amounts,
            vec![
                ("base_fee", 1, 2500),
                ("bytes_queried", 3 * (1u64 << 40), 1500),
                ("bytes_written", 10 * GIB + GIB / 4, 2050),
                ("queries", 2_500_000, 1000),
                ("stored_bytes", 50 * GIB, 1650),
            ]
        );
        assert_eq!(total_cents(&lines), 8700);

        // Under the included amount: a line with nothing billable.
        let small = UsageTotals {
            queries: 999_999,
            ..Default::default()
        };
        let lines = rate(payg, &small, 744);
        let queries = lines.iter().find(|l| l.meter == "queries").unwrap();
        assert_eq!(
            (queries.quantity, queries.billable, queries.amount_cents),
            (999_999, 0, 0)
        );
        assert_eq!(total_cents(&lines), 2500);

        // Half a cent rounds up, less rounds down.
        assert_eq!(charge_cents(1, 2, 1), 1);
        assert_eq!(charge_cents(1, 3, 1), 0);
        assert_eq!(charge_cents(1_250_000, 1_000_000, 400), 500);
        assert_eq!(charge_cents(1_251_250, 1_000_000, 400), 501); // 500.5
                                                                  // No overflow, and never above what BIGINT holds.
        assert_eq!(charge_cents(u64::MAX, 1, u64::MAX), i64::MAX as u64);

        // A free plan rates to zero whatever the usage.
        let free = catalog.plan("free").unwrap();
        assert_eq!(total_cents(&rate(free, &totals, 744)), 0);
    }

    #[test]
    fn free_tier_blocks_the_kind_of_use_that_ran_out() {
        let catalog = catalog();
        let free = catalog.plan("free").unwrap();
        let standing = |totals: &UsageTotals| TenantStanding {
            plan: "free".into(),
            kind: free.kind,
            state: AccountState::Active,
            free_tier_exceeded: free_tier_excess(free, totals),
            oldest_unpaid_due_ms: None,
        };
        let under = standing(&UsageTotals {
            queries: 99,
            bytes_written: 999,
            stored_bytes_latest: 4999,
            ..Default::default()
        });
        for op in [
            Operation::Query,
            Operation::Write,
            Operation::Delete,
            Operation::Other,
        ] {
            assert_eq!(under.block(op), None);
        }

        let queries_out = standing(&UsageTotals {
            queries: 100,
            ..Default::default()
        });
        let block = queries_out.block(Operation::Query).unwrap();
        assert_eq!(block.reason(), "free_tier_exceeded");
        assert!(
            block.message().contains("queries is at 100 of 100"),
            "{}",
            block.message()
        );
        assert_eq!(queries_out.block(Operation::Write), None);

        // Storage is judged on what is stored now, and blocks writes but
        // not the deletes that would free space.
        let storage_out = standing(&UsageTotals {
            stored_bytes_latest: 5000,
            stored_byte_hours: 1,
            ..Default::default()
        });
        assert!(matches!(
            storage_out.block(Operation::Write),
            Some(Block::FreeTier(_))
        ));
        assert_eq!(storage_out.block(Operation::Delete), None);
        assert_eq!(storage_out.block(Operation::Query), None);
        assert_eq!(storage_out.block(Operation::Account), None);

        // The same usage on a paid plan restricts nothing.
        let payg = catalog.plan("payg").unwrap();
        let heavy = UsageTotals {
            queries: u64::MAX,
            ..Default::default()
        };
        assert!(free_tier_excess(payg, &heavy).is_empty());
    }

    #[test]
    fn non_payment_goes_read_only_then_suspended_and_payment_restores() {
        let policy = DunningPolicy::default();
        let due = 1_000 * DAY_MS;
        let state = |days_after_due: i64| {
            account_state(
                PlanKind::Usage,
                Some(due),
                (due as i64 + days_after_due * DAY_MS as i64) as u64,
                &policy,
            )
        };
        assert_eq!(state(-1), AccountState::Active);
        assert_eq!(state(0), AccountState::PastDue);
        assert_eq!(state(6), AccountState::PastDue);
        assert_eq!(state(7), AccountState::ReadOnly);
        assert_eq!(state(20), AccountState::ReadOnly);
        assert_eq!(state(21), AccountState::Suspended);
        assert_eq!(state(400), AccountState::Suspended);
        // Paid: no unpaid invoice.
        assert_eq!(
            account_state(PlanKind::Usage, None, due + 400 * DAY_MS, &policy),
            AccountState::Active
        );
        // Enterprise accounts are flagged, never restricted.
        assert_eq!(
            account_state(PlanKind::Enterprise, Some(due), due + 400 * DAY_MS, &policy),
            AccountState::PastDue
        );

        let standing = |state| TenantStanding {
            plan: "payg".into(),
            kind: PlanKind::Usage,
            state,
            free_tier_exceeded: Vec::new(),
            oldest_unpaid_due_ms: Some(due),
        };
        let past_due = standing(AccountState::PastDue);
        let read_only = standing(AccountState::ReadOnly);
        let suspended = standing(AccountState::Suspended);
        for op in [
            Operation::Query,
            Operation::Write,
            Operation::Delete,
            Operation::Other,
        ] {
            assert_eq!(past_due.block(op), None);
            assert_eq!(suspended.block(op), Some(Block::Suspended));
        }
        assert_eq!(read_only.block(Operation::Write), Some(Block::ReadOnly));
        for op in [Operation::Query, Operation::Delete, Operation::Other] {
            assert_eq!(read_only.block(op), None);
        }
        assert_eq!(suspended.block(Operation::Account), None);
        assert_eq!(Block::ReadOnly.reason(), "billing_read_only");
        assert_eq!(Block::Suspended.reason(), "billing_suspended");
    }

    /// A month of synthetic usage for `tenant` in `period`: every hour a
    /// stored-bytes snapshot of 3 GiB and 2,000 searches, every day one
    /// write of 1 GiB.
    fn synthetic_month(tenant: &str, period: BillingPeriod) -> Vec<UsageEvent> {
        let mut events = Vec::new();
        for hour in 0..period.hours() {
            let at = period.start_ms() + hour * HOUR_MS;
            events.push(event(
                tenant,
                &format!("stored/{hour}"),
                "docs",
                at,
                &[(meter::STORED_BYTES, 3 * GIB)],
            ));
            events.push(event(
                tenant,
                &format!("q/{hour}"),
                "docs",
                at + 60_000,
                &[(meter::QUERIES, 2_000), (meter::BYTES_RETURNED, 1)],
            ));
            if hour % 24 == 0 {
                events.push(event(
                    tenant,
                    &format!("w/{hour}"),
                    "docs",
                    at + 120_000,
                    &[(meter::BYTES_WRITTEN, GIB), (meter::DOCUMENTS_WRITTEN, 10)],
                ));
            }
        }
        events
    }

    /// The issue's "done when", against the fake provider: a customer with
    /// a month of synthetic usage is invoiced the expected amount, the
    /// provider is asked to collect exactly that, and the payment comes
    /// back.
    #[test]
    fn a_month_of_synthetic_usage_is_charged_correctly_end_to_end() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let usage = MemoryUsageStore::new();
        let provider = FakeBillingProvider::new();
        let oct = BillingPeriod::parse("2026-10").unwrap();
        store
            .put_account(&account("acme", "payg", oct.start_ms() - 1))
            .unwrap();
        usage.record(&synthetic_month("acme", oct)).unwrap();
        // Usage on either side of the month is not October's.
        usage
            .record(&[
                event(
                    "acme",
                    "before",
                    "docs",
                    oct.start_ms() - 1,
                    &[(meter::QUERIES, 9_000_000)],
                ),
                event(
                    "acme",
                    "after",
                    "docs",
                    oct.end_ms(),
                    &[(meter::QUERIES, 9_000_000)],
                ),
            ])
            .unwrap();
        let biller = Biller {
            catalog: &catalog,
            store: &store,
            usage: &usage,
            provider: Some(&provider),
        };

        // Not before the month is over.
        let early = biller.close_period(oct, oct.end_ms() - 1).unwrap_err();
        assert!(early.to_string().contains("not over"), "{early}");

        let now = oct.end_ms() + HOUR_MS;
        let report = biller.close_period(oct, now).unwrap();
        assert_eq!(report.errors, vec![]);
        assert_eq!(report.created, vec!["acme/2026-10".to_string()]);
        assert_eq!(report.submitted, 1);

        // 744 hours. Expected, in cents:
        //   base fee                                               2500
        //   queries   744 * 2000 = 1,488,000; 488,000 over 1M
        //             at 400 per 1M = 195.2                         195
        //   bytes_queried 1,488,000 * 3 GiB = 4359.375 TiB
        //             at 500 per TiB = 2,179,687.5              2179688
        //   bytes_written 31 GiB at 200                            6200
        //   stored_bytes avg 3 GiB, 2 over the included 1, at 33     66
        let invoice = store.invoice("acme/2026-10").unwrap().unwrap();
        let line = |meter: &str| {
            let l = invoice.lines.iter().find(|l| l.meter == meter).unwrap();
            (l.quantity, l.billable, l.amount_cents)
        };
        assert_eq!(line("base_fee"), (1, 1, 2500));
        assert_eq!(line("queries"), (1_488_000, 488_000, 195));
        assert_eq!(
            line("bytes_queried"),
            (1_488_000 * 3 * GIB, 1_488_000 * 3 * GIB, 2_179_688)
        );
        assert_eq!(line("bytes_written"), (31 * GIB, 31 * GIB, 6200));
        assert_eq!(line("stored_bytes"), (3 * GIB, 2 * GIB, 66));
        assert_eq!(invoice.total_cents, 2500 + 195 + 2_179_688 + 6200 + 66);
        assert_eq!(invoice.total_cents, 2_188_649);
        assert_eq!(invoice.status, InvoiceStatus::Open);
        assert_eq!(invoice.plan, "payg");
        assert_eq!(
            (invoice.period_start_ms, invoice.period_end_ms),
            (oct.start_ms(), oct.end_ms())
        );
        assert_eq!(invoice.due_at_ms, now + 14 * DAY_MS);

        // The provider was asked to collect that invoice from the
        // tenant's customer, which the account now remembers.
        let submitted = provider.submitted();
        assert_eq!(submitted.len(), 1);
        assert_eq!(submitted[0].customer, "cus_fake_acme");
        assert_eq!(submitted[0].invoice.total_cents, 2_188_649);
        assert_eq!(total_cents(&submitted[0].invoice.lines), 2_188_649);
        assert_eq!(
            invoice.provider_invoice_id.as_deref(),
            Some(submitted[0].provider_invoice_id.as_str())
        );
        let acme = store.account("acme").unwrap().unwrap();
        assert_eq!(acme.provider_customer_id.as_deref(), Some("cus_fake_acme"));
        assert_eq!(acme.created_at_ms, oct.start_ms() - 1);

        // Running the job again bills nothing more.
        let again = biller.close_period(oct, now + HOUR_MS).unwrap();
        assert_eq!(
            (again.created.len(), again.existing, again.submitted),
            (0, 1, 0)
        );
        assert_eq!(provider.create_invoice_calls(), 1);
        assert_eq!(store.invoices(None).unwrap(), vec![invoice.clone()]);

        // Unpaid, the account walks the non-payment path...
        let state = |at: u64| biller.standings(at).unwrap()["acme"].state;
        assert_eq!(state(now), AccountState::Active);
        assert_eq!(
            state(invoice.due_at_ms + 7 * DAY_MS),
            AccountState::ReadOnly
        );
        assert_eq!(
            state(invoice.due_at_ms + 21 * DAY_MS),
            AccountState::Suspended
        );

        // ...until the customer pays and the payment is synced.
        let sync = biller.sync_payments(now).unwrap();
        assert_eq!((sync.checked, sync.paid.len()), (1, 0));
        assert!(provider.pay(&submitted[0].provider_invoice_id));
        let paid_at = invoice.due_at_ms + 22 * DAY_MS;
        let sync = biller.sync_payments(paid_at).unwrap();
        assert_eq!(sync.paid, vec!["acme/2026-10".to_string()]);
        let paid = store.invoice("acme/2026-10").unwrap().unwrap();
        assert_eq!(
            (paid.status, paid.paid_at_ms),
            (InvoiceStatus::Paid, Some(paid_at))
        );
        assert_eq!(state(paid_at), AccountState::Active);
        assert_eq!(biller.sync_payments(paid_at).unwrap().checked, 0);
    }

    #[test]
    fn close_period_covers_default_plan_tenants_and_survives_provider_failure() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let usage = MemoryUsageStore::new();
        let provider = FakeBillingProvider::new();
        let oct = BillingPeriod::parse("2026-10").unwrap();
        let now = oct.end_ms() + 1;
        store.put_account(&account("acme", "payg", 0)).unwrap();
        store.put_account(&account("bigco", "ent", 0)).unwrap();
        // Created after the period: not invoiced for it.
        store
            .put_account(&account("newco", "payg", oct.end_ms()))
            .unwrap();
        // An account on a plan the catalog no longer has.
        store.put_account(&account("lost", "retired", 0)).unwrap();
        usage
            .record(&[
                event(
                    "acme",
                    "q",
                    "docs",
                    oct.start_ms(),
                    &[(meter::QUERIES, 2_000_000)],
                ),
                // No account: the default (free) plan.
                event(
                    "walkin",
                    "q",
                    "docs",
                    oct.start_ms(),
                    &[(meter::QUERIES, 500)],
                ),
                // Pre-tenancy usage has nobody to invoice.
                event("", "q", "bare", oct.start_ms(), &[(meter::QUERIES, 500)]),
            ])
            .unwrap();
        let biller = Biller {
            catalog: &catalog,
            store: &store,
            usage: &usage,
            provider: Some(&provider),
        };

        provider.set_failing(true);
        let report = biller.close_period(oct, now).unwrap();
        assert_eq!(
            report.created,
            vec!["acme/2026-10", "bigco/2026-10", "walkin/2026-10"]
        );
        // Both paid invoices failed to submit; the retired plan is
        // reported; the free one needs no provider.
        let failed: Vec<&str> = report.errors.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(failed, vec!["acme", "bigco", "lost"]);
        assert!(
            report.errors[2].1.contains("retired"),
            "{:?}",
            report.errors
        );
        assert_eq!(report.submitted, 0);

        let walkin = store.invoice("walkin/2026-10").unwrap().unwrap();
        assert_eq!(
            (walkin.total_cents, walkin.status),
            (0, InvoiceStatus::Paid)
        );
        assert_eq!(walkin.plan, "free");
        assert_eq!(
            walkin
                .lines
                .iter()
                .find(|l| l.meter == "queries")
                .unwrap()
                .quantity,
            500
        );
        // A zero invoice does not turn a default-plan tenant into an
        // account.
        assert_eq!(store.account("walkin").unwrap(), None);

        // The provider comes back: the next run submits what is pending,
        // with the amounts first rated.
        provider.set_failing(false);
        let retry = biller.close_period(oct, now + DAY_MS).unwrap();
        assert_eq!(
            (retry.created.len(), retry.existing, retry.submitted),
            (0, 3, 2)
        );
        let submitted = provider.submitted();
        let totals: Vec<(&str, u64)> = submitted
            .iter()
            .map(|s| (s.invoice.tenant.as_str(), s.invoice.total_cents))
            .collect();
        assert_eq!(totals, vec![("acme", 2500 + 400), ("bigco", 500_000)]);
        assert_eq!(submitted[0].invoice.issued_at_ms, now);

        // With no provider, invoices are recorded and stay open.
        let manual = Biller {
            provider: None,
            ..biller
        };
        let nov = oct.next();
        let report = manual.close_period(nov, nov.end_ms()).unwrap();
        assert_eq!(report.submitted, 0);
        let invoice = store.invoice("bigco/2026-11").unwrap().unwrap();
        assert_eq!(
            (invoice.status, invoice.provider_invoice_id),
            (InvoiceStatus::Open, None)
        );
        assert_eq!(
            manual.sync_payments(nov.end_ms()).unwrap(),
            SyncReport::default()
        );
    }

    #[test]
    fn standings_follow_usage_plan_changes_and_voided_invoices() {
        let catalog = catalog();
        let store = MemoryBillingStore::new();
        let usage = MemoryUsageStore::new();
        let provider = FakeBillingProvider::new();
        let biller = Biller {
            catalog: &catalog,
            store: &store,
            usage: &usage,
            provider: Some(&provider),
        };
        let oct = BillingPeriod::parse("2026-10").unwrap();
        let now = oct.start_ms() + 10 * DAY_MS;
        store.put_account(&account("small", "free", 0)).unwrap();
        usage
            .record(&[
                event(
                    "small",
                    "q1",
                    "docs",
                    now - HOUR_MS,
                    &[(meter::QUERIES, 99)],
                ),
                // Last month's searches do not count against this month.
                event(
                    "small",
                    "old",
                    "docs",
                    oct.start_ms() - 1,
                    &[(meter::QUERIES, 1_000)],
                ),
                event(
                    "walkin",
                    "w",
                    "docs",
                    now - HOUR_MS,
                    &[(meter::BYTES_WRITTEN, 1_000)],
                ),
            ])
            .unwrap();

        let standings = biller.standings(now).unwrap();
        assert_eq!(standings["small"].free_tier_exceeded, vec![]);
        // A tenant with no account is on the default plan.
        assert_eq!(standings["walkin"].plan, "free");
        assert_eq!(
            standings["walkin"]
                .block(Operation::Write)
                .map(|b| b.reason()),
            Some("free_tier_exceeded")
        );
        assert_eq!(biller.month_to_date("small", now).unwrap().queries, 99);

        // One more search uses the allowance up.
        usage
            .record(&[event("small", "q2", "docs", now, &[(meter::QUERIES, 1)])])
            .unwrap();
        let standing = &biller.standings(now).unwrap()["small"];
        assert_eq!(
            standing.free_tier_exceeded,
            vec![Excess {
                meter: "queries".into(),
                used: 100,
                included: 100
            }]
        );
        assert!(standing.block(Operation::Query).is_some());

        // The upgrade path: a paid plan lifts it at once.
        store.put_account(&account("small", "payg", 123)).unwrap();
        let standing = &biller.standings(now).unwrap()["small"];
        assert_eq!(
            (standing.plan.as_str(), standing.block(Operation::Query)),
            ("payg", None)
        );
        assert_eq!(store.account("small").unwrap().unwrap().created_at_ms, 0);
        // And a new month starts the free allowance afresh.
        store.put_account(&account("small", "free", 0)).unwrap();
        let standing = &biller.standings(oct.end_ms()).unwrap()["small"];
        assert_eq!(standing.free_tier_exceeded, vec![]);

        // A voided invoice is not owed.
        store.put_account(&account("small", "payg", 0)).unwrap();
        let close_at = oct.end_ms();
        biller.close_period(oct, close_at).unwrap();
        let invoice = store.invoice("small/2026-10").unwrap().unwrap();
        let late = invoice.due_at_ms + 30 * DAY_MS;
        assert_eq!(
            biller.standings(late).unwrap()["small"].state,
            AccountState::Suspended
        );
        assert!(provider.void(invoice.provider_invoice_id.as_deref().unwrap()));
        let sync = biller.sync_payments(late).unwrap();
        assert_eq!(sync.voided, vec!["small/2026-10".to_string()]);
        assert_eq!(
            biller.standings(late).unwrap()["small"].state,
            AccountState::Active
        );

        // A provider outage during sync is reported per invoice.
        biller
            .close_period(oct.next(), oct.next().end_ms())
            .unwrap();
        provider.set_failing(true);
        let sync = biller.sync_payments(late).unwrap();
        assert_eq!((sync.checked, sync.errors.len()), (1, 1));
    }

    #[test]
    fn memory_store_keeps_one_invoice_per_tenant_and_period() {
        let store = MemoryBillingStore::new();
        let invoice = |tenant: &str, period: &str, total: u64| Invoice {
            id: format!("{tenant}/{period}"),
            tenant: tenant.into(),
            period: period.into(),
            period_start_ms: 0,
            period_end_ms: 1,
            plan: "payg".into(),
            lines: Vec::new(),
            total_cents: total,
            issued_at_ms: 5,
            due_at_ms: 6,
            status: InvoiceStatus::Open,
            paid_at_ms: None,
            provider_invoice_id: None,
        };
        assert!(store.insert_invoice(&invoice("b", "2026-10", 1)).unwrap());
        assert!(store.insert_invoice(&invoice("a", "2026-11", 2)).unwrap());
        assert!(store.insert_invoice(&invoice("a", "2026-10", 3)).unwrap());
        assert!(!store.insert_invoice(&invoice("a", "2026-10", 99)).unwrap());
        let ids: Vec<String> = store
            .invoices(None)
            .unwrap()
            .into_iter()
            .map(|i| i.id)
            .collect();
        assert_eq!(ids, vec!["a/2026-10", "a/2026-11", "b/2026-10"]);
        assert_eq!(store.invoices(Some("b")).unwrap().len(), 1);
        assert_eq!(store.invoice("a/2026-10").unwrap().unwrap().total_cents, 3);

        assert!(store.set_provider_invoice("a/2026-10", "in_1").unwrap());
        assert!(!store.set_provider_invoice("zz/2026-10", "in_1").unwrap());
        assert!(store
            .set_invoice_status("a/2026-10", InvoiceStatus::Paid, Some(9))
            .unwrap());
        let paid = store.invoice("a/2026-10").unwrap().unwrap();
        assert_eq!(
            (paid.status, paid.paid_at_ms),
            (InvoiceStatus::Paid, Some(9))
        );
        assert_eq!(paid.provider_invoice_id.as_deref(), Some("in_1"));
        // A paid time is not kept on an invoice that is not paid.
        assert!(store
            .set_invoice_status("a/2026-10", InvoiceStatus::Void, Some(9))
            .unwrap());
        assert_eq!(
            store.invoice("a/2026-10").unwrap().unwrap().paid_at_ms,
            None
        );
        assert!(!store
            .set_invoice_status("zz/2026-10", InvoiceStatus::Paid, Some(9))
            .unwrap());
    }
}
