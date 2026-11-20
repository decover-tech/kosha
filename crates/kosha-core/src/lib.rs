pub mod analysis;
pub mod deletes;
pub mod fields;
pub mod fuzzy;
pub mod rank;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

// ─── Control store trait (in-memory or Postgres) ──────────────────────────

pub mod datetime;
pub mod docset;
pub mod nested;
pub use analysis::{
    AnalysisConfig, AnalyzerSpec, FieldSchema, NamespaceSchema, SegmentAnalysis, TokenFilterSpec,
    TokenizerKind,
};
pub use docset::{evaluate_filter, DocSet};
pub use fields::{FieldBoost, FieldIndex, FieldStats, ALL_FIELD};
pub use fuzzy::{Fuzziness, FuzzyQuery};
pub use rank::{CompiledRankBy, RankBySpec};

/// Pluggable namespace registry and manifest store.
///
/// In-memory (`kosha_control::Controller`) is the default.
/// Postgres (`kosha_control::PgStore`) is used in production via the
/// `postgres` feature and a `DATABASE_URL` env var.
pub trait ControlStore: Send + Sync {
    fn create_namespace(&mut self, id: NamespaceId) -> Result<(), KoshaError>;
    fn ensure_namespace(&mut self, id: NamespaceId);
    fn has_namespace(&self, id: &NamespaceId) -> bool;
    fn manifest(&self, id: &NamespaceId) -> Option<&Manifest>;
    fn manifest_mut(&mut self, id: &NamespaceId) -> Option<&mut Manifest>;

    /// Owned copy of the manifest for a namespace.
    ///
    /// Default implementation clones `manifest()`; stores that cannot return
    /// a borrowed reference (e.g. Postgres) override this to read through.
    fn manifest_cloned(&self, id: &NamespaceId) -> Option<Manifest> {
        self.manifest(id).cloned()
    }

    /// Owned copy of the manifest, distinguishing "no such namespace"
    /// (`Ok(None)`) from "the control store could not be read"
    /// (`Err(KoshaError::ControlPlane)`).
    ///
    /// Query replicas use this to follow published manifests: an
    /// unreachable control plane must leave them serving their
    /// last-known-good manifest (DESIGN.md §17), not report the namespace
    /// missing. The default wraps `manifest_cloned`, which is infallible
    /// for in-memory stores; stores that talk to a database override it.
    fn try_manifest_cloned(&self, id: &NamespaceId) -> Result<Option<Manifest>, KoshaError> {
        Ok(self.manifest_cloned(id))
    }

    /// The stored manifest's version, without its segment list. `Ok(None)`
    /// means the namespace has no manifest; `Err` means the store could not
    /// be read and says nothing about the namespace.
    ///
    /// Has to be cheap: the manifest itself can run to megabytes for a few
    /// hundred segments. The default reads the whole manifest; stores that
    /// can do better override it.
    fn manifest_version(&self, id: &NamespaceId) -> Result<Option<u64>, KoshaError> {
        Ok(self
            .try_manifest_cloned(id)?
            .map(|manifest| manifest.version))
    }

    /// Every namespace's stored manifest version in one read: the query
    /// replicas' manifest refresher polls this (`SELECT namespace_id,
    /// version FROM kosha.manifests` on Postgres) instead of one round trip
    /// per namespace per search. `Err` means the store could not be read.
    fn manifest_versions(&self) -> Result<Vec<(NamespaceId, u64)>, KoshaError> {
        let mut versions = Vec::new();
        for id in self.list_namespaces() {
            if let Some(version) = self.manifest_version(&id)? {
                versions.push((id, version));
            }
        }
        Ok(versions)
    }

    /// Persist a manifest for a namespace (upsert, last-write-wins).
    ///
    /// Bootstrap and tests only: it can overwrite a concurrent writer's
    /// commit, so no publish path (flush, compaction, migrate, admin
    /// import) uses it. Publish with `compare_and_swap_manifest`.
    fn save_manifest(&mut self, id: &NamespaceId, manifest: &Manifest) -> Result<(), KoshaError>;

    /// Publish only over the writer's last committed version. An identical
    /// candidate is an idempotent retry, including after a lost commit reply.
    /// A missing namespace may be created with expected_version = 0.
    fn compare_and_swap_manifest(
        &mut self,
        id: &NamespaceId,
        expected_version: u64,
        new_manifest: Manifest,
    ) -> Result<(), KoshaError>;
    fn list_namespaces(&self) -> Vec<NamespaceId>;
    fn namespace_count(&self) -> usize;
}

// ─── Identifiers ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(pub String);

// ─── Document model ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Keyword,
    Integer,
    Float,
    Date,
    Boolean,
    Vector,
    /// A latitude/longitude pair (issue #212). Accepted as `"lat,lon"`,
    /// `{"lat": .., "lon": ..}` or GeoJSON `[lon, lat]`; stored as given and
    /// copied into the filter columns as the canonical `"lat,lon"` text,
    /// where `geo_distance` filters and geo decays read it.
    GeoPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub value: String,
}

impl Field {
    pub fn text(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Text,
            value: v.into(),
        }
    }
    pub fn keyword(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Keyword,
            value: v.into(),
        }
    }
    pub fn integer(n: impl Into<String>, v: i64) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Integer,
            value: v.to_string(),
        }
    }
    pub fn float_val(n: impl Into<String>, v: f64) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Float,
            value: v.to_string(),
        }
    }
    pub fn date_val(n: impl Into<String>, v: impl Into<String>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Date,
            value: v.into(),
        }
    }
    pub fn boolean(n: impl Into<String>, v: bool) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Boolean,
            value: v.to_string(),
        }
    }
    pub fn geo_point(n: impl Into<String>, lat: f64, lon: f64) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::GeoPoint,
            value: rank::canonical_geo(lat, lon),
        }
    }
    pub fn vector(n: impl Into<String>, v: Vec<f32>) -> Self {
        Self {
            name: n.into(),
            field_type: FieldType::Vector,
            value: serde_json::to_string(&v).unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub fields: Vec<Field>,
}

// ─── Inverted index types (with positions for phrase queries) ──────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Term(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: u32,
    pub term_frequency: u32,
    pub positions: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bm25Params {
    pub k1: f64,
    pub b: f64,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Per-request BM25 overrides (`SearchQuery::bm25_params`).
///
/// Each parameter is optional. A parameter the request leaves out falls
/// back to the value the segment was built with (`Footer::bm25_params`,
/// 1.2 / 0.75 unless the namespace was built with other values), so a
/// request that says nothing scores exactly as the segment's owner
/// configured it, and `{"k1": 2.0}` changes `k1` alone.
///
/// Nothing on disk depends on these values: postings store raw term
/// frequencies and block summaries store `max_tf` / `min_field_length`,
/// so both the score and the block-max WAND ceiling are computed from the
/// effective parameters at query time.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Bm25Overrides {
    /// Term-frequency saturation. Must be finite and `>= 0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k1: Option<f64>,
    /// Length normalization. Must be finite and within `[0, 1]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub b: Option<f64>,
    /// Where IDF and `avgdl` come from (issue #246). Omitted means the
    /// engine default ([`Bm25StatsScope::Namespace`] unless the server
    /// runs with `KOSHA_BM25_STATS=segment`). Not serialized when omitted,
    /// so result-cache keys of requests that don't set it are unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<Bm25StatsScope>,
}

/// Which corpus BM25's collection statistics (`N`, `df`, `avgdl`) are
/// taken from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Bm25StatsScope {
    /// Summed over every segment of the namespace's manifest, so a
    /// document scores the same whichever segment it landed in and scores
    /// from different segments can be merged by value.
    #[default]
    Namespace,
    /// Each segment's own `N`, `df` and `avgdl` — the pre-#246 behavior,
    /// kept as a debug / A-B switch. Scores from different segments are
    /// not comparable under this scope.
    Segment,
}

impl Bm25StatsScope {
    /// Parse the wire / env spelling (`namespace` or `segment`).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "namespace" => Some(Self::Namespace),
            "segment" => Some(Self::Segment),
            _ => None,
        }
    }
}

impl Bm25Overrides {
    /// Both parameters set explicitly.
    pub fn new(k1: f64, b: f64) -> Self {
        Self {
            k1: Some(k1),
            b: Some(b),
            stats: None,
        }
    }

    /// Reject values BM25 is not defined for. A negative `k1` or a `b`
    /// outside `[0, 1]` can make the length-normalization denominator zero
    /// or negative, producing infinite / negative scores, and breaks the
    /// monotonicity the block-max WAND upper bound relies on — so these
    /// are refused rather than clamped.
    pub fn validate(&self) -> Result<(), String> {
        if let Some(k1) = self.k1 {
            if !k1.is_finite() || k1 < 0.0 {
                return Err(format!(
                    "bm25_params.k1 must be a finite number >= 0, got {k1}"
                ));
            }
        }
        if let Some(b) = self.b {
            if !b.is_finite() || !(0.0..=1.0).contains(&b) {
                return Err(format!(
                    "bm25_params.b must be a finite number between 0 and 1, got {b}"
                ));
            }
        }
        Ok(())
    }

    /// The effective parameters for a segment built with `base`.
    pub fn resolve(&self, base: &Bm25Params) -> Bm25Params {
        Bm25Params {
            k1: self.k1.unwrap_or(base.k1),
            b: self.b.unwrap_or(base.b),
        }
    }
}

impl From<Bm25Params> for Bm25Overrides {
    fn from(p: Bm25Params) -> Self {
        Self::new(p.k1, p.b)
    }
}

// ─── Filter types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum FilterClause {
    Term {
        term: std::collections::HashMap<String, String>,
    },
    Terms {
        terms: std::collections::HashMap<String, Vec<String>>,
    },
    Range {
        range: std::collections::HashMap<String, RangeBound>,
    },
    Bool {
        bool: BoolFilter,
    },
    MatchAll {
        match_all: Option<serde_json::Value>,
    },
    /// The document has a value for the field.
    Exists {
        exists: ExistsFilter,
    },
    /// A `GeoPoint` field within `distance` of a point (issue #212).
    GeoDistance {
        geo_distance: GeoDistanceFilter,
    },
}

/// Body of a `geo_distance` clause, in Elasticsearch's shape:
/// `{"distance": "10km", "<field>": {"lat": .., "lon": ..}}`. The point may
/// also be `"lat,lon"` or GeoJSON `[lon, lat]`; the distance a number of
/// meters or a string with a unit (`m`, `km`, `mi`, ...).
#[derive(Debug, Clone, PartialEq)]
pub struct GeoDistanceFilter {
    pub field: String,
    pub lat: f64,
    pub lon: f64,
    /// Meters.
    pub distance: f64,
}

impl Serialize for GeoDistanceFilter {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("distance", &self.distance)?;
        map.serialize_entry(
            &self.field,
            &serde_json::json!({"lat": self.lat, "lon": self.lon}),
        )?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for GeoDistanceFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let mut obj = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        let distance = obj
            .remove("distance")
            .ok_or_else(|| D::Error::custom("geo_distance needs a \"distance\""))?;
        let distance = rank::parse_distance_meters(&distance)
            .filter(|d| d.is_finite() && *d >= 0.0)
            .ok_or_else(|| {
                D::Error::custom(format!(
                    "geo_distance.distance {distance} is not a distance (e.g. \"10km\" or meters)"
                ))
            })?;
        // Elasticsearch options that only change precision are accepted
        // and ignored; everything else is the field.
        obj.remove("distance_type");
        obj.remove("validation_method");
        let mut fields = obj.into_iter();
        let (field, point) = fields
            .next()
            .ok_or_else(|| D::Error::custom("geo_distance needs a field and a point"))?;
        if let Some((extra, _)) = fields.next() {
            return Err(D::Error::custom(format!(
                "geo_distance takes one field, found `{field}` and `{extra}`"
            )));
        }
        let (lat, lon) = match &point {
            serde_json::Value::String(s) => rank::parse_geo_str(s),
            other => rank::parse_geo_json(other),
        }
        .ok_or_else(|| {
            D::Error::custom(format!(
                "geo_distance point for `{field}` must be {{\"lat\": .., \"lon\": ..}}, \"lat,lon\" or [lon, lat] within range"
            ))
        })?;
        Ok(Self {
            field,
            lat,
            lon,
            distance,
        })
    }
}

impl FilterClause {
    /// Call `f` with every field name this clause (recursively) reads.
    pub fn for_each_field(&self, f: &mut impl FnMut(&str)) {
        match self {
            Self::Term { term } => term.keys().for_each(|k| f(k)),
            Self::Terms { terms } => terms.keys().for_each(|k| f(k)),
            Self::Range { range } => range.keys().for_each(|k| f(k)),
            Self::Exists { exists } => f(&exists.field),
            Self::GeoDistance { geo_distance } => f(&geo_distance.field),
            Self::MatchAll { .. } => {}
            Self::Bool { bool } => {
                for clause in bool.must.iter().chain(&bool.must_not).chain(&bool.should) {
                    clause.for_each_field(f);
                }
            }
        }
    }
}

/// Body of an `exists` clause.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExistsFilter {
    pub field: String,
}

/// Clause keys `FilterClause` accepts, as reported in parse errors.
const FILTER_CLAUSE_KEYS: [&str; 7] = [
    "term",
    "terms",
    "range",
    "bool",
    "match_all",
    "exists",
    "geo_distance",
];

/// Deserialized by dispatching on the clause's single key rather than with
/// `#[serde(untagged)]`.
///
/// Untagged deserialization tries variants in order and takes the first that
/// fits, which made `MatchAll` a catch-all: every field of every variant is
/// either defaulted or an `Option`, so an object with an unrecognized key —
/// `{"bogus": 1}`, or OpenSearch's `{"bool": {"filter": [...]}}` once
/// `BoolFilter` started rejecting `filter` — deserialized as
/// `MatchAll { match_all: None }` and silently matched *everything* rather
/// than erroring. On `/search` that quietly returns the whole namespace; on
/// `/delete` (delete-by-query) it would quietly delete it. See issue #170.
impl<'de> Deserialize<'de> for FilterClause {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        let expected = || FILTER_CLAUSE_KEYS.join(", ");
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value.as_object().ok_or_else(|| {
            D::Error::custom(format!(
                "filter clause must be a JSON object with exactly one of: {}",
                expected()
            ))
        })?;

        let mut keys = obj.keys();
        let key = keys.next().ok_or_else(|| {
            D::Error::custom(format!(
                "empty filter clause; expected exactly one of: {}. Use \
                 {{\"match_all\": {{}}}} to match every document",
                expected()
            ))
        })?;
        if let Some(extra) = keys.next() {
            return Err(D::Error::custom(format!(
                "filter clause must have exactly one key, found `{key}` and \
                 `{extra}`; wrap multiple clauses in a `bool`"
            )));
        }

        // `from_value` on the inner value only — the key was matched above, so
        // an error here is about the clause body, not the clause kind.
        let inner = obj.get(key).cloned().unwrap_or(serde_json::Value::Null);
        fn body<T, E>(v: serde_json::Value) -> Result<T, E>
        where
            T: serde::de::DeserializeOwned,
            E: serde::de::Error,
        {
            serde_json::from_value(v).map_err(E::custom)
        }

        match key.as_str() {
            "term" => Ok(FilterClause::Term { term: body(inner)? }),
            "terms" => Ok(FilterClause::Terms {
                terms: body(inner)?,
            }),
            "range" => Ok(FilterClause::Range {
                range: body(inner)?,
            }),
            "bool" => Ok(FilterClause::Bool { bool: body(inner)? }),
            "match_all" => Ok(FilterClause::MatchAll {
                match_all: Some(inner),
            }),
            "exists" => Ok(FilterClause::Exists {
                exists: body(inner)?,
            }),
            "geo_distance" => Ok(FilterClause::GeoDistance {
                geo_distance: body(inner)?,
            }),
            other => {
                let hint = match other {
                    "filter" | "must" | "must_not" | "should" | "minimum_should_match" => {
                        " — these belong inside a `bool` clause, e.g. \
                         {\"bool\": {\"must\": [...]}}"
                    }
                    "match" | "match_phrase" | "wildcard" | "prefix" => {
                        " — full-text and wildcard matching is not part of the \
                         filter language; use the query text instead"
                    }
                    _ => "",
                };
                Err(D::Error::custom(format!(
                    "unknown filter clause `{other}`, expected one of: {}{hint}",
                    expected()
                )))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeBound {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gte: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lte: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lt: Option<String>,
}

/// A boolean combination of filter clauses.
///
/// Parsed through [`BoolFilterWire`], which is `deny_unknown_fields`: an
/// unrecognized key is rejected rather than deserializing to an all-empty
/// `BoolFilter`, which matches every document (issue #170).
///
/// OpenSearch's `bool.filter` is accepted and folded into `must`. Inside a
/// filter nothing is scored, so `filter` and `must` mean exactly the same
/// thing: every clause has to match. Folding at parse time keeps a single
/// representation, so the three consumers (`FilterApplier::apply_bool`, the
/// delete-by-query applier in kosha-write, and `segment_may_match`) cannot
/// drift apart on it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "BoolFilterWire")]
pub struct BoolFilter {
    #[serde(default)]
    pub must: Vec<FilterClause>,
    #[serde(default)]
    pub must_not: Vec<FilterClause>,
    #[serde(default)]
    pub should: Vec<FilterClause>,
    #[serde(default = "default_minimum_should_match")]
    pub minimum_should_match: usize,
}

/// The on-the-wire shape of [`BoolFilter`].
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BoolFilterWire {
    #[serde(default)]
    must: Vec<FilterClause>,
    /// OpenSearch spelling of `must` in filter context.
    #[serde(default)]
    filter: Vec<FilterClause>,
    #[serde(default)]
    must_not: Vec<FilterClause>,
    #[serde(default)]
    should: Vec<FilterClause>,
    #[serde(default = "default_minimum_should_match")]
    minimum_should_match: usize,
}

impl From<BoolFilterWire> for BoolFilter {
    fn from(wire: BoolFilterWire) -> Self {
        let mut must = wire.must;
        must.extend(wire.filter);
        Self {
            must,
            must_not: wire.must_not,
            should: wire.should,
            minimum_should_match: wire.minimum_should_match,
        }
    }
}
fn default_minimum_should_match() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    pub order: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    #[serde(flatten)]
    pub fields: std::collections::HashMap<String, SortOrder>,
}

/// A search request's `highlight` clause (#195).
///
/// Two JSON shapes are accepted and normalised into this one struct:
///
/// * the OpenSearch/Elasticsearch shape, any number of fields, each with
///   optional tags that fall back to the top-level tags and then to the
///   defaults (`<b>` / `</b>`):
///   `{"pre_tags"?, "post_tags"?, "fields": {"title": {}, "body": {"pre_tags": ["<em>"]}}}`
///   (`fields` may also be ES's ordered array form, `[{"title": {}}, {"body": {}}]`);
/// * the legacy single-field shape, `{"field": "body", "pre_tags"?, "post_tags"?}`.
///
/// Giving both `field` and `fields` is an error. Highlighter options Kosha
/// does not implement (`fragment_size`, `type`, ...) are ignored, as they
/// were before. Hits answer a `fields` request with the per-field
/// `ScoredDocument::highlight` map, and a legacy request with the flat
/// `ScoredDocument::highlights` list, exactly as before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightConfig {
    /// Fields to highlight, in request order.
    pub fields: Vec<HighlightField>,
    /// Top-level tags: the fallback for a field that sets none.
    pub pre_tags: Option<Vec<String>>,
    pub post_tags: Option<Vec<String>>,
    /// Parsed from the legacy `{"field": ...}` form: hits report the flat
    /// `highlights` list rather than the per-field `highlight` map.
    pub legacy_single_field: bool,
}

/// One entry of `HighlightConfig::fields`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighlightField {
    pub name: String,
    pub pre_tags: Option<Vec<String>>,
    pub post_tags: Option<Vec<String>>,
}

const DEFAULT_PRE_TAG: &str = "<b>";
const DEFAULT_POST_TAG: &str = "</b>";

fn first_tag(tags: Option<&Vec<String>>) -> Option<&str> {
    tags.and_then(|t| t.first()).map(String::as_str)
}

impl HighlightConfig {
    /// The legacy single-field request, `{"field": name}`.
    pub fn single_field(name: impl Into<String>) -> Self {
        Self {
            fields: vec![HighlightField::new(name)],
            pre_tags: None,
            post_tags: None,
            legacy_single_field: true,
        }
    }

    /// An ES-shaped request for `names`, each with the default tags.
    pub fn for_fields<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fields: names.into_iter().map(HighlightField::new).collect(),
            pre_tags: None,
            post_tags: None,
            legacy_single_field: false,
        }
    }

    /// The opening tag for `field`: its own first tag, else the top-level
    /// one, else `<b>`. An empty list counts as unset.
    pub fn pre_tag<'a>(&'a self, field: &'a HighlightField) -> &'a str {
        first_tag(field.pre_tags.as_ref())
            .or_else(|| first_tag(self.pre_tags.as_ref()))
            .unwrap_or(DEFAULT_PRE_TAG)
    }

    /// The closing tag for `field`; see [`HighlightConfig::pre_tag`].
    pub fn post_tag<'a>(&'a self, field: &'a HighlightField) -> &'a str {
        first_tag(field.post_tags.as_ref())
            .or_else(|| first_tag(self.post_tags.as_ref()))
            .unwrap_or(DEFAULT_POST_TAG)
    }
}

impl HighlightField {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pre_tags: None,
            post_tags: None,
        }
    }
}

/// Per-field options inside `highlight.fields`. Unknown keys are ignored.
#[derive(Default, Serialize, Deserialize)]
struct HighlightFieldOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pre_tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    post_tags: Option<Vec<String>>,
}

/// `highlight.fields`, kept in request order: either an object
/// (`{"title": {}, "body": {}}`) or ES's ordered array of single-key
/// objects (`[{"title": {}}, {"body": {}}]`).
struct HighlightFieldList(Vec<HighlightField>);

impl<'de> Deserialize<'de> for HighlightFieldList {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;

        fn push_entries<'de, A: serde::de::MapAccess<'de>>(
            mut map: A,
            out: &mut Vec<HighlightField>,
        ) -> Result<(), A::Error> {
            while let Some(name) = map.next_key::<String>()? {
                // `"title": null` is accepted as "default options".
                let options: Option<HighlightFieldOptions> = map.next_value()?;
                let options = options.unwrap_or_default();
                if out.iter().any(|f| f.name == name) {
                    return Err(serde::de::Error::custom(format!(
                        "highlight field `{name}` listed more than once"
                    )));
                }
                out.push(HighlightField {
                    name,
                    pre_tags: options.pre_tags,
                    post_tags: options.post_tags,
                });
            }
            Ok(())
        }

        impl<'de> serde::de::Visitor<'de> for FieldsVisitor {
            type Value = HighlightFieldList;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an object of field name to highlight options, or an array of them")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                push_entries(map, &mut out)?;
                Ok(HighlightFieldList(out))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(entry) = seq.next_element::<HighlightFieldList>()? {
                    for field in entry.0 {
                        if out.iter().any(|f: &HighlightField| f.name == field.name) {
                            return Err(serde::de::Error::custom(format!(
                                "highlight field `{}` listed more than once",
                                field.name
                            )));
                        }
                        out.push(field);
                    }
                }
                Ok(HighlightFieldList(out))
            }
        }

        deserializer.deserialize_any(FieldsVisitor)
    }
}

/// The wire form both request shapes deserialize through.
#[derive(Deserialize)]
struct HighlightWire {
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    fields: Option<HighlightFieldList>,
    #[serde(default)]
    pre_tags: Option<Vec<String>>,
    #[serde(default)]
    post_tags: Option<Vec<String>>,
}

impl<'de> Deserialize<'de> for HighlightConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = HighlightWire::deserialize(deserializer)?;
        match (wire.field, wire.fields) {
            (Some(_), Some(_)) => Err(serde::de::Error::custom(
                "highlight: give either `fields` (a map of field name to options) or the legacy \
                 single `field`, not both",
            )),
            (Some(name), None) => Ok(HighlightConfig {
                fields: vec![HighlightField::new(name)],
                pre_tags: wire.pre_tags,
                post_tags: wire.post_tags,
                legacy_single_field: true,
            }),
            (None, Some(fields)) => Ok(HighlightConfig {
                fields: fields.0,
                pre_tags: wire.pre_tags,
                post_tags: wire.post_tags,
                legacy_single_field: false,
            }),
            (None, None) => Err(serde::de::Error::custom(
                "highlight: `fields` is required (e.g. {\"fields\": {\"body\": {}}})",
            )),
        }
    }
}

impl Serialize for HighlightConfig {
    /// Re-emits the shape the request used, so a serialized query (the CLI
    /// client, the result-cache key) deserializes back to the same value.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        if self.legacy_single_field && self.fields.len() == 1 {
            let only = &self.fields[0];
            map.serialize_entry("field", &only.name)?;
            // The legacy form has no per-field tags; fold them into the top
            // level so they survive the round trip.
            let pre = only.pre_tags.as_ref().or(self.pre_tags.as_ref());
            let post = only.post_tags.as_ref().or(self.post_tags.as_ref());
            if let Some(pre) = pre {
                map.serialize_entry("pre_tags", pre)?;
            }
            if let Some(post) = post {
                map.serialize_entry("post_tags", post)?;
            }
        } else {
            if let Some(ref pre) = self.pre_tags {
                map.serialize_entry("pre_tags", pre)?;
            }
            if let Some(ref post) = self.post_tags {
                map.serialize_entry("post_tags", post)?;
            }
            map.serialize_entry("fields", &HighlightFieldsSer(&self.fields))?;
        }
        map.end()
    }
}

struct HighlightFieldsSer<'a>(&'a [HighlightField]);

impl Serialize for HighlightFieldsSer<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for field in self.0 {
            map.serialize_entry(
                &field.name,
                &HighlightFieldOptions {
                    pre_tags: field.pre_tags.clone(),
                    post_tags: field.post_tags.clone(),
                },
            )?;
        }
        map.end()
    }
}

// ─── Wildcard query ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WildcardQuery {
    pub field: String,
    pub pattern: String,
    #[serde(default = "default_case_insensitive")]
    pub case_insensitive: bool,
}
fn default_case_insensitive() -> bool {
    true
}

// ─── Match phrase query ────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchPhraseQuery {
    /// The field the phrase must occur in (#249). `_all`, `*` or empty
    /// means any one `Text` field — never across two of them. Segments
    /// written before #249 have no per-field index and match the phrase
    /// in their combined index.
    #[serde(default)]
    pub field: String,
    pub phrase: String,
    #[serde(default)]
    pub slop: u32,
    /// Several fields the phrase may occur in (any one of them, scored
    /// with the best). Overrides `field` when non-empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
}

impl MatchPhraseQuery {
    /// The specific fields the phrase is restricted to, or `None` when it
    /// may occur in any field (`_all`, `*`, or no field named).
    pub fn restricted_fields(&self) -> Option<Vec<String>> {
        let named: Vec<&str> = if self.fields.is_empty() {
            vec![self.field.as_str()]
        } else {
            self.fields.iter().map(String::as_str).collect()
        };
        let mut out: Vec<String> = Vec::with_capacity(named.len());
        for name in named {
            let name = name.trim();
            if name.is_empty() || name == "*" || name == fields::ALL_FIELD {
                return None;
            }
            if !out.iter().any(|f| f == name) {
                out.push(name.to_string());
            }
        }
        Some(out)
    }
}

// ─── Aggregation types ─────────────────────────────────────────────────────

/// One named aggregation of a search request.
///
/// Serialized untagged (each variant is an object keyed by its type, so the
/// JSON of `terms`, `cardinality` and `composite` is unchanged), but
/// deserialized by dispatching on the type key — see the `Deserialize` impl
/// below — so a misspelled or unsupported type is an error rather than a
/// silent mis-match.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Aggregation {
    Terms {
        terms: AggTerms,
        /// Metric sub-aggregations computed per bucket (ES `aggs`).
        #[serde(skip_serializing_if = "Option::is_none")]
        aggs: Option<std::collections::HashMap<String, Aggregation>>,
    },
    Cardinality {
        cardinality: AggCardinality,
    },
    Composite {
        composite: AggComposite,
        /// Metric sub-aggregations computed per bucket (ES `aggs`).
        #[serde(skip_serializing_if = "Option::is_none")]
        aggs: Option<std::collections::HashMap<String, Aggregation>>,
    },
    /// Smallest value of the field over the matched documents.
    Min {
        min: AggMetric,
    },
    /// Largest value of the field over the matched documents.
    Max {
        max: AggMetric,
    },
    /// Sum of the field's values over the matched documents.
    Sum {
        sum: AggMetric,
    },
    /// Mean of the field's values over the matched documents.
    Avg {
        avg: AggMetric,
    },
    /// Number of values the field has over the matched documents.
    ValueCount {
        value_count: AggMetric,
    },
}

/// The single-value metric aggregations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKind {
    Min,
    Max,
    Sum,
    Avg,
    ValueCount,
}

impl MetricKind {
    /// The wire name of the aggregation type.
    pub fn name(self) -> &'static str {
        match self {
            MetricKind::Min => "min",
            MetricKind::Max => "max",
            MetricKind::Sum => "sum",
            MetricKind::Avg => "avg",
            MetricKind::ValueCount => "value_count",
        }
    }
}

/// Body of a metric aggregation (`min`, `max`, `sum`, `avg`,
/// `value_count`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggMetric {
    pub field: String,
}

/// Aggregation type keys `Aggregation` accepts, as reported in parse errors.
const AGGREGATION_KEYS: [&str; 8] = [
    "terms",
    "cardinality",
    "composite",
    "min",
    "max",
    "sum",
    "avg",
    "value_count",
];

impl Aggregation {
    /// The metric kind and body when this is a metric aggregation.
    pub fn metric(&self) -> Option<(MetricKind, &AggMetric)> {
        match self {
            Aggregation::Min { min } => Some((MetricKind::Min, min)),
            Aggregation::Max { max } => Some((MetricKind::Max, max)),
            Aggregation::Sum { sum } => Some((MetricKind::Sum, sum)),
            Aggregation::Avg { avg } => Some((MetricKind::Avg, avg)),
            Aggregation::ValueCount { value_count } => Some((MetricKind::ValueCount, value_count)),
            _ => None,
        }
    }

    /// The type key this aggregation is written under.
    pub fn type_name(&self) -> &'static str {
        match self {
            Aggregation::Terms { .. } => "terms",
            Aggregation::Cardinality { .. } => "cardinality",
            Aggregation::Composite { .. } => "composite",
            other => other.metric().map(|(kind, _)| kind.name()).unwrap_or(""),
        }
    }

    /// Sub-aggregations of a bucket aggregation, if any.
    pub fn sub_aggregations(&self) -> Option<&std::collections::HashMap<String, Aggregation>> {
        match self {
            Aggregation::Terms { aggs, .. } | Aggregation::Composite { aggs, .. } => aggs.as_ref(),
            _ => None,
        }
    }

    /// Checks the parser enforces, repeated for values built in code: only
    /// metric aggregations may be nested, and only under a bucket
    /// aggregation.
    pub fn validate(&self, name: &str) -> Result<(), String> {
        if let Some(subs) = self.sub_aggregations() {
            for (sub_name, sub) in subs {
                if sub.metric().is_none() {
                    return Err(nested_bucket_error(name, sub_name, sub.type_name()));
                }
            }
        }
        Ok(())
    }
}

fn nested_bucket_error(parent: &str, name: &str, kind: &str) -> String {
    format!(
        "sub-aggregation `{name}` of `{parent}` is a `{kind}` aggregation; only metric \
         aggregations (min, max, sum, avg, value_count) can be nested under a bucket \
         aggregation"
    )
}

/// Deserialized by dispatching on the aggregation's type key rather than
/// with `#[serde(untagged)]`, for the same reason as [`FilterClause`]
/// (issue #170): untagged tries each variant in turn, so a misspelled type
/// or an unsupported one (`{"stats": ...}`) would at best produce serde's
/// unhelpful "did not match any variant", and a new variant whose body
/// happened to fit would silently capture another type's JSON.
///
/// The object holds exactly one type key, plus — for `terms` and
/// `composite` only — an optional `aggs` (or `aggregations`) map of metric
/// sub-aggregations, as in Elasticsearch.
impl<'de> Deserialize<'de> for Aggregation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let expected = || AGGREGATION_KEYS.join(", ");
        let value = serde_json::Value::deserialize(deserializer)?;
        let serde_json::Value::Object(mut obj) = value else {
            return Err(D::Error::custom(format!(
                "aggregation must be a JSON object with exactly one of: {}",
                expected()
            )));
        };

        let subs = match (obj.remove("aggs"), obj.remove("aggregations")) {
            (Some(_), Some(_)) => {
                return Err(D::Error::custom(
                    "aggregation has both `aggs` and `aggregations`; use one",
                ))
            }
            (Some(v), None) | (None, Some(v)) => Some(v),
            (None, None) => None,
        };

        let mut keys = obj.keys();
        let key = keys
            .next()
            .ok_or_else(|| {
                D::Error::custom(format!(
                    "aggregation has no type; expected exactly one of: {}",
                    expected()
                ))
            })?
            .clone();
        if let Some(extra) = keys.next() {
            return Err(D::Error::custom(format!(
                "aggregation must have exactly one type, found `{key}` and `{extra}`; give \
                 each aggregation its own name"
            )));
        }
        let inner = obj.remove(&key).unwrap_or(serde_json::Value::Null);
        fn body<T, E>(kind: &str, v: serde_json::Value) -> Result<T, E>
        where
            T: serde::de::DeserializeOwned,
            E: serde::de::Error,
        {
            serde_json::from_value(v)
                .map_err(|e| E::custom(format!("in `{kind}` aggregation: {e}")))
        }

        let subs: Option<std::collections::HashMap<String, Aggregation>> = match subs {
            None => None,
            Some(v) => {
                if key != "terms" && key != "composite" {
                    return Err(D::Error::custom(format!(
                        "`{key}` aggregation cannot have sub-aggregations; only `terms` and \
                         `composite` buckets take `aggs`"
                    )));
                }
                let map: std::collections::HashMap<String, Aggregation> =
                    serde_json::from_value(v).map_err(D::Error::custom)?;
                for (name, sub) in &map {
                    if sub.metric().is_none() {
                        return Err(D::Error::custom(nested_bucket_error(
                            &key,
                            name,
                            sub.type_name(),
                        )));
                    }
                }
                Some(map)
            }
        };

        match key.as_str() {
            "terms" => Ok(Aggregation::Terms {
                terms: body(&key, inner)?,
                aggs: subs,
            }),
            "cardinality" => Ok(Aggregation::Cardinality {
                cardinality: body(&key, inner)?,
            }),
            "composite" => Ok(Aggregation::Composite {
                composite: body(&key, inner)?,
                aggs: subs,
            }),
            "min" => Ok(Aggregation::Min {
                min: body(&key, inner)?,
            }),
            "max" => Ok(Aggregation::Max {
                max: body(&key, inner)?,
            }),
            "sum" => Ok(Aggregation::Sum {
                sum: body(&key, inner)?,
            }),
            "avg" => Ok(Aggregation::Avg {
                avg: body(&key, inner)?,
            }),
            "value_count" => Ok(Aggregation::ValueCount {
                value_count: body(&key, inner)?,
            }),
            other => {
                let hint = match other {
                    "stats" | "extended_stats" | "percentiles" | "histogram" | "date_histogram"
                    | "range" | "top_hits" | "filter" | "filters" => " (not supported yet)",
                    "count" => " — use `value_count`, or `total_hits` for the number of matches",
                    "average" | "mean" => " — did you mean `avg`?",
                    "minimum" => " — did you mean `min`?",
                    "maximum" => " — did you mean `max`?",
                    _ => "",
                };
                Err(D::Error::custom(format!(
                    "unknown aggregation type `{other}`{hint}; expected one of: {}",
                    expected()
                )))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggTerms {
    pub field: String,
    #[serde(default = "default_agg_size")]
    pub size: usize,
}
fn default_agg_size() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCardinality {
    pub field: String,
    #[serde(default = "default_precision")]
    pub precision_threshold: usize,
}
fn default_precision() -> usize {
    40000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggComposite {
    pub size: usize,
    pub sources: Vec<AggCompositeSource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeSource {
    #[serde(flatten)]
    pub source: std::collections::HashMap<String, AggCompositeTerms>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeTerms {
    pub terms: AggCompositeField,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeField {
    pub field: String,
}

/// Aggregation results keyed by aggregation name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResults {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_document: Option<AggBucketResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_documents: Option<AggMetricResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_docs: Option<AggCompositeResult>,
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggBucketResult {
    pub buckets: Vec<AggBucket>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggBucket {
    pub key: String,
    pub doc_count: usize,
    /// Results of the bucket aggregation's metric sub-aggregations, keyed
    /// by sub-aggregation name and serialized inline next to `key` and
    /// `doc_count`, as Elasticsearch does. Empty when none were asked for.
    #[serde(
        flatten,
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub sub_aggregations: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggMetricResult {
    pub value: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCompositeResult {
    pub buckets: Vec<AggCompositeBucket>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_key: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AggCompositeBucket {
    pub key: std::collections::HashMap<String, String>,
    pub doc_count: usize,
    /// See [`AggBucket::sub_aggregations`].
    #[serde(
        flatten,
        default,
        skip_serializing_if = "std::collections::BTreeMap::is_empty"
    )]
    pub sub_aggregations: std::collections::BTreeMap<String, serde_json::Value>,
}

// ─── Storage abstraction (local fs / S3 / etc.) ───────────────────────────

/// Storage backends implement this to provide file I/O for segments.
/// Kosha's core uses this instead of std::fs directly, so S3 / GCS / etc.
/// can be plugged in without modifying core code.
pub trait StorageBackend: std::fmt::Debug + Send + Sync {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError>;
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError>;
    fn exists(&self, path: &str) -> bool;
    fn delete(&self, path: &str) -> Result<(), KoshaError>;
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError>;
    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError>;
}

/// Local filesystem implementation of StorageBackend.
#[derive(Debug, Clone)]
pub struct LocalStorage {
    pub root: std::path::PathBuf,
}

impl LocalStorage {
    pub fn new(root: std::path::PathBuf) -> Self {
        Self { root }
    }
}

/// Overwrite `path` atomically: write to a sibling temp file, then `rename`
/// over the destination. POSIX (and Windows, via the same
/// `std::fs::rename`-backed semantics) guarantees a rename within one
/// directory is atomic — a reader always sees either the old complete file
/// or the new complete one, never a truncated/partial one.
///
/// `std::fs::write` (what this replaces) truncates the destination before
/// filling it back in. That's fine for a brand-new path nobody else can see
/// yet, but every `StorageBackend::write` call here can also be an in-place
/// rewrite of a file that's already published and being concurrently read
/// (a segment mid-search, a WAL file mid-recovery) — or can simply be
/// interrupted mid-write (process kill, OOM). Either way a reader landing in
/// the truncate-then-fill window, or a later read of a write that never
/// finished, sees a short file. For JSON that surfaces as `serde_json`
/// failing with "EOF while parsing a value"; for a segment's binary files it
/// silently corrupts data that then gets faithfully copied to S3 by whatever
/// syncs the segment afterward, baking the corruption into the source of
/// truth. See PR #45 (`kosha-segment`'s narrower fix for the same class of
/// bug in the footer-rewrite admin paths) for the incident this generalizes.
///
/// The temp filename includes the PID and a per-process atomic counter so
/// concurrent writes to the *same* path never collide on the same temp file.
fn atomic_write(path: &std::path::Path, data: &[u8]) -> Result<(), KoshaError> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let dir = path.parent().ok_or_else(|| {
        KoshaError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("atomic_write: {} has no parent directory", path.display()),
        ))
    })?;
    let file_name = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
        KoshaError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("atomic_write: {} has no file name", path.display()),
        ))
    })?;
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = dir.join(format!(".{file_name}.tmp.{}.{n}", std::process::id()));

    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

impl StorageBackend for LocalStorage {
    fn read(&self, path: &str) -> Result<Vec<u8>, KoshaError> {
        Ok(std::fs::read(self.root.join(path))?)
    }
    fn write(&self, path: &str, data: &[u8]) -> Result<(), KoshaError> {
        atomic_write(&self.root.join(path), data)
    }
    fn exists(&self, path: &str) -> bool {
        self.root.join(path).exists()
    }
    fn delete(&self, path: &str) -> Result<(), KoshaError> {
        let p = self.root.join(path);
        if p.is_dir() {
            std::fs::remove_dir_all(&p)?;
        } else {
            std::fs::remove_file(&p)?;
        }
        Ok(())
    }
    fn list(&self, path: &str) -> Result<Vec<String>, KoshaError> {
        let dir = self.root.join(path);
        let mut entries = Vec::new();
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
        }
        Ok(entries)
    }
    fn create_dir_all(&self, path: &str) -> Result<(), KoshaError> {
        Ok(std::fs::create_dir_all(self.root.join(path))?)
    }
}

// ─── WAL (Write-Ahead Log) types ──────────────────────────────────────────

/// A single WAL record: a batch of documents for one namespace at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalRecord {
    pub namespace: NamespaceId,
    pub documents: Vec<Document>,
    #[serde(default)]
    pub deleted_ids: Vec<DocumentId>,
    pub timestamp: u64,
}

impl WalRecord {
    pub fn new(namespace: NamespaceId, documents: Vec<Document>) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        Self {
            namespace,
            documents,
            deleted_ids: Vec::new(),
            timestamp,
        }
    }
}

/// Summary of a WAL file for recovery ordering.
#[derive(Debug, Clone)]
pub struct WalFileInfo {
    pub path: String,
    pub record_count: u32,
    pub first_timestamp: u64,
}

// ─── Segment metadata ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocRecord {
    pub doc_id: DocumentId,
    pub doc_seq: u32,
    pub field_length: u32,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Footer {
    pub segment_id: SegmentId,
    pub doc_count: u32,
    pub total_field_length: u64,
    pub avg_field_length: f64,
    pub bm25_params: Bm25Params,
    pub created_at: String,
    /// Per string-filter-field bloom filters for segment pruning.
    ///
    /// `None` means a legacy segment written before blooms existed — callers
    /// must not prune. `Some(map)` means field inventory is known: a missing
    /// field key means the segment has no values for that field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_blooms: Option<HashMap<String, BloomFilter>>,
    /// Bloom over the segment's inverted-index vocabulary (tokenized query
    /// terms). Used to skip segments that cannot contain required BM25 terms
    /// without opening `inverted.idx`.
    ///
    /// `None` means a legacy segment — callers must not prune on query terms.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub term_bloom: Option<BloomFilter>,
    /// Segment format version. `0` (the `#[serde(default)]`) means a legacy
    /// segment written before `doc_store.offsets` existed — readers must not
    /// assume that sidecar is present or trustworthy even if it happens to
    /// exist on disk. `1` means the writer emitted `doc_store.offsets`
    /// alongside `doc_store.bin`, enabling lazy per-document loading instead
    /// of parsing the whole segment into memory to open it. `2` means
    /// `inverted.idx` is in the v2 table-of-contents layout
    /// (`kosha_segment::LazyInvertedIndex`), read lazily with zero parsing
    /// at open. Informational for `inverted.idx` — that file self-describes
    /// via a magic prefix, and readers fall back to the v1 stream parse for
    /// older segments regardless of this number.
    #[serde(default)]
    pub format_version: u32,
    /// `ignore_above` cap (in chars) the writer applied to `Text` values
    /// before copying them into `filters.bin` and `filter_blooms` (issue
    /// #243). A `Text` value longer than this is still tokenized into the
    /// inverted index and stored in the doc store, but is not filterable,
    /// sortable or aggregatable by exact value — the same semantics as
    /// Elasticsearch's dynamic-mapping `keyword` subfield (`ignore_above:
    /// 256`). `None` means no cap: a segment written before the cap existed
    /// (every `Text` value was copied) or one written with the cap disabled.
    /// Informational only — `filters.bin`'s byte layout is unchanged, so
    /// readers need not consult it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_filter_ignore_above: Option<u32>,
    /// `true` when the writer moved parsed `Vector` field values out of
    /// `doc_store.bin` into the binary `doc_vectors.bin` sidecar (#243).
    /// The doc store then keeps each such field with an **empty** value as
    /// a placeholder (name, type and position preserved), and
    /// `SegmentReader` fills it back in from the sidecar on every
    /// doc-record read. `false` (the default, and every segment written
    /// before #243) means vectors are inline JSON in the doc store.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub doc_vectors: bool,
    /// `true` when every `postings-*.bin` shard was written raw, never
    /// zstd-wrapped (#245). A raw shard's bytes are exactly the logical
    /// layout the `inverted.idx` TOC points into, so a query node can fetch
    /// just its query terms' `(postings_off, postings_len)` spans with
    /// ranged GETs instead of whole shards. `false` (the default, and every
    /// segment written before #245) means a shard of 4 KiB or more may be
    /// compressed as a whole file and must be fetched whole. Readers that
    /// predate the flag read raw shards unchanged (the wrapper was always
    /// optional), so a rollback still reads these segments.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub posting_blobs_raw: bool,
    /// The columnar filter index directory (#244): one entry per filter
    /// column, naming the `filters-NNN.col` file that holds it, plus its
    /// per-segment stats. `Some` means the segment's filter values live in
    /// `filters.toc` + those column files and it has **no** `filters.bin`;
    /// `None` (the default, and every segment written before #244 or with
    /// `KOSHA_FILTERS_WRITE_VERSION=1`) means they live in `filters.bin`.
    /// This field is how readers and the hydration planner pick the
    /// format: they never guess from which files happen to be local. It
    /// is also what lets a query node fetch only the columns a query
    /// touches without first fetching `filters.toc` to learn file names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_columns: Option<Vec<FilterColumnMeta>>,
    /// The analysis chains this segment's `Text` fields were indexed with
    /// (issues #220, #221). `None` — every segment written before
    /// analyzers existed, and every segment written since with default
    /// settings — means the legacy `standard@1` chain for every field.
    /// Queries are analyzed per segment with these chains, so a segment
    /// keeps matching the way it was written whatever the namespace's
    /// current schema says. See [`analysis`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analysis: Option<SegmentAnalysis>,
    /// Per-field postings and statistics (issue #249). `None` — every
    /// segment written before per-field indexes existed, or with
    /// `KOSHA_FIELD_INDEX=off` — means the segment has only the combined
    /// index: a field-restricted query is answered from it (matching in
    /// any field), never with zero hits. See [`fields`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_index: Option<FieldIndex>,
    /// Bloom over the deletion neighbourhood of the combined-index terms
    /// (each term and every string one character deletion away from it),
    /// for pruning one-edit fuzzy queries (issue #201): see
    /// [`fuzzy::build_fuzzy_bloom`]. `None` — segments written before
    /// #201, and segments whose neighbourhood is too large for a useful
    /// bloom — means fuzzy queries cannot prune the segment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fuzzy_bloom: Option<BloomFilter>,
}

/// Value type of one filter column (#244). A field name can appear with
/// more than one kind in one segment (documents disagree on its type), so
/// a column is keyed by `(name, kind)`, exactly like [`FilterStore`]'s
/// three maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FilterColumnKind {
    /// Keyword, Boolean, Date and (capped) Text values.
    String,
    Integer,
    Float,
}

/// A min/max bound in [`FilterColumnStats`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterStatValue {
    Integer(i64),
    Float(f64),
    String(String),
}

/// Per-segment statistics of one filter column (#244), recorded so later
/// work can skip segments by range the way `filter_blooms` skips them by
/// term. Every field is optional on the wire; `None` means "unknown", and
/// a reader must then not skip on it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FilterColumnStats {
    /// Number of `(doc, value)` entries.
    #[serde(default)]
    pub values: u64,
    /// Number of distinct documents holding at least one value.
    #[serde(default)]
    pub docs: u64,
    /// Number of distinct values. Exact (the dictionary size) for string
    /// columns; exact for numeric ones too (`f64` by bit pattern).
    #[serde(default)]
    pub distinct: u64,
    /// Smallest value. For floats only finite values count, and the bound
    /// is omitted if the column holds none. For strings it is omitted when
    /// longer than [`FILTER_STAT_MAX_STRING_BYTES`] (a truncated minimum
    /// would still be a bound, but a truncated maximum would not).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<FilterStatValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<FilterStatValue>,
    /// `true` when a float column holds a NaN or infinite value, which
    /// `min`/`max` do not describe.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub non_finite: bool,
}

/// Longest string min/max kept in [`FilterColumnStats`].
pub const FILTER_STAT_MAX_STRING_BYTES: usize = 64;

/// One entry of the columnar filter directory (#244). See
/// `Footer::filter_columns`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterColumnMeta {
    pub name: String,
    pub kind: FilterColumnKind,
    /// Column file id: the data lives in `filters-{id:03}.col`. Files are
    /// numbered rather than named after the field because field names are
    /// arbitrary strings (`/`, `..`, very long, non-ASCII).
    pub id: u32,
    /// Size of the column file in bytes.
    #[serde(default)]
    pub bytes: u64,
    #[serde(default)]
    pub stats: FilterColumnStats,
}

impl FilterColumnMeta {
    /// Basename of this column's file inside the segment directory.
    pub fn file_name(&self) -> String {
        filter_column_file_name(self.id)
    }
}

/// Name of the columnar filter directory file (#244).
pub const FILTERS_TOC_FILE: &str = "filters.toc";
/// Name of the legacy row-oriented filter file.
pub const FILTERS_BIN_FILE: &str = "filters.bin";

/// Basename of filter column file `id` (#244): `filters-007.col`.
pub fn filter_column_file_name(id: u32) -> String {
    format!("filters-{id:03}.col")
}

/// Whether `name` is a columnar filter file (`filters.toc` or a
/// `filters-NNN.col`).
pub fn is_filter_column_file(name: &str) -> bool {
    name == FILTERS_TOC_FILE
        || name.strip_prefix("filters-").is_some_and(|rest| {
            rest.strip_suffix(".col")
                .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
        })
}

impl Footer {
    /// The filter files a query touching `fields` needs locally (#244):
    /// `filters.bin` for a legacy segment, otherwise `filters.toc` plus
    /// the column of every `(name, kind)` whose name is in `fields`.
    /// `fields == None` means every column (admin paths, compaction).
    pub fn filter_files_for(
        &self,
        fields: Option<&std::collections::BTreeSet<String>>,
    ) -> Vec<String> {
        match &self.filter_columns {
            None => vec![FILTERS_BIN_FILE.to_string()],
            Some(columns) => {
                let mut files = vec![FILTERS_TOC_FILE.to_string()];
                files.extend(
                    columns
                        .iter()
                        .filter(|c| fields.is_none_or(|f| f.contains(&c.name)))
                        .map(FilterColumnMeta::file_name),
                );
                files
            }
        }
    }
}

/// Current segment format version written by `SegmentWriter`. See
/// `Footer::format_version`.
pub const SEGMENT_FORMAT_VERSION: u32 = 2;

// ─── Bloom filter (segment pruning) ────────────────────────────────────────

/// Target false-positive rate when sizing blooms at write time.
pub const BLOOM_TARGET_FPR: f64 = 0.01;
/// Cap on bloom bitset size per field (keeps footer.json bounded).
pub const BLOOM_MAX_BYTES: usize = 64 * 1024;

/// Compact bloom filter over string filter values in one segment field.
///
/// Negatives are definitive (safe to skip the segment). Positives still require
/// opening `filters.bin` and applying the real predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Base64-encoded bitset.
    pub bits_b64: String,
    /// Number of bits in the bitset.
    pub num_bits: u32,
    /// Number of hash functions.
    pub k: u8,
}

impl BloomFilter {
    /// Build a bloom from unique string values at ~[`BLOOM_TARGET_FPR`].
    pub fn build<'a, I>(values: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let unique: Vec<&str> = {
            let mut seen = HashMap::new();
            for v in values {
                seen.insert(v, ());
            }
            seen.into_keys().collect()
        };
        let n = unique.len().max(1);
        let mut num_bits = bloom_num_bits(n, BLOOM_TARGET_FPR);
        let max_bits = BLOOM_MAX_BYTES * 8;
        if num_bits > max_bits {
            num_bits = max_bits;
        }
        num_bits = num_bits.max(64);
        let k = bloom_num_hashes(num_bits, n).clamp(1, 16) as u8;
        let mut bits = vec![0u8; num_bits.div_ceil(8)];
        let mut filter = Self {
            bits_b64: String::new(),
            num_bits: num_bits as u32,
            k,
        };
        for v in unique {
            filter.insert_into(&mut bits, v);
        }
        filter.bits_b64 = encode_base64(&bits);
        filter
    }

    /// Whether `value` may be in the set (`false` is definite).
    ///
    /// Reads the `k` bits it needs straight out of the base64 text, one
    /// 4-character group each. It used to decode the whole filter into a
    /// fresh buffer on every call, and a search asks this of every
    /// segment's term bloom for every query term, twice (when choosing what
    /// to fetch and again when scoring).
    ///
    /// A filter that cannot be read (empty, or a bit that falls outside the
    /// text) cannot prune: the answer is `true`.
    pub fn may_contain(&self, value: &str) -> bool {
        self.may_contain_hashed(bloom_key_hashes(value))
    }

    /// [`Self::may_contain`] for a value already hashed with
    /// [`bloom_key_hashes`]: the hashes do not depend on the filter, so a
    /// caller probing many blooms with many values hashes each value once.
    pub fn may_contain_hashed(&self, (h1, h2): (u64, u64)) -> bool {
        if self.num_bits == 0 || self.bits_b64.is_empty() {
            return true;
        }
        let text = self.bits_b64.as_bytes();
        let m = self.num_bits as u64;
        for i in 0..self.k {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            match base64_byte(text, (bit / 8) as usize) {
                Some(byte) if byte & (1u8 << (bit % 8)) == 0 => return false,
                Some(_) => {}
                None => return true,
            }
        }
        true
    }

    fn insert_into(&self, bits: &mut [u8], value: &str) {
        let m = self.num_bits as u64;
        let (h1, h2) = bloom_hashes(value);
        for i in 0..self.k {
            let bit = h1.wrapping_add((i as u64).wrapping_mul(h2)) % m;
            let idx = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if idx < bits.len() {
                bits[idx] |= mask;
            }
        }
    }
}

/// The elements of a stored value that holds a list, or `None` when the
/// value is a plain scalar.
///
/// A list-valued field (tags, recipients, custodians) reaches Kosha from the
/// OpenSearch-compatible client as one field whose value is the list's JSON
/// text: `["urgent","sealed"]`. As a filter value that string equals nothing
/// a caller ever asks for, so `term: {tags: "urgent"}` never matched. Filter
/// evaluation, blooms and aggregations therefore treat such a value as its
/// elements. Strings are taken as they are; numbers and booleans by their
/// JSON text. Nested arrays and objects are not list elements and are
/// skipped.
pub fn list_elements(value: &str) -> Option<Vec<String>> {
    let trimmed = value.trim();
    if !(trimmed.starts_with('[') && trimmed.ends_with(']')) {
        return None;
    }
    let parsed: Vec<serde_json::Value> = serde_json::from_str(trimmed).ok()?;
    Some(
        parsed
            .into_iter()
            .filter_map(|element| match element {
                serde_json::Value::String(s) => Some(s),
                serde_json::Value::Number(n) => Some(n.to_string()),
                serde_json::Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .collect(),
    )
}

/// Build per-field blooms from columnar string filter entries.
///
/// A list value contributes its elements as well as its whole text (see
/// [`list_elements`]), so a segment is not pruned for a `term` on one
/// element. Segments written before this was the case have blooms without
/// the elements; `rebuild-filter-blooms` brings them up to date.
pub fn build_filter_blooms(
    string_fields: &HashMap<String, Vec<(u32, String)>>,
) -> HashMap<String, BloomFilter> {
    let mut out = HashMap::new();
    for (field, entries) in string_fields {
        if entries.is_empty() {
            continue;
        }
        let elements: Vec<String> = entries
            .iter()
            .filter_map(|(_, v)| list_elements(v))
            .flatten()
            .collect();
        let bloom = BloomFilter::build(
            entries
                .iter()
                .map(|(_, v)| v.as_str())
                .chain(elements.iter().map(String::as_str)),
        );
        out.insert(field.clone(), bloom);
    }
    out
}

/// Build a bloom over a segment's inverted-index term vocabulary.
pub fn build_term_bloom<'a, I>(terms: I) -> BloomFilter
where
    I: IntoIterator<Item = &'a str>,
{
    BloomFilter::build(terms)
}

/// How [`segment_may_contain_terms`] combines multiple query terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermBloomMode {
    /// All terms must possibly be present (multi-term BM25 / phrase).
    And,
    /// At least one term must possibly be present (wildcard OR expansion).
    Or,
}

/// Whether a segment with the given term bloom might contain `terms`.
///
/// Returns `true` when the segment must be opened (possible match, empty
/// term list, or legacy/`None` bloom). Returns `false` only when the bloom
/// proves the segment cannot satisfy the term constraint.
pub fn segment_may_contain_terms(
    terms: &[String],
    mode: TermBloomMode,
    bloom: Option<&BloomFilter>,
) -> bool {
    if terms.is_empty() {
        return true;
    }
    let Some(bloom) = bloom else {
        return true; // legacy footer — cannot prune
    };
    // A per-field key (#249) is checked as its plain term: the bloom
    // holds only combined-index terms, and every field term is one.
    let may_contain = |t: &String| bloom.may_contain(fields::bloom_term(t));
    match mode {
        TermBloomMode::And => terms.iter().all(may_contain),
        TermBloomMode::Or => terms.iter().any(may_contain),
    }
}

fn bloom_num_bits(n: usize, fpr: f64) -> usize {
    if n == 0 {
        return 64;
    }
    let n = n as f64;
    let bits = (-(n) * fpr.ln() / (2f64.ln().powi(2))).ceil() as usize;
    bits.max(64)
}

fn bloom_num_hashes(num_bits: usize, n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    ((num_bits as f64 / n as f64) * 2f64.ln()).round() as usize
}

/// The two hashes a [`BloomFilter`] probes `value` with (stable FNV-1a
/// double hash; not `DefaultHasher`, which is not portable).
pub fn bloom_key_hashes(value: &str) -> (u64, u64) {
    bloom_hashes(value)
}

fn bloom_hashes(value: &str) -> (u64, u64) {
    let h1 = fnv1a64(value.as_bytes(), 0x9e3779b97f4a7c15);
    let h2 = fnv1a64(value.as_bytes(), 0xbf58476d1ce4e5b9) | 1; // odd → full period
    (h1, h2)
}

fn fnv1a64(data: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ seed;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn encode_base64(data: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Byte `index` of the data a base64 string encodes, decoding only the
/// 4-character group that byte sits in. `None` if the group is missing,
/// malformed, or padded at that position.
fn base64_byte(text: &[u8], index: usize) -> Option<u8> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            b'=' => Some(0),
            _ => None,
        }
    }
    let start = index / 3 * 4;
    let group = text.get(start..start + 4)?;
    let position = index % 3;
    // Byte 1 of a group is absent when its third character is padding, and
    // byte 2 when its fourth is.
    if position > 0 && group[position + 1] == b'=' {
        return None;
    }
    let n = group
        .iter()
        .try_fold(0u32, |n, &c| Some((n << 6) | val(c)?))?;
    Some((n >> (16 - 8 * position)) as u8)
}

/// Whether a segment with the given footer blooms might satisfy `filter`.
///
/// Returns `true` when the segment must be opened (possible match, or not
/// enough bloom metadata to decide). Returns `false` only when blooms prove
/// no document in the segment can match.
/// The stored name behind an OpenSearch sub-field reference.
///
/// OpenSearch mappings give a text field an exact-match twin, addressed as
/// `<field>.keyword` (or `.raw`). Kosha stores one filterable value per
/// field and has no sub-fields, so `tags.keyword` and `tags` are the same
/// column. Callers look a field up by its exact name first and fall back to
/// this, so a field that really is named `a.keyword` still wins.
pub fn base_field_name(field: &str) -> &str {
    field
        .strip_suffix(".keyword")
        .or_else(|| field.strip_suffix(".raw"))
        .unwrap_or(field)
}

/// `map[field]`, or `map[base_field_name(field)]` when the exact name is
/// absent. See [`base_field_name`].
pub fn lookup_field<'a, V>(map: &'a HashMap<String, V>, field: &str) -> Option<&'a V> {
    map.get(field).or_else(|| {
        let base = base_field_name(field);
        if base.len() == field.len() {
            None
        } else {
            map.get(base)
        }
    })
}

/// Whether a segment with no bloom for a field could still hold `value`.
///
/// Blooms are only built for string columns (`build_filter_blooms`), so a
/// missing bloom means one of two things: the segment has no such field, or
/// the field is numeric. A numeric column can only ever equal a value that
/// parses as a number, so only such a value keeps the segment in play;
/// anything else is safe to prune on. Pruning unconditionally here is what
/// made a term filter on an Integer or Float field match nothing.
fn unbloomed_field_may_match(value: &str) -> bool {
    value.trim().parse::<f64>().is_ok()
}

pub fn segment_may_match(
    filter: &FilterClause,
    blooms: Option<&HashMap<String, BloomFilter>>,
) -> bool {
    let Some(blooms) = blooms else {
        return true; // legacy footer — cannot prune
    };
    match filter {
        // Term/Terms field maps are OR'd by the filter evaluator — skip only
        // if every alternative is impossible.
        FilterClause::Term { term } => {
            term.is_empty()
                || term
                    .iter()
                    .any(|(field, value)| match lookup_field(blooms, field) {
                        Some(bloom) => bloom.may_contain(value),
                        None => unbloomed_field_may_match(value),
                    })
        }
        FilterClause::Terms { terms } => {
            terms.is_empty()
                || terms
                    .iter()
                    .any(|(field, values)| match lookup_field(blooms, field) {
                        Some(bloom) => values.iter().any(|v| bloom.may_contain(v)),
                        None => values.iter().any(|v| unbloomed_field_may_match(v)),
                    })
        }
        FilterClause::Bool { bool: b } => {
            for child in &b.must {
                if !segment_may_match(child, Some(blooms)) {
                    return false;
                }
            }
            // FilterApplier always intersects should when non-empty.
            if !b.should.is_empty() && b.minimum_should_match > 0 {
                let matching = b
                    .should
                    .iter()
                    .filter(|c| segment_may_match(c, Some(blooms)))
                    .count();
                if matching < b.minimum_should_match {
                    return false;
                }
            }
            // must_not cannot prove a segment has no matches via bloom.
            true
        }
        // A field with no bloom may still be a numeric column, so the
        // absence of a bloom cannot prove the field is absent.
        FilterClause::Range { .. }
        | FilterClause::MatchAll { .. }
        | FilterClause::Exists { .. }
        | FilterClause::GeoDistance { .. } => true,
    }
}

// ─── Manifest ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub segment_id: SegmentId,
    pub doc_count: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub segments: Vec<ManifestEntry>,
    /// In-memory cache of parsed per-segment footers for search-time
    /// pruning/open. Readers fall back to `footer.json` when an entry is
    /// missing.
    ///
    /// **Never persisted** (issue #241). Footers carry the filter and term
    /// blooms (up to [`BLOOM_MAX_BYTES`] per field), so inlining them made
    /// every publish rewrite O(segments × footer size) bytes into the
    /// control store — a 12 MB row at 1,529 segments. `footer.json` in the
    /// segment prefix is the source of truth; segments are immutable, so a
    /// parsed footer stays valid for the life of its segment id and is
    /// carried across manifest versions by
    /// [`Manifest::carry_segment_footers_from`].
    ///
    /// Still *deserialized* so manifests written before #241 (which inline
    /// footers) keep their warm-start benefit until the next publish.
    /// Stored as `Arc` so cloning a manifest per request stays cheap.
    #[serde(default, skip_serializing)]
    pub segment_footers: HashMap<SegmentId, Arc<Footer>>,
    /// CRC32 of the current `footer.json` for each segment whose footer was
    /// **rewritten after the segment was committed** (e.g. an admin bloom
    /// rebuild). Absent for a segment means its footer is the immutable one
    /// written at flush/compaction time — the normal case, so this map is
    /// usually empty and costs nothing in the row.
    ///
    /// Persisted, because footers themselves are not (#241): it is how a
    /// reader learns that a footer it already parsed, or a `footer.json`
    /// already on its local disk, is stale. A cached footer is only carried
    /// to a new version when this value is unchanged.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub footer_crc32: HashMap<SegmentId, u32>,
    /// WAL records through this sequence are represented by this manifest.
    #[serde(default)]
    pub wal_checkpoint: u64,
    /// Identity of the WAL whose records the checkpoint covers. Independent
    /// ingest disks may have overlapping sequence numbers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_origin: Option<String>,
    /// Durable deletions, shared by ingest and query replicas.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub tombstones: HashMap<SegmentId, std::collections::HashSet<u32>>,
    /// Immutable deletion generations keyed by segment. `tombstones` remains
    /// readable for legacy manifests and is populated in memory by readers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub deletion_vectors: HashMap<SegmentId, deletes::DeletionVector>,
    /// Namespace schema (`POST /v1/namespaces/{ns}/schema`). Read by the
    /// segment writers (flush and compaction) for each field's analyzer.
    /// Segments record the chains they actually used in their own footer,
    /// so editing the schema never changes how an existing segment is
    /// searched. `None` = no schema: every field uses `standard`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<NamespaceSchema>,
}

impl Manifest {
    /// The analysis settings new segments of this namespace are written
    /// with. A schema this binary cannot resolve (one set by a newer
    /// version naming an analyzer this one lacks) falls back to the
    /// default chain: the segments written are then recorded as such in
    /// their footers, so they stay searchable either way.
    pub fn analysis_config(&self) -> AnalysisConfig {
        match self.schema.as_ref().map(NamespaceSchema::resolve) {
            Some(Ok(config)) => config,
            Some(Err(e)) => {
                eprintln!("kosha: namespace schema not resolvable ({e}); using default analysis");
                AnalysisConfig::default()
            }
            None => AnalysisConfig::default(),
        }
    }

    /// Semantic equality, independent of hash-map serialization order.
    pub fn same_contents(&self, other: &Self) -> bool {
        if self.tombstones != other.tombstones {
            return false;
        }
        match (serde_json::to_value(self), serde_json::to_value(other)) {
            (Ok(mut a), Ok(mut b)) => {
                // Sets serialize as arrays with arbitrary iteration order.
                a.as_object_mut().unwrap().remove("tombstones");
                b.as_object_mut().unwrap().remove("tombstones");
                a == b
            }
            _ => false,
        }
    }

    pub fn segment_footer(&self, segment_id: &SegmentId) -> Option<&Footer> {
        self.segment_footers.get(segment_id).map(Arc::as_ref)
    }

    /// The segment's footer for pruning: the manifest's own copy, borrowed,
    /// or `read()` (from the segment's `footer.json`) when the manifest
    /// carries none.
    ///
    /// Borrowed on purpose. A footer holds the segment's term bloom and a
    /// bloom per filter field, and every search looks at every segment's
    /// footer; cloning it per segment per search copied megabytes to read a
    /// few bits.
    pub fn footer_or_read<'a>(
        &'a self,
        segment_id: &SegmentId,
        read: impl FnOnce() -> Option<Footer>,
    ) -> Option<std::borrow::Cow<'a, Footer>> {
        match self.segment_footer(segment_id) {
            Some(footer) => Some(std::borrow::Cow::Borrowed(footer)),
            None => read().map(std::borrow::Cow::Owned),
        }
    }

    pub fn remember_segment_footer(&mut self, footer: impl Into<Arc<Footer>>) {
        let footer = footer.into();
        self.segment_footers
            .insert(footer.segment_id.clone(), footer);
    }

    /// Cache a footer only if its segment is listed in this manifest and no
    /// footer is cached for it yet. Returns whether it was inserted.
    pub fn remember_listed_segment_footer(&mut self, footer: impl Into<Arc<Footer>>) -> bool {
        let footer = footer.into();
        if self.segment_footers.contains_key(&footer.segment_id)
            || !self
                .segments
                .iter()
                .any(|entry| entry.segment_id == footer.segment_id)
        {
            return false;
        }
        self.segment_footers
            .insert(footer.segment_id.clone(), footer);
        true
    }

    /// Reuse already-parsed footers from `previous` for every segment this
    /// manifest still lists but has no footer for, and drop cached footers
    /// for segments it no longer lists. A footer is only carried when the
    /// segment's [`Manifest::footer_crc32`] is the same in both versions —
    /// otherwise the footer was rewritten in between and must be re-read.
    /// Zero I/O.
    pub fn carry_segment_footers_from(&mut self, previous: &Manifest) {
        let listed: std::collections::HashSet<&SegmentId> = self
            .segments
            .iter()
            .map(|entry| &entry.segment_id)
            .collect();
        self.segment_footers.retain(|id, _| listed.contains(id));
        for id in listed {
            if self.footer_crc32.get(id) != previous.footer_crc32.get(id) {
                continue;
            }
            if !self.segment_footers.contains_key(id) {
                if let Some(footer) = previous.segment_footers.get(id) {
                    self.segment_footers.insert(id.clone(), Arc::clone(footer));
                }
            }
        }
    }
}

// ─── kNN query ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnnQuery {
    pub field: String,
    pub vector: Vec<f32>,
    #[serde(default = "default_knn_k")]
    pub k: usize,
    #[serde(default = "default_knn_num_candidates")]
    pub num_candidates: usize,
    #[serde(default)]
    pub filter: Option<FilterClause>,
}
fn default_knn_k() -> usize {
    10
}
fn default_knn_num_candidates() -> usize {
    100
}

/// Vector store for a segment: doc_seq → embedding vector.
#[derive(Debug, Clone, Default)]
pub struct VectorStore {
    pub vectors: Vec<(u32, Vec<f32>)>,
    pub dimensions: usize,
}

// ─── Query / result types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query_text: String,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default)]
    pub from: usize,
    /// Per-request BM25 `k1` / `b`; unset parameters use the segment's
    /// build-time values. Checked by [`SearchQuery::validate`].
    #[serde(default)]
    pub bm25_params: Bm25Overrides,
    #[serde(default)]
    pub filter: Option<FilterClause>,
    #[serde(default)]
    pub sort: Vec<SortSpec>,
    /// OpenSearch-compatible search_after cursor. Values align with `sort`
    /// (or `_id` when sorting by document id). Results strictly after this
    /// cursor in sort order are returned.
    #[serde(default)]
    pub search_after: Option<Vec<String>>,
    #[serde(default)]
    pub highlight: Option<HighlightConfig>,
    #[serde(default)]
    pub aggs: std::collections::HashMap<String, Aggregation>,
    #[serde(default)]
    pub wildcard: Option<WildcardQuery>,
    /// Typo-tolerant term matching (issue #201): each token of
    /// `fuzzy.value` matches the terms within `fuzziness` edits of it. A
    /// lexical retriever of its own, so it cannot be combined with
    /// `query_text`, `wildcard` or `match_phrase`; it can be the lexical
    /// leg of a hybrid request. See [`fuzzy`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fuzzy: Option<FuzzyQuery>,
    #[serde(default)]
    pub match_phrase: Option<MatchPhraseQuery>,
    #[serde(default)]
    pub knn: Option<KnnQuery>,
    /// `None` (default) defers to the engine-wide `KOSHA_EXACT_TOTAL_HITS`
    /// setting (itself `false` unless set) — `total_hits` is capped at
    /// `total_hits_cap` and reported with `relation: "gte"` once exceeded,
    /// OpenSearch `track_total_hits`-style. `Some(true)` forces an exact
    /// count for this query regardless of the engine default: every AND-join
    /// intersection member is visited (no early exit), which is the cost a
    /// broad multi-term query pays to get an exact number nobody reads past
    /// page 1 of. See [`TotalHitsRelation`].
    #[serde(default)]
    pub exact_total_hits: Option<bool>,
    /// Per-query override for the capped-count threshold (only consulted
    /// when `total_hits` isn't being counted exactly). `None` defers to the
    /// engine-wide `KOSHA_TOTAL_HITS_CAP` setting (default `10_000`).
    #[serde(default)]
    pub total_hits_cap: Option<usize>,
    /// `None` (default) is strict AND: a document must contain every
    /// `query_text` term to match at all — unchanged from before this
    /// field existed. `Some(Or)` matches a document containing ANY query
    /// term (union), scored by summing whichever terms it actually
    /// matched — OpenSearch's `operator: or` on a `match` query.
    ///
    /// OR's fast path skips over non-contributing postings the same way
    /// AND's capped-mode early exit does, which is exactly why an exact
    /// union count is incompatible with it: a skipped stretch is never
    /// visited, so it can never be counted either. `exact_total_hits:
    /// true` with `operator: or` therefore falls back to a full,
    /// unskipped union scan instead of the fast join — still correct,
    /// just without OR's speed advantage. See [`QueryOperator`].
    #[serde(default)]
    pub operator: Option<QueryOperator>,
    /// Bypass the server's whole-response result cache for this request:
    /// the search executes in full and its response is not stored. For
    /// measurement and read-after-write tests. `None`/`false` = normal
    /// caching (when the engine has it enabled).
    #[serde(default)]
    pub no_cache: Option<bool>,
    /// Keep only the best hit for each distinct value of a field
    /// (OpenSearch `collapse`). "Best" is by the request's own ordering.
    /// `total_hits` still counts matching documents, not groups.
    #[serde(default)]
    pub collapse: Option<CollapseSpec>,
    /// Result diversification (issue #211): at most `max` hits per distinct
    /// value of `field`, applied while the result list is collected, so the
    /// page is filled from the next-best groups rather than cut short.
    /// `collapse` is the `max: 1` special case with one difference:
    /// documents with no value for the field are never capped here.
    ///
    /// The cap is a function of the result order alone: a hit is kept when
    /// fewer than `max` hits of its group rank above it. A `search_after`
    /// cursor therefore needs no extra state: each page is the same capped
    /// list resumed after the cursor, so a document appears at most once
    /// and a group spent on earlier pages is never refilled.
    ///
    /// `total_hits` (and aggregations) count matching documents before the
    /// cap, as with `collapse`. A pure kNN request is the exception it
    /// already was: there `total_hits` is the size of the returned
    /// neighbour set, which is the diversified one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_per: Option<LimitPerSpec>,
    /// Return only these stored fields with each hit (OpenSearch
    /// `_source.includes`). `None` returns every field.
    #[serde(default)]
    pub source_includes: Option<Vec<String>>,
    /// Leave these stored fields out of each hit (OpenSearch
    /// `_source.excludes`). The common use is dropping the embedding, which
    /// is by far the largest field and almost never read back.
    #[serde(default)]
    pub source_excludes: Option<Vec<String>>,
    /// How a hybrid request (a lexical query plus `knn`) fuses its two
    /// result lists. Elasticsearch's `"rank": {"rrf": {...}}` shape.
    /// `None` on a hybrid request is RRF with the default parameters; on a
    /// request that is not hybrid it must be `None` (see
    /// [`SearchQuery::validate`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank: Option<RankSpec>,
    /// Which retrievers the request intends to run (proto
    /// `RetrievalMode`). `None` infers it from the clauses present. When
    /// given it is checked against them, so a client that asks for
    /// `hybrid` and forgot the vector gets a 400, not lexical-only hits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<RetrievalMode>,
    /// Restrict `query_text` to these `Text` fields, each optionally
    /// boosted: `["title^2", "content"]` (issue #249; Elasticsearch
    /// `multi_match` `most_fields`). A document's score is the sum over
    /// fields of `boost × BM25` computed with that field's own length and
    /// statistics. With the AND operator every query term has to match in
    /// at least one of the fields. Empty (the default) searches the
    /// combined index of every `Text` field, as before; `_all` names that
    /// combined index and may be listed alongside real fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    /// Score modifiers (issue #212): blend numeric, date and geo attributes
    /// into the score with field-value boosts and linear / exp / gauss
    /// decays. Applied in a rescore window: the query is ranked by its own
    /// score first (BM25, fused RRF, or vector similarity, exactly as
    /// without `rank_by`), the best `window_size` hits are rescored from
    /// the filter columns and re-sorted, and only those are returned. See
    /// [`rank::RankBySpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_by: Option<RankBySpec>,
}

/// Proto `RetrievalMode`, as the JSON API spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RetrievalMode {
    /// `query_text` / `wildcard` / `match_phrase` (or a filter alone); no `knn`.
    Lexical,
    /// `knn` only.
    Semantic,
    /// A lexical query and `knn`, fused by Reciprocal Rank Fusion.
    Hybrid,
}

/// See `SearchQuery.rank`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RankSpec {
    #[serde(default)]
    pub rrf: Option<RrfSpec>,
}

/// Reciprocal Rank Fusion parameters (Elasticsearch-compatible names and
/// defaults): `score(d) = Σ_r 1 / (rank_constant + rank_r(d))` over the
/// retrievers `r` whose top-`rank_window_size` list contains `d`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RrfSpec {
    /// How many results each retriever contributes, and how long the fused
    /// list is. Defaults to `max_results` (Elasticsearch: `size`). Must be
    /// at least `max_results`.
    #[serde(default)]
    pub rank_window_size: Option<usize>,
    /// Damps the advantage of the very top ranks. Default 60; must be ≥ 1.
    #[serde(default)]
    pub rank_constant: Option<u32>,
}

/// Default `rank_constant` (Elasticsearch and the original RRF paper).
pub const DEFAULT_RRF_RANK_CONSTANT: u32 = 60;

/// Resolved RRF parameters for one request; see [`SearchQuery::rrf`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RrfParams {
    pub rank_window_size: usize,
    pub rank_constant: u32,
}

/// Per-retriever ranks of a fused hit (`ScoredDocument.rank_details`).
/// Ranks are 1-based positions in each retriever's namespace-wide list;
/// `None` means the hit was not in that retriever's window.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RankDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_rank: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub knn_rank: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub knn_score: Option<f64>,
    /// `rank_by` requests only: the hit's score before the modifiers (BM25,
    /// fused RRF or vector score); `score` is the modified one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_score: Option<f64>,
}

/// See `SearchQuery.collapse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollapseSpec {
    pub field: String,
}

/// See `SearchQuery.limit_per`. JSON: `"limit_per": {"field": "author", "max": 2}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitPerSpec {
    /// A keyword (or other filterable) field. Documents without a value
    /// for it are not capped.
    pub field: String,
    /// Hits allowed per distinct value; must be >= 1.
    pub max: usize,
}

impl SearchQuery {
    /// Request-level checks that do not need any segment: currently the
    /// BM25 parameter ranges. An `Err` is a client error (HTTP 400).
    pub fn validate(&self) -> Result<(), KoshaError> {
        self.bm25_params
            .validate()
            .map_err(KoshaError::InvalidQuery)?;
        if let Some(ref fuzzy) = self.fuzzy {
            fuzzy.validate().map_err(KoshaError::InvalidQuery)?;
            if !self.query_text.trim().is_empty()
                || self.wildcard.is_some()
                || self.match_phrase.is_some()
                || !self.fields.is_empty()
            {
                return Err(KoshaError::InvalidQuery(
                    "fuzzy is a lexical query of its own; it cannot be combined with \
                     query_text, fields, wildcard or match_phrase (restrict it with \
                     fuzzy.field)"
                        .into(),
                ));
            }
        }
        if !self.fields.is_empty() {
            fields::parse_field_boosts(&self.fields).map_err(KoshaError::InvalidQuery)?;
            if self.match_phrase.is_some() || self.wildcard.is_some() {
                return Err(KoshaError::InvalidQuery(
                    "fields applies to query_text; restrict match_phrase with its own \
                     field / fields, and wildcard with its field"
                        .into(),
                ));
            }
        }
        if let Some(ref phrase) = self.match_phrase {
            for name in phrase.fields.iter().chain(std::iter::once(&phrase.field)) {
                if name.contains('^') || (name.contains('*') && name.trim() != "*") {
                    return Err(KoshaError::InvalidQuery(format!(
                        "match_phrase field {name:?}: boosts and field patterns are not supported"
                    )));
                }
            }
        }
        if let Some(ref limit) = self.limit_per {
            if limit.max == 0 {
                return Err(KoshaError::InvalidQuery(
                    "limit_per.max must be >= 1".into(),
                ));
            }
            let field = limit.field.trim();
            if field.is_empty() || field == "_id" || field == "_score" {
                return Err(KoshaError::InvalidQuery(format!(
                    "limit_per.field must name a stored keyword field, not {:?}",
                    limit.field
                )));
            }
            if self.collapse.is_some() {
                return Err(KoshaError::InvalidQuery(
                    "collapse and limit_per cannot be combined; collapse is limit_per with max 1"
                        .into(),
                ));
            }
        }
        if let Some(ref rank_by) = self.rank_by {
            rank_by
                .compile(self.max_results, 0)
                .map_err(KoshaError::InvalidQuery)?;
            if !sort_is_by_score(&self.sort) {
                return Err(KoshaError::InvalidQuery(
                    "rank_by reorders by the modified score; it cannot be combined with a \
                     sort other than [_score desc] / [_score desc, _id asc]"
                        .into(),
                ));
            }
            // The window is the best hits by the query's own score. A
            // filter alone scores every document the same, so its "best"
            // window would be an arbitrary cut that drops correct results.
            if !self.has_lexical_query() && self.knn.is_none() {
                return Err(KoshaError::InvalidQuery(
                    "rank_by rescores the best hits of a query; it needs query_text, \
                     wildcard, match_phrase, fuzzy or knn, not a filter alone"
                        .into(),
                ));
            }
        }
        for (name, agg) in &self.aggs {
            agg.validate(name).map_err(KoshaError::InvalidQuery)?;
        }
        let has_lexical = self.has_lexical_query();
        let has_knn = self.knn.is_some();
        match self.mode {
            Some(RetrievalMode::Hybrid) if !(has_lexical && has_knn) => {
                return Err(KoshaError::InvalidQuery(
                    "mode \"hybrid\" needs both a lexical query (query_text, wildcard, \
                     match_phrase or fuzzy) and knn"
                        .into(),
                ));
            }
            Some(RetrievalMode::Lexical) if has_knn => {
                return Err(KoshaError::InvalidQuery(
                    "mode \"lexical\" cannot have a knn clause".into(),
                ));
            }
            Some(RetrievalMode::Semantic) if !has_knn || has_lexical => {
                return Err(KoshaError::InvalidQuery(
                    "mode \"semantic\" needs knn and no lexical query".into(),
                ));
            }
            _ => {}
        }
        if let Some(ref rank) = self.rank {
            let Some(ref rrf) = rank.rrf else {
                return Err(KoshaError::InvalidQuery(
                    "rank needs a method; only \"rrf\" is supported".into(),
                ));
            };
            if !self.is_hybrid() {
                return Err(KoshaError::InvalidQuery(
                    "rank.rrf fuses a lexical query with knn; the request needs both \
                     (query_text, wildcard, match_phrase or fuzzy, plus knn)"
                        .into(),
                ));
            }
            if rrf.rank_constant == Some(0) {
                return Err(KoshaError::InvalidQuery(
                    "rank.rrf.rank_constant must be >= 1".into(),
                ));
            }
            if let Some(window) = rrf.rank_window_size {
                if window == 0 || window < self.max_results {
                    return Err(KoshaError::InvalidQuery(format!(
                        "rank.rrf.rank_window_size ({window}) must be >= 1 and >= \
                         max_results ({})",
                        self.max_results
                    )));
                }
            }
        }
        Ok(())
    }

    /// The compiled `rank_by` for this request, `"now"` resolved to the
    /// current time. `None` without `rank_by`; an invalid one is an error
    /// ([`Self::validate`] reports it first).
    pub fn compiled_rank_by(&self) -> Result<Option<CompiledRankBy>, KoshaError> {
        let Some(ref rank_by) = self.rank_by else {
            return Ok(None);
        };
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        rank_by
            .compile(self.max_results, now_ms)
            .map(Some)
            .map_err(KoshaError::InvalidQuery)
    }

    /// How many hits each segment must hand back, best first by the
    /// query's own score, for the page to be right: `from + max_results`,
    /// widened to the rescore window under `rank_by` (0 stays 0: nothing
    /// is returned, so nothing is rescored).
    pub fn collect_depth(&self) -> usize {
        let page = self.from.saturating_add(self.max_results);
        match self.rank_by {
            Some(ref rank_by) if page > 0 => page.max(rank_by.window(self.max_results)),
            _ => page,
        }
    }

    /// True when the request has a lexical retriever: query text, a
    /// wildcard or a phrase. A filter alone is not one.
    pub fn has_lexical_query(&self) -> bool {
        !self.query_text.trim().is_empty()
            || self.wildcard.is_some()
            || self.match_phrase.is_some()
            || self.fuzzy.is_some()
    }

    /// The parsed `fields` list, or `None` when `query_text` searches the
    /// combined index (no `fields`, or only `_all` unboosted). An invalid
    /// list is `None` too; [`Self::validate`] rejects it first.
    pub fn field_boosts(&self) -> Option<Vec<FieldBoost>> {
        if self.fields.is_empty() {
            return None;
        }
        let parsed = fields::parse_field_boosts(&self.fields).ok()?;
        if parsed.len() == 1 && parsed[0].is_all() && parsed[0].boost == 1.0 {
            return None;
        }
        Some(parsed)
    }

    /// True when the request has both a lexical retriever and `knn`: its
    /// two result lists are fused with Reciprocal Rank Fusion.
    pub fn is_hybrid(&self) -> bool {
        self.knn.is_some() && self.has_lexical_query()
    }

    /// The RRF parameters for a hybrid request (defaults filled in), or
    /// `None` when the request is not hybrid.
    pub fn rrf(&self) -> Option<RrfParams> {
        if !self.is_hybrid() {
            return None;
        }
        let spec = self.rank.as_ref().and_then(|r| r.rrf.as_ref());
        Some(RrfParams {
            rank_window_size: spec
                .and_then(|s| s.rank_window_size)
                .unwrap_or(self.max_results)
                .max(1),
            rank_constant: spec
                .and_then(|s| s.rank_constant)
                .unwrap_or(DEFAULT_RRF_RANK_CONSTANT)
                .max(1),
        })
    }

    /// True when the vector leg is the only thing that can produce a hit:
    /// no query text, wildcard, phrase or top-level filter.
    pub fn is_pure_knn(&self) -> bool {
        self.knn.is_some()
            && self.query_text.trim().is_empty()
            && self.wildcard.is_none()
            && self.match_phrase.is_none()
            && self.fuzzy.is_none()
            && self.filter.is_none()
    }

    /// Whether a segment with these filter blooms could contribute a hit.
    ///
    /// The top-level filter always applies. `knn.filter` only rules a
    /// segment out when the vector leg is the sole source of hits; next to
    /// a text query, a segment can still contribute text hits whatever the
    /// kNN filter says.
    ///
    /// Segment pruning is decided twice per request, once when choosing
    /// what to fetch from S3 and once when scoring. Both ask this, so they
    /// cannot disagree about which segments are in play.
    pub fn segment_may_match_filters(&self, blooms: Option<&HashMap<String, BloomFilter>>) -> bool {
        if let Some(ref filter) = self.filter {
            if !segment_may_match(filter, blooms) {
                return false;
            }
        }
        if self.is_pure_knn() {
            if let Some(filter) = self.knn.as_ref().and_then(|knn| knn.filter.as_ref()) {
                if !segment_may_match(filter, blooms) {
                    return false;
                }
            }
        }
        true
    }

    /// True when any filter is present that [`Self::segment_may_match_filters`]
    /// can prune on.
    pub fn has_prunable_filter(&self) -> bool {
        self.filter.is_some()
            || (self.is_pure_knn() && self.knn.as_ref().is_some_and(|k| k.filter.is_some()))
    }

    /// Whether answering needs a segment's filter columns (`filters.bin`):
    /// any filter, any aggregation, a collapse or `limit_per`, `rank_by`
    /// (its modifiers read the columns), or a sort on a stored field.
    pub fn needs_filter_columns(&self) -> bool {
        self.filter.is_some()
            || self.collapse.is_some()
            || self.limit_per.is_some()
            || self.rank_by.is_some()
            || self.knn.as_ref().is_some_and(|k| k.filter.is_some())
            || !self.aggs.is_empty()
            || self.sort.iter().any(|spec| {
                spec.fields
                    .keys()
                    .any(|field| field != "_score" && field != "_id")
            })
    }

    /// The filter-column names answering this query can read (#244): every
    /// field named by the filter, the kNN filter, a field sort, the
    /// collapse, `limit_per`, `rank_by` and the aggregations. Each `x.keyword`/`x.raw` name also
    /// adds its base `x`, because lookups fall back to it (see
    /// [`lookup_field`]). Empty when [`Self::needs_filter_columns`] is
    /// false. The query node hydrates only these columns of a columnar
    /// segment.
    pub fn filter_column_fields(&self) -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        let mut add = |field: &str| {
            out.insert(field.to_string());
            let base = base_field_name(field);
            if base.len() != field.len() {
                out.insert(base.to_string());
            }
        };
        if let Some(filter) = &self.filter {
            filter.for_each_field(&mut add);
        }
        if let Some(filter) = self.knn.as_ref().and_then(|k| k.filter.as_ref()) {
            filter.for_each_field(&mut add);
        }
        if let Some(collapse) = &self.collapse {
            add(&collapse.field);
        }
        // `limit_per` groups hits by a field's value and `rank_by` reads
        // its modifiers' fields from the filter columns (#211, #212).
        if let Some(limit_per) = &self.limit_per {
            add(&limit_per.field);
        }
        if let Some(rank_by) = &self.rank_by {
            rank_by.fields().for_each(&mut add);
        }
        for spec in &self.sort {
            for field in spec.fields.keys() {
                if field != "_score" && field != "_id" {
                    add(field);
                }
            }
        }
        // Metric aggregations, and metric sub-aggregations under a bucket
        // aggregation, read their fields from the filter columns too.
        let subs = self
            .aggs
            .values()
            .filter_map(Aggregation::sub_aggregations)
            .flat_map(|subs| subs.values());
        for agg in self.aggs.values().chain(subs) {
            match agg {
                Aggregation::Terms { terms, .. } => add(&terms.field),
                Aggregation::Cardinality { cardinality } => add(&cardinality.field),
                Aggregation::Composite { composite, .. } => {
                    for source in &composite.sources {
                        for terms in source.source.values() {
                            add(&terms.terms.field);
                        }
                    }
                }
                _ => {
                    if let Some((_, metric)) = agg.metric() {
                        add(&metric.field);
                    }
                }
            }
        }
        out
    }

    /// Whether `field` should be returned with a hit, per
    /// `source_includes` / `source_excludes`.
    pub fn returns_field(&self, field: &str) -> bool {
        self.source_includes
            .as_ref()
            .is_none_or(|wanted| wanted.iter().any(|f| f == field))
            && !self
                .source_excludes
                .as_ref()
                .is_some_and(|unwanted| unwanted.iter().any(|f| f == field))
    }
}

/// True when `sort` orders by score, best first (ties by `_id`): no sort,
/// `[_score desc]` or `[_score desc, _id asc]`.
pub fn sort_is_by_score(sort: &[SortSpec]) -> bool {
    let keys: Vec<(&str, &str)> = sort
        .iter()
        .flat_map(|spec| spec.fields.iter())
        .map(|(field, order)| (field.as_str(), order.order.as_str()))
        .collect();
    matches!(
        keys.as_slice(),
        [] | [("_score", "desc")] | [("_score", "desc"), ("_id", "asc")]
    )
}

/// See `SearchQuery.operator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueryOperator {
    And,
    Or,
}
fn default_max_results() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredDocument {
    pub doc_id: DocumentId,
    pub score: f64,
    pub fields: Vec<Field>,
    /// Legacy single-field highlight fragments: set only for a request
    /// that used the legacy `{"highlight": {"field": ...}}` form.
    /// Deprecated: request `highlight.fields` and read `highlight` instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub highlights: Option<Vec<String>>,
    /// Highlight fragments keyed by field name, ES-shaped
    /// (`"highlight": {"title": ["..."], "body": ["..."]}`), for a request
    /// that used `highlight.fields`. A requested field the hit lacks, or
    /// that is not a text field, is left out; no matching field at all
    /// leaves the map unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub highlight: Option<std::collections::BTreeMap<String, Vec<String>>>,
    /// Hybrid (RRF) requests only: where this hit ranked in each
    /// retriever's list. `score` is then the fused RRF score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rank_details: Option<RankDetails>,
}

/// Whether `SearchResult.total_hits` is the exact match count or a capped
/// lower bound (OpenSearch/Elasticsearch `hits.total.relation` naming).
/// See `SearchQuery.exact_total_hits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TotalHitsRelation {
    Eq,
    Gte,
}
fn default_total_hits_relation() -> TotalHitsRelation {
    TotalHitsRelation::Eq
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub results: Vec<ScoredDocument>,
    pub total_hits: usize,
    #[serde(default = "default_total_hits_relation")]
    pub total_hits_relation: TotalHitsRelation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<AggregationResults>,
    /// `None` for a non-kNN query (the concept doesn't apply). For a kNN
    /// query, the number of segments whose vector index search failed and
    /// silently contributed zero candidates for this request — `Some(0)`
    /// means every segment searched cleanly. A page that "succeeded" (200,
    /// non-empty `results`) can still be missing whatever the failed
    /// segment(s) would have contributed; this is the only client-visible
    /// signal of that. Was previously an `eprintln!` on the server only
    /// (kosha-query's `score_segment`) — invisible to callers, so a
    /// benchmark harness checking only HTTP status/latency could not have
    /// told a degraded response from a healthy one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knn_degraded_segments: Option<u32>,
}

// ─── Indexing types ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRequest {
    pub namespace: NamespaceId,
    pub documents: Vec<Document>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub indexed_count: usize,
    pub namespace: NamespaceId,
}

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum KoshaError {
    NamespaceNotFound(NamespaceId),
    ManifestConflict {
        namespace: NamespaceId,
        expected: u64,
        actual: Option<u64>,
    },
    SegmentNotFound(SegmentId),
    Io(std::io::Error),
    Serde(serde_json::Error),
    NotFound(String),
    InvalidFilter(String),
    /// A segment's sidecar index (e.g. `doc_store.offsets`) exists but fails
    /// a sanity check (bad header, doc-count mismatch, truncated file).
    /// Callers should treat this as a signal to fall back to the legacy
    /// full-parse path for that component, not as a fatal error for the
    /// whole segment.
    CorruptSegment(String),
    /// The server is shedding load: admitting this request would push live
    /// segment memory past the configured watermark and it did not free up
    /// within the admission timeout (see `kosha_query::MemoryLedger`).
    /// Transient by construction — callers should retry with backoff, which
    /// the Python `kosha_client` already does. Maps to HTTP 429.
    Overloaded(String),
    /// The control store (Postgres) could not be reached or returned an
    /// error. Says nothing about whether a namespace or manifest exists —
    /// callers that can (query replicas) keep serving their last-known-good
    /// manifest instead of failing (DESIGN.md §17).
    ControlPlane(String),
    /// The request itself is malformed or out of range (e.g. BM25 `b`
    /// outside `[0, 1]`). A client error: maps to HTTP 400.
    InvalidQuery(String),
}

impl std::fmt::Display for KoshaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ManifestConflict { namespace, expected, actual } => write!(f,
                "manifest conflict for {}: expected version {expected}, actual {actual:?}; stale writer must reload before publishing", namespace.0),
            Self::NamespaceNotFound(id) => write!(f, "namespace not found: {}", id.0),
            Self::SegmentNotFound(id) => write!(f, "segment not found: {}", id.0),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Serde(e) => write!(f, "serialization error: {e}"),
            Self::NotFound(msg) => write!(f, "{msg}"),
            Self::InvalidFilter(msg) => write!(f, "invalid filter: {msg}"),
            Self::CorruptSegment(msg) => write!(f, "corrupt segment data: {msg}"),
            Self::Overloaded(msg) => write!(f, "overloaded: {msg}"),
            Self::ControlPlane(msg) => write!(f, "control plane unavailable: {msg}"),
            Self::InvalidQuery(msg) => write!(f, "invalid query: {msg}"),
        }
    }
}
impl std::error::Error for KoshaError {}
impl From<std::io::Error> for KoshaError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
impl From<serde_json::Error> for KoshaError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e)
    }
}

// ─── Filter value store ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct FilterStore {
    pub string_fields: std::collections::HashMap<String, Vec<(u32, String)>>,
    pub integer_fields: std::collections::HashMap<String, Vec<(u32, i64)>>,
    pub float_fields: std::collections::HashMap<String, Vec<(u32, f64)>>,
}

#[cfg(test)]
mod filter_clause_parsing {
    //! Issue #170: an unrecognized filter key must be rejected, never
    //! silently parsed into something that matches every document. The
    //! consequence is asymmetric — on `/search` a match-everything filter
    //! quietly returns the whole namespace, and on `/delete` (delete-by-query)
    //! it would quietly delete it.
    use super::*;

    fn parse(s: &str) -> Result<FilterClause, serde_json::Error> {
        serde_json::from_str(s)
    }

    #[test]
    fn opensearch_bool_filter_key_means_must() {
        // `bool.filter` is OpenSearch's spelling of `bool.must` in filter
        // context. It once deserialized into an all-empty BoolFilter that
        // matched everything (issue #170), then was rejected outright. It
        // now folds into `must`, so the clause is enforced.
        match parse(r#"{"bool": {"filter": [{"term": {"a": "b"}}]}}"#).unwrap() {
            FilterClause::Bool { bool } => {
                assert_eq!(bool.must.len(), 1, "filter clause must be kept");
                assert!(matches!(bool.must[0], FilterClause::Term { .. }));
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn bool_filter_and_must_are_combined_and_nest() {
        // The backend's matter clause sits two levels down:
        // bool.filter -> bool.filter -> bool.should.
        let nested = parse(
            r#"{"bool": {"must": [{"term": {"a": "b"}}],
                        "filter": [{"bool": {"filter": [
                            {"bool": {"should": [{"term": {"m": "1"}}, {"term": {"n": "1"}}]}}
                        ]}}]}}"#,
        )
        .unwrap();
        let FilterClause::Bool { bool: outer } = nested else {
            panic!("expected Bool");
        };
        assert_eq!(outer.must.len(), 2, "must and filter are both kept");
        let FilterClause::Bool { bool: mid } = &outer.must[1] else {
            panic!("expected nested Bool");
        };
        let FilterClause::Bool { bool: inner } = &mid.must[0] else {
            panic!("expected innermost Bool");
        };
        assert_eq!(inner.should.len(), 2);
    }

    #[test]
    fn unknown_bool_key_is_still_rejected() {
        let err = parse(r#"{"bool": {"bogus": [{"term": {"a": "b"}}]}}"#)
            .expect_err("unknown bool key must be rejected");
        assert!(err.to_string().contains("unknown field `bogus`"), "{err}");
    }

    #[test]
    fn unknown_clause_does_not_fall_through_to_match_all() {
        // Untagged deserialization made MatchAll a catch-all, because every
        // field of every variant is defaulted or Option: `{"bogus": 1}` parsed
        // as MatchAll { match_all: None }.
        let err = parse(r#"{"bogus": 1}"#).expect_err("unknown clause must be rejected");
        assert!(
            err.to_string().contains("unknown filter clause `bogus`"),
            "{err}"
        );
    }

    #[test]
    fn bool_inner_clauses_are_validated_recursively() {
        let err = parse(r#"{"bool": {"must": [{"bogus": 1}]}}"#)
            .expect_err("nested unknown clause must be rejected");
        assert!(
            err.to_string().contains("unknown filter clause `bogus`"),
            "{err}"
        );
    }

    #[test]
    fn empty_clause_is_rejected_rather_than_matching_everything() {
        let err = parse("{}").expect_err("empty clause must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("empty filter clause"), "{msg}");
        assert!(
            msg.contains("match_all"),
            "error should point at the explicit form: {msg}"
        );
    }

    #[test]
    fn multiple_keys_are_rejected() {
        let err = parse(r#"{"term": {"a": "b"}, "terms": {"c": ["d"]}}"#)
            .expect_err("two clause keys must be rejected");
        assert!(err.to_string().contains("exactly one key"), "{err}");
    }

    #[test]
    fn bool_only_keys_at_clause_level_get_a_hint() {
        let err = parse(r#"{"must": [{"term": {"a": "b"}}]}"#).expect_err("must is not a clause");
        assert!(err.to_string().contains("inside a `bool` clause"), "{err}");
    }

    #[test]
    fn non_object_clause_is_rejected() {
        assert!(parse("[]").is_err());
        assert!(parse("\"term\"").is_err());
        assert!(parse("null").is_err());
    }

    #[test]
    fn every_valid_clause_still_parses() {
        assert!(matches!(
            parse(r#"{"term": {"a": "b"}}"#).unwrap(),
            FilterClause::Term { .. }
        ));
        assert!(matches!(
            parse(r#"{"terms": {"a": ["b", "c"]}}"#).unwrap(),
            FilterClause::Terms { .. }
        ));
        assert!(matches!(
            parse(r#"{"range": {"n": {"gte": "1", "lt": "9"}}}"#).unwrap(),
            FilterClause::Range { .. }
        ));
        assert!(matches!(
            parse(r#"{"match_all": {}}"#).unwrap(),
            FilterClause::MatchAll { .. }
        ));

        let nested = parse(
            r#"{"bool": {"must": [{"term": {"a": "b"}}],
                        "must_not": [{"terms": {"c": ["d"]}}],
                        "should": [{"range": {"n": {"gte": "1"}}}],
                        "minimum_should_match": 2}}"#,
        )
        .unwrap();
        match nested {
            FilterClause::Bool { bool } => {
                assert_eq!(bool.must.len(), 1);
                assert_eq!(bool.must_not.len(), 1);
                assert_eq!(bool.should.len(), 1);
                assert_eq!(bool.minimum_should_match, 2);
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn bool_defaults_are_unchanged() {
        match parse(r#"{"bool": {"must": [{"term": {"a": "b"}}]}}"#).unwrap() {
            FilterClause::Bool { bool } => {
                assert!(bool.must_not.is_empty());
                assert!(bool.should.is_empty());
                assert_eq!(bool.minimum_should_match, 1);
            }
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_through_serialize() {
        for s in [
            r#"{"term":{"a":"b"}}"#,
            r#"{"terms":{"a":["b","c"]}}"#,
            r#"{"match_all":{}}"#,
            r#"{"bool":{"must":[{"term":{"a":"b"}}],"must_not":[],"should":[],"minimum_should_match":1}}"#,
        ] {
            let parsed = parse(s).unwrap();
            let back = serde_json::to_string(&parsed).unwrap();
            assert!(parse(&back).is_ok(), "re-parse of {back} failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn highlight(json: serde_json::Value) -> Result<HighlightConfig, String> {
        serde_json::from_value(json).map_err(|e| e.to_string())
    }

    /// #195: the ES `fields` map and the legacy `field` form normalise into
    /// one struct; per-field tags fall back to the top level, then `<b>`.
    #[test]
    fn highlight_config_accepts_both_shapes_and_resolves_tags() {
        // Parsed from text: `json!` would sort the keys.
        let es: HighlightConfig = serde_json::from_str(
            r#"{"pre_tags": ["<i>"], "fields": {
                "title": {},
                "body": {"pre_tags": ["<em>"], "post_tags": ["</em>"], "fragment_size": 150},
                "summary": null}}"#,
        )
        .unwrap();
        assert!(!es.legacy_single_field);
        let names: Vec<_> = es.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["title", "body", "summary"], "request order kept");
        let tags = |name: &str| {
            let field = es.fields.iter().find(|f| f.name == name).unwrap();
            (es.pre_tag(field), es.post_tag(field))
        };
        assert_eq!(tags("title"), ("<i>", "</b>"));
        assert_eq!(tags("body"), ("<em>", "</em>"));

        // ES's ordered array form.
        let arr = highlight(serde_json::json!({"fields": [{"b": {}}, {"a": {}}]})).unwrap();
        let names: Vec<_> = arr.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["b", "a"]);

        let legacy = highlight(serde_json::json!({"field": "body", "pre_tags": []})).unwrap();
        assert!(legacy.legacy_single_field);
        assert_eq!(legacy.fields, vec![HighlightField::new("body")]);
        // An empty tag list counts as unset, as it always did.
        assert_eq!(legacy.pre_tag(&legacy.fields[0]), "<b>");

        let err = highlight(serde_json::json!({"field": "a", "fields": {"b": {}}})).unwrap_err();
        assert!(err.contains("not both"), "{err}");
        assert!(highlight(serde_json::json!({})).is_err());
        assert!(highlight(serde_json::json!({"fields": "body"})).is_err());
        assert!(highlight(serde_json::json!({"fields": [{"a": {}}, {"a": {}}]})).is_err());
    }

    /// The serialized form deserializes back to the same value (the CLI
    /// client and the result-cache key both serialize the query), and the
    /// two shapes serialize differently since they answer differently.
    #[test]
    fn highlight_config_serde_round_trips() {
        for json in [
            serde_json::json!({"field": "body"}),
            serde_json::json!({"field": "body", "pre_tags": ["<em>"], "post_tags": ["</em>"]}),
            serde_json::json!({"fields": {"body": {}}}),
            serde_json::json!({"pre_tags": ["<i>"], "fields": {"z": {"post_tags": ["]"]}, "a": {}}}),
        ] {
            let parsed = highlight(json.clone()).unwrap();
            let again = serde_json::to_value(&parsed).unwrap();
            assert_eq!(
                highlight(again.clone()).unwrap(),
                parsed,
                "{json} -> {again}"
            );
        }
        assert_ne!(
            serde_json::to_string(&HighlightConfig::single_field("body")).unwrap(),
            serde_json::to_string(&HighlightConfig::for_fields(["body"])).unwrap()
        );
    }

    #[test]
    fn base64_byte_reads_any_byte_without_decoding_the_rest() {
        // Lengths 1..=8 cover every padding shape; 300 spans many groups.
        for len in (1..=8).chain([300]) {
            let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            let text = encode_base64(&data);
            for (i, byte) in data.iter().enumerate() {
                assert_eq!(
                    base64_byte(text.as_bytes(), i),
                    Some(*byte),
                    "len {len} byte {i}"
                );
            }
            assert_eq!(
                base64_byte(text.as_bytes(), len),
                None,
                "one past the end, len {len}"
            );
        }
        assert_eq!(base64_byte(b"!!!!", 0), None, "malformed group");
    }

    #[test]
    fn bloom_answers_are_unchanged_by_reading_bits_in_place() {
        let values: Vec<String> = (0..500).map(|i| format!("term{i}")).collect();
        let bloom = BloomFilter::build(values.iter().map(String::as_str));
        assert!(
            values.iter().all(|v| bloom.may_contain(v)),
            "no false negatives"
        );
        let false_positives = (0..2000)
            .filter(|i| bloom.may_contain(&format!("absent{i}")))
            .count();
        assert!(false_positives < 200, "{false_positives} of 2000");
        // Unreadable filters cannot prune.
        let empty = BloomFilter {
            bits_b64: String::new(),
            ..bloom.clone()
        };
        assert!(empty.may_contain("anything"));
        let truncated = BloomFilter {
            bits_b64: bloom.bits_b64[..8].to_string(),
            ..bloom
        };
        assert!(values.iter().all(|v| truncated.may_contain(v)));
    }

    #[test]
    fn ids_wrap_strings() {
        let ns = NamespaceId("org1/matter42".to_string());
        let seg = SegmentId("seg-0001".to_string());
        assert_eq!(ns.0, "org1/matter42");
        assert_ne!(ns.0, seg.0);
    }

    #[test]
    fn posting_with_positions() {
        let p = Posting {
            doc_id: 0,
            term_frequency: 2,
            positions: vec![0, 3],
        };
        assert_eq!(p.positions.len(), 2);
        let json = serde_json::to_string(&p).unwrap();
        let back: Posting = serde_json::from_str(&json).unwrap();
        assert_eq!(back.positions, vec![0, 3]);
    }

    #[test]
    fn aggregation_serde() {
        let json = r#"{"per_document": {"terms": {"field": "documentId", "size": 1000}}}"#;
        let aggs: std::collections::HashMap<String, Aggregation> =
            serde_json::from_str(json).unwrap();
        assert!(aggs.contains_key("per_document"));

        let json = r#"{"total_documents": {"cardinality": {"field": "documentId"}}}"#;
        let aggs: std::collections::HashMap<String, Aggregation> =
            serde_json::from_str(json).unwrap();
        assert!(aggs.contains_key("total_documents"));
    }

    #[test]
    fn wildcard_query_serde() {
        let json = r#"{"field": "caseName", "pattern": "*Smith*", "case_insensitive": true}"#;
        let w: WildcardQuery = serde_json::from_str(json).unwrap();
        assert_eq!(w.pattern, "*Smith*");
        assert!(w.case_insensitive);
    }

    #[test]
    fn match_phrase_query_serde() {
        let json = r#"{"field": "content", "phrase": "tribal tax credit", "slop": 2}"#;
        let p: MatchPhraseQuery = serde_json::from_str(json).unwrap();
        assert_eq!(p.phrase, "tribal tax credit");
        assert_eq!(p.slop, 2);
    }

    #[test]
    fn bloom_contains_inserted_values() {
        let bloom = BloomFilter::build(["m1", "m2", "m3"]);
        assert!(bloom.may_contain("m1"));
        assert!(bloom.may_contain("m2"));
        assert!(bloom.may_contain("m3"));
        assert!(!bloom.may_contain("matter-definitely-absent-xyz"));
    }

    #[test]
    fn bloom_roundtrips_through_serde() {
        let bloom = BloomFilter::build(["matter-a"]);
        let json = serde_json::to_string(&bloom).unwrap();
        let back: BloomFilter = serde_json::from_str(&json).unwrap();
        assert!(back.may_contain("matter-a"));
        assert!(!back.may_contain("matter-absent"));
    }

    fn bloomy_footer(id: &str, docs: u32) -> Footer {
        let values: Vec<String> = (0..1_000).map(|i| format!("{id}-value-{i}")).collect();
        let mut filter_blooms = HashMap::new();
        filter_blooms.insert(
            "status".to_string(),
            BloomFilter::build(values.iter().map(String::as_str)),
        );
        Footer {
            segment_id: SegmentId(id.to_string()),
            doc_count: docs,
            total_field_length: 10,
            avg_field_length: 1.0,
            bm25_params: Bm25Params::default(),
            created_at: "2026-01-01T00:00:00Z".into(),
            filter_blooms: Some(filter_blooms),
            term_bloom: Some(BloomFilter::build(values.iter().map(String::as_str))),
            format_version: SEGMENT_FORMAT_VERSION,
            text_filter_ignore_above: Some(256),
            doc_vectors: false,
            posting_blobs_raw: false,
            filter_columns: None,
            analysis: None,
            field_index: None,
            fuzzy_bloom: None,
        }
    }

    fn manifest_with(ids: &[&str]) -> Manifest {
        Manifest {
            version: 7,
            segments: ids
                .iter()
                .map(|id| ManifestEntry {
                    segment_id: SegmentId(id.to_string()),
                    doc_count: 1_000,
                })
                .collect(),
            segment_footers: Default::default(),
            footer_crc32: Default::default(),
            wal_checkpoint: 0,
            wal_origin: None,
            tombstones: Default::default(),
            deletion_vectors: Default::default(),
            schema: None,
        }
    }

    /// #241: footers are an in-memory cache, never part of the persisted
    /// manifest, so a publish costs O(segments × ~100 B), not
    /// O(segments × footer size).
    #[test]
    fn persisted_manifest_excludes_segment_footers() {
        let mut m = manifest_with(&["s1"]);
        m.remember_segment_footer(bloomy_footer("s1", 1_000));
        assert!(m.segment_footer(&SegmentId("s1".into())).is_some());
        let json = serde_json::to_string(&m).unwrap();
        assert!(!json.contains("segment_footers"), "{json}");
        assert!(!json.contains("bits_b64"), "no bloom bytes in the row");
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert!(back.segment_footers.is_empty());
        assert!(
            back.same_contents(&m),
            "footers must not affect CAS idempotency checks"
        );
    }

    /// Acceptance criterion from #241: a 1,500-segment manifest is < 200 KB,
    /// even when every segment's footer (with blooms) is cached in memory.
    #[test]
    fn fifteen_hundred_segment_manifest_row_is_under_200kb() {
        let ids: Vec<String> = (0..1_500)
            .map(|i| format!("paragraph_index_hnsw-{i:06x}"))
            .collect();
        let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let mut m = manifest_with(&refs);
        for id in &ids {
            m.remember_segment_footer(bloomy_footer(id, 1_000));
        }
        let bytes = serde_json::to_vec(&m).unwrap().len();
        assert!(bytes < 200 * 1024, "manifest row is {bytes} bytes");
        assert!(
            bytes / 1_500 < 100,
            "~{} B per segment row, want < 100",
            bytes / 1_500
        );
    }

    /// Manifests written before #241 inline footers; they must still load,
    /// and keep the footers as a warm cache until the next publish.
    #[test]
    fn legacy_manifest_with_inline_footers_still_deserializes() {
        let legacy = serde_json::json!({
            "version": 3,
            "segments": [{"segment_id": "s1", "doc_count": 1000}],
            "segment_footers": {"s1": bloomy_footer("s1", 1000)},
        });
        let m: Manifest = serde_json::from_value(legacy).unwrap();
        let footer = m.segment_footer(&SegmentId("s1".into())).unwrap();
        assert!(footer.filter_blooms.is_some());
        assert!(!serde_json::to_string(&m).unwrap().contains("bits_b64"));
    }

    #[test]
    fn carry_segment_footers_keeps_listed_and_drops_unlisted() {
        let mut prev = manifest_with(&["s1", "s2"]);
        prev.remember_segment_footer(bloomy_footer("s1", 1));
        prev.remember_segment_footer(bloomy_footer("s2", 2));

        // s2 compacted away into s3; s1 survives.
        let mut next = manifest_with(&["s1", "s3"]);
        next.remember_segment_footer(bloomy_footer("s2", 2)); // stale entry
        next.carry_segment_footers_from(&prev);

        let s1 = SegmentId("s1".into());
        assert!(Arc::ptr_eq(
            &next.segment_footers[&s1],
            &prev.segment_footers[&s1]
        ));
        assert!(!next.segment_footers.contains_key(&SegmentId("s2".into())));
        assert!(!next.segment_footers.contains_key(&SegmentId("s3".into())));
    }

    /// A footer rewritten after commit (bloom rebuild) gets a new CRC in
    /// the persisted manifest; readers must drop the old parsed copy.
    #[test]
    fn carry_segment_footers_skips_rewritten_footers() {
        let mut prev = manifest_with(&["s1", "s2"]);
        prev.remember_segment_footer(bloomy_footer("s1", 1));
        prev.remember_segment_footer(bloomy_footer("s2", 2));
        prev.footer_crc32.insert(SegmentId("s2".into()), 7);

        let mut next = manifest_with(&["s1", "s2"]);
        next.footer_crc32.insert(SegmentId("s1".into()), 9); // s1 rewritten
        next.footer_crc32.insert(SegmentId("s2".into()), 7); // s2 unchanged
        next.carry_segment_footers_from(&prev);
        assert!(!next.segment_footers.contains_key(&SegmentId("s1".into())));
        assert!(next.segment_footers.contains_key(&SegmentId("s2".into())));

        let json = serde_json::to_string(&next).unwrap();
        let back: Manifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.footer_crc32, next.footer_crc32, "CRCs are persisted");
    }

    #[test]
    fn remember_listed_segment_footer_ignores_unlisted_and_existing() {
        let mut m = manifest_with(&["s1"]);
        assert!(!m.remember_listed_segment_footer(bloomy_footer("other", 1)));
        assert!(m.remember_listed_segment_footer(bloomy_footer("s1", 1)));
        assert!(!m.remember_listed_segment_footer(bloomy_footer("s1", 99)));
        assert_eq!(
            m.segment_footer(&SegmentId("s1".into())).unwrap().doc_count,
            1
        );
    }

    #[test]
    fn footer_legacy_without_blooms_deserializes() {
        let json = r#"{
            "segment_id": "s1",
            "doc_count": 1,
            "total_field_length": 3,
            "avg_field_length": 3.0,
            "bm25_params": {"k1": 1.2, "b": 0.75},
            "created_at": "2026-01-01T00:00:00Z"
        }"#;
        let footer: Footer = serde_json::from_str(json).unwrap();
        assert!(footer.filter_blooms.is_none());
        assert!(footer.term_bloom.is_none());
    }

    #[test]
    fn segment_may_contain_terms_and_or() {
        let bloom = build_term_bloom(["contract", "warranty"]);
        let both = vec!["contract".into(), "warranty".into()];
        // Distinctive absent tokens avoid rare bloom false positives at 1% FPR.
        let absent = "definitely-absent-term-xyz-qqq";
        let missing = vec!["contract".into(), absent.into()];
        let only_absent = vec![absent.into()];
        assert!(segment_may_contain_terms(
            &both,
            TermBloomMode::And,
            Some(&bloom)
        ));
        assert!(!segment_may_contain_terms(
            &missing,
            TermBloomMode::And,
            Some(&bloom)
        ));
        assert!(segment_may_contain_terms(
            &missing,
            TermBloomMode::Or,
            Some(&bloom)
        ));
        assert!(!segment_may_contain_terms(
            &only_absent,
            TermBloomMode::Or,
            Some(&bloom)
        ));
        assert!(
            segment_may_contain_terms(&missing, TermBloomMode::And, None),
            "legacy footer never pruned"
        );
    }

    #[test]
    fn segment_may_match_prunes_absent_matter() {
        let blooms = HashMap::from([("matterId".into(), BloomFilter::build(["m1"]))]);
        let keep = FilterClause::Term {
            term: HashMap::from([("matterId".into(), "m1".into())]),
        };
        let skip = FilterClause::Term {
            term: HashMap::from([("matterId".into(), "m2".into())]),
        };
        assert!(segment_may_match(&keep, Some(&blooms)));
        assert!(!segment_may_match(&skip, Some(&blooms)));
        assert!(segment_may_match(&skip, None), "legacy footer never pruned");
    }

    #[test]
    fn segment_may_match_terms_and_bool() {
        let blooms = HashMap::from([
            ("matterId".into(), BloomFilter::build(["m1"])),
            ("tag".into(), BloomFilter::build(["alpha"])),
        ]);
        let terms = FilterClause::Terms {
            terms: HashMap::from([("matterId".into(), vec!["m2".into(), "m1".into()])]),
        };
        assert!(segment_may_match(&terms, Some(&blooms)));

        let terms_miss = FilterClause::Terms {
            terms: HashMap::from([("matterId".into(), vec!["m9".into()])]),
        };
        assert!(!segment_may_match(&terms_miss, Some(&blooms)));

        let must = FilterClause::Bool {
            bool: BoolFilter {
                must: vec![
                    FilterClause::Term {
                        term: HashMap::from([("matterId".into(), "m1".into())]),
                    },
                    FilterClause::Term {
                        term: HashMap::from([("tag".into(), "missing".into())]),
                    },
                ],
                must_not: vec![],
                should: vec![],
                minimum_should_match: 1,
            },
        };
        assert!(!segment_may_match(&must, Some(&blooms)));
    }

    /// Regression test for the staging incident this generalizes PR #45's
    /// fix for: `SegmentWriter::finalize()` (and every other caller of
    /// `StorageBackend::write`, e.g. WAL, the S3-hydration local cache
    /// mirror) rewrites files through `LocalStorage::write`. A reader
    /// racing a rewrite of the same path must never observe a
    /// truncated/torn file — before `atomic_write`, plain `fs::write`
    /// (truncate then fill) could hand back anywhere from 0 bytes up to a
    /// partial mix, which is exactly how staging ended up with 0-byte
    /// `doc_store.bin`/`footer.json` in already-published, already-synced
    /// segments.
    #[test]
    fn concurrent_local_storage_write_never_yields_truncated_read() {
        let dir = std::env::temp_dir().join("kosha-core-test-atomic-write-race");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let content_a = vec![b'a'; 5_000];
        let content_b = vec![b'b'; 9_000];
        LocalStorage::new(dir.clone())
            .write("segment.bin", &content_a)
            .unwrap();

        let (writer_dir, reader_dir) = (dir.clone(), dir.clone());
        let (a, b) = (content_a.clone(), content_b.clone());
        let writer = std::thread::spawn(move || {
            let backend = LocalStorage::new(writer_dir);
            for i in 0..200 {
                let data = if i % 2 == 0 { &a } else { &b };
                backend.write("segment.bin", data).unwrap();
            }
        });
        let reader = std::thread::spawn(move || {
            let backend = LocalStorage::new(reader_dir);
            for _ in 0..200 {
                let data = backend.read("segment.bin").unwrap();
                assert!(
                    data == content_a || data == content_b,
                    "read a torn/truncated write: {} bytes (expected {} or {})",
                    data.len(),
                    content_a.len(),
                    content_b.len()
                );
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();

        let leftover: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(
            leftover.is_empty(),
            "leftover temp files after atomic_write: {leftover:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod bm25_overrides {
    //! Issue #247: per-request BM25 parameters.
    use super::*;

    fn query(json: &str) -> SearchQuery {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn absent_params_defer_to_the_segment() {
        let q = query(r#"{"query_text": "x"}"#);
        assert_eq!(q.bm25_params, Bm25Overrides::default());
        let seg = Bm25Params { k1: 2.0, b: 0.2 };
        let p = q.bm25_params.resolve(&seg);
        assert_eq!((p.k1, p.b), (2.0, 0.2));
    }

    #[test]
    fn full_and_partial_params_parse() {
        // The pre-#247 wire shape (both keys) still parses unchanged.
        let q = query(r#"{"query_text": "x", "bm25_params": {"k1": 1.2, "b": 0.75}}"#);
        assert_eq!(q.bm25_params, Bm25Overrides::new(1.2, 0.75));
        let q = query(r#"{"query_text": "x", "bm25_params": {"b": 0.0}}"#);
        let p = q.bm25_params.resolve(&Bm25Params::default());
        assert_eq!((p.k1, p.b), (1.2, 0.0));
    }

    #[test]
    fn stats_scope_parses_and_defaults() {
        let q = query(r#"{"query_text": "x"}"#);
        assert_eq!(q.bm25_params.stats, None);
        let q = query(r#"{"query_text": "x", "bm25_params": {"stats": "segment"}}"#);
        assert_eq!(q.bm25_params.stats, Some(Bm25StatsScope::Segment));
        let q = query(r#"{"query_text": "x", "bm25_params": {"stats": "namespace", "b": 0.5}}"#);
        assert_eq!(q.bm25_params.stats, Some(Bm25StatsScope::Namespace));
        assert_eq!(q.bm25_params.b, Some(0.5));
        assert!(serde_json::from_str::<SearchQuery>(
            r#"{"query_text": "x", "bm25_params": {"stats": "shard"}}"#
        )
        .is_err());
        assert_eq!(
            Bm25StatsScope::parse(" Segment "),
            Some(Bm25StatsScope::Segment)
        );
        assert_eq!(Bm25StatsScope::parse("bogus"), None);
        let v = serde_json::to_value(Bm25Overrides {
            stats: Some(Bm25StatsScope::Segment),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(v, serde_json::json!({"stats": "segment"}));
    }

    #[test]
    fn default_overrides_serialize_empty() {
        // Keeps result-cache keys and fan-out payloads small, and an older
        // node reading this falls back to its own 1.2 / 0.75 default.
        let v = serde_json::to_value(Bm25Overrides::default()).unwrap();
        assert_eq!(v, serde_json::json!({}));
    }

    #[test]
    fn ranges_are_validated() {
        for ok in [
            Bm25Overrides::default(),
            Bm25Overrides::new(0.0, 0.0),
            Bm25Overrides::new(0.0, 1.0),
            Bm25Overrides::new(1e6, 0.5),
        ] {
            assert!(ok.validate().is_ok(), "{ok:?}");
        }
        for bad in [
            Bm25Overrides::new(-1.0, 0.5),
            Bm25Overrides::new(f64::NAN, 0.5),
            Bm25Overrides::new(f64::INFINITY, 0.5),
            Bm25Overrides::new(1.2, -0.1),
            Bm25Overrides::new(1.2, 1.5),
            Bm25Overrides {
                k1: None,
                b: Some(f64::NAN),
                stats: None,
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
        let q = query(r#"{"query_text": "x", "bm25_params": {"b": 2}}"#);
        assert!(matches!(q.validate(), Err(KoshaError::InvalidQuery(_))));
    }
}

#[cfg(test)]
mod filter_column_field_tests {
    //! #244: which filter columns a query reads decides which column files
    //! a query node hydrates, so a field it misses would be filtered
    //! against a column that was never fetched.
    use super::*;

    #[test]
    fn filter_column_fields_covers_every_clause_that_reads_columns() {
        let q: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "x",
            "filter": {"bool": {
                "must": [{"term": {"a.keyword": "1"}}],
                "must_not": [{"range": {"b": {"gte": "2"}}}],
                "should": [{"exists": {"field": "c"}}, {"terms": {"d": ["e"]}}]}},
            "knn": {"field": "v", "vector": [1.0], "filter": {"term": {"k": "1"}}},
            "sort": [{"s": {"order": "asc"}}, {"_score": {"order": "desc"}}, {"_id": {"order": "asc"}}],
            "collapse": {"field": "g"},
            "aggs": {
                "t": {"terms": {"field": "h"}},
                "c": {"cardinality": {"field": "i"}},
                "p": {"composite": {"size": 10, "sources": [{"x": {"terms": {"field": "j"}}}]}}
            }
        }))
        .unwrap();
        let got: Vec<String> = q.filter_column_fields().into_iter().collect();
        assert_eq!(
            got,
            [
                "a",
                "a.keyword",
                "b",
                "c",
                "d",
                "g",
                "h",
                "i",
                "j",
                "k",
                "s"
            ]
        );
        let plain: SearchQuery =
            serde_json::from_value(serde_json::json!({"query_text": "x"})).unwrap();
        assert!(plain.filter_column_fields().is_empty());
    }

    /// `limit_per`, `rank_by` and `geo_distance` read filter columns too:
    /// a query node that hydrated only the columns this set names must
    /// still hold them, or they would silently read as absent.
    #[test]
    fn filter_column_fields_covers_limit_per_rank_by_and_geo() {
        let q: SearchQuery = serde_json::from_value(serde_json::json!({
            "query_text": "x",
            "filter": {"geo_distance": {"distance": "10km", "loc": "0,0"}},
            "limit_per": {"field": "author.keyword", "max": 1},
            "rank_by": {"functions": [
                {"field_value": {"field": "votes", "modifier": "log1p"}},
                {"decay": {"field": "published", "function": "exp",
                           "origin": "2026-01-01", "scale": "30d"}}
            ]}
        }))
        .unwrap();
        assert!(q.needs_filter_columns());
        let got: Vec<String> = q.filter_column_fields().into_iter().collect();
        assert_eq!(
            got,
            ["author", "author.keyword", "loc", "published", "votes"]
        );
    }

    #[test]
    fn column_file_names() {
        assert_eq!(filter_column_file_name(7), "filters-007.col");
        assert_eq!(filter_column_file_name(1234), "filters-1234.col");
        for yes in ["filters.toc", "filters-000.col", "filters-1234.col"] {
            assert!(is_filter_column_file(yes), "{yes}");
        }
        for no in [
            "filters.bin",
            "filters-.col",
            "filters-a1.col",
            "filters-001.col.tmp",
        ] {
            assert!(!is_filter_column_file(no), "{no}");
        }
    }
}

#[cfg(test)]
mod aggregation_parsing {
    //! Issues #196 / #218 / #202: aggregations parse by their single type
    //! key, so a misspelled or unsupported type is an error, the existing
    //! JSON of `terms` / `cardinality` / `composite` is unchanged, and only
    //! metric aggregations nest under a bucket aggregation.
    use super::*;
    use std::collections::HashMap;

    fn parse(s: &str) -> Result<Aggregation, serde_json::Error> {
        serde_json::from_str(s)
    }

    #[test]
    fn existing_bucket_aggregations_round_trip_byte_for_byte() {
        for json in [
            r#"{"terms":{"field":"documentId","size":1000}}"#,
            r#"{"cardinality":{"field":"documentId","precision_threshold":40000}}"#,
            r#"{"composite":{"size":2,"sources":[{"label":{"terms":{"field":"color"}}}],"after":{"label":"x"}}}"#,
            r#"{"composite":{"size":2,"sources":[{"label":{"terms":{"field":"color"}}}]}}"#,
        ] {
            let agg = parse(json).unwrap();
            assert_eq!(serde_json::to_string(&agg).unwrap(), json);
        }
        // Defaults still apply.
        let Aggregation::Terms { terms, aggs } = parse(r#"{"terms":{"field":"f"}}"#).unwrap()
        else {
            panic!("expected terms");
        };
        assert_eq!(terms.size, 10);
        assert!(aggs.is_none());
    }

    #[test]
    fn metric_aggregations_parse_and_round_trip() {
        for (json, kind) in [
            (r#"{"min":{"field":"price"}}"#, MetricKind::Min),
            (r#"{"max":{"field":"price"}}"#, MetricKind::Max),
            (r#"{"sum":{"field":"price"}}"#, MetricKind::Sum),
            (r#"{"avg":{"field":"price"}}"#, MetricKind::Avg),
            (
                r#"{"value_count":{"field":"price"}}"#,
                MetricKind::ValueCount,
            ),
        ] {
            let agg = parse(json).unwrap();
            let (parsed_kind, metric) = agg.metric().unwrap();
            assert_eq!(parsed_kind, kind);
            assert_eq!(metric.field, "price");
            assert_eq!(agg.type_name(), kind.name());
            assert_eq!(serde_json::to_string(&agg).unwrap(), json);
        }
    }

    #[test]
    fn unknown_or_misspelled_types_are_rejected_with_the_valid_list() {
        for (json, needle) in [
            (
                r#"{"stats":{"field":"price"}}"#,
                "unknown aggregation type `stats`",
            ),
            (r#"{"average":{"field":"price"}}"#, "did you mean `avg`"),
            (
                r#"{"term":{"field":"price"}}"#,
                "unknown aggregation type `term`",
            ),
            (r#"{}"#, "aggregation has no type"),
            (r#"[]"#, "must be a JSON object"),
        ] {
            let err = parse(json).unwrap_err().to_string();
            assert!(err.contains(needle), "{json}: {err}");
            if needle.starts_with("unknown") || needle.starts_with("did") {
                assert!(err.contains("value_count"), "{json}: {err}");
            }
        }
        let err = parse(r#"{"min":{"field":"a"},"max":{"field":"a"}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exactly one type"), "{err}");
        let err = parse(r#"{"min":{"field":"a","missing":0}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("in `min` aggregation"), "{err}");
    }

    #[test]
    fn metric_sub_aggregations_nest_under_terms_and_composite() {
        let agg =
            parse(r#"{"terms":{"field":"category"},"aggs":{"total":{"sum":{"field":"amount"}}}}"#)
                .unwrap();
        let subs = agg.sub_aggregations().unwrap();
        assert_eq!(subs["total"].metric().unwrap().0, MetricKind::Sum);
        let json = serde_json::to_value(&agg).unwrap();
        assert_eq!(json["aggs"]["total"]["sum"]["field"], "amount");

        let agg = parse(
            r#"{"composite":{"size":5,"sources":[{"c":{"terms":{"field":"category"}}}]},
                "aggregations":{"avg_amount":{"avg":{"field":"amount"}}}}"#,
        )
        .unwrap();
        assert!(agg.sub_aggregations().unwrap().contains_key("avg_amount"));
    }

    #[test]
    fn nested_bucket_aggregations_and_misplaced_aggs_are_rejected() {
        let err = parse(r#"{"terms":{"field":"a"},"aggs":{"inner":{"terms":{"field":"b"}}}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("only metric aggregations"), "{err}");
        let err = parse(r#"{"min":{"field":"a"},"aggs":{"x":{"max":{"field":"b"}}}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot have sub-aggregations"), "{err}");
        let err = parse(r#"{"terms":{"field":"a"},"aggs":{},"aggregations":{}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("both `aggs` and `aggregations`"), "{err}");
        let err = parse(r#"{"terms":{"field":"a"},"aggs":{"x":{"bogus":{}}}}"#)
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown aggregation type `bogus`"), "{err}");
    }

    #[test]
    fn search_query_validate_rejects_nested_buckets_built_in_code() {
        let mut query: SearchQuery = serde_json::from_str(r#"{"query_text":""}"#).unwrap();
        query.aggs.insert(
            "outer".into(),
            Aggregation::Terms {
                terms: AggTerms {
                    field: "a".into(),
                    size: 10,
                },
                aggs: Some(HashMap::from([(
                    "inner".into(),
                    Aggregation::Cardinality {
                        cardinality: AggCardinality {
                            field: "b".into(),
                            precision_threshold: 10,
                        },
                    },
                )])),
            },
        );
        assert!(query.validate().is_err());
    }

    #[test]
    fn bucket_sub_results_serialize_inline_and_round_trip() {
        let bucket = AggBucket {
            key: "k".into(),
            doc_count: 2,
            sub_aggregations: std::collections::BTreeMap::from([(
                "total".to_string(),
                serde_json::json!({"value": 3.5}),
            )]),
        };
        let json = serde_json::to_value(&bucket).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"key":"k","doc_count":2,"total":{"value":3.5}})
        );
        let back: AggBucket = serde_json::from_value(json).unwrap();
        assert_eq!(back.sub_aggregations["total"]["value"], 3.5);
        // No sub-aggregations: the historical shape exactly.
        let plain = AggBucket {
            key: "k".into(),
            doc_count: 2,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&plain).unwrap(),
            r#"{"key":"k","doc_count":2}"#
        );
    }
}
