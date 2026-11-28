# Authorization and durability

API keys, operator credentials, and the S3 write-ahead log. Moved from the README.

## Authorization and durability

All client namespace routes, including legacy `/index`, `/search`, `/flush`,
`/replace`, `/exists`, and `/delete`, resolve an **unqualified namespace name**
inside the authenticated tenant. Client-supplied paths and qualified names are
rejected. Legacy callers that previously accessed bare stored namespaces need
an explicit migration to tenant-scoped namespaces; this change does not rename
existing data automatically.

A namespace or branch name is a single path component: non-empty, not `.` or
`..`, and free of `/`, `\` and control characters. Names of the exact form
`_shard-NNNN` (four digits) are reserved, because `<tenant>/_shard-0001` is the
storage layout of a shard of the bare namespace `<tenant>`. Other names starting
with `_` or `.` are allowed. New API keys (`POST /v1/admin/api-keys`) refuse a
tenant id starting with `_` or `.`, since namespaces under it would be accounted
to the shared `_legacy` cache tenant; such ids already configured keep working,
and the server logs a warning at startup for any in `KOSHA_API_KEYS`.

- Set `KOSHA_ADMIN_API_KEY` for `/v1/admin/*` and global `/stats` access. With
  a Postgres control plane, give each operator their own credential instead and
  keep this one for bootstrap and break-glass; see
  [Operator admin credentials](#operator-admin-credentials).
- Organizations, users with roles (owner, admin, member, read-only), projects,
  the tenants they own and an audit log of control-plane actions are managed
  under `/v1/admin/users` and `/v1/admin/orgs`; see
  [docs/organizations.md](../organizations.md). Existing tenants and keys need
  no migration.
- `KOSHA_CONSOLE=enabled` lets users sign up (with `KOSHA_SIGNUP=enabled`),
  sign in and manage their organizations and keys themselves at `/console`;
  set `KOSHA_CONSOLE_PUBLIC_URL`, a real mailer (`KOSHA_MAILER=smtp`) and, for
  organization SSO, `KOSHA_CONSOLE_SECRET_KEY`. See
  [docs/organizations.md](../organizations.md#self-service-console) and
  "Self-service console" in [docs/configuration.md](../configuration.md).
- Set a separate `KOSHA_INTERNAL_API_KEY` on every peer participating in segment
  hydration. Tenant API keys do not authorize admin or peer-file access.
- Both service capabilities are disabled when their respective secret is unset.
  Ordinary API-key configuration remains `KOSHA_API_KEY` / `KOSHA_API_KEYS` or
  the Postgres key registry. Namespace-scoped stats remain available to tenants.
- The server refuses to start when no credential is configured at all, unless
  `KOSHA_AUTH=disabled` is set explicitly (local development only). Postgres keys
  are re-read every `KOSHA_API_KEY_REFRESH_SECS` (default 30), request bodies are
  capped at `KOSHA_MAX_BODY_BYTES` (default 64 MiB, 413 above it), and
  the deprecated unversioned routes return 410 by default.
  `KOSHA_LEGACY_ROUTES=on` opts in all tenants, or
  `KOSHA_LEGACY_ROUTES_TENANTS` opts in named tenants (see
  [docs/api/versioning.md](../api/versioning.md#legacy-routes)).

### Scoped API keys (Postgres key registry)

Keys in the Postgres registry carry scopes, an optional namespace allow-list
and an optional expiry. The server enforces them on every tenant route, before
a query pod forwards a write to the ingest pod:

| Scope | Routes |
|---|---|
| `read` | search, exists, namespace stats, reading a schema, listing namespaces and branches, the tenant's own usage (`GET /v1/usage`) and billing state (`GET /v1/billing`) |
| `write` | index, replace, delete documents, flush |
| `admin` | create and delete namespaces and branches, change a namespace's schema or shards, manage the tenant's own keys (`/v1/api-keys`, below), start a billing checkout or open the billing portal (`POST /v1/billing/checkout`, `POST /v1/billing/portal`) |

Scopes are independent (`admin` does not include `write`). A request outside a
key's scopes or namespaces gets a `403` naming what is missing; an expired key
gets `401` "API key expired". The `admin` scope is tenant-level: `/v1/admin/*`
still needs an admin credential. Every key created before scopes existed has
all three scopes, every namespace and no expiry, so it behaves as before. A key
restricted to namespaces sees only those namespaces in `GET /v1/namespaces`,
cannot use the routes that answer for the whole tenant (`GET /v1/usage`,
`GET /v1/billing`), and can only create a namespace or branch whose name is on
its list.

Env keys can be narrowed the same way (#346). A `KOSHA_API_KEYS` entry takes
`;`-separated attributes after its tenant:

```
KOSHA_API_KEYS="sk-app=acme,sk-ci=acme;scopes=read+write;namespaces=docs+wiki;expires_at=1798761600"
```

`scopes` and `namespaces` are `+`-separated, `expires_at` is unix seconds. An
entry without attributes (`sk-app=acme`, and `KOSHA_API_KEY`) has every scope,
every namespace and no expiry, exactly as before. A malformed attribute stops
the server at startup with an error that names the entry, never the key.

Manage keys with the admin credential (`kosha admin ...` wraps these):

| Route | Does |
|---|---|
| `POST /v1/admin/api-keys` | Create a key: `{"tenant_id", "description", "scopes": ["read"], "namespaces": ["docs"], "expires_in_secs": 86400}`. Only `tenant_id` is required. The key is in the response and is never shown again |
| `GET /v1/admin/api-keys[?tenant_id=T]` | List keys that are not revoked: id, tenant, scopes, namespaces, `expires_at`, `last_used_at` (unix seconds). No secrets |
| `POST /v1/admin/api-keys/revoke` | `{"key_id"}`. The pod that answers refuses the key at once, the others within `KOSHA_API_KEY_REFRESH_SECS` |
| `POST /v1/admin/api-keys/rotate` | `{"key_id", "overlap_secs": 86400}`. Issues a replacement with the same tenant, scopes and namespaces; the old key keeps working for the overlap (default one day), so clients switch without downtime |
| `POST /v1/admin/api-keys/scrub-plaintext` | Removes, now, the plaintext that keys created before this feature still hold in the database (the server also does it by itself; see **Upgrading** below) |
| `GET /v1/admin/api-keys/plaintext-count` | Read-only: `{"plaintext_rows": N, "scrub_due": true}`, the rows `scrub-plaintext` would rewrite. The server logs the state at startup (`api keys: N row(s) still hold a plaintext key; the automatic scrub removes it at unix time T`) |

Keys are stored as SHA-256 hashes; a key's `key_id` is that hash. Creating,
rotating and revoking a key are recorded in the audit log, in the same
transaction as the change, naming the admin credential that did it
(`operator` or `operator:<name>`); see
[docs/organizations.md](../organizations.md#audit-log). `last_used_at` is
written about once per refresh interval, not per request.

A revocation reaches the other pods only on their next successful key
refresh. A pod that cannot reach Postgres keeps the keys it last loaded (and
logs `api keys: refresh failed, keeping previous snapshot`), so a tenant key
revoked during the outage keeps working on that pod until a refresh succeeds.
Operator credentials are different: after ten failed intervals the pod
refuses all of them until it can refresh.

**A tenant managing its own keys.** A key with the `admin` scope can list,
create, rotate and revoke its own tenant's registry keys without an operator
(`kosha keys ...` wraps these; Postgres control plane only):

| Route | Does |
|---|---|
| `GET /v1/api-keys` | The tenant's keys the calling key may manage, as in the admin listing, plus `current` for the calling key itself |
| `POST /v1/api-keys` | `{"description", "scopes", "namespaces", "expires_in_secs"}`, all optional. Left out, the scopes, namespaces and expiry are the calling key's own. The key is in the response and is never shown again |
| `POST /v1/api-keys/rotate` | `{"key_id", "overlap_secs", "expires_in_secs"}`, as the admin route |
| `POST /v1/api-keys/revoke` | `{"key_id"}` |

A key never grants more than it holds: what it creates has a subset of its
scopes and namespaces and expires no later than it does (asking for more is a
`403`), and it can see, rotate or revoke only keys of its own tenant that it
could itself have created (any other `key_id` is a `404`). A rotation's
replacement is bounded by the calling key's expiry too. Env keys are never
listed or changed here. Every change is in the audit log with the calling key
as the actor (`api_key:<first twelve characters of its id>`). The admin
credential gets `403` on these routes; operators use `/v1/admin/api-keys`.

### Operator admin credentials

`KOSHA_ADMIN_API_KEY` is one secret shared by everyone who administers the
deployment: it cannot expire, cannot be taken from one person, and the audit log
can only say `operator` did something. With a Postgres control plane, each
operator can hold a named credential instead. It opens exactly what the shared
key opens (`/v1/admin/*`, global `/stats`, `/metrics` under
`KOSHA_METRICS_AUTH=admin`), is sent the same way (`Authorization: Bearer` or
`X-Api-Key`), and is recorded in the
[audit log](../organizations.md#audit-log) as `operator:<name>`. When it is
used to act for a user (`X-Kosha-Actor`), the entry's actor is that user and
its detail carries `"via": "operator:<name>"`.

| Route | Does |
|---|---|
| `POST /v1/admin/operators` | Issue a credential: `{"name": "alice", "description", "expires_in_secs": 7776000}`. Only `name` is required (1 to 64 of letters, digits, `. _ - @ +`). The credential is in the response and is never shown again. `409` if that operator already holds a usable one: rotate it |
| `GET /v1/admin/operators` | List credentials that are not revoked: id, name, `expires_at`, `last_used_at`. No secrets |
| `POST /v1/admin/operators/revoke` | `{"key_id"}` revokes one credential, `{"name"}` every credential of that operator. The pod that answers refuses them at once, the others within `KOSHA_API_KEY_REFRESH_SECS` (a pod that cannot reload its keys for ten intervals refuses every operator credential until it can). `"force": true` overrides the lock-out guard |
| `POST /v1/admin/operators/rotate` | `{"key_id", "overlap_secs": 86400}`. Issues a replacement for the same operator; the old credential keeps working for the overlap (default one day). `"force": true` overrides the lock-out guard |

`kosha admin create-operator --name alice`, `list-operators`,
`rotate-operator <key_id>`, `revoke-operator <key_id>` (or `--name alice`) and
`api-key-plaintext-count` wrap these.

Every other admin request that changes state (segment GC, compaction, imports
and repairs, tenant deletion, billing and usage administration) is written to
the audit log too, as `admin.*` with the method, path and response status and
the credential that made it; see
[docs/organizations.md](../organizations.md#audit-log).

Operator credentials are stored in the key registry like tenant keys (hashed,
same expiry, rotation and refresh), but the two never mix: an operator
credential is not a key for any tenant, a tenant key (whatever its scopes) is
not an admin credential, and the `/v1/admin/api-keys` routes neither list nor
change operator credentials. Creating, rotating and revoking one is written to
`kosha.audit_log` (`operator_credential.created` / `.rotated` / `.revoked`) with
whoever did it, in the same transaction as the change.

**Moving off the shared key.**

1. With `KOSHA_ADMIN_API_KEY`, issue a credential for each operator
   (`POST /v1/admin/operators`), and check each one works.
2. Do the same for everything that is not a person but sends the shared key,
   and switch each to its own credential. In this repository that is the
   segment-GC CronJob (`k8s/base/cronjob-segment-gc.yaml`, which reads the
   `admin-api-key` entry of `kosha-secret`: issue a credential named, say,
   `segment-gc` and store it under that entry), plus whatever you run
   yourself with the key: `kosha admin ...` and `scripts/create_api_key.py`
   in CI or runbooks, scheduled compaction, a metrics scraper under
   `KOSHA_METRICS_AUTH=admin`. A credential issued with `expires_in_secs`
   stops such a job when it expires, so rotate it before then or issue it
   without an expiry.
3. Set `KOSHA_ADMIN_BOOTSTRAP_KEY=disabled` on every pod and restart. The shared
   key is now refused like any unknown key (`401`), for people and for
   automation alike: a job still sending it fails. It can stay in the pods'
   `KOSHA_ADMIN_API_KEY` for step 4.
4. To break glass (every operator credential lost, expired or revoked), set
   `KOSHA_ADMIN_BOOTSTRAP_KEY=enabled` (or remove it) and restart: the shared key
   works again. Rotate it afterwards if it was exposed.

Leaving the variable unset keeps today's behaviour: the shared key and operator
credentials both work. The server refuses to start on any other value.

**Lock-out guard.** While the shared key is off (`KOSHA_ADMIN_BOOTSTRAP_KEY=disabled`,
or `KOSHA_ADMIN_API_KEY` unset), the server refuses with `409` a revoke or
rotate that would leave no usable operator credential, or that makes the last
one lapse within a day (rotating it into a short-lived one, say). Nothing
changes; issue another credential first. To do it anyway, repeat the request
with `"force": true` (`--force` on the CLI), knowing that step 4 is then the
way back in. Credentials that simply reach their expiry are not an API call
the server can refuse, so with the shared key disabled it warns at startup and
about hourly when none is usable or the last one expires within a week.
During a rolling
upgrade, pods still on a build without this feature refuse operator credentials
(as an admin credential and as a tenant key), so issue them once every pod is
upgraded. `/internal/*` and `KOSHA_INTERNAL_API_KEY` are unaffected.

**Upgrading.** The migration (`003_scoped_api_keys.sql`, also applied by the
server at startup) only adds columns and hashes existing keys; it leaves their
plaintext in place, because pods still running a build from before hashed keys
read it. Until every pod is upgraded, a key created through an upgraded pod is
not accepted by the pods still on the old build.

The plaintext is then removed without an operator (`009_api_key_scrub.sql`,
`KOSHA_API_KEY_AUTO_SCRUB`). Every server with Postgres runs a scrub round on
each key refresh, and it rewrites the plaintext rows
`KOSHA_API_KEY_SCRUB_GRACE_SECS` (default one day) after the database last
showed a sign of an old build: the first start of a server with the automatic
scrub (recorded by the server, so applying migrations ahead of a deploy starts
no clock), or a key row only an old build inserts (one without a hash), which
restarts the clock. One pod does it per round, and running it again changes
nothing. The server logs the state at startup and when it changes. The upgrade
path:

1. Roll out the new build as usual. Old and new pods both accept every key.
2. Nothing else. A day after the first new pod started (or after the last old
   pod created a key), the plaintext is gone; the startup line says
   `api keys: no row holds a plaintext key`, and
   `GET /v1/admin/api-keys/plaintext-count` answers `0`.

Upgrading from a build that already has hashed keys (but no automatic scrub)
is the same: such builds read keys by hash, so the scrub cannot lock them out.
If a rollout of a build from before hashed keys could take longer than a day,
raise `KOSHA_API_KEY_SCRUB_GRACE_SECS`, or set `KOSHA_API_KEY_AUTO_SCRUB=off`
and run `kosha admin scrub-plaintext-api-keys` yourself once every pod is
upgraded. After the scrub the table holds no usable key, and a rollback to a
build from before hashed keys would no longer accept Postgres keys (env keys
are unaffected); a rollback to any build with hashed keys is unaffected. The
server's database role must own `kosha.api_keys` and `kosha.api_key_scrub`
(`002_grant_kosha_role.sql`) or an admin must apply the migrations first
(`make db-migrate`).

**Upgrading an instance that was set up with `make db-migrate`.** Tables created
by the database admin belong to the admin until `002_grant_kosha_role.sql` hands
them to the role the server connects as, and the migrations the server applies
at startup cannot change that. Before this release 002 did not hand over
`kosha.hydration_leases`, so on such an instance hydration-lease claims and
namespace or tenant deletion fail with `permission denied for table
hydration_leases`, and rolling out a new server image does not fix it. Re-run
002 once as the database admin: `make db-migrate` (every step is safe to
re-run), or that one file with `psql` against the `kosha` database.
An instance where the server's role created the tables itself (no
`make db-migrate`) is not affected. The server checks at startup and logs
`WARNING: the database role this server connects as cannot read and write
kosha.<table> ...` naming each table or sequence of schema `kosha` it cannot
use; it still starts.

A successful flush uploads its segment snapshot before publishing its manifest.
Upload and manifest-store failures return errors and remain retryable. By
default (`KOSHA_WAL_MODE=local`) buffered writes use a local, fsynced WAL under
`data_dir/_wal`; they are not S3-durable until a successful flush, so losing the
node (not just the pod) loses acknowledged, unflushed writes.
Concurrent appends are group-committed: one WAL file and one fsync cover every
record queued while the previous commit ran, and each caller is acknowledged
only after the fsync covering its own record. Once the previous group held more
than one record, the committer waits up to `KOSHA_WAL_GROUP_COMMIT_MS` (default
2; `0` disables the wait) for more, and cuts a group at
`KOSHA_WAL_GROUP_COMMIT_BYTES` (default 4 MiB). A lone writer never waits.
Every record has its own log sequence number. WAL records are reclaimed only
through the published namespace checkpoint: a file shared by several namespaces
is deleted once each of them has a published checkpoint past its records there,
so a flush never removes later writes or another namespace's records. Partial
parallel flush failures restore the entire snapshot for retry.

Publication uses compare-and-swap against the writer's last committed manifest
version. Concurrent writers cannot replace each other's snapshots. A stale
writer reads the newer manifest back (resolving its deletion sidecars), rebases
its unpublished delta onto it and retries the CAS, up to 3 times with jittered
backoff. The delta is the segments it added, the segments it compacted away and
its new deletes; deletes on a segment both sides list are merged, and the
published WAL checkpoint stays the rebasing writer's own. A writer with nothing
unpublished just adopts the newer manifest. The rebase aborts, leaving local
state and the WAL untouched, when merging would lose data: both writers
compacted the same segment, the other writer deleted documents in a segment this
writer compacted away, or the other writer compacted away a segment that carries
this writer's unpublished deletes. An unresolved conflict returns HTTP 409.
Restarting the writer with its existing data directory loads the published
manifest and replays its pending writes from the WAL. Retrying an identical
committed snapshot is safe after a lost commit response, and a rebase
recognizes such a commit as its own. Segment object keys include a random
writer identifier to avoid collisions before publication. Migration
(`kosha-server migrate`) and `POST /v1/admin/import-namespace` also publish by
CAS against the version they read. They never rebase, and they fail instead of
overwriting a manifest another writer committed (409 for import).

### S3 WAL (`KOSHA_WAL_MODE=s3`)

With `KOSHA_WAL_MODE=s3` (needs the `s3` feature and `KOSHA_S3_BUCKET`), an
ingest node acknowledges `/index`, `/replace` and `/delete` only after the
group commit holding the write is stored in the segment bucket, so an
acknowledged write survives losing the node and its disk. Each group commit is
one object, `{KOSHA_S3_PREFIX}/_wal/{KOSHA_WAL_WRITER_ID}/{lsn:020}.bin`, with
the same checksummed v3 frames as the local WAL, created with a conditional
`PutObject` (`If-None-Match: *`). The stream's `meta/origin` and
`meta/sequence` objects sit next to it.

- **Writer id.** `KOSHA_WAL_WRITER_ID` (default `default`) names the stream.
  Every ingest process running at the same time needs its own id; a
  replacement for a lost node must reuse the lost node's id. Set the mode
  only where writes land: a node with `KOSHA_INGEST_HOST` forwards writes and
  keeps a local WAL. The stock
  single-replica, `Recreate` ingest Deployment can keep the default.
- **LSNs** are allocated per writer stream (per `wal_origin`), not per
  namespace: one object holds every namespace's records that arrived during
  the previous PUT. A manifest's `wal_checkpoint` is still the highest LSN of
  its `wal_origin` that its segments contain. The origin lives in the bucket,
  so a replacement node recognizes its predecessor's checkpoints.
- **One writer per stream.** Group `n + 1` is keyed by the LSN after group
  `n`'s last record. Two processes appending to one stream race for the same
  key; exactly one PUT succeeds. The loser fails that write (HTTP 500),
  rejects every later write, and must be restarted, which re-reads the stream.
  A PUT that timed out but landed is recognized by its bytes on the next PUT
  and kept (its records replay; they were never acknowledged). At startup the
  server checks that the store rejects a second conditional PUT of one key and
  refuses to start otherwise. MinIO and AWS S3 both support `If-None-Match`
  on `PutObject`. Conditional PUTs do not fence a writer that stays paused
  until the object it would collide with has been reclaimed; run one ingest
  process per writer id (the default Deployment's `Recreate` strategy does).
- **Timeouts.** Every WAL S3 call has a deadline of `KOSHA_WAL_S3_TIMEOUT_MS`
  (default 10000). A failed or timed-out PUT fails the write with a 500 and
  nothing is buffered.
- **Recovery.** On startup the node lists the stream, replays every record
  above each namespace's published checkpoint into its write buffers (the
  same path as a local restart) and continues after the highest LSN. A flush
  then builds segments as usual and publishes the manifest by CAS with the new
  `wal_checkpoint`; objects at or below it are deleted after the publication
  is acknowledged, as local files are.
- **Latency trade-off.** An acknowledgement now waits for an S3 PUT instead of
  an fsync. Writes to one namespace are serialized, so a single namespace
  writer gets one PUT round trip per write; concurrent writes to different
  namespaces share a PUT. Groups are written one at a time, so throughput is
  bounded by one PUT stream. `cargo bench -p kosha-write --bench s3_wal_ack`
  (500 kB writes, simulated PUT of 25 ms + 100 MB/s; not real S3):

  | writers | window ms | writes/s | MB/s | p50 ms | p99 ms | PUTs/write |
  |--------:|----------:|---------:|-----:|-------:|-------:|-----------:|
  | 1  | 0 | 26.4  | 13.6 | 38.1  | 38.5  | 1.00 |
  | 4  | 0 | 50.6  | 26.1 | 79.3  | 82.4  | 0.51 |
  | 4  | 2 | 55.6  | 28.6 | 78.9  | 95.2  | 0.41 |
  | 16 | 0 | 108.7 | 55.9 | 143.7 | 177.4 | 0.14 |

  turbopuffer reports p50 ≈ 165 ms for 500 kB writes. Measure on real S3
  before relying on these numbers.
- **Switching local → s3.** On the first start in s3 mode, a non-empty local
  `data_dir/_wal` is copied into the stream with its origin, sequence and
  file keys, so existing manifest checkpoints keep their meaning, and is then
  renamed to `_wal.migrated-{unix seconds}`. If the stream already belongs to
  another origin the node refuses to start: flush every namespace in one mode
  first.
- **Switching s3 → local.** Local mode does not read the S3 stream. Stop
  writes, flush every namespace (`POST /flush`) so the checkpoint covers every
  record, and check that `_wal/{writer id}/` holds no `.bin` objects before
  restarting with `KOSHA_WAL_MODE=local`. The local WAL then starts a new
  origin.

Each WAL directory has a persistent origin identifier. New manifest checkpoints
apply only to that origin, so another writer's checkpoint cannot discard local
pending writes during recovery. Preserve the entire WAL directory, including
its `origin` file, across restarts. Legacy checkpoints without an origin retain
their previous recovery behavior; upgrade all writers before concurrent use.

Deletes include buffered documents and WAL-log resolved document IDs before
applying them. New WAL files hold one or more checksummed v3 frames (the CRC
covers the record's sequence number and payload); vectors are stored as binary
f32 rather than JSON text. Recovery keeps every complete frame of a torn final
file, discards only the incomplete tail, and rejects corrupt records. v1 and v2
files remain readable.
Deletes and writes replay in order, so reinserting a deleted ID remains valid.

Deletion sets are immutable `deletes-{sha256}.roaring` sidecars referenced by the
manifest. Publication uploads these sidecars before CAS, including new generations
for segments uploaded earlier. Query pods load and validate the referenced
bitmaps before installing refreshed manifests; a missing or corrupt bitmap fails
the request. Result caching includes deletion generations, and WAND filters
removed documents before counting or scoring them.

Upserts retain old segments, mark prior copies deleted, and write only incoming
documents into a new segment. Partial replacements read the affected document
spans and merge fields. Segment I/O runs outside the namespace state lock; the
new segment and deletion markers become visible together. Compaction reclaims
these dead copies later. Superseded files remain available to older snapshots;
S3 garbage collection is still deferred.

**Upgrade note:** Postgres `segments_json` now stores the complete manifest
object, including tombstones and checkpoints. New servers can read the old entry
arrays. Old servers cannot read the new object format: upgrade ingest and query
roles together before resuming writes, and do not roll back readers without a
manifest-format migration. Servers predating deletion-vector support ignore
those references, and older WAL readers cannot replay v2 or v3 frames: upgrade
all roles together before accepting writes and preserve the WAL directory. Rolling
back requires converting both formats. The SQL column needs no schema change.
