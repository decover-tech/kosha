# Organizations, users, projects and the audit log

A tenant used to be only a string on an API key. The control plane now has a
model above it (#345):

```text
organization ──< membership (owner | admin | member | read-only) >── user
     │
     ├──< project (optional grouping)
     │
     └──< tenant ──< API keys, `{tenant}/...` namespaces
```

This is the data model and its admin API. Sign-up, email verification, OAuth,
SAML/OIDC and the web console are not built yet; they will sit in front of it.

## What does not change

- Request authentication is untouched: an API key still maps to its tenant id,
  and searching and indexing never read the tables below.
- A tenant that no organization owns (every tenant that existed before this)
  keeps working. Nothing has to be migrated. `POST /v1/admin/api-keys` still
  creates a key for any tenant id.
- An organization only gains control of an existing tenant when the operator
  links it (below).

## Who is calling

Every route is under `/v1/admin/` and needs an admin credential:
`KOSHA_ADMIN_API_KEY`, or a named
[operator credential](../README.md#operator-admin-credentials). A tenant API
key is never enough. The holder of the admin credential is then either:

- **the operator**: no `X-Kosha-Actor` header. Not bound by roles. Audited as
  `operator` with the shared key, as `operator:<name>` with an operator
  credential.
- **acting for a user**: `X-Kosha-Actor: <user id>`. The server enforces that
  user's role in the organization. Audited as `user:<id>`; when the request
  was made with an operator credential, the entry's detail also has
  `"via": "operator:<name>"`, so naming a user never hides which credential
  acted. The shared key has no name, and adds nothing.

`X-Kosha-Actor` is only understood by the routes on this page. Every other
`/v1/admin/*` route (`api-keys`, `gc`, compaction, ...) is operator-only and
answers 403 when the header is present, so a request made for a user can never
run with the operator's powers.

The server does not authenticate users. The admin credential's holder (today an
operator's script, later the console's backend) is trusted to have done so
before it names a user. Never hand the admin credential to an end user.

## Roles

| | read-only | member | admin | owner |
|---|---|---|---|---|
| Read the organization, members, projects, tenants | yes | yes | yes | yes |
| Create an API key for one of its tenants | | yes | yes | yes |
| Create projects and tenants | | | yes | yes |
| Add, remove and change members below owner | | | yes | yes |
| Read the audit log | | | yes | yes |
| Grant, change or revoke the owner role | | | | yes |

- An organization always has at least one owner: the last one cannot be
  removed or demoted, by anyone, the operator included.
- Any member can remove themselves.
- Only the operator creates users, and only the operator links a tenant id of
  its choosing. A user who creates a tenant gets a generated id
  (`{org-slug}-{12 hex}`), so nobody can claim someone else's tenant and mint
  keys for it.

## API

Bodies and responses are JSON. Errors are `{"error": "..."}` with 400 (malformed),
403 (role), 404 (no such thing), 409 (duplicate, or the last owner).

| Route | Body | Who |
|---|---|---|
| `POST /v1/admin/users` | `{email, display_name?}` | operator |
| `GET /v1/admin/users/{user}` | | operator, or the user |
| `POST /v1/admin/orgs` | `{slug, name, owner_user_id?}` | a user (becomes owner), or the operator naming the owner |
| `GET /v1/admin/orgs/{org}` | | any member |
| `GET /v1/admin/orgs/{org}/members` | | any member |
| `POST /v1/admin/orgs/{org}/members` | `{user_id, role}` | admin, owner |
| `POST /v1/admin/orgs/{org}/members/{user}/role` | `{role}` | admin, owner |
| `DELETE /v1/admin/orgs/{org}/members/{user}` | | admin, owner, or the member themselves |
| `GET /v1/admin/orgs/{org}/projects` | | any member |
| `POST /v1/admin/orgs/{org}/projects` | `{slug, name}` | admin, owner |
| `GET /v1/admin/orgs/{org}/tenants` | | any member |
| `POST /v1/admin/orgs/{org}/tenants` | `{project_id?}`; operator only: `{tenant_id}` to link an existing tenant | admin, owner |
| `POST /v1/admin/orgs/{org}/tenants/{tenant}/api-keys` | `{description?}` | member, admin, owner |
| `GET /v1/admin/orgs/{org}/audit-log?limit=&before=` | | admin, owner |

Slugs are 1-63 characters of `a-z`, `0-9` and `-`. Organization slugs and user
emails are unique; project slugs are unique within their organization. Lists
return at most 1000 rows.

From nothing to an indexed document:

```sh
A="authorization: Bearer $KOSHA_ADMIN_API_KEY"
USER=$(curl -s -X POST "$KOSHA/v1/admin/users" -H "$A" -d '{"email":"ada@example.com"}' | jq -r .id)
ORG=$(curl -s -X POST "$KOSHA/v1/admin/orgs" -H "$A" -H "x-kosha-actor: $USER" \
        -d '{"slug":"acme","name":"Acme"}' | jq -r .id)
TENANT=$(curl -s -X POST "$KOSHA/v1/admin/orgs/$ORG/tenants" -H "$A" -H "x-kosha-actor: $USER" | jq -r .tenant_id)
KEY=$(curl -s -X POST "$KOSHA/v1/admin/orgs/$ORG/tenants/$TENANT/api-keys" -H "$A" -H "x-kosha-actor: $USER" | jq -r .api_key)
curl -X POST "$KOSHA/v1/namespaces/docs/documents" -H "authorization: Bearer $KEY" \
  -d '{"documents":[{"id":"1","fields":[{"name":"title","field_type":"Text","value":"hello"}]}]}'
```

A new key works at once on the pod that created it and within
`KOSHA_API_KEY_REFRESH_SECS` (default 30) on the others.

## Audit log

Each change is recorded in the same transaction that makes it, so there is
never a change without its entry or an entry for a change that did not happen.
Refused requests are not recorded.

| Action | Target | Detail |
|---|---|---|
| `user.created` | user | `email` |
| `org.created` | organization | `slug`, `owner_user_id` |
| `member.added` | user | `role` |
| `member.role_changed` | user | `from`, `to` |
| `member.removed` | user | `role` |
| `project.created` | project | `slug` |
| `tenant.linked` | tenant | `project_id`, `existing_tenant` |
| `api_key.created` | tenant | `key_suffix` (last four characters), `description`. A key made with `POST /v1/admin/api-keys` also has `key_id_prefix` (first twelve characters of its `key_id`), `scopes`, `namespaces` and `expires_at` |
| `api_key.rotated` | tenant | The replacement key, as for `api_key.created`, and `replaces`: the old key's `key_id_prefix` and the `expires_at` it now has |
| `api_key.revoked` | tenant | `key_id_prefix`, `description` |

An entry has `id`, `org_id`, `actor` (`operator`, `operator:<name>` or
`user:<id>`), `action`,
`target_type`, `target_id`, `detail` and `created_at_ms`. Any entry by a user
may also carry `via` in its detail (see [Who is calling](#who-is-calling)). The log never holds an
API key. `GET .../audit-log` returns one organization's entries newest first,
100 per page by default (`limit`, at most 1000); pass the response's
`next_before` as `before` for the next page. Keys created, rotated
(`/rotate`) or revoked (`/revoke`) through `POST /v1/admin/api-keys` are
recorded too, with the operator as the actor, under the tenant's organization
when it has one. `user.created`, key entries for a tenant no organization
owns, and changes to operator credentials (`operator_credential.created`,
`.rotated`, `.revoked`; target type `operator`, target id the operator's name)
are recorded with no organization and are only visible in the
`kosha.audit_log` table. An entry never holds a key; a tenant-key entry never
holds a whole `key_id` either, while an operator-credential entry names the
credential by its full `key_id`.

Not recorded yet: namespace deletion and other data-plane actions made with a
tenant API key, the keys removed when a tenant is deleted, and
`POST /v1/admin/api-keys/scrub-plaintext`.

## Storage

With Postgres (`DATABASE_URL` and the `postgres` feature) the model lives in
`kosha.users`, `kosha.organizations`, `kosha.org_members`, `kosha.projects`,
`kosha.tenants` and `kosha.audit_log`, created by
`crates/kosha-control/migrations/006_orgs_users_projects.sql`. The migration
only adds tables; the server applies it at startup, and `make db-migrate`
applies it too.

Without Postgres the model is kept in the server's memory and is lost on
restart, like the rest of the in-memory control plane. Use that for local
development only.
