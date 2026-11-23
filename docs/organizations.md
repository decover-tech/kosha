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

On top of it sit two ways in:

- the **admin API** (`/v1/admin/users`, `/v1/admin/orgs`) for operators and
  their scripts, described from [Who is calling](#who-is-calling) on;
- the **self-service console** (`/v1/console/*` and the web UI at `/console`),
  where an outside user signs up, verifies their address, signs in (password,
  Google, GitHub or their organization's OIDC single sign-on), creates an
  organization, project and tenant, and gets an API key, without anyone at
  Decover touching a database. See [Self-service console](#self-service-console).

SAML is not supported; it is deferred to a follow-up issue. Organizations
that need SSO today use OpenID Connect, which every major identity provider
(Okta, Microsoft Entra ID, Google Workspace, Auth0, Keycloak) offers.

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

On these admin routes the server does not authenticate users. The admin
credential's holder (an operator's script, or a backend of your own) is
trusted to have done so before it names a user. Never hand the admin
credential to an end user. The [self-service console](#self-service-console)
authenticates users itself and does not use the admin credential.

## Roles

| | read-only | member | admin | owner |
|---|---|---|---|---|
| Read the organization, members, projects, tenants | yes | yes | yes | yes |
| Create an API key for one of its tenants | | yes | yes | yes |
| Create projects and tenants | | | yes | yes |
| Add, remove and change members below owner | | | yes | yes |
| Read the audit log | | | yes | yes |
| Revoke any of its API keys (anyone but read-only may revoke the keys they created) | | | yes | yes |
| Configure single sign-on | | | yes | yes |
| Require single sign-on, or remove a required one | | | | yes |
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
| `api_key.revoked` | tenant (operator routes) or `api_key` (console; target id is the `key_id_prefix`) | `key_id_prefix`, `description`; console: `tenant_id`, `description`, or `reason: "member removed"` and `member` |
| `admin.segment_gc`, `admin.compaction`, `admin.tenant_deletion`, `admin.billing`, `admin.usage`, `admin.import_namespace`, `admin.repair`, `admin.request` | `tenant` for `DELETE /v1/admin/tenants/{tenant}`, otherwise `route` (target id: the path) | `method`, `path`, `status`: one entry per state-changing (`POST`, `DELETE`) admin request that has no entry of its own, written after it ran, so refused and failed attempts are there too. Never the request body |
| `member.invited` | email | `role` |
| `sso.configured` | organization | `issuer`, `client_id`, `allowed_domains`, `sso_required`, `client_secret_changed` (never the secret) |
| `sso.removed` | organization | |
| `namespace.deleted` | namespace | `deletion_id`. Actor `tenant:<tenant id>`: a tenant API key made the request |
| `tenant.deleted` | tenant | `deletion_id`, `completed`. Actor `operator` (`DELETE /v1/admin/tenants/{tenant}`) |
| `user.signed_up` | user | `email`, `method` (`password`, `google`, `github`, `sso:<org id>`); no organization |
| `user.email_verified`, `user.password_reset`, `user.identity_linked` | user | `email`; `provider`, `unverified_password_dropped` for a link; no organization |

An entry has `id`, `org_id`, `actor` (`operator`, `operator:<name>`,
`user:<id>`, or `api_key:<first twelve characters of the key's id>` for a
tenant managing its own keys, `api_key:env` when that key is an env key),
`action`,
`target_type`, `target_id`, `detail` and `created_at_ms`. Any entry by a user
may also carry `via` in its detail (see [Who is calling](#who-is-calling)). The log never holds an
API key. `GET .../audit-log` returns one organization's entries newest first,
100 per page by default (`limit`, at most 1000); pass the response's
`next_before` as `before` for the next page. Keys created, rotated
(`/rotate`) or revoked (`/revoke`) through `POST /v1/admin/api-keys` are
recorded too, with the admin credential that made the request as the actor
(`operator` for the shared key, `operator:<name>` for an operator
credential), under the tenant's organization when it has one; so are keys a
tenant manages itself through `/v1/api-keys`, with its key as the actor. A
tenant deletion's `admin.tenant_deletion` entry is under the tenant's
organization too. `user.created`, key entries for a tenant no organization
owns, and changes to operator credentials (`operator_credential.created`,
`.rotated`, `.revoked`; target type `operator`, target id the operator's name)
are recorded with no organization and are only visible in the
`kosha.audit_log` table. An entry never holds a key; a tenant-key entry never
holds a whole `key_id` either, while an operator-credential entry names the
credential by its full `key_id`.

A console-made `api_key.created` entry also has `key_id_prefix` and `scopes`;
`member.removed` from the console has `revoked_keys` (how many). A
`member.added` from an accepted invite has `invited_by`, and a just-in-time SSO
membership has `"via": "sso"`.

`namespace.deleted` and `tenant.deleted` are written after the deletion,
outside its transaction, for tenants an organization owns; a failed write is
logged and does not fail the deletion. Not recorded yet: other data-plane
actions made with a tenant API key, the keys removed when a tenant is deleted
(the deletion itself is), and the automatic plaintext scrub (which changes no
key; it is logged by the server and kept in `kosha.api_key_scrub`). Running
the scrub by hand (`POST /v1/admin/api-keys/scrub-plaintext`) is recorded, with
no organization, as `api_key.plaintext_scrubbed` (target `registry`, detail
`rows`).

## Self-service console

Off by default. `KOSHA_CONSOLE=enabled` (with `KOSHA_CONSOLE_PUBLIC_URL`)
serves the API under `/v1/console/` and the web UI at `/console`; open sign-up
is a second switch, `KOSHA_SIGNUP=enabled`, optionally limited to
`KOSHA_SIGNUP_EMAIL_DOMAINS`. All settings are in
[configuration.md](configuration.md#self-service-console).

### Flows

- **Sign-up.** `POST /v1/console/signup {email, password, display_name?}`
  answers 202 whatever the address: a new address gets a verification link
  (valid 24 hours, once), a registered one gets a note that it already has an
  account. Passwords are at least 12 characters and stored as argon2id hashes.
  With sign-up off, only an invited address can sign up (with the invite's
  `invite_token`), or accounts come from the operator (`POST /v1/admin/users`;
  the user then sets a password with a reset link) or from SSO.
- **Verification.** The link opens `/console#/verify?token=...`, which calls
  `POST /v1/console/verify-email {token}`. A password login is refused (403,
  `"reason": "email_unverified"`) until then, and only after the password was
  right, so the answer reveals nothing to someone without it.
- **Login and sessions.** `POST /v1/console/login {email, password}` sets the
  `kosha_session` cookie (`HttpOnly; Secure; SameSite=Lax; Path=/v1/console`):
  an opaque 256-bit token, stored only as its SHA-256, valid for
  `KOSHA_SESSION_TTL_HOURS` (24) and ended after `KOSHA_SESSION_IDLE_MINUTES`
  (120) without use. `POST /v1/console/logout` revokes it. Unknown address and
  wrong password get the same 401.
- **Password reset.** `POST /v1/console/password-reset {email}` (202 either
  way) mails a link valid one hour, once; `POST
  /v1/console/password-reset/confirm {token, password}` sets the password,
  marks the address verified, and ends every session of the user.
- **Google and GitHub.** `GET /v1/console/oauth/{google|github}/start`
  redirects to the provider (authorization code with PKCE `S256`, a random
  `state` bound to the browser by the `kosha_oauth` cookie, and for Google a
  `nonce`); the provider returns to `/v1/console/oauth/{provider}/callback`.
  Google's ID token is verified against its JWKS (signature `RS256`/`ES256`,
  issuer, audience, expiry, nonce, `email_verified`). GitHub has no ID token:
  its user and emails APIs are read and only the verified primary address is
  accepted. A provider identity signs in as the user it was linked to before,
  else as the user with the same (provider-verified) address, else creates a
  user when sign-up allows that address. If the matching user never verified
  their address, their password is dropped and their sessions ended before
  the link: whoever registered an address without proving it loses access.
- **Organization SSO (OIDC).** An owner or admin sets the organization's
  issuer, client id, client secret and allowed email domains
  (`POST /v1/console/orgs/{org}/sso`; the server fetches the issuer's
  discovery document to check it). Members sign in from the login page with
  the organization's slug (`GET /v1/console/sso/start?org=<slug>`; callback
  `/v1/console/sso/callback`, the redirect URI to register with the provider).
  Unknown addresses in an allowed domain get an account and a `read-only`
  membership on first sign-in; anyone else must already be a member. The
  client secret is sealed with `KOSHA_CONSOLE_SECRET_KEY` (AES-256-GCM, bound
  to the organization) and never returned.
- **Invites.** `POST /v1/console/orgs/{org}/invites {email, role}` mails a link
  valid seven days, once; the invited user (signed in with that exact,
  verified address) accepts it with `POST /v1/console/invites/accept {token}`.
- **Removing a member** revokes the API keys they created through the console
  by default (`DELETE .../members/{user}`), so a person who leaves takes their
  keys with them; `?revoke_keys=false` keeps the keys working (for a key a
  service still depends on, to be rotated later). Keys created through the
  admin API have no recorded creator and are never revoked this way.

### Routes

Everything is JSON; errors are `{"error": "..."}`. Every request that is not a
`GET` needs the `X-Kosha-Console: 1` header (see [Security model](#security-model)).
"Session" means the `kosha_session` cookie of a signed-in user.

| Route | Body / query | Who |
|---|---|---|
| `GET /v1/console/config` | | anyone: sign-up and provider switches |
| `POST /v1/console/signup` | `{email, password, display_name?, invite_token?}` | anyone (sign-up on, or invited) |
| `POST /v1/console/verify-email` | `{token}` | anyone with the link |
| `POST /v1/console/resend-verification` | `{email}` | anyone |
| `POST /v1/console/login` | `{email, password}` | anyone |
| `POST /v1/console/logout` | | session |
| `POST /v1/console/password-reset` | `{email}` | anyone |
| `POST /v1/console/password-reset/confirm` | `{token, password}` | anyone with the link |
| `GET /v1/console/oauth/{google\|github}/start`, `.../callback` | | anyone (redirects) |
| `GET /v1/console/sso/start?org=<slug>`, `GET /v1/console/sso/callback` | | anyone (redirects) |
| `GET /v1/console/me` | | session: the user, how they signed in, their organizations and roles |
| `POST /v1/console/orgs` | `{slug, name}` | session (becomes owner) |
| `POST /v1/console/invites/accept` | `{token}` | session of the invited address |
| `GET /v1/console/orgs/{org}` | | any member |
| `GET /v1/console/orgs/{org}/members` | | any member |
| `POST /v1/console/orgs/{org}/invites` | `{email, role}` | admin, owner (owner role: owner) |
| `POST /v1/console/orgs/{org}/members/{user}/role` | `{role}` | admin, owner |
| `DELETE /v1/console/orgs/{org}/members/{user}?revoke_keys=` | | admin, owner, or the member themselves |
| `GET`, `POST /v1/console/orgs/{org}/projects` | `{slug, name}` | any member reads; admin, owner create |
| `GET`, `POST /v1/console/orgs/{org}/tenants` | `{project_id?}` (the id is generated) | any member reads; admin, owner create |
| `GET /v1/console/orgs/{org}/api-keys` | | any member: id, tenant, description, scopes, created, expires, last used, creator; never the key |
| `POST /v1/console/orgs/{org}/api-keys` | `{tenant_id, description?, scopes?, expires_in_days?}` | member, admin, owner. The response's `api_key` is the only time the key is shown |
| `POST /v1/console/orgs/{org}/api-keys/{key_id}/revoke` | | admin, owner; or the key's creator |
| `GET /v1/console/orgs/{org}/namespaces` | | any member: every tenant's namespaces with `documents`, `segments`, `stored_bytes` |
| `GET /v1/console/orgs/{org}/usage?tenant=&from=&to=` | | any member (the `GET /v1/usage` answer for that tenant) |
| `GET /v1/console/orgs/{org}/billing?tenant=` | | any member (the `GET /v1/billing` answer, plus `actions`, see below) |
| `POST /v1/console/orgs/{org}/billing/checkout`, `.../billing/portal` | `{tenant}` → `{id, url}` | admin, owner: a Stripe Checkout page (card on file, move to the catalog's `upgrade_plan`) or the customer portal for one of the organization's tenants; the browser is sent to `url` |
| `GET /v1/console/orgs/{org}/audit-log?limit=&before=` | | admin, owner |
| `GET`, `POST`, `DELETE /v1/console/orgs/{org}/sso` | `{issuer, client_id, client_secret?, allowed_domains?, sso_required?}` | admin, owner (requiring SSO, or removing a required one: owner) |

Billing: `actions` is `{"checkout": true, "portal": true}` when self-service
billing is on (a payment provider and `KOSHA_BILLING_RETURN_URL`; point that
at `https://<console>/console` so users land back in the console), else
`null` and the plan page says to contact support. No provider session is
created by the `GET`; only the `POST`s above create one, after the role
check. Dunning notices (#349) are also emailed, through the console's
mailer, to the owners and admins of the tenant's organization.

### Security model

- **Sessions never reach the data plane.** The server answers `/console` and
  `/v1/console/*` before it looks at API keys, and a session cookie is not a
  credential anywhere else: searching and indexing still need an API key.
  Console routes accept no API key and no admin credential either.
- **CSRF.** A request that is not a `GET` must carry `X-Kosha-Console: 1`,
  which a cross-site form cannot send and a cross-site script cannot send
  without a CORS preflight (which the server never grants). When the browser
  sends `Origin`, it must be `KOSHA_CONSOLE_PUBLIC_URL`; `Sec-Fetch-Site:
  cross-site` is refused. Session cookies are `SameSite=Lax`. The two `GET`
  callbacks that sign someone in are bound to the browser that started the
  sign-in (the `state` must equal the `kosha_oauth` cookie), so an attacker
  cannot sign a victim into the attacker's account.
- **Secrets at rest.** Passwords: argon2id. Session, verification, reset and
  invite tokens and OAuth `state`: only their SHA-256. Email tokens are single
  use and expire. OIDC client secrets: sealed with `KOSHA_CONSOLE_SECRET_KEY`.
  Nothing secret is written to the audit log.
- **No account enumeration.** Sign-up, resend and reset answer the same for
  any address, and login gives one error for an unknown address and a wrong
  password (a dummy argon2 verification keeps the timing alike).
- **Attempt limits.** Per client IP and per address, in fixed windows: login
  (10 per address and 50 per IP per 15 minutes), sign-up (5 per address, 20
  per IP per hour), verification resends and resets (3 per address, 20 per IP
  per hour), plus token submissions and provider redirects per IP. Beyond
  them: 429 with `Retry-After`. Counters are per pod.
- **Links come from configuration.** Email links and redirect URIs are built
  from `KOSHA_CONSOLE_PUBLIC_URL`, never from the request's `Host`.
- **Organization SSO is trusted for its organization only.** An
  organization's admins choose its identity provider, so what that provider
  says about an address is no proof elsewhere. A session opened through an
  organization's SSO works only for that organization: it cannot open
  another organization, create one, or accept invites. Just-in-time accounts
  are made only for the organization's allowed domains.
- **"SSO required"** refuses every member whose session was not opened
  through the organization's SSO, except owners, who keep a way in when the
  identity provider is down or misconfigured.
- **Static assets** are compiled into the binary, load nothing from other
  origins, and are served with `Content-Security-Policy: default-src 'self'`,
  `frame-ancestors 'none'` and `X-Frame-Options: DENY`. The page renders
  server data as text, never as HTML.

### Running several pods

The console's state is the control plane's (Postgres), so every pod can serve
it and console requests are not forwarded to the ingest pod. A key created on
one pod works there immediately and on the others after their next key
refresh (`KOSHA_API_KEY_REFRESH_SECS`). Attempt-limit counters are per pod.
Without Postgres the console works in one process only (development).

### Not built

- SAML (deferred to a follow-up issue; use OIDC).
- Deleting namespaces, organizations, projects or users from the console;
  renaming them; changing your email address or password while signed in
  (use a reset link); listing or revoking your own sessions.
- Online payment (Stripe Checkout and portal, #349): see the hook above.

## Storage

With Postgres (`DATABASE_URL` and the `postgres` feature) the model lives in
`kosha.users`, `kosha.organizations`, `kosha.org_members`, `kosha.projects`,
`kosha.tenants` and `kosha.audit_log`, created by
`crates/kosha-control/migrations/006_orgs_users_projects.sql`, and the
console's in `kosha.user_credentials`, `kosha.auth_tokens`, `kosha.sessions`,
`kosha.user_identities`, `kosha.org_sso`, `kosha.login_flows` and
`kosha.api_key_creators`, created by `008_identity_sessions.sql`. The
migrations only add tables; the server applies them at startup, and
`make db-migrate` applies them too. Expired sessions and used tokens stay in
their tables (they no longer work); expired OAuth sign-ins are deleted as new
ones start.

Without Postgres the model is kept in the server's memory and is lost on
restart, like the rest of the in-memory control plane. Use that for local
development only.
