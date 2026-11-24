# Self-serve console

`KOSHA_CONSOLE=on` serves a web console at `/console` (#345). Anyone with an
email address can sign up, create an organization, a project and a tenant,
and get an API key, with no operator and no database access. It sits in
front of the [organization model](organizations.md): every action it takes
is that model's, run as the signed-in user, so the same role checks and
audit log apply.

## Sign-up and sign-in

Passwordless. The user enters an email address; the server emails a link
that works once, for 15 minutes. Opening it verifies the address and signs
in, creating the user the first time. The session lasts 7 days or until
**Sign out**.

- Links and sessions are random 244-bit tokens. Only their SHA-256 is stored.
- The link carries its token in the URL fragment (`/console/#signin=...`),
  so it never reaches the server's access logs.
- One link per address per minute (429 otherwise).
- A user the operator created with `POST /v1/admin/users` signs in as
  themselves; no second user is made.
- Sign-up, the first verification and every sign-in are audit-logged
  (`user.signed_up`, `user.email_verified`, `user.signed_in`).

Email goes out through a webhook, so any mail service can deliver it:

| Variable | |
|---|---|
| `KOSHA_CONSOLE_URL` | This server's public origin, the start of every link. Never taken from the request's `Host`, which a caller controls. |
| `KOSHA_CONSOLE_EMAIL_WEBHOOK` | Each email is POSTed here as JSON `{to, subject, text, link}`. A non-2xx answer fails the request. |

The console stays off, with an error in the log, when a webhook is set
without `KOSHA_CONSOLE_URL`, or when `KOSHA_CONSOLE_URL` is a public origin
and no webhook is set. Without a webhook, and with no URL or a loopback one,
the link is written to the server log, for local development:

```sh
KOSHA_CONSOLE=on KOSHA_ADMIN_API_KEY=dev-admin cargo run -p kosha-server
# open http://localhost:8080/console, enter an email, then open the
# "sign-in link for ..." path from the server log
```

## What a signed-in user can do

The console page shows the user's organizations and, in one, its projects,
tenants, API keys (shown once, at creation), members and, for owners and
admins, the audit log. Creating an organization makes the user its owner.
Members are added by user id: each user sees theirs on the console's home
page.

## API

The page calls a small JSON API, answered before API-key authentication.
Every call but the first two needs `Authorization: Bearer <session_token>`.

```text
POST /console/api/signin          {email, display_name?}  -> {sent, expires_at_ms}
POST /console/api/signin/verify   {token}                 -> {user, created, session_token, expires_at_ms}
POST /console/api/signout
GET  /console/api/me                                      -> {user, orgs: [{id, slug, name, role, ...}]}
*    /console/api/orgs[/...]                              the /v1/admin/orgs routes, as this user
```

`/console/api/orgs/...` takes the same paths and bodies as
`/v1/admin/orgs/...` ([organizations.md](organizations.md#api)), with the
session in place of the admin credential and `X-Kosha-Actor`. An admin API
key is not a session and is refused there.

## Storage

Migration 009 adds `kosha.signin_tokens`, `kosha.user_sessions` and
`kosha.email_verifications`, applied at startup like 006. Without Postgres
they are held in memory, and sessions end when the server restarts. Run the
console on Postgres in any multi-pod deployment: each pod would otherwise
have its own users.

## Not yet

Google/GitHub OAuth, SAML/OIDC SSO, invitations by email, and the usage and
plan views are the next parts of #345.
