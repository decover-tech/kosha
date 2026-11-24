//! API-key commands added with #346's second part:
//!
//! - `kosha keys ...`: a tenant manages its own keys with a key that holds
//!   the `admin` scope (`/v1/api-keys`). What it creates never does more,
//!   or lives longer, than the key running the command.
//! - `kosha admin create-operator`, `list-operators`, `rotate-operator`,
//!   `revoke-operator` and `api-key-plaintext-count`: per-operator admin
//!   credentials (`/v1/admin/operators`), flattened into `kosha admin`.
//!
//! Like `kosha admin create-api-key`, a command that issues a key prints
//! only the key on stdout (so `KEY=$(kosha keys create ...)` captures
//! exactly it) and the rest on stderr; `--json` prints the whole response.

use clap::Subcommand;
use reqwest::Method;
use serde_json::Value;

use crate::client::{Client, ClientError, RequestError};
use crate::{admin_error, api_key_lines, create_api_key_body, print_value, KeyScope};

/// `kosha keys`: the calling tenant's own API keys.
#[derive(Debug, Subcommand)]
pub enum KeyCommands {
    /// List this tenant's API keys that the calling key may manage (never
    /// shows a key itself; `*` marks the calling key)
    List,
    /// Create an API key for this tenant, within the calling key's own
    /// scopes, namespaces and expiry
    ///
    /// The key is printed once, on stdout; store it now.
    Create {
        /// Free-text note stored with the key
        #[arg(long, default_value = "")]
        description: String,
        /// What the key may do: read, write, admin. Repeat or
        /// comma-separate; the calling key's own scopes when omitted
        #[arg(long = "scope", value_enum, value_delimiter = ',')]
        scopes: Vec<KeyScope>,
        /// Restrict the key to this namespace (repeatable); the calling
        /// key's own namespaces when omitted
        #[arg(long = "namespace", value_name = "NAMESPACE")]
        namespaces: Vec<String>,
        /// Make the key expire this many seconds from now; when omitted it
        /// expires with the calling key (never, if that one does not)
        #[arg(long, value_name = "SECS")]
        expires_in_secs: Option<u64>,
    },
    /// Replace one of this tenant's keys without downtime; the old one
    /// keeps working for an overlap window
    ///
    /// The new key is printed once, on stdout; store it now.
    Rotate {
        /// The id of the key to replace, from `kosha keys list`
        key_id: String,
        /// How long the old key keeps working (server default: one day)
        #[arg(long, value_name = "SECS")]
        overlap_secs: Option<u64>,
        /// Make the new key expire this many seconds from now
        #[arg(long, value_name = "SECS")]
        expires_in_secs: Option<u64>,
    },
    /// Revoke one of this tenant's keys; servers stop accepting it within
    /// their key refresh interval
    Revoke {
        /// The id of the key to revoke, from `kosha keys list`
        key_id: String,
    },
}

/// The operator-credential commands of `kosha admin`.
#[derive(Debug, Subcommand)]
pub enum OperatorCommands {
    /// Issue a named admin credential for one operator (Postgres control
    /// plane only); the audit log names the operator for everything done
    /// with it
    ///
    /// The credential is printed once, on stdout; store it now.
    CreateOperator {
        /// The operator's name (letters, digits, . _ - @ +), e.g. an email
        #[arg(long)]
        name: String,
        /// Free-text note stored with the credential
        #[arg(long, default_value = "")]
        description: String,
        /// Make the credential expire this many seconds from now
        #[arg(long, value_name = "SECS")]
        expires_in_secs: Option<u64>,
    },
    /// List operator credentials that are not revoked (never shows one)
    ListOperators,
    /// Replace an operator credential without downtime
    ///
    /// The new credential is printed once, on stdout; store it now.
    RotateOperator {
        /// The id of the credential to replace, from `list-operators`
        key_id: String,
        /// How long the old credential keeps working (server default: one
        /// day)
        #[arg(long, value_name = "SECS")]
        overlap_secs: Option<u64>,
        /// Make the new credential expire this many seconds from now
        #[arg(long, value_name = "SECS")]
        expires_in_secs: Option<u64>,
        /// Rotate even if the server refuses because it would let the last
        /// usable operator credential lapse while the shared admin key is
        /// off
        #[arg(long)]
        force: bool,
    },
    /// Revoke one operator credential, or every credential of an operator
    RevokeOperator {
        /// The id of the credential to revoke
        #[arg(required_unless_present = "name", conflicts_with = "name")]
        key_id: Option<String>,
        /// Revoke every credential of this operator instead (offboarding)
        #[arg(long)]
        name: Option<String>,
        /// Revoke even if the server refuses because it would leave no
        /// usable operator credential while the shared admin key is off
        #[arg(long)]
        force: bool,
    },
    /// Show how many stored API keys still hold their plaintext (the server
    /// removes it automatically once no older server build can need it)
    ApiKeyPlaintextCount,
}

/// The rotate request body shared by both kinds of key.
fn rotate_body(key_id: &str, overlap_secs: Option<u64>, expires_in_secs: Option<u64>) -> Value {
    let mut body = serde_json::json!({ "key_id": key_id });
    if let Some(secs) = overlap_secs {
        body["overlap_secs"] = serde_json::json!(secs);
    }
    if let Some(secs) = expires_in_secs {
        body["expires_in_secs"] = serde_json::json!(secs);
    }
    body
}

/// The `POST /v1/api-keys` body: only what was given, so the server
/// applies the calling key's own scopes, namespaces and expiry.
fn create_own_key_body(
    description: &str,
    scopes: &[KeyScope],
    namespaces: &[String],
    expires_in_secs: Option<u64>,
) -> Value {
    let mut body = create_api_key_body("", description, scopes, namespaces, expires_in_secs);
    if let Some(fields) = body.as_object_mut() {
        fields.remove("tenant_id");
    }
    body
}

/// Print a freshly issued key: alone on stdout, notes on stderr.
fn print_issued(value: &Value, json: bool, what: &str) -> Result<(), Box<dyn std::error::Error>> {
    let key = value
        .get("api_key")
        .and_then(|k| k.as_str())
        .ok_or("server response has no api_key")?;
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
        return Ok(());
    }
    eprintln!("{what}:");
    println!("{key}");
    if let Some(old) = value
        .pointer("/replaces/expires_at")
        .and_then(|t| t.as_i64())
    {
        eprintln!("the replaced key keeps working until unix time {old}.");
    }
    eprintln!("this is the only time the key is shown; store it now.");
    Ok(())
}

/// Explain a `/v1/api-keys` failure in plain words.
fn key_error(error: RequestError) -> ClientError {
    match error {
        RequestError::Http(http) if http.status == 403 => ClientError(format!(
            "{} {} was refused (403: {}). `kosha keys` needs a tenant API key with the \
             'admin' scope, and can only grant what that key holds. Operators manage any \
             tenant's keys with `kosha admin create-api-key` and its siblings.",
            http.method, http.path, http.message
        )),
        RequestError::Http(http) => ClientError(http.to_string()),
        RequestError::Other(other) => other,
    }
}

/// One line per operator credential of a `GET /v1/admin/operators` reply.
fn operator_lines(value: &Value) -> Vec<String> {
    fn text<'a>(v: &'a Value, field: &str) -> &'a str {
        v.get(field).and_then(|s| s.as_str()).unwrap_or("")
    }
    let time = |v: &Value, field: &str| match v.get(field).and_then(|t| t.as_i64()) {
        Some(at) => at.to_string(),
        None => "never".to_string(),
    };
    value
        .get("operators")
        .and_then(|o| o.as_array())
        .map(|operators| {
            operators
                .iter()
                .map(|o| {
                    let expired = o.get("expired").and_then(|e| e.as_bool()).unwrap_or(false);
                    format!(
                        "{}  name={}  expires_at={}{}  last_used_at={}  {}",
                        text(o, "key_id"),
                        text(o, "name"),
                        time(o, "expires_at"),
                        if expired { " (expired)" } else { "" },
                        time(o, "last_used_at"),
                        text(o, "description"),
                    )
                    .trim_end()
                    .to_string()
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Run a `kosha keys` command.
pub fn run_keys(
    client: &Client,
    command: KeyCommands,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        KeyCommands::List => {
            let value = client
                .curl(Method::GET, "/v1/api-keys", None)
                .map_err(key_error)?;
            print_value(&value, json, |v| {
                let current: Vec<bool> = v
                    .get("api_keys")
                    .and_then(|k| k.as_array())
                    .map(|keys| {
                        keys.iter()
                            .map(|k| k.get("current").and_then(|c| c.as_bool()) == Some(true))
                            .collect()
                    })
                    .unwrap_or_default();
                let lines = api_key_lines(v);
                if lines.is_empty() {
                    eprintln!("no API keys this key may manage");
                }
                for (line, is_current) in lines.into_iter().zip(current) {
                    println!("{}{line}", if is_current { "* " } else { "  " });
                }
                Ok(())
            })?;
        }
        KeyCommands::Create {
            description,
            scopes,
            namespaces,
            expires_in_secs,
        } => {
            let body = create_own_key_body(&description, &scopes, &namespaces, expires_in_secs);
            let value = client
                .curl(Method::POST, "/v1/api-keys", Some(body))
                .map_err(key_error)?;
            print_issued(&value, json, "created API key")?;
        }
        KeyCommands::Rotate {
            key_id,
            overlap_secs,
            expires_in_secs,
        } => {
            let body = rotate_body(&key_id, overlap_secs, expires_in_secs);
            let value = client
                .curl(Method::POST, "/v1/api-keys/rotate", Some(body))
                .map_err(key_error)?;
            print_issued(&value, json, &format!("replacement for API key {key_id}"))?;
        }
        KeyCommands::Revoke { key_id } => {
            let value = client
                .curl(
                    Method::POST,
                    "/v1/api-keys/revoke",
                    Some(serde_json::json!({ "key_id": key_id })),
                )
                .map_err(key_error)?;
            print_value(&value, json, |_| {
                println!(
                    "revoked API key {key_id}; servers stop accepting it within their key \
                     refresh interval"
                );
                Ok(())
            })?;
        }
    }
    Ok(())
}

/// Run an operator-credential command of `kosha admin`.
pub fn run_operator(
    client: &Client,
    command: OperatorCommands,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        OperatorCommands::CreateOperator {
            name,
            description,
            expires_in_secs,
        } => {
            let mut body = serde_json::json!({ "name": name, "description": description });
            if let Some(secs) = expires_in_secs {
                body["expires_in_secs"] = serde_json::json!(secs);
            }
            let value = client
                .curl(Method::POST, "/v1/admin/operators", Some(body))
                .map_err(admin_error)?;
            print_issued(
                &value,
                json,
                &format!("created credential for operator {name:?}"),
            )?;
        }
        OperatorCommands::ListOperators => {
            let value = client
                .curl(Method::GET, "/v1/admin/operators", None)
                .map_err(admin_error)?;
            print_value(&value, json, |v| {
                let lines = operator_lines(v);
                if lines.is_empty() {
                    eprintln!("no operator credentials");
                }
                for line in lines {
                    println!("{line}");
                }
                Ok(())
            })?;
        }
        OperatorCommands::RotateOperator {
            key_id,
            overlap_secs,
            expires_in_secs,
            force,
        } => {
            let mut body = rotate_body(&key_id, overlap_secs, expires_in_secs);
            if force {
                body["force"] = serde_json::json!(true);
            }
            let value = client
                .curl(Method::POST, "/v1/admin/operators/rotate", Some(body))
                .map_err(admin_error)?;
            print_issued(
                &value,
                json,
                &format!("replacement for operator credential {key_id}"),
            )?;
        }
        OperatorCommands::RevokeOperator {
            key_id,
            name,
            force,
        } => {
            let mut body = match (&key_id, &name) {
                (_, Some(name)) => serde_json::json!({ "name": name }),
                (Some(key_id), None) => serde_json::json!({ "key_id": key_id }),
                (None, None) => return Err("give a credential id or --name".into()),
            };
            if force {
                body["force"] = serde_json::json!(true);
            }
            let value = client
                .curl(Method::POST, "/v1/admin/operators/revoke", Some(body))
                .map_err(admin_error)?;
            print_value(&value, json, |v| {
                for revoked in v
                    .get("revoked")
                    .and_then(|r| r.as_array())
                    .into_iter()
                    .flatten()
                {
                    println!(
                        "revoked operator credential {} ({})",
                        revoked
                            .get("key_id")
                            .and_then(|k| k.as_str())
                            .unwrap_or("?"),
                        revoked.get("name").and_then(|n| n.as_str()).unwrap_or("?"),
                    );
                }
                eprintln!("servers stop accepting it within their key refresh interval");
                Ok(())
            })?;
        }
        OperatorCommands::ApiKeyPlaintextCount => {
            let value = client
                .curl(Method::GET, "/v1/admin/api-keys/plaintext-count", None)
                .map_err(admin_error)?;
            print_value(&value, json, |v| {
                println!(
                    "{} stored API key(s) still hold their plaintext",
                    v.get("plaintext_rows")
                        .and_then(|n| n.as_u64())
                        .unwrap_or(0)
                );
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_key_bodies_carry_only_what_was_given() {
        assert_eq!(
            create_own_key_body("", &[], &[], None),
            serde_json::json!({ "description": "" })
        );
        assert_eq!(
            create_own_key_body("ci", &[KeyScope::Read], &["docs".into()], Some(60)),
            serde_json::json!({
                "description": "ci", "scopes": ["read"], "namespaces": ["docs"],
                "expires_in_secs": 60
            })
        );
        assert_eq!(
            rotate_body("abc", Some(5), None),
            serde_json::json!({ "key_id": "abc", "overlap_secs": 5 })
        );
    }

    #[test]
    fn operator_listing_reads_one_line_per_credential() {
        let lines = operator_lines(&serde_json::json!({"operators": [
            {"key_id": "aa11", "name": "alice", "description": "laptop",
             "expires_at": 1900000000, "expired": true, "last_used_at": null},
            {"key_id": "bb22", "name": "ci", "description": "",
             "expires_at": null, "expired": false, "last_used_at": 1800000000},
        ]}));
        assert_eq!(
            lines,
            [
                "aa11  name=alice  expires_at=1900000000 (expired)  last_used_at=never  laptop",
                "bb22  name=ci  expires_at=never  last_used_at=1800000000",
            ]
        );
        assert!(operator_lines(&serde_json::json!({})).is_empty());
    }
}
