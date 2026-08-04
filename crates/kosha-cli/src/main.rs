//! `kosha` — remote CLI for exploring Kosha namespaces over HTTP.
//!
//! Mirrors the Elastic/OpenSearch client-CLI pattern: the server binary serves
//! traffic; this binary talks to it with profiles, REST-shaped verbs, and a
//! raw `curl` escape hatch.

mod client;
mod config;

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kosha_core::SearchQuery;
use reqwest::Method;
use serde_json::Value;

use crate::client::{parse_document, Client, ClientError};
use crate::config::{default_config_path, load_config, resolve_connection, save_config};

#[derive(Debug, Parser)]
#[command(
    name = "kosha",
    about = "Explore Kosha namespaces over HTTP",
    version,
    propagate_version = true
)]
struct Cli {
    /// Profile name from ~/.kosha/config.toml
    #[arg(long, global = true, env = "KOSHA_PROFILE")]
    profile: Option<String>,

    /// Kosha HTTP base URL (overrides profile / env)
    #[arg(long, global = true, env = "KOSHA_HOST")]
    host: Option<String>,

    /// API key (overrides profile / env)
    #[arg(long, global = true, env = "KOSHA_API_KEY")]
    api_key: Option<String>,

    /// Path to config file (default: ~/.kosha/config.toml)
    #[arg(long, global = true, env = "KOSHA_CONFIG")]
    config: Option<PathBuf>,

    /// Emit raw JSON instead of human-readable output
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Check that the server is reachable
    Health,
    /// Show global or per-namespace stats
    Stats {
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Index documents into a namespace
    Index {
        #[arg(short = 'n', long)]
        namespace: String,
        /// JSONL file of documents (native or shorthand). Use - for stdin.
        #[arg(long, conflicts_with = "doc")]
        file: Option<PathBuf>,
        /// Single document JSON object
        #[arg(long, conflicts_with = "file")]
        doc: Option<String>,
    },
    /// Search a namespace
    Search {
        #[arg(short = 'n', long)]
        namespace: String,
        /// BM25 query text (mutually exclusive with --body)
        query: Option<String>,
        /// Max hits to return
        #[arg(long = "max", default_value_t = 10)]
        max_results: usize,
        /// Filter clause JSON, e.g. '{"term":{"matterId":"m1"}}'
        #[arg(long)]
        filter: Option<String>,
        /// Full SearchQuery JSON (@file.json or inline). Overrides query/max/filter.
        #[arg(long, conflicts_with_all = ["query", "filter"])]
        body: Option<String>,
    },
    /// Flush buffered documents to a segment
    Flush {
        #[arg(short = 'n', long)]
        namespace: Option<String>,
    },
    /// Delete documents matching a filter
    Delete {
        #[arg(short = 'n', long)]
        namespace: String,
        /// Filter clause JSON
        #[arg(long)]
        filter: String,
    },
    /// Rebuild footer filter blooms for a namespace (enables segment pruning)
    RebuildFilterBlooms {
        #[arg(short = 'n', long)]
        namespace: String,
    },
    /// Backfill doc_store.offsets on segments written before lazy doc
    /// loading existed, so they stop paying full-segment materialization
    /// cost on every query without waiting for their next compaction cycle.
    BackfillOffsetTables {
        #[arg(short = 'n', long)]
        namespace: String,
    },
    /// Raw HTTP escape hatch (any path)
    Curl {
        /// HTTP method (GET, POST, …)
        method: String,
        /// Path beginning with / (e.g. /v1/stats)
        path: String,
        /// JSON body (@file.json or inline)
        #[arg(long)]
        body: Option<String>,
    },
    /// Manage connection profiles
    Profile {
        #[command(subcommand)]
        command: ProfileCommands,
    },
}

#[derive(Debug, Subcommand)]
enum ProfileCommands {
    /// List configured profiles
    List,
    /// Show one profile (or the default)
    Show { name: Option<String> },
    /// Set the default profile name
    SetDefault { name: String },
}

fn main() -> ExitCode {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config_path = cli.config.clone().unwrap_or_else(default_config_path);

    if let Commands::Profile { command } = &cli.command {
        return run_profile(command, &config_path, cli.json);
    }

    let config = load_config(&config_path)?;
    let conn = resolve_connection(
        &config,
        cli.profile.as_deref(),
        cli.host.as_deref(),
        cli.api_key.as_deref(),
    )?;
    let client = Client::new(&conn)?;

    match cli.command {
        Commands::Health => {
            let value = client.health()?;
            print_value(&value, cli.json, human_health)?;
        }
        Commands::Stats { namespace } => {
            let value = match namespace {
                Some(ns) => client.namespace_stats(&ns)?,
                None => client.stats()?,
            };
            print_value(&value, cli.json, human_stats)?;
        }
        Commands::Index {
            namespace,
            file,
            doc,
        } => {
            let documents = load_documents(file.as_ref(), doc.as_deref())?;
            if documents.is_empty() {
                return Err("no documents to index".into());
            }
            let count = documents.len();
            let resp = client.index_documents(&namespace, documents)?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&resp)?);
            } else {
                println!(
                    "indexed {} document(s) into namespace {}",
                    resp.indexed_count, resp.namespace.0
                );
                if resp.indexed_count != count {
                    println!("(requested {count})");
                }
            }
        }
        Commands::Search {
            namespace,
            query,
            max_results,
            filter,
            body,
        } => {
            if let Some(body_src) = body {
                let body_val = read_json_arg(&body_src)?;
                let value = client.search_body(&namespace, body_val)?;
                print_value(&value, cli.json, human_search_value)?;
            } else {
                let query_text = query.ok_or("provide a query string or --body")?;
                let mut search = SearchQuery {
                    query_text,
                    max_results,
                    from: 0,
                    bm25_params: Default::default(),
                    filter: None,
                    sort: Vec::new(),
                    search_after: None,
                    highlight: None,
                    aggs: Default::default(),
                    wildcard: None,
                    match_phrase: None,
                    knn: None,
                };
                if let Some(filter_src) = filter {
                    search.filter = Some(serde_json::from_str(&filter_src)?);
                }
                let result = client.search(&namespace, search)?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    human_search(&result)?;
                }
            }
        }
        Commands::Flush { namespace } => {
            let value = client.flush(namespace.as_deref())?;
            print_value(&value, cli.json, |v| {
                let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("ok");
                match namespace.as_deref() {
                    Some(ns) => println!("flushed namespace {ns} ({status})"),
                    None => println!("flushed all namespaces ({status})"),
                }
                Ok(())
            })?;
        }
        Commands::Delete { namespace, filter } => {
            let filter_val: Value = serde_json::from_str(&filter)?;
            let value = client.delete(&namespace, filter_val)?;
            print_value(&value, cli.json, |v| {
                let deleted = v.get("deleted").and_then(|d| d.as_u64()).unwrap_or(0);
                println!("deleted {deleted} document(s) from namespace {namespace}");
                Ok(())
            })?;
        }
        Commands::RebuildFilterBlooms { namespace } => {
            let value = client.rebuild_filter_blooms(&namespace)?;
            print_value(&value, cli.json, |v| {
                let rebuilt = v.get("rebuilt").and_then(|d| d.as_u64()).unwrap_or(0);
                let segments = v.get("segments").and_then(|d| d.as_u64()).unwrap_or(0);
                let errors = v
                    .get("errors")
                    .and_then(|e| e.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                println!(
                    "rebuilt filter blooms for {rebuilt}/{segments} segment(s) in {namespace}"
                );
                if errors > 0 {
                    println!("({errors} error(s) — see --json for details)");
                }
                Ok(())
            })?;
        }
        Commands::BackfillOffsetTables { namespace } => {
            let value = client.backfill_offset_tables(&namespace)?;
            print_value(&value, cli.json, |v| {
                let backfilled = v.get("backfilled").and_then(|d| d.as_u64()).unwrap_or(0);
                let segments = v.get("segments").and_then(|d| d.as_u64()).unwrap_or(0);
                let errors = v
                    .get("errors")
                    .and_then(|e| e.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0);
                println!(
                    "backfilled doc_store.offsets for {backfilled}/{segments} segment(s) in {namespace}"
                );
                if errors > 0 {
                    println!("({errors} error(s) — see --json for details)");
                }
                Ok(())
            })?;
        }
        Commands::Curl { method, path, body } => {
            let method = parse_method(&method)?;
            let body_val = match body {
                Some(src) => Some(read_json_arg(&src)?),
                None => None,
            };
            let value = client.curl(method, &path, body_val)?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Commands::Profile { .. } => unreachable!(),
    }

    Ok(())
}

fn run_profile(
    command: &ProfileCommands,
    config_path: &Path,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = load_config(config_path)?;
    match command {
        ProfileCommands::List => {
            if json {
                println!("{}", serde_json::to_string_pretty(&config)?);
            } else if config.profiles.is_empty() {
                println!("no profiles in {}", config_path.display());
            } else {
                for (name, profile) in &config.profiles {
                    let marker = if config.default_profile.as_deref() == Some(name.as_str()) {
                        "*"
                    } else {
                        " "
                    };
                    let host = profile.host.as_deref().unwrap_or("-");
                    let key = if profile.api_key_env.is_some() {
                        format!("api_key_env={}", profile.api_key_env.as_deref().unwrap())
                    } else if profile.api_key.is_some() {
                        "api_key=***".into()
                    } else {
                        "api_key=-".into()
                    };
                    println!("{marker} {name}\t{host}\t{key}");
                }
            }
        }
        ProfileCommands::Show { name } => {
            let name = name
                .clone()
                .or_else(|| config.default_profile.clone())
                .ok_or("no profile name and no default_profile set")?;
            let profile = config
                .profiles
                .get(&name)
                .ok_or_else(|| format!("unknown profile {name:?}"))?;
            if json {
                println!("{}", serde_json::to_string_pretty(profile)?);
            } else {
                println!("profile: {name}");
                println!("  host: {}", profile.host.as_deref().unwrap_or("-"));
                if let Some(env_name) = &profile.api_key_env {
                    println!("  api_key_env: {env_name}");
                } else if profile.api_key.is_some() {
                    println!("  api_key: ***");
                } else {
                    println!("  api_key: -");
                }
                if config.default_profile.as_deref() == Some(name.as_str()) {
                    println!("  (default)");
                }
            }
        }
        ProfileCommands::SetDefault { name } => {
            if !config.profiles.contains_key(name) {
                return Err(format!(
                    "unknown profile {name:?}; add it to {} first",
                    config_path.display()
                )
                .into());
            }
            config.default_profile = Some(name.clone());
            save_config(config_path, &config)?;
            println!("default profile set to {name}");
        }
    }
    Ok(())
}

fn load_documents(
    file: Option<&PathBuf>,
    doc: Option<&str>,
) -> Result<Vec<kosha_core::Document>, Box<dyn std::error::Error>> {
    if let Some(raw) = doc {
        let value = read_json_arg(raw)?;
        return Ok(vec![parse_document(value)?]);
    }
    let text = match file {
        Some(path) if path.as_os_str() == "-" => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            buf
        }
        Some(path) => fs::read_to_string(path)?,
        None => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };
    let mut docs = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let value: Value =
            serde_json::from_str(line).map_err(|e| format!("line {}: invalid JSON: {e}", i + 1))?;
        docs.push(parse_document(value).map_err(|e| format!("line {}: {e}", i + 1))?);
    }
    // Also accept a single JSON array / object when not JSONL.
    if docs.is_empty() {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            let value: Value = serde_json::from_str(trimmed)?;
            match value {
                Value::Array(items) => {
                    for item in items {
                        docs.push(parse_document(item)?);
                    }
                }
                other => docs.push(parse_document(other)?),
            }
        }
    }
    Ok(docs)
}

fn read_json_arg(src: &str) -> Result<Value, Box<dyn std::error::Error>> {
    if let Some(path) = src.strip_prefix('@') {
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    } else {
        Ok(serde_json::from_str(src)?)
    }
}

fn parse_method(method: &str) -> Result<Method, ClientError> {
    method
        .parse::<Method>()
        .map_err(|_| ClientError(format!("invalid HTTP method {method:?}")))
}

fn print_value<F>(value: &Value, json: bool, human: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnOnce(&Value) -> Result<(), Box<dyn std::error::Error>>,
{
    if json {
        println!("{}", serde_json::to_string_pretty(value)?);
    } else {
        human(value)?;
    }
    Ok(())
}

fn human_health(value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let status = value
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown");
    println!("status: {status}");
    Ok(())
}

fn human_stats(value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(ns) = value.get("namespace").and_then(|v| v.as_str()) {
        println!(
            "namespace: {ns}\n  documents: {}\n  segments: {}\n  version: {}",
            value.get("documents").and_then(|v| v.as_u64()).unwrap_or(0),
            value.get("segments").and_then(|v| v.as_u64()).unwrap_or(0),
            value.get("version").and_then(|v| v.as_u64()).unwrap_or(0),
        );
        return Ok(());
    }
    println!(
        "total_documents: {}\ntotal_segments: {}\ncontrol_plane: {}",
        value
            .get("total_documents")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        value
            .get("total_segments")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        value
            .get("control_plane")
            .and_then(|v| v.as_str())
            .unwrap_or("-"),
    );
    if let Some(namespaces) = value.get("namespaces").and_then(|v| v.as_array()) {
        for ns in namespaces {
            println!(
                "  {}: {} docs, {} segment(s)",
                ns.get("namespace").and_then(|v| v.as_str()).unwrap_or("?"),
                ns.get("documents").and_then(|v| v.as_u64()).unwrap_or(0),
                ns.get("segments").and_then(|v| v.as_u64()).unwrap_or(0),
            );
        }
    }
    Ok(())
}

fn human_search(result: &kosha_core::SearchResult) -> Result<(), Box<dyn std::error::Error>> {
    println!("total_hits: {}", result.total_hits);
    for hit in &result.results {
        let title = hit
            .fields
            .iter()
            .find(|f| f.name == "title")
            .map(|f| f.value.as_str())
            .unwrap_or("");
        println!("  [{}] {}  (score={:.3})", hit.doc_id.0, title, hit.score);
    }
    Ok(())
}

fn human_search_value(value: &Value) -> Result<(), Box<dyn std::error::Error>> {
    let result: kosha_core::SearchResult = serde_json::from_value(value.clone())?;
    human_search(&result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_accepts_mvp_commands() {
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_search_with_namespace_and_query() {
        let cli = Cli::try_parse_from([
            "kosha",
            "--host",
            "http://localhost:8080",
            "search",
            "-n",
            "demo",
            "breach",
            "--max",
            "5",
        ])
        .unwrap();
        match cli.command {
            Commands::Search {
                namespace,
                query,
                max_results,
                ..
            } => {
                assert_eq!(namespace, "demo");
                assert_eq!(query.as_deref(), Some("breach"));
                assert_eq!(max_results, 5);
            }
            _ => panic!("expected search"),
        }
    }

    #[test]
    fn parses_backfill_offset_tables() {
        let cli = Cli::try_parse_from([
            "kosha",
            "--host",
            "http://localhost:8080",
            "backfill-offset-tables",
            "-n",
            "demo",
        ])
        .unwrap();
        match cli.command {
            Commands::BackfillOffsetTables { namespace } => {
                assert_eq!(namespace, "demo");
            }
            _ => panic!("expected backfill-offset-tables"),
        }
    }

    #[test]
    fn parses_curl_escape_hatch() {
        let cli = Cli::try_parse_from(["kosha", "curl", "GET", "/v1/stats"]).unwrap();
        match cli.command {
            Commands::Curl { method, path, .. } => {
                assert_eq!(method, "GET");
                assert_eq!(path, "/v1/stats");
            }
            _ => panic!("expected curl"),
        }
    }
}
