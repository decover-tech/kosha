//! `kosha` — remote CLI for exploring Kosha namespaces over HTTP.
//!
//! Mirrors the Elastic/OpenSearch client-CLI pattern: the server binary serves
//! traffic; this binary talks to it with profiles, REST-shaped verbs, and a
//! raw `curl` escape hatch.

mod client;
mod config;
mod search;

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use kosha_core::SortSpec;
use reqwest::Method;
use serde_json::Value;

use crate::client::{parse_document, Client, ClientError, RequestError};
use crate::config::{default_config_path, load_config, resolve_connection, save_config};
use crate::search::{
    build_search_query, next_search_after, read_json_arg, shell_quote, SearchArgs,
};

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
    /// Search a namespace (BM25, kNN, or hybrid)
    ///
    /// Every flag below except --max is an alternative to setting the same
    /// SearchQuery field in --body, and conflicts with --body.
    Search {
        #[arg(short = 'n', long)]
        namespace: String,
        /// BM25 query text. With --knn-* as well, the search is hybrid.
        #[arg(required_unless_present_any = ["body", "knn_field"])]
        query: Option<String>,
        /// Max hits to return
        #[arg(long = "max", default_value_t = 10)]
        max_results: usize,
        /// Full SearchQuery JSON (@file.json or inline)
        #[arg(long, conflicts_with_all = SEARCH_FLAG_IDS)]
        body: Option<String>,
        /// Boxed: the flags dwarf every other variant.
        #[command(flatten)]
        args: Box<SearchArgs>,
    },
    /// Check whether documents exist (exit status 1 when any is missing)
    Exists {
        #[arg(short = 'n', long)]
        namespace: String,
        /// Document id(s) to check
        #[arg(required = true, value_name = "DOC_ID")]
        ids: Vec<String>,
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
    /// Start a compaction pass on a namespace (admin)
    ///
    /// Runs as a background job on the server; prints the job id. --wait
    /// polls until the job finishes.
    Compact {
        /// Namespace as stored, with its tenant prefix (e.g. default/my-ns).
        /// A bare name is looked up as is, then under the admin key's own
        /// tenant, not the tenant whose data it is.
        #[arg(short = 'n', long)]
        namespace: String,
        /// tiered merges only small segments; full merges everything (up
        /// to the merged-size cap)
        #[arg(long, value_enum, default_value_t = CompactMode::Tiered)]
        mode: CompactMode,
        /// Cap on a merged segment's size; 0 disables the cap
        #[arg(long, value_name = "BYTES")]
        max_segment_bytes: Option<u64>,
        /// Poll the job until it finishes
        #[arg(long)]
        wait: bool,
        /// Give up waiting after this many seconds (the job keeps running)
        #[arg(long, value_name = "SECS", default_value_t = 3600, requires = "wait")]
        timeout: u64,
        /// Seconds between polls
        #[arg(long, value_name = "SECS", default_value_t = 2, requires = "wait")]
        poll_interval: u64,
    },
    /// Show the compaction scheduler's status, or one job's (admin)
    CompactionStatus {
        /// Show this compaction job instead of the scheduler overview
        #[arg(long, value_name = "JOB_ID")]
        job: Option<String>,
    },
    /// Rebuild a namespace's manifest from its segment objects (admin)
    ///
    /// Disaster recovery / snapshot restore: lists the namespace's prefix in
    /// the source, reads every segment footer, and publishes a manifest.
    Import {
        /// Storage namespace as it is laid out in object storage, including
        /// the tenant prefix (e.g. default/my-ns)
        #[arg(short = 'n', long)]
        namespace: String,
        /// Where the segment objects are read from
        #[arg(long, value_enum)]
        source: ImportSource,
        /// Replace the manifest even if the namespace already has segments
        #[arg(long)]
        overwrite: bool,
        /// Request timeout in seconds (every footer is fetched first)
        #[arg(long, value_name = "SECS", default_value_t = 600)]
        timeout: u64,
    },
    /// Admin operations (need the server's KOSHA_ADMIN_API_KEY as --api-key)
    Admin {
        #[command(subcommand)]
        command: AdminCommands,
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
    /// Print a shell completion script to stdout
    #[command(hide = true)]
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Print the kosha(1) man page (roff) to stdout
    #[command(hide = true)]
    Man {
        /// Instead, write kosha.1 and one kosha-<command>.1 page per
        /// subcommand into this directory
        #[arg(long, value_name = "DIR")]
        out_dir: Option<PathBuf>,
    },
}

/// Everything `--body` conflicts with: the query string and every
/// [`SearchArgs`] flag. Listed one by one (not as the `search_flags` group)
/// so clap's error names only the flag actually given.
const SEARCH_FLAG_IDS: [&str; 17] = [
    "query",
    "filter",
    "exact",
    "or",
    "from",
    "sort",
    "search_after",
    "knn_field",
    "knn_vector",
    "k",
    "num_candidates",
    "knn_filter",
    "highlight",
    "pre_tag",
    "post_tag",
    "aggs",
    "no_cache",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompactMode {
    Tiered,
    Full,
}

impl CompactMode {
    fn as_str(self) -> &'static str {
        match self {
            CompactMode::Tiered => "tiered",
            CompactMode::Full => "full",
        }
    }
}

/// Where `kosha import` reads segment objects from. Only S3 exists today;
/// it is still an explicit flag so the command never guesses a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ImportSource {
    /// The server's configured S3 bucket and prefix
    S3,
}

#[derive(Debug, Subcommand)]
enum AdminCommands {
    /// Create an API key for a tenant (Postgres control plane only)
    ///
    /// The key is printed once, on stdout; store it now.
    CreateApiKey {
        /// Tenant the key authenticates as
        #[arg(long)]
        tenant: String,
        /// Free-text note stored with the key
        #[arg(long, default_value = "")]
        description: String,
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
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config_path = cli.config.clone().unwrap_or_else(default_config_path);

    match &cli.command {
        Commands::Profile { command } => {
            run_profile(command, &config_path, cli.json)?;
            return Ok(ExitCode::SUCCESS);
        }
        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            clap_complete::generate(*shell, &mut cmd, "kosha", &mut io::stdout());
            return Ok(ExitCode::SUCCESS);
        }
        Commands::Man { out_dir } => {
            match out_dir {
                Some(dir) => {
                    fs::create_dir_all(dir)?;
                    clap_mangen::generate_to(Cli::command(), dir)?;
                }
                None => clap_mangen::Man::new(Cli::command()).render(&mut io::stdout())?,
            }
            return Ok(ExitCode::SUCCESS);
        }
        _ => {}
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
            body,
            args,
        } => {
            if let Some(body_src) = body {
                let body_val = read_json_arg(&body_src).map_err(|e| format!("--body: {e}"))?;
                let sort: Vec<SortSpec> = body_val
                    .get("sort")
                    .and_then(|s| serde_json::from_value(s.clone()).ok())
                    .unwrap_or_default();
                let max = body_val
                    .get("max_results")
                    .and_then(|m| m.as_u64())
                    .map_or(10, |m| m as usize);
                let value = client.search_body(&namespace, body_val)?;
                print_value(&value, cli.json, |v| {
                    let result: kosha_core::SearchResult = serde_json::from_value(v.clone())?;
                    human_search(&result, &sort, max)
                })?;
            } else {
                let search = match build_search_query(query.unwrap_or_default(), max_results, &args)
                {
                    Ok(search) => search,
                    Err(message) => search_usage_error(message).exit(),
                };
                let sort = search.sort.clone();
                let result = client.search(&namespace, search)?;
                if cli.json {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                } else {
                    human_search(&result, &sort, max_results)?;
                }
            }
        }
        Commands::Exists { namespace, ids } => {
            let value = client.exists(&namespace, &ids)?;
            let missing = value
                .get("missing")
                .and_then(|m| m.as_array())
                .map_or(0, Vec::len);
            print_value(&value, cli.json, |v| {
                human_exists(v, &ids);
                Ok(())
            })?;
            if missing > 0 {
                return Ok(ExitCode::FAILURE);
            }
        }
        Commands::Compact {
            namespace,
            mode,
            max_segment_bytes,
            wait,
            timeout,
            poll_interval,
        } => {
            let mut body = serde_json::json!({
                "namespace": namespace,
                "mode": mode.as_str(),
                "async": true,
            });
            if let Some(bytes) = max_segment_bytes {
                body["max_segment_bytes"] = bytes.into();
            }
            let started = client.compact_namespace(body).map_err(admin_error)?;
            // A server from before async compaction ignores `async` and
            // answers with the finished pass (no `state`).
            let job_id = started.get("job_id").and_then(|j| j.as_str());
            let running = started.get("state").and_then(|s| s.as_str()) == Some("running");
            match (wait, running, job_id) {
                (true, true, Some(job_id)) => {
                    if !cli.json {
                        let resolved = started
                            .get("namespace")
                            .and_then(|n| n.as_str())
                            .unwrap_or(&namespace);
                        eprintln!("compaction job {job_id} started on {resolved}; waiting…");
                    }
                    let job = wait_for_job(
                        || client.compaction_job(job_id).map_err(admin_error),
                        Duration::from_secs(timeout),
                        Duration::from_secs(poll_interval),
                        std::thread::sleep,
                    )
                    .map_err(|e| format!("compaction job {job_id}: {e}"))?;
                    print_value(&job, cli.json, |v| {
                        println!("{}", describe_job(v));
                        Ok(())
                    })?;
                    if job.get("state").and_then(|s| s.as_str()) != Some("succeeded") {
                        return Ok(ExitCode::FAILURE);
                    }
                }
                _ => print_value(&started, cli.json, |v| {
                    if running {
                        let id = job_id.unwrap_or("?");
                        println!(
                            "compaction job {id} started on {} ({})",
                            v.get("namespace")
                                .and_then(|n| n.as_str())
                                .unwrap_or(&namespace),
                            mode.as_str()
                        );
                        println!("check on it: kosha compaction-status --job {id}");
                    } else {
                        println!("{}", describe_pass(v));
                    }
                    Ok(())
                })?,
            }
        }
        Commands::CompactionStatus { job } => match job {
            Some(job_id) => {
                let value = client.compaction_job(&job_id).map_err(admin_error)?;
                print_value(&value, cli.json, |v| {
                    println!("{}", describe_job(v));
                    Ok(())
                })?;
            }
            None => {
                let value = client.compaction_status().map_err(admin_error)?;
                print_value(&value, cli.json, |v| {
                    human_compaction_status(v);
                    Ok(())
                })?;
            }
        },
        Commands::Import {
            namespace,
            source: ImportSource::S3,
            overwrite,
            timeout,
        } => {
            let client = Client::with_timeout(&conn, Duration::from_secs(timeout))?;
            let value = client
                .import_namespace(&namespace, overwrite)
                .map_err(admin_error)?;
            print_value(&value, cli.json, |v| {
                println!(
                    "imported namespace {} from s3: {} segment(s), {} document(s)",
                    v.get("namespace")
                        .and_then(|n| n.as_str())
                        .unwrap_or(&namespace),
                    v.get("segments").and_then(|n| n.as_u64()).unwrap_or(0),
                    v.get("documents").and_then(|n| n.as_u64()).unwrap_or(0),
                );
                Ok(())
            })?;
        }
        Commands::Admin {
            command:
                AdminCommands::CreateApiKey {
                    tenant,
                    description,
                },
        } => {
            let value = client
                .create_api_key(&tenant, &description)
                .map_err(admin_error)?;
            let key = value
                .get("api_key")
                .and_then(|k| k.as_str())
                .ok_or("server response has no api_key")?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                // The key alone goes to stdout so `KEY=$(kosha admin
                // create-api-key …)` captures exactly it.
                eprintln!("created API key for tenant {tenant:?}:");
                println!("{key}");
                eprintln!("this is the only time the key is shown; store it now.");
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
            let value = client
                .rebuild_filter_blooms(&namespace)
                .map_err(admin_error)?;
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
            let value = client
                .backfill_offset_tables(&namespace)
                .map_err(admin_error)?;
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
                Some(src) => Some(read_json_arg(&src).map_err(|e| format!("--body: {e}"))?),
                None => None,
            };
            let value = client.curl(method, &path, body_val)?;
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Commands::Profile { .. } | Commands::Completions { .. } | Commands::Man { .. } => {
            unreachable!("handled before connecting")
        }
    }

    Ok(ExitCode::SUCCESS)
}

/// A `kosha search` usage error found after parsing (a cursor that does
/// not line up with the sort keys, a bad JSON flag), formatted and exiting
/// like clap's own errors, with the search subcommand's usage line.
fn search_usage_error(message: String) -> clap::Error {
    let mut cmd = Cli::command();
    cmd.build();
    match cmd.find_subcommand_mut("search") {
        Some(search) => search.error(clap::error::ErrorKind::ValueValidation, message),
        None => cmd.error(clap::error::ErrorKind::ValueValidation, message),
    }
}

/// Explain an admin route's failure in plain words. The server answers 403
/// when the request's key is not its `KOSHA_ADMIN_API_KEY` (or it has none
/// configured), and 400 when the route needs a backend this server was
/// not built or configured with (Postgres for API keys, S3 for import).
fn admin_error(error: RequestError) -> ClientError {
    match error {
        RequestError::Http(http) if http.status == 403 => ClientError(format!(
            "{} {} was refused (403: {}). Admin commands must authenticate with the \
             server's KOSHA_ADMIN_API_KEY: pass it with --api-key, KOSHA_API_KEY or a \
             profile. A server with no KOSHA_ADMIN_API_KEY set refuses every admin route.",
            http.method, http.path, http.message
        )),
        RequestError::Http(http) if http.status == 400 && backend_unsupported(&http.message) => {
            ClientError(format!(
                "this server does not support {}: {}",
                http.path, http.message
            ))
        }
        RequestError::Http(http) => ClientError(http.to_string()),
        RequestError::Other(other) => other,
    }
}

/// The 400 messages an admin route gives when the server lacks the
/// backend it needs, as opposed to a malformed request.
fn backend_unsupported(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        "postgres",
        "requires the s3 feature",
        "s3 not configured",
        "api keys are disabled",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Poll a compaction job until it leaves `running`, or fail once `timeout`
/// has passed. `fetch` and `sleep` are parameters so the loop is testable
/// without a server or real waiting.
fn wait_for_job<F, S>(
    mut fetch: F,
    timeout: Duration,
    interval: Duration,
    mut sleep: S,
) -> Result<Value, ClientError>
where
    F: FnMut() -> Result<Value, ClientError>,
    S: FnMut(Duration),
{
    let deadline = Instant::now() + timeout;
    loop {
        let job = fetch()?;
        let state = job.get("state").and_then(|s| s.as_str()).unwrap_or("");
        if state != "running" {
            return Ok(job);
        }
        if Instant::now() >= deadline {
            return Err(ClientError(format!(
                "still running after {}s; the job continues on the server \
                 (check with kosha compaction-status --job ID)",
                timeout.as_secs()
            )));
        }
        sleep(interval);
    }
}

/// One line for a finished (synchronous) compaction pass's result.
fn describe_pass(v: &Value) -> String {
    let get = |k: &str| v.get(k).and_then(|n| n.as_u64()).unwrap_or(0);
    if v.get("merged").and_then(|m| m.as_bool()).unwrap_or(false) {
        format!(
            "merged {} segment(s): {} → {} segment(s)",
            get("segments_merged"),
            get("segments_before"),
            get("segments_after")
        )
    } else {
        format!("nothing to merge ({} segment(s))", get("segments_before"))
    }
}

/// One-line summary of a `GET /v1/admin/compaction-jobs/{id}` record.
fn describe_job(v: &Value) -> String {
    let id = v.get("job_id").and_then(|j| j.as_str()).unwrap_or("?");
    let ns = v.get("namespace").and_then(|j| j.as_str()).unwrap_or("?");
    let mode = v.get("mode").and_then(|j| j.as_str()).unwrap_or("?");
    let state = v.get("state").and_then(|j| j.as_str()).unwrap_or("?");
    let duration = v
        .get("duration_ms")
        .and_then(|d| d.as_u64())
        .map(|ms| format!(" in {:.1}s", ms as f64 / 1000.0))
        .unwrap_or_default();
    let detail = match state {
        "succeeded" => v
            .get("result")
            .map(|r| format!(": {}", describe_pass(r)))
            .unwrap_or_default(),
        "failed" => format!(
            ": {}",
            v.get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown error")
        ),
        _ => String::new(),
    };
    format!("compaction job {id} on {ns} ({mode}) {state}{duration}{detail}")
}

fn human_compaction_status(v: &Value) {
    if let Some(s) = v.get("scheduler") {
        let enabled = s.get("enabled").and_then(|e| e.as_bool()).unwrap_or(false);
        let interval = s
            .get("interval_ms")
            .and_then(|i| i.as_u64())
            .map(|ms| format!(", every {ms}ms"))
            .unwrap_or_default();
        println!(
            "scheduler: {}{interval}, {} running (max {})",
            if enabled { "enabled" } else { "disabled" },
            s.get("running").and_then(|r| r.as_u64()).unwrap_or(0),
            s.get("max_concurrent")
                .and_then(|r| r.as_u64())
                .unwrap_or(0),
        );
    }
    if let Some(namespaces) = v.get("namespaces").and_then(|n| n.as_array()) {
        for ns in namespaces {
            let needs = ns
                .get("needs_compaction")
                .and_then(|n| n.as_bool())
                .unwrap_or(false);
            println!(
                "  {}: {} segment(s){}",
                ns.get("namespace").and_then(|n| n.as_str()).unwrap_or("?"),
                ns.get("segments").and_then(|n| n.as_u64()).unwrap_or(0),
                if needs { ", needs compaction" } else { "" }
            );
        }
    }
    for (label, key) in [("running", "running_jobs"), ("recent", "recent_jobs")] {
        let jobs = v
            .get(key)
            .and_then(|j| j.as_array())
            .cloned()
            .unwrap_or_default();
        if !jobs.is_empty() {
            println!("{label} jobs:");
            for job in &jobs {
                println!("  {}", describe_job(job));
            }
        }
    }
}

fn human_exists(v: &Value, ids: &[String]) {
    let listed = |key: &str| -> Vec<String> {
        v.get(key)
            .and_then(|l| l.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|i| i.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let existing = listed("existing");
    for id in ids {
        let status = if existing.contains(id) {
            "exists"
        } else {
            "not found"
        };
        if ids.len() == 1 {
            println!("{status}");
        } else {
            println!("{id}: {status}");
        }
    }
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
        let value = read_json_arg(raw).map_err(|e| format!("--doc: {e}"))?;
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

/// `total_hits` is capped by default (see `SearchQuery.exact_total_hits`) —
/// this prints the "+" a bare number would silently hide, so "10000" never
/// reads as an exact count when it's actually a lower bound. Pulled out as
/// a pure function (no I/O) so the formatting itself is unit-testable.
fn format_total_hits_line(result: &kosha_core::SearchResult) -> String {
    match result.total_hits_relation {
        kosha_core::TotalHitsRelation::Eq => format!("total_hits: {}", result.total_hits),
        kosha_core::TotalHitsRelation::Gte => format!(
            "total_hits: {}+ (capped; pass --exact for an exact count)",
            result.total_hits
        ),
    }
}

/// Plain search output: the hit count, one line per hit (plus its
/// highlight fragments), aggregations as compact JSON, and — for a sorted,
/// full page — the `--search-after` flags that fetch the next page.
fn human_search(
    result: &kosha_core::SearchResult,
    sort: &[SortSpec],
    max_results: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", format_total_hits_line(result));
    for hit in &result.results {
        let title = hit
            .fields
            .iter()
            .find(|f| f.name == "title")
            .map(|f| f.value.as_str())
            .unwrap_or("");
        println!("  [{}] {}  (score={:.3})", hit.doc_id.0, title, hit.score);
        for fragment in hit.highlights.iter().flatten() {
            println!("      … {fragment}");
        }
    }
    if let Some(aggs) = &result.aggregations {
        println!("aggregations: {}", serde_json::to_string(aggs)?);
    }
    if let Some(cursor) = next_search_after(result, sort, max_results) {
        let flags: Vec<String> = cursor
            .iter()
            .map(|v| format!("--search-after {}", shell_quote(v)))
            .collect();
        println!("next page: {}", flags.join(" "));
    }
    Ok(())
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
    fn format_total_hits_line_flags_capped_counts() {
        let mk = |total_hits, relation| kosha_core::SearchResult {
            results: Vec::new(),
            total_hits,
            total_hits_relation: relation,
            aggregations: None,
            knn_degraded_segments: None,
            rerank_degraded: None,
            rerank_degraded_reason: None,
            knn_mode: None,
            knn_filtered_candidates: None,
        };
        assert_eq!(
            format_total_hits_line(&mk(3, kosha_core::TotalHitsRelation::Eq)),
            "total_hits: 3"
        );
        // A capped count must never print as a bare number — that reads as
        // exact when it's actually a lower bound (the bug this guards).
        let capped = format_total_hits_line(&mk(10_000, kosha_core::TotalHitsRelation::Gte));
        assert!(capped.contains("10000+"), "{capped}");
        assert!(capped.contains("--exact"), "{capped}");
    }

    #[test]
    fn parses_search_exact_flag() {
        let cli = Cli::try_parse_from([
            "kosha",
            "--host",
            "http://localhost:8080",
            "search",
            "-n",
            "demo",
            "breach",
            "--exact",
        ])
        .unwrap();
        match cli.command {
            Commands::Search { args, .. } => assert!(args.exact),
            _ => panic!("expected search"),
        }
    }

    #[test]
    fn build_search_query_wires_exact_flag_to_search_query() {
        // `parses_search_exact_flag` proves clap surfaces `exact: bool`,
        // but stops there — the actual contract is that this flag flows
        // through `run()` -> `build_search_query` -> `SearchQuery.exact_total_hits`
        // as `Some(true)`, which is what the `kosha search --exact` user
        // message in `format_total_hits_line` promises ("pass --exact for
        // an exact count"). This test closes that loop end to end without
        // standing up a real HTTP server — exactly what the #105 review
        // flagged as missing.
        let exact = SearchArgs {
            exact: true,
            ..Default::default()
        };
        let with_exact = build_search_query("breach".to_string(), 5, &exact).unwrap();
        assert_eq!(
            with_exact.exact_total_hits,
            Some(true),
            "--exact must wire to SearchQuery.exact_total_hits = Some(true)"
        );
        assert_eq!(with_exact.total_hits_cap, None);
        assert_eq!(with_exact.query_text, "breach");
        assert_eq!(with_exact.max_results, 5);
        assert_eq!(with_exact.from, 0, "`from` stays 0 without --from");
        assert_eq!(
            with_exact.operator, None,
            "--exact alone must not set --or's field"
        );

        // Without `--exact`: defer to the engine-wide default. The
        // important assertion is `None` (NOT `Some(false)`), so the engine
        // default — which the server reads the same way — still applies.
        let without_exact =
            build_search_query("breach".to_string(), 5, &SearchArgs::default()).unwrap();
        assert_eq!(
            without_exact.exact_total_hits, None,
            "absence of --exact must defer to the engine default via None, \
             not eagerly set Some(false)"
        );
        assert_eq!(without_exact.total_hits_cap, None);
        assert_eq!(without_exact.operator, None);
    }

    #[test]
    fn build_search_query_wires_or_flag_to_search_query_operator() {
        // Sibling of `build_search_query_wires_exact_flag_to_search_query`
        // for the `--or` flag: `parses_search_or_flag` proves clap surfaces
        // `or: bool`, this closes the rest of the loop through
        // `build_search_query` -> `SearchQuery.operator`.
        let or = SearchArgs {
            or: true,
            ..Default::default()
        };
        let with_or = build_search_query("breach recall".to_string(), 5, &or).unwrap();
        assert_eq!(
            with_or.operator,
            Some(kosha_core::QueryOperator::Or),
            "--or must wire to SearchQuery.operator = Some(QueryOperator::Or)"
        );

        // Without `--or`: defer to the engine-wide default operator (AND) —
        // `None`, not an eagerly-set `Some(QueryOperator::And)`.
        let without_or =
            build_search_query("breach recall".to_string(), 5, &SearchArgs::default()).unwrap();
        assert_eq!(
            without_or.operator, None,
            "absence of --or must defer to the engine default via None"
        );
    }

    #[test]
    fn parses_search_or_flag() {
        let cli = Cli::try_parse_from([
            "kosha",
            "--host",
            "http://localhost:8080",
            "search",
            "-n",
            "demo",
            "breach recall",
            "--or",
        ])
        .unwrap();
        match cli.command {
            Commands::Search { args, .. } => assert!(args.or),
            _ => panic!("expected search"),
        }
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

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        let mut argv = vec!["kosha", "--host", "http://localhost:8080"];
        argv.extend_from_slice(args);
        Cli::try_parse_from(argv)
    }

    fn search_args(args: &[&str]) -> SearchArgs {
        match parse(args).unwrap().command {
            Commands::Search { args, .. } => *args,
            _ => panic!("expected search"),
        }
    }

    #[test]
    fn every_search_flag_conflicts_with_body() {
        let flags: &[&[&str]] = &[
            &["--filter", "{}"],
            &["--exact"],
            &["--or"],
            &["--from", "10"],
            &["--sort", "created_at:desc"],
            &["--search-after", "x"],
            &["--knn-field", "v", "--knn-vector", "[1]"],
            &["--highlight", "content"],
            &["--aggs", "{}"],
            &["--no-cache"],
        ];
        for flag in flags {
            let mut argv = vec!["search", "-n", "demo", "--body", "{}"];
            argv.extend_from_slice(flag);
            let err = parse(&argv).expect_err(&format!("{flag:?} must conflict with --body"));
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{flag:?}: {err}"
            );
        }
        // --max has always been accepted next to --body.
        parse(&["search", "-n", "demo", "--body", "{}", "--max", "5"]).unwrap();
        // A query string conflicts with --body too.
        assert_eq!(
            parse(&["search", "-n", "demo", "q", "--body", "{}"])
                .unwrap_err()
                .kind(),
            clap::error::ErrorKind::ArgumentConflict
        );
    }

    #[test]
    fn body_conflict_list_covers_every_search_flag() {
        let cmd = Cli::command();
        let search = cmd.find_subcommand("search").unwrap();
        let mut flags: Vec<String> = search
            .get_arguments()
            .filter(|a| {
                !["namespace", "max_results", "body", "help", "version"]
                    .contains(&a.get_id().as_str())
                    && !a.is_global_set()
            })
            .map(|a| a.get_id().to_string())
            .collect();
        let mut listed: Vec<String> = SEARCH_FLAG_IDS.iter().map(|s| s.to_string()).collect();
        flags.sort();
        listed.sort();
        assert_eq!(
            flags, listed,
            "a new search flag must be added to SEARCH_FLAG_IDS"
        );
    }

    #[test]
    fn search_usage_error_names_the_search_subcommand() {
        let err = search_usage_error("bad cursor".into()).to_string();
        assert!(err.contains("bad cursor"), "{err}");
        assert!(err.contains("kosha search"), "{err}");
    }

    #[test]
    fn search_needs_a_query_body_or_knn() {
        assert_eq!(
            parse(&["search", "-n", "demo"]).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        // kNN alone is a valid (semantic) search.
        parse(&[
            "search",
            "-n",
            "demo",
            "--knn-field",
            "v",
            "--knn-vector",
            "[1,2]",
        ])
        .unwrap();
    }

    #[test]
    fn knn_flags_require_each_other() {
        for argv in [
            &["search", "-n", "demo", "q", "--knn-field", "v"][..],
            &["search", "-n", "demo", "q", "--knn-vector", "[1]"],
            &["search", "-n", "demo", "q", "--k", "5"],
            &["search", "-n", "demo", "q", "--num-candidates", "5"],
            &["search", "-n", "demo", "q", "--knn-filter", "{}"],
            &["search", "-n", "demo", "q", "--pre-tag", "<em>"],
        ] {
            assert_eq!(
                parse(argv).unwrap_err().kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{argv:?}"
            );
        }
    }

    #[test]
    fn bad_sort_order_is_a_clap_error() {
        let err = parse(&["search", "-n", "demo", "q", "--sort", "created_at:newest"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
        assert!(err.to_string().contains("newest"), "{err}");
    }

    #[test]
    fn search_flags_map_through_to_search_query() {
        let args = search_args(&[
            "search",
            "-n",
            "demo",
            "breach",
            "--from",
            "20",
            "--sort",
            "created_at:desc",
            "--sort",
            "_id",
            "--search-after",
            "2026-01-01",
            "--search-after",
            "doc-9",
            "--knn-field",
            "embedding",
            "--knn-vector",
            "[0.5, 0.25]",
            "--k",
            "30",
            "--highlight",
            "content",
            "--pre-tag",
            "[",
            "--post-tag",
            "]",
            "--aggs",
            r#"{"by_court":{"terms":{"field":"court"}}}"#,
            "--no-cache",
        ]);
        let q = build_search_query("breach".into(), 10, &args).unwrap();
        assert_eq!(q.from, 20);
        assert_eq!(q.sort.len(), 2);
        assert_eq!(
            q.search_after,
            Some(vec!["2026-01-01".to_string(), "doc-9".to_string()])
        );
        let knn = q.knn.as_ref().unwrap();
        assert_eq!((knn.field.as_str(), knn.k), ("embedding", 30));
        assert!(q.is_hybrid());
        assert!(serde_json::to_string(&q.highlight)
            .unwrap()
            .contains(r#"["["]"#));
        assert!(q.aggs.contains_key("by_court"));
        assert_eq!(q.no_cache, Some(true));

        // A cursor that does not line up with the sort keys is caught
        // before any request.
        let args = search_args(&[
            "search",
            "-n",
            "demo",
            "q",
            "--sort",
            "a",
            "--sort",
            "b",
            "--search-after",
            "1",
        ]);
        assert!(build_search_query("q".into(), 10, &args).is_err());
    }

    #[test]
    fn parses_exists() {
        match parse(&["exists", "-n", "demo", "doc-1", "doc-2"])
            .unwrap()
            .command
        {
            Commands::Exists { namespace, ids } => {
                assert_eq!(namespace, "demo");
                assert_eq!(ids, vec!["doc-1", "doc-2"]);
            }
            _ => panic!("expected exists"),
        }
        assert_eq!(
            parse(&["exists", "-n", "demo"]).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn human_exists_lists_each_id() {
        // Smoke: must not panic on either shape.
        let v = serde_json::json!({"existing": ["a"], "missing": ["b"]});
        human_exists(&v, &["a".into(), "b".into()]);
        human_exists(&serde_json::json!({}), &["a".into()]);
    }

    #[test]
    fn parses_compact_and_status() {
        match parse(&["compact", "-n", "demo"]).unwrap().command {
            Commands::Compact {
                namespace,
                mode,
                wait,
                max_segment_bytes,
                ..
            } => {
                assert_eq!(namespace, "demo");
                assert_eq!(mode, CompactMode::Tiered);
                assert!(!wait);
                assert_eq!(max_segment_bytes, None);
            }
            _ => panic!("expected compact"),
        }
        match parse(&[
            "compact",
            "-n",
            "demo",
            "--mode",
            "full",
            "--wait",
            "--timeout",
            "60",
        ])
        .unwrap()
        .command
        {
            Commands::Compact {
                mode,
                wait,
                timeout,
                ..
            } => {
                assert_eq!(mode, CompactMode::Full);
                assert!(wait);
                assert_eq!(timeout, 60);
            }
            _ => panic!("expected compact"),
        }
        // --timeout only means something with --wait.
        assert_eq!(
            parse(&["compact", "-n", "demo", "--timeout", "60"])
                .unwrap_err()
                .kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert_eq!(
            parse(&["compact", "-n", "demo", "--mode", "everything"])
                .unwrap_err()
                .kind(),
            clap::error::ErrorKind::InvalidValue
        );
        match parse(&["compaction-status", "--job", "cj-1-1"])
            .unwrap()
            .command
        {
            Commands::CompactionStatus { job } => assert_eq!(job.as_deref(), Some("cj-1-1")),
            _ => panic!("expected compaction-status"),
        }
    }

    #[test]
    fn import_needs_an_explicit_source() {
        assert_eq!(
            parse(&["import", "-n", "default/demo"]).unwrap_err().kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        match parse(&[
            "import",
            "-n",
            "default/demo",
            "--source",
            "s3",
            "--overwrite",
        ])
        .unwrap()
        .command
        {
            Commands::Import {
                namespace,
                source,
                overwrite,
                timeout,
            } => {
                assert_eq!(namespace, "default/demo");
                assert_eq!(source, ImportSource::S3);
                assert!(overwrite);
                assert_eq!(timeout, 600);
            }
            _ => panic!("expected import"),
        }
    }

    #[test]
    fn parses_admin_create_api_key() {
        match parse(&["admin", "create-api-key", "--tenant", "acme"])
            .unwrap()
            .command
        {
            Commands::Admin {
                command:
                    AdminCommands::CreateApiKey {
                        tenant,
                        description,
                    },
            } => {
                assert_eq!(tenant, "acme");
                assert_eq!(description, "");
            }
            _ => panic!("expected admin create-api-key"),
        }
        assert!(parse(&["admin", "create-api-key"]).is_err());
    }

    fn http_error(status: u16, message: &str) -> RequestError {
        RequestError::Http(crate::client::HttpError {
            method: Method::POST,
            path: "/v1/admin/api-keys".into(),
            status,
            message: message.into(),
        })
    }

    #[test]
    fn admin_errors_read_plainly() {
        let forbidden = admin_error(http_error(403, "admin credential required")).to_string();
        assert!(forbidden.contains("KOSHA_ADMIN_API_KEY"), "{forbidden}");
        assert!(forbidden.contains("--api-key"), "{forbidden}");

        let pg = admin_error(http_error(
            400,
            "admin API requires postgres feature (compile with --features postgres)",
        ))
        .to_string();
        assert!(pg.starts_with("this server does not support"), "{pg}");
        let s3 =
            admin_error(http_error(400, "import-namespace requires the s3 feature")).to_string();
        assert!(s3.starts_with("this server does not support"), "{s3}");

        // An ordinary 400 keeps the server's message as is.
        let other = admin_error(http_error(400, "missing 'tenant_id'")).to_string();
        assert!(other.contains("400: missing 'tenant_id'"), "{other}");
    }

    #[test]
    fn wait_for_job_polls_until_finished() {
        let mut states = vec!["running", "running", "succeeded"].into_iter();
        let mut sleeps = 0;
        let job = wait_for_job(
            || Ok(serde_json::json!({"job_id": "cj-1", "state": states.next().unwrap()})),
            Duration::from_secs(3600),
            Duration::from_millis(1),
            |_| sleeps += 1,
        )
        .unwrap();
        assert_eq!(job["state"], "succeeded");
        assert_eq!(sleeps, 2);

        // Timeout: a job still running past the deadline is an error, not a hang.
        let err = wait_for_job(
            || Ok(serde_json::json!({"state": "running"})),
            Duration::ZERO,
            Duration::from_millis(1),
            |_| {},
        )
        .unwrap_err();
        assert!(err.to_string().contains("still running"), "{err}");

        // A failed job ends the wait too; the caller reports it.
        let job = wait_for_job(
            || Ok(serde_json::json!({"state": "failed", "error": "boom"})),
            Duration::from_secs(1),
            Duration::from_millis(1),
            |_| {},
        )
        .unwrap();
        assert!(describe_job(&job).contains("failed: boom"));
    }

    #[test]
    fn describe_job_summarizes_results() {
        let job = serde_json::json!({
            "job_id": "cj-1-1", "namespace": "default/demo", "mode": "tiered",
            "state": "succeeded", "duration_ms": 1500,
            "result": {"merged": true, "segments_before": 8, "segments_after": 2,
                       "segments_merged": 7, "max_segment_bytes": 0, "not_hydrated": []}
        });
        assert_eq!(
            describe_job(&job),
            "compaction job cj-1-1 on default/demo (tiered) succeeded in 1.5s: \
             merged 7 segment(s): 8 → 2 segment(s)"
        );
        let idle = serde_json::json!({"merged": false, "segments_before": 1});
        assert_eq!(describe_pass(&idle), "nothing to merge (1 segment(s))");
    }

    #[test]
    fn completions_and_man_are_hidden_and_render() {
        let help = Cli::command().render_long_help().to_string();
        assert!(!help.contains("completions"), "{help}");
        assert!(!help.contains("\n  man "), "{help}");
        assert!(help.contains("exists"));

        for shell in [
            clap_complete::Shell::Bash,
            clap_complete::Shell::Zsh,
            clap_complete::Shell::Fish,
            clap_complete::Shell::PowerShell,
            clap_complete::Shell::Elvish,
        ] {
            let mut out = Vec::new();
            clap_complete::generate(shell, &mut Cli::command(), "kosha", &mut out);
            let script = String::from_utf8(out).unwrap();
            assert!(script.contains("compaction-status"), "{shell}");
            assert!(script.contains("knn-vector"), "{shell}");
        }

        let mut page = Vec::new();
        clap_mangen::Man::new(Cli::command())
            .render(&mut page)
            .unwrap();
        let page = String::from_utf8(page).unwrap();
        assert!(page.starts_with(".ie"), "{}", &page[..40]);
        assert!(page.contains(".TH kosha 1"), "{page}");
        assert!(page.contains("search"));

        match parse(&["completions", "zsh"]).unwrap().command {
            Commands::Completions { shell } => assert_eq!(shell, clap_complete::Shell::Zsh),
            _ => panic!("expected completions"),
        }
        assert!(matches!(
            parse(&["man"]).unwrap().command,
            Commands::Man { out_dir: None }
        ));

        let dir = std::env::temp_dir().join(format!("kosha-cli-man-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        clap_mangen::generate_to(Cli::command(), &dir).unwrap();
        for page in ["kosha.1", "kosha-search.1", "kosha-admin-create-api-key.1"] {
            assert!(dir.join(page).exists(), "{page}");
        }
        assert!(
            !dir.join("kosha-completions.1").exists(),
            "hidden commands get no page"
        );
        std::fs::remove_dir_all(&dir).unwrap();
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
