//! Process-level check of `kosha-server reconcile-usage` (#348): the real
//! binary dispatches the subcommand, and a run that cannot compare anything
//! exits 2, which a CronJob can tell apart from "out of tolerance" (1).

use std::process::{Command, Output};

fn run(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kosha-server"));
    cmd.arg("reconcile-usage")
        .args(args)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default());
    for (name, value) in env {
        cmd.env(name, value);
    }
    cmd.output().expect("spawn kosha-server")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn help_prints_the_usage_and_exits_zero() {
    let out = run(&["--help"], &[]);
    assert_eq!(out.status.code(), Some(0), "stderr: {}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.starts_with("usage: kosha-server reconcile-usage"),
        "{stdout}"
    );
}

#[test]
fn an_invalid_setting_stops_the_run_before_it_reads_anything() {
    for (args, env, names) in [
        (&["--tolerance", "lots"][..], &[][..], "--tolerance"),
        (&["--no-such-flag", "1"][..], &[][..], "--no-such-flag"),
        (
            &[][..],
            &[("KOSHA_USAGE_RECONCILE_TOLERANCE", "5%")][..],
            "KOSHA_USAGE_RECONCILE_TOLERANCE",
        ),
        (
            &[][..],
            &[("KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE", "1m")][..],
            "KOSHA_USAGE_RECONCILE_MAX_SNAPSHOT_AGE",
        ),
    ] {
        let out = run(args, env);
        let stderr = stderr(&out);
        assert_eq!(out.status.code(), Some(2), "{args:?} {env:?}: {stderr}");
        assert!(
            stderr.contains("reconcile-usage failed") && stderr.contains(names),
            "{args:?} {env:?}: {stderr}"
        );
        assert!(out.stdout.is_empty(), "no report on a failed run");
    }
}

/// Without a control plane to read there is nothing to reconcile: exit 2,
/// not a report that says every namespace is fine.
#[test]
fn without_a_database_the_run_fails_instead_of_reporting_nothing_wrong() {
    let out = run(&[], &[]);
    let stderr = stderr(&out);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("reconcile-usage failed"), "{stderr}");
    assert!(
        stderr.contains("DATABASE_URL") || stderr.contains("`postgres` feature"),
        "{stderr}"
    );
    assert!(out.stdout.is_empty());
}
