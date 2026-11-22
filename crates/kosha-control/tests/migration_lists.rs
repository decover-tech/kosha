//! The migrations are listed in three places that must agree (#426):
//! `migrate_once` (`src/postgres.rs`) embeds them in the server,
//! `make db-migrate` runs them as the database admin, and
//! `002_grant_kosha_role.sql` hands every table they create to the `kosha`
//! role the server connects as. A migration or a table missing from one of
//! them only shows on a real instance, so this checks the files themselves.

use std::path::Path;

fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every schema migration, in name order. 000 creates the role and the
/// database and 002 assigns ownership: neither is embedded in the server.
fn schema_migrations() -> Vec<String> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".sql"))
        .filter(|name| !name.starts_with("000_") && !name.starts_with("002_"))
        .collect();
    names.sort();
    assert!(names.len() >= 7, "migrations not found: {names:?}");
    names
}

/// The migrations `text` names after each occurrence of `marker`, in order.
fn listed(text: &str, marker: &str) -> Vec<String> {
    text.match_indices(marker)
        .map(|(at, _)| {
            let rest = &text[at + marker.len()..];
            let end = rest.find(".sql").expect("a .sql file name") + ".sql".len();
            rest[..end].to_string()
        })
        .collect()
}

#[test]
fn the_server_and_make_db_migrate_apply_the_same_migrations_in_the_same_order() {
    let embedded = listed(&read("src/postgres.rs"), "include_str!(\"../migrations/");
    let makefile = read("../../Makefile");
    let by_hand: Vec<String> = listed(&makefile, "-f crates/kosha-control/migrations/")
        .into_iter()
        .filter(|name| !name.starts_with("000_"))
        .collect();

    // `make db-migrate` ends with the ownership grants.
    assert_eq!(
        by_hand.last().map(String::as_str),
        Some("002_grant_kosha_role.sql")
    );
    assert_eq!(embedded, by_hand[..by_hand.len() - 1]);

    let mut sorted = embedded.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        schema_migrations(),
        "a migration file is not applied"
    );
    // 001 creates the schema the others add to.
    assert_eq!(embedded[0], "001_create_kosha_tables.sql");
}

#[test]
fn every_migrated_table_is_handed_to_the_server_role() {
    let grants = read("migrations/002_grant_kosha_role.sql");
    let mut tables = Vec::new();
    for migration in schema_migrations() {
        let sql = read(&format!("migrations/{migration}"));
        const CREATE: &str = "CREATE TABLE IF NOT EXISTS kosha.";
        for (at, _) in sql.match_indices(CREATE) {
            let rest = &sql[at + CREATE.len()..];
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            tables.push((migration.clone(), rest[..end].to_string()));
        }
        assert!(
            !sql.contains("CREATE TABLE kosha."),
            "{migration}: CREATE TABLE without IF NOT EXISTS is not re-runnable"
        );
    }
    assert!(tables.len() >= 16, "tables not found: {tables:?}");
    for (migration, table) in tables {
        let owned = grants.lines().any(|line| {
            let line = line.trim();
            line == format!("ALTER TABLE kosha.{table} OWNER TO kosha;")
                || line == format!("ALTER TABLE IF EXISTS kosha.{table} OWNER TO kosha;")
        });
        assert!(
            owned,
            "kosha.{table} ({migration}) has no 'OWNER TO kosha' in 002_grant_kosha_role.sql"
        );
    }
}
