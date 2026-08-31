//! Model: Grok 4.6
//! Engine CREATE/ALTER surface measurement for V3-6 types (pre-product).
//! pins: v3-6-v3-types/C-001

use std::fs;
use std::path::{Path, PathBuf};

use super::super::*;
use super::common::*;

fn find_ledger(dir: &Path, suffix: &str) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_ledger(&path, suffix) {
                return Some(found);
            }
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with(suffix))
        {
            return Some(path);
        }
    }
    None
}

#[tokio::test]
async fn v3_types_oracle_matrix_is_the_c001_record() {
    let ledgers = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("repo")
        .join("task/ledgers");
    let ledger = find_ledger(&ledgers, "v3-6-v3-types-ledger.md")
        .expect("the V3-6 ledger lives somewhere under task/ledgers/");
    let text = fs::read_to_string(&ledger).expect("C-001 ledger");
    assert!(
        text.contains("arrow.parquet.variant")
            && text.contains("UNSUPPORTED_DATATYPE")
            && text.contains("cannot visit arrow data type: null")
            && text.contains("setting default values in Spark is currently unsupported")
            && text.contains("missing column"),
        "C-001 matrix must stay in the ledger"
    );
}

#[tokio::test]
async fn engine_create_timestamp_ns_unknown_variant_and_default_today() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    for (table, type_sql) in [
        ("t_tsns", "timestamp_ns"),
        ("t_tstzns", "timestamptz_ns"),
        ("t_unknown", "UNKNOWN"),
        ("t_variant", "VARIANT"),
    ] {
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_sql}) USING iceberg \
                 TBLPROPERTIES ('format-version' = '3')"
            ),
        )
        .await
        .expect_err("v3 type CREATE must not silently succeed before this unit lands it")
        .to_string();
        assert!(
            !err.is_empty(),
            "{type_sql} CREATE must refuse with a message"
        );
        let exists = catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap();
        assert!(!exists, "refused CREATE must leave no `{table}` behind");
    }

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.plain (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("plain v3 CREATE");
    let add_default = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.plain ADD COLUMN tag STRING DEFAULT 'x'",
    )
    .await
    .expect_err("ADD COLUMN DEFAULT is not consumed yet");
    let text = add_default.to_string();
    assert!(
        text.to_ascii_lowercase().contains("default")
            || text.to_ascii_lowercase().contains("option")
            || text.to_ascii_lowercase().contains("not supported"),
        "ADD COLUMN DEFAULT must refuse naming the option: {text}"
    );
}
