use std::path::Path;

use repark_core::ReparkSession;

use super::call_orphan_cotenancy::{
    age_tree, create_with_row, file_spellings, ids, load, memory_session, metadata_files, walk,
};
use super::call_orphan_scope::{
    call_error, call_rows, plan_message, plant, register_file_list, submit,
};
use super::common::*;
use crate::call::remove_orphan_files::{
    metadata_probe_ancestors, refuse_scan_over_foreign_metadata,
};

#[test]
fn call_orphan_ancestor_enumeration_walks_to_the_storage_root_or_the_own_location() {
    let cases: [(&str, &str, &[&str]); 9] = [
        (
            "/w/ns/b/data/x",
            "/w/ns/a",
            &["/w/ns/b/data", "/w/ns/b", "/w/ns", "/w", "/"],
        ),
        (
            "s3://bkt/w/ns/b/data",
            "s3://bkt/w/ns/a",
            &[
                "s3://bkt/w/ns/b",
                "s3://bkt/w/ns",
                "s3://bkt/w",
                "s3://bkt/",
            ],
        ),
        (
            "file:///w/ns/b/data",
            "/w/ns/a",
            &["file:///w/ns/b", "file:///w/ns", "file:///w", "file:///"],
        ),
        (
            "file:/w/ns/b/data",
            "/w/ns/a",
            &["file:/w/ns/b", "file:/w/ns", "file:/w", "file:/"],
        ),
        (
            "/w/ns/a/../b/data/",
            "/w/ns/a",
            &["/w/ns/b", "/w/ns", "/w", "/"],
        ),
        ("/w/ns/a/data/x", "/w/ns/a", &["/w/ns/a/data", "/w/ns/a"]),
        ("file:///w/ns/a/data", "/w/ns/a", &["file:///w/ns/a"]),
        ("s3://bkt/w/a/data", "s3://bkt/w/a", &["s3://bkt/w/a"]),
        ("file:///w/ns/a", "/w/ns/a", &[]),
    ];
    for (scan, own, expected) in cases {
        assert_eq!(
            metadata_probe_ancestors(scan, own),
            expected.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "{scan} with own {own}"
        );
    }
}

fn ancestor_metadata_refusal(
    table_arg: &str,
    scan: &str,
    ancestor: &str,
    file: &str,
    uuid: &str,
    own_uuid: &str,
) -> String {
    format!(
        "CALL remove_orphan_files refuses to sweep `{table_arg}`: path `{scan}` lies inside \
         `{ancestor}`, whose metadata directory holds `{file}`, the metadata file of another \
         table (table-uuid `{uuid}`; the swept table's is `{own_uuid}`), such as a table of \
         another catalog or session on the same warehouse. This procedure deletes every file the \
         swept table's own metadata does not reference, which would include that table's live \
         files. Give the table its own LOCATION (`CREATE TABLE ... LOCATION '<path>'`), then \
         sweep it."
    )
}

async fn assert_sibling_data_scan_refused(
    sweeper: &ReparkSession,
    sweeper_catalog: &str,
    owner: &ReparkSession,
    owner_catalog: &str,
    warehouse: &TempDir,
) {
    let swept = load(sweeper, sweeper_catalog, &["ns", "a"]).await;
    let foreign = load(owner, owner_catalog, &["ns", "b"]).await;
    let other_dir = warehouse.path().join("ns").join("b");
    assert_eq!(
        foreign.metadata().location(),
        other_dir.display().to_string()
    );
    let first_foreign = metadata_files(&foreign)
        .into_iter()
        .next()
        .expect("the other table wrote metadata files");
    let foreign_uuid = foreign.metadata().uuid().to_string();
    let own_uuid = swept.metadata().uuid().to_string();
    let orphan = plant(&other_dir, "orphan-file.parquet", 10);
    let deeper = other_dir.join("data").join("sub");
    std::fs::create_dir_all(&deeper).unwrap();
    std::fs::write(deeper.join("deeper-orphan.parquet"), b"PAR1junk").unwrap();
    age_tree(&warehouse.path().join("ns"), 10);
    let before = walk(&other_dir);

    for scan_dir in [other_dir.join("data"), deeper] {
        for (scan, ancestor) in file_spellings(&scan_dir)
            .into_iter()
            .zip(file_spellings(&other_dir))
        {
            let err = call_error(
                sweeper,
                &format!(
                    "CALL {sweeper_catalog}.system.remove_orphan_files(table => 'ns.a', \
                     location => '{scan}')"
                ),
            )
            .await;
            assert_eq!(
                plan_message(err),
                ancestor_metadata_refusal(
                    "ns.a",
                    &scan,
                    &ancestor,
                    &first_foreign,
                    &foreign_uuid,
                    &own_uuid
                ),
                "{scan}"
            );
        }
    }
    register_file_list(sweeper, &[(orphan.display().to_string(), 0)]).await;
    let scan = other_dir.join("data").display().to_string();
    let err = call_error(
        sweeper,
        &format!(
            "CALL {sweeper_catalog}.system.remove_orphan_files(table => 'ns.a', \
             file_list_view => 'v', location => '{scan}')"
        ),
    )
    .await;
    assert_eq!(
        plan_message(err),
        ancestor_metadata_refusal(
            "ns.a",
            &scan,
            &other_dir.display().to_string(),
            &first_foreign,
            &foreign_uuid,
            &own_uuid
        ),
        "file_list_view"
    );
    assert_eq!(walk(&other_dir), before, "a refused sweep deletes nothing");
}

#[tokio::test]
async fn call_orphan_ancestor_two_catalogs_sibling_data_dir_scan_refuses() {
    let warehouse = TempDir::new().unwrap();
    let session = memory_session(&warehouse, &["m1", "m2"]).await;
    for catalog in ["m1", "m2"] {
        submit(&session, &format!("CREATE NAMESPACE {catalog}.ns")).await;
    }
    create_with_row(&session, "m1.ns.a", 1).await;
    create_with_row(&session, "m2.ns.b", 2).await;

    assert_sibling_data_scan_refused(&session, "m1", &session, "m2", &warehouse).await;
    assert_eq!(ids(&session, "m1.ns.a").await, vec![1]);
    assert_eq!(ids(&session, "m2.ns.b").await, vec![2]);
}

#[tokio::test]
async fn call_orphan_ancestor_two_sessions_sibling_data_dir_scan_refuses() {
    let warehouse = TempDir::new().unwrap();
    let first = memory_session(&warehouse, &["ice"]).await;
    let second = memory_session(&warehouse, &["ice"]).await;
    for session in [&first, &second] {
        submit(session, "CREATE NAMESPACE ice.ns").await;
    }
    create_with_row(&first, "ice.ns.a", 1).await;
    create_with_row(&second, "ice.ns.b", 2).await;

    assert_sibling_data_scan_refused(&first, "ice", &second, "ice", &warehouse).await;
    assert_eq!(ids(&first, "ice.ns.a").await, vec![1]);
    assert_eq!(ids(&second, "ice.ns.b").await, vec![2]);
}

async fn two_catalog_session(warehouse: &TempDir) -> ReparkSession {
    let session = memory_session(warehouse, &["m1", "m2"]).await;
    for catalog in ["m1", "m2"] {
        submit(&session, &format!("CREATE NAMESPACE {catalog}.ns")).await;
    }
    create_with_row(&session, "m1.ns.a", 1).await;
    create_with_row(&session, "m2.ns.b", 2).await;
    age_tree(&warehouse.path().join("ns"), 10);
    session
}

async fn assert_swept(session: &ReparkSession, scan: &Path, orphan: &Path) {
    let policy = session.catalogs_snapshot().location_policy("m1");
    let swept = load(session, "m1", &["ns", "a"]).await;
    for spelling in file_spellings(scan) {
        refuse_scan_over_foreign_metadata(policy.as_ref(), &swept, &spelling, "ns.a")
            .await
            .unwrap_or_else(|error| panic!("{spelling}: {error}"));
    }
    std::fs::write(orphan, b"PAR1junk").unwrap();
    age_tree(scan, 10);
    let listed = call_rows(
        session,
        &format!(
            "CALL m1.system.remove_orphan_files(table => 'ns.a', location => '{}')",
            scan.display()
        ),
    )
    .await
    .expect("no table's metadata lies on the path or above it");
    assert_eq!(listed, vec![orphan.display().to_string()]);
    assert!(!orphan.exists());
}

#[tokio::test]
async fn call_orphan_ancestor_own_data_dir_scan_beside_another_catalog_deletes_the_orphan() {
    let warehouse = TempDir::new().unwrap();
    let session = two_catalog_session(&warehouse).await;
    let own_dir = warehouse.path().join("ns").join("a");
    let other_before = walk(&warehouse.path().join("ns").join("b"));
    let orphan = own_dir.join("data").join("orphan-file.parquet");

    assert_swept(&session, &own_dir.join("data"), &orphan).await;
    assert_eq!(walk(&warehouse.path().join("ns").join("b")), other_before);
    assert_eq!(ids(&session, "m1.ns.a").await, vec![1]);
    assert_eq!(ids(&session, "m2.ns.b").await, vec![2]);
}

#[tokio::test]
async fn call_orphan_ancestor_scan_with_no_table_above_it_is_swept() {
    let warehouse = TempDir::new().unwrap();
    let session = two_catalog_session(&warehouse).await;
    let scratch = warehouse.path().join("scratch").join("x");
    std::fs::create_dir_all(&scratch).unwrap();
    let other_before = walk(&warehouse.path().join("ns").join("b"));

    assert_swept(&session, &scratch, &scratch.join("stale.parquet")).await;
    assert_eq!(walk(&warehouse.path().join("ns").join("b")), other_before);
    assert_eq!(ids(&session, "m1.ns.a").await, vec![1]);
    assert_eq!(ids(&session, "m2.ns.b").await, vec![2]);
}

async fn nested_table_session(warehouse: &TempDir, host: &str, nested: &str) -> ReparkSession {
    let catalogs: Vec<&str> = if host == nested {
        vec![host]
    } else {
        vec![host, nested]
    };
    let session = memory_session(warehouse, &catalogs).await;
    submit(&session, &format!("CREATE NAMESPACE {host}.a")).await;
    submit(&session, &format!("CREATE NAMESPACE {nested}.o")).await;
    create_with_row(&session, &format!("{host}.a.t"), 1).await;
    submit(
        &session,
        &format!(
            "CREATE TABLE {nested}.o.x (id INT) LOCATION '{}'",
            warehouse.path().join("a").join("t").join("x").display()
        ),
    )
    .await;
    submit(&session, &format!("INSERT INTO {nested}.o.x VALUES (2)")).await;
    plant(
        &warehouse.path().join("a").join("t").join("x"),
        "orphan-file.parquet",
        10,
    );
    age_tree(&warehouse.path().join("a"), 10);
    session
}

async fn assert_nested_scans_refused(
    session: &ReparkSession,
    host: &str,
    nested: &str,
    warehouse: &TempDir,
    scan_equal_refusal: impl Fn(&str, &str) -> String,
) {
    let swept = load(session, host, &["a", "t"]).await;
    let foreign = load(session, nested, &["o", "x"]).await;
    let nested_dir = warehouse.path().join("a").join("t").join("x");
    let first_foreign = metadata_files(&foreign)
        .into_iter()
        .next()
        .expect("the nested table wrote metadata files");
    let foreign_uuid = foreign.metadata().uuid().to_string();
    let own_uuid = swept.metadata().uuid().to_string();
    let before = walk(&nested_dir);
    for (scan, ancestor) in file_spellings(&nested_dir.join("data"))
        .into_iter()
        .zip(file_spellings(&nested_dir))
    {
        let err = call_error(
            session,
            &format!(
                "CALL {host}.system.remove_orphan_files(table => 'a.t', location => '{scan}')"
            ),
        )
        .await;
        assert_eq!(
            plan_message(err),
            ancestor_metadata_refusal(
                "a.t",
                &scan,
                &ancestor,
                &first_foreign,
                &foreign_uuid,
                &own_uuid
            ),
            "{scan}"
        );
    }
    for scan in file_spellings(&nested_dir) {
        let err = call_error(
            session,
            &format!(
                "CALL {host}.system.remove_orphan_files(table => 'a.t', location => '{scan}')"
            ),
        )
        .await;
        assert_eq!(
            plan_message(err),
            scan_equal_refusal(&scan, &first_foreign),
            "{scan}"
        );
    }
    assert_eq!(walk(&nested_dir), before, "a refused sweep deletes nothing");
    assert_eq!(ids(session, &format!("{host}.a.t")).await, vec![1]);
    assert_eq!(ids(session, &format!("{nested}.o.x")).await, vec![2]);
}

#[tokio::test]
async fn call_orphan_ancestor_same_catalog_scan_of_a_table_nested_in_the_own_location_refuses() {
    let warehouse = TempDir::new().unwrap();
    let session = nested_table_session(&warehouse, "ice", "ice").await;
    let nested_location = warehouse.path().join("a").join("t").join("x");
    assert_nested_scans_refused(&session, "ice", "ice", &warehouse, |scan, _| {
        format!(
            "CALL remove_orphan_files refuses to sweep `a.t`: path `{scan}` holds table \
             `ice.o.x` at `{}`. This procedure deletes every file the swept table's own metadata \
             does not reference, which would include that table's live files. Sweep a path that \
             holds only this table's files.",
            nested_location.display()
        )
    })
    .await;
}

#[tokio::test]
async fn call_orphan_ancestor_other_catalog_scan_of_a_table_nested_in_the_own_location_refuses() {
    let warehouse = TempDir::new().unwrap();
    let session = nested_table_session(&warehouse, "m1", "m2").await;
    let swept = load(&session, "m1", &["a", "t"]).await;
    let foreign = load(&session, "m2", &["o", "x"]).await;
    let own_uuid = swept.metadata().uuid().to_string();
    let foreign_uuid = foreign.metadata().uuid().to_string();
    assert_nested_scans_refused(&session, "m1", "m2", &warehouse, |scan, file| {
        format!(
            "CALL remove_orphan_files refuses to sweep `a.t`: path `{scan}` holds `{file}`, the \
             metadata file of another table (table-uuid `{foreign_uuid}`; the swept table's is \
             `{own_uuid}`), such as a table of another catalog or session on the same warehouse. \
             This procedure deletes every file the swept table's own metadata does not reference, \
             which would include that table's live files. Give the table its own LOCATION \
             (`CREATE TABLE ... LOCATION '<path>'`), then sweep it."
        )
    })
    .await;
}
