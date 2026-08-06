//! Fork-pin proof (ADR-0001): this crate must compile against the OWNED iceberg-rust fork,
//! never a silent crates.io registry fallback.
//!
//! `iceberg::plan_commit_base_load` + `iceberg::CommitBaseLoadPlan` are fork-only public API
//! (the fork's metastore commit-base reuse plan for Glue / S3 Tables, Java
//! `BaseMetastoreTableOperations.commit` parity; verified present at pinned fork rev
//! `b009ac15`, absent from registry iceberg 0.9.1) — so this file COMPILE-FAILS the moment the
//! `[patch.crates-io]` pin stops applying. Belt-and-braces with the ported
//! `catalog::tests::fork_patch_in_effect_deletefilter_is_public` (a different fork-only
//! symbol, name-only): this test additionally asserts the symbol's load-bearing OCC behavior,
//! not just its existence.

use iceberg::{CommitBaseLoadPlan, plan_commit_base_load};

/// The fork's commit-base load planner enforces the OCC contract: a commit base that does not
/// match the service pointer is ALWAYS a conflict — even when a pre-loaded base table's
/// location already matches the service (the stale-base + "current" forge case its docs pin) —
/// while a matching base reuses the provided table and a missing base falls back to full load.
#[test]
fn fork_pin_plan_commit_base_load_occ_contract() {
    // Stale base vs service pointer: conflict, even with a service-matching provided table.
    assert_eq!(
        plan_commit_base_load("s3://wh/meta/v3.json", Some("s3://wh/meta/v2.json"), None),
        CommitBaseLoadPlan::Conflict,
    );
    assert_eq!(
        plan_commit_base_load(
            "s3://wh/meta/v3.json",
            Some("s3://wh/meta/v2.json"),
            Some("s3://wh/meta/v3.json"),
        ),
        CommitBaseLoadPlan::Conflict,
    );
    // Base matches service and the provided table sits at that same location: reuse.
    assert_eq!(
        plan_commit_base_load(
            "s3://wh/meta/v3.json",
            Some("s3://wh/meta/v3.json"),
            Some("s3://wh/meta/v3.json"),
        ),
        CommitBaseLoadPlan::ReuseProvided,
    );
    // No commit base (create edge) or no usable provided table: full metadata load.
    assert_eq!(
        plan_commit_base_load("s3://wh/meta/v3.json", None, None),
        CommitBaseLoadPlan::FullLoad,
    );
    assert_eq!(
        plan_commit_base_load("s3://wh/meta/v3.json", Some("s3://wh/meta/v3.json"), None),
        CommitBaseLoadPlan::FullLoad,
    );
}
