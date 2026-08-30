//! Fork-pin proof (ADR-0001): this crate must compile against the OWNED iceberg-rust fork, never a

use iceberg::{CommitBaseLoadPlan, plan_commit_base_load};

/// The fork's commit-base load planner enforces the OCC contract: a commit base that does not
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
