//! Every refusal is a behavior. Each message class is pinned by the property that makes it
//! useful — it names the shape, and it names what to do instead — not by its exact wording.

use super::*;

/// Q9: the INSERT OVERWRITE refusal names all three replacements and carries the dbt-trino
/// evidence the design records for the absence (graft G10).
#[test]
fn insert_overwrite_refusal_steers_three_ways_and_cites_the_evidence() {
    let message = insert_overwrite("ice.sales.orders").to_string();
    assert!(message.contains("MERGE INTO ice.sales.orders"), "{message}");
    assert!(
        message.contains("DELETE FROM ice.sales.orders"),
        "{message}"
    );
    assert!(
        message.contains("CREATE OR REPLACE TABLE ice.sales.orders"),
        "{message}"
    );
    assert!(
        message.contains("dbt-trino"),
        "the absence cites its evidence: {message}"
    );
    assert!(message.contains("Q9"), "the ruling is citable: {message}");
}

/// Q7: CALL steers to the callable operation and names the trigger that would change the answer.
#[test]
fn call_refusal_steers_to_callable_ops_and_names_the_trigger() {
    let message = maintenance_call("ice.system.rewrite_data_files").to_string();
    assert!(
        message.contains("ice.system.rewrite_data_files"),
        "{message}"
    );
    assert!(message.contains("CALLABLE OPERATION"), "{message}");
    assert!(
        message.contains("TRIGGER"),
        "a refusal without a trigger is a wall: {message}"
    );
}

/// Q7: EXECUTE says out loud that it is the PRE-DESIGNATED future spelling — that is the whole
/// reason it refuses rather than falling through to a parse error.
#[test]
fn alter_execute_refusal_declares_itself_the_future_spelling() {
    let message = alter_table_execute("optimize").to_string();
    assert!(message.contains("optimize"), "{message}");
    assert!(message.contains("reserved"), "{message}");
    assert!(message.contains("TRIGGER"), "{message}");
}

/// TRUNCATE names both plausible meanings, because picking one silently is the failure mode.
#[test]
fn truncate_refusal_names_both_meanings() {
    let message = truncate("ice.sales.orders").to_string();
    assert!(
        message.contains("DELETE FROM ice.sales.orders"),
        "{message}"
    );
    assert!(
        message.contains("CREATE OR REPLACE TABLE ice.sales.orders"),
        "{message}"
    );
}

/// The `ALTER TABLE … EXECUTE` recognizer fires on the real shape and names the procedure.
#[test]
fn alter_execute_recognizer_fires_on_the_statement_shape() {
    let message = recognize_alter_table_execute("ALTER TABLE ice.sales.orders EXECUTE optimize")
        .expect("must be recognized")
        .to_string();
    assert!(message.contains("optimize"), "{message}");

    // Case and whitespace insensitivity — a recognizer that only matches one casing is a trap.
    assert!(
        recognize_alter_table_execute("alter   table  ice.s.t   execute\n  expire_snapshots")
            .is_some()
    );
}

/// …and it does NOT fire on anything else, including the supported ALTER forms and a column
/// literally named `execute`.
#[test]
fn alter_execute_recognizer_does_not_fire_on_other_statements() {
    for sql in [
        "ALTER TABLE ice.s.t ADD COLUMN c INT",
        "ALTER TABLE ice.s.t RENAME TO ice.s.u",
        "ALTER TABLE ice.s.t SET PROPERTIES (format = 'PARQUET')",
        "SELECT execute FROM ice.s.t",
        "EXECUTE something",
        // The word inside a literal is structurally invisible (scrubbed text).
        "ALTER TABLE ice.s.t SET PROPERTIES (format = 'EXECUTE optimize')",
    ] {
        assert!(
            recognize_alter_table_execute(sql).is_none(),
            "must not fire on `{sql}`"
        );
    }
}
