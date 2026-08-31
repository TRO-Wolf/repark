//! Every refusal is a behavior. Each message class is pinned by its safety or routing contract.

use super::*;

/// The INSERT OVERWRITE refusal names all three replacements and carries the dbt-trino evidence.
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
    let message = maintenance_call("ice.system.register_table").to_string();
    assert!(message.contains("ice.system.register_table"), "{message}");
    assert!(message.contains("CALLABLE OPERATION"), "{message}");
    assert!(
        message.contains("TRIGGER"),
        "a refusal without a trigger is a wall: {message}"
    );
}

/// EXECUTE states its pre-designated future spelling so callers have a clear replacement.
#[test]
fn alter_execute_refusal_declares_itself_the_future_spelling() {
    let message = alter_table_execute("optimize").to_string();
    assert!(message.contains("optimize"), "{message}");
    assert!(message.contains("reserved"), "{message}");
    assert!(message.contains("TRIGGER"), "{message}");
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

/// The recognizer does not fire on supported ALTER forms or a column named `execute`.
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

/// The recognizer is anchored to the verb slot after the table name, so a column named `execute` remains legal.
/// Mutation: restore the free `position(|w| w == "EXECUTE")` search → every row below reds.
#[test]
fn alter_execute_recognizer_is_anchored_to_the_verb_slot() {
    for sql in [
        "ALTER TABLE ice.sales.orders ADD COLUMN execute BIGINT",
        "ALTER TABLE ice.sales.orders DROP COLUMN execute",
        "ALTER TABLE ice.sales.orders RENAME COLUMN a TO execute",
        "ALTER TABLE ice.sales.orders ALTER COLUMN execute SET DATA TYPE BIGINT",
        "ALTER TABLE ice.sales.orders SET PROPERTIES (execute = 'no')",
        // The table itself may be named `execute`; the VERB after it is what decides.
        "ALTER TABLE ice.sales.execute ADD COLUMN c INT",
    ] {
        assert!(
            recognize_alter_table_execute(sql).is_none(),
            "the verb slot is not EXECUTE, so this legal statement must pass through: `{sql}`"
        );
    }
}

/// The anchor finds the verb across one-, two-, and three-part names.
#[test]
fn alter_execute_recognizer_finds_the_verb_after_any_name_spelling() {
    for sql in [
        "ALTER TABLE orders EXECUTE optimize",
        "ALTER TABLE sales.orders EXECUTE optimize",
        "ALTER TABLE ice.sales.orders EXECUTE optimize",
        "ALTER TABLE \"ice\".\"sales\".\"orders\" EXECUTE optimize",
        "ALTER TABLE ice.\"my orders\".t EXECUTE optimize",
        "ALTER TABLE ice . sales . orders EXECUTE optimize",
    ] {
        let message = recognize_alter_table_execute(sql)
            .unwrap_or_else(|| panic!("must be recognized: `{sql}`"))
            .to_string();
        assert!(message.contains("optimize"), "{message}");
    }
}
