# Card ICE-TSNS-MERGE-WALL-1: MERGE writes a TIMESTAMP into a timestamp_ns column as its UTC wall

**Date:** 2026-10-09. **Filed by:** Claude (Haiku 5.5), docs lane, from the readiness review of main `40fc916f`.

**Status:** open. Not scheduled. Not attributed to a unit.

**Grade:** B (product Rust). **Executor tier:** not chosen. The tier is decided after the acceptance criteria below are settled. **Verifier:** Opus, in every case.

**Retires:** when registry row `ICE-TSNS-SQL-1-R-007` reads FIXED with a pin on every door in the checklist, or when the owner declines the fix and the row says so.

## Why

Registry row `ICE-TSNS-SQL-1-R-007` ([docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) line 3130) records one defect. In a session whose zone is not UTC, a MERGE insert or update writes a `TIMESTAMP` into a `timestamp_ns` column as the UTC wall of the instant. An `INSERT` of the same value stores the session-zone wall. The two doors store different values for the same input.

Spark cannot write the type, so Spark gives no oracle for this row. The row's own rule is ledger A-1: a microsecond instant written into a naive nanosecond column stores the session-zone wall. The fix makes MERGE follow that rule.

The review's reading of the cause: `is_ntz_wall_target` and `is_ltz_instant_target` in `crates/repark-iceberg/src/write/ntz_store.rs` (lines 18 and 23) match `TimeUnit::Microsecond` only. A `timestamp_ns` target matches neither. The write then takes the plain Arrow cast in `crates/repark-iceberg/src/write/update_cast.rs` (lines 13-19), and the MERGE insert path in `crates/repark-iceberg/src/write/merge/insert.rs` (lines 111-145) and the DELETE lineage path in `crates/repark-iceberg/src/write/predicate_dml/lineage.rs` (line 95) only wrap the microsecond targets. I read all four sites and they match this reading.

## Widening the matchers is not the fix

Widening `is_ntz_wall_target` to nanoseconds alone does not fix the defect. The NTZ wall cast builds microsecond arrays:

- `ntz_wall_cast_sql` in `ntz_store.rs` (lines 31-33) wraps the expression in the `__repark_cast_timestamp_ntz__` UDF.
- `crates/repark-functions/src/timestamp_ntz_cast.rs` imports `TimestampMicrosecondArray` (line 6) and returns `DataType::Timestamp(TimeUnit::Microsecond, None)` from its type helpers (lines 60, 105, 157 and 212).

A wider matcher sends a nanosecond target into that microsecond cast. The cast then drops the sub-microsecond digits or fails the schema check. The fix changes the NTZ wall cast to carry nanoseconds, and widens the matchers in the same change.

## Measured

Readiness review, 2026-10-09, on main `40fc916f`. The review ran the probe in the Evidence section below. Both tests fail. The log is in the same section.

- The INSERT value is `1767323045123456000`. The MERGE value is `1767341045123456000`. The difference is 18,000 seconds, five hours, the New York offset in January.
- `1767323045` is `2026-01-02T03:04:05Z`. The INSERT value is the literal's wall read as UTC. `1767341045` is `2026-01-02T08:04:05Z`, the UTC wall of the same instant. That is the MERGE value.
- The probe's literal is `TIMESTAMP '2026-01-02 03:04:05.123456789'`. Both stored values end in `123456000`. The literal's 789 nanoseconds do not survive the parse. The probe therefore shows the door mismatch, but it does not test the nanosecond path. The acceptance criteria below fix this.

## Acceptance criteria

The unit is done when each line below holds and has a pin.

1. **Wall-clock semantics are kept.** A nanosecond NTZ target stores the session-zone wall, as `INSERT` does, in every zone in item 4.
2. **Nanosecond precision is kept.** An input with non-zero sub-microsecond digits stores all nine digits.
3. **The input carries nanoseconds.** A `TIMESTAMP` literal parses to microseconds, so it cannot test truncation. Build the input from a column that already holds nanoseconds: a `timestamp_ns` column of a source table, or a `timestamp[ns]` Arrow table registered as a view. The executor measures which route the door accepts first and records that route.
4. **Zones.** UTC, America/New_York (a non-UTC zone with a January offset), and a DST-boundary zone. Lean: America/New_York with two inputs, one in the spring-forward gap (2026-03-08 02:30) and one in the fall-back overlap (2026-11-01 01:30). Spark has no nanosecond type, so the `INSERT` door, under ledger A-1, is the oracle for these inputs.
5. **Every write door** stores the same value as `INSERT VALUES` for the same input: `INSERT VALUES`, `INSERT SELECT`, MERGE insert, MERGE update, `UPDATE SET`, DELETE lineage, and overwrite (the whole-table and the partition arms).
6. **A `timestamptz_ns` control.** A `timestamptz_ns` column agrees across every door in every zone. The control is pinned and the fix does not change it.
7. **Pins read the Arrow path.** Each pin reads `collect` or `to_arrow` and checks value and type, never `show` alone. Each pin runs on each entry point where the door exists (AGENTS.md, "Hard rules": the entry-point matrix).
8. **No answer that matches today changes.** The UTC case and the `INSERT` doors stay as they are.

## Step 0 for the executor

1. Re-run the review's probe on the base. Place it at `crates/repark-spark/tests/readiness_review_probe.rs`, the path the log names. It is evidence: copy it as is and do not edit it. Record the result. On the base the review's run shows both tests failing.
2. Record that result in the unit's ledger before any product edit.

## Evidence

The review ran this probe from a worktree under `/tmp`. The log's build path is `/tmp/repark-iceberg-review-20261009-12mnqd0m`. `/tmp` is wiped at boot, so this card is the probe's home. The Rust block is the review's file, byte for byte. The text block is the relevant lines of the review's log, from the same directory.

```rust
use std::sync::Arc;
use datafusion::arrow::array::TimestampNanosecondArray;
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "America/New_York")
        .build().expect("session")
}

async fn run(session: &ReparkSession, sql: &str) {
    session.sql(sql).await.expect("SQL plan").collect().await.expect("SQL execute");
}

#[tokio::test]
async fn insert_and_merge_store_the_same_nanosecond_wall_time() {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session.register_memory_catalog("ice", warehouse.path().to_str().expect("path")).await.expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(&session, "CREATE TABLE ice.ns.t (id INT, ts timestamp_ns) USING iceberg TBLPROPERTIES ('format-version'='3')").await;
    run(&session, "INSERT INTO ice.ns.t VALUES (1, TIMESTAMP '2026-01-02 03:04:05.123456789')").await;
    run(&session, "MERGE INTO ice.ns.t t USING (SELECT 2 AS id, TIMESTAMP '2026-01-02 03:04:05.123456789' AS ts) s ON t.id=s.id WHEN NOT MATCHED THEN INSERT *").await;
    let batches = session.sql("SELECT ts FROM ice.ns.t ORDER BY id").await.expect("read").collect().await.expect("collect");
    let values: Vec<i64> = batches.iter().flat_map(|batch| {
        batch.column(0).as_any().downcast_ref::<TimestampNanosecondArray>().expect("ns values").values().iter().copied().collect::<Vec<_>>()
    }).collect();
    assert_eq!(values.len(), 2);
    println!("INSERT={} MERGE={}", values[0], values[1]);
    assert_eq!(values[0], values[1], "same input must store the same wall time across write doors");
}

#[tokio::test]
async fn encrypted_table_write_must_refuse_without_encryption_support() {
    let warehouse = TempDir::new().expect("warehouse");
    let session = session();
    session.register_memory_catalog("ice", warehouse.path().to_str().expect("path")).await.expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    run(&session, "CREATE TABLE ice.ns.t (id INT) USING iceberg TBLPROPERTIES ('format-version'='3', 'encryption.key-id'='review-test-key')").await;
    let result = match session.sql("INSERT INTO ice.ns.t VALUES (1)").await {
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    };
    assert!(result.is_err(), "write unexpectedly succeeded with encryption requested and no KMS");
}
```

In the log below the checkout's absolute path is replaced by `<checkout>`. Nothing else is changed.

```text
    Finished `test` profile [unoptimized + debuginfo] target(s) in 3m 48s
     Running tests/readiness_review_probe.rs (<checkout>/target/debug/deps/readiness_review_probe-64b9974407291b31)

running 2 tests

thread 'encrypted_table_write_must_refuse_without_encryption_support' (2179247) panicked at crates/repark-spark/tests/readiness_review_probe.rs:49:5:
write unexpectedly succeeded with encryption requested and no KMS
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
test encrypted_table_write_must_refuse_without_encryption_support ... FAILED
INSERT=1767323045123456000 MERGE=1767341045123456000

thread 'insert_and_merge_store_the_same_nanosecond_wall_time' (2179248) panicked at crates/repark-spark/tests/readiness_review_probe.rs:35:5:
assertion `left == right` failed: same input must store the same wall time across write doors
  left: 1767323045123456000
 right: 1767341045123456000
test insert_and_merge_store_the_same_nanosecond_wall_time ... FAILED

failures:

failures:
    encrypted_table_write_must_refuse_without_encryption_support
    insert_and_merge_store_the_same_nanosecond_wall_time

test result: FAILED. 0 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s

error: test failed, to rerun pass `-p repark-spark --test readiness_review_probe`
```

## Pointers

- Registry row: [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) line 3130.
- Ledger: [ice-tsns-sql-1-ledger.md](../../ledgers/staging/ice-tsns-sql-1-ledger.md) (R-007).
- Cast code: `crates/repark-iceberg/src/write/ntz_store.rs`, `crates/repark-functions/src/timestamp_ntz_cast.rs`.
- Write doors: `crates/repark-iceberg/src/write/merge/insert.rs`, `crates/repark-iceberg/src/write/update_cast.rs`, `crates/repark-iceberg/src/write/predicate_dml/lineage.rs`.
- Sibling card for the encryption refusal: [enc-1-first-write-refusal-card-2026-10-09.md](enc-1-first-write-refusal-card-2026-10-09.md).
