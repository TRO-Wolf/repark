# UPDATE-SCHEMA-BATCH-MISMATCH-1, the fix and its red and green runs

**Builder:** `claude-opus-5-5`, 2026-11-13, after the xhigh inventory critic (verdict GAPS: pins and records, seat
confirmed) and the order author's rulings 1-5 of the same day.

## The fix

One seat, two product files:

- `crates/repark-iceberg/src/write/predicate_dml/lineage.rs`: `project_update_data_batch` takes the identity
  stream's planned schema and sets each value field's nullability to the declared value schema's OR that planned
  field's, never `array.is_nullable()`.
- `crates/repark-iceberg/src/write/predicate_dml.rs`: `collect_identity_update_rows` captures the stream's schema once,
  right after `execute_stream`, and passes it to every call.

Every new-value batch of one statement now carries one schema, so `register_update_values_table`'s
`MemTable::try_new` accepts any arrival order. The arrays are untouched; the writer still casts every batch to the
declared write schema (`merge/session_staging.rs:42-54`, `merge/mod.rs:556-576`). No stored value changes.

## Rust pin: `crates/repark-iceberg/src/write/predicate_dml/tests/batch_nullability.rs`

Batches built with explicit schemas (never `RecordBatch::try_from_iter`), an `Int32` value column holding a real NULL
(not a `NullArray`), and the NULL-free batch first.

| Run | Result |
|---|---|
| main `dd77603a`, with the fix set aside and the wrapper on main's two-argument signature (`project_update_data_batch(batch, values)`, the only test line changed) | **4 failed**: the NULL-free-first and the computed-NULL cases fail with `Plan("Mismatch between schema and batches")`; the other two fail on the per-batch schemas (data-derived nullability) |
| this branch | 4 passed |

## Facade pin: `python/repark/tests/iceberg/test_update_schema_batch_mismatch_1.py`

30 repeats per cell, one fresh two-file table per repeat, the stored rows checked against the truth.

**Red on main `dd77603a`'s release build: 14 failed, 33 passed.** The failing cells, as outcome counts out of 30:

| Cell | Kind | Main |
|---|---|---|
| `WHERE d <=> 0.0`, mixed-nulls | v2 COW | answered 16, mismatch 14 |
| `WHERE d <=> 0.0`, mixed-nulls | v3 | answered 13, mismatch 17 |
| `WHERE i IS NULL OR i <= 7`, mixed-nulls | v2 COW | answered 19, mismatch 11 |
| `WHERE i IS NULL OR i <= 7`, mixed-nulls | v3 | answered 20, mismatch 10 |
| `WHERE id IN (SELECT id FROM src)`, mixed-nulls | v2 COW | answered 12, mismatch 18 |
| `WHERE id IN (SELECT id FROM src)`, mixed-nulls | v3 | answered 13, mismatch 17 |
| top-level struct NULL in one file, `WHERE id = 4 OR id = 6` | v2 COW | answered 17, mismatch 13 |
| top-level struct NULL in one file, `WHERE id = 4 OR id = 6` | v3 | answered 12, mismatch 18 |
| `SET r = nullif(r, 1)` into NOT NULL `r` | v2 COW | writer text 19, mismatch 11 |
| `SET r = nullif(r, 1)` | v3 | writer text 15, mismatch 15 |
| `SET r = i` | v2 COW | writer text 18, mismatch 12 |
| `SET r = i` | v3 | writer text 18, mismatch 12 |
| `SET r = if(id = 4, CAST(NULL AS INT), r)` | v2 COW | writer text 14, mismatch 16 |
| `SET r = if(id = 4, CAST(NULL AS INT), r)` | v3 | writer text 14, mismatch 16 |

The 33 cells that passed on main are the controls: every merge-on-read cell, every no-nulls cell, `id = 4` (one
file), the `IN` lists (the fork's route), and the child-NULL struct, list and map cells.

**Green on this branch's release build: 47 passed, twice** (27 s and 33 s of test time).

The NOT NULL cells are scoped to identity-route predicates (`WHERE id = 4 OR id = 6`). On the fork's route,
`SET r = NULL WHERE id IN (4, 6)` refuses with `UPDATE cannot assign NULL to required column 'r'` (9 of 9, the
critic's probe), and Spark 4.1.2 refuses every form with `NOT_NULL_ASSERT_VIOLATION`. Neither RePark text is Spark's.
That second text predates this unit and is not changed here.

## Gates

See [gates.md](gates.md).

Up: [../map.md](../map.md).
