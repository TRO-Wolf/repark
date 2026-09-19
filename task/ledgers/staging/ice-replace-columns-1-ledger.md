# Unit ledger — ICE-REPLACE-COLUMNS-1 · `ALTER TABLE … REPLACE COLUMNS` drops and re-adds every column

**Date:** 2026-09-19 · **Branch:** `fix/ice-replace-columns-1` · **Base:** `22b586c7` (`main`)
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18: 1:1 parity with Spark's Iceberg integration. On main,
`REPLACE COLUMNS` kept a same-named column's field id, so existing rows kept their values where
Spark reads NULL — a silent wrong answer — and refused as an "identity trap" the re-types Spark
answers. 22 of the 34 measured cells differed. Spark lowers its Hive-style form to one
`DeleteColumn` per current top-level column plus one `AddColumn` per listed column, so every
field id is fresh; the fork's `UpdateSchema` already supports exactly that batch.

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, the fork, nested (`a.b`) replace
targets, an older partition spec that names a dropped column, Spark's `== SQL ==` caret block on
parse refusals.

## PROPOSITION LEDGER — ICE-REPLACE-COLUMNS-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The 34-cell Spark 4.1.2 oracle (cells `RC-*`, v2 and v3) is committed verbatim with provenance and SHA-256, and a recorder re-derives every cell on live Spark (`record` / `check`). | Fixture file plus the `python/repark/tests/map.md` row plus the live leg. | PROVEN | SHA-256 `e5fa5372…946b89` over `ice_replace_columns_1_spark_oracle.json`, built from the measured PC-1 recording; `check` folds only run-stamped plan ids, the catalog identity hash and the Py4J gateway name. |
| C-002 | `REPLACE COLUMNS` gives every listed column a fresh field id from `last-column-id + 1` and a new schema, so every pre-existing row reads NULL — on the basic, same-list, re-typed, shorter, renamed and commented shapes. | The per-cell facade pins plus the Rust twins, green after step 2. | PROVEN | 34 measured cells: ids `4, 5` (BASIC), `4, 5, 6` (SAME), `4` (ONE); `last-column-id`, schema count, schema and data all equal to Spark. Red on main for the opposite values (ids 1, 2 and the old rows). |
| C-003 | A complex type in the list goes through the one shared column-type parser and takes Iceberg's level-order fresh ids. | `RC-STRUCT` cells plus `replace_columns_adds_a_struct_with_level_order_fresh_ids`. | PROVEN | `s`=5, `s.a`=6, `s.b`=7, `last-column-id` 7 — Spark's ids. The list is tokenized once (`rewrite_nested_type_tokens`) and parsed with `parse_data_type` + `sql_type_to_iceberg_with_timestamp_type`, the same path CREATE TABLE and ADD COLUMN use, so no second type table exists. Main refused `I7 primitives only`. |
| C-004 | A refusal commits nothing: `NOT NULL` (Hive-style `ParseException`), a duplicate name (`[COLUMN_ALREADY_EXISTS]` / `SQLSTATE: 42711`), a live partition field and a live sort field whose source id would vanish (Iceberg's `Cannot find source column for …`). | The refusal pins plus the table-untouched twin per refused cell. | PROVEN | Texts are Spark's byte-for-byte on the message core, including `1000: cat: identity(3)` and `identity(1) ASC NULLS FIRST`; each twin re-reads the ids `1, 2, 3` and the rows `(1,a,x),(2,b,y)`. Main answered PART-KEEP-NAME and SORTED silently and refused TYPE and DUP with the identity-trap text. |
| C-005 | A row written after a replace reads back, a second replace moves the ids again and NULLs it, and `VERSION AS OF <first snapshot>` still answers the old rows under the old schema. | `RC-THEN-INSERT`, `RC-TWICE`, `RC-TT-OLD` cells plus `replace_columns_twice_nulls_the_row_written_in_between`. | PROVEN | Second replace lands ids `6, 7` with `schema-count` 3, as measured; the time-travel read answers `[[1,a,x],[2,b,y]]` without a change to the time-travel path. |
| C-006 | The native `repark.sql` door has no Hive-style REPLACE COLUMNS and is pinned at its registered parse refusal, leaving the table alone. | `test_native_door_refuses_hive_style_replace_columns`. | PROVEN | The ANSI dialect refuses at the parser (`Expected: ADD, RENAME, PARTITION, … found: REPLACE`); the facade owns the Spark-ism. Wiring a Hive-style form into the ANSI door would be a new product surface, not parity. |
| C-007 | The identity-trap design is gone from the tree: the gate, its helpers and every pin that asserted it. | `rg` over the tree plus the flipped pins listed in §Flipped pins. | PROVEN | `refuse_replace_columns_identity_and_required`, `build_replace_column_changes`, `types_compatible_for_replace`, `is_iceberg_type_promotion` and `parse_sql_type_tokens` are deleted with the old parser; no "identity trap" string survives outside `docs/history/` and the registry's "before" sentence. |
| C-008 | The new module is the only home for the form: `alter.rs` detects and routes, `replace_columns.rs` parses and plans, and no RePark-side schema model is introduced. | The diff plus the crate-DAG and size gates. | PROVEN | 206 lines in `replace_columns.rs`; `alter.rs` 1813 → 1449 and `tests/alter.rs` 1379 → 1184, both ratcheted **down**; the plan is a `Vec<SchemaChange>` handed to the fork's `apply_schema_changes`, so ids come from `UpdateSchema`, never from RePark. |
| C-009 | The harness replay over the DDL group matches the Spark recording on status, error class core and every observation. | `harness.py --engine repark --only DDL` against `spark-pc1.json`. | PROVEN | 34 / 34 cells equal (was 12 / 34): every `ok` cell equal on all observations, every refused cell refused at the same statement with Spark's message core. Residual: exception class names (see §Residues). |
| C-010 | The registry row reads **FIXED 2026-09-19** with before/after, pins and residues, and every touched directory's `map.md` is in lockstep. | Registry diff, map rows, the doc gates. | PROVEN | `ICE-REPLACE-COLUMNS-1` sits with the ALTER rows; no other row described the identity trap, so none needed correcting beyond `python/repark/tests/map.md`, `crates/repark-spark/src/map.md`, `crates/repark-spark/src/tests/map.md` and `scripts/map.md`. |

## Flipped pins

| Pin | Asserted (old design) | Replaced by |
|---|---|---|
| `crates/repark-spark/src/tests/alter.rs::alter_replace_columns_promote_and_identity_trap` | `id INT → BIGINT` kept field id 1 and its rows; `id → STRING` refused "identity trap". | `tests/replace_columns.rs::replace_columns_assigns_fresh_ids_and_nulls_existing_rows` + `…accepts_an_incompatible_type_on_a_kept_name` (cells RC-BASIC, RC-TYPE). |
| `crates/repark-spark/src/tests/alter.rs::alter_replace_columns_float_decimal_promote_and_traps` | float→double / decimal widen kept the ids; double→string and decimal→int refused. | The same two tests plus `…with_the_same_list_still_rewrites_every_id` (cells RC-BASIC, RC-SAME, RC-TYPE). |
| `crates/repark-spark/src/tests/alter.rs::alter_unsupported_forms_refuse_loud` (REPLACE block) | `REPLACE COLUMNS (id STRING, name STRING)` refuses. | The cell RC-TYPE answer; the block is deleted, the rest of the refuse battery is untouched. |
| `python/repark/tests/test_alter_table.py::test_alter_replace_partition_field_and_replace_columns` | `id` kept its value `[1]` after the replace; the re-type refused "identity trap". | The same test, now asserting `[None]` and the answered re-type (cells RC-BASIC, RC-TYPE). |
| `python/repark/tests/test_alter_table.py::test_alter_unsupported_forms_refuse_loud` (REPLACE twin) | the re-type refuses. | Deleted; cell RC-TYPE covers the answer. |

## Residues (declared)

1. **Exception class on the Iceberg validation refusals** — Spark surfaces
   `org.apache.iceberg.exceptions.ValidationException` through `Py4JJavaError`; RePark raises
   `PySparkException` with the same message core (`Cannot find source column for …`). Same shape
   as the ICE-DROP-NS-1 residue.
2. **Parse-refusal context block** — RePark prints Spark's sentence (`NOT NULL is not supported
   in Hive-style REPLACE COLUMNS.`) without the `== SQL (line 1, position 1) ==` caret block.
3. **Spec scope** — the partition / sort source check reads the **default** spec and sort order.
   An older spec that still names a dropped column is out of scope; no measured cell covers it.
4. **Pre-existing, not this unit** — `RC-IDENTIFIER` fails on its first statement
   (`ALTER COLUMN id SET NOT NULL`) on both engines; RePark's class there is
   `UnsupportedOperationException` where Spark raises `AnalysisException`
   (`_LEGACY_ERROR_TEMP_2330`). Untouched by this unit.

## Gates

Measured on the release native built from the step-2 tree:

- Harness replay: `harness.py --engine repark --cells cells_pc.py --only DDL` →
  `repark-rc-round1.json`; 34 / 34 RC cells equal to `spark-pc1.json` on status and every
  observation, refusals matching on class core and message core.
- Offline `-n 4`: `test_ice_replace_columns_1.py` 45 passed, 1 skipped (live-only);
  `test_alter_table.py` 9 passed (the two flipped pins included) — the two files
  `rg -l "REPLACE COLUMNS" python/repark/tests` finds.
- Live (`REPARK_PARITY_LIVE=1`, PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0):
  `test_ice_replace_columns_1.py` 46 passed; the recorder's `check` prints
  `oracle fixture ice_replace_columns_1_spark_oracle.json reproduces on live Spark` (rc 0),
  so all 34 cells re-derive — over a Hadoop catalog, where the InMemory recording was made.
- `cargo test -p repark-spark --lib alter` 58 passed; `--lib replace_columns` 12 passed.
- `cargo fmt --all --check` clean; `cargo clippy -p repark-spark --all-targets -- -D warnings
  -A clippy::disallowed_methods` (the canonical `rust-clippy` invocation) clean.
- `.venv/bin/ruff check .` clean; `ruff format --check` on the changed Python clean.
- `check_rust_file_size.py`, `check_lib_rs.py`, `check_lib_py.py`, `check_ledger_grammar.py`,
  `check_docs_links.py`, `sync_map_md.py --check` clean.
- Comment ban: `comment-ban hits=0` on every commit.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-replace-columns-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every expectation reads from the committed Spark 4.1.2 oracle (cells RC-*,
        v2 and v3) rather than from RePark's own answer; the recorder re-derives the fixture
        on live Spark and the harness replay compares RePark against the same recording.
      artifacts: [python/repark/tests/ice_replace_columns_1_spark_oracle.json, python/repark/tests/_record_ice_replace_columns_1_oracle.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the base tree before the fix — 34 of 46 pins failed, each for the
        measured reason (kept ids and kept values, the identity-trap refusal, the
        primitives-only STRUCT refusal, the silent partition/sort keep); the control cells
        (RENAME, EMPTY-TABLE, IDENTIFIER, the native door) passed before and after, so the
        pins discriminate the fix rather than the harness.
      artifacts: [python/repark/tests/test_ice_replace_columns_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Both doors are pinned — the Spark facade per measured cell and the native
        ANSI door at its registered parse refusal — plus 12 Rust twins at the planner seam,
        each refusal asserting the table's ids and rows are untouched. Format versions 2 and
        3 are separate cells throughout.
      artifacts: [python/repark/tests/test_ice_replace_columns_1.py, crates/repark-spark/src/tests/replace_columns.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The plan loads the table once and reads live metadata for the current schema,
        the default spec and the default sort order; the drops precede the adds in one
        UpdateSchema batch, so the fork commits a single schema and the validation refusals
        happen before the transaction is built. No cached schema is consulted.
      artifacts: [crates/repark-spark/src/replace_columns.rs]
    - id: AT-5
      status: ATTACKED
      evidence: No new panic path — the parser maps every sqlparser error through
        DataFusionError::SQL and the planner propagates the iceberg error; no unwrap or
        expect is introduced. Refusal messages interpolate only names already parsed from
        the statement or read from table metadata.
      artifacts: [crates/repark-spark/src/replace_columns.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The four residues are named in the registry row and in this ledger rather
        than hidden — the Py4J exception class, the missing caret block, the default-spec
        scope, and the pre-existing SET NOT NULL class on RC-IDENTIFIER.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/ice-replace-columns-1-ledger.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock or performance claim; the path adds one metadata read already
        performed by the previous planner.
    - id: AT-8
      status: ATTACKED
      evidence: No STATUS.md, Cargo.toml or Cargo.lock edit, no crate-DAG change, no fork
        change. The two size ratchets move DOWN only (alter.rs 1813 to 1449, tests/alter.rs
        1379 to 1184) and no guard exception was added.
      artifacts: [scripts/check_rust_file_size.py, scripts/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: The registry row reads FIXED 2026-09-19 with before/after, pins and residues;
        every touched directory's map.md carries the change with pins; check_docs_links,
        sync_map_md and check_ledger_grammar are clean.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md, crates/repark-spark/src/map.md, crates/repark-spark/src/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: Every pin that asserted the old identity-trap design was found by sweeping
        the tree for REPLACE COLUMNS, flipped with its replacement named in the flipped-pin
        table, and re-run green; the ALTER suites (58 Rust, 9 facade) pass unchanged
        otherwise.
      artifacts: [crates/repark-spark/src/tests/alter.rs, python/repark/tests/test_alter_table.py]
  complete: true
```
