# Unit ledger — ICE-PROMOTE-READ-1 · reads and DML after a legal type promotion answer Spark

**Date:** 2026-09-16 · **Branch:** `fix/ice-promote-read-1` · **Base:** `32c0e1a3`
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** HIGH. **risk_tier: high** — silent row loss on reads and silent duplicate rows on
MERGE and partition overwrite after a spec-legal `ALTER COLUMN … TYPE`.
**Fork:** `TRO-Wolf/iceberg-rust` branch `fix/ice-promote-read-1`, unit F-PROMOTE-READ-1
(`task/f-promote-read-1-ledger.md` there). RePark consumes it through a local path override
until the orchestrator's pin bump; the override never reaches a RePark commit.

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Why now.** Run-19a report rows V2-10c, V2-06b, V3-11 (promotion), V3-14, graded MISSING:
after `ALTER TABLE t ALTER COLUMN c TYPE <wider>` a table that holds files written before the
promotion answers range filters without the pre-promotion rows, MERGE keyed on the promoted
column duplicates rows, and DML on a table with only pre-promotion files fails loud. Spark
4.1.2 reads and writes the same metadata correctly.

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `.github/`, the fork pin.
DML right after `ADD COLUMN` / `RENAME COLUMN` is unit ICE-EVO-DML-1 (V2-10e).

## Reproduction (step 1, release native `32c0e1a3`, 2026-09-16)

Probes copied to `/tmp/oc-worker/ia-build/probes/` with the scratch prefix rewritten; logs in
`/tmp/oc-worker/ia-build/repro/`.

| Probe | Result on RePark |
|---|---|
| `p_promote_read_rp` (v2, v3) | single era 10/10; mixed era 2/12 — `id < 2`, `id <= 2`, `id > 1`, `id >= 2`, `f < 2.0D`, `f > 2.0D`, `n < 15`, `n > 15` wrong |
| `replay_promo` (orchestrator replay, first block) | `id<2 => []`, `id>1 => [new3]`, `f<2.0D => []`, `id=1 => [old1]`; MERGE → `[(1,'m1'), (1,'old1'), (2,'old2'), (3,'new3')]`, v2 and v3 |
| `p_isolate_merge` | single era MERGE (CoW, MoR, v3), UPDATE, MoR DELETE: `column types must match schema types, expected Int64 but found Int32`; mixed era MERGE duplicates `id = 5` |
| `p_merge_evo` | 24 CORRECT, 4 WRONG (promoted key, mixed era), 4 MERGE-ERROR (decimal, single era) |
| `p_merge_evo2` | MATCHED-only MERGE silently skipped; INT source key duplicates; single era float MERGE refuses |
| `p_promote_partition_dml` | identity / truncate(10) source: every filter `Literal Int(1) … not compatible with accessor type long`; bucket(4): `=`/`IN` right, ranges silently wrong; `UPDATE … WHERE id < 3` → `count=0`; range DELETE right |
| `p_promote_suspects` (new) | **long `IN` list** (`id IN (1,2,4,…,25)`) → `[]`; **`count(*) WHERE id < 2`** → `0`; MoR DELETE/UPDATE on a promoted identity partition source → `Partition value for field p is not compatible with its partition type long`; static `INSERT OVERWRITE … PARTITION (p = 1)` → accessor refusal. `<>` / `NOT IN` DML right. |
| `p_promote_suspects2` (new) | **dynamic partition overwrite** (`INSERT OVERWRITE … PARTITION (p)` and `writeTo(t).overwritePartitions()`) on a promoted identity source keeps the old `p = 1` row beside the new one — silent duplicate, single and mixed era; time travel right |

## Root cause

Fork (`TRO-Wolf/iceberg-rust`, base `edc38c6a`): manifests decode under the schema embedded in each
manifest (`crates/iceberg/src/spec/manifest/mod.rs:62` →
`crates/iceberg/src/spec/manifest/_serde.rs:264`), so a pre-promotion file carries `Int`
bounds and `Int` partition literals while every scan and write binds under the current
`long` type. Java reads manifests through the current specs and decodes bounds with
`Conversions.fromByteBuffer(ref.type(), …)`. The mismatch surfaces at seven seams — the
inclusive metrics evaluator's positive `cmp_fn` (`inclusive_metrics_evaluator.rs:123`) and
`in` arm, the strict evaluator's `not_eq` / `not_in` arms, `StructAccessor::get`
(`expr/accessor.rs:76`), scan-planning partition tuples, `PartitionKey::new`
(`spec/partition.rs:389`), `resolve_partition_deletes` (`transaction/snapshot.rs:709`), and
the page-index evaluator. Detail and per-seam consequences: the fork ledger.

RePark: every DML target scan pins the snapshot id, so on a table with no write since the
promotion the fork scans under the old schema; `conform_scan_batch`
(`crates/repark-iceberg/src/write/merge/mod.rs:534`) rebuilds the batch against the scratch
schema built from the current schema without casting data columns.

## PROPOSITION LEDGER — ICE-PROMOTE-READ-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark's answers are recorded, not hand-computed: `_record_ice_promote_read_1.py` builds 126 cases on PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (ANSI on) and writes `truth.json` plus two Spark-created adopted tables; two independent recordings agree answer for answer; the pin module asserts the recorded catalog digest equals the driver's. | `test_recorded_oracle_covers_the_driver_catalog`; recording logs. | PROVEN | Recording 1 (21:57–21:59) and 2 (22:01–22:03): 126 cases, 0 Spark errors, 0 answer differences; `truth.json` 58,427 bytes. `test_recorded_oracle_covers_the_driver_catalog` green offline and live. |
| C-002 | Filters on promoted unpartitioned columns (`int→bigint`, `float→double`, `decimal(9,2)→(12,2)`) answer Spark row for row with `int64` / `double` / `decimal128(12, 2)` Arrow types on single- and mixed-era tables: `=`, `<`, `<=`, `BETWEEN`, `>`, `>=`, `<>`, short and long `IN`, `NOT IN`, float and decimal ranges, `count(*)` — SQL door and DataFrame door. | `test_sql_door_matches_spark[read/*]`, `test_dataframe_door_matches_spark[read/*]`. | PROVEN | Offline 169 passed on release native 23:00:19 (fork `364748c0` via override); red on base: see §Red evidence (`read/*/mixed` both doors). |
| C-003 | Filters on a promoted partition source (`identity(p)`, `bucket(4, p)`, `truncate(10, p)`) answer Spark on both eras, both doors. | `test_*_door_matches_spark[read_partition/*]`. | PROVEN | Offline 169 passed; red on base: `read_partition/*/mixed` 6 cells per door. |
| C-004 | MERGE keyed on the promoted column of a mixed-era table answers Spark — MATCHED-only UPDATE, MATCHED UPDATE + NOT MATCHED INSERT with BIGINT and INT source keys — v2/v3 × CoW/MoR, SQL door and facade `mergeInto` door. | `test_*_door_matches_spark[merge_key/*]`. | PROVEN | Offline 169 passed; red on base: 12 SQL-door cells. The facade `mergeInto` door renders `ON (target.id = source.id)` over a temp view and answered Spark on the base too (the key-range pushdown does not fire for that spelling): those cells guard the door, the SQL-door cells carry the red. |
| C-005 | Range and long-`IN` DML on a mixed-era table answers Spark: `UPDATE … WHERE id < 3`, `UPDATE`/`DELETE … WHERE id IN (<24 values>)`, `DELETE … WHERE id < 2`, `DELETE … WHERE f < 2.0D`, v2/v3 × CoW/MoR. SQL door only — PySpark has no DataFrame UPDATE/DELETE. | `test_sql_door_matches_spark[dml_range/*]`. | PROVEN | Offline 169 passed; red on base: `update_id_lt_3`, `update_id_in_long`, `delete_id_in_long` × 4; `delete_id_lt_2` / `delete_f_lt_2` were already right. |
| C-006 | DML on a table holding only pre-promotion files answers Spark instead of refusing `column types must match schema types`: MERGE upsert on the promoted key (SQL and `mergeInto`), MERGE non-key float UPDATE, MERGE DELETE on a promoted-decimal table, UPDATE, DELETE — v2/v3 × CoW/MoR. | `test_*_door_matches_spark[dml_single/*]`; `promoted_scan::conform_scan_batch_widens_legally_promoted_columns`, `promoted_scan::target_scan_over_a_single_era_promoted_table_yields_the_current_types`. | PROVEN | Offline 169 passed; red on base: all 20 SQL-door and 4 `mergeInto` cells. After the first fork fix 12 single-era UPDATE cells stayed red (fork DataFusion UPDATE exec) — closed by fork `364748c0`. |
| C-007 | DML on a promoted identity partition source answers Spark on both eras, v2/v3 × CoW/MoR: DELETE / UPDATE by a non-partition predicate and by a partition predicate, MERGE on the partition column, static `INSERT OVERWRITE … PARTITION (p = 1)`, dynamic partition overwrite (SQL under `partitionOverwriteMode=dynamic`, and facade `writeTo(t).overwritePartitions()`). | `test_*_door_matches_spark[dml_partition/*]`. | PROVEN | Offline 169 passed; red on base: 50 SQL-door cells (every MoR cell, every single-era CoW cell, every static/dynamic overwrite and MERGE-on-`p` cell) and the 8 `overwritePartitions` cells. |
| C-008 | RePark adopts the Spark-created, Spark-promoted mixed-era tables (v2 and v3, `identity(p)`, all four columns promoted) through `register_table` and answers every read predicate on both doors, then MERGE `UPDATE SET * / INSERT *`, range UPDATE and `DELETE … WHERE f < 3.0D` equal to Spark's own run of the same statements. | `test_adopted_spark_table_matches_spark[adopted/v2, adopted/v3 × sql, dataframe]`. | PROVEN | Offline 169 passed; red on base: all 4 adopted cells. |
| C-009 | Format v2 and v3 are both measured and pinned for every family above. | The recorded catalog asserts `{"2", "3"}`; every group id carries `v2` and `v3` cells. | PROVEN | Every group carries v2 and v3 cells; the catalog-digest cell asserts `{"2", "3"}`. |
| C-010 | The table-format cause is fixed fork-side with red-first Rust pins (fork unit F-PROMOTE-READ-1, 10 pins in `crates/iceberg/src/spec/promotion_tests.rs`); RePark patches no Iceberg semantics locally. | Fork red and green runs; RePark Python pins green only with the fork change. | PROVEN | Fork `dcd90d2b` (10 pins red) → `7e027cca` (green, 7/7 seam mutations red) → `e92a9e9d` (4 DataFusion pins red) → `364748c0` (green, revert mutation red). RePark Python pins green only on a native built against the fork change. |
| C-011 | RePark's DML target scan conforms a legally promoted column (`Int32→Int64`, `Float32→Float64`, `Decimal128(p,s)→Decimal128(p',s)`) to the current type and still refuses an illegal narrowing. | `promoted_scan` pins in `crates/repark-iceberg/src/write/merge/tests/`. | PROVEN | `cargo test -p repark-iceberg --lib` under the override: 437 passed. Mutation: `conform_scan_batch` reverted to `column.clone()` → the two widening pins red, the narrowing control green. |
| C-012 | Under `REPARK_PARITY_LIVE=1` live Spark re-derives every recorded answer (no golden drift) while the offline cells hold RePark equal to the recording. | `test_live_spark_rederives_every_recorded_answer`. | PROVEN | `REPARK_PARITY_LIVE=1 pytest test_ice_promote_read_1.py` → `170 passed in 153.88s` (live Spark re-derived all 126 recorded answers; no drift). |
| C-013 | Registry rows land in `docs/spark-sql-iceberg-parity.md` for the read-promotion defect, the DML-after-promotion defect and the promoted-partition-source defect, FIXED with pin names or DECLARED with a typed exception; `V3-COV-2` gains a dated pointer note; maps move in lockstep. | Registry diff; `make check-map-sync`. | PROVEN | `docs/spark-sql-iceberg-parity.md` §7 rows **ICE-PROMOTE-READ-1** (reads), **ICE-PROMOTE-DML-1** (MERGE / UPDATE / DELETE), **ICE-PROMOTE-PARTITION-1** (promoted partition source), all FIXED with pin names and the pin-bump dependency; `V3-COV-2` carries the dated 2026-09-16 note pointing at them. `make check-docs-links`: 931 files, 5759 links — clean. No shape is DECLARED: every recorded case answers Spark. |
| C-014 | Gates: fork `cargo test -p iceberg` filters + clippy; RePark `cargo test -p repark-iceberg --lib` under the override; release native; the pin module offline and live; the `*alter*`, `*evo*`, `*merge*`, `*v3_*`, `*ice_spark*` modules; `make verify`; comment and override greps on both branch diffs. | Command → result table. | OPEN | |

## Fix (step 5)

**Fork** (`TRO-Wolf/iceberg-rust` branch `fix/ice-promote-read-1`; detail in its
`task/f-promote-read-1-ledger.md`):

- `7e027cca` — `crates/iceberg/src/spec/promotion.rs` helpers; both metrics evaluators read bounds
  under the reference type; the partition accessor, `PartitionKey::new`, scan planning (data and
  delete manifests) and `resolve_partition_deletes` promote partition tuples; the page-index
  evaluator builds INT32/FLOAT page bounds under the field type.
- `364748c0` — `crates/integrations/datafusion/src/physical_plan/promotion.rs` `widened_batch`,
  used by the DataFusion UPDATE/DELETE execs when they rebuild a scanned batch under the current
  schema.

**RePark** — `crates/repark-iceberg/src/write/conform.rs` `promoted_scan_column` widens only the
legal promotions (`Int32 → Int64`, `Float32 → Float64`, `Decimal128(p,s) → Decimal128(p',s)`);
`write/merge/mod.rs` `conform_scan_batch` calls it for data columns (line-neutral; the file stays
at its 1792 baseline). Every RePark DML target scan (MERGE, identity DELETE/UPDATE, the COW
scratch) passes through that conform.

Local override used for every RePark build and test in this unit (never committed; `Cargo.lock`
restored before each commit): `cargo --config /tmp/oc-worker/ia-build/fork-override.toml …` and
`maturin develop --release --config …`, a `[patch.crates-io]` table pointing the five `iceberg*`
crates at the fork checkout.

## Green evidence

Offline, release native built 23:00:19 against fork `364748c0` + the RePark fix:

```
.venv/bin/python -m pytest python/repark/tests/test_ice_promote_read_1.py -q -p no:cacheprovider -rs
169 passed, 1 skipped in 29.36s
SKIPPED [1] … REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)
```

Intermediate run on the first fork fix (`7e027cca`, native 22:47:26): `12 failed, 157 passed` —
every single-era `UPDATE` cell, `column types must match schema types, expected Int64 but found
Int32`, which located the second fork seam (DataFusion UPDATE exec).

Live:

```
REPARK_PARITY_LIVE=1 .venv/bin/python -m pytest python/repark/tests/test_ice_promote_read_1.py -q -p no:cacheprovider -rs
170 passed in 153.88s (0:02:33)
```

Orchestrator replay block on the fixed native: `id<2 => [old1]`, `id>1 => [new3, old2]`,
`f<2.0D => [old1]`, `id=1 => [old1]`, MERGE → `[(1,'m1'), (2,'old2'), (3,'new3')]`, v2 and v3.

## Red evidence

### Python pins — release native `32c0e1a3` (unfixed), 2026-09-16 22:05–22:11

`TMPDIR=… .venv/bin/python -m pytest python/repark/tests/test_ice_promote_read_1.py -q -p no:cacheprovider -rs`

```
126 failed, 43 passed, 1 skipped in 343.07s (0:05:43)
```

Failing cells by test and group:

```
      4 test_adopted_spark_table_matches_spark adopted
      8 test_dataframe_door_matches_spark dml_partition
      4 test_dataframe_door_matches_spark dml_single
      2 test_dataframe_door_matches_spark read
      6 test_dataframe_door_matches_spark read_partition
     50 test_sql_door_matches_spark dml_partition
     12 test_sql_door_matches_spark dml_range
     20 test_sql_door_matches_spark dml_single
     12 test_sql_door_matches_spark merge_key
      2 test_sql_door_matches_spark read
      6 test_sql_door_matches_spark read_partition
```

Failure messages (counted):

```
     40 repark.errors.PySparkException: datafusion engine error: Arrow error: Invalid argument error: column types must match schema types, expected Int64 but found Int32 at column index 0
     20 repark.errors.PySparkException: DataInvalid => Literal Int(1) at position 0 is not compatible with accessor type long
     16 assert [[1, 1, 'old1... [7, 1, 'ow']] == [[2, 22, 'old... [7, 1, 'ow']]
     12 At index 0 diff: [1, 'a'] != [1, 'm1']
      8 assert [[1, 'a'], [1...'], [4, 'm4']] == [[1, 'm1'], [...'], [4, 'm4']]
      8 assert [[1, 1.5, 'ol... 3.5, 'new3']] == [[1, 1.5, 'u'... 3.5, 'new3']]
      6 repark.errors.PySparkException: DataInvalid => Partition value for field `p` is not compatible with its partition type `long`
      4 repark.errors.PySparkException: DataInvalid => Literal Int(0) at position 0 is not compatible with accessor type long
      4 Right contains one more item: [1, 'old1']
      4 Right contains one more item: [1, 1.5, '1.25', 'old1']
      4 Right contains one more item: [1, 1.5, '1.25', 1, 'old1']
```

The 43 green cells are the shapes the base already answers (single-era reads,
`bucket(4, p)` single era, the facade `mergeInto` door on mixed-era keys, range DELETE,
`<>` / `NOT IN` DML) plus the catalog-digest cell.

### repark-iceberg pins — base, 2026-09-16

`CARGO_BUILD_JOBS=10 cargo test -p repark-iceberg --lib merge::tests::promoted_scan`

```
test write::merge::tests::promoted_scan::conform_scan_batch_still_refuses_an_illegal_narrowing ... ok
test write::merge::tests::promoted_scan::conform_scan_batch_widens_legally_promoted_columns ... FAILED
test write::merge::tests::promoted_scan::target_scan_over_a_single_era_promoted_table_yields_the_current_types ... FAILED
a legally promoted column conforms to the current type: ArrowError(InvalidArgumentError("column types must match schema types, expected Int64 but found Int32 at column index 0"), Some(""))
the single-era scan conforms to the promoted types: ArrowError(InvalidArgumentError("column types must match schema types, expected Int64 but found Int32 at column index 0"), Some(""))
test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 434 filtered out; finished in 0.04s
```

### Fork pins — base `edc38c6a`, 2026-09-16

`CARGO_BUILD_JOBS=10 cargo test -p iceberg --lib spec::promotion_tests` → `0 passed; 10 failed`
(messages verbatim in the fork ledger's §Base-red evidence).
