# Unit ledger — ICE-TSNS-TYPE-PLANNER-1 · the SQL type name decides the unit (fresh build)

**Date:** 2026-10-15 · **Branch:** `fix/ice-tsns-type-planner-1-fresh` · **Base:** main
`fa1e4274` · **Model:** Claude (claude-opus-5-5), Opus build lane · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**
Target: 1.5.6.

**Order:** the [sketch](../../wo/ice-tsns-type-planner-1/sketch.md) and
[amendment 1](../../wo/ice-tsns-type-planner-1/amendment-1.md) (U-1, R-017), the
[fresh step 0](../../wo/ice-tsns-type-planner-1/step0-fresh-2026-10-15.md) and the owner's
rulings 91 to 94 of 2026-10-15. **This ledger closes when the unit's pull request merges.**
The old branch `fix/ice-tsns-type-planner-1` (`fc85b24c`) keeps the old ledger, whose
clauses C-001 to C-005 are step-0 evidence that this branch does not carry (the P5, H2, H3
and H4 records). This ledger keeps the old numbers C-006 to C-014 for the carried work, so
the carried `pins:` lines resolve, and numbers the fresh round's clauses from C-015.

**Why.** DataFusion types the SQL name `TIMESTAMP` as nanoseconds, so a session rule read
every nanosecond cast target as Spark's `TIMESTAMP`, and a NULL beside a nanosecond value
cut it to microseconds (R-017). A `TypePlanner` decides the unit from the written type
name; a widen rule joins mixed units to the finest; the rules that read a nanosecond target
as `TIMESTAMP` are deleted.

## PROPOSITION LEDGER — ICE-TSNS-TYPE-PLANNER-1 — 2026-10-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-006 | The fixture is committed before the mechanism code, its expectations are derived by the harness from the rule, and its main side is recorded on today's main. | The fixture commits and §3. | **PROVEN** | `python/repark/tests/test_ice_tsns_type_planner_1.py`, `_ice_tsns_type_planner_1_rule.py`; `ice_tsns_type_planner_1_main.json` recorded on `01796857` (after U-3): 952 of 11,288 statements error, 7,827 cells red (§3). pins: ice-tsns-type-planner-1/C-006 |
| C-007 | Every census cell (9,600) and every cache cell (420) stores what its typed twin stores, outside the dated residue rows. | The census and cache tests on the head. | **PROVEN** | §5: census 9,570 green, 30 red, all 30 dated residue rows red on main; cache 420 green. pins: ice-tsns-type-planner-1/C-007 |
| C-008 | The 117 reads and the 92 consumer-door cells answer what their twin answers, and every statement main answers answers here outside ruling 38's carve-out. | The two tests on the head. | **PROVEN** | §5: reads 117 and consumer 92 green as cells; the 108 statements main answers that the head loses are exactly the carve-out (consumer 88, `reads|m_udf:…` 20). pins: ice-tsns-type-planner-1/C-008 |
| C-009 | The parking repros of R-017's seven verifier rounds, by name, equal their twins. | The repro test on the head. | **OPEN** | §5: 289 green, 59 red; 51 are dated residue rows. Eight are open: `r017_update_nowhere_ntz_cast` 4 (Q1) and `v2_array_except` / `v2_array_intersect` over `tzns` 4 (Q2). |
| C-010 | The amended pins hold: the twenty mixed-unit cells on both doors in three zones, every lattice position, the H1 refusals, the H2 microsecond pins, P15, R-010, R-013, R-018(1), the Postgres reads. | The pin tests on the head. | **OPEN** | §5: lattice 120 green; doors 12 green, 7 dated residue rows; positions 476 green, 100 red, 96 dated residue rows, 4 open (`array_append` / `array_prepend` beside a `TIMESTAMP_NTZ` value over a `tzns` site in New York and Kolkata, Q2); Postgres 2 skipped (no `REPARK_PG_URL`). |
| C-011 | One planner types the written names on both SQL doors, and a join key in the optimized plan carries one unit. | The Rust pins on the head. | **PROVEN** | `crates/repark-sql/tests/ice_tsns_type_planner_1_lattice.rs`, 10 tests green (§6). pins: ice-tsns-type-planner-1/C-011 |
| C-012 | `SparkTimestampUnitWiden` runs before type coercion on every door and widens mixed timestamp units to the finest, by the types at the position, never narrowing and never touching equal units. | The rule's unit tests and the lattice cells. | **PROVEN** | `crates/repark-functions/src/timestamp_unit_widen.rs` unit tests; lattice 120 green. pins: ice-tsns-type-planner-1/C-012 |
| C-013 | No rule reads a `Timestamp(ns, _)` cast target or literal as the SQL `TIMESTAMP`; the AST lowering of the two nanosecond names is gone. | The deletion commit and the census on the head. | **PROVEN** | Commit `7dfb04c3` (from `6c3688af`); census as in C-007. pins: ice-tsns-type-planner-1/C-013 |
| C-014 | The wall kernels are applied by the analyzer from the type of a cast target on every door. | The lattice cells on both doors. | **PROVEN** | Commit `ca385f17` (from `07cbe563`); `a_widened_position_equals_its_written_nanosecond_cast_on_both_doors` green. pins: ice-tsns-type-planner-1/C-014 |
| C-015 | Ruling 94 (Q6): the ANSI door behind `repark_sql::AnsiDialect` types `TIMESTAMP` as `Timestamp(µs, None)` and `TIMESTAMPTZ` as `Timestamp(µs, "UTC")`, `CAST(<bigint> AS …)` reads seconds, and the Spark door keeps `TIMESTAMP` as the microsecond instant; the Python `repark.sql()` cell is measured and dated. | The Rust pins and the Q6 cell. | **PROVEN** | `the_ansi_door_plans_timestamp_as_the_wall_and_timestamptz_as_the_instant` and the per-door type-name cells green; `q6|repark_sql|timestamp_spellings` red as measured (`timestamp[us, tz=UTC]` for `TIMESTAMP`, `timestamp[ns]` for `TIMESTAMPTZ`), a dated row for PA-02 (`pa-local-fs-policy-1`) (§4). pins: ice-tsns-type-planner-1/C-015 |
| C-016 | M1: `sequence` builds timestamps in the element's unit and zone. | The Rust pin, the 103 cells, the revert check. | **PROVEN** | §5: the 95 reads and 8 repros answer; `a_nanosecond_sequence_steps_and_builds_in_its_element_unit_and_zone`; §7 revert. pins: ice-tsns-type-planner-1/C-016 |
| C-017 | M3: an `aggregate` zero joins the position table beside the element unit of its array. | The Rust pin, the 4 repros, the revert check. | **PROVEN** | §5: `v3_fold_typed_null` 4 green; `an_aggregate_zero_takes_the_unit_of_its_array_elements`; §7. pins: ice-tsns-type-planner-1/C-017 |
| C-018 | M3: `UNION` (DISTINCT) of a nanosecond branch and a widened or zone-coerced branch answers when a branch field carries `PARQUET:field_id`. | The Rust pin, the 3 positions, the revert check. | **PROVEN** | §5: `union|ts|ns` 3 green; `a_union_distinct_over_tagged_fields_takes_the_finest_unit_on_both_doors`; §7. The seam is the kernel's return field, not the rule (§2). pins: ice-tsns-type-planner-1/C-018 |
| C-019 | Ruling 91: an identity `UPDATE … WHERE` into `TIMESTAMP` narrows its SET value through the MERGE insert arm's cast family; `WHERE`, no `WHERE` and a written `CAST(… AS TIMESTAMP)` store one value; U-3's guard stays. | The Python and Rust pins, the census cells, U-3's merge wall, the revert check. | **PROVEN** | §5: census `update_where` 80 green, `update_forms` 8 green; `an_identity_update_stores_what_its_other_forms_store` (Rust); U-3's merge wall `ltz_v2` and `ltz_v3` green; §7. pins: ice-tsns-type-planner-1/C-019 |
| C-020 | Ruling 92: the nanosecond kernel's gate looks through dictionary and run-end encoding and gives the encoded source the plain value type's verdict and value. | The kernel unit test, the encoded cells, U-3's merge wall, the revert check. | **PROVEN** | §5: `encoded|timestamp_ns|…` and `encoded|timestamptz_ns|…` 4 green; U-3's 8 encoded cells green; `an_encoded_source_gets_the_verdict_and_the_value_of_its_value_type`; §7. The microsecond target is Q3. pins: ice-tsns-type-planner-1/C-020 |
| C-021 | Rulings 38 and 93: the dated rows are ruling 38's 108 door cells and residue cells red on the head with a red pin on today's main; a dated row that turns green fails the fixture until it is retired. | The dated-row tests and the main record. | **PROVEN** | `ice_tsns_type_planner_1_dated.json`: 108 carve-out, 186 residue (184 of ruling 93, 2 of ruling 92 under Q3), 1 Q6; every residue row is in the main record's `red`. pins: ice-tsns-type-planner-1/C-021 |
| C-022 | R-010: the fixture's `DELETE` harness reads stored rows through the table, and U-3's `update_literal` cells that main refused by name are re-pinned to the rule. | The harness commit's before and after counts; U-3's merge wall. | **PROVEN** | §3: doors family 11 green / 8 red before, 12 / 7 after; U-3's merge wall 356 green with `r010_answers`. pins: ice-tsns-type-planner-1/C-022 |
| C-023 | No statement today's main answers errors on the head outside ruling 38's carve-out and the `repark.sql()` cell of ruling 94. | The fixture's main-answers check on the head. | **PROVEN** | §5: 108 lost statements, all carve-out rows; the Q6 cell answers. pins: ice-tsns-type-planner-1/C-023 |
| C-025 | No pin of an existing test moves without a ruling. | `cargo test` of the five crates and the facade suite on the head, set against today's main. | **OPEN** | §10: 51 Rust tests in `repark-spark` and 27 facade tests fail on the head and pass on main; every one fails on the carry-over head `ca385f17` already (module `c7809e68`, step 0's scratch module byte for byte), none comes from this round's commits (Q5). |
| C-024 | The 1.5.6 notes draft carries one line per stored-value change and registry row R-017 carries the round's status. | The notes draft and the registry row. | **PROVEN** | `task/roadmap/mid-term/v1-5-6-release-notes-draft.md`; `docs/spark-sql-iceberg-parity.md` R-017 status 2026-10-15. pins: ice-tsns-type-planner-1/C-024 |

## 1. The carry-over (C-011 to C-014)

From the old branch's 13 commits, by the step-0 table. A commit that adds to
`spark_type_planner.rs` needs the planner first, so the order is the old branch's.

| Commit | From | What changed on the way |
|---|---|---|
| `8840209a` | `17a6b171` | The fixture; the old staging ledger hunk dropped. |
| `62089e59` | `aeed9261` | `SparkTypePlanner`, unchanged. The ANSI variant is its own commit (§4). |
| `203f9821` | `4c28bc5b` + `fc85b24c` | `SparkTimestampUnitWiden` with `fc85b24c`'s test-helper fix and test-map rows. |
| `7dfb04c3` | `6c3688af` | The deletions, as is. |
| `ca385f17` | `07cbe563` | Wall kernels by cast target type, as is. |

`22b322a7`, `f8ac1589`, `ebe53487`, `d1411fd1`, `0793571d`, `d997e2cc` are not carried;
`3a00ba80` and `fc85b24c`'s ledger are replaced by this ledger. The comment gate read 0 on
every carried commit.

## 2. Mechanisms and rounds

| Mechanism | Round | Commits | Seam |
|---|---|---|---|
| M1 sequence | 1 | `cb39182b` | `spark_sequence.rs` and its child `rows.rs`: bounds cast to the element type, stepped in its ticks, built as it. |
| M2 pandas and Arrow doors | 0 (no code) | ruling 38's dated carve-out, `fc265297` | none |
| M3 rule gaps | 1 | `0dc4455f` aggregate zero, `7dca3bef` UNION DISTINCT, `60658fec` shape A (91), `177cef20` shape B (92), `44260545` the ruling-91 Rust pin, `d33e3466` the shape-A analysis moved into `lineage.rs` (so `predicate_dml.rs`, 978 lines, stays under its 1,000 ceiling after U-6's 11 lines; merged 989) | one seam each: `timestamp_unit_widen.rs`; `timestamp_ns_cast.rs` kernel return field; `predicate_dml.rs` + `lineage.rs`; `timestamp_ns_cast.rs` kernel gate |

**UNION DISTINCT, the seam moved.** Step 0 placed the gap in the widen rule's union
projection. A probe on the head refutes it: `SELECT ns … UNION SELECT tzns …`, where the
rule does not fire, fails the same way. DataFusion's coercion casts the naive branch to the
zoned type; the union schema is built over that `Cast`, which keeps the source field's
metadata; then `wall_kernel_by_target` swaps the `Cast` for the kernel, whose field carried
no metadata, and the physical plan loses the `PARQUET:field_id` the logical union kept. The
fix is in the kernel's return field, one function. It is one seam, but not the one the brief
named; Q4 asks for the ruling to stand.

## 3. The fixture on today's main (C-006, C-021, C-022)

Main side: `ice_tsns_type_planner_1_main.json` on today's main `01796857` (module
`b57b4714`, after U-3): 948 of 11,274 statements error (10 more than on `293896ce`, all
`update_where_mor` array cells), 7,821 cells red. The `encoded` and `update_forms` groups
were added the same day: 952 of 11,288, 7,827 red.

Dated rows (`ice_tsns_type_planner_1_dated.json`, 295): ruling 38's 108 door cells; 184
residue cells of ruling 93, each red on today's main (census 30, positions 96, repros 51, H1
4, P15 1, ANSI-off out of range 2); 2 cells of ruling 92's microsecond scope (Q3); 1 Q6
cell (§4). Eight residue cells are red on the head and green on main and carry no row (Q2).

The head before the mechanism commits (`953ed397`, module `f294b8b7`): 285 cells red and
288 statements main answers that the head loses (sequence 103, doors 108, aggregate 4,
UNION 3, `UPDATE … WHERE` 70); with the dated rows, 100 open red and 180 open lost.

R-010 (`dba02f50`): the `DELETE` cell read every Parquet file and could not see a
copy-on-write delete; it now reads the table. Doors family: 11 green, 8 red before; 12
green, 7 red after (the 7 are dated residue). U-3's merge wall re-pins the `update_literal`
cells with a `timestamp_ns` or `timestamptz_ns` literal, which main refused by name
(`fc265297`, `r010_answers`).

## 4. Ruling 94, Q6 (C-015)

`AnsiTypePlanner` (`spark_type_planner.rs`) is the one ANSI variant, handed in by
`repark_sql::AnsiDialect`: `TIMESTAMP` → `Timestamp(µs, None)`, `TIMESTAMPTZ` →
`Timestamp(µs, "UTC")`; `CAST(1 AS TIMESTAMP)` and `CAST(1 AS TIMESTAMPTZ)` read one
second. The Spark door keeps `TIMESTAMP` as the instant. Side effect: `CAST(<bigint> AS
TIMESTAMP(6))` on the ANSI door now reads seconds too, since both spell one type.

The Python `repark.sql()` cell (wall `2024-01-02 03:04:05.123456789`):

| Spelling | today's main | head | ruled |
|---|---|---|---|
| `CAST('…' AS TIMESTAMP)` | `timestamp[ns]` `1704164645123456789` | `timestamp[us, tz=UTC]` `1704164645123456` | `timestamp[us]` |
| `TIMESTAMP '…'` | `timestamp[ns]` `1704164645123456789` | `timestamp[us, tz=UTC]` `1704164645123456` | `timestamp[us]` |
| `CAST('…' AS TIMESTAMPTZ)` | `timestamp[ns]` `1704164645123456789` | `timestamp[ns]` `1704164645123456789` | `timestamp[us, tz=UTC]` |

Red, so a dated row for PA-02's routing fix (`pa-local-fs-policy-1`), not a round.

## 5. The head (C-007 to C-010, C-016 to C-020, C-023)

Head `177cef20`, module `1f65588f` (§9). Fixture, all groups, `--jobs 8`:

| Family | Cells | Green | Red | Red, dated | Red, open |
|---|---|---|---|---|---|
| census | 9,600 | 9,570 | 30 | 30 | 0 |
| cache | 420 | 420 | 0 | 0 | 0 |
| reads | 117 | 117 | 0 | 0 | 0 |
| consumer | 92 | 92 | 0 | 0 | 0 |
| repros | 348 | 289 | 59 | 51 | 8 (Q1 4, Q2 4) |
| lattice | 120 | 120 | 0 | 0 | 0 |
| positions | 576 | 476 | 100 | 96 | 4 (Q2) |
| doors | 19 | 12 | 7 | 7 | 0 |
| encoded | 6 | 4 | 2 | 2 | 0 |
| update_forms | 8 | 8 | 0 | 0 | 0 |
| q6 | 1 | 0 | 1 | 1 | 0 |
| Postgres | 2 | skipped (no `REPARK_PG_URL`) | | | |

Statements today's main answers that the head loses: **108**, all ruling 38's carve-out
(consumer 88, reads 20). Outside the carve-outs: **0**.

The step-0 red set on the head:

| Owner | Step 0 | Head |
|---|---|---|
| M1 sequence (reads 95, repros 8) | 103 lost | 0 |
| M2 doors (consumer 88, reads 20) | 108 lost | 108 lost, dated |
| M3 aggregate zero (`v3_fold_typed_null`) | 4 | 0 |
| M3 UNION (`union|ts|ns`) | 3 | 0 |
| Shape A census (`update_where`, `update_where_mor` into `TIMESTAMP`) | 80 | 0 |
| Shape A repros (`r017_update_nowhere_ntz_cast`) | 4 | 4 (Q1) |
| Shape A merge wall (`ltz_v2`, `ltz_v3` `update_literal`) | 2 | 0 |
| Shape B merge wall (encoded × 4 doors × 2 encodings) | 8 | 0 |

U-3's fixtures on the head: `test_ice_tsns_floor_doors_1.py` 246 of 246,
`test_ice_tsns_merge_wall_1.py` 356 of 356 (602 passed, `-n 8`).

## 6. Rust pins

`crates/repark-sql/tests/ice_tsns_type_planner_1_lattice.rs` (10 tests, both doors),
`crates/repark-spark/tests/ice_tsns_type_planner_1_update_forms.rs` (ruling 91),
`spark_sequence::tests::a_nanosecond_sequence_steps_and_builds_in_its_element_unit_and_zone`,
`timestamp_ns_cast::tests::an_encoded_source_gets_the_verdict_and_the_value_of_its_value_type`,
`spark_type_planner::tests` (5).

## 7. Revert checks

Each mechanism commit's product code reverted alone, its pin kept, then restored:

| Commit | Pin | Head | Reverted |
|---|---|---|---|
| `cb39182b` M1 | `a_nanosecond_sequence_steps_and_builds_in_its_element_unit_and_zone` | ok | FAILED |
| `0dc4455f` aggregate | `an_aggregate_zero_takes_the_unit_of_its_array_elements` | ok | FAILED |
| `7dca3bef` UNION | `a_union_distinct_over_tagged_fields_takes_the_finest_unit_on_both_doors` | ok | FAILED |
| `60658fec` shape A | `an_identity_update_stores_what_its_other_forms_store` | ok | FAILED |
| `177cef20` shape B | `an_encoded_source_gets_the_verdict_and_the_value_of_its_value_type` | ok | FAILED |

## 8. `spark.sql.execution.pandas.convertToArrowArraySafely`, measured again

Live PySpark 4.1.2 (`sparkenv-pandas`, pandas 3.0.6), `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`,
`local[1]`, 2026-10-15T08:31: `spark.conf.get("spark.sql.execution.pandas.convertToArrowArraySafely")`
→ `'true'` in a fresh session, `SET` → `'true'`, with ANSI off → `'true'`; not in the
SparkContext conf. The step-0 `pandas_udf` cell answers as at step 0: `true` refuses
(`ArrowInvalid … would lose data: 1767323045123456789`), `false` answers
`[1767323045123456, 0, -1, 1767323045123456]`.

## 9. Modules

| Head | Module sha256 | Head-only string |
|---|---|---|
| `953ed397` (Q6) | `f294b8b7…` | none: the Q6 commit adds no string literal and the `.so` is stripped |
| `177cef20` (mechanisms) | `1f65588f…` | `on the timestamp arm` (M1), 1 hit |
| `ca385f17` (carry-over, for §10's attribution) | `c7809e68…` | `spark_timestamp_unit_widen`, 2 hits; equal to step 0's scratch module |
| `f65cb5b8` (final) | `3b87cc39…` | `on the timestamp arm`, 1 hit; the fixture's cells equal `177cef20`'s |

Both built with `maturin develop --release` inside the shared cargo lock, clean status
before and after.

## 10. Gates and the facade suite (head `f65cb5b8` unless named)

| Gate | Exit |
|---|---|
| unit fixture `test_ice_tsns_type_planner_1.py` (`-n 8`) | 1: 4 groups red on the 12 open cells of Q1 and Q2, 56 passed |
| Rust pins (lattice 10, update forms, sequence, encoded kernel, planner 5) | 0 |
| U-3 fixtures (floor doors 246, merge wall 356) | 0, 602 passed |
| `cargo test -p repark-functions -p repark-core -p repark-iceberg -p repark-spark -p repark-sql` (`--no-fail-fast`, `d33e3466` + the `spark_nullability` harness fix) | 101: 51 failed, all in `repark-spark` |
| `make rust-clippy` | 0 |
| `make rust-panic-ban` | 0 |
| `cargo fmt --all -- --check` | 0 |
| `make check-rust-file-size check-crate-dag` | 0 |
| `make check-docs-links check-docs-compaction check-ledger-grammar` | 0 |
| `make spell-check` | 0 |
| `python3 scripts/sync_map_md.py --check` | 0 |
| `bash scripts/check_map_md.sh --base origin/main` | 0 |
| `uvx ruff@0.15.22 check` / `format --check` on changed Python | 0 / 0 |
| comment gate | 0 hits |
| facade suite, `-n 8` | 1 on both: head 31 failed, main (`01796857`, same files) 63 failed |

**Facade, set against main.** Main-only failures: 49 in the unit fixture and 10 in U-3's
merge wall (the re-pins), as expected. Head-only failures: 27 existing tests. Grouped:
refusal texts now name `TIMESTAMP_LTZ` where they named `TIMESTAMP` (`test_fnp_6d_followup_1`
1, `test_fnp_degrees_rust_1` 4); `repark.sql()` answers an instant where it answered a naive
value (`test_ice_tt_resolve_1` 4, `test_nullability_2` 1); a mixed `VALUES` column
(`test_ntz_9_verify` 1, `test_ntz_store_doors_1` 6); `TIMESTAMP_NTZ` inside an `ARRAY` or
`STRUCT` cast answers where it refused (`test_ntz_1` 2); a `timestamptz_ns` literal in
`UPDATE` stores where main refused it (`test_ice_tstzns_wall_1` 5, R-010's family); and three
statements main answers that the head errors on (`test_stamp_2_r5p6_1` 1: `cannot cast
"Int32" to "TIMESTAMP_NS"`; `test_store_ts_doors_2` 2: `cannot convert text Timestamp(ns)
into byte Timestamp(µs, "UTC")`). All 27 fail on the carry-over module too.

**Rust, set against main.** `cargo test -p repark-spark` passes on main `fa1e4274` and fails
51 tests on the head: `src/tests/ntz_values_mix.rs` 19, `ntz_store.rs` 15,
`v3_timestamp_ns_door.rs` 6, `common.rs`-driven 2, `u9_timestamp_ltz.rs` 1, `ltz_store.rs` 1,
`extension/tests.rs` 1, `benches/ice_read_perf` pins 5, `tests/timestamp_ns_nested_shapes.rs`
1. The carry-over head `ca385f17` fails the same 51 and one more
(`a_dictionary_encoded_wall_stores_inserts_instant_on_the_overwrite_doors`, which ruling 92's
commit closes). The two `spark_nullability` pins that also went red were a test-harness gap
of the carried deletions and hold again (`f65cb5b8`).

## Q. Questions for the hand-back

- **Q1 (ruling 91's written NTZ cast).** `UPDATE T SET v = CAST(c AS TIMESTAMP_NTZ) WHERE
  id >= 0` over a nanosecond `c` truncates (`-1 ns` stores `0`), while the same SET with no
  `WHERE` floors (`-1000` into `timestamp_ns`); 4 repros. Today's main refuses the `WHERE`
  form (TZ-6). The identity `UPDATE … WHERE` re-plans the SET text with plain DataFusion
  SQL, where a written NTZ cast over nanoseconds is an Arrow cast; the floor lives in the
  Spark door's lowering. Closing it needs a second seam (the analyzer's NTZ half in
  `repark-functions`). Lean: its own card beside R-017's NTZ line; the 4 repros become
  dated rows, since they are red on today's main too.
- **Q2 (eight residue cells green on main).** `array_append` / `array_prepend` beside a
  `TIMESTAMP_NTZ` value over a `timestamptz_ns` site (New York, Kolkata) and
  `v2_array_except` / `v2_array_intersect` over `tzns` (UTC, New York) are red on the head
  and green on today's main. They are residue of ruling 93, so U-1 builds nothing for
  them, and they have no red pin on main, so they cannot be dated rows. Lean: dated rows
  owned by the H5 card, with the green-on-main fact on the row.
- **Q3 (ruling 92's microsecond premise).** "Encoded ns into a µs column stays what plain
  ns into µs is today": measured, it is not. On today's main and the head, an encoded
  `-1500 ns` into `TIMESTAMP` stores `-1 µs` and the plain value `-2 µs`. The two cells are
  dated rows under R-024 / NS-ENCODED-NESTED-FLOOR-1. Lean: keep them there.
- **Q4 (the UNION seam).** The UNION fix is one function in the kernel's return field, not
  the rule's union projection the brief named (§2). Lean: it stands as the rule-gap round's
  UNION commit.
- **Q5 (the carry-over moves existing pins).** Step 0 measured the fixture, not the
  existing suites. On the carry-over head, before any round of this brief, 51 Rust tests and
  27 facade tests that pass on main fail (§10); three of the facade tests are statements
  main answers that now error, which is this brief's halt. Their causes sit in the carried
  planner, deletions and by-target kernels (the `TIMESTAMP_LTZ` naming, the native door's
  instant, `VALUES` conform, the nested NTZ refusal, the by-target kernel on an integer
  coercion cast). Lean: rule which of the six groups are intended moves to re-pin (the
  `timestamptz_ns` literal family is R-010's fix; the `repark.sql()` group is ruling 94's
  PA-02 row) and open one carry-over fix round for the rest, beginning with the three
  errors.

