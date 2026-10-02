# Unit ledger — WO NTZ-STORE-DOORS-1 · the remaining write doors store TIMESTAMP and TIMESTAMP_NTZ through the session zone

**Date:** 2026-09-28 · **Branch:** `fix/ntz-store-doors-1` · **Base:** `adc26586`
(`origin/main`) · **Model:** Claude (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The v1.5.1 release differential found two silent wrong values that
predate v1.5.0. PE-1: an LTZ `TIMESTAMP` stored into a `TIMESTAMP_NTZ` column
through `INSERT … BY NAME`, the `INSERT OVERWRITE` family or
`writeTo().overwritePartitions()` kept the UTC wall in a non-UTC session. PE-2:
a `TIMESTAMP_NTZ` stored into a `TIMESTAMP` column through the same doors and
through `MERGE … UPDATE SET`, `UPDATE SET *`, `INSERT (cols)` and `INSERT *` read
the wall as UTC. NTZ-1 fixed VALUES, positional INSERT … SELECT, views, UPDATE,
the MERGE LTZ→NTZ arms and the DataFrame append; these doors missed the same
conversion. NTZ-1 residues R-NTZ-S2-4 and R-NTZ-S2-5 are the LTZ→NTZ half of PE-1.

**What Spark does (measured 2026-09-28, Spark 4.1.2 + Iceberg 1.11.0, hadoop
catalog, `target/ntz-doors/doors_probe.py`, `out/doors-spark.json`).** Store
assignment between `TIMESTAMP` and `TIMESTAMP_NTZ` goes through the session zone
on every door: an LTZ instant stores its session-zone wall, an NTZ wall stores
the session-zone instant (the New York gap `02:30` resolves to `03:30` EDT, the
overlap `01:30` takes the earlier offset, EDT), and a `DATE` stores the
session-zone midnight into a `TIMESTAMP` column. The probe covers 15 doors × 11
sources × 2 target types × 3 zones (UTC, America/New_York, Asia/Kolkata) plus
`days`/`hours` partitioned tables, time travel and `STRUCT<TIMESTAMP_NTZ>`:
1489 statements per engine. On base (`adc26586`), 108 of the 116 non-UTC
must-change cells differed from Spark; `writeTo().overwrite(cond)` already
stored Spark's walls (8 cells EQUAL).

**The fix.** One seam, three call shapes, no per-door arithmetic.
`repark-iceberg` `write/ntz_store.rs` gains `zone_stores` (and its by-name
front `zone_stores_by_name`): given a source frame and its target columns, it
reads the analyzed source types and wraps only a cross-zone column — an
instant into a `TIMESTAMP_NTZ` target through NTZ-1's wall-cast UDF, a naive
microsecond timestamp or a `DATE` into a `TIMESTAMP` target through a `CAST`
to the target type, which the session's `spark_ltz_timestamp_cast` analyzer rule
turns into Spark's `to_timestamp` localization, exactly as it does for the
positional INSERT door. The Spark door's BY NAME append, the stage-then-swap
overwrite (static, dynamic, BY NAME, column list and so
`writeTo().overwritePartitions()` and `insertInto(overwrite=True)`) and the
`PARTITION (…)` overwrite call it after planning the source. MERGE renders the
same conversion as SQL: `store_assignment_cast_sql` wraps an LTZ target's value
in `CAST((…) AS TIMESTAMP)` inside the existing `arrow_cast`, and the MERGE
INSERT stream wraps an LTZ column whose analyzed source is naive or `DATE`.
Refusals are untouched: every gate still judges the unwrapped source, the plan
wrap only fires for timestamp and date sources, and MERGE runs its gates before
it renders. On a session without the Spark rules (the ANSI door, bare test
sessions) the `CAST` reads the naive wall as UTC as `arrow_cast` did.

**Verifier fold (2026-09-29, VD-1).** The scoped Opus verifier found a sideways
move. A `VALUES` list or inline table whose column mixes `TIMESTAMP` and
`TIMESTAMP_NTZ` literals was planned `TIMESTAMP_NTZ`, and each `TIMESTAMP` row
became its UTC wall. The seam above then converted those rows a second time. The
repro is New York, a `TIMESTAMP` column, `INSERT OVERWRITE t VALUES (1,
TIMESTAMP_NTZ '2024-07-01 12:00:00'), (2, TIMESTAMP '2024-01-15 23:30:00')`.
Spark stores 12:00 and 23:30. Base stored 08:00 and 23:30, and the fold's parent
commit stored 12:00 and 2024-01-16 04:30. Spark types the column `TIMESTAMP` and
reads the NTZ rows as session-zone walls. This is the same wider type Spark
chooses for UNION, CASE and coalesce, which RePark already matched. The cause is
DataFusion's `VALUES` union resolution: when `VALUES` is planned, a
`TIMESTAMP '…'` literal is still a naive `Timestamp(ns)` cast, so the mix
resolves to naive microseconds. The fix is at that coercion site, not in the
store helpers. `repark-functions` `timestamp_ns_cast::widen_mixed_values_timestamps`
runs first in `spark_ltz_timestamp_cast`. It rewrites each naive-microsecond
`VALUES` column as the rule would. When the rewritten cells are only instants and
naive walls with at least one of each, it types the column `TIMESTAMP` and casts
each wall cell to `TIMESTAMP`, which the rule localizes in the session zone.
Because it runs before NTZ-1's DML retarget, the positional doors get the same
column type. Before the fold they differed from Spark only on a New York
DST-gap NTZ row stored into a `TIMESTAMP_NTZ` column. No store helper changed.

**Re-verify fold (2026-09-29, RD2-1/RD2-2; model Muse Spark
`muse-spark-1.3-contributor`).** The scoped re-verifier found the widening also
fired on a `DATE` + `TIMESTAMP_NTZ` column, which has no `TIMESTAMP` cell:
DataFusion coerces the `DATE` to a naive cast, the rule rewrites it to
`to_timestamp`, and the widening counted that as an instant. Spark types the
pair `TIMESTAMP_NTZ`. Nine cells moved away from Spark (positional INSERT
VALUES and INSERT SELECT of a DST-gap NTZ wall into a `TIMESTAMP_NTZ` column in
New York and Lord Howe, plus one store read) and eight moved sideways (`BY
NAME` and INSERT OVERWRITE VALUES of the same sources), so the fold's "0 moved
away" claim below and R-6's "leaves unchanged" wording were wrong; both are
corrected in place with this date. The fix decides instant-ness from the
pre-rewrite cell, so a `DATE` never triggers the widening, and normalizes a
column of dates and naive walls with no instant to `TIMESTAMP_NTZ` (RD2-2: the
per-cell rewrite is skipped when no cell can change under it). A user `CAST` of
a naive value to `TIMESTAMP` beside a bare `DATE` plans identically to the
coerced pair and follows the date side; no probe covers that mix, and it is
recorded here rather than fixed. No store helper changed. Corrected 2026-09-29
(second re-verify RD3-1): the Opus re-verifier measured that mix, and it is a silent
wrong value, not an exotic aside; C-008 fixes it.

**Second re-verify fold (2026-09-29, RD3-1/RD3-2; model Claude `claude-opus-5-5`).**
The Opus re-verifier found two S1 holes that are new since base. RD3-1: a written
`CAST(<ntz> AS TIMESTAMP)` beside any `DATE`-typed cell (or beside `CAST(<date> AS
TIMESTAMP)`) typed the column `timestamp_ntz` and kept the unlocalized wall. New York:
`VALUES (1, DATE '2024-03-10'), (2, CAST(TIMESTAMP_NTZ '2024-03-10 02:30:00' AS
TIMESTAMP))` showed 02:30 where Spark and base show 03:30, and it moved 84 SELECT
cells in 4 zones, 12 store cells away and 18 sideways, plus the CTAS column type and
`unix_micros`. At the analyzer that plan is byte-identical to `DATE` + `TIMESTAMP_NTZ`,
because the planner's `VALUES` coercion and a written `CAST AS TIMESTAMP` emit the
same naive `Timestamp(ns)` cast. RD3-2: `CAST(<date> AS TIMESTAMP)` beside only NULL
rows was retyped `timestamp_ntz`, because a NULL cell counted as a naive wall. The
fix separates the cases where they still differ: on the SQL AST. `repark-spark`'s
keyword lowering wraps each written `CAST(… AS TIMESTAMP)` `VALUES` cell in one more
same-typed `CAST(… AS TIMESTAMP)`. The cell's outer source is then a nanosecond
timestamp, so `timestamp_ns_cast` classifies it by its rewritten type, an instant,
and the column takes base's `TIMESTAMP` path. The lowering runs on the SELECT, INSERT,
CTAS and `EXPLAIN` paths, and the MERGE probe now lowers a source that holds such a
cell. The same-typed cast changes nothing downstream. Separately, NULL cells no longer
count toward the `TIMESTAMP_NTZ` normalization, which now needs at least one non-NULL
naive wall. The `TIMESTAMP` widening counts NULLs exactly as before. No fork, `[patch]`
or store-helper change.

**Third re-verify fold (2026-09-29, RD4-1; model Claude `claude-opus-5-5`).** The Opus
re-verifier found a wider form of the RD3-1 class, new since base. RD4-1 (S1): beside a
`DATE`, a cell that Spark types `TIMESTAMP` but that RePark's `VALUES` cell typing makes
naive (`date_trunc('HOUR', <ntz>)`, `from_utc_timestamp` / `to_utc_timestamp` of an NTZ,
and `coalesce` / `if` / `greatest` / `least` mixing `CAST(<ntz|date> AS TIMESTAMP[_LTZ])`
or `try_cast` with an NTZ argument) turned the column `timestamp_ntz` and kept the
unlocalized wall. New York: `VALUES (1, DATE '2024-03-10'), (2, date_trunc('HOUR',
TIMESTAMP_NTZ '2024-03-10 02:30:00'))` answered 02:00 where Spark and base answer 03:00,
MERGE INSERT * into a `TIMESTAMP_NTZ` column stored 02:00, CTAS typed the column
`timestamp_ntz`, and `unix_micros` was off by the zone offset (24 + 14 SELECT cells and 12
store cells). The cause was the classifier: any cell whose rewritten type was naive counted
as a wall. The three earlier folds each patched one shape of that heuristic; this fold
replaces it with one invariant. The `VALUES` pass deviates from base only on positive,
syntactic evidence. A cell is an NTZ wall only if it is a `TIMESTAMP_NTZ` literal or a
top-level `CAST` / `::` / `TRY_CAST` to `TIMESTAMP_NTZ`, optionally plus or minus
`INTERVAL` literals. It is an LTZ instant only if it is a `TIMESTAMP` / `TIMESTAMP_LTZ`
literal or carries the second fold's cast marker. `DATE` and NULL cells are neutral. Every
other cell is unknown, and one unknown non-NULL cell leaves the column on base's path with
no normalization and no widening. The strict reading moved 232 probe cells that equalled Spark on
`fdf98d0b` back to base, so the same fold widens the evidence only syntactically: a
`TIMESTAMP` keyword cell may carry `± INTERVAL` literals or be a top-level `TRY_CAST`, and a
short name table (`to_timestamp_ntz`, `make_timestamp_ntz`, `localtimestamp` as walls,
`from_utc_timestamp`, `to_utc_timestamp` as instants) counts only when RePark's type for
the call agrees. No fork, `[patch]` or store-helper change.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | An LTZ `TIMESTAMP` stored into a `TIMESTAMP_NTZ` column through `INSERT … BY NAME`, `INSERT OVERWRITE` (static, `BY NAME`, `PARTITION (p = v)`, dynamic, column list) and `writeTo().overwritePartitions()` stores Spark's session-zone wall in New York and Kolkata, including the DST gap and overlap rows; UTC sessions store what they stored before. | One Rust pin per door in the NTZ-1 module (UTC, New York, Kolkata; plain, `Z`-suffixed, gap and overlap sources) and the facade replay of Spark's recorded answers per door and zone. | PROVEN | `ntz_store.rs` `by_name_stores_the_ltz_session_wall`, `overwrite_stores_the_ltz_session_wall`, `overwrite_by_name_stores_the_ltz_session_wall`, `overwrite_partition_stores_the_ltz_session_wall`, `dynamic_overwrite_stores_the_ltz_session_wall`, `column_list_overwrite_stores_the_ltz_session_wall` (all red on `adc26586`); `test_ntz_store_doors_1.py` `test_write_door_stores_sparks_value[{utc,ny,kol}/ntz/{byname,ow_static,ow_byname,ow_part,ow_dyn,df_owp,df_ow_cond}]` against `ntz_store_doors_1_spark_oracle.json`. |
| C-002 | A `TIMESTAMP_NTZ` stored into a `TIMESTAMP` column through the C-001 doors and through `MERGE … UPDATE SET`, `UPDATE SET *`, `INSERT (cols)` and `INSERT *` stores Spark's session-zone instant in New York and Kolkata, including the gap and overlap rows; UTC sessions store what they stored before. | One Rust pin per door and the facade replay per door and zone. | PROVEN | `ntz_store.rs` `*_stores_the_ntz_session_instant` for `by_name`, `overwrite`, `overwrite_by_name`, `overwrite_partition`, `dynamic_overwrite`, `column_list_overwrite`, `merge_update`, `merge_update_star`, `merge_insert`, `merge_insert_star` (all red on `adc26586`); `test_ntz_store_doors_1.py` cells `{utc,ny,kol}/ts/*`. |
| C-003 | Nothing else moves: STRING and INT sources refuse with today's text on every door, NULL, same-type (NTZ→NTZ, LTZ→LTZ) and DATE→NTZ stores are unchanged, NTZ-1's doors (VALUES, SELECT, UPDATE, the MERGE LTZ→NTZ arms, the DataFrame append, `writeTo().overwrite(cond)`) are unchanged, time travel reads the pre-insert snapshot unchanged, and the ANSI door's MERGE into an instant column reads a naive wall as UTC as before. | The probe diff before/after, the refusal steps replaying RePark's recorded text, the MERGE LTZ→NTZ pins green on base, and one ANSI-door pin green on base and head. | PROVEN | Probe before/after (`out/doors-before.json`, `out/doors-after.json`): 108 statements change answer, 0 refusal texts, 0 UTC answers, 0 same-type cells; the facade cells assert 132 refusal steps by RePark's recorded text (identical before and after); `ntz_store.rs` `merge_{update,update_star,insert,insert_star}_stores_the_ltz_session_wall` green on base; repark-sql `ansi_ntz_wall_cast.rs` `ansi_merge_into_an_instant_column_keeps_the_utc_reading` green on base and head. |
| C-004 | A `DATE` stored into a `TIMESTAMP` column through the C-002 doors stores Spark's session-zone midnight (base stored the UTC midnight there; the VALUES, SELECT, UPDATE and append doors already stored the session midnight). | The Rust pin over six doors and three zones, and the facade `date` rows. | PROVEN | `ntz_store.rs` `date_stores_the_session_midnight_through_every_door` (red on `adc26586`); `test_ntz_store_doors_1.py` `date` rows of the `ts` cells. |
| C-006 | A `VALUES` list or inline table whose column mixes `TIMESTAMP` and `TIMESTAMP_NTZ` has Spark's type `timestamp`, and its NTZ rows are session-zone walls. A plain SELECT answers Spark's `typeof` and values in UTC, New York, Kolkata and Lord Howe. Every write door stores Spark's value in both `TIMESTAMP` and `TIMESTAMP_NTZ` columns in New York and Kolkata, including a DST-gap row and NULL, in both row orders. The doors are positional INSERT VALUES and SELECT, INSERT OVERWRITE VALUES and SELECT, `BY NAME` append and overwrite, MERGE INSERT *, INSERT (cols), UPDATE SET and UPDATE SET *, `writeTo().overwritePartitions()`, `insertInto(overwrite=True)` and `writeTo().append()`. All-NTZ and all-LTZ columns and every other type pair keep their type. No probe cell that equalled Spark moves. | The Rust pins per SQL door and for the SELECT type. The facade replay of Spark's recorded answers per door, zone, target type and source. The mutation that reverts the coercion. The verifier's full 4,504-statement probe re-run on the fold. | PROVEN | `ntz_values_mix.rs`: ten door pins and `mixed_values_type_the_column_timestamp_through_the_session_zone` are red with the coercion reverted and green with it; `unmixed_values_keep_their_timestamp_type` is green both ways. `test_ntz_store_doors_1.py` `test_mixed_values_column_stores_sparks_value` (52 cells) and `test_mixed_values_column_types_timestamp` (4 zones) replay `ntz_store_doors_1_mixed_spark_oracle.json`. The mixed probe (`target/ntz-mix/mixprobe.py`, 1,061 statements per engine) moves 314 cells to Spark's answer, and 0 move any other way. The verifier's probe moves 46 cells to EQUAL (4,003 → 4,049 of 4,504), and 0 move any other way. Corrected 2026-09-29 (re-verify RD2-1): the re-verify probe measures 9 cells away from Spark and 8 sideways on the `DATE` + `TIMESTAMP_NTZ` pair against base `adc26586`, which C-007 fixes; the counts above hold for the `TIMESTAMP` + `TIMESTAMP_NTZ` pair only. |
| C-008 | In a `VALUES` column each cell's type is its written SQL type: a written `CAST(… AS TIMESTAMP)` is `TIMESTAMP` beside a `DATE`, a `CAST(… AS DATE)` or a `TIMESTAMP_NTZ` cell, so the column is `timestamp` with the NTZ wall localized, as in Spark and base. `CAST(<date> AS TIMESTAMP)` beside only NULL rows stays `timestamp` at the session midnight. `DATE` + `TIMESTAMP_NTZ` literals stay `timestamp_ntz` (C-007), all-NTZ and all-LTZ columns keep their type, and no cell that equalled Spark, base or the two previous heads moves. | Rust pins for the SELECT type and value, CTAS, stores into both target types (New York and Kolkata), and the NULL case (New York and Havana). The mutations named in the record. The full re-verify probe re-run against the recorded Spark, base, `2d0c03ae` and `ed6a8095` answers. | PROVEN | `ntz_values_mix.rs` `a_written_cast_as_timestamp_types_the_column_timestamp`, `a_written_cast_as_timestamp_stores_sparks_walls` and `a_written_cast_as_timestamp_beside_null_stays_timestamp`; M5–M7 below; the probe table in the fold hand-back (`/tmp/xnd/handback.json`). |
| C-009 | The `VALUES` pass changes a column's type or cells only on positive, syntactic evidence: a `TIMESTAMP_NTZ` literal or top-level cast to `TIMESTAMP_NTZ` (± `INTERVAL` literals) is a wall, a `TIMESTAMP` / `TIMESTAMP_LTZ` literal or marked `CAST(… AS TIMESTAMP)` is an instant, `DATE` and NULL cells are neutral, and any other non-NULL cell leaves the column on base's path. In New York a `DATE` beside `date_trunc('HOUR', ntz)`, `coalesce(CAST(ntz AS TIMESTAMP), ntz)` or `from_utc_timestamp(ntz, 'UTC')`, in both row orders, is `timestamp` with Spark's values in a SELECT and stores Spark's walls through MERGE INSERT * into a `TIMESTAMP_NTZ` column. | Rust pins for the SELECT type and value and the MERGE store. The mutation that counts an unknown cell as a wall. The full re-verify probe re-run against the recorded Spark, base and `fdf98d0b` answers. | PROVEN | `ntz_values_mix.rs` `an_expression_cell_beside_a_date_keeps_the_timestamp_type`, `an_expression_cell_beside_a_date_stores_sparks_walls`; M8 below. |
| C-005 | On `days(v)` and `hours(v)` partitioned tables the rows, the `.partitions` values and the equality filter follow the stored value on INSERT … SELECT, `BY NAME`, `MERGE … INSERT *` and `INSERT OVERWRITE` in all three zones. | The facade replay of Spark's recorded rows, partitions and filter answers. | PROVEN | `test_ntz_store_doors_1.py` `test_partition_transforms_follow_the_stored_value[{utc,ny,kol}/{ts,ntz}/{days,hours}]`. |
| C-007 | A `DATE` cell never triggers the `VALUES` timestamp widening: a column of dates and naive walls with no instant is `TIMESTAMP_NTZ`, and the widening still fires when a true instant is present. A plain SELECT answers Spark's `typeof` (`timestamp_ntz`) and keeps the DST-gap wall in both row orders in New York, Lord Howe and Kolkata. Positional INSERT VALUES and SELECT, `BY NAME` append, INSERT OVERWRITE VALUES and MERGE INSERT * store Spark's walls in both target types over the same zones and orders, with a NULL row. `DATE` + `TIMESTAMP` and `DATE` + `TIMESTAMP` + `TIMESTAMP_NTZ` stay `timestamp` with the gap resolved. No probe cell that equalled Spark moves. | The Rust pins per door and for the SELECT type. The mutation that counts a `DATE` as an instant again. The re-verifier's full 1,527-statement probe re-run on the fold. | PROVEN | `ntz_values_mix.rs`: `date_ntz_values_type_the_column_timestamp_ntz`, `date_ntz_{insert_values,insert_select,by_name,overwrite_values,merge_insert_star}_stores_sparks_walls` and `date_timestamp_mixes_keep_their_timestamp_type` are green with the fix; the mutation reds the 6 `date_ntz` pins and nothing else. The re-verifier's probes re-run (`/tmp/rv2/out/r-head2.json`, `/tmp/rv2/out2/r-head2-TIMESTAMP_{LTZ,NTZ}.json`): base to head moves 299 cells toward Spark and 0 away (main 251 toward / 0 away, followup 48 toward / 0 away, NTZ-default 157 same); the 9 away and 8 sideways cells are EQUAL, as are the 8 `dn/*/sel` type cells, the 8 MERGE/`df_append` NTZ cells, the 8 `cell/*/date_ntz` type cells, the 4 CTAS reads and the 2 `dst/*/gapntz_date` reads. Two `dst/*/gapntz_date/unix` cells move sideways (DIFF to DIFF): Spark raises on `unix_micros` over the now-`TIMESTAMP_NTZ` column on both. |

## Mutation record (2026-09-28)

Each mutation removed one routing, the named tests ran, and the file was
restored (the whole working diff hashed identical before and after).

| # | Mutation | Red | Green |
|---|---|---|---|
| M1 | Remove the BY NAME routing (`zone_stores_by_name` call in `append_by_name_projection`). | Rust `by_name_stores_the_ltz_session_wall`, `by_name_stores_the_ntz_session_instant`, `date_stores_the_session_midnight_through_every_door` (at `by_name America/New_York`); facade `{ny,kol}/{ntz,ts}/byname` and the eight non-UTC partition cells (each writes one BY NAME row). | Every other Rust pin (30 of 33 in the module) and facade cell (54 door cells, 4 UTC partition cells). |
| M2 | Remove the MERGE INSERT routing for LTZ targets (`zone_wrapping_stream_sql` instant predicate forced false). | Rust `merge_insert_star_stores_the_ntz_session_instant`, `merge_insert_stores_the_ntz_session_instant`, `date_stores_the_session_midnight_through_every_door` (at `merge_insert_star America/New_York`); facade `{ny,kol}/ts/{merge_ins,merge_ins_star}`. | Every other Rust pin (30 of 33) and facade cell (62 of 66; run before the partition cells existed). |

| M3 | Verifier fold (2026-09-29): revert the VD-1 coercion. `instant_ts.rs` and `timestamp_ns_cast.rs` go back to the fold's parent, and the facade is rebuilt. | Rust: 11 of 12 `ntz_values_mix` pins (all ten door pins and the SELECT-type pin). Facade: 43 of 52 mixed door cells and all 4 mixed SELECT cells. | Rust: `unmixed_values_keep_their_timestamp_type`. Facade: 9 positional-door cells that already equalled Spark (`{ny,kol}/ltz` and `kol/ntz` × `ins_values`, `ins_sel`, `df_append`) and every earlier cell. The working diff hashed identical before and after the revert (`319d5a19…`). |
| M4 | Re-verify fold (2026-09-29): count a `DATE` as an instant again (`ValuesCellClass::Date` adds to `instants`), run the module, restore the file byte-identical. | Rust: the 6 new `date_ntz` pins (5 doors and the SELECT-type pin). | Rust: the 12 VD-1 pins and `date_timestamp_mixes_keep_their_timestamp_type` (13 of 19 in the module). |
| M5 | Second re-verify fold (2026-09-29): classify every naive-target `CAST` as a naive wall (drop the source-type test in `is_values_naive_cell`), run the module, restore the file byte-identical. | Rust: `a_written_cast_as_timestamp_types_the_column_timestamp`, `a_written_cast_as_timestamp_stores_sparks_walls`, and six VD-1 door pins (`insert_values`, `insert_select`, `merge_insert`, `merge_insert_star`, `merge_update`, `merge_update_star`). | Rust: 14 of 22, the RD3-2 pin included (the NULL rule still holds). |
| M6 | Turn the AST mark off (`mark_values_timestamp_casts` marks nothing), run the module, restore. | Rust: `a_written_cast_as_timestamp_types_the_column_timestamp`, `a_written_cast_as_timestamp_stores_sparks_walls`. | Rust: the other 20, the RD3-2 pin included. |
| M7 | M6 plus counting a NULL cell as a naive wall again for the normalization, run the module, restore both files. | Rust: the M6 pair and `a_written_cast_as_timestamp_beside_null_stays_timestamp`. | Rust: the other 19. The working diff hashed identical before and after M5–M7 (`4f101a1f…`). |
| M8 | Third re-verify fold (2026-09-29): count an unknown cell as a naive wall (`values_cell_class` returns `Wall` where it returns `Other` for a cell with no syntactic evidence), run the module, restore the file. | Rust: `an_expression_cell_beside_a_date_keeps_the_timestamp_type`, `an_expression_cell_beside_a_date_stores_sparks_walls`. | Rust: the other 22 of 24 in the module. |

`MERGE … INSERT *` and `MERGE … INSERT (cols)` share one seam
(`insert_stream_checked`; the star expands to explicit columns before SQL
generation), so no mutation can remove the routing for `INSERT *` alone: M2 reds
both INSERT arms and nothing else.

## Tests rewritten

None. No existing pin changed answer: `repark-spark` lib 2487 passed,
`repark-iceberg` lib 759 passed, `repark-sql` all targets green. In the verifier
fold, the facade file's partition test and the new mixed tests share one replay
helper (`_replay_rows`). The partition test's steps and assertions are unchanged.

One existing pin changed answer in the fold: NTZ-1's `test_ntz_9_verify.py` step
`r_wide` (residue R-NTZ-S2-11). It pinned RePark's divergent `2024-03-10 02:30:00`
beside Spark's `03:30:00` for `INSERT … VALUES (1, TIMESTAMP '2024-01-01 12:00:00'),
(5, TIMESTAMP_NTZ '2024-03-10 02:30:00')` into an NTZ column in New York. C-006 is
exactly that widening, so the step now asserts Spark's rows. The NTZ-1 ledger marks
R-NTZ-S2-11 closed, and the stale divergence sentence in the test's docstring was
deleted. The re-verify fold changes no existing pin. The second re-verify fold changes
no existing pin either, and neither does the third.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ntz-store-doors-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is walked against Spark's recorded answer on the same door and zone; the probe covers 15 doors, 11 sources, both target types and three zones, and the facade replays 66 door cells and 12 partition cells.
      artifacts: [python/repark/tests/test_ntz_store_doors_1.py, python/repark/tests/ntz_store_doors_1_spark_oracle.json, python/repark/tests/ntz_store_doors_1_mixed_spark_oracle.json, crates/repark-spark/src/tests/ntz_store.rs, crates/repark-spark/src/tests/ntz_values_mix.rs]
    - id: AT-2
      status: ATTACKED
      evidence: One Rust pin per door and direction over UTC, New York and Kolkata with the gap and overlap rows; the must-not-change cells are asserted by the probe diff and the facade refusal steps.
      artifacts: [crates/repark-spark/src/tests/ntz_store.rs, python/repark/tests/test_ntz_store_doors_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal steps assert RePark's recorded text for every STRING and INT source on every door; M1 and M2 red the named pins and restore green.
      artifacts: [python/repark/tests/ntz_store_doors_1_spark_oracle.json, crates/repark-spark/src/tests/ntz_store.rs]
    - id: AT-4
      status: N/A
      justification: No commit, isolation or concurrency change; the conversion is a projection over the planned source.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; partition values follow the stored value (C-005) and time travel of the pre-insert snapshot is unchanged (C-003).
      artifacts: [python/repark/tests/test_ntz_store_doors_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; the plan wrap adds one analyzer pass only when a target column is a timestamp.
    - id: AT-8
      status: ATTACKED
      evidence: No dependency or Cargo.toml edit and no new crate edge; `insert_overwrite.rs` stays under the 1000-line ceiling (999) and `merge/mod.rs` is untouched.
      artifacts: [crates/repark-iceberg/src/write/ntz_store.rs, crates/repark-iceberg/src/write/merge/insert.rs, crates/repark-iceberg/src/write/update_cast.rs]
    - id: AT-9
      status: N/A
      justification: No registry or user-facing doc change in this unit; the ledger and the maps carry the record.
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts exact walls, instants or refusal texts; M1 and M2 break one routing each and the named pins red before restore; M3 reverts the VD-1 coercion and reds 11 Rust pins and 47 facade cells; M4 counts a DATE as an instant again and reds the 6 new DATE pins. M5 to M7 remove the RD3-1 mark and the RD3-2 NULL rule and red the 3 new C-008 pins.
      artifacts: [crates/repark-spark/src/tests/ntz_store.rs, crates/repark-spark/src/tests/ntz_values_mix.rs, python/repark/tests/test_ntz_store_doors_1.py]
  complete: true
```

## Neighbours (2026-09-28)

Three corpus files re-run on base (`adc26586`) and head, 91 statements:
`ntz7_verify_probe.py` (26) changes 4 answers, all to Spark's recorded answer
(the `INSERT OVERWRITE` and `BY NAME` LTZ→NTZ reads of R-NTZ-S2-4 and
R-NTZ-S2-5, and the two reads after their STRING refusals, which still refuse);
`ntz_probe4.py` (26) and `ltz_verify_probe.py` (39) change none.

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-28 and widened 2026-09-29 (VN-6, NTZ-1 R-NTZ-S2-6, verifier VD-2; no change here, base equals head). `STRUCT<t: TIMESTAMP_NTZ>` stores the UTC wall of an LTZ field on VALUES and `BY NAME`. New York: Spark `2024-01-01 07:00:00`, RePark `2024-01-01 12:00:00`. Kolkata: Spark `2024-01-01 17:30:00`, RePark `2024-01-01 12:00:00` (`x/{ny,kol}/struct/read`). The verifier's `b/` section shows the residue is wider: `STRUCT<TIMESTAMP>` from NTZ, `ARRAY<TIMESTAMP>`, `ARRAY<TIMESTAMP_NTZ>`, `MAP<STRING,TIMESTAMP>` and `MAP<STRING,TIMESTAMP_NTZ>` each differ from Spark on all 13 doors in New York and Kolkata, NTZ-1's VALUES, SELECT and UPDATE included. Example: `ARRAY<TIMESTAMP_NTZ>` from `array(TIMESTAMP '2024-07-01 12:00:00')` in New York, Spark `12:00:00`, RePark `16:00:00`. Same-type ARRAY stores through MERGE INSERT and UPDATE refuse (`List(Timestamp(ns))` is not store-assignable) where Spark stores. `zone_stores` and every NTZ-1 door map only top-level Iceberg `Timestamp` / `Timestamptz`. Follow-up card: nested session-zone store assignment (ARRAY, MAP, STRUCT) and the MERGE array refusal. |
| R-2 | Dated 2026-09-28 (C-004 scope note): the work order listed DATE→`TIMESTAMP` as unchanged ("midnight walls"); base stored the UTC midnight on the PE-2 doors (New York `1710028800000000`), Spark stores the session midnight (`1710046800000000`), and the one seam converts `DATE` with the naive sources. Excluding `DATE` from `needs_ltz_instant_cast` restores base on every door but MERGE UPDATE, which would need a typed render. |
| R-3 | Dated 2026-09-28 (wording, unchanged): STRING and INT refusals on the `BY NAME`, `INSERT OVERWRITE` and MERGE doors keep RePark's `cannot store-assign` text where Spark answers `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` (NTZ-1 R-NTZ-S2-4/5 STRING halves; both refuse). |
| R-4 | Closed 2026-09-29. The hand-back listed `insertInto(overwrite=True)` as measured on the facade only. The verifier measured it on Spark 4.1.2 with static and dynamic `partitionOverwriteMode`, `write.mode('overwrite').insertInto` and a table-sourced `insertInto`. All are EQUAL on the fold's parent in UTC, New York, Kolkata and Lord Howe in both directions, and C-006's `df_ii_ow` cells replay Spark on the fold. |
| R-5 | Dated 2026-09-29 (verifier VD-3, pre-existing, base equals head, no change here). A MERGE `ON` clause comparing a `TIMESTAMP_NTZ` target column with a `TIMESTAMP` source key does not match where Spark matches. Repro, New York: `MERGE INTO m t USING (SELECT TIMESTAMP '2024-07-01 12:00:00' AS k, 9 AS nid) s ON t.v = s.k …` over the row `TIMESTAMP_NTZ '2024-07-01 12:00:00'`. Spark matches and updates `id` to 9; RePark inserts row 10 (`a/{ny,kol,lhi}/ntz/mon*`). The mirrored case, an LTZ target with an NTZ key, matches Spark. This unit does not convert the `ON` clause. Backlog card: MERGE join-key comparison coercion. |
| R-6 | Dated 2026-09-29, corrected the same day (re-verify RD2-1). The `DATE` + `TIMESTAMP_NTZ` half is fixed by C-007: the pair types `timestamp_ntz`, keeps the DST-gap wall on the pinned doors, and the "leaves unchanged by design" wording was wrong — C-006 acted on it. What remains is the verifier's `mx2/` cell: `nvl(ntz, ts)` types `string` where Spark answers `timestamp`, and its INSERT doors store no rows. That is the `nvl` function's own coercion, not the `VALUES` seam: pre-existing and unchanged. The pinned UNION, CASE, coalesce, IF and greatest mixes equal Spark. |
| R-7 | Dated 2026-09-29 (second re-verify RD3-3, S3, pre-existing, base equals head, no change here). `CAST('<DST-gap wall>' AS TIMESTAMP_NTZ)` shifts the wall even outside `VALUES`. New York: `SELECT CAST(CAST('2024-03-10 02:30:00' AS TIMESTAMP_NTZ) AS STRING)` answers `2024-03-10 03:30:00` where Spark answers `02:30:00`; Havana `00:30` becomes `01:30`. It is the string-to-NTZ cast path, not the `VALUES` seam, and runs through every `CN` cell of the re-verify probe. Backlog card: **NTZ-DST-GAP-CAST-1**. |
| R-8 | Dated 2026-09-29 (second re-verify RD3-4a, S3, pre-existing, base equals head). A mixed `VALUES` stored into a `STRING` column keeps per-cell text: a `DATE` stores `2024-03-10` where Spark writes `2024-03-10 00:00:00`, and the New York NTZ gap row of `n_t` stores `02:30` where Spark writes `03:30`. |
| R-9 | Dated 2026-09-29 (second re-verify RD3-4b, S3, pre-existing, base equals head). America/Havana (midnight DST gap): `DATE` + `TIMESTAMP` stored positionally (INSERT VALUES and INSERT SELECT) into a `TIMESTAMP_NTZ` column stores `00:00` where Spark stores `01:00`. |
| R-10 | Dated 2026-09-29 (second re-verify RD3-4c and RD3-4e, S3, pre-existing). Spark refuses a `DATE` or `TIMESTAMP_NTZ` + string-literal inline table with `INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE`; RePark refuses the SELECT but accepts INSERT VALUES and INSERT SELECT (`door/ny/d_n_s/ntz/ins_sel` stores a row where Spark stores none). Separately, RePark answers `unix_micros` over a `TIMESTAMP_NTZ` column where Spark raises `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`; that acceptance is what turned RD3-2's retype into a wrong number, and it is the function's own coercion. The nested `DATE` + `TIMESTAMP_NTZ` mixes of RD3-4d (array, struct and map rows, in-cell arrays, struct and array stores) are DIFF on base and head alike and belong to R-1. |
| R-11 | Dated 2026-09-29 (third re-verify RD4-2, S3, pre-existing, base equals head). Any MERGE whose `USING` source or `SET` clause holds `CAST(… AS TIMESTAMP_LTZ)` fails with `Unsupported SQL type TIMESTAMP_LTZ` where Spark accepts (192 door cells, identical on base and head): only `KeywordLower` renames `TIMESTAMP_LTZ` (`keyword_lower.rs` `lower_expression`), and the MERGE path lowers through `TimestampNsCastLower`, which does not. Under `spark.sql.timestampType=TIMESTAMP_NTZ`, `CAST(x AS TIMESTAMP_LTZ)` is typed `timestamp_ntz` where Spark types `timestamp`, because the rename makes it a bare `TIMESTAMP`. |
| R-12 | Dated 2026-09-29 (third re-verify RD4-3, S3, pre-existing). `try_cast(<ntz> AS TIMESTAMP)` is typed `timestamp_ntz` and keeps the wall where Spark types `timestamp`; `date_trunc`, `from_utc_timestamp` and `to_utc_timestamp` of an NTZ are typed `timestamp_ntz` outside `VALUES` too (the functions' own return types). Arrays, structs and maps mixing `DATE` and `TIMESTAMP` rows fail with an Arrow concat error. R-7 and R-9 are re-seen unchanged. |
| R-13 | Dated 2026-09-29 (orchestrator ruling (a) on the fourth fold's halt). 103 cells that `fdf98d0b` had brought to Spark return to base's answer under C-009: `coalesce`, `if`, `CASE`, `greatest`, `least` or a struct field mixing `CAST(… AS TIMESTAMP)` with an NTZ or NULL, beside a `TIMESTAMP_NTZ`, `CAST(NULL AS TIMESTAMP_NTZ)` or NULL cell (o5select 76, q6 19, q7 6, o8 2; list in `/tmp/oc-worker/direct/wo/fold4-nd/halt-cells.txt`). None is a regression against main. Getting them to Spark needs a rule typed from the arguments; that is carded as NTZ-VALUES-TYPED-MIX-1 for v1.5.2. |
