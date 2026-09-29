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

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | An LTZ `TIMESTAMP` stored into a `TIMESTAMP_NTZ` column through `INSERT … BY NAME`, `INSERT OVERWRITE` (static, `BY NAME`, `PARTITION (p = v)`, dynamic, column list) and `writeTo().overwritePartitions()` stores Spark's session-zone wall in New York and Kolkata, including the DST gap and overlap rows; UTC sessions store what they stored before. | One Rust pin per door in the NTZ-1 module (UTC, New York, Kolkata; plain, `Z`-suffixed, gap and overlap sources) and the facade replay of Spark's recorded answers per door and zone. | PROVEN | `ntz_store.rs` `by_name_stores_the_ltz_session_wall`, `overwrite_stores_the_ltz_session_wall`, `overwrite_by_name_stores_the_ltz_session_wall`, `overwrite_partition_stores_the_ltz_session_wall`, `dynamic_overwrite_stores_the_ltz_session_wall`, `column_list_overwrite_stores_the_ltz_session_wall` (all red on `adc26586`); `test_ntz_store_doors_1.py` `test_write_door_stores_sparks_value[{utc,ny,kol}/ntz/{byname,ow_static,ow_byname,ow_part,ow_dyn,df_owp,df_ow_cond}]` against `ntz_store_doors_1_spark_oracle.json`. |
| C-002 | A `TIMESTAMP_NTZ` stored into a `TIMESTAMP` column through the C-001 doors and through `MERGE … UPDATE SET`, `UPDATE SET *`, `INSERT (cols)` and `INSERT *` stores Spark's session-zone instant in New York and Kolkata, including the gap and overlap rows; UTC sessions store what they stored before. | One Rust pin per door and the facade replay per door and zone. | PROVEN | `ntz_store.rs` `*_stores_the_ntz_session_instant` for `by_name`, `overwrite`, `overwrite_by_name`, `overwrite_partition`, `dynamic_overwrite`, `column_list_overwrite`, `merge_update`, `merge_update_star`, `merge_insert`, `merge_insert_star` (all red on `adc26586`); `test_ntz_store_doors_1.py` cells `{utc,ny,kol}/ts/*`. |
| C-003 | Nothing else moves: STRING and INT sources refuse with today's text on every door, NULL, same-type (NTZ→NTZ, LTZ→LTZ) and DATE→NTZ stores are unchanged, NTZ-1's doors (VALUES, SELECT, UPDATE, the MERGE LTZ→NTZ arms, the DataFrame append, `writeTo().overwrite(cond)`) are unchanged, time travel reads the pre-insert snapshot unchanged, and the ANSI door's MERGE into an instant column reads a naive wall as UTC as before. | The probe diff before/after, the refusal steps replaying RePark's recorded text, the MERGE LTZ→NTZ pins green on base, and one ANSI-door pin green on base and head. | PROVEN | Probe before/after (`out/doors-before.json`, `out/doors-after.json`): 108 statements change answer, 0 refusal texts, 0 UTC answers, 0 same-type cells; the facade cells assert 132 refusal steps by RePark's recorded text (identical before and after); `ntz_store.rs` `merge_{update,update_star,insert,insert_star}_stores_the_ltz_session_wall` green on base; repark-sql `ansi_ntz_wall_cast.rs` `ansi_merge_into_an_instant_column_keeps_the_utc_reading` green on base and head. |
| C-004 | A `DATE` stored into a `TIMESTAMP` column through the C-002 doors stores Spark's session-zone midnight (base stored the UTC midnight there; the VALUES, SELECT, UPDATE and append doors already stored the session midnight). | The Rust pin over six doors and three zones, and the facade `date` rows. | PROVEN | `ntz_store.rs` `date_stores_the_session_midnight_through_every_door` (red on `adc26586`); `test_ntz_store_doors_1.py` `date` rows of the `ts` cells. |
| C-005 | On `days(v)` and `hours(v)` partitioned tables the rows, the `.partitions` values and the equality filter follow the stored value on INSERT … SELECT, `BY NAME`, `MERGE … INSERT *` and `INSERT OVERWRITE` in all three zones. | The facade replay of Spark's recorded rows, partitions and filter answers. | PROVEN | `test_ntz_store_doors_1.py` `test_partition_transforms_follow_the_stored_value[{utc,ny,kol}/{ts,ntz}/{days,hours}]`. |

## Mutation record (2026-09-28)

Each mutation removed one routing, the named tests ran, and the file was
restored (the whole working diff hashed identical before and after).

| # | Mutation | Red | Green |
|---|---|---|---|
| M1 | Remove the BY NAME routing (`zone_stores_by_name` call in `append_by_name_projection`). | Rust `by_name_stores_the_ltz_session_wall`, `by_name_stores_the_ntz_session_instant`, `date_stores_the_session_midnight_through_every_door` (at `by_name America/New_York`); facade `{ny,kol}/{ntz,ts}/byname` and the eight non-UTC partition cells (each writes one BY NAME row). | Every other Rust pin (30 of 33 in the module) and facade cell (54 door cells, 4 UTC partition cells). |
| M2 | Remove the MERGE INSERT routing for LTZ targets (`zone_wrapping_stream_sql` instant predicate forced false). | Rust `merge_insert_star_stores_the_ntz_session_instant`, `merge_insert_stores_the_ntz_session_instant`, `date_stores_the_session_midnight_through_every_door` (at `merge_insert_star America/New_York`); facade `{ny,kol}/ts/{merge_ins,merge_ins_star}`. | Every other Rust pin (30 of 33) and facade cell (62 of 66; run before the partition cells existed). |

`MERGE … INSERT *` and `MERGE … INSERT (cols)` share one seam
(`insert_stream_checked`; the star expands to explicit columns before SQL
generation), so no mutation can remove the routing for `INSERT *` alone: M2 reds
both INSERT arms and nothing else.

## Tests rewritten

None. No existing pin changed answer: `repark-spark` lib 2487 passed,
`repark-iceberg` lib 759 passed, `repark-sql` all targets green.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ntz-store-doors-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause is walked against Spark's recorded answer on the same door and zone; the probe covers 15 doors, 11 sources, both target types and three zones, and the facade replays 66 door cells and 12 partition cells.
      artifacts: [python/repark/tests/test_ntz_store_doors_1.py, python/repark/tests/ntz_store_doors_1_spark_oracle.json, crates/repark-spark/src/tests/ntz_store.rs]
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
      evidence: Each pin asserts exact walls, instants or refusal texts; M1 and M2 break one routing each and the named pins red before restore.
      artifacts: [crates/repark-spark/src/tests/ntz_store.rs, python/repark/tests/test_ntz_store_doors_1.py]
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
| R-1 | Dated 2026-09-28 (VN-6, NTZ-1 R-NTZ-S2-6, no change here): `STRUCT<t: TIMESTAMP_NTZ>` stores the UTC wall of an LTZ field on VALUES and `BY NAME` — New York Spark `2024-01-01 07:00:00`, RePark `2024-01-01 12:00:00`; Kolkata Spark `2024-01-01 17:30:00`, RePark `2024-01-01 12:00:00` (`x/{ny,kol}/struct/read`). |
| R-2 | Dated 2026-09-28 (C-004 scope note): the work order listed DATE→`TIMESTAMP` as unchanged ("midnight walls"); base stored the UTC midnight on the PE-2 doors (New York `1710028800000000`), Spark stores the session midnight (`1710046800000000`), and the one seam converts `DATE` with the naive sources. Excluding `DATE` from `needs_ltz_instant_cast` restores base on every door but MERGE UPDATE, which would need a typed render. |
| R-3 | Dated 2026-09-28 (wording, unchanged): STRING and INT refusals on the `BY NAME`, `INSERT OVERWRITE` and MERGE doors keep RePark's `cannot store-assign` text where Spark answers `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]` (NTZ-1 R-NTZ-S2-4/5 STRING halves; both refuse). |
