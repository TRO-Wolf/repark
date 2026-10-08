# Unit ledger — ZONE-HORIZON-RENDER-1 · an instant after 2099 renders at Spark's wall clock

**Date:** 2026-10-08 · **Branch:** `fix/zone-horizon-render-1` · **Base:** `156be81c` (`main`)
**Model:** claude-opus-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [zone-horizon-render-1-card-2026-10-07.md](../../roadmap/mid-term/zone-horizon-render-1-card-2026-10-07.md).

**Retires:** this ledger moves to `../completed/` when C-012 and C-013 are ruled and the unit's
last commit lands.

**Why now.** The C-2d fold 1 re-verify (PR #991) graded it S2: chrono-tz tabulates zone
transitions up to 2099 and Java applies a zone's final rule for ever. Only the wall clock →
instant direction of the `TIMESTAMP` literal (and, since C-2d, the Postgres localiser) read the
shared horizon. Every other site read chrono-tz directly, so with the session zone
`America/New_York` the instant `2100-07-01 16:00:00 UTC` rendered as `11:00:00` where Spark
gives `12:00:00`.

**Not in this unit:** `STATUS.md`, `.github/`, a new crate edge, any answer before 2100.

## PROPOSITION LEDGER — ZONE-HORIZON-RENDER-1 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | With the session zone New York, Sydney, Lord Howe or Kolkata, `CAST(TIMESTAMP '<y>-07-01 12:00:00' AS STRING)`, `hour(…)` and `date_format(…, 'HH:mm')` answer `<y>-07-01 12:00:00`, `12`, `12:00` for y in 2100, 2104, 2500, 9999. | `test_zone_horizon_render_1.py::test_the_three_expressions_of_the_card_answer_noon`; Rust `tests::zone_horizon_render::the_three_expressions_of_the_card_answer_noon` (also `UTC` and `-08:00`). | PROVEN | Main answered `11:00:00 / 11 / 11:00` in New York at 2100. Green on the fix; red under M1, M2, M6 and M8 (§Mutations). |
| C-002 | Every 2099 control cell answers what main answers. | The same two tests at 2099; the grid diff. | PROVEN | Of the 1392 control cells main and the fix differ from Spark in the same 98, with identical answers (§Grid). |
| C-003 | The horizon has one home for both directions: `zone_horizon::offset_at_instant`, `wall_at_instant`, `zoned_at_instant` and `offsets_at_wall` read the zone at the proxy year outside 1200–2099 and are the zone itself inside it; `tabulated_utc_seconds` is that range in epoch seconds. | `crates/repark-common/src/zone_horizon/tests.rs` (7 tests over a toy zone tabulated to 2099). | PROVEN | `cargo test -p repark-common --lib`: 95 passed. Red under M1, M3, M4 and M5. |
| C-004 | The round trip holds: for sampled instants in 2100–2500 the wall clock of the instant, placed again, is the instant; the literal kernel agrees with the placement on every sample. | `zone_horizon/tests.rs::the_wall_of_an_instant_reads_back_as_the_instant` (toy zone, > 45 000 samples); `tests::zone_horizon_render::the_wall_of_an_instant_reads_back_as_the_instant_in_real_zones` (8 zones, > 12 000 samples each). | PROVEN | With the instant's own offset as the preferred offset every sample returns; without it fewer than 20 samples per zone differ, all inside an overlap. |
| C-005 | The oracle is recorded from live Spark 4.1.2 through the committed recorder, and a live re-run of the committed probes reproduces it. | `zone_horizon_render_1_spark_oracle.json`, `_record_zone_horizon_render_1.py`, `test_live_oracle_matches_committed_fixture`. | PROVEN | 6972 cells, no Spark error cell. `_record_zone_horizon_render_1.py --check` on live Spark: `oracle matches (6972 cells)` (2026-10-08). The pytest live cell runs the same `record` call; no interpreter on this machine holds both pytest and PySpark, so it was not run as a pytest cell (§Gates). |
| C-006 | Every instant → wall-clock cell of the grid outside the residue answers Spark. | `test_every_cell_outside_the_residue_answers_spark` (per zone); Rust `every_recorded_instant_renders_at_the_wall_clock_spark_gives`, `every_extractor_reads_the_final_rule_in_new_york`. | PROVEN | 16 instant functions changed (§Grid). |
| C-007 | Every wall-clock → instant cell outside the residue answers Spark, gap and overlap walls included. | The same per-zone pin; Rust `every_recorded_wall_clock_is_placed_at_the_instant_spark_gives`, `every_constructor_reads_the_final_rule_in_new_york`, `a_time_only_string_takes_today_from_the_final_rule`. | PROVEN | `make_timestamp`, `to_timestamp` with a pattern, `unix_timestamp`, `to_unix_timestamp`, `TIMESTAMP_NTZ` → `TIMESTAMP`, `DATE` → `TIMESTAMP`, `convert_timezone` (§Grid). |
| C-008 | A CSV and a JSON write of instants after 2099 carry Spark's wall clock and offset. | The `csv` and `json` frame cells of the per-zone pin; `test_every_residue_cell_is_explained` asserts no frame cell is in the residue; Rust `an_instant_after_2099_is_written_at_the_final_rule`. | PROVEN | The 6 frame cells of the three shifting zones differed on main; all 12 answer Spark on the fix. |
| C-009 | Every residue cell after 2099 is explained: it differs in the 2099 control too, or it is in a named class. | `test_every_residue_cell_is_explained`. | PROVEN | 832 cells (§Residue). |
| C-010 | `collect()` and the Arrow path are unchanged: the session wall clock and the instant. | `test_collect_and_the_arrow_path_carry_the_same_instant`. | PROVEN | `collect()` converts in Python (`zoneinfo`), which already applied the final rule; it was right on main. |
| C-011 | Every instant ↔ wall-clock site in the workspace reads the zone through the helper; the routed sites carry the pins §Sites names. | §Sites; the Rust pins named there. | PROVEN | 19 sites routed, 0 copies of the rule; two have no pin of their own (R-9). M2, M6 and M8 each make one site bypass the helper and turn its pins red. |
| C-012 | `from_utc_timestamp` and `to_utc_timestamp` read the final rule after 2099. | The eight `*_utc_timestamp*` rows of the grid. | OPEN | Both are `datafusion-spark` kernels, outside the workspace, so they cannot call the helper. Closing question (Q1): own the two functions in `repark-functions` on the funnel in this unit, or in a follow-up unit? Lean: a follow-up — owning them is a port of two Spark functions (their implicit `TIMESTAMP_NTZ` → `TIMESTAMP` cast and gap rule differ from the upstream kernel before 2100 too: 13 control cells), which is wider than the horizon. |
| C-013 | On data before 2100 a 1M-row `hour()` and `date_format` are not more than 2 % slower than on main. | §Perf. | OPEN | By instruction count the three measured statements are within the guard (`hour` +1.5 %, `date_format` +0.3 %, the string cast +0.2 %). By wall clock and cycles this machine reads +1.4 % to +4 % at that instruction parity, and the profile shows the extra cycles spread evenly over code this unit does not touch. Closing question (Q2): does the instruction count close the guard, or does it need a wall-clock reading on a quiet machine? Lean: the instruction count closes it; §Perf gives the reasons. |
| C-014 | Each pin fails for its reason: eight mutations, each red. | §Mutations. | PROVEN | See §Mutations. |
| C-015 | The card row reads closed with the date, the registry carries the unit's row, and the CAST-TS-STRING-1 residue reads closed. | Diff of `task/roadmap/mid-term/map.md`, the card, `docs/spark-sql-iceberg-parity.md`; `check_docs_links.py`. | PROVEN | No next-release notes draft exists (`ls task/roadmap/mid-term | grep -i release-notes` lists only shipped notes and the 1.5.0 draft that shipped), so no notes line was added. |

## Grid

Step 0, measured before any code change. Spark: live PySpark 4.1.2, `local[1]`, ANSI on,
recorded 2026-10-08 into `python/repark/tests/zone_horizon_render_1_spark_oracle.json` (one
answer per cell; the recorder rebuilds each statement from the stored probes). Main: `156be81c`
through the facade, the same statements; its answer for each of the 1516 cells that differ from
Spark is in [zone-horizon-render-1-main-answers.json](zone-horizon-render-1-main-answers.json)
as `[zone|year|probe|function, Spark's answer, main's answer]`. A cell not listed there answered
exactly what Spark answered.

**Shape.** Zones: `America/New_York`, `Australia/Sydney` (Southern hemisphere),
`Australia/Lord_Howe` (a 30-minute shift), `Asia/Kolkata` (no shift), `UTC`, `-08:00` (a fixed
offset). Years: 2099 (control), 2100, 2104 (a leap year), 2500, 9999 (the last year Python's
`datetime` and Spark's default formatter both hold). Instant probes per zone and year: January
15 and July 15 at 12:34:56 UTC, and in a shifting zone the last second before and the first
instant of each of the two transitions. Wall probes: January 15 and July 15 at 12:00:00, and in
a shifting zone one wall clock in the middle of the gap and one in the middle of the overlap.
An instant is spelled `TIMESTAMP '<wall> UTC'`: the first recording used `timestamp_micros(n)`
and every instant cell failed on RePark with `UNRESOLVED_ROUTINE` (R-6).

**Spark against the proxy-year rule.** Spark extrapolates a zone's final rule for ever. The
proxy-year rule reads the same month, day and time in the latest year of 2072–2099 with the same
leap flag and January-1 weekday. Across the grid the two agree on every cell: every transition
Spark reports in 2100, 2104, 2500 and 9999 in the three shifting zones falls on the instant the
proxy year gives, to the second (the `t1_*` / `t2_*` probes, 46 functions each), and every gap
and overlap wall is placed where Spark places it. No finding about the shared horizon, and no
answer before 2100 moves, so the first halt case does not apply. Not measured: a zone whose
final rule is not a weekday-of-month rule (R-7).

**Per function.** Cells that differ from Spark, by year, as `2099 / 2100 / 2104 / 2500 / 9999`
(the two write rows are one cell per zone, so they carry one number). 6972 cells: 1516 differ
on main, 832 after the fix.

| Function | Cells | Main differs | After the fix |
|---|---|---|---|
| `cast_string` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `cast_date` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `cast_ntz` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `hour` | 120 | 0 / 8 / 8 / 8 / 8 | 0 / 0 / 0 / 0 / 0 |
| `minute` | 120 | 0 / 3 / 3 / 3 / 3 | 0 / 0 / 0 / 0 / 0 |
| `second` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `year` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `month` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `dayofmonth` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `dayofweek` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `dayofyear` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `weekofyear` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `quarter` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `weekday` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `last_day` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `date_format` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `date_format_hm` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `to_char` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `date_trunc_hour` | 120 | 0 / 4 / 4 / 4 / 4 | 0 / 0 / 0 / 0 / 0 |
| `date_trunc_day` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `date_trunc_month` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `date_trunc_year` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `trunc_month` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `extract_hour` | 120 | 22 / 22 / 22 / 22 / 22 | 22 / 22 / 22 / 22 / 22 |
| `extract_minute` | 120 | 5 / 5 / 5 / 5 / 5 | 5 / 5 / 5 / 5 / 5 |
| `extract_doy` | 120 | 8 / 8 / 8 / 8 / 8 | 8 / 8 / 8 / 8 / 8 |
| `date_part_hour` | 120 | 22 / 22 / 22 / 22 / 22 | 22 / 22 / 22 / 22 / 22 |
| `date_part_day` | 120 | 8 / 8 / 8 / 8 / 8 | 8 / 8 / 8 / 8 / 8 |
| `from_utc_timestamp` | 120 | 0 / 12 / 12 / 12 / 12 | 0 / 12 / 12 / 12 / 12 |
| `to_utc_timestamp` | 120 | 6 / 12 / 12 / 12 / 12 | 6 / 12 / 12 / 12 / 12 |
| `from_unixtime` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `from_unixtime_pattern` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `timestampadd_hour` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `timestampadd_day` | 120 | 0 / 6 / 6 / 6 / 6 | 0 / 0 / 0 / 0 / 0 |
| `timestampadd_month` | 120 | 0 / 6 / 6 / 6 / 6 | 0 / 0 / 0 / 0 / 0 |
| `interval_day` | 120 | 6 / 6 / 6 / 6 / 6 | 6 / 6 / 6 / 6 / 6 |
| `interval_month` | 120 | 6 / 6 / 6 / 6 / 6 | 6 / 6 / 6 / 6 / 6 |
| `date_add` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `timestampdiff_day` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `window_day_start` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `window_day_end` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `window_hour_start` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `to_json` | 120 | 0 / 9 / 9 / 9 / 9 | 0 / 0 / 0 / 0 / 0 |
| `unix_date` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `next_day` | 120 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `months_between` | 120 | 5 / 9 / 9 / 9 / 9 | 5 / 6 / 6 / 6 / 6 |
| `literal` | 90 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `cast_from_string` | 90 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `to_timestamp` | 90 | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| `to_timestamp_pattern` | 90 | 0 / 6 / 6 / 6 / 6 | 0 / 0 / 0 / 0 / 0 |
| `unix_timestamp` | 90 | 3 / 6 / 6 / 6 / 6 | 3 / 3 / 3 / 3 / 3 |
| `to_unix_timestamp` | 90 | 0 / 6 / 6 / 6 / 6 | 0 / 0 / 0 / 0 / 0 |
| `make_timestamp` | 90 | 0 / 6 / 6 / 18 / 18 | 0 / 0 / 0 / 18 / 18 |
| `make_timestamp_zone` | 90 | 0 / 9 / 9 / 18 / 18 | 0 / 0 / 0 / 18 / 18 |
| `ntz_to_ltz` | 90 | 0 / 6 / 6 / 18 / 18 | 0 / 0 / 0 / 18 / 18 |
| `to_utc_timestamp_wall` | 90 | 4 / 9 / 9 / 18 / 18 | 4 / 9 / 9 / 18 / 18 |
| `from_utc_timestamp_wall` | 90 | 3 / 12 / 12 / 18 / 18 | 3 / 12 / 12 / 18 / 18 |
| `convert_from_utc` | 90 | 0 / 9 / 9 / 18 / 18 | 0 / 0 / 0 / 18 / 18 |
| `convert_to_utc` | 90 | 0 / 9 / 9 / 18 / 18 | 0 / 0 / 0 / 18 / 18 |
| `round_trip` | 90 | 0 / 5 / 5 / 5 / 5 | 0 / 0 / 0 / 0 / 0 |
| `round_trip_hour` | 90 | 0 / 4 / 4 / 4 / 4 | 0 / 0 / 0 / 0 / 0 |
| `date_to_timestamp` | 90 | 0 / 6 / 6 / 6 / 6 | 0 / 0 / 0 / 0 / 0 |
| `csv` | 6 | 3 | 0 |
| `json` | 6 | 3 | 0 |

The names are the recorder's (`INSTANT_FUNCTIONS`, `WALL_FUNCTIONS`); each maps to one SQL
expression there. `show()` is not a row: the facade's styled table prints instants in UTC with
their type, so it never reads the session zone (R-8). `collect()` is C-010.

**What moved.** 700 cells that differed on main answer Spark after the fix: 16 instant
functions (`cast_string`, `cast_ntz`, `hour`, `minute`, `date_format`, `date_format_hm`,
`to_char`, `date_trunc_hour` / `_day` / `_month`, `from_unixtime`, `from_unixtime_pattern`,
`timestampadd_day` / `_month`, `to_json`, and the January and July cells of `months_between`),
11 wall functions (`to_timestamp_pattern`, `unix_timestamp`, `to_unix_timestamp`,
`make_timestamp`, `make_timestamp_zone`, `ntz_to_ltz`, `convert_from_utc`, `convert_to_utc`,
`date_to_timestamp`, and the two round-trip rows) and the 6 write cells of the shifting zones.
In `Asia/Kolkata`, `UTC` and `-08:00` the 6 cells that moved per zone are the ones that name
New York as an argument zone. The literal rows (`literal`, `cast_from_string`, `to_timestamp`)
were already right: they read the horizon before this unit. 16 cells moved the other way (R-5).
No 2099 cell moved: main and the fix differ from Spark in the same 98 control cells with the
same answers.

## Sites

Step 1. `git grep -n "chrono_tz\|Tz::\|offset_from_utc\|with_timezone\|from_utc_datetime" crates/`
plus the spellings that grep misses (`from_local_datetime`, `offset_from_local_datetime`,
`timestamp_opt`, `and_local_timezone`, Arrow's `date_part` over a zone-labelled array).

**Reads the horizon after the fix** (19 sites; each calls `zone_horizon`, none copies the rule):

| Site | Direction | Serves | Pin |
|---|---|---|---|
| `repark-functions` `datetime/session_wall.rs::local_datetime_from_micros` | instant → wall | `date_format`, `date_trunc`, `trunc`, cast to `DATE`, `to_char`, `from_unixtime`, `timestampadd`, interval arithmetic, `convert_timezone`, `TIME` extraction, LTZ → NTZ | `every_recorded_instant_renders_at_the_wall_clock_spark_gives`, `every_extractor_reads_the_final_rule_in_new_york` |
| … `::offset_at_instant` | instant → offset | `date_trunc` (the overlap side) | the round-trip pin |
| … `::offset_before_gap` | instant → offset | a wall clock in a gap | `every_recorded_wall_clock_is_placed_at_the_instant_spark_gives` (the gap rows) |
| … `::micros_from_local_datetime` | wall → instant | `make_timestamp`, `to_timestamp` with a pattern, `to_unix_timestamp`, NTZ → LTZ, `DATE` → `TIMESTAMP`, `convert_timezone`, CSV / JSON string parsing | the same pin; `every_constructor_reads_the_final_rule_in_new_york` |
| … `::within_the_tables` + `::instant_part` (Arrow `date_part` over a zone label only for a batch wholly inside 1200–2099) | instant → wall | `year` … `second`, `quarter`, `weekofyear`, `dayofweek`, `dayofyear`, `weekday` | `the_three_expressions_of_the_card_answer_noon`, the extractor pin, `a_batch_reads_the_tables_only_when_every_instant_is_inside_them` (M6, M8) |
| `timestamp_cast.rs::wall_clock_from_ticks` | instant → wall | the cast to `STRING` | the card pin (M2) |
| `timestamp_cast.rs::unix_seconds_from_string` | wall → instant | `unix_timestamp(<string>)` | the constructor pin |
| `timestamp_ltz_ntz.rs` (LTZ → NTZ) | instant → wall | the cast to `TIMESTAMP_NTZ` | facade `cast_ntz` cells |
| `timestamp_ns_cast.rs::session_wall_nanos` | instant → wall | nanosecond casts | facade grid (indirect) |
| `json/to_json.rs::timestamp_text` | instant → wall + offset | `to_json` | the extractor pin (`to_json`) |
| `json/decode.rs::local_micros` | wall → instant | `from_json` | none of its own (R-9) |
| `collection/array_append/coerce.rs::ZoneSpans::resolve_day` | wall → offset | array element coercion | none of its own (R-9) |
| `spark_string_timestamp/instant.rs::today_in` | instant → date | a time-only string | `a_time_only_string_takes_today_from_the_final_rule` |
| `repark-core` `session/text_write_format.rs::micros_to_wall_zone` | instant → wall + offset | CSV and JSON writes (and the offset cache in `udf.rs`) | `an_instant_after_2099_is_written_at_the_final_rule`; facade `csv` / `json` cells |
| `text_partition.rs::timestamp_partition_text` | instant → wall | text partition directory names | `a_timestamp_partition_after_2099_is_named_by_the_final_rule` |
| `partition_discovery.rs::parse_timestamp_micros_zone` | wall → instant | partition values | `a_timestamp_partition_value_after_2099_is_placed_by_the_final_rule` |
| `orc_scan.rs::writer_wall_to_utc` | wall → instant | the ORC writer zone | `a_writer_wall_clock_after_2099_is_placed_by_the_final_rule` |
| `time_travel/sql_text.rs::zoned_wall_to_ms`, `::format_snapshot_bound_ms` | both | `TIMESTAMP AS OF` bounds and their messages | `a_snapshot_bound_after_2099_reads_the_final_rule_in_both_directions` |
| `session/zone_localiser.rs::place` | wall → instant | Postgres `timestamp` | the C-2d pins (it read `proxy_year` inline before; now the shared helper) |

`spark_string_timestamp/instant.rs::offset_micros_at` already read `proxy_year` and is unchanged:
it hands a proxy-year wall clock to `micros_from_local_datetime`, where the helper is then the
identity. It keeps its own call because it takes years past chrono's calendar.

**Not affected** (no zone rule is read): the `with_timezone("UTC")` / `with_timezone_opt(…)`
label sites (`repark-connect` `provider/scan.rs`, `types/postgres.rs`; `repark-functions`
`instant_ts.rs`, `java_datetime.rs`, `spark_sequence.rs`, `spark_string_timestamp.rs`,
`timestamp_ltz_ntz.rs`, `timestamp_ns_cast.rs`, `spark_time_window.rs`, `csv/from_csv.rs`,
`json/decode.rs`, `try_invert/temporal.rs`, `temporal_ctor/adddiff.rs`, `temporal_ctor/make.rs`,
`collection/array_append.rs`, `coerce.rs`; `repark-core` `partition_discovery.rs`;
`repark-python` `cdf_infer/build.rs`); the `Tz::from_str` parses (`session_time_zone.rs`,
`text_write_format/udf.rs`, `datetime.rs`, `temporal_ctor.rs`, `timestamp_cast.rs`,
`try_invert/strict.rs`, `time_travel/sql_text.rs`, `zone_localiser.rs`); `Utc.from_utc_datetime`
in `orc_scan.rs`; `window` (Spark aligns windows to the epoch, not to the session zone: the
three `window_*` rows agree on main and after).

**Fixed offset only** (no table, so no horizon): `temporal_ctor.rs::wall_to_instant_micros` /
`instant_to_wall_micros` (the `Fixed` arm), `time_travel/sql_text.rs::fixed_offset_zone`, the
`SparkZone::Offset` arms of the literal, and `DateTime::parse_from_str` with an explicit
offset in `csv.rs` and `json/decode.rs`.

**Outside the workspace** (cannot call the helper; no crate edge would change that):
`datafusion-spark`'s `from_utc_timestamp` / `to_utc_timestamp` (C-012); DataFusion's built-in
`extract` / `date_part` (R-2); the facade's `collect()`, which converts in Python and was
already right.

## Residue

The 832 cells of `python/repark/tests/zone_horizon_render_1_residue.json`, each pinned as still
differing:

| Class | Functions | Cells | At 2099 too |
|---|---|---|---|
| R-1 upstream kernel, reads standard time after 2099 | `from_utc_timestamp`, `to_utc_timestamp`, and both over a `TIMESTAMP_NTZ` | 223 | 13 (transition, gap and overlap cells) |
| R-2 built-in never reads the session zone | `extract_hour`, `extract_minute`, `extract_doy`, `date_part_hour`, `date_part_day` | 325 | 65 |
| R-3 nanosecond range (2262) | `make_timestamp`, `make_timestamp_zone`, `ntz_to_ltz`, `convert_from_utc`, `convert_to_utc` at 2500 and 9999 | 180 | 0 (the year is not reachable) |
| R-4 transition-day arithmetic | `interval_day`, `interval_month`, `months_between`, `unix_timestamp` in a gap | 104 | 20 |

Notes on the classes:

- **R-1** is C-012, the one open clause. `from_utc_timestamp` over an instant has no control
  cell that differs: it is right through 2099 and wrong only after it.
- **R-2.** `extract(HOUR FROM t)` and `date_part('HOUR', t)` go to DataFusion's built-in, which
  reads the `UTC` label and never the session zone: New York at `2099-01-15 12:34:56 UTC`
  answers `12` where Spark answers `7`. The repo's own `hour(t)` is right. A defect at every
  year, not a horizon defect; not fixed here (it changes answers before 2100).
- **R-3.** `make_timestamp(2500, …)` raises `long overflow` and `TIMESTAMP_NTZ '2500-…'` raises
  `INVALID_TYPED_LITERAL`: both pass through nanoseconds. Not a horizon defect.
- **R-4.** On a transition day `timestamp ± INTERVAL` and `months_between` read the wall clock
  where Spark reads elapsed time, and `unix_timestamp('<gap wall>')` raises
  `CANNOT_PARSE_TIMESTAMP` where Spark shifts the wall clock forward. All four differ in the
  2099 control. `test_every_residue_cell_is_explained` names `months_between` on a `*_at` probe
  as a class because its 2099 autumn twin hides the difference (both days of month are 1).
- **R-5, the 16 cells that moved away from Spark.** On main these agreed by accident: main saw
  no transition after 2099, so it never met the transition-day defects of R-4 there. With the
  real transition days in place, `unix_timestamp` of the New York gap wall refuses at 2100,
  2104, 2500 and 9999 as it does at 2099 (4 cells), and `months_between` at the first instant
  of the spring change differs as it does at 2099 (12 cells, three zones). They are in the
  residue and explained by their 2099 twins. Closing R-4 closes them.

Out of scope, observed while measuring:

- **R-6.** `timestamp_micros`, `timestamp_seconds`: `UNRESOLVED_ROUTINE`.
- **R-7.** The grid holds only zones whose final rule is "the n-th weekday of a month". A zone
  whose final rule is a fixed date, or whose table ends before 2099, was not measured.
- **R-8.** The facade's styled `show()` prints an instant in UTC with its Arrow type, not in the
  session zone as Spark's `show()` does.
- **R-9.** Two routed sites have no pin of their own: `json/decode.rs::local_micros`
  (`from_json`) and `collection/array_append/coerce.rs::ZoneSpans::resolve_day`. Both are one
  call to `offsets_at_wall`, which the common pins hold.
- **R-10.** An instant after year 262142 is past chrono's calendar
  (`DateTime::from_timestamp_micros` gives none); Spark's range ends at 294247. Unchanged by
  this unit and not measured.
- **R-11.** Other date and time shapes RePark refuses where Spark answers, met while building
  the grid and replaced by supported spellings: `date_format` with `S`, `X`, `x` or `Z`;
  `last_day`, `date_add` and `next_day` over a `TIMESTAMP`; `window(...)` in a `SELECT` with
  no `GROUP BY`; `to_csv`. A CSV write emitted a header with no `header` option set, and one two-row
  ordered frame came back from `coalesce(1).write` in the other order; the recorder sets the
  header and sorts the lines.

## Perf

Step 4. The harness (not committed): 1 000 000 instants of 2024, one every 31 seconds from
2024-01-01, as `Timestamp(µs, "UTC")` in 125 batches, one partition, session zone
`America/New_York`; each statement run 41 times. Built as a standalone example against
`repark-functions` on the `release` profile (thin LTO, one codegen unit), at main `156be81c`
and at this unit's tree, and run alternately, pinned to one core, under `perf stat`.

| Statement | Instructions, main → unit | Branches | Cycles (three alternating runs each) |
|---|---|---|---|
| `SELECT sum(hour(t))` | 18.173 G → 18.450 G, **+1.53 %** | +0.32 % | main 0 / +0.1 / +1.1 %; unit +1.4 / +2.9 / +4.3 % |
| `SELECT sum(length(date_format(t, 'yyyy-MM-dd HH:mm:ss')))` | 217.19 G → 217.76 G, **+0.26 %** (run-to-run spread ±0.3 % on both sides) | +0.38 % | main 0 / 0 / +0.4 %; unit +1.5 / +1.9 / +2.3 % |
| `SELECT sum(length(CAST(t AS STRING)))` | 156.013 G → 156.300 G, **+0.18 %** | +0.14 % | main 0 / +0.1 / +0.3 %; unit +3.9 / +4.0 / +4.3 % |

Each run is 41 million row evaluations, so the instruction deltas are +6.8 per row for
`hour` (one branch-free pass over the batch's ticks; the kernel itself is Arrow's, as on main),
about +14 per row for `date_format` inside its noise, and +7.0 per row for the cast (the year
compare and the `Option` it returns).

**What the wall clock says, and why the instruction count is the reading offered.** Every
wall-clock comparison made on this machine is listed; they do not agree with each other:

| Build | `hour` | `date_format` | string cast |
|---|---|---|---|
| `ci` profile, test binary, first shape of the helper | −4 % | +1.5 % | −3 % |
| `ci` profile, test binary, helper marked `#[inline]` | −8 % | +0.2 % | +2.7 % |
| `release`, test binary, same code | −8 % | +5 % | +3.4 % |
| `release`, test binary, proxy branch moved out of line | −8 % | +1 to +3.7 % | +2.4 % |
| `release`, test binary, **control**: main's own expression restored at the two hot sites | −8 % | +2.5 to +4.7 % | +3 to +5.5 % |
| `release`, example, proxy branch out of line | +3.3 % | +1.3 % | +4.6 % |
| `release`, example, **control** as above | +3.4 % | +3.3 % | +6.0 % |
| `release`, example, the committed code (cycles) | +1.4 % | +1.5 % | +3.9 % |

Three things say the cycle differences are not the helper. (1) The control: with main's exact
expression back at the `date_format` and cast sites, the binary is no faster than with the
helper, and usually slower. (2) The instruction counts are level where the cycles are not.
(3) The profile of the cast statement has the same shape in both binaries — `core::fmt`,
`String::write_str`, `malloc`, `memmove` each keep their share to within half a point — so
the extra cycles fall evenly on code this unit does not touch, libc included. That is what a
change in where code lands does, and any edit to the crate moves it: between two of this
unit's own builds the cast went from 3 % faster to 2.7 % slower. The machine was also busy
with other lanes for the whole session.

So C-013 stays OPEN (Q2). What was done to keep the pre-2100 path identical to main: inside
1200–2099 `wall_at_instant` is the expression the call sites held before
(`zone.from_utc_datetime(utc).naive_local()`), behind one year compare; the proxy branch is
`#[cold]` and out of line; the calendar extractors keep Arrow's kernel for a batch wholly
inside the tables and pay one pass over the ticks per batch.

## Mutations

Step 5. Each is one edit, run against
`cargo test -p repark-common -p repark-functions -p repark-core --lib --features repark-core/postgres -- zone_horizon 2099 final_rule spark_string_timestamp zone_localiser`
and then reverted. Red counts are failing tests.

| Id | Edit | Red | Tests |
|---|---|---|---|
| M1 (brief) | remove the proxy from the helper: `in_proxy_year` returns the moment unchanged | 22 | all five `zone_horizon::tests` that read past a table end; all eight `tests::zone_horizon_render` pins; the five core site pins; three `zone_localiser` pins; `chrono_tz_tables_stop_after_the_last_tabulated_year` |
| M2 (brief) | one site bypasses the helper: the cast to string reads chrono-tz directly | 2 | `the_three_expressions_of_the_card_answer_noon`, `every_extractor_reads_the_final_rule_in_new_york` |
| M3 (brief) | shift the horizon year by one: `LAST_TABULATED_YEAR = 2100` | 16 | `an_instant_past_the_horizon_reads_its_proxy_year`, `the_tabulated_seconds_are_the_years_the_helpers_leave_alone`, five `tests::zone_horizon_render` pins, the five core site pins, two `zone_localiser` pins, `far_future_walls_follow_the_final_dst_rule`, `chrono_tz_tables_stop_…` |
| M4 (own) | the proxy drops the calendar match: every year after 2099 reads 2099 | 5 | `the_change_past_the_horizon_falls_on_the_rule_day_of_the_real_year`, `a_wall_clock_past_the_horizon_keeps_its_gap_and_its_overlap`, `every_recorded_instant_…`, `every_recorded_wall_clock_…`, the real-zone round trip. July noon stays green: only the transition days tell the calendars apart |
| M5 (own) | the fast path swallows the years before 1200 | 1 | `an_instant_before_the_tables_reads_its_proxy_year` |
| M6 (own) | the calendar extractors' slow path reads chrono-tz directly | 3 | the card pin, the extractor pin, `a_batch_reads_the_tables_only_when_every_instant_is_inside_them` |
| M7 (own) | an overlap takes the later offset | 3 | `every_recorded_wall_clock_is_placed_at_the_instant_spark_gives`, and two CAST-TS-STRING-1 pins |
| M8 (own) | the batch check never sends a batch to the slow path | 3 | the same three as M6 |

The facade pins were not run under the mutations. Their red on main is the measurement itself:
main answers 1516 cells differently from Spark and the residue holds 832, so each of the six
per-zone pins fails on main.

## Gates

On the unit's tree, 2026-10-08:

- `cargo test -p repark-common -p repark-functions -p repark-core -p repark-spark --lib`
  (with `--features repark-core/postgres`): 95, 899 (1 ignored), 1291 (1 ignored) and 2630
  (5 ignored) passed, 0 failed.
- `make develop`, then `pytest python/repark/tests -k "timestamp or date or zone or tz or time" -n 8`:
  2193 passed, 46 skipped, 17 xfailed (with `python/repark-parity/src` on `PYTHONPATH`;
  without it 22 modules fail to import `repark_parity`, on main too).
  `test_zone_horizon_render_1.py`: 12 passed, 1 skipped (the live leg). The whole facade suite
  ran once on the tree before the helper was reshaped for §Perf: 15603 passed, 508 skipped,
  153 xfailed, 0 failed.
- Live Spark 4.1.2: `_record_zone_horizon_render_1.py --check` re-ran the committed probes
  and matched all 6972 cells. The pytest live leg calls the same `record`; it was not run as
  a pytest cell because no interpreter on this machine holds both pytest and PySpark.
- The every-round set: see the hand-back.

## Open questions

- **Q1 (C-012).** `from_utc_timestamp` / `to_utc_timestamp` are `datafusion-spark` kernels.
  Own them in `repark-functions` on the funnel in this unit, or in a follow-up? Lean: a
  follow-up; it is a port of two Spark functions, and their pre-2100 edges differ too.
- **Q2 (C-013).** Does the instruction count close the perf guard? Lean: yes.

The coverage attestation is filed when C-012 and C-013 are ruled.
