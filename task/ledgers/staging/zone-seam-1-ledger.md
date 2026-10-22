# Unit ledger — ZONE-SEAM-1 · one session-zone conversion seam for every zoned-timestamp conversion

**Date:** 2026-10-22 · **Branch:** `feat/zone-seam-1` · **Base:** `b7a20167` ·
**Model:** Claude Opus 5.5 (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: release 1.5.6.

**Order:** [../../wo/zone-seam-1/order.md](../../wo/zone-seam-1/order.md) (owner-adopted
2026-10-20); step 0: [../../wo/zone-seam-1/sketch.md](../../wo/zone-seam-1/sketch.md); card
[CONTAINER-CAST-ZONE-1](../../roadmap/mid-term/container-cast-zone-1-card-2026-10-20.md); pin
corpus ZONE-ORACLE-1 (`python/repark/tests/timestamps/`). **This ledger closes when the unit's
pull request merges.**

## Orchestrator rulings on the sketch (2026-10-22, about 00:20 UTC)

Each is recorded as given; the owner reads them in the morning report.

- **ZS-1** — orchestrator ruling ZS-1, 2026-10-22, under the owner's grant: the §4 replay gate
  reads as zero DIFFERENT among the cells the sketch assigns to this unit (sketch §8), with the
  residue listed exactly by id. Condition: this ledger classifies every residue cell by cause
  with Spark's value and main's value side by side (§ Residue below), and the card's `from_json`
  row gets its own paragraph. The residue is carded as two follow-up lines on
  CONTAINER-CAST-ZONE-1, not new cards.
- **ZS-2** — orchestrator ruling ZS-2, 2026-10-22, under the owner's grant: the 20
  `to_utc_timestamp` / `from_utc_timestamp` DATE and untyped-NULL cells are in scope.
- **ZS-3** — orchestrator ruling ZS-3, 2026-10-22, under the owner's grant: numeric targets other
  than `BIGINT` and timestamp map keys stay native; one follow-up line on the ZONE-ORACLE-1
  order names the expressions to record next.
- **ZS-4** — orchestrator ruling ZS-4, 2026-10-22, under the owner's grant: the U-8 matrix cells
  that move from R-011's refusal to a stored value (`ts_struct_cast`, `ts_struct_cast_column`
  outside UTC, up to 160) are listed refusal→EQUAL moves, EQUAL meaning the stored value equals
  the matrix's Spark value cell for cell; any other value, and any EQUAL→other anywhere, is a
  halt. R-011's status line records the move and names A2 as the GATE-RETIRE-1 candidate.
- **ZS-5** — orchestrator ruling ZS-5, 2026-10-22, under the owner's grant: the native
  `repark.sql()` door is pinned as an unchanged control (head equals main on the card's
  tables); one line on CONTAINER-CAST-ZONE-1: "native-door zone semantics: owner decision,
  unscheduled".

## Red on the base (step 1, 2026-10-22)

`cargo test -p repark-functions --lib tests::zone_seam_pairs` on `b7a20167` with the pins of
`crates/repark-functions/src/tests/zone_seam_pairs.rs` added and nothing else:

```
test result: FAILED. 1 passed; 15 failed; 0 ignored; 0 measured; 948 filtered out
a_container_cast_of_an_instant_to_timestamp_ntz_takes_the_session_wall  Struct try=false America/New_York: (Some(Ntz), "1767323045123456") want 1767305045123456
a_container_cast_of_an_instant_to_string_renders_the_session_text      Struct try=false UTC: (Some(Text), "2026-01-02T03:04:05.123456Z") want 2026-01-02 03:04:05.123456
a_container_cast_of_an_instant_to_date_takes_the_session_date          Struct try=false America/New_York: (Some(Day), "20455") want 20454
a_container_cast_of_an_instant_to_bigint_answers_epoch_seconds         Struct try=false UTC: (Some(Seconds), "1767323045123456") want 1767323045
a_container_cast_of_a_wall_to_timestamp_reads_it_in_the_session_zone   Struct try=false America/New_York: (Some(Ltz), "1767323045123456") want 1767341045123456
a_container_cast_of_a_zoneless_string_to_timestamp_reads_it_in_the_session_zone  Struct try=false America/New_York: (Some(Ltz), "1767323045123456") want 1767341045123456
a_container_cast_of_a_date_to_timestamp_starts_at_the_session_midnight Struct try=false America/New_York: (Some(Ltz), "1767312000000000") want 1767330000000000
a_container_cast_of_a_wall_to_string_renders_spark_text                Struct try=false UTC: (Some(Text), "2026-01-02T03:04:05.123456") want 2026-01-02 03:04:05.123456
a_container_cast_of_a_wall_to_bigint_refuses_as_the_leaf_does          Struct try=false: (Some(Seconds), "1767323045123456")
a_container_cast_of_a_bigint_to_timestamp_ntz_refuses_as_the_leaf_does Struct try=false: (Some(Ntz), "1767323045")
a_leaf_try_cast_answers_what_the_leaf_cast_answers                     instant to string UTC: (Some(Text), "2026-01-02T03:04:05.123456Z") want 2026-01-02 03:04:05.123456
a_try_cast_answers_null_where_its_leaf_cannot_convert                  bigint overflow: (Some(Ltz), "9223372036854775807")
to_and_from_utc_timestamp_cast_a_wall_date_or_null_to_an_instant_first to_utc_timestamp of a wall America/New_York: (Some(Ntz), "1767303245123456") want 1767321245123456
to_json_renders_a_timestamp_ntz_leaf_as_its_wall_without_a_zone        America/New_York: (Some(Text), "{\"a\":\"2026-01-01T22:04:05.123-05:00\"}") want {"a":"2026-01-02T03:04:05.123"}
a_container_cast_of_an_instant_column_takes_the_session_wall           left: 1767323045123456 right: 1767305045123456
```

The one test green on the base is the control `an_instant_kept_as_an_instant_and_a_naive_member_stay_native`.
In `a_try_cast_answers_null_where_its_leaf_cannot_convert` only the `BIGINT` overflow is red on
the base; the unplaceable wall (row 5) and the malformed string (row 6) already answer NULL under
`TRY_CAST` on the base, and the pin holds that they still do.

## Corpus cells this unit closes

The replay of the base (sketch §1): 1,199 EQUAL / 411 DIFFERENT / 20 REFUSED / 10 ERROR. The
cells to close are sketch §8's lists, cell id `<expression>.<zone tag>` for every zone whose
letter there is not `E`: step 2, 63 container-cast expressions (219 cells), 8 chained casts
(22), 7 leaf `TRY_CAST` expressions (27), 3 wall function-site expressions (15) and, by ZS-2,
`bare.date.from_utc_timestamp`, `bare.date.to_utc_timestamp` (10 ERROR),
`bare.null.from_utc_timestamp`, `bare.null.to_utc_timestamp` (10 REFUSED); step 3, the six
`*.wall.to_json` expressions (30). 353 cells in all. Watched, not claimed:
`through.json_wall_ltz` (3 DIFFERENT) and `through.json_wall_ntz` (must stay EQUAL).

## Residue (ZS-1): every cell this unit does not close, by cause

105 cells, 21 expressions. Spark's answer is the recorded fixture; main's is the replay of
`b7a20167`. Reading the table: every residue expression is non-EQUAL in all five zones,
`UTC` and `Etc/UTC` included, where a session-zone conversion is the identity, so none is a
zone defect. None is a rendering of a timestamp as text: each is an error condition, a cast or
function argument Spark refuses, a pattern letter, or a parse. Two notes for a reader:
`bare.epoch.to_timestamp_ntz` is the one expression whose RePark answer varies by zone (it reads
the seconds as an instant and takes the session wall), but Spark raises `CAST_INVALID_INPUT` in
every zone, so the defect is that RePark answers at all; and `bare.{inst,wall}.date_format_micros`
is Spark's session text, which RePark refuses for the `S` letter only (its `date_format` without
`S` is EQUAL in every zone).

**The card's `from_json` row (`through.json_inst_ntz`).** The statement is
`from_json(to_json(named_struct('a', <instant>)), 'a TIMESTAMP_NTZ')`. Spark returns NULL in all
five zones. Main returns `1767323045123000` (the UTC wall to the millisecond) in all five zones.
The text `from_json` reads is the same on both engines in every zone, because `to_json` of an
instant is EQUAL everywhere (`struct.inst.to_json`, `{"a":"2026-01-02T03:04:05.123Z"}` in UTC,
`…-05:00` in New York). Spark's `TIMESTAMP_NTZ` parser rejects a text that carries a zone
designator, so the field is NULL; RePark's accepts it and keeps the UTC wall. The difference
holds in `UTC`, where no zone conversion takes place, and main's value is the same in every zone,
so it is not the session zone: it is whether `from_json` into `TIMESTAMP_NTZ` accepts a zoned
text. It stays a residue (follow-up line on the card).

| cell | cause | status on main | Spark 4.1.2 | main `b7a20167` |
|---|---|---|---|---|
| `array.date.ccast_bigint.utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `array.date.ccast_bigint.new_york` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `array.date.ccast_bigint.kolkata` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `array.date.ccast_bigint.p0530` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `array.date.ccast_bigint.etc_utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `array.str.ccast_bigint.utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `array.str.ccast_bigint.new_york` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `array.str.ccast_bigint.kolkata` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `array.str.ccast_bigint.p0530` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `array.str.ccast_bigint.etc_utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.col_str.cast_bigint.utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: Arrow error: Cast error: Cannot cast string '2026-01-02 …` |
| `bare.col_str.cast_bigint.new_york` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: Arrow error: Cast error: Cannot cast string '2026-01-02 …` |
| `bare.col_str.cast_bigint.kolkata` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: Arrow error: Cast error: Cannot cast string '2026-01-02 …` |
| `bare.col_str.cast_bigint.p0530` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: Arrow error: Cast error: Cannot cast string '2026-01-02 …` |
| `bare.col_str.cast_bigint.etc_utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: Arrow error: Cast error: Cannot cast string '2026-01-02 …` |
| `bare.col_str.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.col_str.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.col_str.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.col_str.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.col_str.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.col_wall.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.col_wall.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.col_wall.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.col_wall.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.col_wall.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.date.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.date.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.date.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.date.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.date.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.epoch.date_format.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Execution error: Function 'date_format' user-defined …` |
| `bare.epoch.date_format.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Execution error: Function 'date_format' user-defined …` |
| `bare.epoch.date_format.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Execution error: Function 'date_format' user-defined …` |
| `bare.epoch.date_format.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Execution error: Function 'date_format' user-defined …` |
| `bare.epoch.date_format.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Execution error: Function 'date_format' user-defined …` |
| `bare.epoch.from_utc_timestamp.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'from_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.from_utc_timestamp.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'from_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.from_utc_timestamp.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'from_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.from_utc_timestamp.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'from_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.from_utc_timestamp.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'from_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.to_date.utc` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | raises `OverflowError: days=1767323045; must have magnitude <= 999999999 …` |
| `bare.epoch.to_date.new_york` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | raises `OverflowError: days=1767323045; must have magnitude <= 999999999 …` |
| `bare.epoch.to_date.kolkata` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | raises `OverflowError: days=1767323045; must have magnitude <= 999999999 …` |
| `bare.epoch.to_date.p0530` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | raises `OverflowError: days=1767323045; must have magnitude <= 999999999 …` |
| `bare.epoch.to_date.etc_utc` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | raises `OverflowError: days=1767323045; must have magnitude <= 999999999 …` |
| `bare.epoch.to_timestamp_ntz.utc` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | `timestamp_ntz` `1767323045000000` |
| `bare.epoch.to_timestamp_ntz.new_york` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | `timestamp_ntz` `1767305045000000` |
| `bare.epoch.to_timestamp_ntz.kolkata` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | `timestamp_ntz` `1767342845000000` |
| `bare.epoch.to_timestamp_ntz.p0530` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | `timestamp_ntz` `1767342845000000` |
| `bare.epoch.to_timestamp_ntz.etc_utc` | function legality | DIFFERENT | raises CAST_INVALID_INPUT | `timestamp_ntz` `1767323045000000` |
| `bare.epoch.to_utc_timestamp.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'to_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.to_utc_timestamp.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'to_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.to_utc_timestamp.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'to_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.to_utc_timestamp.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'to_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.to_utc_timestamp.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'to_utc_timestamp' requires Timestamp, but …` |
| `bare.epoch.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.epoch.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.epoch.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.epoch.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.epoch.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.inst.date_format_micros.utc` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.inst.date_format_micros.new_york` | other: pattern letter S | REFUSED | `string` `2026-01-01 22:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.inst.date_format_micros.kolkata` | other: pattern letter S | REFUSED | `string` `2026-01-02 08:34:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.inst.date_format_micros.p0530` | other: pattern letter S | REFUSED | `string` `2026-01-02 08:34:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.inst.date_format_micros.etc_utc` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.str.cast_bigint.utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.str.cast_bigint.new_york` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.str.cast_bigint.kolkata` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.str.cast_bigint.p0530` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.str.cast_bigint.etc_utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `bare.str.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.str.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.str.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.str.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.str.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | raises `AnalysisException: Error during planning: Function 'unix_micros' requires Timestamp, but …` |
| `bare.wall.date_format_micros.utc` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.wall.date_format_micros.new_york` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.wall.date_format_micros.kolkata` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.wall.date_format_micros.p0530` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.wall.date_format_micros.etc_utc` | other: pattern letter S | REFUSED | `string` `2026-01-02 03:04:05.123456` | raises `PySparkException: Execution error: date_format: unsupported pattern letter 'S' …` |
| `bare.wall.unix_micros.utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.wall.unix_micros.new_york` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.wall.unix_micros.kolkata` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.wall.unix_micros.p0530` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `bare.wall.unix_micros.etc_utc` | function legality | DIFFERENT | raises DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE | `bigint` `1767323045123456` |
| `map.date.ccast_bigint.utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | raises DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| `map.date.ccast_bigint.new_york` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | raises DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| `map.date.ccast_bigint.kolkata` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | raises DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| `map.date.ccast_bigint.p0530` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | raises DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| `map.date.ccast_bigint.etc_utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | raises DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION |
| `struct.date.ccast_bigint.utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `struct.date.ccast_bigint.new_york` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `struct.date.ccast_bigint.kolkata` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `struct.date.ccast_bigint.p0530` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `struct.date.ccast_bigint.etc_utc` | cast legality | DIFFERENT | raises DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION | `bigint` `20455` |
| `struct.str.ccast_bigint.utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `struct.str.ccast_bigint.new_york` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `struct.str.ccast_bigint.kolkata` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `struct.str.ccast_bigint.p0530` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `struct.str.ccast_bigint.etc_utc` | error condition | DIFFERENT | raises CAST_INVALID_INPUT | raises `PySparkException: datafusion engine error: Optimizer rule 'simplify_expressions' failed caused …` |
| `through.json_inst_ntz.utc` | from_json | DIFFERENT | `timestamp_ntz` `None` | `timestamp_ntz` `1767323045123000` |
| `through.json_inst_ntz.new_york` | from_json | DIFFERENT | `timestamp_ntz` `None` | `timestamp_ntz` `1767323045123000` |
| `through.json_inst_ntz.kolkata` | from_json | DIFFERENT | `timestamp_ntz` `None` | `timestamp_ntz` `1767323045123000` |
| `through.json_inst_ntz.p0530` | from_json | DIFFERENT | `timestamp_ntz` `None` | `timestamp_ntz` `1767323045123000` |
| `through.json_inst_ntz.etc_utc` | from_json | DIFFERENT | `timestamp_ntz` `None` | `timestamp_ntz` `1767323045123000` |

## Halt after step 3 and ruling ZS-6 (2026-10-22)

**The halt.** The corpus replay of the head after step 3 (`6286e40e`) moved three EQUAL cells to
DIFFERENT: `through.json_wall_ntz` in New York, Kolkata and +05:30. Spark answers
`1767323045123000` in every zone; the head answered `1767341045123000` (New York) and
`1767303245123000` (Kolkata, +05:30). `from_json` into a `TIMESTAMP_NTZ` field read a text with
no zone in the session zone and kept the UTC wall of that instant; on main the cell was EQUAL
because `to_json` wrote the wall shifted into the session zone with its offset and `from_json`
turned that text back into the wall. Step 3 fixed the first defect and exposed the second.

- **ZS-6** — orchestrator ruling ZS-6, 2026-10-22, under the owner's grant (the owner may
  reverse it): this unit takes the `from_json` `TIMESTAMP_NTZ` site in `json/decode.rs`, in its
  own commit "step 3b, from_json reads a TIMESTAMP_NTZ field as a wall", so a revert of that one
  commit restores today's `from_json`. Red first on main; the change is the zone handling only;
  every other spelling keeps today's grammar and answer; the store path and the leaf cast are
  untouched; after step 3b the corpus shows 0 EQUAL→other against main, and a second halt parks
  the unit.

**Step 3b, red on main.** `from_json` is unchanged from main at `6286e40e`, so the pins ran there:

```
test result: FAILED. 1 passed; 1 failed; 0 ignored; 0 measured; 964 filtered out
from_json_reads_a_timestamp_ntz_field_as_its_wall_and_a_zoned_text_as_null
{"a":"2026-01-02T03:04:05.123"} America/New_York: (Some(Ntz), "1767341045123000") want 1767323045123000
{"a":"2026-01-02T03:04:05.123"} Asia/Kolkata: (Some(Ntz), "1767303245123000") want 1767323045123000
{"a":"2026-01-02T03:04:05.123Z"} America/New_York: (Some(Ntz), "1767323045123000") want NULL
{"a":"2026-01-02T03:04:05.123Z"} Asia/Kolkata: (Some(Ntz), "1767323045123000") want NULL
{"a":"2026-01-01T22:04:05.123-05:00"} America/New_York: (Some(Ntz), "1767323045123000") want NULL
{"a":"2026-01-01T22:04:05.123-05:00"} Asia/Kolkata: (Some(Ntz), "1767323045123000") want NULL
from_json_keeps_main_answer_for_a_space_separator_or_a_date_only_text ... ok   (the unchanged controls)
```

**Step 3b, the change.** In `build_timestamp`, a field whose type is `Timestamp(_, None)` reads a
text through `wall_micros`: a text that the two zoned patterns parse (`Z` or an offset, `T` or a
space) answers NULL; a text of the pattern `%Y-%m-%dT%H:%M:%S%.f` (Spark's `TIMESTAMP_NTZ` JSON
form, any fraction width) is its wall with no zone; every other spelling (a space separator,
minutes only, a date only) takes today's `timestamp_micros` path and keeps today's answer, which
reads it in the session zone (the two controls pin a space separator and a date only in New York
at main's values). Those spellings are not held by the corpus and stay as they are. A zoned
field (`TIMESTAMP`) is unchanged. **Untouched:** the `TIMESTAMP_NTZ` store path
(`crates/repark-iceberg/src/write/ntz_store.rs`, `update_cast.rs`) and the leaf
`CAST(<text> AS TIMESTAMP_NTZ)` (`timestamp_ntz_cast.rs`).

**Residue move (appended; the table above is as recorded at step 1).** `through.json_inst_ntz`
leaves the residue: its five cells close by step 3b (Spark NULL; the head NULL). The residue is
now 20 expressions, 90 DIFFERENT and 10 REFUSED cells. The three `through.json_wall_ntz` cells
return to EQUAL by step 3b's own reading of the wall.

**Why `SparkUtcShift` is a second registration, not a second seam.** The order's §2 names the
`to_utc_timestamp` / `from_utc_timestamp` site. For a wall and an untyped NULL an analyzer
rewrite of the argument would have been enough, but a `DATE` argument is rejected while the
plan is built, before any analyzer rule runs: building the projection asks the expression for its
field, which calls `verify_function_arguments` → `fields_with_udf`
(`datafusion-expr` 54.1.0, `src/expr_schema.rs` line 724, raising from
`src/type_coercion/functions.rs` line 868, "Function 'to_utc_timestamp' requires Timestamp, but
received Date"), because DataFusion-Spark's signature is `Coercion::new_implicit(Timestamp,
[String])`. So `SparkUtcShift` wraps the same two UDFs with a user-defined signature that admits
a wall, a date or a NULL first argument and delegates every other type to the inner signature's
own coercion; it converts that argument with the same `to_timestamp_udf` kernel the seam's row 5
and row 7 use and then calls the inner function. It holds no conversion of its own.

## Second halt, park and ruling ZS-7 (2026-10-22)

**The second halt.** The facade suite on the head (`dc0da5a2`) moved three EQUAL cells of the
ZONE-HORIZON-RENDER-1 oracle to DIFFERENT: `to_utc_timestamp` of a wall at the New York overlap
`2099-11-01 01:30` and the gaps `2100-03-14 02:30` and `2104-03-09 02:30`. The unit parked. The
measurement while parked (18 edge walls × 2 session zones × 5 expressions on Spark, head and
main; the table is in the card ZONE-HORIZON-INSTANT-1) showed: the leaf cast equals Spark at every edge;
Spark's wall argument is the leaf cast composed with the instant function (36 of 36); the seam's
`SparkUtcShift` implements exactly that composition; every remaining edge miss is the shared
DataFusion-Spark instant kernel, the same on main and head; main's three cells matched Spark by
value only, as `timestamp_ntz`.

- **ZS-7** — owner ruling ZS-7, 2026-10-22 (option C): the unit does not grow to the instant
  kernel; no step 2c. The three cells are accidental EQUAL on main, wrong type; honest DIFFERENT
  after the seam; they close with ZONE-HORIZON-INSTANT-1, carded
  (`../../roadmap/mid-term/zone-horizon-instant-1-card-2026-10-22.md`), the next Opus unit after
  GATE-RETIRE-1.

**What ZS-7 does to the pins.** The suite's grammar is a residue list: each key in
`zone_horizon_render_1_residue.json` must differ from the oracle's Spark value, and every other
key must equal it. So the three cells are expected-divergence rows whose recorded expectation is
Spark's value (`2099-11-01 05:30:00`, `2100-03-14 07:30:00`, `2104-03-09 07:30:00`), not
red-by-design tests. The list was re-derived on the head with the recorder's `--engine repark
--residue`: 15 cells leave (the wall edge cells the seam now answers as Spark does), 3 join.
`test_zone_horizon_render_1.py` passes (12 passed, the live cell skipped). The 3 joining cells are
explained by the suite's named class `_UPSTREAM_SHIFT`.

## Step 4, the doors (2026-10-22, after merging main `0512470d`, no conflict)

`python/repark/tests/timestamps/test_zone_seam_1.py` (38 tests, green on the head build):

- **Spark SQL door:** the card's plain `SELECT` rows (array, struct, map; leaf control) answer the
  session wall in New York (`1767305045123456`) and the UTC wall in UTC. The "through another type"
  rows store Spark's value in New York: the session wall through `STRING` (`INSERT`, an array, the
  leaf cast over a string the container made, a `MERGE` update), the New York midnight through
  `DATE` (`1767225600000000`), NULL through `from_json(to_json(…))`.
- **Through `BIGINT`:** the `SELECT` raises Spark's `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`.
  The `INSERT` raises it in UTC; outside UTC R-011's gate plans the source first, the plan raises,
  and the gate answers with its own text (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, "the
  router cannot plan it"). No row is written. Main stored the UTC wall there
  (`1767323045123456`, measured 2026-10-22), where Spark refuses: a DIFFERENT cell that now
  refuses, with R-011's condition instead of Spark's outside UTC.
- **Six table-creating writers and a cached frame:** CTAS, `CREATE OR REPLACE TABLE … AS SELECT`,
  `REPLACE TABLE … AS SELECT`, `saveAsTable` overwrite, `writeTo().create()`, `.replace()`,
  `.createOrReplace()`, and a cached frame with `writeTo().append()` store the session wall in New
  York and the UTC wall in UTC.
- **DataFrame door:** `Column.cast(MapType(StringType(), TimestampNTZType()))` and `expr` casts of
  a struct and an array answer the session wall. `Column.cast` to a `StructType` or `ArrayType`
  raises `unknown cast type` on main and on the head alike (measured 2026-10-22): a facade grammar
  gap outside this unit, pinned as unchanged.
- **Native `repark.sql()` door (ZS-5):** measured on main and on the head with the same script: the
  struct, `make_array` and leaf casts of `TIMESTAMP '…+00:00'` answer `1767323045123456` (the
  native door reads that literal as a naive wall) and the struct cast to a `STRING` leaf answers
  `2026-01-02T03:04:05.123456`, identical on both builds; pinned as unchanged controls.
- **U-8 frame pins (ZS-4):** `test_ice_ntz_nested_refuse_1.py`'s frame holding a container cast of
  an instant now stores the New York wall on the seven writers (was the refusal).
- **R-NTZ-S2-9 retired:** `w_ref_from` in `ntz_9_verify_spark_oracle.json` loses its
  `divergence` and `repark_error_contains` annotations; `from_utc_timestamp` of a string returns
  `TIMESTAMP` through `SparkUtcShift`, so the `UPDATE` refusal names the source `"TIMESTAMP"` as
  Spark does (the declared divergence of `task/ledgers/staging/ntz-1-ledger.md` row R-NTZ-S2-9 is
  closed by this unit; that ledger is another unit's and is not edited).
- **Controls unchanged:** the leaf casts, a UTC session, a wall source (Rust `ntz_nested_refuse*`
  pins), and the `timestamp_ns` refusal of R-015 (`timestamp_ns_nested_shapes.rs`, green).

## Step 5, mutants and the revert check (2026-10-22)

Each mutant applied alone to the head, `cargo test -p repark-functions --lib
tests::zone_seam_pairs`, then reverted. M01–M07 remove one `seam_leaf` arm (sketch §4 rows 1, 2
and 8, 3, 4, 5–7, 9, 10); M08 the bare `TRY_CAST` rewrite; M09 the map-cast wrap; M10
`SparkUtcShift`'s argument shift; M11 the parent-null mask; M12 step 3b's `wall_micros`; M13 step
3's `wall_text` branch; M14 the `rewrite_cast` call of the seam.

| mutant | pins red | which |
|---|---|---|
| M01-row1-wall | 2 failed | `a_container_cast_of_an_instant_column_takes_the_session_wall`, `a_container_cast_of_an_instant_to_timestamp_ntz_takes_the_session_wall` |
| M02-row2-8-text | 3 failed | `a_container_cast_of_a_wall_to_string_renders_spark_text`, `a_container_cast_of_an_instant_to_string_renders_the_session_text`, `a_leaf_try_cast_answers_what_the_leaf_cast_answers` |
| M03-row3-day | 2 failed | `a_container_cast_of_an_instant_to_date_takes_the_session_date`, `a_leaf_try_cast_answers_what_the_leaf_cast_answers` |
| M04-row4-seconds | 2 failed | `a_container_cast_of_an_instant_to_bigint_answers_epoch_seconds`, `a_leaf_try_cast_answers_what_the_leaf_cast_answers` |
| M05-row5-7-instant | 5 failed | `a_container_cast_of_a_date_to_timestamp_starts_at_the_session_midnight`, `a_container_cast_of_a_wall_to_timestamp_reads_it_in_the_session_zone`, `a_container_cast_of_a_zoneless_string_to_timestamp_reads_it_in_the_session_zone`, `a_leaf_try_cast_answers_what_the_leaf_cast_answers`, `to_and_from_utc_timestamp_cast_a_wall_date_or_null_to_an_instant_first` |
| M06-row9-refused-seconds | 1 failed | `a_container_cast_of_a_wall_to_bigint_refuses_as_the_leaf_does` |
| M07-row10-refused-wall | 1 failed | `a_container_cast_of_a_bigint_to_timestamp_ntz_refuses_as_the_leaf_does` |
| M08-bare-try | 2 failed | `a_leaf_try_cast_answers_what_the_leaf_cast_answers`, `a_try_cast_answers_null_where_its_leaf_cannot_convert` |
| M09-map-cast | 10 failed | `a_container_cast_of_a_bigint_to_timestamp_ntz_refuses_as_the_leaf_does`, `a_container_cast_of_a_date_to_timestamp_starts_at_the_session_midnight`, `a_container_cast_of_a_wall_to_bigint_refuses_as_the_leaf_does`, `a_container_cast_of_a_wall_to_string_renders_spark_text`, `a_container_cast_of_a_wall_to_timestamp_reads_it_in_the_session_zone`, `a_container_cast_of_a_zoneless_string_to_timestamp_reads_it_in_the_session_zone`, `a_container_cast_of_an_instant_to_bigint_answers_epoch_seconds`, `a_container_cast_of_an_instant_to_date_takes_the_session_date`, `a_container_cast_of_an_instant_to_string_renders_the_session_text`, `a_container_cast_of_an_instant_to_timestamp_ntz_takes_the_session_wall` |
| M10-utc-shift | 1 failed | `to_and_from_utc_timestamp_cast_a_wall_date_or_null_to_an_instant_first` |
| M11-masked | 0 failed | none (survived) |
| M12-from-json | 1 failed | `from_json_reads_a_timestamp_ntz_field_as_its_wall_and_a_zoned_text_as_null` |
| M13-to-json | 1 failed | `to_json_renders_a_timestamp_ntz_leaf_as_its_wall_without_a_zone` |
| M14-seam-call | 13 failed | `a_container_cast_of_a_bigint_to_timestamp_ntz_refuses_as_the_leaf_does`, `a_container_cast_of_a_date_to_timestamp_starts_at_the_session_midnight`, `a_container_cast_of_a_wall_to_bigint_refuses_as_the_leaf_does`, `a_container_cast_of_a_wall_to_string_renders_spark_text`, `a_container_cast_of_a_wall_to_timestamp_reads_it_in_the_session_zone`, `a_container_cast_of_a_zoneless_string_to_timestamp_reads_it_in_the_session_zone`, `a_container_cast_of_an_instant_column_takes_the_session_wall`, `a_container_cast_of_an_instant_to_bigint_answers_epoch_seconds`, `a_container_cast_of_an_instant_to_date_takes_the_session_date`, `a_container_cast_of_an_instant_to_string_renders_the_session_text`, `a_container_cast_of_an_instant_to_timestamp_ntz_takes_the_session_wall`, `a_leaf_try_cast_answers_what_the_leaf_cast_answers`, `a_try_cast_answers_null_where_its_leaf_cannot_convert` |

M11 survived the first run; the pin `a_value_under_a_null_struct_never_reaches_an_ansi_leaf` (a
NULL struct row whose child is a malformed string, cast under ANSI) was added and fails under M11
(`collect` raises) and passes on the head. **Revert check:** with M14 applied the ci-profile build
replays the ZONE-ORACLE-1 corpus at 1,272 EQUAL / 358 DIFFERENT / 10 REFUSED (the head:
1,540 / 90 / 10), so removing the seam reds the corpus.

## Third halt, ZS-8 and ZS-9: the nested NTZ→TIMESTAMP store (2026-10-22)

**The halt.** Step 6's U-8 door matrix, checked against live Spark 4.1.2 (`probe.py --engine
spark --schemas ltz_nested`, New York and Kolkata, 706 cells), found 74 cells EQUAL on main and
DIFFERENT on the head: a container cast of an instant to `STRUCT<v: TIMESTAMP_NTZ, …>` written
into a `STRUCT<v: TIMESTAMP, …>` column outside UTC, on 19 doors (literal source) and 18 (column
source) per zone. Spark stores `1767323045123456`, main stored the same by two cancelling
defects, the head stored `1767305045123456`: the seam made the cast right and the write path's
own nested NTZ→TIMESTAMP conversion still reads the wall as UTC.

- **ZS-8** — owner ruling ZS-8, 2026-10-22: fix the store conversion inside this unit (step 3c),
  §5 halt 3 lifted for that one site, with condition 3: stop with the shape if the site is more
  than one conversion and its pair. **Condition 3 fired.** An instrumented build located three
  sites: A, `write/conform.rs` `entry_store_cast` at the sink with no session zone in scope
  (`INSERT … BY NAME`, `INSERT OVERWRITE` from a query and from `VALUES`, the DataFrame
  `insertInto(overwrite=True)` and `overwritePartitions`); B, `write/update_cast.rs`
  `nested_leaf_cast_sql`'s `arrow_cast` text (MERGE and plain `UPDATE` assignments of the whole
  struct, MERGE `UPDATE SET *`); C, MERGE `INSERT` (`merge/insert.rs` →
  `store_overflow::wrap_store_outputs`). `repark-iceberg` depends only on `repark-common`, so it
  cannot call the seam's helper.
- **ZS-9** — orchestrator ruling ZS-9, 2026-10-22, under the owner's standing word (reversible by
  the owner): the unit lands with a write-time refusal for exactly the moved shape; the three
  sites become WRITE-NTZ-TO-LTZ-NESTED-1 (carded,
  `../../roadmap/mid-term/write-ntz-to-ltz-nested-1-card-2026-10-22.md`). Step 3c is the refusal.

**Step 3c, red on the head without the refusal.** `crates/repark-spark/tests/ntz_into_ltz_nested.rs`
with the refusal stashed:

```
test result: FAILED. 2 passed; 1 failed
a_nested_wall_into_a_nested_timestamp_refuses_outside_utc_on_every_store_door
  "SQL INSERT BY NAME / a container cast of an instant: Ok(())", "… : a value was written",
  (the same for a TIMESTAMP_NTZ literal, and for INSERT OVERWRITE, INSERT OVERWRITE VALUES,
  MERGE INSERT, MERGE UPDATE, MERGE UPDATE SET * and UPDATE: 14 cases, each Ok(()) and written)
a_utc_session_stores_the_wall_read_in_utc_on_every_store_door ... ok   (the UTC control)
doors_that_read_the_wall_in_the_session_zone_store_spark_instant_outside_utc ... ok
```

With the refusal: 3 passed. The refusal sits in the router's U-8 gate only (`refuse_at` →
`refuse_naive_into_instant`), decided per door family: no door reaches sites A, B or C without
the gate, so no predicate is added at a store site. **Top-level** NTZ→TIMESTAMP is not in scope:
a top-level `TIMESTAMP` target is converted by `CAST(… AS TIMESTAMP)` text
(`ntz_store::ltz_instant_cast_sql`), not by the three sites (by reading; not measured).

**The matrix after step 3c** (`probe.py` on the head build, 3,168 cells, against main and the
Spark records): every one of the 74 cells refuses outside UTC (38 `ts_struct_cast`, 36
`ts_struct_cast_column`); the `ntz_literal` → `ltz_nested` cells that stored a wrong value on the
head refuse, 21 per zone (42), and 4 per zone that were engine errors now refuse too; the other
NTZ sources into `ltz_nested` on the same doors refuse (`ntz_cast` 42, `ntz_text_cast` 42,
`ntz_cast_column` 42, and 18 more struct-cast cells that were DIFFERENT on main), all DIFFERENT on
main and head before. Nothing refuses in UTC; no cell that was EQUAL on the head refuses; no other
cell changes against the first head run; no cell that matches Spark on main moves. **Over-refusal
by design, ruled 2026-10-22 (ZS-9), until WRITE-NTZ-TO-LTZ-NESTED-1:** 134 refused cells in New
York and 134 in Kolkata (`ltz_nested`, the store doors), of which 37 per zone stored Spark's
instant on main.

## Step 5 addendum (step 3c's mutants, 2026-10-22)

`cargo test -p repark-spark --test ntz_into_ltz_nested`, each mutant alone then reverted:

| mutant | pins red | which |
|---|---|---|
| M15 the refusal removed (`if false && micros && store`) | 1 failed | `a_nested_wall_into_a_nested_timestamp_refuses_outside_utc_on_every_store_door` |
| M16 the zone check removed (`if store`, refuses in UTC too) | 1 failed | `a_utc_session_stores_the_wall_read_in_utc_on_every_store_door` |
| M17 every `INSERT` a store door (`let store = true`) | 1 failed | `doors_that_read_the_wall_in_the_session_zone_store_spark_instant_outside_utc` |

## Step 6, never worse (2026-10-22, the final code head `a6780177`)

- **ZONE-ORACLE-1** on the head build: 1,540 EQUAL / 90 DIFFERENT / 10 REFUSED / 0 ERROR; against
  the base replay 321 DIFFERENT→EQUAL, 10 ERROR→EQUAL, 10 REFUSED→EQUAL, 0 EQUAL→other; equal to
  the committed difference list (0 left, 0 joined, 0 changed).
- **U-8 door matrix** (`probe.py run --engine repark`, 3168 cells, main `b7a20167` against the
  head): on `ntz_nested`, 50 cells per zone move from the refusal to the stored session wall, equal
  to live Spark 4.1.2 cell for cell (`--engine spark --sources ts_struct_cast,ts_struct_cast_column`,
  176 cells), and 6 per zone move from the refusal to the engine's own error (2 a scalar subquery
  in `VALUES`, which Spark refuses with another condition; 4 a plain `UPDATE` with no `WHERE`,
  which Spark stores and which errors for every source on main); on `ltz_nested`, the 134 per zone
  of step 3c refuse, and the 54 NTZ-source cells the seam made right on the plan-converting doors
  stay right. No cell that matches Spark on main moves to anything else (Spark measured for every
  `ltz_nested` cell and the moved `ntz_nested` cells in New York and Kolkata); UTC cells do not
  move.
- **Cost** (`r1`-style script: the six U-8 statements and a container-cast `SELECT`, each process
  one session with 3 warm-ups and the median of 10 runs; main `b7a20167` and the head alternating,
  6 rounds per zone; the table gives the median of the six; load 2.8–4.9):

| statement | UTC main | UTC head | ratio | New York main | New York head | ratio |
|---|---:|---:|---:|---:|---:|---:|
| plain INSERT | 25.52 ms | 26.19 ms | 1.026 | 24.68 ms | 25.01 ms | 1.013 |
| plain UPDATE | 54.10 ms | 55.17 ms | 1.020 | 50.56 ms | 52.96 ms | 1.048 |
| plain MERGE | 56.92 ms | 57.62 ms | 1.012 | 56.82 ms | 57.39 ms | 1.010 |
| nested-NTZ INSERT | 30.59 ms | 30.56 ms | 0.999 | 28.37 ms | 29.72 ms | 1.047 |
| nested-NTZ UPDATE | 55.69 ms | 57.18 ms | 1.027 | 57.87 ms | 58.27 ms | 1.007 |
| nested-NTZ MERGE | 65.31 ms | 66.12 ms | 1.012 | 66.98 ms | 67.20 ms | 1.003 |
| container-cast SELECT | 3.30 ms | 3.43 ms | 1.039 | 3.32 ms | 3.46 ms | 1.039 |

  Every ratio is within the 1.05x gate.
- **Gates on the head:** `cargo test -p repark-functions` 965 passed (1 ignored, pre-existing);
  `cargo test -p repark-spark` every binary green, `ntz_into_ltz_nested` 3 passed;
  `cargo test -p repark-iceberg --lib nested_ns_gate` 10 passed; the facade suite (`-n 8 --dist
  loadgroup`) 20,326 passed, 485 skipped, 151 xfailed, 10 failed, all ten the `io/` live
  PySpark-worker oracles (`ModuleNotFoundError: No module named 'pandas'` in the worker), which
  pass alone on the head (22 passed) and on main (measured 2026-10-22); `make ci` and the
  comment-gate grep before every push; `make check-comment-density` is not a target on this base.

## Step 7, records (2026-10-22)

The card CONTAINER-CAST-ZONE-1's status and its follow-up lines (TIMESTAMP-LEGALITY-PARITY-1,
DATE-FORMAT-FRACTION-1, native-door zone semantics); the ZONE-ORACLE-1 order's follow-up line
(ZS-3); R-011's and R-015's dated status lines; the v1.5.6 notes lines; sketch §10b; the cards
ZONE-HORIZON-INSTANT-1 and WRITE-NTZ-TO-LTZ-NESTED-1; the `map.md` rows of every touched
directory. Deviation from the sketch: the seam's tests live in `src/tests/zone_seam_pairs.rs`, not
under a new `zone_seam/` directory, and the seam file is about 680 lines.

## PROPOSITION LEDGER — ZONE-SEAM-1 — 2026-10-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A container `CAST` or `TRY_CAST` (struct, every list layout, map value) of a seam pair (sketch §4 rows 1–8) answers, at each seam leaf, what the bare leaf `CAST` answers in the session zone, on `CAST` and `TRY_CAST`, every other member keeping its native cast. | `zone_seam_pairs.rs` rows 1–8 in three zones, struct / array / map, `CAST` and `TRY_CAST`; the mixed-member control; the column source. | **PROVEN** | `zone_seam_pairs.rs` rows 1–8, the column source, the mixed-member control, the null-mask pin; step 5 M01–M05, M09, M11, M14. |
| C-002 | A container cast of a wall to `BIGINT` and of a `BIGINT` to `TIMESTAMP_NTZ` refuses with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`, as the leaf and Spark do. | rows 9–10 pins. | **PROVEN** | rows 9–10 pins; M06, M07. |
| C-003 | A bare `TRY_CAST` of an instant to `STRING`, `DATE` or `BIGINT`, of a wall to `TIMESTAMP` or `STRING`, of a date or a `BIGINT` to `TIMESTAMP` answers what the bare `CAST` answers; `TRY_CAST` answers NULL where its leaf cannot convert (an unplaceable wall, a malformed string, a `BIGINT` overflow). | the leaf `TRY_CAST` pin and the NULL pin. | **PROVEN** | the leaf `TRY_CAST` and NULL pins; M08. |
| C-004 | `to_utc_timestamp` and `from_utc_timestamp` of a wall, a date or an untyped NULL cast the argument to `TIMESTAMP` in the session zone first and return `TIMESTAMP` (ZS-2). | the function-site pin. | **PROVEN** | the function-site pin; M10; `w_ref_from` (R-NTZ-S2-9) retired. |
| C-005 | `to_json` renders a `TIMESTAMP_NTZ` leaf as its wall with no zone, `yyyy-MM-ddTHH:mm:ss.SSS`, in every zone; an instant leaf is unchanged. | the `to_json` pin. | **PROVEN** | the `to_json` pin; M13. |
| C-006 | The ZONE-ORACLE-1 corpus on the head: every cell of § Corpus cells EQUAL, the non-EQUAL set exactly the residue above (plus the watched cell if it stays), no EQUAL cell of the base moves. | replay on main and head; `zone_oracle_1_main_difference.json` re-recorded on the head. | **PROVEN** | Step 6: 1,540 / 90 / 10 / 0, 0 EQUAL→other; the residue table; M14 reds the corpus. |
| C-007 | The card's tables on the Spark SQL door and the DataFrame door answer Spark's values; the native `repark.sql()` door answers on the head as on main (ZS-5). | step 4 pins. | **PROVEN** | `test_zone_seam_1.py`; the native door measured on main and the head. |
| C-008 | The six table-creating writers and a cached frame store the session wall for a container cast of an instant outside UTC. | step 4 pins. | **PROVEN** | `test_zone_seam_1.py` writers in New York and UTC. |
| C-009 | The U-8 door matrix on main and head, and the U-8 Rust pins that held a container-cast refusal (rewritten to the stored session wall, ZS-4; the two engine-error statements fail as their wall twin does): every changed cell is a listed refusal→EQUAL move of ZS-4 or DIFFERENT→EQUAL; no EQUAL cell moves. | `probe.py` on both builds. | **PROVEN** | Step 6 matrix; the rewritten U-8 Rust and Python pins. |
| C-010 | Each seam arm removed alone reds a pin; the seam call removed reds the corpus. | mutants. | **PROVEN** | Step 5 and its addendum: 17 mutants, each reds a pin; M14 reds the corpus. |
| C-011 | Cost: the U-8 verifier's six statements, 10-run medians, head ≤ 1.05x main, load under 5. | timing table. | **PROVEN** | Step 6 cost table, every ratio within 1.05x. |
| C-013 | `from_json` reads a `TIMESTAMP_NTZ` field: a text with no zone in Spark's form is its wall in every session zone; a text with `Z` or an offset is NULL; a space separator and a date-only text keep main's answer (ZS-6). | the two `from_json` pins, red on main. | **PROVEN** | the two `from_json` pins, red on main; M12. |
| C-014 | The ZONE-HORIZON-RENDER-1 residue on the head: 15 wall edge cells leave, 3 New York `to_utc_timestamp_wall` cells join as expected-divergence rows (ZS-7), every other cell unchanged. | `--engine repark --residue` on the head; the suite green. | **PROVEN** | the residue re-derived; the suite green. |
| C-015 | Outside UTC, a nested `TIMESTAMP_NTZ` value into a nested `TIMESTAMP` field refuses on the store doors (sites A, B, C) with the whole text and no write; UTC stores the UTC read; the plan-converting doors store the session read (ZS-9). | `ntz_into_ltz_nested.rs` red without the refusal; the DataFrame pins; the matrix. | **PROVEN** | `ntz_into_ltz_nested.rs` red without the refusal; the DataFrame pins; the matrix; M15–M17. |
| C-012 | Records: the card (follow-up lines of ZS-1 and ZS-5), R-011 and R-015 status lines, the v1.5.6 notes line, the ZONE-ORACLE-1 order follow-up (ZS-3), `map.md`s. | the diff. | **PROVEN** | Step 7. |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: zone-seam-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The order's pairs, sites and card tables were walked against Spark 4.1.2's recorded ZONE-ORACLE-1 corpus, live Spark runs of the U-8 door matrix and the ZONE-HORIZON-RENDER-1 edges; every assigned corpus cell is EQUAL and the residue is listed by cause.
      artifacts: [crates/repark-functions/src/tests/zone_seam_pairs.rs, python/repark/tests/timestamps/test_zone_seam_1.py, python/repark/tests/timestamps/zone_oracle_1_main_difference.json]
    - id: AT-2
      status: ATTACKED
      evidence: Struct, list and map shapes, a column source, NULL parents with a malformed child, an unplaceable wall, a malformed string and a BIGINT overflow under TRY_CAST, gap and overlap walls at the UTC-function site, five session zones.
      artifacts: [crates/repark-functions/src/tests/zone_seam_pairs.rs, task/roadmap/mid-term/zone-horizon-instant-1-card-2026-10-22.md]
    - id: AT-3
      status: ATTACKED
      evidence: Depth-0 CAST, the inner level, natively EQUAL pairs and zoned to_json are untouched; the corpus shows 0 EQUAL→other; a second analysis pass is pinned.
      artifacts: [crates/repark-functions/src/zone_seam.rs, crates/repark-functions/src/tests/zone_seam_pairs.rs]
    - id: AT-4
      status: N/A
      justification: Stateless analyzer rewrite and scalar kernels; no shared or mutable state.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the refusal names no data-deleting action.
    - id: AT-6
      status: ATTACKED
      evidence: Every refusal pin asserts no value is written; the U-8 matrix compared stored values with main and live Spark on every door.
      artifacts: [crates/repark-spark/tests/ntz_into_ltz_nested.rs, crates/repark-spark/tests/ntz_nested_refuse_casts.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Seven statements in two zones, main and head alternating, every 10-run median ratio within 1.05x.
      artifacts: [task/ledgers/staging/zone-seam-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge; repark-iceberg still depends only on repark-common; the to_utc_timestamp wrapper delegates to DataFusion-Spark's own coercion and kernel; no ceiling rises.
      artifacts: [crates/repark-functions/src/zone_seam.rs, crates/repark-iceberg/src/write/nested_ns_gate.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The refusals carry Spark's conditions (CAST_WITHOUT_SUGGESTION, CANNOT_SAFELY_CAST) and the new hint is pinned byte for byte per door family.
      artifacts: [crates/repark-spark/tests/ntz_into_ltz_nested.rs, python/repark/tests/timestamps/test_zone_seam_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Seventeen mutants each red a pin; the seam call removed reds the corpus; M11 was a surviving mutant until its pin was added.
      artifacts: [crates/repark-functions/src/tests/zone_seam_pairs.rs, crates/repark-spark/tests/ntz_into_ltz_nested.rs]
  complete: true
```

Up: [map.md](map.md).
