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

## PROPOSITION LEDGER — ZONE-SEAM-1 — 2026-10-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A container `CAST` or `TRY_CAST` (struct, every list layout, map value) of a seam pair (sketch §4 rows 1–8) answers, at each seam leaf, what the bare leaf `CAST` answers in the session zone, on `CAST` and `TRY_CAST`, every other member keeping its native cast. | `zone_seam_pairs.rs` rows 1–8 in three zones, struct / array / map, `CAST` and `TRY_CAST`; the mixed-member control; the column source. | **OPEN** | Red on the base (above). |
| C-002 | A container cast of a wall to `BIGINT` and of a `BIGINT` to `TIMESTAMP_NTZ` refuses with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`, as the leaf and Spark do. | rows 9–10 pins. | **OPEN** | Red on the base. |
| C-003 | A bare `TRY_CAST` of an instant to `STRING`, `DATE` or `BIGINT`, of a wall to `TIMESTAMP` or `STRING`, of a date or a `BIGINT` to `TIMESTAMP` answers what the bare `CAST` answers; `TRY_CAST` answers NULL where its leaf cannot convert (an unplaceable wall, a malformed string, a `BIGINT` overflow). | the leaf `TRY_CAST` pin and the NULL pin. | **OPEN** | Red on the base. |
| C-004 | `to_utc_timestamp` and `from_utc_timestamp` of a wall, a date or an untyped NULL cast the argument to `TIMESTAMP` in the session zone first and return `TIMESTAMP` (ZS-2). | the function-site pin. | **OPEN** | Red on the base. |
| C-005 | `to_json` renders a `TIMESTAMP_NTZ` leaf as its wall with no zone, `yyyy-MM-ddTHH:mm:ss.SSS`, in every zone; an instant leaf is unchanged. | the `to_json` pin. | **OPEN** | Red on the base. |
| C-006 | The ZONE-ORACLE-1 corpus on the head: every cell of § Corpus cells EQUAL, the non-EQUAL set exactly the residue above (plus the watched cell if it stays), no EQUAL cell of the base moves. | replay on main and head; `zone_oracle_1_main_difference.json` re-recorded on the head. | **OPEN** | Step 6. |
| C-007 | The card's tables on the Spark SQL door and the DataFrame door answer Spark's values; the native `repark.sql()` door answers on the head as on main (ZS-5). | step 4 pins. | **OPEN** | Step 4. |
| C-008 | The six table-creating writers and a cached frame store the session wall for a container cast of an instant outside UTC. | step 4 pins. | **OPEN** | Step 4. |
| C-009 | The U-8 door matrix on main and head, and the U-8 Rust pins that held a container-cast refusal (rewritten to the stored session wall, ZS-4; the two engine-error statements fail as their wall twin does): every changed cell is a listed refusal→EQUAL move of ZS-4 or DIFFERENT→EQUAL; no EQUAL cell moves. | `probe.py` on both builds. | **OPEN** | Step 6. |
| C-010 | Each seam arm removed alone reds a pin; the seam call removed reds the corpus. | mutants. | **OPEN** | Step 5. |
| C-011 | Cost: the U-8 verifier's six statements, 10-run medians, head ≤ 1.05x main, load under 5. | timing table. | **OPEN** | Step 6. |
| C-013 | `from_json` reads a `TIMESTAMP_NTZ` field: a text with no zone in Spark's form is its wall in every session zone; a text with `Z` or an offset is NULL; a space separator and a date-only text keep main's answer (ZS-6). | the two `from_json` pins, red on main. | **OPEN** | Step 3b. |
| C-012 | Records: the card (follow-up lines of ZS-1 and ZS-5), R-011 and R-015 status lines, the v1.5.6 notes line, the ZONE-ORACLE-1 order follow-up (ZS-3), `map.md`s. | the diff. | **OPEN** | Step 7. |

Up: [map.md](map.md).
