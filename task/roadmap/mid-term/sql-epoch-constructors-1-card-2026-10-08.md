# Card SQL-EPOCH-CONSTRUCTORS-1: `timestamp_micros`, `timestamp_seconds` and `timestamp_millis` on the SQL door

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.

**Status:** closed 2026-10-08 — landed by SQL-EPOCH-CONSTRUCTORS-1
(`fix/sql-epoch-constructors-1`): the three names resolve on the Spark SQL door, the
parity rows named in "Measured" carry dated FIXED notes, and the native door stays a
declared refusal (pinned). See "Close" below.

**Retires:** when the unit that registers the three names on the Spark SQL door merges, and the
parity rows below are updated in that change.

## Why

Spark 4.1.2 has `timestamp_micros`, `timestamp_seconds` and `timestamp_millis` as SQL built-ins.
RePark's Spark SQL door refuses all three as unknown routines, while its DataFrame door answers
at least one of them. A SQL user therefore meets an error that a DataFrame user does not.

## Measured

Measured on main `156be81c` (release build) by the orchestrator's brief. This card does not
re-measure them; Step 0 does.

- `spark.sql("SELECT timestamp_micros(1)")`, `spark.sql("SELECT timestamp_seconds(1)")` and
  `spark.sql("SELECT timestamp_millis(1)")` each raise `[UNRESOLVED_ROUTINE] Cannot resolve
  routine ... SQLSTATE: 42883`.
- The DataFrame door, `F.timestamp_micros(col)`, answers `1970-01-01 00:00:00`.
- Spark 4.1.2 has all three as SQL built-ins.

Recorded in the repo:

- [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md), the
  ZONE-HORIZON-RENDER-1 row (about line 3269): "`timestamp_micros` / `timestamp_seconds` do not
  exist." It does not name `timestamp_millis`.
- The same file, the "Surfaced, awaiting pins" queue, row B-TZ-2 (about line 12625):
  "`timestamp_seconds` is not a Spark-door SQL function." It does not name the other two.
- The same file, the EX-7 `timestamp_seconds` entry (about line 12720): the facade schema for
  `timestamp_seconds` is `string` where Spark's is `timestamp`. The values agree under `TZ=UTC`.
- [task/ledgers/staging/zone-horizon-render-1-ledger.md](../../ledgers/staging/zone-horizon-render-1-ledger.md),
  residue class R-6: "`timestamp_micros`, `timestamp_seconds`: `UNRESOLVED_ROUTINE`."
- The DataFrame spellings are `_scalar("timestamp_millis", …)` and `_scalar("timestamp_micros", …)`
  in [python/repark/src/repark/spark/functions_expr.py](../../../python/repark/src/repark/spark/functions_expr.py),
  lines 1100–1111. A search of `crates/*/src` for those names found no registration. The kernel
  that answers them is not yet located.
- No function inventory file was found by name under `docs/` or `task/roadmap/`. The parity
  document is the only record of the missing names.

## Step 0

Before any code changes, on live Spark 4.1.2:

1. Record Spark's answer and result type for each of the three functions on the Spark SQL door,
   for an integer, a decimal, a double, NULL and an overflow argument. Record any error verbatim,
   with its class and SQLSTATE.
2. Record the DataFrame door's answer for the same arguments, for each of the three functions.
3. Compare Step 0 with the parity rows above and say in this card which rows are now stale.

## The ask

Register the three names on the Spark SQL door, over the same kernels the DataFrame door uses.
Where Step 0 finds a difference between the two doors, the SQL door follows Spark and the
difference goes in this card.

Open question for the owner: [AGENTS.md](../../../AGENTS.md) "Hard rules" requires new SQL surface to
land with both spellings and one test row per door. The native door (`repark.sql()`, ANSI and
Trino-style) does not carry Spark built-ins. Lean: register on the Spark door only, and record
the native door as a declared refusal, unless Step 0 shows the native door already resolves
these names.

## Out of scope

- The facade schema of `timestamp_seconds` (`string` against `timestamp`) on the DataFrame door.
  Step 0 records whether the SQL spelling shows the same schema. Fixing it is a separate decision.

## Gates

- Every Step 0 cell on the Spark SQL door, value and type, on the Arrow path, equals Spark's
  recorded answer.
- One test row per door for each of the three names.
- The parity rows named in "Measured" are updated in the same change, dated.
- The `map.md` of every directory the change touches is current.

## Pointers

- The parity document: [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).
- The shape of a card: [fa-6-duplicate-view-schemas-card-2026-10-08.md](fa-6-duplicate-view-schemas-card-2026-10-08.md).

## Close (2026-10-08)

Step 0 recorded 216 cells on live Spark 4.1.2 (54 inputs × `UTC` /
`America/New_York` × ANSI off / on, both doors); all four groups are
byte-identical. `timestamp_seconds` takes NUMERIC (fractional seconds kept,
double NaN/Inf answer NULL, huge doubles saturate, decimal exact-or-error);
`timestamp_millis` / `timestamp_micros` take INTEGRAL only. Strings, booleans,
timestamps and wrongly-typed NULLs refuse `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`
42K09; overflows are unclassified (`long overflow` / `Overflow` /
`Rounding necessary`). Spark refuses the `to_timestamp_*` spellings as
`UNRESOLVED_ROUTINE`.

The owner open question resolved to the lean: Spark door only, native door a
declared refusal (pinned `sql-epoch-constructors-1/C-006`). The out-of-scope
facade-schema item fixed itself: no Python change was needed once the kernel
answered LTZ micros. Stale rows, all updated with dated notes in this change:
the ZONE-HORIZON-RENDER-1 "do not exist" sentence, B-TZ-2, the EX-7
`timestamp_seconds` schema half (whose `string` reading was already stale on
base main), and zone-horizon residue R-6.

Left open, out of scope: `to_timestamp_seconds` / `to_timestamp_millis` /
`to_timestamp_micros` resolve on this engine's SQL door through DataFusion
builtins where Spark refuses them; untouched by this unit.
