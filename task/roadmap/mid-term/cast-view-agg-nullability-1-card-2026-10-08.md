# Card CAST-VIEW-AGG-NULLABILITY-1: `max` over a view of a cast arithmetic column raises an internal schema error

**Date:** 2026-10-08. **Filed by:** Claude (Haiku 5.5), docs lane, from the orchestrator's brief.

**Status:** open. Not scheduled. Not attributed to any unit.

**Retires:** when the cause is fixed at its source, or when it is reported upstream and the
repo records the upstream reference and a pin that fails on the wrong answer.

## Why

A view over a cast of an arithmetic expression over `range` makes an aggregate fail with an
internal DataFusion error. The same frame answers `count(*)`, answers a plain `SELECT ts`, and
answers when written to parquet and read back. Spark has no such failure for this query shape.

## Measured

Measured on main `156be81c` by the orchestrator's brief. This card does not re-measure them.

The repro:

```
spark.sql("SELECT CAST(946684800 + (id * 1577) % 3000000000 AS TIMESTAMP) AS ts FROM range(10)").createOrReplaceTempView("t")
spark.sql("SELECT max(ts) FROM t").collect()
```

It raises:

```
datafusion engine error: Internal error: Physical input schema should be the same as the one converted from logical input schema. Differences: - field nullability at index 0 [ts]: (physical) true vs (logical) false.
```

Controls that answer on main:

- `SELECT count(*) FROM t` over the same view.
- `SELECT ts FROM t` over the same view.
- `count(*)` over a view of `CAST(id AS TIMESTAMP)`.
- `count(*)` over a view of `(id * 2) % 7`.
- The frame written to parquet and read back, then aggregated.

The physical side reports the column nullable. The logical side reports it non-nullable.

## Step 0

1. Shrink the repro. Find which of `+`, `*`, `%`, the cast, the view and `max` are needed to
   raise the error. Record each reduced form and whether it raises.
2. Record Spark 4.1.2's answer to the full repro and to each reduced form that still runs on
   Spark. Record values and types.
3. Identify the expression that reports non-nullable on the logical side and nullable on the
   physical side. The brief places it at the cast of an arithmetic expression over `range`.
   Confirm or correct that with the reduced forms.

## The ask

Find which expression reports the wrong nullability, and fix it at its source. If the source is
DataFusion, report it upstream and record the upstream reference here. A fix in RePark must not
change any answer that already matches Spark. The Step 0 controls stay as pins.

Scope is the one shape above. Other nullability paths are not in the ask.

## Gates

- The full repro answers the Spark value and type recorded in Step 0.
- Each Step 0 control answers as it does on main, pinned.
- A pin that fails on the old behaviour for the reason the card names, not for some other reason.
- If the fix is upstream, the card names the upstream issue or change and the pin that stays red
  until the pinned version carries it.

## Pointers

- The shape of a card: [fa-6-duplicate-view-schemas-card-2026-10-08.md](fa-6-duplicate-view-schemas-card-2026-10-08.md).
