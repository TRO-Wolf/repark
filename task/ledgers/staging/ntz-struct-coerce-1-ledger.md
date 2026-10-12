# Unit ledger — NTZ-STRUCT-COERCE-1 · a struct coerced from a naive wall to an instant localises the wall in the session zone

**Date:** 2026-10-12 · **Branch:** `fix/ntz-struct-ltz-coerce-1` · **Base:** `572130dda35f739795578134348f99fd7b530cfd` ·
**Model:** Grok 4.7 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the charter in the session brief. One arm in `rewrite_cast` and one kernel.
**This ledger closes when the unit's pull request merges.**

**Why.** DataFusion inserts `Cast(struct<v: Timestamp(µs)> → struct<v: Timestamp(µs, "UTC")>)`
when a `CASE`, `COALESCE` or `UNION ALL` mixes a wall struct with an instant struct. Arrow
localises a naive timestamp in the target zone, and RePark's `TIMESTAMP` is
`Timestamp(µs, "UTC")`, so the wall is read as UTC. The top-level cast is already
`to_timestamp`. `rewrite_cast` has no struct arm.

## PROPOSITION LEDGER — NTZ-STRUCT-COERCE-1 — 2026-10-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Main 572130dd is measured before any change on the mixed-struct inventory. | The step-0 probe output. | **OPEN** | §1. `task/wo/ntz-struct-coerce-1/evidence/repark-main.jsonl`. |
| C-002 | Spark 4.1.2 is measured on the same inventory. | The Spark probe output. | **OPEN** | §2. `task/wo/ntz-struct-coerce-1/evidence/spark-4.1.2.jsonl`. |
| C-003 | A struct cast from a naive leaf to an instant leaf localises the wall in the session zone, on reads and writes. | Pins. | **OPEN** | Not built. The measured gap is §3. |
| C-004 | Statements with no such cast are identical to main: plan, EXPLAIN text, answer. | Pins. | **OPEN** | Top-level controls already match Spark (§4). |
| C-005 | The gates are green with real exit codes. | Section "Gates". | **OPEN** | Step 3. |

## 1. Main, measured before any change (C-001)

Build of `572130dda35f739795578134348f99fd7b530cfd`, release facade. Probe
`task/wo/ntz-struct-coerce-1/probe.py`. 144 cells, 144 answered, 0 errors.
Wall `TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'` read as UTC is `1767323045123456`.
The instant `TIMESTAMP '2026-07-04 11:22:33.654321+00:00'` is `1783164153654321` in every zone.

On a struct `CASE`, `COALESCE` and `UNION ALL`, the wall leaf stays `1767323045123456`
in UTC, America/New_York and Asia/Kolkata. The type of that leaf is
`timestamp[us, tz=UTC]`. The instant leaf is unchanged.

A user-written `CAST(st AS STRUCT<v: TIMESTAMP, n: INT>)` returns `timestamp[ns]` and
the tick `1767323045123456000` (the UTC wall scaled to nanoseconds) in every zone.

## 2. Spark 4.1.2 (C-002)

Live PySpark 4.1.2 with Iceberg 1.11.0, in-memory catalog. Python `collect` returns a
naive datetime in the JVM zone `America/New_York`; the probe converts that display back
to a UTC microsecond tick. 144 cells, 144 answered, 0 errors.

Spark localises the wall in the session zone for every struct shape, including the
user-written cast:

| Zone | Localised wall | Struct case id 1 | Top case id 1 | Struct cast id 1 |
|---|---|---|---|---|
| UTC | 1767323045123456 | 1767323045123456 | 1767323045123456 | 1767323045123456 |
| America/New_York | 1767341045123456 | 1767341045123456 | 1767341045123456 | 1767341045123456 |
| Asia/Kolkata | 1767303245123456 | 1767303245123456 | 1767303245123456 | 1767303245123456 |

The instant row stays `1783164153654321`. Comparison of the mixed value with that fixed
instant, and a join on it, agree with main: the January wall is not the July instant in
any reading. Those cells are in the 109 that match.

## 3. Where the answers differ

`evidence/summary.md`: 35 of 144 cells differ, all of them struct cells. None are
top-level. The 35 are read, order, group and write of `CASE`, `COALESCE` and `UNION ALL`
in America/New_York and Asia/Kolkata (24 cells; UTC matches because localising in UTC is
the identity), plus read, order, group and write of the user-written cast in all three
zones, and the cast's write in the two non-UTC zones (11 cells). The cast's UTC write
read-back is the microsecond UTC wall on both engines, so that one cell matches.

RePark's struct coercion reads the wall as UTC. Spark's is the session-zone instant.
The shapes do not already equal Spark. The top-level control does not differ from Spark.

## 4. The seat (C-003's plan question)

`rewrite_cast` is `instant_ts.rs`, called from `SparkLtzTimestampCast::analyze`
(the rule `spark_ltz_timestamp_cast`). On the Spark door that rule is installed once:
`SparkExtension::register` (`extension.rs`) appends `spark_door_post_coercion_rules()`
(`spark_literal_typing.rs`), which is `analyzer_rules()` (`registration.rs`). The
configured analyzer runs DataFusion `TypeCoercion` before that list. Inside
`analyzer_rules()` the timestamp rule sits before a second `TypeCoercion`.

A one-shot analyzer walk (`evidence/seat-rules.txt`) prints the plan around that rule
on a production `SparkExtension` session. Rule order: the configured analyzer's
`type_coercion`, then the post-coercion list, whose `spark_ltz_timestamp_cast` sits
before a second `type_coercion`.

Struct `CASE`, before `spark_ltz_timestamp_cast`:

`CASE WHEN a.id = Int32(2) THEN b.st ELSE CAST(a.st AS Struct("v": Timestamp(µs, "UTC"), "n": Int32)) END`

After the rule, the same cast is still there. The first `type_coercion` inserted it.
The rule sees it and does not rewrite it. No change in `repark-spark` is required.

Top-level control, same seat. Before the rule:
`CASE WHEN a.id = Int32(2) THEN b.v ELSE CAST(a.v AS Timestamp(µs, "UTC")) END`.
After the rule: `CASE WHEN a.id = Int32(2) THEN b.v ELSE to_timestamp(a.v) END`.

The user-written cast is already `CAST(a.st AS Struct("v": Timestamp(ns), "n": Int32))`
in the initial plan and is unchanged at the rule. It reaches the seat. Its target leaf
is nanoseconds, not an `is_ltz_timestamp` leaf, so the arm the ruling describes does not
select it. Spark still localises that cast (§2). Named in §6. The source leaf is
microsecond naive; the nanosecond type is the planned target of `TIMESTAMP` inside the
struct cast, not a nanosecond source leaf.

## 5. Gates

Not this step. The step-0 commit is the probe and this ledger.

## 6. Out of scope, observed

- A user-written `CAST(st AS STRUCT<v: TIMESTAMP, n: INT>)` is planned as
  `Timestamp(ns)` and is not localised. Spark returns a microsecond session instant.
  The ruling's arm matches an `is_ltz_timestamp` target only, and a nanosecond target
  stays on today's plan.
- `INSERT … VALUES` of a mixed struct was not the measured write. The measured write is
  `INSERT … SELECT` of the mixed expression, and that projection carries the coercion cast.
- Compare and join of the mixed struct against the July instant agree with Spark on main,
  because neither the UTC reading nor the session-zone reading equals that instant.
