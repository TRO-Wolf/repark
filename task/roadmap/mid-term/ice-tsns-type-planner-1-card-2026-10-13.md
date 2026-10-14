# Card ICE-TSNS-TYPE-PLANNER-1: the SQL type name decides the unit, so a nanosecond cast means nanoseconds

**Date:** 2026-10-13. **Filed by:** the docs clerk lane, from the Fable 5.1 sketch
[sketch.md](../../wo/ice-tsns-type-planner-1/sketch.md) under the owner's ruling of 2026-10-13.

**Status:** open. Target v1.5.6. Grade A.

**Supersedes:** card ICE-TSNS-NARROW-REFUSE-1
([ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md),
PR #1027, closed unmerged 2026-10-13) and card ICE-TSNS-COERCION-1
([ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md),
PR #1042, closed unmerged 2026-10-13).
Parity row: ICE-TSNS-SQL-1-R-017 in [docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Status 2026-10-13: step 0 round 1 accepted, amendment 1 filed

Step 0 round 1 accepted by the owner: H1, H2 and H3 met, H4 not met.
[Amendment 1](../../wo/ice-tsns-type-planner-1/amendment-1.md) resolves them: H1, scalar
`range` and `generate_series` over timestamps refuse by name, naming `sequence` as Spark's
spelling; H2, proceed, with DDL and `Column.cast` pinned at microseconds; H3, the
`SparkTimestampUnitWiden` rule as the amendment states it (finest unit wins, never narrows,
overflow raises in both ANSI modes); H4, producer P15 added. Step 1 also covers what step 0
could not measure: Postgres cells with the rootless Docker socket inside the lane, join keys
in the optimized plan, NULL beside a nanosecond value in every position, non-default session
zones. Step 1 opens on Opus lane A after the owner's triage. R-009
([order](../../wo/ice-tsns-floor-doors-1.md)) builds after this unit lands.

## Why

DataFusion 54.1 maps the SQL type name `TIMESTAMP` to `Timestamp(Nanosecond, None)`.
RePark's session rule therefore reads every `Expr::Cast` whose target is a nanosecond
timestamp as Spark's `TIMESTAMP` and rewrites it to a microsecond instant, and reads a
nanosecond source cast to a naive nanosecond target as a user narrowing. When the
planner coerces `coalesce(ns, NULL)` it inserts `CAST(NULL AS Timestamp(ns))`; the rule
cannot tell that cast from one the user wrote, turns it into microseconds, and the
nanosecond sibling narrows to match. Two designs tried to tell the planner's cast from
the user's by walking the plan (#1027 marker functions, #1042 feed rewrite, then #1042
narrowed to store-column feeders); seven verifier rounds, three on #1027 (2026-10-10,
2026-10-11, 2026-10-12) and four on #1042 (2026-10-12, twice 2026-10-13), each found a
new plan shape. The set of plan shapes is open. The set of SQL type names is closed.

The fix registers one `SparkTypePlanner`: `TIMESTAMP` and `TIMESTAMP_LTZ` map to a
microsecond instant in the session zone, `TIMESTAMP_NTZ` to naive microseconds, and the
nanosecond spellings `timestamp_ns` / `timestamptz_ns` to nanoseconds. After the
planner, a `Timestamp(ns, _)` cast target means nanoseconds, because no other SQL name
maps to it. The three `rewrite_cast` arms for nanosecond targets come out, the
`timestamp_ns` lowering and the by-name kernel registration retire, and the wall
kernels apply by target type in the analyzer, so `UPDATE … SET`, `DELETE … WHERE` and
the ANSI door get them without lowering.

Owner's sentence, verbatim:

> No plan-walk design for R-017 is accepted at sketch, now or later.

## The ask

- Primary parity row R-017: a nanosecond value beside an untyped NULL keeps nine
  digits through every store door, by type, never by walking the plan.
- Rows R-010, R-013 and R-018(1) are measured against the type-planner head and keep
  their rows only if they still fail.
- Row R-009 stays its own unit.
- Step 0 commits the inventory (§3 of the sketch: producers P1–P14) and the fixture
  (§5: the 9,600-cell census, the 117 reads, the 90 consumer cells, the seven rounds'
  repros as named cells, the 420 cache cells, the R-010/R-013/R-018(1) cells) before
  any product code; a producer with no disposition halts the unit.
- The owner's rules stay exactly as ruled: a cast the user wrote to `TIMESTAMP` over a
  nanosecond value floors, a NULL beside a nanosecond value takes the nanosecond type,
  out of range raises in both ANSI modes, nothing falls back silently.

## Sequence (owner ruling, 2026-10-13)

1. This docs PR (clerk lane, no verifier).
2. Step 0 on Muse xhigh: the inventory and the fixture, red first; halts on any of
   H1–H4; the owner gets the one-paragraph result before step 1.
3. One Opus 5.5 high executor (planner, deletions, analyzer, P5 dispositions).
4. CI green on the branch before any verifier.
5. One scoped Opus 5.5 high verifier; a FAIL parks the unit and the owner hears first.

## Out of scope

- The pandas/Arrow nanosecond arrays: card NS-ARRAY-FLOOR-1 (parity row R-020).
- The struct and map fields holding nanosecond values: card NS-NESTED-PANDAS-1.
- Any plan-walk, marker function, feed rewrite or reader accounting of any kind; the
  unit is refused at review if one appears.
- No new SQL type names. No `unsafe`. No change to any numeric gate.
