# Card TEMP-VIEW-ALTER-AND-CACHE-AS-1: three statements on a temp view refuse, or are gaps, where Spark answers

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the TEMP-VIEW-SESSION-SETTINGS-1 step 0 (PR #1356, head `a2e34b9e`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S3** (owner, 2026-11-17, "defaults on both", question 10: one card, S3). The refusals are loud, and Spark answers.

## Finding

On a temp view, three statements differ from Spark:

- `ALTER VIEW <temp> RENAME TO <new>` refuses `TABLE_OR_VIEW_NOT_FOUND`, also with no `SET`. Spark answers. The refusal comes from `alter_view_missing_target` (`execute.rs:298-315`, reached at `:242`), because a temp view is not a catalog view.
- `ALTER VIEW <temp> AS …` refuses with `ALTER VIEW <viewName> AS is not supported`. Spark re-captures the settings and answers.
- `CACHE TABLE … AS SELECT` refuses with `Unsupported SQL statement`. Spark answers.

## Cells

Domain table §7.4, five direct-gap cells, and the revision's four:

- `ALTER VIEW` rename, `10 / b` created with ANSI on, renamed and read with it off: Spark raises `DIVIDE_BY_ZERO` (creation); RePark raises `TABLE_OR_VIEW_NOT_FOUND`.
- `ALTER VIEW` rename, `hour(ts)` created in one zone and read in the other: Spark answers 2; RePark raises.
- `ALTER VIEW` with no `SET` (`SELECT 1`): Spark answers 1; RePark raises.
- `ALTER VIEW v AS 10 / b` and `ALTER VIEW v AS hour(ts)`: RePark raises; Spark answers.
- `CACHE TABLE ct AS SELECT * FROM tzt`: Spark answers; RePark raises.

The `ALTER TABLE` spelling of the same rename answers null on RePark. That is a view gap, owned by TEMP-VIEW-SESSION-SETTINGS-1, not by this card.

## Relation

ALTER-VIEW-RENAME-OVER-TABLE-1 and ALTER-VIEW-DIGIT-NAME-1, the narrower open cards this one sits beside (sketch §7, question 10). When built, `ALTER VIEW … AS` must re-capture the settings and the rename must copy them; the cells show Spark does both.

## Source

TEMP-VIEW-SESSION-SETTINGS-1 step 0 sketch §7, question 10; step 0 domain table §7.4; step 0 revision hand-back, item 5 (the refusal route); the round 2 re-critic's verdict (GAPS, `a2e34b9e`), items 5 and the rename recount.

Up: [map.md](map.md).
