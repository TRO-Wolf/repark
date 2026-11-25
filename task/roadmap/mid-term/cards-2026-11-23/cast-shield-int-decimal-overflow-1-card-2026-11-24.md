# Card CAST-SHIELD-INT-DECIMAL-OVERFLOW-1: an integer-to-decimal overflow in a filter is silently false under ANSI

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review, measured here.
**Severity:** S2 (owner, 2026-11-24). **Status:** fixed on `fix/cast-shield-int-decimal-overflow-1` (2026-11-25, Ruling 1), awaiting merge review.

## Measured

Measured on a build of main at `fa7d75e4` and on Spark 4.1.2. `t` is `(SELECT CAST(123456 AS BIGINT) AS b)`.

| Statement | ANSI | Spark 4.1.2 | RePark |
|---|---|---|---|
| `SELECT b FROM t WHERE CAST(b AS DECIMAL(5,0)) = 1` | on | `[NUMERIC_VALUE_OUT_OF_RANGE]` | **no rows** |
| `SELECT b FROM t WHERE CAST(b AS DECIMAL(5,0)) = 1` | off | no rows | no rows |
| `SELECT CAST(b AS DECIMAL(5,0)) FROM t` | on | `[NUMERIC_VALUE_OUT_OF_RANGE]` | Arrow error (loud) |
| `SELECT CAST(b AS DECIMAL(5,0)) FROM t` | off | NULL | **Arrow error** (loud) |

- **ANSI on, in a filter:** RePark gives no rows and no error. A caller who relies on ANSI to stop bad data gets no
  signal.
- **ANSI off, in a projection:** RePark crashes where Spark answers NULL.

## Cause (named by the outside review, not yet read here)

The filter-side cast shield and the projection cast disagree about which integer-to-decimal casts can overflow. A
single table of cast semantics, shared by the shield, the stats barrier and the overwrite path, was proposed.

## Fixed (2026-11-25)

Measured on Spark 4.1.2, main and the branch head, both ANSI modes. The full table is in
[evidence/domain-table.md](../../../wo/cast-shield-int-decimal-overflow-1/evidence/domain-table.md): 1019 step-0
cells and 114 conjunction and `OR` shapes. No cell that equals Spark on main moves. The native door is byte-identical
to main.

- **The cause, read here:** DataFusion's `unwrap_cast_in_comparison` moved `CAST(b AS DECIMAL(5,0)) <op> k` onto the
  literal (`b <op> k`), because the cast shield did not count integer to decimal as narrowing. The Spark cast seam
  left the cast on Arrow's kernel, which raises in both modes. The shield and the projection did not disagree, and no
  shared cast table was needed.
- **The fix:**
  - The Spark cast seam rewrites an integer-to-`DECIMAL(p,s)` user `CAST` that can overflow to the decimal narrowing
    kernel. The kernel answers NULL on overflow with ANSI off and raises `NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION`
    with ANSI on.
  - The shield counts that cast as narrowing, for `TRY_CAST`, on the Spark door only (a session-extension switch).
  - A run-once guard on the optimized plan, in the stack query planner, evaluates the kernel call only on the rows
    where Spark evaluates it: after the earlier filter conjuncts, or where the left side of an `AND` is not false or of
    an `OR` is not true.
- **Pruning:** EXPLAIN is byte-identical for queries without the cast. A predicate on the overflowing cast reads every
  row group and file, as Spark's scan does. Spark pushes only `IsNotNull`.

## Still unlike Spark (each present on main, unchanged by the fix)

- **Native door, `TRY_CAST` comparisons.** `TRY_CAST(bi AS DECIMAL(5,0)) < 1` returns id 3, whose cast is NULL in the
  door's own projection. `<> 1` returns ids 2, 3, 4, 6 and 7 where only 7 is right. The native door has no Spark seam,
  and the shield's new rule is gated to the Spark door. The question is the owner's: whether the native door should
  share the rule.
- **The conjunction over-raise of the earlier raising casts.** `SELECT id FROM t_fit WHERE CAST(bi AS INT) = 1`, and
  `... WHERE id IN (1, 5, 7) AND <cast> = 1`, where the conjunct removes the overflowing rows:
  - ANSI on: BIGINT to INT and INT to SMALLINT raise `CAST_OVERFLOW`, and DECIMAL(10,2) to DECIMAL(5,1) raises
    `NUMERIC_VALUE_OUT_OF_RANGE`;
  - both modes: STRING and DOUBLE to DECIMAL(5,0) raise Arrow's error.

  Spark answers `[1]` in all of these (`conj/{big_to_int,int_to_small,dec_to_dec,str_to_dec,dbl_to_dec}/*`). The guard
  is scoped to the integer-to-decimal kernel. Widening its trigger is a separate card.
- **`UPDATE ... SET d = CAST(bi AS DECIMAL(5,0)) WHERE id IN (1, 5, 7)`, ANSI on.** RePark evaluates the cast on rows
  the `WHERE` skips and raises, where Spark answers (`update/fit`). Main raises Arrow's text and the head raises Spark's
  condition. The update path builds its physical expressions outside the session's query planner.
- **A SQL temp view over a `createDataFrame` temp view** refuses `[TABLE_OR_VIEW_NOT_FOUND] __repark_cdf_<uuid>`,
  whatever its body. A view over a Parquet-backed frame works
  ([viewcheck.py](../../../wo/cast-shield-int-decimal-overflow-1/evidence/viewcheck.py)).
- **Out of this card, unchanged:**
  - STRING and DOUBLE to an overflowing DECIMAL raise Arrow's error with ANSI off and Arrow's text with ANSI on
    (`neighbour/{str_to_dec,dbl_to_dec}/*`);
  - the empty `INSERT OVERWRITE` refusal (`insert_overwrite/empty`) belongs to INSERT-OVERWRITE-CONST-CAST-1, which
    is parked.

Up: [map.md](map.md).
