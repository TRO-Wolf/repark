# COLUMNS-LIST-ARG-1 — a Python list of columns is refused where PySpark accepts it

**Rating: S2 (the owner, 2026-11-21), off the label path:** one card over every method that takes a list of columns.

**Date:** 2026-11-20. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-20.
**Source:** Fable's consult reply (2026-11-20), section 14, kept outside the repository; the step 0 is
[task/wo/asof-join-1/](../../../wo/asof-join-1/map.md), and its guidance is
[guidance.md](../../../wo/asof-join-1/guidance.md).

**Status:** filed 2026-11-20; not dispatched.

**Severity: S2, loud, off the label path.** This is Fable's guidance under the owner's delegation, not an owner ruling.

## Finding

**[M] The refusal.** On main, a Python list passed as the column argument is refused. `select` (with string lists too),
`groupBy` and `orderBy` were measured on Spark 4.1.2, where PySpark accepts a list. Main raises `PySparkTypeError`.

Refused with a list on main, per Fable's section 14 (24 call shapes measured on main):

- `select`, `groupBy`, `orderBy`, `sort`, `sortWithinPartitions`, `rollup`, `cube`;
- `orderBy([...], ascending=[...])`;
- `Window.partitionBy([...])` / `Window.orderBy([...])` (its own message);
- `F.struct([...])` (`NOT_COLUMN_OR_STR`);
- `select` with a tuple.

Accepted on main: `dropDuplicates`, `repartition`, `join(on=[...])`, `fillna(subset=)`, `F.array([...])`, `unpivot`,
`pivot` values.

PySpark's side was measured only for `select`, `groupBy` and `orderBy`. The rest is known from PySpark's signatures,
so the step 0 should measure them on the oracle.

## Why S2

`df.select(cols)` and `df.groupBy(keys)` with a list are everyday PySpark. The refusal hits ported code on its first
line, and the fix is thin facade work. It is loud and has a one-character workaround, so it is not S1 and does not
block the label.

## Scope

One card covering every columns-varargs method, not three cards for three methods. `core.py` is pinned at exactly
3,461 lines, so the fix must be line-neutral there or live in a helper module.

## Side observation (not carded separately)

`F.coalesce("i", "i")` with string names raises `AttributeError: 'str' object has no attribute
'_reject_nested_generator'`. PySpark's signature takes column names there. It was not measured on Spark. It needs one
oracle cell and, if confirmed, its own line on this card.

## First step

Measure the methods not yet measured on the PySpark oracle, and the `F.coalesce` cell.

Step 0: [task/wo/asof-join-1/](../../../wo/asof-join-1/map.md).

Up: [map.md](map.md).
