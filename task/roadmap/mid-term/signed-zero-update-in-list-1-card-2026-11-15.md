# Card SIGNED-ZERO-UPDATE-IN-LIST-1: `UPDATE … WHERE v IN (w)` over DOUBLE columns misses a `-0.0` row against `0.0`

**Date:** 2026-11-15. **Filed by:** the DECIMAL-DOUBLE-UNION-COERCION-1 part 1 builder (`claude-opus-5-5`), from
its step 0′ finding F3. The orchestrator ruled it a card on 2026-11-15.

**Status:** filed; not dispatched. Pre-existing: main `dc6b4c94` and part 1's head answer alike.

**Severity: S1, a wrong write.** The UPDATE leaves a row unchanged that Spark changes. It is on scalar DOUBLE
columns of a format-v2 table on the Spark door, so it is **inside the scoped label ICE-V2-FLAT-AWS-LABEL-1** and is
label-blocking. It was measured on a local hadoop catalog; the predicate is planned by the engine, not the catalog.

## Measured (Spark 4.1.2 on JDK 17 against main `dc6b4c94` and the part-1 head, 2026-11-15)

Table `(id INT, v DOUBLE, w DOUBLE)`, rows `(1, -0.0, 0.0)` and `(2, 0.5, 0.5)`, then
`UPDATE t SET id = id + 100 WHERE <predicate>`. The ids after the UPDATE:

| predicate | copy-on-write: Spark / main / head | merge-on-read: Spark / main / head |
|---|---|---|
| `v IN (w)` | [101, 102] / [1, 102] / [1, 102] | [101, 102] / [1, 102] / [1, 102] |
| `v IN (w, 5.0D)` | [101, 102] / [1, 102] / [1, 102] | [101, 102] / [1, 102] / [1, 102] |
| `NOT (v NOT IN (w))` | [101, 102] / [1, 102] / [1, 102] | [101, 102] / [1, 102] / [1, 102] |
| `v = w` (control) | [101, 102] on all three | [101, 102] on all three |
| `v IN (0.0D)` (control) | [2, 101] on all three | [2, 101] on all three |

- The counts are identical in both ANSI modes.
- The `SELECT id … WHERE <predicate>` forms are right on all three builds.
- So only the UPDATE path compares an IN list of FLOAT/DOUBLE **columns** by bits.

In this unit's DML set it shows as `z/cow/update_in` and `z/mor/update_in`:
- Spark `[3, 101, 102]`;
- main `[101, 102, 103]` (the `decimal(30,15)` coercion matches `1e-20`);
- the part-1 head `[1, 3, 102]` (now widened, it misses the `-0.0` row).

## Lead (a seat, not a cause)

The copy-on-write and merge-on-read UPDATE re-plan the predicate inside a CASE or filter over the rendered text. A
one-element IN list over two columns is not rewritten to `=`, so SIGNED-ZERO-NORMALIZE-1's key and literal seats
never see it. Unmeasured as a cause.

## Pin

`python/repark/tests/sql/test_decimal_double_coercion_1.py` carries the two `z/*/update_in` cells as strict xfails
naming this card.

Up: [map.md](map.md).
