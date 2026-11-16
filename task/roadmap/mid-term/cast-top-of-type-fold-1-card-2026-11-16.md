# Card CAST-TOP-OF-TYPE-FOLD-1: a narrowing cast compared with its target type's top or bottom refuses where Spark answers without casting

**Date:** 2026-11-16. **Filed by:** the CAST-LITERAL-UNWRAP-1 builder (`claude-opus-5-5`), under the owner's ruling 4
of 2026-11-16 ("adopt Fable's, run the string-cast sizing": out of that unit, pinned, carded S3).

**Status:** PROPOSED, not dispatched.

**Severity: S3.** A refusal where Spark answers; no answer is wrong. It blocks no label.

## Finding

Spark answers `CAST(c AS T) > MAX`, `MAX < CAST(c AS T)`, `CAST(c AS T) < MIN` with no rows without evaluating the
cast, for `TINYINT` (from DECIMAL) and `SMALLINT` (from `INT`) in both ANSI modes and for `INT` from `BIGINT` with
ANSI off; and `CAST(c AS T) = MAX+1` with no rows. Main answered the first three with the unwrap's wrong rows and
refused `= MAX+1` (Arrow `out of range` on the constant). Since CAST-LITERAL-UNWRAP-1 the cast is kept: such a cast
can overflow, so its `preimage` declines, the cast runs per row and refuses on the first value past the type
(`value of 128 is out of range Int8`, `Can't cast value 32768 to type Int16`), where Spark answers no rows. With ANSI
on and a fixture that is not folded to a local relation, Spark raises `CAST_OVERFLOW` too (measured on a `VALUES`
view by that unit's pins), so the gap is the fold, not the cast.

## Measured (CAST-LITERAL-UNWRAP-1's bar, head `88999454`): WRONG→REFUSE (Q4), 33 cells

- cells, ANSI false (8): `dec_tinyint_overflow/view/where/c1/lt/l`, `dec_tinyint_overflow/view/where/c1/gt/r`, `dec_tinyint_overflow/view/df/col_gt`, `dec_tinyint_overflow/ice_part/df/col_gt`, `bigint_int/view/where/c2/lt/l`, `bigint_int/view/where/c2/gt/r`, `int_smallint/view/where/c1/lt/l`, `int_smallint/view/where/c1/gt/r`.
- shapes, ANSI false (9): `q4/tinyint/gt_top`, `q4/tinyint/top_lt`, `q4/tinyint/lt_bottom`, `q4/smallint/gt_top`, `q4/smallint/top_lt`, `q4/smallint/lt_bottom`, `q4/int/gt_top`, `q4/int/top_lt`, `q4/int/lt_bottom`.
- cells, ANSI true (6): `dec_tinyint_overflow/view/where/c1/lt/l`, `dec_tinyint_overflow/view/where/c1/gt/r`, `dec_tinyint_overflow/view/df/col_gt`, `dec_tinyint_overflow/ice_part/df/col_gt`, `int_smallint/view/where/c1/lt/l`, `int_smallint/view/where/c1/gt/r`.
- shapes, ANSI true (6): `q4/tinyint/gt_top`, `q4/tinyint/top_lt`, `q4/tinyint/lt_bottom`, `q4/smallint/gt_top`, `q4/smallint/top_lt`, `q4/smallint/lt_bottom`.
- native, ANSI native (4): `dec_tinyint_overflow/native_view/where/c1/lt/l`, `dec_tinyint_overflow/native_view/where/c1/gt/r`, `int_smallint/native_view/where/c1/lt/l`, `int_smallint/native_view/where/c1/gt/r`.

`= MAX+1` (`q4/*/eq_top_plus`) refused on main and refuses on the head. Pinned on the head's behaviour by
`python/repark/tests/types/test_cast_literal_unwrap_1.py::test_the_top_of_type_comparison_refuses_per_row`.

## Not yet known

The seat: a rule that folds a comparison of a narrowing cast with a constant at or past the target type's bound before
the cast runs, as Spark's optimizer does, without hiding the overflow raise Spark keeps for the other operators.

Up: [map.md](map.md).
