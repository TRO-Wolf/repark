# Card DEREFERENCE-FORMS-2: an array-of-struct field, a `struct(...)` field name and a lambda variable's field are not dereferenced as Spark does

**Date:** 2026-11-04. **Filed by:** the records clerk (`claude-haiku-5-5`) from ledger lead L4 (clause C-010) of [SUBSCRIPT-DOT-ACCESS-1](../../ledgers/completed/subscript-dot-access-1-ledger.md): the Opus verifier's findings F4 and F5 and the Muse cards C1 and C2.

**Status:** filed, not dispatched. Verifier class S3 (refusals, no wrong value).

**Why:** Three forms that a field access reaches on the Spark door, and that the SUBSCRIPT-DOT-ACCESS-1 rewrite does not cover:
- `SELECT array(named_struct('a', 1), named_struct('a', 2)).a` refuses `type List(Struct(...)) is not Struct, Map, or Null`. Spark answers `[1, 2]` (measured, `q2.log`). This is the table's B-expr row `arr_struct_field_all`, still refused.
- `SELECT struct(1, 2).col1` refuses `Field col1 not found in struct`, because `struct()` names its fields c0 and c1 (D13). Spark names them col1 and col2 and answers 1 (reasoned, not measured).
- `transform(map_values(m), e -> e.arr[0].g)` refuses UNRESOLVED_COLUMN on the lambda variable `e`. The expected Spark answer is `[[6], [8]]` (not measured).

**Fix shape:** the list-of-struct field projection, the `struct()` field naming (D13), and a field access on a lambda variable, each measured on the Spark oracle and pinned.

**Retires:** when each of the three answers Spark's value on the Spark door, pinned; the `struct()` naming closes with D13.

**Relation:** [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md) (ledger clause C-010); the [nested rule doc](../../../docs/corpus/nested-rule-2026-11-03.md) D2 and D13.

Up: [map.md](map.md).
