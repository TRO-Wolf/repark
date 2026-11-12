# Part 1 finish: the gates, the clause item 4 proof, and a halt (resolved)

Head `e710af4f` (the clippy fix on top of `5805a6ee`), base frozen at `276dc503`. Measured 2026-11-12 by
`claude-opus-5-5`.

## 1. Gates on `e710af4f`

| Gate | Exit | Count |
|---|---|---|
| `make rust-clippy` | 0 | |
| `make rust-panic-ban` | 0 | |
| `make py-test-facade` (ci) | 2 | 2 failed, 27900 passed, 711 skipped, 150 xfailed |
| `make py-test-facade FACADE_PROFILE=release` | 2 | 2 failed, 27905 passed, 706 skipped, 150 xfailed |
| `cargo test --locked -p repark-core --lib` | 0 | 1605 passed, 1 ignored |
| `cargo test --locked -p repark-sql --tests` | 0 | 686 passed, 4 ignored |
| `cargo test --locked -p repark-spark --lib` | 0 | 2929 passed, 5 ignored |
| `cargo test --locked -p repark-common` | 0 | 97 passed |
| `cargo test --locked -p repark-python --lib` | 0 | 176 passed |
| `cargo test --locked -p repark-spark --test struct_compare_by_position --test struct_compare_seat_family --test decimal_float_coercion` | 0 | 5 passed |
| the protected Python pin files (below), ci profile, `-rs` | 0 | 3832 passed, none skipped |
| the three touched test files after the fixes (`test_signed_zero_keys_1.py`, `test_polars_is_duplicated_1.py`, `test_group_agg.py`), release and ci | 1 | 1 failed (the halted `collect_set` pin), 67 passed, 1 skipped (no pyspark) |

The same two tests failed in both facade profiles. Neither belongs to part A, unit C, #1290, #1292, #1295 or #1304.
Those units' pin files all passed in both full profiles and again on their own:
`sql/test_signed_zero_normalize_1.py`, `types/test_decimal_float_coercion_1.py`, `sql/test_struct_setop_type_family_1.py`,
`sql/test_struct_compare_by_position_1.py`, `sql/test_struct_compare_seat_family_1.py`,
`sql/test_listview_inner_join_key_1.py`; the Rust pins `repark-sql` `signed_zero_dml.rs`, `struct_setop_reorder_refusal*.rs`,
`struct_setop_type_family_1.rs`, `struct_setop_recursive_kind.rs` (in `--tests`), and the three `repark-spark` files above.

| Failing pin | Base answer | Head answer | Spark 4.1.2 (measured here) | Disposition |
|---|---|---|---|---|
| `dataframe/test_polars_is_duplicated_1.py::test_row_number_and_drop_duplicates_unchanged` (`keep.dropdup_double`, RePark's own base answer, not an oracle) | `dropDuplicates(['c'])` keeps `-0.0` and `0.0` | one `0.0` | one `0.0` | the clause seat DI-6 (`dropDuplicates`), moved to Spark's answer: the `-0.0` row is removed from the keep fixture |
| `dataframe/test_group_agg.py::test_collect_set_signed_zero_preserves_distinct_bits` | `F.collect_set` over `-0.0, 0.0, 0.0` is `[-0.0, 0.0]` | `[0.0]` | `[-0.0]` | the §2 halt, **RESOLVED** by the owner's ruling of 2026-11-12: declared SIGN-ONLY, and the pin is renamed `test_collect_set_keeps_one_zero_and_its_sign_is_the_declared_residue` |

## 2. The halt: `F.collect_set` over both zeros moves, but not to Spark's answer

Order §7: "Any cell outside the clause that moves, unless it moves to Spark's measured outcome and the evidence
names it." The DataFrame `F.collect_set` is not a clause cell. On the head it plans through seat 2's distinct key, so
it returns one zero, as Spark does. But it always returns `0.0`, while Spark returns the sign of the first zero it
meets ([zero_order.py](zero_order.py), [zero-side-legs.jsonl](zero-side-legs.jsonl)):

| Input order | Spark `F.collect_set`, 1 and 2 partitions, and SQL `collect_set` | RePark head `F.collect_set` | RePark head SQL `collect_set` |
|---|---|---|---|
| `-0.0, 0.0, 0.0` | `[-0.0]` | `[0.0]` | `[-0.0, 0.0]` (unchanged from the base) |
| `0.0, -0.0, -0.0` | `[0.0]` | `[0.0]` | `[-0.0, 0.0]` |
| `0.0, -0.0` | `[0.0]` | `[0.0]` | `[-0.0, 0.0]` |

So the cardinality now matches Spark and the sign matches only when the positive zero comes first. This is the class
ruling G declared for `min`/`max` ("Spark returns the first zero it meets"; SIGNED-ZERO-KEYS-1-MINMAX), but the
declaration names `min`/`max` only. The SQL-door `collect_set` does not move; it is a function-internal comparison
(`COLLECTION-FLOAT-EQ-1`, outside part 1).

Proposed default for the owner: declare `F.collect_set`'s zero sign beside MM-1 (SIGN-ONLY, the zeros are equal under
SQL comparison), and change the pin to assert one element.

**RESOLVED, 2026-11-12.** The owner, verbatim: "Go with the defaults on 1,2,4". Item 1 adopts the default above:
`F.collect_set`'s zero sign is declared SIGN-ONLY beside MM-1, as registry row SIGNED-ZERO-KEYS-1-COLLECTSET in
`docs/spark-sql-iceberg-parity.md`. The pin is now
`dataframe/test_group_agg.py::test_collect_set_keeps_one_zero_and_its_sign_is_the_declared_residue`. It asserts one
zero element for `-0.0, 0.0, 0.0` and does not assert its sign. A non-zero control value (`1.5`, in the same group
and in a second group) stays in the set. The `countDistinct('a', 'b')` assertion moves unchanged into its own test,
`test_count_distinct_over_two_columns_keeps_signed_zero_pairs_apart`. Item 2 of the ruling, the card for that finding,
is filed by a records clerk.

**A second finding, unchanged by part 1:** `countDistinct('a', 'b')` over `(-0.0, 0.0), (0.0, 0.0), (-0.0, -0.0)`
answers 3 on the base and the head. Spark answers 1. A multi-argument distinct aggregate's arguments are not a seat-2 key
position, and no clause cell covers them; this is a card candidate.

## 3. Clause item 4: one wrapper per key on the confounded cells

All 84 confounded float cells (decimal twin not EQUAL; the clause's 78 and the six SQ-any cells) reach their decimal
twin's head label. The 38 that answer were explained on the head's release build ([confounded_wrappers.py](confounded_wrappers.py),
[confounded-wrappers-head.jsonl](confounded-wrappers-head.jsonl)); the other 46 refuse on the head as their twins do.

| Cells | Wrappers in the optimized plan | Wrapped arguments |
|---|---|---|
| WI-2 `f-expr` and native, WI-3, WI-5, OB-1, OB-2 native (12) | 1 each | the one window or sort key |
| SO-intersectall, SO-exceptall, `spark-sql` and native (16) | 2 each | the key on each side |
| SQ-any (8) | 6 each | each of the three rewritten subqueries' two keys |
| GB-11 typed pivot (2) | 0 | the pivot has no seat-2 key; its float pairing is the per-value literal comparison, part A's seat 1 |

No wrapper is nested in any cell.

Up: [map.md](map.md).
