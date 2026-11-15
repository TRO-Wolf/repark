# task/wo/decimal-double-union-coercion-1/evidence/fix/ — part 1's build evidence

Part 1 of the split plan (seat E, K2, K1 on written casts, Spark door only), measured on the head's package against
Spark 4.1.2 on JDK 17 and against main `dc6b4c94`, both ANSI modes. Step 0′ is [../step0b/](../step0b/map.md).

- [head-cells.jsonl](head-cells.jsonl) lists every joined cell whose verdict or answer moved between main and the
  head. It has 1,996 rows: 1,850 fixed and none broken, and the rest wrong on both builds in a different way. Each
  row carries Spark's answer, main's and the head's.
- [cost.md](cost.md) has the cost rows: five alternating runs of main, head and main again.
- [probes/](probes/map.md) holds the build's probes.
- [raw/](raw/map.md) holds the raw sets.

## Results (head `120e9d8a` against main `dc6b4c94`)

| set | result |
|---|---|
| joined cells (6,855) | 0 EQUAL→DIFF; 1,850 fixed (925 per mode) |
| coercion fuzz (165,846 ANSI on, 186,570 ANSI off) | 0 held; 72,452 fixed per mode. The ANSI-on 6 wrong are whole-shape `DECIMAL(5,0)` raises with different texts; the ANSI-off 19,820 wrong are CAST-FLOAT-DECIMAL-OVERFLOW-NULL-1, wrong on every build. Equal to the emulated head of step 0′ cell for cell |
| structured `d = v` set (60,000 rows × `=`, `<`, `<=>`, `IN`) | 0 wrong |
| Spark-door DML (97 per mode) | 46 of 48 fixed, 0 held. `z/cow/update_in` and `z/mor/update_in` remain (SIGNED-ZERO-UPDATE-IN-LIST-1) |
| native-door DML (56 per mode) | identical to main (Q2) |
| named cells (`raw/named-*`) | Spark's answer on every cell, apart from two. Spark refuses `writeTo(...).overwrite(filter)` on an unpartitioned table, so the facade pins `replaceWhere` at the union's Spark values. The struct refusal has the same error class with a different text |

Remaining wrong part-1 contexts per mode, outside this unit's seats:
- 96 select-list `IN (subquery)` cells, refused (IN-SUBQUERY-PROJECTION-1);
- 36 DataFrame `intersectAll` / `exceptAll` cells, refused by design;
- `ins_case_tiny` (INSERT-SOURCE-VALUES-ARITY-1) and `upd_where_in_union` (an UPDATE with a subquery, refused);
- `merge_union_src` and `merge_union_big`: the MERGE union source is typed by `merge.rs` before analysis, so part 2;
- `coalesce_nan_card`: a `VALUES` source, so part 2.

## Provenance

- **Builds:**
  - the head is `ddu-work/pkg-head` (`BUILT_FROM` `120e9d8a`, a release wheel with its own target directory);
  - main is `pkg-main-dc6b4c94`;
  - the step 0′ Spark raw sets are re-joined.
- **Commands:**
  - step 0′'s runners on the head;
  - `probes/named.py` and `probes/cards.py` on Spark, main and the head;
  - `../step0/probes/analyze_step0.py <spark> <main> <head> <joined>`.
- **When and where:** 2026-11-15, lane 1. Every run held the shared measure lock on cores 32 to 47, and every Spark
  JVM ran through the shared Spark slot.

Up: [../map.md](../map.md).
