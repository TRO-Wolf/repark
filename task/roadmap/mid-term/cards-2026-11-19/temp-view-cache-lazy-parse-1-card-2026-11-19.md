# Card TEMP-VIEW-CACHE-LAZY-PARSE-1: a lazy `.cache()` of a JSON or CSV frame view parses at the view's registration, where Spark parses at the first action

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under Fable's guidance of 2026-11-19 (the owner's delegation, about 01:28 UTC). **Source:** Fable's reply of 2026-11-19 (reader-parse step 0), §2 row Q8 and §1.4 (the `udf` door), kept outside the repository; the step 0 [domain table](../../../wo/temp-view-frame-reader-parse-1/domain-table.md), §8.3 (the cache shapes, `cache_lazy`); and the step 0 [sketch](../../../wo/temp-view-frame-reader-parse-1/sketch.md), §9 item 4 (Q8).

**Status:** filed 2026-11-19, not dispatched. Its step 0 runs after unit 1 merges, because unit 1 changes when a held frame parses. For the owner's morning read, as Fable's guidance.

**Severity: S1 by class, label-blocking** (Fable's guidance under the owner's delegation of 2026-11-19, about 01:28 UTC; Fable's Q8 ruling (ii), widened to the materialisation-timing family).

## Finding

The parse follows the zone in force where the cache materialises. At `count()` and `CACHE TABLE` both engines use the creating zone. For a lazy `.cache()` with no action, RePark parses at the view's registration, while Spark parses at the first action, under the reading zone.

- Domain table §8.3, `cache_lazy` (`json_s`, `csv_i` on main, 4 cells): "4 WRONG in `ts`, `u`, `b` (main and part 1)"; `pq` (2) EQUAL; `emu_r` EQUAL; `emu_c` WRONG in `ts`, `u`, `b`.
- Fable's Q8 row, [M]: "`cache_lazy` 4 of 4 WRONG in `ts`." [R]: "The `udf` door above may be the same family. Its step 0 runs after unit 1 merges, because unit 1 changes when a held frame parses."
- Fable §1.4, the `udf` door, [R]: a Python UDF column, `SET`, `collect()`: `json_s` 2 of 2 WRONG in `ts`, `b`, `u`; `pq` 2 EQUAL. "If the UDF path materialises when the column is built, it stays wrong and belongs with `cache_lazy` (Q8)."
- Held EQUAL in unit 1's bar (sketch §6): `cache_mat` and `cachetable`, 8 cells.

**Main:** `cache_lazy` answers WRONG in `ts`, `u` and `b`, on main and on part 1, for `json_s` and `csv_i` (4 cells); `pq` is EQUAL.

**Dated 2026-11-19: the `udf` cells.** The four `udf` cells join this card's step 0 (Fable's reply of 2026-11-19, §6.4; ruling 2 in [rulings-2026-11-19.md](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-19.md)). Unit 1's hand-back, held outside the repository (`rp-unit1-out/ready.md`), measured them: `json_s` and `csv_i` 4 WRONG in `t`, `b`, `u` on base and head. The frame is planned when the UDF column is built, under the creating settings; `pq` is EQUAL. Fable confirms the cells belong to this card, with no new card, and the rating follows this card.

Up: [map.md](map.md).
