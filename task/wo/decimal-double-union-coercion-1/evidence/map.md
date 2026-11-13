# task/wo/decimal-double-union-coercion-1/evidence/ — probes, raw cells, provenance

- [probes/](probes/map.md) — the cell definitions and the runners (comment-free Python).
- [raw/](raw/map.md) — one JSON object per cell and engine, as the runners wrote them.

## Provenance

- **RePark:** the package built from main `04d84ab9` (scratchpad `pkg-04d84ab9`, read only), run by the
  lane interpreter with that directory on `PYTHONPATH`; every raw file's `provenance` row names the
  loaded `repark/_native.abi3.so` relative to the scratchpad. Version `1.5.6`.
- **Spark:** PySpark 4.1.2 (the c26 oracle environment), Zulu JDK 17, `local[1]`, UI off, session zone
  UTC, shuffle partitions 1; writes and reads with `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, a private
  hadoop catalog and warehouse per run (`wh-<engine>-<ansi>-*`, deleted with the scratchpad).
- **When:** 2026-11-13, about 07:20–07:45 local time, while two builder lanes held cores 32–57; every probe
  ran under the shared measure lock on cores 48–57 and every Spark JVM under the Spark lock, one at a time.
- **Commands** (from `probes/`, `<raw>` = `../raw`): `run_select.py <spark|repark|native> <true|false>
  <out>`; `run_frames.py <spark|repark> <true|false> <out>`; `run_write.py <spark|repark> <true|false>
  <warehouse-parent> <out> <iceberg-jar>`, then `read_back.py <warehouse> <write.jsonl> <out>
  <iceberg-jar>` on Spark for both engines' warehouses; `replay_888.py <#888 spark json> <out>`;
  `rule_lists.py <out>`; `run_revision.py <spark|repark> <true|false> <warehouse-parent> <out>
  <iceberg-jar>`, then `read_back.py` over each revision warehouse; `revision_table.py <raw>`; `analyze.py <raw> <dir>` writes the verdicts; `render.py <raw>` and
  `write_table.py <raw>` print the tables in [../domain-table.md](../domain-table.md).
- **#888's recording:** `python/repark/tests/test_nvl_type_coercion_1_spark.json` from
  `wip/fix/nvl-type-coercion-1-2026-10-01` (head `54bc9ac2`), read from the branch, not copied here.

## The revision after the inventory critic

The xhigh inventory critic (2026-11-13, `VERDICT: GAPS`, ANSI on only) found five missed write doors,
two wrong "seen" answers (D2, D3) and the `VALUES` limit of an analyzer-only seat. Its cells were
re-measured here in both ANSI modes with this unit's own probes, about 08:20 local time, under the same
locks; its own probes and raw output stay in the scratchpad and are not copied.

## Main since `04d84ab9`

`git log 04d84ab9..origin/main` (head `16974986`) holds 17 commits. The ones touching a seat this sketch
inventories, and what they can move:

- **#1314** (SQL-SETOP-ALL-REFUSE-1): SQL `INTERSECT ALL`, `EXCEPT ALL`, `MINUS ALL` refuse at the
  statement seat (`pre_execute/setop_all.rs`). The ALL forms are not in this table; on that head they
  refuse by source. The DISTINCT forms measured here are untouched. The DataFrame `intersectAll` /
  `exceptAll` refusals (`R` in the DataFrame cells) predate it.
- **#1316, #1321** (F.expr and string-filter NULL ordering): `expr_build.rs` and `extension.rs`, the
  `F.expr` context (inventory C). They change `ORDER BY` NULL placement only; no type decision, so no
  cell here can move.
- **#1304** (LISTVIEW-INNER-JOIN-KEY-1): key-name joins on non-hashable keys (`df_guards/join_keys.rs`).
  The `extra/join_on_mixed` cell joins hashable scalar keys with `ON`, so it cannot move.
- **#1303** (CTAS-QUERY-FAILURE-CLEANUP-1) changes the failure path of CTAS; `ctas_union_tiny` succeeds
  and is unaffected. **#1317** (NATIVE-CTAS-ICEBERG-REFUSE-1) is native-door CTAS only; no native write was
  measured. **#1322** is tests only.

Up: [../map.md](../map.md).
