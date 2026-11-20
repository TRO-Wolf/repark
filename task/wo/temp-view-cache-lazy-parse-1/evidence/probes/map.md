# task/wo/temp-view-cache-lazy-parse-1/evidence/probes/ — the cell definitions, the trace, the cost cases, the analysis

Comment-free Python, run by the commands in [../map.md](../map.md) (Provenance).

- [cells.py](cells.py) — the cells. `cells.py <engine> <warehouse parent> <out> <groups> <modes>`; engine `spark` or
  `repark` (the package on `PYTHONPATH`; prints `repark._native.__file__`). Groups: `R` and `Q` (Spark's rule: a lazy
  cache's first and later actions, a first own-plan action before `cache()`, a child built before materialisation,
  `explain` first, another session, a bare reader cache, a twin plan, a cached UDF or map frame, `CACHE LAZY TABLE`
  then SQL); `C` (fourteen cache doors × six actions: `count` then a third zone's `collect`, `collect`, `show`, a
  derived `filter`/`select`, a join with a Parquet frame, a Parquet write read back); `N` (the same actions on a held
  frame with no cache, the control); `U` (a scalar `udf`, a `pandas_udf`, `mapInArrow`, `mapInPandas`, with the
  timestamp shape built on top of the UDF frame; first action after the `SET`, a first action before it, a `count`
  first); `T` (the same four with the UDF as the last node); `L` (the label path: a lazily cached reader frame or a
  UDF frame into an Iceberg v2 table by `writeTo().create()` and by `INSERT INTO … SELECT` over a view, read back).
  Readers `json_s`, `csv_s`, `csv_i`, controls `pq` and `ice` (an Iceberg v2 table); directions `la_utc`
  (create under Los Angeles, read under UTC, third zone Tokyo) and `utc_tk` (UTC, Tokyo, Los Angeles); ANSI on and off.
- [trace.py](trace.py) — RePark only: wraps the facade's cache, registration, own-plan and bridge sites and records the
  live zone at each call, for twelve scenarios (the inventory's "when it plans").
- [cost.py](cost.py) — the cost cases on 2M-row files: a cached JSON read (`count` then `collect`, `collect` first,
  a view then `count`, a selective child's `count` before and after the parent's fill), a UDF column over a CSV read with the shape on top and with the UDF on top.
- [missed.py](missed.py) — the revision round's cells (`M`): `REFRESH TABLE` on a cached view, `clearCache()` after
  a lazy view and after a materialised cache, `CACHE [LAZY] TABLE … AS SELECT`, `unpersist(blocking=True)` and
  `unpersist` re-reads, cached siblings, a cached self-join, frames on a UDF or map frame (siblings; `count`, `show`,
  `take` or a write, then `collect`), a held Iceberg frame after a write to its table (cached, lazy, `CACHE TABLE`,
  uncached), ANSI flips over an unparsable timestamp (plain, cached, `count`), a `CAST(s AS INT)` across an ANSI
  flip. `missed.py <engine> <warehouse parent> <out>`.
- [cmp.py](cmp.py) — pairs each cell with Spark's; `EQUAL` / `WRONG` / `RAISE` for main and for main before unit 1, the
  columns that differ (`u` the parse, `t` and `h` the zone the frame's own expressions read), the moves between the
  two builds; writes `verdicts.jsonl` and `summary.txt`.

Up: [../map.md](../map.md).
