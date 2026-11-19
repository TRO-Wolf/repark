# task/wo/temp-view-frame-reader-parse-1/evidence/probes/ — the cell definitions and the analysis

Comment-free Python, run by the commands in [../map.md](../map.md) (Provenance).

- [common.py](common.py) — the session builder (`spark` or RePark by package on `PYTHONPATH`; prints
  `repark._native.__file__`), the record writer and the error condition parser. The Iceberg jar comes from
  `ICEBERG_JAR`.
- [rp.py](rp.py) — the cells. `rp.py <engine> <warehouse parent> <out> <part>`; parts `Z0`–`Z7` (zone: the four
  cross-setting passes, then the four matching passes), `A` (ANSI, malformed and overflowing values), `T`
  (`spark.sql.timestampType`), `S` (the SQL file-source doors), `W0`–`W3` (writes into Iceberg), `E` (`EXPLAIN`).
  Extends part 2's reader sweep (`readers.py`, not copied): more readers, five bodies, the join and SQL doors with
  bodies, the matching passes, two emulation readers (`emu`, `csv_fix`).
- [rv.py](rv.py) — the critic's round: `rv.py <engine> <warehouse parent> <out> <part>`, parts `X` (the
  zone-reading `filter`, `unionByName` with Parquet, a double registration, the exact-key join, `crossJoin`, a SQL
  join view, held frames), `XC` (the cache shapes), `N` (three zones nested), `D` (D9–D11), `E` (`EXPLAIN` spellings
  and `sameSemantics`); data crossing midnight both ways; emulation readers `emu_r`, `emu_c`, `emu_live`, `emu_mid`.
- [rvcmp.py](rvcmp.py) — pairs the round's cells with Spark's per column, the emulation readers with Spark's
  `json_s` twin, and the midnight recount with step 0's verdicts; writes `verdicts-r1.jsonl` and `summary-r1.txt`.
- `rp.py` takes an optional fifth argument `midnight` (default `base`, step 0's data); `common.py` now also records
  `names` and per-column `cols` beside `rows`.
- [cmp.py](cmp.py) — pairs each cell with Spark's, gives `EQUAL` / `WRONG` / `RAISE` for main and part 1, the move
  between them, the gap class (`view`: the matching twin is `EQUAL`; `direct`: it is not) and the emulation readers
  against their Spark twins; writes `verdicts.jsonl` and `summary.txt`.

Up: [../map.md](../map.md).
