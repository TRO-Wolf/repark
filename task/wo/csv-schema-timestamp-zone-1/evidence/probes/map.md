# task/wo/csv-schema-timestamp-zone-1/evidence/probes/ — comment-free Python

- `common.py`, `rp.py` — step 0's cell definitions, unchanged (PR #1364): `rp.py <engine> <wh> <out> <part>`.
- `cmp_h.py`, `tables_h.py` — step 0's `cmp.py`/`tables.py` with the second build named `head` and gained/lost read
  from main to the head; `bar.py <verdicts> <out>` prints the bar's counts, every moved cell and every lost EQUAL.
- `x.py <engine> <wh> <out>` — the shapes outside step 0's table and two `EXPLAIN`s; `xcmp.py <raw dir> <build>…`.
- `emu_ab.py` — the seat choice: main's schema read, an all-text read (`utf8_columns`) and a recast through a string,
  on step 0's files, with rough timings; `q1.py` — main casts a zone-less column to `TIMESTAMP` without the zone;
  `q2.py` — the DDL types `TIMESTAMP` as LTZ under a `TIMESTAMP_NTZ` session.
- `readers.py` — part 2's reader sweep, unchanged (its `common.py` takes the runtime jar path).
- `csv_cost.py <case> <label> <data> <out>` — K0 (`k0_count`, `k0_hour`) and K2 over a two-million-row CSV, a warm-up
  then five timed repetitions in one process.

Up: [../map.md](../map.md).
