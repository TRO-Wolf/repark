# task/wo/csv-schema-timestamp-zone-1/evidence/probes/ — comment-free Python

- `common.py`, `rp.py` — step 0's cell definitions, unchanged (PR #1364): `rp.py <engine> <wh> <out> <part>`.
- `cmp_h.py`, `tables_h.py` — step 0's `cmp.py`/`tables.py` with the second build named `head` and gained/lost read
  from main to the head; `bar.py <verdicts> <out>` prints the bar's counts, every moved cell and every lost EQUAL.
- `x.py <engine> <wh> <out>` — the shapes outside step 0's table and two `EXPLAIN`s; `xcmp.py <raw dir> <build>…`.
- `emu_ab.py` — the seat choice: main's schema read, an all-text read (`utf8_columns`) and a recast through a string,
  on step 0's files, with rough timings; `q1.py` — main casts a zone-less column to `TIMESTAMP` without the zone;
  `q2.py` — the DDL types `TIMESTAMP` as LTZ under a `TIMESTAMP_NTZ` session.
- `readers.py` — part 2's reader sweep, made Ruff-clean (annotations, `pathlib`, its setup-error line written with
  `json.dumps`); the cells are unchanged, and its `common.py` takes the runtime jar path.
- `lmt.py <engine> <dir> write|read` — the LMT offset-seconds CSV written by RePark under New York, read back with a
  `TIMESTAMP` and a `STRING` schema in two zones.
- `csv_cost.py <case> <label> <data> <out>` — K0 (`k0_count`, `k0_hour`) and K2 over a two-million-row CSV, a warm-up
  then five timed repetitions in one process.
- `delta1.py <engine> <wh> <out>` — delta 1's cells (Spark live): the verifier's F1 and F2 shapes, its N1 shapes, a
  `STRING`/`DATE` field beside a `TIMESTAMP` field, and the `inferSchema` re-read's shapes on a local and a `file://`
  path (a column empty for its first 1,100 rows, header names with spaces or empty, a `comment` line, `multiLine`).
- `delta2_ab.py <label> <data> <out> named|alltext|twin` — delta 2's one-process A/B: K2, `k0_hour`, `k0_count`
  and the read, each also planned once; `alltext` forces the `inferSchema` pre-scan on the re-read, `twin` builds K2
  over an all-text read and the facade's cast.

Up: [../map.md](../map.md).
