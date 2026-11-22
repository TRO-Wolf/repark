# task/wo/ice-held-frame-snapshot-1/built/evidence/unit-2/ — unit 2's probes and recordings

Scratch paths read `S` (the lane scratchpad) and `N` (the session scratchpad); none is in the repository. The probes
ran from a copy of this directory as `<probe>.py <engine> <warehouse parent> <out> <ansi>`, RePark on
`S/lane-nan-write-canonical-1/.venv/bin/python` with the package on `PYTHONPATH` (each log printed
`repark._native.__file__`), Spark 4.1.2 on the oracle interpreter with `JAVA_HOME` and `ICEBERG_JAR` set, through
the Spark slot, under the shared measure lock on cores 48-57. The external writer reads `XWRITER` and
`XWRITER_ENV` (a Spark process, `writer.py`).

- `u2cells.py` — the state table's main probe: the four cached cells (`C`), the 1a verifier's cached cells (`K`),
  ten write verbs (`W`), the external writers with a refresh (`E`), the fill-kept and fill-refilled timestamp cells
  (`T`), unpersist, uncache and `isCached` (`U`), children, views, joins, SQL views and nested caches (`V`), own-plan
  actions (`O`) and schema changes on a cached bare frame (`D`). `ONLY=<groups>` runs a subset.
- `u2ts.py` — three `current_timestamp()` reads around one event (`T3`): kept, refilled, or a miss at every read.
- `u2alter.py` — `ADD`, `DROP`, `RENAME COLUMN` and `SET TBLPROPERTIES` under five cached holds (`A`): a bare
  `spark.table`, two selects, a filter and a timestamp column.
- `common.py`, `writer.py` — 1a's session builder and external writer, unchanged.
- `u2moves.py` — the verdict rule (an answer equal to Spark's is EQUAL, any other is WRONG) and the moves between
  two builds: `u2moves.py <raw dirs, comma-separated> <first label> <second label>`.
- `cells.jsonl.gz` — every answer, one record per cell, engine (`spark`, `main` for `pkg-a1m-bb8ae8fc`, `head` for
  `pkg-iceu2-eac90dd7`) and ANSI mode; `run` is `v3` for the `V` group re-run after the join cells were renamed
  (the first run's join cells raised a duplicate-name schema error on main, a probe artefact). `gzip -9`, no
  timestamps.

Up: [../map.md](../map.md).
