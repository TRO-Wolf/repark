# task/wo/temp-view-session-settings-1/evidence/probes/ — the step 0 probes (comment-free Python)

Run from this directory; `cells` and `lib` are imported by the runners. Commands and provenance:
[../map.md](../map.md).

- `cells.py` — the fixtures, the bodies per key family (17 ANSI, 10 zone, 3 case, 2 parser, 2 decimal, 2
  timestamp-type, 1 namespace), the doors and their order, the reader positions, the families with their key, values,
  catalog bodies and write types, the nested, reader-side, `now` and unset cells.
- `lib.py` — the session builder for both engines (hadoop catalog `lc`, in-memory catalog `mc`, `UTC`, the ANSI flag
  or none), the answer normaliser, the error condition extractor and the JSONL log.
- `run.py` — one engine and start mode: fixtures, then per family create every door under the first value, read under
  it, `SET` the second, rename, read every door and position, run the direct queries and the writes, create under the
  second value and repeat in reverse; then the nested, reader-side and `now` cells. `unset` runs the unset case only;
  a sixth argument restricts the bodies (`avg` on #1355's package).
- `explain.py` — the plans of a SQL view, a DataFrame view and a held frame read after the `SET`, both engines.
- `run_cs.py` — the twin-column case probe: where Spark refuses a relation with `a` and `A` under
  `caseSensitive=false`.
- `compact.py` — copies the raw outputs into [../raw/](../raw/map.md) without timings, scratch and home paths
  replaced by placeholders.
- `analyze.py` — pairs each RePark cell with Spark's, classifies it (`EQUAL`, `VALUE`, `TYPE`, `REPARK_ANSWERS`,
  `REPARK_RAISES`, `BOTH_RAISE`, `BOTH_RAISE_OTHER`, `CREATE_DIFF`), marks whether the two direct queries agree and
  which setting each engine followed, and writes `verdicts.jsonl`, `summary.md` and `headline.md`.

Up: [../map.md](../map.md).
