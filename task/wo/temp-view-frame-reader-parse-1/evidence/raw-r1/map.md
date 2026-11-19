# task/wo/temp-view-frame-reader-parse-1/evidence/raw-r1/ — the critic's round: raw cells, verdicts, the critic's verdict

Measured 2026-11-19, 03:09–03:18 UTC, as in [../map.md](../map.md) (Provenance, round 1). Scratch paths read
`<scratchpad>`; nothing else is edited.

- `r-<engine>-<part>.jsonl` — `probes/rv.py` cells; `engine` is `spark`, `main` or `part1`; `part` is `X` (the new
  doors), `XC` (the cache shapes), `N` (three-zone nesting), `D` (D9–D11), `E` (`EXPLAIN` spellings, tags `E/…`, and
  `sameSemantics`, tags `SS/…`). A row holds `names` and `cols` (each value as text, rows sorted), or `error`, or
  `create_error`. Tags read `<set>/<made>_<read>/<reader>/<door>`, and `N/<A>_<B>_<C>/<reader>/<shape>`.
- `m-<engine>-Z<i>.jsonl` — `probes/rp.py <engine> … Z<i> midnight`: step 0's eight zone passes on the midnight data,
  tags as in [../raw/map.md](../raw/map.md), with `names` and `cols` added beside `rows`.
- [verdicts-r1.jsonl](verdicts-r1.jsonl) — one row per cell: `v_main`, `v_part1`, the WRONG columns (`wrong_main`,
  `wrong_part1`), `moved`, the emulation readers' verdict against Spark's `json_s` twin (`twin`, `twin_wrong`) and,
  for the midnight set, the gap class beside step 0's (`gap`, `base_gap`). Written by `probes/rvcmp.py`.
- [summary-r1.txt](summary-r1.txt) — the counts `probes/rvcmp.py` prints, quoted in
  [../../domain-table.md](../../domain-table.md) §8.
- [critic-verdict.md](critic-verdict.md) — the critic's verdict on step 0 `13b6d45c` (Opus 5.5 medium, 2026-11-19,
  `VERDICT: GAPS`), copied verbatim; its `N/…` paths name the orchestrator's scratchpad.

Up: [../map.md](../map.md).
