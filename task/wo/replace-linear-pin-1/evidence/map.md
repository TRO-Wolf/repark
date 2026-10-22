# task/wo/replace-linear-pin-1/evidence/ — measurements of REPLACE-LINEAR-PIN-1

Step A was read on the runner (facade shard 1, the shard that runs `test_replace_linear_1.py`) from throwaway draft
pull requests #1144 (main `0512470d`), #1145 (the #1140 head `46ab4255`) and #1146 (the final head); each was closed
and its branch deleted after the read. The child printed `VmHWM`, `VmRSS`, `RssAnon`, `RssFile` and `RssShmem` and the
resident size of the `_native` mapping before and after each mode, and the parent hashed the `.so`. Step B was measured
locally on `maturin develop --profile ci` of the same sources, every run under `taskset -c 0-25` with
`CARGO_BUILD_JOBS=12`, on a box shared with another builder.

- [step-a-table.tsv](step-a-table.tsv) — the eight runner rows (main and the #1140 head, two builds each, both modes):
  the `.so` prefix, bound, pin delta and the before and after of every field.
- [step-a-diag-lines.txt](step-a-diag-lines.txt) — the raw diagnostic lines the eight rows come from, one JSON per
  mode per build.
- [local-with-warmup.tsv](local-with-warmup.tsv) — ten local runs of both modes with the depth-1 warm-up: flat about
  0.79 MB, replace about 0.95 MB, almost all of it `RssFile`.
- [local-without-warmup.tsv](local-without-warmup.tsv) — the mutation check, not committed: the same ten runs with the
  warm-up removed (the test file from main): flat about 4.85 MB, replace about 7.78 MB.
- [pin-file-10-runs.txt](pin-file-10-runs.txt) — `test_replace_linear_1.py` alone, ten times: 11 passed each time.
- [dataframe-dir-n8.txt](dataframe-dir-n8.txt) — `python/repark/tests/dataframe` once under `-n 8`: 1655 passed,
  4 skipped.
- [runner-final-head.tsv](runner-final-head.tsv) — the runner read of the final head with the warm-up (draft #1146).

Up: [../map.md](../map.md).
