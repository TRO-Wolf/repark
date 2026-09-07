# Eager-preview baseline (DFCORE-6, 2026-09-07)

`repr`, `_repr_html_`, and vertical `show` on a full preview ran a full
`count()` only to decide the "only showing top N rows" footer; on
`mapInArrow`-backed frames the eager doors ran the whole UDF twice (once for
the rows, once for the count). DFCORE-6 fetches one row past the cap, renders
the cap, and reads the footer from the extra row — no `count()` — and peeks
bridged frames with the same bound. Rendered output is byte-identical (every
DFCORE-4b golden passes unchanged); only the scan count changes.

Machine/profile: this box (64 threads, 125 GB RAM, shared lane box), release
module (`__debug_assertions__` False), one session per run, 1-minute load
recorded beside each run. Frame: a 1e6-row single-column parquet (`id` int32,
4,571,825 bytes), plus the same frame behind a doubling `mapInArrow` bridge.
Eager eval on at the default cap (20). Each wall cell is the median of five
runs.

| frame | door | before counts | after counts | before median | after median |
|---|---|---|---|---|---|
| parquet | repr | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| parquet | HTML | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| parquet | vertical show | 1 | 0 | 0.004 s (load 6.6) | 0.002 s (load 5.5) |
| mapInArrow | repr | 1 + 2,000,000 UDF rows | 0 + 65,536 UDF rows | 0.821 s (load 6.5) | 0.031 s (load 5.5) |
| mapInArrow | HTML | 1 + 2,000,000 UDF rows | 0 + 65,536 UDF rows | 0.845 s (load 6.2) | 0.031 s (load 5.5) |
| mapInArrow | vertical show | 0 + 65,536 UDF rows | 0 + 65,536 UDF rows | 0.031 s (load 6.2) | 0.032 s (load 5.5) |

The before and after runs carry different loads (6.2–6.6 vs 5.5), so a wall
ratio reads against its own run, never as a cross-run constant; the counts
are exact and carry the committed pins. Wall is recorded, not gated.

Two readings of the UDF-row cells. Computed rows are batch-granular: the bench
UDF yields whole engine batches (65,536 rows here), so one batch is the
smallest unit the peek can pull — the bridge computes one batch and the
preview keeps 21 rows. The committed pin uses a row-at-a-time bridge and
proves the peek stops it after `maxNumRows + 1` yielded rows. The `mapInArrow`
vertical-show row is unchanged by design: that door already peeked, so it is
the control the after column must equal — and does.

Reproduce (from the repo root, release module):

```
.venv/bin/python -m pytest python/repark/tests/test_dfcore_6_eager_preview.py::test_preview_doors_never_count python/repark/tests/test_dfcore_6_eager_preview.py::test_mapinarrow_preview_bounds_udf_rows -q
```

The pins re-derive the counts mechanically. The wall medians come from a
throwaway bench script (five fresh-session runs per cell over the frame
above); they stand recorded.
