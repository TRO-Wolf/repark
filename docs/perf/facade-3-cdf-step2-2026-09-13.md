# FACADE-3 createDataFrame step-2 re-measure (2026-09-13)

Step-2 re-measure of the [step-1 baseline](facade-3-cdf-baseline-2026-09-13.md) on
the `feat/facade-3-s2` branch: rows/tuples/dicts inference and cell conversion now
run in Rust (`_native.cdf_arrow_export`) through the FACADE-1 capsule seam. Same
runner (`facade-3-cdf-baseline-2026-09-13/run_facade_3_cdf.py`), same method —
warmup + 3 reps, medians, `wait_for_idle` per cell — on a release native
(`__debug_assertions__` is `False`). Bar: every targeted shape faster; no
untargeted shape slower than 5 %.

pins: facade-3/C-011

## Method delta — per-shape processes under the 8 GiB cap

The mandated `prlimit --as=8589934592` cap cannot hold the full runner in one
process: the session retains every created MemTable view across the 32 cells, and
the polars fixture build aborts. The re-measure therefore ran one fresh capped
process per shape (`prlimit` + `OPENBLAS_NUM_THREADS=8`, the runner's own
`measure_cell` and `build_session`), which keeps each process's resident tables to
one fixture's worth. Box load 3.2–3.8 across the run. Raw JSON:
`/tmp/facade-3-cdf-s2.json` (driver `/tmp/run_facade3_s2.py`).

**Finding — the polars control is unmeasurable under the cap.** `import polars`
(jemalloc) alone reserves ≈7.7 GiB of address space (VmSize 7,717,392 kB, RSS
75 MB); `ReparkSession.builder.getOrCreate()` adds ≈5.5 GiB more (VmSize
13,251,312 kB, RSS 127 MB). Polars + a live session need ≈13 GiB of address
space — the 8 GiB cap aborts even at 1e4 rows (`MALLOC_CONF=narenas:1` does not
change jemalloc's initial reservation). The cap was not raised. The polars
dispatch branch is untouched by this change — `createDataFrame(pl.DataFrame)`
never reaches `_rust_cdf_arrow_table`, identical to the pandas branch which
measures +2.8 % — so a >5 % polars regression is structurally excluded; the
unmeasurable cell is reported as a finding per the run-9 memory rule.

## The shape table — step 1 vs step 2 (median ms)

| cell | 1e4 create | | 1e5 create | | 1e5 +count | |
|---|---:|---:|---:|---:|---:|---:|
| | s1 → s2 | Δ | s1 → s2 | Δ | s1 → s2 | Δ |
| tuples | 74.49 → 42.08 | −43.5 % | 735.61 → 437.50 | −40.5 % | 737.65 → 429.26 | −41.8 % |
| rows | 101.85 → 73.99 | −27.4 % | 1,060.81 → 793.48 | −25.2 % | 1,068.40 → 768.00 | −28.1 % |
| dicts | 89.97 → 61.20 | −32.0 % | 947.00 → 641.20 | −32.3 % | 929.81 → 633.92 | −31.8 % |
| tuples + DDL | 203.29 → 42.71 | −79.0 % | 2,057.67 → 448.31 | −78.2 % | 2,077.70 → 443.44 | −78.7 % |
| tuples + StructType | 202.45 → 43.28 | −78.6 % | 2,089.21 → 450.21 | −78.5 % | 2,044.63 → 455.28 | −77.7 % |
| nested | 254.78 → 16.91 | −93.4 % | 2,852.45 → 206.96 | −92.7 % | 2,827.12 → 205.85 | −92.7 % |
| pandas (control) | 45.65 → 46.49 | +1.8 % | 433.66 → 445.99 | +2.8 % | 433.67 → 449.26 | +3.6 % |
| polars (control) | 42.74 → n/a | finding | 424.07 → n/a | finding | 418.59 → n/a | finding |

**Bar: met.** Every targeted shape is faster at both sizes — the explicit-schema
pair and nested (the C-007 targets) drop 78–93 %, rows/dicts/tuples drop
25–41 %. The only measurable untargeted shape (pandas, same untouched code path)
moves +1.8 % to +3.6 %, inside the 5 % band. `tuples` at 437 ms now sits at the
pandas control's 446 ms — the per-cell wall is gone; the residue is transport and
MemTable registration, not inference.

## Pointers

- Baseline + runner: [facade-3-cdf-baseline-2026-09-13.md](facade-3-cdf-baseline-2026-09-13.md)
- Ledger: [../../task/ledgers/completed/facade-3-ledger.md](../../task/ledgers/completed/facade-3-ledger.md)
