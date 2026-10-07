# Card GROWN-STACK-GATE-1 — unconditional stack growth at 34 native sites slows main 5.2 %

**Date:** 2026-10-04 · **Filed by:** Grok 4.7, guided execution, clerk grade · **Source:** owner,
2026-10-04: "Card GROWN-STACK-GATE-1, grade B, Rust, Muse executor at max with a scoped Opus medium
verifier." No product code in this filing. Order: [`task/wo/grown-stack-gate-1.md`](../../wo/grown-stack-gate-1.md).

## Why

**What was measured:**
- **The like set got slower.** PERF-ATTR-STAMP-2's three-run re-baseline (2026-10-04, dev builds, `replay_work.py`) puts main's work-equal like set (25,999 cells) at **299.14 s on v1.5.1** (`db3a1f37`) and **314.69 s on v1.5.2** main (`b7f507b9`): **+15.56 s, +5.2 %**.
- **No answers changed:** v1.5.2 main has 0 outcome-class moves against v1.5.1 on the 43,946-cell replay.
- **The stack ratio moved because of it:** the attribute-identity stack's ratio fell from 1.1244 to **1.0902** largely because main slowed.

**Owner, 2026-10-04:** "the gate is met as written, but hold the stack-to-main merge until the baseline is repaired and the ratio is re-measured."

**Mechanism** (DEEP-FILTER-CHAIN-CRASH-1, `task/ledgers/staging/deep-filter-chain-crash-1-ledger.md`, "Root cause"):
- `deep_stack::block_on` passes red zone = segment (128 MiB) to `on_grown_stack_with`, so `stacker` grows on every poll.
- Each grown region mmaps a fresh segment and re-faults its touched pages, about 100 µs per region.

**What R4 fixed and what it left:** R4 made growth conditional at the frame doors (`frame_drive_segment_cached` with `block_on_grown_sized`; `sql_drive_grown` for SQL text). The plain unconditional `deep_stack::block_on` stays at **34 product call sites** in `crates/repark-python/src` on main `22cce0eb`. That count is measured; the owner's list of files adds up to the same 34 (the ruling's text says "39"). The sites:

| file | sites | functions |
|---|---|---|
| `session.rs` | 17 | `finish_session`, `read_parquet`, `read_csv`, `read_json`, `declare_temp_view_sorted`, `materialize_as_temp_view`, `materialize_as_cache_view`, `table_exists`, `register_memory_catalog`, `list_iceberg_table_names`, `refresh_catalog_provider`, `testing_oob_create_table`, `testing_oob_drop_table`, `testing_create_ref`, `testing_list_snapshots`, `register_late_catalogs`, `create_namespace` |
| `text_io.rs` | 3 | `read_text`, `write_text_frame`, `write_text_partitioned` |
| `session_sources.rs` | 3 | `read_iceberg_incremental`, `read_iceberg_path`, `read_iceberg_table_pinned` |
| `session_write_options.rs` | 2 | `session_sql_with_write_options`, `session_write_path` |
| `ml.rs` | 2 | `open_stream`, `for_each_batch` |
| `writer_layout.rs`, `session_runtime.rs`, `plan_introspect.rs`, `orc_io.rs`, `dataframe_stats.rs`, `catalog_census.rs`, `cache_budget.rs` | 1 each | `writer_plan`, `register_late_catalog_block`, `input_files`, `read_orc`, `transpose`, `iceberg_metadata_cache_census`, `retained_cache_bytes` |

A replay cell typically hits several of these sites (a read, a temp view, a write), and each hit pays a fresh grown segment.

## Fix (owner, 2026-10-04; the order rules the details)

**Gate the 34 sites the way R4 gated the frame doors:**
- **SQL text:** `sql_drive_grown`.
- **Frames:** the plan-depth verdict (`frame_drive_segment_cached` on `plan_depths`).
- **Catalog and option calls with no plan:** no growth.

The owner's alternative, "grow once per native call instead of per poll", is not taken. It would still pay one grown region per call at the 23 no-plan sites.

**Segment size is not the knob.**

## Gates (owner-set)

1. **The unit's deep pins stay green.** C-011 (`test_deep_filter_chain_crash_1.py`) and C-012 (`test_deep_expr_build_small_stack_1.py`), plus every DEEP-FILTER-CHAIN-CRASH-1 pin the ledger lists.
2. **The ledger's four micro shapes** (select1 ×1000, filter_count ×1000, iceberg count ×200, import plus session) are back **within 1.005 of v1.5.1** (`db3a1f37`). Use release wheels, 5 fresh processes per side, medians.
   - The ledger records about 2 % run-to-run median noise. So the order measures an A/A spread first and halts with the numbers if that noise alone exceeds 0.005.
3. **The like set:** the work-equal like set on the repaired main is **within noise of v1.5.1's 299 s.** Use the same driver and dev builds, three runs, with the spread reported.
4. **Comment ban.** No comments in Rust.

## After the unit

The PERF-ATTR-STAMP-2 three-run median is re-run against the repaired main, and the stack merges to main when that median is ≤ 1.10.

The stack branch carries main's 34 sites too, through the #932 merge. So the repaired main is merged into the stack before that re-measure, and both sides pay the same cost.

## MAIN-SLOWDOWN-BISECT-1: dev-build findings, closed (2026-10-06)

**What it was.** The bisect unit was opened because GROWN-STACK-GATE-1 recovered only about 0.7 of the roughly 4.1 pt v1.5.1 → v1.5.2 slowdown on the work-equal like set. Its acceptance was declared before any timing:
- **A1:** the gap G, re-measured in the same quiet-box session as the profile;
- **A2:** a per-commit profile;
- **A3:** each candidate step confirmed by a three-run interleaved median against its first parent;
- **Done** when the confirmed slowing steps sum to at least 75 % of G.

All runs used debug builds, `replay_work.py`, and the same work-equal like set (43,843 cells between adjacent main commits).

- **A1.** G = **4.65 pt** (main `9f41347f` against v1.5.1, median 1.0465, spread 0.89 pt).
- **A2, the profile steps (one run each).**

  | commit | step |
  |---|---|
  | `dc613672` text-write timestamp zone | +0.52 |
  | `daf9bbaa` DEEP-FILTER-CHAIN-CRASH-1 | +3.48 |
  | `5d8ee78b` polars-is-duplicated-1 | +1.45 |
  | `cc68833d` TA-CHAIN-1 | −0.71 |
  | `d0c50405` release v1.5.2 | +0.56 |
  | `9f41347f` this unit | −0.21 |

- **A3, confirmed (median of three against the parent; runs and spread).**

  | commit | runs | median | spread |
  |---|---|---|---|
  | `dc613672` | 1.0052 / 1.0073 / 1.0085 | **+0.73 pt** | 0.33 |
  | `daf9bbaa` | 1.0520 / 1.0327 / 1.0382 | **+3.82 pt** | 1.94 |
  | `5d8ee78b` | 1.0421 / 1.0269 / 1.0354 | **+3.54 pt** | 1.51 |
  | `d0c50405` | 1.0022 / 1.0117 / 1.0023 | +0.23 pt, within noise | 0.95 |

  Confirmed slowing steps: +8.32 pt, which is 179 % of G. **Accept: met.** The sum overshoots G for two reasons:
  - the profile also has negative steps (TA-CHAIN-1 at −0.71 and this unit at −0.21);
  - from about 06:35, the `daf9bbaa`, `5d8ee78b` and `d0c50405` pairs ran beside a niced security-fix build on separate cores, which most likely inflates `5d8ee78b` (+3.54 against +1.45 profiled).

  A 20-second CPU-affinity slip at 06:29 touched one `dc613672` run. These are **dev-build findings**: they inform and never gate.

**Resolution (owner, 2026-10-06).** The unit closes with the PERF-ATTR-STAMP-2 release result as its resolution: work-equal like **1.0989 ≤ 1.10** on release builds (see [the gate record](perf-attr-stamp-2-card-2026-10-03.md)). No per-step cards are filed. `daf9bbaa` (the grown-stack poll cost) and `5d8ee78b` remain the two largest dev-build contributors, should main's own speed be taken up later.
