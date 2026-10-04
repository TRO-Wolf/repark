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
