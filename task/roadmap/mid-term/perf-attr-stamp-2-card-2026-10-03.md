# Card PERF-ATTR-STAMP-2 — take attribute-id stamping under 1.10x

**Date:** 2026-10-03 · **Filed by:** ATTR-ID-1 orchestrator, on the owner's ruling (a design
unit, not clerk work; card only, no implementation) · **Source:** owner ruling 2026-10-03; the
PERF-1 hand-back and profiles.

**Release gate (owner, 2026-10-03):** the ATTR-ID-1 stack goes to main at 1.2021x like-for-like as a dated carve-out of the 1.20 bar, and no release tag carries the stack until this card has landed and been measured. If it cannot reach 1.10x, the owner rules on its measured number before any tag.

## Why

**Owner, 2026-10-03, verbatim:** "File a card PERF-ATTR-STAMP-2 for the remaining cost: the
stack was 1.188x before SJ-1a, so the stamping, not the self-join check, carries the
slowdown. Target under 1.10x, scheduled before 1.6. The card is a design unit, not clerk
work."

ATTR-ID-1 gives every column a stable attribute id. The id lives in Arrow field metadata
(`repark.attr`). It is stamped on plan output fields every time a frame is born
(`DataFrame.__init__` calls `_native.stamp_attribute_ids`) and re-read when a Column is
accessed. The SJ slices later added a frame-lineage DAG on top. PERF-1 removed the cheap Python
costs. What remains is the engine running stamped plans. The engine code is the same as on
base, but the plans it runs carry the ids.

**Cumulative like-for-like replay timing against base `db3a1f37` (v1.5.1).** Measured with
`like_for_like_verify.py` over about 30k cells of the S0 replay corpus:

| Point | Ratio | Source |
|---|---|---|
| S4 (before SJ-1a) | 1.188 | orchestrator timing table; PERF-1 brief |
| SJ-1a | 1.186 | same |
| SJ-2 | 1.215 | same |
| SJ-3 | 1.213 | same |
| SJ-4 | 1.210 | same |
| SJ-5 | 1.2286 | same (`sj-5/t-head-3a`) |
| PERF-1, earlier trees | 1.1983 (`t-head-1`), 1.2047 (`t-head-2`) | PERF-1 hand-back |
| PERF-1, final tree, single run | 1.2021 (`t-head-3`: head 187.7 s, base 156.2 s) | PERF-1 hand-back |
| **PERF-1 final median of three (R-PERF1-1, `5dff0f34`)** | **1.2021 / 1.2009 / 1.2026 like-for-like; median 1.2021, spread 0.0017** | runs `t-head-3`, `t-head-4`, `t-head-5`; owner carve-out 2026-10-03 |

**The cost is per plan, not per row.** The family table at SJ-4 is from the PERF-1 brief. The
per-cell columns are derived from it.

| Family | Cells | Head | Base | Ratio | Overhead per cell | Base per cell |
|---|---|---|---|---|---|---|
| `r5p6` | 14038 | 98.3 s | 79.3 s | 1.241 | 1.35 ms | 5.65 ms |
| `r5p6t` | 4474 | 51.9 s | 44.7 s | 1.162 | 1.61 ms | 9.99 ms |
| `r5p7` | 2306 | 14.4 s | 11.8 s | 1.220 | 1.13 ms | 5.12 ms |
| `r3` | 2802 | 11.3 s | 9.2 s | 1.226 | 0.75 ms | 3.28 ms |

`r5p6` alone is 19 s of the 35 s gap, mostly in cells with no join. Each cell pays a fixed
overhead of roughly 0.75 to 1.6 ms. The ratio is worst where the base cell is cheapest.

**What reaching the target means.** The figures use `t-head-3` until the median is filled in.
1.10 × 156.2 s gives a bar of 171.8 s. Head is at 187.7 s, so the unit must cut at least 15.9 s.
Spread over the 29169 judged cells, that is about **0.54 ms per cell, or half of the remaining
1.08 ms/cell gap**.

### Measured: where the remaining per-cell cost goes

The profiles are cProfile runs on 500 `r5p6` cells, chosen with seed 42 (`pick500.py`,
`cells500.json`), all in one process. The table compares `tottime` per function, head minus
base (`prof-base.pstats`), divided by 500.
`fixC.pstats` is the last saved head profile. It is taken after PERF-1 fixes A to C and before
D to F. cProfile inflates Python and per-call crossing costs but not native internals:
`fixCprof` ran 3.4343 s against 3.2569 s for the unprofiled `fixC` on the same cells (+5.4 %).
The native share is therefore, if anything, larger than shown.

| Bucket | Site (`file:function`) | Head-only ms/cell (`fixC`) | `prof-head` (pre-PERF-1) |
|---|---|---|---|
| **Engine, Arrow export** | `repark._native.PyDataFrame:__arrow_c_stream__` | **0.852** | 0.893 |
| **Engine, SQL** | `repark._native.PyReparkSession:sql` | **0.322** | 0.332 |
| **Engine, count** | `repark._native.PyDataFrame:count` | **0.148** | 0.165 |
| Engine, schema read | `PyDataFrame:logical_schema_fields` | 0.076 | 0.085 |
| Engine, builders | `PyDataFrame:select` 0.048, `join_on_names` 0.015, `filter_sql` 0.012, `session_sql_with_write_options` 0.008 | 0.083 | 0.100 |
| *Engine subtotal (same code as base, stamped plans)* | | *1.48* | *1.58* |
| New native, stamping | `_native.stamp_attribute_ids` (1784 calls, 3.6 per cell, about 27 µs each) | 0.099 | 0.100 |
| New native, id/name reads | `attribute_ids` 0.029 (1349 calls; 5817 before PERF-1), `frame_derived` 0.029, `logical_column_names` +0.028, `frame_root` 0.006 | 0.092 | 0.224 |
| New native, binding/refusal | `bind_qualified_free_refs` 0.049, `refuse_ambiguous_free_names` 0.021, `session_case_sensitive` 0.017 (1766 calls), `requalify_join_sides` 0.017, `bind_free_names` 0.012, `refuse_self_join_refs` 0.009, `prepare_join_condition` 0.008, `join_output_sources` 0.006 | 0.139 | ≥ 0.135 (`join_output_sources` below the top 40) |
| Python, frame-node weakrefs | `weakref.py:remove/__setitem__/update/get/__init__` | 0.103 | ≥ 0.028 (`__init__` + `update` only in the top 40) |
| Python, binding | `column_fields.py:_bind_resolved_name` 0.030, `unemitted_ids.py:_refuse_self_join_refs` 0.024, `frame_nodes.py:_resolve` 0.024, `filter_quote.py:_unqualified_candidates` 0.016, `core.py:_join_on_condition_h1` 0.015, `column_fields.py:_strip_attribute_id_metadata` 0.014, `qualified_names.py:_frame_id_snapshot` 0.011, `column_fields.py:_column_of` 0.011, `core.py:select` 0.010, `qualified_names.py:_rewrap_rebound_column` 0.009 | 0.164 | n/a (different set) |

Total head-only `tottime`: `prof-head` 2.40 ms/cell ((8.794 − 7.593) s / 500) and `fixC`
2.17 ms/cell ((8.679 − 7.593) s / 500). The buckets above cover 2.07 ms of the `fixC` figure.
The rest is a one-off `importlib` cost (0.085) and harness generator noise.

**Reading.** About 70 % of the profiled head-only cost (1.48 of 2.07 ms/cell) sits inside
native engine methods whose code is unchanged. PERF-1 recorded this in its hand-back: "base
welds: dataframe.rs, session.rs, arrow_export.rs, analyzer rules all identical base-vs-head;
native blobs are stamped-data costs, not code costs". Its closing summary says "the remaining
~1.0ms/cell gap profiles as 100pct native stamped-data costs (arrow export 0.75 + sql 0.28)".
No saved pstats backs that final-tree split; `fixC.pstats` is the last one on disk.

The stamped plan differs from base in three ways, all in
`crates/repark-core/src/session/df_guards/attr_id.rs`:

- **Metadata on fields.** Every output field carries a `repark.attr` metadata map, which the
  analyzer clones and compares on every schema rebuild.
- **Wrapper projections.** `stamp` → `project_ids` adds a Projection whose aliases carry the ids
  over any relation with an unstamped or computed output (scans, values, aggregates, windows,
  unions).
- **An extra optimizer rule.** `StripAttributeIds` is the first rule in the list
  (`df_guards.rs:unnest_safe_optimizer_rules`). It runs `transform_up_with_subqueries` with
  `recompute_schema` on keyed nodes, and on every optimizer pass it walks the expressions of
  unkeyed nodes.

The export path also strips `repark.attr` from the Arrow result in Python
(`core.py:_apply_export_display_names` → `column_fields.py:_strip_attribute_id_metadata`), so
some ids reach the exported batches.

**cProfile cannot split the 1.48 ms engine share** across the analyzer, the strip rule, the
wrapper projections and export. `perf record` was blocked: the hand-back records
`perf_event_paranoid=4`, no root, and `perf-head.data` is 0 bytes. Step 0 closes that gap.

### Already done by PERF-1, not to be repeated

PERF-1 changed only Python; no Rust changed (hand-back: "Python-only slice, no rebuild"). The
timings below are unprofiled sums of `pr881.times.json` over the same 500 cells. Fixes are
mapped to commits by order.

| Fix | Commit (`/tmp/xsj2`) | What | 500-cell sum |
|---|---|---|---|
| (pre) | `7b0e8178` (SJ-5) | none | 3.3565 s (`preA`) |
| A | `a3b70a2c` | cache stamped ids and engine names per frame (`column_fields.py`, `qualified_names.py`) | 3.2882 s |
| B | `e5302c08` | memoize written-name splits, gate star expansion | 3.2844 s |
| C | `028e7dba` | lazy frame nodes, built on first read (`frame_nodes.py`) | 3.2569 s |
| D | `2c6c0d82` | `renews` guard walks deferred markers without building | 3.1769 s (rerun 3.2067 s) |
| E | `36ca23d5` | deferred node entries snapshot the spawn-time plan (correctness fix for the `_inner` replacement; it gave time back) | 3.2297 s |
| F | `5dff0f34` | funnel metadata readers through snapshot caches | 3.2135 s |

Net: −0.143 s per 500 cells, about 0.29 ms/cell, on the subset. PERF-1 also rejected a cache of
the session case config because of the silent-stale risk. Do not propose it again without a
new invalidation argument.

## Design options (for the design sketch to rule on)

Each estimate is labelled *measured* (read from a profile or timing file) or *inferred*
(reasoned from the profile, not run). Every bound in ms/cell is the profiled `fixC` figure.
The target needs about **0.54 ms/cell, unprofiled**.

| id | Option | What changes | Expected gain | Basis |
|---|---|---|---|---|
| O-1 | **Execute an unstamped twin.** | Strip once at the execution boundary, before the analyzer: `__arrow_c_stream__`, `count`, the SQL and write paths, and temp-view registration. Hand DataFusion the stripped plan, cached per native handle, and take `StripAttributeIds` out of the optimizer list. Identity semantics do not change. | At most 1.33 ms/cell (export 0.852 + sql 0.322 + count 0.148 + write options 0.008). The real gain is that bound minus the one strip per execution. | Bound *measured*; the share it recovers is *inferred* until step 0 splits strip cost from analyzer-on-metadata cost. |
| O-2 | **Stamp at sources and computing nodes, with no wrapper projection.** | Mint ids into scan and values field metadata, and into the existing alias metadata of Projection, Aggregate and Window, so `project_ids` never adds a node. | Unknown. It shrinks plan depth, but every field still carries metadata. | *Inferred*, weak. Only worth it if step 0 shows the wrapper nodes are the cost. |
| O-3 | **Side table instead of field metadata.** | Ids live in a Rust-side `Arc<[Option<AttrId>]>` on the `PyDataFrame`, aligned with output positions and derived from the parent at each operator. Plans never carry `repark.attr`. `StripAttributeIds` and `_strip_attribute_id_metadata` are deleted. The key is the frame handle, not the plan node: DataFusion plan nodes are values rebuilt on every rewrite and have no stable identity. | At most the whole engine share, 1.48 ms/cell, plus stamping 0.099 and the export strip 0.014. Building the side table gives some of that back. | Bound *measured*; net *inferred*. |
| O-4 | **Lazy stamping.** | Frames are born unstamped. Ids are minted on the first identity read (Column access, join, union, self-join check), memoized per native handle. A child forces its parent's minting first, so inherited ids agree. | Proportional to the share of frames whose ids are never read. In `cells500.json`, 172 of 500 cells carry a `\|j_` join token, but non-join cells still read ids through `d.v`-style access, so that share is unknown. | *Inferred*. The share must be counted by an instrumented run. It does not remove engine cost for stamped frames unless combined with O-1. |
| O-5 | **Batch the metadata reads.** | One crossing per frame returns names, ids, qualifiers and the derived/root flags. This extends PERF-1 fixes A and F. | At most 0.092 ms/cell (the id/name read bucket). | Bound *measured*; the gain is below the bound once cProfile's per-call inflation is removed. |
| O-6 | **No Python-to-Rust crossings per Column.** | Bind all Columns of one `select`/`filter` in a single Rust call, instead of `_column_of` → `_bind_resolved_name` → `bind_qualified_free_refs` → `_rewrap_rebound_column` per Column. | At most about 0.128 ms/cell (`bind_qualified_free_refs` 0.049, `_bind_resolved_name` 0.030, `session_case_sensitive` 0.017, `bind_free_names` 0.012, `_column_of` 0.011, `_rewrap_rebound_column` 0.009). | Bound *measured*; net *inferred*. |

**Arithmetic.** O-5 and O-6 together are bounded by about 0.22 ms/cell profiled, well short of
0.54 ms. Any combination of Python-side options is bounded by about 0.60 ms/cell profiled
(every Python and new-native bucket), and PERF-1 concluded that "No safe Python cut remains".
**The target can only be reached by removing stamped-plan cost from the engine path (O-1, O-3,
or O-4 together with O-1).** This is *inferred* from the measured buckets.

## Decisions (proposed)

| id | decision |
|---|---|
| D-1 | Measure first: no product change before step 0 splits the 1.48 ms/cell engine share into analyzer, strip rule, wrapper projections, physical planning and export. |
| D-2 | Try O-1 first. It is the only option that leaves the identity model, every pin and halt rule 3's surface unchanged. |
| D-3 | If the step 1 median for O-1 is not under 1.10x, the design sketch rules between O-3 and O-4+O-1 on step 0's data. Python-side O-5 and O-6 only top up a native fix; they are never the plan of record. |
| D-4 | Whichever carrier is chosen, a frame or column with no id raises the existing internal error (`column_fields.py`: "internal error: stamped field … has no attribute id"). It never falls back to a name, position or side. |
| D-5 | Every cache (stripped twin, lazy memo, side table) is keyed on the native handle and re-derived whenever `_inner` is reassigned. Never key on the Python frame object. |
| D-6 | **Re-based to release builds (owner, 2026-10-06).** The gate is the **work-equal like ratio, median of three, at or under 1.10x, read on release builds of both sides** (`maturin develop --release`, with the codegen-units override recorded in the gate record below). Over the gate is a halt to the owner, with no carve-out. Two runs never pass. Every ratio and the spread are reported. *Work-equal like set:* the cells whose outcome class is the same on both sides; a cell's work is its `fn()` build plus `collect()`, timed by `replay_work.py`. Debug-build numbers stay in this card as dated history labelled **dev**; they inform and never pass or fail the gate. **History.** 2026-10-03 afternoon (owner delegate, Q2): the primary gate was the C-012 `replay.py` wall clock, median of three, ≤ 1.10x of a same-day main run, measured 1.375x on `5e4a0084` (main 498.99 / 499.30 / 497.40 s, head 687.03 / 685.95 / 683.86 s, dev), with like-for-like 1.2344x (1.2366 / 1.2344 / 1.2338 on 26003 cells, dev) as the second gate. 2026-10-03 evening (owner): the wall-clock gate is unreachable by construction, because 7,532 cells now answer where main errored early; the gate was re-based to the work-equal like set ≤ 1.10x, median of three (dev), with the plan-build-only ratio and the full wall clock reported beside it. 2026-10-06 (owner): the build profile was re-based to release; the number did not move. 2026-10-07: the final-head confirmation measured 1.1053 (HALT); the owner ruled no carve-out; STAMP-2-R5P6-1 (#980) brought the final head to 1.0812 (MET, gate record 2026-10-07). |

## Steps

| step | worker | what |
|---|---|---|
| 0 | executor (measurement only) | Native attribution on the frozen PERF-1 tree. Time each optimizer rule via the optimizer observer, plus the analyzer, physical planning and export, on stamped and stripped twins of the `cells500.json` shapes. Use a Rust bench or tracing spans; `perf` only if the owner lowers `perf_event_paranoid`. Also count frames whose ids are never read, for O-4. `micro2.py`'s `collect_stamped`/`collect_stripped` pair is a starting point, but its stripped path goes through `s._spawn(inner)`, which may re-stamp in `DataFrame.__init__`. Verify that before using its numbers. No product code. |
| 1 | Opus design sketch | Rule D-2/D-3 on step 0's numbers. Write out O-1's boundary list (every engine entry point, temp views, the SQL path) and the `_inner`-replacement sites it must cover. For O-3 or O-4: the propagation table per operator family (S3a–S3e, union, join re-mint, self-join check, SQL views), and how Spark carries ids through temp views, measured on the live oracle first. |
| 2 | executor | O-1 slice: the Rust boundary strip and cached twin, with the rule removed from the optimizer list. Full gate. Median of three. |
| 3 | executor(s) | Only if step 2 is short: O-3 or O-4 slices as the sketch orders them, one family per slice, each with the full gate. |
| 4 | clerk | Optional O-5/O-6 top-up if the median sits within noise of the bar, plus `map.md` and ledger lockstep. |

**Home:** `crates/repark-core/src/session/df_guards/attr_id.rs` (`stamp`, `project_ids`,
`StripAttributeIds`); `crates/repark-core/src/session/df_guards.rs` (optimizer rule list);
`crates/repark-python/src/dataframe_names.rs` (`stamp_attribute_ids`, `attribute_ids`);
`python/repark/src/repark/spark/dataframe/core.py` (`__init__`,
`_apply_export_display_names`); `python/repark/src/repark/spark/column_fields.py`;
`qualified_names.py`; `filter_quote.py`; `dataframe/replace_expr.py`;
`dataframe/frame_nodes.py`; the ATTR-ID-1 ledger.

**Gates:**
- **S0 replay, zero answer or class moves** against the stack-head replay current at the start
  of the unit. Known case-only flakes (two `AMBIGUOUS_REFERENCE` `v`/`V` cells, pre-existing
  HashMap order per the PERF-1 hand-back) are proven by three reruns.
- **The gate (D-6, re-based 2026-10-06): the work-equal like ratio, median of three, ≤ 1.10x on release builds of both sides.** The gate record below holds it. Over 1.10 is a halt to the owner, with no carve-out.
- **Reported, not gated:** the plan-build-only ratio and the full C-012 wall clock.
- **Step 0 measured (2026-10-03, r5p6, dev builds):** `replay_timed.py` times only the lazy build `fn()`, and `collect()` runs outside the timer. Of the +122.9 s, the timed build accounts for +46.8 s and the outside time for +76.1 s. Of the outside time, native `__arrow_c_stream__` accounts for +70.8 s: about 50 s from 2156 more executing queries (3772 cells answer where main errored, 3662 of them EQUAL to Spark) and about 29 s from each execution being 19% slower (9.75 to 11.62 ms). `stamp_attribute_ids` costs 2.4 s.
- **The full facade suite** (`python/repark/tests` at `-n 8`, 0 failed) and the parity suite.
- **The 842-cell inventory**, 0 regressions after reruns of the known flakes.
- **`count_129.py` holds at 2.**
- **Rust gates** when Rust changes: `cargo test -p repark-core --lib`, `-p repark-python
  --lib`, clippy `-D warnings`, `cargo fmt --check`, `make rust-panic-ban`.
- **The comment ban at 0 hits.**
- **Scheduled before 1.6.**

## Gate record (2026-10-07, final head, release builds)

**Result: MET at 1.0812.** This is the confirmation the Frontier ruling (2026-10-06 21:45) requires on the final merge head. Under the owner ruling of 2026-10-07 (no carve-out), it lands through STAMP-2-R5P6-1 (#980).

**The pair.** Main `575f57ca` against stack `6bcc46b5`: #968's head `13de60e1`, with main merged in, plus #980. The stack's native build is `f8fc8fa0`, whose `crates/` tree is identical to `6bcc46b5`'s. The facade is the editable checkout at `6bcc46b5`. Both sides are release builds (`uvx maturin@1.14.1 develop --release`, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`). The box was quiet: no lanes ran, the build lock was held through the runs, and the run started after a 300 s settle. The harness is the 10-06 harness: three interleaved `replay_work.py` runs over r5p6 and 23 families, scored on the work-equal like set.

| run | main work (s) | stack work (s) | work-equal like | plan-build only |
|---|---|---|---|---|
| 1 | 39.2 | 41.9 | 1.0710 | 1.1106 |
| 2 | 38.8 | 42.0 | 1.0812 | 1.1230 |
| 3 | 39.0 | 42.4 | 1.0851 | 1.1315 |
| **median** | | | **1.0812** | 1.1230 |

- **Spread:** 1.41 pt, over 26,032 cells. Summing per-cell medians of the three runs gives 1.0768.
- **Per family:** r5p6 carries 2.32 s of the 2.97 s excess (ratio 1.135, down from 1.184). Next are r3 at 1.080 (0.38 s) and r5p7 at 1.178 (0.35 s).
- **The first confirmation, on `13de60e1`** (2026-10-07 05:56, the same harness): **1.1053** (1.1031 / 1.1155 / 1.1053), a HALT to the owner. The owner ruled no carve-out and opened STAMP-2-R5P6-1.
  - Its attribution (`task/ledgers/staging/stamp-2-r5p6-1-ledger.md` C-005) found that no stack code carries the 0.64 pt between the 10-06 record and the first confirmation.
  - The r5p6 excess was 85 % construction. Its largest block was S4's ATTR-token select trigger re-planning every `fillna` over a display-name frame.
  - #980 plans attribute-exact projections natively, with answers unchanged: 2,294-cell and 1,703-cell attack grids show 0 diffs against `13de60e1`, after an Opus verify, two folds and an Opus re-verify.
- **Remaining excess** (out of scope here, recorded in C-005 §6): the join doors' SQL re-plans, the per-frame stamp, and the binding layer.

## Gate record (2026-10-06, release builds)

**Result: MET at 1.0989** (owner ruling 2026-10-06: this quiet three-run is the gate record).

**The pair.** Main `4a643e56` against stack `7f45e460`, the stack with main merged in (#965), so the stack is measured as it will merge. Both sides come from fresh clones, built one after the other with `uvx maturin@1.14.1 develop --release` and `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`. That override replaces the workspace `[profile.release]` value `codegen-units = 1`, while `lto = "thin"` is unchanged. Both sides carry it, and it cuts a build to about 10.7 minutes. The run started after a 300 s settle, with the build lock held through the runs. It used three interleaved `replay_work.py` runs over r5p6 and 23 families, scored on the work-equal like set.

| run | main work (s) | stack work (s) | work-equal like | plan-build only |
|---|---|---|---|---|
| 1 | 42.0 | 46.1 | 1.0990 | 1.1551 |
| 2 | 42.1 | 46.3 | 1.0989 | 1.1552 |
| 3 | 42.2 | 46.2 | 1.0957 | 1.1479 |
| **median** | | | **1.0989** | 1.1551 |

- **Spread:** 0.33 pt, over 25,999 cells. Summing per-cell medians of the three runs gives 1.0964.
- **Per family:** r5p6 carries 3.03 s of the 4.02 s excess (ratio 1.162). Next are r5p7 at 1.216 (0.47 s) and r3 at 1.075 (0.38 s). The r5p6 row is on the v1.5.3 card as a dated, non-blocking follow-up.
- **Plan-build only:** the same cells, timing only the lazy `fn()` build and leaving `collect()` outside the timer. This is the PERF-1 like-for-like measure. It is reported beside the gate and not gated, because the 2026-10-03 evening ruling moved the gate from it to the work-equal set (D-6, History).

**Preview (2026-10-06 05:26, not the record).** Main `9f41347f` against stack `c7c9a881`, release builds, run straight after the two builds at load 10.8:

| run | main work (s) | stack work (s) | work-equal like | plan-build only |
|---|---|---|---|---|
| 1 | 40.7 | 42.8 | 1.0515 | 1.1001 |
| 2 | 39.1 | 42.8 | 1.0935 | 1.1417 |
| 3 | 39.1 | 42.8 | 1.0931 | 1.1445 |
| **median** | | | 1.0931 | 1.1417 |

- **Run 1 flattered the stack:** main ran slow (40.7 s against 39.1 s in runs 2 and 3) while the box was still loaded from the builds. Spread 4.20 pt.
- **The gate record runs 0.58 pt higher.** It is the same harness, on main with TA series S1/S2a/S3, C-1 and RP-57 merged into both sides. Both sides also ran about 8% slower in absolute terms than in the preview.

**Dev history** (debug builds; informs, never gates):

| date | main | stack | work-equal like, median (runs) |
|---|---|---|---|
| 2026-10-04 | `db3a1f37` | `25debe29` (O-1) | 1.1234 (1.1234 / 1.1229 / 1.1238) |
| 2026-10-04 | `5e4a0084` re-baseline replays | `e0eefd30` (O-1 post-fold) | 1.1244 (1.1240 / 1.1244 / 1.1276) |
| 2026-10-05 | `9f41347f` | `14ece68b` (main merged, #949) | 1.1031 (1.1031 / 1.1041 / 1.0975) |
| 2026-10-06 | `9f41347f` | `c7c9a881` (ATTR-VIEW item (a)) | 1.1012 (1.1003 / 1.1012 / 1.1026) |

## Risks

- **Halt rule 3**, verbatim: "no branch maps an unknown frame or id to a side, position or
  name". A lazy or side-table carrier makes "no id here yet" a normal state, which tempts a
  fallback. The PERF-1 brief already treats "a laziness that turns a loud internal error into a
  silent fallback" as a HALT. D-4 holds the line. The sketch must list every branch that reads
  an id and show the error each one raises when the id is absent.
- **Cache invalidation, as PERF-1 hit it.** Lazy frame nodes (fix C) failed two full-suite
  tests: "tighten combinators" and "udf join-union". The hand-back says the "lazy build read
  replaced `_inner`": UDF and cache rewrites replace `_inner` after spawn. Fix E (`36ca23d5`)
  snapshots the spawn-time plan. Every new cache inherits this hazard. Known reassignment sites
  that re-stamp are `filter_quote.py` (`frame._inner = _native.stamp_attribute_ids(...)`) and
  `dataframe/replace_expr.py`. The sketch must find the rest (D-5).
- **Lazy minting order.** Under O-4, a parent minted after a child would give the child ids
  that disagree with its siblings. Minting must be top-down, and inherited ids must be
  identical across siblings.
- **Noise near the bar.** PERF-1's runs differed by up to 0.0064 on one tree and straddled
  1.20x (1.1983 / 1.2047 / 1.2021). Plan for margin under 1.10x, not a landing on it.
- **SQL and temp views.** O-1 and O-3 change what a registered view holds. Spark's attribute
  identity through `createOrReplaceTempView` → `spark.sql` must be measured on the live oracle
  before it is ruled on.

## Pointers

- Up: [map.md](map.md)
- Design: `/tmp/oc-worker/direct/wo/attr-id-1-design.md`; self-join sketch:
  `/tmp/oc-worker/direct/wo/attr-id-1-selfjoin-design.md`.
- PERF-1 brief and ruling R-PERF1-1: `/tmp/oc-worker/direct/wo/attr-id-1-perf-1.md`,
  `/tmp/oc-worker/direct/wo/attr-id-1-perf-1-r1.md`.
- Evidence: `/tmp/oc-worker/direct/wo/attr-id-1/perf-1/` (`prof-head.pstats`,
  `prof-base.pstats`, `fixC.pstats`, `cells500.json`, `micro*.py`, `preA`/`fixA`–`fixF`,
  `t-head-1..3`); PERF-1 hand-back `/tmp/xsj2/handback.json`.
