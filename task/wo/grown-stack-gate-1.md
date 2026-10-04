# GROWN-STACK-GATE-1 — gate the 34 unconditional `deep_stack::block_on` sites the way R4 gated the frame doors      grade: B   engine: Muse Spark 1.3 contributor at max (owner, 2026-10-04)   release: next (main)

Written 2026-10-04 by the orchestrator (Claude, claude-opus-5-5) on the owner's ruling of the same day. Every `path:line` is at `origin/main` `22cce0eb`. Card: `task/roadmap/mid-term/grown-stack-gate-1-card-2026-10-04.md`.

## 0. Why, and what is out of scope

**The cost:**
- `crates/repark-python/src/deep_stack.rs:33` `block_on` drives every future through `on_grown_stack_with(GROWN_STACK_SEGMENT_BYTES, GROWN_STACK_SEGMENT_BYTES, …)`.
- Because the red zone equals the segment, `stacker` grows on every poll: a fresh 128 MiB mmap plus page re-faults, about 100 µs per region (DEEP-FILTER-CHAIN-CRASH-1 ledger, "Root cause").
- The 34 product sites still calling it cost main **+15.56 s (+5.2 %)** on the work-equal like set: 299.14 s on v1.5.1 against 314.69 s on v1.5.2, with 0 answer changes.

This unit makes growth conditional at those sites with the verdicts the frame doors already use. Answers must not change.

**Out of scope, named:**
- Segment and red-zone sizes, and every constant in `deep_stack.rs:14-25`.
- The frame doors already gated (`dataframe/mod.rs`, `arrow_export.rs`, `column/mod.rs`, the `session.rs:169/189` SQL doors).
- The expression and plan caps.
- Any `repark-core` change.
- The attribute-identity stack branch.

## 1. Rulings already made

| id | ruling |
|---|---|
| R-GSG-1 | **Gate per site. Do not "grow once per native call".** The second option still pays one grown region per call at the 23 no-plan sites. |
| R-GSG-2 | **No-plan sites take a plain `runtime.block_on(future)`, with no growth and no `stack_is_small` fallback** (owner: "no growth for catalog and option calls that carry no plan"). There are 23: `session.rs` `finish_session`, `read_parquet`, `read_csv`, `read_json`, `table_exists`, `register_memory_catalog`, `list_iceberg_table_names`, `refresh_catalog_provider`, `testing_oob_create_table`, `testing_oob_drop_table`, `testing_create_ref`, `testing_list_snapshots`, `register_late_catalogs`, `create_namespace`; `text_io.rs` `read_text`; `session_sources.rs` `read_iceberg_incremental`, `read_iceberg_path`, `read_iceberg_table_pinned`; `orc_io.rs` `read_orc`; `session_runtime.rs` `register_late_catalog_block`; `writer_layout.rs` `writer_plan`; `catalog_census.rs` `iceberg_metadata_cache_census`; `cache_budget.rs` `retained_cache_bytes`. |
| R-GSG-3 | **Frame sites take the frame doors' pattern** (`dataframe/mod.rs:241-247` `count` is the model): `let segment = frame_drive_segment_cached(&depths)?;` then `block_on_grown_sized(&runtime, future, segment)`. `depths` is the frame's `PyDataFrame::depths()`, or `plan_depths(plan)` where only a `DataFrame`/`LogicalPlan` is in hand. There are 10: `session.rs` `declare_temp_view_sorted`, `materialize_as_temp_view`, `materialize_as_cache_view`; `text_io.rs` `write_text_frame`, `write_text_partitioned`; `session_write_options.rs` `session_write_path`; `ml.rs` `open_stream` and `for_each_batch`; `plan_introspect.rs` `input_files`; `dataframe_stats.rs` `transpose`. |
| R-GSG-4 | **Streams poll at their opener's verdict.** `ml.rs` `for_each_batch` polls with the segment verdict computed once by `open_stream` for the same plan, as `arrow_export.rs:127` does with `grown`. Never recompute per batch. |
| R-GSG-5 | **The SQL-text site takes the SQL doors' pattern.** `session_write_options.rs` `session_sql_with_write_options` drives with `block_on_grown_if(&runtime, future, crate::deep_stack::sql_drive_grown(query))`. Lines 34-41 of that function already compute it once for the refusal check; compute it once into a local and use it for both. |
| R-GSG-6 | **`deep_stack::block_on` becomes private to `deep_stack.rs`** (`fn`, not `pub(crate) fn`), so the compiler refuses any new unconditional caller. Its remaining callers are `block_on_grown_if` and the module's own tests. No text-search guard is added. |
| R-GSG-7 | **The `crates/repark-python/src/map.md` row for `deep_stack.rs`** (line 40) gains one dated sentence naming this unit and the three verdicts. Each edited file's map row gets the same treatment only where the row already describes `block_on`. |
| R-GSG-8 | **No answer may change.** Any change in the DIFF-PROBE is S1. Growth is a stack-safety mechanism only. |

## 2. Files

| path | change | ceiling (default 1000; none is near it) |
|---|---|---|
| `crates/repark-python/src/deep_stack.rs` | `block_on` → private `fn` | 1000 |
| `crates/repark-python/src/session.rs` | 17 sites per R-GSG-2/3 | 1000 (798 today) |
| `crates/repark-python/src/text_io.rs`, `session_sources.rs`, `session_write_options.rs`, `ml.rs`, `writer_layout.rs`, `session_runtime.rs`, `plan_introspect.rs`, `orc_io.rs`, `dataframe_stats.rs`, `catalog_census.rs`, `cache_budget.rs` | sites per R-GSG-2/3/4/5; import lists adjusted | 1000 |
| `crates/repark-python/src/map.md` | R-GSG-7 | — |
| `python/repark/tests/test_grown_stack_gate_1.py` | new pins (§4 S2) | 1000 |
| `python/repark/tests/map.md` | one row for the new test file | — |
| `task/ledgers/staging/grown-stack-gate-1-ledger.md` | new: clauses C-001…C-006 (§5) with the measured evidence | — |
| `task/ledgers/staging/map.md` | one row | — |

## 3. Design sketch (signatures exist on main; nothing new is public)

```rust
fn block_on<F: Future>(runtime: &Runtime, future: F) -> F::Output
pub(crate) fn block_on_grown_sized<F: Future>(runtime: &Runtime, future: F, segment: Option<usize>) -> F::Output
pub(crate) fn frame_drive_segment_cached(depths: &PlanDepths) -> PyResult<Option<usize>>
pub(crate) fn plan_depths(plan: &LogicalPlan) -> PlanDepths
pub(crate) fn block_on_grown_if<F: Future>(runtime: &Runtime, future: F, grown: bool) -> F::Output
pub(crate) fn sql_drive_grown(query: &str) -> bool
```
- **`ml.rs`:** `open_stream` returns the stream together with its `Option<usize>` segment, and `for_each_batch` takes it.
- **`frame_drive_segment_cached` returns `Err`** past `MAX_PLAN_DEPTH`. At the frame sites that is the same `AnalysisException` the frame doors raise today, which is correct and not a change: those sites previously answered or crashed past the cap. Record every such site in the ledger.
- **If `?` cannot propagate** inside a `py.detach` closure, compute the segment before the closure.

## 4. Steps (one slice per round, one commit each; commit first, then write the provisional hand-back)

1. **S1: the no-plan sites and the private `block_on`** (R-GSG-2, R-GSG-6).
   - Gate: `cargo build -p repark-python` and `cargo test -p repark-python --lib`.
   - Run `test_deep_filter_chain_crash_1.py`, `test_deep_expr_build_small_stack_1.py` and every test file that names `read_parquet`/`read_csv`/`read_json`/`read_text`/`read_orc`/`register_memory_catalog`/`createNamespace`/`tableExists`/`read_iceberg` (`rg -l` them under `python/repark/tests`, run at `-n 8`).
   - Commit.
2. **S2: the frame sites, the stream and the SQL-text site** (R-GSG-3/4/5). Write `test_grown_stack_gate_1.py`. Each pin runs in an isolated interpreter on a thread with an 8 MiB stack, on the debug build, as `test_deep_expr_build_small_stack_1.py` does. Pins:
   - **P1:** a 2,000-deep `filter` chain through `createOrReplaceTempView`, then `spark.sql("SELECT count(*) …")`, answers.
   - **P2:** the same frame through `.cache()`, then `count()`, answers.
   - **P3:** the same frame through `write.csv` into tmp, read back, answers the row count.
   - **P4:** the same frame through `inputFiles()` over a parquet read answers.
   - **P5:** a 2,000-term SQL text through the write-options SQL door (`INSERT INTO … SELECT` with a long `OR`) answers or refuses exactly as main does. Record main's answer first.
   - **P6:** a shallow frame at each of the 10 frame sites answers.
   - **Mutations,** each recorded in the ledger: force `segment = None` at the temp-view, text-write and input-files sites; P1, P3 and P4 must crash (rc < 0) or fail. Restore. Force `grown = false` at the SQL-text site; P5 must crash. Restore.
   - Gate: the S1 set plus the new file, plus `test_ml*`, `test_*transpose*`, `test_*text*write*`, `test_*temp_view*` (`rg -l`, `-n 8`).
   - Commit.
3. **S3: measurement and ledger** (no product edits).
   - **(a) Micro.**
     - Write `micro_gsg.py` in the lane directory (not the repo), to the ledger's "Release per-call cost follow-up" spec: 1000 `SELECT 1` collects; 1000 `filter(...).count()`; 200 counts over a small Iceberg table on a memory catalog; and `import repark` plus session creation in ms.
     - Build release wheels of `db3a1f37` (v1.5.1) and of the head with `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, one at a time, each into its own venv.
     - Run an **A/A first** (v1.5.1 against v1.5.1, 5 fresh processes per side, interleaved, medians). If any shape's A/A ratio is outside 1 ± 0.005, HALT with the A/A numbers.
     - Then A/B: head against v1.5.1, 5 fresh processes per side, interleaved, medians. Every ratio must be ≤ 1.005.
   - **(b) The like set.** STOP and write a provisional hand-back saying "ready for like-set timing". The orchestrator confirms the box is quiet, and resumes you. Then run three interleaved `replay_work.py` runs exactly as `/tmp/oc-worker/direct/wo/attr-id-1/stamp2-rebase/run.sh` does: main = a dev build of v1.5.1 (`/tmp/xverify-attr-base/.venv`, read-only), head = your head's dev build (`make develop`). Compute work-equal over the like set with `stamp2-rebase/l4l_run.py` (directory path adapted only). Report:
     - the head main-side seconds per run, the median and the spread;
     - the v1.5.1 seconds per run;
     - the ratio.
     
     The bar is within noise of v1.5.1: the head median minus the v1.5.1 median must be ≤ the larger of the two sides' spreads.
   - Write the ledger (C-001 no-plan sites ungrown; C-002 frame sites gated, P1–P4/P6; C-003 stream verdict; C-004 SQL text, P5; C-005 micro; C-006 like set) and the map rows.
   - Commit.

## 5. Gates and their expected output

- **Rust:**
  - `cargo test -p repark-python --lib`: `test result: ok`;
  - `cargo clippy --workspace --all-targets -- -D warnings -A clippy::disallowed_methods`: no warnings;
  - `cargo fmt --all --check`: silent;
  - `make rust-panic-ban`: clean.
- **Python and docs:**
  - `make develop`, then the §4 test sets at `-n 8`: `0 failed`;
  - `python3 scripts/check_rust_file_size.py`: clean;
  - `bash scripts/check_map_md.sh --base origin/main`: clean.
- **Comment ban:** `python3 /tmp/oc-worker/_lib/comment_ban.py <clone> origin/main HEAD`: `hits=0`.
- **The full facade suite** is CI's job (owner ruling 2026-09-18). It must be green on the PR.

## 6. Halt rules

- **H1:** a site in R-GSG-2 turns out to receive a user `LogicalPlan` or `DataFrame` (for example `writer_plan`'s request carries a plan). Name it and stop.
- **H2:** a deep pin from DEEP-FILTER-CHAIN-CRASH-1 or this unit goes red after a slice.
- **H3:** a mutation in S2 does NOT go red. That means the site never needed growth; record it and stop.
- **H4:** the A/A spread is outside 1 ± 0.005, or any A/B micro ratio is > 1.005.
- **H5:** the like-set median is not within noise of v1.5.1.
- **H6:** any file needs a ceiling raise, or any edit outside §2.
- **H7:** any answer differs from main on any pinned shape.

**The hand-back on a halt** carries the numbers and the exact line.

## 7. Hand-back

Write `/tmp/oc-worker/direct/wo/grown-stack-gate-1/handback.json` (provisional after each commit, final at the end):
```
{"status":"DONE|HALT|READY_FOR_TIMING","head":"<sha>","commits":[...],"sites":{"no_plan":23,"frame":10,"sql_text":1},"pins":{"P1":"green/red-on-mutation",...},"micro":{"aa":{...},"ab":{"select1":r,"filter_count":r,"iceberg":r,"import_session":r}},"like_set":{"head_s":[...],"v151_s":[...],"median_head":x,"median_v151":y,"spreads":[...]},"gates":[...],"comment_ban":"hits=0","halt":null}
```
The commit trailer is exactly one line: `Authored-By: Muse Spark (muse-spark-1.3-contributor) <noreply@meta.ai>`. Identity TRO-Wolf. Never push. Never use `--no-verify`. **No code comments in any file** (Rust, Python, TOML); relocated code sheds its comments.
