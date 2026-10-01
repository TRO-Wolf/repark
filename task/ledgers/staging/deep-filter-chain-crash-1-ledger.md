# Unit ledger — DEEP-FILTER-CHAIN-CRASH-1 · deep operator chains answer instead of crashing

**Date:** 2026-09-29 · **Branch:** `fix/deep-filter-chain-crash-1` · **Base:** `d415da76`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card row PE-15 (release-diff-1-5-1-pre-existing.md): `count()` after
200 or more chained `df.filter(...)` calls kills the Python process with
SIGSEGV on main (150 passes); the repro is `reverify4-cs2/segv.py`. A crash
takes down the whole interpreter with no error while Spark answers. Step 0
measured first (below): on this lane's debug build the filter crash lands at
610 chained filters and a second, execution-side crash lands at 110 chained
joins. The fix grows no recursion by hand: every native entry point polls on a
256 MiB stacker segment on the calling thread, and the shared tokio runtime
builds worker and blocking threads at 256 MiB.

**What Spark does (measured 2026-09-29; Spark 4.1.2, `America/New_York`,
single JVM session).** The DataFrame door answers every chain: 610 filters
count 50, 400 `withColumn` count 50, 600 selects count 50, 600 unions count
30050, 110 joins count 1. Nested-deep SQL is refused on both engines: 610-deep
nested filters raise Spark `ParseException`
`[FAILED_TO_PARSE_TOO_COMPLEX]`, RePark raises `ParseException`
`RecursionLimitExceeded (current limit: 50)` at 200 and a facade
`RecursionError` at 400+. Flat SQL answers on both: 600 flat `UNION ALL`
count 1202 on Spark (RePark 1202 on its two-row base), 600 flat selects count
50 on both. Two pre-existing divergences, both clean refusals, both out of
this unit: 200-deep nested selects answer 2 on Spark and refuse on RePark
(R-3), and the 200-deep nested-join probe text is ambiguous on Spark
(`AMBIGUOUS_REFERENCE`, a probe alias artifact) while RePark refuses at the
parser limit.

## Step 0 (2026-09-29, unmodified tree, debug build)

Smallest crashing N on the DataFrame door: filter 610 (605 passes), join 110
(100 passes); `withColumn` 200 answers (44.0 s plan build + 7.9 s count) and
400+ exceeds 150 s in plan build alone; select and union answer at 1500. The
SQL door never crashes: nested shapes refuse clean (parser limit 50, then
facade recursion), flat unions answer at 600.

Backtrace 1 (filter, `segv.py true str 610`): thread 1, the main Python
thread (RLIMIT_STACK 32 MiB), SIGSEGV inside ~606 consecutive
`PushDownFilter::rewrite` frames (`datafusion-optimizer-54.1.0`
`push_down_filter.rs:832` self-recursion, fault at `:771`) reached from
`block_on` → `count` → `optimize`. Frame 0 at `0x7ffffe002fd0`, frame 200 at
`0x7ffffea8b150`: 10.5 MiB per 200 levels, ~53 KiB per filter level. The
rule-internal recursion carries no stack guard; the outer tree walk's
`stacker::maybe_grow` does not reach inside it.

Backtrace 2 (join, 120 chained joins): a tokio blocking-pool thread (2 MiB
default stack), SIGSEGV inside 1406 frames of nested `HashJoinStream`
polling (`poll_next_impl` → `OnceFut` → child stream, `hash_join/exec.rs`),
~18 KiB per join level. Execution-side nesting polls off the calling thread,
so caller-side growth alone cannot cover it. Tokio honors
`Builder::thread_stack_size` for blocking-pool threads (`tokio-1.53.1`
`runtime/blocking/pool.rs:228,465-466`).

Mechanism note, recorded because the first attempt failed on it: a generic
`thread::spawn` helper cannot carry the borrowing futures (`&ReparkSession`,
`&mut` streams) across the scoped-thread higher-ranked lifetime bound, and a
macro version explodes under rustfmt's `fn_call_width` and poisons outer
closures with `move`. The landed mechanism needs no `Send`/`'static` bound at
all: `deep_stack::block_on` polls through the existing
`repark_core::column_resolution::on_grown_stack_with` (same-thread growth),
and `build_shared_runtime` sets `thread_stack_size`. No `Cargo.toml` edit, no
fork edit, no new crate edge.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | 1,000 chained DataFrame-door filters then `count()` answers 50, Spark 4.1.2's answer; the base tree SIGSEGVs at 610. | One isolated-interpreter pin asserting the count; the grown-stack recursion units. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_thousand_filter_chain_answers_on_dataframe_door`; `deep_stack.rs` `block_on_drives_recursion_beyond_default_thread_stacks`, `block_on_drives_a_borrowing_future`; `segv.py true str 610` answers 50 on head. |
| C-002 | 200 chained joins count 1 and 300 chained unions count 15050, Spark's answers; the base tree SIGSEGVs at 110 joins. | One isolated-interpreter pin asserting both counts. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_deep_join_and_union_chains_answer_on_dataframe_door`; `deep_stack.rs` `shared_runtime_blocking_threads_survive_deep_recursion`; join-200 and union-300 cells answer on head. |
| C-003 | 120 chained `withColumn` count 50; plan-build time caps the depth, not the stack (44 s build at 200 on base). | One isolated-interpreter pin asserting the count at the recorded ceiling. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_wide_with_column_chain_answers_within_ceiling`; withColumn-120 answers on head. |
| C-004 | 1,000-deep nested SQL raises a catchable `RecursionError`, never a crash (Spark refuses nested-deep SQL with `FAILED_TO_PARSE_TOO_COMPLEX`); flat 600-branch `UNION ALL` SQL counts 601. | One isolated-interpreter pin asserting the refusal class and the flat count. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_deep_sql_shapes_refuse_clean_or_answer`; nested-1000 records `RecursionError`, flat-600 records 601 on head. |
| C-005 | A 1,000-deep filter chain answers 50 under a scalar subquery and under `IN (SELECT ...)` (VD-1); both shapes SIGSEGV on f958d1a8. | One isolated-interpreter pin asserting both counts. | PROVEN | `test_deep_subquery_expression_1.py` `test_thousand_filter_chain_answers_under_subqueries`; `deep_stack.rs` `plan_depths_see_through_scalar_subqueries`. |
| C-006 | A 5,000-term OR through `sql()` answers 50 on the debug build and a 10,000-term OR answers 50 on release (VD-2; Spark answers both too); the same shape through `filter()` raises a catchable `AnalysisException` at build time (Spark raises `StackOverflowError` at `.filter()` the same way). | Debug pin asserting the 5k answer; release or_n_sql N=10000 answers 50 in 108 s; DF-door refusal pins at 2k/20k. | PROVEN | `test_deep_subquery_expression_1.py` `test_five_thousand_term_or_answers_through_sql`, `test_twenty_thousand_term_or_refuses_through_filter`; `deep_stack.rs` `frame_drive_segment_grows_past_the_expression_cap`. |
| C-007 | A 2,000-deep `+1` select raises a catchable `AnalysisException` naming the deep-expression limit, never a crash (VD-3; Spark refuses deep arithmetic with a catchable error too). | One isolated-interpreter pin asserting the refusal class. | PROVEN | `test_deep_subquery_expression_1.py` `test_two_thousand_deep_select_refuses_clean`; `deep_stack.rs` `refuse_expression_depth_splits_at_the_cap`. |
| C-009 | The 200,000-item IN list over `range(1000)` (1,288,936 bytes) counts 1000; no query-text length cap remains (limits R1; base answers 1000 in 60 s, Spark answers with no length limit). | One isolated-interpreter pin asserting the count. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_two_hundred_thousand_item_in_list_answers`. |
| C-010 | 8,193 `DataFrame.union` calls count 409700 (union spines skip the plan cap: base and Spark answer unions at 8193); 8,192 chained filters refuse `AnalysisException` naming the deep-plan limit (base SIGSEGVs and Spark raises `StackOverflowError` there). | Two isolated-interpreter pins asserting the count and the refusal class. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_union_past_plan_cap_answers`, `test_filter_past_plan_cap_refuses_clean`; `deep_stack.rs` `plan_depths_ignore_union_spines_for_the_plan_cap`. |
| C-011 | A 300-term AND answers 50 and a 1,500-term AND refuses `AnalysisException` at `filter()` (limits R3; Spark first refuses `and`/`or`/`+` chains past 300 terms, so the 1500 builder cap refuses above Spark). | Two isolated-interpreter pins asserting the count and the refusal class. | PROVEN | `test_deep_filter_chain_crash_1.py` `test_and_chain_at_must_answer_depth_answers`, `test_and_chain_past_expression_cap_refuses_clean`. |
| C-008 | A 16-deep chain counts 50 on a 512 KiB thread (small-stack backstop: any entry point grows when under 2 MiB remain); debug SIGSEGVs on f958d1a8 and base alike. | One isolated-interpreter pin. | PROVEN | `test_deep_subquery_expression_1.py` `test_shallow_chain_answers_on_small_stack_thread`. |
| C-012 | A 6,000-term OR builds on the caller thread and on an 8 MiB thread, then refuses `AnalysisException` at `filter()` on both (CI segv: the 20,000-term build SIGSEGVs on an 8 MiB main stack before any builder runs). | One isolated-interpreter pin asserting both refusals; the grown-clone/drop Rust units. | PROVEN | `test_deep_expr_build_small_stack_1.py` both tests; `deep_stack.rs` `grown_column_combine_and_drop_survive_deep_trees`, `grown_expression_clone_and_drop_serve_sub_megabyte_threads`. |
| C-013 | Every clone, plan, optimize, execute, format, and drop of a plan or expression runs through the single grown-stack helper sized from cached levels (VD2-1); no public operation overflows any caller thread. | The guard test greens and a planted bypass reds it; the column/frame exactness batteries assert cached levels equal a fresh survey on every rule. | PROVEN | `tests.rs` `grown_stack_guard_rejects_bypass_sites`; `expr_tests.rs` / `display/tests.rs` / `dataframe/tests.rs` / `subquery.rs` batteries; `test_deep_reverify_1.py` all workers survive. |
| C-014 | `sql()` over a temp view holding a 1,000-deep plan counts 50 (VD2-2); the session carries a deep-view high-water mark and the query grows past it. | One isolated-interpreter pin asserting the count. | PROVEN | `test_deep_reverify_1.py` `test_deep_view_sql_and_explain_answer` (`deep_view_count` 50). |
| C-015 | A 2,000-term SQL-text OR answers 50 through `F.expr`, `selectExpr`, and string `filter` (VD2-3; the 1,500 cap no longer applies to SQL text); a 1,500-term DF-built OR refuses `AnalysisException` at `filter()` (Spark raises `StackOverflowError` at `.filter()` on the same shape); a 5,001-term mixed DF/text OR answers 50 (Spark answers 50). | Three isolated-interpreter pins plus the 2026-09-30 Spark verbatim. | PROVEN | `test_deep_reverify_1.py` `test_sql_text_or_chains_answer_on_every_door`, `test_dataframe_built_or_past_cap_refuses_clean`, `test_mixed_dataframe_and_text_or_answers`; Spark `or_df_1500` StackOverflowError, `or_fexpr_5000` 50, `mixed5001` 50. |
| C-016 | Teardown of deep columns and frames on GC/finalizer threads never overflows (VD2-4); owning types drop on a grown segment sized from cached levels. | One isolated-interpreter pin collecting deep cycles on main and on a 256 KiB thread. | PROVEN | `test_deep_reverify_1.py` `test_gc_collects_deep_cycles_on_main_and_small_threads`. |
| C-017 | 256 KiB and 512 KiB caller threads count a 16-deep chain, refuse a 2,000-term DF-built OR, read `.columns` off a 500-deep frame, and collect a deep cycle (VD2-5). | Two isolated-interpreter pins, one per stack size. | PROVEN | `test_deep_reverify_1.py` `test_small_stack_thread_answers_and_collects`, `test_half_meg_stack_thread_answers_and_collects`. |
| C-018 | The debug suite answers within 1.15x of 1,108 s and unpivot stays linear in columns (VD2-6; the per-op tree walks inflated the suite past 2,088 s with exponent 1.120). | CI-shaped wheel repro under `repark.slice` plus the 64 GiB cap, `-n 4`: suite time and the unpivot exponent. | PROVEN | Slice (subquery + unpivot + 4 unit files) 68 passed in 848 s < 1,274 s bar; unpivot exponent 0.950 on the debug wheel. |

## Mutation record (2026-09-29)

`deep_stack.rs` was reverted to the old stack sizes, the module rebuilt, the
pins run under `subprocess` so the crashes are caught, then the file was
restored to its committed state, the module rebuilt, and `git status` shown
clean with the pins green again.

| # | Mutation | Red |
|---|---|---|
| M1 | `block_on` drives `runtime.block_on(future)` directly (no grown stack). | The 1,000-filter pin dies SIGSEGV (rc=-11), restored green. |
| M2 | `build_shared_runtime` back to `Runtime::new` (2 MiB pool threads). | The 200-join pin dies SIGSEGV (rc=139), restored green. |

## Tests rewritten

- `test_deep_filter_chain_crash_1.py::test_deep_sql_shapes_refuse_clean_or_answer`:
  the flat-union expectation 1202 → 601. The worker builds one-row `VALUES`
  branches (601 branches), so 601 is the engine-correct count; the 1202 was
  copied from the two-row Step 0 probe. Test-authoring bug, fixed in the pin
  itself.

## Measurements (2026-09-29, debug build)

Plain 3-filter `count()`, 5 samples: base `0.017 0.014 0.014 0.014 0.014`
(mean 0.0146 s); head `0.017 0.015 0.014 0.014 0.014` (mean 0.0148 s), +1.4%,
inside the 5% bound.

Process memory after `import repark` plus the same 3-filter query
(`/proc/self/status`, 64-CPU box): base `VmSize=8654732 kB VmRSS=316376 kB`;
head `VmSize=15416760 kB VmRSS=314376 kB` (rerun `15416756 kB / 318608 kB`).
The reservation grows ~6.8 GiB virtual — tokio workers and blocking threads
now reserve 256 MiB each and spawn lazily, plus one cached 256 MiB stacker
segment per driving thread — while residency is unchanged (−2 MB then +2 MB,
run-to-run noise): reservation commits only touched pages.

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: deep-filter-chain-crash-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each pin asserts a Spark-measured value on the same shape: filter 50, join 1, union 15050 and 601, withColumn 50, and the RecursionError refusal class; the oracle ran Spark 4.1.2 America/New_York in one JVM session.
      artifacts: [python/repark/tests/test_deep_filter_chain_crash-1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The DataFrame door carries C-001 through C-003 and the SQL door carries C-004 in nested and flat shapes; every native entry point (all 45 product block_on sites: SQL, DataFrame, reads, writes, temp views, catalogs, streams) drives through the same grown-stack helper.
      artifacts: [python/repark/tests/test_deep_filter_chain_crash-1.py, crates/repark-python/src/deep_stack.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Nested-deep SQL raises a catchable RecursionError, never a crash; panics still unwind through block_on to the PyO3 fence (Rust pin block_on_propagates_a_future_panic_to_the_caller); M1 and M2 crash before restore.
      artifacts: [python/repark/tests/test_deep_filter_chain_crash-1.py, crates/repark-python/src/deep_stack.rs]
    - id: AT-4
      status: N/A
      justification: No new shared state and no new concurrency surface; same-thread stack growth plus sized pool stacks, each pin in one isolated interpreter.
    - id: AT-5
      status: N/A
      justification: No AWS, no secrets, no path handling; subprocess pins run scratch sessions only.
    - id: AT-6
      status: ATTACKED
      evidence: Twelve neighbour statements answer identically before and after except pre-existing UNION ALL order noise (both orders observed on head across identical runs, no ORDER BY); the committed facade smoke files run green on head.
      artifacts: [python/repark/tests/test_df_easy.py, python/repark/tests/test_columns.py]
    - id: AT-7
      status: ATTACKED
      evidence: The 3-filter 5-sample check moves +1.4%, inside the 5% bound; the VmSize/VmRSS base-vs-head record sits in Measurements above (reservation +6.8 GiB virtual, residency unchanged).
      artifacts: [task/ledgers/staging/deep-filter-chain-crash-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml edit and no new crate edge (stacker stays transitive through repark-core; repark-python to repark-core pre-exists); size baselines exact (session.rs 1122, column/mod.rs 1012, lib.rs 190); no comments in any source file (comment_ban green).
      artifacts: [crates/repark-python/src/deep_stack.rs, crates/repark-python/src/session.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The map.md of every touched directory moves in the same commits (deep_stack row, lib.rs row, column row, test-file row); this ledger files C-001 through C-004 for the cited pins.
      artifacts: [crates/repark-python/src/map.md, crates/repark-python/src/column/map.md, python/repark/tests/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: M1 breaks the filter pin (rc=-11) and M2 breaks the join pin (rc=139) under subprocess; both answer after the revert with git status clean.
      artifacts: [crates/repark-python/src/deep_stack.rs, python/repark/tests/test_deep_filter_chain_crash-1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-29 (not stack-bound; pin lowered per the brief): `withColumn` plan build is superlinear, 44.0 s at 200 on base, so 400+ exceeds 150 s before any execution; the pin sits at 120. |
| R-2 | Dated 2026-09-29 (no crash; pre-existing optimizer/execution scaling): the join count phase runs 129.2 s at 500 post-fix, so 1,000-deep joins exceed 250 s; the pin sits at 200. |
| R-3 | Dated 2026-09-29 (pre-existing clean-refusal divergence, deliberately untouched): 200-deep nested selects answer 2 on Spark 4.1.2 and refuse at RePark's parser limit 50; relaxing the limit could open SQL-door crashes, so it stays out of this unit. |
| R-4 | Dated 2026-09-29 (caveat on the Measurements reservation): a strict-overcommit (`vm.overcommit_memory=2`) or `ulimit -v` environment could fail the 256 MiB thread/stack reservations with loud errors instead of crashes; residency is unchanged and default overcommit is unaffected. |
| R-5 | Dated 2026-09-29 (limits fold; drop-side stack cost): dropping a deep plan needs ~250 B per level on the dropping thread (an 8201-deep test plan aborts a 2 MiB thread at drop); main-thread drops hold past 20000 and pool threads hold 64 MiB, so only a sub-megabyte thread holding a deep plan is exposed — same as base. |
| R-6 | Dated 2026-09-29 (limits fold; pre-existing terminal split, out of unit scope): the 200k-item IN list answers 1000 through `collect()` and 1 through `count()` on base and head alike (10/10 virgin samples stable per terminal; form-independent); Spark's `count()` answers 1 too, its `collect()` is unmeasured. The C-009 pin uses `collect()`, matching the orchestrator's 1000-in-60 s measurement. |
| R-7 | Dated 2026-09-30 (CI segv; pre-existing, same as base): recursive `Display`/`schema_name` formatting of a deep expression (`display_name`, `make_struct` field naming) still runs on the caller thread; no caller holds a deep expression there. |
| R-8 | Dated 2026-09-30 (CI segv; pre-existing, same as base): `DataFrame::clone` of a plan whose top node holds a deep expression evaluates eagerly on the caller outside the terminal verdict; the unit's shapes top out far below it. |

## Release per-call cost follow-up (2026-09-29)

Release wheels for head and the `d415da76` base worktree
(`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, one build at a time), 5 fresh
processes per side, medians of 1,000 `SELECT 1` collects, 1,000 filter
counts, 200 Iceberg counts, plus import-plus-session milliseconds.

| Round | Head shape | select1 | filter_count | iceberg | import_session |
|---|---|---|---|---|---|
| R1 | 256 MiB segment/pool | 1.3181 | 1.0594 | 1.1946 | 0.9945 |
| R2 | 128 MiB segment, 64 MiB pool | 1.3111 | 1.0845 | 1.0185 | 0.9769 |
| R3 | R2 plus `runtime.spawn` offload | 1.4008 | 1.5844 | 1.4283 | (unrecorded) |
| R4 | conditional growth (shipped) | 1.0200 | 1.0026 | 1.0108 | 1.0043 |

Root cause: `on_grown_stack_with` passes red zone = segment, so
`stacker::maybe_grow` grows on every call; each grown region mmaps a
fresh segment and re-faults its touched pages, ~100 microseconds per
region (~2 regions per `SELECT 1` collect, 1 per `count`). Resizing the
segment cannot help because the cost is touched pages, not reserved
bytes. The `runtime.spawn` offload regressed: its two cross-thread hops
cost scheduling latency on the loaded 64-CPU box (base shows a larger
futex count and still answers faster).

Fix: growth is conditional on logical-plan input depth.
`frame_needs_grown_stack` grows past `DEEP_PLAN_INPUT_DEPTH` (16); 16
levels times the 53 KiB worst case is ~0.85 MiB, safe on a 2 MiB
thread. `count`/`show`/`__arrow_c_stream__` drive through
`block_on_grown_if`, and the verdict rides into
`StreamingBatchReader::new` as `grown_polls` so polls match the plan
that opened them. `session.sql` stays on a direct `runtime.block_on`
(parse plus lazy plan build, never the optimizer); admin and IO paths
keep unconditional `block_on`. The spawn offload is fully reverted.

Verification on the shipped shape: debug pins 4 passed (37 s); the pin
worker on release answers every value exactly (filter 50, join 1,
union 15050, withColumn 50, nested SQL RecursionError, flat union 601);
a 100-deep `collect` plus `to_arrow` answers 50 on release (grown polls
under release codegen); M1 and M2 re-run red (rc=-11 both) and restored
green; `cargo test -p repark-python` 95 plus 25 with 4 new tests (depth
split, grown/ungrown drive, 40-deep grown stream). strace on release
shows no per-query segment growth; the 128 MiB `PROT_NONE` reservations
appear on base too (allocator, startup only).

Release memory after import plus session plus the 3-filter query:
base `VmSize=4339264 kB VmRSS=190864 kB`; head `VmSize=5989636 kB
VmRSS=197304 kB` (rerun `5989648 kB / 191104 kB`). Reservation delta
+1.65 GiB virtual (64 MiB pool stacks plus allocator arenas), down from
+6.8 GiB at 256 MiB; residency delta is run-to-run noise (+6 MB then
+0.2 MB). R-4 now reads 128 MiB segments (allocated only for deep
plans) and 64 MiB pool stacks.

Coverage note: AT-2's "same grown-stack helper" now reads "the same
`block_on`/`block_on_grown_if` pair" — shallow terminal and poll
traffic skips growth by plan-depth verdict; every deep shape still
grows. AT-7's bound is superseded by the R4 row above (1.10x ceiling,
all green). AT-10 re-verified on the shipped shape as recorded here.

## Verifier fold plan (2026-09-29)

Opus findings VD-1..VD-5 against f958d1a8. Fix: keep conditional
growth, extend the depth walk to subquery plans and expression depth
(iterative worklists, no recursion), grow `sql()` behind a 4 KiB length
gate, refuse past caps with `AnalysisException` naming the limit
(expression depth 1500, plan depth 8192, SQL text 1 MiB), scale the
grown segment 128 KiB per plan level (128 MiB floor, 1 GiB ceiling).
Thread-pool execution for every entry was rejected: the R3 `spawn`
measurement (1.40-1.58x) already proves the hand-off cost breaks the
ceiling, and `sql()` plus the streaming polls hold `!Send` futures.
(Limits fold: the SQL text cap is removed and the plan cap counts
non-union nodes; the paragraph above describes 3466b00c, the section
below describes the fold.)

## Verifier fold evidence (2026-09-29, debug build unless noted)

Cap placement follows Spark's own split: Spark answers 10k/20k-term
ORs through SQL (10 s, 16 s) and raises `StackOverflowError` at
`.filter()` on the DF door. The builders therefore refuse past
expression depth 1500 while the terminal verdict grows past any
expression depth (`frame_drive_segment_grows_past_the_expression_cap`;
plan 8192 and SQL 1 MiB refuse everywhere).

Pins: `test_deep_subquery_expression_1.py` 6 passed in 664 s
(subquery-1000 scalar+IN answer 50; or_5000_sql answers 50 in ~220 s;
or_20000 DF-door refuses `AnalysisException`; arith-2000 select
refuses; 1 MiB SQL refuses; 16-deep counts 50 on a 512 KiB thread).
Rust units 878 plus 101.

Verifier-probe rerun (isolated interpreter per probe): all 7 VD-1
subquery probes answer 50 (3 s); small-stack 14-deep and 16-deep
answer 50; N=2000 subquery IN/scalar/direct answer 50 (4-10 s);
`selectexpr_or_2000` answers 50 in 280 s (was crash);
`and_5000_df` refuses in 14.5 s where Spark raises in 14.3 s (both
sides pay the Python/JVM tree build); all filter_1000 terminals,
threads_8, union_2000, values_20000 match the recorded head answers.
Intended flips: `or_5000_df` and the mixed-thread 2000-OR shape now
refuse at `filter()` (Spark refuses both; the old debug build
answered after ~200 s). Still slow-or-hung, matching the verifier's
out-of-scope perf list and release-side records: `upper_1000_df`
(release answers in 227 s both sides), `udf_deep_arg` and
`arith_rightdeep_1000_df` (release timeouts both sides),
`concat_300_df`/`struct_200_df` (timeouts both sides). The
`SELECT 1+1...` hang at n=1000 is pre-existing: the short-text path
is byte-identical to the old `runtime.block_on`.

VD-2 mutation on debug: `sql()` reverted to a plain `block_on`
answers or_n_sql at N=2000 (31 s) and N=5000 (272 s) and times out
past 900 s at N=10000 — no crash. The debug main thread (8 MiB)
survives the parse recursion; the recorded debug or_5000 crash died
at collect time (the old terminal verdict stayed plain for shallow
plans), which the grown terminal verdict now covers. The `sql()`
crash itself is release-side (the verifier's gdb record inside
`PyReparkSession::sql` at N=10000), so the decisive mutation ran
on the release build: plain-`sql()` release answers or_n_sql with
50 at N=10000 (107 s) and N=20000 (554 s) — no crash. The A/B
against the verifier's rhead record (CRASH at both N) isolates the
fix exactly: for this shape the two builds differ only in the
terminal verdict (old: plan-inputs 2, stays plain; new: expression
depth 20001, drives grown), so the crash died at collect/optimize
time on the plain stack, and the expression-aware terminal growth
is what fixes VD-2. The `sql()` length gate stays as instructed
defense-in-depth for parse-time recursion; it costs short queries
nothing and cannot change these outcomes.

## Verifier fold release evidence (2026-09-29)

Wheels head vs `d415da76` (`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`,
one build at a time), 5 fresh processes per side, medians:

| Side | select1 (1000) | filter (1000) | iceberg (200) | import+session |
|---|---|---|---|---|
| Head | 748.9 ms | 1142.4 ms | 335.4 ms | 146.9 ms |
| Base | 766.3 ms | 1188.9 ms | 348.8 ms | 150.1 ms |
| Ratio | 0.9773 | 0.9609 | 0.9614 | 0.9787 |

All four ratios sit under the 1.10x ceiling with margin (head
nominally faster; run-to-run median noise is ~2%). The extended
verdict walk (subquery plans plus expression depth) costs shallow
queries nothing measurable on release.

Release answers: or_n_sql N=10000 answers 50 in 108 s (VD-2 pin;
the verifier's rhead crashes); filter_n_count N=8000 answers 50 in
8 s (clean-probes regression check; the 1 GiB scaled segment spawns
fine); subq_in_deepview_sql / subq_scalar_df answer in 0.5 s,
N=2000 subquery IN/scalar in 1.4 s, repro_vd1.py 1000 rc=0;
upper_1000_df answers 1 in 215 s (rhead: 227 s, no regression).

Release memory after import plus session plus the 3-filter query:
base `VmSize=4338376 kB VmRSS=185-191 MB`; head `VmSize=5989008 kB
VmRSS=156-191 MB`. Residency delta is run-to-run noise. VD-5
reservation correction: the +1.65 GiB virtual delta is 26
anonymous 64 MiB stack mappings (measured in `/proc/self/maps`),
one per runtime thread under this lane's 26-CPU cgroup quota
(`cpu.max 2600000 100000`), i.e. `RUNTIME_THREAD_STACK_BYTES` x
(workers plus blocking-pool threads). The reservation scales with
the CPU count the runtime sizes its pools to — a 64-CPU lane
reserves proportionally more — and it is reserve-only: no
resident cost.

VD-5 non-covered shapes: the verdict measures the logical plan at
the entry point, so it does not see (a) deeply nested DATA TYPES
(`struct<struct<...>>` conversion recursions, unmeasured), (b)
subtrees the optimizer duplicates past the measured shape, (c)
execution-side recursion past the 64 MiB worker stacks (no
reaching shape is known; N=8000 plans answer), (d) recursion in
user UDF code. The `Extension`/`Unnest` no-expression
classification is compile-guarded: the exhaustive
`node_expressions` match fails the build until a new DataFusion
plan variant is classified.

VD-4 DIFF-PROBE rerun at the final head (debug): 305 old
statements re-run with zero real flips (only `sess_version`'s
random appid), plus 9 new `deep-vd` statements, all `ok:true`:
four subquery shapes answer 50 (0.5-3 s), `vd2_or_5000_sql`
answers 50 (262 s), the three DF-door refusal shapes report
`refused:AnalysisException`, `vd_smallstack_16` counts 50.
Base side (`d415da76` debug, solo reruns): the three 1000-deep
subquery shapes, `vd3_and_5000`, `vd3_arith_2000`, and
`vd_smallstack_16` SIGSEGV (rc=-11 each); `vd1_in_300` answers 50
(0.6 s, same as head); `vd2_or_5000_sql` answers 50 in 368 s (head:
262 s, same answer); `vd3_or_2000` answers 50 in 14 s where head
refuses `AnalysisException` (intended flip — Spark raises on this
shape). Six crashes fixed, two same-answers, one intended flip.

## Limits fold (2026-09-29)

Rule: this unit stops crashes, so a refusal survives only where base
crashes on the same statement (a) or Spark 4.1.2 refuses it at the
same threshold, measured (b). Fold base is adc26586 (`/tmp/xrel`);
Spark is 4.1.2 `America/New_York` local[2] on zulu-17 (banner in
`spark_bisect.jsonl`); RePark sides are debug builds, one isolated
interpreter per cell. Depths below are the brief's N (chain length);
expression depth is N+1 for `and`/`or`, N+2 for `+`.

R1: the SQL text cap is deleted (`MAX_SQL_TEXT_LEN`,
`refuse_overlong_sql`, its call sites and pins). The 200,000-item IN
list over `range(1000)` (1,288,936 bytes) answers 1000 on base in
59.9 s over `collect()` and refuses on 3466b00c; Spark has no
text-length limit. Long text now drives the grown stack past 4 KiB
instead of refusing.

| IN shape, 200k items (debug) | base | 3466b00c | Spark |
|---|---|---|---|
| `range()` form over `collect()` | 1000, 60 s | refuses (text cap) | unmeasured |
| `range()` form over `count()` | 1, 42 s | refuses (text cap) | 1, 9 s |
| temp-view form over `collect()` | 1000, 66 s | refuses (text cap) | unmeasured |
| temp-view form over `count()` | 1, 42 s | refuses (text cap) | 1, 10 s |

The split is the terminal, not the form: `collect()` answers 1000
and `count()` answers 1 on base and head alike (10/10 virgin
samples stable per terminal), and Spark's `count()` answers 1 too
(R-6); small IN lists answer exactly on every side (10k items:
Spark 25, correct). The fold pin uses the range form over
`collect()` and expects base's 1000.

R2: the 8192 plan cap stays for filter-led plans and stops counting
`Union` nodes (`PlanDepths.limited`). Base crashes every filter cell
at 4000+ and Spark refuses them (`StackOverflowError`, at build from
4000, at `count` from 1000); the filter-count threshold sits in
(900, 1000]. Base and Spark both answer unions at 8192/8193, so the
blanket cap was a regression there. `withColumn` times out on both
RePark sides at 4000+ (superlinear plan build, R-1 extended); select
answers on base at 8192/8193 but Spark refuses selects from 4000,
so refusal stays allowed by (b).

| Plan cell (debug) | 3466b00c | base | Spark |
|---|---|---|---|
| filter 4000/8192/8193/12000/20000 | ans/ref/ref/ref/ref | crash x5 | refuse x5 |
| union DF 4000/8192/8193/12000/20000 | ans/ref/ref/ref/ref | ans/ans/ans/tmo/tmo | ans/ans/ans/ref/ref |
| union SQL 4000/8192/8193/12000/20000 | ans/ref/ref/ref/tmo | ans/ans/ans/tmo/tmo | ref/ref/ref/ref/ref |
| withColumn 4000/8192/8193/12000/20000 | tmo x5 | tmo x5 | refuse x5 |
| select 4000/8192/8193/12000/20000 | ans/ref/ref/ref/tmo | ans/ans/ans/tmo/tmo | refuse x5 |

`ans`/`ref`/`tmo` = answers / clean refusal / timeout. Spark
refuses union DF at 12000+ (`StackOverflowError` at `count`) and
union SQL from 4000 (`FAILED_TO_PARSE_TOO_COMPLEX`; answers 600 per
Step 0). Union DF 8193: base 409700 in 557 s, Spark 409700 in
1266 s. Union SQL 8193: base 8194 in 991 s. The close-out section
below records the post-fold re-run of every R2 cell.

R3: the 1500 builder expression cap stays. Spark first refuses
`and`/`or`/`+` chains between 300 and 350 terms (depth 301-351,
all three ops, `StackOverflowError` at build), so the head refusal
at depth 1501 sits above Spark's threshold on every op, and every
depth Spark answers (300 and below) answers on head. Base answers
`and` to 3000 (crash by 4000), `or` to 5000 (timeout at 8000), and
`+` to 300 in 159 s (hang at 1000-1100, crash from 1200); every
depth head refuses past 1500 is refused by Spark too, so (b) holds
on each. No cap moves.

| Expression cell (debug) | 3466b00c | base | Spark |
|---|---|---|---|
| and 1000/1499/1500/1501/2000/3000/5000 | ans/ans/ref/ref/ref/ref/ref | ans x6/crash | refuse x7 |
| or 1000/1499/1500/1501/2000/3000/5000 | ans/ans/ref/ref/ref/ref/ref | ans x7 | refuse x7 |
| plus 200/300/1000/1499/1500/1501/2000+ | ans/ans/tmo/ref/ref/ref/ref | ans/ans/tmo/crash x4 | ans/ans/ref x5 |

Pins: C-009 (IN list 1000), C-010 (union 8193 answers 409700,
filter 8192 refuses), C-011 (AND 300 answers 50, AND 1500 refuses),
all in `test_deep_filter_chain_crash_1.py` (gate-covered; worker
timeout 300 s → 1500 s). C-007 keeps the arith-2000 refusal; its
SQL-cap half is deleted with the cap. Rust: `deep_stack.rs` gains
the union-spine unit; the SQL-gate unit pins growth without refusal.

Residues added: R-5 (dropping a deep plan needs ~250 B per level
on the dropping thread: an 8201-deep test plan aborts a 2 MiB
thread at drop, so the union-spine unit runs on a 64 MiB thread;
main-thread drops hold past 20000, sub-megabyte threads must not
hold deep plans — same as base) and R-6 (the 200k-item IN list
answers 1000 over `collect()` and 1 over `count()` on every side;
pre-existing terminal split, out of unit scope).

## Limits fold close-out (2026-09-30)

Post-fold head re-runs all 26 R2 cells plus the IN list (one
isolated interpreter per cell): unions DF/SQL answer at 8192/8193
(DF 8193: 409700 in 540 s; SQL 8193: 8194 in 953 s), union DF 12000
answers 600050 in 1167 s where base times out, the remaining deep
unions time out exactly where base times out, filters refuse past
the cap, selects refuse past the cap, withColumn times out in plan
build on both sides. Zero crashes; zero answered-on-3466b00c-now-
refusing. Select 20000 refuses given the 1200 s budget (the 300 s
pre-fix timeout was a budget artifact, not a verdict).

Mutation M3: the SQL text cap restored (const + refusal + three
call sites) turns the C-009 pin red (the worker dies at the IN
cell; the statement refuses `query-text limit` in 1.0 s); reverted
with `git status` clean, and the IN list answers 1000 in 60 s
again.

DIFF-PROBE replay at the fold head: 314 statements, zero real flips
vs 3466b00c (random `sess_version` appid plus `COLLECT_SET` element
order; both orders reproduce on identical code). Verifier-probe
replay (27 probes on head and base): all 7 VD-1 subquery probes
answer 50, `selectexpr_or_2000` answers 50, `and_5000_df` refuses
clean where base crashes, all filter_1000 terminals answer, the
small-stack and 8-thread shapes answer — VD-1..VD-3 still fixed.
`test_deep_subquery_expression_1.py` 6 passed in 622 s (the
converted 1.1 MB answer pin included).

Release per-call medians vs d415da76 (wheels,
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, 5 fresh processes per
side): select1 0.9843, filter_count 1.0051, iceberg 0.8560,
import+session 1.0096 — all under the 1.10x bar.

Gate fallout fixed in this commit: the CAP-1 mirror row for
`column/mod.rs` ratchets 1012 → 1011 with the script baseline,
plus the scripts and parity-tests map rows the lockstep rule
needs.

## CI segv (2026-09-30)

CI job `build + import smoke (debug, host)` on cb8c8b8d: all six
`test_deep_subquery_expression_1.py` tests ERROR — the fixture worker
dies rc=-11 with empty stdout/stderr; the other 14,083 pass. The lane
is green on the same commit.

Environment difference: the lane main thread is 32 MiB
(`ulimit -s 32768`) while CI's ubuntu default is 8 MiB. Split-shape
probes on the lane develop build: subquery, or_5000_sql, arith_2000,
longsql and smallstack_16 all pass under `ulimit -s 8192`; the
20,000-term `|` reduce crashes mid-build under 8 MiB (rc=139) and
passes under 32 MiB. gdb on the 8 MiB crash: thread 1 SIGSEGV inside
recursive `Expr::clone` (`BinaryExpr::clone` → `Box<Expr>::clone`)
with the stack pointer off the main stack.

Root cause: `PyColumn` operators clone (and then drop) DataFusion
`Expr` trees with compiler-generated recursive Clone/Drop on the
caller thread, with no grown-stack hop. The 1500 builder refusal
cannot cover this: the crash lands while *building* the expression,
before `filter()` runs. Threshold bisect on 8 MiB debug: N=4000
builds, N=6000 crashes, so ~1.6 KiB per level.

Fix: `deep_stack::grown_clone` sizes a sync grown segment from the
iterative expression depth (8 KiB per level, ~5x the measured debug
cost, 1 GiB ceiling) via a new `repark_core` sync primitive
`run_on_grown_stack`; every operator routes operand clones through
the `PyColumn::expr` clone-out, `Drop` drops deep trees grown, a manual
`Clone` grows the by-value argument clones at the PyO3 boundary (the
first after-repro built the 20,000-term tree, then crashed cloning it
into `filter()`), and
the three bodies that clone owned deep exprs (`over`,
`call_scalar_expr`, `count_distinct_argument`) grow around renamed
inner bodies. No refusal added, no threshold moved: the 20,000-term
build still succeeds and still refuses at `filter()`. `column/mod.rs`
ratchets 1011 → 1005 (rustfmt joins three shortened calls; the `Clone` derive leaves with the manual impl).

Rust units M-R1/M-R2/M-R3: `grown_clone` restored to a raw clone
aborts the 20,000-deep combine test (SIGABRT), an empty `Drop` aborts
it too, and a raw `Clone` aborts it as well; all restored green.

CI-shaped proof (fresh debug wheels, `ulimit -s 8192`, `-n 4`):
parent 6 errors in 274 s (the CI failure verbatim); head 6 passed in
966 s. The lane-32 MiB parent run passes in 637 s, which is why the
lane stayed green. Wheel mutation M4: reinstalling the parent wheel
crashes the 20,000-term probe again (rc=139); reinstalling head goes
green; `git status` clean.

Release A/B (head vs parent wheels, 5 fresh processes per side,
medians): select1 1.0114, filter_count 1.0122, iceberg 0.9895,
import_session 1.0014 — max 1.0122, under the 1.05 bar. Memory spot:
VmSize +156 KiB (+0.003%), VmRSS within run noise; the reservation
note holds (no new persistent mappings — grown segments are
transient mmaps).

Reruns, both venvs, all green: subquery file 6/6 (lane 940 s, wheel
966 s), filter-chain file 9/9 (lane 779 s, wheel 668 s), sibling pin
2/2 (lane 87 s, wheel 96 s). Neighbour shapes answer identically
pre/post with unchanged times except the pathological 20,000-term
build. `outer()` over a 6,000-deep column survives on 8 MiB
(DataFusion's own recursion protection holds `transform`).

Gate 15/16: the parity suite's two `test_ci_tier_cell` failures are
one pre-existing sort-cell timeout (KILLED at the 600 s cell budget),
reproduced at base (cb8c8b8d) with the identical signature — not this
change. The golden dates to v1.3 (2026-09-11, 19 days of drift);
tier-1 CI runs the same suite and renders its own verdict. Untouched
as out of scope; the question goes to the orchestrator.

Residues added: R-7 (recursive `Display`/`schema_name` formatting of
a deep expression — `display_name`, `make_struct` field naming —
still runs on the caller thread; no caller holds a deep expression
there, same as base) and R-8 (`DataFrame::clone` of a plan whose top
node holds a deep expression evaluates eagerly on the caller outside
the terminal verdict; the unit's shapes top out far below it, same as
base).

## Re-verify fold (2026-09-30, VD2-1..VD2-6 + CI perf)

The verifier's second round holds every public operation to one bar:
no segfault on any caller thread (main, 256 KiB Python threads,
GC/finalizer) for any plan/expression shape Spark answers, through one
`stacker::maybe_grow` helper per recursion level, with the 1,500
builder cap lifted off SQL text and the debug-suite time back within
1.15x of 1,108 s. Clauses C-013..C-018 close the six items; C-018 stays
OPEN until the CI-shaped wheel repro below records its two numbers.

Mechanism: cached levels, composed O(1). `PyColumn` carries
`(expression, df-built, subquery-plan)` levels and `PyDataFrame`
carries its `PlanDepths`; fixed-shape builders compose them without
walking, variable-shape builders survey once, and every clone, plan,
optimize, execute, format, schema walk, and drop sizes its grown
segment from the cache. The per-op tree walks are gone, which is what
restores the debug-suite time: a walk per operator over thousand-deep
trees was the 2x. R-7 and R-8 close here: `display_name`,
`contains_higher_order`, and `make_struct` field naming read through
`grown_read`, and every builder clone runs through
`grown_clone_frame` / `grown_clone_plan` / `grown_clone_expr`.

The builder cap counts df-built levels only. SQL-text columns
(`F.expr`, `selectExpr`, string `filter`/`where`) enter with df 0, so
SQL-text OR chains answer past 1,500 while DF-built chains still
refuse; mixed trees count only their DF-built levels. Spark oracle
(2026-09-30, same JVM session, verbatim): `or_df_1500` raises
`Py4JJavaError` caused by `java.lang.StackOverflowError` at `.filter()`
(5.0 s), `or_df_20000` raises `Py4JJavaError` at `.filter()` (45.8 s),
`or_fexpr_5000` answers 50 (6.9 s), `mixed5001` answers 50 (3.4 s).
RePark answers 50 / 50 / 50 and refuses `AnalysisException` at
`filter()` on the DF-built shapes — refusal where Spark refuses,
answers where Spark answers.

Inventory: every group below routes through the helper; the guard
test (`tests.rs` `grown_stack_guard_rejects_bypass_sites`) reds any
raw plan/expression clone outside `deep_stack.rs` (a planted
`self.inner().clone()` reds it; reverted clean).

| Group | Mechanism |
|---|---|
| PyColumn builders (~100 operators) | `combine` O(1) fixed shape, `combine_surveyed` once variable shape; operands clone through grown `expr()` |
| PyColumn reads (`display_name`, `contains_higher_order`, `make_struct` naming, `Column.sql`) | `grown_read` / `block_on_grown_if` on the text gate |
| PyColumn `Drop`/`Clone` | grown segments sized from cached levels |
| Frame builders attaching columns (`filter`, `select`, `with_column`, `sort`, `aggregate`, `join_on_condition`, SQL-text `filter`) | `drive_columns` (refuse + grow on df/expr/carried-plan depth); re-survey when a column carries a subquery plan, else O(1) |
| Frame builders without columns (`limit`, `distinct`, `union`, `distinct_on`, renames) | `grown_sync` + O(1), or full survey for conditional shapes |
| Terminals (`count`, `collect`/Arrow export, `show`, analyzed schema) | cached verdict sizes the drive; the analyzed plan drops inside the grown future |
| Frame `Drop` (`ManuallyDrop`) | teardown on a grown segment sized from the cache |
| Session (`sql`, temp views, readers, drops, catalogs) | grown plan/execute; deep-view high-water mark floors result levels (never `limited`) |
| `#[pyfunction]` doors (subquery, names, stack, stats, introspect, write options, ml, `logical_column_names`) | grown clones and grown schema walks throughout |
| Facade `explain` | composition only: temp view + `sql(EXPLAIN …)` + collect, each grown |

Carried-plan crash found and fixed in-fold: `select` over a scalar
subquery of a 1,000-deep chain SIGSEGVd (rc=-11) because the O(1)
frame rule dropped the column's carried plan levels and the terminal
grew for ~5 levels while the optimizer recursed 1,000+ deep
(`PushDownFilter::rewrite` under 296 `LogicalPlan::schema` frames).
Fix: builders re-survey when an input column carries plan levels (the
surveys are iterative, safe on any thread); `drive_columns` and
lateral grow on carried plan depth; builder-internal schema walks
moved inside the grown regions. Regression: `subquery.rs`
`builders_carrying_subquery_columns_match_a_fresh_survey` (red on the
reverted rule, green on the fix) plus the `subquery_1000_scalar` pin
back to 50.

Size-gate retirements (shrink-only, with the CAP-1 mirror, rust count
36 → 34): `column/mod.rs` 1005 → 969 (unit tests verbatim to
`column/expr_tests.rs`, the `PyColumn` struct plus level constructors
verbatim to `column/levels.rs` with `pub(super)` fields) and
`session.rs` 1122 → 802 (unit tests verbatim to `session_tests.rs`).

Rust: 113 lib tests green, clippy clean, rustfmt clean. Dev-wheel
pins: `test_deep_reverify_1.py` 7/7 (main 129 s, small/GC 93 s),
`test_deep_subquery_expression_1.py` 6/6 (826 s),
`test_deep_expr_build_small_stack_1.py` 2/2 (75 s),
`test_deep_filter_chain_crash_1.py` 9/9 (692 s).

## Re-verify fold closing entry (2026-09-30)

Mutations (each rebuilt, reddened, reverted, rebuilt green,
`git status` clean): M1 raw `PyDataFrame` Drop SIGSEGVs (rc=139)
dropping a 3,000-deep frame on a 256 KiB thread, restored rc=0; M2
`maybe_grow` neutralized in `repark-core` SIGSEGVs (rc=-11) the 256
KiB pin worker, restored green; M3 SQL-text cap restored fails the
SQL-text pin (worker rc=1, `AnalysisException`), restored 7/7.

CI-shaped repro: debug wheel to `/tmp/xdeep-dist`, fresh venv
`/tmp/xdeep-wheeltest` with the facade extras, `repark.slice` plus
`ulimit -v 67108864`, `-n 4`. The brief's slice (subquery + unpivot
+ `test_df_easy` + `test_dfcore_1_exports` +
`test_production_file_size` + CAP-1) runs 68 passed in 848 s, under
the 1.15x bar of 1,274 s; the unpivot exponent measures 0.950 on the
debug wheel (medians 0.038/0.162/0.348 s at width 50/250/500). C-018
PROVEN. The full facade suite on the same wheel runs 13,966 passed,
485 skipped, 147 xfailed, 0 failed in 2,053 s.

Release A/B (head vs `a53118a6` wheels,
`CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16`, 5 fresh processes per
side, medians): import_session 0.9805, select1 1.0198, filter_count
0.9718 — max 1.0198, under the 1.05 bar. Re-measured on the final
32 MiB code (`692169b5` wheel vs the same `d415da76` base wheel,
2026-10-01): import_session 1.0355, select1 1.0224, filter_count
1.0156 — max 1.0355, under the 1.05 bar.

DIFF-PROBE replay (305 recorded statements on the new head): 0
regressions — one appid noise cell and one `collect_set` order flip
proven nondeterministic (both orders across 6 identical-head runs);
9 newer vd* statements all answer as expected. VD replay (89
recorded probes): 3 moves, 0 regressions. `concat_300_df` and
`concat_1000_df` move timeout/CRASH to `MemoryError` (a pre-existing
deep-nested-function eval blowup: the base build raises the identical
`MemoryError`, and Spark answers 15350 at 300 but `StackOverflowError`
at 1000) — improved failure mode, out of scope. `upper_1000_df`
moves timeout to the correct answer 1 (Spark `StackOverflowError`s):
RePark answers where Spark overflows. Spark oracle for all three
recorded 2026-09-30. The concat-300 divergence (Spark 15350 vs our
`MemoryError`) is reported for a registry row; no pin (a 116 s
blowup is not pin material).

Closing fix (2026-10-01): the gate's spill cells hung (`KILLED`,
sort wedged past 600 s) because late tokio blocking-thread spawns
map 64 MiB stacks each and fail under the cells' address-space cap
(strace: 4 threads, `mmap(64 MiB, MAP_STACK) = ENOMEM`, spill writes
then frozen, threads parked in futex). The 64 MiB dates to f958d1a8
(pre-fold). 8 MiB aborts
`shared_runtime_blocking_threads_survive_deep_recursion` (12 MiB of
recursion needs the headroom), so the shared runtime moves to 32 MiB:
the pin holds, the spill cells pass in 33 s, and the deep battery
re-runs green: 24/24 in 1858 s on the final 32 MiB code (2026-10-01).
Release A/B re-measured on the final code (max 1.0355, under the 1.05
bar; see above). Full `gate.sh` on the final code: all 16 steps exit
0 — comment ban 0, repark-core lib, clippy, panic ban, fmt, lib-rs,
rust-file-size, lib-py, develop, the 5-file deep pytest step
(62 passed, 818 s),
ruff check, ruff format, parity harness (785 passed, 795 s),
ledger-grammar, docs-links, map-sync — GATE GREEN (2026-10-01).
