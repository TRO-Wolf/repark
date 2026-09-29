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
