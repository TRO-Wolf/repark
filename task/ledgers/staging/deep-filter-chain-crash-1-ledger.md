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
| C-007 | A 2,000-deep `+1` select raises a catchable `AnalysisException` naming the deep-expression limit, never a crash (VD-3; Spark refuses deep arithmetic with a catchable error too). SQL past the 1 MiB text cap refuses the same way. | Isolated-interpreter pins asserting both refusal classes. | PROVEN | `test_deep_subquery_expression_1.py` `test_two_thousand_deep_select_refuses_clean`, `test_overlong_sql_refuses_clean`; `deep_stack.rs` `sql_gates_split_at_the_length_bounds`. |
| C-008 | A 16-deep chain counts 50 on a 512 KiB thread (small-stack backstop: any entry point grows when under 2 MiB remain); debug SIGSEGVs on f958d1a8 and base alike. | One isolated-interpreter pin. | PROVEN | `test_deep_subquery_expression_1.py` `test_shallow_chain_answers_on_small_stack_thread`. |

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
