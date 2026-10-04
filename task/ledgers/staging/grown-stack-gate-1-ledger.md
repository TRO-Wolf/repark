# Unit ledger — GROWN-STACK-GATE-1 · the 34 unconditional grown-stack sites take verdicts

**Date:** 2026-10-04 · **Branch:** `fix/grown-stack-gate-1` · **Base:** `1060ebeb`
(`origin/main`), product code `d0c50405` (v1.5.2 release) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** `deep_stack::block_on` grows a 128 MiB stacker segment on every
poll (~100 µs per region). The 34 product sites still calling it cost main
+15.56 s (+5.2 %) on the work-equal like set (299.14 s on v1.5.1 against
314.69 s on v1.5.2, 0 answer changes). This unit gates those sites with the
verdicts R4 gave the frame doors. Answers must not change.

**Site counts.** 24 no-plan sites drive plain `runtime.block_on` (23 per
R-GSG-2 plus `declare_temp_view_sorted`, reclassified by orchestrator ruling
2026-10-04: it refuses non-`MemTable` views, then collects an in-memory scan,
and the Python door allows source frames only). 9 frame sites drive
`block_on_grown_sized` with `frame_drive_segment_cached`. 1 SQL-text site
drives `block_on_grown_if` on the text gate OR the session deep-view mark
(orchestrator ruling 2026-10-04 amending R-GSG-5; see the H7 episode).
`block_on` is private to `deep_stack.rs` (R-GSG-6); the compiler verifies
zero external callers.

**Orchestrator rulings applied (2026-10-04, resume round).** Q1: declare is
no-plan. Q2: the write-options door ORs in the deep-view mark exactly as
`sql()` does; `deep_view_levels` widens to `pub(crate)`, unchanged
otherwise. Q3: P1 is a must-not-change neighbour; the temp-view mutation
targets `materialize_as_cache_view` with P2. P3 is the R-GSG-5 regression
pin; its mutation drops the deep-view OR.

**H7 episode (2026-10-04, pre-S2).** Under R-GSG-5 as first written (text
gate only), P3 — a 2,000-deep filter chain through `write.csv`, read back,
count — segfaulted (rc=139, no write-done): local path writes register the
source frame as a view, then run short COPY SQL through the write-options
door, so a short query over a deep view planned its inlined 2,000-deep
definition ungrown. Base and the S1 head both answer 21 rows. The Q2
amendment (deep-view OR) repairs it: the S2 head answers 21 rows. P1, P2,
P4 and P5 matched main before and after.

| clause | claim | method | status | evidence |
|---|---|---|---|---|
| C-001 | The 24 no-plan sites drive plain `runtime.block_on` with no growth and no `stack_is_small` fallback; no answer changes. | S1 gate set on the S1 head against base. | PROVEN | `cargo test -p repark-python --lib` 119 passed; deep pins 11/11 on base and S1 alike; the 258-file read/catalog set 10017 passed, 0 failed on S1. |
| C-002 | The 9 frame sites drive the frame doors' segment verdict; P1 (neighbour) counts 20, P2 counts 20, P4 lists 1 parquet file, P6 answers at all 10 sites, and the D-pins answer deep write.text (20), partitioned text (4 parts), localCheckpoint (20) and transpose (1) — every cell equals main. | One isolated interpreter per shape, deep work on 8 MiB threads; a crash fails only that shape's test and names it. | PROVEN | `test_grown_stack_gate_1.py` P1/P2/P4/P6/D tests, 19 passed on head; every constant recorded from a base run first (P3 21 rows: 20 data plus the header row read back as data). |
| C-003 | ML streams poll at the opener's verdict: `open_stream` returns the stream with its segment, `for_each_batch` reuses it, never recomputed per batch. | Deep ML pin plus the P6 ML shape and the `test_ml*` gate files. | PROVEN | `test_grown_stack_gate_1.py` `test_deep_linear_regression_fits` ([2.0, [3.0]], base recorded first) and `test_shallow_linear_regression_fits`; M9 red (below) proves the verdict carries the deep shape. |
| C-004 | The SQL-text site drives on the text gate OR the deep-view mark; P5 (2,000-term OR INSERT, 22,951 bytes) answers 40 rows as on main, and P3 (short COPY over a 2,000-deep view) answers 21 rows as on main. | P5 pin plus the P3 regression pin; M4/M5 red. | PROVEN | `test_grown_stack_gate_1.py` `test_long_or_insert_answers`, `test_deep_frame_csv_roundtrip_counts`; base answers recorded first on both shapes. |
| C-005 | The four micro shapes stay within 1.005 of v1.5.1 (release wheels, 5 fresh processes per side, medians, A/A first). | Orchestrator-run on a quiet box (resume: do not time on the lane). | OPEN | Pending orchestrator timing; the head build for it is this unit's last commit. |
| C-006 | The work-equal like set stays within noise of v1.5.1's 299 s (three interleaved runs, dev builds). | Orchestrator-run on a quiet box. | OPEN | Pending orchestrator timing. |

**Mutations (each built, run in a subprocess, observed red, restored, rebuilt
green; H3 would halt on any green).** M1: `segment = None` at
`materialize_as_cache_view` → P2 SIGSEGV rc=139. M2: `segment = None` at
`write_text_frame` → a 2,000-deep `write.text` shape SIGSEGV rc=139 with no
write-done (unmutated: answers 20). M3: `segment = None` at `input_files` →
P4 SIGSEGV rc=139. M4: `grown = false` at the SQL-text site → P5 SIGSEGV
rc=139. M5: the deep-view OR dropped → P3 SIGSEGV rc=139 with no write-done.
Restores verified by grep (no `None`/`false` remnants) and a green pin run.
**Verifier fold (V-1):** one combined mutant (`segment` → `None` at the 5
V-1 sites, verifier `combined_mutant.diff` plus the M2 `write_text_frame`
change) reds exactly the 5 new D-pins (each rc=-11, named per shape) while
the 14 older pins stay green. M6: `write_text_frame` → D-text red. M7:
`write_text_partitioned` → D-text-part red. M8:
`materialize_as_temp_view` → D-ckpt red. M9: `open_stream` +
`for_each_batch` → D-ml red. M10: `transpose` → D-transpose red. Reverted
by checkout, rebuilt, 19/19 green.

**Over-cap refusals (order §3).** Past `MAX_PLAN_DEPTH` the 9 frame sites
raise through `frame_drive_segment_cached` exactly as the frame doors do;
those shapes previously answered or crashed past the cap. Sites:
`materialize_as_temp_view`, `materialize_as_cache_view`, `write_text_frame`,
`write_text_partitioned`, `session_write_path`, `open_stream`,
`for_each_batch` (inherits the opener's verdict), `input_files`,
`transpose`. In `ml.rs` the refusal surfaces as `MlError::IllegalArgument`
(the module's only plan-failure variant), hence `IllegalArgumentException`
instead of `AnalysisException`; no realistic probe reaches 8,192-deep ML
fits on either engine.

**R-GSG-6 note.** Privatizing `block_on` in the S1 commit could not compile
(the S2 sites still called it), so the one-line change landed here in S3(a)
instead; the S1 and S2 commit bodies record the deferral. `cargo build` and
the lib suite pass with the private helper.

**Residues.** R-1: C-005/C-006 timings run on the orchestrator's quiet box,
not the lane. R-2: `declare_temp_view_sorted` leaves no deep pin of its
own — no deep shape can reach it (source frames only, MemTable only); P6
covers it shallow.
