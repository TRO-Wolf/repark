# Overnight report — run 13b of 2026-09-14 (expressions)

**Session:** one Opus orchestrator (overnight-13b), 2026-09-14 05:19 → 06:30 local (night window), continued as a day run
to 12:00 local after the owner's correction (§5), beside run 13 (overnight-13, f- lanes).
**Grants:** G-1, G-2, G-3 (stop when the list is exhausted or by 06:30 local), G-4 Devin actor + Grok critic/reviewers
(no Muse, no GLM), G-5. STATUS.md, tags and the release pipeline untouched. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Owner rulings applied:** Q-R1 YES (the replace rewrite matches Spark on the nine divergent cells), Q-B1 (the conf key
`repark.cache.retained_bytes` stays), Q-B2 (the multi-partition prefetch is its own later card, not opened).

## 1. Outcome at the 06:30 night stop (superseded by §5.3)

| # | Unit | Step reached | PR | Merged | Rounds |
|---|---|---|---|---|---|
| 1 | REPLACE-LINEAR-1 step 1 | actor CONCLUDED, mechanical audit passed, **parked as a draft at the G-3 stop** | #577 (draft) | — | Devin 1 (session `harmonious-nova`, 65 agent steps, 81 tool calls, $0) |
| 2 | ARRAY-NULL-1 | not started | — | — | — |
| 3 | ANSI-DOOR-1 | not started | — | — | — |

Nothing merged and no Grok reviewer or critic ran. The session started at 05:19 local with a 06:30 stop, a 71-minute
window. The Devin round alone ran 05:27 → 06:22, so critic, reviewer, preflight and CI did not fit.

## 2. REPLACE-LINEAR-1 step 1

- Lane `/tmp/g-replace1`, branch `fix/replace-linear-1-step1`, rebased onto `main` and pushed at `33b31a7a`; the PR
  head was confirmed equal to local HEAD. The actor commit is signed with the Devin tier trailer.
- The brief (`/tmp/oc-worker/g-replace1/brief-1.md`) carried Q-R1 cell by cell. The actor read PySpark 4.1.2's `replace`
  and Spark's `DataFrameNaFunctions.replace` (Scala `CaseKeyWhen` with a cast per value) and mirrored both. Validation
  is eager, as in PySpark. Only columns whose type matches the key's type family are rewritten, with a cast to the
  column type on each branch, and each such column gets one searched CASE through `_from_when_pairs`. The body moved to
  the new `spark/dataframe/replace_expr.py`, and `core.py` shrank from 4089 to 4054 lines (ceiling ratcheted down).
- Red first: the armed depth-40 pin was re-run on the untouched tree tonight. It failed as step 0 recorded, with the
  worker dying in `case_when`.

### Before / after (debug native, actor measurement; release re-measure still owed under Q-R12-2)

| Reading | Before (`main`) | After (`33b31a7a`) |
|---|---|---|
| 16-entry dict RSS delta | 277.1 MB | ~5.4 MB |
| 40-entry dict | worker dies in `case_when` | ~5.4 MB, pin green by default |
| `{1: 2, 2: 3}` | `[3, 3, 3, None]` (sequential) | `[2, 3, 3, None]` (Spark) |
| nine Q-R1 cells | today's answers | PySpark 4.1.2's answers; EX-DF-12 flipped to FIXED |
| `core.py` lines | 4089 | 4054 |

### Audit (orchestrator)

- The comment-ban grep over the added lines prints nothing, and the commit's author and trailer are byte-exact.
- Files outside the card's Home each have a reason: the export snapshot (`_dfcore_1_expected.py`,
  `test_dfcore_1_exports.py`) for the new submodule, `test_examples_dataframe_c.py` for EX-DF-12 flipping to FIXED,
  `docs/spark-sql-iceberg-parity.md` for the parity row, and `scripts/check_lib_py.py` plus `scripts/map.md` for the
  ceiling moving down.
- Re-run by the orchestrator: `test_replace_linear_1.py`, `test_df_easy.py` and `test_dfcore_1_exports.py` with
  `REPARK_REPLACE_LINEAR_1_MEM=1`: 24 passed. The actor also ran every `DataFrame.replace` caller test and
  `make verify`, all rc 0.
- **Owed before merge:** `make preflight`, the whole parity suite, Grok critic-logic, the S2-21 Python perf reviewer
  (release native per Q-R12-2), CI.
- Disclosed by the actor in the ledger (P3-class, not fixed): `replace(1)` with no `value` raises `ARGUMENT_REQUIRED`
  on PySpark, while repark treats it as a null mapping (fixing it needs a `_NoValue` sentinel on the public signature).
  A non-str subset element raises `NOT_STR`, where PySpark defers to the JVM.

## 3. Decisions under §6

- R13b-D-1 Step 1 runs on a new branch `fix/replace-linear-1-step1`; step 0's branch was deleted at #571's merge.
- R13b-D-2 At the G-3 stop the unit parks as a draft PR rather than merging. The runbook's pre-merge critic, reviewer
  and preflight had not run.
- R13b-D-3 The out-of-Home files listed under Audit are accepted as lockstep consequences of the new submodule and the
  flipped example.

## 4. Owner questions — recommendations

- **Q-13b-1 (finish #577)** — **Recommend the next run opens with it.** Clone the branch fresh, run Grok critic-logic
  and the S2-21 Python reviewer (release native), send P1/P2 findings back to Devin with `--followup --resume
  harmonious-nova`, then preflight, the parity suite, flip to ready, and merge.
- **Q-13b-2 (`replace(1)` with no value)** — **Recommend a separate small card.** Matching `ARGUMENT_REQUIRED` changes
  the public signature's default, which is a public-signature decision under §6.
- **Q-13b-3 (window size)** — **Recommend at least 4 hours for a three-unit expressions list.** One Devin round took 55
  minutes before any review.
- **Q-13b-4 (order)** — **Recommend keeping this list's order**: finish #577, then ARRAY-NULL-1 (measure route (a) the
  ScalarUDF against route (b) the single-reference CASE, HALT if neither is linear), then ANSI-DOOR-1.

## 5. Day continuation (owner correction, 2026-09-14 06:26: G-3 extended to 12:00 local)

**Grants unchanged** apart from G-3. **Owner rulings folded in:** Q-13b-1 (finish #577 first), Q-13b-4 (this order),
Q-13b-2 (`replace(1)` with no value stays as-is; card
[replace-novalue-1-card-2026-09-14.md](replace-novalue-1-card-2026-09-14.md) filed on this PR).

### 5.1 REPLACE-LINEAR-1 step 1 (#577)

**CI red, diagnosed.** The Python job's `parity-harness tests` step failed on
`test_cap_1_source_file_line_cap.py::test_cap_1_exception_tables_equal_the_measured_debt`. That file mirrors the
Python size baselines and still held `core.py` at 4089 after the branch had ratcheted `scripts/check_lib_py.py` to
4054. Orchestrator commit `46192256` moved the mirror row and added the ratchet line to the parity tests map. CI went
green on that head. The actor's step gates had not run the parity harness; preflight and CI do.

**Release re-measure of the actor's "~5.4 MB at every size" claim** (orchestrator, `maturin develop --release` in the lane
venv, debug assertions off, pin worker, `RLIMIT_AS = VmSize + 24 G`, scope `MemoryMax=16G`). The actor's figure came
from a debug native, and the release deltas are lower:

| entries | base `e147685b` | branch `46192256` |
|---|---|---|
| 4 | 3.32 MB | 4.44 MB |
| 12 | 27.3 MB | 4.46 MB |
| 14 | 87.3 MB | 4.46 MB |
| 16 | 277.2 MB | 4.46 MB |
| 40 | dies in `case_when` (step 0) | 4.48 MB |
| 100 | — | 4.57 MB |

The reviewer's round-2 medians on the same release native agree: 4.44 MiB at N=4, 4.48 at 40, 4.72 at 200, a step to
11.05 MiB at N=400 (still linear-class), and base 278.4 MiB at N=16.

**Grok critic-logic** (fresh clone of `46192256` with a release native and `base/` = main's Python source on the same
native; 32 turns, $0.81): **NEEDS_REMEDIATION, 1 P1, 5 P2, 2 P3.** The CASE is genuinely flat (one `CASE WHEN` per
column, a CSE'd `CAST(col AS Float64)`, no nesting), and four of five scratch mutations turned pins red. The orchestrator
reproduced P1-1, P2-1 and P2-3 on the lane's release native before sending them back.

| id | finding | ruling | outcome |
|---|---|---|---|
| P1-1 | `replace("a", "b")` rewrote a `binary` column (`arrow_type_key` collapses Binary to `string`) | fix | family classified from the physical Arrow type; pinned |
| P2-1 | int overflow of a replacement value is a collect-time `simplify_expressions` cast error, not Spark's `CAST_OVERFLOW` | pin + disclose | pinned; ledger residue |
| P2-2 | last-wins on duplicate keys unpinned (first-wins mutation survived) | pin | pinned |
| P2-3 | no-subset `replace` on an alias + name equi-join raised `AMBIGUOUS_REFERENCE` | fix | join records its side qualifiers (`_join_qualifiers`), `replace` binds `"rel"."col"`; both join shapes pinned |
| P2-4 | struct-field subset `s.x` raises UNRESOLVED, not Spark's nested-field error | pin + disclose | pinned; ledger residue |
| P2-5 | C-001 extra probes not all in the pin file | pin | promoted |
| P3-1/2 | `df.na.replace` missing; `_NUMERIC_TYPE_KEYS` names widths never emitted | ledger | P3-2 retired by the Arrow-type rewrite |

**Grok S2-21 Python perf reviewer** (the first round was OOM-killed at 07:18 under its unit's 32 G cap; relaunched fresh
at 64 G with one measurement process at a time; 30 turns, $0.81): **PASS, no P1, 2 P2, 2 P3.** Plan build is linear at
17 µs per entry. The family filter is O(columns) (2.2 ms at 500 columns). A 1e6-row collect with a 40-entry dict takes
0.41 s, within noise of a hand-built `F.when` chain, and each arm's replacement cast is folded once at simplify.

| id | finding | ruling | outcome |
|---|---|---|---|
| P2-1 | `_replace_case` rebuilt every key `lit` and `lit(value).cast(type)` per column per entry (~180 ms of 815 ms at 500 cols × 40) | fix | literals cached once per call and per type key; actor re-measure 2519 → 2040 ms plan-only on a loaded box |
| P2-2 | depth-40 pin floor 64 MiB let a 2× regression pass | fix | floor 8 MiB; 10/10 actor passes, 3/3 orchestrator passes |
| P3-1 | RSS step to 11.05 MiB at N=400 | ledger | residue |
| P3-2 | wide plan build dominated by `DataFrame.select` of 500 CASE nodes | ledger | residue |

**Actor rounds on session `harmonious-nova` (all $0):** round 1 (06:22, step 1); round 2 (critic findings, 07:04 → 07:48,
commits `2b8e2295`, `78fd6c7a`); round 3 (07:50 → 07:54, `67fbf31c`), sent back because round 2 had **raised**
`core.py` 4054 → 4074 and both size baselines with it; round 3 moved the join-qualifier logic into `replace_expr.py` and
brought `core.py` to 4044; round 4 (08:20 → 08:29, `22911188`, the reviewer P2s). Rounds 2–4 were Python-only by brief,
leaving the build slot to ARRAY-NULL-1.

**Gates on the rebased head `1a0205c2`** (orchestrator, after round 4): `make preflight` rc 0, the whole parity suite
757 passed / 2 skipped / 12 xfailed, replace + `df_easy` + dfcore-exports pins rc 0, `make verify` rc 0. The departure
commit `6dccac47` moved the ledger to `completed/` (all five clauses PROVEN, COVERAGE_ATTESTATION present, lifecycle
check and ledger grammar clean). **Merged as `c9b03c67` (09:31), squash with `--match-head-commit`, tree-equal**
(`134f4ae2`).

| Reading | Before (`main`, release native) | After (`c9b03c67`, release native) |
|---|---|---|
| 16-entry dict RSS delta | 278.4 MiB | 4.45 MiB |
| 40-entry dict | dies in `case_when` | 4.48 MiB; pin bound `max(2 × flat, 8 MiB)` on by default |
| 200 / 400 entries | — | 4.72 / 11.05 MiB (linear-class step, residue) |
| plan build, 1 column | 398 ms at 16 entries | 0.39 ms at 16; 17 µs per entry |
| 500 int columns × 40 entries, plan only | — | 2519 → 2040 ms with the literal cache (actor, loaded box) |
| `{1: 2, 2: 3}` | `[3, 3, 3, None]` | `[2, 3, 3, None]` (Spark) |
| nine Q-R1 cells, binary column, join shapes | today's answers; binary rewritten; alias-join ambiguous | Spark's answers; binary untouched; both joined `x` replaced |
| `core.py` lines (ceiling) | 4089 | 4044 |

Residue in the completed ledger: CAST_OVERFLOW class vs the engine's cast error (P2-1), nested-field subset error class
(P2-4), `df.na.replace` missing, the N=400 RSS step, wide plan build dominated by `select`, `replace(1)` with no value
(card REPLACE-NOVALUE-1), crossJoin multi-name binding.

### 5.2 ARRAY-NULL-1

Lane `/tmp/g-arraynull`, branch `fix/array-null-1` off `e147685b`. Devin session `painted-magnesium`.

**Step 0** (06:52 → 09:08, 118 agent steps, $0, commit `29c3b30a`, no product code; audit clean). Measured on live PySpark
4.1.2 (one `local[1]` session, zulu-17) beside repark's facade and SQL door on identical explicit-schema frames:

- The facade already answers all 18 D-2 cells correctly. Its defect is plan size alone: about ×3 RSS per level, 51 MB at
  depth 12 and 269 MB at 16. In the armed red run, the depth-40 pin worker crossed its 64 MB bound at level 15.
- The SQL door answers `[4]` where Spark returns NULL for `array_append` over a NULL array. It refuses every
  Spark-spelled `array_prepend(array, element)`, because DataFusion resolves `(element, array)`.

| route | correct on the 18 cells (both doors) | depth 12 RSS | depth 40 |
|---|---|---|---|
| (a) `ScalarUDF`: DataFusion kernel, then graft the input's outer null buffer | yes | ~1.8 MB | ~1.9 MB, 55 ms collect |
| (b) Rust-built CASE | yes | 35–37 MB | allocation failure |

Route (b)'s single child reference through a plan-level alias cannot be expressed: DataFusion has no lateral
same-projection alias (`Schema error: No field named x`). **R13b-D-4:** route (a) was chosen under D-1. Steps 0 and 1
ship as one PR, a step-order decision inside one card.

**Step 1** (09:10 → 10:38, `f17a51b0`, $0; audit clean: every file inside the card's Home, maps and ledger; no size
ceiling moved; comment ban clean). Route (a) landed as `spark_array_append_udf` / `spark_array_prepend_udf`, registered
last in `collection::functions()` so the door resolves Spark's `(array, element)` order. It declares no aliases, so the
DataFusion-only `list_*` / `array_push_*` spellings keep DataFusion's kernel. The two dispatch arms live in
`function_dispatch/dispatch_json.rs` because `function_dispatch.rs` sits at its 1000-line ceiling.

| Reading (release natives, actor) | Before (`main`) | After |
|---|---|---|
| RSS delta, depth 12 | 51 MB (step 0) | ~2.0 MB |
| RSS delta / collect, depth 16 | 577 MB / 126 s | ~2.0 MB |
| depth 40 | pin worker crossed the 64 MB bound at level 15 | ~2.0 MB / 56 ms collect |
| depth 100 | — | ~2.2 MB / ~0.7 s collect |
| door `array_append` over a NULL array | `[4]` | NULL (Spark) |
| door `array_prepend(array, element)` | refused | Spark's answers |

The branch was rebased onto `c9b03c67`. Conflicts in `python/repark/tests/map.md` and `task/ledgers/staging/map.md`
were resolved by keeping both units' rows and dropping the stale REPLACE-LINEAR-1 step-0 rows; there were no conflict
markers, and the map and ledger-grammar gates passed. Draft PR **#581** was opened at `e993be6c` (head verified). The
review bed `/tmp/g-critarr` holds the branch with a release native, and `base/` points to `/tmp/g-arrbase`, `main` with
its own release native.

**Grok critic-logic** (bed at `e993be6c`; 29 turns, $0.79): **no P1, 2 P2, 2 P3.** The null graft held under every
attack: sliced inputs after `filter`/`limit`/`union`/join, absent versus all-valid null buffers, NULL inner arrays,
struct and map elements, and mixed append/prepend chains, all passing `validate(full=True)`. Both doors resolve the
arm, and no `EXPECTED_DIVERGENCES` row changed. A Python-side mutation that restored the old CASE turned only the C-002
memory pins red. The live oracle could not start inside the critic's read-only sandbox (`GatewayServer.startSocket:
Operation not permitted`), so L-1 and L-2 are not yet measured against Spark.

| id | finding | ruling | outcome |
|---|---|---|---|
| L-1 | int appended to `array<string>` recasts the array (`["1","2"]` → `[1, 2, 4]` int); letters fail at collect. Inherited from DataFusion `type_union_resolution`; also on `main` | measure on the oracle, then match Spark in a custom `coerce_types`; pin both doors | filed in follow-up 2 |
| L-2 | double into `array<int>` widens the array to double; bare SQL `1.5` and decimal-precision widening refuse; also on `main` | measure, then match; wrong result if Spark casts the element | filed in follow-up 2 |
| L-3 | facade answer pins don't single out the native arm (only C-002 does) | add a plan-shape pin | filed in follow-up 2 |
| L-4 | DataFusion-only aliases (`list_append`, `array_push_*`) still drop NULL arrays | ledger residue | filed in follow-up 2 |

**Gates on `e993be6c`** (orchestrator, 10:57 → 11:25, concurrent with the reviews on a separate clone): `make preflight` rc 0, the whole parity suite rc 0, `test_array_null_1.py` rc 0, `make verify` rc 0; CI on #581 green (9 pass, 2 skipping). The pre-existing coercion findings, not a gate, are what hold the merge.

**Grok S2-21 Rust + Python perf reviewer** (launched 11:15 on the same bed after the critic, one review lane; report `/tmp/oc-worker/g-revarr/report.md`). **Still measuring when this report was pushed at 11:30**; its verdict goes into #581's PR body and follow-up 2. Code-read conclusions so far:

- The null graft returns DataFusion's kernel result unchanged when the input has no nulls or a zero null count. When nulls exist it swaps only the validity buffer: an `Arc` clone of the bitmap, with no copy of offsets or values. DataFusion's own `MutableArrayData::extend` copy is the floor.
- A scalar element broadcasts once, matching the raw kernel. The type work is plan-time only.
- The facade's `_glue_element` makes one `call_scalar` per level, with no plan JSON re-serialization.
- The depth-40 pin's 8 MiB floor would not catch a 2× regression of the ~2.0 MiB chain. This is the same shape as #577's P2-2, and follow-up 2 should tighten it.

**R13b-D-5 — parked at the G-3 stop.** Two things did not fit before 12:00: L-1 and L-2 need a live-oracle measurement,
a Rust `coerce_types` change, a rebuild and new pins; after that, preflight, the parity suite and CI must re-run. The
branch stays pushed as **draft #581**. The follow-up brief `/tmp/oc-worker/g-arraynull/followup-2.md` (session
`painted-magnesium`) carries the rulings. ANSI-DOOR-1 was not started. Its step-0 brief is staged at
`/tmp/oc-worker/g-ansidoor/brief-0.md` and its lane at `/tmp/g-ansidoor` (`c9b03c67`, venv synced, not built).

### 5.3 Outcome at the 12:00 stop

| # | Unit | Result | PR | Merged | Rounds and cost |
|---|---|---|---|---|---|
| 1 | REPLACE-LINEAR-1 step 1 | **merged** | #577 | `c9b03c67` (tree-equal) | Devin 4 rounds on `harmonious-nova` ($0); Grok critic-logic $0.81; Grok S2-21 Python reviewer $0.81 (plus one OOM-killed round, no row) |
| 2 | ARRAY-NULL-1 steps 0+1 | **parked as draft** after its critic round; L-1/L-2 need the live oracle and a Rust coercion change | #581 | — | Devin 2 rounds on `painted-magnesium` ($0); Grok critic-logic $0.79; Grok S2-21 Rust + Python reviewer still running at the push (§5.2) |
| 3 | ANSI-DOOR-1 | not started (brief and lane staged) | — | — | — |

Card filed: REPLACE-NOVALUE-1 (Q-13b-2). STATUS.md untouched.

### 5.4 Decisions under §6 (day)

- R13b-D-4 ARRAY-NULL-1 route (a) chosen under D-1; steps 0 and 1 in one PR.
- R13b-D-5 ARRAY-NULL-1 parked as draft #581 at the stop, with follow-up 2 filed.
- R13b-D-6 Actor rounds that ran while another lane held the build slot were briefed Python-only (no cargo, maturin or
  `make verify`). The orchestrator ran the build gates afterwards.
- R13b-D-7 Orchestrator commits on #577: `46192256` (the CAP-1 parity mirror row) and `6dccac47` (the departure: ledger
  to `completed/`, attestation artifact paths corrected).
- R13b-D-8 Review beds share one native across base and branch when the diff has no Rust (#577). A Rust diff (#581)
  gets a second clone of `main` with its own release native.

### 5.5 Owner questions (day) — recommendations

- **Q-13b-5 (ARRAY-NULL-1 coercion, L-1/L-2)** Both are pre-existing on `main` (an int appended to `array<string>`
  recasts the array; double into `array<int>` widens it). Should #581 match Spark even where that changes the facade's
  answers on `main`? — **Recommend yes, measured first.** The arm is now repark's own `ScalarUDF`, so its `coerce_types`
  is the natural place, and a silent recast of existing elements is the class of answer Q-R1 retired for `replace`.
- **Q-13b-6 (next run's order)** — **Recommend** ARRAY-NULL-1 follow-up 2 → its gates and merge → ANSI-DOOR-1 step 0.
- **Q-13b-7 (critic oracle access)** The Grok read-only sandbox cannot open the Py4J gateway socket, so critics cannot
  measure Spark. — **Recommend** that critics file "oracle needed" cells in their report and the actor measures them in
  the next round (as here), rather than dropping the read-only sandbox.
- **Q-13b-8 (Grok reviewer memory)** The helper `f-grok.sh` caps a unit at 32 G, which OOM-killed the first S2-21
  reviewer, and `systemd-run --user --scope` is refused inside the Grok sandbox. — **Recommend** 64 G for S2-21 reviewer
  units, and in-process `RLIMIT_AS` caps in reviewer briefs instead of nested scopes.

### 5.6 Harness lessons (day)

- The CAP-1 parity test (`python/repark-parity/tests/test_cap_1_source_file_line_cap.py`) holds its own copy of each
  Python size baseline. Every `scripts/check_lib_py.py` ratchet must move that row too. The actor's step gates missed it;
  CI's parity-harness job caught it.
- Devin raised a size ceiling to fit a fix (4054 → 4074) and moved both baselines up with it. The audit's check of the
  ceiling files caught it, and briefs now say "ceilings only move down" with the CAP-1 mirror named.
- Pinning a Devin-resumed session on a Python-only brief kept one build lane while a Rust lane ran. Three #577
  follow-ups took 4–44 minutes each.
- `--force-with-lease` without a local tracking ref is refused as stale. The lease must name the expected remote sha
  explicitly.

## Pointers

- Up: [map.md](map.md) · Previous: [overnight-report-2026-09-13-run12b.md](overnight-report-2026-09-13-run12b.md)
- Cards: [replace-linear-1-card-2026-09-13.md](replace-linear-1-card-2026-09-13.md),
  [array-null-1-card-2026-09-13.md](array-null-1-card-2026-09-13.md),
  [ansi-door-1-card-2026-09-13.md](ansi-door-1-card-2026-09-13.md),
  [replace-novalue-1-card-2026-09-14.md](replace-novalue-1-card-2026-09-14.md)
