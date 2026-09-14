# Overnight report — run 13b of 2026-09-14 (expressions)

**Session:** one Opus orchestrator (overnight-13b), 2026-09-14 05:19 → 06:30 local, beside run 13 (overnight-13, f- lanes).
**Grants:** G-1, G-2, G-3 (stop when the list is exhausted or by 06:30 local), G-4 Devin actor + Grok critic/reviewers
(no Muse, no GLM), G-5. STATUS.md, tags and the release pipeline untouched. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Owner rulings applied:** Q-R1 YES (the replace rewrite matches Spark on the nine divergent cells), Q-B1 (the conf key
`repark.cache.retained_bytes` stays), Q-B2 (the multi-partition prefetch is its own later card, not opened).

## 1. Outcome

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

## Pointers

- Up: [map.md](map.md) · Previous: [overnight-report-2026-09-13-run12b.md](overnight-report-2026-09-13-run12b.md)
- Cards: [replace-linear-1-card-2026-09-13.md](replace-linear-1-card-2026-09-13.md),
  [array-null-1-card-2026-09-13.md](array-null-1-card-2026-09-13.md),
  [ansi-door-1-card-2026-09-13.md](ansi-door-1-card-2026-09-13.md)
