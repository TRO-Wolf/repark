# Charter ledger — LIVE-v3-M · the first live measurement of the v3 acceptance legs

**Date:** 2026-09-02 · **Branch:** `docs/live-v3-first-measurement` · **Base:** `origin/main`
`8c4bc55` · **Model:** claude-opus-5 (medium) · **Registry:**
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) row
`S3T-V3-1` · **North star:**
[../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md)
§3 "Live: Glue + S3 Tables v3 legs" · **Path:** STANDARD (`risk_tier: standard`; one Actor cycle).

**Retires:** moved to `completed/` in this unit's last commit; the next pickup's
`make ledger-archive` files it under `../archive/2026-09/`.

**Why now.** LIVE-v3 shipped the two legs and refused any green claim until a run existed. The
run exists. Documents that still say "unmeasured" are now the defect.

**Not in this unit:** `.github/`, any AWS or `gh` call, the fork, any Rust or engine change, the
leg code and its local pins (unchanged and still green), and `V3-ROWID-3` / unit **V3-11**.

## 1. The run this unit records

| Field | Value |
|---|---|
| Workflow | `aws-acceptance.yml`, dispatched `--ref main` (orchestrator-run) |
| Base | merged `main` `8c4bc55` |
| Run | [33635288918](https://github.com/TRO-Wolf/repark/actions/runs/33635288918), conclusion **success** |
| Job | "tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)", **success** |
| Acceptance module | `6 passed in 122.13s` — the four pre-existing legs plus the two v3 legs |
| Glue leg | `test_v3_dv_dml_maintenance_against_glue` passed at `exact_commit_counts=True`: the local numbers reproduced exactly |
| S3 Tables leg | `test_v3_dv_dml_maintenance_against_s3tables` passed at `exact_commit_counts=False`; **no** `S3T-V3-1 refused-at-create` record and no pytest warnings summary in the log, so the ACCEPTED branch ran — the service took `format-version = 3` at CREATE |
| Relaxed on S3 Tables | service commit counts only (sequence numbers, snapshot totals); row sets, `_row_id` values and every data- and delete-file count exact |

## 2. PROPOSITION LEDGER — LIVE-v3-M — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The two documents that own the answer state it: registry `S3T-V3-1` is retitled to the measured state, carries the run id, its link, base `8c4bc55` and the `6 passed in 122.13s` line, says which branch the S3 Tables leg took, keeps the decision table as one line of history, keeps its pins, and moves from BACKLOG to a dated FIXED-by-measurement disposition; the north-star §3 "Live" row is ✅ with the same dated clause and keeps the MW-10 permission sentence and every evidence link. | The tree pin reads both, and refuses `❌`, a missing run id, and a surviving BACKLOG class. | **PROVEN** | §3. `test_live_v3_docs.py::test_registry_row_records_the_measured_run`, `::test_northstar_live_row_is_green_and_dated`. Maps: `docs/map.md`, `task/roadmap/epic-term/map.md`. Citation: `python/repark-parity/tests/map.md`. |
| C-002 | The two documents that own the *question* now state the answer without importing run state: `docs/tier2-aws.md` §6's two v3 rows read "answered 2026-09-02" in one line each and carry no run id (§6's own rule sends measured state to STATUS), and `docs/design/format-v3-track.md` §7's "Nothing was measured on Glue or S3 Tables" carries a dated correction beside the existing expirable-snapshot one. | The tree pin reads §6 for both rows and asserts the run id is absent there; it reads §7 for both dated corrections. | **PROVEN** | §3. `test_live_v3_docs.py::test_tier2_runbook_lists_every_leg_and_needs_no_new_iam`, `::test_format_v3_track_claims_carry_their_dated_corrections`. Maps: `docs/map.md`, `docs/design/map.md`. Citation: `python/repark-parity/tests/map.md`. |
| C-003 | STATUS's v3 workstream says measured green with the run id instead of unmeasured, stays under its dual-pinned 25,000-byte ceiling, and keeps the `V3-ROWID-3` / V3-11 line; no pending wording survives in the registry row, the north-star row or the STATUS clause; the meta-pin is rewritten to pin the measured state and now refuses a *pending* claim where it used to refuse a green one. | The tree pin, the byte ceiling, and the two neighbouring meta-pins that read exact STATUS and north-star prefixes. | **PROVEN** | §3, §4. `test_live_v3_docs.py::test_status_names_the_measured_legs`, `::test_no_document_still_calls_the_legs_unmeasured`; STATUS 24,981 bytes; `test_plan_1_northstar_fnp_sequence.py` and `test_reg_1_registry_truth_up.py` green. Citation: `python/repark-parity/tests/map.md`. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: live-v3-first-measurement
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every claim written is read back out of the file by the tree pin, and the pin refuses the pre-state as well as the absence of the post-state (no pending wording, run id present, class moved).
      artifacts: [python/repark-parity/tests/test_live_v3_docs.py]
    - id: AT-2
      status: ATTACKED
      evidence: The two catalog shapes are recorded separately - Glue exact, S3 Tables accepted-branch with only service commit counts relaxed - rather than as one "both green" sentence that would hide which branch ran.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md]
    - id: AT-3
      status: N/A
      justification: No engine or leg code changed; row lineage and every measured count are the LIVE-v3 pins, untouched and still green.
    - id: AT-4
      status: N/A
      justification: No shared mutable state and no concurrency; this unit edits markdown and one meta-pin.
    - id: AT-5
      status: ATTACKED
      evidence: No account id, ARN, bucket or credential is written anywhere; the run is cited by its public run id and URL only, and .github/ is untouched.
      artifacts: [docs/spark-sql-iceberg-parity.md, STATUS.md]
    - id: AT-6
      status: ATTACKED
      evidence: The refusal branch is kept as one line of history rather than deleted, so a future S3 Tables refusal of format-version 3 reds the leg against this row instead of silently re-opening a closed question.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No recursion, no allocation, no runtime cost; the meta-pin reads six small files once.
    - id: AT-8
      status: N/A
      justification: No dependency, lock or toolchain change.
    - id: AT-9
      status: ATTACKED
      evidence: The document boundary is preserved under the truth-up - semantics in the registry, state in STATUS, and the tier-2 runbook asserted to carry no run id - so the measurement is single-homed rather than restated in four places.
      artifacts: [docs/tier2-aws.md, STATUS.md, python/repark-parity/tests/test_live_v3_docs.py]
    - id: AT-10
      status: ATTACKED
      evidence: Three clauses pinned; five map.md files in lockstep plus the two ledger maps; mutation 8 red of 8, each restored and re-run green.
      artifacts: [python/repark-parity/tests/test_live_v3_docs.py, python/repark-parity/tests/map.md]
  complete: true
```

## 3. Document truth-up (C-001, C-002, C-003)

| Document | Was | Now |
|---|---|---|
| `docs/spark-sql-iceberg-parity.md` §7 `S3T-V3-1` | "the live v3 legs are wired (2026-09-02); the first measurement is pending"; **BACKLOG**; "nothing has run against AWS yet"; oracle "not yet run" | "FIXED (LIVE-v3-M, 2026-09-02): both live v3 legs are green; S3 Tables accepts `format-version = 3` at CREATE"; FIXED by measurement; the run, its link, base `8c4bc55`, `6 passed in 122.13s`, which branch ran and what was relaxed; oracle is the run; pins unchanged; the refusal branch kept as one line of history |
| `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md` §3 "Live: Glue + S3 Tables v3 legs" | ❌ "still nothing measured live on v3"; the full statement sequence; "the first … run answers two questions"; "the v3 live legs themselves stay unmeasured" | ✅ with the dated clause (run 33635288918, S3 Tables accepts v3 at CREATE, Glue exact), the sequence compacted to a pointer at the registry row and the local pin; MW-10 sentence and both evidence links kept |
| `STATUS.md` v3 workstream | "the two live v3 legs are wired … both stay **unmeasured** until the nightly `aws-acceptance` run" | "both live v3 legs green on `aws-acceptance` run 33635288918 — S3 Tables takes `format-version = 3` at CREATE, Glue reproduces the local numbers (`S3T-V3-1`)"; 24,959 → 24,981 bytes |
| `docs/tier2-aws.md` §6 (two v3 rows) | "whether Glue reproduces the local v3 numbers"; "whether S3 Tables accepts `format-version = 3` at CREATE" | "answered 2026-09-02": Glue reproduces them exactly; S3 Tables accepts v3 at CREATE and the leg runs the accepted branch, the refusal branch staying wired and unused. No run id — §6's own rule keeps run state in STATUS |
| `docs/design/format-v3-track.md` §7 | "**Nothing was measured on Glue or S3 Tables.**" (undated, now false) | the same sentence with a dated correction beside the existing expirable-snapshot one, citing the run and registry `S3T-V3-1` |
| `python/repark-parity/tests/test_live_v3_docs.py` | seven tests refusing any green claim | eight tests pinning the measured state and refusing a *pending* claim; `V3-ROWID-3` assertions unchanged; the tier-2 rows and the leg-`def` existence checks kept |

The §3 gate paragraph ("v1.0 tags when every row above is ✅ or its residual is a dated DECLARED
row … each with a pin") is now satisfied **for this row**: it is ✅ and pinned. No other row was
read or touched, so this unit makes no claim about the gate as a whole.

## 4. Mutation (C-001, C-003)

Each mutation applied alone to the document, the pin re-run, the file restored.

| # | Mutation | Result |
|---|---|---|
| 1 | north-star row `✅` → `❌` | red |
| 2 | run id dropped from the north-star row | red |
| 3 | run id dropped from the registry row | red |
| 4 | registry heading reverted to "the first measurement is pending" | red |
| 5 | registry Rationale reverted to `BACKLOG` | red |
| 6 | STATUS clause reverted to "**unmeasured** until the nightly …" | red |
| 7 | the run id added to `docs/tier2-aws.md` §6 | red |
| 8 | `format-v3-track.md` §7 correction removed | red |

8 red of 8.

## 5. Gates

| Gate | Result |
|---|---|
| `.venv/bin/python -m pytest python/repark-parity/tests/test_live_v3_docs.py python/repark-parity/tests/test_plan_1_northstar_fnp_sequence.py python/repark-parity/tests/test_reg_1_registry_truth_up.py -q` | 20 passed |
| `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction` | clean |
| `python3 scripts/ledger_lifecycle.py check --base 8c4bc55` | clean |
| `uv run --no-sync ruff check python` | clean |
| `uv run --no-sync ruff format --check python` | clean |
