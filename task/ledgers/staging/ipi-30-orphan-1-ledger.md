# Unit ledger — IPI-30-ORPHAN-1 · remove_orphan_files Spark parity: Python pins, runbook, registry (IPI-30 orph-r2)

**Date:** 2026-09-22 · **Branch:** `fix/proc-orphan-spark-parity` · **Base:** `885b2271` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** R1 (commits `e9a6caa8`, `0ad67659`, `f8be1911`) made `CALL remove_orphan_files` answer like Spark 4.1.2: a bare `older_than` is now minus 3 days, and `dry_run` defaults to FALSE, so a bare call DELETES. Every Python pin, the MW-7 runbook step, the user guide and the parity registry still described the old OD-2 posture (cutoff required, dry-run default). This round rewrites them to Spark's answers, passes `dry_run => true` explicitly where a listing is meant, and retires registry rows ORPHAN-1 and ORPHAN-2 per owner ruling Q-55-2 (option A). The 24 h floor for a caller-supplied `older_than` stays: it is Spark parity.

**Not in this unit:** any `.rs` change (the Rust half is R1's, done); Cargo.toml / Cargo.lock; STATUS.md; `task/roadmap` (the north-star §3.1 surface-residuals row 14 still names ORPHAN-1/ORPHAN-2 — stale, listed under Observed); docs/cutover, docs/design, docs/perf (stale lines listed, not edited); `docs/history/**` and `task/ledgers/archive/**` (immutable).

## PROPOSITION LEDGER — IPI-30-ORPHAN-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A bare `CALL … remove_orphan_files(table => …)` runs with `older_than` at now minus 3 days: the planted 10-day-old orphan is listed, the planted 1-day-old orphan is not. | Rust `call_remove_orphan_files_bare_call_deletes_with_sparks_three_day_default`; Python `test_remove_orphan_files_defaults_older_than_to_three_days`. | **PROVEN** | Both doors list exactly the old orphan (`orphan-old.parquet` on the Python door, `orphan-old.parquet` on the Rust door). |
| C-002 | A bare call deletes: `dry_run` defaults to false, so the listed old orphan is gone from disk afterwards and the live rows still read. | Rust `call_remove_orphan_files_bare_call_deletes_with_sparks_three_day_default` plus `call_remove_orphan_files_armed_deletes_orphans_and_nothing_else`; Python `test_remove_orphan_files_defaults_older_than_to_three_days` plus `test_remove_orphan_files_deletes_by_default`. | **PROVEN** | Both doors remove exactly the listed orphans and reread the live rows (1 row on the Python fixtures, `SELECT *` count on the Rust ones). |
| C-003 | `dry_run => true` lists the same rows and deletes nothing. | Rust `call_remove_orphan_files_dry_run_lists_without_deleting`; Python `test_remove_orphan_files_deletes_by_default` (dry arm first, then the bare arm). | **PROVEN** | Both doors list both planted orphans with the files still on disk, then the bare arm deletes the same set. |
| C-004 | A caller-supplied `older_than` inside 24 hours refuses with the floor text; just outside it the call runs. | Rust `call_remove_orphan_files_enforces_sparks_twenty_four_hour_floor`; Python `test_remove_orphan_files_floor_matches_spark`. | **PROVEN** | `now` and `now - 23h` refuse with "less than 24 hours" and delete nothing; `now - 25h` runs, on both doors. |
| C-005 | `max_concurrent_deletes => 2` is accepted and the dry listing is unchanged. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments`. | **PROVEN** | The loop arm lists the one planted orphan with the table directory byte-identical before and after. |
| C-006 | `stream_results => true` is accepted and the dry listing is unchanged. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments`. | **PROVEN** | Same loop-arm shape as C-005. |
| C-007 | `prefix_listing => true` is accepted and the dry listing is unchanged. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments`. | **PROVEN** | Same loop-arm shape as C-005. |
| C-008 | `prefix_mismatch_mode => 'IGNORE'` is accepted and the dry listing is unchanged. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments`. | **PROVEN** | Same loop-arm shape as C-005. |
| C-009 | `equal_schemes` and `equal_authorities` maps are accepted and the dry listing is unchanged. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments` (one arm, both maps). | **PROVEN** | `equal_schemes => map('file','file'), equal_authorities => map('a','a')` lists the planted orphan with nothing moved. |
| C-010 | `file_list_view => 'v'` still refuses `NotImplemented` naming the v1 deferral, deleting nothing. | Rust `call_remove_orphan_files_accepts_sparks_optional_arguments` (trailing refusal arm, removed 2026-09-22). | **REJECTED** (superseded 2026-09-22 by ipi-30-orphan-guard-narrow-1 C-005 / C-006) | Was proven by the NotImplemented refusal; `file_list_view` is now accepted, so the proposition no longer holds and its pin arm is gone. |
| C-011 | Near misses refuse with their exact texts and touch nothing: a bogus `prefix_mismatch_mode`, a non-map `equal_schemes`, a quoted `stream_results`, an in-floor `older_than`, a quoted `dry_run`, and an unknown argument. | Rust `call_remove_orphan_files_near_misses_still_refuse` plus `call_remove_orphan_files_refuses_a_quoted_dry_run`. | **PROVEN** | All six messages asserted by equality; the table directory is byte-identical before and after every refused call. |
| C-012 | The runbook orphan step passes `dry_run => true` explicitly: the driver's SQL, the guide's printed block and the mw7/mw8 pins agree. | `maintenance_sequence` orphan SQL; `test_maintenance_is_the_charters_sequence`; `test_the_runbook_runs_the_documented_procedures_in_order`; `test_the_printed_cycle_matches_the_sequence_the_engine_runs`. | **PROVEN** | All three asserts read `"dry_run => true"` on the driven and printed sides; the printed-cycle loop also pins the literal value `true` against the driver's. |

## Design notes (why, not what)

- Pins cite this ledger from test docstrings, never from `# pins:` comment lines: the owner's comment ban counts an added `#` line as a hit, while the ledger grammar reads `pins:` citations from any tracked file under `crates/`, `python/` or `scripts/`, docstrings included.
- The mw8 printed-side assert compares the parsed `dry_run` VALUE to `"true"` rather than testing `"dry_run => true" in` the argument-name list, which can never match: the loop above it already pins every literal value, and the targeted assert names step 6.
- `_SURFACE_RESIDUALS` in `test_v1_gate_docs.py` is untouched: its row-14 residuals (`ORPHAN-1`, `ORPHAN-2`) still match the north-star text, which this unit may not edit; the test stays green and the stale row is listed under Observed.

## Observed, out of unit

- `task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md:138` — the §3.1 surface-residuals row "14 · expiry / orphans" still reads "`ORPHAN-1` (`older_than` required) and `ORPHAN-2` (dry-run default with Spark's result shape)" with class "DECLARED, owner decision OD-2 (ruled 2026-08-21)". Both rows retired 2026-09-22 by Q-55-2.
- `task/roadmap/epic-term/map.md:70` — names `ORPHAN-1/2` among the tabled surface residuals.
- `task/roadmap/mid-term/ice-parity-inventory-2026-09-19.md:160` — inventory row 47 cites ORPHAN-1, ORPHAN-2 as the registry home of the orphan-defaults cell.
- `task/roadmap/mid-term/overnight-report-2026-09-14-run14.md:152` — the C5 note cites ORPHAN-2 for the `dry_run => false` migration edit.
- `docs/cutover/inventory.md:24` and `docs/cutover/production-iceberg-status-2026-09-14.md` (lines 80, 226, 337, 365, 409, 562, 635) — describe the required-`older_than` / dry-run-default posture.
- `docs/design/v3-statement-coverage.md:185`, `docs/design/v1-0-api-review-2026-09-02.md:54` and `docs/design/v1-0-api-review-2026-09-02.json:1448-1449` — cite ORPHAN-1/ORPHAN-2 as open rows.
- `docs/perf/engine-iceberg-analysis-2026-09-04.md:99` — cites ORPHAN-2 for the 24 h refusal (the floor itself is unchanged).
- `python/repark/tests/test_maintenance_call.py` carried no planting helper before this round: `_plant_orphan` and `_orphan_names` are new, modelled on the `plant_orphans` / `orphan_locations` pair in `call_orphan.rs`.

## Close

All twelve clauses C-001…C-012 were PROVEN by pin; C-010 has since been superseded (REJECTED above); the attestation below covers the ten categories for the whole unit.

```text
COVERAGE_ATTESTATION:
  pr_unit: ipi-30-orphan-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Walked all twelve clauses against the diff and the runs. Every clause cites a Rust pin and, where the door differs, a Python pin; the runbook clause cites the driver SQL, the guide block and both pins.
      artifacts: [crates/repark-spark/src/tests/call_orphan.rs, python/repark/tests/test_maintenance_call.py, python/repark-parity/bench/mw7/measure.py, python/repark/tests/test_mw8_runbook.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised on both doors: 10-day-old vs 1-day-old orphans against the 3-day default, 23h vs 25h against the 24h floor, the zero-row warehouse, a bogus mode, non-map schemes, quoted booleans and an unknown argument.
      artifacts: [call_remove_orphan_files_bare_call_deletes_with_sparks_three_day_default, call_remove_orphan_files_near_misses_still_refuse, test_remove_orphan_files_defaults_older_than_to_three_days]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal pins class plus exact text and asserts the table directory is byte-identical before and after; the armed run asserts EXACTLY the planted orphans were removed and the table still reads.
      artifacts: [call_remove_orphan_files_near_misses_still_refuse, call_remove_orphan_files_armed_deletes_orphans_and_nothing_else]
    - id: AT-4
      status: N/A
      justification: No shared state, no lock, no ordering assumption beyond run-before-assert; the new Python helpers are pure file writes plus one CALL each.
    - id: AT-5
      status: N/A
      justification: No privilege or credential change; planted paths stay under the test table's own data directory and the shared-CTAS-root refusal is untouched.
    - id: AT-6
      status: ATTACKED
      evidence: Result schemas and rows match the Spark 4.1.2 plus Iceberg 1.11 oracle on the 2026-09-22 scoreboard (the bare call lists and deletes the old orphan; dry_run lists and keeps it); the registry rows flipped to the retired wording with the renamed pins in the same change.
      artifacts: [docs/spark-sql-iceberg-parity.md, test_remove_orphan_files_deletes_by_default]
    - id: AT-7
      status: N/A
      justification: Two planted files per pin; no loop, no growth, nothing system-breaking.
    - id: AT-8
      status: ATTACKED
      evidence: The driver orphan SQL and the guide printed block are compared argument-for-argument (names, order, literal values) by the mw8 pin, so the explicit dry_run cannot drift on one side; Cargo.toml untouched.
      artifacts: [test_the_printed_cycle_matches_the_sequence_the_engine_runs, python/repark-parity/bench/mw7/measure.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every new failure names the argument and the rule in one line, pinned by equality through the existing Plan and NotImplemented channels; the step-6 asserts carry the Spark-default-deletes message.
      artifacts: [call_remove_orphan_files_near_misses_still_refuse, test_maintenance_is_the_charters_sequence]
    - id: AT-10
      status: ATTACKED
      evidence: Every clause has a named pin on each door it touches, and both rewritten Python pins went RED against the stale pre-R1 build (AnalysisException on the bare call) before passing on the rebuilt one, so the suite discriminates the new behaviour. No new branch lacks a nameable input: planting, listing shape and on-disk effects are each asserted.
      artifacts: [python/repark/tests/test_maintenance_call.py, python/repark/tests/test_mw7_scale_smoke.py, python/repark/tests/test_mw8_runbook.py]
```
