# Charter ledger — ICE-PAGE-PRUNE-1 · the RePark pins (Muse round 1, run 24a)

**Date:** 2026-09-19 · **Branch:** `perf/ice-page-prune-1` · **Base:** `5ceeb2cc` · **Model:**
muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Why now.** Slate unit 1 of
[task/roadmap/mid-term/ice-read-perf-slate-2026-09-18.md](../../roadmap/mid-term/ice-read-perf-slate-2026-09-18.md):
the fork unit F-PAGE-PRUNE-1 turns on page-level row selection on the
DataFusion scan, and these pins must pass on today's pruning-off engine so
they stay green when pruning turns on. This round writes tests, fixtures, the
recorder and docs only — no product code.

**Not in this unit:** the fork pin bump, any product-code change, `STATUS.md`,
size-ceiling or guard-exception changes, Python-side decisions.

**Oracle.** Live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0,
`local[4]`, UTC. The fork lane recorded
`/tmp/oc-worker/pb-oracle/page_prune_truth.json` from its recorder
(`/tmp/oc-worker/pb-oracle/record_page_prune.py`); this round's recorder
re-derives every answer from the same seeds, properties and predicates and
matches that truth cell for cell (run log `/tmp/pb-record.log`: "live answers
agree with /tmp/oc-worker/pb-oracle/page_prune_truth.json cell for cell").
Re-running the recorder verifies byte-for-byte the answers; `--rewrite`
re-records.

## PROPOSITION LEDGER — ICE-PAGE-PRUNE-1 — 2026-09-19

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The recorder builds the five page-prune tables on live Spark with the fork lane's seeds, properties and predicates, matches the fork truth cell for cell, rewrites each table with `CALL …rewrite_table_path` to `/tmp/repark-ice-page-prune-1/wh`, and writes the compacted `truth.json` (id runs, row-id and sequence segments on v3, `expand_cell` decoding). | `_record_ice_page_prune_1.py` `--rewrite` rc 0 plus the fork-agreement line; verify-mode rc 0. | PROVEN | `--rewrite` rc 0, "live answers agree … cell for cell"; verify-mode rc 0, "oracle matches the checked-in truth"; `truth.json` 66,161 bytes against the fork 2.35 MB. pins: ice-page-prune-1/C-001 |
| C-002 | The five canonical tables are frozen as live-files-only fixtures (600,773 bytes) with a fixture map; the pin test materializes them back to the canonical prefix under a lock and adopts them with `register_table`. | Fixture byte total; offline pins green on adopted tables. | PROVEN | 600,773 bytes, 4/4/7/9/8 data files per table, no `.crc` sidecars; 4 offline tests green on adopted warehouses. pins: ice-page-prune-1/C-002 |
| C-003 | The SQL door equals Spark on every fixture predicate, lineage, and unfiltered cell — except the three bare-decimal `d`-range cells, which refuse loud under ICE-NAN-DECIMAL-LITERAL-1 (BACKLOG) and pin the `Overflowing on NaN` needle. | `test_sql_door_answers_every_recorded_cell` and `test_sql_door_bare_decimal_ranges_refuse_loud` green. | PROVEN | Both green; the three cells failed red with the decimal needle before the split, and the frame door answers them with double bounds. pins: ice-page-prune-1/C-003 |
| C-004 | The DataFrame door equals Spark on every fixture cell (ids; lineage is SQL-door-only — the frame schema does not resolve `_row_id`, measured). | `test_dataframe_door_answers_every_recorded_cell` green. | PROVEN | Green on all 95 truth cells plus 5 unfiltered; the lineage-in-select refusal was measured on base_v3 before scoping to ids. pins: ice-page-prune-1/C-004 |
| C-005 | RePark-written v2 tables (300,000 rows, DELETE + UPDATE) answer every selective predicate with the unfiltered read filtered in Python, on both doors. | `test_repark_written_v2_answers_its_own_reads` green. | PROVEN | Green: 8 predicates × 2 doors over ~280k survivors, NaN folded to a token for comparison. pins: ice-page-prune-1/C-005 |
| C-006 | RePark-written v3 tables do the same with every surviving row keeping its unfiltered `_row_id` and `_last_updated_sequence_number` on both doors (frame door on data columns, lineage on the SQL door). | `test_repark_written_v3_answers_its_own_reads_with_stable_lineage` green. | PROVEN | Green; the frame lineage refusal from C-004 recurs on RePark-written v3 and is scoped the same way. pins: ice-page-prune-1/C-006 |
| C-007 | RePark's writer emits page indexes on every column chunk, with stated page counts. | `test_repark_written_files_carry_page_indexes` green; counts in the tests map row. | PROVEN | Green over data plus position-delete files; measured per column chunk on full 65,536-row files: four pages on narrow columns, six on the wide 86-char string column, one to two on remnant and delete files; Spark fixture files carry five plus a one-page tail group. pins: ice-page-prune-1/C-007 |
| C-008 | `REPARK_PARITY_LIVE=1` rebuilds the five tables on live Spark 4.1.2 and re-derives every truth cell, then RePark matches truth on the adopted live tables. | `test_live_grid_replays_spark` green under the JVM lock. | PROVEN | Green in 35 s; no oracle drift on any of the 95 cells; the un-rewritten live `del_v2` reads clean on RePark, isolating the C-003 fixture refusal to rewrite-stale manifest sizes. pins: ice-page-prune-1/C-008 |

## Red-first log

Each divergence below failed red on today's engine first, then pinned green in
its disposed form. No pin passed before its red run.

- **Bare-decimal `d` ranges (C-003):** `d < 100.0`, `d > 1000.0`,
  `NOT (d < 1000.0)` on the NaN-holding double column failed with `Cannot cast
  to Decimal128(30, 15). Overflowing on NaN` — the ICE-NAN-DECIMAL-LITERAL-1
  BACKLOG shape — while the frame door with double bounds answered Spark's
  rows exactly (67 / 1333 / 1333 on `base_v2`).
- **Rewritten `del_v2` (C-003):** every read failed with `Failed to load
  Parquet metadata` (sub-message races: corrupt footer or short read).
  Measured cause: `rewrite_table_path` rewrote the position-delete bytes
  (embedded file paths, new uuid) without updating `file_size_in_bytes`
  (manifest 1705/1587/1578 against files 1711/1572/1563); RePark sizes reads
  from the manifest, Spark reads the footer. Spark reads the rewritten table
  clean (1903 rows), and the un-rewritten live `del_v2` reads clean on RePark
  (C-008) — the refusal is rewrite-staleness, not position deletes as such.
- **Frame lineage (C-004/C-006):** `select("_row_id")` on the DataFrame door
  refuses `cannot be resolved` on adopted and RePark-written v3 alike
  (SQL-door-only today); both frame legs pin ids.
- **Recorder encoding:** `_last_updated_sequence_number` varies per row on
  `del_v3` ({1, 5}), so lineage encodes as segments, not one sequence per
  table; segments additionally require consecutive ids after a phantom-row
  fault was caught on a hand case before recording.

## Open questions for the orchestrator

- **Q-1 (RULING): del_v2 disposition.** The rewritten `del_v2` fixture pins a
  loud refusal while Spark answers. Keep the divergence pin (this round's
  choice — the recorded answers stay the fix target), or re-record the delete
  tables direct-at-canonical without `rewrite_table_path` so the pins assert
  rows? The live leg proves RePark reads exact-size v2 position deletes.
- **Q-2 (RULING): frame-door lineage.** Lineage columns stay SQL-door-only in
  this round's pins. File a product gap for frame-door `_row_id`, or leave it
  unfiled as known engine scope?

```
COVERAGE_ATTESTATION:
  pr_unit: ice-page-prune-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the slate unit-1 shapes (lineage, position/equality/deletion-vector deletes, evolution, promotion, nulls, NaN, truncated strings) and the round brief steps 1-6; the Q-1/Q-2 dispositions went to the orchestrator as open questions, not silent pins.
      artifacts: [task/ledgers/staging/ice-page-prune-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: The full predicate grid on five tables (20 base/del queries, 10 evolution queries, unfiltered, v2 and v3) on the SQL door with lineage and Arrow int64 checks, the DataFrame door on ids, plus 8 self-consistency predicates on two 300k-row RePark-written tables.
      artifacts: [python/repark/tests/test_ice_page_prune_1.py]
    - id: AT-3
      status: N/A
      justification: No product code changed in this round (tests, fixtures, recorder, maps, ledger, docs only); the Rust panic and async bans are untouched.
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; fixture copies run under a directory lock shared with the NaN pattern.
    - id: AT-5
      status: N/A
      justification: Local files only; the live tier uses the repo live-cell rules (private catalog, single-statement seeds, no env mutation) and never touches AWS.
    - id: AT-6
      status: ATTACKED
      evidence: Red-first throughout: the decimal cells, the del_v2 reads and the frame lineage select all failed red before their disposed pins; the evo v9/v10 misread during development was a scratch-glob error, not a pin, corrected to the int-sorted newest metadata the test uses.
      artifacts: [python/repark/tests/test_ice_page_prune_1.py]
    - id: AT-7
      status: ATTACKED
      evidence: Offline file green (7 passed), live leg green under the JVM lock (1 passed, 35 s), ruff check and format clean on both new modules, comment-ban hits=0, the four doc gates green.
      artifacts: [python/repark/tests/test_ice_page_prune_1.py, python/repark/tests/_record_ice_page_prune_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The only Spark oracle is the recorded truth: the recorder re-derives every cell from live Spark 4.1.2, matches the fork lane truth cell for cell, and fails on drift; hand-computed values appear nowhere except Python-side mirrors of engine semantics in the self-consistency leg.
      artifacts: [python/repark/tests/_record_ice_page_prune_1.py, /tmp/oc-worker/pb-oracle/page_prune_truth.json]
    - id: AT-9
      status: N/A
      justification: No registry, example, or public-name change in this round; error needles pin existing contracts (ICE-NAN-DECIMAL-LITERAL-1, the manifest-size refusal).
    - id: AT-10
      status: ATTACKED
      evidence: The del_v2 and decimal pins assert loud-failure needles with recorded fix targets rather than absorbing the divergence; the frame door asserts ids where lineage is unavailable, never NULL-filled rows.
      artifacts: [python/repark/tests/test_ice_page_prune_1.py]
  complete: true
```
