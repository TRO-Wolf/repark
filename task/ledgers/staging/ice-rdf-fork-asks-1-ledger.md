# Unit ledger — ICE-RDF-FORK-ASKS-1 · the 13 RDF strict xfails re-measured on RP-23, and their fork asks named

**Date:** 2026-09-17 · **Branch:** `docs/ice-rdf-fork-asks-1` · **Base:** `126b8285` ·
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
`risk_tier: standard`.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card C-6 predicted the RP-23 fork pin (`4151b488`, rolling writer splits
rewrite output at the target file size) would turn some of
`python/repark/tests/test_ice_rdf_options_1.py`'s thirteen strict xfails into XPASSes.
The orchestrator measured it 2026-09-17 on a release native from main `71482620`
(RP-23 is on main) and the prediction was wrong: zero XPASS. Nothing is un-marked,
`ICE-RDF-DANGLE-2` stays a fork ask, and the three other reasons — until now only
reason strings in `_VALUE_XFAIL` / `_SNAPSHOT_XFAIL` — become named registry rows.

**Not in this unit:** the test file, the fixture, STATUS.md, `briefs/next-sequence.md`,
anything under `crates/`, any native build, any pytest run by this lane (card: record
the measurement, do not re-derive it).

**The measurement (orchestrator, 2026-09-17, recorded verbatim — this lane's red evidence).**

```
.venv/bin/python -m pytest python/repark/tests/test_ice_rdf_options_1.py \
    python/repark/tests/test_rdf_schema_evo_1.py -q -p no:cacheprovider -rX
129 passed, 1 skipped, 13 xfailed in 115.79s
```

**Red-first note.** The card forbids this lane from running pytest, so the run above is
the red: all thirteen strict xfails still fail on the RP-23 tree, zero XPASS. This lane
verified the thirteen marks statically on the base tree instead: 7 entries in
`_VALUE_XFAIL` (`target_small`, `max_group_size`, `partial_progress_groups`,
`delete_file_threshold`, `remove_dangling`, `rpd_rewrite_all`, `rpd_min_input_files_1`),
3 in `_SNAPSHOT_XFAIL` (`partial_progress_groups`, `rpd_rewrite_all`,
`rpd_min_input_files_1`), 2 in `test_option_cell_removed_counts` (`remove_dangling`,
`delete_file_threshold`, reason `ICE-RDF-DANGLE-2`), and
`test_residue_matches_spark_zero_delete_files` (reason `ICE-RDF-DANGLE-2`) — 7 + 3 +
2 + 1 = 13, matching the measured `13 xfailed`. Prefix variance recorded honestly:
`target_small`'s reason string opens `FORK-WRITE-GRANULARITY` while the card groups it
under `FORK-GROUP-GRANULARITY`; row `ICE-RDF-GRANULARITY-1` quotes the strings verbatim
and covers all three cells. No reason string names a fork file, so no row names one.

## PROPOSITION LEDGER — ICE-RDF-FORK-ASKS-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Row `ICE-RDF-GRANULARITY-1` names the 4 granularity xfails with RePark-vs-Spark numbers from the reason strings and the fixture cells. | Row text quotes the strings and cites `target_small`, `max_group_size`, `partial_progress_groups` cells by name; status OPEN 2026-09-17 fork ask; dated re-measure line present. | **PROVEN** | `docs/spark-sql-iceberg-parity.md`: RePark 8→2 / 8→8-added vs Spark 8→4 (`added_data_files_count = 4`, bytes 9229), 11 vs 10 snapshots on the groups cell; the 4 pins (`test_option_cell_values[target_small\|max_group_size\|partial_progress_groups]`, `test_option_cell_snapshots[partial_progress_groups]`); re-measured line with `4151b488`. Red: all 4 in the 13-nihil run above. |
| C-002 | Row `ICE-RDF-COW-BYTES-1` names the 2 byte-accounting xfails with RePark-vs-Spark numbers from the reason strings and the fixture cells. | Row text quotes the strings and cites `delete_file_threshold`, `remove_dangling` cells by name; status OPEN 2026-09-17 fork ask; dated re-measure line present. | **PROVEN** | Registry: 5869 vs 7592 and 11878 vs 13522 (1644-byte DELETE-written file) vs fixture bytes 4544 / 9229 with `removed_delete_files_count = 0`; the 2 pins (`test_option_cell_values[delete_file_threshold\|remove_dangling]`); re-measured line with `4151b488`. Red: both in the run above. |
| C-003 | Row `ICE-RDF-RPD-COMMITS-1` names the 4 RPD xfails with RePark-vs-Spark numbers from the reason strings and the fixture cells. | Row text quotes the strings and cites `rpd_rewrite_all`, `rpd_min_input_files_1` cells by name; status OPEN 2026-09-17 fork ask; dated re-measure line present. | **PROVEN** | Registry: fork 8→2 per-group commits (11 snapshots) vs Spark 8→8 one commit (rewritten 8 / added 8, 10 snapshots, 16 data + 8 deletes); the 4 pins (value + snapshot cells); re-measured line with `4151b488`. Red: all 4 in the run above. |
| C-004 | Row `ICE-RDF-DANGLE-2` carries the dated RP-23 re-measure line; its 3 xfails stay red. | Dated line present in the row naming `4151b488` and the rolling-writer non-closure. | **PROVEN** | Row now ends its Rationale with "Re-measured 2026-09-17 on RP-23 (`4151b488`): still xfailed; the rolling writer's target-size split did not close it." Red: the 2 removed-count cells plus the residue zero cell in the run above. |
| C-005 | Row `ICE-RDF-OPTIONS-1` points at the four named rows and keeps the count of thirteen. | Sentence replaced by pointers; the number thirteen still stated; 4 + 2 + 4 + 3 = 13 auditable against the test file. | **PROVEN** | Registry sentence now names `ICE-RDF-GRANULARITY-1` (3 value + 1 snapshot), `ICE-RDF-COW-BYTES-1` (2), `ICE-RDF-RPD-COMMITS-1` (2 value + 2 snapshot), `ICE-RDF-DANGLE-2` (2 removed + 1 residue). No other text in the row changed. |
| C-006 | `python/repark/tests/map.md` names the four registry rows on the `test_ice_rdf_options_1.py` entry. | Entry extended with the four row names and the dated re-measure note. | **PROVEN** | Entry now ends with the four-row pointer plus "(re-measured 2026-09-17 on RP-23 `4151b488`: still xfailed, zero XPASS)". |
| C-007 | Gates green: `make check-map-sync`, the pre-commit hook on commit, and the no-comments fence. | Both gates exit 0; the fence grep prints nothing. | **PROVEN** | `make check-map-sync` exit 0; fence `git diff --cached -- '*.rs' '*.py' '*.toml' '*.sh' '*.yml' \| grep -P '^\+\\s*(//|#(?! noqa))'` printed nothing (this unit touches only `.md` files); the commit-time hook passed. |

COVERAGE_ATTESTATION:
  pr_unit: ice-rdf-fork-asks-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause C-001..C-007 walked against the read test file and fixture cells, not a paraphrase — all 13 xfail marks mapped to exactly one of the four rows (7 value, 3 snapshot, 2 removed-count, 1 residue), every quoted number read from its reason string or its named fixture cell.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_rdf_options_1.py, python/repark/tests/ice_rdf_options_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries checked — the thirteen count kept in ICE-RDF-OPTIONS-1 (4+2+4+3), the target_small FORK-WRITE-GRANULARITY prefix variance recorded instead of normalised, no fork file named because no reason names one, no test/status/crates file touched.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/map.md]
    - id: AT-3
      status: ATTACKED
      evidence: No Spark, polars, or DataFusion behaviour asserted beyond the orchestrator's recorded run and the committed fixture — the card forbids re-derivation and this lane ran no pytest, cargo, or maturin.
      artifacts: [task/ledgers/staging/ice-rdf-fork-asks-1-ledger.md]
