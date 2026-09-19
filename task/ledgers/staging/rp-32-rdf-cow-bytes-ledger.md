# Unit ledger — RP-32-RDF-COW-BYTES · the flips fork #301 causes, each tied to a Spark cell

## Round 1 (2026-09-19)

**Date:** 2026-09-19 · **Branch:** `chore/rp-32-fork-pin` · **Base:** `dd1fa0b6` (RP-32 pin bump)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** — test-only unit: one recorder, one fixture, five
re-pinned tests, five registry rows, maps. No product code changes.
**Fork:** `TRO-Wolf/iceberg-rust` #301 (F-RDF-COW-BYTES-1), in the workspace pin since the RP-32
bump `dd1fa0b6`: `rewrite_data_files` keeps parquet position deletes that still apply (removal
by reference is Puffin-DV only), and a merging commit retires delete files older than every
live data file, as Java's `dropDeleteFilesOlderThan`.
**Registry:** the `DELETE-COW-BYTES` reason namespace (closed), `ICE-RDF-COW-BYTES-1`,
`ICE-RDF-DANGLE-2`, `RDF-DANGLING-1`, `RDF-1`, `ICE-RDF-OPTIONS-1`.

**Why now.** The bump alone reds CI exactly where the fork changed meaning: two Rust dangling
tests (removed 1 vs 0), two COW-BYTES value cells plus two removed-count cells plus one
residue cell as strict XPASS, and three changed-meaning asserts (`deletes == 2` twice,
`removed == 1` in the mw7 runbook). Every line resolves in this round against a Spark cell.

**Not in this unit:** `STATUS.md`, `Cargo.toml` / `Cargo.lock` (the bump is done), any
product-code change, the `RDF-1` two-referent residue (F-16 residue 2, still pinned by the
unchanged partition control), the `DELETE`-path divergence below (R-2), the rating-residue
docs outside the parity registry.

## The measured oracle

Six Spark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` cells (banner 4.1.2, UTC) from the
committed recorder `_record_rp32_rdf_cow_bytes.py`, for the programs no recorded cell covered
(see `rp32_rdf_cow_bytes_spark_oracle.json`): `rwd_remove_dangling` (six single-row files, one
DELETE, options-map remove-dangling — removed 0, 5→1 files, row 2 gone) with `rwd_legacy_flag`
(the legacy-flag spelling is RePark-only: `PARSE_SYNTAX_ERROR`); `rfs_merge_rewrite` (six
single-row files, one MERGE update, plain rewrite — removed 0, 7→1 data files, the delete
survives, merged row kept); `dead_file` (one 2,000-row file fully shadowed by a MERGE, RPD
no-op, rewrite — removed 0, one data plus one delete file, seed gone); `null_map_key`
(NULL-map-key-only rewrite refused `INTERNAL_ERROR`) with `null_map_key_legacy` (the
NULL-plus-flag spelling is RePark-only: `PARSE_SYNTAX_ERROR`). All paths are basenames, so the
fixture carries no local paths. Everything else pins against the recorded
`ice_rdf_options_1_spark_oracle.json` cells (`delete_file_threshold`, `remove_dangling`,
`residue_rpd_then_rdf`).

## Fix

Test-only. Strict-xfail flips run plain against the recorded Spark values (C-002…C-004);
changed-meaning pins move to Spark's measured answers with the cell named in the test
(C-005…C-008); five registry rows close or come true (C-009).

## Rulings

- **R-1 — neither NULL spelling runs on Spark.** The NULL-map-key-only CALL refuses
  `INTERNAL_ERROR`; the NULL-plus-legacy-flag CALL refuses `PARSE_SYNTAX_ERROR` (both recorded
  in the rp32 fixture). The precedence pin (`removed == 0`) is a RePark-Java-default contract
  pin; its `deletes == 0` and row counts cite the recorded `residue_rpd_then_rdf` sequence,
  which is the same 2×8 RPD-then-RDF skeleton with the flag neutralised.
- **R-2 — the rwd DELETE path diverges and stays out of scope.** On the six-file program
  Spark's DELETE drops the all-dead single-row file with no delete file (`deleted-data-files:
  1`, `total-position-deletes: 0` in the recorded snapshot summary) while RePark's DELETE
  writes one position delete. The rewrite then keeps what it gets on both engines
  (`removed == 0`). The Rust legacy-flag test therefore pins `removed == 0` plus the row
  outcomes (Spark's values) and does not pin the surviving-delete count; survival with a full
  Spark twin is pinned by the MERGE test instead. The DELETE path itself belongs to another
  unit.
- **R-3 — compaction granularity is not pinned.** Spark's dead-file rewrite answers 2
  rewritten / 1 added where RePark answers 1 / 0; rows, deletes, and removed agree. Iceberg
  admits both layouts, so the C-011 pin asserts `rewritten > 0` only, as before.
- **R-4 — `RDF-DANGLING-1` closes on the same fork fix.** Its phenomenon (2 deletes outliving
  the sequence) no longer reproduces on the measured shape; the row is FIXED, not merely
  trued, with the history kept dated in place.

## PROPOSITION LEDGER — RP-32-RDF-COW-BYTES — 2026-09-19

| ID | Claim | Evidence | Status | Notes |
|----|-------|----------|--------|-------|
| C-001 | The six Spark cells are in the repository with recorder, banner, and map rows. | `rp32_rdf_cow_bytes_spark_oracle.json` (Spark 4.1.2, UTC, basenames only) + `_record_rp32_rdf_cow_bytes.py` (required `--warehouse`/`--ivy`, critics replay it) + tests map rows. | PROVEN | Commit `4f0603ac`. |
| C-002 | The two COW-BYTES value cells run plain against the recorded Spark values. | `test_option_cell_values[delete_file_threshold]` and `[remove_dangling]` — no xfail; bytes, counts, files, rows match the fixture. | PROVEN | Commit `99ef3af9`. |
| C-003 | Every removed-count cell runs plain at Spark's zero. | `test_option_cell_removed_counts` — the DANGLE-2-only xfail marks deleted, all non-RPD cells green. | PROVEN | Commit `99ef3af9`. |
| C-004 | The residue sequence ends with zero delete files on both engines. | `test_residue_matches_spark_zero_delete_files` plain; `test_residue_repark_sequence_pins_current_shape` asserts 400 rows and `deletes == 0` per `residue_rpd_then_rdf`. | PROVEN | Commits `99ef3af9`, `4f0603ac`. |
| C-005 | The NULL map key still wins over the flag and the sequence ends at zero. | `test_remove_dangling_null_map_key_wins_over_flag` — `removed == 0`, `deletes == 0`, 400 rows; no Spark twin for either spelling per R-1 (both refusals recorded). | PROVEN | Commit `4f0603ac`. |
| C-006 | The legacy-flag CALL reports Spark's zero with rows intact. | `call_rewrite_data_files_remove_dangling_deletes_keeps_the_live_delete` — `removed == 0`, id-2 gone, 5 rows; cell `rwd_remove_dangling`, spelling per `rwd_legacy_flag`, DELETE-path split per R-2. | PROVEN | Commit `4f0603ac`. |
| C-007 | The MERGE delete survives the rewrite exactly as on Spark. | `call_rewrite_data_files_keeps_the_merge_delete_that_still_applies` — `removed == 0`, 1 data + 1 delete file, merged row kept, 6 rows, all equal to cell `rfs_merge_rewrite`. | PROVEN | Commit `4f0603ac`. |
| C-008 | The 100 %-dead file is reclaimed while its delete survives, exactly as on Spark. | `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_survives` — `removed == 0`, seed gone, 1 delete with 2,000 records and seed-naming bounds, 2,000 rows, all equal to cell `dead_file`. | PROVEN | Commit `4f0603ac`. |
| C-009 | The five registry rows state the new truth with pins. | `docs/spark-sql-iceberg-parity.md`: `ICE-RDF-COW-BYTES-1` and `ICE-RDF-DANGLE-2` FIXED 2026-09-19, `RDF-DANGLING-1` FIXED on the same fix, `RDF-1` converged onto its 2026-09-02 oracle, `ICE-RDF-OPTIONS-1` counts and ask list true. | PROVEN | This unit's last commit. |
| C-010 | `target_small` answers Spark's 8→4 at fork `e3eef24f` (#302, folded into RP-32 by ruling Q-23b-5); the other granularity cells stay strict xfails with the file-size residual. | `test_option_cell_values[target_small]` plain; `max_group_size`, `partial_progress_groups` and the RPD cells still xfail (re-measured by the orchestrator at `e3eef24f`: 1 XPASS(strict) on `target_small`, 7 xfails unchanged). | PROVEN | Orchestrator local gate at `d7ed6c5e`: `target_small` the only failure, `[XPASS(strict)] FORK-WRITE-GRANULARITY`; registry row ICE-RDF-GRANULARITY-1 dated 2026-09-19. |

## Red evidence

- Bump-alone red (reproduced in-round on the release native before any pin moved): Rust
  `call_rewrite_data_files_remove_dangling_deletes_reports_a_true_count` panicked at
  `call_rewrite_dangling.rs:40` (`removed = 0`, wanted `>= 1`) and
  `call_rewrite_data_files_drops_the_merge_delete_that_names_one_data_file` at `:102`
  (`removed = 0`, wanted `1`); Python `test_option_cell_values[delete_file_threshold]` and
  `[remove_dangling]`, `test_option_cell_removed_counts[delete_file_threshold]` and
  `[remove_dangling]`, and `test_residue_matches_spark_zero_delete_files` XPASS(strict);
  `test_remove_dangling_null_map_key_wins_over_flag` and
  `test_residue_repark_sequence_pins_current_shape` failed `assert 0 == 2`;
  `test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies` failed
  `assert 0 == 1` on `removed_delete_files_count`.
- Convergence red (by construction): the residue zero tests fail if deletes return; the
  removed-count cells fail if removal returns; the renamed keeps fail if the keep breaks.
- Neighbourhood re-run green at the new pin: `test_rdf_schema_evo_1.py`,
  `test_maintenance_call.py`, `test_v3_dv_compaction.py` (28 passed — the v3 DV `removed == 1`
  and `== 6` pins hold, removal by reference is still Puffin-DV only) and the Rust
  `call_v3_dv` + `v3e3` suites (19 passed).

## Facade evidence (release native, `CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 maturin develop --release`)

- Rust: `cargo test -p repark-spark --lib call_rewrite_dangling` → 3 passed.
- Offline: `pytest python/repark/tests/test_ice_rdf_options_1.py -q -p no:cacheprovider -n 4` →
  121 passed, 1 skipped, 8 xfailed.
- Offline: `pytest python/repark/tests/test_mw7_scale_smoke.py -q -p no:cacheprovider -n 4` →
  19 passed, 1 skipped.
- Live (`REPARK_PARITY_LIVE=1` under the JVM lock, pinned submit args): both files →
  142 passed, 8 xfailed (the oracle re-derivation and the v3 live oracle run inside).
- Lint: `ruff check .` whole tree clean; `ruff format --check` clean on the three touched
  Python files; `cargo fmt --all -- --check` clean; no added code comments in the diff.
- Links and maps: `check_docs_links.py` clean (995 files, 5993 links);
  `sync_map_md.py --check` clean (302 maps).

## Gates

- `ruff check`, `ruff format --check` on the recorder and both touched test files: clean.
- Pre-commit hooks (map-sync, crate-dag, lib-rs, rust-file-size, lib-py,
  docstring-presence, docs-compaction, manifest) green on every commit of this unit.
- `check_ledger_grammar.py`, `check_docs_links.py`, `sync_map_md.py --check` clean at the
  last commit (§Coverage attestation).
- Comment-ban driver over the branch against `origin/main`: 0 hits expected (§Coverage
  attestation; verified by hand: the diff adds no `#` or `//` line).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: rp-32-rdf-cow-bytes
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every new expectation is read from a committed Spark 4.1.2
        recording (the rp32 fixture with its in-file banner, or the
        pre-existing options fixture); the recorder is committed and
        replayable, so no hand-computed row, count, or byte value exists.
      artifacts: [python/repark/tests/rp32_rdf_cow_bytes_spark_oracle.json, python/repark/tests/ice_rdf_options_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Red on the old pin is the bump-alone CI list reproduced
        in-round before any pin moved; red on convergence is constructed
        into the zero, removed-count, and renamed keep pins. Both
        directions bite without touching the pins.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py, python/repark/tests/test_mw7_scale_smoke.py, crates/repark-spark/src/tests/call_rewrite_dangling.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The keep is pinned on five independent shapes (two value
        cells, the residue sequence, the NULL-precedence shape, the
        six-file MERGE shape, the 2,000-row dead file), each with a live
        row-count guard beside the delete counts, so a silent row loss
        reds with the keep.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py, python/repark/tests/test_mw7_scale_smoke.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; the unit adds no product code.
    - id: AT-5
      status: ATTACKED
      evidence: The offline pins run on tmp_path memory catalogs removed
        with the session; the recorder takes its warehouse as a required
        argument outside the tree; no network beyond the warm Ivy cache,
        no credentials.
      artifacts: [task/ledgers/staging/rp-32-rdf-cow-bytes-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: Every pinned value is a measured value (the committed Spark
        cells, the release-native row and file outcomes), not prose: the
        banner, the basenames, the snapshot summaries, the recorded
        refusals, the five registry rows with their pin pointers.
      artifacts: [python/repark/tests/rp32_rdf_cow_bytes_spark_oracle.json, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: The tree carries only the pins, the fixture, the recorder,
        the five registry rows, four map.md entries and this ledger; no
        Cargo.toml, lockfile, workflow, STATUS.md or product change.
      artifacts: [task/ledgers/staging/rp-32-rdf-cow-bytes-ledger.md]
    - id: AT-9
      status: ATTACKED
      evidence: The keep lives in its registered homes (COW-BYTES-1 and
        DANGLE-2 FIXED, DANGLING-1 FIXED, RDF-1 converged, OPTIONS-1 true)
        with the pin pointers beside them; the fixture, recorder, tests,
        crates, bench, and staging maps carry their entries in the same
        round.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/map.md]
    - id: AT-10
      status: ATTACKED
      evidence: The neighbouring removed-count suites re-ran green at the
        new pin (28 Python pins across schema-evo, maintenance, and v3 DV
        compaction; 19 Rust pins across call_v3_dv and v3e3), so the keep
        did not move the DV or schema-evolution contracts.
      artifacts: [task/ledgers/staging/rp-32-rdf-cow-bytes-ledger.md]
  complete: true
```

## Hand-back

`Model: muse-spark-1.3-contributor`. `risk_tier: standard`. Round 1 CONCLUDED with all
nine clauses PROVEN and the gates green.
