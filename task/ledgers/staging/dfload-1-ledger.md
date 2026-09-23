# Unit ledger — DFLOAD-1 (U10) · `format("iceberg").load(<path>)` through a static table (lane xo58-dfl)

**Date:** 2026-09-23 · **Branch:** `fix/u10-df-load-path` · **Base:** `f411192d` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard** (a new read door beside the catalog one; the catalog route is untouched).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** On `f411192d` `spark.read.format("iceberg").load(p)` hands every argument to the catalog
identifier parser, so any `p` containing `/` — a table location or a metadata file — refuses
`AnalysisException: invalid table identifier '<p>': invalid unquoted segment` (measured,
`/tmp/xo-xo-opus58/probe/dfload-finding.md`; scoreboard cells `R-DF-LOAD-PATH` and
`R-DF-LOAD-METADATA-JSON`). Spark 4.1.2's `IcebergSource` treats an argument containing `/` as a
filesystem path: `.metadata.json` pins that file, anything else is a table location whose current
metadata is resolved. Both cells record Spark answering `[[2,"b","y"],[3,"c","x"]]` on the
create-insert-delete fixture.

**The route as ruled (WO dfload-r1 §3).** The `format("iceberg").load` arm tests `"/" in argument`
first — Spark's own rule — and nothing else changes: identifiers without `/` keep the catalog route
byte-for-byte, including `ns.t`, `cat.ns.t`, `ns.t.snapshots`, `` `ns`.`t` `` and the legacy
`snapshot-id`/`as-of-timestamp`/`tag` pins on an identifier. On the path route, a `.metadata.json`
suffix loads that file directly through the fork's `StaticTable::from_metadata_file`; any other path
is a location — strip one trailing `/`, read `<loc>/metadata/version-hint.text` when it exists (a
bare integer selects `v<N>.metadata.json`, which must exist on disk else `AnalysisException` names
the path and the hinted file; a hint that is not a single integer falls through, like Iceberg's
`HadoopTableOperations.findVersion`), else list `<loc>/metadata/` and take the `*.metadata.json`
with the highest leading integer (`NNNNN-<uuid>` or `v<N>`); no resolvable metadata raises
`AnalysisException` naming the supplied path. The resolved table reads through
`IcebergStaticTableProvider::try_new_from_table` — a static, read-only provider; nothing is
registered in a catalog. Any time-travel or incremental reader option beside a path refuses the
pinned `AnalysisException` before resolution — declared, never silently ignored.

**Not in this unit:** the fork (pin `604edca` unchanged — `StaticTable::from_metadata_file`,
`FileIO::list`, `IcebergStaticTableProvider` are already public), `Cargo.toml`/`Cargo.lock`,
`time_travel.rs` (both crates), `wap.rs`, `metadata_tables.rs`, `STATUS.md`, existing expected
values, and the catalog identifier route's behaviour.

## PROPOSITION LEDGER — DFLOAD-1 — 2026-09-23

| C-001 | A load argument containing `/` that is a table location reads that table's current snapshot, with or without one trailing `/`. | `python/repark/tests/test_iceberg_load_path.py::test_load_table_location_reads_current_rows`, `test_load_table_location_trailing_slash`. | **PROVEN** | Both shapes answer `[[2,"b","y"],[3,"c","x"]]` — the current snapshot after the delete — with BIGINT/STRING Arrow types. Forcing the catalog route turns both red with the measured invalid-identifier refusal. |
| C-002 | A load argument ending `.metadata.json` loads that exact file's snapshot: the latest file the current rows, an older file that version's rows. | `test_load_latest_metadata_file_reads_current_rows`, `test_load_older_metadata_file_reads_that_version`. | **PROVEN** | Latest file answers the two-row current snapshot; the pre-delete file answers all three rows. Resolving current instead of the pinned file flips the older-file pin red. |
| C-003 | Metadata resolution prefers `version-hint.text` — a bare integer selects `v<N>.metadata.json`, which must exist on disk else `AnalysisException` names the path and the hinted file; a non-integer hint falls through to listing — else the highest leading-integer metadata file in `NNNNN-<uuid>` or `v<N>` form, where the `v` stem is a whole ASCII-digit integer and the numbered form's suffix is a canonical 36-char UUID. | Rust `iceberg_path::tests`: `version_hint_wins_over_directory_listing`, `hinted_metadata_file_must_exist`, `unparsable_hint_falls_back_to_listing`, `numbered_metadata_files_pick_the_highest_version`, `numbered_metadata_requires_a_canonical_uuid`, `v_form_metadata_files_pick_the_highest_version`, `v_form_requires_a_whole_integer_stem`, `names_outside_the_two_forms_are_ignored`, `valid_forms_still_resolve`; Python `test_load_hinted_missing_metadata_names_the_path`. | **PROVEN** | The hint pins `v<N>` over a higher numbered file; a hint naming a missing file and a hint that is not an integer each have a pin; `00010-` beats `00002-`; `v10` beats `v9` and `v999junk`; `999-`, `12-notauuid`, `+12-<uuid>`, `-<uuid>`, `v+3`, `12-<uuid>-extra`, non-hex-UUID and non-metadata names are skipped; `00001-<uuid>`, `v1`, `v007` and an uppercase-hex UUID still resolve. Reverting the full-stem parse turns `v_form_requires_a_whole_integer_stem` red; dropping the hinted-file exists check turns `hinted_metadata_file_must_exist` red; accepting an empty or unchecked UUID suffix turns `numbered_metadata_requires_a_canonical_uuid` and `names_outside_the_two_forms_are_ignored` red. |
| C-004 | A time-travel or incremental reader option beside a path refuses the pinned `AnalysisException` before resolution. | `test_load_path_with_time_travel_option_refuses`, `test_load_path_with_incremental_option_refuses`; facade `reader_iceberg_path.load_iceberg_path`. | **PROVEN** | `snapshot-id` and a window option each raise `AnalysisException` with the pinned text `format('iceberg').load(<path>) reads one pinned metadata snapshot and does not support time-travel or incremental options; got <keys>`. Skipping the check lets the options ride silently — the pins go red. |
| C-005 | A location with no resolvable metadata raises `AnalysisException` naming the supplied path. | `test_load_missing_location_names_the_path`, `test_load_empty_metadata_dir_names_the_path`; Rust `missing_location_reports_the_supplied_path`, `empty_metadata_dir_reports_the_supplied_path`. | **PROVEN** | `/no/such/dir` and an existing empty `metadata/` dir both refuse `AnalysisException` containing the path verbatim, on both the Rust resolver and the Python door. A generic refusal without the path turns the pins red. |
| C-006 | An argument without `/` keeps the catalog route unchanged, including metadata-table selectors, quoted segments and catalog time-travel pins. | `test_load_two_part_identifier_keeps_catalog_route`, `test_load_three_part_identifier_keeps_catalog_route`, `test_load_metadata_table_identifier_keeps_catalog_route`, `test_load_quoted_identifier_keeps_catalog_route`, `test_load_identifier_snapshot_id_keeps_spark_refusal`. | **PROVEN** | `ns.events` and `mem.ns.events` answer the table, `ns.events.snapshots` keeps its measured table-not-found refusal, `` `ns`.`events` `` resolves, and `snapshot-id` on `ns.events` keeps Spark's `IllegalArgumentException` legacy-option text — all green before and after. |
| C-007 | The path read is static and catalog-free: both scoreboard cells answer Spark's recorded rows on the lane build. | `python/repark/tests/test_iceberg_load_path.py` end-to-end rows; `nc-inventory` replay of `R-DF-LOAD-PATH` and `R-DF-LOAD-METADATA-JSON`. | **PROVEN** | `cells_read.py` replay on the lane build answers `[[2,"b","y"],[3,"c","x"]]` for both cells, equal to the recorded live-Spark answer; the provider is `IcebergStaticTableProvider` over `StaticTable::from_metadata_file`, registered nowhere. |

## Design notes (why, not what)

- The engine lives in `crates/repark-core/src/iceberg_path.rs` with its picker battery in `iceberg_path/tests.rs`: `session.rs` sits exactly at its 1000-line ceiling, so the new method is a sibling free-standing impl block in a new module rather than an addition there. `lib.rs` gained `mod iceberg_path;` at net-zero by removing a stale half-finished comment whose text already lives at `session.rs`.
- The PyO3 door is a module-level `#[pyfunction] read_iceberg_path` in `session_sources.rs` — the same free-function shape the incremental binding uses — because `crates/repark-python/src/session.rs` sits at its exact 1122-line baseline and cannot take a method.
- Python holds only the routing decision and the option refusal: `reader.py` tests `"/" in path` inside the iceberg arm and delegates to `reader_iceberg_path.load_iceberg_path`, which refuses option keys, forwards the raw path, and wraps the returned frame. Resolution order, FileIO construction and provider wiring all live in the engine, matching the bindings-as-thin-adapter discipline.
- `file_io_for_location` is reused rather than re-implemented, so local paths, `file:` spellings and `s3://`/`s3a://` locations all build the FileIO the same way the catalog path does.
- The option refusal fires before `StaticTable` construction, so a `snapshot-id` beside a path can never be half-applied: the path pins exactly one metadata snapshot by construction.

## Observed, out of unit

- `load()` on a path that is not Iceberg-shaped (e.g. a parquet directory) now reaches metadata resolution and refuses with `AnalysisException` naming the path, instead of the old invalid-identifier refusal — a better message for the same refusal class; callers passing a non-Iceberg path with `format("iceberg")` were already refusing.
- Spark's `IcebergSource` also accepts `OPTIONS (path '...')`-style table-function spellings; that door is unmeasured and out of scope here.
- A `version-hint.text` that is not a single trimmed integer (or not UTF-8) falls through to directory listing — Iceberg's `HadoopTableOperations.findVersion` shape — pinned by `unparsable_hint_falls_back_to_listing`. A parseable hint whose `v<N>.metadata.json` does not exist is a hard `AnalysisException` naming the path and the file (critic finding V-001), pinned by `hinted_metadata_file_must_exist` and `test_load_hinted_missing_metadata_names_the_path`.

## Close

All seven clauses C-001…C-007 are PROVEN by pin. The attestation below covers the ten categories for the whole unit.

```text
COVERAGE_ATTESTATION:
  pr_unit: dfload-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every ruled behaviour — the contains-slash route, both resolution shapes, the option refusal, the missing-metadata refusal and the untouched catalog route — has a clause with a named pin; the picker ordering has a Rust pin per rule.
      artifacts: [python/repark/tests/test_iceberg_load_path.py, crates/repark-core/src/iceberg_path/tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries covered are location vs trailing-slash location, latest vs older metadata file, hint vs listing, hinted file present vs missing, parsable vs unparsable hint, NNNNN-uuid vs vN naming, canonical vs malformed UUID suffixes and digit stems, whole-integer vs junk v stems, highest-integer selection, empty metadata dir vs missing dir, and the slash-free near-miss identifiers.
      artifacts: [test_load_table_location_trailing_slash, numbered_metadata_files_pick_the_highest_version, numbered_metadata_requires_a_canonical_uuid, version_hint_wins_over_directory_listing, hinted_metadata_file_must_exist, v_form_requires_a_whole_integer_stem, names_outside_the_two_forms_are_ignored, valid_forms_still_resolve]
    - id: AT-3
      status: ATTACKED
      evidence: Both refusals are read-path only — nothing is registered, written or mutated on refusal; the option refusal fires before any FileIO or metadata access.
      artifacts: [python/repark/src/repark/spark/session/reader_iceberg_path.py]
    - id: AT-4
      status: N/A
      justification: No new shared state. The read is one async resolution plus a static provider; the session's existing lock discipline is unchanged.
    - id: AT-5
      status: ATTACKED
      evidence: The route is bounded by the contains-slash test — a catalog identifier can never reach filesystem resolution, and a path can never reach the catalog. FileIO is built by the shared file_io_for_location, so no new scheme surface is parsed here.
      artifacts: [crates/repark-core/src/iceberg_path.rs, python/repark/src/repark/spark/session/reader.py]
    - id: AT-6
      status: ATTACKED
      evidence: Both scoreboard cells replayed against the lane build answer the recorded live-Spark rows [[2,"b","y"],[3,"c","x"]] exactly.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_iceberg_load_path.py]
    - id: AT-7
      status: N/A
      justification: One metadata list or one hint read plus one metadata file per load — the same shape Spark's path resolution performs; no per-row work added.
    - id: AT-8
      status: ATTACKED
      evidence: Fork pin 604edca unchanged; only already-public fork APIs are consumed (StaticTable::from_metadata_file, FileIO::list, IcebergStaticTableProvider::try_new_from_table). Cargo.toml and Cargo.lock untouched.
      artifacts: [crates/repark-core/src/iceberg_path.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The missing-metadata refusal names the supplied path verbatim; the option refusal names the offending option keys; catalog-route refusals keep their existing Spark texts byte-for-byte.
      artifacts: [test_load_missing_location_names_the_path, test_load_path_with_time_travel_option_refuses]
    - id: AT-10
      status: ATTACKED
      evidence: The Python pins ran red on the pre-change branch (5 path failures, 6 near-misses green, commit d82b7ca6) before the engine and facade made them green; each Rust picker pin fails on a one-line inversion of the rule it covers. Round 2 (critic r1): the three new resolver pins ran red on the rebased tree — hinted_metadata_file_must_exist and unparsable_hint_falls_back_to_listing and v_form_requires_a_whole_integer_stem — and both ruled mutations (drop the exists check, revert the full-stem parse) were shown red before restore.
      artifacts: [python/repark/tests/test_iceberg_load_path.py, crates/repark-core/src/iceberg_path/tests.rs]
```
