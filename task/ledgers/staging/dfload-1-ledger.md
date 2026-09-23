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
`HadoopTableOperations.findVersion`), else list the DIRECT children of `<loc>/metadata/` — the
fork's local `FileIO::list` is recursive, matching `findVersion`'s non-recursive `listStatus` —
and take the `*.metadata.json` with the highest leading integer (`NNNNN-<uuid>` or `v<N>`); no
resolvable metadata raises
`AnalysisException` naming the supplied path. The resolved table reads through
`IcebergStaticTableProvider::try_new_from_table` — a static, read-only provider; nothing is
registered in a catalog. Any time-travel or incremental reader option beside a path refuses the
pinned `AnalysisException` before resolution — declared, never silently ignored. **WO dfload-r5
§2:** a `file://` argument whose next character is not `/` (a non-empty authority, `localhost`
included) refuses `IllegalArgumentException` with Spark's `Wrong FS: <p>, expected: file:///` text
before `file_io_for_location` runs — `<p>` is the argument verbatim for `.metadata.json`, else the
argument with one trailing `/` removed plus `/metadata` (measured on live Spark 3.5 + Iceberg,
`/tmp/xo-xo-opus62/probe/dflfs-spark.out`).

**Not in this unit:** the fork (pin `604edca` unchanged — `StaticTable::from_metadata_file`,
`FileIO::list`, `IcebergStaticTableProvider` are already public), `Cargo.toml`/`Cargo.lock`,
`time_travel.rs` (both crates), `wap.rs`, `metadata_tables.rs`, `STATUS.md`, existing expected
values, and the catalog identifier route's behaviour.

## PROPOSITION LEDGER — DFLOAD-1 — 2026-09-23

| C-001 | A load argument containing `/` that is a table location reads that table's current snapshot, with or without one trailing `/`. | `python/repark/tests/test_iceberg_load_path.py::test_load_table_location_reads_current_rows`, `test_load_table_location_trailing_slash`. | **PROVEN** | Both shapes answer `[[2,"b","y"],[3,"c","x"]]` — the current snapshot after the delete — with BIGINT/STRING Arrow types. Before the change the same `load(<path>)` refused `AnalysisException` `invalid table identifier '<path>': invalid unquoted segment` (measured, `/tmp/xo-xo-opus58/probe/dfload-finding.md`). |
| C-002 | A load argument ending `.metadata.json` loads that exact file's snapshot: the latest file the current rows, an older file that version's rows. | `test_load_latest_metadata_file_reads_current_rows`, `test_load_older_metadata_file_reads_that_version`; Rust `explicit_metadata_path_resolves_verbatim`. | **PROVEN** | Latest file answers the two-row current snapshot; the pre-delete file answers all three rows; the resolver returns a `.metadata.json` path verbatim. Removing the `.metadata.json` early return turns `explicit_metadata_path_resolves_verbatim` red (r4 mutation). |
| C-003 | Metadata resolution prefers `version-hint.text` — a bare integer selects `v<N>.metadata.json`, which must exist on disk else `AnalysisException` names the path and the hinted file; a non-integer hint falls through to listing — else the highest leading-integer metadata file among the DIRECT children of `<loc>/metadata/` in `NNNNN-<uuid>` or `v<N>` form, where the `v` stem is a whole ASCII-digit integer and the numbered form's suffix is a canonical 36-char UUID (dashes at 8/13/18/23). | Rust `iceberg_path::tests`: `version_hint_wins_over_directory_listing`, `hinted_metadata_file_must_exist`, `unparsable_hint_falls_back_to_listing`, `non_utf8_hint_falls_back_to_listing`, `numbered_metadata_files_pick_the_highest_version`, `numbered_metadata_requires_a_canonical_uuid`, `v_form_metadata_files_pick_the_highest_version`, `v_form_requires_a_whole_integer_stem`, `names_outside_the_two_forms_are_ignored`, `uuid_dashes_at_wrong_positions_are_rejected`, `valid_forms_still_resolve`, `each_valid_form_parses_to_its_version`, `nested_metadata_files_are_not_candidates`, `nested_metadata_files_are_not_candidates_trailing_slash`, `nested_metadata_files_are_not_candidates_file_scheme`, `nested_numbered_metadata_is_not_a_candidate`, `only_nested_metadata_names_the_supplied_path`; Python `test_load_hinted_missing_metadata_names_the_path`. | **PROVEN** | The hint pins `v<N>` over a higher numbered file; a hint naming a missing file, a hint that is not an integer and a non-UTF-8 hint each have a pin; `00010-` beats `00002-`; `v10` beats `v9` and `v999junk`; `999-`, `12-notauuid`, `+12-<uuid>`, `-<uuid>`, `v+3`, `12-<uuid>-extra`, non-hex-UUID, wrong-dash-position UUIDs and non-metadata names are skipped; `00001-<uuid>`, `v1`, `v007` and an uppercase-hex UUID each parse to their version; nested `metadata/sub/…` or `metadata/metadata/…` files are not candidates under `/abs`, `/abs/` and `file:///abs` roots, and a location whose only metadata is nested refuses. Reverting the full-stem parse turns `v_form_requires_a_whole_integer_stem` red; dropping the hinted-file exists check turns `hinted_metadata_file_must_exist` red; accepting an empty or unchecked UUID suffix turns `numbered_metadata_requires_a_canonical_uuid` and `names_outside_the_two_forms_are_ignored` red; dropping the direct-child filter turns all five nested pins red; accepting any four dashes turns `uuid_dashes_at_wrong_positions_are_rejected` red; erroring on a non-UTF-8 hint turns `non_utf8_hint_falls_back_to_listing` red (each mutation run this round or r3 and restored). |
| C-004 | A time-travel or incremental reader option beside a path refuses the pinned `AnalysisException` before resolution. | `test_load_path_with_time_travel_option_refuses`, `test_load_path_with_incremental_option_refuses`; facade `reader_iceberg_path.load_iceberg_path`. | **PROVEN** | `snapshot-id` and a window option each raise `AnalysisException` with the pinned text `format('iceberg').load(<path>) reads one pinned metadata snapshot and does not support time-travel or incremental options; got <keys>`. Dropping the `unsupported` check lets the options ride silently — both pins go red (r4 mutation). |
| C-005 | A location with no resolvable metadata raises `AnalysisException` naming the supplied path. | `test_load_missing_location_names_the_path`, `test_load_empty_metadata_dir_names_the_path`; Rust `missing_location_reports_the_supplied_path`, `empty_metadata_dir_reports_the_supplied_path`, `only_nested_metadata_names_the_supplied_path`. | **PROVEN** | `/no/such/dir`, an existing empty `metadata/` dir and a dir whose only metadata is nested all refuse `AnalysisException` containing the path verbatim, on the Rust resolver, with the first two also on the Python door. A generic refusal without the path turns all three Rust pins red (r4 mutation). |
| C-006 | An argument without `/` keeps the catalog route unchanged, including metadata-table selectors, quoted segments and catalog time-travel pins. | `test_load_two_part_identifier_keeps_catalog_route`, `test_load_three_part_identifier_keeps_catalog_route`, `test_load_metadata_table_identifier_keeps_catalog_route`, `test_load_quoted_identifier_keeps_catalog_route`, `test_load_identifier_snapshot_id_keeps_spark_refusal`. | **PROVEN** | `ns.events` and `mem.ns.events` answer the table, `ns.events.snapshots` keeps its measured table-not-found refusal, `` `ns`.`events` `` resolves, and `snapshot-id` on `ns.events` keeps Spark's `IllegalArgumentException` legacy-option text, each asserted by its pin on the r5 build. |
| C-007 | The path read is catalog-free — it registers nothing in the session — and both scoreboard cells answer Spark's recorded rows on the lane build. | `python/repark/tests/test_iceberg_load_path.py::test_load_path_registers_no_catalog_table`; `cells_read.py` replay of `R-DF-LOAD-PATH` and `R-DF-LOAD-METADATA-JSON`. | **PROVEN** | The session's `listCatalogs()` names, `listDatabases()` names, `listTables(<ns>)` names for each namespace, `listTables()` temp views and `list_temp_view_names()` are equal before the load and after the frame is collected, and `spark.catalog.table_exists("path.events")` stays false. Registering the provider as a temp view inside `read_iceberg_path` turns the pin red (r5 mutation). The `cells_read.py` replay on the r5 lane build answers `[[2,"b","y"],[3,"c","x"]]` for both cells, equal to the recorded live-Spark answer. |
| C-008 | A load argument that starts `file://` and whose next character is not `/` — a non-empty authority, `localhost` included — refuses `IllegalArgumentException` with Spark's `Wrong FS: <p>, expected: file:///`, where `<p>` is the argument verbatim when it ends `.metadata.json` and otherwise the argument with at most one trailing `/` removed plus `/metadata`; `file:///<abs>`, `file:///<abs>/` and `file:///<abs>/metadata/<latest>.metadata.json` still read the current rows. | Rust `iceberg_path::tests`: `file_authority_location_refuses_wrong_fs`, `file_authority_location_trailing_slash_refuses_wrong_fs`, `file_authority_metadata_file_refuses_wrong_fs`, `file_localhost_location_refuses_wrong_fs`, `file_localhost_metadata_file_refuses_wrong_fs`; Python `test_load_file_authority_location_refuses_wrong_fs`, `test_load_file_authority_metadata_file_refuses_wrong_fs`, `test_load_file_scheme_location_reads_current_rows`, `test_load_file_scheme_latest_metadata_file_reads_current_rows`. | **PROVEN** | Each Rust pin asserts `Error::IllegalArgument(message)` with `message ==` the full `Wrong FS` string for `file://tmp/…`, `file://tmp/…/`, `file://tmp/…/metadata/v1.metadata.json`, `file://localhost/tmp/…` and `file://localhost/tmp/…/metadata/v1.metadata.json`; the two Python pins assert `IllegalArgumentException` whose `str` equals the full string for the location and latest-metadata-file forms. The `file:///` near misses answer `[[2,"b","y"],[3,"c","x"]]` with BIGINT/STRING Arrow types. On `c2f719aa` all seven refusal pins ran red (the location forms refused the no-metadata `AnalysisException`, the metadata-file forms reached the metadata read); dropping the refusal call turns the five Rust pins red (r5 mutation). |

## Design notes (why, not what)

- The engine lives in `crates/repark-core/src/iceberg_path.rs` with its picker battery in `iceberg_path/tests.rs`: `session.rs` sits exactly at its 1000-line ceiling, so the new method is a sibling free-standing impl block in a new module rather than an addition there. `lib.rs` gained `mod iceberg_path;` at net-zero by removing a stale half-finished comment whose text already lives at `session.rs`.
- The PyO3 door is a module-level `#[pyfunction] read_iceberg_path` in `session_sources.rs` — the same free-function shape the incremental binding uses — because `crates/repark-python/src/session.rs` sits at its exact 1122-line baseline and cannot take a method.
- Python holds only the routing decision and the option refusal: `reader.py` tests `"/" in path` inside the iceberg arm and delegates to `reader_iceberg_path.load_iceberg_path`, which refuses option keys, forwards the raw path, and wraps the returned frame. Resolution order, FileIO construction and provider wiring all live in the engine, matching the bindings-as-thin-adapter discipline.
- `file_io_for_location` is reused rather than re-implemented, so the path route builds its FileIO the same way the catalog path does; the pinned spellings that reach it and read are `/abs`, `/abs/`, `file:///abs`, `file:///abs/` and their `.metadata.json` forms. A `file://<authority>/…` argument never reaches it: `refuse_file_authority` is the first statement of `read_iceberg_path` (C-008).
- The option refusal fires in the facade before the native call, so a `snapshot-id` beside a path can never be half-applied: the path pins exactly one metadata snapshot by construction (C-004's pins raise before any read).
- The fork's local `FileIO::list` is recursive (it walks subdirectories), so `resolve_metadata_location` keeps only entries whose parent is the listed `metadata/` directory — `is_direct_child`, comparing `file://`-stripped spellings because local `FileInfo.location` comes back without the scheme and slash-normalized (measured for `/abs`, `/abs/` and `file:///abs` roots, each pinned by a `nested_metadata_files_are_not_candidates*` test). A `file://<authority>` root, whose stripped spelling would not match the listed parent, is refused before listing (C-008), so every spelling that reaches `is_direct_child` starts with `/` on both sides.

## Observed, out of unit

- `load()` on a path that is not Iceberg-shaped (e.g. a parquet directory) now reaches metadata resolution and refuses with `AnalysisException` naming the path, instead of the old invalid-identifier refusal — a better message for the same refusal class; callers passing a non-Iceberg path with `format("iceberg")` were already refusing.
- Spark's `IcebergSource` also accepts `OPTIONS (path '...')`-style table-function spellings; that door is unmeasured and out of scope here.
- A `version-hint.text` that is not a single trimmed integer (or not UTF-8) falls through to directory listing — Iceberg's `HadoopTableOperations.findVersion` shape — pinned by `unparsable_hint_falls_back_to_listing` and `non_utf8_hint_falls_back_to_listing`. A parseable hint whose `v<N>.metadata.json` does not exist is a hard `AnalysisException` naming the path and the file (critic finding V-001), pinned by `hinted_metadata_file_must_exist` and `test_load_hinted_missing_metadata_names_the_path`.
- Path spellings WO dfload-r5 left UNRULED, measured on the r5 build and handed to the orchestrator unchanged: `file:/abs` refuses `AnalysisException` `malformed storage location` from `file_io_for_location` while the same spelling of a `.metadata.json` file also refuses there; relative `a/b` refuses `AnalysisException` `is not an absolute path`; `s3://` / `s3a://` without a configured region fail `PySparkException` `region is missing`. The Spark 3.5 probe that measured the Wrong FS text answered `file:///abs` and `file:/abs` with `AnalysisException [TABLE_OR_VIEW_NOT_FOUND]`, while RePark reads `file:///abs` as the ruled near miss.

## Close

All eight clauses C-001…C-008 are PROVEN by pin. The attestation below covers the ten categories for the whole unit.

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
      evidence: Boundaries covered are location vs trailing-slash location vs file-scheme spelling vs `file://<authority>` spelling (refused Wrong FS), latest vs older metadata file, hint vs listing, hinted file present vs missing, parsable vs unparsable vs non-UTF-8 hint, NNNNN-uuid vs vN naming, canonical vs malformed UUID suffixes and digit stems and dash positions, whole-integer vs junk v stems, highest-integer selection, direct vs nested metadata children, empty metadata dir vs missing dir vs only-nested dir, and the slash-free near-miss identifiers.
      artifacts: [test_load_table_location_trailing_slash, numbered_metadata_files_pick_the_highest_version, numbered_metadata_requires_a_canonical_uuid, version_hint_wins_over_directory_listing, hinted_metadata_file_must_exist, v_form_requires_a_whole_integer_stem, names_outside_the_two_forms_are_ignored, valid_forms_still_resolve, each_valid_form_parses_to_its_version, uuid_dashes_at_wrong_positions_are_rejected, non_utf8_hint_falls_back_to_listing, nested_metadata_files_are_not_candidates, nested_metadata_files_are_not_candidates_trailing_slash, nested_metadata_files_are_not_candidates_file_scheme, nested_numbered_metadata_is_not_a_candidate, only_nested_metadata_names_the_supplied_path, explicit_metadata_path_resolves_verbatim, file_authority_location_refuses_wrong_fs, file_authority_location_trailing_slash_refuses_wrong_fs, file_authority_metadata_file_refuses_wrong_fs, file_localhost_location_refuses_wrong_fs, file_localhost_metadata_file_refuses_wrong_fs, test_load_file_scheme_location_reads_current_rows, test_load_file_scheme_latest_metadata_file_reads_current_rows]
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
      evidence: The Python pins ran red on the pre-change branch (5 path failures, 6 near-misses green, commit d82b7ca6) before the engine and facade made them green; each Rust picker pin fails on a one-line inversion of the rule it covers. Round 2 (critic r1): the three new resolver pins ran red on the rebased tree — hinted_metadata_file_must_exist and unparsable_hint_falls_back_to_listing and v_form_requires_a_whole_integer_stem — and both ruled mutations (drop the exists check, revert the full-stem parse) were shown red before restore. Round 3 (critic r2): numbered_metadata_requires_a_canonical_uuid and names_outside_the_two_forms_are_ignored ran red on the r2 tree, and both UUID-suffix mutations (empty suffix, unchecked suffix) were shown red before restore. Round 4 (critic r3): the five nested-listing pins ran red on the r3 tree; the mutations shown red before restore were drop the direct-child filter (five nested pins), accept any four dashes (uuid_dashes_at_wrong_positions_are_rejected), error on a non-UTF-8 hint (non_utf8_hint_falls_back_to_listing), remove the .metadata.json early return (explicit_metadata_path_resolves_verbatim), drop the path from no_metadata_error (the three path-naming pins), drop the facade option check (both option-refusal pins), and the r2 pair re-run (full-stem revert, exists-check drop). Round 5 (critic r4): the five Rust and two Python `Wrong FS` refusal pins ran red on c2f719aa; dropping the refusal call turned the five Rust pins red, and registering the provider as a temp view turned the C-007 inventory pin red, each before restore.
      artifacts: [python/repark/tests/test_iceberg_load_path.py, crates/repark-core/src/iceberg_path/tests.rs]
```
