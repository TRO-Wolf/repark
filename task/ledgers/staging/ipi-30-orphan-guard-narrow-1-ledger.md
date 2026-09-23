# Unit ledger — IPI-30-ORPHAN-GUARD-NARROW-1 · remove_orphan_files sweeps a fallback table's own directory and accepts file_list_view (lane xo55-orph2)

**Date:** 2026-09-22 · **Branch:** `fix/ipi-30-orphan-guard-narrow` · **Base:** `85011ea0` · **Model:** claude-opus-5-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: high** (the procedure deletes files; the change widens what it may sweep).

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** On `85011ea0` every memory-catalog table created in a namespace without a `location` refused `remove_orphan_files`, because the table sits under the shared CTAS fallback root `<warehouse>/repark_ctas/<catalog>/<ns>/<table>` and the guard refused anything *in* that root. Spark 4.1.2 sweeps the same shape (scoreboard cells P-ORPHAN-DEFAULT … -STREAM-RESULTS, nine refuse on RePark). Owner ruling Q-55-6 "(A) NARROWED" (claims file, 2026-09-22) keeps only the safety property "never sweep a directory that holds another table's files". The same round accepts `file_list_view` (cell P-ORPHAN-FILE-LIST-VIEW), which refused `NotImplemented`.

**The guard as ruled.** The scan path is the `location` argument, or the table location when it is absent. It refuses only when the scan path (a) is `<root>/repark_ctas` or `<root>/repark_ansi_ctas` after the lexical and `file:` / `file://` / `file:///` normalisation, (b) is a parent of that root, or (c) equals or contains the location of another table in the same catalog, found by walking every namespace and table through the catalog API, excluding the swept table. (a) and (b) keep the old refusal text; (c) names the other table. The guard fires only on a `TempFallbackAllowed` catalog, the same scope the old guard had.

**Not in this unit:** Cargo.toml / Cargo.lock and the fork (the view path reuses the fork's public `DeleteReachableFiles` collector for the referenced set); `ctas.rs` (Q-55-7); the scoreboard harness; `refuse_service_managed_orphan_sweep`; STATUS.md; the 24-hour `older_than` floor.

## PROPOSITION LEDGER — IPI-30-ORPHAN-GUARD-NARROW-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A scan path equal to the fallback root refuses with the unchanged "shared CTAS fallback root" text, for `repark_ctas` and `repark_ansi_ctas`, a trailing slash, `file:/`, `file://` (hostless), `file:///`, and `..` spellings that denote the root, and deletes nothing. | Rust `call_orphan_shared_ctas_root_rule`, `call_remove_orphan_files_refuses_a_location_arg_at_the_fallback_root`, `call_remove_orphan_files_refuses_the_warehouse_and_the_fallback_root`; Python `test_remove_orphan_files_sweeps_a_fallback_table_but_never_the_shared_root`. | **PROVEN** | Every alias row of the unit pin refuses with the root text; the end-to-end pins keep the planted orphan on disk after each refusal. |
| C-002 | A scan path that is a parent of the fallback root (the warehouse, `/`) refuses with the same text and deletes nothing. | Rust `call_orphan_shared_ctas_root_rule` (`/scratch`, `/`), `call_remove_orphan_files_refuses_the_warehouse_and_the_fallback_root`, `call_remove_orphan_files_file_list_view_near_misses_refuse` (warehouse in view mode); Python `test_remove_orphan_files_sweeps_a_fallback_table_but_never_the_shared_root`. | **PROVEN** | The warehouse refuses on both doors and in `file_list_view` mode; the orphan survives. |
| C-003 | A `location` that equals or contains another table of the same catalog refuses, names that table as `<catalog>.<ns>.<table>`, and deletes nothing from either table. The walk reaches every top-level namespace and every nested one. | Rust `call_remove_orphan_files_refuses_a_location_holding_another_table`, `call_remove_orphan_files_refuses_a_location_holding_a_table_of_another_namespace`, `call_remove_orphan_files_refuses_a_location_holding_a_nested_namespace_table`. | **PROVEN** | The namespace directory, the other table's own directory and a `file://` spelling of the namespace directory all refuse naming `ice.ns.b`; both planted orphans and the other table's live data file survive. A scan of `ns`'s directory that holds `other.c` refuses naming `ice.other.c`, and a scan holding `ns.inner.d` (created through the catalog API, since the SQL door takes two-part namespaces only) refuses naming `ice.ns.inner.d`. Walking only the swept namespace turns the `other.c` pin red; dropping the child-namespace walk turns the nested pin red. |
| C-004 | A fallback table's own directory, and any path inside it, is sweepable: the bare call lists the 10-day-old `data/orphan-file.parquet` as one row and deletes it, the live rows still read, and a 1-day-old orphan returns zero rows and is kept. | Rust `call_remove_orphan_files_sweeps_a_fallback_tables_own_directory`, `call_orphan_shared_ctas_root_rule` (accept rows); Python `test_remove_orphan_files_sweeps_a_fallback_table_but_never_the_shared_root`. | **PROVEN** | Both doors answer Spark's P-ORPHAN-DEFAULT / P-ORPHAN-YOUNG shapes on a namespace with no `location`. |
| C-005 | `file_list_view => 'v', dry_run => true` returns exactly the view's orphan rows, each path verbatim as the view gave it, and deletes nothing; a referenced data file the view lists is not an orphan. | Rust `call_remove_orphan_files_file_list_view_dry_run_lists_the_view_orphans_verbatim`. | **PROVEN** | The view lists the table's live data file and a planted orphan by bare path; the result is exactly the orphan's bare path (Spark's P-ORPHAN-FILE-LIST-VIEW answer) and both files remain. |
| C-006 | `file_list_view` with `dry_run => false` deletes exactly the listed orphans: a planted file the view does not list survives, the referenced file survives, the table still reads. | Rust `call_remove_orphan_files_file_list_view_armed_deletes_only_the_listed_orphans`. | **PROVEN** | One listed orphan deleted, the unlisted orphan and the live data file on disk, one live row. |
| C-007 | `file_list_view` near misses refuse and delete nothing: a view that does not exist answers `[TABLE_OR_VIEW_NOT_FOUND]` naming `` `no_such_view` ``, and a view whose `last_modified` is not a timestamp answers "Invalid last_modified column: … is not a timestamp". | Rust `call_remove_orphan_files_file_list_view_near_misses_refuse`. | **PROVEN** | Both refusals asserted by content; the planted orphan survives each. |
| C-008 | The guard never fires on a `RequireExplicitLocation` or `ServiceManagedLocation` catalog, or with no policy, and a sibling whose name merely begins with `repark_ctas` is not the root. | Rust `call_orphan_shared_ctas_root_rule`. | **PROVEN** | The remote and no-policy rows accept the root path itself; `/scratch/repark_ctas_other/t` accepts. |
| C-009 | The listing mode's argument surface is unchanged: the unknown-argument refusal still lists `file_list_view` among the allowed names, and every pre-existing orphan pin stays green. | Rust `call_remove_orphan_files_near_misses_still_refuse` and the rest of `call_orphan`. | **PROVEN** | 14 of 14 pre-existing `call_orphan` pins green at the unit head. |
| C-010 | A sibling table in the same namespace does not block the bare sweep: `table => 'ns.a'` beside `ns.b` deletes a's 10-day orphan, keeps b's orphan and b's live data file, and both tables still read. | Rust `call_remove_orphan_files_sweeps_one_fallback_table_beside_a_sibling`. | **PROVEN** | One row ending `/ns/a/data/orphan-file.parquet`; b's two files on disk; one row from each table. Replacing the containment test with `true` (refuse whenever another table exists) turns the pin red. |

## Design notes (why, not what)

- The referenced set for the view path is the fork's `DeleteReachableFiles` walk with a collecting `delete_with`, which the fork documents as the way to gather that set without deleting. It reads every snapshot's manifest list, every manifest, every entry including DELETED tombstones, the whole metadata log, the version hint and the statistics files: the same set `DeleteOrphanFiles` uses, so no Cargo or fork edit is needed.
- The view join mirrors Java `compareToFileList`: candidates are the rows with `last_modified < older_than` whose path lies under the scan location, joined on the URI path with the `equal_schemes` / `equal_authorities` maps and the `prefix_mismatch_mode` classification. The containment test is component-wise after the guard's normalisation, not Java's raw `startsWith`: a `..` escape or a sibling that shares a name prefix is not a candidate. The pinned Spark cells answer identically.
- `gc.enabled = false` refuses the view path with the fork's own text, because the view path does not run the fork action that owns that gate.
- The procedure body moved from `call.rs` to `call/remove_orphan_files.rs`, with the view join in `call/orphan_file_list.rs`, so `call.rs` stays well under its size ceiling. Moved code sheds its comments.

## Observed, out of unit

- `docs/spark-sql-iceberg-parity.md` already carries `ORPHAN-2` (the retired dry-run-default row), so the brief's new row is filed as `ORPHAN-3` under the title the brief gave.
- `task/ledgers/staging/ipi-30-orphan-1-ledger.md` C-010 ("`file_list_view` still refuses `NotImplemented`") is falsified by C-005 here; it is marked REJECTED with a pointer, and the tests map drops its citation.
- The other-table rule is scoped to `TempFallbackAllowed` catalogs, as the old guard was. A Glue or REST catalog can still sweep a `location` that holds a sibling table. That is unchanged behaviour, not a regression.

## Close

All nine clauses C-001…C-009 are PROVEN by pin. The attestation below covers the ten categories for the whole unit.

```text
COVERAGE_ATTESTATION:
  pr_unit: ipi-30-orphan-guard-narrow-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Walked the ruling's three refusal conditions and its accept condition against the guard and the pins. Each has a unit row and an end-to-end pin; file_list_view dry and armed each have an end-to-end pin.
      artifacts: [crates/repark-spark/src/call/remove_orphan_files.rs, crates/repark-spark/src/tests/call_orphan.rs, crates/repark-spark/src/tests/call_orphan_scope.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries covered are the root itself against a table directory one level below it, a trailing slash, dot-dot and file-scheme aliases, the name-prefix sibling, the warehouse and slash as parents, a 10-day against a 1-day orphan, and a view that lists a referenced file.
      artifacts: [call_orphan_shared_ctas_root_rule, call_remove_orphan_files_sweeps_a_fallback_tables_own_directory]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal pin asserts that the planted orphans, and where present the other table's live file, are still on disk. The armed view pin asserts that the unlisted orphan survives.
      artifacts: [call_remove_orphan_files_refuses_a_location_holding_another_table, call_remove_orphan_files_file_list_view_armed_deletes_only_the_listed_orphans]
    - id: AT-4
      status: N/A
      justification: No new shared state across calls. The referenced-set collector is a call-local Mutex that the fork's sequential walk fills, and poisoning is recovered, not unwrapped.
    - id: AT-5
      status: ATTACKED
      evidence: The widening is bounded by rule (c). A sweep over a directory that holds another table's files refuses before any listing or deletion, and the view path applies the same guard plus a component-wise scope filter, so a view cannot name files outside the scan path.
      artifacts: [call_remove_orphan_files_refuses_a_location_holding_another_table, crates/repark-spark/src/call/orphan_file_list.rs]
    - id: AT-6
      status: ATTACKED
      evidence: The answers match the scoreboard's Spark 4.1.2 cells. P-ORPHAN-DEFAULT gives one row and the file deleted, P-ORPHAN-YOUNG gives zero rows and the file kept, and P-ORPHAN-FILE-LIST-VIEW gives the path verbatim and the file kept.
      artifacts: [docs/spark-sql-iceberg-parity.md, call_remove_orphan_files_file_list_view_dry_run_lists_the_view_orphans_verbatim]
    - id: AT-7
      status: N/A
      justification: The catalog walk is one list per namespace and one load per table, run once per CALL, on memory catalogs only. The view is collected once.
    - id: AT-8
      status: ATTACKED
      evidence: Cargo.toml, Cargo.lock and the fork are untouched. The fork surface used is the public DeleteReachableFiles builder and the TableProperties gc constants.
      artifacts: [crates/repark-spark/src/call/orphan_file_list.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Rule (c) names the swept table, the scan path, and the other table with its location. A missing view answers Spark's TABLE_OR_VIEW_NOT_FOUND condition, and a mistyped view column names the column and its type.
      artifacts: [call_remove_orphan_files_file_list_view_near_misses_refuse]
    - id: AT-10
      status: ATTACKED
      evidence: Every new or rewritten pin went red on 85011ea0 before going green. The own-directory, other-table, three file_list_view and unit-rule pins failed with the old refusal or NotImplemented texts, and the Python pin raised the old AnalysisException.
      artifacts: [crates/repark-spark/src/tests/call_orphan_scope.rs, python/repark/tests/test_maintenance_call.py]
```
