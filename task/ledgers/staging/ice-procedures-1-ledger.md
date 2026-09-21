# Unit ledger — ICE-PROCEDURES-1 · CALL declared-parameter binder, parser first (IPI-31 D-1)

**Date:** 2026-09-20 · **Branch:** `fix/ipi-30-31-procedures` · **Base:** `3dd7b754` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Three inventory cells fail on argument-parser shape, not missing engine work: `P-POS-RDF` and `P-POS-RPD` die on per-handler positional arity caps, and `P-CALL-MIXED-ARGS` dies on a mixed positional-plus-named refusal Spark does not have. This round replaces the caps and the refusal with one declared-parameter table (`call/params.rs`, transcribed from `procedure-params.txt`) plus a binder (`bind` in `call_args.rs`), and switches the RDF and RPD handlers onto it. Nothing else changes product behaviour: the not-yet-wired parameters keep their exact current refusals, and the `P-CALL-*-ERR` cells stay with the IPI-51 lane.

**Not in this unit:** wiring `branch` (RDF), `sort_by` (RM), the four expire arguments, or RPD `where`; the `Condition` enum; `SUPPORTED_PROCEDURES`; the registry row (next round files it); `docs/spark-sql-iceberg-parity.md`; `STATUS.md`; versions/tags.

**PR1b (2026-09-21, lane `xb-procs`):** this round wires the three PR1a-deferred behaviours on the same branch — `add_files` routing (C-015…C-019), RPD `where` (C-012), and the four expire arguments (C-013, C-014) — files the registry row and retires it, and keeps `branch` / `sort_by` refusing loud (C-020). Clauses C-012…C-020 open here and flip PROVEN in this round's last commit; the Close and attestation below cover C-001…C-011 until that commit extends them.

## PROPOSITION LEDGER — ICE-PROCEDURES-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The four-positional RDF form binds in declared order: `rewrite_data_files('t', 'binpack', NULL, map('min-input-files', '2'))` compacts, answering Spark's five columns in order with the third positional (SQL NULL) treated as unset `sort_order`. | Rust `call_rdf_four_positional_form_binds_in_declared_order`; Python `test_pos_rdf_positional_form_compacts`; replay `P-POS-RDF` EQUAL. | **PROVEN** | Rust pin compacts the six-file shape and answers zeros on the `min-input-files 7` twin; Python pin answers Spark's recorded `[2, 1, >0, 0, 0]`; replay EQUAL byte-identical. Mutations M2 (order swap) and M3 (NULL-as-value) turn the pins red. |
| C-002 | The two-positional RPD form binds the map as `options`: `rewrite_position_delete_files('t', map('rewrite-all', 'true'))` answers Spark's four columns in order. | Rust `call_rpd_two_positional_options_bind`; Python `test_pos_rpd_positional_options_bind`; replay `P-POS-RPD` EQUAL. | **PROVEN** | Both pins answer Spark's four zero columns on a delete-free table; replay EQUAL byte-identical. |
| C-003 | Mixed positional and named CALL arguments bind: `rollback_to_snapshot('t', snapshot_id => <id>)` rolls back and answers Spark's two columns. | Rust `call_rollback_mixed_positional_and_named_binds` plus the re-pinned `call_mixed_named_and_positional_binds`; Python `test_call_mixed_args_rollback_binds`; replay `P-CALL-MIXED-ARGS` EQUAL. | **PROVEN** | Rollback pins assert previous/current ids from metadata; replay EQUAL byte-identical. Mutation M1 (rejection re-added) turns both Rust pins red. |
| C-004 | Binding the same parameter positionally and by name refuses loud with the new duplicate-binding Plan error, distinct from the duplicate-named-key check; class and message pinned. | Rust `call_bind_duplicate_positional_and_named_refuses` plus unit `bind_duplicate_positional_and_named_refuses`; Python `test_bind_duplicate_binding_refuses`. | **PROVEN** | Class asserted by destructuring `DataFusionError::Plan`, message by equality. Mutation M4 (check dropped) turns both Rust pins red. |
| C-005 | An unknown argument refuses with today's Plan string and the allowed list is the declared names plus extras in order; `branch => 'b1'` on RDF answers it byte-identical to before. | Rust `call_bind_unknown_argument_names_allowed` plus unit `bind_unknown_named_names_allowed`; Python `test_bind_unknown_argument_refuses`. | **PROVEN** | Full message asserted by equality in Rust and by escaped match in Python. Mutation M5 (branch accepted) turns both Rust pins red. |
| C-006 | A missing required parameter refuses with today's Plan string naming the declared position. | Rust `call_bind_missing_required_names_position` plus unit `bind_missing_required_names_position`; Python `test_bind_missing_required_refuses`. | **PROVEN** | Full message asserted by equality, including the NULL-on-required twin at unit level. |
| C-007 | More positionals than declared parameters refuses with today's arity string; RePark-only extras are named-only. | Rust `call_bind_excess_positional_names_arity` plus unit `bind_excess_positional_names_arity`; Python `test_bind_excess_positional_refuses`. | **PROVEN** | `at most 5` (RDF) and `at most 3` (RPD) asserted by equality; the extra takes no positional slot. |
| C-008 | SQL NULL on an optional parameter means unset, positionally and by name: `sort_order => NULL` with `binpack` succeeds. | Rust `call_rdf_named_null_is_unset` (plus C-001 for the positional NULL) plus unit `bind_positional_follows_declared_order_and_null_means_unset`; Python `test_bind_null_is_unset`. | **PROVEN** | Named NULL succeeds where it used to refuse; mutation M3 turns 4 pins red, including the pre-existing `call_rdf_options_empty_map_and_null_are_defaults`. |
| C-009 | Not-yet-wired parameters stay loud with exact current refusals: the four expire arguments, RM `sort_by`, RDF `branch` (C-005), and RPD `where` — including a positionally bound `where`. | Rust `call_rpd_positional_where_stays_a_loud_refusal` and the untouched `call_rewrite_position_delete_files_validates_options_and_refuses_where`; Python `test_not_yet_wired_params_stay_loud`. | **PROVEN** | Positional `where` refuses `NotImplemented` with the exact current text; expire and RM handlers untouched so their refusals hold by construction. |
| C-010 | `remove-dangling-deletes` stays accepted as a RePark-only extra named parameter on RDF and the options-map key still wins; the existing dangling tests stay green. | Existing `call_rewrite_dangling` tests plus `call_rdf_options_empty_map_and_null_are_defaults`; Python `test_dangling_extra_and_precedence_kept`. | **PROVEN** | Full lib suite 1376 passed (dangling files included); options file 122 passed; new Python pin binds the quoted extra and the NULL map key wins. |
| C-011 | `params.rs` transcribes the jar lists: RDF and RPD bindable names, order and required flags asserted; `branch` is deliberately absent from the RDF bindable set so the binder refuses it (C-005); the array helpers parse `array(…)` and `ARRAY[…]` of literals. | Rust `rewrite_data_files_params_follow_the_jar_order`, `rewrite_position_delete_files_params_follow_the_jar_order`, `params_for_resolves_every_transcribed_procedure`, and the three array-helper unit tests. | **PROVEN** | Transcription mechanically verified with zero mismatches against `procedure-params.txt`; `#[allow(dead_code)]` marks the type field and array helpers until the wiring rounds call them. |
| C-012 | RPD `where` wires through CALL: `rewrite_position_delete_files(table, where => 'cat = "x"', options => rewrite-all)` compacts only the matching partition's deletes and answers Spark's four columns with row `[3,3,>0,>0]`. | Rust `call_rpd_where_restricts_to_matching_partition` plus the converted `call_rpd_positional_where_binds`; Python `test_rpd_where_rewrites_matching_partition`; replay `P-RPD-WHERE` EQUAL. | OPEN | PR1b proves it. |
| C-013 | Expire `snapshot_ids` wires: `snapshot_ids => array(<id>)` expires exactly those ids in array order and answers Spark's six columns with row `[0,0,0,0,1,0]`. | Rust `call_expire_snapshot_ids_expires_exactly_those`; Python `test_expire_snapshot_ids_expires_exactly_those`; replay `P-EXPIRE-SNAPSHOT-IDS` EQUAL. | OPEN | PR1b proves it. |
| C-014 | Expire `stream_results`, `max_concurrent_deletes` and `clean_expired_metadata` are accepted and ignored: each answers the plain `older_than` row `[1,0,0,1,3,0]`. | Rust `call_expire_accept_and_ignore_trio_equals_plain`; Python `test_expire_accept_and_ignore_trio_equals_plain`; replay of the three cells EQUAL. | OPEN | PR1b proves it; the fork behaviour behind `clean_expired_metadata` is carded, not implemented (INDEX decision 15). |
| C-015 | `add_files` routes partitioned and unpartitioned directory imports with Spark's two columns and a NULL `changed_partition_count`: rows `[[2,null]]` / `[[1,null]]`, and the table reads the imported rows. | Rust `call_add_files_partitioned_imports_two_files` and `call_add_files_unpartitioned_imports_one_file`; Python `test_add_files_partitioned` and `test_add_files_unpartitioned`; replay of the two cells. | OPEN | PR1b proves it. The output row is NULL by mapping (D-5); the fork's merge-append writes a `changed-partition-count` summary key Java's add_files path does not, so `md.snapshots` stays a fork residue (see PR1b design notes). |
| C-016 | `add_files` `partition_filter` restricts the import: `map('cat','x')` imports 1 file and 1 row. | Rust `call_add_files_partition_filter_restricts`; Python `test_add_files_partition_filter`; replay `P-ADD-FILES-PARTITION-FILTER`. | OPEN | PR1b proves it. |
| C-017 | `add_files` `parallelism` equals the serial import (D-5 accepted-and-ignored). | Rust `call_add_files_parallelism_matches_serial`; Python `test_add_files_parallelism_matches_serial`; replay `P-ADD-FILES-PARALLELISM`. | OPEN | PR1b proves it. |
| C-018 | `add_files` with `check_duplicate_files => true` raises when a source path is already live in the table, with Java's `IllegalStateException` text. | Rust `call_add_files_check_duplicate_files_raises`; Python `test_add_files_check_duplicate_raises`; replay `P-ADD-FILES-CHECK-DUP` SPARK-CANNOT. | OPEN | PR1b proves it; the exact Spark message is quoted in the PR1b evidence. |
| C-019 | Every `add_files` import commits Java's pretty-printed `schema.name-mapping.default` JSON when the table properties lack it; imported files stay under the source directory (no byte copy); source columns bind by name. | Rust `call_add_files_commits_name_mapping_and_binds_by_name` plus the mapping assertion on every Rust add_files pin; Python `test_add_files_name_mapping_no_copy_by_name` plus the mapping assertion on every Python add_files pin. | OPEN | PR1b proves it. |
| C-020 | `branch` (RDF) and `sort_by` (RM) keep refusing loud; the fork halves land later and PR2 wires them. | Rust `call_bind_unknown_argument_names_allowed` (branch) and the RM `sort_by` unknown-argument pin; Python `test_not_yet_wired_params_stay_loud` narrowed arm. | OPEN | PR1b proves it. |

## Design notes (why, not what)

- `branch` is absent from the RDF bindable list on purpose. The jar declares it, but the fork builder has no branch parameter, so accepting it would ship a silently ignored argument. Excluding it makes the binder itself emit the exact current unknown-argument string, which keeps T-8 (loud, never dropped) with no second copy of the allowed list. The round that wires `branch` adds it to the list.
- RPD `where` stays in the bindable list and the handler refuses it with the exact current `NotImplemented` text. Binder-level refusal would change that text to the generic unknown-argument string; the exact-text clause wins over the binder-refuses letter, and the refusal stays loud either way, including for a positionally bound `where`.
- The duplicate-binding error keeps today's `Plan` class with a new message rather than a Spark condition: the `Condition` enum at this head has no `DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT` variant, and the error catalogue is the IPI-51 lane's job.
- Rollback and the other unswitched handlers keep their own arity and unknown-argument checks; only RDF and RPD call `bind()` in this round, so only their positional forms widen.
- One dual-violation precedence moves: `bind` checks missing-required before the handler coerces anything, so a missing `table` plus a malformed `strategy` now reports the missing table where the old read order reported the malformed strategy. Every single-violation message is byte-identical; the new order matches Java, which validates required parameters at bind time.

## Mutation record (all red, all reverted)

Each mutation was applied, run scoped, observed red, and reverted with a clean `git diff` before the next. The release `.so` was built before the first mutation and the mutations never touched it.

| Mutation | Break | Pins observed red |
|---|---|---|
| M1 | Re-add the mixed-args rejection in `CallArgs::parse` | `call_rollback_mixed_positional_and_named_binds`, `call_mixed_named_and_positional_binds` (2 failed) |
| M2 | Swap `sort_order`/`options` in `REWRITE_DATA_FILES_PARAMS` | `rewrite_data_files_params_follow_the_jar_order`, `call_rdf_four_positional_form_binds_in_declared_order` (2 failed) |
| M3 | `is_sql_null` returns false (NULL as value) | `bind_positional_follows_declared_order_and_null_means_unset`, `call_rdf_named_null_is_unset`, `call_rdf_options_empty_map_and_null_are_defaults` (pre-existing), `call_rdf_four_positional_form_binds_in_declared_order` (4 failed) |
| M4 | Drop the duplicate-binding check in `bind` | `bind_duplicate_positional_and_named_refuses`, `call_bind_duplicate_positional_and_named_refuses` (2 failed) |
| M5 | Accept `branch` in `REWRITE_DATA_FILES_PARAMS` (T-8 silent drop) | `rewrite_data_files_params_follow_the_jar_order`, `call_bind_unknown_argument_names_allowed` (2 failed) |
| Base bite-proof | New Python C-001 pin against the pre-change release build | `test_pos_rdf_positional_form_compacts` failed with the old `at most 2 positional` refusal, then passed after the rebuild |

## Replay evidence (2026-09-21)

`harness.py --engine repark --cells cells_proc.py cells_deep.py --out out/repark-procs-after.json` on the lane release build, then `compare.py`. `/tmp/nc-build` does not exist on this box, so the run used the lane venv in place of `run-engine.sh`'s repark leg; everything else matches the brief's command. Official `classify` verdicts: `P-POS-RDF` EQUAL, `P-POS-RPD` EQUAL, `P-CALL-MIXED-ARGS` EQUAL — all three byte-identical on cols, rows and data. Verdict diff over the 160 cells of the two families against the stale inventory outputs shows 27 changes and zero regressions: the 3 target cells plus 14 other-lane improvements toward EQUAL, and 10 newly routed procedures (ancestors/stats/RTP, another unit's merged handlers) moving from unregistered refusal to row-differs on UUID-and-timestamp keys; no cell moved away from EQUAL or SPARK-CANNOT.

## Observed, out of unit

- The `call.rs` test module needed two line-neutral re-pins (mixed-args and third-positional now bind); the file stays at its exact 1287 baseline with no added comment lines. Done in this round's step-4 commit, reported here because the brief's touch list did not name the file: the full-crate gate leaves no other consistent reading.
- Rollback and the other unswitched handlers still prefer the named value when one parameter arrives both ways; only RDF and RPD raise the new duplicate-binding error. A follow-up round extends `bind()` to them.

## Close

PR1a clauses C-001…C-011 are PROVEN by pin plus mutation; the attestation below covers the ten categories for those clauses. PR1b extends both in this round's last commit.

```text
COVERAGE_ATTESTATION:
  pr_unit: ice-procedures-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Walked the eleven charter clauses against the diff and the runs. Every clause cites a Rust pin and, where the door differs, a Python pin and a replay verdict.
      artifacts: [crates/repark-spark/src/tests/call_procedures_1.rs, python/repark/tests/test_ice_procedures_1.py, /tmp/oc-worker/nc-inventory/out/repark-procs-after.json]
    - id: AT-2
      status: ATTACKED
      evidence: Exercised NULL positional and named, empty and NULL options maps, over-arity on both arities, unknown names, duplicate bindings, missing required, and the quoted dashed extra key.
      artifacts: [call_args::tests::bind_positional_follows_declared_order_and_null_means_unset, tests::call_procedures_1::call_bind_excess_positional_names_arity, python/repark/tests/test_ice_procedures_1.py::test_not_yet_wired_params_stay_loud]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal pins error class plus exact text, and every new branch went red under mutation. No IO, retry, or timeout surface exists on this change: bind is synchronous and the handlers commit exactly as before.
      artifacts: [mutation record M1-M5 above, tests::call_procedures_1::call_rpd_positional_where_stays_a_loud_refusal]
    - id: AT-4
      status: N/A
      justification: bind is a synchronous pure function over already-parsed args with no shared state; the diff adds no await point, no lock, and no ordering assumption.
    - id: AT-5
      status: N/A
      justification: No privilege, credential, or path-resolution change. Table-idents still flow through the untouched resolve_table_ident with its path-escape refusals, pinned by the existing tests.
    - id: AT-6
      status: ATTACKED
      evidence: Output schemas and rows are byte-identical except the three target cells, and every kept refusal string is pinned by equality. The replay verdict diff over 160 cells shows no move away from EQUAL or SPARK-CANNOT.
      artifacts: [/tmp/oc-worker/nc-inventory/out/repark-procs-after.json, tests::call_procedures_1::call_bind_unknown_argument_names_allowed]
    - id: AT-7
      status: N/A
      justification: bind scans at most twelve declared names per CALL with no data loops, no allocation beyond the bound vec, and nothing retained across calls.
    - id: AT-8
      status: ATTACKED
      evidence: Unswitched handlers keep their own checks by untouched code. The extract_option_pairs signature change reaches exactly its two call sites. SUPPORTED_PROCEDURES and Cargo.toml are untouched, verified by diff and the T-2 grep.
      artifacts: [crates/repark-spark/src/call.rs, crates/repark-spark/src/call/rewrite_data_files.rs, crates/repark-spark/src/call/rewrite_options.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every new failure names the parameter and the rule in one line, pinned by equality, through the existing Plan channel. No new silent path exists: unset means absent, never a defaulted value.
      artifacts: [call_args::tests::bind_duplicate_positional_and_named_refuses, tests::call_procedures_1::call_bind_missing_required_names_position]
    - id: AT-10
      status: ATTACKED
      evidence: Five mutations plus a base-build bite-proof all went red on the named pins. Every branch the diff adds has a nameable input in the suite: mixed, duplicate, unknown, excess, missing, NULL named, NULL positional, extra named-only.
      artifacts: [mutation record M1-M5 above, crates/repark-spark/src/tests/call_procedures_1.rs, python/repark/tests/test_ice_procedures_1.py]
```
