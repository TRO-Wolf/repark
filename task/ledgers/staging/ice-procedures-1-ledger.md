# Unit ledger — ICE-PROCEDURES-1 · CALL declared-parameter binder, parser first (IPI-31 D-1)

**Date:** 2026-09-20 · **Branch:** `fix/ipi-30-31-procedures` · **Base:** `3dd7b754` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Three inventory cells fail on argument-parser shape, not missing engine work: `P-POS-RDF` and `P-POS-RPD` die on per-handler positional arity caps, and `P-CALL-MIXED-ARGS` dies on a mixed positional-plus-named refusal Spark does not have. This round replaces the caps and the refusal with one declared-parameter table (`call/params.rs`, transcribed from `procedure-params.txt`) plus a binder (`bind` in `call_args.rs`), and switches the RDF and RPD handlers onto it. Nothing else changes product behaviour: the not-yet-wired parameters keep their exact current refusals, and the `P-CALL-*-ERR` cells stay with the IPI-51 lane.

**Not in this unit:** wiring `branch` (RDF), `sort_by` (RM), the four expire arguments, or RPD `where`; the `Condition` enum; `SUPPORTED_PROCEDURES`; the registry row (next round files it); `docs/spark-sql-iceberg-parity.md`; `STATUS.md`; versions/tags.

## PROPOSITION LEDGER — ICE-PROCEDURES-1 — 2026-09-20

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The four-positional RDF form binds in declared order: `rewrite_data_files('t', 'binpack', NULL, map('min-input-files', '2'))` compacts, answering Spark's five columns in order with the third positional (SQL NULL) treated as unset `sort_order`. | Rust `call_rdf_four_positional_form_binds_in_declared_order`; Python `test_pos_rdf_positional_form_compacts`; replay `P-POS-RDF` EQUAL. | **OPEN** | Not yet implemented. |
| C-002 | The two-positional RPD form binds the map as `options`: `rewrite_position_delete_files('t', map('rewrite-all', 'true'))` answers Spark's four columns in order. | Rust `call_rpd_two_positional_options_bind`; Python `test_pos_rpd_positional_options_bind`; replay `P-POS-RPD` EQUAL. | **OPEN** | Not yet implemented. |
| C-003 | Mixed positional and named CALL arguments bind: `rollback_to_snapshot('t', snapshot_id => <id>)` rolls back and answers Spark's two columns. | Rust `call_rollback_mixed_positional_and_named_binds` plus the re-pinned `call_mixed_named_and_positional_binds`; Python `test_call_mixed_args_rollback_binds`; replay `P-CALL-MIXED-ARGS` EQUAL. | **OPEN** | Not yet implemented. |
| C-004 | Binding the same parameter positionally and by name refuses loud with the new duplicate-binding Plan error, distinct from the duplicate-named-key check; class and message pinned. | Rust `call_bind_duplicate_positional_and_named_refuses`; Python `test_bind_duplicate_binding_refuses`. | **OPEN** | Not yet implemented. |
| C-005 | An unknown argument refuses with today's Plan string and the allowed list is the declared names plus extras in order; `branch => 'b1'` on RDF answers it byte-identical to before. | Rust `call_bind_unknown_argument_names_allowed`; Python `test_bind_unknown_argument_refuses`. | **OPEN** | Not yet implemented. |
| C-006 | A missing required parameter refuses with today's Plan string naming the declared position. | Rust `call_bind_missing_required_names_position`; Python `test_bind_missing_required_refuses`. | **OPEN** | Not yet implemented. |
| C-007 | More positionals than declared parameters refuses with today's arity string; RePark-only extras are named-only. | Rust `call_bind_excess_positional_names_arity`. | **OPEN** | Not yet implemented. |
| C-008 | SQL NULL on an optional parameter means unset, positionally and by name: `sort_order => NULL` with `binpack` succeeds. | Rust `call_rdf_named_null_is_unset` (plus C-001 for the positional NULL); Python `test_bind_null_is_unset`. | **OPEN** | Not yet implemented. |
| C-009 | Not-yet-wired parameters stay loud with exact current refusals: the four expire arguments, RM `sort_by`, RDF `branch` (C-005), and RPD `where` — including a positionally bound `where`. | Rust `call_rpd_positional_where_stays_a_loud_refusal` and the untouched `call_rewrite_position_delete_files_validates_options_and_refuses_where`; Python `test_not_yet_wired_params_stay_loud`. | **OPEN** | Not yet implemented. |
| C-010 | `remove-dangling-deletes` stays accepted as a RePark-only extra named parameter on RDF and the options-map key still wins; the existing dangling tests stay green. | Existing `call_rewrite_dangling` tests plus `call_rdf_options_empty_map_and_null_are_defaults`; Python `test_dangling_extra_and_precedence_kept`. | **OPEN** | Not yet implemented. |
| C-011 | `params.rs` transcribes the jar lists: RDF and RPD bindable names, order and required flags asserted; `branch` is deliberately absent from the RDF bindable set so the binder refuses it (C-005); the array helpers parse `array(…)` and `ARRAY[…]` of literals. | Rust `rewrite_data_files_params_follow_the_jar_order` and the `call_args.rs` array-helper unit tests. | **OPEN** | Not yet implemented. |

## Design notes (why, not what)

- `branch` is absent from the RDF bindable list on purpose. The jar declares it, but the fork builder has no branch parameter, so accepting it would ship a silently ignored argument. Excluding it makes the binder itself emit the exact current unknown-argument string, which keeps T-8 (loud, never dropped) with no second copy of the allowed list. The round that wires `branch` adds it to the list.
- RPD `where` stays in the bindable list and the handler refuses it with the exact current `NotImplemented` text. Binder-level refusal would change that text to the generic unknown-argument string; the exact-text clause wins over the binder-refuses letter, and the refusal stays loud either way, including for a positionally bound `where`.
- The duplicate-binding error keeps today's `Plan` class with a new message rather than a Spark condition: the `Condition` enum at this head has no `DUPLICATE_ROUTINE_PARAMETER_ASSIGNMENT` variant, and the error catalogue is the IPI-51 lane's job.
- Rollback and the other unswitched handlers keep their own arity and unknown-argument checks; only RDF and RPD call `bind()` in this round, so only their positional forms widen.

## Open questions (why every verdict is OPEN)

Each clause closes by pin plus mutation: the named tests pass on the branch, and breaking the implementation they guard (re-adding the mixed rejection, binding in the wrong order, treating NULL as a value, dropping the duplicate check) turns them red. No code is committed yet, so nothing here is claimed PROVEN and no mutation record is filed.
