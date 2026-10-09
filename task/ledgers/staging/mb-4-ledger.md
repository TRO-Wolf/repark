# Unit ledger — MB-4 · the streaming facade, SURFACE HALF

**Date:** 2026-10-08 · **Branch:** `feat/mb-4-facade` · **Base:** `origin/main` `40fc916f` ·
**Model:** muse-spark-1.3-contributor (muse-worker build lane) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Scope.** Order steps 1 and 2 only of the [MB-4 order](../../wo/microbatch/mb-4-facade.md):
the surface against a stub binding, and the three IPI-47 cells measured against the stub.
Binding: the [design sketch](../../wo/microbatch/mb-design-2026-10-06.md) §0 row 7, §1 W8,
Q1 to Q3, Q7, Q9, §3.5 to §3.7, §4 the error-class table, §6 the mb-4 row, §8 and §9;
the [North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) §2
authority order; the brief's rulings 1 to 8. Step 3 (wire-up) and step 4 (SES-DECL flip
and registry) are explicitly out of this round. MB-3 is merged, so the order's halt
rule 2 does not bind.

## Halt checks — 2026-10-08

- **Order halt rule 1 fires for five questions and the round hands back HALT.**
  The sketch's §4, §3.7, Q7 and Q9 do not answer: the valid-spec stub terminal (Q1),
  writer-side unknown options (Q2), the missing-format default and the writer
  `NOT_IMPLEMENTED` feature shape (Q3), the `processingTime` interval grammar (Q4), or
  a sourceless `load()` (Q5). Each is recorded below as an OPEN clause and in the
  hand-back with the implemented lean where one exists. Everything §3.7, Q7, Q9 and §4
  do answer is built, committed and gated.
- **Order halt rule 2** does not bind: MB-3 is merged; the brief says so.
- **Order halt rule 3** does not fire: no Python row compute anywhere in the round.
- **Order halt rule 4** does not fire: `session_surface.py`, `surface_a.py`,
  `streaming_batch.py` and `session_core.py` are unedited, and the session-surface
  neighbours answer byte-identical to the unmodified tree (C-007).

## PROPOSITION LEDGER — MB-4 — 2026-10-08

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The stub binding validates reader options with the §4 classes: a skip option set true refuses MBE-3 with the exact `[REPARK_MICROBATCH.SKIP_OPTION_REFUSED]` text, parameters and no SQLSTATE; a prefixed unknown refuses MBE-17 the same way; known and plain keys pass; the first sorted key wins between two refusals; keys fold case. | The option-validation pins in `crates/repark-python/src/streaming.rs`. | **PROVEN** | 7 pins green: `skip_options_set_true_refuse_with_skip_option_refused` (both keys, three casings of true; class, exact text, `{option}`, no `SQLSTATE`), `skip_options_set_false_pass`, `skip_option_keys_fold_case`, `unknown_prefixed_reader_options_refuse_with_unknown_option` (all three prefixes), `known_and_plain_reader_options_pass` (the two caps, the timestamp, the snapshot key, plus `checkpointLocation` and a plain key passing on the reader), `first_sorted_reader_key_wins_between_two_refusals` (`streaming-aaa` beats the skip, the `from_options` order). pins: mb-4/C-001 |
| C-002 | The stub doors enforce the start-time §4 checks, then stop: `load_stream` validates and stops; `start_stream` orders checkpoint before sink before writer unknowns; `to_table_stream` enforces the checkpoint; MBE-4 renders the MB0-W8 text verbatim under `_LEGACY_ERROR_TEMP_1298` with empty parameters and no SQLSTATE; MBE-10 renders its exact text, parameters and no SQLSTATE; a spec that passes every check raises the builtin `NotImplementedError` terminal and nothing else. | The door pins in `crates/repark-python/src/streaming.rs`. | **PROVEN** | 8 pins green: `missing_checkpoint_refuses_with_the_w8_text` (exact text, condition, `{}`, no `SQLSTATE`), `checkpoint_option_or_conf_passes` (either casing of the option, an empty option counting as specified, the conf), `undeclared_foreach_sink_refuses_with_sink_undeclared` (exact text, `{}`, no `SQLSTATE`; a folded sink key passes), `unknown_prefixed_writer_options_refuse_with_unknown_option` (the Q2 lean), `known_writer_options_pass` (the four named writer keys plus a plain key), `load_stream_validates_then_stops_at_the_stub`, `start_stream_orders_checkpoint_before_sink_before_unknowns` (the order, the terminal, no-sink-needed without a body), `to_table_stream_validates_then_stops_at_the_stub`. Each terminal assertion is type-only. pins: mb-4/C-002 |
| C-003 | The two native exceptions shape the hierarchy: both subclass `PySparkException` (hence `RuntimeError`); neither subclasses the other; `RecoveryRequiredException` carries `query_id`, `epoch` and `durable_offset` as plain attributes; both register on `_native` and re-export from the root. `lib.rs` stays at 183 lines under its 190 ceiling by the verbatim arm move. | The hierarchy pin and the root ceiling gate. | **PROVEN** | 1 pin green: `streaming_exceptions_shape_the_hierarchy_and_carry_recovery_fields` (both directions of the non-subclassing, the three attributes round-tripped). `lib.rs` is 183 lines; the commit-unknown `to_py_err` arm moved verbatim to `exceptions::commit_state_unknown_py_err`, and the pre-existing `to_py_err_commit_state_unknown_is_typed_and_carries_operation_id` pin stays green. The `errors.py` re-export and structured-method loop entries are C-016. pins: mb-4/C-003 |
| C-004 | The builders carry the §3.7 surface: every listed method exists with its sketch signature; argument checks mirror the pinned PySpark 4.1.2 source; options store via the `to_str` mirror with case-insensitive last-write-wins and secret registration; `None` values drop at the door call; `load` falls back to the `path` option; `start`/`toTable` merge kwargs in PySpark order through the validated setters. | The builder pins in `python/repark/tests/test_mb_4_streaming_surface.py`. | **PROVEN** | 30 pins green: chaining on both builders; option storage, overwrite and conversion; `options`; the `load` path check; the `table` `NOT_STR` check; trigger single/interval/flag checks and storage; `foreachBatch` storing without validation; the kwarg-merge order on both doors (including a kwarg overriding builder state); per-option pass-through for the two caps, the timestamp and the snapshot key on both `load` and `table`; the `None` drop; the path-option fallback. pins: mb-4/C-004 |
| C-005 | The facade raises every driver-free §4 refusal end to end with class, SQLSTATE and message text: MBE-3 and MBE-17 through the reader doors, MBE-4 through both start doors, MBE-10 on the `foreachBatch` door, MBE-6 on complete/update, MBE-7 on a non-iceberg format and on `trigger(continuous)`. | The refusal pins in `python/repark/tests/test_mb_4_streaming_surface.py`. | **PROVEN** | 14 pins green: `test_reader_skip_overwrite_true_refuses_mbe3_mb0_r3`, `test_reader_skip_delete_true_refuses_mbe3_mb0_r6`, `test_reader_unknown_prefixed_option_refuses_mbe17`, `test_writer_missing_checkpoint_refuses_mbe4_mb0_w8` (both doors, the W8 text verbatim), `test_writer_foreach_without_sink_refuses_mbe10_mb0_w4`, `test_writer_unknown_prefixed_option_refuses_mbe17`, `test_writer_complete_mode_refuses_mbe6_mb0_w3`, `test_writer_update_mode_refuses_mbe6`, `test_reader_format_must_be_iceberg_mbe7`, `test_writer_format_must_be_iceberg_mbe7`, `test_writer_continuous_trigger_refuses_mbe7`, `test_reader_format_match_is_case_sensitive_mbe7`, `test_writer_append_mode_passes`, `test_writer_unknown_mode_passes_to_wireup`. Each asserts the class, the parameters, the SQLSTATE (`None`) and the exact text. pins: mb-4/C-005 |
| C-006 | The query surface is complete and honest: `StreamingQuery` carries every §3.7 member with the driver-owned bodies behind the stub terminal and a real `awaitTermination` timeout check; `StreamingQueryManager` answers `active == []` and `get() is None` for real, validates the `awaitAnyTermination` timeout, and checks the session is alive on all three methods. | The manager and query pins in `python/repark/tests/test_mb_4_streaming_surface.py`. | **PROVEN** | 9 pins green: `test_manager_active_is_empty_on_idle_session` (the `streams_active` oracle cell), `test_manager_get_returns_none_without_queries`, `test_manager_await_any_termination_validates_timeout` (`< 0` refuses, `0` passes validation, then the terminal), `test_manager_methods_refuse_on_stopped_session`, `test_query_await_termination_validates_timeout` (`<= 0` refuses, the valid shapes reach the terminal), `test_query_surface_raises_stub_terminal` (all nine members). Terminal assertions are type-only. pins: mb-4/C-006 |
| C-007 | The three IPI-47 cells run through the stubbed surface and answer: `R-STREAM-READ` validates and stops at the stub terminal; `R-STREAM-READ-SKIP` refuses MBE-3; `W-STREAM-WRITE-FILESRC` refuses MBE-7 on both doors. The inventory's verdicts are untouched, and the session-surface neighbours answer byte-identical to the unmodified tree (57 passed, 9 skipped, 22 pre-existing collection errors for the missing `repark_parity` module, same before and after). | The probe run and the neighbour runs, recorded here. | **PROVEN** | Probe of 2026-10-08 through the module-direct builders: `R-STREAM-READ` raises builtin `NotImplementedError` (the Q1 terminal); `R-STREAM-READ-SKIP` raises `IllegalArgumentException` `[REPARK_MICROBATCH.SKIP_OPTION_REFUSED]` with `{option}` and no SQLSTATE; `W-STREAM-WRITE-FILESRC` raises `PySparkNotImplementedError` `NOT_IMPLEMENTED` with `{feature: readStream.format(parquet)}` on the read and `{feature: writeStream.format(parquet)}` on the write. What each needs is D-17. Neighbours: `pytest python/repark/tests -k "session_surface or ses_decl or readStream or streams or writeStream"` reads 57 passed, 9 skipped, 22 pre-existing errors on the unmodified tree and identical at head. pins: mb-4/C-007 |
| C-009 | The wire-up round replaces the stub terminal: `load`/`table` return streaming frames, `start`/`toTable` register and run, the manager waits and the query members answer, per Q7 and Q9. | The wire-up round's brief and pins. | **OPEN** | The closing question is the hand-back's Q1: what a validated spec answers in the stub. The implemented lean is the builtin `NotImplementedError` terminal, asserted type-only so the ruling flips no text. |
| C-010 | Writer-side unknown options are ruled: the mirror-reader lean (prefixed unknowns refuse MBE-17, plain keys pass, over the four named writer keys) or ruling 4's letter (every unnamed writer key refuses MBE-17). | The orchestrator's ruling on the hand-back's Q2. | **PROVEN** | The read ruled Q2 (the mirror-reader lean stands) and the brief left it uncontradicted; `validate_writer_options` keeps the lean and routes the refusal through the mapper. The M1 pins (`test_writer_totable_unknown_option_refuses_mbe17`, the Rust `to_table` unknown case) pin it on the second door. W-Q6's value questions stay open for items 6–7. pins: mb-4/C-010 |
| C-011 | The format gaps are ruled: the missing-format default and the writer `NOT_IMPLEMENTED` feature shape. | The orchestrator's ruling on the hand-back's Q3. | **PROVEN** | The read ruled Q3 and the brief confirmed it: `load` and plain `start` refuse a missing format as the `parquet` default and any non-iceberg format as MBE-7, matching case-insensitively; `table`, `toTable` and the `foreachBatch` start never look at the format. Implemented in `readers.py`; pins cover the fold (F4/F5), the ignored doors (F6, F9/F11, foreach) and keep the `load`/plain-`start` refusals. Spark's own answers on the refused doors (F1, F13, F15) are the dated file-source card's. pins: mb-4/C-011 |
| C-012 | The `processingTime` interval grammar is ruled: which strings parse, to what durations, and which error answers an unparsable one. | The orchestrator's ruling on the hand-back's Q4. | **PROVEN** | Closed by C-020: the brief ruled Q4 and the Rust parser plus 16 facade pins implement the measured grammar. pins: mb-4/C-012 |
| C-013 | A sourceless `load()` is ruled: `load()` with no path argument and no `path` option under the iceberg format. | The orchestrator's ruling on the hand-back's Q5. | **PROVEN** | The brief ruled Q5 for `load()`: `IllegalArgumentException`, Spark's text, no condition, no SQLSTATE; pin `test_reader_load_without_path_or_option_refuses_mb0c_f7` asserts class, `None` condition, `None` SQLSTATE and the F7 text. The `start()`-side no-path refusal moves to item 6 with the sink mapping (D-33). pins: mb-4/C-013 |
| C-014 | The option cases with no §4 row are classified at wire-up: cap, timestamp and snapshot-id value parsing, non-boolean skip values, the four unsupported Spark keys, the two start keys together, and folded-key collisions. The stub passes all of them to the terminal; the wire-up refuses them with whatever classes the ruling gives. | The wire-up round's brief and pins. | **PROVEN** | Each listed case refuses through `from_options` as `Catalog`, rendered at start as `AnalysisException` with the passthrough verbatim, no condition, no SQLSTATE (W-Q1). Pins: the value-cases loop (skip `maybe`, both caps bad and zero, bad timestamp, bad snapshot id), the unsupported-keys loop (all four), both start keys, and the folded-key collision through the module door. pins: mb-4/C-014 |
| C-015 | Step 4 flips `spark.readStream` / `spark.streams` (with the `session_core.py` stops-every-query fold and its lowered baseline), flips the SES-DECL rows and files the registry rows drafted in this ledger. | The wire-up round's brief and pins. | **OPEN** | Round 4: the stops-every-query fold (C-030), the flip (C-031) and the SES-DECL retirements all landed; only the registry rows drafted in this ledger still ride item 15. |
| C-016 | The two exceptions gain their `errors.py` re-export and structured-method loop entries in the wire-up round. | The wire-up round's brief and pins. | **PROVEN** | `errors.py` re-exports both exceptions with structured-method loop entries and `__module__` re-homing; verified live (`getCondition`/`getSqlState`/`getMessageParameters` on a `STREAM_FAILED` instance). pins: mb-4/C-016 |
| C-017 | The PySpark members outside §3.7 land in the wire-up round or a dated card: the reader's `csv`, `json`, `orc`, `parquet`, `schema`, `text` and `xml`, the writer's `clusterBy`, `foreach` and `partitionBy`, the manager's `resetTerminated`, the query's `processAllAvailable`, and `load`'s `format`/`schema` kwargs. | The wire-up round's brief and pins. | **OPEN** | The stub implements exactly the §3.7 surface; anything else is an `AttributeError` or `TypeError` today. |
| C-018 | The driver-owned start checks land at wire-up: MBE-8 (local catalog), MBE-15 (isolation), MBE-13 at start (sink busy), the stateful-operator arm of MBE-6, and the run-time rows MBE-1, MBE-2, MBE-11, MBE-12 and MBE-16 — with the streaming frame, `isStreaming` and the `writeStream` answer. | The wire-up round's brief and pins. | **OPEN** | The stub cannot reach a session's catalogs without re-implementing resolution, and `surface_a.py` / `streaming_batch.py` stay vacuous-correct until frames exist. |
| C-019 | MB-0c records the facade oracle on live Spark 4.1.2 + Iceberg 1.11.0: 31 cells covering the no-format doors, format case, sourceless load, 152 trigger strings with millisecond durations, client checks, invalid output mode, DM-1, DM-3, the manager gets, the duplicate query name and the writer runs. A re-run is byte-identical; every read-scratch pair matches; the MB-0 and MB-3 recordings are untouched; the JSON carries a `sha256sum -c` pin. | The recorder, the recording, the re-run diff and the scratch comparison. | **PROVEN** | `mb0c_facade_oracle.py` (752 lines) records F1–F15, T1/T1B/T1C/T4, T2, T3, O1–O2, D1–D2, M1–M3, W1–W3; two full runs diff zero cells; the `MB0C_CELLS` merge path re-verified byte-identical; all 22 read trigger strings, all client checks and all error cells match the scratch probe in class and full text (F2 differs only by the scrubbed warehouse path, M3 only by the chosen query name, both by design). `mb0_streaming_oracle.json` and `mb3_fold_oracle.json` verify against their shas. pins: mb-4/C-019 |
| C-020 | The trigger interval grammar is parsed in Rust and called from `trigger()`, refusing with Spark's class, condition, SQLSTATE and text: all 152 recorded strings pinned to millis-or-refusal in three Rust sweep tables, and 16 facade pins (one per refusal class plus accepts and padded-input strip proofs) asserting class, condition, SQLSTATE, text and message parameters. The neighbour selection answers identical to baseline. | The parser, the Rust sweeps and the facade pins. | **PROVEN** | `trigger_interval.rs` implements the measured grammar (single `interval` prefix, case-insensitive units, fraction on seconds only, `i32` month/day counts, the measured raw-versus-wrapped overflow split, month refusal before negativity, truncation to millis); `sweep_t1/t1b/t1c_matches_mb0c_t1*` pin all 152 cells with full texts and the T4 params; 16 `test_writer_trigger_*_mb0c_*` pins cover every class through the door; neighbours read 68 passed, 9 skipped, identical to the unmodified tree. This closes the C-012 question; threading the `Duration` into the start spec belongs to item 6. pins: mb-4/C-020 |
| C-021 | The M1 and M4 weak pins are fixed at both levels and all four read mutants re-run by hand: M1 and M4 go red on the new pins, M2 stays red as found, M3 survives because its fix is item 3's `from_options`. | The new pins and the mutant re-runs, recorded here. | **PROVEN** | Rust: a `to_table_stream` unknown-option case (M1) and a both-violations order case asserting `SINK_UNDECLARED` (M4) extend the two door tests. Facade: `test_writer_totable_unknown_option_refuses_mbe17` and `test_writer_sink_refusal_precedes_unknown_option`. Re-runs: M1 red, M4 red, M2 red, M3 survived — no pin added for M3 because it would codify behaviour item 3 reverses (C-014 keeps those cases unpinned deliberately). pins: mb-4/C-021 |
| C-022 | P1 plus item 2 land: `MicroBatchError` and its direct field types re-export through `repark-core`; `microbatch_py_err` maps every variant to its §4 shape plus the W-Q1 start/run split, with one Rust unit test per variant asserting class, condition, SQLSTATE and text (MBE-1/2/16 against R2/R5/W6, MBE-4 against W8); the two exceptions gain their `errors.py` re-export. | The re-export, the mapper, the 30 Rust tests and the live re-export check. | **PROVEN** | `microbatch/mod.rs` re-exports the error, `RecoveryReason`, the seven offset ids, `Generation` and `iceberg::spec::Operation` (D-28). `streaming_errors.rs` maps all 29 variants: §4 classes and conditions, `Some(QueryHead)` selecting the run rendering with Spark's `STREAM_FAILED` head, `None` the start rendering; MBE-6/MBE-7 raise the Python class by import; the recovery class carries its three attributes; the writer checks route through the mapper. 30 `streaming_errors::tests` green (NonAppend split by operation, the four W-Q1 duals both phases, all six recovery reasons); the MBE-6/MBE-7 tests inject a stub `repark.errors` at the real import path. `errors.py` re-export verified live. pins: mb-4/C-022 |
| C-023 | Item 3 lands: `load_stream` calls `streaming_frame` and the facade returns the frame; the stub reader validation is deleted; the format rule folds case and leaves `table`/`toTable`/foreach-start alone; the sourceless `load` takes its ruled shape; the is-streaming predicate backs `isStreaming`; every batch-action door refuses DM-3 with per-family batch controls; `withWatermark` refuses MBE-7 on a stream; the wireup battery splits out; neighbours answer identical and all four mutants re-kill. | The wire-up, the pins, the neighbour runs and the mutant re-runs. | **PROVEN** | `load`/`table` return streaming frames with the MB0-R1 schema through both doors; the eight stub-terminal reader tests return frames; the Q3/Q5 pins take their ruled shapes and the MBE-10 pin drops its format. `count`/`show`/`__arrow_c_stream__` call `refuse_streaming_action` first; the DM-3 pin covers 13 doors with the D1 class and text verbatim; four batch controls hold. `test_mb_4_streaming_wireup.py` (13 tests) splits out at 270 lines, the surface battery at 805. Batteries: 83 passed. Neighbours: 69 passed, 9 skipped — baseline 68/9 plus the one new watermark pin, zero flips. M1/M2/M3/M4 all red on re-run, M3 killed by the new C-014 pin. pins: mb-4/C-023 |

| C-024 | Item 5 lands: `PyStreamingQuery` over `QueryHandle` plus the shared runtime answers id, run id, name, `is_active`, `status`/`last_progress`/`recent_progress` as JSON, `await_termination` with the ruled timeout rendering, `stop` with the CC-1 rule-3 `RecoveryRequired` re-raise, and `exception`; the facade `StreamingQuery(handle)` delegates every member; a non-test `start_below_catalog_check` seam starts on a local catalog while plain `start` still refuses MBE-8 (both claims are about the Rust methods only — fold 1 corrects the door reading: every public door called the seam, so the MBE-8 refusal was unreachable from Python until C-036). | The binding class, the facade constructor, the seam, and the pins. | **PROVEN** | `streaming_query.rs` implements the class; `query.py` binds the handle and parses the native JSON; `driver.rs` drops the `#[cfg(test)]` on `CatalogCheck::Skip` and `start_below_catalog_check` (5 lines, no option key, under the ceiling). `test_query_constructor_needs_a_handle` and `test_query_members_delegate_to_the_handle` pin the facade; the Rust tests start on a memory catalog and pin the MBE-8 refusal of plain `start`. Mutant (the seam launches with `Enforce`): both Rust pins red; restored green. pins: mb-4/C-024 |
| C-025 | Item 6 lands: both Table doors build the `StreamSpec` (output mode first, trigger, frame template, source options, the `spark.sql.streaming.*` conf map, `RecordedLocation`, catalog timeout), refuse a duplicate query name with Spark's text, then register and start below the catalog check; `start` resolves the sink by path argument, else the path option, else the Q5 text. | The doors and the replay pins. | **PROVEN** | MB0-W1/W2/W5/W7/T1/T3 replay through the facade; MBE-6 both arms; MBE-13 at start; the invalid output mode against its MB-0c cell; start-by-path and start-by-path-option; the W3/M3 texts; the dated missing-sink row; the matching sink-option arm; the catalog-timeout grammar; conf effects and the Spark set-time refusals. Mutant (the duplicate-name scan compares against a fixed non-name): the M3 pin red; restored green. pins: mb-4/C-025 |
| C-026 | Item 7 lands except the `count` leg: the foreach arm checks checkpoint, then the declared sink, then writer unknowns, then starts through the shared spec builder with `SinkSpec::ForeachBatch`; `BatchBodyAdapter` wraps the epoch frame as the facade `DataFrame` and calls the body under `block_in_place` with the stream-poll no-detach hatch; a raising body fails the query with the Python error in the `STREAM_FAILED` cause. | The arm, the adapter, and the pins. | **PROVEN** | First pin: a `writeTo` body commits once, stamped with the `repark.cdc` keys. Replays: MB0-T2 batches, MB0-R1 resume rows, MB0-W4 rows with one body append plus one stamp per epoch. MBE-16: full `STREAM_FAILED` text with Spark's head and `batch 0 failed: RuntimeError: ...`; MBE-10 and the W8-rule refusal through the live door; `collect`/`show` in the body complete. Mutant (sink check replaced by a fixed sink): both MBE-10 pins red; restored green. `count` in the body is C-027 OPEN. pins: mb-4/C-026 |
| C-027 | A `count()` on the batch frame inside a `foreachBatch` body completes, as `collect` and `show` do under C-026. | The batch-action pin's `count` leg. | **PROVEN** | Round 3c: `test_foreach_door_batch_actions_in_body_complete` pins `counts == [(0, 3)]` beside the `collect`/`show` legs on one 3-row batch. The leg rides the granted provider fix (D-43): `count` pushes an empty projection and the provider now rebuilds zero columns with the row count stated. pins: mb-4/C-027 |
| C-028 | Item 8 lands: `active` lists running queries and drops stopped ones, `get` parses the id and returns the active query or `None`, `awaitAnyTermination` reports a termination (`None` without a timeout, `True` with one, `False` on a timeout, the query's error when it failed), and `resetTerminated` clears the record; the stub terminal is gone. | The manager pins: the M1/M2 `get` pins in the surface battery, the manager battery, and the two Rust manager tests. | **PROVEN** | `active` lists one running daemon query and drops it after `stop`; `get` returns the query by id; malformed ids refuse MB0c-M1 verbatim and unknown ids answer MB0c-M2 `None`; `awaitAnyTermination` answers `None`/`True` after one of two queries stops, `False` on a timeout, and raises the failed query's `STREAM_FAILED` text; `resetTerminated` clears and the next stop re-reports. Rust pins cover folded/short-form ids, the refusal shapes, and the reset-then-clear wait. Mutant (the reap keeps running queries instead of terminated ones): the Rust await-any pin red; restored green. pins: mb-4/C-028 |
| C-029 | Race-lane Q1 lands: `conclude()` adopts the reported durable record into `Lifecycle.durable` when the lifecycle holds none, so `QueryHandle::durable()`, the exception and the `ShutdownOutcome` agree on a query that ends `RecoveryRequired` before its first resume; the item-14 test pins `restart.durable()`. | The driver lines, the pin, and the no-op argument on the other ending paths. | **PROVEN** | A 3-line `if lifecycle.durable.is_none()` in `conclude`'s `RecoveryRequired` arm; `assert_eq!(restart.durable(), Some(racer.clone()))` beside the outcome pin in `a_restart_after_the_fences_recovery_required_ending_refuses_by_name`, green. No-op elsewhere, shown by reading the callers: the in-run `RecoveryRequired` paths report `shared.durable()` (already adopted) and the stop-timeout path calls `resumed()` before `finish`, so only the pre-resume case changes; every existing `durable() == None` pin ends `Failed`/`Stopped`/`RecoveryRequired`-with-`None`. The mutant case is R-4, which measured `durable()` as `None` on the refused restart. pins: mb-4/C-029 |
| C-030 | Item 9 lands: native `streams_stop_all` stops every known query through the shared `stop` (the first `RecoveryRequired` raises, `Failed` never does) and prunes known to still-active handles; `spark.stop()` calls `session_surface.release_session_resources` in one line, which takes the live handle first, stops the queries, runs the auto-warehouse and artifact-dir cleanups unchanged, then raises the first `RecoveryRequiredException` (W-Q4); `session_core.py` ratchets 2277 → 2269. | The native function, the release function, the one-line call, the ceiling records, and the pins. | **PROVEN** | `streams_stop_all` reuses `PyStreamingQuery::stop` per known handle and prunes after; `release_session_resources` catches the first `RecoveryRequiredException`, runs both cleanups, clears `_inner` first so the raise path still reads stopped, then raises; `stop` keeps its teardown line. Pins: a running query goes inactive and a second stop is quiet, a failed query never raises, a recovery-required query (sink replaced under the body) raises after both dirs are gone and a second stop is quiet. Rust pins cover the stop, the prune (a cleared wait answers `Some(false)`) and the empty session. Mutant (the raise moved before the cleanups): the recovery pin red on the uncleared token; restored green. The auto-warehouse and artifact-dir pins pass unchanged; `check_lib_py.sh` accepts the lowered baseline. pins: mb-4/C-030 |
| C-031 | Item 10 lands: `spark.readStream` / `spark.streams` answer a fresh reader / manager per access, `df.writeStream` answers a writer on a streaming frame and keeps `WRITE_STREAM_NOT_ALLOWED` on a batch frame; the two SES-DECL rows retire; the three declared pins flip to the oracle cells `readStream_type`, `streams_type`, `streams_active`; every other session/DataFrame-surface pin answers identical to main; the `isStreaming` docstring is corrected line-neutrally; the freeze inventory is untouched. | The doors, the retired rows, the flipped pins, the base-vs-head neighbour diff, and the freeze test. | **PROVEN** | The doors construct through local imports after `_ensure_alive`; `streaming_batch.write_stream` branches on `is_streaming_frame` with the batch refusal verbatim. The three pins are renamed to answer pins asserting the oracle cells (`test_read_stream_declared` → `test_read_stream_answers_reader`, `test_streams_declared` → `test_streams_answers_manager`, `test_declared_properties_raise_under_hasattr` → `test_streaming_properties_answer_under_hasattr` with `dataSource`/`client` still raising). Neighbours: the round-2 selection reads 68 passed on `40fc916f` (the verified `read1/main` copy) and 69 passed at head — the delta is exactly the three renames plus the branch's watermark pin, zero flips. `core.py` stays 3461 lines; `test_api_freeze.py` green, no regen. pins: mb-4/C-031 |
| C-032 | Item 11 lands: a credential-bearing `checkpointLocation` and option value appear in none of status, lastProgress, `repr(query)`, the run error, the snapshot summaries and the table properties, and the value-echo refusals carry the masked form (fold 1 replaces the two vacuous legs — the EXPLAIN refusal-text leg and the never-stored checkpoint leg — with echoing-refusal legs and moves the pin to the fold-1 battery; C-037). | The pin; any shown surface would have halted the item. | **PROVEN** | `test_microbatch_no_credential_on_any_surface_1` runs a `toTable` query and a failing `foreachBatch` query with userinfo-shaped secrets, then asserts all four secret strings absent from every surface; the echoing refusals pin the masked form both ways (masked present, password absent). No surface shows a credential. pins: mb-4/C-032 |
| C-033 | Item 12 lands: the three IPI-47 dispositions measured through the public doors — `R-STREAM-READ` reads EQUAL to MB0-R1, `R-STREAM-READ-SKIP` is the registered refusal MBE-3, `W-STREAM-WRITE-FILESRC` is defined in inventory row 55 as the Iceberg sink fed by a file-source stream with disposition registered refusal MBE-7 — and the FILE-SOURCE-1 card is filed under MB-5. | The probe run, the inventory row, and the card. | **PROVEN** | Probe of 2026-10-09 through `spark.readStream` / `df.writeStream`: R batches `[(0, [1, 2, 3]), (1, [4, 5])]` with the MB0-R1 schema; SKIP-true refuses `IllegalArgumentException` `[REPARK_MICROBATCH.SKIP_OPTION_REFUSED]` (SKIP-`maybe` refuses the C-014 value check, Spark reads it as false — the second divergence, folded into the MB-4-SKIP-OPTIONS-1 draft); FILESRC refuses `NOT_IMPLEMENTED` with `{feature: readStream.format(parquet)}` on the read and `{feature: writeStream.format(parquet)}` on the write. Row 55 carries the definition and all three dispositions; the card quotes both doors, cites the MB-0c F-cells as Spark's match target, and leaves the exact file-to-Iceberg combination for MB-5 step 0. pins: mb-4/C-033 |
| C-034 | The W-Q2 follow-through lands: MB-0c cell D3 records `foreachBatch` with no `checkpointLocation` on live Spark — rows, the exact temporary-checkpoint warning, and a second start replaying from the start — with the `sha256` pin in step; the TEMP-CHECKPOINT-1 card holds the Spark-matching answer; the product keeps the W8 refusal. | The recorder extension, the recording with its byte-identity proof, the sha check, and the card. | **PROVEN** | `cell_d3` (with `captured_driver_log`, a log4j2 `FileAppender` over the JVM door — the gateway owns `stderr`, so an fd redirect cannot see it) records D3: both starts read batch 0 with 3 rows, the warning fires twice with the temp path normalized to `$TEMP_CHECKPOINT`, the sink holds each row twice. Two runs diff zero; the merge keeps the 31 older entries and the preamble byte-identical; all three oracle shas verify. DM-1's owed cell and card from D-39 are filed. pins: mb-4/C-034 |
| C-035 | The `foreachBatch` door is exactly-once through the public doors: a body that writes and an epoch that then fails or a process that dies before the trailing stamp never lands a row twice. | The owner's ruling on the hand-back's open question (parity-with-Spark at-least-once vs the exactly-once claim), then the ruling's pins. | **OPEN** | The Opus verify FAIL S1 (D-54): a single-write body duplicates (sink 1,1,2,2,3,3; f_writeraise/f_exitafterwrite/f_kill/f_killcommit dup 4/4/12/16), the merged MB-3 default, parity with Spark's at-least-once contract and a breach of sketch Q9. Fold 1 records it and touches no driver line. |
| C-036 | Every public start door enforces MBE-8: `toTable`, `start(path)` and `foreachBatch` start refuse `AnalysisException` `[REPARK_MICROBATCH.LOCAL_CATALOG_REFUSED]` on a memory catalog and on a `type=hadoop` catalog; `start_below_catalog_check` stays reachable only through the private `_native` test seam, which the facade batteries' fixtures call explicitly and nothing under `repark.spark` references. | The six refusal pins plus the seam-run and seam-unreferenced pins, and the mark unit test. | **PROVEN** | `start_spec` launches through `QueryHandle::start` unless the session carries the `TestLocalCatalogAllowed` extension the seam sets. The fold-1 battery pins all three doors on both catalogs (class, condition, text), a marked session running `toTable` to 3 rows, and zero seam references under `src/repark`. The four older batteries' `spark` fixtures opt in; their 191 pins stay green. Mutant (`start_spec` always below the check): exactly the two refusal pins red; restored green. pins: mb-4/C-036 |
| C-037 | The three leaking refusal texts mask credential-shaped values like the neighbouring echoes: the `repark.cdc.catalog-timeout` interval parse error on the `toTable` and `foreachBatch` doors (text and parameters), the `repark.cdc.sink` vs `toTable` mismatch text, and the `format(<value>)` refusal (text and parameters, via the format refusal's move to the native `check_stream_format`); `format(5)` refuses `NOT_STR` eagerly at the setter on both builders, as Spark's eager refusal (measured 2026-10-09: `Py4JError` at `format()`). | The rewritten credential pin with its echoing-refusal legs, the `format(5)` pins, and the Rust discriminator test. | **PROVEN** | Masking is `mask_user_visible` in Rust at the three raise sites; the pre-fix leaks are the verify's `cred.out` HITs. The pin asserts exact masked texts and parameter dicts (both-ways: masked present, password absent) for the interval refusal on both doors, the sink mismatch, and both format refusals. The native discriminator pins the fold, the `parquet` default, and the 64-char echo cap. This closes the read-1 S2 "refusal logic in Python" for format. pins: mb-4/C-037 |
| C-038 | `query.awaitTermination()` and `spark.streams.awaitAnyTermination()`, with and without a timeout, honour Ctrl-C: the wait raises `KeyboardInterrupt` promptly and the query keeps running, as Spark does (measured 2026-10-09: the wait ends at the signal, the query stays active, `stop()` works after). | The four-variant subprocess pin. | **PROVEN** | Both waits loop in short slices (50 ms query waits, 10 ms manager polls) and call `py.check_signals()` between slices; the reap-before-timeout order and every timeout rendering are unchanged. Each variant runs in a subprocess, takes SIGINT at 2 s, asserts the child saw `KeyboardInterrupt` with `isActive` true, stopped cleanly, and exited within 10 s. The pre-fix behaviour is the verify's `sigint.py` (the wait ignored SIGINT for 15 s). pins: mb-4/C-038 |
| C-039 | `isStreaming` stays True through `udf`, `pandas_udf`, `mapInArrow`, `mapInPandas` and `applyInPandas`; starting such a query refuses `PySparkNotImplementedError` MBE-18 naming the real cause on every start door; batch actions on it answer the DM-3 guard through the same native guard as a plain streaming frame. The STREAM-PYTHON-UDF-1 card holds the Spark-matching answer. | The predicate pin, the fifteen start-door cells, and the DM-3 door pins. | **PROVEN** | The predicate walks the map-bridge chain to the streaming scan (no plan-build change, so no HALT). All five frames report True, answer `writeStream`, and refuse `withWatermark` MBE-7; all five refuse MBE-18 on `toTable`, `start(path)` and `foreachBatch` start with the exact text and parameters; `collect` refuses DM-3 on all five and the full thirteen DM-3 doors refuse on `udf` and `mapInArrow` with the exact class, SQLSTATE and text. The MBE-18 row joins the sketch registry; the card cites the verify's `plan.udf.*` Spark cells. pins: mb-4/C-039 |
| C-040 | Fold 1b moves the remaining read-1 Python refusal logic to Rust: the continuous-trigger refusal (the facade `_start_checks` is deleted and `build_trigger` is the single home), the reader `path`-option lookup and the option de-duplication (the native `store_stream_option` / `stream_option_path` over the facade dict, in the new `streaming_options` module). Folded `PATH` options feed the reader `load` fallback and the plain-`start` path fallback. | The deleted Python, the new module with its unit tests, and the two facade pins. | **PROVEN** | `store_stream_option` drops case-variant keys then sets (the PH pin's `_options` assertion reads the same store-time state); `stream_option_path` returns the first non-empty folded `path`. The reader pin loads a stream through `.option("PATH", table)` with no path argument (kills read-1 P4 / verify PF); the writer pin reaches MBE-4 through `.option("PATH", sink)` with no path argument (a case-sensitive lookup would answer NO_PATH). The surface continuous pin gains the path its start needs to reach the trigger check; its MBE-7 assertion is unchanged. Read-1 mutant P2 (`toTable` without `_start_checks`) is not applicable: the method is gone. pins: mb-4/C-040 |
| C-041 | `toTable('')` refuses `ParseException` `PARSE_EMPTY_STATEMENT` with Spark's text byte-exact (a blank name echoes itself in the `== SQL ==` section); `toTable(5)`, `toTable(None)`, `streams.get(5)` and `streams.get(None)` refuse `PySparkTypeError` `NOT_STR`. | The native blank-name check with its unit test and the four facade pins. | **PROVEN** | `check_table_name_not_blank` runs in `to_table_stream` after the spec builds, so checkpoint, sink-match, unknown-option, trigger and mode refusals keep their order; the unit test pins the class, condition, both texts and the empty params. The facade `NOT_STR` checks sit at the native-call boundary (after kwargs/alive/UDF in `toTable`, after the alive check in `get`), the same position the builtin `TypeError` failed at, and PySpark's `toTable` applies its kwargs first too. Spark's own `toTable(5)`/`toTable(None)` answers are py4j-gateway shapes with no native twin, so read 1's `NOT_STR` is the ruled translation (the C-037 `format(5)` precedent). The writer-option check family moves unchanged to `streaming_options.rs` to fund `streaming.rs`'s ceiling (929 lines); the 60 streaming Rust tests stay green. pins: mb-4/C-041 |
| C-042 | Verify RM5 dies on a `query.stop()` pin: after the sink is replaced under a `foreachBatch` body, `awaitTermination` and `stop` both raise `RecoveryRequiredException`. `refuse_streaming_action` has a direct Rust pin (DM-3 on a streaming frame, pass on a batch frame). The `microbatch_py_err` wildcard stands: all 29 variants are named with a test each, the arm is unreachable today, and `#[non_exhaustive]` in `repark-iceberg` mandates it — the standing control is the repin duty, not an exhaustive match. | The facade pin, the Rust pin with its hand mutant, and the variant count. | **PROVEN** | The stop pin mirrors item 9's deterministic trigger; `stop` re-raises the stored `RecoveryRequired` outcome, and the raise is `stop`'s only `Err` arm, so the mutant (no raise) provably goes red. The guard pin loads a stream through `load_stream` on a memory catalog and asserts the DM-3 class, condition and verbatim text, plus the batch-frame pass; the inverted-predicate mutant goes red on it (the verdict's un-run mutant) and is restored green. The option-store, option-lookup and blank-name mutants each go red on exactly their new unit test and are restored green. pins: mb-4/C-042 |
| C-043 | The five absent surface members are declared (SES-DECL): `DataStreamWriter.partitionBy`, `StreamingQuery.processAllAvailable`, `StreamingQuery.explain` and `StreamingQueryManager.addListener`/`removeListener` raise `PySparkNotImplementedError` naming themselves instead of a bare `AttributeError`. | The five declarations and their pins. | **PROVEN** | Each member follows the `session_data_source` declared shape (the alive check where the receiver has one, then the member-naming `NOT_IMPLEMENTED` with `pins:` in the docstring); the pins assert class, text, parameters and SQLSTATE on the live door (`explain` in both arities). The `partitionBy=` start keyword stays accepted and ignored: Spark ignores it on existing v2 tables (PySpark `toTable` notes; the verify's three cells ran). `explain` and the listener methods were never in C-017's enumeration; C-017 stays OPEN for the remaining members. pins: mb-4/C-043 |
| C-044 | Fold 1b records the residue: the STREAM-SURFACE-RESIDUE-1 card lists the 31 divergent action/writer doors, the five declared absent members and the fence test that never reaches the append fence; MBE-16 (`__cause__`) is carried to the Opus lane with the placement analysis. | The card, the dated rows, and the handoff note. | **PROVEN** | The card groups the 31 by shape with both sides' answers and Spark's match target, notes the four no-pandas artefacts for re-measure, and records the fence counts (0 refusals in 5 runs of the old test; the race pin refuses 47 of 50 at the shipped delay). MBE-16 stays open: `BatchBodyAdapter::call_body` maps the body `PyErr` with `error.to_string()`, so the Python exception object is gone before the driver stores the error and `exception()` cannot rebuild a `__cause__`; the adapter is Opus-owned. pins: mb-4/C-044 |

## Decisions

- **D-1 (2026-10-08). The stub builds no frames.** The builders validate for real and
  stop at the terminal; the writer holds its frame opaquely and never inspects it; no
  token frame exists. The alternative (inert Python frames) would need an invented answer
  for every other `DataFrame` method, and DM-3 is unmeasured. Consequence:
  `surface_a.py` and `streaming_batch.py` need no stub edit and stay untouched, which
  keeps halt rule 4 trivially true.
- **D-2 (2026-10-08). The stub mirrors the `from_options` discriminators instead of
  calling it.** The binding cannot name `MicroBatchError` (no `repark-iceberg` edge,
  and `Cargo.toml` is out of scope) and no core file is in scope for a re-export, so
  the stub re-implements the §4-discriminated checks (skip-true, prefixed unknowns,
  first sorted key wins) as scaffolding. The wire-up deletes the scaffolding for the
  single source once it owns the reuse. The mirror covers only §4-discriminated cases;
  everything else is C-014.
- **D-3 (2026-10-08). Raise-site affinity splits the refusals.** MBE-3, MBE-17, MBE-4
  and MBE-10 raise from Rust as native exceptions. MBE-6 and MBE-7 discriminate in
  Python because `PySparkNotImplementedError` has no native class; the discriminators
  are exact mirrors of `check_output_mode`'s match, Q11 and the §3.7 continuous rule.
- **D-4 (2026-10-08). Ruling 4 governs the surface vocabulary, not merged behaviour.**
  Read strictly, "an option the sketch does not name is refused" would refuse plain
  reader keys that merged MB-1 passes (R-8, Spark-tolerant) and the four unsupported
  keys that `from_options` classes as `Catalog`. The stub follows `from_options`
  exactly on the reader and records the reading here; the writer has no merged
  behaviour, so its unknown handling is the Q2 question.
- **D-5 (2026-10-08). The stub refuses exactly the §4-discriminated cases.** Ruling 5
  gives every refusal a §4 class, so a case with no §4 row passes to the terminal
  instead of refusing with an invented class. That is C-014's list.
- **D-6 (2026-10-08). Check order inside the doors.** `start`/`toTable` order
  checkpoint, then sink, then writer unknowns (W8 primacy, then door completeness, then
  hygiene); `load`/`table` follow `from_options` key order. The sketch does not order
  multiple violations; any order satisfies "every §4 refusal real", and the pins hold
  this one.
- **D-7 (2026-10-08, acted on the North Star default; open for the owner to overrule).**
  Option plumbing mirrors PySpark and the batch reader: values convert via the `to_str`
  mirror (bool to lowercase, `None` held, else `str`), keys fold case with
  last-write-wins, every stored string registers for secret redaction, and `None`
  values drop at the door call (pyo3 cannot carry them, and treating `None` as unset
  answers MBE-4 where Spark crashes).
  - **The question:** how option keys and values convert and store.
  - **Flink:** not applicable; this is surface plumbing, not a guarantee.
  - **Spark:** the pinned PySpark 4.1.2 `to_str` converts bool to lowercase, holds
    `None`, and `str()`s the rest; the batch reader folds keys case-insensitively.
  - **The default acted on:** mirror both; drop `None` at the door.
- **D-8 (2026-10-08, acted on the North Star default; open for the owner to overrule).**
  Argument checks mirror the pinned PySpark 4.1.2 source method by method, including
  its inconsistencies (`table` checks `NOT_STR` while `toTable` checks nothing;
  `load` checks its path while `start` checks nothing; the manager refuses `< 0`
  while the query refuses `<= 0`). `foreachBatch` and `format` store without checks
  because PySpark does. Ruling 5 covers engine refusals; these facade checks keep
  their PySpark classes.
  - **The question:** which argument checks the builders run and when.
  - **Flink:** not applicable; this is surface shape, not a guarantee.
  - **Spark:** the pinned PySpark 4.1.2 method sources, read, not executed.
  - **The default acted on:** mirror each check at call time, omissions included.
- **D-9 (2026-10-08). The trigger stores validated strings unparsed.** The single-trigger,
  non-empty-string and is-`True` checks are real; the interval-to-duration parse waits
  for the Q4 grammar in the wire-up's Rust.
- **D-10 (2026-10-08). Pre-check details, each literal.** A missing output mode means
  `append` (Spark's default); `complete`/`update` fold case into MBE-6 and anything
  else passes to the wire-up; the format must equal `iceberg` exactly (Q11 literal,
  case-sensitive); mode and format strings are not trimmed; a refused format truncates
  to 64 characters in the feature (the batch mirror).
- **D-11 (2026-10-08). Checkpoint and sink presence is literal.** An option set to the
  empty string counts as specified; the session-conf fallback reads the facade conf,
  which accepts the key; the sink check runs only on the `foreachBatch` door.
- **D-12 (2026-10-08). The door shapes are wire-up-final.** `load_stream`,
  `start_stream` and `to_table_stream` take the session and frame handles, the trigger
  pair, the writer options, the checkpoint conf, the query name and the door extras
  (body, path, partitions) that the wire-up needs; the stub ignores the handles and
  the extras. An unset trigger crosses as the internal token `default` (R-15 maps it
  to `ProcessingTime(0)`). The two doors carry the standard too-many-arguments allow.
- **D-13 (2026-10-08). `errors.py` is not edited.** The file is outside the brief's
  list and no stub test needs the re-export or the structured methods; the streaming
  package imports the two exceptions from `repark._native` until C-016.
- **D-14 (2026-10-08). Exactly the §3.7 surface.** Every PySpark member the sketch does
  not list is omitted (C-017), including `load`'s `format`/`schema` kwargs; the sketch
  binds.
- **D-15 (2026-10-08). Test conventions for the round.** No docstrings or comments in
  the battery (brief ruling 7; the gate requires none in tests); cell ids and clause
  hints live in the test names; `pins:` citations live in `map.md`; terminal
  assertions are type-only so the Q1 ruling flips no text.
- **D-16 (2026-10-08). `lib.rs` funds its lines by the verbatim arm move.** The long
  exception names defeat one-line registration (`fn_call_width`), so the commit-unknown
  `to_py_err` arm moves verbatim to `exceptions::commit_state_unknown_py_err`; the
  root reads 183 of 190 and the moved pin stays green.
- **D-17 (2026-10-08). The IPI-47 answers and what each needs.** `R-STREAM-READ`
  validates and stops at the terminal; it needs the wire-up (frames, driver,
  execution) to read EQUAL. `R-STREAM-READ-SKIP` refuses MBE-3 where Spark skips
  (MB0-R3/R6); it needs a divergence disposition at step 4, never EQUAL.
  `W-STREAM-WRITE-FILESRC` refuses MBE-7 on both doors; it needs the dated
  file-source card (Q11), not just the wire-up. No inventory verdict moves.
- **D-18 (2026-10-08). The two MB-3 S3s carry untouched.** No driver-level race pin
  and no post-fence restart pin are run or written; both belong to the wire-up round
  and its verifier, and both are listed in the hand-back's `carried`.
- **D-19 (2026-10-08, acted on the North Star default; open for the owner to overrule).**
  The manager's vacuous answers are real: `active == []` and `get() is None` with no
  driver, and the timeout checks run before the terminal or the alive check.
  - **The question:** what the manager answers with no driver behind it.
  - **Flink:** not applicable; this is surface shape, not a guarantee.
  - **Spark:** the recorded `streams_active` oracle cell answers `[]` on an idle
    session, and the pinned `awaitAnyTermination` validates its timeout first.
  - **The default acted on:** answer the vacuous shapes for real, validate first.
- **D-20 (2026-10-08). Round-2 scope split.** Items 1 (C-019) and 4 (C-020) land with
  the M1/M4 pin hardening (C-021); items 2–3 halt on the hand-back's P1/P2 questions;
  items 5–15 are untouched. Applied brief rulings of 2026-10-08: W-Q1 (start/run
  split, per-variant records below), W-Q3/W-Q4/W-Q6/W-Q7 leans (W-Q7's empty-checkpoint
  text untouched — items 3–4 never reach `check_checkpoint`), W-Q2/W-Q5 pending (no
  action), Q3/Q4/Q5, Rust-first (reader validation and trigger move; outputMode and
  the format rule stay in Python), and the repark-core fence (only the relation.rs
  predicate; the P1 question asks for one re-export more).
- **D-21 (2026-10-08). P1: the binding cannot name `MicroBatchError`.** No crate
  outside `repark-iceberg` and `repark-core` names it; `repark-python` has no
  `repark-iceberg` edge by a deliberate EC-2 non-edge, `Cargo.toml` is out of scope,
  and the DAG script is unfenced. The mapper (item 2) and the reader wire-up (item 3)
  both need the type and its field types. Recommended: a `pub use` re-export through
  `repark-core`, following the three `repark_iceberg::write` precedents; the exact
  module (relation.rs beside the predicate, or a dedicated re-export) is the ruling.
- **D-22 (2026-10-08). P2: the DM-3 answer has no home in scope.** A batch action on
  a streaming frame must answer `_LEGACY_ERROR_TEMP_3102` with Spark's text, but the
  natural scan error classifies to `PySparkException` (pinned shape in
  `relation_tests.rs`, which must not move). Candidates: `error_map.rs` (fenced core),
  the native collect/export path (unlisted), `export_errors.py` marker mapping
  (unlisted, precedented), or `core.py` (the plan halts on it). Recommended: the
  orchestrator picks the layer; the brief's behaviour requirement stands either way.
- **D-23 (2026-10-08). Trigger parser records.** `trigger()` validates only; the
  parsed millis return to Rust tests and item 6 threads a `Duration` into the start
  spec — no unread stored attribute. `T4` carries the message parameters because the
  cells did not. Uncovered micro-cases, each unreachable or same-class with the
  natural rule: a lone dot refuses `INVALID_VALUE`; the 3262 echo keeps the input's
  case; multi-group day accumulation past `i32` answers raw (micros total overflows
  first); years scale to months exactly like weeks scale to days; a sign word before
  a signed word refuses `INVALID_VALUE` on the second; counts past `i128` refuse
  wrapped like every other huge count.
- **D-24 (2026-10-08). Mutant re-runs.** M1 red (the new `to_table` unknown-option
  pin), M4 red (the new both-violations order pin), M2 red as the read found, M3
  survived: its fix is item 3's `from_options` (a `skip=maybe` refusal), so no pin —
  a passing pin now would codify behaviour item 3 reverses.
- **D-25 (2026-10-08). Round-1 findings disposition.** Closed: S2-trigger-interval
  (Rust parser, full grammar, T1/T1B/T1C/T4 cells). Partial: S2-python-refusals (the
  trigger moved; reader, outputMode and format stay for items 3/6). Moved to round 3:
  S1, S2-format, S2-case-fold, S2-Q5-class, S2-reader-dup, S2-output-mode,
  S2-binding-shape, S3-None-return, S3-empty-checkpoint.
- **D-26 (2026-10-08). Deferred oracle cells.** `awaitAnyTermination` /
  `resetTerminated` and the y-sequence need stateful multi-query choreography the
  recorder cannot take without design; the `bogusfmt` and missing-table doors raise
  `Py4JJavaError`, a shape no cell needs yet; output-mode runs belong to item 6.
  All three move to round 3 with their scratch measurements intact.
- **D-27 (2026-10-08). Round-2 clause numbers.** C-017/C-018 were already taken by
  round-1 OPEN clauses, so the round-2 clauses are C-019 (oracle), C-020 (parser)
  and C-021 (mutant pins); the `pins:` citations in the touched maps use these.
- **D-28 (2026-10-08). P1 record: the re-export set.** `microbatch/mod.rs`
  re-exports `MicroBatchError`, `RecoveryReason`, the seven offset ids (`Epoch`,
  `FilePosition`, `QueryId`, `RunId`, `SinkRecord`, `SnapshotId`, `TableUuid`),
  `repark_common::Generation` and `iceberg::spec::Operation`: the error plus its
  direct field types, nothing else — no driver machinery, no `StreamSpec`, no
  `Uuid`, no `SinkRecord` components, no `Cargo.toml` edit, no new DAG edge.
  Follows the three `repark_iceberg::write` precedents (`partition_overwrite_mode.rs`,
  `session/writer_layout.rs`, `error_map.rs`). Two consequences for the mapper
  tests: id values come from `RunId::fresh().get()` with the `Uuid` type inferred
  and never named, and the `RecoveryRequired` pin covers `durable: None` only —
  a `Some` needs the un-re-exported offset-container types for a two-line
  `Debug` branch (round-3 call whether to extend P1).
- **D-29 (2026-10-08). Mapper rendering rules.** `microbatch_py_err` takes an
  optional `QueryHead`: `Some` selects the run rendering (Spark's `STREAM_FAILED`
  head with the live query and run ids, no progress dump — the dump needs live
  query state at the item-5/8 call sites), `None` the start rendering. The head
  presence never changes a variant's class. Run causes: MBE-1/2/16 carry the
  `Display` verbatim (Spark's prefix for MBE-1/2); MBE-11 carries its four named
  conditions; MBE-12/13 follow the same SCREAMING-variant pattern
  (`SOURCE_SNAPSHOT_EXPIRED`, `TRUNCATED_HISTORY`, `UNSUPPORTED_OFFSET_FORMAT`,
  `SINK_COMMITTED_TWICE`, `SINK_BUSY`); the W-Q1 run causes carry the `Display`
  with no condition bracket, the records' literal "as the cause". A run-class
  variant with no head renders headless (`[STREAM_FAILED] <cause> SQLSTATE:
  XXKST`) — total but unreachable via `load_stream`, since `open()` never
  produces MBE-12. `durable_offset` renders the `SinkRecord` `Debug`, the only
  total rendering that names no new type; no oracle cell measures the shape.
  Every `Display`-derived message and param is masked. The `#[non_exhaustive]`
  wildcard mirrors the duals' passthrough with the `error_map.rs`
  `match_same_arms` allow; a fork-added variant renders instead of failing the
  build, and the repin duties re-verify.
- **D-30 (2026-10-08). MBE-6/MBE-7 test seam.** The mapper imports
  `repark.errors` for the Python-defined class (the `cdf_infer/named.rs`
  precedent); the facade package is not importable from `cargo test`, so the
  three tests inject a stub `repark` parent plus a stub `repark.errors` carrying
  the real constructor shape into `sys.modules` and assert the exact message,
  condition, params and SQLSTATE the mapper passes. The real class accepts those
  kwargs — every facade MBE-6/MBE-7 pin proves it — so the only unverified step
  is the import succeeding in the wheel, where the facade imports the same
  module. The stubs are same-content idempotent and never removed, so parallel
  test threads cannot flake each other; no other `cargo test` imports under
  `repark.*`.
- **D-31 (2026-10-08). DM-3 placement and text.** The guard is one helper in
  the binding (`refuse_streaming_action`), called first by `count`, `show` and
  `__arrow_c_stream__`, which carry every facade batch action (collect, take,
  head, first, tail, `isEmpty`, `toLocalIterator`, count, show, `to_arrow`,
  batches, pandas, numpy). The class and text are MB-0c cell D1 verbatim —
  the brief's "cell D2" is a numbering slip (D2 is the DM-1 rows cell); the
  named class `_LEGACY_ERROR_TEMP_3102` disambiguates. The recorded tail
  (`;\niceberg`) ships verbatim per the "text as recorded" ruling. Deliberately
  unguarded, each with its reason: `transpose` (no Spark twin), ML
  `open_stream`, `input_files` (physical planning calls `scan`), batch writes
  (Spark's write-on-streaming answer is unmeasured and not DM-3-shaped) —
  all fail loud through the pinned scan refusal instead. No Rust unit test for
  the helper by design: building a streaming frame needs a catalog, which the
  facade pins own.
- **D-32 (2026-10-08). Item-3 reader rules.** `withWatermark` runs Spark's full
  validation first and refuses MBE-7 last, so argument errors keep their batch
  answers on both frame kinds. The format check folds inside `_refuse_format`
  (the refusal echoes the input's case); `table`, `toTable` and the
  `foreachBatch` start never call it, `load` and plain `start` always do, and
  the check order on plain `start` is unchanged (format, mode, continuous).
- **D-33 (2026-10-08). Q5-start deferred to item 6.** The `start()`-side
  no-path refusal and the path-to-sink mapping belong with the `StreamSpec`
  sink work, not the reader wire-up; item 3 implements the Q5 `load` half
  only. No existing pin breaks: every `start()` pin either passes a sink or
  reaches its check before any path rule could fire.
- **D-34 (2026-10-08). Superseded round-1 pins.** C-001's seven stub-validation
  pins are deleted with `validate_reader_options` (the cases now belong to
  `from_options` plus the C-014 facade pins); C-002's `load_stream` pin is
  replaced by the open-through-the-mapper pin while the writer pins hold
  through the mapper routing; the surface battery's `passes_to_stub` tests
  return frames and the case-sensitive-format pin becomes the fold pin. The
  C-001/C-002/C-004/C-005 rows keep their round-1 verdicts as history; this
  decision is the forward pointer.
- **D-35 (2026-10-08). Round-2b findings disposition.** Closed: S1 (every
  door), S2-format, S2-case-fold, S2-Q5-class, S2-reader-dup, S3-None-return.
  Still open for round 3: S2-output-mode (item 6), S2-binding-shape (items
  5–8), S3-empty-checkpoint (W-Q7), plus the new carry: Q5-start (D-33), the
  stale `isStreaming` docstring on `core.py` (out of fence), the
  `durable: Some` pin (D-28), and `dropDuplicatesWithinWatermark`/`writeStream`
  on streaming frames (still refusing; unmeasured, unwired).
- **D-36 (2026-10-08). Round-2b mutant re-runs.** M1 red (writer unknowns
  forced to pass; the `toTable` MBE-17 pin), M2 red (the format arm forced to
  accept `parquet`; the load MBE-7 pin), M3 red (the `from_options` skip flag
  forced to refuse non-`false`; the new C-014 value-cases pin — the kill
  round 2 deferred), M4 red (the sink check moved after unknowns; the order
  pin). Each mutation applied, observed red, reverted, and re-observed green;
  the tree is clean.
- **D-37 (2026-10-08). Round-2b size and neighbours.** Five commits: P1
  (14+/1−), item 2 (979+/1−: ~230 product, ~730 tests, 8 errors.py), item 3
  (486+/308−), the one-line clippy allow, the ruff-format pass. Batteries:
  83 passed (70 surface + 13 wireup). Binding suite 202, core microbatch 113.
  Neighbours: 69 passed, 9 skipped against a 68/9 baseline — the delta is the
  one new watermark pin, zero flips. Largest files: `streaming_errors.rs` 982
  (default ceiling 1000; the next mapper extension splits tests to a `#[path]`
  child), `lib.rs` 186/190, surface battery 805.
- **D-38 (2026-10-08). Observed, unruled, left alone.** A `load`/`table` of a
  missing table answers `AnalysisException` with the `Catalog` passthrough
  verbatim (no MB-0c cell pins our answer). `open()` never resolves offsets,
  so no MBE-12 variant reaches the mapper at load. A no-query `STREAM_FAILED`
  is mapper-total but caller-unreachable in this round. `core.py` is untouched
  per P2, so its `isStreaming` docstring still says batch-only.
- **D-39 (2026-10-08). W-Q2 answered: REFUSE EVERYWHERE.** The owner's delegate rules the `foreachBatch` door keeps the W8 refusal (no `checkpointLocation`, no query). DM-1 is no longer an open owner question: it is a dated divergence (Spark runs the door on a temporary checkpoint directory plus a warning), with an oracle cell and a Spark-matching card owed in round 4. The refusal pins as the W8 rule with the citation, not as a parity answer.
- **D-40 (2026-10-08). The no-detach hatch in the adapter.** `BatchBodyAdapter::call_body` runs under `with_stream_poll_no_detach` (the hatch already used by the stream-poll path in `dataframe.rs`): it keeps the body on a thread the GIL bridge never detaches, because a batch action in the body (`collect`, `count`, `show`) re-enters the runtime from a `block_in_place` worker and detaching there crashed the process with a fatal GIL error. Without the hatch the first pin died; with it the body completes.
- **D-41 (2026-10-08; corrected 2026-10-09, fold 1). W4 stamp shape.** Each foreach epoch lands two sink snapshots: the body's append (`added-records` only) and the driver's stamp-only commit (`repark.cdc.*` keys, no `spark.sql.streaming.*` keys, per the R-5 driver rule). The query id is stable across a resume, the run id fresh. The 2026-10-08 tail ("not a divergence row") was wrong: the Opus verify measured the door duplicating rows when the body writes and the epoch then fails before the trailing stamp (D-54, C-035 OPEN). The two-snapshot shape is still the offset mechanism, but the stamp history hides replays, so the shape is evidence for the open question, not a closed record.
- **D-42 (2026-10-08). Round-3b halt.** The batch-action pin's `count` leg fails (C-027 OPEN): the micro-batch provider cannot serve the empty projection `count` plans. `collect` and `show` pin green on their own test; the count fix sits in `repark-iceberg`, outside the fence. Item 8, the inventory regen, neighbours and the round-3 close ride the next round after the ruling.
- **D-43 (2026-10-08). The count-in-body fix (round-3c Q1, option a).** Granted
  narrow: `read_batches` in `crates/repark-iceberg/src/microbatch/provider.rs`
  serves a zero-field projected schema through `zero_column_batch`, which
  rebuilds with `RecordBatch::try_new_with_options` and the incoming row count
  stated explicitly. Non-empty projections still read through the crate's
  `conform_batch`, which is NOT edited, so no other provider moves. The unit
  test pins `COUNT(*)` plus an explicit empty projection over three planned
  files including an empty one, and the vacant batch directly.
- **D-44 (2026-10-08). Zero-column shape measured, not fixed, on the other
  `conform_batch` providers.** On a scratch table each fails `count(*)` with
  the identical engine-Internal cause the provider fix removes:
  `Internal error: iceberg scan could not rebuild batch: Invalid argument
  error: must either specify a row count or at least one column`. Changelog:
  `SELECT COUNT(*) FROM t.changes` fails. Incremental: a snapshot-windowed
  reader `.count()` fails. Lineage: `SELECT COUNT(*) AS _row_id FROM v3`
  fails (the alias trips the token-based lineage rewrite while the scan
  projection stays empty; the `WHERE _row_id IS NOT NULL` form answers
  because the filter forces a non-empty scan projection). Each becomes a card
  the orchestrator files; all three sit outside the round-3 fence.
- **D-45 (2026-10-08). The manager state lives in the binding (W-Q3).** The
  driver drops terminal handles from its registry, so the binding keeps its
  own per-session `BindingManagerState` as a DataFusion config extension:
  known handles plus the terminated-since-reset set. `start_spec` records
  every start at the one site both doors share. `awaitAnyTermination` reaps
  before it answers and level-reports until `resetTerminated`, which also
  prunes terminal handles from known — without the prune the next reap
  re-reports the stopped query (found by the C-028 pins, fixed in the item).
  A failed termination raises the query's own `STREAM_FAILED`, per the
  scratch y-sequence and the PySpark contract; no MB-0c cell covers it.
- **D-46 (2026-10-08). `get` parses Java's UUID grammar, not the canonical
  shape.** `UUID.fromString` accepts any five dash-separated hex groups, so
  `get("1-2-3-4-5")` parses and answers `None` instead of refusing; lookup
  compares parsed values, so folded and short forms find the query. Empty or
  overlong groups refuse `Invalid UUID string` where Java raises
  `NumberFormatException`; that corner is unmeasured and kept as the one
  deviation.
- **D-47 (2026-10-09). Race-lane Q1: `durable()` joins the exception and the
  outcome.** The race lane measured (R-4) that a query ending `RecoveryRequired`
  before its first resume reports the record on the exception and the
  `ShutdownOutcome` but `QueryHandle::durable()` stays `None`; the ruling adopts
  the reported record into `Lifecycle.durable` in `conclude`, guarded to the
  unset case so resumed queries never change. C-029.
- **D-48 (2026-10-09). Item 9 stops every known query, then releases, then
  raises (W-Q4).** `streams_stop_all` iterates the binding's known handles (an
  active-only sweep would leave the W-Q4 raise nearly dead, since most
  `RecoveryRequired` endings happen asynchronously) and prunes known after, so
  a second stop finds nothing and stays quiet. The facade takes `_inner` first:
  `_inner is None` is the stopped marker, so the raise path must clear it
  before the cleanups run. The deterministic recovery trigger is the sink
  replaced under a `foreachBatch` body. C-030.
- **D-49 (2026-10-09). Item 10 renames the three flipped pins.** Keeping
  `test_read_stream_declared` on an answering pin would lie; the rename map is
  in C-031 and the battery map row. Neighbours run against `40fc916f` through
  the `read1/main` copy, verified byte-identical to the base tree (4963 blobs;
  the prebuilt native module predates streaming), driven by this lane's venv
  with `PYTHONPATH` shadowing. The `refuse_write_stream` → `write_stream`
  rename moves no frozen name. C-031.
- **D-50 (2026-10-09; legs replaced 2026-10-09, fold 1). Item 11's error legs
  split vacuous from biting.** Seven surfaces assert absence as regression
  guards; the value-echo refusal leg bites on the mapper masking today
  (`s3://u:***@h/x`), pinned both ways so it fails on a leak and on a silenced
  echo alike. The verify called two legs vacuous (the EXPLAIN refusal-text leg
  and the never-stored checkpoint leg); fold 1 replaces both with
  echoing-refusal legs and moves the pin to the fold-1 battery. C-032, C-037.
- **D-51 (2026-10-09). Item 12 folds the skip-`maybe` second divergence into
  the draft.** D-17's owed disposition: the MB-4-SKIP-OPTIONS-1 draft below now
  records that a non-boolean skip value refuses at the C-014 value check while
  Spark reads it as false (MB0-R5 scratch, per the wireup plan). The registry
  doc itself is untouched — item 15 files both rows. C-033.
- **D-52 (2026-10-09). W-Q2's owed cell and card are filed; DM-1's record now
  cites them.** D3's capture attaches a log4j2 `FileAppender` over the JVM
  door: the first attempt (fd redirect around the starts) caught nothing
  because the gateway child inherits `stderr` at spawn. The temp path
  normalizes to `$TEMP_CHECKPOINT`; zero warnings is a recorder crash, never a
  recorded row. D-39's owe is closed; the TEMP-CHECKPOINT-1 card holds the
  Spark-matching answer and the product keeps refusing. C-034.
- **D-53 (2026-10-09). EMPTY-PROJECTION-COUNT-1 re-measured first-hand.**
  All three D-44 repros fail on this tree with the texts the card quotes; the
  lineage `WHERE` control answers 4. One delta from D-44: the batch changelog
  read answers its rows here (D-44 recorded a clean MBE planning-error
  refusal), so only the empty projection fails on that door now.
- **D-54 (2026-10-09). Fold-1 S1 recorded, not fixed: the `foreachBatch`
  door duplicates rows in its declared sink when the body writes and the epoch
  then fails or the process dies before the trailing stamp.** The stamping is
  the merged MB-3 default and an owner question, so fold 1 touches no driver
  line. Verbatim from the Opus verify (`verdict.json`, FAIL S1; one-process
  repro `p/repro_s1.py`; choreographies `p/eo_all.sh f_writeraise |
  f_exitafterwrite | f_kill | f_killcommit` over harness `p/eo.py`, public
  doors only, memory catalog re-attached per process with `CALL
  sc.system.register_table` on the newest metadata file; source 6..30 one-file
  commits of 4 rows, `streaming-max-files-per-micro-batch=1`, body =
  `df.writeTo('sc.db.snk').append()`, option `repark.cdc.sink=sc.db.snk`, same
  `checkpointLocation` and `queryName` on every start): "Interleaving: epoch N
  body appends (unstamped snapshot, summary has no `repark.cdc.*` key) -> body
  raises / `os._exit` before the driver's trailing `commit_stamp_only` ->
  restart with the same checkpoint and query name starts silently (no
  `RecoveryRequired`, no `UnstampedSinkCommit`), resumes at epoch N, body
  appends the same rows again. `f_writeraise` (`ValueError` after the write at
  epoch 2, one restart): src 24, sink 28, dup 4. `f_exitafterwrite`
  (`os._exit(42)` after the write at epoch 2): sink 28, dup 4. `f_kill` (10
  `os._exit` at random 150-1350 ms): src 120, sink 132, dup 12. `f_killcommit`
  (8 kills on seeing a new sink metadata file): src 80, sink 96, dup 16. Epoch
  stamps stay unique and gapless in every run, so the summary history hides the
  duplicates. In-process a body that appends twice is not refused either (sink
  1,1,2,2,3,3)." The minimal repro (source 1,2,3; body appends then raises
  once; second start, same checkpoint and name) lands sink 1,1,2,2,3,3 with
  snapshots append/None, append/None, append/epoch 0. Origin is the merged
  MB-3 driver default (MB-3 ledger D-2 / C-005 b-c, "acted on the default,
  open for the owner to overrule"), worded there as "at-least-once on
  multi-write bodies"; the verify shows a single-write body duplicates too.
  Spark's own `foreachBatch` contract is at-least-once (MB-0 W4/W6, not
  re-measured by the verify), so the measured behaviour is parity with Spark
  and a breach of the exactly-once claim and sketch Q9; which binds is the
  owner's call. Raise-before-write, kill-before-write, stop/restart x8 and
  `spark.stop` x5 on this door were exact, and the `toTable` door was exact
  through 8 stop/restarts, 6 random kills, 8 kills at commit and 5 `spark.stop`
  cycles. C-035 OPEN.
- **D-55 (2026-10-09). Fold 1 puts the R-12 test seam on the session, not the
  door.** `start_spec` is the one launch site, so the mark lives beside
  `BindingManagerState` as a `SessionConfig` extension: no global flag (the
  server-prep discipline), no option key (R-12 forbids one), no `driver.rs`
  line, and the facade doors take no new parameter a user could pass. The
  batteries opt in once per `spark` fixture; the refusal pins build their own
  unmarked sessions. C-036.
- **D-56 (2026-10-09). Fold 1 masks at the raise site, in Rust, with the
  neighbours' helper.** `mask_user_visible` only rewrites credential-shaped
  values (registered secrets, URL userinfo), so every innocent echo the sweep
  and facade pins assert passes through byte-identical; plain-token echoes
  stay visible everywhere by the same design. The format refusal keeps its
  exact feature shapes and door dispatch and moves its body to
  `check_stream_format`, following the `outputMode` setter/native-check split.
  `format(None)` keeps meaning unset (Spark passes it at `format()` time and
  fails later at `load()`); only non-str non-`None` refuses `NOT_STR`. C-037.
- **D-57 (2026-10-09). Fold 1 interrupts waits at the GIL, never inside the
  driver.** The slices shorten the Rust block, never the driver's own wait:
  the query task is untouched, so an interrupt cannot strand or half-stop a
  query, and the timeout accounting still reads the driver's answers against
  one deadline. The 10 s pin bound covers a loaded build box (the signal lands
  at 2 s; the rest is interpreter start). C-038.
- **D-58 (2026-10-09). Fold 1 refuses UDF-over-stream actions where actions
  execute, not where plans build.** The DM-3 check sits in `_action_inner`
  and `_consume_map_in_arrow_batches`: plan snapshots keep the batch-only
  reader and their base answers, and `persist` / `cache` stay lazy-ok as at
  base. The DM-3 text keeps one home: the facade calls the newly exposed
  native guard against the streaming ancestor instead of re-raising the text.
  No UDF plan-build line changed, so the brief's HALT tripwire never fired.
  The two guard call sites would have grown `core.py` past its exact
  baseline, so the map-bridge execution family moves to `map_bridge.py` as a
  pure move (bodies byte-identical, bound as methods, proven by diff) and the
  baseline ratchets 3461 → 3260. C-039.
- **D-59 (2026-10-09). Fold 1b reorders the continuous refusal after the
  start checks.** Deleting `_start_checks` moves the MBE-7 refusal from the
  facade pre-check into `build_trigger`, so on a multi-violation start the
  path, checkpoint, unknown-option and output-mode refusals now answer before
  the trigger refusal. Spark runs continuous triggers, so on every such
  combination Spark answers the other violation — the new order matches Spark
  where the old one contradicted it. A continuous trigger still refuses on
  every door; no input is newly admitted. This supersedes D-32's
  `(format, mode, continuous)` start order. C-040.
- **D-60 (2026-10-09). Fold 1b places the `toTable` argument checks where the
  driver failed.** The blank-name check runs after the spec builds because the
  driver's identifier check runs at register, after every start check — so a
  blank name with no checkpoint still answers MBE-4, as at base. The one order
  change is blank name plus duplicate query name, which now answers the parse
  refusal instead of the name collision (the name check lives in the shared
  `start_spec`, past which a `toTable`-only check cannot reach). `table('')`,
  the missing-table `TABLE_OR_VIEW_NOT_FOUND` shape and two-part/one-part name
  resolution are the same finding's other halves and stay as they are: step 5
  scopes to `toTable('')`, `toTable(5)` and the `get` checks. C-041.
- **D-61 (2026-10-09). Fold 1b narrows D-31 and keeps D-29.** D-31's "no Rust
  test for the helper" rested on catalog-backed frames belonging to the facade
  pins; the C-024 MBE-8 Rust tests already build memory-catalog sessions, so
  one Rust pin now covers the helper directly and the facade pins keep the
  door-level coverage. D-29's wildcard stands as written: an exhaustive match
  over a `#[non_exhaustive]` fork enum is a compile error, so the verdict's
  "exhaustive match" expectation is unimplementable and the repin duty in the
  crate map is the control that keeps the 29 named arms complete. C-042.
- **D-62 (2026-10-09). Fold 1b records the 31 divergent doors without
  touching them.** Groups: 9 batch writes (`PySparkException` scan refusal
  vs `CALL_ON_STREAMING_DATASET_UNSUPPORTED`), 3 lazy-ok returns, 2
  `explain` prints, 6 generic-scan-instead-of-DM-3, 4
  `UnsupportedOperationException`, 1 `crosstab` condition variant (3102 vs
  Spark's 3063), 4 no-pandas artefacts owed a re-measure, 2 miscellaneous
  (`rdd`, `__arrow_c_stream__`). The 53 matching doors keep the recorded
  `;\niceberg` DM-3 tail; only the text-as-recorded ruling moves it. C-044.
- **D-63 (2026-10-09). The old fence test is recorded, not rewritten.**
  `two_drivers_on_one_sink_land_each_epoch_exactly_once` lands 0 fence
  refusals in each of 5 runs (verdict logs `fence_old_two_drivers_1..5`);
  the race pin refuses 47 of 50 at the shipped 0–12 ms delay and stays
  green at 0, 0–3 and 0–60 ms. The older test's name claims a fence it
  does not exercise. C-044.
- **D-64 (2026-10-09). MBE-16 rides the Opus lane.** The body's Python
  exception object is dropped in `BatchBodyAdapter::call_body`
  (`MicroBatchError::Catalog(error.to_string())`), so only type and
  message reach the stored error and `query.exception()` has no object
  to chain. Attaching the `__cause__` needs the adapter to preserve the
  `PyErr`, which the fold-1b brief assigns to the Opus lane with the
  driver and `repark-iceberg` microbatch. This round edits none of
  `streaming_query.rs`'s adapter, `driver.rs` or anything under
  `crates/repark-iceberg/` or `crates/repark-core/`. C-044.

## W-Q1 records (2026-10-08, brief ruling of 2026-10-08)

At start: `IllegalArgumentException` for option-value refusals, `AnalysisException`
for the rest. At run: `StreamingQueryException` `STREAM_FAILED` with the variant as
cause. One record per variant with no §4 row, for item 2's mapper round.

- **`Catalog(String)`.** Start: `AnalysisException` (a catalog passthrough, never an
  option-value refusal). Run: `STREAM_FAILED` with the string as the cause.
  Text: the passthrough verbatim. No SQLSTATE, no condition.
- **`CatalogTimeout`.** Start: `AnalysisException` (a catalog call during table
  resolution). Run: `STREAM_FAILED` with the variant as the cause.
  Text: the call name, the waited `Duration`, the offset-did-not-advance tail.
- **`AwaitFromDriver`.** Never raised at start; raised only from wait paths on a
  running query, so the run branch: `STREAM_FAILED` with the variant as the cause.
  Text: Spark's `IllegalStateException` text verbatim, which the variant carries.
  PySpark 4.1.2 has no `IllegalStateException` (verified against the
  `/tmp/sparkenv` inventory), so the class cannot be copied, only the text.
- **`DriverPanicked`.** Run-only (batch N panicked): `STREAM_FAILED` with the
  variant as the cause. Text carries the epoch and the panic message.
  Never raised at start.
- **`AlreadyCommitted`.** Run (a replayed epoch commit): `STREAM_FAILED` with the
  variant as the cause. Text names the query and the epoch. Never raised at start.
- **`SnapshotNotInLineage`.** Start (resume planning): `AnalysisException`. Run
  (first trigger): `STREAM_FAILED` with the variant as the cause.
  Text: the full guidance with table, snapshot, head and the start-after fix.
- **`OffsetPositionOutOfRange`.** Start (resume planning): `AnalysisException`. Run
  (first trigger): `STREAM_FAILED` with the variant as the cause.
  Text: the corrupt-offset guidance with table, snapshot, position and file count.

## Round-4 carry-over (2026-10-09)

- Items 9–12 landed (C-030, C-031, C-032, C-033): `stop` releases through the
  native stop-all, the public doors answer, credentials appear on no surface,
  and the IPI-47 dispositions are recorded with the FILE-SOURCE-1 card under
  MB-5. Race-lane Q1 landed (C-029); the W-Q2 cell and card landed (C-034,
  TEMP-CHECKPOINT-1); EMPTY-PROJECTION-COUNT-1 covers the D-44 doors.
- Round 5 is item 15 only: file the MB-4-SKIP-OPTIONS-1 and MB-4-FORMAT-1 rows
  (drafts above, the skip row now carrying the `maybe` arm), close the ledger
  to `completed/`, and settle C-009, C-015, C-017 and C-018.
- C-015 stays OPEN with the flip, the stop fold and the SES-DECL retirements
  landed; only its registry rows ride item 15.
- Neighbours at close: the round-2 selection reads 68 passed on `40fc916f`
  and 69 passed at head — the delta is exactly the three item-10 renames plus
  the branch's watermark pin, zero flips. The full gate roster is green
  (ledger grammar, map sync, manifest, freeze, example coverage, the touched
  crates, the MB-4 batteries).

## Round-3c carry-over (2026-10-08)

- Items 5–8 landed (C-024, C-025, C-026, C-027, C-028); items 9–12 move to
  round 4, item 15 (registry rows, ledger close) after them.
- `readStream`/`streams`/`writeStream` still refuse exactly as on main: no
  `session_core.py` edit, no SES-DECL row flips.
- DM-1 stays a dated divergence with an oracle cell and a Spark-matching card
  owed in round 4 (D-39); the zero-column `count(*)` failures on the
  changelog, incremental and lineage doors each owe a card too (D-44).
- `awaitAnyTermination`/`resetTerminated` are wired and pinned to the scratch
  y-sequence, but no MB-0c oracle cell covers them; the remaining deferred
  cells ride with their items. W-Q4 binds item 9 by its lean; W-Q5 is still
  with the orchestrator for item 12.
- Neighbours at close: the round-2 selection collects 69 tests with per-test
  outcomes identical at 6c463029 and head (69 passed both, zero flips). Summary
  skips read 6 at base and 9 at head; the 3 extra are whole-module collection
  skips for missing `duckdb` in the older `.venv` (the fresh worktree sync has
  it), in smoke files this round never touches.

## Round-3 carry-over (2026-10-08)

- Items 2–3: blocked on the hand-back's P1 (mapper home) and P2 (DM-3 home).
- Items 5–15 untouched; `readStream`/`streams`/`writeStream` still refuse as on main.
- Deferred cells: `awaitAnyTermination`/`resetTerminated`, the y-sequence,
  `bogusfmt`/missing-table doors (`Py4JJavaError`), output-mode runs (D-26).
- W-Q2/W-Q5 pending with owner/orchestrator; empty-`checkpointLocation` text (W-Q7)
  untouched; outputMode and the format rule stay Python-side until item 6 (D-20).

## Round-2b carry-over (2026-10-08)

- Items 2–3 landed (C-022, C-023); P1/P2 answered and implemented.
- Items 5–15 untouched; `readStream`/`streams`/`writeStream` still refuse as on
  main; Q5-start, the `core.py` docstring, the `durable: Some` pin and the
  streaming `dropDuplicatesWithinWatermark`/`writeStream` answers ride with
  their items (D-35).
- Deferred cells, W-Q2/W-Q5 and W-Q7 unchanged from the round-3 carry-over.

## Registry drafts (step 4 files these; not yet added to the registry doc)

- **MB-4-SKIP-OPTIONS-1** — a streaming read that sets a skip option refuses.
  - **repark** — `streaming-skip-overwrite-snapshots` / `streaming-skip-delete-snapshots`
    set true refuse at `load`/`table` with `IllegalArgumentException`
    `[REPARK_MICROBATCH.SKIP_OPTION_REFUSED]` (MBE-3), naming the option, no SQLSTATE.
    Set false they pass.
  - **Apache Spark** — skips the snapshots and runs on. *(oracle: MB0-R3, MB0-R6.)*
  - **Pin** — `crates/repark-python/src/streaming.rs::skip_options_set_true_refuse_with_skip_option_refused`,
    `python/repark/tests/test_mb_4_streaming_surface.py::test_reader_skip_overwrite_true_refuses_mbe3_mb0_r3`,
    `::test_reader_skip_delete_true_refuses_mbe3_mb0_r6`
  - **Rationale** — DECLARED 2026-10-08 (MB-4, sketch O-5). Bronze is append-only; a
    skip hides rows in a store that cannot rewind, so the read refuses instead.
    A non-boolean value (measured `maybe`, item 12) refuses at the C-014 value
    check with `streaming-skip-delete-snapshots needs true or false, got
    "maybe"`; Spark reads it as false and runs (MB0-R5 scratch, per the wireup
    plan), and that second divergence stands refused with this row.
- **MB-4-UNKNOWN-OPTION-1** — an unknown option under an interpreted prefix refuses.
  - **repark** — `load`/`table`/`start`/`toTable` refuse an unknown `streaming-*`,
    `stream-*` or `repark.cdc.*` key with `IllegalArgumentException`
    `[REPARK_MICROBATCH.UNKNOWN_OPTION]` (MBE-17), naming the key (masked), no
    SQLSTATE. Plain keys pass, as Spark ignores them.
  - **Apache Spark** — ignores unknown keys everywhere. *(oracle: no MB-0 cell; the
    North Star §5 deny-unknown-fields rule disposes the divergence.)*
  - **Pin** — `crates/repark-python/src/streaming.rs::unknown_prefixed_reader_options_refuse_with_unknown_option`,
    `::unknown_prefixed_writer_options_refuse_with_unknown_option`,
    `python/repark/tests/test_mb_4_streaming_surface.py::test_reader_unknown_prefixed_option_refuses_mbe17`,
    `::test_writer_unknown_prefixed_option_refuses_mbe17`
  - **Rationale** — DECLARED 2026-10-08 (MB-4, NS §5). A misspelled interpreted key
    silently unbounds a Bronze read; the refusal names the key.
- **MB-4-CHECKPOINT-1** — an Iceberg-sink query without a checkpoint refuses at start.
  - **repark** — `start`/`toTable` without the `checkpointLocation` option or the
    `spark.sql.streaming.checkpointLocation` conf refuse with `AnalysisException`
    `_LEGACY_ERROR_TEMP_1298` (MBE-4), the W8 text verbatim, no SQLSTATE.
  - **Apache Spark** — refuses the same way for `toTable`. *(oracle: MB0-W8,
    text-verbatim.)*
  - **Pin** — `crates/repark-python/src/streaming.rs::missing_checkpoint_refuses_with_the_w8_text`,
    `python/repark/tests/test_mb_4_streaming_surface.py::test_writer_missing_checkpoint_refuses_mbe4_mb0_w8`
  - **Rationale** — PARITY 2026-10-08 (MB-4, sketch Q1). Same class, same text.
- **MB-4-OUTPUT-MODE-1** — `complete` and `update` output modes refuse.
  - **repark** — `start`/`toTable` with either mode refuse with
    `PySparkNotImplementedError` `NOT_IMPLEMENTED` (MBE-6),
    `{feature: outputMode(complete|update)}`, no SQLSTATE.
  - **Apache Spark** — runs both. *(oracle: MB0-W3 runs `complete`.)*
  - **Pin** — `python/repark/tests/test_mb_4_streaming_surface.py::test_writer_complete_mode_refuses_mbe6_mb0_w3`,
    `::test_writer_update_mode_refuses_mbe6`
  - **Rationale** — DECLARED 2026-10-08 (MB-4, sketch R-3). Bronze is append-only;
    only `append` runs.
- **MB-4-FORMAT-1** — a non-iceberg streaming format refuses.
  - **repark** — any `readStream.format(<x>)` / `writeStream.format(<x>)` with
    `<x>` other than `iceberg` refuses with `PySparkNotImplementedError`
    `NOT_IMPLEMENTED` (MBE-7), `{feature: readStream.format(<x>)}` /
    `{feature: writeStream.format(<x>)}`, no SQLSTATE. A missing format refuses as
    the `parquet` default. `trigger(continuous)` refuses the same way.
  - **Apache Spark** — runs file sources and continuous triggers. *(oracle: no MB-0
    cell; Q11 and NS-9 dispose the divergence; the file source is a dated card.)*
  - **Pin** — `python/repark/tests/test_mb_4_streaming_surface.py::test_reader_format_must_be_iceberg_mbe7`,
    `::test_writer_format_must_be_iceberg_mbe7`,
    `::test_writer_continuous_trigger_refuses_mbe7`
  - **Rationale** — DECLARED 2026-10-08 (MB-4, sketch Q11, NS-9). The Bronze door is
    the only streaming source and sink in this round.
- **MB-4-SINK-1** — a `foreachBatch` start without a declared sink refuses.
  - **repark** — `start` with a batch body and no `repark.cdc.sink` option refuses
    with `IllegalArgumentException` `[REPARK_MICROBATCH.SINK_UNDECLARED]` (MBE-10),
    naming the option, no SQLSTATE.
  - **Apache Spark** — needs no sink on this door. *(oracle: MB0-W4.)*
  - **Pin** — `crates/repark-python/src/streaming.rs::undeclared_foreach_sink_refuses_with_sink_undeclared`,
    `python/repark/tests/test_mb_4_streaming_surface.py::test_writer_foreach_without_sink_refuses_mbe10_mb0_w4`
  - **Rationale** — DECLARED 2026-10-08 (MB-4, sketch Q9). The stamp lands on the
    declared sink; without one the query has nowhere to record itself.


## Race pins carried from MB-3 — plan items 13 and 14 (2026-10-08, Opus worker lane)

Branch `test/mb-4-race-pins`, cut from `feat/mb-4-facade` at `6c463029`. Test-only: no
product file changes. The clause ids are the item numbers plus 100 so they cannot collide
with the rows the parallel lane adds to the table above.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-113 | Item 13: two drivers of one query (one `QueryId`, two `RunId`s) in two sessions over one catalog, started off a barrier with no injected commit, land every source row in the `toTable` sink exactly once and stamp each epoch once, over 50 iterations; the driver that lost ends `Fenced` or `RecoveryRequired` naming the winner's run; and the race reaches the append fence, not only the in-process resume-point check. | `crates/repark-core/src/microbatch/race_tests.rs::two_sessions_racing_one_query_land_every_row_exactly_once`, three runs, plus one hand mutant on the fence. | **PROVEN** | Green three times: 19.64 s, 23.76 s, 17.87 s test time (debug build, cores 48-63, 8 jobs), under the 60 s bound, so the count stays 50. The loser was refused at the commit in 46, 48 and 46 of 50 iterations. Mutant M13 (`AppendFence::update_table` forwards every commit, `append_fence.rs`): red at iteration 1, sink rows `[1, 1, 2, 2, 3, 4]`, history epoch 0 stamped by both runs; restored. |
| C-114 | Item 14: after the fence's `RecoveryRequired` ending (a property-only foreign commit inside the driver's commit refresh), a restart of the same sink and `queryName` on a fresh session and manager refuses with the error the sketch names, `RecoveryRequired(StampedSnapshotExpired)` carrying the property's record as the durable offset, on both doors and on a second restart; it runs no body and leaves the sink with no snapshot, stamp or row. | `crates/repark-core/src/microbatch/race_tests.rs::a_restart_after_the_fences_recovery_required_ending_refuses_by_name`, plus hand mutants on the resume. | **PROVEN** | Green, 0.20 s. Sketch Q10 and §3.4: "Property present but no stamped snapshot retained: `RecoveryRequired(StampedSnapshotExpired)`". Mutant M14a (`read_resume_point` returns the bare property as the resume point, `sink_offsets.rs`): red, on the first run's ending (`Fenced` instead of `RecoveryRequired`), because the fence shares the function. Mutant M14b (both `read_resume_point` calls in `run.rs` accept the durable record of a `RecoveryRequired`): red on the `foreachBatch` door, "the refused restart ran a body", 1 call for 0; the `toTable` door stayed green under M14b because the claim's epoch check refuses the commit with the identical error. Both restored. |

- **R-1 (2026-10-08). The free-running in-process race does not reach the fence.** Measured
  first without `SlowCatalog`, with the drivers' polling delay at zero: 50 of 50 iterations
  correct, 0 of 50 with a staged file the sink never added. The sink's `BatchScope` serialises
  the two drivers and the later one is fenced by `run.rs`'s resume-point check. A pin that
  only exercised that path would stay green with the append fence deleted, so the pin holds
  each loaded sink view for a seeded 0 to 12 ms (a test catalog wrapper, no product hook) and
  asserts at least 10 of 50 iterations were refused at the commit.
- **R-2 (2026-10-08). `RecoveryRequired` as the loser's ending was never observed.** Every
  losing driver in the three green runs ended `Fenced`. The pin accepts `RecoveryRequired`
  only when its durable record names the winner's run, as the plan's item 13 allows.
- **R-3 (2026-10-08). The restart's answer is a refusal, and it is stable.** The property-only
  commit leaves the sink with this query's offsets property and no stamped snapshot. The
  restart reads that in `trigger_loop` before it plans anything and ends
  `RecoveryRequired(StampedSnapshotExpired)`; a second restart answers the same. The reason's
  text names snapshot retention as the fix, which is the expiry case; here no stamped snapshot
  ever existed. The sketch gives both cases the one reason, so the pin holds it as written.
- **R-4 (2026-10-08). Observed, not changed: `QueryHandle::durable()` is `None` on the refused
  restart** while its exception and its `ShutdownOutcome` both carry the property's record.
  `conclude` puts the reported record on the outcome and the exception and leaves the
  lifecycle's own copy unset when the driver failed before `resumed` ran. The pin asserts the
  outcome and the exception and makes no claim on `durable()`. `driver.rs` is outside this
  slice; the question goes to the orchestrator in the hand-back.
