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
| C-010 | Writer-side unknown options are ruled: the mirror-reader lean (prefixed unknowns refuse MBE-17, plain keys pass, over the four named writer keys) or ruling 4's letter (every unnamed writer key refuses MBE-17). | The orchestrator's ruling on the hand-back's Q2. | **OPEN** | The stub implements the mirror-reader lean in `validate_writer_options` with the Q2 pin beside it; a strict ruling reworks one function and one pin. |
| C-011 | The format gaps are ruled: the missing-format default and the writer `NOT_IMPLEMENTED` feature shape. | The orchestrator's ruling on the hand-back's Q3. | **OPEN** | The stub implements the lean (the PySpark `parquet` default refused as `readStream.format(parquet)` / `writeStream.format(parquet)`); the alternative is the builder-flavoured feature. Pins sit beside the lean. |
| C-012 | The `processingTime` interval grammar is ruled: which strings parse, to what durations, and which error answers an unparsable one. | The orchestrator's ruling on the hand-back's Q4. | **OPEN** | The stub stores the validated strings unparsed; the parse belongs to the wire-up in Rust. |
| C-013 | A sourceless `load()` is ruled: `load()` with no path argument and no `path` option under the iceberg format. | The orchestrator's ruling on the hand-back's Q5. | **OPEN** | The stub implements the lean (a batch-mirrored plain `AnalysisException` naming the fix) with the pin beside it. |
| C-014 | The option cases with no §4 row are classified at wire-up: cap, timestamp and snapshot-id value parsing, non-boolean skip values, the four unsupported Spark keys, the two start keys together, and folded-key collisions. The stub passes all of them to the terminal; the wire-up refuses them with whatever classes the ruling gives. | The wire-up round's brief and pins. | **OPEN** | `from_options` refuses each of these as `Catalog`, which §4 does not class; ruling 5 keeps them out of the stub. Deliberately unpinned here so the wire-up writes the pins once. |
| C-015 | Step 4 flips `spark.readStream` / `spark.streams` (with the `session_core.py` stops-every-query fold and its lowered baseline), flips the SES-DECL rows and files the registry rows drafted in this ledger. | The wire-up round's brief and pins. | **OPEN** | Nothing in this round touches `session_surface.py`, `session_core.py` or the parity doc. |
| C-016 | The two exceptions gain their `errors.py` re-export and structured-method loop entries in the wire-up round. | The wire-up round's brief and pins. | **OPEN** | The facade imports them from `repark._native` until then; no stub test needs the structured methods. |
| C-017 | The PySpark members outside §3.7 land in the wire-up round or a dated card: the reader's `csv`, `json`, `orc`, `parquet`, `schema`, `text` and `xml`, the writer's `clusterBy`, `foreach` and `partitionBy`, the manager's `resetTerminated`, the query's `processAllAvailable`, and `load`'s `format`/`schema` kwargs. | The wire-up round's brief and pins. | **OPEN** | The stub implements exactly the §3.7 surface; anything else is an `AttributeError` or `TypeError` today. |
| C-018 | The driver-owned start checks land at wire-up: MBE-8 (local catalog), MBE-15 (isolation), MBE-13 at start (sink busy), the stateful-operator arm of MBE-6, and the run-time rows MBE-1, MBE-2, MBE-11, MBE-12 and MBE-16 — with the streaming frame, `isStreaming` and the `writeStream` answer. | The wire-up round's brief and pins. | **OPEN** | The stub cannot reach a session's catalogs without re-implementing resolution, and `surface_a.py` / `streaming_batch.py` stay vacuous-correct until frames exist. |

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

