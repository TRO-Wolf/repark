> **Errata (2026-10-08, MB-3).** **D-5a, DECLARED: the processing-time half of D-5 is now
> measured.** MB0b-R18 runs R17's shape under `trigger(processingTime="0 seconds")` with
> `streaming-max-files-per-micro-batch=1`: Spark delivers `a` only (batch 0, offset `(a, 0)`),
> then fails `STREAM_FAILED` at the overwrite (and at the delete), so its capped `latestOffset`
> looks one snapshot ahead when the files cap fills on a snapshot's last file.
> `WindowLimit::Capped` keeps deliver-first under ruling Q1 (MB-1 fold 2) and delivers `a` then
> `b` before the refusal. Registry row `MB-3-LOOKAHEAD-1`; the MB-3 ledger carries the driver
> pin. D-5 below stands unedited.

# Unit ledger — MB-1 · the batch source over the incremental append scan

**Date:** 2026-10-07 · **Branch:** `feat/mb-1-source` · **Base:** `origin/main`
`575f57ca` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit (round 3).

**Scope.** Three rounds per the guided brief. Round 1 (this commit): the
sketch's §3.1 offsets and §3.2 errors —
`crates/repark-iceberg/src/microbatch/{mod.rs, offset.rs, error.rs, map.md}`,
the `pub mod microbatch;` line, the `Cargo.toml` lines, and the lockfile edges
they entail. Round 2: the window and provider (§3.3). Round 3: the core
source wrapper (§3.5). No other file is touched; `session.rs` is not touched.

## PROPOSITION LEDGER — MB-1 round 1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every §3.1 newtype constructs through `new` and reads through `get`; `TableUuid::of` reads table metadata, `RunId::fresh` yields v4, `Epoch::{FIRST, next, get}` and `OffsetFormatVersion::CURRENT` hold. | The newtype pins in `offset.rs`'s test module, including the memory-catalog `of` pin. | **PROVEN** | 4 pins green (`newtypes_new_get_round_trip`, `epoch_first_next_get`, `run_id_fresh_yields_unique_v4`, `table_uuid_of_matches_table_metadata`). pins: mb-1/C-001 |
| C-002 | `QueryId::derive` is deterministic over (sink uuid, name), sensitive to sink and name, distinguishes `None` from `Some("")`, and matches an independent RFC-4122 computation on two fixed vectors. | The derive pins in `offset.rs`'s test module. | **PROVEN** | 2 pins green (`query_id_derive_matches_independent_vector` against Python-`uuid`-computed values, `query_id_derive_is_deterministic_and_sensitive`). pins: mb-1/C-002 |
| C-003 | `OffsetVector::single`/`get`/`inputs` behave; `try_from_inputs` sorts by table uuid and refuses an empty vector or a repeated table, loud (FL-1). | The vector pins in `offset.rs`'s test module. | **PROVEN** | 3 pins green (`offset_vector_single_get_inputs`, `offset_vector_try_from_inputs_sorts_by_table_uuid`, `offset_vector_try_from_inputs_refuses_empty_and_duplicates`). pins: mb-1/C-003 |
| C-004 | `summary_entries` on both doors and `property` round-trip through `from_summary`/`from_property`; the spark keys ride the `Table` door only; an absent stamp reads `None`; a bad version or partial stamp refuses (FL-2). | The stamp pins in `offset.rs`'s test module. | **PROVEN** | 5 pins green (`summary_entries_round_trip_on_both_doors`, `property_round_trip_through_from_property`, `from_summary_returns_none_without_stamp`, `from_summary_refuses_bad_version_and_partial_stamp`, `from_property_refuses_corrupt_value`). pins: mb-1/C-004 |
| C-005 | `spark_source_offset_json` renders Spark's exact Iceberg JSON shape byte for byte. | The rendering pin in `offset.rs`'s test module. | **PROVEN** | 1 pin green (`spark_source_offset_json_matches_spark_shape`). pins: mb-1/C-005 |
| C-006 | `MicroBatchError` carries every §3.2 variant and `RecoveryReason` carries every reason; every display text is non-empty, and the §4-quoted rows render verbatim (MBE-1, MBE-2, MBE-3, MBE-4, MBE-8, MBE-10, MBE-12, MBE-15). | The display pins in `error.rs`'s test module. | **PROVEN** | 11 pins green (one per quoted row plus `every_error_variant_renders_a_message`, `every_recovery_reason_renders_a_message` and `recovery_reasons_render`). pins: mb-1/C-006 |
| C-007 | The module is wired (`mod.rs` headed by `#![forbid(unsafe_code)]`, the `lib.rs` line, the `Cargo.toml` lines per D-1), every touched map is current, and the round gate list is green. | The 11 round gates. | **PROVEN** | 11/11 green: 830 lib tests pass, clippy/panic-ban/fmt clean, file-size/lib-rs/crate-dag clean, map-sync/lockstep/ledger-grammar clean, comment-ban `hits=0`. pins: mb-1/C-007 |

VERDICT: 7 clauses, 7 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows

- **D-1 (2026-10-07).** `crates/repark-iceberg/Cargo.toml` carries two lines
  beyond the granted `thiserror.workspace = true`, both forced by the
  sketch's fixed content: the `v5` feature on `uuid` (§3.1 `QueryId::derive`
  calls `Uuid::new_v5`, and the unified features in the `repark-iceberg`
  closure are `v4`/`v7`/`serde`/`std`, so the call cannot compile without it),
  and `serde_json.workspace = true` in `[dependencies]` (the Q6 offset and
  property JSON carries arbitrary table-name strings, which need a real JSON
  library's escaping; a hand-rolled parser is the larger attack surface). No
  version moved; the `Cargo.lock` change is edges only (`thiserror` on
  `repark-iceberg`, `sha1_smol` on `uuid`).
- **FL-1 (2026-10-07).** Question: what does `OffsetVector::try_from_inputs`
  refuse — the sketch fixes the `Result` but no failure mode. Flink: corrupt
  checkpoint state fails the restore, never guesses. Spark: a corrupt offsets
  file fails the query. North Star default: refuse loud (NS-6) through the
  enum's generic `Catalog` carrier — an empty vector and a repeated table uuid
  both refuse with the cause named. Acted on; pins in C-003.
- **FL-2 (2026-10-07).** Question: what does `from_summary` do with a partial
  or corrupt stamp. Flink: corrupt state fails the restore. Spark: corrupt
  offsets fail the query. North Star default: the absent discriminator reads
  `None` (an unstamped snapshot is ordinary); a parsed-but-wrong version
  refuses `UnsupportedOffsetFormat`; a version that does not parse and any missing or
  unreadable companion key refuse `Catalog` naming the key. `from_property`
  refuses the same way with no absent case. Acted on; pins in C-004.
- **D-2 (2026-10-07).** Two saturations keep infallible signatures total.
  `QueryId::derive` saturates the `u32` length prefix at `u32::MAX` past 4
  GiB of name; the full name still feeds the v5 digest, so distinct names keep
  distinct ids. `Epoch::next` saturates at `u64::MAX` rather than wrapping to
  `FIRST`. Both reasons live in `microbatch/map.md`.

## Gates — round 1

All exit 0, in brief order: `cargo test -p repark-iceberg --lib` (830 passed,
0 failed — 804 pre-existing plus 26 new pins), `make rust-clippy` (clean
after inlining six const args and splitting one 112-line test),
`cargo fmt --check`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py` (1052 files clean),
`./scripts/check_lib_rs.sh` (11 roots clean), `./scripts/check_crate_dag.sh`
(23 edges clean), `python3 scripts/sync_map_md.py --check` (365 maps clean),
`bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_ledger_grammar.py` (304 live ledgers clean), and the
comment-ban probe (`hits=0`).

Two mutation probes, both red as required: dropping the duplicate-table check
in `try_from_inputs` fails
`offset_vector_try_from_inputs_refuses_empty_and_duplicates`; drifting the
`Cannot process` prefix fails exactly the two verbatim pins. Both mutants
were restored and the restore verified by diff.

```
COVERAGE_ATTESTATION:
  pr_unit: mb-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-016 walked one by one against the sketch (§3.1, §3.2, §3.3, §3.5, §4, Q4, Q6); every clause carries its proof obligation and pin citation in the verdict tables. Round 2 adds C-008..C-013 for the window and provider; round 3 adds C-014..C-016 for the source wrapper and options; fold 1 round A adds C-017..C-025, round B C-026..C-031. Fold 2 adds C-032..C-039 for the re-verify's G1–G8 rulings.
      artifacts: [task/ledgers/staging/mb-1-ledger.md, crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window.rs, crates/repark-iceberg/src/microbatch/provider.rs, task/ledgers/completed/mb-1-ledger.md, crates/repark-core/src/time_travel/microbatch_source.rs, crates/repark-iceberg/src/microbatch/window_fold2_pins.rs, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Negative pins cover the empty vector, the repeated table, the absent stamp, the future and unparsable versions, the partial stamp, the corrupt property, and the unknown-table lookup; round 2 adds the unknown start id, the past-head stamp, the empty table, the dangling and replaced from, the mid-snapshot resume past a non-append, and the unknown end snapshot; round 3 adds the two skip keys, unknown prefixed keys, malformed values, two start keys, unknown catalogs and tables, malformed identifiers, and the empty table; fold 1 adds the non-append start snapshot, the timestamp past the head, the out-of-range position, real expiry, non-canonical versions, and the missing record count; round B adds the table replaced under a live source, mixed-case skip and unknown keys, case twins with different values, non-boolean skip values, the four unsupported Iceberg streaming keys, and caps outside the int range. Fold 2 adds the Unbounded walk over an overwrite or delete, positions past a replace or overwrite start, the delete as head, a snapshot rolled out of the lineage, a head below T under skewed timestamps, Unicode-folded keys, and boolean twins that differ in meaning.
      artifacts: [crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, crates/repark-iceberg/src/microbatch/window_fold_pins.rs, crates/repark-iceberg/src/microbatch/provider.rs, crates/repark-core/src/time_travel/microbatch_source.rs, crates/repark-core/src/time_travel/microbatch_source_tests.rs, crates/repark-iceberg/src/microbatch/window_fold2_pins.rs, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: All 24 variants and 5 reasons render non-empty; the 8 Spark-quoted rows (MBE-1, MBE-2, MBE-3, MBE-4, MBE-8, MBE-10, MBE-12, MBE-15) are pinned byte-exact; round 2 pins the MBE-1/MBE-2 prefixes plus variant fields on the live refusal path; round 3 pins the MBE-3 and MBE-17 texts byte-exact on the live from_options path. Fold 2 adds `SnapshotNotInLineage` (25 variants), pinned byte-exact with Spark's `snapshotAfter` prefix and checked never to say "expired"; the Unicode-key refusal and the `SourceReplaced` text under the opened name are pinned byte-exact.
      artifacts: [crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, crates/repark-core/src/time_travel/microbatch_source.rs, crates/repark-iceberg/src/microbatch/window_fold2_pins.rs, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Round 2 awaits load immutable manifests and plan scans only; no spawn, no lock, no shared mutable state, and the held table is never mutated across an await. Round 3 awaits only the planner, the catalog load and the batch collect, holds no lock across an await, and shares no mutable state. Fold 1 round B reloads the table at the top of each call into a local; the source keeps only the catalog handle and identifier, so no table state crosses calls and two concurrent calls each plan over their own load. Fold 2's read schema is an immutable `SchemaRef` captured once at `open` and only cloned into each provider; the planner's name is an owned string set before any await.
      artifacts: [crates/repark-iceberg/src/microbatch/window.rs, crates/repark-iceberg/src/microbatch/provider.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Every stamp key and error text reviewed: identifiers, uuids, and numbers only. No location, DSN, token, or path field exists in the types, so none can reach a summary, a property, or a message. Round 2 names tables by catalog identifier only; file paths stay inside the planned tasks and never render. Round 3 names identifiers, option keys and snapshot ids only. Round B's case-twin refusal names both key spellings and never a value, so an unprefixed credential option cannot render.
      artifacts: [crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Spark-verbatim rows pinned against the MB-0 oracle cells; the change is purely additive and the full pre-existing lib suite stays green beside the new pins (round 1: 804 + 26; round 2: 830 + 20; round 3: iceberg 850 unchanged from the unmodified baseline, core 1026 + 13). The batch append path keeps its silent skip; the refusal lives only in the new planner. Round 3 pins MBE-3/MBE-17. Fold 2: MB0b-R17 recorded with the 27 earlier entries byte-identical; the full lib suites stay green beside the new pins (iceberg 873 = 804 + 69, core 1053 + 1 ignored = 1046 + 7).
      artifacts: [crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, python/repark-parity/tests/live_spark/mb0_streaming_oracle.json, crates/repark-core/src/time_travel/microbatch_source.rs, crates/repark-iceberg/src/microbatch/window_fold2_pins.rs, crates/repark-core/src/time_travel/microbatch_source_fold2_tests.rs]
    - id: AT-7
      status: N/A
      justification: No hot path; offsets are built once per batch, windows are planned once per batch, and no measurement is claimed.
    - id: AT-8
      status: ATTACKED
      evidence: 11/11 round gates green in rounds 1-2, 12/12 in round 3; new files under the default ceiling (round 2 split window.rs at 1013 lines into 364 + 649 with no ceiling raised; round 3 lands microbatch_source.rs at 563; fold 1 round B splits its tests under #[path], leaving 297 + 622); every touched map updated in the same commits; the ledger moves to completed/ in the last commit. Fold 2 lands its pins in two new #[path] children rather than growing the fold-1 files.
      artifacts: [task/ledgers/staging/mb-1-ledger.md, crates/repark-iceberg/src/microbatch/map.md, crates/repark-iceberg/src/catalog/map.md, task/ledgers/completed/mb-1-ledger.md, crates/repark-core/src/time_travel/map.md]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; every failure is a typed value with a named fix.
    - id: AT-10
      status: ATTACKED
      evidence: Round 1: two mutants, both red on exactly the pins that own the behavior (duplicate-check removal, verbatim-prefix drift). Round 2: dropping with_fail_on_non_append fails exactly the 2 refusal pins of 46. Round 3: letting the skip keys pass fails exactly the MBE-3 pin of 13. Fold 1: m1, m2, m5, m6 and m7 are red on exactly their owning pins (the tails are in the fold-1 gates); round B: m8 (no per-call reload) and m9 (no lowercase step) are red on exactly their two pins each. All restores were verified by diff, and fold 1's by cmp. Fold 2: g1, g2a, g2b, N8, g3b, g4, g5a, g5b, g6a, g6b, g7a, g7b and g8 are red on their owning pins (the tails are in the fold-2 gates), each restored from a backup and confirmed clean by `git status`.
      artifacts: [task/ledgers/staging/mb-1-ledger.md, task/ledgers/completed/mb-1-ledger.md]
```

## PROPOSITION LEDGER — MB-1 round 2 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-008 | `WindowPlanner::initial_offset` resolves `Earliest` to `(oldest ancestor, 0)` and refuses `TruncatedHistory` on a broken chain; `FromTimestamp` is inclusive and a past-head stamp consumes through the head (FL-4); `AfterSnapshot(x)` marks `x` consumed and refuses `SourceSnapshotExpired` for an unknown id; an empty table reads `None` on every start. | The start pins in `window_tests.rs`. | **PROVEN** | 7 pins green (`earliest_offset_points_at_oldest_ancestor`, `initial_offset_on_empty_table_reads_none_on_every_start`, `after_snapshot_marks_named_snapshot_consumed`, `after_snapshot_with_unknown_id_refuses_naming_oldest`, `from_timestamp_matches_second_commit_inclusively`, `from_timestamp_zero_starts_at_oldest`, `from_timestamp_past_head_consumes_through_head`). pins: mb-1/C-008 |
| C-009 | `next_window` streams `(from, head]` oldest-first with path-ordered files inside a snapshot, resumes mid-snapshot from `from.position`, splits by `max-files`/`max-rows` with at least one whole file, takes everything under `Unbounded`, and returns `None` when nothing is new; a replaced table refuses `SourceReplaced` and a dangling `from` refuses `SourceSnapshotExpired` (FL-3). | The window and cap pins in `window_tests.rs`. | **PROVEN** | 8 pins green (`window_streams_appends_oldest_first`, `window_returns_none_when_from_is_consumed_head`, `position_resumes_mid_snapshot`, `max_files_splits_snapshot_into_single_file_windows`, `max_rows_keeps_first_file_whole`, `unbounded_limit_ignores_caps`, `unknown_from_snapshot_refuses_naming_oldest`, `replaced_table_refuses_before_any_scan`). pins: mb-1/C-009 |
| C-010 | An overwrite or delete inside a window refuses `NonAppendSnapshot` with the snapshot id and operation; the fork's `PreconditionFailed` is the single decider (detail walk runs only on its error, FL-5 keeps the `from` bound exclusive); dropping `with_fail_on_non_append(true)` turns the refusal pins red. | The fail-loud pins in `window_tests.rs` plus the mutation probe. | **PROVEN** | 2 pins green (`overwrite_inside_window_refuses` with the MBE-1 prefix and the post-overwrite streaming half, `delete_inside_window_refuses` with the MBE-2 prefix); the mutant fails exactly these 2 of 46, restore verified by diff and a green re-run. pins: mb-1/C-010 |
| C-011 | A `replace` snapshot inside a window is skipped silently and later appends stream. | The replace pin in `window_tests.rs`. | **PROVEN** | 1 pin green (`replace_inside_window_skipped_silently`: fixture asserts `Operation::Replace`, plan skips it and streams the later append). pins: mb-1/C-011 |
| C-012 | `MicroBatchTableProvider` reads exactly the planned files through `ArrowReaderBuilder` and the crate's `conform_batch`, takes the arrow schema from the end snapshot, and refuses an unknown end snapshot instead of guessing. | The provider pins in `provider.rs`'s test module. | **PROVEN** | 2 pins green (`provider_reads_exactly_planned_files` over real parquet through SQL, `provider_refuses_unknown_end_snapshot`). pins: mb-1/C-012 |
| C-013 | The round is wired (`window`/`provider` mod lines, the one-word `scan_batches` visibility per D-3), every touched map is current, and the round gate list is green with no neighbour pin changed. | The 11 round gates. | **PROVEN** | 11/11 green: 850 lib tests pass, clippy/panic-ban/fmt clean, file-size/lib-rs/crate-dag clean, map-sync/lockstep/ledger-grammar clean, comment-ban `hits=0`; `window.rs` split at 1013 lines into 364 + 649 with no ceiling raised. pins: mb-1/C-013 |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows — round 2

- **D-3 (2026-10-07).** `crates/repark-iceberg/src/catalog/mod.rs` widens
  one word, `mod scan_batches` to `pub(crate) mod scan_batches`. The sketch's
  §3.3 requires the provider to read through "the crate's `conform_batch`",
  which is unreachable from `microbatch/` while the module is private to
  `catalog/`. No signature, behaviour, or public surface changes.
- **D-4 (2026-10-07).** Two unreachable-in-practice fallbacks stay loud
  without inventing variants. `next_window` against a table with no snapshots
  refuses `Catalog` naming the table and the dangling snapshot: no other
  variant can name an oldest that does not exist. A planned task without a
  manifest record count reads as 0 rows: the fork sets `Some` on this path,
  so the default never fires on a real scan.
- **FL-3 (2026-10-07).** Question: `next_window` whose `from.snapshot` sits
  in metadata but off the current ancestry. Flink: unrestorable state fails
  the restart. Spark: an unreachable start snapshot fails the query. North
  Star default: refuse loud (NS-6) with `SourceSnapshotExpired` naming the
  oldest available, the same resume standard as a fully expired id. Acted on;
  pins in C-009.
- **FL-4 (2026-10-07).** Question: `FromTimestamp` past the head. Flink: a
  start after the end waits at the end. Spark: the query starts and waits for
  new data. North Star default: return the head fully consumed, so later
  appends stream and nothing replays. Acted on; pins in C-008.
- **FL-5 (2026-10-07).** Question: the `from.snapshot` itself is not an
  append. Flink: the start bound is exclusive. Spark: the start offset is
  consumed. North Star default: the refusal range stays `(from, current]` and
  the `from` listing runs without fail-loud, so a non-append start
  contributes no files and never refuses. Acted on; pins in C-009.

## Gates — round 2

All exit 0, in brief order: `cargo test -p repark-iceberg --lib` (850 passed,
0 failed — 830 pre-existing plus 20 new pins), `make rust-clippy` (clean
after removing a `to_string` in format args, de-asyncing the file reader,
and borrowing its table), `cargo fmt --check`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py` (1055 files clean),
`./scripts/check_lib_rs.sh` (11 roots clean), `./scripts/check_crate_dag.sh`
(23 edges clean), `python3 scripts/sync_map_md.py --check` (365 maps clean),
`bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_ledger_grammar.py` (304 live ledgers clean), and the
comment-ban probe (`hits=0`).

One mutation probe, red as required: dropping `with_fail_on_non_append(true)`
fails exactly the two refusal pins (`overwrite_inside_window_refuses`,
`delete_inside_window_refuses`) with the other 44 microbatch pins green.
The mutant was restored and the restore verified by diff plus a green
re-run. `window.rs` crossed the ceiling at 1013 lines and was split into
`window.rs` (364) plus `window_tests.rs` (649) under `#[path]`; no ceiling
was raised and no exception added.

## PROPOSITION LEDGER — MB-1 round 3 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-014 | `SourceOptions::from_options` defaults to unbounded caps and `Earliest`; parses the two cap keys, `stream-from-timestamp` millis and `repark.cdc.start-after-snapshot-id`; refuses the two skip keys `SkipOptionRefused` (MBE-3) and any other `streaming-`/`stream-`/`repark.cdc.` key `UnknownOption` (MBE-17); passes unprefixed keys; and refuses malformed values and two start keys `Catalog` (FL-6, FL-7). | The option pins in `microbatch_source.rs`'s test module. | **PROVEN** | 9 pins green (`from_options_defaults_to_unbounded_earliest`, `from_options_parses_caps`, `from_options_parses_from_timestamp`, `from_options_parses_start_after_snapshot_id`, `from_options_refuses_both_skip_keys`, `from_options_refuses_unknown_keys_under_interpreted_prefixes`, `from_options_passes_unprefixed_keys`, `from_options_refuses_malformed_values`, `from_options_refuses_two_start_keys`). pins: mb-1/C-014 |
| C-015 | `MicroBatchSource::open` resolves a three-part table (FL-8) and refuses unknown catalogs and tables, malformed identifiers and non-three-part names; `initial_offset` and `next_batch` delegate to the planner; `next_batch` reads exactly the planned files through the new `provider_for_plan` seam (D-4), reports `num_input_rows` per plan, and reads `None` once consumed; an empty table reads `None`. | The source pins in `microbatch_source.rs`'s test module over a memory-catalog two-snapshot fixture. | **PROVEN** | 4 pins green (`open_streams_two_appends_then_reports_none`, `capped_batches_agree_with_their_plans_row_for_row`, `open_refuses_unknown_tables_and_malformed_identifiers`, `initial_offset_on_an_empty_table_reads_none`). pins: mb-1/C-015 |
| C-016 | The round is wired (the `time_travel.rs` mod line, the `provider_for_plan` seam with the `dead_code` allows lifted, every touched map current, the ledger moved to `completed/`), and the round gate list is green with no neighbour pin changed. | The 12 round gates. | **PROVEN** | 12/12 green: 850 iceberg lib tests pass (identical to the unmodified baseline), 1039 core lib tests pass plus 1 pre-existing ignored (1026 pre-existing plus 13 new), clippy/panic-ban/fmt clean, file-size/lib-rs/crate-dag clean, map-sync/lockstep/ledger-grammar clean, comment-ban `hits=0`. pins: mb-1/C-016 |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

- **D-4 (2026-10-07).** Question: `MicroBatchSource::next_batch`
  (core) must return a `DataFrame` over exactly `plan.files`, but the
  sketch pins `MicroBatchTableProvider` `pub(crate)` with no cross-crate
  door. Flink: one codebase, no boundary. Spark: one codebase, no
  boundary. North Star default: keep the reader in `repark-iceberg`
  (NS-10) over the existing allowed core-to-iceberg edge (NS-19) — a
  `pub fn provider_for_plan(table, plan) -> Result<Arc<dyn TableProvider>>`
  beside `try_new`, type-erased so the struct stays `pub(crate)` exactly
  as sketched; both `dead_code` allows lift with the first live caller.
  Acted on; pins in C-015.
- **FL-6 (2026-10-07).** Question: what does `from_options` do with a
  malformed value (a zero or unparsable cap, a non-integer timestamp or
  snapshot id). Flink: typed options fail at job build. Spark: a bad
  submission fails the query; the exact texts are not recorded. North
  Star default: refuse at start (NS §5 `deny_unknown_fields` spirit)
  through the enum's generic `Catalog` carrier, naming the key, the
  value and the expected shape; values parse strictly with no trimming.
  Acted on; pins in C-014.
- **FL-7 (2026-10-07).** Question: both start keys set at once. Flink: a
  single start bound. Spark: not recorded. North Star default: refuse
  loud naming both keys, never guess which start wins. Acted on; pins
  in C-014.
- **FL-8 (2026-10-07).** Question: which table identifiers does `open`
  accept. Flink: n/a. Spark: `table()` names resolve in the session
  catalog. North Star default: the three-part `catalog.namespace.table`
  shape `load_iceberg_table` already enforces, refused otherwise with
  the received identifier named. Acted on; pins in C-015.

## Gates — round 3

All exit 0, in brief order: `cargo test -p repark-iceberg --lib` (850 passed,
0 failed — identical to the unmodified-tree baseline, so no neighbour
changed), `cargo test -p repark-core --lib` (1039 passed, 0 failed, 1 ignored
— 1026 pre-existing plus 13 new; the ignored one is the pre-existing silver
measure printer), `make rust-clippy` (clean first run, including the lifted
`dead_code` allows), `cargo fmt --check`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py` (1056 files clean),
`./scripts/check_lib_rs.sh` (11 roots clean), `./scripts/check_crate_dag.sh`
(23 edges clean), `python3 scripts/sync_map_md.py --check` (365 maps clean),
`bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_ledger_grammar.py` (304 live ledgers clean — green once
the C-016 row the maps already cited was added), and the comment-ban probe
(`hits=0`).

One mutation probe, red as required: letting the two skip keys pass
`from_options` fails exactly `from_options_refuses_both_skip_keys` with the
other 12 source pins green. The mutant was restored and the restore verified
by diff plus a green re-run. `microbatch_source.rs` lands at 563 lines and
`provider.rs` at 349, both under the default ceiling; no ceiling was raised
and no exception added. `session.rs` untouched. The commit hook rejected the
first attempt on one ledger typo, fixed before landing.
After the ledger move to `completed/`, the map-sync and ledger-grammar gates
re-ran green on the post-move tree.

## Fold 1 — 2026-10-07 — verifier FAIL on PR #979, round A (`microbatch/` in `repark-iceberg`)

**Model:** claude-opus-5-5 (opus-worker build lane). **Evidence:** the verifier's
`verdict.json` (FAIL: 4 S1, 6 S2, 5 S3) and its probe modules `verify_probes` and
`verify_core_probes`, ported here as pins rather than copied. Round B owns the core
wrapper (the table refresh, case-insensitive keys, the option S3s), and nothing in
`microbatch_source.rs` changed here. MB-0 cells R3, R6 and R9 still differ by
ruling O-5.

### Step 0 — MB-0b measured before changing behaviour

Three cells were recorded on Spark 4.1.2 + Iceberg 1.11.0 through `MB0_CELLS`. The
24 MB-0 entries and the preamble stayed byte-identical: compared as parsed JSON, and
every recorded `statement` re-derived unchanged after the recorder split. The answers
are tabled in [mb-0-oracle.md](../../wo/microbatch/mb-0-oracle.md) §4.

- **MB0b-R14 (FL-4).** Nothing streams while no snapshot has a timestamp at or after T.
  This holds before the append below T, on a resume after it, and from a fresh
  checkpoint. Once a snapshot at or after T lands, only that snapshot streams.
  **Agrees with the verifier's bytecode reading.**
- **MB0b-R15 (max rows).** A file is added, then the batch stops once its rows reach the
  cap (`>=`). Over three 2-row files, max 3 gives `[4][2]`, max 4 gives `[4][2]` and
  max 5 gives `[6]`, in one snapshot and across three. Max 4 was measured beyond the
  brief to settle the equality boundary that m2 needs. **Agrees with the verifier.**
- **MB0b-R16 (first snapshot overwrite).** `STREAM_FAILED` with the cause `Cannot
  process overwrite snapshot: <first id>, to ignore overwrites, set
  streaming-skip-overwrite-snapshots=true`. No batch runs. **Agrees with the verifier.**

The measurement and the verifier's reading never disagree, so every ruling below
implements the measured answer as written.

## PROPOSITION LEDGER — MB-1 fold 1 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-017 | (F1) A window whose start snapshot is an `overwrite` or `delete` with `from.position` below its added-data-file count refuses `NonAppendSnapshot` (`from` = the start). That count is the ADDED data entries in the snapshot's own manifests. `AfterSnapshot(x)` positions past `x`'s files, so the restart advice resumes. `Earliest` and `FromTimestamp` landing on a non-append with no added data files refuse at `initial_offset` (FL-9). | The F1 pins in `window_fold_pins.rs` plus mutation m5. | **PROVEN** | 3 pins green: `earliest_on_first_snapshot_overwrite_refuses_then_after_snapshot_resumes` (p14), `from_timestamp_landing_on_overwrite_refuses_in_the_window` (p12, including the mid-snapshot position and `AfterSnapshot` = 2), and `from_timestamp_landing_on_delete_refuses_at_the_initial_offset` (the delete twin). m5 red. pins: mb-1/C-017 |
| C-018 | (F4) `FromTimestamp` reads `None` while no ancestor has a timestamp at or after T, including after an append below T. Once one lands, it starts at `(oldest such ancestor, 0)`, and the snapshot below T never streams. | The F4 pins. | **PROVEN** | 2 pins green: `from_timestamp_past_head_reads_none` (renamed from `…_consumes_through_head`) and `from_timestamp_past_head_waits_for_a_snapshot_at_or_after_it` (p15 plus the later landing; it asserts the below-T and at-or-after-T fixture timestamps). pins: mb-1/C-018 |
| C-019 | (F5) The window walks `(from, head]` in order and plans a snapshot only while it has room. `replace` is skipped. `overwrite` or `delete` ends a non-empty window before it and refuses only an empty window entering it. The up-front fail-loud scan and `with_fail_on_non_append` are gone. Draining N single-file snapshots at max-files 1 plans at most 2 listings per window. | The F5 pins plus mutation m6. | **PROVEN** | 4 pins green: `capped_window_delivers_appends_before_refusing_the_overwrite` (p17: a, b, c delivered, then the refusal `from` = S1), `capped_windows_plan_a_bounded_number_of_snapshots` (listings `[1,2,2,2,2,2,2,2,1]` over 8 snapshots through the `#[cfg(test)]` counter), and `overwrite_inside_window_refuses` / `delete_inside_window_refuses` (deliver-first, then the refusal). m6 red. pins: mb-1/C-019 |
| C-020 | (F6) A `from.position` above the start snapshot's added-file count refuses `OffsetPositionOutOfRange { table, snapshot, position, files }`. A position equal to the count resumes. | The F6 pins plus mutation m7. | **PROVEN** | 2 pins green: `position_past_the_added_files_refuses` (p16 `(S1,99)`, `(S1,3)`, head `(S2,2)`, then `(S1,2)` resumes and the drained head reads `None`) and `offset_position_refusal_names_position_count_and_snapshot` (the text, byte-exact). m7 red. pins: mb-1/C-020 |
| C-021 | (F7) `Earliest` over history truncated by a real `expire_snapshots` refuses `TruncatedHistory` naming the oldest snapshot and the expired parent. | The F7 pin plus mutation m1. | **PROVEN** | 1 pin green: `earliest_refuses_truncated_history_after_expiry`. m1 (`.filter(\|_\| false)`) is red, where it survived 46/46 before. pins: mb-1/C-021 |
| C-022 | (F8) The provider reads every planned file under the end snapshot's schema, matched by Iceberg field id, with null for a column the file lacks. | The F8 pin, red without the fix. | **PROVEN** | 1 pin green: `provider_reads_older_files_under_the_end_schema_by_field_id` (c03's shape over real parquet: two inserts, ADD COLUMN `note`, one insert, then an `Earliest` `Unbounded` read gives `[(1,null),(2,null),(3,null),(4,"x")]`). Without the re-stamp it fails with the verifier's `iceberg scan missing column 'note'`. pins: mb-1/C-022 |
| C-023 | (F9) Max rows follows MB0b-R15: add the file, then stop once rows `>=` the cap. Batch sizes and end offsets are exact for caps 3, 4 and 5, across snapshots and inside one; a zero-row tail at cap 2 is `[g0][g1]`. | The F9 pins plus mutation m2. | **PROVEN** | 2 pins green: `max_rows_adds_the_crossing_file_then_stops_at_the_cap` and `max_rows_rule_holds_inside_one_snapshot_and_on_a_zero_row_tail` (exact `(rows, end snapshot, end position)` vectors). m2 (`>=` to `>`) is red on both, where it survived before. pins: mb-1/C-023 |
| C-024 | (F10) Both stamp readers accept the canonical version `1` only. Any other text, including `+1`, `01`, `-1`, non-numeric and out-of-range, refuses `UnsupportedOffsetFormat` carrying the text found, never a saturated number. The writers return `Result` with no `unwrap_or_default`. A task without a record count refuses `Catalog`. | The F10 pins. | **PROVEN** | 4 pins green: `from_summary_accepts_only_the_canonical_version_text` (o02), `from_property_reports_the_version_it_found` (o03, including `4294967296`, `18446744073709551615`, `1.0`, `"1"` and `null`), `from_summary_refuses_bad_version_and_partial_stamp`, and `a_task_without_a_record_count_refuses`. pins: mb-1/C-024 |
| C-025 | MB0b-R14…R16 are recorded on live Spark 4.1.2 + Iceberg 1.11.0, merged into the recording with the 24 MB-0 entries and the preamble byte-identical, and the SHA-256 is rewritten. | `sha256sum -c`, the parsed comparison against the pre-run file, and the statement re-derivation. | **PROVEN** | `mb0_streaming_oracle.json: OK`. 24/24 old entries are equal and the preamble is equal. Statement drift is `[]` across 27 cells after the `mb0_bench.py` split. ruff check and format are clean. pins: mb-1/C-025 |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows — fold 1

- **FL-9 (2026-10-07).** Question: a delete snapshot adds no data files, so under F1's
  "position below the added count" rule, `(D, 0)` from `Earliest` or `FromTimestamp`
  reads as "consumed" and passes silently. That is the verifier's p12 delete case.
  Flink: no answer; a start bound is a position, not an operation. Spark:
  `shouldProcess` refuses an `overwrite` or `delete` start on every batch, at any
  position (MB0b-R16, measured for overwrite). North Star default: refuse loud (NS-6).
  `Earliest` and `FromTimestamp` landing on an `overwrite` or `delete` with zero added
  data files refuse `NonAppendSnapshot` (`from: None`) at `initial_offset`.
  `AfterSnapshot(D)` gives `(D, 0)`, which the window reads as consumed. Acted on;
  pins in C-017. This is surfaced to the orchestrator as a ruling question.
- **D-5 (2026-10-07, rewritten in fold 2, G1).** The answer depends on the trigger.
  Under `Trigger.AvailableNow` and `Trigger.Once`, Spark's
  `prepareForTriggerAvailableNow` fixes the end offset with an uncapped
  `latestOffset` walk, and `nextValidSnapshot` throws at the first `overwrite` or
  `delete` before batch 0, whatever the caps. MB0b-R17 measures it: two one-file
  appends, then an overwrite (or a delete), then `availableNow` with
  `streaming-max-files-per-micro-batch=1` runs no batch and writes no offset. RePark's
  `WindowLimit::Unbounded`, the walk that fixes an AvailableNow/Once target, now
  refuses the same way (C-032). Under processing-time triggers with caps, Spark plans
  each batch up to the cap and delivers the appends ahead of the snapshot.
  `WindowLimit::Capped` keeps deliver-first, so the restart advice stays lossless. The
  fold-1 text, "Under caps the two agree (p17)", cited a RePark probe rather than a
  Spark cell and is withdrawn.
- **D-6 (2026-10-07).** An append's added-file count comes from the same
  incremental-append listing the window delivers, so positions and counts cannot
  disagree. It is the same set as the snapshot's ADDED manifest entries. Every other
  operation counts its own manifests' ADDED data entries (F1). Both listings tick the
  `#[cfg(test)]` counter.
- **Superseded (2026-10-07).** FL-4 by C-018. FL-5 by C-017 and C-019. C-010's
  mechanism (the fork's `PreconditionFailed` as decider) by C-019; its two refusal pins
  stand with deliver-first expectations. D-4's "record count reads as 0" by C-024.
  C-008's past-head pin is renamed.

## Gates — fold 1

Mutation probes, each restored from a backup with the restore checked by `cmp`
(`git diff` cannot show it because the rewrite was uncommitted). The output tails:

```
m1 window.rs `oldest.parent_snapshot_id()` -> `.filter(|_| false)`
test microbatch::window::tests::window_fold_pins::earliest_refuses_truncated_history_after_expiry ... FAILED
test result: FAILED. 60 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
m2 window.rs `self.rows >= max.get()` -> `self.rows > max.get()`
test microbatch::window::tests::window_fold_pins::max_rows_rule_holds_inside_one_snapshot_and_on_a_zero_row_tail ... FAILED
test microbatch::window::tests::window_fold_pins::max_rows_adds_the_crossing_file_then_stops_at_the_cap ... FAILED
test result: FAILED. 59 passed; 2 failed; 0 ignored; 0 measured; 804 filtered out
m5 window.rs `if from.position.get() < count {` -> `if false && …`
test …::earliest_on_first_snapshot_overwrite_refuses_then_after_snapshot_resumes ... FAILED
test …::from_timestamp_landing_on_overwrite_refuses_in_the_window ... FAILED
test result: FAILED. 59 passed; 2 failed
m6 window.rs `if window.files.is_empty() {` -> `if true {` (eager refusal)
test microbatch::window::tests::delete_inside_window_refuses ... FAILED
test microbatch::window::tests::overwrite_inside_window_refuses ... FAILED
test result: FAILED. 59 passed; 2 failed
m7 window.rs `if from.position.get() > files {` -> `> files.saturating_add(1000)`
test …::position_past_the_added_files_refuses ... FAILED
test result: FAILED. 60 passed; 1 failed
```

## Fold 1 — 2026-10-07 — round B (the core wrapper in `repark-core`)

**Model:** claude-opus-5-5 (opus-worker build lane). **Evidence:** the verifier's
`verify_core_probes` (c01, c02), ported here as pins. Round B owns
`crates/repark-core/src/time_travel/microbatch_source.rs`; its tests move under
`#[path]` to `microbatch_source_tests.rs` so the source stays well under the ceiling.
Nothing in `crates/repark-iceberg/src/microbatch/` changed: `WindowPlanner::new`
already takes a `Table` by value.

## PROPOSITION LEDGER — MB-1 fold 1 round B — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-026 | (F2) `MicroBatchSource` holds the catalog handle and the `TableIdent`, never a frozen `Table`. `initial_offset` and `next_batch` reload the table on every call and build the `WindowPlanner` from the fresh table, as Spark's `latestOffset` calls `table.refresh()` every trigger. One source sees a snapshot committed after it opened, and refuses `SourceReplaced` when the table is dropped and re-created under the same name between two `next_batch` calls. | The F2 pins in `microbatch_source_tests.rs` plus the reload mutation. | **PROVEN** | 2 pins green: `the_same_source_sees_appends_committed_after_open` (c02: open, drain 5 rows, INSERT `(6),(7)`, the same source returns exactly `[6, 7]`, then `None`) and `the_same_source_refuses_a_table_replaced_under_its_name` (drop, re-create, insert, then `SourceReplaced` with the recorded and the new uuid). The reload mutation is red on both. pins: mb-1/C-026 |
| C-027 | (F3) Every streaming option key matches ASCII case-insensitively, as Spark's `CaseInsensitiveStringMap` does: the Iceberg Spark keys, the skip keys and the `repark.cdc.` prefix. Each key is lowercased once at parse; values keep their case; an unknown prefixed key refuses under the spelling the user passed. Two spellings of one key with different values refuse `Catalog` naming both spellings and no value; with equal values they are accepted. | The F3 pins in `microbatch_source_tests.rs` plus the lowercase mutation. | **PROVEN** | 2 pins green: `from_options_matches_keys_ascii_case_insensitively` (c01: the mixed-case caps, `Stream-From-Timestamp` and `Repark.CDC.start-after-snapshot-id` apply; `STREAMING-SKIP-OVERWRITE-SNAPSHOTS=true` and `Streaming-Skip-Delete-Snapshots=true` refuse MBE-3; `Streaming-Bogus` refuses MBE-17 under its own spelling) and `from_options_refuses_case_twins_with_different_values` (the text byte-exact, then equal twins parse). The lowercase mutation is red on both. pins: mb-1/C-027 |
| C-028 | (F11) `streaming-skip-overwrite-snapshots` and `streaming-skip-delete-snapshots` set to `false` in any case are accepted as a no-op, since false is Spark's default. Only `true` in any case refuses `SkipOptionRefused` (O-5, MBE-3). Any other value refuses `Catalog` naming the key (D-7). | The F11 pins in `microbatch_source_tests.rs`. | **PROVEN** | 2 pins green: `from_options_refuses_both_skip_keys` (now `true`, `TRUE`, `True` on both keys, byte-exact MBE-3) and `from_options_accepts_skip_keys_set_to_false_as_a_no_op` (`false`, `FALSE`, `False` on both keys and an upper-case key leave the caps and start untouched; `""`, `yes`, `0`, `" false"` refuse byte-exact). pins: mb-1/C-028 |
| C-029 | (F12) The Iceberg 1.11 `SparkReadOptions` streaming keys RePark does not implement refuse `Catalog` as recognised but unsupported, in any case, never with "fix the spelling": `streaming-snapshot-polling-interval-ms` (the trigger interval governs polling), `async-micro-batch-planning-enabled`, `async-queue-preload-file-limit` and `async-queue-preload-row-limit` (RePark plans synchronously). The list is read from the jar's `SparkReadOptions` constants (D-8). | The F12 pin in `microbatch_source_tests.rs`. | **PROVEN** | 1 pin green: `from_options_refuses_recognised_spark_streaming_keys_it_does_not_support` (all four keys, two in mixed case, each text byte-exact, none containing "fix the spelling"). pins: mb-1/C-029 |
| C-030 | (F13) `streaming-max-files-per-micro-batch` and `streaming-max-rows-per-micro-batch` parse in Spark's `intConf` range: a value above `i32::MAX` (or at or below zero) refuses `Catalog` naming the key and the bound; `2147483647` and `+2147483647` are accepted, as Java's `Integer.parseInt` accepts them. | The F13 pin in `microbatch_source_tests.rs`. | **PROVEN** | 1 pin green: `from_options_refuses_caps_above_the_spark_int_range` (`3000000000`, `2147483648` and `-2147483648` on both keys refuse byte-exact; the maximum parses on both). pins: mb-1/C-030 |
| C-031 | (FL-9, ruled KEEP) `docs/spark-sql-iceberg-parity.md` carries the dated DECLARED row `MB-1-FL-9` beside the streaming rows: RePark refuses `NonAppendSnapshot` at `initial_offset` where Spark idles on a zero-added-files `overwrite`/`delete` start until data arrives, then fails the same way. The row states its oracle basis (MB0b-R16 recorded for the overwrite-with-files start; the idle half from the 1.11 bytecode, unmeasured) and cites round A's pin. | The registry row, the docs-link gate and the registry-reading Python suites. | **PROVEN** | The row cites `window_fold_pins.rs::from_timestamp_landing_on_delete_refuses_at_the_initial_offset` (green in round A's C-017); `check_docs_links.py` clean; `test_parity_live.py`, `test_dropin_disclosure.py` and `test_lrs3_registered_divergences.py` 63 passed, 65 skipped. pins: mb-1/C-031 |

VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows — fold 1 round B

- **D-7 (2026-10-07).** A skip key's value is `true` or `false` in any ASCII case.
  Spark reads it through `SparkConfParser$BooleanConfParser`, which calls
  `Boolean.parseBoolean`, so `yes`, `0` or `" false"` read as false there and change
  nothing. RePark refuses them `Catalog` instead, as FL-6 refuses every other
  malformed value. Either answer leaves the stream unskipped, so the only cost is a
  loud start on a value Spark would ignore.
- **D-8 (2026-10-07).** The unsupported list comes from `javap -constants` on
  `org.apache.iceberg.spark.SparkReadOptions` in
  `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`. Its streaming constants are the five
  RePark parses (the two caps, the two skip keys and `stream-from-timestamp`) and
  four it does not: `streaming-snapshot-polling-interval-ms`,
  `async-micro-batch-planning-enabled`, `async-queue-preload-file-limit` and
  `async-queue-preload-row-limit`. The three `async-` keys carry no `streaming-` or
  `stream-` prefix, so before this fold they passed silently. Each refuses whatever
  its value, as the ruling's recognised-but-unsupported message reads; Spark's
  default for `async-micro-batch-planning-enabled` is false, so `false` there is
  refused even though it changes nothing.
- **FL-9 ruled KEEP (orchestrator, 2026-10-07).** Round A's question is answered:
  `Earliest` and `FromTimestamp` landing on an `overwrite` or `delete` with zero added
  data files keep refusing `NonAppendSnapshot` at `initial_offset`. The one Spark
  difference, refusing at start where Spark idles until data arrives, is the dated
  DECLARED row `MB-1-FL-9` in the parity registry (C-031).
- **Superseded (2026-10-07).** C-014's "refuses the two skip keys" by C-028 (only
  `true` refuses), its exact-case key match by C-027, and its "any other prefixed key
  refuses `UnknownOption`" by C-029 for the four recognised Iceberg keys. C-015's
  single load at `open` by C-026.

## Gates — fold 1 round B

Mutation probes, each restored from a backup with the restore checked by `cmp`:

```
m8 microbatch_source.rs: `open` keeps the first loaded `Table` and `load` returns it (no per-call reload)
test time_travel::microbatch_source::tests::the_same_source_refuses_a_table_replaced_under_its_name ... FAILED
test time_travel::microbatch_source::tests::the_same_source_sees_appends_committed_after_open ... FAILED
test result: FAILED. 13 passed; 2 failed; 0 ignored; 0 measured; 1027 filtered out
m9 microbatch_source.rs: `folded.entry(key.to_ascii_lowercase())` -> `folded.entry(key.clone())` (no lowercase step)
test time_travel::microbatch_source::tests::from_options_refuses_case_twins_with_different_values ... FAILED
test time_travel::microbatch_source::tests::from_options_matches_keys_ascii_case_insensitively ... FAILED
test result: FAILED. 15 passed; 2 failed; 0 ignored; 0 measured; 1027 filtered out
```

All exit 0, in brief order, on the round-B tree: `cargo test -p repark-core --lib
time_travel::microbatch` (20 passed: 13 before plus 7 new), `cargo test -p
repark-iceberg --lib microbatch` (61 passed, unchanged from round A), `make
rust-clippy`, `cargo fmt --check`, `make rust-panic-ban`,
`python3 scripts/check_rust_file_size.py` (1058 files clean; `microbatch_source.rs`
297 lines, `microbatch_source_tests.rs` 622), `./scripts/check_lib_rs.sh`,
`python3 scripts/sync_map_md.py --check`, `bash scripts/check_map_md.sh --base
origin/main`, `python3 scripts/check_docs_links.py`,
`python3 scripts/check_ledger_grammar.py`, and the comment-ban probe (`hits=0`). The
full `cargo test -p repark-core --lib` is 1046 passed, 1 ignored (1039 before plus
the 7 new pins); no neighbour pin changed.

## Fold 2 — 2026-10-07 — the re-verify's S2s and S3s

**Model:** claude-opus-5-5 (opus-worker build lane). **Evidence:** the re-verifier's
`verdict.json` (PASS: 13 CLOSED, 2 CHANGED, 3 S2, 5 S3), its probe modules
`rv_probes` and `rv_core_probes`, and its mutation N8, ported here as pins. The
rulings G1–G8 are the orchestrator's. Every mutation below was applied from a backup,
run, and restored.

### Step 0 — MB0b-R17 measured before changing behaviour

`MB0_CELLS=MB0b-R17` on Spark 4.1.2 + Iceberg 1.11.0. The 27 earlier entries and the
preamble stayed byte-identical (parsed comparison), and the SHA-256 is rewritten. Two
one-file appends, then an `INSERT OVERWRITE` (table `r17`) or a whole-file `DELETE`
(table `r17_delete`), then `availableNow` with `streaming-max-files-per-micro-batch=1`:
`batches: []`, `offsets: []`, and `STREAM_FAILED` / `IllegalStateException` with
`Cannot process overwrite snapshot: <id>, to ignore overwrites, set
streaming-skip-overwrite-snapshots=true` (and the delete twin), thrown from
`prepareForTriggerAvailableNow` while the query is `INITIALIZING`. **Agrees with the
re-verifier**, so G1 is implemented as ruled.

## PROPOSITION LEDGER — MB-1 fold 2 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-032 | (G1) `WindowLimit::Unbounded` refuses `NonAppendSnapshot` at the first `overwrite` or `delete` in `(from, head]` even when the window already holds files, as MB0b-R17 measures for `availableNow`. `WindowLimit::Capped` keeps deliver-first over the same history. | The G1 pins in `window_fold2_pins.rs` plus mutation g1. | **PROVEN** | 2 pins green: `unbounded_walk_refuses_the_first_non_append_before_any_file` (R17's shape, overwrite and delete, uncapped and max-files 1: the refusal names the non-append, `from` = the first append, `to` = the head) and `capped_walk_delivers_both_appends_then_refuses_the_non_append` (ends `(a,1)`, `(b,1)`, then the refusal `from` = b). g1 red. pins: mb-1/C-032 |
| C-033 | (G2) The read schema is the table's current schema when `MicroBatchSource::open` resolves the table, held for the source's lifetime and passed to `provider_for_plan` on every batch. Under capped windows a rename reads every batch under the new name, a drop and re-add reads `note=null` for the dropped field's file, a promotion reads `Int64`/`Float64` in every batch, and a schema change after open leaves the batch schema as it was. | The G2 pins in `microbatch_source_fold2_tests.rs` plus mutations g2a and g2b. | **PROVEN** | 4 pins green, each at `streaming-max-files-per-micro-batch=1`, the rendered schema and rows byte-exact per batch: `a_rename_before_start_reads_every_batch_under_the_new_name` (k03), `a_drop_and_re_add_never_shows_the_dropped_field` (k04: row 1 `note` empty, never `old`), `a_type_promotion_before_start_reads_every_batch_promoted` (k05) and `a_schema_change_after_open_leaves_the_batch_schema_alone`. g2a and g2b red. pins: mb-1/C-033 |
| C-034 | (G3) The F6 range contract holds on a non-append start snapshot: a `from.position` above the added data files of a `replace` or an `overwrite` start refuses `OffsetPositionOutOfRange { snapshot, position, files }` under both limits, and a position equal to the count resumes. | The G3 pins in `window_fold2_pins.rs` plus the re-verifier's N8 and g3b. | **PROVEN** | 2 pins green: `a_replace_start_past_its_added_files_refuses` (`(r,2)` and `(r,99)` refuse with `files = 1`; `(r,1)` resumes to the head) and `an_overwrite_start_past_its_added_files_refuses` (`(o,3)` and `(o,99)` refuse with `files = 2`; `(o,2)` resumes). N8 (the replace arm becomes `Ok(())`), which survived 61/61 in the re-verify, is red; g3b (the overwrite arm drops `check_position`) is red. pins: mb-1/C-034 |
| C-035 | (G4) The FL-9 divergence is pinned on the shape where Spark idles: a zero-added-files `delete` as the head with nothing after it. `FromTimestamp` landing on it refuses `NonAppendSnapshot` (`from: None`, `to` = the delete) at `initial_offset`, and still refuses (`to` = the new head) once an append follows. `AfterSnapshot(D)` gives `(D, 0)`, reads `None` while nothing follows, then resumes to the append. The registry row `MB-1-FL-9` cites this pin, and its Spark half reads "idles while no snapshot follows it". | The G4 pin in `window_fold2_pins.rs`, the registry row, the docs-link gate, plus mutation g4. | **PROVEN** | 1 pin green: `a_delete_as_head_refuses_at_the_initial_offset` (q10's shape). g4 (the landing refusal drops its zero-added-files test, `&& false`) is red on it and on fold 1's `from_timestamp_landing_on_delete_refuses_at_the_initial_offset`. pins: mb-1/C-035 |
| C-036 | (G5) A snapshot still in the metadata but not an ancestor of the head refuses `SnapshotNotInLineage { table, snapshot, head }`: `AfterSnapshot(x)` at `initial_offset`, and a resumed `from` at `next_window` under both limits and at any position. The text opens with Spark's `Cannot find snapshot after <id>: not an ancestor of table's current snapshot` and never says "expired". An ancestor still resumes. | The G5 pins in `window_fold2_pins.rs` and `error.rs`, plus mutations g5a and g5b. | **PROVEN** | 2 pins green: `a_snapshot_rolled_out_of_the_lineage_refuses_as_not_an_ancestor` (q09's shape: append s1, append s2, `rollback_to(s1)`, append s3; `AfterSnapshot(s2)`, `(s2,0)` and `(s2,1)` refuse naming s2 and s3; `(s1,1)` resumes to s3) and `lineage_refusal_starts_with_spark_text_and_never_says_expired` (byte-exact). The variant joins `every_error_variant_renders_a_message`. g5a and g5b red. pins: mb-1/C-036 |
| C-037 | (G6) `FromTimestamp` mirrors `oldestAncestorAfter`: a head below T reads `None`; otherwise the walk from the head returns the snapshot after the first one with `ts < T`, a snapshot with `ts == T` itself, or the oldest ancestor when none is below T. | The G6 pin in `window_fold2_pins.rs` plus mutations g6a and g6b. | **PROVEN** | 1 pin green: `from_timestamp_walks_back_from_the_head_as_oldest_ancestor_after` (a real s1 at offset 0, then skewed snapshots at +50 s, +20 s and +60 s built through `TableMetadataBuilder`: T = +30 s lands on the head where the oldest-first rule gave +50 s; +60 s the head; +20 s the equal snapshot; +10 s the +50 s snapshot; 0 and -1 s s1; +60.001 s `None`; with the +20 s snapshot as head, T = +30 s reads `None` where the old rule landed on +50 s). g6a (the old oldest-first search) and g6b (no head check) red. pins: mb-1/C-037 |
| C-038 | (G7) After the ASCII fold, a key containing any non-ASCII character whose Unicode lowercase starts with `streaming-`, `stream-` or `repark.cdc.` refuses `Catalog` naming the key, so the Kelvin-sign `repar\u{212A}.cdc.start-after-snapshot-id` can no longer pass as an ignored key and silently replay from `Earliest`. A non-ASCII key outside the prefixes still passes. Two spellings of a skip key compare as booleans: `false`/`FALSE` are accepted, `TRUE`/`true` refuse MBE-3, and `false`/`TRUE` or `false`/`0` refuse as different values; every other key keeps the exact comparison. | The G7 pins in `microbatch_source_fold2_tests.rs` plus mutations g7a and g7b. | **PROVEN** | 2 pins green: `a_key_that_folds_to_a_streaming_prefix_only_under_unicode_refuses` (four keys byte-exact, two non-prefixed keys pass) and `boolean_twins_compare_by_meaning`. g7a and g7b red. pins: mb-1/C-038 |
| C-039 | (G8) Planner errors name the table as the source was opened: `MicroBatchSource` builds both planners with `WindowPlanner::named(self.name)`, so `SourceReplaced` reads `source table ice.sales.orders was replaced (…)`. The offsets keep the table identifier (`sales.orders`) in `table_name`. | The G8 pin in `microbatch_source_fold2_tests.rs` plus mutation g8. | **PROVEN** | 1 pin green: `planner_errors_name_the_table_as_the_source_was_opened` (k07's shape: drain, drop, re-create; the text byte-exact with both uuids; the drained offset's `table_name` is `sales.orders`). g8 red. pins: mb-1/C-039 |

VERDICT: 8 clauses, 8 PROVEN, 0 OPEN, 0 REJECTED.

## Dated decision rows — fold 2

- **G2 (2026-10-07).** Supersedes F8's end-snapshot schema (C-022's "under the end
  snapshot's schema"). The read schema is the table's current schema when the source
  first resolves its start. `open` always runs first and already loads the table, so
  it captures the schema there, before `initial_offset` and for a resumed run alike.
  `SparkScan.toMicroBatchStream` hands the scan's `expectedSchema` to the stream in the
  same way. A schema change committed after the source opened does not change the batch
  schema: a column added later stays unread, a renamed column keeps its name at open,
  and only a new source (a new run) reads the new schema. The provider pin is renamed
  `provider_reads_older_files_under_the_read_schema_by_field_id` and passes the
  table's current schema.

- **G4 (2026-10-07).** C-031's registry pin moves from
  `from_timestamp_landing_on_delete_refuses_at_the_initial_offset`, whose fixture has an
  append after the delete (there Spark's walk passes the delete and `shouldProcess` fails
  batch 0, so both engines refuse at once), to the delete-as-head pin of C-035. The Spark
  half now reads "idles while no snapshot follows it"; its idle half stays unmeasured, as
  the row says.
- **G5 (2026-10-07).** A new variant rather than a reused one: `SourceSnapshotExpired`
  tells the user to raise `expire_snapshots` retention, which cannot help a snapshot
  that `rollback_to` left behind, and `Catalog` would lose the typed fields. The
  sketch's §4 table gains no row in this fold; the text is RePark-owned beyond Spark's
  `SnapshotUtil.snapshotAfter` prefix, read from the 1.11 jar with `javap -constants`.
  FL-3 now covers only a `from` that is gone from the metadata.
- **G6 (2026-10-07).** Read from the 1.11 jar: `determineStartingOffset` returns
  `START_OFFSET` when `currentSnapshot().timestampMillis() < T`, else
  `oldestAncestorAfter(table, T)`, and falls back to `oldestAncestor` when that throws
  for a history whose oldest ancestor has an expired parent. RePark's walk ends on the
  oldest ancestor in both cases. Supersedes C-018's "(oldest such ancestor, 0)"; the two
  agree whenever ancestor timestamps grow along the chain.
- **G7 (2026-10-07).** The fold stays ASCII (F3's ruling). Java's
  `toLowerCase(Locale.ROOT)` folds U+212A to `k`, so Spark would read the Kelvin key as
  the option; RePark refuses it loud instead of guessing either way. Rust's
  `to_lowercase` is the probe. Supersedes C-027's "with equal values they are accepted"
  for the skip keys, which now compare by meaning.
- **G8 (2026-10-07).** Only the refusal texts change. `InputOffset.table_name` stays
  the table identifier, because offsets are stamped into the sink and read back by
  later runs; a display-only change must not move stored bytes. A planner built
  without `named` keeps the identifier.
- **Superseded (2026-10-07).** C-019's "refuses only an empty window entering it" by
  C-032 for `WindowLimit::Unbounded`. C-022's end-snapshot schema by C-033. C-018's
  "(oldest such ancestor, 0)" by C-037. C-027's equal-value rule for the skip keys by
  C-038. C-031's registry pin by C-035. C-008/C-009's `SourceSnapshotExpired` for an
  off-ancestry snapshot still in the metadata by C-036.
- **Observed, not acted on (2026-10-07).** Spark's capped `latestOffset`
  (`SyncSparkMicroBatchPlanner`, 1.11 bytecode) checks the files cap only before adding
  the next file inside a snapshot. When a window fills `max-files` exactly on a
  snapshot's last file, the walk still calls `nextValidSnapshot`, which throws if the
  next snapshot is an `overwrite` or `delete`. So under a processing-time trigger at
  `max-files`, Spark refuses one batch earlier than RePark's `Capped` window in that case
  (for R17's shape at max-files 1 it delivers `a` only, where RePark delivers `a` then
  `b`). The rows cap stops without that look-ahead, as RePark's does. The G1 ruling keeps
  deliver-first for `Capped`; this is unmeasured and is handed back for a ruling.

## Gates — fold 2

```
g1 window.rs `|| limit == WindowLimit::Unbounded` removed (Unbounded delivers first again)
test microbatch::window::tests::window_fold2_pins::unbounded_walk_refuses_the_first_non_append_before_any_file ... FAILED
test result: FAILED. 62 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g2a provider.rs `let schema = Arc::clone(read_schema);` -> the end snapshot's schema when it resolves (fold 1's per-window schema)
test …::microbatch_source_fold2_tests::a_type_promotion_before_start_reads_every_batch_promoted ... FAILED
test …::microbatch_source_fold2_tests::a_rename_before_start_reads_every_batch_under_the_new_name ... FAILED
test …::microbatch_source_fold2_tests::a_schema_change_after_open_leaves_the_batch_schema_alone ... FAILED
test …::microbatch_source_fold2_tests::a_drop_and_re_add_never_shows_the_dropped_field ... FAILED
test result: FAILED. 20 passed; 4 failed; 0 ignored; 0 measured; 1027 filtered out
g2b microbatch_source.rs `&self.read_schema` -> the reloaded table's current schema on every call
test …::microbatch_source_fold2_tests::a_schema_change_after_open_leaves_the_batch_schema_alone ... FAILED
test result: FAILED. 23 passed; 1 failed; 0 ignored; 0 measured; 1027 filtered out
N8 window.rs enter_start's `Operation::Replace` arm -> `Ok(())` (no range check)
test microbatch::window::tests::window_fold2_pins::a_replace_start_past_its_added_files_refuses ... FAILED
test result: FAILED. 65 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g3b window.rs the overwrite/delete arm drops `self.check_position(from, count)?;`
test microbatch::window::tests::window_fold2_pins::an_overwrite_start_past_its_added_files_refuses ... FAILED
test result: FAILED. 65 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g4 window.rs landing `) && self.added_file_count(snapshot).await? == 0` -> `) && false`
test microbatch::window::tests::window_fold2_pins::a_delete_as_head_refuses_at_the_initial_offset ... FAILED
test microbatch::window::tests::window_fold_pins::from_timestamp_landing_on_delete_refuses_at_the_initial_offset ... FAILED
test result: FAILED. 64 passed; 2 failed; 0 ignored; 0 measured; 804 filtered out
g5a window.rs initial_offset's `return Err(self.not_in_lineage(snapshot, head_id));` removed
test microbatch::window::tests::window_fold2_pins::a_snapshot_rolled_out_of_the_lineage_refuses_as_not_an_ancestor ... FAILED
test result: FAILED. 67 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g5b window.rs next_window's `if self.table.metadata().snapshot_by_id(from_id).is_some() {` -> `if false {`
test microbatch::window::tests::window_fold2_pins::a_snapshot_rolled_out_of_the_lineage_refuses_as_not_an_ancestor ... FAILED
test result: FAILED. 67 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g6a window.rs the head-first walk -> `chain.iter().rev().skip_while(ts < T).take(1)` (the oldest ancestor at or after T)
test microbatch::window::tests::window_fold2_pins::from_timestamp_walks_back_from_the_head_as_oldest_ancestor_after ... FAILED
test result: FAILED. 68 passed; 1 failed; 0 ignored; 0 measured; 804 filtered out
g6b window.rs `if head.timestamp_ms() < millis { return Ok(None); }` removed
test microbatch::window::tests::from_timestamp_past_head_reads_none ... FAILED
test microbatch::window::tests::window_fold2_pins::from_timestamp_walks_back_from_the_head_as_oldest_ancestor_after ... FAILED
test microbatch::window::tests::window_fold_pins::from_timestamp_past_head_waits_for_a_snapshot_at_or_after_it ... FAILED
test result: FAILED. 66 passed; 3 failed; 0 ignored; 0 measured; 804 filtered out
g7a microbatch_source.rs `if !key.is_ascii() && has_interpreted_prefix(…)` -> `if false && …`
test time_travel::microbatch_source::tests::microbatch_source_fold2_tests::a_key_that_folds_to_a_streaming_prefix_only_under_unicode_refuses ... FAILED
test result: FAILED. 25 passed; 1 failed; 0 ignored; 0 measured; 1027 filtered out
g7b microbatch_source.rs `|| (boolean && …)` -> `|| (false && …)` (exact twin comparison)
test time_travel::microbatch_source::tests::microbatch_source_fold2_tests::boolean_twins_compare_by_meaning ... FAILED
test result: FAILED. 25 passed; 1 failed; 0 ignored; 0 measured; 1027 filtered out
g8 microbatch_source.rs next_batch's planner drops `.named(self.name.clone())`
test time_travel::microbatch_source::tests::microbatch_source_fold2_tests::planner_errors_name_the_table_as_the_source_was_opened ... FAILED
test result: FAILED. 26 passed; 1 failed; 0 ignored; 0 measured; 1027 filtered out
```

All exit 0, in brief order, on the fold-2 tree: `cargo test -p repark-iceberg --lib
microbatch` (69 passed: 61 before plus 8 new), `cargo test -p repark-core --lib
time_travel::microbatch` (27 passed: 20 before plus 7 new), `make rust-clippy`,
`cargo fmt --check`, `make rust-panic-ban`, `python3 scripts/check_rust_file_size.py`
(1060 files clean), `./scripts/check_lib_rs.sh`, `python3 scripts/sync_map_md.py
--check`, `bash scripts/check_map_md.sh --base origin/main`,
`python3 scripts/check_docs_links.py`, `python3 scripts/check_ledger_grammar.py`, ruff
check and format on `mb0_streaming_oracle.py` and `mb0_bench.py`, and the comment-ban
probe (`hits=0`). The full lib suites are iceberg 873 passed and core 1053 passed, 1
ignored; no neighbour pin changed. The registry-reading suites
(`test_parity_live.py`, `test_dropin_disclosure.py`,
`test_lrs3_registered_divergences.py`) are 63 passed, 65 skipped after the G4 row edit.
