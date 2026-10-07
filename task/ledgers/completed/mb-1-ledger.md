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
      evidence: Clauses C-001..C-016 walked one by one against the sketch (§3.1, §3.2, §3.3, §3.5, §4, Q4, Q6); every clause carries its proof obligation and pin citation in the verdict tables. Round 2 adds C-008..C-013 for the window and provider; round 3 adds C-014..C-016 for the source wrapper and options.
      artifacts: [task/ledgers/staging/mb-1-ledger.md, crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window.rs, crates/repark-iceberg/src/microbatch/provider.rs, task/ledgers/completed/mb-1-ledger.md, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Negative pins cover the empty vector, the repeated table, the absent stamp, the future and unparsable versions, the partial stamp, the corrupt property, and the unknown-table lookup; round 2 adds the unknown start id, the past-head stamp, the empty table, the dangling and replaced from, the mid-snapshot resume past a non-append, and the unknown end snapshot; round 3 adds the two skip keys, unknown prefixed keys, malformed values, two start keys, unknown catalogs and tables, malformed identifiers, and the empty table.
      artifacts: [crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, crates/repark-iceberg/src/microbatch/provider.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-3
      status: ATTACKED
      evidence: All 24 variants and 5 reasons render non-empty; the 8 Spark-quoted rows (MBE-1, MBE-2, MBE-3, MBE-4, MBE-8, MBE-10, MBE-12, MBE-15) are pinned byte-exact; round 2 pins the MBE-1/MBE-2 prefixes plus variant fields on the live refusal path; round 3 pins the MBE-3 and MBE-17 texts byte-exact on the live from_options path.
      artifacts: [crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Round 2 awaits load immutable manifests and plan scans only; no spawn, no lock, no shared mutable state, and the held table is never mutated across an await. Round 3 awaits only the planner, the catalog load and the batch collect, holds no lock across an await, and shares no mutable state.
      artifacts: [crates/repark-iceberg/src/microbatch/window.rs, crates/repark-iceberg/src/microbatch/provider.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Every stamp key and error text reviewed: identifiers, uuids, and numbers only. No location, DSN, token, or path field exists in the types, so none can reach a summary, a property, or a message. Round 2 names tables by catalog identifier only; file paths stay inside the planned tasks and never render. Round 3 names identifiers, option keys and snapshot ids only.
      artifacts: [crates/repark-iceberg/src/microbatch/offset.rs, crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window.rs, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Spark-verbatim rows pinned against the MB-0 oracle cells; the change is purely additive and the full pre-existing lib suite stays green beside the new pins (round 1: 804 + 26; round 2: 830 + 20; round 3: iceberg 850 unchanged from the unmodified baseline, core 1026 + 13). The batch append path keeps its silent skip; the refusal lives only in the new planner. Round 3 pins MBE-3/MBE-17.
      artifacts: [crates/repark-iceberg/src/microbatch/error.rs, crates/repark-iceberg/src/microbatch/window_tests.rs, python/repark-parity/tests/live_spark/mb0_streaming_oracle.json, crates/repark-core/src/time_travel/microbatch_source.rs]
    - id: AT-7
      status: N/A
      justification: No hot path; offsets are built once per batch, windows are planned once per batch, and no measurement is claimed.
    - id: AT-8
      status: ATTACKED
      evidence: 11/11 round gates green in rounds 1-2, 12/12 in round 3; new files under the default ceiling (round 2 split window.rs at 1013 lines into 364 + 649 with no ceiling raised; round 3 lands microbatch_source.rs at 563); every touched map updated in the same commits; the ledger moves to completed/ in the last commit.
      artifacts: [task/ledgers/staging/mb-1-ledger.md, crates/repark-iceberg/src/microbatch/map.md, crates/repark-iceberg/src/catalog/map.md, task/ledgers/completed/mb-1-ledger.md, crates/repark-core/src/time_travel/map.md]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; every failure is a typed value with a named fix.
    - id: AT-10
      status: ATTACKED
      evidence: Round 1: two mutants, both red on exactly the pins that own the behavior (duplicate-check removal, verbatim-prefix drift). Round 2: dropping with_fail_on_non_append fails exactly the 2 refusal pins of 46. Round 3: letting the skip keys pass fails exactly the MBE-3 pin of 13. All restores verified by diff.
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
