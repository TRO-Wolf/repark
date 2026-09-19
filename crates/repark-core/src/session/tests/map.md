# map — repark-core/src/session/tests

## Purpose

Session test modules. `session.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `session.rs` — ported v1 session battery plus P2G R2 / A13 / metadata-enumeration pins. RP-5: the bare-session half of the metadata-table enumeration contract (fork F-8 listing); mutation — make `information_schema` expect a `$snapshots` twin and the pin reds. pins: rp-5-fork-repin/C-003
  Child: [session/catalog_registration.rs](session/map.md).
  RP-5: `information_schema` hide pin now cites fork F-8 listing (no engine shim).
  pins: rp-5-fork-repin/C-003
- `aws_gate.rs` — E-2 offline AWS-gate pins.
- `conf_unread.rs` — **CONF-UNREAD-1 step 1 (2026-09-11):** the four
  accepted-but-unread keys. `coalesce_batches` refuses loud at build and at
  runtime `SET`, naming the key and the reason (DataFusion 54.1.0 defines the
  option but no engine path reads it); `enable_page_index` and
  `bloom_filter_on_read` set to `false` reach both `SessionConfig` and the
  session table options the scan source reads (control leg: the default session
  reads `true`); `write_batch_size` set to `1000` reaches both structures
  (control leg: the default reads `1024`).
  pins: conf-unread-1/C-001, C-002, C-003, C-004
  Step 2 (2026-09-11): the refusal pins hold the message's load-bearing tokens
  (key named, `cannot take effect`) that `docs/guide/session-and-conf.md`
  quotes verbatim in its `datafusion.*` paragraph.
  pins: conf-unread-1/C-007
- `df_guard.rs` — seven DataFusion 54.1 guard pins.
- `io_stats.rs` — **ICE-READ-PERF-0 (2026-09-19):** a session-level read through a registered
  memory catalog counts data-file ranged reads into `iceberg_io_stats()`, and
  `reset_iceberg_io_stats()` zeroes the set. pins: ice-read-perf-0/C-003
- `namespace_create.rs` — `create_namespace` location-guard pins (G-6 Q1 / R-6).
- `nlj_tight_pool.rs` — **NEVER-OOM-PANIC-1 (2026-09-16):** the tight-pool nested-loop-join
  loop pin. The plan shape is guarded (`NestedLoopJoinExec` in `EXPLAIN`), every iteration
  runs under an 8 MiB pool with 4 partitions and proves its own tightness (the pool recorded
  a refusal).
  **Round 2 (2026-09-16):** two pins of three iterations — the INNER join asserts spilled
  values (`count(*)` 2016, `sum(id)` 41664, or the typed refusal), the LEFT join asserts
  the typed `Resources exhausted … fair(` refusal (the fallback DataFusion documents as
  unsafe must not emit rows), never a panic payload. The shape guard and the refusal-shape
  assertion are helpers shared by both pins.
  pins: never-oom-panic-1/C-003, C-006, C-012
- `a13.rs` — `file://` warehouse fallback-root pin.
- `pool_refusals.rs` — **H3-SPILL-RESIDUE-1 (2026-09-06):** the wiring pins. A bounded
  `build()` installs a pool that still reports `MemoryLimit::Finite` and now carries a refusal
  log that starts at zero and counts the session's own refusal; `memory_limit_bytes(0)` installs
  no log, so the containment cannot fire on an unbounded session. Two more hold the SET path:
  a runtime resize keeps the very same log (`Arc::ptr_eq`) and the new pool records into it,
  and a runtime `= '0'` drops the log with the pool.
  pins: h3-spill-residue-1/C-002
- `cache_budget.rs` — **EAGER-BUDGET-1 step 1 (2026-09-13):** D-2 pins. Two cache views
  registered over the same `RecordBatch` count one buffer set; a sliced array counts its
  parent's buffer once (dedupe is `Buffer::data_ptr()`, the allocation base — `as_ptr()`
  double-counts because arrow-58 slices the `Buffer` itself); no view answers 0; a dropped
  view stops counting; `user_view` and `__repark_ckpt_*` names are ignored. One function-level
  pin holds the reuse contract: a second `distinct_buffer_bytes` call over the same batches
  with the same pointer set returns 0.
  pins: eager-budget-1/C-002, C-003
  **EAGER-BUDGET-1 step 2 (2026-09-13):** D-1/D-3/D-4 admission pins. A three-batch source
  under a one-batch budget refuses at batch two with the tag, budget, `retained`, and
  `admitted` in the message — `admitted` lands strictly below the unbudgeted result, so the
  stream was dropped mid-collection and nothing registered. A scan over a live cache view
  admits zero new bytes (shared buffers seed `seen`) and leaves `retained` unchanged, while a
  fresh-buffered frame under `budget = retained` refuses naming that retained figure.
  `max_bytes` keeps its legacy message and registers nothing; an admitted cache registers and
  its `retained` equals the admitted distinct bytes.
  **Review round (2026-09-13, R12b-D-4):** `max_bytes_measures_this_result_even_when_buffers_are_shared`
  pins the per-result metric — a scan of a live cache view under `(max_bytes=1,
  max_total_bytes=u64::MAX)` refuses with this result's `get_array_memory_size` integer even
  though every buffer is shared.
  pins: eager-budget-1/C-005, C-007, C-008
- `commit_unknown.rs` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `engine_err` classification
  pins for the ambiguous-commit path — the stamped `CommitStateUnknownError` wrapper maps to
  `Error::CommitStateUnknown` carrying the minted `operation_id`, a bare iceberg
  `CommitStateUnknown` kind maps to the same variant with `None`, and the definite kinds
  (`CatalogCommitConflicts` included) stay in the `Error::Iceberg` base bucket. Mutation:
  fold the stamped arm and the kind arm back to `Error::Iceberg` and both classification
  pins red; the repark-common routing pin and the repark-python `to_py_err` pin red under
  the matching `exception_class` → `Base` leg of the same mutant.
  pins: ice-commit-unknown-1/C-001, C-004, C-007
- `window_rescan.rs` — **WIN-SLIDE-1 (2026-09-04):** six capability pins for the
  `sliding_frame_rescan` rule in [../df_guards/window_rescan.rs](../df_guards/window_rescan.rs). The throwaway
  `winslide_probe_sum` UDAF exists only here: it has no `retract_batch`, so it proves the fallback
  fires on an aggregate the rule has never heard of, and its `default_value` is the sentinel
  `-1.0`, so the empty-frame pin distinguishes "fresh accumulator" from "aggregate default".
  Mutation: make the rule probe `accumulator` instead of `create_sliding_accumulator` and
  `a_retractable_aggregate_keeps_datafusions_sliding_accumulator` reds.
  pins: win-slide-1/C-005, C-006
- `subquery.rs` — **DF-SUBQUERY-1 (2026-09-15):** plan-level pins for the three
  [../df_guards/subquery.rs](../df_guards/subquery.rs) rules — the scalar guard wraps a
  non-singleton subplan in `__repark_single_row` and leaves a zero-group `count(*)`
  aggregate untouched (count-bug compensation stays native), the lateral hoist lifts a
  `Subquery`-wrapped right projection onto the left input and refuses a correlated
  `Unnest`, and the EXISTS-in-projection rewrite keeps the plan's field name.
  **Round-3 (2026-09-16):** the hoist keeps a `SubqueryAlias` qualifier on its
  lifted outputs (`t.dbl` resolves in the rewritten schema, a still-correlated
  right keeps its `Subquery` marker under the alias), and the scalar guard
  strips a correlated `LIMIT 1` into `__repark_any_row` while an uncorrelated
  `LIMIT 1` stays untouched.
  pins: df-subquery-1/C-001, C-002, C-004

## Pointers

- Up: [../map.md](../map.md)
