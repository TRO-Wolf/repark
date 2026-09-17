# Charter ledger — ICE-RDF-OPTIONS-1 · `rewrite_data_files` / `rewrite_position_delete_files` options map

**Date:** 2026-09-17 (round 1, RePark side) · **Branch:** `feat/ice-rdf-options-1` · **Base:** `b8e79fca` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `ICE-RDF-OPTIONS-1` appended beside `RDF-SORT-1` in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7.
**Fork half:** `F-RDF-OPTIONS-1` builds concurrently in `/tmp/ic-fork` (branch `fix/ice-rdf-options-1`);
this round touches only the RePark side at the current pin `edc38c6a`.

**Why now.** Rating report 2026-09-16 row V2-08 / claim C-4: every
`CALL <cat>.system.rewrite_data_files(table => …, options => map(…))` refuses with
`options map is not supported in v1`, and the same refusal sits on
`rewrite_position_delete_files`. The owner's weekly maintenance uses the options map.
Evidence: `/tmp/oc-worker/ice-rating/worker-findings.md` §V2-08 and §V2-03 (residue #37);
orchestrator-recorded oracle `/tmp/oc-worker/ic-build/rdf_options_spark.json` from
`/tmp/oc-worker/ic-build/rdf_options_probe.py` (34 cells, Spark 4.1.2 + Iceberg 1.11.0).

**Not in this unit:** the fork half (`partial-progress.*` commit batching, `rewrite-all`
planning flag, `output-spec-id` cross-spec rewrite, `rewrite-job-order` ordering,
`max-concurrent-file-group-rewrites` application); `sort`/`z-order` strategies (`RDF-SORT-1`);
`STATUS.md` (never edited by a unit).

## PROPOSITION LEDGER — ICE-RDF-OPTIONS-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The options map is accepted on `rewrite_data_files` with every key in the oracle (16 keys). | `call_rdf_options.rs` + `test_ice_rdf_options_1.py` offline pins. | **PROVEN** | All 16 keys parse; the 9 pin-supported knobs apply; fork-owned keys validate and wait on the struct for round 2 (round-1 no-op actuals pasted in Green re-run). |
| C-002 | Spark's exception class and message for unknown keys, bad integers, bad booleans (silently false), and each validation message. | Same pins; error cells assert class + message against the fixture. | **PROVEN** | All 17 rdf + 2 rpd error cells green with `IllegalArgumentException`; dup-key text green as base `PySparkException` (gap pinned, RuntimeError catch holds). Q1 mapping recorded in Rulings. |
| C-003 | File/snapshot counts equal to the oracle per cell; fork-dependent cells `xfail(strict, BLOCKED-ON-FORK F-RDF-OPTIONS-1)`. | Same pins; live tier re-runs the generator and asserts the fixture. | **PROVEN** | 7 cells fully green incl. `output_spec_id_current` and `where_plus_options`; 5 more green on snapshots; 14 value + 12 snapshot pins xfail with measured modes in Green re-run. Live tier pending `REPARK_PARITY_LIVE=1` at commit time. |
| C-004 | `where` + options composes (`where_plus_options` cell). | `where_plus_options` value + snapshot pins. | **PROVEN** | Full match: 4/1, 9 snapshots, 5 files, 400 rows. |
| C-005 | `rewrite_position_delete_files` accepts its own options map on a MoR table with ≥5 position-delete files. | Generator `rpd_*` cells (8 deletes measured) + offline pins. | **PROVEN** | 8-key subset measured and pinned; data-only keys refuse with the BIN-PACK text; `rewrite-all` wires to the existing builder. |
| C-006 | Residue #37 measured step by step on both engines. | Generator `res_*` cells + offline residue pins + registry row. | **PROVEN** | Residue unfixed by design (fork work): C-006 answer in Spark edge semantics — the default data rewrite drops post-RPD deletes and keeps pre-RPD ones. Registry row `RDF-DANGLING-1` (BACKLOG) filed. |
| C-007 | Registry row `ICE-RDF-OPTIONS-1` beside `RDF-SORT-1` (OPEN-in-progress) + dangling-delete row. | The registry diff. | **PROVEN** | Both rows filed; orchestrator flips FIXED after round 2. |

## Rulings

- R-Q1 (self, 2026-09-17): no `DataFusionError` variant classifies to `IllegalArgumentException`
  today (`classify_datafusion_error`: `Plan` → `Analysis`, `NotImplemented` → `Unsupported`,
  everything else → base). The CALL layer returns `DataFusionError`, so Spark's
  `IllegalArgumentException` cells need a new narrow path: a marker error type carried in
  `DataFusionError::External`, matched to `Error::IllegalArgument` in `repark-core/src/error_map.rs`.
  Additive only; no existing arm changes. **Recommendation for Q-19c:** keep bad-int on
  `IllegalArgumentException` (PySpark's `NumberFormatException` subclasses it, so every
  `except IllegalArgumentException` keeps working); add a native `NumberFormatException` leaf in
  round 2 only if a caller needs the leaf catch.
- R-FORK-STORE (self, 2026-09-17): keys needing fork code parse and validate fully now and sit on
  the parsed options struct; round 2 wires them. Stored-and-unwired is data-safe (defaults compact
  less, never wrong rows); the `xfail(strict)` pins are the loud marker until round 2.
- R-GRAMMAR-EDGES (self, 2026-09-17): Java `Double.parseDouble` corners stay on the Rust
  parse — a `d`/`f` suffix (`'2d'`) raises `NumberFormatException`-mapped `IllegalArgumentException`
  where Java accepts, and hex floats fail on both sides with the same class. Unmeasured
  (no oracle cell); recorded, not pinned.
- R-ORDER (self, 2026-09-17): lexical errors surface in map order; semantic checks run in
  fork order (spec → job → concurrent → commits → target → band → min-input → max-group →
  ratio); unknown keys refuse before any value parses. Multi-violation orders are
  unmeasured single-violation pins cannot distinguish; the choice is documented here.
- R-DANGLING-PRECEDENCE (self, 2026-09-17): the options-map `remove-dangling-deletes` key,
  when present, wins over the legacy top-level flag (the modern spelling is more specific);
  an absent key keeps the flag. Spark has no top-level flag, so no oracle cell can pin this.

## Red-first

Unfixed tree (`b8e79fca` + ledger/generator/test files only; native built pre-change).

Python (`test_ice_rdf_options_1.py`, 2026-09-17):
`30 failed, 5 passed, 1 skipped, 32 xfailed in 19.12s` — zero XPASS.
The 5 passes are the no-options cells (`baseline` value+snapshots, `rpd_baseline`
value+snapshots) and `test_residue_repark_sequence_pins_current_shape` (default
sequence already runs; it guards the residue direction, the `xfail` partner carries the gap).
Representative red (`err_unknown_key`):
`UnsupportedOperationException: This feature is not implemented: CALL rewrite_data_files
options map is not supported in v1 — use table properties / defaults (fork R135 binpack
defaults: min_input_files=5, …)`.
Every other options cell fails with the same refusal (value, snapshot, and error cells).

Rust (`cargo test -p repark-spark --lib`, 2026-09-17):
`call_rewrite_position_delete_files_validates_options_and_refuses_where` FAILED:
`refusal must name the deferred argument, got: This feature is not implemented: CALL
rewrite_position_delete_files options map is not supported in v1 — use table properties /
defaults (fork bin-pack planner groups by (spec, partition))`.
New-module unit tests (`call_rdf_options.rs`) test new code with no prior behaviour; their
bite is proven by the stash check in the implement section (compile-fail without the parser).

## Spark edge semantics (measured 2026-09-17, PySpark 4.1.2 + Iceberg 1.11.0, scratch probes)

RDF accepted set (16 keys): `target/min/max-file-size-bytes`, `min-input-files`,
`rewrite-all`, `max-file-group-size-bytes`, `delete-file-threshold`, `delete-ratio-threshold`,
`use-starting-sequence-number`, `remove-dangling-deletes`, `output-spec-id`, `rewrite-job-order`,
`partial-progress.enabled`, `partial-progress.max-commits`, `partial-progress.max-failed-commits`,
`max-concurrent-file-group-rewrites`. `max-open-partition-writers` is UNKNOWN (rejected).
RPD accepted set (8 keys): `target/min/max-file-size-bytes`, `min-input-files`, `rewrite-all`,
`max-file-group-size-bytes`, `rewrite-job-order`, `partial-progress.enabled`,
`partial-progress.max-commits`, `max-concurrent-file-group-rewrites`; the six delete-scoring,
spec, sequence, dangling, and max-failed keys are UNKNOWN on RPD with the same BIN-PACK message.
Longs use Java `Long.parseLong` (leading `+` accepted, whitespace rejected:
`'+1'` rewrites, `' 1'` is `NumberFormatException`). Booleans use `Boolean.parseBoolean`
(`'maybe'` silently false). Ratio uses `Double.parseDouble` (`'2'` renders `2.0` in messages;
`'inf'` gap noted below). `rewrite-job-order` names are case-insensitive
(`none`, `BYTES-DESC`, `files-desc` accepted). `partial-progress.max-commits` validates only
when enabled; `max-failed-commits` parses with no positivity check (`-1` accepted silently).
`min-input-files` and `max-concurrent-file-group-rewrites` reject `<= 0` echoing the value;
`target-file-size-bytes` rejects `<= 0`; `delete-file-threshold=0` is accepted and forces
candidacy. Unknown keys join in map order (`[aaa, bbb]`). Duplicate map keys never reach the
procedure: Spark's `map()` itself raises `[DUPLICATED_MAP_KEY] … SQLSTATE: 23505`
(`SparkRuntimeException`).
C-006 answer: on 16 data + 16 deletes, `rewrite_position_delete_files` (default) rewrites
16→16 deletes (one commit), then `rewrite_data_files` (default) rewrites 16→2 data and the
delete count falls 16→0 with `removed_delete_files_count=0` — the default data rewrite drops
them. `rewrite_data_files` alone on the same shape rewrites 16→2 but leaves all 16 deletes.
So Spark's post-RPD deletes die in the data rewrite while pre-RPD ones survive it (file-scoped
shape difference — fork work, registry row).

## Gates

- `cargo test -p repark-spark --lib`: `1071 passed; 0 failed; 4 ignored` (2026-09-17,
  includes the 29 new `call_rdf_options` pins and the updated rpd validation pin; the
  threshold/`>= 0` fix and the `number_format` quote change re-verified on the affected
  pins after).
- `cargo test -p repark-core --lib`: `572 passed` + 2 new `error_map` marker pins (2026-09-17).
- `cargo test -p repark-common --lib`: `13 passed` (2026-09-17).
- `cargo clippy --locked --workspace --all-targets -- -D warnings
  -A clippy::disallowed_methods`: clean on the touched crates (2026-09-17). One detour is
  recorded: the options struct by value tripped `large_futures` (16 KiB) at six
  `v3_subquery_dml` sites, so both rewrite CALL arms dispatch behind `Box::pin` — the same
  remedy `run_maintenance` already uses (see `src/call/map.md`).
- Release native (`maturin develop --release`) +
  `pytest python/repark/tests/test_ice_rdf_options_1.py`: `39 passed, 1 skipped, 28 xfailed`
  (2026-09-17; the skip is the live tier without `REPARK_PARITY_LIVE=1`; re-measured 42/28
  after the three added cells — see the green re-run note below).
- Neighbours on the release native: `test_rewrite_data_files_options.py` +
  `test_maintenance_call.py`: `20 passed` (2026-09-17).
- Live tier (`test_live_oracle_still_matches_spark`, 2026-09-17): PASSED in 77 s —
  banner `4.1.2`, the 45-cell generator re-run reproduces the fixture after byte-field
  normalisation (third independent Spark run confirming the oracle).
- `make verify`: GREEN exit 0 on the final tree (2026-09-17) — fmt, clippy `-D warnings`,
  panic ban, crate DAG, lib roots, Rust/Python file sizes, Python conventions, docstrings,
  manifest, ledgers, ledger grammar, map sync, docs compaction/links, owner ruling, full
  Rust workspace suite.

## Green re-run (2026-09-17, after the threshold/`>= 0` fix and the three added oracle cells)

- Rust `call_rdf_options`: 29 passed (adds `call_rdf_options_negative_delete_threshold`).
- Python new cells: `err_group_size_0`, `err_delete_threshold_neg`,
  `test_option_dup_key_matches_spark_map_text` — 3 passed on the release native.
- Full file re-run: `39 passed, 1 skipped, 28 xfailed` (2026-09-17, before the three
  added cells; the re-run after them is in Green re-run).
- Final full file run (2026-09-17, all edits in): `42 passed, 1 skipped, 28 xfailed`
  (the skip is the live tier without `REPARK_PARITY_LIVE=1`; zero XPASS).
- **Residue exact remainder (RePark, 2×8 half-deleted MoR shape, 2026-09-17):** DELETE leaves
  16 data + 16 deletes; `rewrite_position_delete_files` reports 16→2; the following
  `rewrite_data_files` reports 16 rewritten / 2 added / 0 removed; the table ends with
  4 files (2 data + 2 deletes) and 400 rows. Spark's sequence on the same shape ends with
  0 delete files. Pinned as `deletes == 2`.
- Round-1 green snapshot cells (unmarked from `xfail` after measurement): `target_small`,
  `delete_file_threshold`, `output_spec_id`, `output_spec_id_current`, `remove_dangling`
  (single-commit plans already match; only file/spec counts wait for the fork).
  Fully matching cells: `output_spec_id_current` and `where_plus_options` (result, files,
  snapshots, rows).
- Fork-cell failure modes (all `xfail(strict)`): the ten fork-flag RDF cells are round-1
  no-ops (zeros, 8 snapshots, 8 files — stored-not-wired as designed); `min_input_files_1`
  matches on result/files but commits twice (10 snapshots vs 9); `rpd_rewrite_all` and
  `rpd_min_input_files_1` combine 8→2 deletes (Spark rewrites 1:1) and commit per group
  (11 snapshots vs 10); `delete_file_threshold` / `remove_dangling` / `output_spec_id` /
  `target_small` match on result counts and snapshots but differ on file/spec/delete
  remainders (RePark drops the file-scoped delete where Spark keeps one; the single-group
  plan commits once like Spark).
- Live-tier note: delete-rewrite `rewritten/added_bytes_count` varies ±0.5 % between Spark
  runs (12543↔12578 on the same shape — parallel parquet write nondeterminism); counts,
  snapshots, and messages are stable. The live tier normalises those four byte fields and
  asserts them positive instead of byte-exact.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-rdf-options-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Walked C-001..C-007 against the diff and the runs. Every oracle error cell (17 rdf, 2 rpd, dup-key) asserts Spark's class and text end to end; every success cell asserts result, file, row, and snapshot state; fork-owned cells xfail strict with measured modes, and five snapshot XPASSes forced unmarking, so the suite bites in both directions.
      artifacts: [crates/repark-spark/src/tests/call_rdf_options.rs, python/repark/tests/test_ice_rdf_options_1.py, python/repark/tests/ice_rdf_options_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Empty map, NULL, integer literal, duplicate keys, empty/upper-case/unknown keys, zero/negative longs, leading-plus and spaced integers, silent-false booleans, case-varied job orders, non-current and unknown spec ids, non-map and odd-arity maps, and null map values all exercised against live Spark first, then pinned.
      artifacts: [python/repark/tests/_record_rdf_options_1_oracle.py, python/repark/tests/ice_rdf_options_1_spark_oracle.json, crates/repark-spark/src/tests/call_rdf_options.rs, python/repark/tests/test_ice_rdf_options_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: All validation refuses before any catalog write (options parse after table load, plan before commit); the bad-integer class maps to IllegalArgumentException (PySpark's NumberFormat superclass, Q-19c recorded); duplicate keys surface Spark's map-construction text as a base PySparkException with the RuntimeError catch intact; no retryable commit path is added, so no idempotency surface changes.
      artifacts: [crates/repark-core/src/error_map.rs, python/repark/tests/test_ice_rdf_options_1.py::test_option_dup_key_matches_spark_map_text, python/repark/tests/test_ice_rdf_options_1.py::test_number_format_maps_to_illegal_argument]
    - id: AT-4
      status: ATTACKED
      evidence: Ordering pinned where it matters: missing table still reports before a malformed legacy flag (existing pin kept), unknowns refuse before value parsing, semantic checks run in fork order; multi-violation check order is unmeasured and recorded as a gap. No shared state, no spawn, validation is synchronous — no race surface added.
      artifacts: [crates/repark-spark/src/tests/call_rewrite_options.rs::call_rewrite_missing_table_reports_the_table_before_a_bad_flag, crates/repark-spark/src/call/rewrite_options.rs]
    - id: AT-5
      status: N/A
      justification: No privilege, secret, or trust boundary is touched; option keys and values are SQL string literals that flow only into the validation result and the refusal text, the same sink every existing CALL argument already uses.
    - id: AT-6
      status: ATTACKED
      evidence: Every success pin asserts live row counts (400/399/370/200/100 per shape) plus file and snapshot state, so a dropped or duplicated row reds the suite; rewrite execution itself is unchanged fork code, and the residue direction (rows kept, deletes left) is pinned exact.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py, python/repark/tests/test_ice_rdf_options_1.py::test_residue_repark_sequence_pins_current_shape]
    - id: AT-7
      status: N/A
      justification: Validation is O(map entries) with no new loops, scans, or retained state; the compile-time 16 KiB future episode was a gate sizing matter fixed by arm-level Box::pin (recorded in Gates), not a runtime resource path.
    - id: AT-8
      status: ATTACKED
      evidence: No upstream behavior presumed: the rpd 8-key subset, the per-key value grammars, and every refusal text were measured against PySpark 4.1.2 + Iceberg 1.11.0 (edge probes) instead of copied from docs; only pre-existing fork builder methods are called; the fixture replays byte-identical across runs except the noted delete-byte variance.
      artifacts: [python/repark/tests/ice_rdf_options_1_spark_oracle.json, python/repark/tests/_record_rdf_options_1_oracle.py, crates/repark-spark/src/call/rewrite_options.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal path was read end to end in Python and carries Spark's verbatim diagnostic (class plus text), so a field failure diagnoses itself the way Spark's does; no new log or metric surface was needed for a validation-only change.
      artifacts: [python/repark/tests/test_ice_rdf_options_1.py::test_option_cell_errors]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held both ways: 30 Python pins failed red on the refusal and the 28 Rust pins failed 28/28 with the implementation stashed; five strict-xfail snapshot pins XPASSed on measurement and were unmarked, proving XPASS strictness bites. Every new branch has a nameable input (one pin per message, per key subset, per scalar shape); the threshold `>= 0` branch was added by a probe finding, not by reading code.
      artifacts: [crates/repark-spark/src/tests/call_rdf_options.rs, python/repark/tests/test_ice_rdf_options_1.py]
  reattested: [AT-2, AT-3, AT-10]
  complete: true
```

## Round 2 (RePark side, fork `2f5323ca` via the local path override)

**Date:** 2026-09-17 · **Branch:** `feat/ice-rdf-options-1` · **Base:** `97ec905f` ·
**Model:** muse-spark-1.3-contributor.
Fork half: TRO-Wolf/iceberg-rust#283 head `2f5323ca`, read at `/tmp/ic-fork-rdfsrc`
(read-only; round-2 ledger §9 there). `Cargo.toml` / `Cargo.lock` / `.ivy2` are
`skip-worktree` in this clone and are never staged.

### Round-2 proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-003 | Every fork-dependent cell is green, or stays `xfail(strict)` with a precise reason. | `test_ice_rdf_options_1.py` full run, zero XPASS. | **PROVEN** | 17 XPASS unmarked; 11 red kept with dated per-cell reasons (§Wiring and triage). |
| C-005 | RPD wires every Spark key the fork supports; the rest refuse loud. | New `UnsupportedOperationException` pins (Rust + Python). | **PROVEN** | 5 Python refusal params + 2 Rust refusal tests green; RPD wiring unchanged (the 6 fork keys). |
| C-006 | Residue re-measured on fork `2f5323ca`. | Round-2 measurement below. | **PROVEN** | Still 2 vs 0; `ICE-RDF-DANGLE-2` (OPEN) filed; current-count pin + zero xfail kept. |
| C-007 | Registry `ICE-RDF-OPTIONS-1` → FIXED 2026-09-17; `RDF-SORT-1` untouched. | Registry diff. | **PROVEN** | Row flipped; `RDF-SORT-1` and `RDF-DANGLING-1` untouched; `ICE-RDF-DANGLE-2` added. |

### Round-2 red-first (unwired tree `97ec905f` + fork `2f5323ca`, release native)

`pytest python/repark/tests/test_ice_rdf_options_1.py -q` (2026-09-17):
`1 failed, 42 passed, 1 skipped, 27 xfailed`. The one failure is
`test_option_cell_snapshots[min_input_files_1]` with `[XPASS(strict)]` — the fork's
one-commit default lands even through the unwired CALL, so the pin bites exactly as
designed. All ten fork-flag value cells still xfail (stored-not-wired zeros); the RPD
`rewrite_all` / `min_input_files_1` cells still xfail (fork RPD untouched by #283).
A scratch probe confirmed the binary carries the new fork: `min-input-files=1` on the
2×4 shape answers 8/2 with 9 snapshots (old fork: 10).

### Round-2 residue re-measurement (C-006, fork `2f5323ca`, wired, 2026-09-17)

2×8 half-deleted MoR shape (DELETE halves 800→400 rows; 16 data + 16 deletes, 17
snapshots). Default RPD: 16→2 deletes, 17→19 snapshots (fork RPD still commits per
group — #283 did not touch it), 400 rows. Default RDF: 16 rewritten / 2 added /
0 removed; ends with 4 files (2 data + 2 deletes), 20 snapshots, 400 rows. Spark's
recorded sequence on the same shape ends with 0 delete files in 19 snapshots.
Remainder: RePark keeps 2 dangling, Spark 0 — unchanged from round 1. Finding stays
fork-side: new registry row `ICE-RDF-DANGLE-2` (OPEN, fork ask); the current-count pin
(`test_residue_repark_sequence_pins_current_shape`, `deletes == 2`) stays green as the
documented divergence and the zero pin stays `xfail(strict)`.

### Round-2 wiring and triage (C-003, C-005, 2026-09-17)

Rust (`crates/repark-spark/src/call/rewrite_data_files.rs::run_rewrite`) now wires every
fork-owned RDF key into the fork builders: `rewrite_all`, `partial_progress`,
`partial_progress_max_commits`, `output_spec_id`, `rewrite_job_order` (local-to-fork
`From` map, same five variants), `max_concurrent_file_group_rewrites` (accepted, runs
sequentially in the fork). `partial-progress.max-failed-commits` stays accepted and
unwired: Spark parses it with no positivity check and no effect on a clean run, and the
fork has no such builder. Non-convertible numerics (only reachable as a Spark no-op:
`max-commits <= 0` with progress off) keep the fork default through `try_from`, never a
panic. RPD (`parse_rpd_options`) refuses the four Spark-accepted keys the fork's
`RewritePositionDeleteFiles` cannot honour — `rewrite-job-order`,
`partial-progress.enabled`, `partial-progress.max-commits`,
`max-concurrent-file-group-rewrites` — with `NotImplemented` (=
`UnsupportedOperationException`) naming the key and `ICE-RDF-OPTIONS-1, 2026-09-17`,
checked after lexical typing so garbage values still report `IllegalArgumentException`
first, key-based (even no-op spellings refuse, so no key is ever silently ignored).

Wired-tree run: `17 failed, 48 passed, 1 skipped, 11 xfailed` where all 17 failures are
`[XPASS(strict)]`. Unmarked (now green): values `rewrite_all`, `partial_progress`,
`partial_progress_max1`, `job_order_bytes_desc`, `job_order_files_asc`,
`use_start_seq_false`, `concurrent`, `output_spec_id`; snapshots `min_input_files_1`,
`rewrite_all`, `max_group_size`, `partial_progress`, `partial_progress_max1`,
`job_order_*`, `use_start_seq_false`, `concurrent`. Still red, kept `xfail(strict)`
with precise dated reasons: values `target_small` (fork one file per group, 8→2, Spark
splits to target, 8→4), `max_group_size` + `partial_progress_groups` (8→8 added, Spark
8→4), `delete_file_threshold` (counts match 4/1; bytes 5869 vs vanished-sum 7592),
`remove_dangling` (bytes 11878 vs 13522, `removed_delete` 1 vs 0),
`rpd_rewrite_all` + `rpd_min_input_files_1` (fork RPD untouched by #283: 8→2 in
per-group commits, Spark 8→8 in one); snapshots `partial_progress_groups` (11 vs 10:
8 groups under max-commits 3), both RPD cells (11 vs 10). New pins: five RPD refusal
params + `test_rdf_max_failed_commits_accepted_without_effect` (all green); Rust
`call_rpd_options_partial_progress_refuses_unsupported` +
`call_rpd_options_unwired_keys_refuse_unsupported` (green, 30/30 in module).

Correction to the round-1 Green re-run note: on the 2×8 shape the DELETE does not leave
"16 data + 16 deletes" — it leaves **32 data + 16 deletes** (verified 2026-09-17:
16 inserts → 16 files; `DELETE WHERE id % 2 = 0` hitting every file → 32 + 16).
RePark's MoR DELETE copy-rewrites each affected data file alongside the position
delete (`id < 30`, one file hit → 9 data files, the 9th 1644 B). That DELETE-written
file is what the rewrite folds in without counting (the two MoR byte gaps above).
DELETE-shape DML behavior is outside this unit; recorded for the orchestrator, not
fixed here.

### Round-2 gates (release native unless noted, 2026-09-17)

- `pytest python/repark/tests/test_ice_rdf_options_1.py`: `65 passed, 1 skipped, 11
  xfailed` (zero XPASS; the skip is the live tier without `REPARK_PARITY_LIVE=1`).
- Live tier (`test_live_oracle_still_matches_spark`, `REPARK_PARITY_LIVE=1`, one JVM):
  PASSED in 105 s — fixture still replays from the generator.
- Neighbours: `test_rewrite_data_files_options.py` + `test_maintenance_call.py` +
  `test_maintenance_policy_1.py` + `test_rdf_schema_evo_1.py` + `test_v3_dv_compaction.py` +
  `test_ice_spark_table_1.py`: `40 passed, 2 skipped`.
- `cargo test -p repark-spark --lib`: `1073 passed; 0 failed; 4 ignored` (30/30 in
  `call_rdf_options` incl. the 2 new RPD refusal tests).
- Clippy: the brief's literal `cargo clippy -p repark-spark --all-targets -- -D warnings`
  reds only on 3295+ pre-existing `unwrap`/`expect` hits in test targets — the repo gate
  (`-A clippy::disallowed_methods` for `--all-targets`, panic ban live on `--lib`) is
  clean on the touched crate, and the panic-ban `--lib` run adds zero new findings.
- `python3 scripts/sync_map_md.py --check`: 279 maps clean after the round-2 map edits.

## Hand-back

(TBD.)
