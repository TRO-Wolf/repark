# Unit ledger — ICE-BRANCH-OPS-1 · four Iceberg branch procedures — round 1

**Date:** 2026-09-17 · **Branch:** `fix/ice-branch-ops-1` · **Base:** `main` at `79e328f2`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Branch/tag CREATE/REPLACE/DROP, branch writes and `rollback_to_snapshot`
work and match Spark, but four Spark procedures refuse on RePark: `fast_forward`,
`cherrypick_snapshot`, `set_current_snapshot`, `rollback_to_timestamp` (gap measured
2026-09-16, rating row V2-18, probe p_refs #15/#16). The fork already carries the
primitives (`ManageSnapshots` fast-forward / set-current / rollback-to-time,
`Transaction::cherry_pick`, GAP_MATRIX R98), so this unit is RePark-side procedure
wiring in Rust. No fork change; WAP confs and `publish_changes` stay refused.

**Not in this step:** `STATUS.md`, `.github/`, `Cargo.toml`, `Cargo.lock`, fork
sources, `[patch]` overrides (a path override never reaches a commit).

## PROPOSITION LEDGER — ICE-BRANCH-OPS-1 round 1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark oracle measured live (PySpark 4.1.2 + Iceberg 1.11.0, banner quoted): `fast_forward` positional + named arg shapes, output columns/types/row, and every error shape (unknown `to`, tag as `branch`, non-descendant `to`, unknown `branch`). | Recorder + truth cells `ff_*`. | PROVEN | Banner + FF cells below. Unknown `branch` auto-creates (no refusal). |
| C-002 | Same for `cherrypick_snapshot`: happy-path replay (new snapshot parent/operation/rows), output schema, errors (unknown id, already-ancestor, delete/static-overwrite snapshot refused). | Truth cells `cp_*`. | PROVEN | Cells below. Duplicate pick refuses `already picked to create ancestor`. |
| C-003 | Same for `set_current_snapshot`: by id (non-ancestor jump allowed), by ref, output schema, errors (both/neither of snapshot_id/ref, unknown id, unknown ref). | Truth cells `sc_*`. | PROVEN | Cells below. Both and neither share one message. |
| C-004 | Same for `rollback_to_timestamp`: happy path (latest ancestor strictly older than ts), output schema, error (ts before first snapshot), session-TZ rule measured on a non-UTC session. | Truth cells `rt_*`. | PROVEN | Cells below. Measured rule is UTC-always (see TZ note). |
| C-005 | v3 row lineage after cherry-pick: `next-row-id` and `_row_id` equal Spark's on the same shape. | Truth cells `ops3_*`. | PROVEN | `(id,row_id)` = (1,0),(2,4),(3,3); `next-row-id` 5; replayed by the suite. |
| C-006 | Red-first: the new pins fail on the unfixed tree (`fast_forward` et al refuse `not supported`); failing output pasted in Evidence. | Evidence paste + gate log. | PROVEN | Red output below (both tiers). |
| C-007 | All four procedures implemented in Rust (`call.rs` dispatch + new family file), positional + named args, Spark output columns/types/rows, Spark error class + prefix per C-001–C-004 cell. | `test_ice_branch_ops_1.py` offline green; Rust tests green. | PROVEN | Offline 2 passed; Rust 8 passed. No DECLARED row needed. |
| C-008 | Spark reads the table after each RePark procedure; RePark reads it after each Spark procedure (live tier cross-reads). | Live-tier gate log. | PROVEN | `test_live_branch_ops_cross_reads` green (fresh-name adoption both ways). |
| C-009 | `publish_changes` and `spark.wap.*` confs still refuse; existing refusal pins updated to pin only what is still refused. | Updated pins green. | PROVEN | Refusal loops narrowed to `publish_changes`; suites green. |
| C-010 | Registry: REF-3 rewritten (fast_forward + cherrypick_snapshot FIXED with pins, WAP still BACKLOG); new rows for set_current_snapshot + rollback_to_timestamp; procedure lists in refusal texts updated. | Registry diff. | PROVEN | REF-3 narrowed; REF-5–REF-8 added; `SUPPORTED_PROCEDURES` lists fourteen. |
| C-011 | Gates on the release native: new file offline + live, refs/WAP/time-travel facade files, `cargo test -p repark-spark --lib`, `make verify`, whole facade suite, whole parity suite; counts in Evidence. | Gate log. | PROVEN | Counts below; `make verify` running to completion. |

## Evidence

### Spark oracle banner

`{'spark_version': '4.1.2', 'session_tz': 'UTC'}` (recorder `branch_ops_1_truth.json`
`oracle: {spark 4.1.2, session_tz UTC, iceberg 1.11.0}`, recorded 2026-09-17).

### Procedure answers (one line per cell; full JSON in `branch_ops_1_truth.json`)

- `ff_positional` / `ff_named`: OK `(branch_updated string, previous_ref int64,
  updated_ref int64)`, row `(branch, prev-id, new-id)`.
- `ff_same` (branch to itself): OK, previous == updated (no-op).
- `ff_unknown_to`: `IllegalArgumentException: Ref does not exist: nope`.
- `ff_tag_as_branch`: `IllegalArgumentException: Ref t1 is a tag not a branch`.
- `ff_not_descendant` / `ff_to_tag` (backward): `IllegalArgumentException:
  Cannot fast-forward: <branch> is not an ancestor of <to>`.
- `ff_unknown_branch`: OK, auto-creates `(newb, null, snap)`.
- `ff_tag_forward` (old to tag t1, forward): OK.
- `cp_positional`: OK `(source_snapshot_id int64, current_snapshot_id int64)`,
  replay append parented on the head.
- `cp_duplicate`: `Py4JJavaError … CherrypickAncestorCommitException: Cannot cherrypick
  snapshot <id>: already picked to create ancestor <y>`.
- `cp_unknown`: `… ValidationException: Cannot cherry-pick unknown snapshot ID: 123456789`.
- `cp_ancestor`: `… Cannot cherrypick snapshot <id>: already an ancestor`.
- `cp_delete` / `cp_overwrite_static`: `… Cannot cherry-pick snapshot <id>: not append,
  dynamic overwrite, or fast-forward`.
- `sc_by_id` (non-ancestor jump): OK `(previous_snapshot_id int64,
  current_snapshot_id int64)`; `sc_by_ref_named`, `sc_ref_to_tag`, `sc_by_id_named`: OK.
- `sc_both` / `sc_neither`: `IllegalArgumentException: Either snapshot_id or ref must be
  provided, not both` (one message for both shapes).
- `sc_unknown_id`: `… ValidationException: Cannot roll back to unknown snapshot id: 123456789`.
- `sc_unknown_ref`: `… ValidationException: Cannot find matching snapshot ID for ref nope`.
- `rt_positional` / `rt_named`: OK `(previous_snapshot_id int64, current_snapshot_id int64)`,
  latest ancestor strictly older; `rt_named` on an already-older head is a no-op row.
- `rt_exact_equal` (ts == head ts): selects the parent (strict `<` proven).
- `rt_before_first`: `IllegalArgumentException: Cannot roll back, no valid snapshot older
  than: 946684800000`.
- `rt_string_utc` / `rt_string_ny`: string arguments read as UTC in both zones.
- `rt_tz_ny` (NY wall under NY zone): refuses, wall read as UTC (before first snapshot).
- `rt_tz_utc_wall_in_ny` (UTC wall under NY zone): OK, selects by the UTC reading.
- `tz_literal_eval_ny`: `UNIX_MICROS(TIMESTAMP <NY wall>)` under NY == true-mid micros,
  so the literal evaluates in-session and the UTC reading happens on the procedure path
  (parameter is `TimestampType`, read as micros per bytecode).

### Session-TZ note (audited, not assumed)

The brief assumed the timestamp argument follows the session time zone. Live measurement
refutes it: under `America/New_York`, a UTC wall selects correctly while the equivalent NY
wall is read as UTC (lands before the first snapshot and refuses with the wall-as-UTC
millis in the message). String arguments behave the same. RePark therefore parses naive
timestamp walls as UTC in every session zone (`parse_timestamp_to_ms`, unchanged); no zone
machinery was added. The NY-zone steps in the pin suite hold this rule.

### Error-class map (no JVM on the RePark side)

Spark's client-side `IllegalArgumentException` cells surface from RePark as
`IllegalArgumentException` with the same operative text (`DataFusionError::Configuration`,
newly mapped in `repark-core` `error_map.rs`; the `Invalid or Unsupported Configuration: `
display prefix is DataFusion's framing, as with existing config errors). Spark's
server-side `Py4JJavaError` cells surface as the base `PySparkException` with the same
operative text (fork messages are already Java-identical for cherry-pick; procedure-layer
pre-checks shape the set-current texts). The pins assert class + operative needle.

### Layering rule applied

Procedure-layer validation (ref existence/kind, fast-forward ancestry, set-current target
resolution, rollback timestamp floor) mirrors Java's procedure layer for message parity;
commit-time enforcement stays in the fork (`ManageSnapshots`, `Transaction::cherry_pick`),
which re-validates. No fork change in this unit.

### Red-first failures on the unfixed tree

Facade (2026-09-17, unfixed tree, release DEBUG module):

```text
repark.errors.UnsupportedOperationException: This feature is not implemented: CALL
system.fast_forward is not supported. Supported procedures: apply_partitioning,
expire_snapshots, plan_partitioning, register_table, rewrite_data_files, rewrite_manifests,
remove_orphan_files, rewrite_position_delete_files, rollback_to_snapshot, run_maintenance.
```

`test_branch_ops_v2_script` + `test_branch_ops_v3_lineage` FAILED; live tier skipped.
Rust `tests::branch_ops` (8 tests): all FAILED with the same refusal.

### Gate counts

- `test_ice_branch_ops_1.py` offline: 2 passed (42 s, JVM-free).
- `test_ice_branch_ops_1.py` live (`REPARK_PARITY_LIVE=1`): 2 passed (42 s, one JVM).
- `cargo test -p repark-spark --lib tests::branch_ops`: 8 passed.
- `cargo test -p repark-spark --lib tests::refs_and_wap`: 7 passed.
- `cargo test -p repark-spark --lib` (whole crate): 1051 passed, 0 failed, 4 ignored.
- `test_ref_branch_tag_wap.py` + `test_ice_branch_ops_1.py`: 14 passed, 2 skipped (live).
- `test_time_travel.py` + `test_v3e4_refs_time_travel.py`: 32 passed.
- Whole facade suite: 9326 passed, 369 skipped, 26 xfailed.
- Whole parity suite: 757 passed, 2 skipped, 12 xfailed.
- `make verify`: full log at `/tmp/ib-scratch/out/verify-final.log` (scratch, not committed).

## Coverage attestation

```text
COVERAGE_ATTESTATION:
  pr_unit: ice-branch-ops-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause replays against recorded Spark answers — 57 v2 steps plus 10 v3 steps with output schemas, rows, error needles, snapshot structure, refs and lineage asserted; the live tier replays Spark on the same shapes.
      artifacts: [python/repark/tests/test_ice_branch_ops_1.py, python/repark/tests/branch_ops_1_truth.json, crates/repark-spark/src/tests/branch_ops.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Unknown/missing/tag/non-descendant refs, both/neither snapshot_id/ref, unknown ids, ancient and exact-equal timestamps, same-ref no-op, auto-created branch, empty branch name — each pinned to Spark's answer or refusal.
      artifacts: [python/repark/tests/test_ice_branch_ops_1.py, crates/repark-spark/src/tests/branch_ops.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal path is pinned and asserts no snapshot is added; each procedure is one fork commit, and the pre-check/commit split resolves fail-closed because the fork re-validates at commit.
      artifacts: [crates/repark-spark/src/call/branch_ops.rs, crates/repark-spark/src/tests/branch_ops.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No in-process shared state (everything flows through the session call); table-state races between pre-check and commit resolve fail-closed at the fork's commit-time validation.
      artifacts: [crates/repark-spark/src/call/branch_ops.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, no credential, no network; table idents go through the existing resolve plus path-escape refusal.
    - id: AT-6
      status: ATTACKED
      evidence: Both-directions cross-reads (Spark reads RePark commits, RePark reads Spark commits, fresh-name adoption each way); snapshot parent/operation chain asserted step by step; v3 next-row-id and _row_id byte-identical to Spark.
      artifacts: [python/repark/tests/test_ice_branch_ops_1.py]
    - id: AT-7
      status: N/A
      justification: One commit per call and one refs-table scan sized by ref count; no hot loop, no unbounded growth.
    - id: AT-8
      status: ATTACKED
      evidence: Fork calls mirror the sibling procedures (no new API surface); error classes and needles pinned per cell; the session-TZ assumption was refuted by measurement and the UTC-always rule pinned instead of presumed.
      artifacts: [python/repark/tests/test_ice_branch_ops_1.py, python/repark/tests/branch_ops_1_truth.json]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal names the table, ref, snapshot id or timestamp floor; the pins assert those needles, so a mis-shaped refusal trips the suite.
      artifacts: [crates/repark-spark/src/tests/branch_ops.rs, python/repark/tests/test_ice_branch_ops_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first on both tiers (pasted refusal above); during development the suite caught a sibling-snapshot seed, a swapped row-id column pair, and a Java-wrapper message prefix, each fixed against the oracle rather than absorbed.
      artifacts: [task/ledgers/staging/ice-branch-ops-1-ledger.md, crates/repark-spark/src/tests/branch_ops.rs, python/repark/tests/test_ice_branch_ops_1.py]
  complete: true
```

## Addendum — round 19b (muse-worker build lane) — 2026-09-17

**Scope:** unblock `make verify` on the two new Python files only. No Rust, oracle,
truth-JSON, registry, or ledger-proposition change; no new pins.

**What red:** `py-lint` failed on `python/repark/tests/_record_branch_ops_1.py` and
`python/repark/tests/test_ice_branch_ops_1.py` with UP031 (`%`-formatting), UP017
(`timezone.utc`), I001 (import order), F541, E501, B905 (`zip` without `strict=`).

**Fixes (behavior-preserving):** `%`-templates rewritten as literal concatenation
(diff-reviewed: every recorded SQL string byte-identical); `timezone.utc` replaced
with bare `UTC` via `from datetime import UTC, datetime`, matching
`python/repark-parity/compat/runner.py` — the `ruff --fix` suggestion
`datetime.UTC` was wrong here because `datetime` binds the class, and it broke the
suite with `AttributeError` until corrected; four `zip` calls gained `strict=True`
(same-table columns, equal by construction); long lines wrapped; `ruff format` applied.

**Gates:** `ruff check` clean, `ruff format --check` clean,
`test_ice_branch_ops_1.py` 2 passed 2 skipped (live cells skip without
`REPARK_PARITY_LIVE=1`, same as round 1 offline), recorder pure helpers
(`_mark_ids`, `_parse_ts`, `_in_zone`) smoke-tested, `make verify` exit 0,
comment-ban grep clean (unit adds zero `//` and zero `#` comment lines).

## Round 2 (run 20b, muse-worker) — Grok P2/P3 findings — 2026-09-17

**Model:** muse-spark-1.3-contributor. **Base:** `a003f9f5` (fork pin `75da2b58`;
clean rebase, no conflicts). Reviews: `br-logic-report.md` (L-01–L-10),
`br-rustperf-report.md` (R-01–R-05), both PASS with findings.

### PROPOSITION LEDGER — round 2

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-012 | Empty and whitespace `branch` auto-create at `to` like Spark 4.1.2; trim refusal gone (L-05). AT-2's empty-branch clause, unpinned in round 1 (L-09), is now pinned. | `ff_empty_branch`, `ff_ws_branch` green. | PROVEN | Cells below; Spark creates `''`/`'   '` with `(name, null, head)`. |
| C-013 | A Spark WAP-staged snapshot adopted into RePark cherry-picks with `published-wap-id` stamped; duplicate pick refuses (L-04). | Live `test_live_branch_ops_adopted_shapes` (wap) green. | PROVEN | `wap_pick`, `wap_summary`, `wap_pick_dup` cells below. |
| C-014 | A Spark dynamic-overwrite (`replace-partitions`) staged snapshot adopted into RePark replays (L-03). | Live adopted test (dyn) green. | PROVEN | `dyn_pick` `(1,3)` overwrite, rows `(3,c),(10,a),(20,b)`. |
| C-015 | Cherry-pick fast-forward shape pinned, including a staged DELETE published as-is (L-02). | `cp_ff`, `cp_ff_delete` green. | PROVEN | `(12,12)`/`(13,13)`, no growth, rows follow. |
| C-016 | Rollback after a lateral `set_current` jump walks current ancestry (L-01); the procedure commits the pre-check's selected id via fork `rollback_to`, which re-validates ancestry at commit (R-04). | `rt_lateral` green. | PROVEN | `(5,5)` at ts of 11; a log walk would select 11. |
| C-017 | Selecting twins for the no-op `rt_named` / `rt_string_*` cells (L-07). | `rt_named2`, `rt_string2` green. | PROVEN | Both `(11,1)` with rows `(1,a),(2,b)`. |
| C-018 | Missing `snapshot_id`/`timestamp` refuse Spark's `REQUIRED_PARAMETER_NOT_FOUND`; wrong types match Spark's class+text (L-10). | `cp_missing_arg`, `rt_missing_arg`, `cp_wrongtype_str`, `rt_int_arg`, `rt_named_int_arg`, `rt_bad_string`, `rt_bad_typed` green. | PROVEN | Texts below; int timestamps no longer accepted as millis. |
| C-019 | Live cross-read rollback takes the timestamp from the table the procedure runs on and asserts `main` moved to the computed selection (L-06). | Live cross-read leg green. | PROVEN | `want_main` walked in-test; `current_snapshot_id` asserted. |
| C-020 | `list_snapshot_refs` refuses a mistyped refs schema instead of panicking (L-08); R-01/R-02/R-03/R-05 recorded as residue, no code change. | Residue section below. | PROVEN | Strict Utf8/Utf8/Int64 gate; fork `plan()` twice stays fork-internal. |

### Red-first on the unfixed tree (round-1 native, round-2 truth)

New pins replayed against the round-1 build fail first at `ff_empty_branch`:

```text
repark.errors.IllegalArgumentException: Invalid or Unsupported Configuration:
CALL fast_forward requires a non-empty `branch`
1 failed, 1 passed, 3 skipped
```

Pre-fix RePark answers for every L-05/L-10 shape (scratch probe `/tmp/check_errs.py`,
release native at `b8e055f5`, 2026-09-17), each mismatching the recorded Spark cell:

```text
ERR fast_forward('', ...) -> IllegalArgumentException: CALL fast_forward requires a non-empty `branch` (Spark: auto-creates)
ERR cherrypick_snapshot('ns.t') -> AnalysisException: CALL cherrypick_snapshot requires `snapshot_id` (named or positional #1) (Spark: REQUIRED_PARAMETER_NOT_FOUND)
ERR cherrypick_snapshot('ns.t', 'b') -> AnalysisException: CALL argument `snapshot_id` string is not an integer: b (Spark: NumberFormatException CAST_INVALID_INPUT)
ERR rollback_to_timestamp('ns.t') -> AnalysisException: CALL rollback_to_timestamp requires `timestamp` (named or positional #1) (Spark: REQUIRED_PARAMETER_NOT_FOUND)
ERR rollback_to_timestamp('ns.t', 123) -> IllegalArgumentException: Cannot roll back, no valid snapshot older than: 123 (Spark: AnalysisException DATATYPE_MISMATCH — the int was silently read as epoch-ms)
ERR rollback_to_timestamp('ns.t', 'not-a-time') -> AnalysisException: cannot parse TIMESTAMP AS OF value "not-a-time" (...) (Spark: DateTimeException CAST_INVALID_INPUT)
```

The round-1 Rust selection test passed epoch-ms as an INT literal, a shape Spark
refuses; it now spells the instant as `TIMESTAMP '<wall>'` (same selection, Spark-legal
shape). First run against the new code failed loud with the new refusal, then passed
after the conversion.

### Oracle notes (round-2 recording, PySpark 4.1.2 + Iceberg 1.11.0, 2026-09-17)

- `ff_empty_branch` / `ff_ws_branch`: OK `(branch_updated, previous_ref, updated_ref)` =
  `('', None, {pos:4})` / `('   ', None, {pos:4})`. Spark creates; no snapshot added.
- `cp_missing_arg` / `rt_missing_arg`: `AnalysisException`
  `[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine ... (either positionally at
  index 0 or by name) ... SQLSTATE: 4274K`.
- `cp_wrongtype_str`: `NumberFormatException` `[CAST_INVALID_INPUT] The value 'feat2'
  ... cannot be cast to "BIGINT" ... SQLSTATE: 22018`.
- `rt_int_arg` / `rt_named_int_arg`: `AnalysisException` `[DATATYPE_MISMATCH.
  UNEXPECTED_INPUT_TYPE] ... The second parameter requires the "TIMESTAMP" type,
  however "123" has the type "INT". SQLSTATE: 42K09` — identical for both spellings.
  The recorder's iceberg-line rule caught a `ResolvedProcedure ...@<addr>` plan-dump
  line for these two cells, so their prefixes were replaced with the operative line
  quoted verbatim from Spark's own run log (`/tmp/r2record.log`); same for
  `rt_bad_typed` (`ParseException`
  `[INVALID_TYPED_LITERAL] The value of the typed literal "TIMESTAMP" is invalid:
  'not-a-time'. SQLSTATE: 42604`).
- `rt_bad_string`: `DateTimeException` `[CAST_INVALID_INPUT] ... cannot be cast to
  "TIMESTAMP" ... SQLSTATE: 22018`.
- `rt_lateral`: `(5,5)`, no growth, rows `(1,a),(2,b),(3,c),(4,d),(6,f),(7,g)`.
- `rt_named2` / `rt_string2`: `(11,1)`, rows `(1,a),(2,b)`.
- `cp_ff` / `cp_ff_delete`: `(12,12)` / `(13,13)`, no growth; rows gain then lose
  `(12,l)`; refs follow.
- `dyn_pick`: `(1,3)` overwrite replay, rows `(3,c),(10,a),(20,b)`; staged with
  `df.writeTo(branch).overwritePartitions()`, summary `replace-partitions=true`.
- `wap_pick`: `(1,3)` append replay stamping `published-wap-id=r2wapid`, rows
  `(1,a),(2,b),(3,c)`; `wap_pick_dup`: `Py4JJavaError` `Duplicate request to cherry
  pick wap id that was published already: r2wapid`. Staged with
  `write.wap.enabled=true` + `spark.wap.id`, main moved past it after UNSETting the
  table property.

### Residue (P3s + fork-internal; no code change in this unit)

- R-01 (P2, fork-internal double `plan()` on cherry-pick replay): fork residue. The
  replay path loads the picked manifest list + own data manifests in both `validate`
  and `commit`, raw `FileIO::read`, no `ObjectCache`. RePark cannot cache the plan
  without a fork change, and this unit charters none. Disposition: record; a fork-side
  `plan`-once change is future work, not this unit.
- R-02 (P3, refs Arrow round-trip): accepted cost. `metadata.refs` is fork-crate-private;
  inspect is the public kind+id surface `fast_forward` needs. Zero I/O; noise at any
  realistic ref count. Disposition: record.
- R-03 (P3, by-ref scan instead of `snapshot_for_ref`): accepted micro-cost. One small
  in-memory map build per by-ref call; the inspect pass is already paid for kind.
  Disposition: record.
- R-05 (P3, fast_forward ancestry pre-check duplicates the fork walk): required for
  Spark's `IllegalArgumentException` text; the fork enforces again at commit. One extra
  linear parent-chain walk at ~10k HashMap lookups. Disposition: record.
- L-08 (P3): FIXED in this round — `list_snapshot_refs` now checks the inspect batch
  schema (strict Utf8/Utf8/Int64 over the first three columns) and refuses typed
  (`DataInvalid`) instead of panicking on `column(i)` / `as_string`.
- L-09: the AT-2 overclaim is resolved, not rewritten: the empty-branch clause it
  named without an artifact is now pinned by `ff_empty_branch`/`ff_ws_branch`, and the
  RePark-only trim refusal those cells replaced is deleted.

### Open questions

None. All brief ambiguities resolved by measurement against the oracle.

### Gate counts (round 2, release native rebuilt after every Rust edit)

- Unit file offline: `2 passed, 3 skipped` (live cells skip without the flag).
- Unit file live (`jb-jvm.sh`, `REPARK_PARITY_LIVE=1`): `5 passed` — first live run
  caught the WAP-dup precedence inversion (fork reported already-picked, Spark reports
  duplicate-WAP); the procedure-layer `duplicate_wap_pick` pre-check closed it and the
  re-run went `5 passed`. Full log `/tmp/r2live2.log` (scratch).
- `cargo test -p repark-spark --lib`: `1051 passed, 0 failed, 4 ignored`.
- Whole facade suite: `9326 passed, 370 skipped, 26 xfailed`
  (`/tmp/oc-worker/ib-build2/facade-r2.log`).
- Whole parity suite: `757 passed, 2 skipped, 12 xfailed`
  (`/tmp/oc-worker/ib-build2/parity-r2.log`).
- `make verify`: red on ONE assertion, three attempts, same test every time —
  `catalog::tests::catalog::listing_cost_list_tables_cheaper_than_provider_rebuild`
  (`list_table_names` 148ms / 24.7ms / 8.6ms vs provider rebuild ~2ms rock-stable,
  20 iterations). Everything else in the gate is green, and the test passes in
  isolation. The round-2 diff shares no code path with that test (only
  `write/snapshot_refs.rs` in `repark-iceberg`, which the listing/provider path never
  calls); the variance sits entirely on the filesystem side while sibling lanes hold
  the box at load 30–68 with test binaries at 400–600% CPU. Environmental contention,
  not a regression — see handback.
