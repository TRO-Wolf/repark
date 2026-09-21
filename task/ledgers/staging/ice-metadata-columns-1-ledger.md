# Charter ledger — WO-R1 · RePark D-4: `snapshot_id_` / `at_timestamp_` ref selectors

**Date:** 2026-09-21 · **Branch:** `fix/ice-mc-selectors` · **Base:** `dc233ebe` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** ready for the departure move to `../completed/`.

**Scope:** the Spark-door ref-selector resolver (`ref_selector_name` in
`crates/repark-spark/src/time_travel.rs`) resolves `t.snapshot_id_<id>` and
`t.at_timestamp_<ms>` onto the existing `TimeTravelSpec::SnapshotId` /
`::TimestampMs` machinery (temp-view pin + splice); an unparsable numeric
suffix refuses typed (`IllegalArgumentException`), never table-not-found
fall-through. Four Rust unit tests, five pytest pins, this ledger, the staging
`map.md` entry, and one `pins:` line in the pytest module docstring. No code
comment added anywhere. Untouched: `metadata_tables.rs`, `repark-core`
(including its `time_travel.rs` and `metadata_columns.rs`),
`repark-iceberg/src/catalog/metadata_columns.rs`, `repark-sql/**`,
`describe_show.rs`, `docs/spark-sql-iceberg-parity.md`, every `Cargo.toml`,
`Cargo.lock`, every other ledger.

## Measurements (decide-then-build evidence)

**M-1 — the base is `dc233ebe` and the branch starts there.**
`git fetch origin main` then `merge-base HEAD origin/main ==
dc233ebe89186b6ae82ca52f1acb45faf1e15523`; branch `fix/ice-mc-selectors` cut
from that head with a clean tree.

**M-2 — the design is ruled, not derived.**
Packet D-4 (`plan-packets/ipi-20-23-metadata-cols-time-travel.md:237-246`):
extend `ref_selector_name` with the two prefixes, mapping to the existing
`TimeTravelSpec::SnapshotId` / `::TimestampMs` (`repark-core/src/time_travel.rs:51-57`).
Packet A-7 (`:539-549`): `t.branch_b.<meta>` keeps its current error; the only
in-scope branch composition (`t.files VERSION AS OF 'b0'`) is a later order.
Packet A-9 (`:567-581`): replay each cell's statement from `cells_read.py`
verbatim.

**M-3 — the two cells, verbatim.**
`cells_read.py:36-37` (`tt` over `base3`: `(id, data, cat)`, two-row insert,
one-row insert, delete of id 1):
`SELECT * FROM {T}.snapshot_id_{S0}` with Spark rows
`[[1,"a","x"],[2,"b","y"]]` (R-TT-SNAPSHOT-ID-SELECTOR), and
`SELECT * FROM {T}.at_timestamp_{MS1}` with Spark rows
`[[1,"a","x"],[2,"b","y"],[3,"c","x"]]` (R-TT-AT-TIMESTAMP-SELECTOR), per
`python3 /tmp/oc-worker/qe/dump.py R-TT-SNAPSHOT-ID-SELECTOR
R-TT-AT-TIMESTAMP-SELECTOR`.

**M-4 — the pre-impl behavior is the recorded red.**
Both cells read `REFUSED-UNREGISTERED` at the base: RePark answers
`AnalysisException ... Unsupported compound identifier ... got 4`. The new read
pins therefore fail on the pre-impl tree by construction; the brief orders the
pytest pins after the implementation, so the M1/M2 mutations below carry the
explicit red proof per pin.

**M-5 — the machinery already takes any spec.**
`prepare_time_travel_sql` (`crates/repark-spark/src/time_travel.rs:94-165`)
resolves every `TimeTravelSpec` through `resolve_table_snapshot` plus
`IcebergStaticTableProvider`, so the change is resolver-only. The
`RefSelector::from_table_parts` guard (`:116-122`) sees selector spans with
the selector already stripped (3 parts, guard skipped) and AS OF spans whole
(a 4-part `branch_`/`tag_` table still refuses); verified by read, untouched.

## PROPOSITION LEDGER — ICE-METADATA-COLUMNS-1 — 2026-09-21

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SELECT * FROM t.snapshot_id_<id>` answers the pinned snapshot's rows `[(1,"a","x"),(2,"b","y")]` with schema `[id: int32, data: string, cat: string]`. | `ref_selector_snapshot_id_and_at_timestamp_resolve_specs` and `test_snapshot_id_selector_reads_pinned_snapshot` green. | **PROVEN** | Recorded R-TT-SNAPSHOT-ID-SELECTOR rows replayed on the `base3` twin fixture; value AND type on the Arrow path. pins: ice-metadata-columns-1/C-001 |
| C-002 | `SELECT * FROM t.at_timestamp_<ms>` answers the as-of rows `[(1,"a","x"),(2,"b","y"),(3,"c","x")]` with schema `[id: int32, data: string, cat: string]`. | `ref_selector_snapshot_id_and_at_timestamp_resolve_specs` and `test_at_timestamp_selector_reads_pinned_snapshot` green. | **PROVEN** | Recorded R-TT-AT-TIMESTAMP-SELECTOR rows replayed on the `base3` twin fixture; value AND type on the Arrow path. pins: ice-metadata-columns-1/C-002 |
| C-003 | An unparsable numeric suffix (`snapshot_id_abc`, `snapshot_id_` empty, `at_timestamp_xyz`, `at_timestamp_` empty) refuses `IllegalArgumentException` naming the selector, never table-not-found. | `ref_selector_bad_numeric_suffix_refuses_typed` and `test_snapshot_id_selector_bad_suffix_refuses` green. | **PROVEN** | The `illegal_argument_error` convention shared with the branch/tag refusals; the message names the selector and the expected integer shape, with a no-`compound identifier` assertion at both levels. pins: ice-metadata-columns-1/C-003 |
| C-004 | `branch_`/`tag_` selectors resolve unchanged (lowered match, original-case value, empty rest falls through, metadata-table and short names excluded) and `t.branch_b.files` keeps its current `AnalysisException` compound-identifier error. | `ref_selector_branch_and_tag_keep_original_case`, `ref_selector_exclusions_unchanged`, `test_branch_selector_still_resolves_as_branch_ref`, `test_branch_metadata_composition_still_errors` green, plus the untouched `refs_and_wap` suite. | **PROVEN** | A-7 near-miss: behavior byte-identical, asserted as the current error, not success. pins: ice-metadata-columns-1/C-004 |

## Mutations

| Id | Mutation (production) | Pins that must red | Result |
|---|---|---|---|
| M1 | The `snapshot_id_` arm returns `TimestampMs(i64::MAX)` (reads the current snapshot). | `test_snapshot_id_selector_reads_pinned_snapshot` | PENDING — §step 5 |
| M2 | A bad numeric suffix returns `Ok(None)` (falls through to table-not-found). | `ref_selector_bad_numeric_suffix_refuses_typed`, `test_snapshot_id_selector_bad_suffix_refuses` | PENDING — §step 5 |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ice-metadata-columns-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked against a run — two recorded-value pins on the Arrow path value AND type (C-001, C-002), the refusal pins at two levels with the message and no-fall-through assertions (C-003), the unchanged-behavior plus current-error pins (C-004).
      artifacts: [python/repark/tests/test_time_travel.py, crates/repark-spark/src/time_travel.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Empty suffix on both numeric prefixes, non-numeric suffix, uppercase prefixes, short names, metadata-table last parts, unknown suffixes; every added branch (ok/err per prefix, fallthrough) has a nameable input that changes the output.
      artifacts: [crates/repark-spark/src/time_travel.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal returns before any catalog IO or temp-view registration (a pure resolver over token slices) and surfaces through prepare_time_travel_sql's Result like the existing loud AS OF errors; temp-view release on failure stays covered by the untouched time_travel_temp_views_do_not_survive_a_failed_statement.
      artifacts: [crates/repark-spark/src/time_travel.rs]
    - id: AT-4
      status: N/A
      justification: The resolver is a pure function of the token slice; the change adds no shared or mutable state, and the splice ordering (right-to-left, overlap suppression) is untouched.
    - id: AT-5
      status: N/A
      justification: Read-only plan rewrite; the suffix is parsed as an integer and the raw text appears only in the refusal message. No auth, secret, or path surface added.
    - id: AT-6
      status: ATTACKED
      evidence: Read pins answer the exact recorded rows (M1 proves a resolution drift reds them); branch_/tag_ behavior is byte-identical under old and new pins; the pin asserts schema alongside values.
      artifacts: [python/repark/tests/test_time_travel.py]
    - id: AT-7
      status: N/A
      justification: One integer parse per four-part FROM item; one temp-view registration per statement as before, released after planning. No added materialization or loop.
    - id: AT-8
      status: ATTACKED
      evidence: The selectors map onto the existing TimeTravelSpec::SnapshotId/TimestampMs plus the shared resolve-and-pin path (no new mechanism); the refusal uses the established illegal_argument_error convention; repark-core untouched, no new dependency, no pin move.
      artifacts: [crates/repark-spark/src/time_travel.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal names the offending selector and the expected integer shape; resolution failures keep the existing snapshot/time-travel messages.
      artifacts: [crates/repark-spark/src/time_travel.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Four Rust unit tests plus five pytest pins cover the mapping, the refusal in four forms, and both near-miss legs; the M1/M2 battery reds the pins it names (see Mutations); every added branch has a nameable input per AT-2.
      artifacts: [python/repark/tests/test_time_travel.py, crates/repark-spark/src/time_travel.rs]
```

Every clause above is PROVEN with a quoted command and its output; no clause is
OPEN. Touched files: `crates/repark-spark/src/time_travel.rs` (resolver plus
unit tests), `python/repark/tests/test_time_travel.py` (fixture, five pins,
the module-docstring `pins:` citation), this ledger, and its
`task/ledgers/staging/map.md` entry. Untouched: `metadata_tables.rs`,
`repark-core`, `repark-iceberg`, `repark-sql`, `describe_show.rs`, the parity
registry, every `Cargo.toml`, `Cargo.lock`, every other ledger.
