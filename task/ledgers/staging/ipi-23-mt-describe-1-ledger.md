# Charter ledger — IPI-23-MT-DESCRIBE-1 · `DESCRIBE` on an Iceberg metadata table, Spark door

**Date:** 2026-09-22 · **Branch:** `fix/ipi-23-describe-metadata-table` · **Base:** `743f1be9` (`origin/main`) · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** row `DESC-1` in `docs/spark-sql-iceberg-parity.md` re-ruled this unit (metadata-table suffixes served, no longer fall-through).

**Retires:** in flight.

**Scope:** `DESCRIBE [TABLE] / DESC cat.ns.t.<meta>` on the Spark door answers one
row per column of the metadata table — `col_name`, `data_type` (the Spark DDL
type name), `comment` NULL — exactly the columns `SELECT *` returns, in the same
order. `crates/repark-spark/src/describe_show/metadata_table.rs` (new: the
`try_describe_metadata_table` intercept fed by the same `TableProvider` SELECT
resolves, plus the four-part parser for names the metadata rewrite leaves
untouched and the registry-defaults fallback for the facade-expanded two-part
form) with a five-line hook in `describe_show.rs` (`execute_describe_table`
before `load_table`), a two-line `or_else` in `router.rs`, and one `mod` line in
`describe_show.rs`; the facade pins
`python/repark/tests/test_ice_mt_describe_1.py` (eight offline pins plus the live
snapshots leg); the DESC-1 registry row; four `map.md` files
(`crates/repark-spark/src/`, `crates/repark-spark/src/describe_show/`,
`python/repark/tests/`, `task/ledgers/staging/`); and
this ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`, the ANSI
door, `time_travel.rs`, `wap.rs` and the metadata rewrite itself are untouched;
no code comment added anywhere. Critic r1 (2026-09-23) adds the three-way
base-load match in `dollar_metadata_table` (V-002), six facade pins (C-010..C-015:
three V-001 red specs, V-002, two sweep pins), and its section; V-001 stays OPEN
pending the orchestrator ruling.

## Measurements (decide-then-build evidence)

**M-1 — the oracle cell.** `R-MT-DESCRIBE` (`sb-mt/cells_dfmerge.py:31`,
`CREATE TABLE T (id BIGINT) USING iceberg` then `DESCRIBE T.snapshots`,
recorded 2026-09-22 against live PySpark 4.1.2 + Iceberg 1.11.0) answers six
rows — `[committed_at, timestamp]`, `[snapshot_id, bigint]`, `[parent_id,
bigint]`, `[operation, string]`, `[manifest_list, string]`, `[summary,
map<string,string>]`, every `comment` NULL — with no partition section and no
blank rows. The pinned local oracle is PySpark 4.1.2 (`uv.lock`), the same
version.

**M-2 — RePark main replays the failure.** At `743f1be9`,
`DESCRIBE c.n.t.snapshots` raises `AnalysisException
[TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE 42P01` naming `t$snapshots` (the rewrite
fires, then `load_table("t$snapshots")` misses); the four-part form with a
missing base and the `EXTENDED` form (the rewrite skips non-relation contexts,
so no `$` form is produced) both answer `Unsupported compound identifier …
Expected 1, 2 or 3 parts, got 4`; the two-part form after `USE c.n` answers
`NamespaceNotFound => No such namespace: NamespaceIdent(["t"])`. The ten red
pins in the facade file replay each of these before the fix.

**M-3 — audit posture.** Per the `audit-repark-parity` pass: no DESCRIBE rows
exist in the smoke suite; the DESC-1 claim "metadata-table suffixes fall
through unchanged" is the one stale claim and is re-ruled on its row in this
unit; the live re-measure runs in the work order's gate (`L=0`), not in this
lane's JVM-free loop, and is recorded in the gates table below.

## PROPOSITION LEDGER — IPI-23-MT-DESCRIBE-1 — 2026-09-22

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DESCRIBE mt.ns.t.snapshots` answers the six R-MT-DESCRIBE rows exactly (names, Spark DDL type strings, `comment` None), in order, with `col_name`/`data_type` non-nullable and `comment` nullable. | `test_snapshots_describe_matches_spark_rows` green. | **PROVEN** | Recorded cell `R-MT-DESCRIBE`; the live leg re-measures Spark and diffs repark row for row. pins: ipi-23-mt-describe-1/C-001 |
| C-002 | For `snapshots`, `files`, `history`, `refs`, `manifests`, `entries`: DESCRIBE's `col_name` list equals the column names of `SELECT * FROM mt.ns.t.<meta>` and each `data_type` equals the Spark DDL name of that column's Arrow type. | `test_describe_columns_match_select_star` green for all six. | **PROVEN** | DESCRIBE builds its batch from the same provider schema SELECT resolves, so the equality holds by construction; pinned per meta table. pins: ipi-23-mt-describe-1/C-002 |
| C-003 | `DESCRIBE TABLE mt.ns.t.snapshots`, `DESC mt.ns.t.SNAPSHOTS`, `DESCRIBE EXTENDED …` and `DESCRIBE TABLE FORMATTED …` answer C-001's rows. | `test_table_and_desc_upper_spellings_match` green. | **PROVEN** | Upper-case suffixes normalize through the rewrite or the canonical suffix; EXTENDED/FORMATTED print the column rows only per the work-order ruling (unmeasured on Spark). pins: ipi-23-mt-describe-1/C-003 |
| C-004 | After `USE mt.ns`, `DESCRIBE t.snapshots` answers C-001's rows. | `test_two_part_after_use_matches` green. | **PROVEN** | The two-part form resolves the base table in the session default namespace once the literal `namespace.table` reading misses. pins: ipi-23-mt-describe-1/C-004 |
| C-005 | `DESCRIBE` of a plain two-column table still answers exactly its two column rows. | `test_plain_table_describe_unchanged` green. | **PROVEN** | Near miss pinned: `[("a","bigint",None),("b","string",None)]` unchanged from main. pins: ipi-23-mt-describe-1/C-005 |
| C-006 | A real table named `snapshots` describes itself (`id bigint`), not a metadata table. | `test_real_table_named_snapshots_wins` green. | **PROVEN** | Near miss pinned: the rewrite's real-table-wins rule is untouched. pins: ipi-23-mt-describe-1/C-006 |
| C-007 | `DESCRIBE mt.ns.missing.snapshots` raises `TABLE_OR_VIEW_NOT_FOUND` / `42P01` naming the base table `missing` with no `$` in the text. | `test_missing_base_not_found_names_base` green. | **PROVEN** | A missing base maps to today's `table_or_view_not_found(catalog, namespace, base)`; any other provider error passes through. pins: ipi-23-mt-describe-1/C-007 |
| C-008 | `DESCRIBE mt.ns.t.nope` keeps today's error class, SQLSTATE and text. | `test_unknown_suffix_keeps_compound_identifier_error` green. | **PROVEN** | Near miss pinned: `AnalysisException: Error during planning: Unsupported compound identifier '`mt`.`ns`.`t`.`nope`'. Expected 1, 2 or 3 parts, got 4`, recorded on main before the fix. pins: ipi-23-mt-describe-1/C-008 |
| C-009 | The Rust row builder turns a three-field schema incl. a map into three rows with `comment` None. | `metadata_table_describe_batch_spells_column_rows` green. | **PROVEN** | Unit test in `crates/repark-spark/src/describe_show/metadata_table.rs` asserts names, `bigint`/`string`/`map<string,string>` spellings, null comments, and the `col_name`/`data_type`/`comment` output nullability. pins: ipi-23-mt-describe-1/C-009 |

## Critic r1 (2026-09-23) — V-001/V-002 + class sweep

Round head `ca95fadd` (md-r1 rebased onto `origin/main` `88b6f59f`, clean).
Critic r1 (head `2d1e18d0`, NEEDS_REMEDIATION) confirmed both premises below;
V-002 is remediated here, V-001 is OPEN pending an orchestrator ruling, and the
sweep table records every name shape. The COVERAGE_ATTESTATION below covers the
md-r1 clauses C-001..C-009; the critic-r1 clauses C-010..C-015 are tracked in
this section.

**V-001 (P1) — OPEN.** `unqualified_metadata_table` recovers explicit three-part
names through the session default namespace: after `USE mt.ns`,
`DESCRIBE mt.otherns.snapshots` and `DESCRIBE mt.t.snapshots` return the six
snapshots rows while `SELECT * FROM mt.otherns.snapshots` is table-not-found
(both measured at R0). The prescribed engine-side bool (catalog omitted as
written) cannot work: the facade expands the two-part form to
``DESCRIBE `mt`.`t`.`snapshots` `` before the engine, byte-identical to the
explicit three-part form (measured via `_expand_bare_table_names_in_sql`), so no
engine gate can keep the §4.4 two-part pin green while the explicit form falls
through. Red specs C-010/C-011/C-014 pin the required errors; the ruling (drop
§4.4, declare the corner, or open the facade expansion) is with the orchestrator.

**V-002 (P2) — REMEDIATED.** `dollar_metadata_table` mapped only
`ErrorKind::TableNotFound` to `table_or_view_not_found`, so
`DESCRIBE mt.nosuchns.t.snapshots` answered `failed to resolve schema: nosuchns`
(measured at R0, both spellings). The base-load match is now three-way:
`TableNotFound | NamespaceNotFound` maps to `table_or_view_not_found(catalog,
namespace, base)`; any other base error surfaces via the plain path's
`iceberg_err` mapping; base `Ok` keeps the provider error. The Spark answer for
the missing-namespace form is UNMEASURED; the 42P01 mapping follows the md-r1 §3
rule that an absent base names the base table.

**Mechanics.** R0 is red on `check-lib-rs` through main's drift, not this
round: `origin/main` filled the repark-spark root ceiling exactly (152) with
`view_dispatch`/`view_ddl`, and the md-r1 `mod describe_metadata_table;` line
takes the tree to 153. Both files that can resolve it (`lib.rs`,
`check_lib_rs.py`) are outside this round's file list, the pre-commit hook
blocks every commit while the tree is red, and `--no-verify` is forbidden, so
this round's work is verified on the tree and handed back uncommitted. No new
file was added, so no `map.md` change is owed.

**Sweep** (class: the intercept claims or misreports a name the plain DESCRIBE
path owns; measured on the V-002 tree):

| Shape | Answer | Pin |
|---|---|---|
| two-part after USE | six rows | C-004 (mechanism pending V-001 ruling) |
| two-part without USE | `NamespaceNotFound ns=["t"]` | none (ruling-dependent, OPEN) |
| explicit three-part, real table exists | describes the real table | C-006 |
| explicit three-part, namespace exists, table missing | six rows (wrong) | C-010 red spec (OPEN) |
| explicit three-part, namespace missing | six rows (wrong) | C-011 red spec (OPEN) |
| explicit three-part, namespace named like a default-namespace table | six rows (wrong) | C-014 red spec (OPEN) |
| four-part, base present | six rows | C-001 |
| four-part, base missing | 42P01 naming base | C-007 |
| four-part, namespace missing | 42P01 naming base | C-012 |
| four-part, unknown suffix | compound-identifier error | C-008 |
| four-part, upper-case suffix | six rows | C-003 |
| quoted three-part ``t$suffix`` | six rows (same provider as SELECT) | C-013 |
| EXTENDED/FORMATTED of base-missing | 42P01 naming base | C-015 |
| EXTENDED of unknown suffix | compound-identifier error | C-015 |
| EXTENDED of upper-case suffix | six rows | C-015 |
| FORMATTED of namespace-missing | 42P01 naming base | C-015 |
| temp view named like base + two-part after USE | expansion ignores the view, six rows | none (V-001-dependent, OPEN) |
| SELECT counterpart of the V-001 shape | table-not-found | critic-measured, no pin (read path) |

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-V-001
  severity: S1
  category: AT-1
  clause: C-010, C-011, C-014
  claim: Explicit three-part DESCRIBE names take default-namespace recovery and answer metadata rows for tables that do not exist, while SELECT on the same name is table-not-found.
  evidence: R0 `DESCRIBE mt.otherns.snapshots` and `DESCRIBE mt.t.snapshots` answer six rows after USE mt.ns; `SELECT * FROM mt.otherns.snapshots` raises table-not-found; facade expansion of the two-part form is byte-identical to the explicit form so no engine gate separates them
  disposition: OPEN (ruling with the orchestrator; red specs test_explicit_three_part_with_real_namespace_falls_through, test_explicit_three_part_missing_namespace_keeps_plain_error, test_explicit_three_part_namespace_shadowing_base_falls_through)
```

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-V-002
  severity: S2
  category: AT-3
  clause: C-012
  claim: A missing namespace on a four-part DESCRIBE leaked the provider error `failed to resolve schema` instead of 42P01 naming the base table.
  evidence: R0 `DESCRIBE mt.nosuchns.t.snapshots` and the EXTENDED spelling both raised `failed to resolve schema: nosuchns`; post-fix both raise the full TABLE_OR_VIEW_NOT_FOUND message naming `mt`.`nosuchns`.`t`
  disposition: REMEDIATED (three-way base-load match in dollar_metadata_table; pinned by test_missing_namespace_maps_to_not_found_naming_base; Spark's missing-namespace answer is UNMEASURED)
```

## PROPOSITION LEDGER — IPI-23-MT-DESCRIBE-1 critic r1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-010 | After `USE mt.ns` with table `mt.ns.otherns` and namespace `mt.otherns` present and no table `mt.otherns.snapshots`, `DESCRIBE mt.otherns.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND message naming `` `mt`.`otherns`.`snapshots` ``. | `test_explicit_three_part_with_real_namespace_falls_through` green. | **OPEN** | Red at R0 (`DID NOT RAISE`, six rows); needs the V-001 ruling. pins: ipi-23-mt-describe-1/C-010 |
| C-011 | After `USE mt.ns` with table `mt.ns.t`, `DESCRIBE mt.t.snapshots` raises exactly the plain-path error `NamespaceNotFound => No such namespace: NamespaceIdent(["t"])`, never rows. | `test_explicit_three_part_missing_namespace_keeps_plain_error` green. | **OPEN** | Red at R0 (`DID NOT RAISE`, six rows); needs the V-001 ruling. pins: ipi-23-mt-describe-1/C-011 |
| C-012 | `DESCRIBE mt.nosuchns.t.snapshots` and `DESCRIBE EXTENDED mt.nosuchns.t.snapshots` raise 42P01 naming `` `mt`.`nosuchns`.`t` ``. | `test_missing_namespace_maps_to_not_found_naming_base` green. | **PROVEN** | Three-way base-load match; Spark's form UNMEASURED. pins: ipi-23-mt-describe-1/C-012 |
| C-013 | `DESCRIBE mt.ns.`t$snapshots`` answers the six snapshots rows. | `test_quoted_dollar_name_answers_metadata_rows` green. | **PROVEN** | Same provider as SELECT by construction. pins: ipi-23-mt-describe-1/C-013 |
| C-014 | After `USE mt.ns` with namespace `mt.t` present, `DESCRIBE mt.t.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND message naming `` `mt`.`t`.`snapshots` ``. | `test_explicit_three_part_namespace_shadowing_base_falls_through` green. | **OPEN** | Red (`DID NOT RAISE`, six rows); same V-001 class as C-010. pins: ipi-23-mt-describe-1/C-014 |
| C-015 | EXTENDED/FORMATTED of base-missing, unknown-suffix, upper-case-suffix, and namespace-missing four-part names match their plain answers. | `test_four_part_extended_formatted_matrix` green. | **PROVEN** | All five matrix rows measured and pinned. pins: ipi-23-mt-describe-1/C-015 |

## Gates

| Command | Result |
|---|---|
| `build-slot.sh local-gate.sh xo55-md "repark-spark:--lib+describe" test_ice_mt_describe_1.py test_describe_table.py test_metadata_tables.py test_cap_1_source_file_line_cap.py` | `CB=0 R=0 T=0 U=0 L=0`: rust 62 passed; offline 63 passed, 2 skipped; live 65 passed |
| `harness.py --engine repark --only R-MT-DESCRIBE --out out/repark-md1.json` (`sb-mt`) | `ok`: the six rows byte-identical to `out/spark-dfmerge.json` |
| `cargo clippy --locked --workspace --all-targets -- -D warnings -A clippy::disallowed_methods` | exit 0 |
| panic-ban gate (workspace `--lib --bins` excl. repark-python + repark-python `--lib`, disallowed/unwrap/expect/panic/todo/unimplemented/unreachable) | exit 0 |
| `./scripts/check_rust_file_size.sh` | exit 0 (808 files clean) |
| `bash scripts/check_map_md.sh --base origin/main` | exit 0 (run at the final head) |
| `python3 scripts/check_ledger_grammar.py` | exit 0 (246 live ledgers clean) |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xo55-md origin/main HEAD` | exit 0 (`hits=0`, after every commit) |
| `uvx ruff@0.15.22 format --check` + `ruff check` on the new test file | exit 0 |
| critic r1 tree-gate (same four files) | `CB=0 R=0 T=0 U=1 L=1`: exactly the three V-001 red specs fail (offline 3 failed/66 passed/2 skipped; live 3 failed/68 passed); V-002, C-013/C-015 and every md-r1 pin green in both legs |
| critic r1 `cargo clippy` workspace + panic-ban (both invocations) + `./scripts/check_rust_file_size.sh` + ruff on the test file + `check_ledger_grammar.py` + `comment_ban.py origin/main` | all exit 0 |

## COVERAGE_ATTESTATION

```
COVERAGE_ATTESTATION:
  pr_unit: ipi-23-mt-describe-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each clause walked to its recorded oracle cell or main-branch behavior — the six R-MT-DESCRIBE rows with Arrow-path value, type and nullability asserts (C-001), per-meta DESCRIBE-vs-SELECT column equality across six metadata tables (C-002), spelling variants incl. EXTENDED/FORMATTED column-rows-only (C-003), the two-part USE form (C-004), and the four near misses pinning unchanged behavior or today's exact error (C-005/C-006/C-007/C-008).
      artifacts: [python/repark/tests/test_ice_mt_describe_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: The pins cover the `$` form via the rewrite, the un-rewritten four-part form (missing base, EXTENDED), the two-part session-default form, upper-case suffixes, a real table shadowing a metadata name, and unknown suffixes; the Rust unit test pins the row builder on bigint/string/map fields.
      artifacts: [python/repark/tests/test_ice_mt_describe_1.py, crates/repark-spark/src/describe_show/metadata_table.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Missing base maps to TABLE_OR_VIEW_NOT_FOUND/42P01 naming the base table with no `$` leak (C-007); unknown suffixes keep the compound-identifier plan error byte for byte (C-008); any other provider error passes through unchanged by construction of the match.
      artifacts: [python/repark/tests/test_ice_mt_describe_1.py, crates/repark-spark/src/describe_show/metadata_table.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Resolution runs per query through Session state (`ctx.table_provider` plus catalog `load_table` probes); no shared mutable state or global registration is added.
      artifacts: [crates/repark-spark/src/describe_show/metadata_table.rs]
    - id: AT-5
      status: N/A
      justification: Read-only describe of the session's own catalog metadata; no auth, secret, or injection surface added.
    - id: AT-6
      status: ATTACKED
      evidence: Registry row DESC-1 moved from "metadata-table suffixes fall through unchanged" to the recorded served behavior; the replaced fall-through was itself the reported bug, and every previously green neighbor (plain tables, real tables named like metadata tables, unknown suffixes) is pinned unchanged.
      artifacts: [docs/spark-sql-iceberg-parity.md, python/repark/tests/test_ice_mt_describe_1.py]
    - id: AT-7
      status: N/A
      justification: One provider-schema read per DESCRIBE plus at most two catalog probes on narrow paths; no added materialization, unbounded growth, or hot-loop pattern.
    - id: AT-8
      status: ATTACKED
      evidence: The batch builds from the same TableProvider schema SELECT resolves, so DESCRIBE and SELECT agree column for column by construction; no dependency or pin move.
      artifacts: [crates/repark-spark/src/describe_show/metadata_table.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The missing-base error names the base table without the `$` suffix form, so a typo is diagnosable from the error; rejected shapes keep today's loud errors.
      artifacts: [crates/repark-spark/src/describe_show/metadata_table.rs, python/repark/tests/test_ice_mt_describe_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: The recorded R-MT-DESCRIBE cell replays verbatim in the facade test and the gate's live leg; every clause carries a pins citation in the test file or the crate map; C-001 asserts Arrow field types and nullability alongside values.
      artifacts: [python/repark/tests/test_ice_mt_describe_1.py, python/repark/tests/map.md, crates/repark-spark/src/describe_show/map.md]
```

Every md-r1 clause above is PROVEN against the recorded R-MT-DESCRIBE cell and
the main-branch behaviors pinned beside it; critic r1 leaves C-010/C-011/C-014
OPEN pending the V-001 ruling with red specs filed. Touched files per the Scope
paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`, the ANSI door,
`time_travel.rs`, `wap.rs` and the metadata rewrite are untouched. `make verify`
and the whole-workspace suites were not run per the work order's gate list; the
gates table above is the proof.
