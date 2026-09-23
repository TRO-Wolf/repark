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
untouched) with a five-line hook in `describe_show.rs` (`execute_describe_table`
before `load_table`), a two-line `or_else` in `router.rs`, and one `mod` line in
`describe_show.rs` (moved under it 2026-09-23, `lib.rs` back to its ceiling); the facade pins
`python/repark/tests/test_ice_mt_describe_1.py` (22 offline pins plus the live
snapshots leg); the DESC-1 registry row; four `map.md` files
(`crates/repark-spark/src/`, `crates/repark-spark/src/describe_show/`,
`python/repark/tests/`, `task/ledgers/staging/`); and
this ledger. The fork, every `Cargo.toml`, `Cargo.lock`, `STATUS.md`, the ANSI
door, `time_travel.rs`, `wap.rs` and the metadata rewrite itself are untouched;
no code comment added anywhere. Critic r1 (2026-09-23) adds the three-way
base-load match in `dollar_metadata_table` (V-002), six facade pins (C-010..C-015:
three V-001 red specs, V-002, two sweep pins), and its section; V-001 stays OPEN
pending the orchestrator ruling. Critic r3fix (2026-09-23) applies the Spark
rulings: the recovery is deleted (V-001 option (a)), the four-part not-found
names the full name via `table_or_view_not_found_parts`, C-004/C-007/C-012/C-015
are re-pinned, C-016 is a strict xfail, and the module move resolves the ceiling
without touching `check_lib_rs.py`. Critic r4 (2026-09-23, NEEDS_REMEDIATION)
is remediated by md-r6fix: a base load failing with `NamespaceNotFound` maps to
the same 42P01 answer as `TableNotFound` (C-004/C-011 re-pinned to the
facade-expanded name; D-1 narrows to the name shape), the four-part intercept
probes the name as written so a real table wins it (new nested-namespace
collision pin; the nested-namespace DESCRIBE gap is recorded as D-4), and the
C-016 row plus the test docstring are corrected. Critic r5 (2026-09-23,
NEEDS_REMEDIATION) is remediated by md-r7fix: the missing-base/namespace 42P01
answer now names the identifier as written — `written_parts` (case kept) on the
un-rewritten four-part form, the parsed `catalog.namespace.t$suffix` name on the
`$` form — and this paragraph's deleted-fallback and pin-count claims are trued.

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
| C-004 | After `USE mt.ns`, `DESCRIBE t.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND / SQLSTATE 42P01 message naming the facade-expanded `` `mt`.`t`.`snapshots` ``. | `test_two_part_after_use_is_plain_not_found` green. | **PROVEN** | Re-ruled by Spark measurement and md-r6fix: Spark answers 42P01 naming `` `t`.`snapshots` `` as written; RePark names the facade-expanded name (D-1). pins: ipi-23-mt-describe-1/C-004 |
| C-005 | `DESCRIBE` of a plain two-column table still answers exactly its two column rows. | `test_plain_table_describe_unchanged` green. | **PROVEN** | Near miss pinned: `[("a","bigint",None),("b","string",None)]` unchanged from main. pins: ipi-23-mt-describe-1/C-005 |
| C-006 | A real table named `snapshots` describes itself (`id bigint`), not a metadata table. | `test_real_table_named_snapshots_wins` green. | **PROVEN** | Near miss pinned: the rewrite's real-table-wins rule is untouched. pins: ipi-23-mt-describe-1/C-006 |
| C-007 | `DESCRIBE mt.ns.missing.snapshots` raises `TABLE_OR_VIEW_NOT_FOUND` / `42P01` naming the full `` `mt`.`ns`.`missing`.`snapshots` `` with no `$` in the text. | `test_missing_base_not_found_names_full_name` green. | **PROVEN** | A missing base maps to `table_or_view_not_found_parts` over the full four-part name; any other provider error passes through. pins: ipi-23-mt-describe-1/C-007 |
| C-008 | `DESCRIBE mt.ns.t.nope` keeps today's error class, SQLSTATE and text. | `test_unknown_suffix_keeps_compound_identifier_error` green. | **PROVEN** | Near miss pinned: `AnalysisException: Error during planning: Unsupported compound identifier '`mt`.`ns`.`t`.`nope`'. Expected 1, 2 or 3 parts, got 4`, recorded on main before the fix. pins: ipi-23-mt-describe-1/C-008 |
| C-009 | The Rust row builder turns a three-field schema incl. a map into three rows with `comment` None. | `metadata_table_describe_batch_spells_column_rows` green. | **PROVEN** | Unit test in `crates/repark-spark/src/describe_show/metadata_table.rs` asserts names, `bigint`/`string`/`map<string,string>` spellings, null comments, and the `col_name`/`data_type`/`comment` output nullability. pins: ipi-23-mt-describe-1/C-009 |

## Critic r1 (2026-09-23) — V-001/V-002 + class sweep

Round head `ca95fadd` (md-r1 rebased onto `origin/main` `88b6f59f`, clean).
Critic r1 (head `2d1e18d0`, NEEDS_REMEDIATION) confirmed both premises below;
V-002 is remediated here, V-001 is OPEN pending an orchestrator ruling, and the
sweep table records every name shape. The COVERAGE_ATTESTATION below covers the
md-r1 clauses C-001..C-009; the critic-r1 clauses C-010..C-015 are tracked in
this section.

**V-001 (P1) — REMEDIATED as option (a).** Spark measures (2026-09-23, §2 table
below) that `DESCRIBE t.snapshots` after `USE` is TABLE_OR_VIEW_NOT_FOUND, so
the md-r1 §4.4 two-part recovery answered a name Spark does not resolve: the
recovery (`unqualified_metadata_table`) is deleted, and after `USE mt.ns` the
two-part form, `DESCRIBE mt.t.snapshots` and `DESCRIBE mt.otherns.snapshots`
all take the plain path. Since md-r6fix the plain path maps a missing
namespace to the same 42P01 answer, so RePark names the facade-expanded
`` `mt`.`t`.`snapshots` `` where Spark names `` `t`.`snapshots` `` — both
TABLE_OR_VIEW_NOT_FOUND / 42P01, the residue being the name shape only:
known divergence D-1, facade- and plain-path-owned. The
r2fix red specs C-010/C-011/C-014 are green unchanged.

**V-002 (P2) — REMEDIATED, re-ruled to the full name.** Spark measures
TABLE_OR_VIEW_NOT_FOUND naming the full four-part name for a missing base or
namespace, so the md-r1 "naming the base" rule is replaced:
`TableNotFound | NamespaceNotFound` on the base load answers 42P01 naming
`` `catalog`.`namespace`.`base`.`suffix` `` via the new
`table_or_view_not_found_parts` helper (the three-part function delegates to it,
so the tail is shared byte-for-byte); any other base error surfaces via
`iceberg_err`; base `Ok` keeps the provider error. A suffix spelled upper-case
was named canonicalized until md-r7fix; the answer now names the parts as
written (C-018), and a quoted `` `t$snapshots` `` with a missing base names the
written `$` name (C-019).

**Mechanics.** R0 is red on `check-lib-rs` through main's drift, not this
round: `origin/main` filled the repark-spark root ceiling exactly (152) with
`view_dispatch`/`view_ddl`, and the md-r1 `mod describe_metadata_table;` line
takes the tree to 153. Both files that can resolve it (`lib.rs`,
`check_lib_rs.py`) are outside this round's file list, the pre-commit hook
blocks every commit while the tree is red, and `--no-verify` is forbidden, so
this round's work is verified on the tree and handed back uncommitted. Resolved
2026-09-23 by moving the module under `describe_show/` (`lib.rs` 152, no
`check_lib_rs.py` edit); the r2fix tree was committed as the move commit. No new
file was added, so no `map.md` change is owed.

**Spark answers (§2, measured 2026-09-23, PySpark 4.1.2 + Iceberg 1.11.0,
`sc.db.t` + `sc.db.otherns` + `sc.otherns`):** `USE sc.db` then `DESCRIBE
t.snapshots` → not-found `` `t`.`snapshots` ``; `DESCRIBE sc.t.snapshots` →
not-found `` `sc`.`t`.`snapshots` ``; `DESCRIBE otherns.snapshots` /
`sc.otherns.snapshots` → not-found (name as written); `DESCRIBE db.t.snapshots`
→ six rows; `DESCRIBE sc.db.t.snapshots` / `sc.db.t.SNAPSHOTS` → six rows;
`DESCRIBE [EXTENDED] sc.db.missing.snapshots` → not-found full four-part name;
`DESCRIBE [EXTENDED] sc.nosuchns.t.snapshots` → not-found full four-part name;
`DESCRIBE sc.db.t.nosuchmeta` → not-found full four-part name; `DESCRIBE
sc.db.`t$snapshots`` → not-found `` `sc`.`db`.`t$snapshots` ``. SELECT gives the
same not-found answers. md-r7fix re-measured (same PySpark 4.1.2 + Iceberg
1.11.0, catalog `mt`): `DESCRIBE mt.ns.missing.SNAPSHOTS`,
`DESCRIBE mt.ns.Missing.snapshots` and `DESCRIBE EXTENDED
mt.ns.missing.SNAPSHOTS` → AnalysisException TABLE_OR_VIEW_NOT_FOUND /
SQLSTATE 42P01 naming the parts as written (`` `mt`.`ns`.`missing`.`SNAPSHOTS` ``,
`` `mt`.`ns`.`Missing`.`snapshots` ``); `DESCRIBE mt.ns.`missing$snapshots`` →
not-found naming `` `mt`.`ns`.`missing$snapshots` ``.

**Sweep** (class: the intercept answers a name Spark does not resolve, or names
a not-found differently from Spark; RePark measured on the ruled tree):

| Shape | RePark | Spark (§2) | Pin |
|---|---|---|---|
| four-part, base present (+upper) | six rows | six rows | C-001, C-003 |
| four-part, base missing (+EXTENDED/FORMATTED, +upper-case suffix/base) | 42P01 name as written | 42P01 name as written | C-007, C-015, C-018 |
| four-part, namespace missing (+EXTENDED/FORMATTED) | 42P01 full four-part name | 42P01 full four-part name | C-012, C-015 |
| four-part, unknown suffix (+EXTENDED) | compound-identifier error | 42P01 full four-part name | C-008, C-015 (D-2, not fixed) |
| quoted three-part ``t$suffix``, base present | six rows (same provider as SELECT) | 42P01 naming the `$` name | C-013 (D-3, not fixed) |
| quoted three-part ``t$suffix``, base missing | 42P01 written `$` name `` `mt`.`ns`.`missing$snapshots` `` | 42P01 written `$` name | C-019 |
| two-part after USE | 42P01 facade-expanded `` `mt`.`t`.`snapshots` `` | 42P01 `` `t`.`snapshots` `` | C-004 (D-1 name shape) |
| explicit three-part, namespace missing | 42P01 `` `mt`.`t`.`snapshots` `` | 42P01 (name as written) | C-011 (D-1 family, name shape) |
| explicit three-part, table missing | 42P01 full three-part name as written | 42P01 (name as written) | C-010, C-014 |
| four-part name where a real table occupies the written path (nested namespace) | compound-identifier error | the real table's rows | Rust `describe_metadata_table_real_table_at_written_path_wins` (D-4 gap) |
| three-part `ns.t.snapshots` after USE | fallthrough `table 'ns.t.snapshots' not found` | six rows | C-016 strict xfail (OPEN) |
| two-part without USE | 42P01 `` `mt`.`t`.`snapshots` `` | unmeasured | none (plain path, settled) |
| temp view named like base + two-part | 42P01 expanded name (view ignored) | unmeasured | none (plain path, settled) |
| plain table / real table named `snapshots` | unchanged | — | C-005, C-006 |

**Known divergences (not fixed here):** D-1 — RePark names the facade-expanded
name where Spark names the name as written; since md-r6fix both legs answer
TABLE_OR_VIEW_NOT_FOUND / 42P01, so the residue is the name shape only
(two-part after USE, explicit three-part with missing namespace). D-2 —
unknown metadata suffix keeps the plain compound-identifier error instead of
Spark's 42P01. D-3 — quoted
``t$snapshots`` answers metadata rows (agreeing with RePark's SELECT) instead of
Spark's not-found. D-4 — plain DESCRIBE cannot describe a nested-namespace
table: a four-or-more-part name answers the compound-identifier plan error even
when a real table occupies the path (measured `mt.ns.sub.plain`; the r6fix
collision pin defers to that same answer rather than claiming the name).

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-V-001
  severity: S1
  category: AT-1
  clause: C-010, C-011, C-014
  claim: Explicit three-part DESCRIBE names take default-namespace recovery and answer metadata rows for tables that do not exist, while SELECT on the same name is table-not-found.
  evidence: R0 `DESCRIBE mt.otherns.snapshots` and `DESCRIBE mt.t.snapshots` answer six rows after USE mt.ns; `SELECT * FROM mt.otherns.snapshots` raises table-not-found; facade expansion of the two-part form is byte-identical to the explicit form so no engine gate separates them
  disposition: REMEDIATED as option (a) (default-namespace recovery deleted; the two-part and explicit three-part forms take the plain path; pinned by test_two_part_after_use_is_plain_not_found plus the three red specs, green unchanged; D-1 recorded above)
```

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-V-002
  severity: S2
  category: AT-3
  clause: C-012
  claim: A missing namespace on a four-part DESCRIBE leaked the provider error `failed to resolve schema` instead of 42P01 naming the full metadata-table name as written.
  evidence: R0 `DESCRIBE mt.nosuchns.t.snapshots` and the EXTENDED spelling both raised `failed to resolve schema: nosuchns`; post-fix both raise the full TABLE_OR_VIEW_NOT_FOUND message naming `mt`.`nosuchns`.`t`.`snapshots`
  disposition: REMEDIATED (three-way base-load match in dollar_metadata_table naming the full four-part name via table_or_view_not_found_parts; pinned by test_missing_namespace_maps_to_not_found_naming_full_name and the C-015 matrix)
```

## PROPOSITION LEDGER — IPI-23-MT-DESCRIBE-1 critic r1 — 2026-09-23

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-010 | After `USE mt.ns` with table `mt.ns.otherns` and namespace `mt.otherns` present and no table `mt.otherns.snapshots`, `DESCRIBE mt.otherns.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND message naming `` `mt`.`otherns`.`snapshots` ``. | `test_explicit_three_part_with_real_namespace_falls_through` green. | **PROVEN** | Recovery deleted; the plain path answers. pins: ipi-23-mt-describe-1/C-010 |
| C-011 | After `USE mt.ns` with table `mt.ns.t`, `DESCRIBE mt.t.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND / SQLSTATE 42P01 message naming `` `mt`.`t`.`snapshots` ``, never rows. | `test_explicit_three_part_missing_namespace_names_full_name` green. | **PROVEN** | md-r6fix: the plain path maps the missing namespace to 42P01 (D-1 name shape). pins: ipi-23-mt-describe-1/C-011 |
| C-012 | `DESCRIBE mt.nosuchns.t.snapshots` and `DESCRIBE EXTENDED mt.nosuchns.t.snapshots` raise 42P01 naming the full `` `mt`.`nosuchns`.`t`.`snapshots` ``. | `test_missing_namespace_maps_to_not_found_naming_full_name` green. | **PROVEN** | Three-way base-load match over the full four-part name. pins: ipi-23-mt-describe-1/C-012 |
| C-013 | `DESCRIBE mt.ns.`t$snapshots`` answers the six snapshots rows. | `test_quoted_dollar_name_answers_metadata_rows` green. | **PROVEN** | Same provider as SELECT by construction. pins: ipi-23-mt-describe-1/C-013 |
| C-014 | After `USE mt.ns` with namespace `mt.t` present, `DESCRIBE mt.t.snapshots` raises the full TABLE_OR_VIEW_NOT_FOUND message naming `` `mt`.`t`.`snapshots` ``. | `test_explicit_three_part_namespace_shadowing_base_falls_through` green. | **PROVEN** | Recovery deleted; the plain path answers. pins: ipi-23-mt-describe-1/C-014 |
| C-015 | EXTENDED/FORMATTED of base-missing, unknown-suffix, upper-case-suffix, and namespace-missing four-part names match their plain answers. | `test_four_part_extended_formatted_matrix` green. | **PROVEN** | All five matrix rows measured and pinned; missing names are full four-part. pins: ipi-23-mt-describe-1/C-015 |
| C-016 | After `USE mt.ns`, `DESCRIBE ns.t.snapshots` answers the six snapshots rows. | `test_namespaced_table_meta_after_use_answers_rows` strict xfail. | **OPEN** | Strict xfail (`1 xfailed` in the gate): RePark fallthrough errors `table 'ns.t.snapshots' not found`; serving it needs default-catalog recovery, out of scope. pins: ipi-23-mt-describe-1/C-016 |
| C-017 | With a real table at the written path (`mt.ns.t.snapshots` under nested namespace `mt.ns.t` beside Iceberg table `mt.ns.t`), `DESCRIBE mt.ns.t.snapshots` is not claimed by the metadata intercept and answers what plain resolution answers (the compound-identifier error). | `describe_show::metadata_table::tests::describe_metadata_table_real_table_at_written_path_wins` green. | **PROVEN** | md-r6fix real-table-wins probe over the written parts, shared with SELECT's rule; Spark would describe the real table (D-4). pins: ipi-23-mt-describe-1/C-017 |

## Critic r4 (2026-09-23) — V-001/V-002/V-003 remediated (md-r6fix)

Critic r4 at `a0321e45` returned NEEDS_REMEDIATION on three findings; md-r6fix
closes all three.

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-R4-V-001
  severity: S1
  category: AT-1
  clause: C-004, C-011
  claim: After `USE mt.ns`, `DESCRIBE t.snapshots` and `DESCRIBE mt.t.snapshots` answered `NamespaceNotFound => ...` instead of Spark's TABLE_OR_VIEW_NOT_FOUND / SQLSTATE 42P01.
  evidence: both pins re-raised `NamespaceNotFound => No such namespace: NamespaceIdent(["t"])` at `a0321e45` against the re-pinned 42P01 expectation before the Rust change (red-first)
  disposition: REMEDIATED (execute_describe_table maps ErrorKind::NamespaceNotFound to the same table_or_view_not_found answer as TableNotFound; C-004/C-011 pin class, 42P01 and the full message naming the facade-expanded name — the name shape is divergence D-1)
```

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-R4-V-002
  severity: S1
  category: AT-2
  clause: C-017
  claim: The four-part DESCRIBE intercept claimed names without the real-table-wins rule SELECT applies, so a real table at the written path (nested namespace) answered metadata rows.
  evidence: the collision test answered six metadata rows at `a0321e45` before the probe existed (red-first); `DESCRIBE mt.ns.sub.plain` measured the plain four-part answer `Unsupported compound identifier … got 4`
  disposition: REMEDIATED (dollar_metadata_table probes the written parts via the shared table_exists_parts before claiming the name; a real table keeps the ordinary compound-identifier refusal — same answer plain resolution gives, per the ruling's fallback clause since plain DESCRIBE cannot describe any nested-namespace table)
```

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-R4-V-003
  severity: S3
  category: AT-6
  clause: C-016
  claim: The test module docstring claimed the two-part USE form answers rows, and the C-016 ledger row's evidence implied a green pin.
  evidence: docstring lines 3-6 grouped the two-part form with the served forms; C-016's proof obligation read "green" while the gate shows `1 xfailed`
  disposition: REMEDIATED (docstring now states the two-part USE form is a TABLE_OR_VIEW_NOT_FOUND refusal; C-016 proof obligation and evidence read strict xfail with `1 xfailed` in the gate; the C-011 pin name and the sweep/divergence prose were trued to the measured answers)
```

## Critic r5 (2026-09-23) — V-001/V-002 remediated (md-r7fix)

Critic r5 at `c49ba03c` returned NEEDS_REMEDIATION on two findings; md-r7fix
closes both.

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-R5-V-001
  severity: S1
  category: AT-3
  clause: C-007, C-018, C-019
  claim: The missing-base 42P01 answer named the canonicalized suffix — `DESCRIBE mt.ns.missing.SNAPSHOTS` printed `missing`.`snapshots` — where Spark's TABLE_OR_VIEW_NOT_FOUND quotes the identifier as written, and the quoted `t$snapshots` form was split into a four-part name.
  evidence: both new pins re-raised the wrong name at `c49ba03c` before the Rust change (red-first): `missing`.`snapshots` for the upper-case suffix, `missing`.`snapshots` for `missing$snapshots`; Spark 4.1.2 measured 2026-09-23 names `missing`.`SNAPSHOTS`, `Missing`.`snapshots` and `missing$snapshots` as written; re-introducing the canonical `suffix` re-reds the pins (mutation check)
  disposition: REMEDIATED (dollar_metadata_table builds the error from describe.written_parts — the parsed catalog.namespace.t$suffix name when written_parts is empty — via table_or_view_not_found_parts; C-018 pins both written-case spellings, C-019 the quoted `$` name)
```

```yaml
FINDING:
  id: F-IPI-23-MT-DESCRIBE-1-R5-V-002
  severity: S2
  category: AT-6
  clause: C-004
  claim: The Scope paragraph described a registry-defaults fallback for the facade-expanded two-part form that r3fix deleted, and counted "eight offline pins".
  evidence: no `unqualified_metadata_table` exists in the tree and C-004 pins the refusal; `pytest --collect-only -q` on the test file counts 23 items at the remediated head — 22 offline pins plus the live leg — not eight
  disposition: REMEDIATED (Scope paragraph rewritten to the code as it is; the offline pin count states the counted 22)
```

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-018 | `DESCRIBE mt.ns.missing.SNAPSHOTS` and `DESCRIBE mt.ns.Missing.snapshots` raise the full TABLE_OR_VIEW_NOT_FOUND / SQLSTATE 42P01 message naming the parts as written — `` `mt`.`ns`.`missing`.`SNAPSHOTS` `` and `` `mt`.`ns`.`Missing`.`snapshots` ``. | `test_missing_base_not_found_names_written_case` green. | **PROVEN** | Spark 4.1.2 names the written parts (measured 2026-09-23); red at `c49ba03c` (canonicalized suffix), green after md-r7fix. pins: ipi-23-mt-describe-1/C-018 |
| C-019 | `DESCRIBE mt.ns.`missing$snapshots`` raises the full TABLE_OR_VIEW_NOT_FOUND / SQLSTATE 42P01 message naming the written three-part name `` `mt`.`ns`.`missing$snapshots` ``. | `test_quoted_dollar_missing_base_names_written_name` green. | **PROVEN** | Spark names the `$` name as written (same measurement); red at `c49ba03c` (split `missing`.`snapshots`), green after md-r7fix. pins: ipi-23-mt-describe-1/C-019 |

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
| critic r3fix tree-gate (same four files) | `CB=0 R=0 T=0 U=0 L=0`: rust 62 passed; offline 69 passed, 2 skipped, 1 xfailed; live 71 passed, 1 xfailed |
| critic r3fix `cargo clippy` workspace + panic-ban (both invocations) + `./scripts/check_rust_file_size.sh` + `check_lib_rs.py` + `check_map_md.sh --base origin/main` + ruff on the test file + `check_ledger_grammar.py` + `comment_ban.py` | all exit 0 |
| md-r7fix local-gate (same four files) at the r7fix head | `CB=0 R=0 T=0 U=0 L=0`: rust 63 passed; offline 71 passed, 2 skipped, 1 xfailed; live 73 passed, 1 xfailed |
| md-r7fix `make rust-clippy` + `make rust-panic-ban` + `make check-rust-file-size` + `check_lib_rs.py` + `check_map_md.sh --base origin/main` + `check_ledger_grammar.py` + `comment_ban.py` + ruff format/check on the test file | all exit 0 |

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
      evidence: Missing base maps to TABLE_OR_VIEW_NOT_FOUND/42P01 naming the full metadata-table name as written with no `$` leak (C-007, identifier case kept per C-018; a quoted `$` name keeps the `$` per C-019); unknown suffixes keep the compound-identifier plan error byte for byte (C-008); any other provider error passes through unchanged by construction of the match.
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
      evidence: The missing-base error names the full metadata-table name as written without the `$` suffix form, so a typo is diagnosable from the error; rejected shapes keep today's loud errors.
      artifacts: [crates/repark-spark/src/describe_show/metadata_table.rs, python/repark/tests/test_ice_mt_describe_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: The recorded R-MT-DESCRIBE cell replays verbatim in the facade test and the gate's live leg; every clause carries a pins citation in the test file or the crate map; C-001 asserts Arrow field types and nullability alongside values.
      artifacts: [python/repark/tests/test_ice_mt_describe_1.py, python/repark/tests/map.md, crates/repark-spark/src/describe_show/map.md]
```

Every md-r1 clause above is PROVEN against the recorded R-MT-DESCRIBE cell and
the main-branch behaviors pinned beside it; only C-016 stays OPEN
(the strict xfail on `ns.t.snapshots` after USE, needing default-catalog
recovery that is out of scope), and md-r7fix adds the PROVEN C-018/C-019
written-name pins. Touched files per the Scope
paragraph; the fork, `Cargo.toml`/`Cargo.lock`, `STATUS.md`, the ANSI door,
`time_travel.rs`, `wap.rs` and the metadata rewrite are untouched. `make verify`
and the whole-workspace suites were not run per the work order's gate list; the
gates table above is the proof.
