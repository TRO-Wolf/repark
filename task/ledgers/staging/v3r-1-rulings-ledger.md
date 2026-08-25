# V3R-1 — the 2026-08-25 rulings

**Date:** 2026-08-25 · **Branch:** `feat/v3r-1-rulings` · **Base:** `b57d424` (`origin/main`, #240) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3 / §5 ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, procedural context break) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (the Iceberg DML write path)

Five owner rulings, all dated 2026-08-25, recorded where the gate reads them — and the one
that is engine code built and pinned. (1) **Guard** copy-on-write DML on format-v3 tables
(registry `V3-COW-1`). (2) The S3 Tables live legs are **in** v1.0 (OD-3b): the scoped IAM
statement is documented, never executed here. (3) Shredded-Parquet `variant` is **DECLARED**
out of the gate. (4) `geometry` / `geography` are **DECLARED** out of the gate (registry
`V3-GEO-1`). (5) The v2 → v3 in-place upgrade is **built behind the create opt-in, after
V3-3** — a matrix row, no code. Do not implement DV writes. Do not touch IAM, `.github/`, or
`Cargo.toml [patch]`.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3R-1
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3R-1 (one PR unit)
  charter_trace: C-001..C-013
  preconditions:
    - origin/main at b57d424 (#240 DL-4): SATISFIED
    - pickup commit 2fff33a on feat/v3r-1-rulings (DL-4 archived, gate clean): SATISFIED
    - the five owner rulings, in the owner's words, 2026-08-25: SATISFIED
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 5 — the DML write path is
      data-integrity; fails size — more than 5 files → STANDARD → ccc)
  success_condition: every clause below PROVEN at unit scope except C-013 (departure)
  step_risks:
    - the guard misses a DML path (a v3 COW write still commits): HANDLED — pinned per
      door per verb per form (plain-WHERE and subquery-WHERE), plus a v2 control
    - the guard over-reaches into v2 or into INSERT: HANDLED — v2 control pins; the guard
      sits only in the DELETE/UPDATE/MERGE arms
    - a DECLARED row without a pin (registry §6): HANDLED — V3-GEO-1 lands with its pin;
      shredded variant is queued, not rowed
    - IAM executed by the unit: HANDLED — documentation only; the owner executes
  tripwire_scan: CLEAN
  uncertainty: whether `DeleteObject` on S3 Tables storage is authorized by
    `s3tables:PutTableData` — named as unverified in both docs; measured by a later unit
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit. Rubric: STANDARD (criterion 5 fails — the DML write path; size fails — 20+
files). `critic_engine: ccc`. `claims_critic=true`. Native DataFrame is N/A (C-012).

## Scope / out of scope

| In | Out |
|---|---|
| `V3-COW-1` guard: both seats (write-mode resolver; passthrough valve on both doors) | DV writes (V3-3 / fork F-13) |
| Refusal pins: Spark + ANSI + facade, per verb, plain-`WHERE` and subquery-`WHERE`; MoR still refuses; v2 control | Lineage through rewrites (V3-4 / V3-5 / fork F-7) |
| Registry: `V3-COW-1` rewritten (BACKLOG, refusal); `V3-GEO-1` DECLARED; `V3-VARIANT-SHRED-1` queued | Any `variant` / `geometry` implementation (V3-6) |
| North-star §3 rows (COW, types, upgrade), §5 OD-3b, §6 | The upgrade build itself (after V3-3) |
| `docs/tier2-aws.md` §2: the scoped S3 Tables statement | Executing IAM; `.github/`; `[patch]` |
| Type-refusal pins (`GEOMETRY` / `GEOGRAPHY` / `VARIANT` at CREATE), both doors + facade | V3E-4 / V3E-5 |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`, no IAM.

## Entry-point matrix

| Surface | Spark SQL | ANSI SQL | Facade `.sql()` | Native DataFrame |
|---|---|---|---|---|
| COW `DELETE` on v3 refuses (plain `WHERE`, passthrough seat) | C-001 | C-001 | C-006 | N/A (C-012) |
| COW `DELETE` / `UPDATE` on v3 refuses (subquery `WHERE`, resolver seat) | — (the router valve fires first on this door) | C-001, C-002 | — | N/A |
| COW `UPDATE` on v3 refuses (plain `WHERE`) | C-002 | C-002 | C-006 | N/A |
| COW `MERGE` on v3 refuses (unset + explicit mode, resolver seat) | C-003 | C-003 | C-006 | N/A |
| MoR DML on v3 still refuses | C-004 (`MERGE`, plain `DELETE`) | C-004 (`MERGE`) | — | N/A |
| v2 COW `DELETE` still commits (control) | C-005 | C-005 | — (existing v2 facade DML suite) | N/A |
| `GEOMETRY` / `GEOGRAPHY` / `VARIANT` column refuses at CREATE | C-008, C-009 | C-008, C-009 | C-008, C-009 | N/A |

## PROPOSITION LEDGER — V3R-1 — 2026-08-25

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Copy-on-write `DELETE` on a format-v3 table refuses on both SQL doors before any write, naming `V3-COW-1`, row lineage and the verb; snapshot, live rows and lineage counters unchanged — plain-`WHERE` form (passthrough seat) on both doors, subquery-`WHERE` form (resolver seat) on the ANSI door | Spark + ANSI pins | **PROVEN** | `adopted_v3_cow_delete_refuses_rather_than_reassign_row_lineage` (both doors); ANSI `adopted_v3_cow_subquery_where_dml_refuses_at_the_resolver_seat` |
| C-002 | The same for copy-on-write `UPDATE` | Spark + ANSI pins | **PROVEN** | `adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage` (both doors); the ANSI subquery-`WHERE` pin |
| C-003 | Copy-on-write `MERGE INTO` on a v3 table refuses with `write.merge.mode` unset **and** explicitly `copy-on-write`, same shape, table untouched | Spark + ANSI pins | **PROVEN** | `adopted_v3_cow_merge_refuses_with_unset_and_explicit_mode` (both doors) |
| C-004 | Merge-on-read DML on a v3 table still refuses (R113): `MERGE` on both doors, plain-`WHERE` `DELETE` on the Spark door — with C-001..C-003, a v3 table is append-only | Spark + ANSI pins | **PROVEN** | `adopted_v3_mor_merge_still_refuses` (both doors); `adopted_v3_mor_delete_still_refuses` (Spark) |
| C-005 | A v2 copy-on-write `DELETE` still commits and drops the matched row — the guard reaches nothing below v3 | Spark + ANSI controls | **PROVEN** | `v2_cow_delete_still_commits_control` (both doors); the whole existing v2 DML suite stays green (`make test`) |
| C-006 | Facade Spark `.sql()` COW `MERGE`, `DELETE`, `UPDATE` on an adopted v3 table raise `UnsupportedOperationException` naming `V3-COW-1`; rows untouched | `python/repark/tests/test_v3_cow_dml.py` | **PROVEN** | `test_facade_adopted_v3_cow_dml_refuses_and_leaves_the_table_untouched` (needs `make develop`) |
| C-007 | Registry `V3-COW-1` is rewritten as a refusal row — still BACKLOG, dated 2026-08-25, pins named, V3E-1's numbers kept as the pre-guard record; the north-star COW row says 🚫 and names the ruling | registry + north-star edit; tree pin | **PROVEN** | `docs/spark-sql-iceberg-parity.md` §7 `V3-COW-1`; north star §3; `test_v3r_1_rulings.py::test_v3_cow_1_is_a_refusal_row_dated_by_the_ruling` |
| C-008 | `CREATE TABLE … (v GEOMETRY)` / `GEOGRAPHY` refuses on both SQL doors and the facade, naming the type, leaving no table; registry `V3-GEO-1` DECLARED, dated 2026-08-25, pinned | pins on three doors + registry §4 row | **PROVEN** | `v3_type_columns_geometry_geography_variant_refuse_naming_the_type` (Spark `create_table.rs`, ANSI `v3_types.rs`); facade `test_v3_geometry_geography_variant_columns_refuse_naming_the_type`; `test_v3_geo_1_is_declared_and_shredded_variant_is_queued_not_rowed` |
| C-009 | Shredded-Parquet `variant` is DECLARED out of the v1.0 gate in the north star (§3 types row, §6) and queued in the registry as `V3-VARIANT-SHRED-1` — not a row until V3-6 gives it a pin; `VARIANT` at CREATE refuses today and is pinned so V3-6's landing reds it | north-star + registry queue edits; the CREATE pin; tree pin | **PROVEN** | same three CREATE pins; `test_v3_geo_1_is_declared_and_shredded_variant_is_queued_not_rowed`; `test_north_star_matrix_carries_the_three_engine_rulings` |
| C-010 | The north-star upgrade row reads "build it, behind `repark.sql.allowCreateFormatVersion3`, after V3-3" (dated); `ALTER … SET TBLPROPERTIES ('format-version' = '3')` still refuses (V3-2 C-008 pin unchanged) | north-star edit; tree pin; existing pin | **PROVEN** | north star §3; `test_north_star_matrix_carries_the_three_engine_rulings`; `or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in` (untouched, green) |
| C-011 | OD-3b is ruled **in** (north star §5) and `docs/tier2-aws.md` §2 carries the scoped `s3tables` statement — table ARN + `s3tables:namespace` condition, no `DeleteTable`, the unverified `DeleteObject` authorization named; no IAM executed by the unit | docs edits; tree pin | **PROVEN** | `test_od_3b_is_ruled_in_and_the_runbook_carries_the_scoped_statement`; `git diff --stat` touches no `.github/`, no credentials |
| C-012 | Native `DataFrame` is N/A — no Iceberg DML write surface | surface matrix | **PROVEN** | rustdoc on the Spark leaf cites C-012 |
| C-013 | Pickup archives DL-4 (and drops the stale G11/G15 open-ruling claim); departure trues STATUS's v3 workstream, empties V3R-1 from the slate (V3E-4 stays #1), no obituary | standing rule 7; gate | **PROVEN** | Pickup `2fff33a`; the departure commit; `make check-docs-compaction`; `test_the_unit_leaves_no_obituary` |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 13/13.

```yaml
KILLED_ASSUMPTIONS:
  - "Guarding the write-mode resolvers covers every copy-on-write arm": REMOVED by the
    Actor's own pins — the plain-WHERE DELETE/UPDATE never reach predicate_dml; both
    doors delegate them to DataFusion → the fork's TableProvider. Second seat added
    (the passthrough valve beside the BUG-001 valve), and the ANSI subquery-WHERE form
    pins the first seat on its own.
  - "sqlparser parses GEOMETRY / GEOGRAPHY / VARIANT as distinct types": IRRELEVANT —
    all three fall through both doors' type mappings and refuse naming the type.
  - "A merge-on-read plain DELETE on v3 might write position deletes into a v3 table on
    the passthrough path": REMOVED — measured refusing (adopted_v3_mor_delete_still_refuses).
RISK_HEATMAP:
  - A v3 COW write that still commits (silently wrong lineage): MITIGATED — per door,
    per verb, per form pins; v2 control
  - The guard reaching INSERT (append is correct and must stay open): MITIGATED — the
    guard lives only in the DELETE/UPDATE/MERGE arms; the seed INSERT in every pin
    proves append still assigns lineage (next_row_id = 3)
  - Two metadata loads per plain-WHERE DELETE/UPDATE (BUG-001 valve + this valve): ACCEPTED
    — one catalog load, refusal-path only; folding would couple two unrelated hazards
CLARIFYING_QUESTIONS: []
```

## Execution record

- Pickup `2fff33a`: `make ledger-archive` filed DL-4 (`task/ledgers/archive/2026-08/`),
  gate clean (STATUS 30,063 B / slate 4,923 B); STATUS's stale "G11/G15 (owner rulings)"
  open-claim removed (both closed 2026-08-12, #67 / #71); V3R-1 queued at #0.
- Guard: `crates/repark-iceberg/src/write/row_lineage_guard.rs` —
  `refuse_v3_cow_dml_that_would_reassign_row_lineage` (called from
  `predicate_dml::resolve_write_mode`'s COW arm and both COW arms of
  `merge::resolve_merge_mode`) and `refuse_v3_cow_dml` (async passthrough valve; merge-on-read
  passes, R113 owns it). Door seats: `repark-spark::normalize::refuse_v3_cow_dml` after the
  BUG-001 valve in `router::execute_delete` / `execute_update`;
  `repark-sql::guards::refuse_v3_cow_dml` in the delegated `DELETE | UPDATE` branch. Both
  doors' target resolution refactored into one `dml_target_ident` helper each (no behavior
  change; the BUG-001 valve now shares it).
- First run of the rewritten pins: 2 of 8 Spark-door and 2 of 5 ANSI-door pins RED —
  DELETE/UPDATE still committed. Root cause: the plain-`WHERE` form bypasses `predicate_dml`.
  Second seat added; all green. That RED is the evidence the pins are not hollow.
- Green: `cargo test -p repark-spark -p repark-sql --lib -- v3_cow create_table::v3_type
  v3_types` 16 passed; `make test` (workspace) green; facade
  `test_v3_cow_dml.py` + `test_v3_create_opt_in.py` 4 passed against `make develop`; parity
  `test_v3r_1_rulings.py` 5 passed.
