# V3R-1 — the 2026-08-25 rulings

**Date:** 2026-08-25 · **Branch:** `feat/v3r-1-rulings` · **Base:** `b57d424` (`origin/main`, #240) ·
**Intake:** [task/roadmap/epic-term/v1-0-iceberg-v3-northstar.md](../../../roadmap/epic-term/v1-0-iceberg-v3-northstar.md) §3 / §5 ·
**Sequence:** [briefs/next-sequence.md](../../../../briefs/next-sequence.md) ·
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
- Cycle-1 green: Spark 8 + ANSI 5 + 2 type pins; `make test` (workspace) green; facade
  `test_v3_cow_dml.py` + `test_v3_create_opt_in.py` 4 passed against `make develop`; parity
  `test_v3r_1_rulings.py` 5 passed. Build commit `292b723`.
- **CCC cycle 1 (procedural, scratch clone of `292b723`)** — three S1s, all in the guard's own
  failure class (a v3 copy-on-write write that commits and reassigns lineage): SEC-001
  short names under a session default catalog, SEC-002 a padded merge-on-read spelling,
  SEC-003 a dotted quoted name on the ANSI door. Records below.
- Cycle-2 remediation (the second code commit on the branch): the passthrough valve refuses **every** v3 table (the
  merge-on-read reason for merge-on-read tables, `V3-COW-1`'s for the rest); both doors'
  target resolution completes short names from `datafusion.catalog.default_catalog` /
  `default_schema`; the ANSI door resolves the target from the AST (`delete_target_name` /
  `object_name_of`) instead of the scrubbed-text scraper — the BUG-001 valve shares both
  helpers and is tightened the same way (over-refuse is its documented direction). Five
  regression pins (two Spark, three ANSI), each RED on the cycle-1 tree by construction of the
  probe and GREEN after. Cycle-2 green: Spark 11, ANSI 9 + `v3_types` 1 + `guards::tests` 2;
  `make test` workspace green; facade 4 passed against a rebuilt module; `make ci` clean except
  the attestation this section files.
- **CCC cycle 2 (fresh scratch clone of the cycle-2 tree)** — the three probes re-executed through
  the public SQL entry points refuse with lineage `3 / 0 / 3`; mutation M1 (guard function
  returns `Ok` unconditionally) reds 4 Spark + 6 ANSI copy-on-write pins and leaves every
  merge-on-read / v2 / type pin green; mutation M3 (both routers' valve calls stripped) reds
  exactly the passthrough-seat pins (4 Spark, 5 ANSI) and leaves the resolver-seat pins
  (`MERGE`, the ANSI subquery-`WHERE`) green. The pins are load-bearing per seat.
- **Process miss, owned:** the scratch clone built with `CARGO_TARGET_DIR` pointed at the
  live worktree's `target/`; cargo then served the clone's **mutated** `repark-sql` test binary
  to the live tree as fresh (5 live pins red until `cargo clean -p`). The live tree's source,
  `git status`, stash list and remotes were untouched. Rule from here: a scratch clone builds
  in its own target dir, never the live tree's.

## CCC pass — findings and attestation

Context break executed; attacking artifacts, not memory. Procedural (single session), so R3's
compensation applies: every claim in the silently-wrong-results class was re-executed through
the public SQL entry points with **novel** inputs absent from the committed tests at the time
(probes P1–P5 in the cycle-1 clone; P1–P3 re-executed in the cycle-2 clone), cited below with
observed-versus-expected. Risk tier high (the DML write path); Critic-1 crates contract applied
to `crates/repark-iceberg/src/write/row_lineage_guard.rs` and the two door wrappers.

```yaml
FINDING:
  id: SEC-001
  severity: S1
  category: AT-2
  clause: C-001, C-002
  disposition: REMEDIATED
  claim: with `SET datafusion.catalog.default_catalog = 'ice'` / `default_schema = 'sales'`, `DELETE FROM sales.adopt_p1 WHERE id = 2` and `DELETE FROM adopt_p1 WHERE id = 3` on a v3 table bypassed the valve (`< 3 parts → Ok`) on BOTH doors and committed — next_row_id 3 → 6, rows [(1,a)]; expected a refusal and 3 / 0 / 3
  evidence: probe P1 (cycle-1 clone, both doors); fix — `dml_target_ident` completes short names from the session defaults; pins `adopted_v3_cow_dml_with_default_catalog_short_names_refuses` (Spark + ANSI): red on the cycle-1 tree, green on the cycle-2 tree, red again under M1 and M3

FINDING:
  id: SEC-002
  severity: S1
  category: AT-8
  clause: C-001, C-004
  disposition: REMEDIATED
  claim: `'write.delete.mode' = ' Merge-On-Read '` (padded) on a v3 table — the valve's trim + case-fold read it as merge-on-read and stepped aside; the fork read it as copy-on-write and committed a rewrite: next_row_id 3 → 5, zero delete files; expected a refusal
  evidence: probe P3 (cycle-1 clone, Spark door); fix — the valve refuses every v3 table, branching only the message on the mode; pins `adopted_v3_padded_merge_on_read_spelling_still_refuses` (Spark + ANSI); the upstream-behaviour presumption (the fork's property parsing) is what AT-8 names

FINDING:
  id: SEC-003
  severity: S1
  category: AT-2
  clause: C-001
  disposition: REMEDIATED
  claim: ANSI door — `CREATE TABLE ice.sales."a.b" … WITH (format_version = 3)` succeeds, and `DELETE FROM ice.sales."a.b" WHERE id = 2` committed (3 → 5): the text scraper split the quoted name on `.`, the load failed, the valve passed; expected a refusal
  evidence: probe P2 (cycle-1 clone); fix — `dml_target_ident(cx, &Statement)` reads the target from the AST (quoted identifiers are one part); pin `adopted_v3_cow_delete_on_a_dotted_quoted_name_refuses`; the Spark door already used AST parts (`name_parts`) and was not affected

FINDING:
  id: Q-001
  severity: S2
  category: AT-10
  clause: C-001
  disposition: REMEDIATED
  claim: the ANSI `live_pairs` helper built `ice.sales.a.b` unquoted, so the SEC-003 pin panicked in the helper (v3_cow.rs:94) rather than exercising the guard — a pin that cannot reach its claim
  evidence: the leaf identifier is now double-quoted in the helper; the pin runs to its assertions and is red under M1 / M3

FINDING:
  id: Q-002
  severity: S3
  category: AT-8
  clause: C-001
  disposition: ACCEPTED_FLAGGED
  claim: `dml_target_ident` (name → catalog + ident with session-default completion) exists once per door — `repark-spark::normalize` and `repark-sql::guards` are sibling crates with no shared home below `repark-iceberg`, whose valve takes the resolved ident
  evidence: crate DAG (`make check-crate-dag`); the shared piece — the valve and its refusal — lives once in `row_lineage_guard.rs`; below the S1 floor, recorded for a later hoist if a third door appears

FINDING:
  id: Q-003
  severity: S3
  category: AT-7
  clause: C-001, C-002
  disposition: ACCEPTED_FLAGGED
  claim: a plain-`WHERE` DELETE / UPDATE now loads table metadata twice before delegation (the BUG-001 valve and this valve each load) — one extra catalog round-trip per statement, not system-breaking
  evidence: `refuse_mor_unpartitioned_multi_spec_dml` + `refuse_v3_cow_dml` both call `load_table`; folding them would couple two unrelated hazards behind one message; recorded, not blocking

FINDING:
  id: L-001
  severity: S3
  category: AT-1
  clause: C-004
  disposition: ACCEPTED_FLAGGED
  claim: for a v3 table whose `write.delete.mode` is unknown / bogus, the valve (and the resolver) refuse with the copy-on-write reason — Iceberg's default for an unrecognised mode is copy-on-write, so the reason is right, but the message does not say the value was unrecognised
  evidence: probe P5 (cycle-1 clone): `'write.delete.mode' = 'bogus'` → refused, lineage 3 / 0 / 3; the resolver's own unknown-mode refusal covers `MERGE`; the DELETE / UPDATE passthrough message names `V3-COW-1`, which is the operative fact

FINDING:
  id: CL-001
  severity: S3
  category: AT-10
  clause: C-007, C-013
  disposition: REMEDIATED
  claim: the cycle-1 ledger and the registry row described the valve as stepping aside for merge-on-read tables and cited "16 passed" for a combined run — both stale after cycle 2
  evidence: registry `V3-COW-1`, the three map rows and the execution record above rewritten to the cycle-2 behaviour with per-crate counts; identity across the branch is the repository's (`%ae` on every commit since the base equals the repository's configured author); zero co-author or session trailers on any commit; the diff touches no `.github/`, `Cargo.toml [patch]`, credentials, or home paths (`git diff b57d424..HEAD`)
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: v3r-1-rulings
  cycle: 2
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Actor, then CCC quad (claims_critic) on scratch clones of the cycle-1 and cycle-2 trees
    (cycle 2). Cycle-1 S1s SEC-001/002/003 remediated with regression pins that are red
    under mutation and green on the tree. Fresh execution (R3): probes P1-P5, novel inputs
    through the public SQL entry points, observed vs expected recorded in the findings.
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs, python/repark/tests/test_v3_cow_dml.py, python/repark-parity/tests/test_v3r_1_rulings.py]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs, crates/repark-spark/src/tests/create_table.rs, crates/repark-sql/src/v3_types.rs]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs, crates/repark-spark/src/tests/v3_cow.rs]
    - id: AT-4
      status: N/A
      justification: the guard runs at write-mode resolution before any file is written and commits nothing; no shared state, no ordering with the OCC loop it precedes
    - id: AT-5
      status: ATTACKED
      artifacts: [crates/repark-sql/src/guards.rs, crates/repark-spark/src/normalize.rs]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs]
    - id: AT-7
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs, crates/repark-spark/src/normalize.rs, crates/repark-sql/src/guards.rs]
    - id: AT-9
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/row_lineage_guard.rs]
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/v3_cow.rs, crates/repark-sql/src/v3_cow.rs, crates/repark-sql/src/guards/tests.rs]
```

**Attack notes per category.** AT-1: every clause walked against the tree — C-001..C-006 by
pin, C-007..C-011 by tree pin and the registry / north-star / runbook diff, C-012 by the
rustdoc, C-013 by this departure. AT-2: novel inputs P1 (short names under a default
catalog), P2 (dotted quoted name), P3 (padded mode spelling), P5 (bogus mode) — three found
holes, remediated. AT-3: a refusal is a clean error before any write (the seed rows,
snapshot and lineage counters are asserted unchanged by every pin); a missing table passes
to the planner's own error. AT-5: the target resolution reads the AST — a quoted identifier
cannot smuggle a second name part; the session defaults are the engine's own config, not user
text. AT-6: the guard is the data-integrity control itself; the v2 control pins the guard
does not reach v2, the seed INSERT in every pin proves append still assigns lineage. AT-7:
one extra metadata load (Q-003), not system-breaking. AT-8: the fork's property parsing is no
longer presumed (SEC-002); `MorDmlKind::verb` / `mode_property` contracts honoured; no
`unwrap` / `expect` / panic in production code (`make rust-panic-ban`). AT-9: every refusal
names the row, the verb, the table and the reason; nothing fails silently. AT-10: mutation M1
/ M3 evidence above; the `guards::tests` valve test now feeds a parsed `Statement`.

**Convergence: `CCC-CONVERGED`** — every required Critic phase has artifacts; no open finding
at or above S1; the three S1s carry regression proof; `make verify` and `make preflight` green
at departure (execution record); Critic-4 attestation above.

