# V3-2 — CREATE/CTAS format-version=3 behind an explicit session opt-in

**Date:** 2026-08-24 · **Branch:** `feat/v3-2-create-v3-opt-in` · **Base:** `fb91233` (`origin/main`) ·
**Design:** [docs/design/format-v3-track.md](../../../../docs/design/format-v3-track.md) §5 ·
**Sequence:** [briefs/next-sequence.md](../../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, `/sepmo-core`) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1

Lift the CREATE/CTAS `format-version = 3` / `format_version = 3` refusal behind an explicit
session opt-in. The default stays v2 until V3-3, because a v3 table this engine cannot do
row-level writes on is a trap. SQL must still request v3; the knob alone does not change
the create default. ALTER stays refused. Native DataFrame has no format-version create
surface (matrix N/A).

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3-2
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter V3-2 (one PR unit)
  charter_trace: C-001..C-015
  preconditions:
    - origin/main at fb91233 (engineering-method + SEPMO v2.3): SATISFIED
    - pickup commit d60f598 on feat/v3-2-create-v3-opt-in: SATISFIED
    - MW closed (#230) and RP-1 landed (#228): SATISFIED
    - user confirmation: SATISFIED (session: "lets get that unit started" + /sepmo-core)
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 5 — Iceberg create / format version
      is data-integrity-relevant; fails criterion 1 — two doors + session conf public
      surface → STANDARD → ccc, not LIGHT)
  success_condition: every clause below PROVEN at unit scope; make verify green
  step_risks:
    - accidental default-v3 create: HANDLED (knob default false AND SQL must request 3)
    - ANSI product edge onto repark-functions: HANDLED (C-013 — entries() like SEC-02)
    - V3-LINEAGE-1 blast-radius claim going stale: HANDLED (C-011 + C-014)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit. Rubric: STANDARD (criterion 5 fails — create path / format metadata;
criterion 1 fails — public session conf + both SQL doors). `critic_engine: ccc`.
`claims_critic=true`. Native DataFrame is N/A (C-012).

## Scope / out of scope

| In | Out |
|---|---|
| CREATE + CTAS format v3 behind session opt-in, both SQL doors + facade | V3-3 DV / MoR writes |
| Default create stays v2 | Lifting V3-LINEAGE-1 |
| ALTER format-version=3 still refused | `_row_id` read surface (V3-4) |
| V3-LINEAGE-1 still refuses rewrite on engine-created v3 | MW-9 / F-16 |
| | AWS / IAM / `.github/` / `[patch]` |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`.

## PROPOSITION LEDGER — V3-2 — 2026-08-24

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Session knob `repark.sql.allowCreateFormatVersion3` (underscore alt accepted at builder parse) defaults **false**; present-but-unparsable values fail loud naming the canonical key | Parse + default pins | PROVEN | `cardinality.rs::resolve_create_format_version_v3_needs_opt_in` (includes `notabool`); `config_map_camel_and_snake` |
| C-002 | Unspecified format version creates Iceberg **v2**, including when the opt-in is true — the knob does not change the create default | CREATE/CTAS with no version property, opt-in on, metadata is V2 | PROVEN | Spark `create_table`/`ctas` unspecified-with-opt-in pins; ANSI `with_format_version` companion |
| C-003 | Requested `'2'` creates format v2 (Spark `format-version` + ANSI `format_version`; CREATE + CTAS) | Metadata `FormatVersion::V2`; reserved key not stored as a table property | PROVEN | existing `ctas_format_version_two_consumed_others_rejected`; `with_format_version_sets_the_table_format_version`; resolve helper |
| C-004 | Requested `'3'` **without** the opt-in refuses, names the conf key, and creates no table — Spark CREATE, Spark CTAS, ANSI CREATE, ANSI CTAS | Four refuse pins; table_exists false | PROVEN | `the_engine_still_cannot_produce_a_v3_table`; ANSI/Spark v3-without-opt-in twins |
| C-005 | Requested `'3'` **with** opt-in on the Spark door creates a table whose metadata format version is V3 (column-def CREATE **and** CTAS) | `FormatVersion::V3` on both statements | PROVEN | `create_table`/`ctas` opt-in pins |
| C-006 | Requested `format_version = 3` **with** opt-in on the ANSI door creates V3 (column-def CREATE **and** CTAS, including OR REPLACE of an existing v2) | `FormatVersion::V3` on both statements | PROVEN | `v3_create.rs::format_version_three_opt_in_creates_v3`; `or_replace_applies_requested_v3` |
| C-007 | Requested `'1'` still refuses on both doors (CREATE + CTAS); nothing is created | Message names the property; table_exists false | PROVEN | existing v1 pins (`create_table` C6-F2, `ctas_format_version_two_consumed_others_rejected`, ANSI `with_format_version_sets_the_table_format_version`) |
| C-008 | `ALTER TABLE … SET TBLPROPERTIES/PROPERTIES (format-version/format_version = 3)` still refuses; an existing v2 table stays v2 — including when the create opt-in is on | Spark ALTER + ANSI ALTER pins | PROVEN | `the_engine_still_cannot_produce_a_v3_table` ALTER arms; `or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in`; `alter::tests::reserved_and_unchangeable_keys_refuse_loud` |
| C-009 | The opt-in is session-scoped builder conf (`repark.sql.*` ConfigExtension), not an env read at query time | No `std::env` on the new path; tests drive the extension / builder map | PROVEN | `SparkExtension::configure` already installs `ReparkSqlConfig`; ANSI reads `ConfigOptions::entries()` |
| C-010 | Facade Spark `.sql()`: default session refuses `format-version=3`; a session with the conf creates v3 | UnsupportedOperationException without conf; Arrow collect + rewrite refuse naming row lineage with conf | PROVEN | `python/repark/tests/test_v3_create_opt_in.py` |
| C-011 | `rewrite_data_files` still refuses an **engine-created** v3 table and names row lineage (V3-LINEAGE-1 is not lifted) | CALL after opt-in CREATE | PROVEN | `call_v3.rs` opt-in create + rewrite pin; facade C-010 companion |
| C-012 | Native DataFrame API has no format-version create surface (matrix N/A) | No DataFrame create-table API takes a format version | PROVEN | surface matrix: Spark + ANSI rows only; no native DF row added |
| C-013 | The ANSI door does **not** take a product `repark-functions` edge; it reads the same knob through `ConfigOptions::entries()` (SEC-02 pattern) | `check_crate_dag.py` has no `repark-sql` → `repark-functions` **normal** edge | PROVEN | `scripts/check_crate_dag.py`; `create_table.rs` entries() reader |
| C-014 | Registry row `V3-LINEAGE-1` blast-radius prose is updated: default create still cannot produce v3; opt-in CREATE/CTAS can; ALTER still cannot | Parity row + `call.rs` comment name the opt-in | PROVEN | `docs/spark-sql-iceberg-parity.md` V3-LINEAGE-1; `call.rs` refuse banner |
| C-015 | Merge-on-read `MERGE INTO` still refuses an engine-created v3 table and names the format version (copy-on-write MERGE/DELETE/UPDATE are the existing default and are not format-gated — V3-3) | CREATE with `write.merge.mode = merge-on-read` then MERGE | PROVEN | `opt_in_create_produces_v3_and_rewrite_still_refuses` MoR arm |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 15/15.

```yaml
KILLED_ASSUMPTIONS:
  - Opt-in alone creates v3 tables: REMOVED (SQL must still request 3; C-002)
  - ANSI product-depends on repark-functions to read the knob: REMOVED (C-013, SEC-02 entries())
  - Creating v3 lifts V3-LINEAGE-1: REMOVED (C-011)
  - ALTER SET format-version=3 is in this unit: REMOVED (C-008 stays refuse)
RISK_HEATMAP:
  - Spark users copy format-version=3 TBLPROPERTIES and get a table they cannot MERGE: MITIGATED (default refuse + message names the conf)
  - Native ReparkSession without SparkExtension never installs ReparkSqlConfig: MITIGATED fail-closed (absent extension reads as false); native Iceberg DDL from PyReparkSession.native is a recorded residual, not this unit
CLARIFYING_QUESTIONS: []
```

```yaml
ACTOR_BUILD_SUMMARY:
  pr_unit: V3-2
  charter_trace: C-001..C-015
  what_was_built: >
    Session knob repark.sql.allowCreateFormatVersion3 (default false). Spark CREATE/CTAS
    consume format-version and apply TableCreation.format_version at execute. ANSI stores
    format_version 2|3 at parse and resolves at execute via ConfigOptions::entries()
    (no product repark-functions edge). ALTER still refuses. V3-LINEAGE-1 still fires on
    opt-in CREATE. Facade pins rebuilt against maturin develop.
  success_conditions_met:
    - C-001: cardinality resolve + config_map pins
    - C-002: unspecified-with-opt-in stays v2 (Spark CREATE/CTAS, ANSI v3_create)
    - C-003: requested 2 stays v2
    - C-004: requested 3 without opt-in refuses both doors + facade
    - C-005: Spark CREATE + CTAS with opt-in are V3
    - C-006: ANSI CREATE + CTAS with opt-in are V3
    - C-007: v1 still refuses
    - C-008: ALTER still refuses
    - C-009: SparkExtension configure installs ReparkSqlConfig
    - C-010: python/repark/tests/test_v3_create_opt_in.py (2 passed after make develop)
    - C-011: opt_in_create_produces_v3_and_rewrite_still_refuses
    - C-012: matrix rows Spark + ANSI only
    - C-013: check_crate_dag has no sql→functions normal edge
    - C-014: V3-LINEAGE-1 prose + call.rs banner
    - C-015: MoR MERGE on engine-created v3 still refuses
  tests: make verify green (2026-08-24); facade file 2 passed after make develop
  out_of_scope_observed:
    - Copy-on-write MERGE/DELETE/UPDATE on engine-created v3 is currently allowed (MoR-only
      format-version guard). V3-3 owns DV/row-level writes; lineage-preserving COW on v3
      is not this unit.
    - Native PyReparkSession.native() does not install ReparkSqlConfig; opt-in is fail-closed.
  self_logic_reviews: [SLR-ACTOR-V3-2]
  status: CONCLUDED
```

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-ACTOR-V3-2
  agent: Actor
  action: conclude ACTOR_BUILD
  charter_trace: C-001..C-015
  preconditions:
    - make verify: SATISFIED (exit 0)
    - facade C-010: SATISFIED after make develop (2 passed)
  success_condition: every clause pinned; default create still v2
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## Entry-point matrix (C-004/C-005/C-006/C-010/C-012)

| Entry point | CREATE column-def | CTAS | ALTER SET format-version=3 |
|---|---|---|---|
| Native DataFrame | N/A (C-012) | N/A | N/A |
| ANSI SQL | C-004 refuse / C-006 opt-in V3 | C-004 / C-006 | C-008 refuse |
| Spark facade `.sql()` | C-004 / C-005 / C-010 | C-004 / C-005 / C-010 | C-008 refuse |

## CCC cycle 1 — findings and remediations

Critic-2 CLEAN. Critic-1/3/4 ≥S1 remediations below. Floor S1. `make verify` green after the fix half. Facade `test_v3_create_opt_in.py` 2 passed after `make develop`.

```yaml
FINDING:
  id: F-V3-2-1
  severity: S1
  category: AT-1
  clause: C-005, C-006
  disposition: REMEDIATED
  claim: CREATE OR REPLACE of existing v2 with requested 3 + opt-in stayed v2 (fork replace reads reserved property, not TableCreation.format_version)
  evidence: stamp_requested_format_version on replace only; pins or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in, ctas_format_version_three_needs_opt_in OR REPLACE arm, v3_create::or_replace_applies_requested_v3

FINDING:
  id: F-V3-2-2
  severity: S1
  category: AT-7
  clause: C-008
  disposition: REMEDIATED
  claim: Spark ALTER format-version=3 unpinned with opt-in on
  evidence: or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in ALTER arm

FINDING:
  id: F-V3-2-3
  severity: S1
  category: AT-7
  clause: C-001
  disposition: REMEDIATED
  claim: unparsable allowCreateFormatVersion3 had no pin
  evidence: resolve_create_format_version_v3_needs_opt_in notabool arm

FINDING:
  id: F-V3-2-4
  severity: S1
  category: AT-6
  clause: C-015
  disposition: REMEDIATED
  claim: error text said v3 cannot do row-level writes; COW MERGE is not format-gated
  evidence: resolve message now names merge-on-read; C-015 ledger text names COW as V3-3
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: V3-2
  cycle: 1
  risk_tier: high
  critic_engine: ccc
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-spark/src/create_table.rs, crates/repark-spark/src/ctas.rs, crates/repark-sql/src/create_table.rs, crates/repark-functions/src/cardinality.rs]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/create_table.rs, crates/repark-spark/src/tests/ctas.rs, crates/repark-sql/src/v3_create.rs]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-spark/src/create_table.rs, crates/repark-sql/src/create_table.rs]
    - id: AT-4
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/create_table.rs::or_replace_applies_requested_v3_and_alter_still_refuses_with_opt_in]
    - id: AT-5
      status: ATTACKED
      artifacts: [crates/repark-spark/src/extension.rs, crates/repark-sql/src/create_table.rs]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-functions/src/cardinality.rs, crates/repark-sql/src/create_table.rs]
    - id: AT-7
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/call_v3.rs, python/repark/tests/test_v3_create_opt_in.py]
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-spark/src/create_table.rs, crates/repark-sql/src/create_table.rs]
    - id: AT-9
      status: N/A
      justification: CREATE/CTAS format-version resolve is one config read per statement, not a per-row hot path
    - id: AT-10
      status: ATTACKED
      artifacts: [crates/repark-spark/src/call.rs, docs/spark-sql-iceberg-parity.md]
```

