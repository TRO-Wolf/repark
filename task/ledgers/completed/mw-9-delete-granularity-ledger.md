# MW-9 — honor `write.delete.granularity` (close MOR-2)

**Date:** 2026-08-24 · **Branch:** `feat/mw-9-delete-granularity` · **Base:** `70026af` (`origin/main`, #232) ·
**Intake:** [task/roadmap/mid-term/roadmap-intake-2026-08-23.md](../../roadmap/mid-term/roadmap-intake-2026-08-23.md) ·
**Sequence:** [briefs/next-sequence.md](../../../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD (`critic_engine: ccc`, `/sepmo-core`) · **claims_critic:** true ·
**max_cycles:** 2 · **severity_floor:** S1 · **risk_tier:** high (Iceberg write path)

Close registry `MOR-2`: the merge-on-read writer honors Iceberg `write.delete.granularity`
(`file` / `partition`). Unset matches Spark (`SparkWriteConf` default `file`), not
Iceberg-core's `PARTITION` default. Contents are unaffected.

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-MW-9
  agent: Orchestrator
  action: PRE_EXECUTION_REVIEW then ACTOR_BUILD for PR-carved charter MW-9 (one PR unit)
  charter_trace: C-001..C-011
  preconditions:
    - origin/main at 70026af (#232 V3-2): SATISFIED
    - pickup commit 5296dea on feat/mw-9-delete-granularity: SATISFIED
    - owner sequenced MW-9: SATISFIED (session: "Alright lets get the MW-9 going")
    - LIGHT/STANDARD rubric: SATISFIED (fails criterion 5 — Iceberg write/commit;
      fails criterion 1 — public table property + both SQL doors → STANDARD → ccc)
  success_condition: every clause below PROVEN at unit scope; make verify green
  step_risks:
    - default flip reds fixtures that encoded implicit partition counts: HANDLED (C-008)
    - two Java defaults disagree (core PARTITION vs Spark FILE): HANDLED (match Spark, C-001)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## PR carving

One PR unit. Rubric: STANDARD (criterion 5 fails — write path; criterion 1 fails —
public table property + both SQL doors + facade). `critic_engine: ccc`.
`claims_critic=true`. Native DataFrame is N/A (C-011).

## Scope / out of scope

| In | Out |
|---|---|
| Honor `write.delete.granularity` in `write_position_deletes` | V3-3 DV / MoR writes on v3 |
| Spark default `file` when unset | RDF-1 / F-16 / delete-laden rewrite |
| Explicit `file` and `partition`; unknown refuses | Equality deletes |
| MERGE through `write_position_deletes` | Fork `TableProvider` DELETE/UPDATE grouping (C-005) |
| Retarget MOR-2 pin; keep MW-7/MW-8 arithmetic via explicit `partition` | AWS / IAM / `.github/` / `[patch]` |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`.

## Entry-point matrix

| Entry point | unset = file | explicit partition | unknown refuse | MoR DELETE/UPDATE |
|---|---|---|---|---|
| Native DataFrame | N/A (C-011) | N/A | N/A | N/A |
| ANSI SQL | C-001 | C-003 | C-004 | C-005 Spark DELETE residual (ANSI MERGE is this writer) |
| Spark facade `.sql()` | C-001 / C-010 | (writer + Spark SQL cover C-003/C-004) | (Spark SQL) | C-005 Spark DELETE residual |

Writer unit tests cover grouping (C-001/C-002/C-003) independent of SQL.

## PROPOSITION LEDGER — MW-9 — 2026-08-24

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | Unset `write.delete.granularity` writes **file** granularity: one MERGE touching N live data files writes N position-delete files | Unpartitioned 6-file MERGE → 6 delete files; live row count unchanged | PROVEN | Spark `call_mor2_merge_writes_one_position_delete_per_data_file_by_default`; ANSI `unset_default_is_file_granularity`; writer `default_file_granularity_writes_one_delete_file_per_data_file`; facade `test_unset_granularity_writes_one_delete_file_per_data_file` |
| C-002 | Explicit `'file'` matches the unset default | Same 6-file MERGE with the property set → 6 | PROVEN | Spark `explicit_file_granularity_matches_the_unset_default`; writer `explicit_file_granularity_writes_one_delete_file_per_data_file` |
| C-003 | Explicit `'partition'` writes one delete file per `(spec, partition)` group | Same 6-file unpartitioned MERGE → 1 delete file | PROVEN | Spark `explicit_partition_granularity_writes_one_delete_file`; ANSI `explicit_partition_granularity_writes_one_delete_file`; writer `partition_granularity_writes_one_delete_file_for_an_unpartitioned_table` |
| C-004 | Unknown values refuse before any write, naming the property and the two legal values; the table is unchanged | `'banana'` MERGE errors (not `'files'`: that is a substring of `'file'`); zero delete files; rows intact | PROVEN | `parse_delete_granularity_spark_default_and_refuse` (`'file'`/`'partition'` quoted); Spark `unknown_delete_granularity_refuses_before_any_write`; ANSI `unknown_delete_granularity_refuses` |
| C-005 | SQL `DELETE`/`UPDATE` that delegate to the fork `TableProvider` are **not** this writer — the fork has no granularity knob (ENGINE_CONTRACT §7). MW-9 is the RePark-owned MERGE path (`write_position_deletes`) | Literal `DELETE`/`UPDATE` `… WHERE id IN (1..6)` on six data files writes **one** delete file (fork partition grouping) | PROVEN | Spark `fork_table_provider_delete_is_not_this_writer`; `fork_table_provider_update_is_not_this_writer` |
| C-006 | Contents are unaffected: live row set after the MoR write is the same at both granularities | 6-row table stays 6 rows (UPDATE/MERGE) or 0 (DELETE all) | PROVEN | C-001/C-003/C-005 live-row asserts |
| C-007 | The property is table-scoped and read at write time: ALTER SET then the next MERGE honors the new value | Unset CREATE, ALTER to `partition`, MERGE six files → 1 delete file | PROVEN | Spark `alter_set_granularity_is_honored_on_the_next_merge`; ANSI `alter_set_granularity_is_honored_on_the_next_merge` |
| C-008 | Fixtures that encoded implicit partition counts set `'partition'` explicitly | `mor_props()`, MW-7 `MOR_PROPERTIES`, MW-8 via that driver | PROVEN | `streaming_scan_tests.rs::mor_props`; `python/repark-parity/bench/mw7/measure.py` `MOR_PROPERTIES` |
| C-009 | Registry `MOR-2` is FIXED; the Spark-default pin is the named evidence | Registry rationale FIXED; pin name updated | PROVEN | `docs/spark-sql-iceberg-parity.md` MOR-2; `call.rs` banner |
| C-010 | Facade Spark `.sql()` default session writes file granularity | Arrow collect of `.files` content=1 is 6 | PROVEN | `python/repark/tests/test_mw9_delete_granularity.py` |
| C-011 | Native DataFrame API has no MoR DML write surface (matrix N/A) | No DataFrame MERGE/DELETE/UPDATE that writes position deletes | PROVEN | surface matrix: Spark + ANSI rows only |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 11/11.

```yaml
KILLED_ASSUMPTIONS:
  - Iceberg-core default PARTITION is what Spark writes: REMOVED (ENGINE_CONTRACT §7; SparkWriteConf is FILE; match Spark)
  - Unpartitioned file vs partition are the same: REMOVED (6 files → 6 vs 1)
  - seed_mor_delete_files (8 separate MERGEs) would change counts: REMOVED (one data file per commit either way)
RISK_HEATMAP:
  - Default flip reds MW-7/MW-8 arithmetic: MITIGATED (C-008 explicit partition on those fixtures)
  - Silently writing partition when Spark users expect file: MITIGATED (C-001 is the default)
CLARIFYING_QUESTIONS: []
```

```yaml
FINDING:
  id: Q-001
  severity: S1
  category: AT-7
  clause: C-002
  disposition: REMEDIATED
  claim: explicit 'file' was unpinned at the writer (Spark door only)
  evidence: writer explicit_file_granularity_writes_one_delete_file_per_data_file

FINDING:
  id: Q-002
  severity: S1
  category: AT-7
  clause: C-004
  disposition: REMEDIATED
  claim: refuse pin used 'files' / unquoted file — both are substrings of the property name
  evidence: banana + quoted 'file'/'partition' in parse unit test and both SQL doors

FINDING:
  id: Q-003
  severity: S1
  category: AT-7
  clause: C-010
  disposition: REMEDIATED
  claim: facade matrix claimed C-003/C-004 without facade pins
  evidence: entry-point matrix narrowed; facade pins C-001/C-010 only

FINDING:
  id: Q-004
  severity: S1
  category: AT-8
  clause: C-005
  disposition: REMEDIATED
  claim: sql map.md implied UPDATE honors granularity
  evidence: crates/repark-sql/src/map.md MERGE-only wording

FINDING:
  id: Q-005
  severity: S1
  category: AT-8
  clause: C-008
  disposition: REMEDIATED
  claim: MW-7/MW-8 maps still described implicit partition as the live default
  evidence: mw7/map.md, test_mw7_scale_smoke.py, test_mw8_runbook.py name explicit partition

FINDING:
  id: CRATE-001
  severity: S1
  category: AT-8
  clause: C-001
  disposition: REMEDIATED
  claim: write_position_deletes exceeded clippy too_many_lines after grouping landed
  evidence: prepare_position_delete_groups extracted

FINDING:
  id: CRATE-002
  severity: S1
  category: AT-8
  clause: C-005
  disposition: REMEDIATED
  claim: ENGINE_CONTRACT in rustdoc tripped clippy doc_markdown
  evidence: backticks around ENGINE_CONTRACT in position_delete.rs docs

FINDING:
  id: CRATE-003
  severity: S1
  category: AT-8
  clause: C-001
  disposition: REMEDIATED
  claim: SparkWriteConf in call.rs rustdoc tripped clippy doc_markdown
  evidence: call.rs MOR-2 pin rustdoc

FINDING:
  id: SAF-MW9-1
  severity: S1
  category: AT-3
  clause: C-004
  disposition: REMEDIATED
  claim: unknown granularity parsed after MATCHED UPDATE already wrote data files
  evidence: parse_delete_granularity in resolve_merge_mode before any IO; pin unknown_delete_granularity_refuses_before_any_write with banana (not files)

FINDING:
  id: CL-GUIDE-RUNBOOK-DEFAULT
  severity: S1
  category: AT-10
  clause: C-009
  disposition: REMEDIATED
  claim: runbook stated one delete file per partition as current default
  evidence: iceberg-guide.md trigger paragraph names file default and partition as the MW-7 layout

FINDING:
  id: CL-MOR2-FIXED-QUANTIFIER
  severity: S1
  category: AT-10
  clause: C-009
  disposition: REMEDIATED
  claim: MOR-2 FIXED overclaimed DELETE/UPDATE
  evidence: registry rationale MERGE-only; residual fork pin named

FINDING:
  id: C2-Q-001
  severity: S1
  category: AT-3
  clause: C-004
  disposition: REMEDIATED
  claim: identity MoR UPDATE wrote parquet then parsed granularity
  evidence: parse in resolve_write_mode; pin unknown_granularity_refuses_identity_update_before_any_parquet_write (warehouse parquet count unchanged)

FINDING:
  id: C2-SAF-001
  severity: S1
  category: AT-3
  clause: C-004
  disposition: REMEDIATED
  claim: same hole as C2-Q-001 on the shared writer
  evidence: resolve_write_mode before register_streaming_target / write_new_data_files_from_stream; Spark unknown_granularity_refuses_identity_update_before_any_write

FINDING:
  id: C2-Q-002
  severity: S1
  category: AT-7
  clause: C-007
  disposition: REMEDIATED
  claim: ANSI SET PROPERTIES then MERGE unpinned
  evidence: mw9_delete_granularity.rs::alter_set_granularity_is_honored_on_the_next_merge

FINDING:
  id: C2-Q-003
  severity: S1
  category: AT-7
  clause: C-005
  disposition: REMEDIATED
  claim: fork TableProvider UPDATE unpinned
  evidence: fork_table_provider_update_is_not_this_writer (six-file literal UPDATE → 1 delete file)

FINDING:
  id: C2-Q-004
  severity: S2
  category: AT-8
  clause: C-008
  disposition: REMEDIATED
  claim: seed_mor_delete_files rustdoc claimed explicit partition while CREATE omitted it
  evidence: CREATE TBLPROPERTIES now sets write.delete.granularity=partition

FINDING:
  id: C2-CRATE-001
  severity: S2
  category: AT-7
  clause: C-004
  disposition: REMEDIATED
  claim: parse unit test did not require the illegal value in the message
  evidence: parse_delete_granularity_spark_default_and_refuse contains banana

FINDING:
  id: C2-CL-001
  severity: S1
  category: AT-10
  clause: C-004
  disposition: REMEDIATED
  claim: merge/map.md silent on refuse-before-IO
  evidence: crates/repark-iceberg/src/write/merge/map.md MW-9 resolve_merge_mode sentence

FINDING:
  id: C2-CL-002
  severity: S1
  category: AT-10
  clause: C-009
  disposition: REMEDIATED
  claim: maps/pin rustdoc quantified closed MOR-2 without MERGE residual
  evidence: spark map.md, tests/map.md, staging/map.md, call.rs rustdoc name MERGE + fork residual

FINDING:
  id: C2-CL-003
  severity: S1
  category: AT-10
  clause: C-009
  disposition: REMEDIATED
  claim: STATUS.md still lists MOR-2 as an open remaining row
  evidence: departure commit — STATUS remaining-row list and MW-7 scorecard; slate queue empty
```

```yaml
ACTOR_BUILD_SUMMARY:
  pr_unit: MW-9
  charter_trace: C-001..C-011
  what_was_built: >
    write_position_deletes groups by parse_delete_granularity (absent → file,
    matching SparkWriteConf; explicit partition; unknown refuses). Existing
    MoR fixtures that encoded partition counts set the property explicitly
    (mor_props, MW-7 MOR_PROPERTIES). SQL DELETE/UPDATE via the fork
    TableProvider are disclosed as not this writer (C-005).
  tests: writer grouping + Spark door + ANSI door + facade; RPDF compaction pin still green
  status: CONCLUDED
```

```yaml
COVERAGE_ATTESTATION:
  pr_unit: MW-9
  cycle: 2
  risk_tier: high
  critic_engine: ccc
  complete: true
  note: >
    Cycle 1 remediations then cycle 2 quad. Cycle-2 S1s remediating (identity UPDATE
    refuse-before-IO, ANSI C-007, fork UPDATE pin, maps). C2-CL-003 remediating
    in this departure. Critics did not re-run after the
    cycle-2 fix half (max_cycles=2). make verify 2026-08-24 exit 0.
  categories:
    - id: AT-1
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/position_delete.rs]
    - id: AT-2
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/delete_granularity.rs, crates/repark-sql/src/mw9_delete_granularity.rs]
    - id: AT-3
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/delete_granularity.rs::unknown_delete_granularity_refuses_before_any_write]
    - id: AT-4
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/delete_granularity.rs::alter_set_granularity_is_honored_on_the_next_merge]
    - id: AT-5
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/position_delete.rs::parse_delete_granularity]
    - id: AT-6
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/position_delete.rs]
    - id: AT-7
      status: ATTACKED
      artifacts: [crates/repark-spark/src/tests/call.rs::call_mor2_merge_writes_one_position_delete_per_data_file_by_default, python/repark/tests/test_mw9_delete_granularity.py]
    - id: AT-8
      status: ATTACKED
      artifacts: [crates/repark-iceberg/src/write/position_delete.rs]
    - id: AT-9
      status: N/A
      justification: grouping is one HashMap pass per commit, not a per-row hot path
    - id: AT-10
      status: ATTACKED
      artifacts: [docs/spark-sql-iceberg-parity.md, crates/repark-spark/src/call.rs]
```
