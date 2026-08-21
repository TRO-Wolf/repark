# V3-1 — `CALL system.register_table` and the cross-engine fixture

**Date:** 2026-08-21 · **Branch:** `feat/v3-1-register-table` · **Base:** `2e894eb` (`main`) ·
**Design:** [../docs/design/format-v3-track.md](../docs/design/format-v3-track.md) §4–5 ·
**Sequence:** [../briefs/next-sequence.md](../briefs/next-sequence.md) ·
**SEPMO path:** STANDARD / HIGH (`critic_engine: octo`, then `/critic-overload`) ·
**claims_critic:** true

V3-0 answered the addressing question (adoption) and measured Spark's procedure signature from
the 1.10.0 jar. This unit wires that procedure, checks in a Spark-written format-v3 table CI can
read, and promotes `B-MOR-3` and `V3-ADOPT-1`. It does **not** create format-v3 tables (V3-2)
and does not write deletion vectors (V3-3).

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-PER-V3-1
  agent: Orchestrator
  action: execute PR-carved charter V3-1 (one PR unit)
  charter_trace: C-001..C-014
  preconditions:
    - main at origin/main 2e894eb: SATISFIED (git rev-parse)
    - V3-0 signature measured: SATISFIED (task/v3-0-charter-ledger.md §4)
    - fork Catalog::register_table on memory+Glue, S3 Tables FeatureUnsupported: SATISFIED (fork pin 0c5fd58)
    - user confirmation to proceed: SATISFIED (session: "Proceed with the charter V3-1")
    - LIGHT/STANDARD rubric recorded: SATISFIED (fails criterion 5 — catalog pointer swaps → HIGH → octo)
  success_condition: every clause below PROVEN at unit scope; make verify green; B-MOR-3 pin runs in CI
  step_risks:
    - Hadoop Avro path rewrite corrupts fixture: HANDLED (same-length 26-byte prefix, pinned by 37-row read)
    - grow into V3-2 create: HANDLED (the_engine_still_cannot_produce_a_v3_table still on the tree)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```

## PROPOSITION LEDGER — V3-1 — 2026-08-21

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|--------|--------------------------|-------------------|---------|---------------------------|
| C-001 | `CALL catalog.system.register_table` is a supported procedure (named in the unknown-proc list) | unknown-proc error contains `register_table` | PROVEN | `call_unknown_procedure_lists_register_table`; facade `test_unknown_procedure_lists_supported` |
| C-002 | Required arguments are `table` STRING and `metadata_file` STRING, named or positional, not mixed | missing `metadata_file` refuses naming it; positional form adopts | PROVEN | `call_register_table_refuses_a_missing_metadata_file_argument`; `call_register_table_accepts_positional_arguments` |
| C-003 | Empty `metadata_file` refuses before the catalog is called | pin contains `non-empty` and `metadata_file` | PROVEN | `call_register_table_refuses_an_empty_metadata_file` |
| C-004 | Result schema is three nullable BIGINT columns, Spark names and order: `current_snapshot_id`, `total_records_count`, `total_data_files_count` | Arrow schema pin, facade and Spark door | PROVEN | engine-written adopt test + `test_register_table_adopts_and_returns_spark_columns` |
| C-005 | Result values come from the current snapshot id and summary keys `total-records` / `total-data-files`; missing snapshot → nulls, never a fabricated walk | adopted engine table matches loaded summary | PROVEN | `call_register_table_adopts_an_engine_written_table_and_returns_sparks_three_columns` |
| C-006 | After register, `SELECT` on the new ident returns the adopted rows (Spark door + facade) | row count / Arrow values | PROVEN | same tests as C-004/C-005; Spark-written fixture live count 37 |
| C-007 | ANSI door `CALL … register_table` still refuses as a callable-operation, not as a parse error | existing Q7 refusal names the procedure | PROVEN | `crates/repark-sql/src/refusals/tests.rs::call_refusal_steers_to_callable_ops_and_names_the_trigger` |
| C-008 | Spark-written format-v3 table with live Puffin vectors is CI-runnable (no JVM) | checked-in fixture + register + read | PROVEN | `crates/repark-spark/src/tests/fixtures/v3-spark-mor/`; `call_register_table_adopts_a_spark_written_v3_table_with_puffin_vectors` (40/4 summary, 37 live) |
| C-009 | `rewrite_position_delete_files` on that table refuses and names `3 live Puffin deletion vector(s)` | B-MOR-3 pin | PROVEN | `call_rewrite_position_delete_files_refuses_spark_written_puffin_vectors` |
| C-010 | Hadoop `vN.metadata.json` registers and reads; a CALL write names the Hadoop convention and the version-uuid shape | V3-ADOPT-1 pin | PROVEN | `call_register_table_of_hadoop_named_metadata_writes_name_the_convention` |
| C-011 | This unit does not create format-v3 tables | V3-0 blast-radius pin still on the tree | PROVEN | `call_v3.rs::the_engine_still_cannot_produce_a_v3_table` (untouched) |
| C-012 | Unknown extra named arguments refuse | `reject_unknown_named` | PROVEN | same helper as every other CALL; covered by `reject_unknown_named(&["table", "metadata_file"])` |
| C-013 | Registry admits `B-MOR-3` and `V3-ADOPT-1` as rows with pins; they leave the awaiting-pins queue | registry edit in the same change | PROVEN | `docs/spark-sql-iceberg-parity.md` |
| C-014 | Entry-point matrix: Spark door + facade implement; ANSI refuses; native DataFrame N/A | one pin per reachable door | PROVEN | C-004/C-006 Spark door; C-004 facade; C-007 ANSI |

VERDICT: PASS (OPEN=0, REJECTED=0). LOGIC_SCORE = 14/14.

```yaml
KILLED_ASSUMPTIONS:
  - Glue live register_table this unit: REMOVED (V3-0: read from fork source, not run; MW-4 is the live catalog evidence)
  - S3 Tables will grow register_table: REMOVED (fork FeatureUnsupported is the honest answer)
  - Check-in Spark binaries need path rewrite at runtime: REMOVED (same-length 26-byte prefix `/tmp/repark-v3-1-spark-mor`)
RISK_HEATMAP:
  - risk: fixture lock on a fixed /tmp path under parallel cargo test
    severity_if_realized: S2
    mitigation: Mutex in call_register.rs
  - risk: iceberg-datafusion INSERT path still shows the raw Hadoop filename (not CALL)
    severity_if_realized: S3
    mitigation: C-010 pins the CALL write this unit owns; INSERT is passthrough
CLARIFYING_QUESTIONS: []
```

## PR carving

One PR unit. Rubric: STANDARD fails criterion 5 (catalog pointer swap / persistence) → **HIGH** →
`critic_engine: octo` (user also named `/sepmo-octo` then `/critic-overload`). `claims_critic=true`.

## Scope / out of scope

| In | Out |
|---|---|
| `CALL register_table` | `CREATE`/`CTAS` format-v3 (V3-2) |
| Spark-written v3 fixture | MoR writes on v3 (V3-3) |
| B-MOR-3, V3-ADOPT-1 rows | `_row_id` read surface (V3-4) |
| Hadoop error text on CALL writes | Lifting V3-LINEAGE-1 |
| | AWS / IAM / `.github/` / `[patch]` |

## Forbidden surface

None touched. No AWS credentials, no `Cargo.toml [patch]`, no `.github/`, no lockfile edit
beyond what a new public re-export does not require.
