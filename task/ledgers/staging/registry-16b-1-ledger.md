# Charter ledger — REGISTRY-16B-1 · three BACKLOG registry rows for the RuntimeConfig and JDBC-format surfaces

**Date:** 2026-09-15 · **Branch:** `docs/registry-16b-1` · **Base:** `7bcb68de` · **Model:**
zai/glm-5.3-flash (pins), claude-opus-5 (rows, ledger) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `CONF-UNSET-1`, `CONF-WAP-1` and `IO-JDBC-FORMAT-1`, all BACKLOG 2026-09-15, in
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md).

## Scope

Run 15c's report filed three P2 findings against run 15b's `RuntimeConfig` surface (runtime ANSI / time-zone sets stored
but not applied, `conf.unset` over a builder value, `spark.wap.*` stored silently) and the owner ruled Q-15B-4 on the
`format("jdbc")` alias. The ANSI and time-zone finding already has rows (`SET-ANSI-RUNTIME-1`, `TZ-3`) and belongs to the
SQL-SET runtime unit, so it adds nothing here. This unit measures the other two plus the JDBC alias on live PySpark 4.1.2
and on repark's `main`, files one row each, and pins today's answers so each pin reds when its fix lands. No product
code changes.

## Proposition ledger

| Clause | Claim | Verdict | Evidence |
|---|---|---|---|
| C-001 | `CONF-UNSET-1` states the measured divergence (repark raises on `get` after `unset`; Spark answers the registered default; both restore the builder value on `RESET`) and lands with its pins. | PROVEN | `test_conf_unset_builder_sql_conf_raises_on_get`, `test_sql_reset_restores_builder_value` pass on base `7bcb68de` (6 passed in the file); Spark's answers are probe cells `unset_builder_sql_conf` (`["3", "200"]`) and `sql_reset_builder_value` (`"7"`). |
| C-002 | `CONF-WAP-1` states that `spark.wap.*` stores through `conf.set`, reports modifiable, and fails on the SQL `SET` door, against Spark's stored keys, `isModifiable` False and the `SET` row, and lands with its pins. | PROVEN | `test_conf_wap_keys_store_and_report_modifiable`, `test_sql_set_wap_branch_raises` pass on base; Spark cells `wap_branch_set_get`, `wap_id_set_get`, `wap_is_modifiable` (`false`), `wap_sql_set`. |
| C-003 | `IO-JDBC-FORMAT-1` states that `format("jdbc")` reaches the PostgreSQL path where `spark.read.jdbc` refuses a non-PostgreSQL URL, against Spark's one shared driver path, and lands with its pins. | PROVEN | `test_format_jdbc_non_postgres_url_is_not_the_declared_refusal`, `test_format_jdbc_missing_url_names_postgres` pass on base; Spark cells `jdbc_format_mysql_url`, `jdbc_method_mysql_url` (`No suitable driver`), `jdbc_format_no_url`. |

## Rulings

| Id | Ruling | Applied |
|---|---|---|
| Q-15B-4 | Owner, 2026-09-15: `format("jdbc")` URL dispatch is 1.6 connector work; record the divergence as a registry row now. | `IO-JDBC-FORMAT-1` (C-003). |
| R-16b-9 | Run 16b orchestrator, 2026-09-15: run 15c's "unset hides a builder value where Spark restores it" was measured before filing. Spark answers the registered default after `unset` and restores the builder value only on `RESET`, so the row records the measured divergence (the raised `get`). | `CONF-UNSET-1` (C-001). |
| R-16b-10 | Run 16b orchestrator, 2026-09-15: the runtime ANSI / time-zone finding is already `SET-ANSI-RUNTIME-1` and `TZ-3` and is owned by the SQL-SET runtime unit (Q-15c-3); no new row. | Scope. |
| R-16b-11 | Run 16b orchestrator, 2026-09-15: the GLM worker could not read the oracle directory (tool sandbox) and wrote only the pins; the orchestrator wrote the rows, maps and this ledger in the same commit. | Commit trailers name both. |

VERDICT: 3 clauses, 3 PROVEN, 0 OPEN, 0 REJECTED.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: registry-16b-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each row was checked against both measurements, live PySpark 4.1.2 and repark main, before it was written; the 15c claim about unset was overturned by the Spark cell and the row says so.
      artifacts: [docs/spark-sql-iceberg-parity.md, task/ledgers/staging/registry-16b-1-ledger.md]
    - id: AT-2
      status: ATTACKED
      evidence: Edge arms measured - a SQL conf and a custom key under unset, a never-set key, RESET after set, both WAP keys, isModifiable, the SQL SET spelling, MySQL and PostgreSQL URLs and a missing url.
      artifacts: [python/repark/tests/test_registry_16b_1.py]
    - id: AT-3
      status: N/A
      justification: No product code and no Rust.
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; each pin builds and stops its own session.
    - id: AT-5
      status: ATTACKED
      evidence: The JDBC pins use an unroutable loopback port and assert only messages, so no connection succeeds and no credential is involved; the WAP row names the silent-write risk.
      artifacts: [python/repark/tests/test_registry_16b_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Rows are BACKLOG with dated rationale and a named fix; no existing row was edited or reordered, and SET-ANSI-RUNTIME-1 and TZ-3 stay the single home of the runtime-set finding.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The pin file runs in well under a second; docs links, ledger grammar and lifecycle, map sync and ruff pass on the branch.
      artifacts: [python/repark/tests/test_registry_16b_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: Spark's contracts are the recorded probe cells, not prose; the build-dependent PostgreSQL connector text is asserted only by what it is not, so the pin holds on builds with or without the connector.
      artifacts: [python/repark/tests/test_registry_16b_1.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every row names its pins and its oracle cells, and each pin's docstring cites its clause.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Each pin asserts today's divergent answer, so the fix that closes its row turns it red, which the registry's retirement rule requires.
      artifacts: [python/repark/tests/test_registry_16b_1.py]
  complete: true
```
