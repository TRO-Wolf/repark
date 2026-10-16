# Unit ledger — PA-DBT-MISMATCH-REDACT-1 · the dbt profile mismatch names keys, never values

**Scope (2026-10-16):** audit PA-01 (U-5 part 1). The refusal in `acquire_session` and dbt's
connection report stop carrying property values; what the refusal refuses does not change.

**Date:** 2026-10-16 · **Branch:** `fix/pa-dbt-mismatch-redact-1` · **Base:** `60134633` ·
**Model:** Claude (claude-opus-5-5) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** [../../wo/pa-dbt-mismatch-redact-1.md](../../wo/pa-dbt-mismatch-redact-1.md), from
[the production audit](../../../docs/cutover/production-audit-2026-10-11.md) finding PA-01.
**This ledger closes when the unit's pull request merges.**

## PROPOSITION LEDGER — PA-DBT-MISMATCH-REDACT-1 — 2026-10-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The refusal compares the full configuration: a profile that differs from the live one only in a property value, secret or URL, is still refused. | Pins that rotate one value per dictionary and expect the refusal. | **PROVEN** | `test_catalog_secret_change_names_the_catalog_key`, `test_session_secret_change_names_the_session_key`, `test_source_url_change_names_both_url_keys_sorted`; `session_key` and the `!=` comparison are unchanged. |
| C-002 | The refusal text names the live and requested `database` and `warehouse` and the sorted changed keys, each prefixed by its dictionary, and no property value. | Pins over a database-only change, a catalog key, a session key, both URL keys, and an added and a removed key. | **PROVEN** | `test_only_database_differs_names_both_and_no_key` (`changed keys: none`), the three tests above, `test_added_and_removed_keys_are_named`; `_mismatch_message` and `_changed_keys` in `session.py`. |
| C-003 | The real dbt error and debug paths carry no sentinel, with sentinel secrets in both dictionaries and a source URL carrying a password in each. | `dbt debug` through `dbtRunner` and `ReparkConnectionManager.open`, both pinned. | **PROVEN** | `test_dbt_debug_reports_the_refusal_without_a_value` reads the printed events and `logs/dbt.log`; `test_refusal_still_raises_and_keeps_the_live_session` goes through `ReparkConnectionManager.open`. The §1 (b) probe re-run on the head with no live session: no sentinel in either sink. |
| C-004 | The refusal still works: it raises `DbtRuntimeError`, the connection is marked failed, and the live session and its key are unchanged and still served to the matching profile. | A pin through `ReparkConnectionManager.open`. | **PROVEN** | `test_refusal_still_raises_and_keeps_the_live_session`, green on main and on the head. |
| C-005 | Every value the boundary does print goes through the project's redaction, `_native.redact_property_value`: the two identifiers in the refusal and the `catalog_properties` values in dbt's connection report. | Pins on the masked warehouse userinfo and on the redacted connection report. | **PROVEN** | `test_warehouse_url_credentials_are_masked`, `test_connection_info_renders_catalog_properties_redacted`; `_identifiers` in `session.py`, `connection_info` and `_redact_properties` in `connections.py`. No new rule. **Amended 2026-10-16 (ruling 122):** the connection report also shows `database`, its alias `catalog` and `warehouse` through `redact_property_value`, in `dbt debug` and in the Jinja `target` that `Profile.to_target_dict` builds; a warehouse URL shows `s3://u:***@bucket/warehouse`. Pinned by `test_jinja_target_renders_identifiers_and_catalog_properties_redacted` and by `test_dbt_debug_reports_the_refusal_without_a_value`, whose profile now carries that warehouse URL; both were red on `0203dc00`. `connection_info` and `_redact_report_value` in `connections.py`. **Amended 2026-10-16 (ruling 123):** the pins for the identifiers are these. `database`: `target.database` of the Jinja target `Profile.to_target_dict` builds, for a catalog `u:SENTINEL_DB_PW@h`, is `u:***@h` (`test_jinja_target_renders_identifiers_and_catalog_properties_redacted`). `catalog`: `target.catalog` of that target, same profile, is `u:***@h` (same test). `warehouse`: `target.warehouse` is `s3://u:***@bucket/warehouse` (same test); `dbt debug`'s events and `dbt.log` carry no `SENTINEL_WH_PW` (`test_dbt_debug_reports_the_refusal_without_a_value`); the refusal names it `s3://u:***@bucket/warehouse` (`test_warehouse_url_credentials_are_masked`). Not pinned: masking of `database` in `dbt debug`'s `database:` line or in the refusal. |
| C-006 | Step 0 measured main: the refusal and the debug display carry the sentinels named in §1, and the redaction rule's answer for each value. | §1 below, re-derived by the pins red on main. | **PROVEN** | §1; §2 records 8 of 9 pins red on main. |

## 1. Step 0 — measured on main `60134633`

Sentinels: `SENTINEL_CAT_SECRET` at `catalog_properties["spark.sql.catalog.ice.s3.secret-access-key"]`,
`SENTINEL_SESSION_SECRET` at `session_properties["spark.hadoop.fs.s3a.secret.key"]`, and
`postgresql://u:SENTINEL_URL_PW@h/db` at `catalog_properties["spark.sql.catalog.ice.uri"]` and
`session_properties["repark.source.pg.url"]`. The native module was built from `60134633` in
release mode. No credential, cloud request or AWS was used; dbt's connection attempt in (b)
failed on the catalog block's missing kind before any engine work.

**(a) The mismatch, changing only `database`** (`ice` live, `other` requested; the live
session a stand-in, so nothing is built). `acquire_session` raised `DbtRuntimeError`; the
live session was unchanged. Sentinels seen: **all three**, each twice.

```
Runtime Error
  a RePark session is already live for a different profile; release it before connecting with another catalog or warehouse (live ('ice', None, (('spark.sql.catalog.ice.s3.secret-access-key', 'SENTINEL_CAT_SECRET'), ('spark.sql.catalog.ice.uri', 'postgresql://u:SENTINEL_URL_PW@h/db')), (('repark.source.pg.url', 'postgresql://u:SENTINEL_URL_PW@h/db'), ('spark.hadoop.fs.s3a.secret.key', 'SENTINEL_SESSION_SECRET'))), requested ('other', None, (('spark.sql.catalog.ice.s3.secret-access-key', 'SENTINEL_CAT_SECRET'), ('spark.sql.catalog.ice.uri', 'postgresql://u:SENTINEL_URL_PW@h/db')), (('repark.source.pg.url', 'postgresql://u:SENTINEL_URL_PW@h/db'), ('spark.hadoop.fs.s3a.secret.key', 'SENTINEL_SESSION_SECRET'))))
```

**(b) `dbt debug`** through `dbtRunner` (dbt-core 1.9.11, dbt-spark 1.9.3) with the same
profile and no live session. Sentinels seen in the printed events and in `logs/dbt.log`:
**`SENTINEL_CAT_SECRET` and `SENTINEL_URL_PW`**, both from the connection report.
`SENTINEL_SESSION_SECRET` does not appear: `_connection_keys` lists no `session_properties`.
The connection-test error text carries no sentinel.

```
Connection:
  database: ice
  schema: gold
  warehouse: None
  catalog_properties: {'spark.sql.catalog.ice.s3.secret-access-key': 'SENTINEL_CAT_SECRET', 'spark.sql.catalog.ice.uri': 'postgresql://u:SENTINEL_URL_PW@h/db'}
  Connection test: [ERROR]
```

dbt builds the same pairs into the Jinja `target` (`Profile.to_target_dict` reads
`connection_info(with_aliases=True)`) and hashes them for partial parsing, so the report is
not only a display.

**(c) `_native.redact_property_value`** for each value:

| Key | Value | Answer |
|---|---|---|
| `spark.sql.catalog.ice.s3.secret-access-key` | `SENTINEL_CAT_SECRET` | `***` |
| `spark.sql.catalog.ice.uri` | `postgresql://u:SENTINEL_URL_PW@h/db` | `postgresql://u:***@h/db` |
| `spark.hadoop.fs.s3a.secret.key` | `SENTINEL_SESSION_SECRET` | `***` |
| `repark.source.pg.url` | `postgresql://u:SENTINEL_URL_PW@h/db` | `postgresql://u:***@h/db` |
| `warehouse` | `s3://u:SENTINEL_URL_PW@bucket/warehouse` | `s3://u:***@bucket/warehouse` |

The probes ran out of tree; the step 1 pins re-run (a) and (b) through the same paths.
Step 3 runs: (b) showed two sentinels.

## 2. Pins

`python/dbt-repark/tests/test_profile_mismatch_redact.py`, 9 tests. On main `60134633` (the commit that adds them): **8 red, 1 green**; the green
one is the C-004 control. Each red fails on its sentinel or redaction assertion. After step 2,
the two connection-report pins stay red; after step 3, all 9 are green.

## 3. Hand mutant

The old message restored over the head (`f"(live {_ACTIVE['key']!r}, requested {key!r})"`):
**7 red, 2 green**. Red: the six refusal pins and the `dbt debug` pin. Green: the C-004
control and `test_connection_info_renders_catalog_properties_redacted`, which does not read
the message. Restored: 9 green.

**Ruling 122 round (2026-10-16).** Two pins added, 10 tests. On `0203dc00` both were red on
`SENTINEL_WH_PW`: the Jinja target pin at `_assert_no_sentinel(repr(target))` and the
`dbt debug` pin at `_assert_no_sentinel(printed)`. Green after the fix. Hand mutant,
`connection_info` yielding each value unredacted: **3 red, 7 green**. The target pin is red on
`['SENTINEL_CAT_SECRET', 'SENTINEL_URL_PW', 'SENTINEL_WH_PW']`, and the connection-report and
`dbt debug` pins are red too. The verifier's M10, redaction only when `with_aliases` is false:
**1 red**, the target pin. Restored: 10 green.

**Ruling 123 round (2026-10-16).** One assertion added to
`test_jinja_target_renders_identifiers_and_catalog_properties_redacted`: a profile whose
catalog is `u:SENTINEL_DB_PW@h` renders `target.database` and `target.catalog` as `u:***@h`
and carries no sentinel; green on `e3641707`. M14, `catalog` dropped from
`_REDACTED_IDENTIFIERS`: `target.catalog` is raw, killed by that test (1 red, 9 green, on
`assert ['SENTINEL_DB_PW'] == []`). M15, `database` dropped: `target.database` is raw, killed
by the same test (1 red, 9 green, same assertion). Restored: 10 green.

## 4. Decisions inside the order, and what was not measured

- `database` and `warehouse` are named through `_native.redact_property_value`, under the
  order's rule that a value that must appear goes through the project's redaction. A
  warehouse URL with userinfo shows `s3://u:***@bucket/warehouse`.
- Step 3 renders `catalog_properties` redacted rather than dropping it. dbt builds the Jinja
  `target` from the same pairs, so `target.catalog_properties` now shows redacted values. A
  macro that read a secret from it would get `***`. No macro in this repo reads it.
- The partial-parse profile hash now covers the redacted values, so rotating a secret alone
  no longer forces a re-parse. Parsing reads no property value.
- Not measured: `dbt run` and `dbt test` output with a failing connection; they reach
  `acquire_session` through the same `open` the C-004 pin drives.
- The `dbt debug` pin passes `--no-send-anonymous-usage-stats`, so dbt sends no usage events.

## 5. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: pa-dbt-mismatch-redact-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Each sentence of the PA-01 required result is a clause, and each clause is pinned in the new test file.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-2
      status: ATTACKED
      evidence: A database-only change, a changed value per dictionary, a URL password in both dictionaries, an added and a removed key, a warehouse URL with userinfo, and a None warehouse.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal still raises DbtRuntimeError through ReparkConnectionManager.open, marks the connection failed, and dbt debug reports it as a failed connection test.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-4
      status: N/A
      justification: The message is built under the existing lock from two immutable key tuples; no ordering or shared state changes.
    - id: AT-5
      status: ATTACKED
      evidence: Sentinel secrets only; the refusal text, the dbt debug events and dbt.log are scanned for every original and rotated sentinel.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-6
      status: ATTACKED
      evidence: The comparison keeps the full configuration, so no profile that differs is served the live session; the matching profile is still served the live one.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-7
      status: N/A
      justification: The message is built only on the refusal path, and the connection report only when dbt asks for it; no hot path changes.
    - id: AT-8
      status: ATTACKED
      evidence: The message keeps its leading sentence, which the adapter map's debug row quotes; only the parenthetical detail changes.
      artifacts: [python/dbt-repark/src/dbt/adapters/repark/session.py]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal still says which identifiers and which keys differ, so a user can find the mismatch without seeing a value.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
    - id: AT-10
      status: ATTACKED
      evidence: 8 of 9 pins are red on main, and the hand mutant that restores the old message reds 7.
      artifacts: [python/dbt-repark/tests/test_profile_mismatch_redact.py]
  complete: true
```
