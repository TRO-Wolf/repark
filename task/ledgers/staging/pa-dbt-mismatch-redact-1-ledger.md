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
| C-001 | The refusal compares the full configuration: a profile that differs from the live one only in a property value, secret or URL, is still refused. | Pins that rotate one value per dictionary and expect the refusal. | OPEN | Step 1 pins, step 2 fix. |
| C-002 | The refusal text names the live and requested `database` and `warehouse` and the sorted changed keys, each prefixed by its dictionary, and no property value. | Pins over a database-only change, a catalog key, a session key, both URL keys, and an added and a removed key. | OPEN | Step 1 pins, step 2 fix. |
| C-003 | The real dbt error and debug paths carry no sentinel, with sentinel secrets in both dictionaries and a source URL carrying a password in each. | `dbt debug` through `dbtRunner` and `ReparkConnectionManager.open`, both pinned. | OPEN | §1 (a) and (b) measure main. |
| C-004 | The refusal still works: it raises `DbtRuntimeError`, the connection is marked failed, and the live session and its key are unchanged and still served to the matching profile. | A pin through `ReparkConnectionManager.open`. | OPEN | Step 1 pins. |
| C-005 | Every value the boundary does print goes through the project's redaction, `_native.redact_property_value`: the two identifiers in the refusal and the `catalog_properties` values in dbt's connection report. | Pins on the masked warehouse userinfo and on the redacted connection report. | OPEN | §1 (c) records the rule's answers. |
| C-006 | Step 0 measured main: the refusal and the debug display carry the sentinels named in §1, and the redaction rule's answer for each value. | §1 below. | OPEN | §1. |

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
