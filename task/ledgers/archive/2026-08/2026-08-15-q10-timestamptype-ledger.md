# Unit ledger — Q10: `spark.sql.timestampType` LTZ default + NTZ opt-in

**Unit:** Q10 · stretch (conductor-13 A13, T4 exhausted) · **Date:** 2026-08-15 ·
**Lane:** repark · **Executor:** Grok (grok-4.6) ·
**Worktree:** `/tmp/grok-m16` · **Branch:** `grok/q10-timestamptype` ·
**Base (FROZEN):** `cd0db4f459e62994b45f8aadd1d5b58f040d90a5`

**Charter:** `BRIEF-stretch-q10-a11.md` Q10 + `TZ4-DESIGN.md` Q10=A leftover
(no `spark.sql.timestampType` in TZ-4) + ADR-0004 (everything-through-Session).
**SEPMO:** octo + C4. Floor S1. One PR. Do not start A11.

Registry / STATUS.md / lockfiles / AWS / fork pin: **CLOSED** (orchestrator-side).

### Proposition ledger (scope audit)

| ID | Proposition | Verdict |
|---|---|---|
| C-001 | Default `TIMESTAMP_LTZ` is byte-identical to today's type-resolution. | PROVEN — missing carrier → LTZ; existing pins unedited. |
| C-002 | `TIMESTAMP_NTZ` resolves bare TIMESTAMP literals / CAST / DDL to NTZ. | PROVEN — analyzer NTZ arm + Spark DDL mapping + door tests. |
| C-003 | Both doors + DataFrame, value AND Arrow type. | PROVEN — Spark door, ANSI `sql_with`, `F.expr` / createDataFrame. |
| C-004 | Conf get/set round-trip; invalid value names both legal tokens. | PROVEN — facade + `configure` + builder. |
| C-005 | No SessionBuildConf break; no env reads at query time. | PROVEN — sibling ConfigExtension, parsed in `configure()`. |
| C-006 | Honest cut: no engine campaign beyond the designed knob. | PROVEN — §0 surface table. |

---

## 0. Surface enumeration (recon)

TZ-4 already built the type-resolution seam (LTZ = µs+UTC / Iceberg `timestamptz`;
NTZ = naive µs / Iceberg `timestamp`). Q10 is the leftover *knob* that picks
which one bare SQL `TIMESTAMP` means.

| Surface | Where it consumed the default | Q10 action |
|---|---|---|
| SQL `TIMESTAMP '…'` literals | `instant_ts::wrap_ns_literal` always localized → LTZ | NTZ: naive µs, no localize |
| `CAST(… AS TIMESTAMP)` | `instant_ts::rewrite_cast` → `to_timestamp` / wrap LTZ | NTZ: wrap as naive µs |
| Spark DDL `CREATE (ts TIMESTAMP)` | `sql_type_to_iceberg` hardcoded timestamptz | follows carrier; wrapper stays LTZ |
| Spark DDL `ALTER ADD/ALTER COLUMN` | same helper | threaded from `SessionContext` |
| Spark `REPLACE COLUMNS` | parse-time, no session | **stays LTZ wrapper** (named) |
| ANSI DDL `CREATE` | already `CAST(NULL AS type)` through the planner | follows analyzer on Spark-extended session |
| ANSI door SQL | session-scoped analyzer | Spark-extended + `sql_with` |
| `to_timestamp` / `now` / `current_timestamp` | LTZ producers | **unchanged** (not the SQL type name) |
| Explicit `TIMESTAMP_NTZ` / `WITHOUT TIME ZONE` / `WITH TIME ZONE` | own mappings | **unchanged** |
| `Column.cast("timestamp")` / `parse_data_type` | binding, session-less, always LTZ | **out of fence** (bindings); DataFrame pin is `F.expr` |
| `TimestampType()` class | always LTZ | **unchanged** (explicit class) |
| createDataFrame inference | `_infer_arrow_type_from_python_sample` | reads live session default |
| CSV smart rung | `TimestampType()` | same helper |
| Native extension-less ANSI | no `SparkExtension` | **no carrier** (ansi.enabled precedent) |

**Honest cut:** the seam did **not** require engine work beyond the designed knob.
No HALT.

**Runtime `conf.set`:** store-only on the facade (ansi.enabled precedent). Engine
resolution is at session build. NTZ behavior pins use builder `.config`.

---

## 1. Shape

- `repark-functions::timestamp_type` — `ConfigExtension` `PREFIX=repark.timestamp`,
  default LTZ, parse fail-loud naming both tokens.
- `SparkExtension::configure` installs it from the builder map.
- `instant_ts` analyzer: default path **untouched**; NTZ is a separate rewrite.
- Spark `sql_type_to_iceberg` wrapper stays LTZ; `_with_timestamp_type` is the knob.
- Facade: `_SQLCONF_DEFAULTS`, get/set validate+store, builder normalize,
  getOrCreate engine-knob set (no lying fold).

---

## 2. Tests

| Pin | What it catches |
|---|---|
| `configure_defaults_timestamp_type_ltz` | missing key still installs LTZ carrier |
| `configure_honors_timestamp_type_ntz` | builder NTZ lands |
| `configure_refuses_invalid_timestamp_type` | loud, names both tokens |
| `instant_ts::ntz_opt_in_*` | literals/CAST naive; `to_timestamp`/`now` stay LTZ; int CAST seconds |
| Spark door `session_timestamp_type.rs` | LTZ default, NTZ SQL, invalid build, DataFrame CAST, DDL both ways |
| ANSI `session_timestamp_type_ansi_door.rs` | doors agree on NTZ |
| `test_timestamp_type.py` | get/set, invalid set+builder, SQL+F.expr+createDataFrame, to_timestamp fence |
| `bare_timestamp_follows_session_timestamp_type` | DDL mapping unit |

Existing default-mode pins: **zero edits**.

---

## 3. Named residues

- `REPLACE COLUMNS` parse-time mapping stays LTZ (no session at that parse).
- Binding `parse_data_type("timestamp")` stays LTZ (session-less; fence).
- `F.expr("TIMESTAMP '…'")` analyzes on a bare `SessionContext` (no carrier) —
  same binding-owned shape as H-1a's `PyColumn.sql`. DataFrame pin is
  `selectExpr` → `session.sql`.
- Runtime `conf.set` does not re-resolve the engine (ansi.enabled precedent).
- Native extension-less ANSI session has no carrier.
- `CAST(ltz AS TIMESTAMP)` under NTZ is a metadata strip (UTC ticks as NTZ wall).
  Tests use zoneless strings / UTC walls so values match Spark.

---

## 4. Gates

- `make verify` — exit 0 (fmt, clippy both modes, crate-dag, lib-rs, rust-file-size,
  lib-py, manifest, parity-live dual-wire, matrix-test-liveness, cargo test
  --workspace, ruff, taplo, typos).
- `make preflight` — exit 0 (verify + `py-test-facade` 3128 passed / 71 skipped
  after the two DataFrame pins were retargeted to `selectExpr` + tuple
  inference + cargo-deny + pip-audit + zizmor + workflow parse).
- Existing default-mode pins: **zero edits**.
