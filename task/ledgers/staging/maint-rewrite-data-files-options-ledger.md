# Charter ledger — rewrite_data_files `where` / `sort_order` / `strategy` (v0.6 merge order 4 of 4)

**Date:** 2026-08-31 · **Branch:** `feat/maint-rewrite-data-files-options` · **Base:**
`60225cc427673cbc2e4bf23e90db376e602773dd` · **Policy:** [../../../AGENTS.md](../../../AGENTS.md)
"Verify before done" and [../../../docs/testing.md](../../../docs/testing.md) · **Path:**
STANDARD · **risk_tier:** high (compaction rewrites live data). Owner-pre-authorized
2026-08-30 v0.6 plan.

**Retires:** moved to `completed/` in this unit's last commit. This file must not be archived
from this unit; pickup archives it later.

**Why now.** v0.6 merge order 4 of 4. DML-B (#273), DML-C (#274), and DML-A stay parked —
this unit does not touch their surfaces. Format-v3 lineage stays V3-5's: the v3 refusal pins
must remain byte-stable.

## PROPOSITION LEDGER — maint-rewrite-data-files-options — 2026-08-31

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **Fork ceiling measured at the pinned rev.** `RewriteDataFiles` at iceberg-rust `d408da42` exposes `filter(Predicate)` (file-selection only, no residual) and bin-pack only. Sort and z-order are named deferred in the action module and GAP_MATRIX R135. F-7 lineage carry is not in this rev. | Direct read of `maintenance/rewrite_data_files.rs` at the cargo checkout for `d408da42`. | **PROVEN** | Measured 2026-08-31: `pub fn filter(mut self, filter: Predicate)`; module deferred table lists "sort and Z-order \| only bin-pack is ported". HALT is not required: `where` and `strategy=binpack` are honerable. |
| C-002 | **Spark result columns measured first.** Live PySpark 4.1.2 + `iceberg-spark-runtime-4.1_2.13:1.11.0` (`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`) returns five non-nullable columns in this order: `rewritten_data_files_count` int, `added_data_files_count` int, `rewritten_bytes_count` bigint, `failed_data_files_count` int, `removed_delete_files_count` int. A six-file v2 compact reports `6, 1, rewritten_bytes, 0, 0`. The engine already ships this schema; this unit must not change names, types, or nullability. | Oracle session transcript; existing five-column pin stays green. | **PROVEN** | Oracle 2026-08-31 warehouse `/tmp/rdf-opt-ijeohnv5`: default CALL `Row(6, 1, 3713, 0, 0)`; named `strategy => 'binpack'` `Row(6, 1, 3672, 0, 0)`; positional `'binpack'` same. Arrow types: Int32 / Int32 / Int64 / Int32 / Int32, all `nullable=False`. |
| C-003 | **Filtered scope is file-byte identity, not row identity.** On a v2 table, `where` selects files via the fork filter. Files outside the filter's partition/metrics scope keep the same path, length, and SHA-256. Live rows of a selected file all survive (filter is file-selection, not a residual). Result columns stay Spark-shaped. | Spark-door + facade pins; before/after path+length+sha256 of the excluded file. | **PROVEN** | Spark 2026-08-31: `where => 'part = 0'` on 5×part=0 + 1×part=1 files → `rewritten=5, added=1, bytes=3075, failed=0, removed=0`. The part=1 file path, size 615, and sha256 `f6eb5600bc2fbedd12812991689964ad55a57a276ca0a4a581c50cd1fa084886` were identical after the CALL. |
| C-004 | **`strategy` `binpack` is honored; `sort` is refused loud.** Named or positional `binpack` (case-insensitive) runs the fork action. `strategy => 'sort'` (named or positional) refuses with a registry-documented fork-ceiling reason. Spark 4.1.2 + Iceberg 1.11.0 **does** run sort; this engine must not silently binpack. | Red-first pins for binpack success and sort refusal; registry row. | **PROVEN** | Spark: `'SORT'` with `sort_order => 'id ASC'` compacted 6→1, lower/upper id bounds `{1..6}` / `{n1..n6}`. Spark unknown strategy: `unsupported strategy: {name}. Only binpack or sort is supported` (`nope`, `zorder`). Spark does **not** trim `' BinPack '`. Fork cannot honor sort at `d408da42`. |
| C-005 | **`sort_order` is never silently ignored.** A named `sort_order` refuses loud (fork ceiling), including when `strategy` is omitted or `binpack`. Spark with `sort_order` and no `strategy` still binpacks and warns; this engine must not copy that ignore. | Pin that `sort_order => 'id ASC'` refuses and leaves the table uncompacted. | **PROVEN** | Spark 2026-08-31: `sort_order => 'id ASC'` without strategy returned `Row(6, 1, 2370, 0, 0)` (binpack) plus `SparkShufflingFileRewriteRunner` warning. Current engine already refuses; keep the refusal, document it. |
| C-006 | **Unknown strategy matches Spark's message.** `strategy => 'nope'` and `strategy => 'zorder'` refuse with Spark's exact text `unsupported strategy: {name}. Only binpack or sort is supported`. `sort` is not this class — it is C-004's fork-ceiling refusal. | Spark-door + facade pins with the verbatim needle. | **PROVEN** | Oracle 2026-08-31: `IllegalArgumentException: unsupported strategy: nope. Only binpack or sort is supported` (same for `zorder`). |
| C-007 | **Bad `where` matches Spark's message.** Unparsable SQL, a non-boolean expression, an empty string, and an unknown column all refuse with `Cannot parse predicates in where option: {expr}`. Convertible predicates (eq/cmp/AND/OR/NOT/IS NULL/IN/BETWEEN on primitive columns) are accepted and bound case-insensitively to the table schema. | Pins for each refusal class; one accepted `part = 0` (or `id`) path. | **PROVEN** | Oracle 2026-08-31 `IllegalArgumentException`: `id === 1`, `no_such_col = 1`, `id`, and `''` all `Cannot parse predicates in where option: …`. Missing-column is that wrapper, not Iceberg's bind error. |
| C-008 | **v3 lineage refusal is unchanged.** `refuse_v3_rewrite_that_would_lose_row_lineage` and `call_rewrite_data_files_refuses_a_v3_table_rather_than_reassigning_row_lineage` stay as they are. This unit does not lift `V3-LINEAGE-1`. | Identity of the existing v3 pins; a v2 control still compacts. | **PROVEN** | Existing pins in `crates/repark-spark/src/tests/call_v3.rs`. Do not edit the refusal text. |
| C-009 | **Documents match the pins.** Registry row for the sort fork ceiling; `call/map.md`, tests maps, facade map, STATUS under its 25 000 B ceiling. `options` stays refused (not in this ask). ANSI CALL stays Q7. | `make check-map-sync`, `check-ledger-grammar`, registry diff. | **PROVEN** | Sort missing-column on Spark is `ValidationException: Cannot find field 'does_not_exist' in struct: …`. Unreachable here until a fork sort lands; the sort refusal is the pin. |
| C-010 | **Gates.** `make verify`, `make check-map-sync check-ledger-grammar`, `python3 scripts/ledger_lifecycle.py check --base 60225cc427673cbc2e4bf23e90db376e602773dd`, full `make py-test`. | Gate exits in the Actor summary. | **PROVEN** | Bound commands. |

VERDICT: PROVEN — 10 clauses, 10 PROVEN, 0 OPEN, 0 REJECTED.

## 5. Actor coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: maint-rewrite-data-files-options
  cycle: actor
  risk_tier: high
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001..C-010 walked against the charter. where file-byte identity, Spark
        unknown-strategy and bad-where text, sort/sort_order refuse, v3 pins untouched.
      artifacts: [crates/repark-spark/src/tests/call_rewrite_options.rs]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Empty where, unparsable `id === 1`, unknown column, non-boolean `id`,
        unknown strategy nope/zorder, named BINPACK, sort_order without strategy.
      artifacts: [crates/repark-spark/src/tests/call_rewrite_options.rs]
    - id: AT-3
      status: ATTACKED
      evidence: >
        sort_order refusal leaves file count unchanged. Bad where does not compact.
      artifacts: [call_rewrite_sort_order_refuses_and_does_not_compact]
    - id: AT-4
      status: N/A
      justification: single-session CALL; no concurrent rewrite.
    - id: AT-5
      status: ATTACKED
      evidence: >
        where string with `;` is refused. Existing CALL path-escape guards unchanged.
      artifacts: [crates/repark-spark/src/call/rewrite_where.rs]
    - id: AT-6
      status: ATTACKED
      evidence: >
        Five files in the excluded partition stay path-and-byte identical. Live id
        multiset conserved. v3 refusal text unchanged.
      artifacts: [call_rewrite_where_keeps_out_of_scope_files_byte_identical]
    - id: AT-7
      status: N/A
      justification: no hot-path rewrite of the planner; fork action is sequential.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Fork filter() is the only where path. Sort is refused as R135/RDF-SORT-1.
        Result schema stays Spark's five non-nullable columns.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Unknown strategy and bad where name the argument in Spark's measured text.
      artifacts: [call_rewrite_unknown_strategy_matches_spark_message]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Byte-identity pin fails if where is ignored (both partitions would compact).
        Spark-text pins fail if messages drift. Existing sort-refuse pin still green.
      artifacts: [crates/repark-spark/src/tests/call.rs]
```

## 1. Out of scope

- DML-A / DML-B / DML-C surfaces.
- Format-v3 rewrite lineage (V3-5 / `V3-LINEAGE-1` / F-7). Do not lift the guard.
- `rewrite_data_files` `options` map (still refused).
- `rewrite_position_delete_files` `where` (still refused; not this procedure).
- Fork `Cargo.toml` `[patch]` / iceberg-rust rev. Sort rewrite waits on a later fork rev.
- ANSI-door `CALL` (Q7 CALLABLE OPERATION).

## 2. Sequence

1. This charter (measure-first).
2. Wire `where` through `RewriteDataFiles::filter`; keep sort/`sort_order` loud; match Spark
   unknown-strategy and bad-where messages; pin file-byte identity and result columns.
3. Registry + maps; gates; departure.

## 3. Fork vs Spark (recorded, not guessed)

| Input | Spark 4.1.2 + Iceberg 1.11.0 | Fork `d408da42` | This unit |
|---|---|---|---|
| no extra args / `strategy=binpack` | five-column result, binpack | binpack action | honor |
| `strategy=sort` + `sort_order` | runs sort rewrite | not ported | refuse loud, registry |
| `sort_order` without sort strategy | binpacks, warns | n/a | refuse loud (no silent ignore) |
| `strategy=nope` / `zorder` | `unsupported strategy: … Only binpack or sort is supported` | n/a | Spark text |
| `where => 'part = 0'` | file prune; excluded file byte-identical | `filter(Predicate)` file-selection | honor |
| bad / empty / unknown-col where | `Cannot parse predicates in where option: {expr}` | n/a | Spark text |
| `sort_order` on missing column | `Cannot find field '…' in struct` | n/a | unreachable; sort refused first |
| format v3 | Spark rewrites and keeps lineage | rewrite would reassign | keep existing refusal |

## 4. Self Logic Review (charter file)

```yaml
SELF_LOGIC_REVIEW:
  id: SLR-rdf-opt-charter
  agent: Actor
  action: File the measure-first charter ledger for rewrite_data_files options
  charter_trace: [C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010]
  preconditions:
    - fork surface at d408da42 read: SATISFIED (RewriteDataFiles::filter present; sort deferred)
    - Spark 4.1.2 + Iceberg 1.11.0 oracle ran: SATISFIED (sessions /tmp/rdf-opt-ijeohnv5 and /tmp/rdf-opt2-lligoplw)
    - v3 pins identified and out of scope: SATISFIED (call_v3.rs)
  success_condition: staging ledger exists, map.md lockstep, clauses C-001..C-010 stated checkably
  step_risks:
    - silently honoring sort by binpacking: HANDLED(C-004 refuse)
    - where residual dropping live rows: HANDLED(fork filter is file-selection; C-003 byte-identity)
    - changing v3 refusal text: HANDLED(C-008)
  contingencies:
    - whole ask fork-blocked: EXECUTABLE(additive — HALT; sort half is partial, where is not blocked)
  tripwire_scan: CLEAN
  uncertainty: NONE
  verdict: PROCEED
  escalation: —
```
