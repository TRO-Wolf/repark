# Unit ledger — WO LTZ-STORE-INT-1 · an INT stored into a TIMESTAMP column refuses like Spark

**Date:** 2026-09-28 · **Branch:** `fix/ltz-store-int-1` · **Base:** `9392dbc3` (`origin/main`)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Residue R-NTZ-S2-2: with default ANSI, `INSERT INTO sc.ns.l VALUES (0, 1)`
into `(id INT, c TIMESTAMP)` refuses on Spark 4.1.2 and silently wrote
`1970-01-01 00:00:00.000001` on RePark. The shared matrix
(`ansi_store_assignable`) already judges INT → TIMESTAMP unassignable — the
SELECT, UPDATE, MERGE and DataFrame doors all refuse through it — but the
VALUES door never consults it: DataFusion's `values_with_schema`/`infer_inner`
wraps each VALUES literal in its synthesized conform cast *inside* the `Values`
node, byte-identical to a user-written CAST, so the `InsertStoreAssignment`
analyzer rule sees conformed (TIMESTAMP) source types and passes. The fix judges
VALUES cells against TIMESTAMP columns one stage earlier, at the existing
`refuse_insert_void_values` gate site in `spark_ast`'s passthrough (a
`void_type` child module, the `void_store.rs` pattern), through the same
`incompatible_update_message` gate, for LTZ targets only — NTZ targets stay with
NTZ-1 Slice 2 (#866, unmerged).

**What Spark does (recorded `/tmp/oc-worker/direct/wo/ntz-1-probes/ntz4-spark.json`,
read verbatim, never paraphrased).** `ins_l_int`: error `AnalysisException`,
condition `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, SQLSTATE `KD000`,
message `[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write
incompatible data for the table `sc`.`ns`.`l`: Cannot safely cast `c` "INT" to
"TIMESTAMP". SQLSTATE: KD000`. `ins_l_str` refuses the same way with `"STRING"`.
No recorded Spark answer exists for BIGINT, DOUBLE or BOOLEAN into `TIMESTAMP`,
nor for UPDATE/MERGE/append of any of these five into `TIMESTAMP`: those shapes
are unmeasured, and RePark is held only to refusing with the
`CANNOT_SAFELY_CAST` class there.

**Measured on main (RePark, 2026-09-28, UTC).** VALUES writes epoch micros for
INT, BIGINT, DOUBLE and STRING; BOOLEAN VALUES dies in the cast kernel without
the class; NULL, DATE, TIMESTAMP and TIMESTAMP_NTZ store. INSERT … SELECT,
UPDATE SET, MERGE (with a matched row) and both DataFrame appends (`writeTo`,
`insertInto`) refuse INT/BIGINT/DOUBLE/STRING/BOOLEAN with the class, except
BOOLEAN SELECT which dies in DataFusion's own `Cannot automatically convert`
plan error. So the one site is the VALUES door, and the fix is one gate plus
two lines at the existing void VALUES gate site.

**Fold (2026-09-28, critic V-001).** The critic's 36-shape probe found typed
decimals (`CAST(x AS DECIMAL(p,s))`, `DECIMAL '1.5'`) still writing epoch micros
through VALUES while SELECT refused them: the shared gate returns None for
sources with no Spark name, and the VALUES gate read None as pass. The fold
stays in the same gate: null-valued cells pass (`CAST(NULL AS …)` is legal;
corrected 2026-09-28 by the verifier fold: only bare NULL passes, `CAST(NULL
AS T)` is judged by `T`);
every numeric source refuses even when the shared gate cannot name it, with the
DDL-name fallback carrying the class, condition and SQLSTATE; `1L` reads
`BIGINT`; DECIMAL casts and typed literals read by declared type. The fold also
scopes the gate to microsecond targets: the base unit's `Timestamp(_, Some(_))`
predicate refused the TIMESTAMP_NS door's pinned 9-digit strings, reds the
`session_write_conf` typing pin and two `v3_timestamp_ns_door` pins on
`15b50e3b`, and the fold heals them. Pins: the
`typed_numeric_values_into_timestamp_refuse` sibling (every numeric CAST,
`DECIMAL '1.5'`, `1L`, two `CAST(NULL …)` rows, a mixed multi-row refusal that
writes nothing) plus two classifier tests; the sibling reds with the fold
stashed (M2). V-002 is residue R-LTZ-1 below, owned by NTZ-1.

**Fold (2026-09-28, verifier VL-1..VL-6).** The verifier's hand-back
(`verify-ltz-opus-handback.json`, NEEDS_REMEDIATION, 1 S1) re-measured every
finding against Spark 4.1.2 (`ltz_verify_probe.py`, outputs
`target/ltz-verify/ltzv-{spark,repark}.json`, zone UTC, table `sc.ns.l (id
INT, ts TIMESTAMP)`): Spark WRITES `nvl`/`ifnull` over DATE and TIMESTAMP on
the VALUES door (VL-1, Spark agrees with the verifier); REFUSES typed NULLs by
declared type with Spark's exact text (VL-2); answers
`INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE`/42000 on mixed rows
(VL-3); names `DECIMAL(2,1)` and `BIGINT` (VL-4); stores `CAST(1 AS TIMESTAMP)`
as `1970-01-01 00:00:01` (existing pin confirmed). The fix stays in the same
gate (~31 product lines): only bare NULL passes; a probed STRING from a
function call defers instead of refusing, so the four `nvl`/`ifnull` VALUES
cells store with exact read-backs; fractionals read `DECIMAL(p,s)` and
out-of-`INT`-range integers read `BIGINT`. VL-5 and the pre-existing gaps are
dated residues R-LTZ-2..R-LTZ-9 (R-LTZ-10 retired by the re-verify fold);
R-1's OVERWRITE claim is corrected; C-001 now
claims the message body with the planning prefix held separately.

**Fold (2026-09-28, re-verify RL-1..RL-3).** The re-verify hand-back
(`reverify-ltz-opus-handback.json`, NEEDS_REMEDIATION, 1 S1) measured the VL-1
deferral admitting 13 STRING-function shapes that 830ef80e refused (RL-1): the
skip keyed on any function call instead of `nvl`/`ifnull`. The fix retypes only
two-argument `nvl`/`ifnull` (any case, `Nested` looked through) as `coalesce(a,
b)` — Spark's own rewrite, which DataFusion types correctly — and judges every
other function by its probed type. `upper('2024-01-01')`,
`concat('2024','-01-01')` and `nvl(NULL,'2024-01-01')` refuse with the recorded
`ins_l_str` body; each pin reds at 67b23744 and the M-D rerun reds the new pin
(M4). R-LTZ-10 is deleted: every shape it listed now refuses at planning with
Spark's text. RL-3: leading-zero fractions count precision from significant
digits (`0.05` → `DECIMAL(2,2)`, live-measured on Spark 4.1.2 alongside `0.5`
→ `(1,1)` and `1.50` → `(3,2)`).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | INT into a `TIMESTAMP` (LTZ) column refuses with Spark's recorded `ins_l_int` answer — error class, condition, SQLSTATE and message body, the RePark-only planning prefix pinned separately (R-LTZ-3) — on the VALUES door, and with the `CANNOT_SAFELY_CAST` class on SELECT, UPDATE, MERGE and DataFrame append; NULL, DATE, TIMESTAMP and TIMESTAMP_NTZ sources still store, as do `nvl`/`ifnull` over DATE and TIMESTAMP; typed NULLs refuse by declared type. | Rust `tests::ltz_store` (VALUES exact text incl. column-list, reordered and negative shapes plus the BIGINT probe wording; class on the other three doors; exact read-backs of the legal rows; typed-NULL exact text; the mixed-rows residue pin; `nvl`/`ifnull` store read-backs; `DECIMAL(2,1)`/`BIGINT` wording; STRING-function refusals with the `ins_l_str` body; `0.05` names `DECIMAL(2,2)`) and `test_ltz_store_int_1.py` (recorded body plus the separately pinned prefix; DataFrame append; `nvl`/`ifnull` store). | PROVEN | `cargo test -p repark-spark --lib tests::ltz_store`: 7 passed; the refuse test reds with the fix stashed (VALUES writes); the three fold pins red with the fold stashed (M3); the new STRING-function pin reds at 67b23744 and under M4. Facade file: 3 passed. |

## Mutation record (2026-09-28)

| # | Mutation | Red |
|---|---|---|
| M1 | Bypass the gate for LTZ targets (the unit predicate returns false) | `int_into_timestamp_refuses_on_every_door` reds (VALUES writes epoch micros), restored green |
| M2 | Stash the V-001 fold's gate change, keep the sibling pin | `typed_numeric_values_into_timestamp_refuse` reds (decimals write), restored green |
| M3 | Stash the verifier fold's gate change, keep the fold pins | `typed_numeric_values_into_timestamp_refuse`, `nvl_and_ifnull_over_temporal_values_store` and `fractional_and_large_integer_literals_name_spark_types` red, the other three green; restored green |
| M4 | Re-run the verifier's M-D: the gate skips every function call | `string_valued_functions_into_timestamp_refuse` reds, the other six green; restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ltz-store-int-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 is walked shape by shape: VALUES refusal equals Spark's recorded text on the test catalog, the other three SQL doors carry the class, the DataFrame append refuses, and every legal source reads back its exact value.
      artifacts: [crates/repark-spark/src/tests/ltz_store.rs, python/repark/tests/test_ltz_store_int_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: NULL, DATE, TIMESTAMP, TIMESTAMP_NTZ, explicit CAST (user intent still stores), positional and reordered column lists, signed integers, fractional numbers, strings and booleans are all exercised; partitioned inserts, arity mismatches, unresolvable columns and probe failures fall through to pre-change behavior by construction.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/tests/ltz_store.rs]
    - id: AT-3
      status: ATTACKED
      evidence: The refusal is a plan error with Spark's class and SQLSTATE; every shape the gate cannot map returns Ok, so no new failure mode reaches a legal write. Refused statements write nothing (seeded row intact, empty table stays empty).
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/tests/ltz_store.rs]
    - id: AT-4
      status: N/A
      justification: No shared or mutable state; each pin runs on a fresh warehouse in one session.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the gate reads table metadata and plans per-cell probes only.
    - id: AT-6
      status: ATTACKED
      evidence: No stored-format change; the epoch-micros silent write is closed and the pins assert refused statements leave prior data intact.
      artifacts: [crates/repark-spark/src/tests/ltz_store.rs, python/repark/tests/test_ltz_store_int_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; the gate loads one table schema per VALUES insert and plans at most one probe per non-literal LTZ cell, and only for tables that have an LTZ column.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency or crate edge; the gate reuses the shared matrix, the presented-schema helper and the void door's column-name helper, and adds a child module plus two lines in `void_type.rs` with no `lib.rs` change. TIMESTAMP_NTZ targets are skipped so NTZ-1 Slice 2 (#866) applies cleanly beside this change.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/void_type.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The refusal carries Spark's class, condition and SQLSTATE through the facade's stamped-message parse, and the exact-text pins red if the failure mode changes.
      artifacts: [crates/repark-spark/src/tests/ltz_store.rs, python/repark/tests/test_ltz_store_int_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Every new branch has a nameable input that changes the output (classifier unit tests, mapping and probe asserts, legal read-backs); M1 bypasses the gate and the refuse pin reds; the stash check reds the same pin on unfixed code; M3 stashes the verifier fold and the three fold pins red; M4 skips every function call and the new STRING-function pin reds. The bail-out branches (partitioned, arity, unmapped, probe failure) are deliberately unpinned: pinning them would pin the residual hole.
      artifacts: [crates/repark-spark/src/void_type/ltz_values_store.rs, crates/repark-spark/src/tests/ltz_store.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-LTZ-1 | Dated 2026-09-27 (critic V-002, owned by NTZ-1 #866's NTZ store gate, not this unit): an INT into a TIMESTAMP_NTZ cell beside an LTZ one still writes (`VALUES (0, TIMESTAMP '…', 1)` into `(id INT, c TIMESTAMP, n TIMESTAMP_NTZ)` commits one row); Spark refuses INT to NTZ (recorded `ins_n_sel_int` on the SELECT door). The gate deliberately skips non-LTZ targets, and the mixed table's LTZ cell still refuses. |
| R-LTZ-2 | Dated 2026-09-28 (verifier VL-3): `VALUES (3, TIMESTAMP '…'), (4, 5)` earns Spark's `INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE`/42000 (measured `ltzv-spark.json` key `mixed_rows`); RePark refuses `CANNOT_SAFELY_CAST`/KD000. Both write nothing. Residue pin: `mixed_timestamp_and_int_rows_refuse_without_writing`. |
| R-LTZ-3 | Dated 2026-09-28 (verifier VL-6): the facade refusal carries the RePark-only `Error during planning: ` prefix ahead of Spark's message body. Sibling of U9 R-37, which holds the same prefix for the two STRING side-insert steps. |
| R-LTZ-4 | Dated 2026-09-28 (verifier VL-5, no fix this round): every TIMESTAMP/DATE-literal cell pays a full `SELECT <cell>` planning probe, and every Iceberg VALUES insert loads its table twice (LTZ gate plus void gate). Measured on a debug build: 2000 TIMESTAMP-literal rows +15%, no change without an LTZ column. |
| R-LTZ-5 | Dated 2026-09-28 (verifier probe, pre-existing): `INSERT INTO l SELECT * FROM VALUES (8, 1)` writes epoch micros; Spark refuses `CANNOT_SAFELY_CAST` naming `INT` (measured `ltzv-spark.json` key `select_star_values`). |
| R-LTZ-6 | Dated 2026-09-28 (verifier probe, pre-existing): TIMESTAMP leaves inside STRUCT/ARRAY/MAP VALUES cells write epoch micros. Spark's answer unmeasured. |
| R-LTZ-7 | Dated 2026-09-28 (verifier probe, pre-existing): with `caseSensitive=true`, `INSERT INTO l (ID, C) VALUES (0, 1)` skips the unmatched columns and writes epoch micros. Spark's answer unmeasured. |
| R-LTZ-8 | Dated 2026-09-28 (verifier probe, pre-existing): `spark.sql.storeAssignmentPolicy=LEGACY` is ignored on every door; Spark disallows LEGACY on V2 tables (`_LEGACY_ERROR_TEMP_1000`, measured `ltzv-spark.json` key `legacy_values`). |
| R-LTZ-9 | Dated 2026-09-28 (verifier VL-1 SELECT door, pre-existing on base): `nvl`/`ifnull` over temporal values via `INSERT … SELECT` refuses (`Utf8 is not ANSI-store-assignable`); Spark writes (measured `ltzv-spark.json` key `nvl_col_select`). Fixing it needs the global `nvl`/`ifnull` retype, out of this round. |
| R-1 | Dated 2026-09-28 (out of scope, separate work orders): the VALUES residual stays open for every non-LTZ target (`VALUES (true)` into INT still writes `1`); partitioned VALUES inserts into TIMESTAMP columns are not judged; `INSERT OVERWRITE … VALUES` refuses through the overwrite gate with RePark's own wording rather than Spark's (corrected 2026-09-28; Spark's text unmeasured) while BY NAME evolution VALUES bypass the `spark_ast` gate site and stay silent; BOOLEAN SELECT dies in DataFusion's own plan error without the class; a decimal literal (`1.5`) into TIMESTAMP stores through UPDATE because `spark_update_type_name` has no DECIMAL name; MERGE over an empty target passes as a no-op where Spark refuses at analysis; the native `repark.sql` door has no store-assignment gate at all. |

**CI fix round (2026-09-28, PR #875).** The fix also closes U9 R-1: both `side-select` rows replay EQUAL and both `side-insert-string` steps refuse with Spark's class, condition, SQLSTATE and message body; the prefix-only text difference is held as U9 R-37.
