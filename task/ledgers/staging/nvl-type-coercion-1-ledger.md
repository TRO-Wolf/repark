# Unit ledger — NVL-TYPE-COERCION-1 · `nvl`-family Spark type widening

**Date:** 2026-09-29 · **Branch:** `fix/nvl-type-coercion-1` · **Base:** `adc26586` (`origin/main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The `nvl` family mistypes mixed-type calls on the Spark door:
`typeof(nvl(DATE '2024-01-01', TIMESTAMP '2024-01-01 05:00:00'))` answers
`string` where Spark 4.1.2 answers `timestamp`. The step-0 matrix (C-001) measures
~400 oracle cells; this unit makes `nvl`/`ifnull` widen like `coalesce`, `nvl2`
widen like `if`, `nullif` compare after widening and return its first argument's
type, and `zeroifnull`/`nullifzero` behave as `coalesce(arg, 0)` /
`nullif(arg, 0)` — on every door (SQL, facade, view, INSERT).

**Not in this step:** `coalesce` itself (guarded, keeps DataFusion widening);
the struct/map→string CAST gap; array-timestamp-element string format; interval
literal representation (MonthDayNano) and interval→string format; YearMonth
interval-literal parsing; plain `if`/`CASE`/`=`/CAST methodology probes;
`STATUS.md`, `briefs/`, `.github/`, `Cargo.toml`, `Cargo.lock`.

## PROPOSITION LEDGER — NVL-TYPE-COERCION-1 — 2026-09-29

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The oracle matrix is measured on live PySpark 4.1.2 (ANSI on, UTC + America/New_York sessions): `nvl`/`ifnull` widen like `coalesce` (incl. date+timestamp→timestamp, date+ntz→ntz, timestamp+ntz→timestamp, decimal widening, recursive complex); `nvl2` branches widen like `if`; `nullif` compares after widening and returns its first argument's type; `zeroifnull` is `coalesce(arg, 0)`; `nullifzero` is `nullif(arg, 0)`; refusals carry `DATATYPE_MISMATCH.*` / `WRONG_NUM_ARGS.*` classes. | The recorded Spark JSONs + the widening table under Evidence. | PROVEN | Measured 2026-09-28/29, PySpark 4.1.2 `local[1]`, `spark.sql.ansi.enabled=true`. Recorded beside the pins in `test_nvl_type_coercion_1_spark.json`. |
| C-002 | The Spark door resolves the six `nvl`-family spellings to Spark-widening UDFs (`spark_nvl.rs` tables, `spark_nvl_udf.rs` UDFs, `spark_nvl_rule.rs` rewrite seated last on the session and `F.expr` doors); the facade calls one native `_scalar` per spelling; the headline cell answers `timestamp`. | `test_nvl_date_timestamp_headline` green; module rows in the touched `map.md` files. | PROVEN | Headline pin green; 17 facade `typeof` ops pinned; `typeof` spells intervals. Fold 2026-09-29: evaluation moved from the kernels into the rule's `CASE` lowering; the seat moved last. |
| C-003 | Every in-scope differing cell flips: 372 strict (type+value or refusal class) and 69 loose (both sides raise, runtime-cast class differs structurally); struct/map cells pin typeof plus collected values. | The pins in `test_nvl_type_coercion_1.py` green. | PROVEN | 781 passed 2026-09-29 (fold keeps every strict and loose flip, adds VN pins). Loose cells assert raises; Spark classes recorded in the JSON. |
| C-004 | Residuals outside the change surface are ledgered, not absorbed: struct/map→string CAST gap, array-timestamp-element format, interval representation + format, YearMonth-literal parse gap, plain `if`/`CASE`/`=`/CAST/interval-literal methodology probes. | This ledger's residual table + the five known-divergence pins. | PROVEN | Five `test_known_divergence` pins lock current behavior with Spark values beside. |
| C-005 | Nothing else moves: zero guard breaks across the matrix; every `coalesce` cell keeps its base answer (95 guard pins); all shared refusals keep refusing; zero existing corpus pins change. | Rerun diff `GUARD BREAK: 0`; coalesce guards green; existing suite green. | PROVEN | The order-contract test gains the fifth pre-coercion seat (test edit, not an answer change). |
| C-006 | The pins are genuine: the headline pins fail on the base tree, and the 25-statement neighbour file answers identically on base and head except the two intended `zeroifnull` SQL-door flips (unresolved routine → answers). | Mutation red output + neighbour diff pasted under Evidence. | PROVEN | 2 failed on base as required; neighbour 23/25 identical, 2/25 intended flips. |
| C-007 | The lane gate is green as written. | `bash /tmp/xnvl/gate.sh` exit 0. | PROVEN | GATE GREEN (see hand-back). |
| C-008 | VN-1: `nvl`/`ifnull`/`nvl2`/`zeroifnull` lower to short-circuit `CASE`; an untaken branch never evaluates (`nvl(1, 1/0)` answers; div-by-zero, overflow, `assert_true`, bad-cast and column forms) and never casts. | The `vn1` pins green; the verify replay lazy cells match Spark; the eager mutant goes red on exactly the lazy cells. | PROVEN | 10 `vn1` pins green; replay `lazy/*` all FIX/OK; mutant (rule emits the plain UDF call) reds 21 pins incl. all 6 `nvl` expression-lazy cells, and reds `nvl2_div0`/`zeroifnull_div0` on the verify rig. |
| C-009 | VN-2: widening casts are session-zoned Spark casts; string→timestamp reads the session zone (UTC, America/New_York) on SELECT paths. | The zone-matrix pins green in both harness zones; the verify replay shows 0 NEW on zone cells; the diff-probe tz controls do not move. | PROVEN | Harness UTC + NY matrices green at fold head; replay 0 NEW incl. `ty/ny/*`; `x/tz_kol` unmoved (head==base). Kolkata-oracle and Iceberg-value coverage belong to the fold round, not re-measured here. |
| C-010 | VN-3: string-leaf casts follow Spark (trim, `d`/`f` suffix, partial dates, boolean vocabulary, `on` refuses) and mistyped leaves raise `CAST_INVALID_INPUT` naming Spark's class. | The `vn3` pins green; the verify replay string-cast cells match Spark. | PROVEN | `vn3` pins green at fold head incl. `typeof(nvl('abc', 2))` and `zeroifnull` over bad strings; replay string cells FIX/OK. |
| C-011 | The widened zero survives re-analysis: `zeroifnull` emits `CAST(0 AS W)`, and `zeroifnull`/`nullifzero` `typeof` matches Spark for every harness input type (int, bigint, double, decimal, string, date, ts, ntz, bool, nulls, tiny, small, float). | The 3 snapshot-red `zeroifnull(CAST(NULL AS STRING))` pins pass unmodified; the harness typeof sweep green; the answers match the C-001 recorded oracle. | PROVEN | 781 pins green incl. the 3 fixed pins (assertions untouched); the recorded live-Spark oracle answers `bigint`/`int`/`string`/`int` for the spot cells. |
| C-012 | VN2-1: every `nvl`-family argument evaluates at most once. `nvl`/`ifnull` lower to `coalesce` of the two `nvl_cast` branches, `zeroifnull` to `coalesce` with the widened zero, scalar `nullif` to DataFusion's own `nullif` over shaped operands; `nvl2` keeps its single-use `CASE`, `nullifzero` keeps the pick form like Spark. | The 8 `rand()`/`uuid()` repro pins assert 0 rows; the CASE-lowering mutant reds the sensitive 7. | PROVEN | 8/8 pins green; mutant reds 7/8 — `nullif`+`rand()` stays 0 under both because the engine shares identical `rand()` draws (`SELECT rand(), rand()` returns twins; the attack's own `nd/rand_nullif_hit` is 0 at old head too), so the `uuid()` pin carries the `nullif` proof. |
| C-013 | VN2-2: nested lowerings plan in linear time. Depth 4/8/12/16 plan in under 0.2 s each; the depth-12 `nvl`+`nullif` pin plans in under 1 s. | Timed `EXPLAIN` at depths 4, 8, 12, 16; the depth pin. | PROVEN | nvl 0.018/0.019/0.029/0.044 s, nullif 0.155/0.038/0.058/0.085 s (EXPLAIN, second run); depth pin green. |
| C-014 | VN2-3: `ROLLUP`/`CUBE` over the `nvl` family plans: grouping-set inner expressions keep their names across the rewrite. | Exact-row `ROLLUP(nvl)`, `CUBE(ifnull)`, `ROLLUP(nullif)` pins; the re-verify grouping cells plan. | PROVEN | 3/3 grouping pins green with Spark-equal row sets; `gs/*` attack cells plan. |
| C-015 | VN2-4: `typeof` skips its planned-type fold when the argument holds `greatest`/`least` over a string, restoring the refusal (`CAST_INVALID_INPUT`, as base) where Spark refuses `DATA_DIFF_TYPES`. | The three repro cells refuse; the 34 TSN `typeof\|greatest\|least` cells return to 93f5d499 behavior; no fold-created Spark win exists to lose. | PROVEN | Repro cells refuse; `greatest('1',2)`/`greatest('a','b')`/`typeof(nvl('abc',2))` still answer; TSN gained set holds no `greatest`/`least` cell. |

## Evidence

### C-001 widening table (live Spark 4.1.2, ANSI on)

`nvl`/`ifnull` follow `coalesce`: string+integral→bigint, string+fractional→double,
string+date→date, string+timestamp→timestamp, string+boolean→boolean,
string+binary→binary; date+timestamp→timestamp; date+ntz→ntz; timestamp+ntz→timestamp;
decimal widening with the 38-cap (`decimal(10,2)`+int→`decimal(12,2)`);
array/struct/map widen recursively with tight map keys. `nvl2` widens its two
branches like `if`. `nullif` widens for the comparison (structs positionally) and
returns its first argument's type; maps refuse `INVALID_ORDERING_TYPE`.
`zeroifnull(x)` is `coalesce(x, 0)` (`decimal(10,2)`→`decimal(12,2)`);
`nullifzero(x)` is `nullif(x, 0)`. Unpicked `nvl` branches never cast
(`nvl(1, 's')` answers `1`); `nullif` with a NULL side answers without casting.
Arity refusals name `WRONG_NUM_ARGS.WITHOUT_SUGGESTION`.

### C-003 flip counts (rerun 2026-09-29)

`guard_ok: 230`, `flipped strict: 372`, `flipped loose: 69`, `GUARD BREAK: 0`.
Residual misses, all expected: 16 `coalesce` carve-outs (head==base), 8
methodology probes (head==base), 45 struct/map cells (typeof + values pinned
through collect; the probe's `CAST(... AS STRING)` wrapper hits the CAST gap),
1 array-timestamp-element format cell (t-only pin + divergence pin), 2 interval
cells (divergence pins).

### C-004 residual table

| Residual | Shape | Owner |
|---|---|---|
| struct/map→STRING cast raises | `Unsupported CAST ... to Utf8View`, pre-existing at base | CAST engine |
| array timestamp elements format `[2024-01-01T00:00:00Z]` | value format only; instant correct | CAST engine |
| day intervals parse as MonthDayNano; `INTERVAL '1' DAY` stringifies `1 days` | representation + format | parser / CAST engine |
| YearMonth interval literals refuse at parse | `Unsupported Interval Expression`, pre-existing | parser |
| plain `if`/`CASE`/`=`/CAST/interval-literal probes | methodology, never in scope | — |

### C-006 mutation + neighbour (2026-09-29)

Base tree (`git stash -u`, rebuilt module): `test_nvl_date_timestamp_headline`
and `test_select_utc[sel/UTC/nvl/p00/fwd]` both FAILED as required (2 failed).
Neighbour file (`/tmp/nvl_neighbour.py`, 25 statements): 23/25 byte-identical
base vs head; `n20`/`n21` (`zeroifnull` SQL) flip `UNRESOLVED_ROUTINE` → answers
by design — the SQL door gains the routine.

### C-002 files

New: `crates/repark-functions/src/spark_nvl.rs`, `spark_nvl_rule.rs`,
`spark_nvl_udf.rs`. Wired: `lib.rs` (register), `cardinality.rs` (pick fold),
`repark-spark/src/extension.rs` + `extension/tests.rs` (fifth seat),
`column/function_dispatch.rs` (family arm), `column/expr_build.rs` (`F.expr`
seat), `column/door_parity_tests.rs` (`SCALAR_NAMES`),
`functions_expr.py` (one `_scalar` per spelling), `time_family.rs`
(`typeof` intervals).

### Verifier fold VN-1..VN-3 + zero shield (2026-09-29)

Snapshot `7882493e` (fold, CASE lowering + Spark casts) plus this round's
`widened_zero` shield (`spark_nvl_rule.rs`: the zero emits as `CAST(0 AS W)`
because both doors analyze twice and the literal rules narrow a bare
`Int64(0)` on the second pass). VN-4 (`-0.0` vs `0.0` in `nullif`) and VN-5
(double→decimal store gaps) stay residues: pre-existing, out of this fold.

Replay at fold head (`probe.py` repark leg, 735 cells vs recorded base+spark):
0 NEW, 375 FIX, 13 CHG, 142 PRE, 205 OK. The 13 CHG are all outside the
zeroifnull zero path (5 interval-`typeof` representation cells, 1
nullif NTZ-gap value cell, 2 `nullifzero(-0.0)` VN-4 cells, 5 Iceberg
write-shape cells where base lacks the routine or errors first); every
`zeroifnull` replay cell is FIX/OK/PRE and no replay cell asserts a
bigint-zero `typeof`, so the shield moves nothing there.

DIFF-PROBE at fold head (513 statements vs recorded base+spark): 0
`REGRESSION*` verdicts; the 3 `REGRESSION-ddl?` rows are byte-identical to
the original round's run; the 10 `NO-SPARK` rows differ in one nullability
flag only (CASE-lowering signature, identical rows); all `zeroifnull` rows
are `SPARK-CONFIRMED*`/`NEITHER*` with rows matching Spark.

Eager mutant (rule emits the plain UDF call instead of `CASE`, widening
kept): 21/781 pins red — all 6 `nvl` expression-lazy `vn1` cells plus 15
`zeroifnull` answer cells — and the verify rig reds `nvl_div0`,
`nvl2_div0_else/then`, `zeroifnull_div0` and the column forms with
`DIVIDE_BY_ZERO`, while `coalesce`/`if`/`CASE` controls still answer. The
two `nvl2` `vn1` pins pin cast-laziness, which the kernel shares, so that
mutant cannot move them; the rig's `div0` cells carry the `nvl2`
expression-laziness proof instead.

### Re-verify VN2-1..VN2-4 (2026-09-29)

The rule lowers `nvl`/`ifnull` to `coalesce` of the two `nvl_cast`
branches, `zeroifnull` to `coalesce` with the widened zero, scalar
`nullif` to DataFusion's own `nullif` over shaped operands, while `nvl2`
keeps its single-use `CASE`; grouping-set inner expressions rewrite
with names kept, and `typeof` skips its fold under `greatest`/`least`
over a string. All 811 pins pass, including the 12 new VN2 pins (8
`rand()`/`uuid()` single-evaluation repros, 1 depth-12 nesting pin, 3
`ROLLUP`/`CUBE` grouping pins).

Single-evaluation mutant (rule restored to `CASE` lowering): reds 7 of
the 8 VN2-1 pins. `nullif`+`rand()` stays 0 under both lowerings because
the engine shares identical `rand()` draws (`SELECT rand(), rand()`
returns twins; 0 at old head too), so the `uuid()` pin carries the
`nullif` proof.

Depth-12 `nvl`+`nullif` pin plans in under 1 s; timed `EXPLAIN` at
depths 4/8/12/16 answers in under 0.2 s each. Five-deep execution on
100k rows, identical workload both trees (5 samples base, 3 head):
base `CASE` median 0.6526 s, head `coalesce` median 0.1914 s — head runs
at 0.29x base, inside the 1.2x envelope at 3.4x faster.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: nvl-type-coercion-1
  complete: true
  reattested: [AT-1, AT-2, AT-3, AT-6, AT-7, AT-8, AT-10]
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-007 plus re-verify C-012..C-015 walked one by one against behavior — the oracle matrix measured on live PySpark 4.1.2 in two session zones, the widening verified per cell, the residuals ledgered with divergence pins, the guards re-run to zero breaks, the mutation run red-first on the base tree, the CASE mutant red on the re-verify pins, and the gate run as written; every clause is PROVEN and cited from the maps.
      artifacts: [task/ledgers/staging/nvl-type-coercion-1-ledger.md, python/repark/tests/test_nvl_type_coercion_1.py]
    - id: AT-2
      status: ATTACKED
      evidence: Boundary cells actually exercised — NULL and typed-NULL on every side, uncastable strings picked and unpicked, decimal 38-cap pairs, date/timestamp/ntz in UTC and America/New_York, struct/map/array recursion, zero-arg and over-arity calls; the answer pins assert Arrow value AND type per cell.
      artifacts: [python/repark/tests/test_nvl_type_coercion_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Refusal cells raise the recorded Spark classes verbatim where the engine owns the check (DATATYPE_MISMATCH, WRONG_NUM_ARGS, 101 strict pins); runtime cast failures raise while Spark names CAST_INVALID_INPUT (55 loose pins assert raises with the Spark class recorded beside); invented nulls in the kernel raise CAST_INVALID_INPUT.
      artifacts: [python/repark/tests/test_nvl_type_coercion_1.py, crates/repark-functions/src/spark_nvl_udf.rs]
    - id: AT-4
      status: ATTACKED
      evidence: The facade resolves the same kernels as the SQL door (SCALAR_NAMES ratchet, door-parity suite green); each pin session is created per fixture with explicit timeZone and ANSI settings and the INSERT leg stops its session; temporal kernel casts read the session zone from live config.
      artifacts: [python/repark/tests/test_nvl_type_coercion_1.py, crates/repark-python/src/column/door_parity_tests.rs]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — pure type widening over values already in the engine.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per oracle cell through collect and typeof; facade display keeps the nvl/ifnull/nvl2/nullif/zeroifnull/nullifzero spellings via one native _scalar call each; struct/map answers pinned through collected values, not the stringifying wrapper.
      artifacts: [python/repark/tests/test_nvl_type_coercion_1.py, python/repark/src/repark/spark/functions_expr.py]
    - id: AT-7
      status: ATTACKED
      evidence: Five-deep nvl on 100k rows runs at 0.29x the CASE-lowering base (0.1914 s vs 0.6526 s medians), inside the 1.2x envelope; depth-12 nesting plans in under 1 s and depths 4/8/12/16 explain in under 0.2 s each.
      artifacts: [task/ledgers/staging/nvl-type-coercion-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: The nullif rule seats fifth before type_coercion on both doors with the contract test extended; file-size baselines move DOWN only (functions_expr.py 2171 -> 2170 with the CAP-1 mirror in lockstep); the fixture holds only asserted cells at 772 lines.
      artifacts: [crates/repark-spark/src/extension/tests.rs, scripts/check_lib_py.py, python/repark/tests/test_nvl_type_coercion_1_spark.json]
    - id: AT-9
      status: N/A
      justification: No log-format or diagnosis-path change; refusals carry Spark error classes and SQLSTATEs through the existing typed exception families.
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — the headline pins failed on the base tree (typeof answered string) and pass at head; the neighbour file answers 23/25 identically with the 2 zeroifnull-SQL flips intended; no dead branch ships (every fallback returns the unshifted or uncast value by construction).
      artifacts: [python/repark/tests/test_nvl_type_coercion_1.py, task/ledgers/staging/nvl-type-coercion-1-ledger.md]
```
