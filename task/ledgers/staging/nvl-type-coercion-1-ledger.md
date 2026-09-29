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
| C-002 | The Spark door resolves the six `nvl`-family spellings to Spark-widening UDFs (`spark_nvl.rs` tables, `spark_nvl_udf.rs` kernels, `spark_nvl_rule.rs` rewrite seated before `type_coercion` on the session and `F.expr` doors); the facade calls one native `_scalar` per spelling; the headline cell answers `timestamp`. | `test_nvl_date_timestamp_headline` green; module rows in the touched `map.md` files. | PROVEN | Headline pin green; 17 facade `typeof` ops pinned; `typeof` spells intervals. |
| C-003 | Every in-scope differing cell flips: 372 strict (type+value or refusal class) and 69 loose (both sides raise, runtime-cast class differs structurally); struct/map cells pin typeof plus collected values. | The 756 pins in `test_nvl_type_coercion_1.py` green. | PROVEN | 756 passed 2026-09-29. Loose cells assert raises; Spark classes recorded in the JSON. |
| C-004 | Residuals outside the change surface are ledgered, not absorbed: struct/map→string CAST gap, array-timestamp-element format, interval representation + format, YearMonth-literal parse gap, plain `if`/`CASE`/`=`/CAST/interval-literal methodology probes. | This ledger's residual table + the five known-divergence pins. | PROVEN | Five `test_known_divergence` pins lock current behavior with Spark values beside. |
| C-005 | Nothing else moves: zero guard breaks across the matrix; every `coalesce` cell keeps its base answer (95 guard pins); all shared refusals keep refusing; zero existing corpus pins change. | Rerun diff `GUARD BREAK: 0`; coalesce guards green; existing suite green. | PROVEN | The order-contract test gains the fifth pre-coercion seat (test edit, not an answer change). |
| C-006 | The pins are genuine: the headline pins fail on the base tree, and the 25-statement neighbour file answers identically on base and head except the two intended `zeroifnull` SQL-door flips (unresolved routine → answers). | Mutation red output + neighbour diff pasted under Evidence. | PROVEN | 2 failed on base as required; neighbour 23/25 identical, 2/25 intended flips. |
| C-007 | The lane gate is green as written. | `bash /tmp/xnvl/gate.sh` exit 0. | PROVEN | GATE GREEN (see hand-back). |

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
