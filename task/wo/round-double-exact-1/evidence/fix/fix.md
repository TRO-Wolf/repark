# ROUND-DOUBLE-EXACT-1 — the build

Builder `claude-opus-5-5`, branch `fix/round-double-exact-1`. The fix commit is `7c12b87c`, on main
`954423e0`. The build follows the step 0 inventory, the critic's priority list (Muse 1.3, xhigh, `GAPS`,
seat judged correct) and the orchestrator's rulings:
- four product files;
- the native door unchanged and recorded;
- a column scale refused as Spark refuses it;
- FLOAT `bround` returns FLOAT;
- the INT ANSI overflow as a separate card.

## The seat (four product files)

| file | change |
|---|---|
| `crates/repark-common/src/java_double/mod.rs` | `JavaRounding`, `java_double_round` and `java_float_round` beside step 1's K1 (detail below) |
| `crates/repark-functions/src/spark_math/bround.rs` | `SparkExactRound`, the `round` UDF over datafusion-spark's `SparkRound` (detail below) |
| `crates/repark-functions/src/spark_math.rs` | `round_udf()` joins `functions()`, after datafusion-spark's set in `register_all`, so it replaces `round` by name on the SQL, `F.expr`, `selectExpr` and `filter(str)` doors |
| `crates/repark-python/src/column/function_dispatch.rs` | `F.round` and `Column.round` build `call_round`. The file is 993 lines, under the 1,000-line ceiling |

**The kernel** (`mod.rs`):
- **The rule:** JDK 17's text of the value (of the widened double, for a FLOAT), rounded half up or half
  even at an `i32` scale.
- **Back conversion, done once:**
  - an unscaled value under 2^53 and `|s| ≤ 22`: one IEEE divide or multiply;
  - a FLOAT under 2^24 and `|s| ≤ 10`: an `f32` divide or multiply, never an `f64` narrowed;
  - otherwise: `f64::from_str` or `f32::from_str` on the digits.
- **Zero and non-finite:** a zero result is `+0.0`, and NaN and ±Infinity pass through.
- **Fast path:** the binary product when it is more than 4 ulps from a half, with no libm call.
- **Unchanged guard:** when `abs(v)·10^s ≥ 2·10^16`, the text has no more than `s` fractional digits and
  the input comes back.

**The UDF** (`bround.rs`):
- its FLOAT and DOUBLE arms call the kernel;
- every other type delegates to datafusion-spark's arm;
- `bround`'s float arms call the kernel half even, and `bround(FLOAT)` returns FLOAT;
- a column scale refuses with `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT`.

## Pins, red first

| pin | on main | on the head |
|---|---|---|
| `crates/repark-common/src/java_double/round_tests.rs`: Spark-measured cells, zero sign, non-finite values, FLOAT, and every fast path against the digit path (200,000 random values; 45 scales × 400 half points × ±3 ulps) | does not compile (the kernel is new) | 6 passed |
| `crates/repark-functions/src/spark_math/round_tests.rs`: the UDF through `register_all` | 5 failed, 1 passed ([raw/rust-red-summary.txt](raw/rust-red-summary.txt)). The one that passes pins the unchanged INT and DECIMAL arms | 6 passed |
| `python/repark/tests/sql/test_round_double_exact_1.py`: the step 0 values × 34 scales on every Spark door, in SELECT, folded literal, `WHERE`, `GROUP BY`, every store, NULL scale, column scale, the card, a view, a subquery and `F.round` of an INT, in both ANSI modes, plus the native gap | 262 failed, 30 passed ([raw/facade-red-summary.txt](raw/facade-red-summary.txt)) | 292 passed |

The facade test's Spark answers come from the model, which equals Spark in all 82,720 step 0 SELECT cells and
in all 45,120 extreme-scale cells. The model starts from Spark's recorded JDK 17 text, and the DECIMAL stores
use Spark's recorded answers (`round_double_exact_1_spark_oracle.json`, written by
[make_oracle.py](make_oracle.py)). Every literal is a `CAST('…' AS DOUBLE)` spelling (critic M8).

## The step 0 table on the head

[moves.py](moves.py) re-runs the step 0 probes on the head package and compares every cell with main's
answer and with Spark's ([raw/moves-head.json](raw/moves-head.json)):

| verdict on main → on the head | value contexts, both modes | misc cells, both modes |
|---|---|---|
| EQUAL → EQUAL | 816,208 | 232 |
| **EQUAL → DIFF (the halt)** | **0** | **0** |
| DIFF → EQUAL | 189,424 (94,712 per mode) | 258 |
| DIFF → DIFF | 165,842, all on the native door | 338 |

**What still differs:**
- **The native door, recorded as a gap** (orchestrator ruling 2): 11,158 `round` cells per mode, 71,763
  `bround` cells (no `bround`), and `ROUND(v, 309)` answering NaN.
- **Controls left as they are:** the INT overflow with ANSI on (card ROUND-INT-ANSI-OVERFLOW-1), the
  DECIMAL types, and the decimal-literal types.
- **The string-argument side finding:** `ROUND('1.005', 2)`.

Two head builds, before and after the fast path lost its libm calls, give identical answers in every cell.
The committed build differs from the second only by two lint-only edits (a `&'static str` and a `map_or_else`).

## Cost: the brief's halt fires

**Method.** Five alternating runs of main (`base`), the head (`head`, `7c12b87c`) and main again (`base-aa`,
the A/A control). Each sample ran under `flock -x measure`, the link lock and both cargo locks, on cores
32-47, after waiting 60 s whenever load1 was above 4. The harness is [cost_run.sh](cost_run.sh) and
[cost_sample.sh](cost_sample.sh) with [cost.py](cost.py); the raw samples are
[raw/cost-samples.jsonl](raw/cost-samples.jsonl) and [raw/cost-round.jsonl](raw/cost-round.jsonl), summarised by
[cost_summary.py](cost_summary.py).
- **The C-012 set:** `replay_work.py`, the whole wall.
- **The 10M-row rows:** an Iceberg table of 10,000,000 rows, each query's median of three after one warm-up:
  - `v` is `id · 1.2345678901 / 1000`, the ordinary case;
  - `p` is `(id % 100000) / 1000`, prices, with exact halves at every tenth value;
  - `f` is `v` as FLOAT;
  - the stores write 2,000,000 rows.

| row | runs | base median (s) | head median (s) | head/base | base-aa/base |
|---|---|---|---|---|---|
| C-012 `c012p6` | 5 | 38.41 | 38.57 | **0.997** | 1.002 |
| C-012 `c012rest` | 5 | 41.88 | 41.92 | **1.004** | 1.014 |
| `sum(round(v, 2))`, 10M DOUBLE | 5 | 0.0336 | 0.0509 | **1.490** | 1.006 |
| `sum(round(v, 18))` | 5 | 0.0346 | 0.0353 | **1.021** | 1.149 |
| `sum(bround(v, 2))` | 5 | 0.0522 | 0.0509 | **0.974** | 1.036 |
| `sum(round(f, 2))`, 10M FLOAT | 5 | 0.0201 | 0.0416 | **1.720** | 1.080 |
| `sum(round(p, 2))`, prices | 5 | 0.0321 | 0.0614 | **1.909** | 0.951 |
| store `round(v, 2)` into DOUBLE, 2M | 5 | 0.0566 | 0.0571 | **1.032** | 0.976 |
| store `round(v, 2)` into `DECIMAL(10,2)`, 2M | 5 | 0.0630 | 0.0673 | **1.092** | 0.999 |

The ratio column is the median of the per-run head/base ratios.

**Rows above 1.05x:**
- **The plain `round` projections:** 1.49x for DOUBLE, 1.72x for FLOAT, and 1.91x for prices. The queries
  are tens of milliseconds, so the ratio is close to the per-value cost of the kernel against
  datafusion-spark's multiply-round-divide. That is about +1.7 ns per DOUBLE value and +2.2 ns per FLOAT
  value.
- **Prices** pay the digit path at every exact half (one value in ten).
- **The `DECIMAL(10,2)` store:** 1.09x.

**What holds 1.05x at 10M rows:** the C-012 set, `round(v, 18)` (the unchanged guard) and `bround`, which was
already paying a per-row builder. At 100M rows only the C-012 set holds (below).

**The verifiers' re-run at 100M rows.** Both PR #1337 verifiers (Muse 1.3, xhigh, both PASS) re-ran `cost.py`
with `ROWS = 100,000,000` and `STORE_ROWS = 20,000,000`. Each ran five alternating runs of main `d1679e4f`, the
head `d859b961` and a second copy of main as the control, under the exclusive measure lock, verifier 1 on 16
cores and verifier 2 on 10. The ratio is the median of the per-run head/base ratios.

| row | verifier 1 head/base | control | verifier 2 head/base | control |
|---|---|---|---|---|
| `sum(round(v, 2))` | **1.751** | 1.013 | **1.717** | 1.012 |
| `sum(round(v, 18))` | **1.168** | 0.984 | **1.145** | 1.019 |
| `sum(bround(v, 2))` | **1.057** | 0.990 | **1.051** | 0.954 |
| `sum(round(f, 2))`, FLOAT | **2.440** | 1.019 | **2.702** | 0.979 |
| `sum(round(p, 2))`, prices | **2.264** | 1.010 | **2.393** | 1.010 |
| store into DOUBLE, 20M | **1.177** | 1.015 | **1.190** | 1.018 |
| store into `DECIMAL(10,2)`, 20M | **1.164** | 1.021 | **1.116** | 0.980 |

- **The added time per value matches the 10M rows:** +1.5 to +2.0 ns per DOUBLE value and +1.7 to +2.3 ns per
  FLOAT value.
- **The ratios are higher** because fixed overhead dilutes less at 100M rows.
- **So `round(v, 18)`, `bround` and both stores do not hold 1.05x** at this length.
- **Noise:** verifier 1's control was within 0.984 to 1.021. Verifier 2's control spread on the DOUBLE store
  reached 0.912 to 1.250, so that verifier calls its store row inconclusive.
- **The raw samples** are in the verifiers' scratch work, outside the repository.

**A tried and dropped optimisation.** `#[inline]` and `#[cold]` hints on the kernel changed nothing beyond
noise: 1.50x, 2.17x and 1.67x on the same three rows ([raw/cost-inline-samples.jsonl](raw/cost-inline-samples.jsonl),
[raw/cost-inline-round.jsonl](raw/cost-inline-round.jsonl)). They are not in the commit.
