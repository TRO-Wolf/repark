# Card TA-SINGLE-SERIES-PARALLEL-1 — one long TA series uses one core

**Date:** 2026-10-04 · **Filed by:** Grok 4.7, guided execution, clerk grade · **Source:** owner,
2026-10-04: "New card TA-SINGLE-SERIES-PARALLEL-1, design grade, next TA item after v1.5.2." No
product code in this filing.

## Why

**The workload:** one instrument (ES futures), 1,000,000 rows, with the `ta.*` indicators computed in dependent `withColumns` levels and no partition column. The owner's data is one contract series, so **partitioning is not a fix**.

**What the owner measured:**
- **Setup:** the 1.5.2 wheel against polars_talib and polars 1.43.1, using the owner's reTest benchmark (materialised: `.eager()` for RePark, `.collect()` for polars).
- **Timings:**

  | Engine | Wall | CPU |
  |---|---|---|
  | RePark | 0.49 s | 0.50 s |
  | polars_talib | 0.12 s | 0.76 s |

  One `ta.ema(close, 21)` alone takes 0.026 s.
- **The plan:** `CoalescePartitionsExec`, then three `WindowAggExec` on **one** partition, each evaluating its independent indicators serially.

**RePark does less total work than polars and loses on parallelism only.**

### Measured environment (orchestrator, 2026-10-04)

- **reTest venv** (`~/CodeRepos/myTemp/reTest/.venv`): `repark 1.5.2`, `polars 1.43.1`, `polars_talib 0.1.6`, `pyarrow 25.0.1`.
  - The owner's ruling names polars_talib **0.1.5**, the version the TA goldens were recorded with. The venv has **0.1.6**.
  - The sketch lane records which version each timing used, and uses the owner's venv for the gate.
- **Data:** `test_futures.parquet` has 1,000,000 rows and 14 distinct `contract_symbol` values (`ESZ08`, `ESH08`, `ESH09`, `ESM08`, `ESU08`, …): one instrument across rolled quarterly contracts, computed as one series.
- **Session config** (`repark.toml`, `[default.session]`): `target_partitions = 16`, `batch_size = 64000`.
- **The RePark side of the script:**
  - four `withColumns` levels, of which the first three carry TA calls;
  - 17 `TA.*` calls: `ema` ×7 (two of them `ema(close, 21)`), `rsi` ×3, `sma` ×3, `adx` ×3, `linearreg` ×1, plus one `trange`;
  - the owner's ruling says 18 indicators. The sketch counts the evaluated window expressions in the physical plan and records that number.
- **The polars side:** 16 `plta.*` calls.

### The exact script (owner's `benchmark_script.py`, 2026-10-04 11:30 EDT)

The text is verbatim except that the absolute home-directory prefix of `CONF_FILE` and
`DATA_FILE` is written as `~`. The original uses the absolute path.

```python
import os
import boto3
import repark as rp
from repark import DataFrame as ReparkDataFrame

from repark import ReparkSession
from repark.functions import (
    col,lit,when, row_number, concat, coalesce
)
from repark import Window
from repark import functions as F
from repark import ta as TA
import polars as pl
import polars_talib as plta
import time


CONF_FILE = "~/CodeRepos/myTemp/reTest/repark.toml"
DATA_FILE = "~/CodeRepos/myTemp/reTest/test_futures.parquet"


repark = (
    ReparkSession.builder
    .appName("Benchmark Script")
    .configFile(CONF_FILE)
    .getOrCreate()
)

def load_repark(file_path=DATA_FILE) -> ReparkDataFrame:
    return (
        repark.read.format('parquet').load(file_path)
        .select(
            'ticker_epoch',
            'contract_symbol',
            'event_timestamp_utc',
            'open',
            'high',
            'low',
            'close'
        )
        .sort('event_timestamp_utc')
        .eager()
        .lazy()
    )


def load_polars(file_path=DATA_FILE):
    return (
        pl.scan_parquet(file_path)
        .select([
            'ticker_epoch',
            'contract_symbol',
            'event_timestamp_utc',
            'open',
            'high',
            'low',
            'close'
        ])
        .sort('event_timestamp_utc')
        .collect()
        .lazy()
    )


def run_polars_ta(pl_df):
    return (
        pl_df.lazy()
        .with_columns(
            ema21 = plta.ema(pl.col("close"), timeperiod=21).round(4),
        )
        .with_columns(
            tr = plta.trange(pl.col("high"), pl.col("low"), pl.col("close")),
        )
        .with_columns(
            ema5  = plta.ema(pl.col("close"), timeperiod=5).round(4),
            rsi13 = plta.rsi(pl.col("close"), timeperiod=13).round(4),
            rsi21 = plta.rsi(pl.col("close"), timeperiod=21).round(4),
            rsi34 = plta.rsi(pl.col("close"), timeperiod=34).round(4),
            sma10 = plta.sma(pl.col("close"), timeperiod=10).round(4),
            sma20 = plta.sma(pl.col("close"), timeperiod=20).round(4),
            sma34 = plta.sma(pl.col("close"), timeperiod=34).round(4),
            ETR5  = plta.ema(pl.col("tr"), timeperiod=5) / pl.col("close"),
            ETR13 = plta.ema(pl.col("tr"), timeperiod=13).round(4),
            ETR21 = plta.ema(pl.col("tr"), timeperiod=21).round(4),

            DIP21 = pl.col("close") / pl.col("ema21"),
            lr5   = plta.linearreg(pl.col("close"), timeperiod=5).round(4),
            ADX5  = plta.adx(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=5).round(4),
            ADX13 = plta.adx(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=13).round(4),
            ADX21 = plta.adx(pl.col("high"), pl.col("low"), pl.col("close"), timeperiod=21).round(4),
        )
        .with_columns(
            new_open = pl.coalesce(pl.col("open"), pl.lit(0.0)),
            new_high = pl.when(pl.col("open") > pl.col("high"))
                        .then(pl.lit("YES"))
                        .otherwise(pl.lit("NO")),
        )
    )


def run_repark_ta(
    new_df: ReparkDataFrame,
):
    return (
        new_df.lazy()
        .withColumns({
            "ema21": TA.ema("close", timeperiod=21).round(4),
            "test_21": (TA.ema("close", timeperiod=21).round(4)) / F.col("close")
        })
        .withColumns({
            "tr": TA.trange("high", "low", "close")
        })
        .withColumns({
            "ema5": TA.ema("close", timeperiod=5).round(4),
            "rsi13": TA.rsi("close", timeperiod=13).round(4),
            "rsi21": TA.rsi("close", timeperiod=21).round(4),
            "rsi34": TA.rsi("close", timeperiod=34).round(4),
            "sma10": TA.sma("close", timeperiod=10).round(4),
            "sma20": TA.sma("close", timeperiod=20).round(4),
            "sma34": TA.sma("close", timeperiod=34).round(4),
            "ETR5": (TA.ema("tr", timeperiod=5).round(4)) / F.col('close'),
            "ETR13": TA.ema("tr", timeperiod=13, null_lookback=True).round(4),
            "ETR21": TA.ema("tr", timeperiod=21),
            
            "DIP21": (col("close") / col("ema21")),
            "lr5":  TA.linearreg("close", timeperiod=5).round(4),
            "ADX5": TA.adx("high", "low", "close", timeperiod=5).round(4),
            "ADX13": TA.adx("high", "low", "close", timeperiod=13).round(4),
            "ADX21": TA.adx("high", "low", "close", timeperiod=21).round(4)
        })
        .withColumns({
            "RSID13": F.col("rsi13") / F.col("close"),
            "new_open": coalesce(col("open"), lit(0.0)),
            "new_high": F.when(col('open') > col('high'), F.lit('YES')).otherwise(F.lit('NO')),
        })
    )




def test_repark():
    repark_base_df = load_repark()
    start = time.time()
    result = run_repark_ta(repark_base_df)
    result.eager()
    end = time.time()
    print(f"Execution time: {end - start:.4} seconds")
    return result

def test_polars():
    polars_base_df = load_polars()
    start = time.time()
    result = run_polars_ta(polars_base_df)
    result.collect()
    end = time.time()
    print(f"Execution time: {end - start:.4} seconds")
    return result



if __name__ == "__main__":
    test_repark()
    test_polars()
```

## The design question (Opus 5.5 sketch; no executor until the owner has read it)

**The plan as it stands:** the TA expressions are window functions over one ordered, unpartitioned input. DataFusion plans each `withColumns` level as one `WindowAggExec` with a single input partition, and evaluates that operator's independent window expressions one after another.

The sketch weighs two designs against the DataFusion pin (workspace `datafusion = "54.1.0"`), and names the files and functions where each one lives:

1. **Evaluate the independent window expressions of a one-partition `WindowAggExec` in parallel.** Per batch or per whole partition, the work for K expressions is spread across threads, and the output columns are kept in expression order.
2. **Rewrite the plan into K same-input window operators zipped by position.** A physical or logical rewrite splits one window operator's expressions into groups over the same ordered input. The groups run on separate tasks, and their outputs are zipped back together by row position.

**For each design the sketch states:**
- the expected wall time on this benchmark (an arithmetic bound from measured per-expression costs, marked *bound*);
- the order and determinism guarantees;
- memory peak;
- the interaction with the TA-CHAIN-1 leading-run handling;
- whether it needs a fork of DataFusion or fits in RePark-owned code;
- the pins each option would need, with one mutation per boundary.

**Rust-first:** all of this is native. The Python facade does not change.

### The explicit `Window.orderBy` spelling is in scope

- **The problem:** the owner measured the explicit `Window.orderBy` spelling of the same pipeline at **0.61 s**, slower than the unwindowed 0.49 s.
- **Required:** it must land at the same speed as the unwindowed spelling.
- **Script gap:** that spelling is not in `benchmark_script.py`. The sketch lane writes it as a sibling script beside the owner's, and records its exact text in the sketch, so the gate has a fixed input.

## Gates (owner-set)

1. **On this benchmark:** run interleaved, median of 5, in one process, on the same box. RePark's wall time must be **≤ polars_talib's** (ratio ≤ 1.00, as fast or faster). The gate is the ratio; there is no fixed seconds target. This applies to the unwindowed spelling and to the explicit `Window.orderBy` spelling.
2. **The 158 kernel goldens** (`crates/repark-ta/tests/goldens/*.bin`) and **the 13 TA-CHAIN-1 prefix goldens** (`crates/repark-ta/tests/goldens/prefix/`) are byte-identical.
3. **`python/repark-parity/bench/ta/bench_kernel_race.py`** stays at **≤ 1.02x** of today.
4. **Partitioned timing** is not slower than today's 0.18 s.
5. **No code comments; Rust-first.**

## Order of work (owner, 2026-10-04)

1. **This card,** as a docs PR at clerk grade.
2. **The Opus 5.5 design sketch** runs when the PERF-ATTR-STAMP-2 lane frees, not alongside it. One Opus lane remains the cap.
3. **No executor** until the owner has read the sketch.
