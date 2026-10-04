# v1.5.2 release notes (2026-10-03)

v1.5.2 is the second patch on 1.5.0. Every change is additive under the API freeze.

## TA indicators: chained indicators answer (TA-CHAIN-1)

Chained TA indicators (an indicator computed from another indicator's output) now answer where polars_talib answers; a leading NaN or NULL run on any `ta.*` input is skipped instead of propagating.

Before this fix, `ta.ema(ta.trange(high, low, close), 21)` answered NaN on every row, because
TRANGE's first output row is NaN and the C-faithful kernel carried that NaN through EMA
forever. The `ta_*` window wrapper now skips each input's leading NaN/NULL run, starts the
kernel at the latest first valid row across the inputs, and returns the skipped rows as NaN,
as polars_talib 0.1.5 does. This applies to both the SQL `ta_*(…) OVER (…)` door and
the Python `ta.*` door. An interior NaN still propagates exactly as in C TA-Lib, and the
kernel layer (`repark_ta::ema` and the other kernels) is unchanged. With `null_lookback=True`,
the facade still NULLs exactly the first `lookback` rows by row position; a skipped run in
front of those rows stays NaN. Ledger:
[ta-chain-1-ledger.md](../../ledgers/staging/ta-chain-1-ledger.md).

## Column.is_duplicated() (POLARS-IS-DUPLICATED-1, #883, 5d8ee78b)

`Column.is_duplicated()` answers as Polars 1.43.2 on both `rp.col` and `F.col`.
On the PySpark door this is a documented RePark extension; PySpark has no such
method. The owner pulled this one expression ahead of the v1.9 Polars functions
work.

- The mask is true on every occurrence of a value that appears more than once.
- null equals null, NaN equals NaN, and 0.0 equals -0.0. Strings compare exactly.
- `filter` keeps the input order. A sort before the mask survives, and a sort
  after it is untouched.
- It works in `filter` / `where`, `select` and `withColumn` / `with_columns`,
  with `~`, `&` and `|`, over expressions, and after a prior filter.
- 76 of 76 cells match Polars, order-exact.
- User-written window predicates in `filter` keep today's refusal.

## Deep operator chains (DEEP-FILTER-CHAIN-CRASH-1, #892, daf9bbaa)

Deep operator chains no longer crash the interpreter. Planning runs on a stack
sized for them. Before this fix, `count()` after 200 or more chained filters
killed the Python interpreter with SIGSEGV. 200 joins and 300 unions crashed
it too.

Every entry point, `sql()` and the DataFrame builders run on a large stack, so
subquery and expression depth no longer crash. There is no query-text length
cap. Depth limits refuse only where base crashes or where Spark refuses. The
1,500 builder cap counts df-built levels, so SQL-text columns answer past it
while df-built chains refuse where Spark refuses.

Measured: 1,000 filters, 200 joins, 300 unions and 120 `withColumn` calls
answer with Spark-agreeing values. Deeply nested SQL still refuses cleanly.

## VALUES doors for TIMESTAMP, DATE and -NULL (STORE-TS-DOORS-2, #894, 5f917f93)

A VALUES node inside `INSERT … SELECT`, and a static-partition `INSERT
OVERWRITE`, refuse TIMESTAMP, DATE and `-NULL` sources like Spark. Both doors
predate v1.5.1 and shipped in it.

- `INSERT INTO t SELECT … FROM VALUES (…, TIMESTAMP'…')` into a BIGINT column
  stored epoch seconds. It now refuses `CANNOT_SAFELY_CAST`, as Spark does.
- `-NULL` into a TIMESTAMP column through `INSERT OVERWRITE … PARTITION (p='a')
  VALUES` stored NULL. It now refuses, as Spark does.
- A STRING arm beside a TIMESTAMP or DATE arm is judged by the widened type and
  stores like Spark. Function cells, table arms and views widen like Spark.

The refusal text matches Spark byte for byte.

## SQL string literals (STRING-LITERAL-ESCAPE-1, #890, 02c2f1be)

SQL string literals unescape exactly as Spark 4.1.2 does, on all five doors and
under both `spark.sql.parser.escapedStringLiterals` settings. That covers
escaped and doubled quotes, adjacent literals, raw strings, `\u` escapes, `\U`
escapes and octal escapes.

Before this fix, `SELECT "x\\""y"` returned `x\""y`; Spark returns `x\"y`.

Verbatim keep-exact covers query literals and OPTIONS values. Property keys and
comments always unescape. `filter` and `where` follow the flag, and DML
predicates keep doubled quotes. SQL the facade builds from Python values parses
in default mode, while user-written text follows the verbatim flag.
`SQLTransformer` statements are user-written SQL and follow the verbatim flag.
Nested struct assignments keep doubled quotes.

Under the default setting, five cells still differ. All are a raw string
holding a run of three quotes. `F.expr` with `escapedStringLiterals=true` is an
accepted residual. That setting is not the default.

## Overflow into an integer column (CAST-OVERFLOW-INSERT-1, #891, 458d30fd)

An out-of-range fractional value stored into an integer column refuses with
Spark's `CAST_OVERFLOW_IN_TABLE_INSERT` on all nine write doors. Before this
change it wrote silently. The overflow rule judges the truncated value. Residue
R-INTDIV-9 is closed.

A `LIMIT 0`, or a skipping `OFFSET` beneath the defining projection, writes
nothing and never refuses. A row with both a type fault and an overflow reports
`CANNOT_SAFELY_CAST`, as Spark does. A positional store with the wrong column
count reports the arity error first, as Spark does. UUID text keeps storing.

## CSV and JSON timestamps (TEXT-WRITE-TIMESTAMP-ZONE-1, #889, dc613672)

CSV and JSON writes format TIMESTAMP values in the session time zone with
Spark's default pattern, as Spark 4.1.2 does. They honour `timestampFormat`,
`timestampNTZFormat` and `dateFormat`, on local paths and on s3a. Parquet is
untouched.

Pattern quotes, year widths and signs, `VV` ids, a LEGACY refusal and
AST-bound patterns match Spark. `VV` writes `UTC` and `GMT` for zero offsets,
and `g` pads. The time parser policy default is CORRECTED. Zone-correct writes
run at base speed.

A failed s3a text write removes the objects it created, so no partial output
is left. It deletes only the objects its own sink created, never a concurrent
writer's. The eager Spark door rolls back too.

## Also in this release

RP-56 (`a0c2a884`, #879) repins the fork to `e1d74bef` (fork #364): case-twin
columns build like Java, and CREATE and CTAS with `a` and `A` answer under
`caseSensitive=true`. CI-1 (`ffee8692`, #924) runs the wheel smoke as changes,
then build, then three shards, then an aggregate, with a docs-only
short-circuit, the `ci` profile at opt-level 2, and the wheel cache written on
main. C-0 (`82f1483c`, #907; `3406dbb3`, #917) adds the disposable Postgres
harness, and after a reboot the label-based reap removes stopped containers.

## Matrix

The 842-cell matrix on a fresh build of the release candidate (`8148c29c`, scoreboard 2026-10-03, the v1.5.1 harness and compare rules) reads **705 EQUAL / 132 SPARK-CANNOT / 5 REFUSED-REGISTERED / 0 DIFFERENT**, unchanged from v1.5.1.
