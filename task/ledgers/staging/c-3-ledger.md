# Unit ledger — C-3 · partitioned parallel Postgres reads and the ConnectorX benchmark

**Date:** 2026-10-07 · **Branch:** `feat/c-3-partitioned-reads` · **Base:** `156be81c` (C-2a…C-2d
merged) · **Model:** Claude Opus 5.5 (`claude-opus-5-5`, high) · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Order:** [c-3-partitioned-reads.md](../../wo/c-3-partitioned-reads.md), grade B, R-1…R-4.
**Sketch it builds on:** [c-2-design.md](../../wo/c-2-design.md). **Standing defaults:**
[the North Star](../../roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md) NS-1…NS-19.
**Card:** 1.6 of [the design plan](../../roadmap/epic-term/roadmap-design-plan-2026-08-29.md).
**Owner ruling (2026-10-07 evening):** this unit runs on an Opus 5.5 high lane with one Opus
verifier on the PR.

The order's three PENDING items (the provider shape, the partition-column surface, the benchmark
queries) are settled in §0 from the C-2 design and from measurements, before any product code.

## 0. The design note (step 0, 2026-10-07)

Every Spark statement below is a recorded cell of
[c3_partition_oracle.json](../../../python/repark-parity/tests/live_spark/c3_partition_oracle.json)
(live Spark 4.1.2, pgjdbc 42.7.13, Postgres 16.15), cited by cell id. Every Postgres statement is
a `psql` measurement on the C-0 container, quoted in §0.2.

### 0.1 The surface

**Source lines.** c-2-design §0 line 7 ("`partitionColumn` / `lowerBound` / `upperBound` /
`numPartitions` / `predicates` refuse until C-3"), §2.3's declared-keys table (`read_postgres`
only) and §2.11 ("Any of `partition_column`, `lower_bound`, `upper_bound`, `num_partitions` or
`predicates` refuses with `CONNECT-DECL-pg-partitioned-read` (C-3)").

**Decision.** Spark's four JDBC options are the surface, on the three doors that already carry
them: `read_postgres(partition_column=, lower_bound=, upper_bound=, num_partitions=)`,
`spark.read.jdbc(url, table, column, lowerBound, upperBound, numPartitions)` and
`format("jdbc" | "postgres")` with the four options. The four spellings inside `properties`
partition too, as Spark's do (`C3-J02`). `predicates` stays declared. `repark.toml` carries no
partition options: a mounted source is a catalog of every relation on the server, so a
per-relation column has no key to sit under, and C-2d's door gives the four spellings no
carriage (they are unknown keys there, unchanged).

**Measured Spark behaviour, and what RePark does with each.**

| case | Spark 4.1.2 (cell) | RePark |
|---|---|---|
| all four given, integer column | N range queries; rows below `lowerBound`, above `upperBound` and NULL rows are all returned (`C3-P01`: 26 of 26 rows, NULLs and `-500` in the first stride, `5000` in the last) | the same strides, the same rows |
| one of `partitionColumn` / `lowerBound` / `upperBound` missing, or `numPartitions` missing beside a column | `IllegalArgumentException`: "When reading JDBC data sources, users need to specify all or none for the following options: 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions'" (`C3-M01`…`M04`, `M06`, `M07`) | refuses with the same sentence, before any connection |
| `numPartitions` alone | one unpartitioned read (`C3-M05`) | the same: accepted, no effect on a read |
| `lowerBound > upperBound` | `IllegalArgumentException`: "Operation not allowed: the lower bound of partitioning column is larger than the upper bound. Lower bound: 200; Upper bound: 0" (`C3-B01`) | refuses with the same sentence |
| `lowerBound == upperBound` | one unpartitioned read (`C3-B02`) | the same |
| `numPartitions` of `1`, `0` or `-1` | one unpartitioned read, no error (`C3-N01`…`N03`); the column is still verified first | the same |
| `upperBound - lowerBound < numPartitions` | the count shrinks to the difference (`C3-B03`: `0..3` at 10 gives 3 strides; a difference of 1 gives one unpartitioned read) | the same |
| a bound that is not an `i64` (`abc`, `1.5`, `9223372036854775808`) | `NumberFormatException` (`C3-B06`…`B08`) | refuses, naming the option |
| `numPartitions` not an integer | `NumberFormatException` (`C3-N04`) | refuses, naming the option |
| the column is not in the relation | `AnalysisException` `_LEGACY_ERROR_TEMP_1286`: "User-defined partition column nope not found in the JDBC relation: struct<…>" (`C3-C01`) | refuses, naming the column and the relation's columns |
| the column in another case, or quoted | resolves (`C3-C02` `N`, `C3-C03` `"n"`) | the same: exact name first, else the one case-insensitive match; a quoted name matches exactly. Two columns that differ only in case and neither matching exactly refuse as ambiguous (Spark takes the first; refusing is the safe half) |
| a `text` or `boolean` column | `AnalysisException` `_LEGACY_ERROR_TEMP_1287`: "Partition column type should be numeric, date, or timestamp, but string found." (`C3-T03`, `C3-T10`) | refuses with the same sentence |
| `int2`, `int4`, `int8` | partitions (`C3-T01`, `C3-T02`, `C3-P01`) | partitions |
| `date`, `timestamp`, `timestamptz` (bounds are date or timestamp text; an integer bound refuses, `C3-T05`) and `numeric`, `float8` (integer bounds) | partitions (`C3-T04`, `T06`…`T09`); `timestamptz` strides are placed by the JVM zone (`C3-T07`: the last stride is empty) | **declared**: refuses naming `CONNECT-DECL-pg-partitioned-read`. Timestamp strides depend on the zone D-M2 found Spark takes from the JVM, and numeric and float strides are integer cuts over a non-integer column; each needs its own measured arithmetic |
| `query` with `partitionColumn` | `IllegalArgumentException`: "Options 'query' and 'partitionColumn' can not be specified together. Please define the query using `dbtable` option instead…" (`C3-Q01`) | refuses with the same first sentence and the same fix |
| `dbtable` as a parenthesised subquery, partitioned | partitions (`C3-Q02`) | partitions |
| `predicates` | one query per predicate (documented) | **declared**, unchanged |

**Rows outside the bounds and NULL rows are returned exactly once.** Spark's first stride is
`"n" < 50 or "n" is null` and its last is `"n" >= 150`, with no bound on the open side
(`C3-P01`). RePark renders the same three shapes. The bounds choose the strides; they never
filter. Flink's JDBC source differs here (its `scan.partition.lower-bound` and `upper-bound`
filter the rows); the four-line record is FL-2 in §2.

**The strides are Spark's, number for number.** `JDBCRelation.columnPartition` divides each bound
by the count as a `BigDecimal` under `MathContext.DECIMAL128`, rounds each quotient to 18 places
half-even, truncates the difference to the stride, and shifts the first cut by half the strides
the truncation lost. RePark ports that arithmetic and checks it against the recorded grid: 40
edge triples (the `i64` extremes, equal bounds, one stride, more strides than values, negative
ranges) and 700 seeded random triples across eight magnitudes and counts up to 64
([c3_stride_grid.txt](../../../python/repark-parity/tests/live_spark/c3_stride_grid.txt)). Every
recorded cut list is strictly increasing. The planner also refuses any plan whose cuts are not
strictly increasing, so an arithmetic surprise outside the grid is a refusal and never a
duplicated or dropped row.

### 0.2 Consistency

**What C-2 promises.** c-2-design §0 line 1: "A scan returns the rows of **one Postgres statement
snapshot**." One scan is one `COPY` statement in `READ COMMITTED`, so the promise holds by
construction. §2.12 leaves the partitioned case to this unit: "`pg_export_snapshot` would make
the partitions see one snapshot".

**What Spark promises.** Nothing: each partition is its own query on its own connection, each
with its own snapshot. A row that moves across a stride boundary, or is inserted or deleted
between two partition queries, appears twice, once or not at all.

**What Flink's JDBC source does.** The same: each split is its own statement on its own
connection, with no shared snapshot (documented; nothing run). Flink CDC's snapshot phase is the
connector that reconciles chunks, and it does so with the log, which is 1.7's.

**Decision: export one snapshot.** Order rule H-SKEW requires a partitioned result to equal the
C-2 scan row for row, and C-2's ruled guarantee is one snapshot per scan. A concurrent writer
breaks both unless the strides share a snapshot, so C-3 keeps C-2's guarantee and is stricter
than both engines. On unchanging data it returns exactly Spark's rows, so North Star §8.2 does
not trigger (the four-line record is FL-1 in §2). The mechanism:

1. The scan takes its connections (§0.3) before any statement runs.
2. Each opens `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`. The first runs
   `SELECT pg_catalog.pg_export_snapshot()`; every other runs
   `SET TRANSACTION SNAPSHOT '<id>'` as its first statement. The id is checked against
   `[0-9A-Fa-f-]` before it enters the text (a `SET` takes no bound parameter).
3. Only when every connection holds the snapshot does the first `COPY` start. A connection that
   runs more than one stride runs them back to back in the same transaction.
4. Each connection commits and goes through `release_clean`, as a C-2 scan does.

**Measured on the C-0 container (2026-10-07, `psql`, sessions opened with the pool's pins
`default_transaction_read_only=on`, `search_path=`, `idle_in_transaction_session_timeout`):**

| probe | result |
|---|---|
| export in a `REPEATABLE READ READ ONLY` transaction on a read-only session | an id (`00000003-00001D08-1`) |
| a writer then inserts one row and deletes ten; a fresh reader | 91 rows |
| an importer of the id, same moment | 100 rows, the exporter's snapshot |
| the importer after `COMMIT` and `RESET_SESSION`: `statement_timestamp() = transaction_timestamp()`, `transaction_isolation`, `default_transaction_read_only` | `t`, `read committed`, `on`: the pool's reset and pin check hold, the session returns to the pool clean |
| an import after the exporter committed | `ERROR: invalid snapshot identifier`, hence step 3: every import completes while the exporter's transaction is open |
| an import after any query in the transaction | `ERROR: SET TRANSACTION SNAPSHOT must be called before any query`, hence the query source's search-path statement runs after the import |
| an import in `READ COMMITTED` | `ERROR: a snapshot-importing transaction must have isolation level SERIALIZABLE or REPEATABLE READ` |

The pool's session reset and pin check hold a snapshot-importing transaction cleanly, so the
brief's halt condition does not arise.

**One execution, one snapshot, structurally.** The scan stays **one DataFusion partition** and
runs its connections inside it (§0.3). A plan with N DataFusion partitions would need all N
`execute(i)` calls of one run to find the same snapshot, and DataFusion gives a provider no
identity for "one run": the calls may come lazily, or a second run may reuse the plan. Inside
one `execute(0)` the export, the imports and every `COPY` belong to one stream, so nothing
outside it can pair a stride with the wrong snapshot.

### 0.3 The bound on parallelism

**Source line.** c-2-design §0 line 5: "**Pool:** `pool_max_size` connections (default 4, ceiling
64); checkout timeout 30 s … **Partitions:** one per scan; C-3 parallelises."

- **Strides and connections are different numbers.** `numPartitions` (after Spark's shrink) is
  the number of strides, N. The scan runs them on L connections, `1 <= L <= min(N,
  pool_max_size)`, each connection taking strides `j, j + L, j + 2L, …` in order. So
  `numPartitions` above the pool is legal and never opens a connection past the pool: 16 strides
  on a pool of 4 run as four connections of four strides each.
- **How the connections are taken.** The first is an ordinary checkout: it queues behind other
  scans for up to `pool_checkout_timeout_ms` and refuses with `PoolExhausted` after it, C-2's
  contract unchanged. Every further connection is taken only if a permit is free at that
  moment. The scan never waits while holding a connection, so two partitioned scans on one pool
  (a self-join) cannot deadlock each other into a timeout: under contention a scan runs on fewer
  connections, down to one, and still returns the same rows from one snapshot.
- **`target_partitions`.** It does not bound the scan and the scan does not read it. The scan
  reports one output partition; when `target_partitions > 1` DataFusion places its own
  `RepartitionExec` above the scan for the operators that want it, as it does over a C-2 scan.
  The scan's parallelism is decided by `numPartitions` and `pool_max_size` alone.
- **Tasks.** Each connection is driven by one tracked task (`SpawnedTask`, aborted when the
  stream drops), so decoding runs on the runtime's workers in parallel. The tasks feed one
  bounded channel of L batches.
- **Memory.** One batch decoding per connection plus at most L in the channel plus the one the
  consumer holds; every batch carries its own slice of the scan's `MemoryReservation` from the
  moment it is decoded until the consumer replaces it, so the pool sees all of them. C-2's
  "one batch in flight per scan" becomes "per connection", bounded by `pool_max_size`.

### 0.4 Composition with C-2c and C-2d

- **Filters and projection.** A stride is one more conjunct on the C-2 statement: the pushed
  `Exact` conjuncts, then the stride. Its compares are `OPERATOR(pg_catalog.<)` and
  `OPERATOR(pg_catalog.>=)` against `current_setting('repark.pN')::pg_catalog.int8`, the cuts
  bound through the `set_config` carriage like every other value. `int2` and `int4` columns
  compare against `int8` through the catalog's cross-type operators, so a cut outside the
  column's own range (Spark sends them too) is a comparison and never a cast error. The
  projection is untouched: the partition column need not be selected.
- **`LIMIT`.** Pushed per stride and capped above. When C-2's rule allows a push (no residual
  conjunct, `pushdown_limit`, the limit fits `i64`), every stride statement carries `LIMIT n`,
  so no stride returns more than n rows; the scan then stops at n rows itself and drops the
  rest, cancelling the connections still copying. DataFusion keeps its own limit above the
  scan as it does over a C-2 scan. Not pushing would be correct and slow; pushing without the
  cap would return up to N × n rows from the scan, which the engine's limit would still cut,
  but the scan's own `pushed_limit` would then lie. Which n rows a `LIMIT` without `ORDER BY`
  returns is unspecified in both engines; the count is the pin. Spark pushes no limit through a
  V1 JDBC scan (`C3-L01`: `CollectLimit` over a bare query, the same as D-M2's `DM2-S03`).
- **The deferred per-value refusal.** Unchanged per connection: a stride that meets a value its
  Arrow type cannot hold emits the rows before it, then the refusal. The scan forwards what
  each connection sent in arrival order and ends at the first error, dropping every other
  connection (C-2d's fused stream, C-103).
- **Cancel.** Dropping the stream aborts every task; each task's lease drops, which fires the
  cancel request and aborts the connection task (C-2b's lease). No connection returns to the
  pool from a cancelled scan.
- **Ordering.** C-2 promises none and neither does this: batches arrive interleaved across
  connections.
- **EXPLAIN.** A partitioned scan adds `partition_column=`, `strides=` and `max_connections=`
  to its line; `Verbose` shows the first stride's statement. EXPLAIN still opens no connection.

### 0.5 What stays declared

`CONNECT-DECL-pg-partitioned-read` is rewritten to what remains:

- `predicates` (one query per predicate);
- a partition column of type `date`, `timestamp`, `timestamptz`, `numeric`, `float4` or `float8`
  (Spark partitions these);
- automatic choice of the column and bounds (ConnectorX's `partition_on` with a min/max query;
  Spark has no such option);
- partition options in `repark.toml`.

### 0.6 The provider trait and the crates touched

No change to the provider trait C-2 shipped: `PostgresTable` gains one inherent builder
(`partitioned`), `TableProvider::scan` keeps its signature, and `PostgresScanExec` stays one
partition. Crates touched: `repark-connect` (the arithmetic, the pool's multi-checkout, the
connection lanes, the exec), `repark-core` (`PostgresRead` carries the four values) and the
binding and facade's option carriage. Nothing here sends the unit to a halt.

### 0.7 The benchmark queries

`SELECT *` over one 10M-row table of nine columns (`int8` key, `int4`, `int2`, `float8`,
`numeric(12,2)`, `text`, `date`, `timestamptz`, `bool`), partitioned on the `int8` key with the
bounds at its minimum and maximum: RePark unpartitioned and at 4 and 8, ConnectorX `read_sql`
with `return_type="arrow"` at the same settings, pandas `read_sql` over SQLAlchemy. Three runs
each, the median reported (§4).

## PROPOSITION LEDGER — C-3 — 2026-10-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The Spark halves of §0.1 are a recording, not documentation: `c3_partition_oracle.py` records 41 cells (`C3-P`, `M`, `B`, `N`, `T`, `C`, `Q`, `J`, `L`) and a 740-triple stride grid (`c3_stride_grid.txt`) on live Spark 4.1.2 through pgjdbc 42.7.13 against Postgres 16.15, and `c3_partition_oracle.sha256` holds both digests. | `sha256sum -c c3_partition_oracle.sha256`; the cells cited in §0.1. | PROVEN | §0.1; the recording. |
| C-002 | The stride arithmetic equals Spark's on every recorded triple. | Open until the planning slice lands. | OPEN | Which pin compares the grid? The planning slice's `strides_equal_sparks_recorded_grid`. |
