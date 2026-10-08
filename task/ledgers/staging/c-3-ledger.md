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
4. Each connection commits. The first goes through `release_clean`, as a C-2 scan's does, and
   is pooled; every other is closed (`retire`). A frame from the read door owns its own pool,
   so pooling every connection would leave `pool_max_size` idle sessions per frame for as long
   as the frame lives (the pool reaps idle connections only on its next checkout). The first
   full run of the live Python cells showed it: nine partitioned frames held 36 idle sessions
   and the container refused the next connection (`53300`). At rest a partitioned frame now
   holds one idle connection, exactly what a C-2 frame holds; a repeated read reopens the
   others in parallel.

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
| C-002 | The stride arithmetic is Spark's, number for number: `stride_cuts(lower, upper, num_partitions)` returns the cut list Spark 4.1.2 recorded for each of the 740 grid triples (the `i64` extremes, bounds equal, one stride, more strides than distinct values, negative ranges and 700 seeded random triples) and refuses the one triple Spark refuses. A count of one or less, or equal bounds, is no cut, checked before the bounds' order; the count shrinks to the span when the span is smaller. Cuts that are not strictly increasing refuse, so the planner cannot duplicate or drop a row. | `strides_equal_sparks_recorded_grid`, `strides_at_the_i64_bounds_neither_wrap_nor_repeat`, `one_stride_means_an_unpartitioned_read`, `more_partitions_than_values_shrink_to_the_span`, `negative_and_straddling_ranges_follow_spark`, `strides_hold_every_value_exactly_once`. | PROVEN | §1; `cargo test -p repark-connect --test it partition`. |
| C-003 | The option rule is Spark's (§0.1): a column or either bound needs all four, else the measured sentence; `numPartitions` alone is no partitioning; reversed bounds refuse in the measured sentence; the four spellings lift out of a property map case-insensitively; a bound that is not an `i64` refuses naming the option and never the value; `predicates` stays declared naming `CONNECT-DECL-pg-partitioned-read`. Each refusal folds to Spark's class: `IllegalArgument`, `NumberFormat`, `Analysis`, and `Unsupported` for a declared column type. | `the_four_options_are_all_or_none_as_spark_rules`, `reversed_bounds_refuse_in_sparks_words`, `the_four_spellings_lift_out_of_the_properties`, `a_bound_that_is_not_an_i64_refuses_naming_the_option_never_the_value`, `predicates_stay_declared_naming_the_row`, `a_declared_column_type_is_unsupported_and_names_the_row`. | PROVEN | §1. |
| C-004 | A stride is one conjunct after everything pushed (§0.4): the first is open below and carries the only NULL test, a middle one is closed below and open above, the last is open above; every compare is `OPERATOR(pg_catalog.…)`; every cut is bound through the `set_config` carriage and read back as `::pg_catalog.int8`; projection and `LIMIT` are untouched; a cut past the 1024-slot bound answers `None`. | `rendering::the_first_stride_is_open_below_and_takes_the_nulls`, `a_middle_stride_is_closed_below_and_open_above`, `the_last_stride_is_open_above`, `a_stride_follows_the_pushed_conjuncts_and_keeps_projection_and_limit`, `the_strides_of_one_plan_bind_every_cut_once_per_side`, `a_stride_refuses_past_the_slot_bound_or_the_columns`. | PROVEN | §1. |
| C-005 | Execution (§0.2…§0.4). A partitioned scan is one DataFusion partition over between one and `min(strides, pool_max_size)` connections; `checkout_up_to` queues for its first connection only and never opens past the pool; every connection reads one exported snapshot, so a writer between strides changes nothing, on four connections and on one; the partitioned rows equal the unpartitioned rows value for value across the mapped types, with NULL and out-of-bounds rows exactly once; pushed filters, projection and a per-stride `LIMIT` capped by the scan compose with the strides; a refused value keeps its per-connection contract; a dropped stream leaves no backend; the connections return to the pool clean. | The no-server pins of `partition_plan.rs` and `pool.rs`; the nine live cells of `live_partition.rs`; `cargo test -p repark-connect -- --include-ignored`, three runs. | PROVEN | §3; 214 passed, 0 failed, three runs. |
| C-006 | The doors (§0.1). `read_postgres`, `spark.read.jdbc` with Spark's four arguments, `format("postgres" \| "jdbc")` with the four options and the four spellings inside `properties` each partition a read and equal psycopg's reading; a `dbtable` subquery partitions; `query` with a column, an incomplete option bag, reversed bounds, a missing column, a `text` or `boolean` column and a bound that is not a 64-bit integer refuse in Spark's class and sentence, before any connection where Spark refuses before one; `numPartitions` alone, counts of `1`, `0` and `-1`, and equal bounds read unpartitioned; `predicates` and the `date`, timestamp, `numeric` and float columns refuse naming `CONNECT-DECL-pg-partitioned-read`, whose registry row is rewritten to what C-3 delivers and what it still declares; in `repark.toml` the spellings stay unknown keys. | `partition_options_refuse_as_spark_does_before_any_connection`, `num_partitions_alone_is_no_partitioning_and_never_a_setting` (core); `read_postgres_refusals_name_their_row_and_never_echo_credentials` (binding); `declared_keys_refuse_naming_their_row` (settings); the eight live cells of `test_c3_partitioned.py`; the facade subset. | PROVEN | §3; live_db 31 passed, 5 xfailed (C-0's strict xfails); facade subset 692 passed, 9 skipped. |
| C-007 | The benchmark (§0.7, order R-3) is a harness beside the live cells and a recorded run: `c3_bench.py` loads a 10M-row, nine-column table into the C-0 container and times `SELECT *` on RePark's release wheel (unpartitioned, 4 strides, 8 strides), ConnectorX `read_sql` with an Arrow return at the same partition settings and pandas over SQLAlchemy, three runs each in a fresh interpreter, the build lock held, the median reported with rows per second and the factor against ConnectorX. The acceptance factor is the owner's: the measurement is recorded and no pass is declared. | §4; the harness's JSON report, quoted. | PROVEN | §4. |
| C-008 | The ledger cites ConnectorX (the partitioned-read design and the benchmark bar) and ADBC, order R-4. | §5. | PROVEN | §5. |

## 1. Mutations (2026-10-08)

Each mutation was applied to the committed tree (`2dc42c6d`) alone, the crate's integration
binary run with the server up (`cargo test -p repark-connect --test it <filter> --
--include-ignored`), and the file restored; `git status` was clean after the last one. "Red" is
every pin that failed. Four are the brief's (m1 to m4; "let partitions exceed the pool" has two
layers, the plan's cap and the pool's permits, so it is two mutations and a combined run); eight
are the executor's own.

| id | clause | mutation (file) | what it would let through | red? | red in |
|---|---|---|---|---|---|
| m1 | C-004, C-005 | the brief's: drop a stride's upper-open bound, `<` becomes `<=` (`src/read/postgres.rs`) | a row on a cut is read by two strides | RED | 13 pins: live `a_busy_pool_narrows_a_partitioned_read_and_never_fails_it`, live `a_partitioned_read_equals_the_unpartitioned_read_across_the_types`, live `a_refused_value_keeps_its_contract_in_every_stride`, live `cancel_aborts_every_connection_and_leaves_no_backend`, live `filters_projection_and_limit_compose_with_the_strides`, live `null_and_out_of_bounds_rows_arrive_exactly_once`, live `num_partitions_above_the_pool_never_opens_past_it`, live `one_connection_reads_its_strides_in_one_snapshot`, `rendering::a_middle_stride_is_closed_below_and_open_above`, `rendering::a_stride_follows_the_pushed_conjuncts_and_keeps_projection_and_limit`, `rendering::the_first_stride_is_open_below_and_takes_the_nulls`, `rendering::the_strides_of_one_plan_bind_every_cut_once_per_side`, `plan::explain_names_the_column_the_strides_and_the_connection_bound` |
| m2 | C-004, C-005 | the brief's: drop the NULL stride, the first stride loses `OR col IS NULL` (`src/read/postgres.rs`) | NULL rows vanish | RED | 9 pins: live `a_partitioned_read_equals_the_unpartitioned_read_across_the_types`, live `a_writer_between_strides_never_changes_a_partitioned_read`, live `filters_projection_and_limit_compose_with_the_strides`, live `null_and_out_of_bounds_rows_arrive_exactly_once`, live `one_connection_reads_its_strides_in_one_snapshot`, `rendering::the_first_stride_is_open_below_and_takes_the_nulls`, `rendering::the_strides_of_one_plan_bind_every_cut_once_per_side`, `plan::every_stride_carries_the_pushed_filter_the_projection_and_the_limit`, `plan::explain_names_the_column_the_strides_and_the_connection_bound` |
| m3a | C-005 | the brief's, the plan's half: `max_connections` no longer capped at `pool_max_size` (`src/provider/table.rs`) | the plan asks for 16 connections on a pool of 2 | RED | 2 pins: `plan::a_partitioned_scan_is_one_partition_over_bounded_connections`, `plan::explain_names_the_column_the_strides_and_the_connection_bound` |
| m3b | C-005 | the brief's, the pool's half: the extra checkouts take permits from a fresh semaphore (`src/pool.rs`) | the pool opens past `max_size` | RED | 3 pins: live `a_busy_pool_narrows_a_partitioned_read_and_never_fails_it`, `pool::a_multi_checkout_queues_for_its_first_connection_only`, `pool::a_multi_checkout_takes_what_is_free_and_never_passes_the_pool` |
| m4 | C-005 | the brief's: drop the snapshot import, the other connections open `REPEATABLE READ` without `SET TRANSACTION SNAPSHOT` (`src/read/postgres_lanes.rs`) | strides read after the writer see its rows | RED | 2 pins: live `a_writer_between_strides_never_changes_a_partitioned_read`, live `one_connection_reads_its_strides_in_one_snapshot` |
| m5 | C-005 | own: `BEGIN_SNAPSHOT_SCAN` becomes `BEGIN READ ONLY`, so one connection's strides are `READ COMMITTED` statements (`src/read/postgres_lanes.rs`) | a connection's later strides see the writer | RED | 2 pins: live `one_connection_reads_its_strides_in_one_snapshot`, `plan::the_snapshot_statements_are_repeatable_read_and_read_only` |
| m6 | C-005 | own: drop the scan's limit cap (`src/provider/partitioned.rs`) | the scan returns up to strides x limit rows | RED | 1 pins: live `filters_projection_and_limit_compose_with_the_strides` |
| m7 | C-002 | own: drop the half-stride alignment (`src/partition.rs`) | cuts differ from Spark's | RED | 4 pins: `more_partitions_than_values_shrink_to_the_span`, `negative_and_straddling_ranges_follow_spark`, `strides_at_the_i64_bounds_neither_wrap_nor_repeat`, `strides_equal_sparks_recorded_grid` |
| m8 | C-002 | own: keep the requested count when the span is smaller (`src/partition.rs`) | cuts differ from Spark's | RED | 4 pins: live `a_partitioned_read_equals_the_unpartitioned_read_across_the_types`, `more_partitions_than_values_shrink_to_the_span`, `strides_equal_sparks_recorded_grid`, `strides_hold_every_value_exactly_once` |
| m9 | C-005 | own: accept any column type (`src/provider/table.rs`) | a `text` column partitions | RED | 1 pins: `plan::only_integer_columns_partition_and_the_rest_refuse_by_kind` |
| m10 | C-005 | own: pool every connection of a partitioned scan (`src/read/postgres_lanes.rs`) | a frame keeps `pool_max_size` idle sessions | RED | 2 pins: live `a_writer_between_strides_never_changes_a_partitioned_read`, live `num_partitions_above_the_pool_never_opens_past_it` |
| m11 | C-003 | own: `numPartitions` alone refuses (`src/partition.rs`) | Spark accepts it | RED | 1 pins: `the_four_options_are_all_or_none_as_spark_rules` |
| m12 | C-005 | own: strides past the connection count are never queued (`src/read/postgres_lanes.rs`) | their rows vanish | RED | 4 pins: live `a_busy_pool_narrows_a_partitioned_read_and_never_fails_it`, live `a_partitioned_read_equals_the_unpartitioned_read_across_the_types`, live `num_partitions_above_the_pool_never_opens_past_it`, live `one_connection_reads_its_strides_in_one_snapshot` |
| m3a+m3b | C-005 | both halves of the pool bound at once | sixteen connections on a pool of two | RED | live `num_partitions_above_the_pool_never_opens_past_it` (`(16, 16)` against `(16, 2)`) |

Not mutated, and why: the cancel path is the lease's `Drop` (C-2b's, pinned there and by the
live cancel cell here); there is no line of C-3's to remove that would leave a backend running
short of deleting the task handles' drop, which is not a one-line mutation. The task join on
channel close has no pin: nothing in the suite makes a connection task panic.

## 2. Four-line records (2026-10-07)

Questions no ruled row answered, each acted on under its default (North Star §1). Flink's
answers are cited as documented for its JDBC connector; nothing in Flink was run. Spark's are
recorded cells.

| id | date | question | Flink | Spark | default acted on |
|---|---|---|---|---|---|
| FL-1 | 2026-10-07 | Do the partitions of one read see one snapshot? | No: each split is its own statement on its own connection. | No: each partition is its own query (`C3-P01`'s four `whereClause`s run as four statements). | Yes: one exported snapshot (§0.2). The two engines agree with each other and promise nothing; C-2's ruled guarantee (c-2-design §0 line 1) and order rule H-SKEW outrank both, and the stricter answer returns Spark's rows on unchanging data, so §8.2 does not trigger. |
| FL-2 | 2026-10-07 | Do `lowerBound` and `upperBound` filter rows? | Yes: `scan.partition.lower-bound` and `upper-bound` bound the rows read. | No: they only place the strides; rows outside them and NULL rows are returned (`C3-P01`, `C3-B04`, `C3-B05`). | Spark's: the options are Spark's surface (NS §2 rank 2) and the brief pins it. Flink's reading would make a Spark workload lose rows; the disagreement is on the surface's meaning, and Spark governs the surface. Filed for the owner as a recorded disagreement, not a halt. |
| FL-3 | 2026-10-07 | What does a partition count above the available connections do? | The source's parallelism is the job's; splits queue on the readers. | Opens `numPartitions` connections, one per task slot. | Strides queue on at most `pool_max_size` connections (§0.3); NS-7 bounds every resource, and c-2-design §0 line 5 already fixes the pool as the bound. |
| FL-4 | 2026-10-07 | Is a `LIMIT` pushed into a partitioned read? | The JDBC source implements limit push-down per split. | No: a V1 JDBC scan pushes none (`C3-L01`). | Pushed per stride and capped by the scan (§0.4), C-2's FL-9 carried over. A pushed limit changes which rows an unordered `LIMIT` returns, never how many, in both engines. |

## 3. The live run (2026-10-07)

`DOCKER_HOST=unix:///run/user/1000/docker.sock make pg-up` (Postgres 16.15, `max_connections=50`),
then `cargo test -p repark-connect -- --include-ignored`: **215 passed, 0 failed, 0 ignored**,
four consecutive runs (21.3 s to 22.7 s) on the final tree. The nine `live_partition` cells are
part of it. Python, after `make develop`: `pytest python/repark-parity/tests/live_db` **31
passed, 5 xfailed** (C-0's five strict xfails), the eight `test_c3_partitioned.py` cells among
them; `pytest python/repark/tests -k "source or postgres or toml or named" -n 8` **692 passed, 9
skipped**.

Four things the first live runs showed, each fixed before its commit:

- **The cells, not the product, exhausted the server.** The first equality cell opened one pool
  per partition plan (nine pools of four) and the suite hit `53300 too many clients`. The cells
  now share one pool per cell and take turns; the product's own bound (`max_connections =
  min(strides, pool_max_size)`) was never exceeded.
- **An `OFFSET` under a nested `ORDER BY` returns no rows in DataFusion 54.1 alone.**
  `SELECT v FROM (SELECT v FROM t ORDER BY v LIMIT 5 OFFSET 4990) ORDER BY v` over a
  one-partition input plans `GlobalLimitExec: skip=4990, fetch=0` above `TopK(fetch=4990)` and
  returns 0 rows; the same statement over 16 partitions returns 5. It reproduces with no Postgres
  scan in the plan (`generate_series` under `target_partitions = 1`) and on the unpartitioned
  C-2 scan, so it is not a partition plan changing an answer and H-SKEW does not apply. The cell
  uses the un-nested form; the engine defect is filed in the hand-back as out of scope.
- **Idle connections per frame.** The first full Python run exhausted the container again, this
  time through the product: every connection of a partitioned scan returned to its frame's
  pool, so nine live frames held 36 idle sessions. §0.2 step 4 records the change: the first
  connection is pooled, the rest are closed.
- **Two cells asserted more than the contract.** After a refused value a stride that had
  already finished may have pooled its connection, so "no backend" became "no busy backend and
  at most one pooled"; and the cancel cell's activity query matched an autovacuum worker on the
  table, so it now counts client backends of the cell's own application name.

## 4. The benchmark (2026-10-08, order R-3)

**The acceptance factor is the owner's. This section records the measurement and declares no
pass.**

**Setup.** [c3_bench.py](../../../python/repark-parity/tests/live_db/c3_bench.py), not collected
by pytest. The table: `c3_bench.mixed`, 10 000 000 rows, 1.23 GB with its primary key, nine
columns (`int8` key, `int4`, `int2`, `float8`, `numeric(12,2)`, `text`, `date`, `timestamptz`,
`bool`). The query: `SELECT * FROM c3_bench.mixed`, partitioned on `id` with bounds `1` and
`10000000`. The server: the C-0 container, Postgres 16.15, **capped at 2 CPUs and 2 GB** by its
compose file. The client: 16 cores (`taskset -c 32-47`), Python 3.12.3. RePark 1.5.3 is the
release wheel (`make build-wheel`, thin LTO) built from commit `2dc42c6d`, installed with
ConnectorX 0.4.6, pandas 3.0.6, SQLAlchemy 2.1.4, psycopg2-binary 2.9.13 and pyarrow 25.0.1
into a scratch environment under the clone (`.bench-venv`, git-excluded), never into `.venv`.
Each case ran three times in its own fresh interpreter; the whole run held
`/tmp/oc-worker/build-slots/opus-cargo.lock`, so no build ran under it (2026-10-08 05:00 UTC).
RePark and ConnectorX materialise one Arrow table; pandas its own frame.

| case | runs (s) | median (s) | rows / s | factor against ConnectorX |
|---|---|---|---|---|
| RePark, unpartitioned | 9.856, 9.323, 9.610 | 9.610 | 1 040 615 | **1.353** (ConnectorX unpartitioned) |
| RePark, 4 strides on 4 connections | 8.365, 7.923, 8.021 | 8.021 | 1 246 704 | **0.980** (ConnectorX at 4) |
| RePark, 8 strides on 8 connections (`pool_max_size = 8`) | 7.690, 6.997, 7.008 | 7.008 | 1 426 915 | **1.034** (ConnectorX at 8) |
| RePark, 8 strides on the default pool of 4 | 6.877, 6.267, 6.236 | 6.267 | 1 595 644 | **1.156** (ConnectorX at 8) |
| ConnectorX, unpartitioned | 13.433, 13.005, 12.751 | 13.005 | 768 913 | 1 |
| ConnectorX, `partition_num = 4` | 8.133, 7.688, 7.862 | 7.862 | 1 271 913 | 1 |
| ConnectorX, `partition_num = 8` | 7.485, 6.834, 7.244 | 7.244 | 1 380 538 | 1 |
| pandas `read_sql` over SQLAlchemy | 54.742, 54.425, 54.310 | 54.425 | 183 738 | 0.239 of ConnectorX unpartitioned |

RePark unpartitioned is 5.66 times pandas.

**The Arrow each engine returned differs, so the work is not identical.** RePark:
`id: int64 not null`, `amount: decimal128(12, 2)` (the column's own type), `at: timestamp[us,
tz=UTC]`. ConnectorX: `id: int64` nullable, `amount: decimal128(38, 10)`, `at: timestamp[us,
tz=+00:00]`. pandas: `amount` as `float64`, `day` as Python objects. RePark's partitioned reads
also do what ConnectorX's do not: every stride reads one exported snapshot, and the first stride
carries Spark's NULL test.

**What bounds the numbers: the server.** The container has two CPUs, so adding client
connections past two adds contention on the server and little throughput: RePark gains 1.37
times from one connection to eight, ConnectorX 1.80 times from a slower start. Measured the
same hour under the same lock with `psql … COPY (…) TO STDOUT (FORMAT BINARY) > /dev/null`
(no decoding at all):

| server-only probe | runs (s) |
|---|---|
| one stream, the whole table | 12.71, 12.66, 12.71 (`psql` itself is the bottleneck here) |
| four streams, four index ranges (`id < a`, `id >= a AND id < b`, …) | 6.11, 6.13, 6.58 |
| four streams, the first as Spark and RePark write it (`id < a OR id IS NULL`) | 7.37, 7.16, 7.46 |

So four-stream RePark (8.02 s, decoding into Arrow and assembling one table) sits 0.6 s to 0.9 s
above the server's own time for the same four statements.

**A measured cost, not taken: the first stride's NULL test.** `EXPLAIN` shows the first stride
as a sequential scan of the whole table (`Filter: (id < … OR id IS NULL)`), while every other
stride is an index range scan; a bound cut plans exactly as a literal does (Postgres estimates
a stable `current_setting` at plan time), so the binding costs nothing. The `OR … IS NULL` arm
costs about 1.1 s of a 7.3 s four-stream read here (first stride alone: 3.79 s against 2.80
s). Dropping the arm for a `NOT NULL` column would remove it, and was **not** done: a
constraint dropped between planning and the scan would then lose NULL rows silently, the one
failure this unit may not have. The safe forms (the NULL test as its own stride, or a catalog
check inside the scan's snapshot) are a follow-up, filed in the hand-back.

## 5. R-4 citations (ConnectorX and ADBC)

- **ConnectorX is the bar and the nearest design.** Its `read_sql(conn, query, partition_on=,
  partition_range=, partition_num=)` splits one query into `partition_num` range queries over
  a numeric column, runs each on its own connection and thread, and writes straight into the
  destination; with no `partition_range` it first asks the server for the column's `MIN` and
  `MAX`. Its PostgreSQL source reads `COPY … TO STDOUT WITH BINARY`. C-3 keeps the shape (range
  queries over one column, one connection each, straight into Arrow, the same COPY protocol
  C-2 took from it) and its `read_sql` with `return_type="arrow"` is §4's bar. **Where C-3
  departs, each for a recorded reason:** the surface and the stride arithmetic are Spark's,
  not ConnectorX's even split (NS §2: Spark governs the surface; §0.1); rows outside the range
  and NULL rows are returned, where ConnectorX's documentation requires a partition column
  without NULLs and its range bounds the rows read (FL-2); every range reads one exported
  snapshot, where ConnectorX's ranges are independent statements (FL-1); the cuts are bound,
  where ConnectorX writes them into the query text (C-2's FL-13); connections come from the
  source's bounded pool, so a count above it queues (FL-3); and the column and its bounds are
  never chosen automatically (declared, §0.5). ConnectorX's documentation and its 0.4.6
  wheel's behaviour in §4 are the sources; its code was read for nothing beyond that.
- **Arrow ADBC.** C-2 took its type contract and its COPY-binary read from the ADBC PostgreSQL
  driver's documentation and departed where Spark's dialect differs (c-2-ledger §4). ADBC's API
  has a partitioned-result call (`AdbcStatementExecutePartitions`) for backends that hand back
  partition descriptors a client can read in parallel; per its documentation the PostgreSQL
  driver does not implement it, so ADBC offers no partitioned Postgres read to compare with
  and C-3 takes nothing new from it. ADBC's cursor contract (one statement, batches until the
  end, a typed error otherwise) is what each connection's stride sequence keeps: batches in
  order per connection, the scan ending at the first error. The documentation was read;
  nothing was run.

## 6. Gates (2026-10-08)

Run on the final tree in one hold of the build lock, the server up (2026-10-08). Every cargo
command ran as `flock /tmp/oc-worker/build-slots/opus-cargo.lock nice -n 10 taskset -c 32-47 env
CARGO_BUILD_JOBS=8 bash -c 'ulimit -v 67108864; …'`.

| gate | exit | line that means green |
|---|---|---|
| `cargo fmt --check` | 0 | no diff |
| `make rust-clippy` | 0 | no warning |
| `make rust-panic-ban` | 0 | no finding |
| `python3 scripts/check_rust_file_size.py` | 0 | 1127 files clean (default ceiling 1000; 34 exceptions) |
| `./scripts/check_lib_rs.sh` | 0 | 11 crate roots clean (ceilings held) |
| `./scripts/check_crate_dag.sh` | 0 | 25 internal edges clean |
| `python3 scripts/sync_map_md.py --check` | 0 | 369 maps clean |
| `bash scripts/check_map_md.sh --base origin/main` | 0 | silent |
| `python3 scripts/check_docs_links.py` | 0 | 1364 files, 7352 links checked, clean |
| `python3 scripts/check_ledger_grammar.py` | 0 | 313 live ledgers clean |
| `python3 /tmp/oc-worker/_lib/comment_ban.py /tmp/xc1 origin/main HEAD` | 0 | `comment-ban hits=0` |
| `cargo test -p repark-connect` (no server) | 0 | 155 passed, 0 failed, 60 ignored |
| `cargo test -p repark-connect -- --include-ignored` (server up) | 0 | 215 passed, 0 failed, 0 ignored |
| `cargo test -p repark-core -p repark-python --lib` | 0 | 1288 passed, 1 ignored (core); 151 passed (binding) |
| `make develop` | 0 | the editable module installed |
| `pytest python/repark-parity/tests/live_db` | 0 | 31 passed, 5 xfailed (C-0's strict xfails) |
| `pytest python/repark/tests -k "source or postgres or toml or named" -n 8` | 0 | 692 passed, 9 skipped |
| `.venv/bin/python -I scripts/check_example_coverage.py --require-execute` | 0 | 1080 public names, 968 covered, 251 examples |
| `cargo deny check` | 0 | advisories ok, bans ok, licenses ok, sources ok |
| `./scripts/check_manifest.sh` | 0 | silent |

Two process notes, recorded because the machine rules name them:

- **One build ran outside the lock.** At 23:01 on 2026-10-07 a bare `uv sync` started a
  from-source build of the native module and ran about nine minutes before its timeout, with no
  lock held. No timing run existed yet. Every later environment step used
  `--no-install-package repark` or `--no-install-workspace`, and every build ran under the lock.
- **Four long steps ran as tracked jobs.** The release wheel's last crate, the benchmark, the
  mutation battery and this gate run each need more than the ten minutes one foreground command
  is given. They ran as jobs the session tracks, never detached, and the session waited on each
  before doing anything else.

```
COVERAGE_ATTESTATION:
  pr_unit: c-3
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every item of the brief's step 0 is a subsection of the design note with its C-2 source line or a measurement; the order's three PENDING items are settled there; each delivered behaviour and each declared remainder is a clause; the departures from the first design (one pooled connection per frame, the NULL test kept on NOT NULL columns) are recorded where they were decided.
      artifacts: [task/ledgers/staging/c-3-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-2
      status: ATTACKED
      evidence: The i64 extremes as bounds, equal bounds, a span of one, counts of 1, 0 and -1, a count above the span, negative and straddling ranges, 700 seeded triples against Spark's own numbers, cuts outside the column's own integer range, bounds wholly outside the data, NULL and out-of-bounds rows, a column in another case, quoted, missing and ambiguous, a bound that is not an i64, LIMIT 0 and a limit past the table, 16 strides on a pool of 2, a pool with one free permit and with none.
      artifacts: [crates/repark-connect/tests/it/partition.rs, crates/repark-connect/tests/it/partition_plan.rs, crates/repark-connect/tests/it/live_partition.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal is a typed ConnectError with an enum reason folded to Spark's class; cuts that are not strictly increasing refuse instead of planning; the arithmetic is checked throughout and answers a refusal on overflow; a refused value ends the scan after its stride's earlier rows; a task that dies surfaces as ExecutionJoin when the channel closes; no unwrap, expect or panic in product code (panic-ban gate).
      artifacts: [crates/repark-connect/src/partition.rs, crates/repark-connect/src/provider/partitioned.rs, crates/repark-connect/src/error.rs]
    - id: AT-4
      status: ATTACKED
      evidence: One execute call owns the export, every import and every COPY, so no stride can pair with another run's snapshot; imports complete before any COPY starts; the multi-checkout waits for its first connection only, so two scans cannot hold each other; connection tasks are tracked and aborted on drop; a writer between strides, a dropped stream, a busy pool and an exhausted pool are live cells; the whole live suite ran four times in parallel with the rest of the crate's cells.
      artifacts: [crates/repark-connect/src/read/postgres_lanes.rs, crates/repark-connect/src/pool.rs, crates/repark-connect/tests/it/live_partition.rs, crates/repark-connect/tests/it/pool.rs]
    - id: AT-5
      status: ATTACKED
      evidence: Cuts are bound through the set_config carriage and never enter SQL text; the one value that must enter text, the snapshot id, is checked to be hex digits and dashes first; the column renders through PgIdent; every compare is pg_catalog-qualified; a bound that is not an integer and a predicates value are never echoed; EXPLAIN names no endpoint; every transaction is READ ONLY on a read-only session.
      artifacts: [crates/repark-connect/src/read/postgres.rs, crates/repark-connect/src/read/postgres_lanes.rs, crates/repark-connect/tests/it/partition_plan.rs, crates/repark-core/src/session/tests/read_postgres.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Partitioned rows equal unpartitioned rows value for value and type for type across the mapped types through eight partition plans in Rust and nine doors in Python; every id arrives exactly once; twelve Python reads under a concurrent writer return every id once; pushed filters with pushdown on and off agree; the first live run's empty OFFSET result was traced to DataFusion alone and is not a partition plan changing an answer.
      artifacts: [crates/repark-connect/tests/it/live_partition.rs, python/repark-parity/tests/live_db/test_c3_partitioned.py]
    - id: AT-7
      status: ATTACKED
      evidence: Decoding runs in one task per connection; the channel holds as many batches as there are connections and every batch carries its slice of the memory reservation; connections never exceed min(strides, pool_max_size) and a frame keeps one at rest; the 10M-row benchmark is recorded against ConnectorX and pandas with the server's two-CPU cap and the cost of the first stride's NULL test measured.
      artifacts: [crates/repark-connect/src/provider/partitioned.rs, python/repark-parity/tests/live_db/c3_bench.py, task/ledgers/staging/c-3-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency added and Cargo.toml and Cargo.lock unchanged (the decimal is Arrow's i256, the tasks DataFusion's SpawnedTask); no new crate edge; the provider trait and TableProvider::scan keep their signatures; C-2's one-statement scan runs the same statements through the refactored reader and its 51 live cells stay green; the facade's existing jdbc pins pass unchanged.
      artifacts: [crates/repark-connect/src/read/postgres.rs, crates/repark-connect/Cargo.toml]
    - id: AT-9
      status: ATTACKED
      evidence: Spark's refusal sentences are kept where Spark was measured; the declared refusals name the registry row and the supported types; the query refusal names the dbtable subquery form; EXPLAIN shows the column, the strides and the connection bound; the guide gains a partitioned-reads section; the registry row states what is delivered, what is declared and the two deliberate differences.
      artifacts: [crates/repark-connect/src/partition.rs, docs/guide/repark-toml.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Thirteen mutations and one combined run, each red in named pins with the tree restored, the brief's four among them; the stride arithmetic is compared with a recording of live Spark rather than with itself; two things without a pin are named in section 1.
      artifacts: [task/ledgers/staging/c-3-ledger.md, python/repark-parity/tests/live_spark/c3_stride_grid.txt]
  complete: true
```
