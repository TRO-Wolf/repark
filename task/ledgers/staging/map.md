# map — task/ledgers/staging/

## Purpose
Ledgers of units in flight. A ledger here on `main` is a charter whose retirement event has not
happened yet; every other ledger leaves for `../completed/` in its unit's last commit.

## Contents
- [csv-infer-perf-1-ledger.md](csv-infer-perf-1-ledger.md) —
  **CSV-INFER-PERF-1 (2026-09-06), in flight:** local CSV `inferSchema` no longer
  materializes the frame per candidate cast. Native DataFusion inference plus
  Utf8-only timestamp columns; `nullValue` keeps one `try_cast` aggregation.
  300k × 8 True 2.339 s → 0.079 s (0.95× of False). `risk_tier: standard`.
  Branch `perf/csv-infer-perf-1`.
  pins: csv-infer-perf-1/C-001, C-002, C-003, C-004, C-005, C-006
- [write-distribution-1-ledger.md](write-distribution-1-ledger.md) —
  **WRITE-DISTRIBUTION-1 (2026-09-06), in flight:** the hash distribution rule before a
  partitioned Iceberg write — Spark's `write.distribution-mode = hash`. A `RepartitionExec` under
  the CTAS write node, `Partitioning::Hash` over one `PartitionTransformExpr` per partition field
  (the fork's transform over the cast source column), so one partition value lands in one writer:
  the partitioned 1e6 CTAS goes 64 → 8 data files (Spark's count) and 3.44× → 1.96× of the
  parquet-sink control; the unpartitioned CTAS is untouched by decision. No dependency, no spawn.
  `risk_tier: standard`. Branch `perf/write-distribution-1`.
  pins: write-distribution-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [ex-28-scalar-remainder-ledger.md](ex-28-scalar-remainder-ledger.md) —
  **EX-28 (2026-09-06), in flight:** the v1.1 example backfill's `F.*` scalar
  remainder — the 34-name roster at base `57f21b9b`; seven names covered by
  extending three `docs/examples/functions/` files (backlog 136 → 129).
  Twenty-seven stay with existing EX-FN / BL-17 / FNP-15 / FNP-16 rows; two
  new §7 rows (EX-FN-20, EX-FN-21) pin `try_to_timestamp` and the
  `unix_timestamp` format arm. Every asserted value measured on live
  PySpark 4.1.2 (ANSI on, UTC). `risk_tier: standard`. Branch
  `docs/ex-28-scalar-remainder`.
  pins: ex-28-scalar-remainder/C-001, C-002, C-003, C-004, C-005, C-006
- [ex-27-ml-ledger.md](ex-27-ml-ledger.md) —
  **EX-27 (2026-09-05, round 2 2026-09-06), in flight:** the v1.1 example
  backfill's `ml.*` family — the 28-name roster at base `282607f5`; all 28 names
  covered by five `docs/examples/ml/` files (backlog 164 → 136). Round 2
  re-measured every oracle cell on live PySpark 4.1.2, including the
  session-level cells round 1 printed as "equal" without collecting. Nine §7
  rows (EX-ML-1..9) pin the diverged arms, with nine tests in
  `test_examples_ml.py`. Mixins are taught only through concrete stages.
  `risk_tier: standard`. Branch `docs/ex-27-ml`.
  pins: ex-27-ml/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [dynflatten-listnull-1-ledger.md](dynflatten-listnull-1-ledger.md) —
  **DYNFLATTEN-LISTNULL-1 (2026-09-06), in flight:** Spark's parquet reader infers
  `optional int32 element (Null)` as `array<int>`; repark kept `List(Null)` and
  `drop_null_lists=True` dropped `user_properties`. FIX: `promote_parquet_null_types`
  in `read_parquet_nullable` maps Arrow `Null` to `Int32` after the nullability relax.
  Default `drop_null_lists` stays True; SQL `make_array()` still drops. Live
  `read.parquet` + `dynamicFlatten` matches Spark including `user_properties` int32
  NULLs. `risk_tier: standard`. Branch `fix/dynflatten-listnull-1`.
  pins: dynflatten-listnull-1/C-001, C-002, C-003, C-004, C-005, C-006
- [sepmo-e2-ledger.md](sepmo-e2-ledger.md) —
  **SEPMO-E2 (2026-09-06), in flight, round 3:** compact role packets. Packet
  format v1 (eight field groups, stable prefix then dynamic, source identity,
  version), assembler `scripts/sepmo_packet.py` plus
  `scripts/sepmo_packet_extract.py` (`build` / `check` / `diff`), three
  converted campaign briefs as fixtures plus two prefix-only briefs,
  constraint-preservation tests (sidecar `STABLE_RULES` equality, trailer,
  re-render, `bash -n` through `build`/`check`, unbackticked boundary paths
  through `build`, prefix-negating phrases), and a baseline table against E-0
  cached/uncached ratios with no token-savings claim. Adoption proposal names
  `--brief` / `--followup`. `risk_tier: standard`. Branch
  `sepmo/e2-compact-packets`.
  pins: sepmo-e2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [sepmo-e0-e1-ledger.md](sepmo-e0-e1-ledger.md) —
  **SEPMO-E0E1 (2026-09-06), in flight, round 3:** telemetry inventory (E-0) and usage
  collector (E-1). Minority truncated JSONL and exit-without-terminal are degraded
  records; majority-bad still fails. Muse tokens come from the session store
  (`runs.tsv` join, `.msp-view-v1` pinned); cost is still absent. Grok live keys
  include `cache_read_input_tokens` and `modelUsage`. OpenCode sqlite has token
  and cost columns. Claude transcripts are not accessible. Collector is
  `scripts/sepmo_usage.py`. `risk_tier: standard`. Branch
  `sepmo/e0-e1-usage-collector`.
  pins: sepmo-e0-e1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [perf-ice-writepath-1-ledger.md](perf-ice-writepath-1-ledger.md) —
  **PERF-ICE-WRITEPATH-1 (2026-09-05), in flight:** the two write-path defects PERF-ANALYSIS-1
  ranked together, because both are read off the same CTAS pair. Fork half **F-28**: the
  partition splitter groups a batch with Arrow kernels and materializes one `Literal::Struct`
  per group instead of one per row, keeping the row-wise path where Arrow total-order equality
  is not Iceberg `Struct` equality. RePark half: `IcebergPartitionWriteExec`, a CTAS write node
  with one output partition per writer, so the parquet encode and zstd run on the executor's
  threads instead of sharing one task — no `tokio::spawn`, no new dependency. The commit is an
  ordering, not a layout: the manifest ascends by content and `_row_id` tiles it contiguously,
  while the layout and a row's `_row_id` vary with the scan's file grouping
  (`WRITE-GROUPING-CTAS-1`); a failed write into a fresh table deletes every data file it made. `risk_tier: elevated`. Branch `perf/ice-writepath-1`.
  pins: perf-ice-writepath-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009,
  C-010, C-011
- [h3-spill-residue-1-ledger.md](h3-spill-residue-1-ledger.md) —
  **H3-SPILL-RESIDUE-1 (2026-09-06), in flight:** the two Never-OOM failure shapes H3-SPILL-1
  filed and did not fix. `collect()` under an `RLIMIT_AS` ceiling now raises `MemoryError`:
  every CPython allocation on the row fast path goes through `Bound::from_owned_ptr_or_err`,
  because pyo3's safe constructors reach `assume_owned` and panic on NULL **even where the
  signature returns `PyResult`** — and that panic consumes the `MemoryError` on its way out, so
  catching it later cannot recover it. A nested-loop join at a bounded pool now refuses with the
  same typed exception every other operator gives: a bounded session's `FairSpillPool` is wrapped
  in `RefusalRecordingPool`, and the Arrow reader reports a fenced panic that a recorded refusal
  caused as that refusal. The DataFusion defect behind it is **upstream and still open** — 54.1's
  `NestedLoopJoinExec` re-executes partition 0 of its build child on the OOM fallback path — and
  the issue text is in the ledger; no dependency changed. Measured before and after on release
  modules: the matrix's only `internal_error` cell is `clean_error` 3/3, the other 17 operators
  at 8 MiB are identical cell for cell, and the `collect` happy path's two five-run distributions
  overlap. **Round 2 (2026-09-06)** answered five critic findings, one of them S1: the containment
  rule was unbounded — an injected `index out of bounds` panic after one refusal came back as a
  pool refusal — so a fourth gate now requires the payload to be one DataFusion 54.1 can reach on
  its refusal and spill-fallback paths, cited line by line. The scope claim was corrected rather
  than the code: the refusal log is session-scoped, not per-stream, and cannot be per-stream. Two
  more honest-limits disclosures landed: a contained refusal still prints 4 panic blocks to
  stderr, and `toPandas()` under a 64 MiB address-space headroom aborts the process where
  `collect()` raises `MemoryError`. Seven mutations, seven kills. `risk_tier: elevated`.
  Branch `harden/h3-spill-residue-1`, PR #401.
  pins: h3-spill-residue-1/C-001, C-002, C-003, C-004, C-005
- [h3-spill-1-ledger.md](h3-spill-1-ledger.md) — Round 3: C-004 counts 22 pins.
  **H3-SPILL-1 (2026-09-05), in flight:** the Never-OOM truth table. 180 cells (18 operators ×
  5 pool sizes × 2 scales), each a fresh subprocess on a release module under a resident-memory
  watchdog: **zero aborts, zero wrong answers**, and 115 of the 144 bounded cells carrying a
  disclosed content digest that equals the unbounded run (163 run digests once repeats are
  counted). Pins only — no product code changed. Round 2 answered eight critic findings, all
  about what the matrix checked and claimed rather than what it measured — a row count is not an
  answer digest, a repeat's digest must not be discarded, and a published error string must be
  one that was recorded. Two failure-shape defects filed as §7 BACKLOG rows
  with pins that red when fixed: `H3-SPILL-NLJ-1` (a nested-loop join at a tight pool answers
  with a caught Rust panic from DataFusion's `RepartitionExec`) and `H3-SPILL-COLLECT-1`
  (`collect()` under an address-space limit panics on a null `PyObject` instead of raising
  `MemoryError`). The document's honest limit is §2: windows, `Unnest`, the Iceberg scan and the
  facade boundary take no pool reservation at all, so no pool bounds them. `risk_tier: standard`.
  Branch `harden/h3-spill-1`.
  pins: h3-spill-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-ice-catalog-io-2-ledger.md](perf-ice-catalog-io-2-ledger.md) —
  **PERF-ICE-CATALOG-IO-2 (2026-09-05), landed default-OFF per the round-2 ruling:** the
  RePark side of CATALOG-IO part 3 at fork pin `79119643` (RP-12, already on the base). A
  session key `repark.iceberg.manifestCacheBytes`
  (alias `repark.iceberg.manifest_cache_bytes`, default `0` = off, set bytes to opt in)
  sizes the fork's shared manifest `ObjectCache` for the memory catalog; every table the
  catalog materializes carries the one cache. Shipped: the part-3 pin un-skipped and green
  with the knob set explicitly (`t_many/count_id/stmt2` **115.81 → 10.95 ms**, target ≤ 20;
  repeated reads open no manifest at all); the six IO-1 staleness pins re-run green with
  the cache off (the default) plus new explicit-knob Python legs per cell (MERGE, DROP +
  re-CREATE, `register_table`, rewrite + expire, time-travel, branch); the funnel pinned
  by manifest deletion; correctness pinned under a 512-byte budget over eight tables; two
  lineage detector pins holding `PERF-CATALOG-LINEAGE-CACHE-1`'s shape (knob-on NULLs,
  knob-off assigned). `PERF-ICE-MANIFEST-1` BACKLOG with before/after;
  `PERF-CATALOG-CACHE-BOUND-1` NARROWED to the metadata cache;
  `PERF-CATALOG-COMMIT-CACHE-1` / `F-CATIO-COMMIT` filed BACKLOG — the
  census showed DML saving read-side repeats only, because the fork's transaction paths never
  consult the cache (0 vs 166+ direct loads), which re-reads but never serves stale. Glue and
  S3 Tables are NOT wired (their builders have no such method at the pin). `git diff
  origin/main -- Cargo.toml Cargo.lock` empty.
  Risk-first: **five** Rust parse pins plus **four** Rust delete-manifest pins plus **thirteen**
  Python legs, with a **seven**-mutation score (one escape closed: the knob-off control parsed
  no string until it was strengthened to). The in-lane critic pass found two claim-scope
  overstatements (the map's "every table shares", the unrecorded `memory_catalog()` behaviour
  change) and both are remediated in the ledger's critic table.
  **FINDING S1-1 (C-004 REJECTED), resolved by the round-2 ruling:** the unit HALTED mid-flight
  when the facade suite redded 4 upgrade-lineage tests — the fork's `(path, schema)` manifest
  key does not carry the list-entry lineage range, so a v2-context parse poisons v3 reads of
  the same path (`_row_id` NULL). The ruling landed option (b): default OFF, so the four tests
  are green by default and the fix (`F-CATIO-KEY`, separate fork unit) plus a later default-ON
  flip close the remainder. Filed `PERF-CATALOG-LINEAGE-CACHE-1` / `F-CATIO-KEY` (fork-side,
  no RePark fix exists). `risk_tier: standard`. Branch `perf/ice-catalog-io-2`.
  pins: perf-ice-catalog-io-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-ice-catalog-io-3-ledger.md](perf-ice-catalog-io-3-ledger.md) —
  **PERF-ICE-CATALOG-IO-3 (2026-09-05), landed default-ON:** the flip IO-2's round-2
  ruling named as the follow-up, on the fixed pin `2ed39cb0` (RP-13, `F-CATIO-KEY`).
  `DEFAULT_MANIFEST_CACHE_BYTES` is 32 MiB; the four HALT tests, the staleness
  battery and the lineage pins run on default sessions and are green; a two-session
  concurrency leg and a 500-table subprocess RSS comparison (332.2 vs 323.9 MB, delta
  8.3, bar 64) prove the fork-fix contract and the bound.
  `t_many/count_id/stmt2` **123.47 → 11.27 ms** on the default session (target ≤ 20);
  `PERF-ICE-MANIFEST-1` FIXED with the default-session number;
  `PERF-CATALOG-CACHE-BOUND-1` narrowed with the measured RSS. Charter committed
  red-first (the default pins redded on the base, then green after). 7 PROVEN, 0 OPEN.
  `risk_tier: elevated`. Branch `perf/ice-catalog-io-3`.
  pins: perf-ice-catalog-io-3/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [perf-agg-avg-1-ledger.md](perf-agg-avg-1-ledger.md) —
  **PERF-AGG-AVG-1 (2026-09-05), in flight:** the `GroupsAccumulator` for the Spark
  `avg` / `try_avg` UDAF (PERF-ANALYSIS-1 slate item 8, candidate 10) — Float64 and
  Decimal32/64/128/256 grouped paths with Spark's result rules and `try_avg`
  overflow → NULL on the 2×-MAX shape (`AVG-DEC-SUMWRAP-1` files the wrap shape),
  the retract path untouched for window frames. 5 PROVEN, 0 OPEN,
  1 REJECTED (Q17 ≤ 3× missed with the sum-floor proof; avg/sum ≤ 1.3× met); gates
  green, attestation filed. `risk_tier: standard`. Branch `perf/agg-avg-1`.
- [perf-ice-catalog-io-1-ledger.md](perf-ice-catalog-io-1-ledger.md) —
  **PERF-ICE-CATALOG-IO-1 (2026-09-05), in flight:** the catalog-IO unit at base `6eaccd5e`.
  Shipped: a session-scoped Iceberg metadata cache keyed by metadata-file **location**, built once
  per session and handed to every **memory** catalog it builds, behind `repark.iceberg.metadataCache`
  (default on) and `repark.iceberg.metadataCacheEntries` (default 512, a high-water clear at the
  statement door). `metadata.json` READS fall from 2 (SELECT) and 3–6 (DML) to **0 on every
  statement that reads an existing table** — the analysis' §7.6 TOTALS split into reads and the
  commit's own write, reads + writes reproducing §7.6 exactly. `CREATE TABLE` and CTAS still read
  1 with the knob on and off: creation is not cacheable. `PERF-CATALOG-CALLS-1` FIXED **narrowly**
  — the metadata document is fetched once per location; the count of catalog round trips per
  statement is UNCHANGED, and Glue / S3 Tables are NOT wired at all.
  Fork-gated and NOT shipped, one registry row each: one load per planning round
  (`PERF-CATALOG-LOADS-1` / `F-CATIO-A`), the shared path-keyed manifest cache
  (`PERF-ICE-MANIFEST-1` / `F-CATIO-B`), the Glue and S3 Tables metadata cache
  (`PERF-CATALOG-AWS-CACHE-1` / `F-CATIO-AWS`) and a bounded LRU inside the fork's cache
  (`PERF-CATALOG-CACHE-BOUND-1` / `F-CATIO-BOUND`). A and B are implemented and test-green in
  `$HOME/repark-lanes/lanes/catio-fork` and measured through a temporary, never-committed path
  override: `t_many` second-statement `count_id` **120.01 → 11.33 ms** (target ≤ 20) with a
  repeated read opening no manifest at all. Three pins SKIP naming their ask;
  `git diff origin/main -- Cargo.toml Cargo.lock` empty.
  Risk-first: **twelve** Rust pins on two doors over ONE catalog plus **thirteen** always-run
  Python legs, green before and after, with a **six**-mutation score (two of them escapes that
  were closed). **Round 2** (Opus critic) reproduced the engine independently and found no stale
  read or lost write; its eleven findings were all claim, scope and filing, and each is dispositioned
  in the ledger's "Round 2 — review gaps" table. `risk_tier: elevated`. Branch
  `perf/ice-catalog-io-1`.
  pins: perf-ice-catalog-io-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
- [ex-26-io-session-ledger.md](ex-26-io-session-ledger.md) —
  **EX-26 (2026-09-06), in flight:** the v1.1 example backfill's reader/writer/session long-tail
  batch — the 50-name roster at base `24932dee` (= `origin/main` at dispatch); 29 names covered
  by twelve new `docs/examples/{io,session,dataframe}/` files (backlog 193 → 164;
  `BACKLOG_BASELINE` 193 → 164), every asserted value measured on live PySpark 4.1.2 (ANSI on,
  UTC) or on repark's documented answer for the repark-only names; seventeen roster names keep
  their prior stays rows and the four excel names stay with the new §7 EX-IO-7 row, while eleven
  new rows (EX-IO-1..10, EX-SES-6) pin the diverged arms of covered names, with thirteen
  tests in `test_examples_io_session.py` (plus a dated EX-SES-1 Spark-half correction). Red-first: 29
  has-no-example findings with the files held out (exit 1), and the wrong-bytes control in
  `writer_csv.py` failed the execute leg by name (exit 1). `risk_tier: standard`. Branch
  `docs/ex-26-io-session`.
  pins: ex-26-io-session/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-013, C-014, C-015
- [ex-25-functions-a-ledger.md](ex-25-functions-a-ledger.md) —
  **EX-25 (2026-09-05), in flight:** the v1.1 example backfill's `F.*` long-tail (a) batch —
  the 45-name roster at base `bc7c76cc` (= `origin/main` at dispatch); 20 names covered by five
  new `docs/examples/functions/` files plus the `F.hours` arm in `partition_transforms.py`
  (backlog 213 → 193; `BACKLOG_BASELINE` 213 → 193), every asserted value measured on live
  PySpark 4.1.2 (ANSI on, UTC); the other 25 stay with nineteen new §7 rows (EX-FN-1..19;
  `F.base64` keeps BL-17), pinned by twenty tests in `test_examples_functions_a.py`. No
  `csv_json.py`: all four CSV/JSON names refuse. Red-first: 20 has-no-example findings with
  the files held out (exit 1), and the wrong-median control in `stats.py` failed the execute
  leg by name (exit 1). `risk_tier: standard`. Branch `docs/ex-25-functions-a`.
  pins: ex-25-functions-a/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [ex-24-ta-b-ledger.md](ex-24-ta-b-ledger.md) —
  **EX-24 (2026-09-04), in flight:** the v1.1 example backfill's TA-kernels (b) batch — the
  remaining 45 `ta.*` backlog names at base `188499a6` (= `origin/main` at dispatch); all 45
  covered by twelve `docs/examples/ta/` files (backlog 258 → 213; `BACKLOG_BASELINE` 258 → 213)
  measured against the recorded C TA-Lib 0.4.0 goldens on the 5000-row OHLCV fixture (Spark has
  no TA kernels — the goldens are the family's oracle, the same `.bin` files
  `test_ta.py`/`test_ta_volume.py` pin bit-identically); all 45 bit-identical, zero divergences,
  no §7 row, no new pin file; the `over_columns`/`with_indicators` composition helpers are
  covered through fused examples whose every produced column is asserted bit-exact. Red-first:
  45 has-no-example findings with the files held out (exit 1), and the bit-exact control named
  kernel, row and both values on a bulk overwrite (exit 1). `risk_tier: standard`. Branch
  `docs/ex-24-ta-b`. pins: ex-24-ta-b/C-001, C-002, C-003, C-004
- [ex-23-ta-a-ledger.md](ex-23-ta-a-ledger.md) —
  **EX-23 (2026-09-04), in flight:** the v1.1 example backfill's TA-kernels (a) batch — the
  first 40 `ta.*` backlog names at the dispatch base `671a7144` (shipped on `bfef4a62`); all 40 covered by eight
  `docs/examples/ta/` files (backlog 298 → 258 shipped; 340 → 300 at dispatch) measured against the recorded C TA-Lib 0.4.0
  goldens on the 5000-row OHLCV fixture (Spark has no TA kernels — the goldens are the family's
  oracle, the same `.bin` files `test_ta.py`/`test_ta_volume.py` pin bit-identically); all 40
  bit-identical, zero divergences, no §7 row, no new pin file. Round 2 (critic): the examples'
  durable control is now full-array bit-exact (`expect_bit_exact` over all 5000 rows — the
  tail-only 1e-9 control was blind to the NaN prefix), the 24 helper docstrings are stripped to
  house form, and the red-first re-run (four mutations) all exit 1. `risk_tier: standard`. Branch
  `docs/ex-23-ta-a`. pins: ex-23-ta-a/C-001, C-002, C-003, C-004
- [win-slide-1-ledger.md](win-slide-1-ledger.md) — **WIN-SLIDE-1 (2026-09-04), in flight:** the
  thirteen aggregates that refused over a sliding frame now answer Spark-equal on both doors.
  One mechanism, not thirteen: a `sliding_frame_rescan` analyzer rule on every core session
  re-evaluates the frame per row into a fresh accumulator when DataFusion's sliding accumulator
  cannot retract — by capability, so a future aggregate never refuses. The physical `WindowExpr`
  route is closed in DF 54.1 (`WindowFn` is unexported) and §7.2 names the gap. Two door bugs
  found and fixed on the way (`WIN-RANGE-DF-1`, `WIN-COLLECT-DOOR-1`) and the frame case of the
  `percentile_approx` accuracy divergence filed (`WIN-SLIDE-PCT-ACC-1`). `risk_tier: standard`.
  Branch `feat/win-slide-1`.
  pins: win-slide-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [ex-21-catalog-session-ledger.md](ex-21-catalog-session-ledger.md) —
  **EX-21 (2026-09-04, r2), in flight:** the v1.1 example backfill's `Catalog.*` remainder +
  `SparkSession` surface (a) batch — 35 roster names at base `b5b17f0`; 34 covered by sixteen
  `docs/examples/catalog/` and `docs/examples/session/` files (backlog 411 → 377; 374 → 340
  after the EX-20 merge), `list_databases` stays (same function object as the divergent
  `listDatabases`, §7 `EX-CAT-2`), the `registerFunction` return, `newSession` promotion,
  empty `create_dataframe`, unset-key `conf.get`, and missing-path reader arms are §7
  `EX-SES-1`..`EX-SES-5`, pins in
  `python/repark/tests/test_examples_window_catalog.py`. `risk_tier: standard`. Branch
  `docs/ex-21-catalog-session`. pins: ex-21-catalog-session/C-001
- [fnp-9-collections-json-ledger.md](fnp-9-collections-json-ledger.md) — **FNP-9/10
  (2026-09-05), in flight:** the collections and JSON function families. Ten names built
  Spark-equal on both Spark-facade doors (`get_json_object`, `json_array_length`,
  `json_object_keys`, `to_json`, `from_json`, `schema_of_json`, `create_map`, `map_concat`,
  `array_insert`, `arrays_zip`); seven §7 rows file what the unit measured and did not build,
  each with a pin that reds when the seam closes. Six unbuilt names share ONE seam — Spark's
  multi-column generators (`posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `stack`,
  facade `json_tuple`) need a plan shape the facade select path does not have, so the seam is
  filed once instead of six one-column impostors. The JSON reader is hand-written so
  `Cargo.lock` stays untouched, and it is the better fit anyway — Spark keeps an integer token
  verbatim and re-renders the rest through `Double.toString`.
  Branch `feat/fnp-9-collections-json`.
  pins: fnp-9-collections-json/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008,
  C-009
- [fn-regexp-extract-1-ledger.md](fn-regexp-extract-1-ledger.md) — **FN-REGEXP-EXTRACT-1
  (2026-09-04):** Spark `regexp_extract(str, regexp[, idx])` on both doors (the last
  regexp kernel; closes the R-FN-BATCH1 gap the FN-FIX-2-CTRL-1 control exposed).
  Round 2: idx validated only inside the match arm; §7 `FN-REGEX-LOOKAROUND-1` filed;
  facade 2-arg widening disclosed.
  pins: fn-regexp-extract-1/C-001, C-002, C-003, C-004
- [fn-fix-2-string-rows-ledger.md](fn-fix-2-string-rows-ledger.md) — **FN-FIX-2 (2026-09-04):**
  six silent string rows become Spark-equal (`FN-INITCAP-1`, `FN-CHR-1`,
  `FN-TRIM-CHARS-1`, `FN-ELT-1`, `FN-REGEX-POSIX-1`, `FN-LIKE-ESCEND-1`).
  pins: fn-fix-2-string-rows/C-001, C-002, C-003, C-004
- [fn-fix-2-ctrl-1-controls-ledger.md](fn-fix-2-ctrl-1-controls-ledger.md) — **FN-FIX-2-CTRL-1
  (2026-09-04), in flight:** the seven incidental controls FN-FIX-2's critic found
  missing, measured on live PySpark 4.1.2 (both ANSI modes) and pinned; controls 2–7
  Spark-equal, control 1 (`regexp_extract`) refusal pinned on both doors
  (FINDING F-FN-FIX-2-CTRL-1-1, ACCEPTED_FLAGGED round-3; Spark `'alpha'`/`''`;
  flag superseded by FN-REGEXP-EXTRACT-1 — answer pin since merge `60ad77b0`);
  round-3 adds NULL `ltrim`/`rtrim` pins, the SQL `RLIKE`-keyword refusal pin
  (§7 FN-RLIKE-KEYWORD-1), and reversible ANSI legs.
  `risk_tier: standard`. Branch
  `fix/fn-fix-2-ctrl-1-controls`. pins: fn-fix-2-ctrl-1-controls/C-001, C-002, C-003, C-004
- [fnp-0-charter-ledger.md](fnp-0-charter-ledger.md) — **the Spark function parity campaign's
  scope audit and approval gate (2026-08-20):** the twelve-clause proposition ledger, the spike
  evidence behind it; C-007 (the four sub-project families) was closed by ruling D-7 on
  2026-08-20 and the gate passed. Design:
  [../docs/design/spark-function-parity.md](../../../docs/design/spark-function-parity.md); CAP-1
  appends a compatibility note that points its dated file-size premise at the live guards; slate:
  [../briefs/spark-function-parity.md](../../../briefs/spark-function-parity.md).
- [v3-0-charter-ledger.md](v3-0-charter-ledger.md) —
  **V3-0 (2026-08-21):** the format-v3 scope audit, and the defect it found. Intended as a
  charter with no product change and it does not close that way. **Read §3 first**:
  `rewrite_data_files` had no format-version check and reassigned every row's lineage on a v3
  table while returning the correct rows, where Spark carries lineage through unchanged. It is
  reachable on a v3 table that was already in the catalog, which is the drop-in case, so the
  guard shipped with the audit (`V3-LINEAGE-1`). §2 is the other half of the news, and it is
  good: v3 reads and v3 appends are already correct, round-tripped through Spark, including the
  row lineage the format mandates. §4 answers A12's stated first question — adoption, through
  `register_table`, whose Spark signature is measured there.

- [dbt-1-adapter-ledger.md](dbt-1-adapter-ledger.md) — **DBT-1 (2026-09-04), in flight:** a dbt
  path for RePark, so cutover step C6 can move gold off Spark/Glue
  ([../../../docs/cutover/inventory.md](../../../docs/cutover/inventory.md) ruling 2). Design
  first: every statement shape dbt emits for the two gold models and their ten test blocks was
  run through `repark.sql()` on a memory catalog, and the seventeen refusals are §3.2. They are
  all in the **statement surface**, not the transport, so a Spark-Thrift endpoint was rejected on
  measurement — the route is an in-process `dbt-repark` adapter subclassing `dbt-spark`'s
  `SparkAdapter`, so `file_format='iceberg'` keeps one reading rather than two.
  `dbt run` + `dbt test` build both models and pass all ten blocks on the S6 answers (59 passed,
  1 skipped, via `make py-test-dbt` in `preflight`); the Glue leg is written and skipped for the
  orchestrator. Ten registry rows: §2.5 `DBT-VIEW-1`, `DBT-TEMPVIEW-1`, `DBT-DESC-1`,
  `DBT-TBLPROPS-1` (extended in round 2 to cover `SHOW TABLE EXTENDED`, whose message it shares
  verbatim), `DBT-CREATENS-1`; §7 `B-TZ-5` (promoted from the awaiting-pins queue when this unit
  pinned it), `DBT-CTASCLAUSE-1`, `DBT-RELCOMMENT-1`, `DBT-COLCOMMENT-1`, `DBT-QUALIFY-1`.
  **Round 2 (Opus critic, FAIL on 7 S2 + 3 S3) is §10**; read §6 for the mutation table, which
  now carries a zero-red control and states which mutations are true no-ops rather than gaps.
  `risk_tier: standard`. Branch `feat/dbt-1`.
  pins: dbt-1-adapter/C-001, C-002, C-003, C-004, C-005

## Pointers
- Up: [../map.md](../map.md)
- [perf-scan-1-plan-once-ledger.md](perf-scan-1-plan-once-ledger.md) —
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04), in flight:** `TargetScanStream` caches
  `FileScanTask`s across concurrent `StreamingTable` re-executes (hardening). Registry
  `PERF-SCAN-3PASS-1` stays BACKLOG: production identity DELETE is 1 + 0 + 1 opens, not
  3 × N at scan. `risk_tier: standard`. Branch `perf/scan-1-plan-once`.
  pins: perf-scan-1-plan-once/C-001, C-002, C-003, C-004
- [sql-harden-1-cutover-shapes-ledger.md](sql-harden-1-cutover-shapes-ledger.md) —
  **SQL-HARDEN-1 (2026-09-04), in flight:** the cutover pipeline cutover Iceberg SQL shapes S1–S7
  measured against live Spark on the memory catalog; Glue + S3 Tables legs. Four registry
  rows filed, `V3-COV-7` cited, 0 FIXED. `risk_tier: standard`. Branch
  `feat/sql-harden-1-cutover-shapes`. pins: sql-harden-1-cutover-shapes/C-001
- [sql-harden-2-cow-shapes-ledger.md](sql-harden-2-cow-shapes-ledger.md) —
  **SQL-HARDEN-2 (2026-09-04), in flight:** S1/S2/S4 at v2 and v3 copy-on-write (S8/S9).
  `delete_files` empty both engines; data-file count 1 after the second MERGE; remaining
  DIVERGES are `CUTOVER-CTAS-REQ-1` / `V3-COV-7`. No `CUTOVER-COW-*` row. Glue + S3 Tables
  PASS. `risk_tier: standard`. Branch `feat/sql-harden-2-cow-shapes`.
  pins: sql-harden-2-cow-shapes/C-001, C-002, C-003, C-004
- [rp-10-repin-f25-ledger.md](rp-10-repin-f25-ledger.md) — **RP-10 (2026-09-04), in flight:**
  the fork repin `594bdbe5` → `85a4aaf0` (F-25). `validate_fresh_dvs_only` stops once every
  `added_dvs` key is found; `PERF-DVCLOSE-STMT-1` closes. `risk_tier: standard`. Branch
  `feat/rp-10-repin-f25`.
- [date-fn-1-spark-date-spelling-ledger.md](date-fn-1-spark-date-spelling-ledger.md) —
  **DATE-FN-1 (2026-09-04), in flight:** Spark SQL `date()` spelling and `unix_timestamp`;
  `CUTOVER-DATE-1` FIXED; S6 gold rows Spark-equal, program still DIVERGES on `V3-COV-7`.
  `risk_tier: standard`. Branch `fix/date-fn-1-spark-date-spelling`.
  pins: date-fn-1-spark-date-spelling/C-004
- [ex-15-dataframe-a-ledger.md](ex-15-dataframe-a-ledger.md) —
  **EX-15 (2026-09-04), in flight:** the v1.1 example backfill's first `DataFrame.*` batch —
  36 roster names at base `c70a306`; 28 covered by eight `docs/examples/dataframe/` files
  (backlog 578 → 550), 8 measured divergences stay with §7 rows `EX-DF-1`…`EX-DF-6` and pins in
  `python/repark/tests/test_examples_dataframe_a.py`. `risk_tier: standard`. Branch
  `docs/ex-15-dataframe-a`. pins: ex-15-dataframe-a/C-001
- [ex-16-dataframe-b-ledger.md](ex-16-dataframe-b-ledger.md) —
  **EX-16 (2026-09-04), in flight:** the v1.1 example backfill's second `DataFrame.*` batch —
  36 roster names at base `f3968aa`; 32 covered by eight `docs/examples/dataframe/` files
  (backlog 550 → 518); `intersectAll`/`intersect_all` and `groupingSets`/`grouping_sets` stay
  with §7 rows `EX-DF-7`/`EX-DF-8`, and the narrow `mergeInto`/`printSchema` arms are recorded as
  §7 rows `EX-DF-9`/`EX-DF-10`, pins in `python/repark/tests/test_examples_dataframe_b.py`.
  `risk_tier: standard`. Branch `docs/ex-16-dataframe-b`. pins: ex-16-dataframe-b/C-001
- [ex-18-dataframe-c-ledger.md](ex-18-dataframe-c-ledger.md) —
  **EX-18 (2026-09-04), in flight:** the v1.1 example backfill's third `DataFrame.*` batch —
  36 roster names at base `e3600a1`; 35 covered by eleven `docs/examples/dataframe/` files (backlog 484 →
  449 through the EX-16/EX-17 merges), `toJSON` stays (R-DF-BATCH2), §7 `EX-DF-11`…`EX-DF-17`, pins in `python/repark/tests/test_examples_dataframe_c.py`. `risk_tier: standard`. Branch `docs/ex-18-dataframe-c`. pins: ex-18-dataframe-c/C-001
- [ex-17-column-a-ledger.md](ex-17-column-a-ledger.md) —
  **EX-17 (2026-09-04, r2), in flight:** the v1.1 example backfill's `Column.*` (a) batch —
  40 roster names at base `e3600a1`; 34 covered by ten `docs/examples/column/` files
  (backlog 550 → 516 at base; 484 after the EX-16 merge), 6 engine-plumbing rows stay (no PySpark analog), the two
  measured divergent bare-name arms are §7 rows `EX-COL-1`/`EX-COL-2` with pins in
  `python/repark/tests/test_examples_column_a.py`. `risk_tier: standard`. Branch
  `docs/ex-17-column-a`. pins: ex-17-column-a/C-001
- [df-printschema-1-trailing-newline-ledger.md](df-printschema-1-trailing-newline-ledger.md) —
  **DF-PRINTSCHEMA-1 (2026-09-04), in flight:** `printSchema` stdout byte-identical to
  Spark's (flat, nested, array, `level=1` exact captures); `EX-DF-10` flipped to FIXED in
  the merge commit `68e408d`. `risk_tier: standard`. Branch
  `fix/df-printschema-1-trailing-newline`.
  pins: df-printschema-1-trailing-newline/C-004
- [ex-20-window-catalog-ledger.md](ex-20-window-catalog-ledger.md) —
  **EX-20 (2026-09-04), in flight:** the v1.1 example backfill's `Window`/`WindowSpec` +
  first `Catalog.*` batch — 40 roster names at base `3484f8d7`; 37 covered by eight files
  under `docs/examples/window/` and `docs/examples/catalog/` (backlog 411 → 374 shipped;
  449 → 412 at the dispatch base), 3 stay
  (`getDatabase`/`get_database`, `listDatabases`) with §7 rows `EX-CAT-1`/`EX-CAT-2`, the
  `functionExists` dbName arm is `EX-CAT-3`, and the DataFrame-door tied-key default frame
  is `EX-WIN-1`, pins in `python/repark/tests/test_examples_window_catalog.py`.
  `risk_tier: standard`. Branch `docs/ex-20-window-catalog`. pins: ex-20-window-catalog/C-001
- [ex-19-dataframe-d-window-ledger.md](ex-19-dataframe-d-window-ledger.md) —
  **EX-19 (2026-09-04, r3), in flight:** the v1.1 example backfill's fourth `DataFrame.*` batch —
  the 39-name DataFrame remainder plus GroupedData, Row, na, and stat surfaces at base `7496049`;
  38 covered by ten `docs/examples/dataframe/` files (backlog 449 → 411 shipped after the EX-18
  merge; 518 → 480 at the dispatch base), `stat.freqItems` stays
  with §7 `EX-DF-19`, the `withColumnsRenamed` duplicate-name arm is §7 `EX-DF-18`, the struct
  `Row` field arm is §7 `EX-ROW-1`, pins in
  `python/repark/tests/test_examples_dataframe_d.py`. `risk_tier: standard`. Branch
  `docs/ex-19-dataframe-d-window`. pins: ex-19-dataframe-d-window/C-001
- [perf-dynflatten-2-null-mask-ledger.md](perf-dynflatten-2-null-mask-ledger.md) —
  **PERF-DYNFLATTEN-2 (2026-09-04), in flight:** the one candidate PERF-DYNFLATTEN-1 queued,
  built. A scalar UDF unions the parent struct's validity into the child array instead of a
  per-leaf `CASE WHEN parent IS NULL`; `struct_d6`'s isolated null cost 64.83 ms → 0.01 ms
  (0.1x its run's floor), every bed row set, schema and ordered-row digest identical against a
  rebuilt pre-extractor module, `DYNFLATTEN-QUALNAME-1` FIXED as a side effect and re-pinned as
  an answer pin. `risk_tier: standard`. Branch `perf/dynflatten-2-null-mask`.
  pins: perf-dynflatten-2-null-mask/C-001, C-002, C-003, C-004, C-005
- [ex-22-types-writerv2-ledger.md](ex-22-types-writerv2-ledger.md) —
  **EX-22 (2026-09-04), in flight:** the v1.1 example backfill's `types` + `DataFrameWriterV2`
  batch — all 43 roster names at base `b5827be6`; 42 covered by eleven files under
  `docs/examples/types/` (new) and `docs/examples/io/` (backlog 340 → 298 shipped; 374 → 332
  at the dispatch base), the flagged
  `VariantType`/`TimeType`/`CharType`/`VarcharType` measured Spark-equal, the Arrow helpers and
  four snake_case spellings covered as repark extensions; `DataFrameWriterV2.overwrite` stays
  with §7 `EX-W2-1`, the empty-source `overwritePartitions` arm is §7 `EX-W2-2`, the
  `option`/`options` branch-tag arm is §7 `EX-W2-3`, and the round-2 unpartitioned-table
  `overwritePartitions` parser leak is §7 `EX-W2-4` (OPEN, fix unit
  `WRITERV2-OVERWRITE-UNPART-1`), pins in
  `python/repark/tests/test_examples_window_catalog.py`. `risk_tier: standard`. Branch
  `docs/ex-22-types-writerv2`. pins: ex-22-types-writerv2/C-001, C-002, C-003, C-004, C-005
- [perf-dynflatten-1-measure-ledger.md](perf-dynflatten-1-measure-ledger.md) —
  **PERF-DYNFLATTEN-1 (2026-09-04), in flight:** measure `dynamicFlatten` on the
  nested bed; rank the three H-3 intake candidates. `risk_tier: standard`.
  Branch `perf/dynflatten-1-measure`.
  pins: perf-dynflatten-1-measure/C-001, C-002, C-003, C-004
- [perf-facade-1-ledger.md](perf-facade-1-ledger.md) —
  **PERF-FACADE-1 (2026-09-04), in flight:** slate items 1 and 2 of PERF-ANALYSIS-1, the two
  biggest measured user-visible walls. `collect()` row materialization moves into
  `repark-python` (`collect_rows.rs` emits value tuples; the facade builds every `Row` from one
  shared names tuple per batch with the collector suspended): 1e6 x 7 **4,908 -> 956 ms**
  (5.14x), from 1.37x slower than Spark to 3.79x faster. `DataFrame.columns` answers from the
  plan's logical schema (`logical_names.rs`) and `with_columns` reads it once per call instead
  of once per existing column: depth-100 chain build **2,385 -> 367 ms** (6.50x, 5,750 analyzer
  passes -> 0), from 3.19x slower than Spark to 2.04x faster. The 150 ms chain target is NOT
  met and is reported as missed: the residue is DataFusion's own per-expression projection
  validation, and the collapse that would close it measures **65.04 ms** — under the bar, so
  `PERF-FACADE-CHAIN-2` is deferred on correctness (plan lineage, `_origin_plan_id`,
  `MISSING_ATTRIBUTES`) and not because the prize is small. Mutation 8 of 8 red; an independent
  critic reproduced the unit on its own clone and reds 7 of its own. Round 2 replaced the
  baseline with a tracked runner (`python/repark-parity/bench/facade/`, `make facade-bench`) and
  re-measured every number with it, because the first baseline's probes were untracked.
  `risk_tier: standard`. Branch `perf/facade-1`.
  pins: perf-facade-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
- [cutover-schema-1-ledger.md](cutover-schema-1-ledger.md) —
  **CUTOVER-SCHEMA-1 (2026-09-04), in flight:** nullability derived the way Spark
  derives it — reader relax, CTAS all-optional on both doors, decimal-cast analyzer
  rule, export-boundary Utf8 coercion. Closes `CUTOVER-CTAS-REQ-1` and
  `CUTOVER-DEDUP-SCHEMA-1`, the nullability half of `V3-COV-8`, converges
  `DYNFLATTEN-READNULL-1`. `risk_tier: standard`. Branch
  `fix/cutover-schema-1`.
  pins: cutover-schema-1/C-001, C-002, C-003, C-004, C-005, C-006
- [nullability-2-ledger.md](../completed/nullability-2-ledger.md) —
  **NULLABILITY-2 (2026-09-05), complete:** the analyzer's remaining nullability
  and cast residues, Spark-equal — generalized cast nullability, boolean→decimal,
  null-safe equal non-null, reader relax at every depth, tz-naive dtype mapping.
  Closes or narrows `CAST-NULL-1`, `CAST-BOOL-DEC-1`, `DEC-9` (remainder),
  `G6-4`, `G12-1`, `G12-2`, `CUTOVER-NULLDEPTH-1`, `READ-TSNTZ-DTYPE-1`.
  `risk_tier: elevated`. Branch `fix/nullability-2`.
  pins: nullability-2/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
- [perf-facade-cdf-1-ledger.md](perf-facade-cdf-1-ledger.md) —
  **PERF-FACADE-CDF-1 (2026-09-05), in flight:** PERF-ANALYSIS-1 candidate 2 —
  `createDataFrame(list of tuples)` stops normalizing every cell in Python five times and
  infers + converts column-wise, with nested columns delegated to the unchanged per-cell
  path. Target `create/100000/tuples_count` ≤ 100 ms. `risk_tier: standard`. Branch
  `perf/facade-cdf-1`.
  pins: perf-facade-cdf-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
