# task/wo/ — work orders, by grade
The orders an engine builds from. [TEMPLATE.md](TEMPLATE.md) defines the three grades (A self-directed, B guided,
C clerk) and the skeleton every order follows; the engine band table (which grade an engine runs) is owner-gated, outside the repo. Since 2026-10-01 orders are committed here, not in a `/tmp` directory.

The full rows as of 2026-11-20 are in [docs/history/task-wo/wo-map-2026-11-20.md](../../docs/history/task-wo/wo-map-2026-11-20.md).

- [TEMPLATE.md](TEMPLATE.md) — the three grades, the test of a grade-C order, the skeleton, the standing rules every order inherits.
- [T-1](t-1-spark-tests-it.md) — **grade C:** fold `repark-spark`'s `src/tests/` into one `tests/it/main.rs` binary; pure move.
- [C-0](c-0-postgres-harness.md) — **grade C:** the disposable Postgres container script and its `pg_live` fixture; no product code.
- [C-1](c-1-connect-skeleton.md) — **grade B:** the `repark-connect` skeleton, no provider; its settings stub and Postgres type map.
- [c-2-postgres-read.md](c-2-postgres-read.md) — **C-2, grade-B skeleton:** the Postgres read path; settings PENDING on C-1.
- [c-2-design.md](c-2-design.md) — **C-2 design sketch (Claude Opus 5.5, 2026-10-06):** North Star §7 answers; 14 records; 6 owner questions.
- [c-3-partitioned-reads.md](c-3-partitioned-reads.md) — **C-3, grade-B skeleton:** partitioned parallel reads; the acceptance factor PENDING.
- [c-4-writes.md](c-4-writes.md) — **C-4, grade-B skeleton:** Postgres writes via COPY FROM STDIN with row fallback; the flag surface PENDING.
- [c-5-sqlserver.md](c-5-sqlserver.md) — **C-5, grade-B skeleton:** SQL Server over TDS as a pip opt-in; packaging PENDING on the C-0 CC-5 measurement.
- [c-6-python-conveniences.md](c-6-python-conveniences.md) — **C-6, grade-B skeleton:** `read_database` / `write_database` and their CC-4 rows; PENDING on C-2…C-5.
- [CI-1](ci-1-wheel-smoke-time.md) — **grade C:** the wheel smoke check cut from 45–50 minutes to under 25, unweakened; no `crates/` edit.
- [CI-2](ci-2-slow-test-inputs.md) — **grade C (2026-10-03):** slow-test inputs once per xdist worker; **superseded 2026-10-19** ([../lessons.md](../lessons.md)).
- [GROWN-STACK-GATE-1](grown-stack-gate-1.md) — **grade B (owner, 2026-10-04):** 34 unconditional `deep_stack::block_on` sites take the frame doors' verdicts.
- [TA-CHAIN-1](ta-chain-1-leading-prefix.md) — **grade B (v1.5.2, 2026-10-03):** a leading NaN/NULL run on a `ta_*` input is skipped; 1.02x cap.
- [merge-mod-split-1/](merge-mod-split-1/) — **MERGE-MOD-SPLIT-1 (owner, 2026-10-12):** split `write/merge/mod.rs` at its planned boundary; pure move.
- [attr-view-semantics-1/](attr-view-semantics-1/) — **ATTR-VIEW-SEMANTICS-1, grade B (owner, 2026-10-05):** temp views and aliases mint attribute ids; a foreign column raises `MISSING_ATTRIBUTES`.
- [attr-id-1/](attr-id-1/) — the ATTR-ID-1 orders: design sketch, S0…S4, resumes, fixes.
- [microbatch/](microbatch/) — the micro-batch packet (docs only, 2026-10-05): decisions, MB-0 cells, sketch and slice orders.
- [ice-tsns-coercion-1/](ice-tsns-coercion-1/map.md) — **ICE-TSNS-COERCION-1 slice 0 (2026-10-12):** the twin and Spark-lattice measurement for R-017; no product code.
- [ICE-TSNS-FRAME-DOOR-1](ice-tsns-frame-door-1/map.md) — **(2026-10-13):** the frame door's cost evidence for nanosecond timestamps.
- [ice-ntz-nested-wall-1/](ice-ntz-nested-wall-1/map.md) — the design note for the nested TIMESTAMP_NTZ store kernel (parked 2026-10-12).
- [ice-v2-flat-aws-label-1/](ice-v2-flat-aws-label-1/map.md) — the scoped v2 label's live-cell records (G6 flat matrix, G5 CTAS failure).
- [ICE-TSNS-TYPE-PLANNER-1](ice-tsns-type-planner-1/map.md) — **(2026-10-13):** Fable 5.1 sketch for R-017 by SQL type name; docs only; closes #1027 and #1042.
- [ZONE-SEAM-1](zone-seam-1/map.md) — **grade B (owner adopted 2026-10-20):** one session-zone seam for zoned-timestamp conversions; root of R-007, R-015, R-011, ENC-1.
- [MAP-COLUMN-PANIC-1](map-column-panic-1/map.md) **(2026-10-20)** and [MAP-COLUMN-PANIC-NATIVE-DOOR-1](map-column-panic-native-door-1/map.md) **(2026-10-21)**: `map(<key>, <non-literal value>)` answers Spark on every door. [PANIC-FATAL-SUITE-1](panic-fatal-suite-1/map.md) **(2026-10-21, PR #1134, merged):** a caught panic fails any facade test.
- [REPLACE-LINEAR-PIN-1](replace-linear-pin-1/map.md) — **(2026-10-22):** the REPLACE-LINEAR-1 depth-40 pin warms at depth 1. [SPILL-REACH-FAIR-POOL-FLAKE-1](spill-reach-fair-pool-flake-1/map.md), **same day:** spill probes use an 80M pool; hangs carded as FAIR-POOL-HANG-1.
- [zone-oracle-1/](zone-oracle-1/map.md) — **ZONE-ORACLE-1, grade C (owner adopted 2026-10-20):** a recorded Spark oracle: about 1,500 timestamp and zone `SELECT` cells.
- [ICE-STRUCT-PAIRING-1](ice-struct-pairing-1/map.md) — **(U-6, R-016, 2026-10-13):** the sketch and grade A order for one struct pairing rule; docs only.
- [ice-tsns-floor-doors-1.md](ice-tsns-floor-doors-1.md) — **ICE-TSNS-FLOOR-DOORS-1 (U-3, R-009, grade B, 2026-10-13):** one floor on every door into microsecond TIMESTAMP and TIMESTAMP_NTZ columns.
- [WRITE-BODY-CARRIERS-1](write-body-carriers-1/map.md) — **step 0 (2026-10-14):** the CTAS-with-write-body measurements; measurement only.
- [ICE-SPARK-CROSS-READ-1](ice-spark-cross-read-1/map.md) — **(U-10, 2026-10-14):** Spark reads what RePark writes (tests only); AWS leg written, not run.
- [ICE-INTEGRITY-PINS-1](ice-integrity-pins-1/map.md) — **(U-9, 2026-10-14):** the Opus sketch of three integrity pins; docs only.
- [ice-replace-kill-doors-1/](ice-replace-kill-doors-1/map.md) — **ICE-REPLACE-KILL-DOORS-1, grade B (release 1.5.6, 2026-10-27):** kill-mid-write pins K-8, K-9; ruling RKD-1.
- [explain-dml-write-1/](explain-dml-write-1/map.md) — **EXPLAIN-DML-WRITE-1, grade B (2026-10-20, awaits the owner's adopt of the plan, P3):** `EXPLAIN ANALYZE` of DML and `PREPARE` of a write refuse on every door.
- [write-spine-1/](write-spine-1/map.md) — **WRITE-SPINE-1 records (card 2026-10-22):** the read-only CRATE-BOUNDARY-EVAL-1; options A–D, D recommended.
- [ICE-RESOURCE-BOUND-1](ice-resource-bound-1/map.md) — **(U-11), grade B (2026-10-20, awaits the owner's adopt of the plan):** resource pins PP-01, PP-02, PA-10, PA-11; opens after U-9.
- [ICE-V2-LABEL-1](ice-v2-label-1/map.md) — **(U-13), grade C (2026-10-26):** the format-v2 PRODUCTION label order and its Outcome block.
- [pa-dbt-mismatch-redact-1.md](pa-dbt-mismatch-redact-1.md), [pa-local-fs-policy-1.md](pa-local-fs-policy-1.md) — **U-5, grade B (2026-10-14), Muse xhigh. PA-DBT-MISMATCH-REDACT-1 (part 1, audit PA-01):** the dbt mismatch refusal never names a value or secret. **PA-LOCAL-FS-POLICY-1 (part 2, audits PA-02 to PA-04):** the local-file write policy below the plan root.
- [NS-ARRAY-FLOOR-1](ns-array-floor-1/map.md) — **(2026-10-14):** the pandas and Arrow doors' cells and refusal sites for R-020; no product code.
- [REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](repark-sql-update-subquery-where-1/map.md) — **(ruling 106, 2026-10-16):** routing sketch (docs only); seven owner questions.
- [ice-ntz-nested-refuse-1/](ice-ntz-nested-refuse-1/map.md) — **ICE-NTZ-NESTED-REFUSE-1 (U-8, R-011, ruling T-3, 2026-10-20):** outside UTC, every door refuses a not-provably-naive nested `TIMESTAMP_NTZ` value.
- [AWS-EPHEMERAL-RUNS-1](aws-ephemeral-runs-1/map.md) — **(2026-10-24):** step 0 census of table-creating and -reading sites in the aws-acceptance legs.
- [GATE-RETIRE-1](gate-retire-1/order.md) — **grade B (1.5.6), concluded 2026-10-25, arm retained (GR-9):** folded into WRITE-SPINE-1 step 4.
- [ns-truthup-1/](ns-truthup-1/map.md) — **NS-TRUTHUP-1, grade C (v3 core label plan, 2026-10-26):** nanosecond rows R-010, R-013, R-018 part 1, R-001; re-measured, re-dated.
- [ice-tsns-update-range-class-1/](ice-tsns-update-range-class-1/map.md) — **ICE-TSNS-UPDATE-RANGE-CLASS-1 (U-4, row R-012), grade C (2026-10-26):** `UPDATE` reports INSERT's refusal class and text; 16 red-first pins.
- [empty-dml-shared-guard-1/](empty-dml-shared-guard-1/map.md) — **EMPTY-DML-SHARED-GUARD-1, step 0.3 (2026-11-08):** probe tables on the Spark oracle; no product code.
- [sql-setop-all-multiset-1/](sql-setop-all-multiset-1/map.md) — **SQL-SETOP-ALL-MULTISET-1 (2026-11-23, owner ruling 2026-11-22):** the multiset domain table against Spark 4.1.2 on main, the mutant and head; candidate B and its cost; red/green. It retires SQL-SETOP-ALL-REFUSE-1.
- [native-ctas-iceberg-refuse-1/](native-ctas-iceberg-refuse-1/map.md), [sql-setop-all-refuse-1/](sql-setop-all-refuse-1/map.md), [native-dataframe-write-silent-1/](native-dataframe-write-silent-1/map.md) — **NATIVE-CTAS-ICEBERG-REFUSE-1 (2026-11-12):** native CTAS inventory; **SQL-SETOP-ALL-REFUSE-1 (2026-11-12 ruling):** `INTERSECT ALL` / `EXCEPT ALL` on both doors; **NATIVE-DATAFRAME-WRITE-SILENT-1 (2026-11-13):** writer step 0.
- [fexpr-window-null-order-1/](fexpr-window-null-order-1/map.md), [filter-string-null-order-1/](filter-string-null-order-1/map.md) — **FEXPR-WINDOW-NULL-ORDER-1 (S2, 2026-11-12)** and **FILTER-STRING-NULL-ORDER-1 (S2, 2026-11-13):** NULL placement in `F.expr` `ORDER BY` and string `filter`, against Spark 4.1.2.
- [update-schema-batch-mismatch-1/](update-schema-batch-mismatch-1/map.md), [zero-partition-merge-1/](zero-partition-merge-1/map.md) — **UPDATE-SCHEMA-BATCH-MISMATCH-1 (S3, 2026-11-13):** a copy-on-write `UPDATE` refused by batch order; **ZERO-PARTITION-MERGE-1 (S1, 2026-11-13):** `0.0` and `-0.0` partitions apart; [NAN-WRITE-CANONICAL-1](nan-write-canonical-1/map.md) — **(Q11 C):** stored NaN bits on write doors.
- [NAN-MINMAX-1](nan-minmax-1/map.md) — **(part 1, S1, 2026-11-13):** constant-file NaN reads vs Spark 4.1.2; [SIGNED-ZERO-CONST-COMPARE-1](signed-zero-const-compare-1/map.md) (2026-11-14): folded zeros, by bits.
- [decimal-double-union-coercion-1/](decimal-double-union-coercion-1/map.md) — **DECIMAL-DOUBLE-UNION-COERCION-1 sketch (2026-11-13):** DECIMAL with FLOAT/DOUBLE types; [nan-compare-spark-1/](nan-compare-spark-1/map.md) — **NAN-COMPARE-SPARK-1 sketch (2026-11-13):** NaN by bits; [double-decimal-store-round-1/](double-decimal-store-round-1/map.md) — **DOUBLE-DECIMAL-STORE-ROUND-1 (2026-11-13), the coercion's prerequisite:** stores fix.
- [round-double-exact-1/](round-double-exact-1/map.md) — **ROUND-DOUBLE-EXACT-1 (2026-11-14):** FLOAT and DOUBLE `round` and `bround` match Spark's text; native gap carded.
- [cast-literal-unwrap-1/](cast-literal-unwrap-1/map.md) — **CAST-LITERAL-UNWRAP-1 step 0 (S1, 2026-11-16).** [decimal-common-type-1/](decimal-common-type-1/map.md) — **DECIMAL-COMMON-TYPE-1 step 0, built (S1, 2026-11-16).** [avg-decimal-round-1/](avg-decimal-round-1/map.md) — **AVG-DECIMAL-ROUND-1 step 0 (S1, 2026-11-17)**, with DECIMAL-DIVIDE-LITERAL-1. [sum-dec-i128wrap-1/](sum-dec-i128wrap-1/map.md) — **SUM-DEC-I128WRAP-1 step 0 (2026-11-17).** [decimal-divide-literal-1/](decimal-divide-literal-1/map.md) — **DECIMAL-DIVIDE-LITERAL-1, built (S1, 2026-11-18).**
- [csv-schema-timestamp-zone-1/](csv-schema-timestamp-zone-1/map.md) — **CSV-SCHEMA-TIMESTAMP-ZONE-1, built (S1, 2026-11-19):** a CSV schema's `TIMESTAMP` parses under the session zone.
- [temp-view-session-settings-1/](temp-view-session-settings-1/map.md) — **TEMP-VIEW-SESSION-SETTINGS-1 step 0, revised (2026-11-17):** temp views under other session settings vs Spark 4.1.2.
- [avg-decimal-grouped-run-variance-1/](avg-decimal-grouped-run-variance-1/map.md) — **AVG-DECIMAL-GROUPED-RUN-VARIANCE-1 step 0 (S2, 2026-11-19):** grouped decimal `avg` vs Spark 4.1.2, 22 configurations.
- [temp-view-frame-reader-parse-1/](temp-view-frame-reader-parse-1/map.md) — **TEMP-VIEW-FRAME-READER-PARSE-1 step 0 (2026-11-19):** a JSON or CSV frame view's parse vs Spark 4.1.2. Unit 1 built 2026-11-19 ([built/](temp-view-frame-reader-parse-1/built/map.md)).
- [string-hour-minute-second-1/](string-hour-minute-second-1/map.md) — **STRING-HOUR-MINUTE-SECOND-1 step 0 (S1, 2026-11-19):** `hour`, `minute`, `second` over a `STRING` vs Spark 4.1.2, 75,032 cells.
- [map-ratchet-ceiling-1/](map-ratchet-ceiling-1/order.md) — **MAP-RATCHET-CEILING-1, grade A (H-A, 2026-11-19):** map size rows are ceilings.
- [csv-string-field-text-1/](csv-string-field-text-1/map.md) — **CSV-STRING-FIELD-TEXT-1 step 0 (S1, 2026-11-20):** a CSV `STRING` field's text vs Spark 4.1.2, 7,704 cells.
- [ice-held-frame-snapshot-1/](ice-held-frame-snapshot-1/map.md) — **ICE-HELD-FRAME-SNAPSHOT-1 step 0, revised (S1, 2026-11-20):** a held Iceberg frame after a same-session write vs Spark 4.1.2, 3,896 cells.
- [temp-view-cache-lazy-parse-1/](temp-view-cache-lazy-parse-1/map.md) — **TEMP-VIEW-CACHE-LAZY-PARSE-1 step 0 (S1, 2026-11-20):** a cache's and a UDF frame's parse vs Spark 4.1.2, 2,402 cells.
- [asof-join-1/](asof-join-1/map.md) — **ASOF-JOIN-1 step 0 (S3 proposed, 2026-11-20):** `DataFrame._joinAsOf` and `merge_asof` vs Spark 4.1.2, 3,523 main cells.

Up: [../map.md](../map.md).
