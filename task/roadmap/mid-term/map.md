# map — task/roadmap/mid-term/

## Purpose
Evaluated intakes awaiting an owner charter. A document here names a unit list and the measurements behind it; it leaves when the owner charters it (a brief under `briefs/`) or declines it (a dated ruling in the intake, then the archive).

## Contents
- [MAP-GOVERNANCE-1](map-governance-1-card-2026-10-29.md) — **closed (merged as PR #1221):** pin bindings to sidecars, the map size ratchet and the lifecycle sidecar. [Ledger](../../ledgers/completed/map-governance-1-ledger.md); leads C-045 and C-046 stay OPEN.
- [crate-layout-review-2026-09-30.md](crate-layout-review-2026-09-30.md) — **crate layout review (2026-09-30):** RePark's test placement (53 % of Rust lines are tests; 36 single-file `tests.rs` directories; 45 integration binaries) measured against Sail, Polars and DataFusion, the four options with their costs (one integration binary per crate, the `#[path]` rule question, family grouping, statement-and-answer tests as data), and the recommended order after v1.5.1. Leaves when the owner charters an option or declines the review.
- [read-path-obligations-2026-09-27.md](read-path-obligations-2026-09-27.md) — **WO READ-PATH-1 (2026-09-27):** the obligations inventory — all 211 binding sentences from AGENTS.md, CLAUDE.md and the three skills, each with its old home, new home and role trigger. Closes when the PR merges.
- [v1-5-0-remainder-spec-2026-09-23.md](v1-5-0-remainder-spec-2026-09-23.md) — **the v1.5.0 remainder, cell by cell:** every non-EQUAL gate cell on the 09-23 main grouped into units with Spark's recorded answer, the ruling it needs, the executor tier and a size; the input to the next run's lists.
- **run 27, 09-21 → 22:** [27-orchestrating-note](day-report-2026-09-22-27-orchestrating-note.md) (the orchestrating note): the scoreboard, the owner's rulings and what the next run inherits. The lanes' reports: [muse2](night-report-2026-09-21-27-muse2.md) views · [grok2](night-report-2026-09-21-27-grok2.md) IPI-51 · [grok47](night-report-2026-09-21-27-grok47.md) IPI-51 exception types, Grok 4.7 · [glmflash2](night-report-2026-09-21-27-glmflash2.md) IPI-26/27 parser, DDL · [opus2](night-report-2026-09-21-27-opus2.md) IPI-32, IPI-05 close-out · [muse3](night-report-2026-09-21-27-muse3.md) IPI-41 ORC/Avro, fork half · [muse4](night-report-2026-09-21-27-muse4.md) IPI-30/31 procedures · [muse5](night-report-2026-09-21-27-muse5.md) views · [muse6](night-report-2026-09-21-27-muse6.md) metadata columns · [muse7](night-report-2026-09-21-27-muse7.md) non-Iceberg functions · [muse8](night-report-2026-09-21-27-muse8.md) finish-time order · [muse9](day-report-2026-09-22-27-muse9.md) functions · [muse10](day-report-2026-09-22-27-muse10.md) WAP, SET LOCATION, RP-47 · [opus3](day-report-2026-09-22-27-opus3.md) IPI-40 views on Opus 5 high · [opus55](day-report-2026-09-22-27-opus55.md) READ and PROC residue on Opus 5.5 high · [opus56](day-report-2026-09-22-27-opus56.md) functions, successor of muse9 · [opus57](day-report-2026-09-22-27-opus57.md) RP-47 landed, successor of muse10.
- **run 27 (2026-09-20):** [27-orchestrating-note](day-report-2026-09-20-27-orchestrating-note.md) (orchestrating note): the tick-driven harness, the four-engine bake-off and the gate scoreboard 461 → 537 EQUAL. The bake-off's pre-registered [protocol](run27-eval-protocol.md), [blind grades](run27-blind-grades.md) and [agent-pattern ledger](run27-agent-lessons.md). [27-opus-closeout](day-report-2026-09-20-27-opus-closeout.md) (the Opus close-out lane): closed the seven units stopped on 2026-09-20. [27-grok](day-report-2026-09-20-27-grok.md) · [27-muse](day-report-2026-09-20-27-muse.md) · [27-glmflash](day-report-2026-09-20-27-glmflash.md) · [27-glm](day-report-2026-09-20-27-glm.md) (the four bake-off lanes' own reports): error conditions (Grok), system functions (Muse), map gate step 1 and the small parser unit (GLM 5.3 Flash), catalog and session commands (GLM 5.3, paused).
- [MAP-CURRENCY](map-currency-card-2026-09-20.md) — **card (2026-09-20):** maps checked like a build target — 310 maps, 3.06 MB, the lockstep rule unenforced in CI and firing per commit; step 1
  (work order MAP-PR-GATE-1, chartered) moves the rule to the pull request; steps 2–3 (generated navigation with per-row blob ids, `pins.toml`, a size gate, a clerk-written prose pass) wait for five owner rulings.
- **run 25, the night reports (2026-09-20):** [25a](night-report-2026-09-20-25a.md) (every bump, the perf remainder, the RePark halves): RP-39 and RP-40, the `count(*)` fold, the bench baseline switch, metadata-only DELETE, delete-manifest rewrite, writer metrics config on main; the AWS pair and the first gate scoreboard in the orchestrating note. [25b](night-report-2026-09-20-25b.md) (fork lane one): the wrong `replace` snapshot summary fixed, F-PAGE-PRUNE-2, branch-read schema merged; output-spec-id ready; WAP stage-only a draft. [25c](night-report-2026-09-20-25c.md) (parity, RePark side): `REPLACE COLUMNS`, four routed procedures and `spark.wap.branch` on main; #733 session write confs ready and unmerged. [25d](night-report-2026-09-20-25d.md) (fork lane two): rewrite sort and zorder, transform sort orders, `add_files` and location providers merged; a nested residual rename that silently lost rows fixed; container accessors a draft hand-over. [25e](night-report-2026-09-20-25e.md) (the plan packets): thirteen execution packets for the large parity units, 222 cells; four units found to need no fork work.
- [csv-header-default-1.md](csv-header-default-1.md) — **card CSV-HEADER-DEFAULT-1 (filed 2026-09-28 on the owner's ruling, option B):** `df.write.csv(path)` writes a header by default where Spark 4.1.2 does not; 9 of U12's 16 residues (`R-S3-CSV-HEADER`); RePark 1.5.x keeps today's default, the change waits for a minor release with a release note.
- [s3-path-write-1-5-0.md](s3-path-write-1-5-0.md) — **card S3-PATH-WRITE-1 (filed 2026-09-22 as a v1.5.1 card; owner
  2026-09-23: in the v1.5.0 target; owner 2026-09-27: back to v1.5.1):** plain Parquet, CSV and JSON path writes to `s3://` — reads reach S3, Iceberg
  tables on S3 write, but the path writer is a local staging-and-rename protocol; step 0 is a recorded Spark oracle
  (`W-PATH-S3-*` cells join the gate), then six design questions ruled Rust-first, one day lane, no fork work.
- [ice-streaming-1-6.md](ice-streaming-1-6.md) — **card ICE-STREAMING (2026-09-19, v1.6.0, owner ruling C-1;
  re-pointed to 1.7 on 2026-10-04):** structured streaming read and write of Iceberg tables leaves the v1.5.0
  parity gate (3 inventory cells, IPI-47); the design is ruled in the epic-term micro-batch plan, step 0 is slice
  MB-0.
- [ice-variant-1-6.md](ice-variant-1-6.md) — **card ICE-VARIANT (2026-09-27, v1.6.0, owner ruling C-4):** the `variant` type leaves the v1.5.0 gate (cell `TY-VARIANT-V3`, shredded Parquet in the Spark leg, fork issue R88 open) and is scheduled as integration of the upstream `parquet-variant*` crates the fork already links (schema mapping, unshred on scan, shred on write, CREATE on the three doors, four SQL functions, repin); step 0 is the recorded shapes dump, size 4–6 executor rounds.
- [ns-nested-1-6.md](ns-nested-1-6.md) — **card NS-NESTED (2026-09-27, v1.6.0, owner ruling C-2):** nested namespaces leave the v1.5.0 gate (cell `D-NS-NESTED`, registry row NS-2; Spark leg recorded, RePark refusing on the two-part name) and are scheduled with the recorded Spark child-listing row, eight shapes still to measure (table in `sc.a.b`, DROP plain and CASCADE, the non-empty refusal, all four again one level deeper), then five design questions to rule (session-catalog scope, four-part resolution, DDL arity, DROP semantics, per-catalog coverage); size M, 3–5 rounds, tier terra + opus. Closes readiness finding F-6.
- **run 24:** [24a](day-report-2026-09-19-24a.md) (read-performance unit 0): the bench bed, page pruning and the catalog and footer caches on main. [24b](day-report-2026-09-19-24b.md) (the read-performance fork lane): fork #310, #311, #312, #316, #317 merged; a pre-existing silent wrong answer on `_pos` / `_row_id` projections fixed. [24c](day-report-2026-09-19-24c.md) (parity, RePark side): time travel, `DROP NAMESPACE`, overwrite partition sets and string-to-timestamp casts answer Spark on main; RP-34 and RP-35. [24d](day-report-2026-09-19-24d.md) (parity, fork side): fork #308, #309, #313, #315, #318, #320 merged; a strict-metrics decimal-scale defect found and fixed.
- **run 23, the night reports (2026-09-19):** [23a](night-report-2026-09-19-23a.md) (the fork residue lane): nine fork PRs merged; #303 held for the owner. [23b](night-report-2026-09-19-23b.md) (the RePark side and every pin bump): ten PRs merged, RP-29 → RP-33.
- [23d](night-report-2026-09-19-23d.md) — **the silent top of the slate:** ICE-TT-RESOLVE-1 (IPI-01 + IPI-02) a draft at r3; Spark truths re-recorded for namespaces (26 cells) and overwrite modes (60 cells); briefs ready for ICE-DROP-NS-1 and ICE-OVERWRITE-MODE-1.
- [23c](night-report-2026-09-19-23c.md) and its [inventory](ice-parity-inventory-2026-09-19.md) — **the Spark–Iceberg parity inventory (2026-09-19, owner direction "1 to 1 parity … NO STONES LEFT"):** 842 cells enumerated from the iceberg-spark-runtime 1.11.0 jar and the 1.11 docs, measured on Spark 4.1.2 and RePark main `6a140eb3` — 424 EQUAL, 57 DIFFERENT, 126 unregistered refusals, 28 not parsed, 87 registered refusals, 120 Spark-cannot; cross-engine interop 13 of 13 shapes both ways; filter pushdown 0 silent of 369 predicate evaluations; a ranked slate of 56 units led by reader `versionAsOf` / `timestampAsOf` being ignored, `TIMESTAMP AS OF <expression>`, the static-mode partition overwrite (contradicts DML-1), WAP writes landing on main, and a partition column name that makes the table unreadable.
- **run 22:** [22a](day-report-2026-09-18-22a.md) (the fork lane and the pin bumps): fork #294 to #296 merged; RP-26 to RP-28 on main. [22b](day-report-2026-09-18-22b.md) (the RePark pull requests and the registry): eight PRs merged, including #676 and #701.
- [v3-multiarg-1.md](v3-multiarg-1.md) — **card V3-MULTIARG-1 (2026-09-18, post-1.x,
  owner ruling 2026-09-18):** read, then write, multi-argument partition transforms
  (`source-ids`); NOT in 1.x (declared in registry V3-MULTIARG-1, rating row V3-05).
  FIRST STEP is a Java-API fixture (Iceberg 1.11.0 `UpdatePartitionSpec` /
  `PartitionSpec.builderFor`, measured first), then the fork asks (spec parsing in
  `iceberg-rust`, transform evaluation, pruning). No code in 1.x.
- **run 21:** [21a](overnight-report-2026-09-18-21a.md) (2026-09-17 → 18, Iceberg remediation night 3): #685, #687, #682, #692 (MERGE and DELETE; Spark itself commits 1 of 8 on the headline probe), fork #292 and #690; open #695, fork #294; every INSERT into an Iceberg ARRAY column fails loud. [21b](overnight-report-2026-09-18-21b.md): #678 ICE-V3-WRITE-DEFAULT-1 merged; fork #293 closes the run-20 residues; #676 parked; RP-26 not opened. [21c](overnight-report-2026-09-18-21c.md): #691 ICE-SORTED-INSERT-1 remediation merged (V2-12); #686 re-measured on RP-23 (0 XPASS); #693, #689 and draft #694 open.
- [ice-read-perf-slate-2026-09-18.md](ice-read-perf-slate-2026-09-18.md) — **Iceberg read performance on S3 Tables and Glue, slate for v1.5.0 (2026-09-18, owner-instructed):** page pruning off on the DataFusion path, no shared catalog cache, an unfed footer hook; wave 1 is ICE-READ-PERF-0 plus three cache and prune cards and a re-measure gate; nothing measured yet. Owner rulings 2026-09-18: v1.5.0 waits for wave 1; AWS-managed compaction is the S3 Tables default; the bench spend is capped at $25 with a 3 GB table-size flag.
- **runs 12b to 15c:** [run12b](overnight-report-2026-09-13-run12b.md) (2026-09-13, cache and expressions, beside run 12): EAGER-BUDGET-1 merged (#569: `repark.cache.max_total_bytes` refuses, never evicts; distinct-buffer `repark.cache.retained_bytes`; incremental admission; the per-call slowdown did not reproduce under 2/4/8 GB caps); REPLACE-LINEAR-1 step 0 merged (#571), step 1 parked on Q-R1 (nine Spark divergences); cards ARRAY-NULL-1 and ANSI-DOOR-1; owner questions Q-B1, Q-B2, Q-R1, Q-R2 with recommendations. [run13b](overnight-report-2026-09-14-run13b.md) (2026-09-14, expressions, beside run 13; night window 05:19–06:30, day continuation to 12:00): REPLACE-LINEAR-1 step 1 (#577): a flat searched CASE with Spark semantics per Q-R1, 277 MB → 4.5 MiB at 16 entries on a release native; Grok critic-logic 1 P1 + 5 P2 and S2-21 Python reviewer 2 P2, all remediated. Then ARRAY-NULL-1 and ANSI-DOOR-1, with owner questions and recommendations. [run14](overnight-report-2026-09-14-run14.md): the Iceberg production-cutover slate; fork and RePark units #587 to #590 landed, the aws-acceptance run green, owner questions Q-R14-1..6. [run14b](overnight-report-2026-09-14-run14b.md) (2026-09-14, day run beside run 14): FACADE-4 step 1 merged (#582) and ARRAY-NULL-1 (#581); rulings R14b-D-1..13, incidents, owner questions Q-R14b-1..6 with recommendations. [15b](overnight-report-2026-09-15-15b.md) (2026-09-15, the 1.5 facade surfaces): Row, types, Catalog, SparkSession and DataFrame surfaces A and B merged; DF-PLAN-INTROSPECT-1 and IO-TEXT-1 end as branches. [15c](overnight-report-2026-09-15-15c.md) (2026-09-15, the 1.5 Spark-parity SQL door): SQL-SET-DOOR-1 #601 and FNP-6D #609 merged; DOOR-CONVERGE-1 #616, JAVA-DOUBLE-STR-1 #612 and FNP-4B #611 left as PRs for the owner.
- [ice-cutover-slate-2026-09-14.md](ice-cutover-slate-2026-09-14.md) — **Iceberg production-cutover slate (2026-09-14, run 14, from the production assessment's G-1..G-6 and rulings Q-ICE-1..7):** F-GLUE-REPLACE-1 (fork Glue replace publish), RP-20 + ICE-GOLD-TWICE-1 (repin, CREATE OR REPLACE twice in the nightly, gold dbt twice in aws-acceptance), ICE-SPARK-TABLE-1 (RePark MERGE and maintenance on a Spark-created table, Spark reads back), ICE-COMMIT-UNKNOWN-1 (own exception class), ICE-TRINO-READ-1 (parked behind 1.8 Trino) and the pipeline-side SHADOW-1 change.
- [f-append-window-failloud-1-2026-10-05.md](f-append-window-failloud-1-2026-10-05.md) — **fork card (2026-10-05):** an opt-in fail-loud mode on `IncrementalAppendScan` when the window holds a non-append snapshot, with skip opt-ins mirroring Spark's `streaming-skip-overwrite-snapshots` / `-delete-snapshots`; default unchanged; consumer MB-1 (O-5: Bronze refuses deletes).
- [f-commit-offset-property-1-2026-10-05.md](f-commit-offset-property-1-2026-10-05.md) — **fork card (2026-10-05):** the summary half of the sink's offsets already exists at the pin (both MERGE arms pass summary extras). The request is a red-first proof that a table-property update in the same `Transaction` as a row delta or overwrite survives the commit-retry and rebase path, plus a measurement of the same-key race. A fork fix lands only if the proof fails. Consumers: MB-2a and MB-2c.
- [F-APPEND-PIN-BASE-1](f-append-pin-base-1-2026-10-07.md) — **fork card (2026-10-07):** the MB-2c append-fence pins; the RePark-side append fence is in (`feat/mb-2c-append-fence`), so this ask is retired.
- [f-partspec-redundant-1-2026-09-26.md](f-partspec-redundant-1-2026-09-26.md) — **card
  F-PARTSPEC-REDUNDANT-1 (2026-09-26, filed by WO PARTNAME-1):** the fork's partition-spec
  update path allows distinct transforms on one source and refuses only exact duplicates
  like Java's `BaseUpdatePartitionSpec`, with Java's texts; the CREATE-door redundant-time
  and bucket+bucket texts are the second item. Closes PARTNAME-1 residue R-1 on its repin.
- [tz-offset-seconds-1-card-2026-09-16.md](tz-offset-seconds-1-card-2026-09-16.md) — **card TZ-OFFSET-SECONDS-1
  (2026-09-16, 1.6, ruling Q-17c-1):** sub-minute fixed session offsets (`+05:30:30`) carried as seconds east of UTC
  in a typed zone instead of an Arrow `Tz` string; closes the dated declaration SET-ANSI-RUNTIME-4. No 1.5 code.
- [plan-depth-1-card-2026-09-30.md](plan-depth-1-card-2026-09-30.md) — **card PLAN-DEPTH-1
  (2026-09-30, mid-term, ruling OD-3):** `count()` over 200 or more chained `DataFrame.filter`
  calls segfaults (stack overflow); verifier finding RC5-5, S3, pre-existing. Covered by PR #892
  (DEEP-FILTER-CHAIN-CRASH-1); closes when #892 merges, after re-running the repro with string
  and Column filters under both `caseSensitive` settings on main.
- [sql-lambda-scope-1-card-2026-09-30.md](sql-lambda-scope-1-card-2026-09-30.md) — **card SQL-LAMBDA-SCOPE-1
  (2026-09-30, mid-term, ruling OD-3):** the `spark.sql` door scopes higher-order-function
  lambdas to the wrong rows (`exists` predicates answer `[2]`/`[1]`/`[1,5]`/`[5]` where Spark
  answers `[1,2]`/`[1,5]`/`[1,2,5]`/`[1,5]`); verifier finding RC5-6, S3, pre-existing, base and
  head identical. The `DataFrame.filter` forms already answer Spark.
- [sort-parent-column-1-card-2026-09-30.md](sort-parent-column-1-card-2026-09-30.md) — **card SORT-PARENT-COLUMN-1
  (2026-09-30, mid-term, ruling OD-3):** `orderBy(parent Column)` on a case-twin frame sorts by
  the output twin instead of the hidden parent attribute under `caseSensitive=true`;
  verifier finding RC5-7, S3, pre-existing, base and head identical. Home is the R-CS2-18 family.
- [unicode-case-version-1-card-2026-09-30.md](unicode-case-version-1-card-2026-09-30.md) — **card UNICODE-CASE-VERSION-1
  (2026-09-30, mid-term, ruling OD-3):** case folding uses Rust's Unicode 16 tables while JDK 17
  Spark uses Unicode 13, so Unicode 14–16 case pairs fold in RePark but stay distinct in Spark;
  verifier finding RC3-5 / residue R-CS2-17, S3, 34 cells. The boundary moves with the deployment JDK.
- [bl11-encoder-perf-1-card-2026-09-16.md](bl11-encoder-perf-1-card-2026-09-16.md) — **card BL11-ENCODER-PERF-1
  (2026-09-16, P3 perf, ruling Q-17c-5):** the numeric → BINARY encoder builds fixed-width output in bulk (arithmetic
  offsets, cloned validity) instead of a per-row builder; starts from 17c's measured 26.81 ms vs 18.35 ms (~1.5×).
- [DECIMAL-CACHE-1](decimal-cache-1-card-2026-09-15.md) — **card (2026-09-15,
  owner report from a Windows 1.4.1 notebook, reproduced on the published 1.4.1 wheel):** decimal arithmetic
  whose Spark result type needs the precision-overflow adjustment refuses `.eager()` / `.cache()` / `.persist()`
  with `Mismatch between schema and batches` while `collect()` succeeds; the physical batches do not carry the
  logical decimal field. Oracle `/tmp/oc-worker/qd-decimal/oracle-decimal-cells.json`. Not started.**
- [DDL-DEPTH-1](ddl-depth-1-card-2026-09-14.md) — **card (2026-09-14, run 14b under
  owner ruling Q-R13-13, card only):** a nesting-depth cap on the DDL type parser at `SPARK_TYPE_NAME_MAX_DEPTH` with a
  typed refusal, measured on the Spark oracle first; no new refusal ships until the card runs.
- [REPLACE-NOVALUE-1](replace-novalue-1-card-2026-09-14.md) — **card (2026-09-14,
  run 13b under owner ruling Q-13b-2, card only):** `DataFrame.replace(to_replace)` with no `value` raises PySpark's
  `ARGUMENT_REQUIRED` through a private sentinel default; today repark reads it as a NULL replacement.
- [ARRAY-NULL-1](array-null-1-card-2026-09-13.md) — **card (2026-09-13, run 12b
  under G-5; ABS-EXPR-1's C-004 audit residue):** `F.array_append` / `F.array_prepend` compose
  `when(isnull(arr), NULL).otherwise(flatten(array(arr, array(x))))`, embedding the array twice per level
  (19 MB at depth 12, 66 MB at 14), because DataFusion's kernels drop the input null buffer. D-1 a
  null-preserving native arm measured first between a wrapping `ScalarUDF` and a Rust-built single-reference CASE
  (HALT if neither is linear); D-2 Spark oracle cells (NULL array, NULL element, empty, nested, coercion); D-3 the
  SQL door's divergence row; step 0 measures and lands the red-first depth-40 memory pin. Not started.
- [ANSI-DOOR-1](ansi-door-1-card-2026-09-13.md) — **card (2026-09-13, run 12b,
  card only):** the SQL door's datafusion-spark kernels read `execution.enable_ansi_mode`, which repark never sets,
  so `spark.sql("SELECT abs(x)")` wraps on signed minimums while the facade raises `ARITHMETIC_OVERFLOW` as Spark
  does with ANSI on. D-1 wire `spark.sql.ansi.enabled` (session build and runtime set) to that option; D-2 measure
  every ANSI-aware door kernel against the oracle first; D-3 the `abs` row leaves `EXPECTED_DIVERGENCES`. Not started.
- [EAGER-BUDGET-1](eager-budget-1-card-2026-09-13.md) — **card (2026-09-13, run 11 §7; merged in run 12b as #569):** a session cache budget and retained-bytes accounting — `repark.cache.max_total_bytes` refuses (never evicts, Q-E2) when a materialization would push the sum of distinct Arrow buffers across live `__repark_cache_*` MemTables past the budget (D-1/D-2), admission checked incrementally per batch before the next pull (D-3), `repark.cache.max_bytes` unchanged in meaning (D-4), no spill (D-5), no registration or handle after a refusal or collection failure (D-6); step 0 measures the 30-iteration loop under 2/4/8 GiB cgroup caps, base vs main.
- [REPLACE-LINEAR-1](replace-linear-1-card-2026-09-13.md) — **card (2026-09-13, run 12b under G-5; ABS-EXPR-1's C-004 audit residue; steps merged as #571 and #577):** `DataFrame.replace`'s dict loop nests the running expression once per mapping entry — `when(expression == lit(old), lit(new)).otherwise(expression)` embeds the previous expression twice, so N entries reference the column 2^N times (28 MB at 12, 297 MB at 16). D-1 builds ONE searched `CASE WHEN col = a THEN b … ELSE col END` per target column (linear in entries); D-2 pins the live PySpark 4.1.2 oracle cells first (NULL key/value, subset shapes, a missing column, type coercion, list vs scalar `value`, the `{1: 2, 2: 3}` mapping-order case); D-3 rules that if the oracle answers 2 the CASE follows Spark; D-4 keeps `core.py`'s ceiling and puts any helper in a sibling module. Step 0 measures the oracle cells and lands the red-first 40-entry memory pin; step 1 rewrites.
- [EAGER-OWN-1](eager-own-1-card-2026-09-13.md) — **card (2026-09-13, owner charter; run 11; merged as #565):** eager results own their materialization — a refcounted handle held by every frame that scans a cache view (D-2), eager-on-eager reuses the backing (D-4), no plan-equivalence caching (D-5), budget deferred to EAGER-BUDGET-1 (D-6); step 0 measures the million-row TA loop first; owner questions Q-E1 (cache()/persist() lifetime) and Q-E2 (session budget policy).
- [EAGER-OWN-1 review](../../../docs/history/eager-own-1/eager-materialization-retention-review-2026-09-13.md) — **the owner's source review (2026-09-13, verbatim):** bare `eager()` calls each register an unowned `__repark_cache_*` MemTable, the findings table, usage guidance, the three proposed corrections and the validation table the unit's pins follow. Archived to [docs/history/eager-own-1/](../../../docs/history/eager-own-1/map.md) on 2026-09-13 at the unit's close.
- [dyncfg-1-cards-2026-09-12.md](dyncfg-1-cards-2026-09-12.md) — **the DYNCFG-1 intake cut
  (2026-09-12, Devin reading round at `0233d96f`):** the sweep verdict — the entry criterion is
  met (three re-measure-confirmed read-profile wins, ten regression-only knobs, five no-effect,
  per-knob table traced to the committed CSVs) — rulings DC-1…9 the numbers decide (matrix
  membership, exclusions, the derivation rule, the measured per-cell budget), owner questions
  DQ-1…7 (write-back target, key names, writes, the sample, regression knobs, live-session
  semantics, overrun behaviour), and five cards: DYNCFG-1-BED (the reproducible, budgeted
  harness first), then TOML / RUNNER / WRITEBACK / DOCS behind the DQ rulings.
- [rest-catalogs-intake-2026-09-12.md](rest-catalogs-intake-2026-09-12.md) — **generic Iceberg
  REST catalog support (Lakekeeper, Apache Polaris), scoped 2026-09-12 by an Opus reading agent at
  fork pin `9e3522e3`:** the owned fork already ships a complete `iceberg-catalog-rest` (OAuth2 client
  credentials with proactive refresh, bearer tokens, `header.*`, `/v1/config` prefix routing,
  pagination, views, the commit-outcome taxonomy MERGE and compaction rely on, vended-credential
  overlay by longest prefix) that RePark consumes nowhere; the thirteen ranked gaps by owning layer
  (no `rest` catalog kind; one storage factory per catalog chosen from a scheme a REST warehouse
  never shows; vended-credential lifetime — the fork's R160; namespace property updates; views;
  `header.*` in `repark.toml`; SigV4), the zero-dependency-delta measurement, the CI-only docker
  strategy, the slate REST-0…4 plus four fork cards, and decisions REST-D-1…8 for the owner.
- [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) — **the 2026-09-08 owner slate,
  cut for mechanical-tier workers:** the owner rulings (5+5 polars edges, lazy `repr` shows the
  schema — R-22 of 2026-09-10 superseding R-2's data render, card DISPLAY-LAZY-1 — `repark.toml` pulled ahead of 1.2/1.3, the Ballista audit joins the Rust migration pilot,
  fixtures pin `spark` style, the object stays a RePark DataFrame) and seven work cards with
  pre-made decisions, one step per worker round, tier per step, red-first pin names, gates and
  hand-back conditions: SQL-DESCRIBE-1, DF-EXPLAIN-1, DISPLAY-POLARS-1, CFG-1, DF-EAGER-1,
  BALLISTA-AUDIT-0, PROFILES-1; plus the ADAPT-PART and DYNCFG-1 epic intakes and the sequence.
- [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) — **slate 2 (owner-chartered 2026-09-09):** roadmap 2.1 maintenance policy, 1.2 torture-test dataset suite and 1.3 spill-coverage matrix, four cards in slate 1's format; the run-8 to S2-29 rulings are dated in the slate itself.
- [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) — **how a Standing instruction added 2026-09-14: Rust first — new kernels, functions and planner rules land in Rust, Python stays a thin facade. Standing instructions of 2026-09-15 (evening): the Devin/Muse actor mix, the 200 G second-clone line and the shared-target lock, the queue-file merge rule, the alternates rule, and the evening rulings Q-16a/16b/16c.
  cheaper orchestrating session runs the slate unattended:** the four owner grants, the five
  reads, lane/brief/launch commands per launcher, the hand-back audit checklist, the gated
  push/PR/merge chain, bounded decision authority, one night's order, stop conditions, the
  morning report, and the launch command the owner runs. Standing instructions added 2026-09-16 (morning): the Rust-first decision sentence, whole-suite gates, the commit as a numbered step, the box rules and the run-17 rulings.
- [2026-09-09](overnight-report-2026-09-09.md) and [night2](overnight-report-2026-09-09-night2.md) — **the first two unattended runs:** 2026-09-08/09, BALLISTA-AUDIT-0 #426 and DF-EXPLAIN-1 #427 merged, SQL-DESCRIBE-1 parked on D-3 (#428 draft); 2026-09-09, LEDGER-READING-1 #432, SQL-DESCRIBE-1 #428 and PREFLIGHT-PARITY-1 #433 merged; DISPLAY-POLARS-1 step 2 parked (#434 draft).
- **runs 6 to 13:** [run6](overnight-report-2026-09-10-run6.md) (2026-09-10 15:06 → 2026-09-11, the review-fix run, one orchestrator across two processes): review-fix-slate §1 rows 1–11 plus M-0 and O-1, each size-baseline question answered by a comment-funded ratchet down, never a raise; the evening grant moved M-tier rounds to Devin SWE-2. [run7](overnight-report-2026-09-11-run7.md): REVIEW-FIX-15b, TORTURE-1 steps 3–5, AP-1 step 2 and BALLISTA-M2-A steps 1–2 merged (#496–#501, #503); NEVEROOM-1 step 2 merged. [run8](overnight-report-2026-09-11-run8.md) and [run9](overnight-report-2026-09-12-run9.md) (2026-09-11, 2026-09-12). [run9b](overnight-report-2026-09-12-run9b.md) (2026-09-12/13, orchestrator B beside run 9, Devin actors + Grok S2-21 reviewers): CFG-2 named sources in two steps (#551 Rust seam, #556 Python door; `[<profile>.database]` loads, lists, refuses on use naming roadmap 1.10) and the DYNCFG-1 intake (#547: five measure-first cards, DQ-1..DQ-7 parked for the owner). [run10](overnight-report-2026-09-13-run10.md) (2026-09-13, Grok actor + Grok critic-logic): COMMENT-CORE-1 (#561) removes 347 non-pragma comments from `dataframe/core.py`, no code change (AST-identical, 4468 → 4117 lines, facade 5985/369 before and after); 312 reasons moved to the dataframe map, 35 narration lines deleted; the critic’s 11 findings (2 lost rationale, 9 distorted paraphrases) fixed before the PR. [run11](overnight-report-2026-09-13-run11.md) (2026-09-13, Devin actor + Grok critic-logic + Grok S2-21 Python reviewer): EAGER-OWN-1 (#565), a refcounted handle owns every `__repark_cache_*` view (10 → 0 registrations, peak RSS 3,041 → 1,257 MB on the million-row TA loop; the slowdown did not reproduce on 125 GiB), Q-E1/Q-E2 recommendations, and card EAGER-BUDGET-1 seeded with the numbers. [run12](overnight-report-2026-09-13-run12.md) (2026-09-13, Devin actor + Grok S2-21 Rust/Python reviewers + Grok critic-logic, beside run 12b): FACADE-3 step 3 (Row/dict lists into native, abi3 temporal route; rows −54 %, dicts −55 %, tuples −47 % at 1e5; three P1 subclass/cache bugs fixed before the PR), the FACADE-4/5 briefs not opened, the remaining switchover measured in lines, owner questions Q-R12-1..3. [run13](overnight-report-2026-09-14-run13.md) (2026-09-14, Devin actor, beside run 13b): FACADE-4 step 0 (conversion baseline: no wall, so step 1 is a correctness consolidation; three-table census D1–D21; DDL/Arrow goldens), PR left for the per-PR critic-logic round; FACADE-5 not opened, its step-0 brief written; owner questions Q-R13-1..11.
- **runs 3 to 5:** [run3](overnight-report-2026-09-09-run3.md) (2026-09-09, 08:56–15:00 local): five merged (DISPLAY-POLARS-1 steps 2 and 3, DOCS-LINKS-1, the PREFLIGHT-PARITY-1 close, CFG-1 seed + step 1), two open at the stop (PROFILES-1 step 0, DF-EAGER-1 step 1), none parked. Eight §6 decisions, three questions for the owner. [run4](overnight-report-2026-09-09-run4.md): seven merged (CFG-1 steps 1b to 4; DISPLAY-POLARS-1 steps 4 and 5; DF-EAGER-1 steps 2 and 3); DISPLAY-BRIDGE-1 #454 parked on a merge conflict with DF-EAGER-1; CFG-1's ledger waits on one clause. [run5](overnight-report-2026-09-10-run5.md) (2026-09-10, orchestrator A): seven units merged, one parked green as a draft (NEVEROOM-1 step 1); AP-0's O-run refuted P-3's byte model; a live-Spark run narrowed TORTURE-1's `DATE-INTERVAL-NSBOUND-1` to a day width; runbook corrections.
- [iceberg-rust-handoff-2026-08-23.md](iceberg-rust-handoff-2026-08-23.md) — **the fork-side
  handoff (2026-08-23; F-3 / V3-DANGLE-1 errata 2026-08-31, V3-5; RP-5 consumed F-6b/F-6c / F-8 / F-16r / F-0 follow-up):** the document handed to the owned `iceberg-rust` fork's orchestrator —
  every fork-side item the 2026-08-23 intake surfaced (position-delete rewrite admission gate,
  expire-report split, dangling-delete removal in rewrite, `RewriteManifests` result counts,
  `ReplacePartitions` remainder, branch commit target, metadata-table projection, S3 Tables
  `register_table`, declared sort order → output ordering, and the format-v3 spine (F-7,
  plus F-13–F-15 added 2026-08-23 with the v1.0 north-star ruling): the deletion-vector write
  path, the Hadoop metadata-pointer math, the v3 type system), each with the engine-side
  evidence, the consumed surfaces a change must not break, and the engine pin that flips when
  it lands (F-15 excepted — it carries neither observation nor pin until V3-6 charters).
  **F-16 added 2026-08-24 from MW-7:** the delete-RATIO candidate clause in `RewriteDataFiles`
  (`tooHighDeleteRatio`, Java `DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`), deferred in the fork, so
  a correctly sized 100 %-dead data file is never compacted and its dead rows are retained
  without bound. Registry `RDF-1`. **Residue 2 re-homed 2026-09-02 (RDF-1):** the
  bounds-absent half was RePark's own truncated `file_path` statistics and is fixed here; the
  pin flipped to
  `test_mw7_scale_smoke.py::test_delete_laden_in_band_file_is_rewritten_and_its_delete_file_dies`.
  What still waits on the fork is one delete file naming two or more data files.
  **F-17 added 2026-08-28 from RP-2:** DML that supersedes one blob in a shared Puffin must
  carry every still-live sibling blob. The measured engine fixture loses the untouched sibling
  delete; the fork reuses its maintenance sibling-closure primitive and proves Java read-back.
  Landed fork #237 the same day; RP-3 (2026-08-30) wired the call from the engine's own MOR path.
  **F-5 corrected 2026-08-29:** answered in fork #217 (inside the engine pin) — the static half
  of the ask was void; DML-B is not blocked.
- [ROADMAP-INTAKE](roadmap-intake-2026-08-21.md) — **the roadmap intake (2026-08-21):** campaign briefs reduced to eleven open workstreams, one closed ledger and five items to verify. It carries the **MW campaign** (Iceberg merge-on-read operability) and the MW-0 measurements, including an undeclared `rewrite_data_files` result-schema divergence.
- [roadmap-intake-2026-08-23.md](roadmap-intake-2026-08-23.md) — **roadmap intake
  (2026-08-23), two tracks** (Track B's MW-4b row delivered as MW-10 — measured allow 2026-08-30). Track A: the six DuckDB window-operator optimizations evaluated
  against the pinned DataFusion 54.1.0 sources — two already in DataFusion, two upstream
  operator work this engine should not own, two real gaps (non-retractable aggregates over
  sliding frames; sort elision via Iceberg ordering provenance) — proposed as a measure-first
  W-0 battery plus W-1…W-3. Track B: Iceberg merge-on-read readiness at format v2 — the
  verdict (correctness production-grade, operability wired, evidence missing), the ranked gaps,
  and the post-#218 units — the Glue dispatch, MW-4b (S3 Tables leg, owner-gated), MW-5…MW-9,
  DML-A/B/C. Track C points at the fork handoff.

- [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md) — **the REVIEW-1 critic sweep (2026-09-10):** twenty-four Grok critic rounds, 51 numbered findings (39 CONFIRMED), fifteen fix cards (REVIEW-FIX-1…15, none opened) and eight owner questions. Read it before fix work on the units it names; D-1 and D-2 coverage is complete, and the card closes when this document reaches `main`.
- [review-fix-slate-2026-09-10.md](review-fix-slate-2026-09-10.md) — **the REVIEW-1 disposition
  (2026-09-10):** ten rulings RF-1..RF-10 on the sweep's eight owner questions and Milestone 1's
  codec wall (`datafusion-proto` is taken), the fifteen fix cards ordered security-first behind
  DISPLAY-LAZY-1, the orchestrator steps (ledger departure, ci.yml dual-wire, the M2 seed) and
  card BALLISTA-M2-A.
- [run5b](overnight-report-2026-09-10-b.md) — **2026-09-10, the day's second orchestrating session:** the Grok lane (Ballista Milestone 1 A to D and the REVIEW-1 sweep) and FACADE-AUDIT-0 on Muse; six units merged; the G-2 decisions and runbook process notes.
- [BARE-READ-DEFAULT-NAMESPACE-1](bare-read-default-namespace-1-card-2026-11-02.md) — **card (2026-11-02, grade C, P3, open):** a bare read the facade cannot qualify resolves against `datafusion.public`, not the current namespace; read errors only.
- [DROP-QUALIFIED-QUOTED-TABLE-1](drop-qualified-quoted-table-1-card-2026-11-02.md) — **card (2026-11-02, folded into IDENT-DIALECT-1 on 2026-11-02):** class 1, data-deleting.
- [CATALOG-API-GAPS-1](catalog-api-gaps-1-card-2026-11-02.md) — **card (2026-11-02, grade C, P3, open):** catalog-API answers that differ from Spark 4.1.2 at `d35a792b`; none changes a table.
- [ALTER-VIEW-DIGIT-NAME-1](alter-view-digit-name-1-card-2026-11-02.md) — **card (2026-11-02, grade C, P3, open):** `ALTER VIEW 1t` refuses a rename Spark performs and answers another class for a property change; held.

## Pointers
- Up: [../map.md](../map.md)
- **runs 15a to 18c, the 1.5 Spark-parity campaign:** [run15a](overnight-report-2026-09-15-15a.md) (THE FUNCTIONS): census slice before/after, per-PR table with reviewer verdicts and costs, rulings, owner questions. [run16a](overnight-report-2026-09-15-16a.md) (the functions slice) of the 2026-09-15 overnight: census 51 → 34, PRs #606, #623, #613 merged and #618 gated, four step-1 drafts, 30 rulings, the oracle recordings and the Rust-first roll-call. [16b](overnight-report-2026-09-15-16b.md) (day report, the facade surfaces and IO): census slice 17 → 7 missing names, #610 / #621 / #626 / #630 merged, rulings R-16b-1..44, owner questions Q-16b-1..4, the Rust-first roll-call, and the carry-overs DF-RUST-3, DF-SUBQUERY-1, IO-ORC-1, DF-METADATA-COL-1 with their oracles. [run16c](overnight-report-2026-09-15-16c.md) (day report, 2026-09-15): the SQL door, the type table and the registry backlog — census slice, per-PR table with reviewer verdicts and costs, oracle batches 12–19, rulings R-16c-1…17, owner questions Q-16c-1/2, hand-offs, the Rust-first roll-call. [run17a](overnight-report-2026-09-16-17a.md) (overnight report): the functions slice, census 37 → 22 missing names; #618 and #627 merged, #629 handed over. [run17b](overnight-report-2026-09-16-17b.md) (overnight report, 2026-09-15/16): the facade surfaces, IO and type widths (DataFrame census 108 → 114 of 115), rulings R-17b-1…24 and owner questions Q-17b-1…5; its lesson: a review finding can be real while its diagnosis is wrong. The live oracle showed freqItems key equality is primitive IEEE `==`, not the `doubleToLongBits` reasoning the critic filed. [run17c](overnight-report-2026-09-16-17c.md) (overnight report, 2026-09-15/16): the SQL door, the config carriers and the registry backlog — registry … [run18a](overnight-report-2026-09-16-18a.md) (day report): the functions slice; #657 merged tree-equal, #625 and #628 pushed as drafts. [run18b](overnight-report-2026-09-16-18b.md) (day report, 2026-09-16): #652, #653 and #655 merged, #659 ready, #662 parked as a draft. Lesson: a panic removal exposed LEFT joins returning four times their rows. [run18c](overnight-report-2026-09-16-18c.md) (day report): the SQL door, planner contracts and registry backlog; #654 and #656 merged, three cards for run 19.
- **run 20:** [run20a](overnight-report-2026-09-17-20a.md) and [run20b](overnight-report-2026-09-17-20b.md): **2026-09-17 morning.** 20a: the Iceberg remediation read path, DML planner, writer paths and conflict detection; merges, rulings Q-20a-1…7 and owner questions. Central lesson: three of six reviewed heads needed remediation on defects no gate could see. 20b: the Iceberg remediation follow-on after run 19 (the silent-wrong smalls, the catalog and identifier paths, the registry sweep); two units that simulated Iceberg semantics in RePark were sent back to the fork's API. [run20c](overnight-report-2026-09-17-20c.md) (morning report): Iceberg maintenance and writer-knob slice; six fork and RePark PRs merged, the AWS acceptance run green.
- [harness-loop-rebalance-2026-09-24.md](harness-loop-rebalance-2026-09-24.md) — HARNESS-LOOP-REBALANCE-1 (DEFERRED, owner 2026-09-24): the delegated-lane loop measured on run 28/29 (96 executor + 110 critic rounds for ~14 PRs, 373 orchestrator wakes, a three-PR trace ranking the wastes), the drafted fixes (severity-tagged critic findings, verdict tiers, cap-3 disposition, one PR per lane, event-only wakes, worker auto-resume) and the permission-classifier findings; direct orchestration until the owner schedules it.
- **runs 28 and 29, then direct orchestration:** [run28-opus58](day-report-2026-09-23-28-opus58.md), [run28-opus59](day-report-2026-09-23-28-opus59.md), [run28-opus60](day-report-2026-09-23-28-opus60.md) and [run28-opus61](day-report-2026-09-23-28-opus61.md): hand-offs of lanes xo-opus58 to xo-opus61 (run 28, day 2026-09-23): items, rounds, residues, and the hand-off to run 29. [run29-opus62](night-report-2026-09-23-29-opus62.md), [run29-opus63](night-report-2026-09-23-29-opus63.md), [run29-opus64](night-report-2026-09-23-29-opus64.md), [run29-opus65](night-report-2026-09-23-29-opus65.md) and [run29 note](night-report-2026-09-23-29-orchestrating-note.md): close-outs of lanes xo-opus62 to xo-opus65 (run 29, night 2026-09-23/24): PRs merged, critic rounds, rulings taken, numbered residues; the note, for runs 28 and 29: 20 RePark PRs and fork #347 merged, scoreboard 638 EQUAL, the override-grant rulings, the fork CI blocker, and the switch to direct orchestration. [day 2026-09-25](day-report-2026-09-25-direct.md) (direct orchestration, 2026-09-24 20:00 to 09-25 11:00): six RePark PRs and fork #353 and #354 merged; scoreboard 668 EQUAL / 45 to go with zero regressions. [day 2026-09-26](day-report-2026-09-26-direct.md): direct orchestration (2026-09-26): nine RePark PRs and one fork PR merged (#843 #844 #846 #847 #848 #849 #850 #851 #852; fork #358), scoreboard 688 EQUAL / 24 to go with zero regressions; the day's rulings, lessons and spend.
- [guided-executor-bands-2026-09-26.md](guided-executor-bands-2026-09-26.md) — GUIDED-EXECUTOR-BANDS-1 (ACTIVE, owner 2026-09-26): executor work-order shape follows the engine's Terminal-Bench 4.0 band (self-directed 50 and above, guided 25 to 49, clerk below 25), the observation behind it (the Muse U11 round halting on open rulings), the measured cost picture, and the standing weekly re-pull duty.
- **The shipped tags:** [v1.5.0 readiness](v1-5-0-release-readiness-2026-09-27.md) and [v1.5.0 notes draft](v1-5-0-release-notes-draft-2026-09-27.md) (the release-readiness audit, WO RELEASE-READY-1, and the FINAL notes; tag `v1.5.0`, shipped 2026-09-27): every non-EQUAL matrix cell against its carve-out. Release notes: [v1.5.1](v1-5-1-release-notes-2026-09-29.md) (2026-09-29): the first patch on 1.5.0, covering `9392dbc3` (v1.5.0) to `8568e57a`: U12 S3 path writes with the live AWS acceptance result, CASESENS-1, the store-assignment units, INTDIV-1, the RP-55 fork repin, one performance note, the rerun matrix (705/132/5/0), and the v1.5.2 follow-ups. Every line comes from a commit subject, a ledger, a verifier hand-back or the matrix. · [v1.5.2](v1-5-2-release-notes-2026-10-03.md) (2026-10-03): the second patch on 1.5.0, from `db3a1f37` (v1.5.1): TA-CHAIN-1, POLARS-IS-DUPLICATED-1, DEEP-FILTER-CHAIN-CRASH-1, STORE-TS-DOORS-2, STRING-LITERAL-ESCAPE-1, CAST-OVERFLOW-INSERT-1 and TEXT-WRITE-TIMESTAMP-ZONE-1, the RP-56 fork repin, CI-1 wheel-smoke sharding and the C-0 disposable Postgres harness. · [v1.5.3](v1-5-3-release-notes-2026-10-07.md) (2026-10-07): the third patch on 1.5.0, from `d0c50405` (v1.5.2) to `392f2350`: SOURCE-URL-REDACT-1, -1-FN and -2, ATTR-ID-1 with its ruled release note and the STAMP-2 gate record (1.0812), the TA single-series slices S0 to S3, the ANSI -0.0 division, GROWN-STACK-GATE-1, the connect and micro-batch groundwork that is not exposed, and the rerun matrix. · [v1.5.4](v1-5-4-release-notes-2026-10-10.md) (2026-10-10): the fourth patch on 1.5.0, after v1.5.3 (`7a8fcf1a`): the cross-join condition fix (#1010), the pyarrow floor 25.0.1 (#1015), `COUNT(*)` over the changelog, incremental and lineage readers (#1017), the nanosecond wall on every write door (#1018, R-007), the `encryption.key-id` refusal (#1019, ENC-1), the SQL door's `timestamp_*` family (#1012), and the known issues carried to v1.5.5. Tag, PyPI and matrix: `TODO(owner)` or `TODO(orchestrator)`. · [v1.5.5](v1-5-5-release-notes-2026-10-13.md) (2026-10-13): the fifth patch on 1.5.0, after v1.5.4 (`f4e424c1`): the `timestamptz_ns` wall on every write door (R-008), the pandas/polars nanosecond frame door with its three behaviour changes (R-019), `current_date` in the session zone, the Postgres write core with both-door routing, the merge module split, two lock-only dependency bumps, and the known issues carried to v1.5.6. Tag, PyPI and matrix rerun: `TODO(owner)` or `TODO(orchestrator)`. Reconciled 2026-10-22: the section "Added by the re-pointed tag" lists what the 2026-10-20 re-tag to `7fb5f6b2` shipped (U-1, U-3, U-5, U-6, U-7, the subquery refusals, the fork repins) and "Known issues in this release" what the wheel carries that main has since fixed. · [v1.5.6](v1-5-6-release-notes-2026-11-08.md) (2026-11-08): the empty-DML guard as the data-safety lead, the ruling D divergences, what the release does not carry (the v2 and v3 labels, see [ice-v2-label-1-card-2026-11-05.md](ice-v2-label-1-card-2026-11-05.md)), one line per change since `v1.5.5`. The [qualification record](v1-5-6-qualification-record.md) holds the source, lock, CI, verdicts, gates and owner actions.
- [v1.5.3 card](v1-5-3-card-2026-10-04.md) — **(2026-10-04):** the third patch on 1.5.0, from main after `d0c50405`: PARITY-LIVE-STOP-1 (clerk grade) and SOURCE-URL-REDACT-1 and -2 (security, a release item, masking a password in URL- or DSN-shaped values); the folds and the re-verify findings are on the card.
- [RELEASE-DIFF-1-5-1-PE](release-diff-1-5-1-pre-existing.md) — **(2026-09-29):** the wrong answers found on main by the v1.5.1 release differential and the per-PR verifiers that were already there before this release. PE-3 to PE-26 each carry a severity, Spark's answer and a card name, with the silent rows (S1) first.
- [cast-overflow-in-table-insert-1.md](cast-overflow-in-table-insert-1.md) — **CAST-OVERFLOW-IN-TABLE-INSERT-1 card (2026-09-28):** an out-of-range fractional store into an integer column refuses `CAST_OVERFLOW_IN_TABLE_INSERT`, as Spark does. It is in flight as PR #891 (v1.5.2).
- [polars-is-duplicated-1.md](polars-is-duplicated-1.md) — **POLARS-IS-DUPLICATED-1 card (2026-09-28, owner ruling):** `.filter(col('x').is_duplicated())` on both `rp.col` and `F.col`, with Polars 1.43.2 semantics (every occurrence, null==null, NaN==NaN, 0.0==-0.0). It is in flight as PR #883 (v1.5.2).
- [MISSING-REF-RESOLVE-1](missing-ref-resolve-1-card-2026-10-03.md) — **card (2026-10-03, ATTR-ID-1 SJ-6):** non-join missing references resolve the way Spark does — `select` refuses, `filter`/`orderBy` resolve through the child — with the `p2_lineage` oracle cells and the three pins it flips. Verifier V-3. **SM-1 (2026-10-06):** the D-4 orderBy grid gains the six `halt3` cells.
- [VIEW-LINEAGE-SELFJOIN-1](view-lineage-selfjoin-1-card-2026-10-03.md) — **card (2026-10-03, ATTR-ID-1 SJ-6):** a view read keeps its frame's lineage for self-join checks — the temp-view registry stores the registering frame's node — with the `J_view_sel_d`/`J_sql_sel_d` oracle cells.
- [LOCAL-CHECKPOINT-NEW-FRAME-1](local-checkpoint-new-frame-1-card-2026-10-03.md) — **card (2026-10-03, ATTR-ID-1 SJ-6, ruled R-SJ4-1):** `localCheckpoint()` returns a new frame, so `I_checkpoint_sel_d` answers Spark's rows; the same-object pins are rewritten to the new semantics.
- [PERF-ATTR-STAMP-2](perf-attr-stamp-2-card-2026-10-03.md) — **card (2026-10-03, owner ruling, design unit, before 1.6):** take attribute-id stamping under 1.10x like-for-like; the stack ships to main at 1.2021x as a dated carve-out and no release tag carries it until this card lands and is measured.
- [PERF-BASELINE-2](perf-baseline-2-card-2026-11-08.md) — **card (2026-11-08, owner ruling B, filed, not scheduled):** the ETL-vs-Spark gate and the Polars and DuckDB targets; protocol and baseline PENDING.
- [fa-6-duplicate-view-schemas-card-2026-10-08.md](fa-6-duplicate-view-schemas-card-2026-10-08.md) — **card FA-6 (2026-10-08, filed, needs an owner decision):** temp views over frames with duplicate display names. A first design (unique engine names plus an ambiguity audit) was withdrawn from #1002 for silent wrong answers; the ask is a design where the planner's own scoping resolves the names, or the declared refusal stays. Carries the verifier's shapes as the acceptance grid.
- [STAMP-2-R5P6-2](stamp-2-r5p6-2-card-2026-10-07.md) — **card (2026-10-07, filed, not scheduled):** the remaining r5p6 construction cost after #980 (family 1.135 on the met 1.0812 gate record): native join doors when attribute-exact, shared field-metadata maps, binding only if a hot spot is measured.
- [attr-id-1-release-note-2026-10-03.md](attr-id-1-release-note-2026-10-03.md) — **ATTR-ID-1 release note (2026-10-03, SJ-6):** the user-visible tightening that ships with the ATTR-ID-1 stack, not v1.5.2: the `s.join(d).select(d.v)` 1182 refusal, the `failAmbiguousSelfJoin=false` escape hatch, the free-name `AMBIGUOUS_REFERENCE` tightening and the alias fix; fresh attribute ids on each read of a SQL temp view (ATTR-VIEW-SEMANTICS-1 item (a)); the SM-1, SM-2 and SM-2d folds.
- [fexpr-operator-paren-1-card-2026-10-01.md](fexpr-operator-paren-1-card-2026-10-01.md) — **card FEXPR-OPERATOR-PAREN-1
  (2026-10-01, HIGH, from the overnight owner-items backlog, #890 re-verify 3 VE4-1):** an `F.expr`
  fragment without spaces around a binary operator is composed without parentheses, so
  `cube(F.expr('a-b')*2)` and `F.expr('(a) - (b)')` give wrong cube/rollup grouping keys
  (RePark -1/-16/3, Spark -14/0/10/4); cause `python/repark/src/repark/spark/functions.py:326-329,338`.
- [concat-nested-memory-1-card-2026-10-01.md](concat-nested-memory-1-card-2026-10-01.md) — **card
  CONCAT-NESTED-MEMORY-1 (2026-10-01, HIGH, from the overnight owner-items backlog, #892 lane):**
  300 nested `F.concat(e, F.lit('x'))` over a 50-row frame grew a debug process to 90+ GB over
  4.5 h (119 GB in another run); the `/tmp/concat.py` probe is gone (evidence not retained), so the
  repro is rebuilt inline and runs must be memory-capped (`ulimit -v 67108864`, about 240 s).
- [string-to-date-truncate-1-card-2026-10-01.md](string-to-date-truncate-1-card-2026-10-01.md) — **card
  STRING-TO-DATE-TRUNCATE-1 (2026-10-01, from the overnight owner-items backlog, VN6-4 S3):**
  `string_to_date` (`crates/repark-functions/src/cast_map/leaf.rs:562-576`) rejects
  `'2024-01-01 junk'` and `'2024-01-01Tjunk'` where Spark truncates to the date; ANSI-off
  `nullif(DATE '2024-01-01', '2024-01-01 junk')` returns the date where Spark returns NULL.
- [s3a-overwrite-commit-1-card-2026-10-01.md](s3a-overwrite-commit-1-card-2026-10-01.md) — **card
  S3A-OVERWRITE-COMMIT-1 (2026-10-01, from the overnight owner-items backlog, #889 rollback fold
  on moto):** an s3a `mode("overwrite")` text write deletes the destination's existing objects
  BEFORE the COPY, so a failed overwrite leaves the destination empty; Spark's commit protocol
  keeps the old data until commit; same on `3d4f2030` and on base.
- [coalesce-shuffle-flake-1-card-2026-10-01.md](coalesce-shuffle-flake-1-card-2026-10-01.md) — **card
  COALESCE-SHUFFLE-FLAKE-1 (2026-10-01, from the overnight owner-items backlog, #888 re-verify 7):**
  an attack3 coalesce/shuffle wrong-result flake, reproduced on base; evidence
  `/tmp/oc-worker/direct/wo/reverify7-nvl/` (evidence location, retained).
- [json-ntz-null-inference-1-card-2026-10-01.md](json-ntz-null-inference-1-card-2026-10-01.md) — **card
  JSON-NTZ-NULL-INFERENCE-1 (2026-10-01, from the overnight owner-items backlog, #889 fold):** a
  mostly-null NTZ column drops out of JSON read-back key inference, so appending such a frame to
  its own seed fails append validation.
- [view-ansi-capture-1-card-2026-10-01.md](view-ansi-capture-1-card-2026-10-01.md) — **card
  VIEW-ANSI-CAPTURE-1 (2026-10-01, from the overnight owner-items backlog, #888 re-verify 8
  VN9-4):** views are re-analyzed under the current session's ANSI setting, not the setting they
  were created under, so a view created under ANSI on with `sequence(1, '3')` or a CAST changes
  behaviour when read under ANSI off; Spark keeps the creation-time setting.
- [const-eval-ceiling-1-card-2026-10-01.md](const-eval-ceiling-1-card-2026-10-01.md) — **card
  CONST-EVAL-CEILING-1 (2026-10-01, the owner's card, from the overnight owner-items backlog):**
  replace the mirror constant evaluator (`cardinality.rs` const_i128/const_f64/exact_i128, and
  `cardinality_nullif.rs`) with a ceiling check on DataFusion's ExprSimplifier/ConstEvaluator
  output; includes the line-67 unknown-versus-NULL defect (`nullif(101,
  coalesce(CAST('0' AS INT), 101))` proves a false equality) as a known instance.
- [verbatim-ctas-escape-1-card-2026-10-01.md](verbatim-ctas-escape-1-card-2026-10-01.md) — **card
  VERBATIM-CTAS-ESCAPE-1 (2026-10-01, from the overnight owner-items backlog, #890 re-verify
  VE2-3):** in verbatim mode (`spark.sql.parser.escapedStringLiterals=true`), CTAS and INSERT
  OVERWRITE with a `\'` literal fail with a parse error, while Spark accepts them.
- [cte-temp-view-precedence-1-card-2026-10-01.md](cte-temp-view-precedence-1-card-2026-10-01.md) — **card
  CTE-TEMP-VIEW-PRECEDENCE-1 (2026-10-01, from the overnight owner-items backlog, one card covering
  lines 69, 77 and 81):** CTE-versus-temp-view precedence gives silent wrong data on column-list
  INSERT, static-partition OVERWRITE and CTAS (a CTE should shadow a same-named temp view); the
  facade region walker expands CTE references on the column-list door; VT4-2 (quoted-case CTE name)
  and VT4-3 (`PARTITION (p)` / `PARTITION (p='v')` doors).
- [merge-update-ts-widening-1-card-2026-10-01.md](merge-update-ts-widening-1-card-2026-10-01.md) — **card
  MERGE-UPDATE-TS-WIDENING-1 (2026-10-01, from the overnight owner-items backlog, #894 re-verify
  2, pre-existing on base):** MERGE and UPDATE refuse the STRING-plus-TIMESTAMP widening that
  Spark stores.
- [fexpr-timestamp-zone-1-card-2026-10-01.md](fexpr-timestamp-zone-1-card-2026-10-01.md) — **card
  FEXPR-TIMESTAMP-ZONE-1 (2026-10-01, from the overnight owner-items backlog, #888 re-verify
  10):** `F.expr` TIMESTAMP literals resolve in UTC instead of the session zone (a DataFrame
  `F.expr` door gap; SQL resolves them in the session zone).
- [ml-cv-fold-test-strength-1-card-2026-10-01.md](ml-cv-fold-test-strength-1-card-2026-10-01.md) — **card
  ML-CV-FOLD-TEST-STRENGTH-1 (2026-10-01, from the overnight owner-items backlog, VE4-2):**
  `test_ml_boost_oracle.py::test_cross_validator_materializes_fold_labels` cannot tell folds that
  ignore the materialized view from folds that use it, because the fold-count query reads the view anyway.
- [unpivot-perf-flake-1-card-2026-10-01.md](unpivot-perf-flake-1-card-2026-10-01.md) — **card
  UNPIVOT-PERF-FLAKE-1 (2026-10-01, from the overnight owner-items backlog, CI flake):**
  `python/repark/tests/perf/test_perf_unpivot_1.py::test_stack_is_linear_in_columns` is timing-based
  and fails on shared CI runners with no product cause (hit #891 at exponent 1.224 and #892 at
  1.120); needs a noise-robust bound.
- [spark-string-ts-functions-1-card-2026-10-01.md](spark-string-ts-functions-1-card-2026-10-01.md) — **card
  SPARK-STRING-TS-FUNCTIONS-1 (2026-10-01, from the overnight owner-items backlog, #894 fold 4):**
  RePark registers no Spark `string()`, `timestamp()` or `current_user()` function
  (datafusion-spark 54.1 ships none); `SELECT string(1)` raises `UNRESOLVED_ROUTINE` where Spark answers.
- [string-numeric-parse-perf-1-card-2026-10-01.md](string-numeric-parse-perf-1-card-2026-10-01.md) — **unit
  STRING-NUMERIC-PARSE-PERF-1 (2026-10-01, the owner's new unit, from the overnight owner-items
  backlog):** a fast Spark-compatible string→number parse in the shared `cast_map`
  (`crates/repark-functions/src/cast_map/leaf.rs`, `string_to_fractional`, then
  `java_double/parse_float.rs`); target string→DOUBLE/INT/BIGINT/DECIMAL within 1.05x of
  DataFusion's native cast with byte-identical Spark semantics; carried by #888 as VN7-5, not a #888 blocker.
- [nvl-type-coercion-2-card-2026-10-02.md](nvl-type-coercion-2-card-2026-10-02.md) — **card
  NVL-TYPE-COERCION-2 (2026-10-02, design-heavy, from the #888 park):** one pre-coercion
  mode-aware rewrite for the nvl family (Spark types `nvl('a', 5)` BIGINT on, STRING off;
  the rewrite runs after `type_coercion`), deleting the base route, `core_declared`, the
  fexpr markers, the F.expr `type_coercion` strip and the ceiling mirror evaluator; the
  review experiment fixed 16 of 31 nested cap-b cells with zero ANSI-off movement. An Opus
  design sketch comes first; no more folds on `54bc9ac2`.
- [coalesce-ansi-widen-1-card-2026-10-02.md](coalesce-ansi-widen-1-card-2026-10-02.md) — **card
  COALESCE-ANSI-WIDEN-1 (2026-10-02, from the #888 review):** bare `coalesce`/`array`
  widening over STRING with DATE, BINARY and DECIMAL, pre-existing on base (four measured
  cells: `coalesce('2024-01-02', DATE'2024-01-02')`, `array(sdt, dt)`, `upper(TIMESTAMP)`,
  `coalesce(1.50BD, '1.5')`); BINARY shapes not yet measured.
- [bare-literal-function-column-1-card-2026-10-02.md](bare-literal-function-column-1-card-2026-10-02.md) — **card
  BARE-LITERAL-FUNCTION-COLUMN-1 (2026-10-02, S1, from the overnight owner-items backlog,
  #894 round-6 VT7-1):** a STRING column named `current_timestamp`/`current_date` resolves
  as the function (`bare_nullary.rs:9` lacks column-first); SELECT returns and stores the
  current time where Spark uses the column; 24 cells, pre-existing on base.
- [session-conf-live-1-card-2026-10-02.md](session-conf-live-1-card-2026-10-02.md) — **card
  SESSION-CONF-LIVE-1 (2026-10-02, from ATTR-ID-1 S3c):** frames keep the case rule from
  creation in `task_ctx`, so `conf.set` flips leave facade-live and native planning
  disagreeing (121 gap-moved cells); a session-infrastructure card, outside S3a–S3e.
- [createdataframe-dup-names-1-card-2026-10-02.md](createdataframe-dup-names-1-card-2026-10-02.md) — **card
  CREATEDATAFRAME-DUP-NAMES-1 (2026-10-02, from ATTR-ID-1 S3d residue 2):** RePark's
  `createDataFrame` refuses duplicate column names where Spark mints distinct ids (probe
  `s3d18` unbuildable; the multi-id-union refusal is pinned via alias-dup instead).
- [selectexpr-dup-field-join-1-card-2026-10-02.md](selectexpr-dup-field-join-1-card-2026-10-02.md) — **card
  SELECTEXPR-DUP-FIELD-JOIN-1 (2026-10-02, from ATTR-ID-1 S3e):** `selectExpr` over
  duplicate-field join frames fails at the scratch-view scan, on main-identical untouched
  paths; a view/engine follow-up.
- [qualified-display-name-1-card-2026-10-02.md](qualified-display-name-1-card-2026-10-02.md) — **card
  QUALIFIED-DISPLAY-NAME-1 (2026-10-02, from ATTR-ID-1 S3e):** Spark strips qualifiers in
  compound display names (`(v + v)`) and `selectExpr` (`(v + 1)`); RePark keeps the written
  qualifier; a display-naming follow-up.
- [star-error-shape-1-card-2026-10-02.md](star-error-shape-1-card-2026-10-02.md) — **card
  STAR-ERROR-SHAPE-1 (2026-10-02, from ATTR-ID-1 S3e):** `INVALID_USAGE_OF_STAR_OR_REGEX`
  for `F.col("q.*")` in filter/withColumn/orderBy, `CANNOT_RESOLVE_STAR_EXPAND` for unknown
  stars, and shaped `getitem` `q.*`, where RePark gives engine errors; an error-shaping
  follow-up.
- [unresolved-suggest-qualified-1-card-2026-10-02.md](unresolved-suggest-qualified-1-card-2026-10-02.md) — **card
  UNRESOLVED-SUGGEST-QUALIFIED-1 (2026-10-02, from ATTR-ID-1 S3e):** Spark's UNRESOLVED
  suggestion lists are qualifier-qualified; RePark echoes bare names, unchanged from main;
  a suggestion-list follow-up.
- [withcolumn-dup-display-1-card-2026-10-02.md](withcolumn-dup-display-1-card-2026-10-02.md) — **card
  WITHCOLUMN-DUP-DISPLAY-1 (2026-10-02, from ATTR-ID-1 S3e):** `withColumn` over
  duplicate-display frames raises a bare AMBIGUOUS_REFERENCE via `_iter_bound_columns`,
  main-identical; an engine/bounds follow-up.
- [qualified-twin-bind-1-card-2026-10-02.md](qualified-twin-bind-1-card-2026-10-02.md) — **card
  QUALIFIED-TWIN-BIND-1 (2026-10-02, record only, from ATTR-ID-1 S3e):** single-id
  multi-hit qualified binds against Spark's two-attribute refusal under the settled
  S1/C-038 select-dup model; target "revisit if a cell disagrees".
- [DF-METADATA-COL-1](df-metadata-col-1-card-2026-10-02.md) — **card
  DF-METADATA-COL-1 (2026-10-02, re-cut from closed draft PR #662):**
  `DataFrame.metadataColumn` and the hidden `_metadata` struct on file scans, two weeks
  stale with review findings open (P1 L-501 + 3 P2 + 1 P3, R-18b-16..19, Spark cells
  recorded); kept branch `feat/df-metadata-col-1` at `e07acb9c`, round-4 tree saved as a
  patch.
- [JAVA-REGEX-FEATURES-1](java-regex-features-1-card-2026-10-02.md) — **card
  JAVA-REGEX-FEATURES-1 (2026-10-02, re-cut from closed draft PR #658):** `split` /
  `regexp_*` / `rlike` answer Java lookaround, backreferences and possessive quantifiers
  via `fancy-regex 0.11`, verification-clean but never queued; kept branch
  `feat/java-regex-features-1` at `4c36c6f6`; three declared residue cells with recorded
  Spark answers.
- [FNP-AGG-1](fnp-agg-1-card-2026-10-02.md) — **card FNP-AGG-1
  (2026-10-02, re-cut from closed draft PR #625, step 1 of 5):** the aggregate names
  still only on the branch — slice (d) (`grouping_id` + foundation) landed as `41534851`,
  slices (a)–(c) unbuilt with 10 names remaining per the 09-21 rescue report; kept branch
  `feat/fnp-agg-1` at `4a3379fe`; oracle recorded.
- [ta-chain-1-card-2026-10-03.md](ta-chain-1-card-2026-10-03.md) — **card TA-CHAIN-1
  (2026-10-03, owner ruling, v1.5.2):** `ta.ema` over `ta.trange` answers all-NaN because a
  leading lookback NaN propagates through C-faithful kernels; polars_talib skips the leading
  run (measured); wrapper-level fix, order at `task/wo/ta-chain-1-leading-prefix.md`.
- [grown-stack-gate-1-card-2026-10-04.md](grown-stack-gate-1-card-2026-10-04.md) — **card
  GROWN-STACK-GATE-1 (2026-10-04, owner, grade B):** 34 unconditional `deep_stack::block_on` sites grow
  a 128 MiB stack on every poll; main's work-equal like set +5.2 % from v1.5.1; gate them as R4 gated
  the frame doors; order at `task/wo/grown-stack-gate-1.md`; the stack merge waits on it.
- [ta-single-series-parallel-1-card-2026-10-04.md](ta-single-series-parallel-1-card-2026-10-04.md) — **card
  TA-SINGLE-SERIES-PARALLEL-1 (2026-10-04, owner, design grade, next TA item after v1.5.2):** one
  1,000,000-row series with dependent `ta.*` levels runs 0.49 s on one core against polars_talib's 0.12 s;
  parallel window-expression evaluation or a K-operator zip, sketch first; gate ratio ≤ 1.00.
- [variance-alias-1-card-2026-10-03.md](variance-alias-1-card-2026-10-03.md) — **card
  VARIANCE-ALIAS-1 (2026-10-03, owner delegate, post-1.5.2):** SQL `variance(x)` is
  `UNRESOLVED_ROUTINE` on RePark; Spark and `var_samp` answer double `2.333333333333333`.
  The DataFrame door already answers. Predates the ATTR-ID-1 stack.
- [coalesce-nan-double-1-card-2026-10-03.md](coalesce-nan-double-1-card-2026-10-03.md) — **card
  COALESCE-NAN-DOUBLE-1 (2026-10-03, owner delegate, post-1.5.2):** `coalesce` of a DOUBLE NaN
  with decimal literal `-1.0` overflows `Decimal128(30, 15)` on RePark; Spark returns double
  `NaN` and `1.5`. Predates the ATTR-ID-1 stack.
- [qualifier-leak-h-1-card-2026-10-03.md](qualifier-leak-h-1-card-2026-10-03.md) — **card
  QUALIFIER-LEAK-H-1 (2026-10-03, owner delegate, post-1.5.2):** cell
  `r3.F_cp_bare_getU_join_parent` raises `AMBIGUOUS_REFERENCE` naming `_repark_jl_<hex>`.`V`;
  Spark returns the joined rows. Predates the ATTR-ID-1 stack.
- [zizmor-gate-1-card-2026-10-03.md](zizmor-gate-1-card-2026-10-03.md) — **card ZIZMOR-GATE-1
  (2026-10-03, clerk, after CI-1):** `--format sarif` exits 0 while findings exist (plain
  mode exits 14); the ci.yml map guard passes `github.base_ref` via `env:`, and zizmor.yml
  runs plain mode as the blocking step before the SARIF upload.
- [attr-view-semantics-1-card-2026-10-04.md](attr-view-semantics-1-card-2026-10-04.md) — **card
  ATTR-VIEW-SEMANTICS-1 (2026-10-04, measurement plus filing, no product code):** SQL aliases
  and SQL-defined views mint ids in Spark (`v#3L`, `id#16L/v#17L`) and foreign columns raise
  MISSING_ATTRIBUTES; measured on Spark 4.1.2, 1.5.2 and the stack (`d5c97862`) — 11/7 on main,
  9/9 on the stack, the two V-3 internal errors the only new stack differences; scope (a)–(d)
  with proposed pins, V10/F16 out; **owner rulings 2026-10-05:** grade B with an Opus sketch after the TA
  series work, (d) fixed on the stack before the stack-to-main PR, (a)–(c) their own PR on main after
  the stack merges, (c) ships with a release note in the minor (no warning-first), V6b out.
- [explain-root-sort-1-card-2026-10-04.md](explain-root-sort-1-card-2026-10-04.md) — **card
  EXPLAIN-ROOT-SORT-1 (2026-10-04, measured on 1.5.2):** the facade's `explain()` plans the
  frame through a scratch view, so DataFusion's subquery sort elimination hides a root `Sort`
  that `collect()` executes; fix by explaining the frame's own plan; pins per sort shape.
- [distributed-encode-1-card-2026-10-04.md](distributed-encode-1-card-2026-10-04.md) — **card
  DISTRIBUTED-ENCODE-1 (2026-10-04):** three `repark-distributed --features cluster` tests red on
  main (two `LazyMemTableExec` encode failures, one pushdown-surface test); the codec encodes only
  `IcebergTableScan`, so RePark execs such as `NljBuildSideExec` likely fail to encode (inferred);
  measure first, then codec arms or single-node flags.
- [cache-order-read-1-card-2026-10-05.md](cache-order-read-1-card-2026-10-05.md) — **card
  CACHE-ORDER-READ-1 (2026-10-05, filed by Muse Spark 1.3 for the orchestrator, from TA series
  S1):** filter reads of a sorted single-partition `eager()` frame interleave (0 of 10 on S1's
  head, 1 of 10 on 1.5.2, 3 of 3 ordered on Spark 4.1.2) because `BatchSplitStream` splits the
  stored batch under `RoundRobinBatch`; two design options (unsplit rule, trailing
  `SortPreservingMerge`) with costs; grade lean B with an Opus sketch, release after S1.
- [ansi-remainder-class-1-card-2026-10-05.md](ansi-remainder-class-1-card-2026-10-05.md) — **card
  ANSI-REMAINDER-CLASS-1 (2026-10-05, filed by Muse Spark 1.3 for the orchestrator, from the
  PR #943 check):** the 10-cell ANSI-on matrix where Spark raises `REMAINDER_BY_ZERO` — RePark
  raises `DIVIDE_BY_ZERO` on `%`, answers `NULL` on `mod`/`pmod`, `NaN` on a `-0.0` divisor,
  and cannot parse infix `div`; the guard learns its operator, with the error text per operator;
  grade lean B, small.
- [mb-pending-window-1-card-2026-10-07.md](mb-pending-window-1-card-2026-10-07.md) — **card
  MB-PENDING-WINDOW-1 (2026-10-07, filed by claude-opus-5-5 for the orchestrator, from MB-3
  fold 1, PR #994):** a restart after a failed batch replans that batch's window, so the same
  batch id covers a wider window when the source grew (Spark replays the logged window, cell
  MB3-W9); option (a) a pending-window stamp before the body at one more commit per batch,
  option (b) leave registry row `MB-3-REPLAY-WINDOW-1` declared; needs an owner decision.
- [ZONE-HORIZON-RENDER-1](zone-horizon-render-1-card-2026-10-07.md) — **closed (2026-10-08, PR #991):** an instant past 2099 in a DST zone rendered at standard time; every site reads `repark_common::zone_horizon`. `from_utc_timestamp` / `to_utc_timestamp` stay open as ledger C-012.
- [transition-day-arithmetic-1-card-2026-10-08.md](transition-day-arithmetic-1-card-2026-10-08.md) — **card
  TRANSITION-DAY-ARITHMETIC-1 (2026-10-08, open, not scheduled, ruled 2026-10-08 from ZONE-HORIZON-RENDER-1
  C-016 and residue R-4):** on a DST transition day, `unix_timestamp` of a gap wall, a gap wall cast to
  `TIMESTAMP_NTZ` or `from_json`, and `months_between` and interval arithmetic answer differently from
  Spark; a defect at every year, hidden after 2099 on main only because main had no transitions there;
  1,208 verifier cells after 2099 and 20 at 2099 of the unit's grid.
- [preview-bounded-source-1-card-2026-10-07.md](preview-bounded-source-1-card-2026-10-07.md) — **card
  PREVIEW-BOUNDED-SOURCE-1 (2026-10-07, filed, not scheduled, from the C-2d fold 1 re-verify verdict,
  PR #991):** the styled `show(n)` on a lazy frame counts once and reads the tail once under the
  `polars` and `duckdb` styles; a bounded or refusal-tolerant preview over a mounted source, and
  whether PERF-EAGER-PREVIEW-1 covers the styled doors; owner decision before any code.
- [c-2-catalog-doors-card-2026-10-07.md](c-2-catalog-doors-card-2026-10-07.md) — **card
  C-2-CATALOG-DOORS-1 (2026-10-07, filed, not scheduled, from the C-2d fold 1 re-verify verdict,
  PR #991):** the neighbouring Spark-door catalog and DDL doors answer a mounted source with
  misleading text (`getTable`, `listColumns`, `SHOW VIEWS`, `SHOW COLUMNS`, `DESCRIBE TABLE`,
  `TRUNCATE TABLE`, `USE`); none writes; each door gives the declared listing or names its registry row.
- [OFFSET-NESTED-SORT-1](offset-nested-sort-1-card-2026-10-08.md) — **closed (2026-10-08 by its unit; the product PR's verifier pending):** an `OFFSET` under a nested `ORDER BY` answered 0 rows where 16 partitions answer 5; a DataFusion 54.1.0 defect, guarded in `repark-core`.
- [threaded-collect-segv-1-card-2026-10-08.md](threaded-collect-segv-1-card-2026-10-08.md) — **card
  THREADED-COLLECT-SEGV-1 (2026-10-08, filed, not scheduled, from the orchestrator's measurements, PR #998):**
  `collect()` on a second thread segfaults with pyarrow 25.0.0 and survives with 25.0.1, in 1.5.1 through
  1.5.3 alike; the ask is to find whether pyarrow or RePark's native row export faults, fix or pin it with a
  threaded test, and decide the dependency floor, an owner ruling. **Cause found 2026-10-08:** pyarrow
  25.0.0's bundled mimalloc (apache/arrow GH-50471, fixed in 25.0.1), reproduced without RePark; the
  threaded test is in the facade suite and the floor awaits the owner.
- [CROSS-JOIN-CONDITION-1](cross-join-condition-1-card-2026-10-08.md) — **closed (2026-10-08 by its unit, sequenced after PR #997):** a cross with a condition now takes the inner path on both routes.
- [JOIN-CONDITION-REFUSALS-1](join-condition-refusals-1-card-2026-10-08.md) — **card (2026-10-08, closed 2026-10-08 by CROSS-JOIN-CONDITION-1 fold 2, re-opened 2026-10-09 by fold 3 narrowed to residues R-CJC-1..R-CJC-5, from the CROSS-JOIN-CONDITION-1 Opus verifier):** every join door answered conditions Spark 4.1.2 refuses (non-deterministic expressions, an untyped NULL condition); the fold's `JoinConditionRefusals` analyzer rule now refuses them with Spark's class, condition and text on every `how` of both doors.
- [c-3-null-stride-1-card-2026-10-08.md](c-3-null-stride-1-card-2026-10-08.md) — **card
  C-3-NULL-STRIDE-1 (2026-10-08, filed, not scheduled, from C-3 question Q3, PR #998):** the first
  stride's `OR col IS NULL` arm is a sequential scan of the whole table (about 1.1 s of a 7.3 s
  four-stream read); the lean is the NULL test as its own stride, measured on an unindexed column
  before adopting.
- [c-3-partition-types-1-card-2026-10-08.md](c-3-partition-types-1-card-2026-10-08.md) — **card
  C-3-PARTITION-TYPES-1 (2026-10-08, filed, not scheduled, from C-3 question Q4, PR #998):** C-3 takes
  `int2`/`int4`/`int8` partition columns and refuses the rest under `CONNECT-DECL-pg-partitioned-read`;
  the ruled order is `date` first, then timestamp once the zone rule is ruled, each with its own Spark grid.
- [SQL-EPOCH-CONSTRUCTORS-1](sql-epoch-constructors-1-card-2026-10-08.md) — **closed (2026-10-08):** `timestamp_micros`, `timestamp_seconds` and `timestamp_millis` resolve on the SQL and DataFrame doors with Spark's values; the native door stays a declared refusal.
- [cast-view-agg-nullability-1-card-2026-10-08.md](cast-view-agg-nullability-1-card-2026-10-08.md) — **card
  CAST-VIEW-AGG-NULLABILITY-1 (2026-10-08, open, not scheduled, not attributed to a unit):** `max` over a view of
  `CAST(… AS TIMESTAMP)` of arithmetic over `range` raises a DataFusion internal schema error (a field nullability
  mismatch at `ts`); `count(*)`, a plain `SELECT` and a parquet round trip answer. Step 0 shrinks the repro and
  records Spark's answer; the fix is at the source or reported upstream.
- [ice-tsns-merge-wall-1-card-2026-10-09.md](ice-tsns-merge-wall-1-card-2026-10-09.md) **card ICE-TSNS-MERGE-WALL-1
  (2026-10-09, open, grade B, executor tier not chosen, Opus verifier):** in a non-UTC session a MERGE writes a
  `TIMESTAMP` into a `timestamp_ns` column as the UTC wall, while INSERT stores the session-zone wall (registry row
  `ICE-TSNS-SQL-1-R-007`). Widening the matchers alone is not the fix, because the NTZ wall cast is microsecond. The card
  holds the review's probe and its log, the acceptance criteria and the write-door checklist.
- [enc-1-residue-1-card-2026-10-09.md](enc-1-residue-1-card-2026-10-09.md) **card ENC-1-RESIDUE-1
  (2026-10-09, open for items 1 and 5):** what the keyed-table refusal does not cover, from the re-verify of PR #1019: a stale handle's
  one orphan Parquet, an unkeyed table placed under a keyed table's location, ref-only commits that publish rows staged
  before the key, the thirteen shapes whose clean refusal rests on entry checks, the textual gate and the
  caller-built catalog, and the Glue metadata-directory limit. Owner 2026-10-14 (ruling 32): items 2, 3, 4 and 6
  accepted. Items 1 and 5 built in #1071, final verifier FAIL (V1 to V5, 2026-10-17), parked under ruling 121(b);
  the 2026-10-20 plan's carve-out row (P9) awaits the owner.
- [enc-ctas-replace-1-card-2026-10-15.md](enc-ctas-replace-1-card-2026-10-15.md) **card ENC-CTAS-REPLACE-1
  (2026-10-15, open, not scheduled, clerk under owner ruling 98 (a)):** a key landing between `begin_replace`
  and the publish of `CREATE OR REPLACE TABLE … AS SELECT` leaves an orphan unencrypted file under the keyed
  table's `data/`, referenced by no snapshot, so never served. Retires with staged-file removal or a
  pre-staging refusal plus a pin; Spark-side behaviour not measured; main the same.
- [repark-sql-update-subquery-where-1-card-2026-10-15.md](repark-sql-update-subquery-where-1-card-2026-10-15.md) **card
  REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (2026-10-15, open, critical, clerk under owner ruling 106 (a)):**
  through `repark.sql()`, an `UPDATE` whose `WHERE` holds a subquery updates every row and reports
  success. Retires with `repark.sql()` routed through the ANSI door's dialect, or a refusal on that
  door as the stopgap; own unit next on the U-6 lane after #1062 merges. **2026-10-16:** `DELETE`
  is in, and the stopgap has landed (round 2: `repark.sql()` refuses every subquery-predicate
  `UPDATE` and `DELETE` with the G3-E8 text); routing stays this card, and a later tidy unit
  folds the Spark SQL door's G3-E8 copy into the shared home. **2026-10-16, ruling 131 (b):**
  `EXPLAIN` is in; the refusal fires inside `EXPLAIN` and `EXPLAIN ANALYZE`. **2026-10-17,
  ruling 135 (a):** the refusal reads every write the statement executes (the shared walk), so
  `PREPARE` and `EXPLAIN ANALYZE` refuse and a plain `EXPLAIN` answers its plan again; the pins
  run on Iceberg tables. **2026-10-17, ruling 139:** both repark-spark doors' strict pregate
  refuses each walked subquery-predicate `DELETE` and `UPDATE`, so `PREPARE` of one refuses there
  too; DataFusion's own `EXPLAIN ANALYZE` on those doors goes to card EXPLAIN-DML-WRITE-1.
- [ICE-REPLACE-KILL-DOORS-1](ice-replace-kill-doors-1-card-2026-10-27.md) — **card ICE-REPLACE-KILL-DOORS-1 (2026-10-27), open, owner ruled a card plus a small pin unit:** the RTAS and `CREATE OR REPLACE TABLE … AS SELECT` doors have no kill cell; `publish_replace_table` is forwarded unheld by the kill decorator (ice-integrity-pins-1 C-024).
- [EXPLAIN-DML-WRITE-1](explain-dml-write-1-card-2026-10-16.md) — **closed (2026-10-21 as PR #1137, `b7a20167`):** `EXPLAIN ANALYZE` and `PREPARE` of a DML write refuse on every door. Unscheduled follow-up: on `repark.sql()` a wrapped write DataFusion cannot plan keeps DataFusion's error.
- [ice-struct-inline-table-from-1-card-2026-10-15.md](ice-struct-inline-table-from-1-card-2026-10-15.md) **card
  ICE-STRUCT-INLINE-TABLE-FROM-1 (2026-10-15, open, not this round, clerk under owner ruling 107 (a),
  struct-pairing family of ruling 57):** an inline table in `FROM` stores a reordered struct by name
  where Spark stores it by position. Retires with positional pairing and Spark's `INVALID_INLINE_TABLE`
  plus pins.
- [ice-tsns-ansi-cast-floor-1-card-2026-10-16.md](ice-tsns-ansi-cast-floor-1-card-2026-10-16.md) **card ICE-TSNS-ANSI-CAST-FLOOR-1
  (2026-10-16, open, not scheduled, clerk under owner ruling 120 (a)):** on the ANSI doors
  `CAST(ns AS TIMESTAMP)` truncates a pre-1970 sub-microsecond value toward zero instead of flooring
  (Q-21c-8); stored values equal main's; register row ICE-TSNS-SQL-1-R-025.
- [mb-enc-undeleted-field-1-card-2026-10-15.md](mb-enc-undeleted-field-1-card-2026-10-15.md) **card
  MB-ENC-UNDELETED-FIELD-1 (2026-10-15, parked under the streaming freeze, clerk under owner ruling
  109 (c)):** the keyed micro-batch refusal names the sink but not a staged file it could not delete.
  Retires with the variant carrying the undeleted paths when the freeze lifts.
- [repark-sql-local-ddl-1-card-2026-10-15.md](repark-sql-local-ddl-1-card-2026-10-15.md) **card
  REPARK-SQL-LOCAL-DDL-1 (2026-10-15, open, not scheduled, clerk under owner ruling 104 (a)):** once
  PA-LOCAL-FS-POLICY-1 lands, `repark.sql()` cannot run any local `COPY` or `CREATE EXTERNAL TABLE`,
  read-only registration included, and has no opt-in. Retires with an opt-in path (or read-only
  registration allowed) plus pins; the owner field on closing before 1.5.6 ships stays open.
- [local-fs-create-table-location-1-card-2026-10-15.md](local-fs-create-table-location-1-card-2026-10-15.md)
  **card LOCAL-FS-CREATE-TABLE-LOCATION-1 (2026-10-15, open, clerk under owner ruling 105 (a)):**
  a Spark-door `CREATE TABLE … LOCATION '<outside>'` writes data and metadata outside every registered
  warehouse with the opt-in off. Same class as PA-02: the next U-5-kind unit on that lane after the
  PA-LOCAL-FS-POLICY-1 re-verifier. Retires with the location held to the `COPY` policy, door pins.
- [local-fs-write-root-1-card-2026-10-15.md](local-fs-write-root-1-card-2026-10-15.md) **card
  LOCAL-FS-WRITE-ROOT-1 (2026-10-15, open, not scheduled, clerk under owner ruling 105 (a)):** one
  DataFrame write to a path makes later SQL writes to that path allowed. Retires when a DataFrame
  write no longer widens the SQL allow-list, or the widening is an explicit, documented opt-in.
- [fork-tablecommit-updates-1-card-2026-10-09.md](fork-tablecommit-updates-1-card-2026-10-09.md) **card
  FORK-TABLECOMMIT-UPDATES-1 (2026-10-09, open, fork work):** a read accessor for `TableCommit`'s updates, so the
  ENC-1 commit guard matches on values and stops reading `Debug` text.
- [cluster-feature-ci-1-card-2026-10-09.md](cluster-feature-ci-1-card-2026-10-09.md) **card CLUSTER-FEATURE-CI-1
  (2026-10-09, open):** no CI job builds or tests `repark-distributed --features cluster`; a regression there went
  unseen until a verifier ran it by hand. **Owner ruling 2026-10-10:** the CI job is added; the failing codec test is
  fixed first on its own small PR; the job is not a required check until it has been green for a week.
  unseen until a verifier ran it by hand. CLUSTER-CODEC-TEST-1 (2026-10-10) fixed the
  failing test ahead of the job; the card retires with the job.
- [enc-1-first-write-refusal-card-2026-10-09.md](enc-1-first-write-refusal-card-2026-10-09.md) **card
  ENC-1-FIRST-WRITE-REFUSAL-1 (2026-10-09, open, clerk guided by Muse, Opus verifier):** the owner's 2026-10-01 ruling
  (ES-3) that the first write to a table carrying `encryption.key-id` refuses, as Spark does without a KMS. The card's
  coverage checklist lists every write and maintenance door named in the maps, and the pin
  `v3_create_with_encryption_key_id_still_scans_without_a_kms` flips on purpose.
- [utc-timestamp-port-1-card-2026-10-08.md](utc-timestamp-port-1-card-2026-10-08.md) — **card
  UTC-TIMESTAMP-PORT-1 (2026-10-08, open, not scheduled, ruled 2026-10-08 as a follow-up to ZONE-HORIZON-RENDER-1,
  from its C-012, R-1 and R-12):** `from_utc_timestamp` and `to_utc_timestamp` read standard time after 2099 because
  they are `datafusion-spark` kernels; the port owns them in `repark-functions` on the zone-horizon helper and hoists
  the helper's year check, returning `CAST AS DATE` and `CAST AS TIMESTAMP_NTZ` before 2100 to main's cost (1.043 and
  1.041 of main on 20,000,000 rows). The ledger's 223 residue cells (13 at 2099) are the grid.
- [csv-writer-door-1-card-2026-10-08.md](csv-writer-door-1-card-2026-10-08.md) — **card
  CSV-WRITER-DOOR-1 (2026-10-08, open, not scheduled, from FA-5's fold-1 ledger and its reverify verdict):** the csv
  write door's byte differences from Spark that FA-5 recorded and did not fix (header, trimming, quoting, binary,
  coalesce, empty writes), one refusal shape, and the verifier's S2: under `caseSensitive=true` a case-twin
  partition column writes to the wrong directory. Decided per row by the owner.
- [using-side-key-design-1-card-2026-10-08.md](using-side-key-design-1-card-2026-10-08.md) — **design card
  USING-SIDE-KEY-DESIGN-1 (2026-10-08, open, not scheduled, requires an Opus design sketch before any executor):**
  PR #1000 (USING-PER-SIDE-KEYS-1) parked at `e1e763fa` after three verifies, each finding the plan-level marker lost
  or unread in one more pass (aggregate and window, the retry binding the left key, the WINDOW clause: 18 third
  answers of 575 statements). Asks how a qualified side key resolves structurally; governing rule: never worse than
  main, a third answer is an S1.
- [c-4-write-only-role-1-card-2026-10-10.md](c-4-write-only-role-1-card-2026-10-10.md) — **card
  C-4-WRITE-ONLY-ROLE-1 (2026-10-10, filed, not scheduled, from the C-4 measure round, Frontier D13):** a role
  with INSERT but not SELECT cannot reach the write core because C-2's `discover` reads metadata first; Spark's
  JDBC writer also reads metadata before writing, so the card records the parity limitation and asks for no fix.
- [c-4-unmapped-column-write-1-card-2026-10-10.md](c-4-unmapped-column-write-1-card-2026-10-10.md) — **card
  C-4-UNMAPPED-COLUMN-WRITE-1 (2026-10-10, filed, not scheduled, from the C-4 measure round, Frontier D13):** a
  relation with an unmapped column does not resolve even when the write omits it; the follow-up resolves the
  relation with unmapped columns marked unavailable and refuses only when one is named, with live pins on both
  doors.
- [EMPTY-PROJECTION-COUNT-1](empty-projection-count-1-card-2026-10-09.md) — **closed (2026-10-09 by its unit):** `COUNT(*)` planned an empty projection that `conform_batch` refused; the shared fix serves all readers. The MB-4 branch's open copy must reconcile at merge (ledger D-1).
- [ice-tsns-nested-1-card-2026-10-09.md](ice-tsns-nested-1-card-2026-10-09.md) — **card
  ICE-TSNS-NESTED-1 (2026-10-09, open):** store a nested `timestamp_ns` leaf by the session
  wall on every door. Filed by the split of ICE-TSNS-MERGE-WALL-1 (pull request #1018), which
  refuses such a write by name after three verifies failed the nested conform. The card
  holds the starting evidence: the three verifies' tables, the three leak routes, the 700
  planner-error cells, the pairing question (parity row R-016), the Arrow layout list and the
  pins that carried no digit below the microsecond. Sibling: the nested microsecond
  `TIMESTAMP_NTZ` split (R-011).
- [ice-tsns-narrow-refuse-1-card-2026-10-10.md](ice-tsns-narrow-refuse-1-card-2026-10-10.md) — **card
  ICE-TSNS-NARROW-REFUSE-1 (2026-10-10, open, target v1.5.5, one Opus unit):** the owner's rule that a narrowing the user
  wrote stores and a narrowing the analyzer inserted refuses by name (parity row R-017). 480 of 650 top-level cells
  store a value cut to microseconds at main `58bf67e3`; a lineage test that refuses them also refuses written casts that
  store today. Carries the recorded disagreement over three written casts with no `WHERE`. Sibling: ICE-TSNS-COERCION-1.
- [ice-tsns-coercion-1-card-2026-10-10.md](ice-tsns-coercion-1-card-2026-10-10.md) — **card
  ICE-TSNS-COERCION-1 (2026-10-10, open, no release assigned):** nanosecond-aware type coercion so `coalesce`, `nvl`,
  `array`, `CASE ... ELSE NULL` and `if` keep nanoseconds beside a `timestamp_ns` operand (parity row R-017). Turns the
  refusals of ICE-TSNS-NARROW-REFUSE-1 back into stores. Sibling: ICE-TSNS-NARROW-REFUSE-1.
- [ice-ntz-nested-wall-1-card-2026-10-10.md](ice-ntz-nested-wall-1-card-2026-10-10.md) — **card ICE-NTZ-NESTED-WALL-1 (2026-10-10, open, target v1.5.5, own unit):** a nested microsecond `TIMESTAMP_NTZ` field stores the session wall through field assignment (28 cells) and the UTC wall through every other door (106 cells) (parity row R-011). The release note must say that nested NTZ values written by INSERT before the fix hold the UTC wall. Sibling: ICE-TSNS-NESTED-1. **2026-10-20:** the stopgap refusal shipped by U-8 (ICE-NTZ-NESTED-REFUSE-1); the lift is 1.5.7.
- [write-body-carriers-1-card-2026-10-10.md](write-body-carriers-1-card-2026-10-10.md) — **card
  WRITE-BODY-CARRIERS-1 (2026-10-10, build round 1 landed 2026-10-14):** owner ruling 2026-10-10 that `CREATE [OR REPLACE] TABLE
  z AS INSERT INTO t ...` refuses at parse, as Spark does; today it commits the inner INSERT and then fails with
  `UInt64 is not supported`. Measures Spark 4.1.2 on the CTAS shape before the refusal lands. `EXPLAIN ANALYZE <write>`
  and `PREPARE` / `EXECUTE` run the write body, documented with no parity claim. Records 36 of 48 top-level
  `EXPLAIN ANALYZE INSERT` nanosecond cells failing with a DataFusion internal error.
- [ice-tsns-type-planner-1-card-2026-10-13.md](ice-tsns-type-planner-1-card-2026-10-13.md) — **card
  ICE-TSNS-TYPE-PLANNER-1 (2026-10-13, open, target v1.5.6, grade A):** the SQL type name
  decides the unit, so a nanosecond cast means nanoseconds (parity row R-017 primary; R-010,
  R-013, R-018(1) measured against the head; R-009 stays its own unit). Supersedes the parked
  ICE-TSNS-NARROW-REFUSE-1 (#1027) and ICE-TSNS-COERCION-1 (#1042). Sequence: this docs PR,
  step 0 on Muse xhigh, one Opus 5.5 high executor, CI green, one scoped Opus 5.5 high verifier.
- [ns-array-floor-1-card-2026-10-13.md](ns-array-floor-1-card-2026-10-13.md) — **card
  NS-ARRAY-FLOOR-1 (2026-10-13, open, target v1.5.6):** floor a nanosecond datetime array
  to microseconds under `array<timestamp>` and refuse it under `array<timestamp_ntz>`, as
  Spark 4.1.2 measured (parity row R-020). A redesign that replaces draft PR #1047, whose
  integer rule refused a correct microsecond integer and whose hint then stored a wrong
  value. Driven by the dtype, never by integer comparison; integer cells stay as main.
- [ns-nested-pandas-1-card-2026-10-13.md](ns-nested-pandas-1-card-2026-10-13.md) — **card
  NS-NESTED-PANDAS-1 (2026-10-13, open, unscheduled, severity S0 on main):** nanosecond
  values inside struct and map fields returned from `applyInPandas` (parity row R-020).
  Cells from the #1047 verdict and ledger; the array door itself is NS-ARRAY-FLOOR-1.
- [struct-case-twin-1-card-2026-10-14.md](struct-case-twin-1-card-2026-10-14.md) — **card
  STRUCT-CASE-TWIN-1 (2026-10-14, open, unscheduled, not in the closeout):** a BY NAME
  struct write whose source carries two fields differing only in letter case stores
  `[[1,10,20],[2,1,2]]` where Spark 4.1.2 refuses
  `[INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME]` at `` `st`.`a` `` (parity row
  R-021; owner ruling, question 37, 2026-10-14). Format v3 not measured; non-struct
  case twins already covered by ID-1a.
- [ICE-TSNS-UPDATE-NTZ-CAST-1](ice-tsns-update-ntz-cast-1-card-2026-10-15.md) —
  card ICE-TSNS-UPDATE-NTZ-CAST-1 (2026-10-15, filed by U-1's carry-over fix round from Q1 and
  the orchestrator's provisional ruling on it): a written `CAST(ns AS TIMESTAMP_NTZ)` in
  `UPDATE … WHERE` truncates where the no-`WHERE` form floors; outside ruling 91, four dated
  rows on U-1's branch (red on today's main), no code in U-1.
  **2026-10-14:** the measured shape closed by ICE-STRUCT-PAIRING-1 (R-021 CLOSED, cell
  `by_name_case_twins`); the card's wider scope waits on the owner.
- [ns-encoded-nested-floor-1-card-2026-10-14.md](ns-encoded-nested-floor-1-card-2026-10-14.md) —
  **card NS-ENCODED-NESTED-FLOOR-1 (2026-10-14, open, target v1.5.7, nanosecond
  carve-out):** floor a dictionary-encoded or run-end-encoded nanosecond source, and a
  nanosecond leaf in a struct, list or map source, into a microsecond `TIMESTAMP` or
  `TIMESTAMP_NTZ` leaf and read its wall in the session zone, the rule of R-009 (parity row
  R-024; owner ruling, question 53, 2026-10-14). From finding F4 of the verifier of PR
  #1059: same on main in every cell. Reconciles with the R-015 refusal of a nested
  `timestamp_ns` target leaf.
- [ice-commit-rebase-dup-1-card-2026-10-15.md](ice-commit-rebase-dup-1-card-2026-10-15.md) — **card ICE-COMMIT-REBASE-DUP-1 (2026-10-15; fork half done via fork #368 and RePark #1095, pin now `d80a2884`; U-9 drill half open):** the fork's re-base after a CAS rejection can append the same rows twice (a lost reply, an SDK re-send, a stale rejection, then the re-base), from the owner's ruling 84; fork work (refresh and check for our snapshot id before re-basing, SDK retry off on the two commit calls), retired by a pin running the exact sequence with no double append in U-9's live drill.
- [commit-unknown-operation-id-1-card-2026-10-15.md](commit-unknown-operation-id-1-card-2026-10-15.md) — **card COMMIT-UNKNOWN-OPERATION-ID-1 (2026-10-15, open, not scheduled):** thread the fork's minted operation id to the plain `INSERT` unknown-outcome error, which raises with `operation_id = None` today, from the owner's ruling 85; the error text keeps the snapshot-list read-back, retired when the id reaches the error, pinned.
- [facade-hang-98-1-card-2026-10-14.md](facade-hang-98-1-card-2026-10-14.md) — **card
  FACADE-HANG-98-1 (2026-10-14, open, not scheduled):** the facade suite (`pytest -n 8`
  over `python/repark/tests/`) sometimes stalls at 98–99 % with idle xdist workers on CI
  and locally until the job timeout, and a re-run passes; not reproduced on demand. First
  step is a branch run with `--durations=0` and a per-test timeout to name the test that
  never returns; a CI-only change is the owner's. Retires when the named test is fixed or
  marked with a dated row.
- [spill-reach-fair-pool-flake-1-card-2026-10-14.md](spill-reach-fair-pool-flake-1-card-2026-10-14.md) — **card
  SPILL-REACH-FAIR-POOL-FLAKE-1 (2026-10-14, open, not scheduled):**
  `test_sort_merge_join_spills_under_small_fair_pool` (64 MiB fair pool, two 400,000-row
  md5 series, spill count above zero asserted) refuses intermittently on CI with
  `Resources exhausted` from `ExternalSorterMerge` (2026-10-05 shard 2, 2026-10-13 shard
  0; both re-runs passed). Open question: a real spill gap under a fair pool (product)
  or a pool sized too tight for the runner (test). Unmeasured until N repeated runs.
  **2026-10-22 (SPILL-REACH-FAIR-POOL-FLAKE-1):** measured locally; the default pool of the file is now 72 MiB, the runner
  half is unmeasured and the first CI run of the shard is the only runner sample; the card stays open until that read.
  Orchestrator ruling the same day: the default is 80 MiB, not 72 MiB, because the stress table's hang counts decide.
- [fair-pool-hang-under-load-1-card-2026-10-22.md](fair-pool-hang-under-load-1-card-2026-10-22.md) — **card
  FAIR-POOL-HANG-1 (2026-10-22, open, not scheduled):** tests of `test_t2_spill_reach.py` sometimes sit inside the native
  `to_arrow_batches` / `to_arrow` call for minutes instead of one second, alone once and under contention often (step A:
  3 of 30, 2 of 14, 1 of 10, 0 of 14 whole-suite repeats; stress, 200 processes per pool: 18, 10, 1, 1 at 64M, 72M, 80M,
  96M), falling with the pool and not reaching zero at 72M; Python stacks only, no native stack yet. Step 0 for an Opus 5.5
  agent: `gdb` stacks and the CPU-versus-blocked state of a stuck process. May be the stall of FACADE-HANG-98-1. No fix proposed.
- [update-subquery-store-1-card-2026-10-15.md](update-subquery-store-1-card-2026-10-15.md) — **card
  UPDATE-SUBQUERY-STORE-1 (2026-10-15, open, not scheduled):** store Spark's value for
  `UPDATE … SET col = (SELECT …)` where the identity path does not run (`WHERE true`, an `IN`
  list, no `WHERE`, every non-comparison predicate; every `WHERE` on `repark.sql()`), which
  refuse under owner ruling 90(a) at `PreExecute::guard` where they reported success and wrote
  nothing. Names the step-0 causes with `path:line`: the admission in `predicate_dml/plain.rs`,
  `scalar_set_assignments`, the fall-through to `execute_passthrough`, and DataFusion's
  `extract_update_assignments` dropping a decorrelated same-name column as an identity
  assignment. Registry row ICE-STRUCT-PAIRING-1-UPDATE-SUBQUERY; since owner ruling 95
  (2026-10-15) it also retires row ICE-STRUCT-PAIRING-1-UPDATE-SUBQUERY-WHERE-FALSE, the cells
  that match no row.
- [occ-disjoint-merge-flake-1-card-2026-10-14.md](occ-disjoint-merge-flake-1-card-2026-10-14.md) — **card
  OCC-DISJOINT-MERGE-FLAKE-1 (2026-10-14, open; measured 2026-10-18):**
  `test_disjoint_key_merges_refuse_like_spark[3-serializable-dataframe]` committed 2 of
  8 on CI (2026-10-07) where Spark's recording commits 1; losers keep the expected
  `Found conflicting files … matching TRUE` shape. The test fires eight MERGEs on
  disjoint keys with no target-only conjunct through a barrier (ledger clause C-012 of
  the ice-occ-scoped-1 ledger); two winners fit a check-then-commit timing window.
  Open question: product race or test assumption. Unmeasured until N repeated runs.
  Seen again 2026-10-16 as `[3-snapshot-dataframe]` (PR #1085 CI, 2 of 8); on the
  orchestrator's list as a U-9 drill cell (an owner-relayed suggestion, not a ruling).
  Measured 2026-10-18 (unit ledger `occ-disjoint-merge-flake-1-ledger.md`): legitimate
  ordering, not a product race. The commit path cannot land two MERGEs on one base; a
  second winner is a MERGE that loaded its base after the first commit. Spark does the same
  under that schedule. The pin's tolerance waits on an owner ruling.
- [mtime-metadata-pick-1-card-2026-10-14.md](mtime-metadata-pick-1-card-2026-10-14.md) — **card
  MTIME-METADATA-PICK-1 (2026-10-14, open, not scheduled):** every remaining file under
  `python/repark/tests/` that orders or picks Iceberg metadata files by `st_mtime` /
  `st_mtime_ns`, with path, line and whether the use picks a metadata file (the
  `f8f1d252` flake shape) or does something else. Proposed fix is one shared helper
  that picks by the version number in the file name, as `f8f1d252`'s
  `_metadata_version` does. Retires when every listed use is converted.
- [mb-fence-body-flake-1-card-2026-10-18.md](mb-fence-body-flake-1-card-2026-10-18.md) — **card MB-FENCE-BODY-FLAKE-1 (2026-10-18, fixed in the unit's PR, one open question):** `two_drivers_on_one_sink_land_each_epoch_exactly_once` failed on CI (run 39157958057, main's tree) with `left: [0, 0, 1, 2]`: a fenced driver's `foreachBatch` body ran epoch 0. Cause: `enter_scope` loaded the sink before it entered the sink's scope, against MB-3 D-19. A deterministic pin reproduces it; the fix enters the scope first. Open: the same pin also misses its 10 s bound on a slow disk (the PA-15 family), measured to `fsync`; the owner keeps the bound or rules otherwise.
- [parity-live-drift-1-card-2026-10-20.md](parity-live-drift-1-card-2026-10-20.md) — **card
  PARITY-LIVE-DRIFT-1 (2026-10-20, open, not scheduled):** the `parity-live` nightly is red every
  morning since 2026-10-14 on one test, `test_ice_meta_delete_1.py::test_live_oracle_fixture_reproduces`:
  the ICE-META-DELETE-1 recorder re-runs Spark and 52 of 72 cells drift from the committed fixture
  in data-file counts (first `all_rows_nopred_v2_cow`). Two candidate causes: a Spark behaviour
  change to re-record with a dated note, or a recorder defect (a `local[1]` `getOrCreate` that may
  reuse a `local[2]` session, a lead by reading). The 2026-10-20 plan's PARITY-LIVE-RERECORD-1,
  gate D5/P5 two green mornings; awaits the owner's adopt.
- [write-spine-1-card-2026-10-22.md](write-spine-1-card-2026-10-22.md) — **card WRITE-SPINE-1
  (2026-10-22, open, scheduled after GATE-RETIRE-1):** one conform stage every write door passes
  through (zone rule, nanosecond floor, struct pairing applied once by construction), the
  conversion helpers in a crate both `repark-functions` and `repark-iceberg` can call, and a door
  matrix enumerated from the code; filed on the owner's word after ruling ZS-9 measured the nested
  NTZ store as three sites that cannot call the seam helper. Carries the 2026-10-22 process rule: a
  step-0 door-by-shape census in every write-path order and a "doors I did not touch" hand-back line.
  Adopted 2026-10-22 as option D of the crate-boundary evaluation (no new crate, no new edge);
  WRITE-NTZ-TO-LTZ-NESTED-1 folded into its step (2).
- [FORK-NS-UNIT-FAIL-CLOSED-1](fork-ns-unit-fail-closed-1-card-2026-10-23.md) — **card (2026-10-23, open, clerk grade, next fork repin after RP-60):** the fork's record-batch writer refuses an Arrow timestamp unit that is not the column's unit, before any file is written; the fork half of the owner's 2026-10-23 nanosecond ruling (the engine half is WRITE-SPINE-1 step 2).
- [container-cast-zone-1-card-2026-10-20.md](container-cast-zone-1-card-2026-10-20.md) — **card
  CONTAINER-CAST-ZONE-1 (2026-10-20, open, not scheduled):** a `CAST`/`TRY_CAST` of a struct,
  array or map holding a zoned timestamp leaf, and `to_json` of one, render the leaf in UTC
  instead of the session zone for every target type (`TIMESTAMP_NTZ`, `STRING`, `DATE`,
  `BIGINT`), in a plain `SELECT`, in every write and in the six table-creating writers, on main
  `c516c54a`; the leaf cast is session-aware. Found by U-8's second verifier; U-8's gate does not
  see a value that passed through another type first. Owner rules: grow U-8, or fix the cast as
  its own unit (the orchestrator's lean).
- [write-ntz-to-ltz-nested-1-card-2026-10-22.md](write-ntz-to-ltz-nested-1-card-2026-10-22.md) —
  **card WRITE-NTZ-TO-LTZ-NESTED-1 (2026-10-22, open; the next write-path Opus unit after
  GATE-RETIRE-1; rulings ZS-8, ZS-9):** the Iceberg write path stores a nested `TIMESTAMP_NTZ`
  value into a nested `TIMESTAMP` field as if the wall were UTC, at three sites (the sink's
  `conform` cast with no session zone in scope, `update_cast.rs`'s `arrow_cast` text, the MERGE
  `INSERT` store wrap); `repark-iceberg` depends only on `repark-common`. Fix shape: one
  zone-aware store conversion threaded through the write path; ZONE-SEAM-1's refusal retires.
  WRITE-SPINE-1 would make it a one-site fix; the owner picks the order. *2026-10-22:* folded
  into WRITE-SPINE-1 step 2 (owner, on the crate-boundary evaluation's recommendation).
  *2026-10-23:* the cells line reads 37 Spark-equal + 79 DIFFERENT + 18 engine error per zone.
- [zone-horizon-instant-1-card-2026-10-22.md](zone-horizon-instant-1-card-2026-10-22.md) — **card
  ZONE-HORIZON-INSTANT-1 (2026-10-22, open; the next Opus unit after GATE-RETIRE-1; owner ruling
  ZS-7):** DataFusion-Spark's `to_utc_timestamp` / `from_utc_timestamp` of an instant, shared by
  main and ZONE-SEAM-1's head, misses Spark 4.1.2 at DST gaps and overlaps and after 2099 (19 and
  10 of 36 measured edge cells). The 180-cell edge table on Spark, head and main; the 102
  instant-argument residue cells of ZONE-HORIZON-RENDER-1 and the three ZS-7 cells it would
  close; the fallback ZS-7 did not take, with its cost. Names the kernel only, no fix.
- [MAP-COLUMN-PANIC-1](map-column-panic-1-card-2026-10-20.md) — **card (merged PR #1131 `d6ee0411`; PR #1134 settled `REPARK_PANIC_FATAL`; native-door follow-up open):** `map('a', <column>)` panicked on every door; evidence under [`task/wo/map-column-panic-1/`](../../wo/map-column-panic-1/map.md).
- [lateral-correlated-memory-1-card-2026-10-21.md](lateral-correlated-memory-1-card-2026-10-21.md) —
  **card LATERAL-CORRELATED-MEMORY-1 (2026-10-21, open, not scheduled; the owner schedules):** a
  `LATERAL` subquery with its own `FROM` and a predicate on the outer column (`LATERAL (SELECT
  a.c AS s FROM ice.ns.src b WHERE b.id = a.id)`) over a one-row table grows past 100 GB
  resident before any result on main `c516c54a` (112 GB measured by U-8's verifier; stopped by
  the OOM killer at a 40 G scope cap after 166 s in fix round 3). It predates U-8 and is not its
  gate. Probes of this shape run under a memory cap.
- [ICE-RESOURCE-BOUND-1](ice-resource-bound-1-card-2026-10-20.md) — **card
  ICE-RESOURCE-BOUND-1 (U-11, 2026-10-20, built on the branch 2026-10-26, verifiers passed on head 76bd5891, PR #1191 open for the merge):** a stated resource bound for ordered writes (PP-01) and merge-on-read MERGE
  (PP-02), measured on the acceptance sizes and written in docs; above it the statement refuses
  with one text and the old snapshot stands, under #1044's "refuses cleanly where it cannot
  spill"; PA-10 and PA-11 declared or closed. Follows U-9, whose kill pin keeps a crash and a
  refusal two pinned outcomes. Order under `task/wo/ice-resource-bound-1/`.
- [WRITE-DOOR-CLASSIFIER-D8-D9-1](write-door-classifier-d8-d9-1-card-2026-10-26.md) — **card WRITE-DOOR-CLASSIFIER-D8-D9-1 (2026-10-26, open, clerk grade):** the static door classifier answers D8 for a MERGE `UPDATE SET t.a = s.a` where the per-session runtime record answers D9, found by the ICE-RESOURCE-BOUND-1 step 0 probe; retires when the two agree under a pin; linked to WRITE-SPINE-1.
- [PA-10-CONSTRAINED-PROCESS-TEST-1](pa-10-constrained-process-test-1-card-2026-10-26.md) — **card PA-10-CONSTRAINED-PROCESS-TEST-1 (2026-10-26, open, not scheduled, clerk grade):** a disposable constrained process under a nested memory cap proves the default pool follows the cap; the O-13 acceptance-only residue of PA-10 (ICE-RESOURCE-BOUND-1 Q-C7). The fixture cells are in `repark-core` `spill.rs`.
- [PA-10-HEADROOM-DOC-1](pa-10-headroom-doc-1-card-2026-10-26.md) — **card PA-10-HEADROOM-DOC-1 (2026-10-26, open, not scheduled, clerk grade):** a dated, measured document of the memory outside the pool for decoded batches, Arrow results, caches, writer buffers and several sessions, which the PA-10 row requires.
- [STACK-LINEAR-FLAKE-1](stack-linear-flake-1-card-2026-10-15.md) — **card STACK-LINEAR-FLAKE-1 (2026-10-15, open, not scheduled):** `test_stack_is_linear_in_columns` failed once on CI (exponent 1.128 over the 1.1 bound at millisecond medians, about 380x under the 21.5 s bridge wall; ledger clause C-002 of perf-unpivot-1): loaded-runner noise or a real slope change, unmeasured until N quiet runs.
- [update-where-binary-flake-1-card-2026-10-20.md](update-where-binary-flake-1-card-2026-10-20.md) — **card UPDATE-WHERE-BINARY-FLAKE-1** (2026-10-20, open, not scheduled): `UPDATE … WHERE` after a mixed set of insert doors including a `BINARY` literal fails about one time in four on main `c516c54a` with `Mismatch between schema and batches`; found by the U-8 verifier, outside U-8; lead is a per-door Arrow field difference on the `BINARY` column.
- [VALUES-ALIAS-COLUMNS-1](values-alias-columns-1-card-2026-10-23.md) — **card VALUES-ALIAS-COLUMNS-1 (2026-10-23, open, not scheduled):** `MERGE … USING (VALUES …) AS s(id, name)` fails `UNRESOLVED_COLUMN` on `s.id` where Spark 4.1.2 refuses at parse with `[COLUMN_ALIASES_NOT_ALLOWED]`; found in the U/X first live run. Retires when RePark refuses that text at parse with Spark's message.
- [NTZ-CAST-OFFSET-TEXT-UPDATE-1](ntz-cast-offset-text-update-1-card-2026-10-23.md) — **card NTZ-CAST-OFFSET-TEXT-UPDATE-1 (2026-10-23, open, not scheduled):** a plain `UPDATE` that assigns a `TIMESTAMP_NTZ` cast of offset-bearing text honours the offset where Spark 4.1.2 ignores it; `INSERT` and `SELECT` agree with Spark. Names the measured cells; proposes no fix.
- [AWS-EPHEMERAL-RUNS-1](aws-ephemeral-runs-1-card-2026-10-24.md) — **card AWS-EPHEMERAL-RUNS-1 (2026-10-24, ruled, not scheduled):** the owner's cross-read rulings: run-id table prefixes, one shared wheel build, a delete grant scoped to the `testing_*` tables; C-060 is superseded by C-066 and C-067 (REJECTED, measured); C-068 to C-070 are OPEN.
- [CONTAINER-CAST-SHAPE-1](container-cast-shape-1-card-2026-10-25.md) — **card CONTAINER-CAST-SHAPE-1 (filed 2026-10-25, grade C):** a whole-container cast whose fields do not line up with the target stores in UTC and errors in Spark, outside the R-011 gate (pre-existing on main); R-011's arm stays for these shapes until the card retires. Leaves when the owner charters it or declines it.
- [NESTED-TEXT-LEAF-STORE-1](nested-text-leaf-store-1-card-2026-10-25.md) — **card NESTED-TEXT-LEAF-STORE-1 (filed 2026-10-25, grade C):** a text leaf into a nested TIMESTAMP stores the UTC reading where Spark refuses CANNOT_SAFELY_CAST (102 census cells, D3, D4 and D10); the conform stage excludes text leaves. Leaves when the owner charters it or declines it.
- [NESTED-LITERAL-SEAM-1](nested-literal-seam-1-card-2026-10-25.md) and [NESTED-MAP-KEY-ZONE-1](nested-map-key-zone-1-card-2026-10-25.md) — **closed (2026-10-27; the second by NESTED-LITERAL-SEAM-1, row C-004):** the nested literal and constant cells store the session midnight; [ledger](../../ledgers/staging/nested-literal-seam-1-ledger.md); C-012 stays OPEN for step (4).
- [NATIVE-DOOR-ZONE-RULING-1](native-door-zone-ruling-1-card-2026-10-25.md) — **card NATIVE-DOOR-ZONE-RULING-1 (filed 2026-10-25, grade C):** D19 and D20 take the Spark zone rule through the stage while D14 to D16 keep the UTC reading; the owner owes a ruling (WS2-16). Leaves on that ruling.
- [NATIVE-DOOR-REPLACE-KILL-1](native-door-replace-kill-1-card-2026-10-27.md) — **card (filed 2026-10-27, grade C):** the native door's replace publish (`crates/repark-sql/src/create_table.rs`, about line 332) has no kill-mid-write cell; the Spark door's replace is pinned by ICE-REPLACE-KILL-DOORS-1. Leaves when a kill cell covers it. That path measured wrong 2026-11-12.
- [NATIVE-CTAS-ICEBERG-REFUSE-1](native-ctas-iceberg-refuse-1-card-2026-11-12.md) — **card (2026-11-12, S1, FIXED as a refusal):** `repark.sql()` CTAS into Iceberg stored zero rows with success, and `OR REPLACE` dropped an existing table (F4); now it refuses first. Real native CTAS: the routing decision.
- [PARTITION-PAIRING-CARVE-OUT-CLOSE-1](partition-pairing-carve-out-close-1-card-2026-10-25.md) — **card PARTITION-PAIRING-CARVE-OUT-CLOSE-1 (filed 2026-10-25, grade C):** plan-time pairing on the PARTITION overwrite site stored Spark's rows on 106 of 106 carve-out cells (ice-struct-pairing-1 C-031) before fix round 1; step 2 restored the refusal because ruling 58(a) stands. Leaves when the owner rules on the carve-out.
- [SHOW-FUNCTIONS-1](show-functions-1-card-2026-10-26.md) — **card SHOW-FUNCTIONS-1 (filed 2026-10-26, clerk grade):** bare `SHOW FUNCTIONS` and `information_schema.routines` fail on both Python doors (`datafusion.information_schema.parameters` and `spark_catalog.information_schema.routines` not found), so registry row R-013 cannot be measured. Leaves when R-013 closes.
- [DEPS-1](deps-1-card-2026-10-26.md) — **card DEPS-1 (filed 2026-10-26, open, clerk grade):** the zstd 0.14 bump waits on upstream: RUSTSEC-2026-0391, -0398 and -0403 are ignored in `deny.toml` and `.cargo/audit.toml` while nine dependents pin the 0.13 line. Trigger: the datafusion release on arrow 60, then the fork repin, then the bump with the ignores removed. Leaves when that PR merges.
- [DESCRIBE-NS-NAMES-2](describe-ns-names-2-card-2026-10-26.md) — **card DESCRIBE-NS-NAMES-2 (filed 2026-10-26, clerk grade):** the remaining DESCRIBE surfaces (`DESCRIBE <table> <column>`, the partition section, nested struct, array and map leaves, `DESCRIBE TABLE` on a view) still show microsecond names under R-001; one renderer seam, not scheduled. Leaves when the R-001 residue line is empty.
- [META-DELETE-DATE-LITERAL-1](meta-delete-date-literal-1-card-2026-10-27.md) — **card META-DELETE-DATE-LITERAL-1 (filed 2026-10-27, P3, clerk grade):** the metadata-delete door translates no DATE literal, so a whole-month DATE delete writes row-level position deletes where Spark deletes the data file; the result is correct. Leaves when the owner charters it or declines it.
- [META-TABLE-UNQUOTED-READ-1](meta-table-unquoted-read-1-card-2026-10-27.md) — **card META-TABLE-UNQUOTED-READ-1 (filed 2026-10-27, P3, not dispatched):** an unquoted current-snapshot `cat.ns.t.<meta>` read fails to parse on the Rust `ReparkSession` Spark door, while the quoted and `VERSION AS OF` spellings answer; the failure is the same at both RP-60 pins, so it is not an RP-60 regression. Leaves when the owner charters it or declines it.
- [DELETE-TARGET-ALIAS-1](delete-target-alias-1-card-2026-10-27.md) — **card DELETE-TARGET-ALIAS-1 (filed 2026-10-27, P3, not dispatched):** `DELETE FROM t AS x WHERE x.col ...` fails with `UNRESOLVED_COLUMN` on both RP-60 pins while Spark accepts the alias (measured by the Opus 5.5 verifier in RP-60's round 1, outside that unit). Leaves when the owner charters it or declines it.
- [F-POSDEL-BATCH-1](f-posdel-batch-1-card-2026-10-27.md) and [RP-60-POSDEL-REPIN-1](rp-60-posdel-repin-1-card-2026-10-27.md) — **closed (PR #1200):** a large DELETE panicked (`offset overflow`); the memory side is the named follow-up.
- [f-eqdel-convert-batch-1-card-2026-10-27.md](f-eqdel-convert-batch-1-card-2026-10-27.md) — **fork card F-EQDEL-CONVERT-BATCH-1 (filed 2026-10-27, P3, not dispatched):** `convert_equality_delete_files` writes one `Utf8` array per equality delete and panics with `offset overflow` past `i32::MAX` bytes of path text at fork `ea0b7741`; the fix shares the F-POSDEL-BATCH-1 chunking. Plus three Parquet-only delete opens (2026-10-28), one dispatch. Leaves when the owner charters it or declines it.
- [NATIVE-DOOR-POST-PUBLISH-1](native-door-post-publish-1-card-2026-10-28.md) — **card NATIVE-DOOR-POST-PUBLISH-1 (filed 2026-10-28, grade C; measured 2026-10-29 by the second audit, AUD-011):** the native door's 13 post-commit refresh sites call the old invalidation and report a committed change as failed (the AUD-002 class); CTAS, TRUNCATE and ALTER are measured; the fix is the after-commit variant, pinned per family. Leaves when every site calls it.
- [TEMP-VIEW-OVER-RENAMED-TABLE-1](temp-view-over-renamed-table-1-card-2026-11-02.md) — **card (filed 2026-11-02, grade C, P3, open):** a view over a temporary view reads the table on Spark after the name is renamed or dropped; RePark refuses, loud, no wrong value. Leaves when pinned or declined.
- [TEMP-VIEW-WRITE-REFUSAL-SQLSTATE-SPACING-1](temp-view-write-refusal-sqlstate-spacing-1-card-2026-11-02.md) — **card (filed 2026-11-02, grade C, P3, open):** the AUD-006 write refusals print one space before `SQLSTATE`, where Spark 4.1.2 prints two. Leaves when pinned or declined.
- [alter-table-identifier-clause-parse-1-card-2026-11-02.md](alter-table-identifier-clause-parse-1-card-2026-11-02.md), [alter-table-keyword-view-name-1-card-2026-11-02.md](alter-table-keyword-view-name-1-card-2026-11-02.md), [alter-rename-quoted-destination-1-card-2026-11-02.md](alter-rename-quoted-destination-1-card-2026-11-02.md), [line-comment-carriage-return-1-card-2026-11-02.md](line-comment-carriage-return-1-card-2026-11-02.md) and [drop-view-if-exists-over-table-1-card-2026-11-02.md](drop-view-if-exists-over-table-1-card-2026-11-02.md) — **cards (2026-11-02, grade C, P3, open):** five parser and DDL residues. Leave when pinned or declined.
- [QUOTED-TARGET-NO-VIEW-1](quoted-target-no-view-1-card-2026-11-02.md), [UNICODE-WORD-IDENTIFIER-1](unicode-word-identifier-1-card-2026-11-02.md) and [DROP-TARGET-TRAILING-COMMENT-1](drop-target-trailing-comment-1-card-2026-11-02.md) — **cards (2026-11-02, each folded into IDENT-DIALECT-1 on 2026-11-02):** in order, class 1 with no view in scope, S1 on the deleting heads; class 2; class 4, lead L13.
- [IDENT-DIALECT-1](ident-dialect-1-card-2026-11-02.md) and [IDENT-DIALECT-1B](ident-dialect-1b-card-2026-11-02.md) — **closed (1B on 2026-11-03):** the quoted-segment, non-ASCII-word and over-refusal classes (folds four cards above); 1B's relation after a comma that follows a VALUES row list or a LATERAL subquery, ruled out by owner ruling A.
- [IDENT-DIALECT-2](ident-dialect-2-card-2026-11-02.md) — **card IDENT-DIALECT-2 (2026-11-02, grade C, P2, open):** identifier positions outside the head target and the FROM, JOIN and comma relations, including the from-first forms (S3 over-refusals). Residue rows from IDENT-DIALECT-1B are appended.
- [ALTER-VIEW-RENAME-OVER-TABLE-1](alter-view-rename-over-table-1-card-2026-11-02.md) — **card ALTER-VIEW-RENAME-OVER-TABLE-1 (filed 2026-11-02, lead L14, open):** plain `ALTER VIEW t RENAME TO t2` over a same-named table refuses where Spark 4.1.2 renames the view; held. Leaves when pinned or declined.
- [SPARK-DOOR-TEMP-VIEW-TARGET-1](spark-door-temp-view-target-1-card-2026-10-29.md) — **closed (merged 2026-10-30):** a one-part DML target wrote the same-named table (AUD-006),
- [SPARK-DOOR-TEMP-VIEW-DDL-1](spark-door-temp-view-ddl-1-card-2026-11-01.md) — **card SPARK-DOOR-TEMP-VIEW-DDL-1 (2026-11-01, grade A, P1, Critical; dispatched, not merged):** `ALTER TABLE` through a view name changes the same-named table (AUD-015, a label blocker). Leaves when every ALTER kind refuses as Spark does.
- [MAP-STRUCT-KEY-SUBSCRIPT-1](map-struct-key-subscript-1-card-2026-11-01.md) — **card MAP-STRUCT-KEY-SUBSCRIPT-1 (2026-11-01, grade A, P1, High; dispatched, not merged):** a map subscript with a struct key panics where Spark answers (AUD-016, a label blocker). Leaves when the subscript answers Spark's values.
- [TEMP-VIEW-SURFACE-LEADS-1](temp-view-surface-leads-1-card-2026-10-29.md) — **card TEMP-VIEW-SURFACE-LEADS-1 (filed 2026-10-29, grade C, open):** three class and text leads from the SPARK-DOOR-TEMP-VIEW-TARGET-1 probe: `SHOW VIEWS` lists internal `__repark_cdf_*` scratch views, and two names refuse with other text or class. None changes a table. Items 2 and 3 overlap [TEMP-VIEW-NAME-VALIDATION-1](temp-view-name-validation-1-card-2026-10-30.md). Leaves when each matches Spark 4.1.2 or is declined.
- [TEMP-VIEW-NAME-VALIDATION-1](temp-view-name-validation-1-card-2026-10-30.md) — **card TEMP-VIEW-NAME-VALIDATION-1 (filed 2026-10-30, grade C, open, awaiting the owner's charter):** two name-level differences from Spark 4.1.2: an unquoted digit-led name is refused where Spark accepts it, and the `createOrReplaceTempView` name check disagrees with Spark's parser both ways. Loud or harmless; no table changes. Leaves when pinned to Spark's parser or declined.
- [DDL-OVER-TEMP-VIEW-1](ddl-over-temp-view-1-card-2026-10-30.md) — **card DDL-OVER-TEMP-VIEW-1 (filed 2026-10-30, grade C, open, awaiting the owner's charter):** `CREATE OR REPLACE TABLE t AS SELECT` and `DROP TABLE t` with a temporary view `t` in scope, both outside the six write routes that SPARK-DOOR-TEMP-VIEW-TARGET-1 refuses; Spark measured both shapes equal on 2026-11-01. Leaves when both shapes match Spark 4.1.2 on the probe and are pinned, or the charter declines them.
- [ICE-HADOOP-HINT-WINDOW-1](ice-hadoop-hint-window-1-card-2026-10-22.md), [ICE-HADOOP-GZIP-METADATA-1](ice-hadoop-gzip-metadata-1-card-2026-10-29.md), [HADOOP-HINT-CATALOG-STYLE-NAME-1](hadoop-hint-catalog-style-name-1-card-2026-11-01.md), [HADOOP-HINT-UNREADABLE-FALLBACK-1](hadoop-hint-unreadable-fallback-1-card-2026-11-01.md), [HADOOP-HINT-NEGATIVE-1](hadoop-hint-negative-1-card-2026-10-29.md), [HADOOP-DISCOVERY-RULE-1](hadoop-discovery-rule-1-card-2026-11-03.md), [HADOOP-HINT-MISSING-VERSION-TEXT-1](hadoop-hint-missing-version-text-1-card-2026-11-03.md), [F-STRUCT-EMPTY-CHILD-PANIC-1](f-struct-empty-child-panic-1-card-2026-10-29.md), [F-IDLESS-STRUCT-CHILD-REMAP-1](f-idless-struct-child-remap-1-card-2026-10-31.md), [F-IDLESS-TOPLEVEL-POSITIONAL-1](f-idless-toplevel-positional-1-card-2026-11-01.md) and [RP-65-IDLESS-REPIN-1](rp-65-idless-repin-1-card-2026-11-03.md) — **closed (2026-10-28 to 2026-11-03):** a commit's window on `type=hadoop` answered the older version (AUD-001, PR #1205); gzip metadata names no longer return a deleted row or hide a current file (AUD-007, AUD-010, PR #1218); the hadoop group by PR #1251; the id-less residues (AUD-008, S-11, S-16, S-20, S-26), leads L1 and L2 in the RP-65 card; [ledger](../../ledgers/completed/hadoop-discovery-rule-1-ledger.md).
- [f-map-key-drop-alter-1-card-2026-10-31.md](f-map-key-drop-alter-1-card-2026-10-31.md) — **fork card F-MAP-KEY-DROP-ALTER-1 (filed 2026-10-31, grade C, P3, not dispatched):** the fork accepts `drop m.key.a` where Spark refuses the ALTER (S-14); the read is correct. RePark refuses at both doors since MAP-KEY-ALTER-1 (2026-11-04).
- [MAP-KEY-ALTER-1](map-key-alter-1-card-2026-11-04.md), [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md) and [STAGED-PUBLISH-CLEANUP-1](staged-publish-cleanup-1-card-2026-11-04.md) — **closed (2026-11-04: PRs #1256, #1259 and #1261):** every ALTER and union under a map key refuses as Spark does ([ledger](../../ledgers/completed/map-key-alter-1-ledger.md)); the Spark door answers a field after a subscript, call or parenthesis ([ledger](../../ledgers/completed/subscript-dot-access-1-ledger.md)).
- [DF-APPEND-MAP-KEY-UNION-1](df-append-map-key-union-1-card-2026-11-04.md) — **card DF-APPEND-MAP-KEY-UNION-1 (2026-11-04, S3, not dispatched):** a DataFrame append that differs only inside a map key skips the union; the owner decides the scope.
- [ICE-CREATE-MAP-KEY-FIELD-IDS-1](ice-create-map-key-field-ids-1-card-2026-11-04.md) — **card ICE-CREATE-MAP-KEY-FIELD-IDS-1 (2026-11-04, S3, not dispatched):** CREATE numbers a map key's struct children before the value id, so refusal texts carry other ids.
- [MAP-KEY-REFUSAL-TEXTS-1](map-key-refusal-texts-1-card-2026-11-04.md) — **card MAP-KEY-REFUSAL-TEXTS-1 (2026-11-04, S3, not dispatched):** refusal texts and parse refusals around map-key fields that differ from Spark's (leads L4 to L10).
- [SUBSCRIPT-DOT-ACCESS-NATIVE-1](subscript-dot-access-native-1-card-2026-11-04.md) — **card SUBSCRIPT-DOT-ACCESS-NATIVE-1 (2026-11-04, not dispatched):** the native door; wiring it now writes wrong 1-based answers (D5 first).
- [STAGED-PUBLISH-OPTIONS-PATH-1](staged-publish-options-path-1-card-2026-11-04.md) — **card STAGED-PUBLISH-OPTIONS-PATH-1 (2026-11-04, S3, open).**
- [DEREFERENCE-FORMS-2](dereference-forms-2-card-2026-11-04.md) — **card DEREFERENCE-FORMS-2 (2026-11-04, not dispatched):** `array(...).a`, `struct(...).col1`, a lambda variable's field.
- [AVRO-NULL-COLLECTION-ROUNDTRIP-1](avro-null-collection-roundtrip-1-card-2026-11-04.md) — **card AVRO-NULL-COLLECTION-ROUNDTRIP-1 (2026-11-04, S2, not dispatched):** a null Avro map or list reads back empty.
- [AVRO-MAP-ENTRY-ORDER-1](avro-map-entry-order-1-card-2026-11-04.md) — **card AVRO-MAP-ENTRY-ORDER-1 (2026-11-04, S3, not dispatched):** Spark-written Avro map entries read in another order, run to run.
- [ADD-FILES-IDLESS-PROMOTION-1](add-files-idless-promotion-1-card-2026-11-01.md) — **card ADD-FILES-IDLESS-PROMOTION-1 (filed 2026-11-01, grade C, P3, not dispatched):** `add_files` refuses a promotable id-less file (int into BIGINT) with an internal message where Spark admits it.
- [ADD-FILES-IDLESS-DECIMAL-BOUNDS-1](add-files-idless-decimal-bounds-1-card-2026-11-03.md) — **card ADD-FILES-IDLESS-DECIMAL-BOUNDS-1 (2026-11-03, grade C, P3, open, Spark unmeasured):** `add_files` refuses an id-less DECIMAL(12,2) file holding an out-of-precision value under a DECIMAL(10,2) table when column-bounds metrics are on; the fork ledger's S-27 record says Spark admitted the shape.
- [element-at-struct-key-shape-1-card-2026-11-02.md](element-at-struct-key-shape-1-card-2026-11-02.md) — **MAP-STRUCT-KEY-SUBSCRIPT-1 residues (2026-11-02, grade C, P3, not dispatched):** ELEMENT-AT-STRUCT-KEY-SHAPE-1; [MAP-SUBSCRIPT-PER-ROW-SCALAR-KEY-1](map-subscript-per-row-scalar-key-1-card-2026-11-02.md); [HADOOP-FACADE-FRESH-SESSION-1](hadoop-facade-fresh-session-1-card-2026-11-02.md) (design); [SUBSCRIPT-DOT-ACCESS-1](subscript-dot-access-1-card-2026-11-02.md); [MAP-CONSTRUCTOR-MAP-KEY-REFUSAL-1](map-constructor-map-key-refusal-1-card-2026-11-02.md); [STRUCT-KEY-CAST-OVERFLOW-TEXT-1](struct-key-cast-overflow-text-1-card-2026-11-02.md); [CAST-DOUBLE-TO-DECIMAL-ROUNDING-1](cast-double-to-decimal-rounding-1-card-2026-11-02.md); [FEXPR-SUBSCRIPT-AGGREGATE-CONTEXT-1](fexpr-subscript-aggregate-context-1-card-2026-11-02.md); [SCALAR-FLOAT-KEY-SIGNED-ZERO-NAN-1](scalar-float-key-signed-zero-nan-1-card-2026-11-02.md); [CAST-INFINITY-TO-DECIMAL-NULL-1](cast-infinity-to-decimal-null-1-card-2026-11-02.md); [MAP-CONSTRUCTOR-FLOAT-DUPLICATE-KEY-1](map-constructor-float-duplicate-key-1-card-2026-11-02.md).
- [ANSI-OFF-LEGACY-CAST-1](ansi-off-legacy-cast-1-card-2026-11-02.md) — **card ANSI-OFF-LEGACY-CAST-1 (2026-11-02, grade B):** ANSI-off array-element casts of map keys: the 39 domain-table cells, one binary-to-string cell, and the F2 and R5-1 cells. Leaves when they answer as Spark does, pinned per door.
- [TEMP-VIEW-SESSION-SETTINGS-1](temp-view-session-settings-1-card-2026-11-02.md) — **card TEMP-VIEW-SESSION-SETTINGS-1 (2026-11-02, grade not set, not dispatched):** a temp view is re-analysed under the reader's SQL settings, where Spark keeps the creator's. Leaves when the view cells answer as Spark does.
- [IF-BINARY-NULL-COERCION-1](if-binary-null-coercion-1-card-2026-11-02.md) — **card IF-BINARY-NULL-COERCION-1 (2026-11-02, grade not set, not dispatched):** `IF` with a BINARY and a NULL branch fails RePark's coercion; the `conf.set` whitespace note. Leaves when both answer as Spark does.
- [f-nested-type-mismatch-embedded-ids-1-card-2026-11-01.md](f-nested-type-mismatch-embedded-ids-1-card-2026-11-01.md) — **fork card F-NESTED-TYPE-MISMATCH-EMBEDDED-IDS-1 (filed 2026-11-01, grade B, P2, not dispatched):** on a Parquet file with embedded field ids an incompatible nested type reads a silent NULL struct where the id-less route refuses (S-23).
- [CATALOG-NAMESPACE-PRECEDENCE-1](catalog-namespace-precedence-1-card-2026-10-29.md) — **card CATALOG-NAMESPACE-PRECEDENCE-1 (filed 2026-10-29, grade C):** a two-part name whose first part is also a catalog name resolves to a namespace table on the Spark door, where Spark refuses (AUD-009, Low). Leaves when the name resolves as Spark resolves it on every route.
- [f-maint-avro-orc-delete-residue-1-card-2026-10-29.md](f-maint-avro-orc-delete-residue-1-card-2026-10-29.md) — **fork card F-MAINT-AVRO-ORC-DELETE-RESIDUE-1 (filed 2026-10-29, grade C):** `rewrite_data_files` leaves an Avro or ORC delete file that Spark removes, and `rewrite_position_delete_files` skips Avro and ORC position deletes (AUD-013, Low; the F-EQDEL-CONVERT-BATCH-1 second item). Leaves when both procedures match Spark's delete-file count.
- [ICE-HADOOP-METADATA-CLEANUP-1](ice-hadoop-metadata-cleanup-1-card-2026-10-29.md) — **card ICE-HADOOP-METADATA-CLEANUP-1 (filed 2026-10-29, grade C):** the delete-after-commit and previous-versions-max properties are ignored on RePark's hadoop-type writes (AUD-014, Low). Leaves when the commit keeps the versions Spark keeps.
- [hadoop-hint-unicode-digits-1-card-2026-11-03.md](hadoop-hint-unicode-digits-1-card-2026-11-03.md), [hadoop-metadata-dir-unlistable-1-card-2026-11-03.md](hadoop-metadata-dir-unlistable-1-card-2026-11-03.md) — **cards (2026-11-03, S3):** Unicode-digit hint (accepted leniency); unlistable metadata directory (open).
- [HADOOP-HINT-ZERO-TEXT-1](hadoop-hint-zero-text-1-card-2026-11-03.md) — **card HADOOP-HINT-ZERO-TEXT-1 (2026-11-03, grade C, S3):** a zero hint refuses with RePark's text; Spark's is TABLE_OR_VIEW_NOT_FOUND (84 cells). Leaves when pinned.
- [ICE-HADOOP-GZIP-CODEC-PROPERTY-1](ice-hadoop-gzip-codec-property-1-card-2026-11-03.md) — **card ICE-HADOOP-GZIP-CODEC-PROPERTY-1 (2026-11-03, grade C, S3):** under the gzip codec RePark writes plain `v<N>.metadata.json` names where Java writes the gzip name; reads stay equal. Leaves when the charter rules.
- [TEMP-VIEW-NAME-UNICODE-FOLD-1](temp-view-name-unicode-fold-1-card-2026-11-01.md) — **card TEMP-VIEW-NAME-UNICODE-FOLD-1 (2026-11-01, grade C, P3, not dispatched):** a view name with a letter newer than the JVM's Unicode folds on RePark and not on Spark (AUD-019, Low). Leaves when the fold matches Spark on the oracle JVM.
- [NATIVE-DOOR-DELETE-FORMAT-1](native-door-delete-format-1-card-2026-10-28.md), [ICE-DELETE-FORMAT-GUARD-1](ice-delete-format-guard-1-card-2026-10-28.md), [F-POSDEL-FORMAT-READ-1](f-posdel-format-read-1-card-2026-10-28.md) and [RP-61-DELETE-FORMAT-REVERT](rp-61-delete-format-revert-card-2026-10-28.md) — **all closed (PR #1213; PR #1206, reverted by RP-61; fork PR #375; PR #1209):** AUD-003; RP-61 restores the inherited position-delete format.
- [ICE-POST-PUBLISH-OUTCOME-1](ice-post-publish-outcome-1-card-2026-10-28.md) — **card ICE-POST-PUBLISH-OUTCOME-1 (filed 2026-10-28, Medium, dispatched 2026-10-28 on an Opus 5.5 lane shared with ICE-DELETE-FORMAT-GUARD-1):** a `CREATE TABLE … AS SELECT` whose post-publish catalog refresh fails reports failure for a table that is committed with its row (audit AUD-002, reproduced for CTAS only); verification round 4 PASS; merges on the owner's word.
- [WAP-BRANCH-TARGET-SILENT-1](wap-branch-target-silent-1-card-2026-10-28.md) — **card WAP-BRANCH-TARGET-SILENT-1 (filed 2026-10-28, P2, not dispatched):** under a session WAP branch, a case-varied `DELETE` on a Parquet WAP table writes to main silently (the main snapshot moves, and the branch is untouched or not created), because the branch loader takes the name as written; re-described in fix round 3 (R3-V2); pre-existing on main.
- [WAP-LATE-REFUSAL-BRANCH-1](wap-late-refusal-branch-1-card-2026-10-28.md) — **card WAP-LATE-REFUSAL-BRANCH-1 (filed 2026-10-28, P3, not dispatched):** under a session WAP branch, a refusal that a later check raises on the redirected statement (four shapes) still creates the branch and writes one or two metadata files; identical on main. The guard's own refusals create nothing (ICE-DELETE-FORMAT-GUARD-1 C-020); sibling of WAP-BRANCH-TARGET-SILENT-1.
- [REPLACE-WHERE-ORPHAN-1](replace-where-orphan-1-card-2026-10-28.md) — **card REPLACE-WHERE-ORPHAN-1 (filed 2026-10-28, P3, not dispatched):** an `INSERT INTO ... REPLACE WHERE` that fails with "Cannot delete file where some, but not all, rows match" leaves an orphan data file; pre-existing on main.
- [REPLACE-WHERE-AVRO-PARTIAL-MATCH-1](replace-where-avro-partial-match-1-card-2026-10-28.md) — **card REPLACE-WHERE-AVRO-PARTIAL-MATCH-1 (filed 2026-10-28, P3, not dispatched):** `INSERT INTO ... REPLACE WHERE` on Avro data fails with "Cannot delete file where some, but not all, rows match" though every row matches; Parquet succeeds; pre-existing on main; measured by the Opus 5.5 round-3 verifier's throwaway probe (see the card's Measured line).
- [CROSS-READ-GATE-CLASS-1](cross-read-gate-class-1-card-2026-10-28.md) and [RP-62-EQDEL-REPIN](rp-62-eqdel-repin-card-2026-10-28.md) — **closed (PRs #1211 and #1213):** the cross-read helper accepts only the unsupported-type exception (AUD-005); the fork's Rust pin for AUD-004.
- [SPARK-DOOR-TARGET-NORMALISATION-1](spark-door-target-normalisation-1-card-2026-10-28.md) — **card SPARK-DOOR-TARGET-NORMALISATION-1 (filed 2026-10-28, P3, not dispatched):** the Spark door's name-based DML claims take a case-varied target name as written, so it fails where the planner would resolve it; a session write conf refuses a fork-route branch spelling on Parquet as on Avro.
- [F-EQDEL-DROPPED-KEY-1](f-eqdel-dropped-key-1-card-2026-10-28.md) — **fork card F-EQDEL-DROPPED-KEY-1 (2026-10-28, landed as fork `#377`; RePark pinned Parquet data in RP-62):** a dropped equality-delete key column fails the scan (AUD-004); Avro and ORC data are F-EQDEL-DROPPED-KEY-2.
- [f-eqdel-dropped-key-2-card-2026-10-29.md](f-eqdel-dropped-key-2-card-2026-10-29.md) — **fork card F-EQDEL-DROPPED-KEY-2 (filed 2026-10-29, P2, not dispatched):** the Avro and ORC data readers refuse a scan whose equality-delete key column was dropped (`DataInvalid`, naming the data file), where the Parquet data path reads the survivors; RP-62 pins the refusal as loud-or-correct until the fork projects the key by field id.
- [f-eqdel-promoted-key-pos-1-card-2026-10-28.md](f-eqdel-promoted-key-pos-1-card-2026-10-28.md) — **fork card F-EQDEL-PROMOTED-KEY-POS-1 (filed 2026-10-28, P3, not dispatched):** a key promoted from `int` to `long` after an equality delete fails loud under a `_pos` or `_row_id` projection, which also fails the fork's merge-on-read DELETE and UPDATE on such tables with the table unchanged. Leaves when the owner charters it or declines it.
- [f-interop-test-s3-dir-1-card-2026-10-28.md](f-interop-test-s3-dir-1-card-2026-10-28.md) — **fork card F-INTEROP-TEST-S3-DIR-1 (filed 2026-10-28, P3 housekeeping, not dispatched):** `cargo test -p iceberg` leaves an untracked `s3:`-named directory in the crate, because three interop tests resolve an s3 path as a local relative path. Leaves when the owner charters it or declines it.
- [F-CI-MACOS-CAP-1](f-ci-macos-cap-1-card-2026-10-29.md) — **fork card F-CI-MACOS-CAP-1 (filed 2026-10-29, P3 housekeeping, not dispatched):** the fork's two macOS `Tests` jobs were cancelled at exactly fifteen minutes on every run of fork PRs `#375` and `#377` on 2026-10-28; they are not required checks, so both PRs merged on the required checks, and the cancel source is not identified (no `timeout-minutes` is set). The fix is the fork's, in its `.github/`; no RePark lane takes it.
- [DEP-AUDIT-0414-1](dep-audit-0414-1-card-2026-10-28.md) — **closed (merged 2026-10-29, PR #1210):** the RUSTSEC-2026-0414 failure of `cargo audit` on main.
- [v3-label-gate-dimensions-2026-10-28.md](v3-label-gate-dimensions-2026-10-28.md) — **gate input for the format-v3 label (filed 2026-10-28, not dispatched):** the fifteen dimensions of section 5 of the independent audit, each with its required discriminating evidence, copied unchanged; the input to the V3-LABEL-1 order (label of record 2026-11-17).
- [RP-POSDEL-BATCH-2](rp-posdel-batch-2-card-2026-10-27.md) — **card RP-POSDEL-BATCH-2 (filed 2026-10-27, P2, product grade; dispatched 2026-10-28 on an Opus 5.5 lane; PR #1203):** RePark's position-delete writers built one array per partition or identity set, so a large batch still panicked after RP-60; the fix is a chunked write. Closes on the merge of PR #1203; ledger [rp-posdel-batch-2-ledger.md](../../ledgers/staging/rp-posdel-batch-2-ledger.md) in staging.
- [SPARK-DOOR-DELETE-THROUGH-VIEW-1](spark-door-delete-through-view-1-card-2026-10-29.md) — **card SPARK-DOOR-DELETE-THROUGH-VIEW-1 (filed 2026-10-29, grade B, P2, not dispatched; re-measured 2026-10-29, F-3):** Spark 4.1.2 deletes through a temporary view over an Iceberg table; RePark refuses with `EXPECT_TABLE_NOT_VIEW`. Leaves when that DELETE matches Spark on both table kinds.
- [WRITER-PATH-TEMP-VIEW-TARGET-1](writer-path-temp-view-target-1-card-2026-10-29.md) — **card WRITER-PATH-TEMP-VIEW-TARGET-1 (filed 2026-10-29, grade C, narrowed 2026-10-29):** `writeTo`, `insertInto`, `saveAsTable` and `createOrReplace` already write the table on both engines; the remainder is the Rust door's bare names, which do not follow `USE` (a loud refusal, no silent write). Leaves when they match Spark under `USE` and `caseSensitive`.
- [MERGE-SCHEMA-EVOLUTION-SPELLING-1](merge-schema-evolution-spelling-1-card-2026-10-29.md) — **card MERGE-SCHEMA-EVOLUTION-SPELLING-1 (filed 2026-10-29, grade C, open, awaiting the owner's charter):** `MERGE WITH SCHEMA EVOLUTION INTO ns.t` executes on Spark and refuses on the facade, whose MERGE target expansion does not recognise the two-part spelling. Leaves when it matches Spark.
- [CROSS-READ-SAMPLING-FLAKE-1](cross-read-sampling-flake-1-card-2026-10-30.md) — **card CROSS-READ-SAMPLING-FLAKE-1 (filed 2026-10-30, grade C, open, awaiting the owner's charter):** the cross-read sampling test drops a phase under host load (1 of 18 cells failed where the branch did not touch the file; 1 to 2 of 39 per run at load 45; 60 idle runs clean). Wall-clock only. Leaves when the sampler is deterministic under load, or the charter declines it.
- [MAP-RATCHET-1](map-ratchet-1-card-2026-10-31.md) — **card MAP-RATCHET-1 (filed 2026-10-31, grade C, queued near-term by the owner):** the 51 excepted maps come under the 200-line / 16 KiB ceilings, three or four per round, the top 20 by bytes first. Leaves when `EXCEPTIONS` in `scripts/check_map_size.py` is empty and the maps-policy Open bullet records the date.
- [ICE-V2-LABEL-1](ice-v2-label-1-card-2026-11-05.md) — **card ICE-V2-LABEL-1 (2026-11-05; re-audit: LABEL DOES NOT STAND, five blocking findings):** the format-v2 label text; the maturity stays WITHDRAWN (C-072). Ledger [ice-v2-label-1-ledger.md](../../ledgers/staging/ice-v2-label-1-ledger.md).
- [DQ-IDENTIFIERS-CONFIG-REFUSE-1](dq-identifiers-config-refuse-1-card-2026-11-05.md) — **closed (2026-11-05, PR #1266):** AUD-021 refused; leads in [DQ-IDENTIFIERS-CONFIG-RESIDUE-1](dq-identifiers-config-residue-1-card-2026-11-05.md); fix cards [ANSI-SUBSCRIPT-AND-AMBIGUOUS-FIELD-1](ansi-subscript-and-ambiguous-field-1-card-2026-11-05.md), [CTAS-QUERY-FAILURE-CLEANUP-1](ctas-query-failure-cleanup-1-card-2026-11-05.md) and [NAN-UNDER-ICEBERG-SCAN-FILTERS-1](nan-under-iceberg-scan-filters-1-card-2026-11-05.md).
- [nested-expression-answers-1-card-2026-11-05.md](nested-expression-answers-1-card-2026-11-05.md), [native-door-silent-values-1-card-2026-11-05.md](native-door-silent-values-1-card-2026-11-05.md), [map-struct-key-collect-1-card-2026-11-05.md](map-struct-key-collect-1-card-2026-11-05.md), [facade-over-refusals-1-card-2026-11-05.md](facade-over-refusals-1-card-2026-11-05.md) and [label-record-precision-1-card-2026-11-05.md](label-record-precision-1-card-2026-11-05.md) — **cards (2026-11-05, not dispatched):** D10 and D13 expression answers under owner ruling 1, and four non-blocking re-audit findings (AUD-026 to AUD-029; C-102 to C-106).
- [META-DELETE-EMPTY-COMMIT-1](meta-delete-empty-commit-1-card-2026-11-08.md) — **card META-DELETE-EMPTY-COMMIT-1 (2026-11-08, S3, not dispatched):** a DELETE on a required column that matches no row commits an empty snapshot through the metadata-delete door; no wrong write. Leaves when fixed or declined.
- [NATIVE-MOR-FULL-DELETE-FILE-1](native-mor-full-delete-file-1-card-2026-11-08.md) — **card NATIVE-MOR-FULL-DELETE-FILE-1 (2026-11-08, S3, not dispatched):** a full DELETE on the native door over v2 merge-on-read writes a position-delete file where Spark leaves `data/` unchanged. Leaves when fixed or declined.
- [EXPLAIN-EMPTY-DML-SHAPE-1](explain-empty-dml-shape-1-card-2026-11-08.md) — **card EXPLAIN-EMPTY-DML-SHAPE-1 (2026-11-08, S3, not dispatched):** plain EXPLAIN of an empty DML shows the rewritten plan; its pin asserts only non-empty output. Leaves when pinned or ruled.
- [V3-LINEAGE-NATIVE-ASSERT-1](v3-lineage-native-assert-1-card-2026-11-08.md) — **card V3-LINEAGE-NATIVE-ASSERT-1 (2026-11-08, S3, not dispatched):** the empty-DML table asserts v3 lineage on the Spark door only. Leaves when asserted on every door or declined.
- [FACADE-AFFECTED-ROW-SHAPE-1](facade-affected-row-shape-1-card-2026-11-08.md) — **card FACADE-AFFECTED-ROW-SHAPE-1 (2026-11-08, S3, not dispatched):** the facade's affected-row column shape depends on the route. Leaves when pinned or declined.
- [EMPTY-DML-SET-ERROR-DIVERGENCE-1](empty-dml-set-error-divergence-1-card-2026-11-08.md) — **card EMPTY-DML-SET-ERROR-DIVERGENCE-1 (2026-11-08, S3, not dispatched):** a no-op UPDATE whose SET expression errors is count 0 where Spark refuses on v2 merge-on-read; no pin yet. Leaves when the cell and its registry row land together.
- [STRUCT-COMPARE-BY-POSITION-1](struct-compare-by-position-1-card-2026-11-09.md), [STRUCT-COMPARE-SEAT-FAMILY-1](struct-compare-seat-family-1-card-2026-11-10.md) and [STRUCT-SETOP-TYPE-FAMILY-1](struct-setop-type-family-1-card-2026-11-09.md) — **closed (PR #1292 `68f70b0f`; PR #1295 `68b09d18`, 2026-11-11; PR #1290, 2026-11-10):** comparisons, `IN` lists and join keys, then key-list joins, `BETWEEN`, simple `CASE` and `nullif`, pair struct fields by position, closing the family (C-116 and C-118 CLOSED; residue carded); set operations pair a reordered struct by position (ledger C-117 CLOSED; F-2 and F-7 carded; F-5 and F-6 appended to FACADE-INGEST-ARROW-NORMALIZE-1).
- [STRUCT-SETOP-LISTVIEW-COERCION-1](struct-setop-listview-coercion-1-card-2026-11-10.md) — **card STRUCT-SETOP-LISTVIEW-COERCION-1 (filed 2026-11-10, S3, not dispatched):** a `ListView` set operation against a plain `List` with the same names is refused by coercion where Spark answers. Leaves when fixed or declined.
- [RCTE-PROJECTION-PRUNE-1](rcte-projection-prune-1-card-2026-11-10.md) — **card RCTE-PROJECTION-PRUNE-1 (filed 2026-11-10, S3, not dispatched):** a recursive CTE that selects only some columns fails on both doors, one form as a caught panic. Leaves when fixed or declined.
- [STRUCT-USER-CAST-POSITIONAL-1](struct-user-cast-positional-1-card-2026-11-09.md) — **card STRUCT-USER-CAST-POSITIONAL-1 (filed 2026-11-09, S2, not dispatched):** an explicit struct cast pairs fields by name where Spark pairs by position. Leaves when fixed or declined.
- [STRUCT-TYPE-UNION-ACCEPTS-REORDER-1](struct-type-union-accepts-reorder-1-card-2026-11-09.md) — **card STRUCT-TYPE-UNION-ACCEPTS-REORDER-1 (filed 2026-11-09, S3, not dispatched):** `CASE`, `IF`, `coalesce` and `array` accept a reordered struct pair that Spark refuses; no wrong value measured. Leaves when pinned or declined.
- [STRUCT-COMPARE-PLANNER-CLASS-1](struct-compare-planner-class-1-card-2026-11-10.md) — **card STRUCT-COMPARE-PLANNER-CLASS-1 (filed 2026-11-10, S3, class only, not dispatched):** 80 cells refuse with DataFusion's class, not Spark's.
- [STRUCT-MAP-SUBSCRIPT-DML-1](struct-map-subscript-dml-1-card-2026-11-10.md) — **card STRUCT-MAP-SUBSCRIPT-DML-1 (filed 2026-11-10, S3, not dispatched):** 20 DML cells fail on an internal field name.
- [STRUCT-MAP-SUBSCRIPT-JOIN-1](struct-map-subscript-join-1-card-2026-11-10.md) — **card STRUCT-MAP-SUBSCRIPT-JOIN-1 (filed 2026-11-10, S3, not dispatched):** 10 join cells refuse with `AMBIGUOUS_REFERENCE`.
- [SPARK-DOOR-PREPARE-1](spark-door-prepare-1-card-2026-11-10.md) — **card SPARK-DOOR-PREPARE-1 (filed 2026-11-10, S3, ruled A 2026-11-10, not dispatched):** `PREPARE`, `EXECUTE <name>` and `DEALLOCATE` accepted where Spark refuses.
- [SPARK-DOOR-PARAMETERIZED-SQL-1](spark-door-parameterized-sql-1-card-2026-11-10.md) — **card SPARK-DOOR-PARAMETERIZED-SQL-1 (filed 2026-11-10, S3, not dispatched):** `EXECUTE IMMEDIATE` and `sql(query, args=...)` fail where Spark answers.
- [STRUCT-FSL-SIZE-COMPARE-1](struct-fsl-size-compare-1-card-2026-11-10.md) — **card STRUCT-FSL-SIZE-COMPARE-1 (filed 2026-11-10, S3, over-refusal, not dispatched):** struct `FixedSizeList` of different sizes refused.
- [FSL-SIZE-COMPARE-1](fsl-size-compare-1-card-2026-11-10.md) — **card FSL-SIZE-COMPARE-1 (filed 2026-11-10, S2, out of clause, not dispatched):** non-struct `FixedSizeList` of different sizes compares wrong.
- [MAP-IN-SUBQUERY-ORDERING-1](map-in-subquery-ordering-1-card-2026-11-10.md) — **card MAP-IN-SUBQUERY-ORDERING-1 (filed 2026-11-10, S3, not dispatched):** `c IN (SELECT m)` over a map answers where Spark refuses.
- [STRUCT-MAP-FIELD-COMPARE-1](struct-map-field-compare-1-card-2026-11-10.md) — **card STRUCT-MAP-FIELD-COMPARE-1 (filed 2026-11-10, S3, not dispatched):** a struct of map fields compares where Spark refuses.
- [FEXPR-UNRESOLVED-FIELD-ACCESS-1](fexpr-unresolved-field-access-1-card-2026-11-10.md) — **card FEXPR-UNRESOLVED-FIELD-ACCESS-1 (filed 2026-11-10, S3, not dispatched):** `F.expr` field access fails; a Spark cell is needed.
- [STRUCT-POSITION-EXPLAIN-NAME-1](struct-position-explain-name-1-card-2026-11-10.md) — **card STRUCT-POSITION-EXPLAIN-NAME-1 (filed 2026-11-10, S3, not dispatched):** EXPLAIN shows the internal struct-by-position UDF.
- [SET-COMPARISON-ANY-1](set-comparison-any-1-card-2026-11-10.md) — **card SET-COMPARISON-ANY-1 (filed 2026-11-10, S3, not dispatched):** `= ANY (subquery)` accepted; Spark has no such syntax.
- [LATERAL-USING-ACCEPT-1](lateral-using-accept-1-card-2026-11-10.md) — **card LATERAL-USING-ACCEPT-1 (filed 2026-11-10, S3, not dispatched):** `LATERAL … USING` accepted where Spark refuses.
- [ARRAY-NVL-REFUSAL-TEXT-1](array-nvl-refusal-text-1-card-2026-11-10.md) — **card ARRAY-NVL-REFUSAL-TEXT-1 (filed 2026-11-10, S3, not dispatched):** `array_remove`, `nvl`, `nvl2` refusal class or text differs.
- [STRUCT-KEY-MAP-READ-1](struct-key-map-read-1-card-2026-11-10.md) — **card STRUCT-KEY-MAP-READ-1 (filed 2026-11-10, unrated, not dispatched):** reading a struct-keyed map fails in Python; a cell comes first.
- [STRUCT-COMPARE-REVIEW-RESIDUE-1](struct-compare-review-residue-1-card-2026-11-10.md) — **card STRUCT-COMPARE-REVIEW-RESIDUE-1 (filed 2026-11-10, S3, not dispatched):** class (c) residue items from the PR #1292 verdicts.
- [FACADE-INGEST-ARROW-NORMALIZE-1](facade-ingest-arrow-normalize-1-card-2026-11-09.md) — **card FACADE-INGEST-ARROW-NORMALIZE-1 (filed 2026-11-09, unscheduled):** narrow the Arrow list kinds and run-end encoding at `createDataFrame` to Spark's plain types; costs offset narrowing and the 2^31 limit; needs a sketch and a measurement.
- [LISTVIEW-INNER-JOIN-KEY-1](listview-inner-join-key-1-card-2026-11-11.md) — **card (filed 2026-11-11, S1, owner ruled the fix 2026-11-11; FIXED for v1.5.7 by #1304):** a DataFrame key-name inner join on a `ListView` key returns the cross product (Q2059, Q2063).
- [FEXPR-WINDOW-NULL-ORDER-1](fexpr-window-null-order-1-card-2026-11-12.md) and [FILTER-STRING-NULL-ORDER-1](filter-string-null-order-1-card-2026-11-13.md) — **cards (S2, FIXED: the first 2026-11-12 for v1.5.7, the second 2026-11-13 before the scoped label):** an `ORDER BY` in `F.expr`, then in a string `filter` / `where`, placed NULLs backwards; three findings each.
- [SQL-SETOP-ALL-MULTISET-1](sql-setop-all-multiset-1-card-2026-11-12.md) — **card SQL-SETOP-ALL-MULTISET-1 (filed 2026-11-12, S2, not dispatched):** SQL and DataFrame `INTERSECT ALL` / `EXCEPT ALL` answer Spark's multiset; until then SQL-SETOP-ALL-REFUSE-1 refuses them (owner, 2026-11-12, blocks ICE-V2-FLAT-AWS-LABEL-1).
- [CATALOG-REPLACE-CACHE-LEAK-1](catalog-replace-cache-leak-1-card-2026-11-12.md) — **card CATALOG-REPLACE-CACHE-LEAK-1 (filed 2026-11-12, S4, not dispatched):** the fork's Glue and S3 Tables replace publishes never invalidate the replaced metadata location; UUID names make it a capped leak, not a wrong read.
- [S3TABLES-CATALOG-IMPL-NAME-1](s3tables-catalog-impl-name-1-card-2026-11-12.md) — **card S3TABLES-CATALOG-IMPL-NAME-1 (filed 2026-11-12, S4, not dispatched):** tests name a `catalog-impl` class that does not exist; RePark's suffix match hides it.
- [S3T-CTAS-SHELL-CREATE-1](s3t-ctas-shell-create-1-card-2026-11-12.md) — **card S3T-CTAS-SHELL-CREATE-1 (filed 2026-11-12, after the scoped label, not dispatched):** an S3 Tables CTAS creates its entry with no first version and adopts such an entry on retry, as the AWS jar does.
- [NAN-PARTITION-VALUE-1](nan-partition-value-1-card-2026-11-12.md) — **card (2026-11-12, S1, FIXED by #1319 and fork #383):** a NaN or ±Infinity FLOAT/DOUBLE partition value written by `INSERT`, `writeTo` or `saveAsTable` reached the manifest as NULL.
- [LOCAL-DROP-RECREATE-STALE-WRITE-1](local-drop-recreate-stale-write-1-card-2026-11-12.md) — **card (filed 2026-11-12, S1 wrong write, blocked the full v2 label; FIXED in v1.5.7 by fork #382 and #1313):** a Hadoop drop and recreate read and wrote on the dropped table's metadata.
- [STRUCT-POSITION-GROUPING-SETS-1](struct-position-grouping-sets-1-card-2026-11-11.md), [NOT-BETWEEN-DML-CSE-1](not-between-dml-cse-1-card-2026-11-11.md), [STRUCT-COMPARE-FAMILY-RESIDUE-1](struct-compare-family-residue-1-card-2026-11-11.md), [STRUCT-POSITION-ERROR-NAMES-1](struct-position-error-names-1-card-2026-11-11.md) and [MAP-NESTED-ORDERING-ON-1](map-nested-ordering-on-1-card-2026-11-11.md) — **cards (filed 2026-11-11, not dispatched):** S3 except STRUCT-POSITION-ERROR-NAMES-1 (S4). Each leaves when fixed or declined.
- [USING-SCALAR-STRUCT-KEY-CLASS-1](using-scalar-struct-key-class-1-card-2026-11-11.md), [STRUCT-SETOP-CONTROL-CLASS-1](struct-setop-control-class-1-card-2026-11-11.md), [USING-CHAIN-AMBIGUOUS-1](using-chain-ambiguous-1-card-2026-11-11.md), [NATURAL-EXISTENCE-JOIN-ACCEPT-1](natural-existence-join-accept-1-card-2026-11-11.md) and [MAP-ORDERING-FORMS-1](map-ordering-forms-1-card-2026-11-11.md) — **cards (filed 2026-11-11, S3, not dispatched):** the class-only gaps and the NATURAL (X0001 to X0060) and map-ordering acceptances; each leaves when fixed or declined.
- [struct-case-name-cast-1-card-2026-11-11.md](struct-case-name-cast-1-card-2026-11-11.md), [struct-typed-form-schema-1-card-2026-11-11.md](struct-typed-form-schema-1-card-2026-11-11.md) and [facade-dup-field-row-1-card-2026-11-11.md](facade-dup-field-row-1-card-2026-11-11.md) — **cards (filed 2026-11-11, S3, not dispatched):** head-leg class gaps from the pin file; FACADE-DUP-FIELD-ROW-1's true card for X0030 and X0032 is NATURAL-EXISTENCE-JOIN-ACCEPT-1.
- [PIVOT-EMPTY-CELL-COUNT-1](pivot-empty-cell-count-1-card-2026-11-12.md) — **card (filed 2026-11-12, S2, not dispatched):** a pivoted count answers 0 where Spark answers NULL for an absent pair.
- [LOG-NONPOSITIVE-NULL-1](log-nonpositive-null-1-card-2026-11-12.md) — **card (filed 2026-11-12, S2, not dispatched):** `ln`, `log10` and `log2` of zero or a negative answer -Infinity or NaN where Spark answers NULL.
- [NAN-PAYLOAD-1](nan-payload-1-card-2026-11-12.md) — **card (named by the signed-zero sketch; card file 2026-11-12, unrated, not dispatched):** math functions return sign-set NaN, so `max`/`min`, `greatest`/`least` and `ORDER BY` differ from Spark; the owner queued a follow-up unit.
- [PIVOT-COLUMN-ORDER-1](pivot-column-order-1-card-2026-11-12.md), [SPARK-DOOR-IS-DISTINCT-COLUMNS-1](spark-door-is-distinct-columns-1-card-2026-11-12.md), [SPARK-DISTRIBUTE-BY-1](spark-distribute-by-1-card-2026-11-12.md), [POW-NEGATIVE-FRACTIONAL-1](pow-negative-fractional-1-card-2026-11-12.md) and [NOT-IN-MULTICOLUMN-1](not-in-multicolumn-1-card-2026-11-12.md) — **cards (filed 2026-11-12, S3, not dispatched):** a discovered pivot's column order and NaN name; over-refusals of two-column `IS DISTINCT FROM` (raised priority), `DISTRIBUTE BY`, `pow(-8.0, 0.5)` (1.5.6) and `(a, b) NOT IN`.
- [COUNT-DISTINCT-MULTI-SIGNED-ZERO-1](count-distinct-multi-signed-zero-1-card-2026-11-12.md) — **card (filed 2026-11-12, S2, not dispatched):** `countDistinct('a', 'b')` over signed-zero rows answers 3 where Spark answers 1; owner ruling 2.
- [RECURSIVE-UNION-COUNT-PANIC-1](recursive-union-count-panic-1-card-2026-11-12.md) — **card (filed 2026-11-12, S2 provisional, not dispatched):** `count(*)` over a recursive `UNION` CTE panics in DataFusion's `multi_group_by`.
- [MEMORY-CATALOG-HADOOP-ABA-1](memory-catalog-hadoop-aba-1-card-2026-11-12.md) — **card (filed 2026-11-12, S2, not dispatched):** the fork's Hadoop commit compares a path string, so a concurrent drop and recreate lets a commit resurrect the dropped table.
- [MEMORY-CATALOG-PUT-AFTER-UNLOCK-1](memory-catalog-put-after-unlock-1-card-2026-11-12.md), [HADOOP-CHAIN-LOCATIONS-BOUND-1](hadoop-chain-locations-bound-1-card-2026-11-12.md) and [SQL-MINUS-REFUSE-1](sql-minus-refuse-1-card-2026-11-12.md) — **cards (filed 2026-11-12, not dispatched):** S3 except HADOOP-CHAIN-LOCATIONS-BOUND-1 (S4): the memory catalog's cache put after the unlock (fork #382's follow-up), an unbounded Hadoop drop eviction, and `MINUS` refused where Spark answers.
- [NATIVE-DATAFRAME-WRITE-SILENT-1](native-dataframe-write-silent-1-card-2026-11-13.md) — **card (2026-11-13, S1, not dispatched):** native frame writes report success and store nothing.
- [NATIVE-CREATE-SCHEMA-NOT-DURABLE-1](native-create-schema-not-durable-1-card-2026-11-13.md) — **card (2026-11-13, S2, not dispatched):** native `CREATE SCHEMA` under Iceberg is in-memory and can shadow a namespace.
- [NAN-PARTITION-RAW-BITS-1](nan-partition-raw-bits-1-card-2026-11-13.md) — **card (2026-11-13, S2, not dispatched):** fan-out writes keep a raw NaN partition; RePark's `= NaN` misses rows.
- [FLOAT-PARTITION-INSERT-REFUSE-1](float-partition-insert-refuse-1-card-2026-11-13.md) — **card (2026-11-13, S2 loud, not dispatched):** an `INSERT` of an inexact FLOAT partition value refuses.
- [ICEBERG-NAN-FILE-PRUNE-1](iceberg-nan-file-prune-1-card-2026-11-13.md) — **card (2026-11-13, S2 provisional, not dispatched):** the scan drops a NaN row for `x > 0.5`; Spark-on-Iceberg keeps it.
- [MATH-NAN-CANONICAL-1](math-nan-canonical-1-card-2026-11-13.md) — **card (2026-11-13, step 0; the owner picks seat A, B or C):** Spark keeps sign-set NaN in memory too.
- [NATIVE-CREATE-VIEW-ICEBERG-EMPTY-1](native-create-view-iceberg-empty-1-card-2026-11-13.md), [NATIVE-UNION-LITERAL-INT-1](native-union-literal-int-1-card-2026-11-13.md), [NATIVE-CONCAT-NULL-1](native-concat-null-1-card-2026-11-13.md), [NATIVE-LISTING-FLOAT-PARTITION-1](native-listing-float-partition-1-card-2026-11-13.md), [NATIVE-EXPLAIN-EXECUTE-1](native-explain-execute-1-card-2026-11-13.md) — **cards (2026-11-13, not dispatched):** S3; the last S4.
- [AGG-ORDER-BY-ACCEPTED-1](agg-order-by-accepted-1-card-2026-11-13.md), [FEXPR-COUNT-STAR-AGG-1](fexpr-count-star-agg-1-card-2026-11-13.md), [NAN-PARTITION-FILE-SPLIT-1](nan-partition-file-split-1-card-2026-11-13.md), [S3T-STAGE-CREATE-ORACLE-1](s3t-stage-create-oracle-1-card-2026-11-13.md) — **cards (2026-11-13, S3, S3, S4, unrated; not dispatched):** the first refuses after the label (owner); the last waits on the owner.
