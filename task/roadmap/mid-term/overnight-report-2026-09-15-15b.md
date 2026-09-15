# Overnight report — run 15b of 2026-09-15 (the facade surfaces of the 1.5 PySpark-parity campaign)

**Session:** one Opus orchestrator (overnight-15b), 2026-09-14 20:45 → 2026-09-15 07:15 EDT · **Orchestrator:** Claude (claude-opus-5) · **Lanes:** `pb-` · **Worker spend:** $34.02 (GLM $2.46, Grok $31.56; Devin and Muse rounds report $0) ·
**Beside:** run 15a (functions), run 15c (SQL door + registry BACKLOG) · **Grants:** G-1 yes, G-2 yes, G-3 stop 06:30 / done
07:15, G-4 GLM + Muse + Devin + Grok, G-5 yes. **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md). STATUS.md, tags and the release pipeline
untouched; no file owned by run 15a or 15c edited outside the agreed seams.

## 1. Census slice — before / after

Census basis: run 15's dump (`main` 629f3d5f vs PySpark 4.1.2, `/tmp/oc-worker/run15/*-surface.json`) for **before**. **After** is
`census.py` run against `main` 1aa95356 at 06:07 (after #595, #596, #598, #600, #602, #603 and #604), with the PRs still in the queue
listed beside it. A name counts as present when it is on the facade class. Declared refusals are present by design, with their
registry rows named in each PR.

| Surface | Missing at start | Present on main at 06:07 | Still missing on main | Lands with | Not started tonight |
|---|---|---|---|---|---|
| Row | 2 (`count` `index`) | 3/3 | — | #595 (merged) | — |
| types | 13 | 38/38 real type names | — | #598 (merged) | `TYPES-GEO-DDL-1` spatial DDL arm (owner question) |
| Catalog | 13 | 27/27 | — | #602 (merged) | — |
| SparkSession | 19 | 36/36 | — | #603 (merged) | — |
| DataFrameWriterV2 | 1 (`clusterBy`) | 12/12 | — | #604 (merged) | — |
| DataFrameWriter | 7 | 14/18 | `jdbc` `orc` `text` `xml` | #610 (`jdbc` `orc` `xml`); IO-TEXT-1 branch (`text`) | — |
| DataFrameReader | 3 | 10/13 | `orc` `text` `xml` | #610 (`orc` `xml`); IO-TEXT-1 branch (`text`) | — |
| DataFrameNaFunctions | 1 (`replace`) | 2/3 | `replace` | #610 | — |
| Column | 7 | 29/36 | `astype` `dropFields` `isNaN` `isin` `name` `outer` `withField` | #605 | — |
| GroupedData | 6 | 9/15 at 06:07; 15/15 after #608 (06:44) | — | #608 (merged) | — |
| DataFrame | 25 | 103/115 at 06:07; 106/115 after #607 (06:15) | 9 | #607 (merged: `foreach` `foreachPartition` `observe`); DF-PLAN-INTROSPECT-1 branch (`inputFiles` `semanticHash`) | `freqItems` `transpose` `metadataColumn` (Rust cards, build clone never free); `exists` `scalar` `lateralJoin` `asTable` (DF-SUBQUERY-1, needs #605 merged) |

At the stop, all the slice's names that depend on the queue are in green PRs: #605, #607, #608 and #610. Two units remain on pushed branches in review
(DF-PLAN-INTROSPECT-1, IO-TEXT-1). Seven DataFrame names were not started.

## 2. Per-PR table

| PR | Unit | Actor rounds (tier, cost) | Critic-logic (Grok) | S2-21 perf | Merged | Notes |
|---|---|---|---|---|---|---|
| #595 | ROW-TUPLE-1 | GLM 2 rounds $0.21 | round 1 NEEDS_REMEDIATION 0 P1 / 4 P2 → pins, $0.44 | not run (R-5) | bc024735 tree-equal | orchestrator: example + EX counts, attestation (CI caught AT-8..AT-10) |
| #596 | DF-STREAM-BATCH-1 | Devin 2 rounds $0 (`evening-october`) | round 1 2 P1 / 3 P2 → fixed; re-check PASS, $0.83 cumulative | not run (R-8) | 8cd6d1e6 tree-equal | net CalendarInterval negativity, empty-string gate |
| #598 | TYPES-BASES-1 | Devin 2 rounds $0 (`adhesive-comma`) | round 1 2 P1 / 2 P2 / 1 P3 → fixed with the FACADE-4 rebase; re-check all FIXED, $1.51 cumulative | not run (R-6) | 03df3ad8 tree-equal | TYPES-GEO-DDL-1 BACKLOG (spatial DDL arm is Rust type table, outside fence) |
| #600 | DF-SURFACE-A-1 | Devin 3 rounds $0 (`actually-arthropod`; one cut by the rate limit) | round 1 7 P1 / 3 P2 → rulings R-5..R-9 + fix; re-check PASS + L-101 → DF-TO-BINARY-1, $0.55 + re-check | not run (R-12) | | inputFiles / semanticHash split to Rust card DF-PLAN-INTROSPECT-1; DF-METADATA-1 BACKLOG |
| #605 | COLUMN-PARITY-1 | Devin 1 round (`motley-work`) + Devin R-4 round cut by the rate limit → Muse continuation (both trailers) → Muse round 2 | round 1 4 P1 / 6 P2 (deferred Python withField design) → R-4 Rust UpdateFields; re-check PASS + 3 P2 → round 2; ≈$1.5 | Python perf round 1 P1 isin OR-chain SIGSEGV at 10k ($1.33) → re-check closed, 1 P2 parent-null child copies | | CI CHECKS-RED 05:52 on `docs/examples/dataframe/schema_reconcile.py` (from #600, expected withMetadata's dict to drop; #605's native surfaces it as Spark does), example fixed 05:57 (d04d2ca4), queue slot given to #607; smoke passed 06:23, re-queued after #608 |
| #602 | CATALOG-SURFACE-1 | Devin (`ahead-blouse`) round 1 + follow-up (cut by the rate limit, resumed) | round 1 1 P1 (stale cache after write) / 5 P2 / 1 P3 → R-6/R-7 fix; re-check PASS, ≈$1.5 | answered inside the critic brief: 0.53–0.60 ms per cached spark.table call (R-8) | 5c172af0 tree-equal (02:58) | rebased onto #601; five test_sql_set_door_1.py reds proven stale-native |
| #603 | SESSION-SURFACE-1 | Devin (`prickly-microwave`) 2 rounds | round 1 1 P1 (artifact dir leaks onto sys.path) / 6 P2 → R-4..R-6; re-check PASS (P3 finalizer note), ≈$1.5 | not run (R-8) | f6312c7b tree-equal (03:25) | rebase conflict with #602's helper removal resolved; session_core.py 2296 → 2290 by joining a signature |
| #604 | IO-BUCKET-CLUSTER-1 | GLM 2 rounds $0.89 | round 1 0 P1 / 4 P2 (+ Q-1 insertInto, Q-2 ruled) → GLM fix table; re-check L-101/L-102 residuals fixed by the orchestrator (R-4), ≈$1.8 | not run (R-5) | 1aa95356 tree-equal (05:28; squash retried past a 502 and a GraphQL error) | PR-245 literal inventory red after a helper move, fixed |
| #607 | DF-SURFACE-B-1 (foreach, foreachPartition, observe, Observation) | Grok actor $7.97 (100 turns) → Devin R-5 round (`tough-macaw`) | round 1 3 P1 / 4 P2 → R-5 (Observation binds to the observed frame); re-check all fixed + L-101 P1 explain fills / L-102 P2 temp view fills → orchestrator fix under R-6, L-103 tail(0) fixed, L-104 P3 recorded | not run (R-8) | 0d46bc5b tree-equal (06:15; took #605's slot, rebase resolved #602's catalog temp-view registration inside the fill suppression) | freqItems split to a Rust card |
| #608 | GROUPED-SURFACE-1 | Devin 2 rounds (`quill-wander`, $0) | round 1 0 P1 / 1 P2 (L-001 0-column empty Arrow result KeyErrors) $1.06; re-check PASS (boundary scan byte-identical on 27 key shapes) $0.73 | Python perf $1.12: 0 P1, P2-1 per-row as_py scan → R-3 pyarrow.compute boundaries (4.28 s → 0.02 s at 10 groups, 5.28 s → 1.32 s at 1e5); P2-2/P2-3/P3 R-4 | b3a6e52c tree-equal (06:44) | |
| #610 | IO-DECLARED-1 (orc, xml, jdbc declared; na.replace) | GLM 3 rounds $0.71 + $0.37 (R-3 restored `spark.read.jdbc` PostgreSQL reads) + $0.28 (L-001/L-002) | round 1 NEEDS_REMEDIATION 0 P1 / 2 P2 (L-001 write.jdbc save mode case-sensitive; L-002 `postgres://` no longer reaches read_postgres) $0.96; re-check PASS $0.29 | not run (R-4: refusals and one delegation, no data path) | not merged — green and ready at the stop; no queue slot before 07:15, passes to the owner | card error caught at audit: R-2 would have removed a working Postgres read path; CAP-1 mirror row re-added by the rebase, removed |
| — | DF-PLAN-INTROSPECT-1 | Muse round 1 on the build clone: `inputFiles` (physical file-group walk) and `semanticHash` (canonical analyzed-plan hash, Java-int fold) in `crates/repark-core/src/plan_introspect.rs` → Muse follow-up (R-1..R-5) 04:41–05:44, commit 5d564655: construction-identity MemTable hash, cache-lineage expansion, CUBE/ROLLUP tags, view strip, streaming hash with the GIL released (depth-200 0.41 s → ~0.0001 s); workspace Rust 3111/0, facade 6783; re-check (05:53, $0.79): R-1/R-2/R-4 and most of R-3 FIXED or CLOSED; NEW L-101 P1 — R-5's analyze-skip makes DF `filter` and SQL `WHERE` (and a view over a filtered frame) hash unequal; L-102 P3 identity select. **Ends the night as pushed branch 5d564655, no PR** (no build-clone slot before the stop); next-run brief `followup-planintro-2-NEXT-RUN.md` | Grok critic (first launch stopped after one turn, $0.02; resumed $0.69): NEEDS_REMEDIATION 3 P1 / 5 P2 / 1 P3; orchestrator measured the UNMEASURED items on live PySpark → R-1..R-4 (L-006 and L-009 closed as Spark-matching) | Grok Rust perf ($ in runs.tsv): 0 P1; P2-1 O(n²) Debug-string hash + re-analyze (200-deep withColumn 417 ms), P2-2 GIL held → R-5 to the actor; P2-3 re-list per inputFiles call, P3-1, P3-2 recorded | | rebased onto #603; release native rebuilt 03:36 |
| — | IO-TEXT-1 (read.text, write.text) | Muse round 1 on the build clone (follow-up T-1..T-9 + P-1..P-3 launched 05:47, inside the 06:00 contingency), cut by the 04:17 provider outage after 210 steps, resumed from its session (45 more steps): streaming TableProvider scan and part-file writer in `crates/repark-core/src/text_io.rs` | Grok critic $0.62: NEEDS_REMEDIATION 0 P1 / 7 P2 (recursiveFileLookup=False refused; empty write lineSep; invalid UTF-8; two-string write class; globs without a row; partitionBy implementable; hollow split pins) / 2 P3 (_SUCCESS, PATH_NOT_FOUND); strong null reports on line splitting across buffer boundaries; UNMEASURED items measured on live PySpark before ruling | Grok Rust perf: P1 `write_text_frame` collects the whole frame (4 GiB write → 4293 MiB RSS vs 141 MiB streaming count); P2 one partition, two copies per line, limit not threaded, one batch per tiny file (497–642 MiB/s vs Spark warm CSV 1925 MiB/s; 10k tiny files 28× faster than Spark); P3 buffer reuse, memchr, batch size → P-1..P-3 to the actor; byte-range splits recorded | | orchestrator stripped 39 Rust doc-comment lines the worker kept (brief bans comments; house pattern is an allow attribute); gzip, the text SQL door and partitionBy filed as refusals, partitionBy put to the critic as a likely gap |

Coordination: run 15a asked for a merge hold at ~01:00 while its #597 rebased for the fourth time (main moved under it three
times); 15b stopped #602's merge chain and held #603 until 15a reports #597 landed. #597 landed 01:54 (efcb14ef). The three runs then
agreed one serial order for main: 15c #601 → 15b #602 → 15b #603 → 15a #594 → 15b #604 → 15b #605 → 15a FNP-11a / 15c FNP-6D as
announced; each PR rebases when its turn comes, and whoever lands pings the next.
DF-SURFACE-B-1: Grok actor round ($7.97, 100 turns — the costliest round of the night) → critic 3 P1 / 4 P2 → ruling R-5 (the
Observation binds to the observed frame; one extra aggregation over it) → fresh Devin round (`tough-macaw`) → re-check (L-101/L-102 fixed by the orchestrator under R-6). Lane cap: the queue counter did not count
Grok actor lanes, so the Column round-2 Muse launch at 00:54 briefly made four worker lanes (fixed in the queue scripts).

Events: Devin free-tier rate limit at 23:17 EDT cut three rounds at once (shared account with runs 15a/15c); resumed after the
reset, the Rust Column round moved to Muse with both trailers. Main merged FACADE-4 step 1 (#582) and ARRAY-NULL-1 (#581)
mid-run: the thin clones' native module went stale twice (rebuilt once at #582; #581's 175 facade failures proven
environmental on a pristine main worktree).

Run 15a's #594 merged at 04:43 after a CI `Repo guards` rerun (uv manifest-fetch timeout). #602 and #603 had landed at 02:58 and 03:25. Pre-rebases at 05:17–05:20 (after #609): #605 and #608 rebased cleanly and pushed. #607 met a real semantic conflict: #602 had replaced `create_or_replace_temp_view`'s registration with `catalog_surface._register_temp_view`, while DF-B's L-102 fix routed the same call site through `surface_b.register_view_without_fill`. The resolution keeps #602's registration inside the fill suppression; `core.py` 4034 → 4031, and the DF-B, Catalog and temp-view pins stay green. A shared-shell race cost one edit at 06:20: parallel Bash calls share one working directory, and a concurrent `cd` into a lane clone made a relative-path report edit miss its file. Every later parallel call uses absolute paths. Muse provider outage 04:17: every Muse lane on the box exited 1 ("network connection could not be opened"), including 15b's IO-TEXT-1 round on the build clone after 210 steps. Run 15a flagged it, and the round was resumed from its session at 04:21 with the uncommitted tree kept. #604 and #605 were pre-rebased onto main and pushed at 03:54 and 04:19 so their merge slots start with CI already green; #605 needed a manual resolution of the PR-245 inventory, the dataframe map and the core.py baseline, and `rebase.sh` now unions the PR-245 file.

## 3. Rulings taken under G-2

- GROUPED-SURFACE-1 R-3 (02:42): the perf reviewer's P2-1 goes back to the actor — the grouping scan finds run boundaries with pyarrow.compute and calls as_py only on boundary rows; P2-2 (per-group from_batches), P2-3, P3-1, P3-2 recorded.
- CATALOG-SURFACE-1 (02:38): five test_sql_set_door_1.py timezone reds after rebasing onto #601 ruled environmental — #601 added a Rust `current_timezone` the thin clone's native lacks; #602 touches none of those files; PR CI builds a fresh native.

- IO-DECLARED-1 R-3 (03:08): the card's R-2 was wrong about `spark.read.jdbc` — main already reads PostgreSQL through it; GLM's round 1 replaced a working path with a refusal. Back to GLM: restore the PostgreSQL read path with Spark's camelCase signature plus main's keyword spellings, refuse other drivers and every `write.jdbc`, restore the rewritten pins. Lesson: a card that declares a name must check main for an existing working body first.

- IO-DECLARED-1 R-4 (03:41): no S2-21 perf reviewers — every name is a refusal or a delegation to an existing path. The R-3 round made passing both `properties` and `connection_properties` to `spark.read.jdbc` a TypeError (main silently preferred `properties`); accepted as the Spark-signature alias contract and recorded.
- DF-PLAN-INTROSPECT-1 (03:36): the thin clones' five SQL SET door reds confirmed stale-native — green on the rebuilt release native.

- DF-PLAN-INTROSPECT-1 R-1..R-4 (03:58), from a live PySpark 4.1.2 probe (`oracle/probe_planintro.py`):
  - R-1: local frames hash by construction identity. Spark never equates two `createDataFrame` frames, even with identical data.
  - R-2: caching never changes a hash; a scan of a cache view hashes its lineage.
  - R-3: CUBE and ROLLUP differ; hex normalization is limited to repark-generated names; temp views inline.
  - R-4: the critic's L-006 (cached-child `inputFiles` is `[]`) and L-009 (`file:///`) match Spark, so both are pinned without a code change. L-004 (Iceberg `table()` snapshots) is UNMEASURED and recorded.

- IO-TEXT-1 T-1..T-9 (05:00), from a live PySpark 4.1.2 probe (`oracle/probe_iotext.py`). Every critic finding stood once measured:
  - Spark reads with `recursiveFileLookup=False`.
  - It refuses an empty write `lineSep` with its exact `IllegalArgumentException` text.
  - It replaces each malformed UTF-8 sequence with U+FFFD.
  - A two-string write raises `_LEGACY_ERROR_TEMP_1290`.
  - Globs expand, and `foo[bar].txt` is a character class that matches `foob.txt`.
  - `partitionBy` writes `k=x/part-*.txt`; both empty and non-empty writes end with `_SUCCESS`.
  - A missing path raises `PATH_NOT_FOUND`.
  - All of these go to the actor with perf P-1..P-3. Reading a partitioned text directory back becomes a loud DECLARED refusal (`IO-TEXT-PART-READ-1`); Spark's partition type inference is out of scope tonight.

- IO-TEXT-1 contingency (05:41): the build clone is the only Rust lane and DF-PLAN-INTROSPECT-1's follow-up still holds it (a full `cargo test --workspace` run at 05:40). If that round has not handed back by 06:00, IO-TEXT-1 ends the night as its pushed branch `feat/io-text-1` (c607b312) with no PR. A PR carrying a known P1 (writer collects the whole frame) and seven measured P2 parity findings must not open. Its combined follow-up brief (`followup-iotext-2.md`: T-1..T-9 from the live probe, P-1..P-3 from the perf review) and the probe fixture carry to the next run.

- DF-PLAN-INTROSPECT-1 carry-over (06:17): the re-check's L-101 is a regression my own perf ruling R-5 caused. Skipping the analyzer for already-analyzed plans let the DataFrame door and the SQL door hash the same query differently. The fix (always analyze) needs the build clone, which IO-TEXT-1 holds until the stop. The unit ends as its pushed branch with a written next-run brief, and no PR opens with a known P1. Lesson: a perf ruling that changes what a hash covers needs a cross-door equality pin before it ships.

## 4. Owner questions (with recommendations)

- **Q-15B-1: ORC and XML readers/writers.** An engine-native ORC or XML path needs a crate the workspace does not depend on
  (`orc-rust` for ORC; `quick-xml` for XML), which is a dependency decision. Tonight they are dated DECLARED refusals with Spark's
  classes (`IO-ORC-1`, `IO-XML-1`, BACKLOG). Recommendation: approve `orc-rust` for a read-only ORC scan in 1.5, since ORC is
  common in Hive-era lakes and the crate is Arrow-native. Keep XML declared until a user asks, because Spark's XML inference has a
  large surface and a thin crate would not reach parity.
- **Q-15B-2: TYPES-GEO-DDL-1.** The spatial DDL arm (`geometry` / `geography` in schema strings) lives in the repark-spark Rust type
  table, outside run 15b's fence. Recommendation: schedule it as a 1.5 Rust card for the run that owns the type table.
- **Q-15B-3: `inputFiles` on Iceberg tables.** DF-PLAN-INTROSPECT-1 answers `[]` for an Iceberg scan. The critic read Spark 4.1.2's
  `Dataset.inputFiles`: it collects only file relations and V2 `FileScan`, so Spark also answers `[]` for Iceberg. Recommendation: keep
  Spark's answer and add a one-line registry note, because users may expect data files. Offering a separate `repark`-namespaced method
  that lists them from the manifest is an owner choice.
- **Q-15B-4: `format('jdbc')` alias.** `spark.read.format("jdbc").load()` still routes to the PostgreSQL connector for any URL,
  while `spark.read.jdbc` now refuses non-PostgreSQL URLs. Recommendation: make the alias apply the same URL dispatch in the 1.6
  connector work, and record the divergence as a registry row until then.

## 5. Lessons

- **Measure the critic's UNMEASURED cells before ruling.** Two live PySpark probes (plan-introspect, IO-TEXT-1) took under three
  minutes each. They closed two critic findings as Spark-matching (cached-child `inputFiles`, `file:///`), widened another
  (Spark never equates two `createDataFrame` frames), and gave every IO-TEXT-1 finding an exact Spark answer to pin.
- **A card that declares a name must first read main's body for it.** IO-DECLARED-1's card would have replaced a working
  `spark.read.jdbc` PostgreSQL reader with a refusal; the audit caught it before review.
- **A perf ruling that changes what a hash covers needs a cross-door pin.** R-5's analyze-skip made the DataFrame and SQL doors
  hash one query differently (DF-PLAN-INTROSPECT-1 L-101).
- **Examples encode behaviour.** #600's `schema_reconcile.py` asserted today's lossy `withMetadata`, and #605 fixed the behaviour,
  so CI's executed-example gate went red only after the rebase. A unit that changes a registry row's answer should grep
  `docs/examples/` for the old answer.
- **One build clone serialises every Rust unit.** Tonight three Rust units (DF-PLAN-INTROSPECT-1, IO-TEXT-1, and freqItems/transpose
  never started) queued behind one another, and two ended as branches. A second build clone would have cost ~40 G against 240 G free.
- **Harness:** parallel Bash calls share one working directory, so use absolute paths; `pgrep -f <path>` matches its own command
  line, so check exit files before removing a clone; Grok rounds can end after one turn with a "starting" hand-back, so check the
  turn count before reading a report; Muse's output log can sit idle through a long `cargo test`, so check the process tree before
  calling a round hung; GitHub's squash endpoint returned a 502 and a GraphQL error back to back, and a bounded retry that
  re-checks state and head before each attempt merged on the first retry.

## Pointers

- Ledgers: `task/ledgers/completed/{row-tuple-1,df-stream-batch-1,types-bases-1,df-surface-a-1,catalog-surface-1,session-surface-1,io-bucket-cluster-1,df-surface-b-1,grouped-surface-1,io-declared-1,column-parity-1}-ledger.md`.
- Branches without a PR: `feat/df-plan-introspect-1` (5d564655), `feat/io-text-1`.
- Registry: `docs/spark-sql-iceberg-parity.md` rows DF-FOREACH-1, DF-OBSERVE-1, DF-METADATA-1, DF-TO-BINARY-1, GROUPED-ARROW-1,
  GROUPED-COGROUP-1, IO-ORC-1, IO-XML-1, IO-JDBC-1, TYPES-GEO-DDL-1 and the unit rows named in each PR.
