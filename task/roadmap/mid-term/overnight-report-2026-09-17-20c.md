# Morning report — run 20c, 2026-09-17 (05:07 → 15:00 EDT)

**Slice:** maintenance procedures, writer knobs, `timestamp_ns` on the SQL door, and the aws-acceptance run. This is the same slice as
run 19c (lane prefix `jc-`). **Why the run exists:** the owner ruled on 2026-09-16 at 21:05: "If those complete before 5 AM tomorrow
morning then proceed to kick off anything else we need to get this Iceberg stuff closed out. You can use Muse workers." Run 19 was killed at
00:38 (SIGTERM, cause unknown), so this run first rebuilt run 19c's leftover work from its lanes. **Actor tier:** Muse Spark 1.3
contributor for every unit. **Reviewer tier:** Grok 4.6, read-only. **Siblings:** 20a (read path, DML, conflict detection) and 20b
(identifiers, catalog, registry sweep). **Base at start:** `e100f72d`-era main; fork main `75da2b58`.

## 1. Run 19c's leftover work, rebuilt at 05:10

Run 19c had not written a report, so its state was rebuilt from its lane clones, its Muse hand-backs and the fork PRs.

| Unit | Rating row | State found | Source |
|---|---|---|---|
| ICE-RDF-OPTIONS-1 (owner-named) | V2-08, C-4 | fork #283 open, verification critic CLOSED, CI green; RePark branch at round 2 (11 strict xfails), no RePark review yet | `/tmp/ic-build`, `/tmp/oc-worker/ic-rv/fork-verify-1-report.md` |
| ICE-SORTED-INSERT-1 | V2-12, C-7 | fork #287 open at round 1; round 2 (`8ad199ae`) committed locally, not pushed, not verified; no RePark side | `/tmp/ic-fork2` |
| ICE-TARGET-FILE-SIZE-1 | V2-15, V3-COV-7 | fork branch pushed at round 1, no PR, no review, no RePark side | `/tmp/ic-fork` |
| ICE-RTAS-BYNAME-1 | V2-24 | BY NAME round 1 cut at its 400-step budget, uncommitted work in the tree (saved as `/tmp/oc-worker/run20c/ic-b2.rtas-byname.dirty.patch`); RTAS operations named as fork ask F-RTAS-OPS-1 but not started | `/tmp/ic-b2` |
| ICE-WRITE-OPTIONS-1 | V2-29 | not started | — |
| ICE-TSNS-SQL-1 | V3-06 | not started | — |
| ICE-AWS-ACCEPT-1 | V2-20b, V2-21 | never dispatched by 19c | `gh run list` |

## 2. What landed

### Fork (TRO-Wolf/iceberg-rust)

| PR | Unit | Muse rounds | Merged | Review (Grok 4.6) |
|---|---|---|---|---|
| **#283** | F-RDF-OPTIONS-1: `RewriteDataFiles` options parity with Java 1.11 | 2 (19c) | `d73b914a` 05:38 | 19c: logic L-001 P1 and CI-1, both CLOSED by the verification critic; P3 V-001 is doc drift |
| **#287** | F-SORTED-INSERT-1: `insert_into` sorts by the default order and stamps `sort_order_id` | 2 (19c, pushed and verified by 20c) | `5a0666b9` 06:18 | 19c logic: L-01 P1 (signed NaN vs `Float.compare`), L-02/L-03 P2, L-04 P3. 20c verification: all **CLOSED**, each mutation-proven (4/13, 2/5, 1/13, 1/1 red) |
| **#290** | F-RTAS-OPS-1: a staged CREATE OR REPLACE commits `overwrite` / `delete` like Java's RTAS | 3 (20c) | `de476d1c` 10:36 | Rust perf PASS. Logic L-001 P2 (the replace's live file set was not pinned) and two P3s. Verification **CLOSED**, with the history-fallback mutation red 4/8; new P3 V-001 (sort order not observed) |
| **#288** | F-TARGET-FILE-SIZE-1: rolls at the target on 1000-row slices, dictionary-OFF engine writes, zstd codec stamp on create | 3 (1 in 19c, 2 in 20c) | `4151b488` ~10:55 | logic PASS (P3: `>=` vs `>` cannot be caught by a test); Rust perf PASS (1e7 rows 1554 ms vs 1598 ms, dictionary-OFF 9–23 % smaller) |

Rounds 2 and 3 of #288 were CI repairs with no product-code changes: GAP_MATRIX row collisions, one RPD tail-fixture recalibration for the
1000-row roll step, and the `iceberg-catalog-sql` property expectations that now include the stamp. Ruling Q-20c-3: those rounds did
not get a second verification critic.

### RePark (TRO-Wolf/repark)

| PR | Unit | Muse rounds | State | Review (Grok 4.6) | Gates (release native) |
|---|---|---|---|---|---|
| **#667** | RP-22 pin bump 75da2b58 → **96fc9f1f** (#283, #287, and run 20a's #285) | orchestrator | merged `444323f2` 11:41, tree-equal | — | preflight rc=0; facade 9326 passed; parity 757 passed |
| **#670** | ICE-RTAS-BYNAME-1: `INSERT … BY NAME` on the SQL door | 2 (19c + 20c) | **merged `69ad8c8c`** 14:42, tree-equal | Rust perf PASS; logic **L-001 P1** (`PARTITION` + BY NAME dropped), L-002/L-003 P2, L-004 P3; verification **CLOSED**; P3 residues V-001 (dynamic `partitionOverwriteMode`, owned by 20a) and V-002 (RESET of a builder-seeded `caseSensitive=1`) | facade 9366 passed; parity 757 passed |
| **#672** | ICE-RDF-OPTIONS-1: `CALL rewrite_data_files(options => map(...))` | 3 (19c + 20c) | queued (see §9) | Rust perf PASS (P3s); logic L-01..L-04 P2 and L-05..L-07 P3; verification **CLOSED** (live Spark overturned the L-01 premise); new P3 V-01 (duplicate check, not reachable) | facade 9449 passed; parity green (two torture cells lost a `/tmp` race and passed on rerun) |
| **#671** | RP-23 pin bump 96fc9f1f → **4151b488** (#289, #290, #288) | 1 (consequences round) | gates running at the report (see §9) | — | first run: 26 facade reds, all traced to #288 (§4) |
| — | ICE-SORTED-INSERT-1: V2-12 RePark pins on both doors, plus `sort_order_id` stamped at three RePark-owned writer sites | 2 | **carried**: branch `feat/ice-sorted-insert-1` at `96d784ba`, not merged — the review found two P1s | Rust perf PASS (one P3). Logic **NEEDS_REMEDIATION**: L-01 P1 (v3 partitioned MERGE/UPDATE/DELETE stamps `sort_order_id` on unsorted bytes), L-02 P1 (the MERGE pin is a no-op and two of three stamp knobs stay green on revert), L-03..L-05 P2 | `make verify` red on ledger grammar; facade and parity not reached |
| — | ICE-WRITE-OPTIONS-1 | 3 | **carried**: branch `feat/ice-write-options-1` at `d4ea3b52`, round 3 implemented, not gated or verified | logic **L-01 P1** (user-typed `OPTIONS` honoured on the SQL door), **L-02 P1** (UTF-8 corrupted), L-03/L-04 P2; perf P-01..P-04 P2. Round 3 answers all of them (Q-20c-4…6) | Muse's own lines only: 42 passed on the unit file |

Grok cost for the run: **$10.82** over 14 rounds (sort verify $1.00, TFS logic $0.86, TFS perf $1.03, RDF logic $0.49, RDF perf $0.35,
RDF verify $0.63, BY NAME logic $1.27, BY NAME perf $0.44, BY NAME verify $0.68, RTAS logic $1.50, RTAS perf $0.65, RTAS verify $0.96,
write-options logic $0.53, write-options perf $0.43). The two sorted-insert RePark reviews started at 13:46 are not counted.

## 3. The AWS acceptance run (ICE-AWS-ACCEPT-1, rows V2-20b and V2-21)

- **Dispatched once,** at 05:40, as run `35207108462` on main `c42a4692`. That main carries #665 (RP-21: NaN pushdown #284, Hadoop vN
  #286), the first merged Iceberg fix. Result: **success**. The acceptance module had 10 passed in 381 s, and dbt gold acceptance
  1 passed.
- **Measured correction to the rating:** the rating said "no aws-acceptance run exists at the current revision (last: run 34901483202 on
  `0b33f5b7`)". That was already out of date. The workflow runs **nightly on a schedule**, and every recent run is green: `34949166588`
  (09-15), `35075929450` (09-16), and `35201705355` (09-17 04:48 on `79e328f2`).
- **No approval gate exists:** the `aws-acceptance` environment has one protection rule, a branch policy, and no required reviewer. The
  run started on dispatch. One Slack note went to the owner anyway.
- **The next scheduled run** (09-18 ~04:48) will be the first on RP-22/RP-23. #667 moved `V3_EXPECTED_SNAPSHOTS_BEFORE_EXPIRE` from 14 to
  13, which the S3 Tables floor check also reads (§4).
- **Not done:** the unknown-outcome drill (a Glue commit whose response is dropped after the catalog applied it). Card below (§8, C-1).

## 4. What the pin bumps needed

Neither bump was a clean five-line change. Every extra change was a direct result of the new pin. Each is listed in its PR body, and none
is a rider.

- **RP-22 (#667).**
  - `_acceptance_v3.py` now expects **13** snapshots before expire instead of 14: the rewrite commits all file groups in one snapshot, as
    Spark does (run 19c measured next-row-id 24 on the one-commit path).
  - `v3_subquery_dml.rs` wraps its awaits in `Box::pin`, because the fork's futures now exceed clippy `large_futures`. This forced a
    re-record of the `v3_lineage.rs` byte-hash check.
  - The root `map.md` wording avoids the CAP-1 carrier literal.
- **RP-23 (#671).** 26 facade reds, in three groups, handled in a Muse round with ledger `rp-23-pin-bump-ledger.md`:
  - **A.** Table-property readbacks now include `write.parquet.compression-codec=zstd`. This is Spark's answer (`ctas_codec_props`).
    14 `sql_harden` verdicts and `create-v3-properties` now move to EQUAL with Spark.
  - **B.** File-layout fixtures were re-derived for the 1000-row roll step without weakening any assertion.
  - **C.** MERGE delete-file counts: diagnosed as layout effects, not a delete-writer regression, from raw DEL counts. Muse's verdict
    was that no STOP was needed.

## 5. Rulings

- **Q-20c-1.** Size ceilings only move down. F-RTAS-OPS-1 round 1 raised three fork legacy ceilings; it was rejected and fixed by moving code
  out (overwrite_files 3429 → 3383, snapshot 3490 → 3486).
- **Q-20c-2.** The empty-commit permission F-RTAS-OPS-1 needs is an explicit opt-in that only the staged replace sets. Every other
  `overwrite_by_row_filter` caller keeps main's `PreconditionFailed`, and a test pins it.
- **Q-20c-3.** A fork round that changes only test fixtures, expectations or GAP_MATRIX numbering, after product code passed logic and perf
  review, does not get a second verification critic (#288 rounds 2 and 3).
- **Q-20c-4.** Write options travel **out of band** (a Python dict into a native entry point), not as an `OPTIONS(...)` clause spliced into
  generated SQL. The text channel let a user-typed `spark.sql("INSERT … OPTIONS('snapshot-property.x'='y')")` add snapshot properties,
  which Spark 4.1.2 never does on SQL. It also bypassed the existing CTAS `OPTIONS` refusal, and its hand-written lexer corrupted UTF-8.
- **Q-20c-5.** A user `snapshot-property.<k>` never replaces a metric the engine computes, and `engine.operation-id` is always RePark's. Round
  3 measured four Spark cells and follows them: metric keys refuse, and `operation` is dropped.
- **Q-20c-6.** gzip with a table-property compression level refuses the same way as the option + option case, before any file is written.
- **Q-20c-7.** `catalog::tests::catalog::listing_cost_list_tables_cheaper_than_provider_rebuild` is a wall-clock ≤2× comparison. It failed
  `make verify` twice under box load and passed 3/3 alone on both clones, so it was not treated as a unit defect. It is a candidate for a
  load-independent rewrite (card C-4).
- **Q-20c-8.** Each pin bump's consequential fixes ride in the bump PR (§4). A pin that turns tests red cannot merge without them, and
  splitting them out would leave main red.
- **Q-20c-9.** RP-22 went to fork main `96fc9f1f` instead of `5a0666b9`, at run 20a's request, so one bump also covered 20a's #285. RP-23
  went to `4151b488`, which covers 20a's #289.
- **Q-20c-10.** Muse added doc comments on RePark files twice (BY NAME rounds 1 and 2: `commit_append_to`, `case_sensitive.rs`). Both times
  the orchestrator removed them in a commit carrying both trailers, using the repository idiom
  `#[allow(clippy::missing_errors_doc)]`, instead of spending another round. Write-options round 1 added 135 comment lines; that round was
  rejected outright and redone.
- **Q-20c-11.** A reviewer's claim about Spark is measured before it becomes a ruling. On RDF L-01, the live oracle showed Spark REJECTS
  `min-file-size-bytes=-1`, the opposite of the reviewer's premise, and the fix follows the measurement.

## 6. Residual matrix — my rows, before and after

"After" is main at the end of the run, measured by the pins and gates above. Rows marked *pending merge* depend on a PR still in the queue
at the report (§9).

| Row | Before (rating 2026-09-16) | After |
|---|---|---|
| V2-08 `rewrite_data_files` options (C-4) | **MISSING**: every key refused | **FIXED** on #672's merge. 16 keys honoured, Spark's errors; RPD keys the fork cannot honour refuse loud; residue ICE-RDF-DANGLE-2 OPEN (2 dangling deletes vs Spark's 0, fork ask) |
| V2-12 sort order on INSERT (C-7) | unsorted, `sort_order_id` NULL | plain INSERT is **fixed and pinned on main** through the fork (#287 via RP-22). The RePark-owned writers' stamp is **carried, not merged**: the critic showed it stamps a sort order on bytes that are not sorted on the v3 partitioned DML path, which is a silent wrong answer of exactly the kind the shape rule forbids. `WRITE-ORDER-TRANSFORM-1` stays OPEN |
| V2-15 target file size | 573,667 B file vs Spark max 57,178 B | fork fixed (#288: 20 files, 23,666–68,446 B vs Spark 20 files, 23,735–57,178 B); reaches RePark with RP-23 #671 (§9). ORC/Avro write formats: DECLARED row lands with ICE-WRITE-OPTIONS-1 (carried) |
| V3-COV-7 CTAS codec stamp | missing | fork fixed (#288); every create stamps zstd; RePark gets it with RP-23 #671 |
| V2-24 BY NAME / RTAS operations | parse error / `append,append` | BY NAME **FIXED** on #670's merge. RTAS: fork #290 merged, reaches RePark with RP-23; the one-line RePark opt-in (`with_replace_write(true)` on the RTAS paths) plus flipping its five strict xfails is the next unit (card C-2) |
| V2-29 write options | EXPERIMENTAL: dropped behind a UserWarning | **carried**: round 3 implemented (out of band, streaming, one-commit CTAS), not yet gated or verified |
| V3-06 `timestamp_ns` SQL door | EXPERIMENTAL | **not started** (card C-3) |
| V2-20b live AWS evidence | none at current revision (rating) | **green** at `c42a4692` (dispatch) and nightly (§3) |
| V2-21 unknown-outcome drill | never met a lost response | **not done** (card C-1) |

## 7. Owner questions

1. **Q-20c-O1 (write-options size ceilings).** Muse's round 3 split three size-capped files (`session/write_options.rs`,
   `session_write_options.rs`, `append_fanout_serial.rs`) instead of raising their ceilings. *Recommendation:* keep the splits. They follow
   the CSV-INFER-PERF-1 precedent, and ceilings only move down.
2. **Q-20c-O2 (dictionary-OFF engine writes).** Fork #288 turns parquet dictionary encoding off for DataFusion-engine writes, because every
   Spark-written oracle file was PLAIN (60/60). Neither Muse nor Grok found the Java setting that does this. Files get 9–23 % smaller on the
   measured shapes, but this is a byte-level change to every INSERT. *Recommendation:* keep it (measured Spark parity and smaller files).
   The next Spark-parity run should find the Java mechanism, so the setting cites a source rather than an observation.
3. **Q-20c-O3 (AWS environment gate).** The `aws-acceptance` environment has no required reviewer, only a branch policy. *Recommendation:*
   leave it. The nightly schedule is the evidence the rating asked for, and the branch policy already keeps it on main.

## 8. Cards for the next run

- **C-1 ICE-AWS-UNKNOWN-DRILL-1 (V2-21).** Add a fault-injected Glue commit to the acceptance suite: send the `UpdateTable`, let the catalog
  apply it, drop the response, and assert `CommitStateUnknownException` carries the operation id and a reload finds the commit. The
  harness has no fault-injection seam today. The first step is an injectable transport in the fork's Glue client.
- **C-2 ICE-RTAS-OPS-2 (V2-24).** After RP-23 merges, call `with_replace_write(true)` on the RTAS paths (`repark-spark/src/ctas.rs:243`,
  `repark-sql/src/create_table.rs:197`, and RTAS on a new table), flip the five strict xfails in `test_ice_rtas_byname_1.py`, and move
  registry RTAS-OPS-1 to FIXED.
- **C-3 ICE-TSNS-SQL-1 (V3-06).** `timestamp_ns` / `timestamptz_ns` on the SQL door. Spark 4.1.2 cannot read these types (measured by the
  rating), so the oracle must be the Java Iceberg API or PyIceberg, not PySpark.
- **C-4 LISTING-COST-FLAKE-1.** Rewrite `listing_cost_list_tables_cheaper_than_provider_rebuild` to count object-store calls instead of
  comparing wall-clock time (Q-20c-7).
- **C-5 ICE-WRITE-OPTIONS-1 finish.** Gate `d4ea3b52` (all three cargo suites, remaining pytest files, `make ci`, live tier), run a Grok
  verification critic against the two report files, and the PR.
- **C-7 ICE-SORTED-INSERT-1 remediation.** L-01: either sort the bytes on the v3 partitioned MERGE/UPDATE/DELETE path before stamping, or do
  not stamp there. A stamp on unsorted bytes is worse than NULL, because a reader may trust it. L-02: make the MERGE pin observe the order.
  L-03: canonicalise NaN on the owned-path sort as the fork does. L-04: `rewrite_data_files` output bypasses both the sort and the stamp.
- **C-6 ICE-RDF-DANGLE-2 / RDF granularity xfails.** Once RP-23 is on main, re-measure the 13 strict xfails. The rolling writer now splits
  rewrite output at the target, so some granularity cells may XPASS and must be un-marked.

## 9. State at 15:00

**Merged in this run:** fork #283, #287, #290, #288; RePark #667 (RP-22) and **#670 ICE-RTAS-BYNAME-1** (`69ad8c8c`, tree-equal, 14:42).

**Left running, deliberately:** three merge watchers as user systemd units — `jc-merge-671b` and `jc-merge-672c`. Each waits
for its line to reach the head of `/tmp/oc-worker/run16/merge-queue.txt`, then checks that every PR check passes, that the head is an
ancestor-descendant of main, and that main's tree equals the PR head's tree after the squash. They may land after 15:00. The orchestrating
session can stop either of them (`systemctl --user stop jc-merge-671b`) without side effects; nothing else is in flight.

| PR | State at 15:00 | Gates |
|---|---|---|
| **#671** RP-23 pin bump to `4151b488` | rebased onto `69ad8c8c` at 14:43 (`0c933ad4`), at the queue head, watcher running | `make verify` rc=0, facade 9345 passed, parity 757 passed |
| **#672** ICE-RDF-OPTIONS-1 | rebased onto current main at 14:41 (`2b30537a`) after a real conflict in `error_map.rs`: main grew a bare `IllegalArgument` variant while this branch carried a marker-carrying one, and the resolution keeps both arms (`IllegalArgument`, `IllegalArgumentMarked`). Watcher running | on the rebased head: repark-core 578 passed, repark-spark 1094 passed, the unit's own files 135 passed / 1 skipped / 13 xfailed. The whole-suite legs (facade 9449, parity) were measured on the pre-rebase head |
| **#670** ICE-RTAS-BYNAME-1 | **merged** `69ad8c8c`, tree-equal | facade 9366 passed, parity 757 passed |
| `feat/ice-sorted-insert-1` | pushed, **no PR** — two P1s (§2) | — |
| `feat/ice-write-options-1` | pushed at `d4ea3b52`, **no PR** — round 3 ungated | — |

**Lanes left on disk for the orchestrating session's reclaim:** `/tmp/ic-build` (44 G, #672's clone — keep until it merges), `/tmp/jc-bump` (#671's clone, same), `/tmp/jc-sort` and `/tmp/jc-build` (the two carried branches),
and `/tmp/jc-report` (this report). Everything else 20c opened was removed at the close.

## 10. STATUS.md lines that need correction (for the orchestrating session)

- The Iceberg maintenance line that calls `rewrite_data_files` options "table properties only" should now name the options map (#672).
- The aws-acceptance line (if it cites run `34901483202` / `0b33f5b7`) should cite the nightly schedule and dispatch `35207108462` on
  `c42a4692`.
- The fork pin line should read RP-22 `96fc9f1f` (or RP-23 `4151b488` once #671 merges).

## 11. Rust-first roll-call

- **RDF options.** Parsing, validation, error text and wiring are all in `crates/repark-spark/src/call/rewrite_options.rs`; the table-format
  semantics are in the fork. No Python.
- **BY NAME.** The strip is in `repark-sql/src/sniff.rs`, the resolver in `repark-spark/src/insert_by_name/`, and the caseSensitive carrier
  is a Rust config extension. Python only forwards `spark.sql.caseSensitive` to the native setter.
- **Sorted insert, target file size, RTAS operations.** All three live in the fork; RePark only stamps the order id, in Rust.
- **Write options (carried).** Round 3 moved the channel from Python text rendering to a native dict entry point. Python serialises nothing
  and branches on nothing.

## 12. Disk

- **Freed:** `/tmp/ic-fork2` (99 G, sort merged); run 19c's Grok scratch trees (`ic-gfork-verify*`, `ic-gsort-*`, ~12 G); every 20c Grok
  clone once its report was read (`jc-gsort-verify`, `jc-gtfs-*`, `jc-grdf-*`, `jc-grtas-*`, `jc-gbn-*`, `jc-gwo-*`, with their target dirs).
  At the close, `/tmp/ic-b2` (#670 merged), `/tmp/ic-fork` (98 G), `/tmp/jc-fork` (59 G), the last two Grok clones and the three
  read-only fork checkouts went too. `df -h /` read 472 G free at the 08:40 low, 614 G at 13:48 and **957 G at 14:46** — about
  **340 G freed** across the run.
- **Kept for the orchestrating session's reclaim:** see §9.
