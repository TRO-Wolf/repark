# Morning report: run 21c, 2026-09-17 19:50 → 2026-09-18 07:00 EDT

> **Orchestrating-session note (2026-09-18 07:30 EDT).** Where this report says the campaign directory "came back from an
> earlier copy" or that `/tmp/oc-worker` "was a real directory, not the symlink": that is an inference, and the measurement
> does not support it. The durable directory holds files written in every hour of the night (3 / 7 / 14 / 21 / 35 / 38 /
> 21 / 10 / 9 files for the hours 20:00 through 04:00), and the merge queue inside it was last written at 04:34, so the
> symlink was in place until the freeze. Individual late files are missing — most likely never flushed during four hours
> of memory thrash before the hard reset — and every lane under `/tmp` was lost, as the report says.

**Slice:** maintenance, writer knobs and fixtures, the same boundaries as run 20c. Lane prefix `kc-`.
**Actors:**
- Claude Opus at high effort: sorted-insert, write-options, timestamp_ns, and round 2 of RTAS.
- Muse Spark 1.3 contributor: RTAS round 1, the RDF re-measure, the listing-cost flake.

Grok 4.6 was the reviewer tier.

**The box froze.** It ran out of memory (no swap; OOM kills from 00:10, the desktop froze at about 04:46) and was rebooted at 05:02. The reboot wiped `/tmp`. Only what I had pushed to GitHub survived, plus part of the campaign root: `kc-build/` and `kc-sort/` kept their night files, but `kc-wo/`, `kc-tsns/` and most of the `run21/` reviewer reports and oracle files written after about 20:14 are **gone**. Every figure below comes from GitHub, from the files that survived, or from this report's own record of what I read before the freeze. I flag each one that I can no longer re-open.

## 1. Result by unit

| # | Unit (rating row) | Tier / rounds | PR | State at 07:00 |
|---|---|---|---|---|
| 1 | ICE-RTAS-OPS-2 (V2-24) | Muse r1 + Opus r2 | **#693** | open, CI all green at `ee391835`, not queued (local gates lost; §3) |
| 2 | ICE-SORTED-INSERT-1 remediation (V2-12, C-7) | Opus r3–r4 (after Muse r1–2 in 20c) | **#691** | **MERGED `3bd667e6`**, tree-equal |
| 3 | ICE-WRITE-OPTIONS-1 finish (V2-29) | Muse r1–3 (20c) + Opus r4–r5 (+r6 lost) | **#689** | open, **conflicts with main** (§3) |
| 4 | C-6: RDF xfail re-measure → ICE-RDF-FORK-ASKS-1 | Muse, 1 round | **#686** | **MERGED `e980d945`**, tree-equal |
| 5 | C-4 LISTING-COST-FLAKE-1 | Muse, 1 round | **#688** | **MERGED `6617d215`**, tree-equal |
| 6 | ICE-TSNS-SQL-1 (V3-06) | Opus r1–r2 | **#694** (draft) | draft, orchestrator gates lost (§3) |
| 7 | C-1 ICE-AWS-UNKNOWN-DRILL-1 | — | — | not started (not tonight by the list) |

**Comment gate** (`comment_ban.py` against `origin/main`) was 0 hits on every PR head: #686, #688, #689 (`9dc9bef3`), #691, #693 (`0fa85177`, then `ee391835`) and #694 (`cf87bb56`). No round was rejected for comments tonight.

## 2. What landed

- **#686 ICE-RDF-FORK-ASKS-1.**
  - I measured card C-6's premise before briefing anyone, and it was false. On a release native from main `71482620` (RP-23 on main), `test_ice_rdf_options_1.py` + `test_rdf_schema_evo_1.py` gave **129 passed, 1 skipped, 13 xfailed, 0 XPASS**, so there was nothing to un-mark.
  - The unit became a docs unit. The three xfail reasons that had no registry row are now OPEN fork-ask rows: `ICE-RDF-GRANULARITY-1`, `ICE-RDF-COW-BYTES-1` and `ICE-RDF-RPD-COMMITS-1`, beside `ICE-RDF-DANGLE-2`. Each cites its fixture numbers; I re-checked every number against the JSON.
  - CI first failed on ledger grammar (the attestation was not fenced, and pins were cited only from `docs/`). I fixed that and requeued.
- **#688 LISTING-COST-FLAKE-1.**
  - The wall-clock ≤ 2× pin became an exact call count through the existing `CountingCatalog`.
  - Muse measured, instead of assuming, that a provider rebuild makes **0** `load_table` calls because the fork loads tables lazily. The card had assumed one per table. The pin asserts listing 0/1/0 against rebuild 2/1/0 (`list_namespaces`/`list_tables`/`load_table`).
  - The mutation goes red (8 vs 0). Under load it passed 5/5 for the actor and 3/3 for me. `make ci` exit 0.
- **#691 ICE-SORTED-INSERT-1.**
  - Before the actor started I recorded 15 live Spark 4.1.2 cells, reading sortedness from the parquet bytes. They showed three things:
    - Spark's v2/v3 partitioned MERGE, UPDATE and DELETE rewrites are **sorted and stamped**.
    - **Spark's binpack `rewrite_data_files` sorts and stamps too.** This overturned the run-20c critic's L-04 premise.
    - Float order is NULLS FIRST, then values ascending, then NaN last.
  - Round 3 (Opus) did four things:
    - L-01 P1: sorts the v3 lineage rewrite before it stamps.
    - L-02 P1: adds pins that really write and read the parquet bytes.
    - L-03: canonicalizes NaN.
    - L-04: files it as fork ask `F-RDF-SORT-STAMP-1`. It also found a second fork gap, `F-COW-UPDATE-STAMP-1`. Both have strict xfails.
  - Verification critic 1 CLOSED L-01..L-05 and opened V-01 P2: the *unsorted* lineage path had become an unbounded collect.
  - Round 4 made that path stream again, with a Rust pin that tells the two arms apart. Verification critic 2 CLOSED V-01..V-04.
  - Gates on the head rebased onto `6617d215`:
    - facade **9745 passed / 0 failed**;
    - parity 756 passed;
    - `make verify` exit 0 with 3833 Rust tests. The first run was OOM-killed, when three verify runs were going at once.
    - The live fixture `check` replayed on Spark: "oracle matches the checked-in truth".

## 3. Open PRs: exactly what each still needs

- **#693 ICE-RTAS-OPS-2.** The branch merges cleanly with main.
  - **Round 1 (Muse).** Sets `with_replace_write(or_replace)` on both staged arms of the Spark door. I stopped the round at about 5 h, after it had looped on `make verify` for hours under box load; its content was staged, and I committed it.
    - On head `888d7143`: facade 9692 passed, with 2 environmental failures that pass alone; parity 756 passed.
    - Grok logic critic: **PASS**, with L-01 P2 (the native ANSI door still recorded `append`) and L-02 P2 (a service-managed new-table RTAS still recorded `append`, or no snapshot).
  - **Round 2 (Opus).** Fixes both doors.
    - The service-managed arm goes through the fork's **public** `overwrite_files().overwrite_by_row_filter(AlwaysTrue).allow_empty_commit()`, via `repark_iceberg::write::commit_replace_write`.
    - Grok verification critic: **CLOSED**. It confirmed the API is public, so this is not a fork-rule-3 patch, and that the nightly AWS replace-twice test is unaffected.
  - **Needs:** the whole facade suite, parity and `make verify` on the round-2 head `0fa85177`. My run of those died in the freeze. After the reboot I cancelled a re-run: both shared build slots were held by the sibling runs, and about 1.5 h of gates at 6 jobs could not fit before 07:00.
  - CI on the PR then went red on `python/dbt-repark/tests/test_gold_models.py`. That test pinned `append` for a dbt table model, which dbt builds as `CREATE OR REPLACE … AS SELECT`, so it now records `overwrite`, Spark's answer. That is a real consequence my local gates would not have caught, because the lane gate does not run the dbt suite.
  - I fixed it in `ee391835` (the test plus `dbt-repark/tests/map.md`; comment gate 0).
  - **CI is now all green:** Python, Rust test (workspace), Rust lint, build + import smoke with the dbt adapter, repo guards, cargo-deny, typos, taplo, zizmor.
  - It is not queued, because its local gates were not completed. *Recommendation:* merge it; CI covers everything except the PySpark-gated cells and parity, which passed on the round-1 head.
- **#689 ICE-WRITE-OPTIONS-1.** This was verified and fully gated, but it is **not mergeable**.
  - Verification record:
    - Live tier 52/52 cells matched on three recorders.
    - Grok verification critic 2: CLOSED.
    - facade 9753 passed, with 1 environmental failure that passes alone.
    - parity 754 passed, with 2 environmental failures that pass alone.
    - `make verify` exit 0, 3827 Rust tests.
  - Its merge driver's update-branch **conflicted** with main after 21a's #682 (ice-dyn-overwrite-1) and #687 (ice-evo-dml-1) landed. The conflict is in 13 files: router, insert_overwrite, insert_by_name, session, python lib, `writer_readwriter.py` and the size caps.
  - I squashed it locally and started an Opus round 6 to rebase it. That round was **lane-only and was lost** in the freeze. GitHub still has the unsquashed `9dc9bef3`.
  - **Needs:** a rebase round that keeps three things together:
    - this unit's options threading;
    - #682's dynamic-overwrite path, which must honour the options or refuse them loudly (Q-21c-5);
    - #687's evo-DML planning.
  - Then re-gate and merge. This is design-sensitive; Opus tier.
- **#694 ICE-TSNS-SQL-1 (draft).** The branch merges cleanly with main.
  - Round 1 (Opus) implemented the whole SQL-door contract (ruling Q-21c-6) with a PyIceberg 0.12.0 read-back oracle.
  - Grok logic critic: NEEDS_REMEDIATION.
    - L-01 P1: SQL `CAST(ns_col AS TIMESTAMP)` was a no-op while the DataFrame spelling narrowed.
    - L-02 P2: VALUES and SELECT stored different values for the same TIMESTAMP.
    - L-03 P3: EXPLAIN did not lower the new casts.
  - Grok Rust perf: PASS, with P-01..P-04 P2. Two of those were costs on non-ns paths: a VALUES matrix clone and a MERGE AST clone.
  - Round 2 (Opus) fixed all of them under rulings Q-21c-8 and Q-21c-9. Verification critic: **CLOSED**, no new P1 or P2.
  - Actor gates: `make verify` exit 0, targeted subset 1324 passed.
  - **Needs:** the orchestrator's whole facade, parity and `make verify` on `cf87bb56`, which the freeze cut off. Then mark it ready and queue it.

## 4. Measurements taken before any actor touched the units (live PySpark 4.1.2 + Iceberg 1.11.0)

1. **Column-def `CREATE OR REPLACE TABLE t (cols)` commits no snapshot** in Spark, on all three shapes. This cut the card's third RTAS site (Q-21c-1).
2. **Sorted-insert: 15 cells**, including binpack rewrite sorted and stamped (Q-21c-2). They were committed as a fixture with a replayable recorder.
3. **Write options: 9 snapshot-property collision cells.** Spark refuses only keys the engine actually computed for that snapshot: `deleted-data-files` *lands* on an append. RePark's round-3 rule was a prefix sweep, so this became finding V-04, and the cells became fixture `COLL-00..08`.
4. **timestamp_ns** (Spark cannot read or write it):
   - RePark's SQL-door red map.
   - A PyIceberg 0.12.0 read-back of a RePark-written v3 ns table: exact nanoseconds, correct day boundary. This answers the rating's "nothing else can read it" for the read direction.
   - PyIceberg 0.12.0 **cannot write v3** (apache/iceberg-python#1551).

The raw transcripts were under `run21/oracle/` and did not survive the reboot. The committed fixtures in #691, #689 and #694 carry the measured cells.

## 5. Rulings

- **Q-21c-1.** Only four RTAS xfails flip. `test_dataframe_writeto_appends_by_name` is F-DML-FIELD-ID-1. The column-def replace takes no opt-in (measured).
- **Q-21c-2.** Spark's binpack sorts and stamps. RePark files a fork ask plus a strict xfail and does not patch locally (rule 3).
- **Q-21c-3.** Only for the RTAS unit: test-only (#688) and docs-only (#686) units got no Grok review. The orchestrator re-checked the mutation or every cited number instead.
- **Q-21c-4.** Card C-6 becomes a docs unit, because the measurement closed nothing.
- **Q-21c-5.** A write path that cannot honour a non-empty write-options map refuses it (`refuse_if_non_empty`) and never drops it.
- **Q-21c-6.** The timestamp_ns SQL-door contract: the Iceberg spec plus a PyIceberg read-back is the oracle; five clauses; `DESCRIBE` names and the write-direction oracle are out of scope.
- **Q-21c-7.** Target file size is honoured at RP-23's 1000-row-slice granularity (Java `ROWS_DIVISOR`). The round-1 per-batch tests were stale; they had never compiled.
- **Q-21c-8.** The SQL type *name* decides precision: `TIMESTAMP` is always Spark µs LTZ.
- **Q-21c-9.** Two confirmations. `.cast("timestamp")` of an ns column now floors and localizes like the SQL cast; Arrow's plain cast was silently wrong outside UTC. A bare string into an ns column is assigned at nine digits.

## 6. Owner questions (each with my recommendation)

1. **Q-21c-O1: the box.** Tonight's failure was memory, not CPU: about nine concurrent cargo test and link jobs (89 `rust-lld` at 75 GB in one dump). Earlier in the night I had also seen facade suites that looked hung. They were not deadlocks. The last running test in every run was `test_perf_ice_catalog_io_1.py::test_peak_rss_over_five_hundred_tables_*`, which creates 500 tables with an fsync each and crawled under shared I/O while the other xdist workers sat idle; I killed two such runs at 22:35 before understanding this.
   *Recommendation:* keep the new caps (two cargo slots box-wide, `-n 4`). Move that 500-table test behind a perf marker that is off in the lane gate and on in CI and nightly.
2. **Q-21c-O2: `/tmp/oc-worker` was a real directory tonight, not the symlink** the preamble described. Everything written after about 20:14 was lost with `/tmp`.
   *Recommendation:* make `orch-start.sh` assert the symlink before launching an orchestrator.
3. **Q-21c-O3: #689 ownership.** It needs a design-level rebase over 21a's #682 and #687.
   *Recommendation:* the next run gives it to the Opus tier first thing, with 21a's dynamic-overwrite ledger in the brief.
4. **Q-21c-O4: fork asks filed tonight,** for the next fork lane (one lane, then one pin bump, per Q-20b-B):
   - `F-RDF-SORT-STAMP-1`: binpack output sorted and stamped;
   - `F-COW-UPDATE-STAMP-1`: COW UPDATE exec stamps;
   - `F-TSNS-HOUR-1`: `hours()` on ns;
   - the three RDF rows from #686.

## 7. Residues and flakes observed (not fixed)

- `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren` fails whenever a run crosses UTC midnight (20:00–24:00 EDT). It compares the engine's UTC date with the host-local `date.today()`.
- Dead Ivy paths in recorders outside my slice: `_record_ice_nan_pushdown_1.py:128` and `_record_ice_hadoop_vn_1.py:57` point at `/tmp/ib-scratch/.ivy2` (21b's area).
- `parity torture/test_torture_v3_dv.py` re-copies a fixed host-wide directory, so concurrent runs on one host clobber each other.
- `task/ledgers/staging/map.md` on main carries the `array-null-1` entry three times.
- In the write-options overwrite family:
  - (P3) `deleted-*` / `removed-files-size` are over-refused when an overwrite removes nothing.
  - (P3, V2-01) `partitions.*` is under-refused, but only when `write.summary.partition-limit > 0`.

## 8. Costs, STATUS.md, Rust-first, disk

- **Grok costs:** `/tmp/grok-worker/runs.tsv` was wiped. The one cost I recorded before the freeze is write-options verify-1 at **$0.79** (27 turns). About 9 more Grok reviews ran:
  - sorted-insert: verify ×2;
  - write-options: verify-2;
  - timestamp_ns: logic, perf, verify;
  - RTAS: logic, verify;
  - and the one above.

  Their individual costs are lost. The Opus actor `total_cost_usd` values were in round logs; only `kc-build/round-2.log` (RTAS round 2) survived.
- **STATUS.md:** no edit. None of my rows (V2-12, V2-24, V2-29, V3-06) is a clause of the "Iceberg rating residue" entry. Suggested lines for the orchestrating session:
  - V2-12 closed by #691 (RePark-owned writers), with two fork asks;
  - C-4 flake closed by #688;
  - three new RDF fork-ask rows from #686.
- **Rust-first:**
  - all product changes are Rust: `ctas.rs`, `create_table.rs`, `overwrite_commit.rs`, `row_lineage.rs`, `canonical_float.rs`, the options channel and collision rule, and the ns casts and conform;
  - Python only forwards (the options dict) or tests.
- **Disk:** `/` had about 1.2–1.3 T free all night. At the reboot the lanes died with `/tmp`, so no reclaim is needed.

## 9. Late gate results and the AWS nightly

- **aws-acceptance nightly:** run `35326252631` on main `3bd667e6` (the first on RP-23 and RP-24, and it includes #691 sorted-insert) ended **success**. The acceptance module passed 10 in 295 s, and dbt gold acceptance passed 1. RTAS (#693) is not on main yet. Its replace-twice helper change and the dbt `overwrite` expectation get their first real Glue / S3 Tables proof on the first nightly after it merges; nothing in the gold acceptance asserts the operation name, only snapshot counts.
- **#694** CI: all 9 required checks pass. It is still a draft for the reasons in §3.
- **#689**: still conflicting with main; no CI run on a merge.
