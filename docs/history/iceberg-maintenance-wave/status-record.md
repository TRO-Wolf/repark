# iceberg-maintenance-wave — STATUS record

## Cut from STATUS.md — closed 2026-08-23 by #224

- **Iceberg maintenance wave (MW)** (chartered 2026-08-21; **closed by MW-5**). Merge-on-read
  was production-grade as a *write* path and fenced off as an *operational* one: the maintenance
  procedures refused on exactly the catalogs holding production data. Design:
  Design and slate:
  [docs/history/iceberg-maintenance-wave/](README.md)
  (archived 2026-08-23). Charter:
  [task/ledgers/completed/mw-0-charter-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-23-mw-0-charter-ledger.md).
  - **Delivered:** MW-0 the measured charter ([#195](https://github.com/TRO-Wolf/repark/pull/195)),
    MW-1 the fence lifted for both catalog policies plus Spark's six-column `expire_snapshots`
    ([#196](https://github.com/TRO-Wolf/repark/pull/196)), MW-2 `rewrite_position_delete_files`
    and Spark's fifth `rewrite_data_files` column
    ([#197](https://github.com/TRO-Wolf/repark/pull/197)), MW-3 `remove_orphan_files`
    ([#198](https://github.com/TRO-Wolf/repark/pull/198)), MW-4 Glue live MOR compact+expire
    ([#218](https://github.com/TRO-Wolf/repark/pull/218)), MW-4b Glue dotted metadata-table
    rewrite ([#219](https://github.com/TRO-Wolf/repark/pull/219)), MW-6 `rewrite_manifests`
    (post-campaign, owner-chartered 2026-08-23). **Six maintenance
    procedures** run through `CALL`; no procedure omits a Spark column. V3-1 adds
    `register_table` (adoption, not maintenance).
  - **Scorecard.** The MW-0 growth demo reproduces: ten sequential MERGEs into a 1,000-row v2
    merge-on-read table, each touching the same 200 ids, grow position-delete files **1→10**.
    After `rewrite_position_delete_files` + `rewrite_data_files` + `expire_snapshots`, delete
    files are **10→1** and data files are **1**. Pin:
    `python/repark/tests/test_mw5_baseline_delta.py::test_mw0_demo_delete_files_grow_then_compact_reclaims`
    (`assert` 10 then 1 deletes; `assert` 1 data file after rewrite+expire). `COUNT(*)` stays
    **1,000** (`int64`) on the Arrow path. Data-file count *before* compact was **41** on this
    host (2026-08-23, logged, not asserted). Wall-clock on this host (not a CI pin): merge 2
    **56.1 ms**, merge 10 **131.3 ms** (**2.3×**, MW-0 was 60.1→127.9 ms / 2.1×), warmed
    post-maintenance **96.6 ms**.
    Scan cost still tracks delete-file growth; compact reclaims the files. It does not restore
    merge-2 wall-clock on this machine, and MW-5 does not claim a timing SLA.
  - **Live Glue proof.** Post-#219 `aws-acceptance` dispatch
    [32640855145](https://github.com/TRO-Wolf/repark/actions/runs/32640855145) on `d3c248c`
    (2026-08-23 12:56Z) is green. OD-3 is owner-executed `s3:DeleteObject` on the warehouse
    scratch prefix. Glue tables still cannot be dropped. **S3 Tables MOR compact+expire is
    out of this campaign** (OD-3 is the Glue warehouse prefix). The 2026-08-23 intake's
    "MW-4b" candidate (S3 Tables MOR leg, needs OD-3b) is a **different id** from campaign
    MW-4b (#219) and is not sequenced.
  - **Divergences that remain rows**, not closed here — `ORPHAN-1`,
    `ORPHAN-2`, `B-MOR-3`, and MW-6's `MANIFEST-1` (delete manifests are not rewritten; Spark
    rewrites them in a second leg), `MANIFEST-2` (`spec_id` refuses; `use_caching` is an
    accepted no-op and takes a boolean literal where Spark also casts a string) and
    `MANIFEST-3` (above `commit.manifest.target-size-bytes` the two engines write a different
    number of manifests, so `added_manifests_count` diverges; the rewritten count matches), and
    MW-7's `RDF-1` (a correctly sized data file whose rows are all deleted is never a
    `rewrite_data_files` candidate, so its dead rows and the delete file covering it are
    retained without bound; Spark reclaims both) in
    [docs/spark-sql-iceberg-parity.md](../../spark-sql-iceberg-parity.md). `MOR-1` retired at
    RP-1 (fork F-1, floor 5). `MOR-2` retired at MW-9 for RePark-owned MERGE (Spark-default
    `file`); SQL `DELETE`/`UPDATE` via the fork `TableProvider` still partition-group. The two result-schema
    gaps the charter queued for MW-5 were **closed in MW-1/MW-2**, not registered. Two of the
    remaining rows (`ORPHAN-1` required `older_than`, `ORPHAN-2` dry-run by default) invert
    Spark's defaults on the one procedure with no undo, under owner decision **OD-2**.
  - **A13** (merged [#217](https://github.com/TRO-Wolf/repark/pull/217)) set
    `register_memory_catalog`'s fallback root to the supplied warehouse. MW-3 still refuses
    orphan cleanup of that fallback tree.
  - **MW-7 scale scorecard (measured 2026-08-24, this host — ratios, not absolutes).**
    1e7-row partitioned v2 table, 50 MERGEs of 200,000 ids each (2 %), 8 partitions, a
    merge-on-read leg and a copy-on-write leg. **The charter said 100 merges; the measured
    projection put 1e7 × 100 at 3.72 h on top of 0.509 h already spent against a ~4 h budget,
    so it ran 1e7 × 50** — the arithmetic is §1 of the ledger. Run wall 2:09:29, **peak RSS
    4,461 MiB** (`getrusage` and `/usr/bin/time -v` agree).
    Merge-on-read grows linearly per merge — four of the five census rates exact: **+8
    position-delete files (one per partition — explicit `'partition'` layout; MW-9's unset
    default is Spark `file`), +200,000 delete records,
    +32 data files, +2 manifests, ~479 manifest-list bytes (mean)**. Its predicate scans reach **4.18×** (point) and **4.58×**
    (partition) the copy-on-write control by merge 50, crossing **2× at 19.6 merges**. The
    copy-on-write control is **flat** over the same 50 merges (1.08× / 1.18×). The gap is the
    delete files **plus the data-file fan-out** merge-on-read leaves behind — at merge 50 it
    also carries 16.3× the control's data files and 1.83× its live bytes, because every MERGE
    appends rather than rewrites; this unit does not separate the two. Copy-on-write pays on
    write:
    MERGE plateaus at **~113 s** against merge-on-read's **~28 s** (4.1×), and its warehouse
    held **14,782 MB for a 342 MB table (43×)** until `expire_snapshots` ran.
    The full maintenance sequence took **142.34 s** on the merge-on-read leg (delete files
    400→8, data files 1,696→170, manifest list 25,665→3,659 B) and **21.2 s** on the
    copy-on-write leg. **It does not close the gap:** 8 delete files holding
    10,000,000 records survive and the table still reads at **2.45× / 2.02×** the control while
    holding 1.90× its live bytes. They are **not dangling** — they name live data files.
    `rewrite_data_files` never selects a delete-laden file, because the fork at `5e7b2e4` defers
    Java's `tooHighDeleteRatio` clause (`DELETE_RATIO_THRESHOLD_DEFAULT = 0.3`) and defaults the
    delete-count threshold to `usize::MAX`, so a correctly sized 100 %-dead file is invisible to
    compaction and its dead rows are retained without bound. Spark ends the same sequence at
    **zero** delete files at both `write.delete.granularity` settings with
    `remove-dangling-deletes` off. Registry row **`RDF-1`**, fork ask **F-16**, ledger finding
    F-MW7-1 (OPEN), pinned by `test_delete_laden_in_band_file_survives_the_runbook`. Driver: `python/repark-parity/bench/mw7/`; machinery pin:
    `python/repark/tests/test_mw7_scale_smoke.py`. Ledger:
    [task/ledgers/completed/mw-7-scale-measurement-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-24-mw-7-scale-measurement-ledger.md).
    **The verdict the charter asked for: MW-9 was urgent** — the point probe went
    **858 → 3,878 ms** for a predicate returning 0.02 % of the rows, because `'partition'`
    granularity opens every delete file in every partition it touches (400 files,
    10,000,000 records, for 2,000 rows returned). **MW-9 is delivered in this PR**
    (unset default is Spark `file`; that measurement is the explicit-`'partition'` layout).
    MW-8's defaults follow from §6 there: run the
    sequence every 10 merges, with merge 20 the ceiling that already measures 2.05×.
  - **MW-8 the maintenance runbook (delivered 2026-08-24).** Docs plus one executable test; no
    engine change. [docs/guide/iceberg-guide.md](../../guide/iceberg-guide.md) "The maintenance
    runbook" is the Airflow-shaped cycle — merge workload →
    `rewrite_position_delete_files` → `rewrite_data_files` → `rewrite_manifests` →
    `expire_snapshots` → orphan dry run → orphan armed — with the cadence, the load-bearing
    order, the delete-file trigger, the day of latency on the orphan net, how to retry a step
    and the six edits a migrating Spark DAG needs. MW-7's numbers are cited to that ledger
    rather than re-homed. **`expire_snapshots` takes an explicit `older_than` here** (Critic
    finding F-MW8-1): the engine passes a cutoff only when the argument is present, so the
    fork falls back to `history.expire.max-snapshot-age-ms` (5 days) and a cycle without one
    reclaimed nothing across three passes while the warehouse grew 6.00×. The cutoff is the
    table's time-travel window, and the guide says what spending it costs. **The runbook's
    stated limit is `RDF-1`:** a cycle does not return a table to baseline, so an operator sees
    a merge-on-read table still reading at **2.02×** (point) / **2.45×** (partition) a
    compacted control and holding **1.90×** its live bytes, with correct answers throughout.
    Pin: `python/repark/tests/test_mw8_runbook.py` (ten clauses, one documented cycle censused
    after every step, 4.4 s; C-010 parses the guide's printed `CALL` block and compares it to
    the measured sequence). Ledger:
    [task/ledgers/completed/mw-8-maintenance-runbook-ledger.md](../../../task/ledgers/archive/2026-08/2026-08-24-mw-8-maintenance-runbook-ledger.md).
  - **Sequenced remainder (owner-chartered 2026-08-23):** RP-1, MW-6, MW-7, MW-8,
    **V3-2** ([#232](https://github.com/TRO-Wolf/repark/pull/232)), and **MW-9**
    ([#233](https://github.com/TRO-Wolf/repark/pull/233) —
    `write.delete.granularity` / `MOR-2` for MERGE) are delivered.
    The queue on [briefs/next-sequence.md](../../../briefs/next-sequence.md) now carries
    **DL-4 (live-document compaction, chartered 2026-08-25) then the Lane A
    remainder, V3E-4 first** (refs + time travel on v3; expiry/orphans
    with real work). V3E-3 merged as [#236](https://github.com/TRO-Wolf/repark/pull/236). V3-3 (deletion-vector writes)
    remains owner-sequenced. The intake S3 Tables MOR leg stays unsequenced.
