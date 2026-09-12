# Overnight report — run 8 of 2026-09-11 (Grok-only workers)

**Session:** one Opus orchestrator, 2026-09-11 16:19 → 22:15 local.
**Grants:** G-1 (squash-merge on green), G-2 (bounded decisions), G-3 (stop 06:30 or list
exhausted — the list was exhausted), G-4 **Grok only** for every worker round (owner, 2026-09-11
afternoon), G-5 (seed commits). **Procedure:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Slates:** [slate 2](cheap-tier-slate-2-2026-09-09.md) rulings S2-16/S2-17/S2-18,
[review-fix slate §3](review-fix-slate-2026-09-10.md), [slate 1 §3 ADAPT-PART](cheap-tier-slate-2026-09-08.md).

## 1. What landed

| # | Unit | PR | Outcome | Rounds |
|---|---|---|---|---|
| 1 | NEVEROOM-1 step 3 — CI golden, the 27-cell CSV pin, Never-OOM at v1.3 as measured (S2-18); unit complete | [#507](https://github.com/TRO-Wolf/repark/pull/507) | **merged `6e13b3de`**, tree-equal | 1 |
| 2 | **v1.3.0 release PR** — the four-file shape, prepared unmerged | [#508](https://github.com/TRO-Wolf/repark/pull/508) | **merged by the owner `0dde3faa`** | 0 (orchestrator) |
| 3 | fork **F-MINIO-QUAY** — the MinIO fixtures move to quay.io | [iceberg-rust#277](https://github.com/TRO-Wolf/iceberg-rust/pull/277) | **merged `5f65759d`**, tree-equal | 0 (orchestrator) |
| 4 | fork **F-WRITE-COMPRESS-1** (S2-16) — INSERT data files carry the table's parquet codec | [iceberg-rust#276](https://github.com/TRO-Wolf/iceberg-rust/pull/276) | **merged `090bc821`**, tree-equal | 1 |
| 5 | **BALLISTA-M2-B** (S2-17) — typed `IcebergTableScan` rebuild; the rebuild-and-compare guard retires | [#509](https://github.com/TRO-Wolf/repark/pull/509) | **merged `a10062b8`**, tree-equal | 2 (+ seed) |
| 6 | **RP-16** — fork pin `85db42f2` → `090bc821`; `PERF-CATALOG-CACHE-WEIGHT-1` closed FIXED | [#510](https://github.com/TRO-Wolf/repark/pull/510) | **merged `864e3483`**, tree-equal | 1 |
| 7 | **AP-2** — `CALL apply_partitioning()` executes a printed plan | [#512](https://github.com/TRO-Wolf/repark/pull/512) | **merged `7c7fa4f1`**, tree-equal | 2 (+1 stall) |
| 8 | **AP-1 re-measure** on the RP-16 pin — AP-1-R-001 stays OPEN | [#514](https://github.com/TRO-Wolf/repark/pull/514) | **merged `c7d34915`**, tree-equal | 2 |
| 9 | This report | [#515](https://github.com/TRO-Wolf/repark/pull/515) | **unmerged, for the owner** | 0 |

Every merge used `gh pr update-branch` → `gh pr checks --watch` → explicit
`gh pr merge --squash --delete-branch --match-head-commit`, never `--auto`, each followed by the
tree-equality check. Three of the last chains needed a branch update and two needed a conflict resolved by hand — a
second session was merging into `main` through the same window (#511 ledger-hygiene-1, #513
ex-29). Both conflicts were in a `map.md` contents list; both were resolved keeping **both**
sides, with `git diff --diff-filter=U` empty and `make check-ledgers` + `make verify` re-run
before the merge commit.

**Grok:** 10 rounds, **$20.86** (`/tmp/grok-worker/runs.tsv`). No Devin, no Muse, no GLM.
One turn-1 stall (AP-2 round 1, `num_turns` 1, placeholder summary) — resumed with the proceed
mandate on the same session and it completed; no second stall, no fabrication seen.

## 2. The three findings worth the owner's eye

1. **Docker Hub dropped MinIO.** `hub.docker.com/v2/repositories/minio/minio/` and `…/minio/mc/`
   both answer 404 (measured 2026-09-11). Every fork integration run had been dying at
   `make docker-up` before any test started, so fork `main` was unpinnable under fork-sync rule 2.
   Fixed in one orchestrator PR by pulling the same release tags from `quay.io`, pinned by digest
   (each manifest fetched anonymously and hashed against its pin). Fork CI is green again.
2. **`rewrite_data_files` writes UNCOMPRESSED files.** The AP-1 re-measure compacted a 3.07 MB
   zstd bed and got **7.93 MB** of live data back. The cause is fork residue
   **F-WRITE-COMPRESS-1-R-002** (`crates/iceberg/src/maintenance/rewrite_data_files_write.rs:69`
   builds default `WriterProperties`); `#276` fixed the INSERT path only and filed four such sites.
   Card **F-WRITE-COMPRESS-2** is written in §5 — it is the next fork unit.
3. **`PERF-CATALOG-CACHE-WEIGHT-1` closed on its own red-when-fixed pin.** Fork `#274` made the
   manifest cache charge real object graphs; the registry's forecast pin went red on the bumped
   pin exactly as designed. Measured by bisection: 256 small tables need **1,071,000** bytes of
   budget to be retained and evict at **1,070,000** (≈ 4,184 B per table, against 768 B estimated
   before) — so the 32 MiB default now holds roughly 5.5× fewer tables than the old estimate
   implied, which is the honest number, not a regression.

## 3. Decisions taken under G-2 (each logged in its unit)

- **NEVEROOM-1 R-2:** a CI-tier cell whose three reps disagree would be left out of the golden as a
  residue row rather than halting. It did not fire — all three cells agreed (`sort` spilled,
  `hash_aggregate` spilled, `hash_join` refused).
- **AP-2 D-8:** `apply_partitioning` takes an optional `target_file_size_bytes`. Round 1's ledger
  found that pair candidates derive from the top three singles at the planning target, so a
  two-field plan's `plan_id` was unfindable at a fixed lookup target — the caller would be refused
  a plan they had legitimately seen. New argument, new refusal clause, two red-first pins.
- **BALLISTA-M2-B seed (G-5):** `iceberg-datafusion` **and** `iceberg` behind the `cluster`
  feature. S2-17 names only the first; the second is needed to name the types the accessors return
  (`Table`, `expr::Predicate`, `spec::Datum`). Both go through the workspace table, so they follow
  the single fork rev and add no sixth `rev` line.
- **F-MINIO-QUAY** was taken as an orchestrator fixture fix (no worker round) because the fork's
  CI could not pass without it and no card covered it.
- **Three M2-B audit findings sent back before the PR:** the wire decided a filter item's kind by
  trial-decoding protobuf (ambiguous, and it silently dropped SQL filters when `Expr` bytes were
  present) → an explicit tag byte; timestamptz literals used `"+00:00"` where the fork's Arrow
  schema emits `"UTC"`; `StartsWith` refused `%` and `_` prefixes but not the backslash, which
  DataFusion's LIKE reads as the escape character.

## 4. Parked for the owner

- **Q-1 (AP-1-R-001, still OPEN).** Should `byte_ratio` multiply the footers' **uncompressed** sum
  rather than the stored file bytes — i.e. predict the *rewrite's* codec rather than the input's?
  Measured both ways this run: projection 1 156 376 against live actuals 7 928 680 / 7 672 169
  (−85.4 % / −84.9 %). The formula was deliberately not tuned; S2-10 fixed the current model, so
  changing it is a ruling, not an actor's call. Note that finding 2 above pollutes the actual: once
  compaction compresses again, this comparison must be re-run before any formula change.
- **The v1.3.0 tag walk** is yours; #508 is merged as `0dde3faa`.
- **STATUS.md** carries no sentence yet for BALLISTA-M2-B, AP-2 or RP-16 — they landed after the
  v1.3.0 release PR was cut, and the file sits near its 25 000 B ceiling (22 959 B after the
  release rewrite). They should ride the v1.4 release PR.
- **Perf review agents** (the 2026-09-02 rule: Rust + Python optimization reviewers beside the
  critic on every unit branch) did not run this session — G-4 restricted every worker round to
  Grok and the critic tier rule forbids Opus reviewers. Say whether you want them back as Grok
  critic rounds.

## 5. Next: card F-WRITE-COMPRESS-2 (fork)

**Why.** `#276` fixed `IcebergWriteExec` only. Four production writer sites still build default
`WriterProperties` (UNCOMPRESSED), and one of them is measured inflating a real table 2.6×.

**Home.** `crates/iceberg/src/maintenance/rewrite_data_files_write.rs` (R-002),
`crates/integrations/datafusion/src/physical_plan/row_lineage.rs` (R-001, COW/MoR rewrite data
files), `crates/iceberg/src/maintenance/partition_key_audit.rs` (R-003),
`crates/iceberg/src/writer/base_writer/position_delete_writer.rs` (R-004, the shared
`position_delete_writer_properties` helper — its callers reuse it, so check each), their `map.md`
files, `task/f-write-compress-2-ledger.md`.

**Decisions.** D-1 each site takes the table's codec through the existing
`parquet_compression_from_properties`, the helper `#276` already added — no second parser.
D-2 the position-delete helper keeps its `statistics_truncate_length` behaviour and gains the codec
(RePark's own `position_delete_writer_properties_for` is the reference shape). D-3 red first: a
compaction pin that a `rewrite_data_files` output file's column chunks carry the table's codec, and
the same for the MoR/COW rewrite path. D-4 no behaviour change other than the codec.

**Consumer.** A pin bump (RP-17, its own PR) and then AP-1's 20 % check re-run a third time — with
compaction compressing, the projection and the actual are finally measured under one codec, which
is the state Q-1 should be ruled in.

## Pointers

- Up: [map.md](map.md) · Runbook: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- Previous: [overnight-report-2026-09-11-run7.md](overnight-report-2026-09-11-run7.md)
- Slates: [slate 1](cheap-tier-slate-2026-09-08.md) · [slate 2](cheap-tier-slate-2-2026-09-09.md) · [review-fix](review-fix-slate-2026-09-10.md)

## 6. Housekeeping

- Lane clones `/tmp/grok-fork-wc`, `/tmp/grok-noom3` and `/tmp/oc-report8` survive the session:
  the harness refuses `rm -rf` on a directory it has treated as a workspace (the same block run 7
  hit with `/tmp/dv-tort5`). Free space at the end: 372 GB. The owner can remove them.
- Stop condition: the scoped list was exhausted at 22:15 local, well before the 06:30 stop time.
  Card F-WRITE-COMPRESS-2 (§5) was deliberately **not** opened — it is outside this run's scope.
