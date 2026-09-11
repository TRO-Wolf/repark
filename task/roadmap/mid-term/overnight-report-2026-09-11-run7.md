# Overnight report — run 7 of 2026-09-11 (the Devin-first run)

**Session:** Opus 5 orchestrator (unit `overnight-7`), alone on the box, 00:57 → ~08:10 local (stop time 07:30 for opening lanes; the one lane open at 07:30 was finished) ·
**Grants:** G-1, G-2, G-3 stop 07:30 local, G-4 Devin SWE-2 default (every M round, first try of
every I round), Grok fallback per S2-15, Muse at most one lane / four rounds only after Devin and
Grok fail an I round, no GLM; G-5 · **Order:** REVIEW-FIX-15b, TORTURE-1 steps 3–5, AP-1 step 2,
BALLISTA-M2-A steps 1–2, NEVEROOM-1 steps 2–3 (alone) · **Cards:**
[cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md) (S2-8…S2-15),
[review-fix-slate-2026-09-10.md](review-fix-slate-2026-09-10.md) §3.

## 1. What landed

| # | Unit | PR | Result | Worker | Rounds |
|---|---|---|---|---|---|
| 1 | REVIEW-FIX-15b (S2-14) — `_pins` bindings deleted, citations in `tests/map.md` | [#496](https://github.com/TRO-Wolf/repark/pull/496) | **merged `de5835ca`**, tree-equal | Devin | 1 |
| 2 | TORTURE-1 step 3 — `secrets` family, `flag_secret_columns` off/warn/refuse | [#497](https://github.com/TRO-Wolf/repark/pull/497) | **merged `3f4a8adc`**, tree-equal (one CI red: byte tripwire, re-hashed by the orchestrator) | Devin | 1 |
| 3 | AP-1 step 2 — footer byte ratio; residue AP-1-R-001 | [#498](https://github.com/TRO-Wolf/repark/pull/498) | **merged `ecdcc58b`**, tree-equal | Devin | 1 |
| 4 | TORTURE-1 step 4 — `v3_dv` family, 315 KB Puffin-DV fixture | [#499](https://github.com/TRO-Wolf/repark/pull/499) | **merged `65116847`**, tree-equal | Devin (alone, one JVM) | 1 |
| 5 | TORTURE-1 step 5 — full tier at 1M rows (release), S2-12 measured | [#500](https://github.com/TRO-Wolf/repark/pull/500) | **merged `75406e42`**, tree-equal | Devin (alone) | 1 |
| 6 | BALLISTA-M2-A steps 1–2 — delegating `PhysicalExtensionCodec`, `IcebergTableScan` crosses | [#501](https://github.com/TRO-Wolf/repark/pull/501) | **merged `e756c8e0`**, tree-equal | Devin (step 1 + one audit remediation + step 2, one resumed session) | 3 |
| 7 | NEVEROOM-1 step 2 — the 27-cell matrix (release, 3 reps) | [#503](https://github.com/TRO-Wolf/repark/pull/503) | **merged `f7a7b61f`**, tree-equal — 18 refused, 6 completed, `hash_join`-4× **KILLED** 3/3, `hash_aggregate`-2× and `window_unbounded`-2× **UNSTABLE**; no stable spill at 1 GB | Devin | 1 |
| — | NEVEROOM-1 step 3 | — | not opened — past the 07:30 stop (step 2 handed back at 07:33) | — | — |

Every merge followed runbook §5 with auto-merge off: `gh pr update-branch`, checks watched to
completion, explicit squash with `--match-head-commit`, then the tree-equality check against the PR
head. The whole parity suite and `make verify` were re-run by the orchestrator before every push.

## 2. Workers and cost

Devin SWE-2 (`swe-2-high`, free tier) took every round. **Devin → Grok switches: none** — no
Devin round misbehaved (every round concluded with commits, a real hand-back, no
`ask_user_question` stall). **Grok rounds: 0. Muse rounds: 0** (of the four allowed). GLM: 0.
REVIEW-FIX-15b was Devin's acceptance card for this run: one commit inside Home, no comment lines,
author and committer `%ae` byte-exact, trailer last, red-first evidence pasted — passed.

| Lane | Agent steps | Tool calls | Prompt tokens | Completion tokens |
|---|---|---|---|---|
| dv-f15b | 25 | 38 | 1.70 M | 21.5 k |
| dv-tort3 | 100 | 127 | 16.33 M | 69.7 k |
| dv-ap1 | 125 | 156 | 20.37 M | 69.2 k |
| dv-tort4 | 78 | 95 | 12.46 M | 52.8 k |
| dv-tort5 | 73 | 109 | 9.81 M | 48.4 k |
| dv-bm2a (step 1) | 102 | 115 | 16.65 M | 86.2 k |
| dv-bm2a (audit remediation, resumed) | 87 | 85 | 11.43 M | 59.7 k |
| dv-bm2a (step 2, resumed) | 123 | 120 | 18.10 M | 79.2 k |
| dv-noom2 | 115 | 132 | 18.07 M | 76.3 k |

## 3. Decisions taken under G-2 (one line each)

- **D-4a (TORTURE-1 step 3):** "exact-name match only against the existing needle set" = `prop_key_is_secret` applied to each top-level column name as the schema spells it; no new needle list, no nested walk, no value inspection.
- **AP-1 D-4 (formula, S2-10 as written):** `byte_ratio = Σ compressed / Σ uncompressed` column-chunk bytes over every live data file's footer; projection = `Σ file_size_in_bytes × byte_ratio`; fallback constant 0.55 on any unreadable footer.
- **AP-1 D-5:** the ratio rides every row's `notes` as `byte_ratio=<r> (footers|fallback)`; no new frame column (D-1's columns are a public shape).
- **TORTURE-1 D-7 (S2-12):** a `datasets/` family retires only when every shape is covered AND it has no consumer outside its own tests.
- **Accepted out-of-Home edits:** `.typos.toml` (one exclude line for the committed binary DV fixture, #499); `python/repark/tests/test_production_file_size.py` + its map row (orchestrator re-hash of `_CSV_NATIVE_OPTION_KEYS` / `_JSON_NATIVE_OPTION_KEYS`, #497); `task/ledgers/completed/map.md` REVIEW-FIX-15 entry reworded to S2-14 (#496).
- **Card fact corrected:** the run prompt said "three parity tests"; the tree had four `_pins` bindings, all in `test_dl_2_ledger_grammar.py` — all four went.
- **BALLISTA-M2-A audit (not a switch):** the step-1 encode path scraped the scan node's `Debug`/`Display` text; the orchestrator sent it back in the same session (F-1/F-2) for an encode-time identity check (rebuild through the decode path, compare field by field, refuse loud on any difference) plus adversarial pins. The remediation found that on the first commit a plain string predicate encoded into an un-rebuildable spec. The direct `iceberg-datafusion` dependency that would retire the parsing was **not** taken (§6: a dependency beyond the seed) — owner question 7.
- **Stacked step (TORTURE-1 step 5):** branched from step 4's local tip so it could run alone while #499 was in CI; `main` merged in afterwards with a per-file resolution (ours + main's AP-1 delta; `docs/perf/map.md` unioned) — branch diff re-checked equal to the worker's six files.
- **"Alone" read as "no other worker or local build":** a lane waiting only on remote CI was not counted as open (TORTURE-1 step 4 launched while #498 was in CI; NEVEROOM-1 step 2 while #501 was).
- **NEVEROOM-1 step 2 opened at 06:24**, before the 07:30 stop, as the last scope row; the worker held the matrix for D-3 while a foreign JVM sat on the box (≈06:24–07:00), then ran all 27 cells × 3; the round handed back at 07:33 and was finished (gated, PR, merge chain) rather than abandoned past the stop time.

## 4. Findings and questions for the owner

1. **`INSERT INTO` writes uncompressed parquet** (AP-1 step 2 finding): the fork's
   `iceberg-datafusion` task writer uses `WriterProperties::default()` and ignores
   `write.parquet.compression-codec`, while CTAS and `rewrite_data_files` apply RePark's zstd writer
   properties. That is why the footer ratio reads 1.0 on the synthetic beds and why S2-10's ratio
   cannot predict a codec-changing rewrite (projection error still +76.2 % / +88.0 %, residue
   AP-1-R-001). Question: is the uncompressed INSERT path a fix card (fork or RePark writer
   properties), and does AP-2 then re-measure P-3's byte model on compressed inputs?
2. **AP-1 footer reads are serial** — one ranged GET per data file. Fine for plan-only on local beds;
   slow on an S3 table with thousands of files. A bounded-concurrency follow-up if AP-2 applies plans
   to real tables.
3. **`flag_secret_columns` covers CSV and JSON only** (TORTURE-1 step 3): parquet, `table` and
   format-less `load` refuse the key loud rather than ignoring it; `warn` writes one line to stderr
   (the crate has no tracing dependency). Question: extend the flag to the parquet/table readers?
4. **S2-12 retirement:** zero `datasets/` families retired. `nested` and `secrets` are fully covered by
   the torture generators but still consumed by the dynflatten bench suite, `test_datasets_facade.py`
   and `test_datasets_manifest_types.py`; retiring them needs a rewire ruling.
5. **Runbook grep:** the §3/§4 comment-ban pattern `^\+\s*(//|#(?! noqa))` matches Rust `#[…]`
   attributes; every Rust diff tonight "hit" it on attributes only. Suggest `#(?! noqa|\[)`.
6. **`pgrep -f java` is never empty while an orchestrator runs** — the orchestrator's own command line
   contains `JAVA_HOME`. Briefs now say `pgrep -x java`; the runbook §3 line should too.
7. **BALLISTA-M2-A-R-001 — typed scan accessors.** The fork's `IcebergTableScan` exposes `table()`, `snapshot_id()`, `projection()`, `predicates()`; using them needs `iceberg-datafusion` as a direct optional dependency of `repark-distributed` behind `cluster` (already in the lockfile, via `repark-iceberg`). That retires the text recovery and lets string-literal predicates cross (today they refuse loud; DATE predicates drop at the fork's pushdown; TIMESTAMP string literals cannot form a node). Question: grant the dependency (a G-5 seed line)?
8. **A foreign lane on the box:** `/tmp/dv-nightly` (created 06:16, not this run's) ran a live Spark leg (`test_cutover_schema_1.py` live cells, zulu-17 JVM) at 06:24, beside NEVEROOM-1 step 2. Left untouched. The owner's interactive Devin (pts/15) and Grok (pts/4) sessions sat in the live checkout all night; neither was touched.
9. **NEVEROOM-1 — Never-OOM is not yet true at 1 GB with 4 partitions.** No full-tier cell spills stably: the fair pool's concurrent reservations run out before a spill cycle completes, so loud refusal is the norm (18/27); `window_sliding` and `dynamic_flatten` complete; `hash_join`-4× dies by SIGABRT on an unaccounted allocation (no hash-join spill path, datafusion#24768); `hash_aggregate`-2× races refuse vs spill; `window_unbounded`-2× races the unaccounted `WindowAggExec` cache (datafusion#22758). The card's done-when is unmet by upstream behaviour; step 3 (CI golden) can pin the stable cells. Question: does W-3 take the three failing cells, and should the matrix be re-run at `target_partitions = 1` as a labelled extra?
10. **STATUS.md was not edited** (39 B of headroom): the BALLISTA Milestone 2 opening, TORTURE-1's completion and AP-1's residue have no STATUS sentence yet; each needs a matching cut.

## 5. Mechanics learned

- The merge chain must wait out `mergeStateStatus = UNKNOWN` right after another PR lands; the
  first #498 attempt skipped `update-branch`, and the ancestor check before the squash caught it
  (no bad merge). Fixed in the chain script.
- Run the **full facade suite** before pushing any change to facade source: the byte tripwire
  `test_moved_symbol_bodies_match_the_integrated_baseline` lives there, not in `make verify` or the
  parity suite (#497's one red CI round).
- A step that must run alone can be stacked on the previous step's branch while that PR is in CI;
  the later merge of `main` then needs a per-file resolution (ours + main's other deltas).


## Pointers

- Up: [map.md](map.md) · Previous: [overnight-report-2026-09-10-run6.md](overnight-report-2026-09-10-run6.md)
- Procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
