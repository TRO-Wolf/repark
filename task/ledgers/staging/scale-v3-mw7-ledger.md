# Charter ledger — SCALE-v3 · the MW-7 `10^7 x 50` scale workload on a format-v3 table

**Date:** 2026-09-02 · **Branch:** `feat/scale-v3-mw7` · **Base:** `origin/main` `cda526e` ·
**Model:** claude-opus-5 (medium) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md) ·
**Path:** STANDARD.

**Retired:** moved to `../completed/` in this unit's last commit.

**Why now.** North star §3 row "Scale" is the last ⚠ that needs no fork work: MW-7 measured
`1e7 x 50` on format v2 (2026-08-24), and v1.0 requires the same measurement on v3.

**Not in this unit:** any engine change under `crates/`; a new probe; a third compacting leg;
`.github/`; dependency files.

## PROPOSITION LEDGER — SCALE-v3 — 2026-09-02

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The driver takes a `format_version` threaded from `run_mw7.py --format-version {2,3}`, default 2 (every prior invocation unchanged); `3` arms `repark.sql.allowCreateFormatVersion3` on the session. On v3 the MoR leg writes file-scoped Puffin deletion vectors and the COW leg keeps `_row_id`; `rewrite_position_delete_files` refuses on live DVs (`B-MOR-3`) and the driver records the refusal instead of raising, so the other four procedures are still measured. | Six pins in `test_mw7_scale_smoke.py`, one of them the live oracle at matched layout; mutation N red of M. | **PROVEN** | §1, §2. Citation: `python/repark/tests/map.md`, `python/repark-parity/bench/mw7/map.md`. |
| C-002 | The `1e7 x 50` workload runs on v3 at the v2 knobs (8 partitions, `--touch-fraction 0.02`, checkpoints every 10, 7 reps, 4 MiB target) on a quiet box, both legs, and the v3-vs-v2 ratios are recorded from counts. | The run JSON, the census and timing tables, the ratio table. | OPEN | §3 (pending the run). |
| C-003 | North star §3 "Scale" carries the dated v3 numbers and the evidence path; `docs/design/format-v3-track.md` §5 Step 6, STATUS and `python/repark-parity/bench/mw7/map.md` are in lockstep; this ledger moves to `completed/` last. | `make check-map-sync check-ledger-grammar check-ledgers check-docs-compaction`. | OPEN | §4 (pending the run). |

## 1. The knob, measured at smoke scale (20,000 rows x 6 MERGEs, 2 partitions, 256 KiB target)

| Leg | Checkpoint | v2 data / delete / delete records | v3 data / delete / delete records |
|---|---:|---|---|
| MoR | 0 | 2 / 0 / 0 | 2 / 0 / 0 |
| MoR | 3 | 8 / 6 / 1,200 | 8 / 2 / 1,200 |
| MoR | 6 | 14 / 12 / 2,400 | 14 / 2 / 2,400 |
| COW | 6 | 4 / 0 / 0 | 4 / 0 / 0 |

**The v3 shape.** Delete files hold at the seeded data-file count (one DV per data file that
carries deletes) where v2 grows `partitions x merges`; the delete RECORDS grow at the same
rate on both. Every v3 delete file is content 1, `PUFFIN`, and names exactly one live data
file through `referenced_data_file`.

**The maintenance divergence, recorded not tuned.** On the v3 MoR leg
`rewrite_position_delete_files` refuses:

> `CALL rewrite_position_delete_files found 2 live Puffin deletion vector(s) on ns.t and will
> not report a partial result … B-MOR-3 stays.`

That is registry row `B-MOR-3`, already dated. The driver records the refusal on the step
(`refusal` field, armed only at `format_version >= 3`) and runs the remaining four
procedures; on v2 the exception still propagates, because a refusal there would be a defect.

| v3 MoR maintenance step | result | data / delete / delete records after |
|---|---|---|
| `rewrite_position_delete_files` | REFUSED (`B-MOR-3`) | 14 / 2 / 2,400 |
| `rewrite_data_files` | rewrote 12 → 2, `removed_delete_files_count` 0 | 4 / 2 / 2,400 |
| `rewrite_manifests` | 9 → 1 | 4 / 2 / 2,400 |
| `expire_snapshots` | 12 data + 5 position-delete + 31 manifests + 9 manifest lists | 4 / 2 / 2,400 |
| `remove_orphan_files` | 0 rows (24-hour floor) | 4 / 2 / 2,400 |

The two surviving DVs cover the two seeded data files, which are in the bin-pack band and
12 % deleted — below Java's 0.3 delete-ratio clause, so `rewrite_data_files` correctly leaves
them. `COUNT(*)` is 20,000 at every row above.

## 2. The live oracle at matched layout (4,000 rows x 3 MERGEs, 2 partitions, 256 KiB target)

PySpark 4.1.2 + Iceberg 1.11.0, `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `REPARK_PARITY_LIVE=1`.

| Reading | RePark | PySpark 4.1.2 |
|---|---|---|
| delete files `(content, file_format, record_count)` | `[(1, PUFFIN, 120), (1, PUFFIN, 120)]` | `[(1, PUFFIN, 120), (1, PUFFIN, 120)]` |
| data files | 8 | 8 |
| `COUNT(*)` | 4,000 | 4,000 |

Equal on every cell. Pinned as `test_v3_delete_file_layout_matches_live_spark`.

## 3. MEASUREMENTS — 1e7 rows x 50 MERGEs on format v3

Pending.

## 4. Lockstep

Pending.
