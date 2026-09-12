# Unit ledger — AP-1-CLOSE-1 · `projected_files_at_target` is an upper bound, pinned; AP-1-R-001 closes (S2-27)

**Unit:** AP-1-CLOSE-1 step 1 · **Date:** 2026-09-12 · **Branch:** `chore/ap-1-close-1` · **Base:** `origin/main` (fork pin `9e3522e3`, RP-18 merged)
**Model:** swe-2-high
**Policy:** [AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Why now.** Four re-measures (run 7, RP-16, RP-17, RP-18) chased a 20 % target
that no footer-derived projection can meet in both directions: before the fork
fixes the rewrite wrote MORE than its inputs (dead dictionary pages,
uncompressed files), after them it writes LESS (fewer, larger files compress
better). What the inputs' compressed bytes bound is the ceiling. S2-27 retires
the target: `projected_files_at_target` is documented and pinned as an
upper-bound estimate, and residue AP-1-R-001 closes as an estimator property.

**Retires:** this ledger moves to `../completed/` when the unit's last commit
lands.

**Not in this step:** any formula change (D-1: the AP-3 uncompressed-footer-sum
× `byte_ratio` basis stays), any fork or dependency pin, `STATUS.md`,
`briefs/next-sequence.md`, any JVM, any `apply_partitioning` change.

## PROPOSITION LEDGER — AP-1-CLOSE-1 — 2026-09-12

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `plan_note_upper_bound`: `RESIDUE_NOTE` and every frame row's `notes` spell `projected_files_at_target` as an upper bound from the inputs' compressed bytes, and no product text or AP doc/guide still claims a ±20 % prediction. | Update the constant; the every-row notes pin asserts the new spelling; grep `20 %`/`20%` over the AP-1 perf docs, the guide, and product text shows no standing prediction claim. | **PROVEN** | `RESIDUE_NOTE` now reads "AP-1-R-001 closed 2026-09-12: projected_files_at_target is an upper bound from the inputs' compressed bytes — the footers' uncompressed byte sum times byte_ratio, counting compression once (a same-codec rewrite into fewer, larger files does not compress worse); projected_partitions measured exact". `plan_every_row_carries_the_r001_file_count_caveat` asserts the new spelling on every row (28 plan_partitioning lib tests green). Live `--plan` on the rebuilt beds carried the note verbatim on all 59 frame rows with `byte_ratio=0.38 (footers)` / `0.46 (footers)` preserved. `grep -n "20 %\|20%"` over the AP-1 perf docs, the guide and `crates/`+`python/repark/` product text leaves only historical measurement prose inside dated perf docs (each carrying the closing note that retires the check) and a modulo-operator false positive. |
| C-002 | `rp18_three_bed_upper_bound`: on the three AP-0 beds rebuilt with run 8's exact commands on the release module, the unpartitioned row's `projected_files_at_target` × target is ≥ the live actual bytes and ≤ 2× it, and the candidate ranking equals RP-18's frame order per bed. | In-module pin over the measured bed shapes plus a live `--plan`/rewrite leg re-measure; red first by doctoring the bound; paste the red. | **PROVEN** | Red first (doctored the inequality to demand the projection land under the live actual): `rp18_beds_projection_bounds_the_live_actual ... FAILED — RP-18 uniform: projection x target 3145728 under the live actual 1839168`. Restored `actual <= bound <= 2*actual` → green. The in-module pins reconstruct the beds faithfully — 200 batches split at 65 536-boundaries into 206 items per synthetic bed with the measured per-split uncompressed sums, the futures bed's 14 rated columns verbatim — and reproduce RP-18's frame order exactly (uniform: identity(grp), months(ts), years(ts), unpartitioned, …; skewed: months(ts), identity(grp), years(ts), unpartitioned, …; futures: the six months() leads). Live leg on the release module, run 8's exact commands (`run_adaptpart.py --scratch /tmp/ap1r-bed --futures-parquet … --plan`): unpartitioned `projected_files_at_target` = 6 on both synthetic beds (bound 3 145 728) and 51 on futures (bound 26 738 688). Rewrite leg on fresh `_orun` copies: rewritten 206 / added 20 / failed 0 per bed; live actuals uniform 1 840 192 and skewed 1 759 577 (RP-18 recorded 1 839 168 / 1 755 749 — run-to-run zstd variance ~0.1 %, the pin asserts the recorded values). Bounds hold live: 3 145 728 ≥ 1 840 192 / 1 759 577 ≤ 2×; futures 26 738 688 ≥ 26 729 684 live input bytes (the rewrite declines below the 5-file floor) ≤ 2×. |
| C-003 | `r001_residue_closed`: the residue row AP-1-R-001 reads CLOSED (2026-09-12) with the four measurements (run 7, RP-16, RP-17, RP-18: projection basis, projection, actual, Δ) in one table. | The residue lives in `task/ledgers/completed/ap-1-ledger.md` "Step 2 residue" — the card's `docs/spark-sql-iceberg-parity.md` names no such row; the freeze gate admits only a dated errata prepended at the ledger's top, which is where the closure lands. | **PROVEN** | Errata "AP-1-R-001 CLOSED (2026-09-12, AP-1-CLOSE-1 / S2-27)" prepended at the top of `task/ledgers/completed/ap-1-ledger.md` — the residue's registered home (the parity doc is the Spark-divergence registry and has no `plan_partitioning` row; `grep` confirms no AP-1-R-001 there — the card's filename was a mislabel, recorded here as the ruling the RP-16/17/18 convention already set: the residue row is updated wherever it lives). The errata carries the four-measurement table — run 7 (+76.2 % / +88.0 %, stored×1.0), RP-16 (−85.4 % / −84.9 %), RP-17 (−74.2 % / −72.0 %), RP-18 (+54.5 % / +61.8 %, AP-3 basis) — and states the bound and the unchanged ranking. `completed/map.md` rows updated. |
| C-004 | `guide_s224_line_retired`: the maintenance guide's S2-24 known-issues line is retired and replaced by one sentence that compaction is a net-size win on zstd tables at the RP-18 pin, with the projection described as an upper-bound estimate. | Edit `docs/guide/maintenance-policy.md`; the `plan_partitioning` section reads the AP-3 formula plus the upper-bound semantics and no open known-issue remains. | **PROVEN** | `docs/guide/maintenance-policy.md` now documents `projected_files_at_target` as an upper-bound estimate of the rewrite output file count: the footers' compressed and uncompressed column-chunk sums, `byte_ratio` applied once, projected bytes = the inputs' compressed bytes = a ceiling for a same-codec rewrite into fewer/larger files; the 0.55 fallback kept. The known-issues line is replaced by the single sentence that compaction is a net-size win on zstd tables at the RP-18 pin. `docs/guide/map.md` row updated. |
| C-005 | `remeasure_ledger_departs`: `task/ledgers/staging/ap-1-remeasure-ledger.md` moves to `completed/` through `python3 scripts/ledger_lifecycle.py move … completed` (links rewritten by the tool) and this ledger's clauses are all PROVEN. | Run the move; `make check-ledgers` and `make check-ledger-grammar` clean; the four remeasure docs' links resolve. | **PROVEN** | `python3 scripts/ledger_lifecycle.py move task/ledgers/staging/ap-1-remeasure-ledger.md completed` → "moved 1 file(s), rewrote 4 link(s), wrote 4 file(s)". Stale backtick path references the tool does not rewrite (perf docs' inline paths, rp-17/rp-18 `artifacts:` lists) fixed by hand; the frozen `ap-1-ledger.md` keeps its link *text* reading `staging/…` while the target correctly resolves into `completed/` — the freeze gate forbids touching the body beyond link targets. `staging/map.md` / `completed/map.md` updated in lockstep. |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## Red first

The bound pin `rp18_beds_projection_bounds_the_live_actual` (now in `plan_partitioning/tests.rs` — the `mod tests` block moved there so `plan_partitioning.rs` stays under the 1 000-line gate) ran first with the
inequality doctored to demand the projection land under the live actual — the
wrong direction, the failure the card wants to see:

```text
test call::plan_partitioning::tests::rp18_beds_projection_bounds_the_live_actual ... FAILED
RP-18 uniform: projection x target 3145728 under the live actual 1839168
```

The notes pin `plan_every_row_carries_the_r001_file_count_caveat` was equally
red against the base `AP-0-R-001` spelling before `RESIDUE_NOTE` changed:

```text
upper-bound note on every row, got: AP-0-R-001: projected_files_at_target applies byte_ratio …
```

Both went green after the constant moved; the ranking pin reproduced RP-18's
frame order on the first run (the bed reconstruction is faithful, so it could
never have been red — its red would mean the reconstruction was wrong).

## Gates

| Gate | Exit |
|---|---|
| `cargo test -p repark-spark --lib plan_partitioning` | 0 — 28 passed, 0 failed |
| `cargo clippy --locked -p repark-spark --all-targets -- -D warnings -A clippy::disallowed_methods` | 0 |
| `.venv/bin/python -m pytest python/repark/tests -q -k "plan_partitioning or adapt or apply_partitioning"` | 0 — 1 passed, 5 skipped, 6302 deselected |
| `PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q -p no:cacheprovider` | 0 — 747 passed, 1 skipped, 11 xfailed in 67.66s |
| `make check-docs-links` | 0 — 785 files, 5052 links |
| `make check-ledgers` | 0 — 329 ledgers in bins (218 archived), 905 links resolve, frozen rule clean |
| `make check-ledger-grammar` | 0 — 111 live ledgers clean |
| `make verify` | 0 |
| Comment fence (`git diff --cached` grep for added `//`/`#` lines) | prints only four `#[test]` attribute lines — no comments |
| Live leg: release `make develop`, `run_adaptpart.py --plan` on the three rebuilt beds, `_orun` rewrite leg | 0 — frames carry the new note on all 59 rows; actuals 1 840 192 / 1 759 577; bounds hold |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ap-1-close-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-002's pins reconstruct the three AP-0 beds faithfully — 206 items per synthetic bed (200 batches split at the measured 65 536-boundaries with the recorded per-piece uncompressed sums) and the futures bed's full rated-column set — reproducing RP-18's exact frame order and scores, not a sampled prefix.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-2
      status: ATTACKED
      evidence: Live leg re-ran end to end on the release module — run 8's exact commands rebuilt the beds, `--plan` dumped every frame row (all 59 carry the new note), and the `_orun` copies ran ALTER + rewrite_data_files to fresh actuals.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md, python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-3
      status: N/A
      justification: No refusal path or error contract touched — the change is a note string plus in-module pins.
    - id: AT-4
      status: N/A
      justification: No shared mutable state added; `RESIDUE_NOTE` stays a `&str` constant.
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under /tmp/ap1r-bed and /tmp/ap1r-orun; no network, no credential, no JVM, REPARK_PARITY_LIVE unset.
      artifacts: [python/repark-parity/bench/adaptpart/run_adaptpart.py]
    - id: AT-6
      status: ATTACKED
      evidence: Bound values come from the live `files` metadata sums and the frame's own `projected_files_at_target` (6 / 6 / 51), not the notes prose; the rewrite actuals are current-snapshot file-size sums over the live `data/grp=*/` set.
      artifacts: [docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-7
      status: N/A
      justification: No wall-clock claim anywhere in the change.
    - id: AT-8
      status: ATTACKED
      evidence: `git status` shows no Cargo.toml/Cargo.lock/pyproject/uv.lock/.github diff; the only code delta is `RESIDUE_NOTE` plus the two in-module tests.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs]
    - id: AT-9
      status: ATTACKED
      evidence: AP-1-R-001 closed in its registered home (completed ap-1-ledger prepended errata with the four-measurement table); every AP-1 perf doc carries the dated closing note; the guide's S2-24 line is retired.
      artifacts: [task/ledgers/completed/ap-1-ledger.md, docs/guide/maintenance-policy.md, docs/perf/adapt-part-ap1-remeasure-3-2026-09-12.md]
    - id: AT-10
      status: ATTACKED
      evidence: Before state is RP-18's +54.5 %/+61.8 % against the retired 20 % bar; after state is the pinned bound 3 145 728 ≥ live actuals 1 840 192 / 1 759 577 ≤ 2× and futures 26 738 688 ≥ 26 729 684 — red-first evidence in §Red first.
      artifacts: [crates/repark-spark/src/call/plan_partitioning.rs, task/ledgers/completed/ap-1-ledger.md]
  complete: true
```
