# Run 27, night of 09-21 and day of 09-22 — orchestrating note

The second and third legs of run 27, orchestrated by the same session as the first
([day-report-2026-09-20-27-orchestrating-note.md](day-report-2026-09-20-27-orchestrating-note.md)).
Sixteen lane reports are filed beside this note (`night-report-2026-09-21-27-*.md`,
`day-report-2026-09-22-27-*.md`); this note carries only what the lanes could not see: the rulings,
the scoreboard, the tooling changes and the comparison the owner asked for.

## The numbers

| | main | EQUAL / 722 gate cells | delta |
|---|---|---|---|
| morning 09-22 | `8de204f6` | **573** | +36 on 09-21, zero regressions |
| morning 09-23 | `88b6f59f` | **607 of 718 (four cells moved to SPARK-CANNOT)** | +35 EQUAL, one regression (REG-1, `R-TT-TIMESTAMP-AS-OF-EPOCH`, bisect owed) |

Merged on 09-22 (04:00Z → 04:40Z next day): **20 RePark pull requests and 4 fork pull requests**
(#779 #786 #787 #791 #795 #796 #797 #798 #799 #800 #801 #802 #803 #804 #767 and the night's, plus
fork #343 #344 #345 #346). Every product merge went through the four-gate path: local gate zeros,
critic on the current head, CI green, comment gate clean, tree-equal squash.

## The owner's rulings of 09-22

| when | ruling |
|---|---|
| 07:2x | one Opus lane at high for IPI-40 views, Devin executors (xo-opus3) |
| ~08:00 | `xorch` renamed **coordinator** and committed as `scripts/coordinator/` (#796); the Claude engine stays out of the repo |
| 13:0x | Opus 5.5 is released: one orchestrator lane on `claude-opus-5-5` at high (xo-opus55), this session only |
| 14:0x | **Q-55-1 GO** — one commit's data files reach the manifest in Spark's `java.util.HashMap` bucket order of the partition keys, a measured exact fit; the 2026-09-02 orchestrator declaration `V3-FILEORDER-1` ("an unmaintainable anti-feature") is reversed; fork #346 adds the commit-order hook |
| 15:1x | **Q-55-2 (A)** — `remove_orphan_files` matches Spark: `older_than` defaults to now − 3 days, `dry_run` defaults to false, the 24 h floor stays; owner decision OD-2 (2026-08-21) retired. The decisive argument: option B's silent failure (a migrated pipeline calling it bare would list, exit 0 and never clean); Spark and Trino both delete by default and the age floor is the real cross-engine safeguard |
| 15:3x | S3-PATH-WRITE-1: plain Parquet/CSV/JSON path writes to `s3://` are a **v1.5.1 card** (#801) — reads reach S3 and Iceberg tables on S3 write, the path writer is local staging-and-rename |
| 18:2x | the two Muse lanes become Opus 5.5 orchestrators at high (xo-opus56 ← xo-muse9, xo-opus57 ← xo-muse10); lanes extended to 23:30 to land, not to open |
| 19:1x | the Codex sandbox is opened (`danger-full-access`; critic rounds stay read-only) and permission rules added; **GPT-5.6 Terra ≤ max and Sol ≤ high are executors** (Terra smoke: commit, hand-back, write outside the clone) |
| 19:5x | the **`opus` executor tier** (Opus 5.5 at high through `r7-launch.sh`) is live; fallback Muse max / Devin |
| evening | "close out, then spec what is left for 1.5" |

## The orchestrating session's rulings (each logged in the claims file, each reversible by the owner)

- **Q-55-4** — the fork leg of the file-order work is a separate PR with an order hook RePark drives; the RePark PR lands first. (Fork #346 merged the same evening.)
- **Q-55-5** — four file-size baseline raises **refused**; the shuffle-partitions threading goes into its own module so every touched file stays at baseline (the rule: raising a baseline is the owner's alone; the intended path is the split seam).
- **Q-10-1** — `L-INSERT-OVERWRITE` is a ~50/50 flake with GOOD verdicts before every candidate commit (16 harness runs at 5 commits): no regression, no revert target, no second PR; handed to the file-order unit with an N ≥ 6 determinism proof required.
- **Q-55-6** — the `refuse_shared_temp_fallback_location` guard (#198/#217) is **narrowed** to its safety property (never sweep a directory that equals or contains the shared CTAS root or another table's location); a table's own subdirectory is sweepable, which is Spark's hadoop-catalog shape. (#807 open.)
- **Q-55-7** — the memory catalog's default create location becomes `<warehouse>/<ns>/<t>` (the Java InMemoryCatalog / HadoopCatalog layout Spark measures) as a unit of its own; normalising paths in the harness is refused because it hides a divergence. Flagged for the owner: this moves where a memory-catalog table's files land on disk (ephemeral catalog, nothing persisted).
- **#767** — critic r10 (valid: 65 turns, evidence at every angle, current head) returned one P2 of a class already remediated three times (an unpinned expression wrapper in the view-body walker) while itself measuring the behaviour Spark-equal. Queued as a logged exception; the pin (V-009) is PR2's first task.

## The Opus 5.5 comparison, with its confound declared

The two Muse-to-5.5 takeovers inherited pull requests Muse had already carried most of the way, so
this is not a crossover and is not a ranking (the run-27 evaluation protocol still governs any claim
of that kind). What the artifacts show:

| lane | model | ticks | cost | outcome |
|---|---|---|---|---|
| xo-muse10 → xo-opus57 | Muse max → Opus 5.5 high | 64 → 25 | free → $13 | #798 (RP-47, 13 cells) unmerged after 64 Muse ticks; gate in 5 ticks, merged in 25 |
| xo-muse9 → xo-opus56 | Muse max → Opus 5.5 high | 52 → 39 | free → $19 | #797 merged; #625 slices carried forward |
| xo-opus55 | Opus 5.5 high | 136 | $92 | #799 #800 #802 #803 #804 + fork #345 #346 merged; #805 #806 #807 open; seven questions, every one measured before it was asked |
| xo-opus3 | Opus 5 high | 95 | $74 | #767 merged at 00:34 after ten critic rounds; PR2–PR4 built and stacked |

Two lessons were tool fixes, not instructions: a 5.5 tick at high exhausts a 60-turn budget before it
writes its state file (B14 → `XORCH_TURNS=150` for Opus-family lanes), and a Grok critic verdict with
zero tool calls is void (B13 → evidence-first briefs; the verdict reader must check the tool count).

## Tooling changes

- `scripts/coordinator/` — the tick driver in the repo (#796), knobs `COORDINATOR_*`, engines grok/grok47/muse/glm/glmflash/sol; the Claude engine and the Opus worker wrapper stay outside the repo by the owner's permission rules.
- `_lib/opus-worker.sh` + `r7-launch.sh <lane> opus` — Opus 5.5 at high as an executor, muse/devin round layout and hand-back contract.
- Codex sandbox opened for worker and orchestrator rounds; `codex-worker.sh` caps unchanged (Terra ≤ max, Sol ≤ high, Astra ≤ medium).
- Scoreboard staging takes one date (`scoreboard/stage.sh <date> <prev>`); the hard-coded `ROOT` that survived staging on 09-22 cannot recur.

## What the next run inherits

Open: #805, #806, #807 (each with a critic round's findings in hand), the #625 slices c/b/a, views PR2–PR4
stacked on the merged PR1 (first task: the V-009 pin), RP-48 (fork pin bump for the order hook), the
memory-catalog layout unit (Q-55-7), and the residues each lane report numbers. The v1.5.0 remainder
is specified cell by cell in [v1-5-0-remainder-spec-2026-09-23.md](v1-5-0-remainder-spec-2026-09-23.md).
