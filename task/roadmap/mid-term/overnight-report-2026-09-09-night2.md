# Overnight report — run 2, morning of 2026-09-09 (05:26–09:00 local)

**Orchestrator:** Claude Opus 5, headless, launched by the owner per
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) §9 ·
**Grants:** G-1 yes, G-2 yes, G-3 stop 09:00 local, G-4 GLM and Muse, G-5 yes ·
**Slate:** [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) · **Order:** runbook §7
night-2 · **Previous run:** [overnight-report-2026-09-09.md](overnight-report-2026-09-09.md) (run 1,
the night of 2026-09-08/09; this is the second run of the same calendar morning, hence the suffix).

## Lanes

| Unit | Steps | Worker | Rounds | Reached | PR | Outcome |
|---|---|---|---|---|---|---|
| LEDGER-READING-1 | 1–2 | GLM 5.3 Flash | 2 | done | [#432](https://github.com/TRO-Wolf/repark/pull/432) | **merged** `e24f4c68` |
| SQL-DESCRIBE-1 | 2–3 | Muse Spark 1.3 | 3 (one a rejection) | done | [#428](https://github.com/TRO-Wolf/repark/pull/428) | **merged** `7db61bfa` |
| PREFLIGHT-PARITY-1 | 1 | GLM 5.3 Flash | 1 | done | [#433](https://github.com/TRO-Wolf/repark/pull/433) | **merged** `f8a9f1ac` |
| DISPLAY-POLARS-1 | 2 | GLM 5.3 Flash | 1 | implemented, halted | [#434](https://github.com/TRO-Wolf/repark/pull/434) | **parked** — draft, owner ruling needed |

Three merged, one parked. Worker cost: GLM $0.37 over four rounds;
Muse unmetered over three.

Not reached before the stop time, in §7 order: DISPLAY-POLARS-1 step 3, CFG-1 (seed + steps 1–2),
PROFILES-1 step 0, AP-0, DF-EAGER-1 step 1.

## Decisions taken under §6

| # | Decision | Why it was mine |
|---|---|---|
| 1 | LEDGER-READING-1's step-2 brief ruled four points the card left implicit: the READING marker goes in the ballista ledger's header before any flip; the existing `COVERAGE_ATTESTATION` block is kept rather than duplicated; the "Why every clause reads OPEN" section is rewritten; evidence anchors are read off the document, not guessed. | Wording of docs the card names; the worker had already surfaced all four as out-of-scope observations. |
| 2 | Both LEDGER-READING-1's and BALLISTA-AUDIT-0's ledgers moved to `completed/` in one departure commit. | BALLISTA-AUDIT-0's ledger was held in `staging/` **only** by rule B, which R-10 lifts; its own header says it closes when the unit merges, and it merged in #426. |
| 3 | SQL-DESCRIBE-1 step 3 ran on Muse rather than the card's M tier. | Night-1's lesson that long rounds lose work on the GLM gateway; the step carried a live-Spark measurement and the Muse session already held the step-1 capture. |
| 4 | `python/dbt-repark/tests/test_statement_surface.py` was accepted in the SQL-DESCRIBE-1 diff although the card's Home does not name it. | The old pin asserted the *pre-fix* DataFusion shape (`column_name` / `Utf8`); the fix makes it red by construction. A forced consequence, not scope creep. Flagged in the PR body. |
| 5 | PREFLIGHT-PARITY-1's ledger was **left in `staging/`** and its departure move reverted. | Its C-005 is an open owner question. §5's rule — if the departure edit is not certain, leave the ledger in staging and say so in the PR body. |
| 6 | `make preflight` was not re-run by the orchestrator on the PREFLIGHT-PARITY-1 branch. | The worker ran it once, alone, from the commit tree, exit 0 in 14m18s; re-running a 14-minute gate would not have fit before the stop time. CI was the confirming run and it passed; the PR body says so. |

## Parked questions for the owner

1. **DISPLAY-POLARS-1 step 2 (#434), the pin conflict.** D-5 says the styled renderer first fetches
   `limit(max_rows + 1)`. That is one 11-row export, and
   `test_styled_show_does_not_full_collect` caps the polars section at `max_rows_per_export = 5`,
   so **the card's own ruling reds a protected pin** (`assert 11 <= 5`). The worker halted rather
   than edit it, which was right. Recommended: re-pin `max_rows_per_export` to the probe size
   `2 * edge + 1 = 11` for the polars section — the cap still forbids full-collect-then-slice
   (11 < 12) and keeps every other tooth. Rejected: probing through `to_arrow_batches()` to hide
   the fetch from the spy, which is pin evasion. Parked because loosening an anti-full-collect
   guard is not a §6 call.
2. **DISPLAY-POLARS-1 step 2, scope (#434).** Does the D-5 probe extend to the `duckdb` style, or
   is step 2 polars-only? Recommended: polars-only now, duckdb settled in step 4 where
   `repark.display.max_rows` re-touches the keep-set semantics anyway.
3. **PREFLIGHT-PARITY-1 C-005 (#433, merged).** Should `AGENTS.md`'s gate-roster sentence name
   `py-test-parity-cap`, or are `DEVELOPMENT.md` and the root `map.md` the right home? The card
   deliberately left `AGENTS.md` alone. One word closes the clause and the ledger moves.
4. **`make check-docs-links` does not exist.** LEDGER-READING-1's D-3 and its step-2 gate list both
   name it; there is no such target and no such script. The `docs:` anchors that reading ledgers now
   carry are therefore validated by nothing mechanical. Worth a gate; it was outside the card.
5. **A stale header.** `task/roadmap/epic-term/ballista-audit-2026-09-08.md` still says "State:
   proposal; step 1 landed, step 2 pending" and "disposition in step 3" — stale since #426. Left
   alone as outside every card's Home this run.

## Notes for the next runbook revision

- **The comment ban needs to be in the brief twice.** Muse's first SQL-DESCRIBE-1 round came back
  with about two dozen Rust `///` and `//!` doc comments despite the preamble's first hard rule. One
  follow-up round removed them and moved the facts to `crates/repark-spark/src/map.md`. Repeating
  the rule as a per-round fence, with the audit grep spelled out, is cheap insurance.
- **§3's build guard is wrong as written.** `while pgrep -f "cargo|maturin"` matches the
  orchestrator's own shell command line, so it never terminates. Use `pgrep -x cargo` /
  `pgrep -x maturin`, and put the launch sequence in a script file rather than an inline compound
  command.
- **Muse's run directory is `/tmp/muse-worker/<lane>/`, not `/tmp/oc-worker/<lane>/`.** §3's wait
  loop only names the latter.
- **Merging `origin/main` into a lane branch can conflict** in `python/repark-parity/tests/map.md`,
  where every unit appends a CAP-1 ratchet line. It happened once this run and was resolved by
  keeping both rows.
- **A merge inside a `||` fallback leaves an unresolved merge in progress silently.** Check
  `.git/MERGE_HEAD` before assuming a fetch-and-merge chain succeeded.

## Pointers
- Up: [map.md](map.md) · The cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md)
- The procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
