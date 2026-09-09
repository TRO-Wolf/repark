# Overnight report — run 3, 2026-09-09 (08:56–15:00 local)

**Orchestrator:** Claude Opus 5, headless, launched by the owner per
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) §9 ·
**Grants:** G-1 yes, G-2 yes, G-3 stop 15:00 local, G-4 GLM and Muse, G-5 yes ·
**Slate:** [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md) · **Order:** runbook §7
run-3 · **Previous runs:** [run 1](overnight-report-2026-09-09.md),
[run 2](overnight-report-2026-09-09-night2.md).

## Lanes

| Unit | Steps | Workers | Rounds | PR | Outcome |
|---|---|---|---|---|---|
| DISPLAY-POLARS-1 | 2 (resume of the parked draft) | GLM | 1 | [#434](https://github.com/TRO-Wolf/repark/pull/434) | **merged** `8a2928c2` |
| DOCS-LINKS-1 | 1 (whole card) | GLM | 3 | [#437](https://github.com/TRO-Wolf/repark/pull/437) | **merged** `16d5ca37` |
| PREFLIGHT-PARITY-1 | ledger close | orchestrator | — | [#438](https://github.com/TRO-Wolf/repark/pull/438) | **merged** `b0106a99` |
| DISPLAY-POLARS-1 | 3 | GLM | 1 | [#439](https://github.com/TRO-Wolf/repark/pull/439) | **merged** `2215315a` |
| CFG-1 | seed (O) + 1 | orchestrator, then GLM → Muse | 3 | [#440](https://github.com/TRO-Wolf/repark/pull/440) | **merged** `774779e1` |
| PROFILES-1 | 0 | GLM → Muse | 2 | [#441](https://github.com/TRO-Wolf/repark/pull/441) | open, green-pending |
| DF-EAGER-1 | 1 | GLM | 1 | (opened at the stop) | open |

Five merged, two open at the stop time, none parked. Worker cost: GLM **$1.05** over ten rounds;
Muse unmetered over two.

Not reached, in §7 order: DISPLAY-POLARS-1 steps 4–5, CFG-1 steps 2–4, DF-EAGER-1 steps 2–3, AP-0.

## What each merge changed

- **DISPLAY-POLARS-1 step 2** — the polars renderer probes `2 * edge + 1 = 11` rows; a shorter
  frame renders whole with **no `count()` and no tail fetch**. R-11 unparked it by re-pinning the
  protected `test_styled_show_does_not_full_collect` to 11 for the polars section only; its
  `< 12` tooth is untouched, so collect-then-slice still reds.
- **DOCS-LINKS-1** — `make check-docs-links` reads every tracked `*.md`: relative links resolve to
  tracked files, `#anchors` match GitHub-style heading slugs, and `docs:` ledger evidence cells
  follow the same rule. 0.82 s over the tree, so it sits in `make ci`. The 10-link measured
  baseline is a **ratchet**, not an exemption list.
- **PREFLIGHT-PARITY-1** — its last open clause closed under R-12, attestation written, ledger to
  `completed/`.
- **DISPLAY-POLARS-1 step 3** — `repr` honours the display style. Under `polars`/`duckdb` it is
  the same `_render_styled_show` text `show()` would print, regardless of eager-eval, and
  `_repr_html_` returns `None` so Jupyter shows it. Under `spark`, byte-identical to before.
- **CFG-1 seed + step 1** — `serde` and `toml` join the workspace; `repark.toml` discovery,
  `[default]` + `[<profile>]` merge under `REPARK_ENV`, and `${VAR}` interpolation land in
  `repark-core` behind 24 pins. No wiring yet.

## Decisions taken under §6

| # | Decision | Why it was mine |
|---|---|---|
| 1 | **DOCS-LINKS-1 D-4:** the gate's tests live at `python/repark-parity/tests/test_dl_6_docs_links.py`, not in a new `scripts/tests/` tree the card's Home named — that tree does not exist here. | File names inside the card's Home; LEDGER-READING-1 took the same ruling in run 2. |
| 2 | **DOCS-LINKS-1 D-8:** the allowlist ratchets down mechanically — an entry matching no finding is itself a gate failure. Round 1 shipped the allowlist without that tooth while three prose homes claimed "only shrinks". | The card's D-3 already demanded "the same ratchet shape the other gates use"; round 1 under-delivered its own clause. |
| 3 | **DOCS-LINKS-1 D-9:** the allowlist key drops the line number (`path:link`, not `path:LINE:link`). | Found by merging `main`: three allowlisted rows moved ten lines and the gate reported three "new" broken links **plus** three stale entries on a tree where nothing about those links changed. A gate that reds on an unrelated PR's line shift is not usable. |
| 4 | **CFG-1 D-5..D-10** in the step-1 brief: no dependency edits (the seed covers them); `ConfigFile` grows only step-1 fields; `load()` keeps its ruled signature; **discovery takes its environment as a parameter** so no pin mutates the process environment; absent file is the empty config, `REPARK_CONFIG=""` disables, a named-but-missing path refuses; `${VAR}` only. | Test/fixture layout and file names inside the card's Home; the parameterised-environment rule is what makes the suite parallel-safe. |
| 5 | **DF-EAGER-1 D-7..D-12** in the step-1 brief, chiefly **D-9**: every not-yet-implemented pin carries `pytest.mark.xfail(strict=True)`, so the red file rides in a green suite and step 2 turns each pin green by deleting its marker. | A pins-first step has to leave `make py-test-facade` green; `strict=True` makes it a ratchet in both directions rather than a silenced test. |
| 6 | **PROFILES-1 D-5:** step 0's report is a committed document under `docs/perf/`, not only a hand-back. | The card's "no edits" means no *engine* edits; a measurement step 1 depends on must survive in the repository. |
| 7 | **DISPLAY-POLARS-1-S3-Q-001 ruled and filed as residue** — see the parked-questions section. | The card already says "measure"; the measurement disagreed with nothing the card fixed, so it is a residue row, not a new semantic. |
| 8 | **PROFILES-1 Q1/Q2 ruled** (Muse's two hand-back questions): the provisioning command gets one line in step 1's bed header, no re-measurement; the timing sweep suffices for the three execution-only keys. | Follow-up step order inside one card; both adopt the worker's own lean. |

## Questions carried to the owner

1. **DISPLAY-POLARS-1-S3-Q-001 — should `show()`'s bridge-peek hijack be reconciled, as its own
   card?** Measured: for an uncached `mapInArrow`-bridged frame under a styled style, `repr(df)`
   renders the styled table while `df.show()` prints the Spark-grid peek, because `_show` checks
   `_use_bridge_peek` *before* it resolves the style. The doors already disagreed for these frames
   (schema form versus peek grid); step 3 made the disagreement two table renderings. Accepted for
   DISPLAY-POLARS-1 and filed as a disclosed residue in its ledger; reconciling it is a behaviour
   change to `show()` with its own pins, outside steps 4 and 5.
2. **CFG-1 `${}` edge semantics.** The step-1 round chose, and pinned: `$$` is **not** an escape
   (`$${TOTAL}` with `TOTAL=42` yields `$42`), and an unterminated `${` is left verbatim rather
   than refusing. Neither is ruled design. If POSIX-style escaping or a refusal is wanted, that is
   a ruling **before** step 4 documents the file.
3. **The GLM gateway dropped three long rounds today** (CFG-1 step 1 at 34 calls, its resume at 4,
   PROFILES-1 step 0 at 105) — each time after the work was done and before the ledger or document
   was written. Muse finished both from the artifacts on disk with no re-measurement. The
   runbook's tier note already says long rounds go to Muse; run 3 suggests making that a rule for
   any round expected to produce a document, not only for long ones.

## Runbook and process notes from this run

- The `gh pr checks … --jq 'all(.bucket=="pass")'` chain in §5 **never fires** on this repository:
  `manylinux release wheels (tags)` always reports `skipping`, so `all(pass)` is false on a fully
  green PR. The gate that works is
  `all(.bucket=="pass" or .bucket=="skipping") and any(.bucket=="pass")`.
- `main` moved five times during the run, so every lane needed a second `git merge origin/main`
  between its `preflight` and its merge. The `python/repark-parity/tests/map.md` conflict the
  runbook warns about fired once and was resolved by keeping both rows.
- Two gates fired on orchestrator-written content rather than worker content:
  `check-ledger-grammar` demanded a `COVERAGE_ATTESTATION` block the moment CFG-1's last clause
  went PROVEN, and `py-lint` reads tracked `*.py` **anywhere**, including the probe scripts copied
  under `docs/perf/`. Both are worth a line in a future brief.
- PREFLIGHT-PARITY-1 earned its keep on day one: DISPLAY-POLARS-1 step 3 moved
  `scripts/check_lib_py.py`'s baseline without the CAP-1 mirror, and `make preflight` caught it
  locally — the exact failure mode #427 hit in CI.

## Pointers

- Up: [map.md](map.md) · The cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md)
- How the run is driven: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
