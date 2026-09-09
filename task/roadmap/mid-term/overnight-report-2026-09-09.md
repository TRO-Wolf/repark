# Overnight report — the cheap-tier slate, night of 2026-09-08/09

**Run:** the unattended orchestrator of
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) over
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md).
**Grants:** G-1 yes (squash-merge on green inside the card's Home), G-2 yes (bounded decision
authority), G-3 stop 03:00 local, G-4 GLM and Muse.
**Base at start:** `f00ed9ea` (v1.1.1). **Base at end:** see the merges below.

## What happened, per lane

| Unit | Steps reached | PR | Outcome | Rounds | Worker |
|---|---|---|---|---|---|
| BALLISTA-AUDIT-0 | 1, 2, 3 (complete) | [#426](https://github.com/TRO-Wolf/repark/pull/426) | **MERGED** `22201c45`, tree-equal | 2 lost to GLM + 2 on Muse + 1 orchestrator | GLM → Muse |
| DF-EXPLAIN-1 | 1, 2 (complete) | [#427](https://github.com/TRO-Wolf/repark/pull/427) | **MERGED** `5b83dc53`, tree-equal | 3 | GLM 5.3 Flash, ≈ $0.33 |
| DISPLAY-POLARS-1 | 1 of 5 | [#429](https://github.com/TRO-Wolf/repark/pull/429) | **MERGED** `d2b11e42`, tree-equal | 3 (1 halt, 1 build, 1 example fix) | Muse Spark 1.3 |
| SQL-DESCRIBE-1 | 1 done, 2 halted | [#428](https://github.com/TRO-Wolf/repark/pull/428) (draft) | **PARKED** on one ruling | 1 lost to GLM + 2 on Muse | GLM → Muse |
| CFG-1, DF-EAGER-1, PROFILES-1, AP-0 | not opened | — | — | — | — |

**Merged: 3. Parked: 1.** DISPLAY-POLARS-1 has four steps left; its ledger stays in `staging/`
with C-003 to C-006 OPEN, so the unit is correctly recorded as in flight even though step 1 merged.

## The merged units

**BALLISTA-AUDIT-0** — Milestone 0 of the Rust migration pilot (slate ruling R-5). Audited
upstream `datafusion-ballista` at tag `54.1.0` / `f4e66525` (root `Cargo.toml` `datafusion = "54"`,
the D-1 match against this workspace's `54.1.0`). Recommendation: **DEPEND** on four Ballista
crates at a pinned tag rather than import them — the override seats suffice against an import cost
of ~59k source plus ~20k test lines.

Its serialization chapter answered the standing question in ADR-0004, and **corrected the ADR's
premise**. The ADR ruled out Ballista-for-writes because the protobuf plan serialization "cannot
satisfy" RePark's Iceberg write/commit nodes. Measured: `BallistaPhysicalExtensionCodec` is a
single override slot, so a write/commit node **can** travel, through a delegating wrapper. The
ruling stands on its other ground — a commit must not issue from a task — and that is now a dated
disposition in the ADR rather than a silent edit.

**DISPLAY-POLARS-1 step 1** — the default display style flips to `polars` (owner ruling R-1/R-2's
foundation), resolved through a new `default_display_style()` that reads `REPARK_DISPLAY_STYLE`
first; the facade suite pins `spark` at conftest import so no existing expectation changes meaning
(R-6), proven by the whole suite staying green (5820 passed) rather than by argument.

**DF-EXPLAIN-1** — `explain()` printed `Row(plan_type='physical_plan', plan='…\n…')` with literal
`\n`; it now prints the plan under Spark's section headers with DataFusion's text verbatim, and
never routes through `DataFrame.collect` (held by a spy pin). Measured rather than assumed: the
`logical_plan` text carries no trailing newline while the physical, tree and metrics texts do, so
the blank line between sections is conditional — a blind join would have silently dropped it.

## Decisions taken under G-2

| # | Decision | Why it was mine |
|---|---|---|
| D-5 (ballista) | The audit document is `ballista-audit-2026-09-08.md`. | A file name inside the card's Home. |
| D-6 (ballista) | Resolved D-1's version pair myself (tag `54.1.0`, `f4e66525`) instead of spending a worker round on it. | A measurement the card specified the rule for. |
| D-5 (df-explain-1) | `core.py` hit its **exact** file-size baseline (4555 vs 4539). Ruled the split: the rendering support moves to `explain.py` and the baseline ratchets **DOWN** 4539 → 4536 in the same commit. | The gate's own first-listed sanctioned out, needing no owner approval; option (b), a baseline amendment, explicitly does. Precedent: `display.py`, `sampling.py`, `statistics.py` all left `core.py` this way. |
| D-9 (display-polars-1) | `default_display_style()` lives in `session_configuration.py`, not `session_core.py`. | The card's Home was wrong: `_DEFAULT_DISPLAY_STYLE` and `normalize_display_style` are both defined there; `session_core.py` only star-imports them. Verified before ruling. |
| D-11 (display-polars-1) | The two doc examples the flip broke follow the new default: `display_style.py` documents the polars default truthfully, and `show_sort.py` pins `repark.display.style=spark` because its assertions are about the spark grid while it demonstrates `sort`. | Required follow-through of owner ruling R-1, not a new decision; verified by running all 209 examples green against a built native. |
| D-10 (display-polars-1) | The parity half of D-2 is dropped — no parity `conftest.py` exists — **and the drop had to be measured**: the parity suite runs green under the flipped default (624 passed). | Dropping a card instruction on evidence, with the evidence required rather than assumed. |
| Ledger verdicts (ballista) | A reading unit's clauses are recorded **OPEN**, not PROVEN. | See the parked question below — the contract, not a judgement call. |

## Parked for the owner

**P-1 — SQL-DESCRIBE-1 D-3: there is no shared Iceberg-to-Spark type spelling.** Step 2 halted
before writing any code, on the card's own hand-back condition. `create_table.rs`'s
`sql_type_to_iceberg` maps only the forward direction; the fork's `PrimitiveType` `Display` spells
**`long`**; the only Spark-DDL-like spellings are private Arrow helpers in `repark-python`, a crate
`repark-spark` cannot depend on — and they also spell `Int64` as `long`, while the live capture
measured **`bigint`**. Two questions: where should the single canonical spelling live, and which
spelling is canonical? A cross-crate decision about a Spark-visible type spelling sits outside the
overnight grant. Draft PR [#428](https://github.com/TRO-Wolf/repark/pull/428).

**P-2 — a reading unit's ledger cannot satisfy the pins-citation contract.**
[docs/testing.md](../../../docs/testing.md) defines `PROVEN` as "cited by at least one test", and
`scripts/check_ledger_grammar.py` enforces it over `crates/`, `python/`, `scripts/`. BALLISTA-AUDIT-0
adds no code and therefore no test, so none of its twelve discharged clauses can honestly be
`PROVEN`. They were recorded `OPEN` with the reason, and the ledger stayed in `staging/`. The
exception list is ratchet-down-only, so widening it was not the orchestrator's call. Candidate
rulings: give reading units an `EXCEPTIONS` row (ceiling equal to their clause count, attestation
required), or rule that a reading unit files evidence without a clause table at all.

## Three environment findings worth keeping

- **Live Spark needs `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`.** The box's default `java` is 11
  (class version 55); Spark 4.1.2 is built for 17+ and dies with `UnsupportedClassVersionError`,
  then `[JAVA_GATEWAY_EXITED]`. Any brief for a live-oracle step should name the JDK.
- **The CAP-1 mirror is not reached by `make preflight`.**
  `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` holds a second copy of every
  file-size baseline, and the parity suite is not a preflight member — so a ratchet that edits only
  `scripts/check_lib_py.py` passes preflight green and fails the `Python` CI leg. It did exactly
  that on #427. Ratchet both tables in the same commit.
- **Doc examples are not reached by any local gate that would notice a default change.**
  `check_example_coverage.py` skips execution when the native module is not importable, so
  `make preflight` prints "skipping example execution" and passes; and examples run outside
  pytest, so a `conftest.py` pin cannot reach them. Flipping the display default broke two
  examples and only CI's `build + import smoke` leg caught it. Any change to a user-visible
  default should run `check_example_coverage.py --require-execute` against a built native before
  the PR.

## On the workers

GLM's gateway (`api.z.ai`) dropped the socket mid-round **three times**, each time losing the
round's work — including a completed set of Ballista measurements and a completed live-Spark
capture, neither of which had been written to disk yet. Muse Spark 1.3 completed the same work on
the first attempt each time. Two mitigations were applied and both held: briefs now say *write the
file as you measure, a number not yet in the file does not exist*, and long or measurement-heavy
rounds went to Muse.

The audit discipline earned its cost twice: the live-Spark capture was **re-run by the orchestrator
against live Spark** rather than believed (it matched exactly, including two places where the
measurement contradicts the card), and the Ballista audit had two Half A numbers and five Half B
citations reproduced against the pinned upstream — which also surfaced a coverage gap of five
`.rs` files that the next step then closed.

## Deviation from the runbook's §7 ordering

§7 pairs DF-EXPLAIN-1 with SQL-DESCRIBE-1 step 1, but §3 also forbids two lanes building natives at
once, and a fresh clone has no `.venv`, so DF-EXPLAIN-1's first round *must* run `maturin develop`.
Those two constraints cannot both hold, so DF-EXPLAIN-1 was paired with BALLISTA-AUDIT-0 (§4 lists
it parallel-safe, no repo code, no build) and SQL-DESCRIBE-1 took the next free slot.

## Pointers
- Up: [map.md](map.md) · The runbook: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- The cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md)
