# Overnight report — run 4 of 2026-09-09

**Session:** Opus 5 orchestrator, 14:29 → 22:29 local, grants G-1…G-5 (G-3 stop 22:29,
G-4 GLM + Muse, G-5 yes) · **Order followed:**
[overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md) §7
run-4 order, docs read at the PR-#444 tip · **Cards:**
[cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md)

## 1. What landed

| Unit / step | PR | Result | Worker | Rounds |
|---|---|---|---|---|
| CFG-1 step 1b (R-14: `$$` escape, unterminated `${` refuses) | [#445](https://github.com/TRO-Wolf/repark/pull/445) | **merged `ed4c1fba`**, tree-equal | GLM | 1 |
| CFG-1 step 2 (`sources.rs`, `redact.rs`) | [#447](https://github.com/TRO-Wolf/repark/pull/447) | **merged `3dac2922`**, tree-equal | GLM | 1 |
| DISPLAY-POLARS-1 step 4 (the four keys, polars fidelity) | [#448](https://github.com/TRO-Wolf/repark/pull/448) | **merged `ad81e9ec`**, tree-equal | Muse | 2 + 1 dropped |
| DISPLAY-POLARS-1 step 5 + departure (**unit complete**) | [#449](https://github.com/TRO-Wolf/repark/pull/449) | **merged `df74ae3e`**, tree-equal | Muse | 1 |
| CFG-1 step 3 (the loader reaches a session) | [#451](https://github.com/TRO-Wolf/repark/pull/451) | **merged `89287739`**, tree-equal | Muse | 2 + 1 dropped |
| DF-EAGER-1 steps 2 + 3 + departure (**unit complete**) | [#452](https://github.com/TRO-Wolf/repark/pull/452) | **merged `ca629ee7`**, tree-equal | Muse | 3 |
| DISPLAY-BRIDGE-1 (R-13, one-round card, **unit complete**) | [#454](https://github.com/TRO-Wolf/repark/pull/454) | open, CI green at hand-off | GLM | 1 |
| CFG-1 step 4 (the typed mirror and the guide) | [#455](https://github.com/TRO-Wolf/repark/pull/455) | open, CI running at hand-off | Muse | 1 |

**Three units finished tonight:** DISPLAY-POLARS-1, DF-EAGER-1, DISPLAY-BRIDGE-1 — all three
ledgers in `completed/` with attestation blocks. **CFG-1 is finished as work** (steps 0, 1, 1b, 2,
3, 4) but its ledger stays in `staging/` on one open clause, C-024, below.

**Not opened** (the §7 order ran out of clock, not out of order): PROFILES-1 steps 1–3, AP-0,
CONF-UNREAD-1. Those plus slate 2 are the next run's queue.

**Worker cost:** GLM 3 rounds, $0.33 total. Muse 11 rounds (2 dropped, see §4), unmetered.

## 2. Owner questions — four, none blocking a merge

1. **C-024 — `[<profile>.conf]` ordering.** CFG-1 D-1 promises those keys apply "in file order".
   `toml::Table` is `BTreeMap`-backed in this build, so file order is not recoverable without
   enabling `toml`'s `preserve_order` feature — a dependency change, which §6 puts outside my
   authority. Shipped behaviour is **sorted-key order**, deterministic and pinned. Either amend
   D-1 to say sorted order, or authorise the feature flag. **CFG-1's ledger waits on this.**
2. **C-025 — the database section refuses at LOAD.** The ruled CFG-2 card says a database source
   in this era is "parsed, validated, listed, and refuses-loud **on use**". With no listing
   surface yet, the step chose to refuse at load. I accepted it — the alternatives were a silent
   drop or inventing CFG-2's registry — but the consequence is real: **until CFG-2 lands, a
   `repark.toml` declaring a database source cannot open a session at all.**
3. **`type = "rest"`.** The ruled loader card lists it, `CatalogKind` has no `Rest` variant, and
   no Java-class spelling exists either. It refuses loud today. Confirm that `CatalogKind::Rest`
   belongs to the REST-catalog card.
4. **A `type = "glue"` block connects to AWS at session build** (`CredentialsNotLoaded` on this
   box), so the guide's runnable examples use `type = "memory"`. Worth a sentence in
   `iceberg-guide.md` too, if you agree.

## 3. Decisions taken under §6

| # | Decision | Why it was mine |
|---|---|---|
| D-5 (CFG-1) | `SourceSpec` does not exist anywhere in the tree though the ruled card names it; defined `pub(crate)`, not `pub`, marked `#[allow(dead_code)]`. | The name and shape were already ruled; keeping it crate-private leaves the frozen public API untouched and lets step 3 decide what escapes. |
| D-7 (DF-EAGER-1) | `_eager_shape` is filled by one count over the already-materialised MemTable, not by `to_arrow()`. | The first cut copied every row into Python to keep two integers. D-1's "no separate count" exists to stop an extra pass over the *source plan*; a count over a built MemTable is not that, and it is strictly cheaper than the copy. The free option (a Rust return value) is the card's own deferred step, filed as residue R-001. |
| D-12 (DISPLAY-POLARS-1) | `repark.display.str_len` defaults to **30**, not the card's 32. | D-6 says the fidelity targets are measured against polars itself; polars 1.43.2's own default measures 30. The measurement wins over the guess. |
| — | `[<profile>.conf]` applies in sorted-key order **for this round**, with C-024 left OPEN. | Landing step 3 without deciding the D-1 wording; the question goes to the owner intact. |
| — | Three ceiling raises refused and absorbed (§4). | `check_lib_py.py` and `check_rust_file_size.py` both rule that a baseline increase needs explicit owner approval, which I cannot give. |

## 4. What went wrong, and what to change

**Ceiling raises are the recurring ask, and absorption always worked.** Three rounds asked for
"owner ratification at merge" of a file-size baseline: `plan_collapse.py` 1168 → 1357 and
`session_core.py` 2411 → 2448 (DISPLAY-POLARS-1 step 4), and `crates/repark-python/src/session.rs`
1177 → 1198 (CFG-1 step 3). All three were refused and sent back, and **every one absorbed with
zero behaviour change and every pin byte-identical** — new `dataframe/polars_cells.py`, the key
plumbing into `session_configuration.py`, `drain_arrow_c_stream` into the existing
`arrow_export.rs`. Final numbers all ratchet **DOWN**: 1168 → 1057, 2411 → 2305, 1177 → 1128,
and DF-EAGER-1 took `core.py` 4525 → 4487 on its own initiative. **Put the refusal in the brief
up front** — every later brief said "do NOT raise a baseline; put the code in a new module and
ratchet DOWN", and no round after that asked.

**The parity suite is not in `preflight`, and it is what catches a new public name.** Two PRs went
red on CI after a green local `preflight`: `test_ex_0_example_coverage.py` pins the enumerated
public surface count (921 → 923 for `Builder.config_file`/`configFile`, → 926 with DF-EAGER-1's
three), and `test_cap_1_source_file_line_cap.py` mirrors the size baselines. Add to the runbook:
**a PR that adds a public name or moves a ceiling runs the parity suite before pushing**
(`PYTHONPATH=python/repark-parity/src VIRTUAL_ENV=$PWD/.venv uv run --no-project python -m pytest python/repark-parity/tests -q`).
Two such PRs open at once each carry their own count; the second to merge is rebased and bumped
again.

**A `completed/` ledger is frozen and the gate means it.** DISPLAY-BRIDGE-1 flipped its residue by
editing `display-polars-1-ledger.md` in place; `ledger_lifecycle.py check` accepts only a link
repair or a note **prepended at the top of the file**. Converted to an errata note. Brief this
whenever a round is expected to flip an older unit's residue.

**Muse's `--effort max` is rejected by the contributor model.** R-17 says pass no flags and let
the launcher default carry it, but the launcher defaulted to `max` and the API answered
`reasoning_effort 'max' is not supported for model 'muse-spark-1.3-contributor'. Supported values:
[minimal, low, medium, high, xhigh]`. One round died at 0 tool calls before I caught it. I set the
launcher default to **`xhigh`**, the model's top, and every later round ran clean.
**Check the run's `exit` file within a minute of launching** — that failure was silent for 22
minutes because I only watched the event count.

**Muse dropped one long round on transport errors** (`body-truncated`, six failed attempts over a
12-minute ceiling, 37 tool calls lost, nothing committed). A fresh launch of the same brief
finished it. `dirty=false` in the run record is the reassuring part: no half-written work.

**Renaming a lane clone breaks it in two ways.** I renamed a warm clone to reuse its build cache;
`.venv`'s `.pth` files and script shebangs carry absolute paths (fixable with one `sed`), and
`target/` bakes `CARGO_MANIFEST_DIR` into compiled test binaries, so a fixture copy read a path
that no longer existed and two `repark-iceberg` `dv_close` tests failed for two `preflight` runs
before I found it. `cargo clean -p repark-iceberg` fixed it. **Reuse a warm clone by pointing
`--repo` at its existing path; do not rename it.** Warm clones are worth reusing — `make develop`
took 26 s instead of a full rebuild.

**`dv_close`'s fixture path is global** (`/tmp/repark-v3e3-partdv`), so two clones running the
workspace Rust tests at once collide. I serialised `preflight` runs after noticing. Worth a fix
in the test itself.

## 5. Numbers

- 6 PRs merged, all tree-equal after the squash; 2 PRs open and green/running at hand-off.
- 14 worker rounds: 3 GLM ($0.33), 11 Muse (2 dropped and relaunched).
- 4 orchestrator follow-up rounds, all from audit findings, all fixed on the first return.
- `main` moved 8 times during the run; every lane needed at least one `git merge origin/main`.
- Two merges conflicted for real (both in `session_configuration.py` and the size baselines, where
  DISPLAY-POLARS-1 and CFG-1 had appended to the same module on parallel branches); resolved as
  the union with the baselines re-measured against the merged files.

## Pointers
- Up: [map.md](map.md) · Cards: [cheap-tier-slate-2026-09-08.md](cheap-tier-slate-2026-09-08.md),
  [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
- The runbook this followed: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- Previous runs: [overnight-report-2026-09-09.md](overnight-report-2026-09-09.md),
  [overnight-report-2026-09-09-night2.md](overnight-report-2026-09-09-night2.md),
  [overnight-report-2026-09-09-run3.md](overnight-report-2026-09-09-run3.md)
