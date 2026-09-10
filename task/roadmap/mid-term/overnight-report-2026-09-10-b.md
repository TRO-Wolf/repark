# Overnight report — 2026-09-10, run 5b (orchestrator B)

**Session:** the second orchestrating session of 2026-09-10, run beside run 5 on the same box.
**Procedure:** [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md).
**Scope granted:** the Grok lane of [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
(BALLISTA-M1-A step 0 seed under G-5, then M1-A…M1-D as Grok actor rounds, with REVIEW-1 critic
rounds interleaved two at a time) plus FACADE-AUDIT-0 on Muse. Run 5 owned slate-1 leftovers,
MAINT-POLICY-1, TORTURE-1, NEVEROOM-1 and AP-1 beside this session.
**Grants:** G-1 yes · G-2 yes · G-3 stop 13:29 local · G-4 Grok and Muse (no GLM) · G-5 yes.
**Lane names:** every lane prefixed `b-` so nothing collided with run 5's clones.

## 1. What shipped

| Unit | Steps | Tier | Rounds | PR | Outcome |
|---|---|---|---|---|---|
| FACADE-AUDIT-0 | 1–3 | Muse ×2, orchestrator | 2 | [#459](https://github.com/TRO-Wolf/repark/pull/459) | **merged** `0994d539` |
| BALLISTA-M1-A | 0–2 | orchestrator seed + Grok ×2 (one turn-1 stall) | 2 | [#460](https://github.com/TRO-Wolf/repark/pull/460) | **merged** `07a96902` |
| BALLISTA-M1-B | 1–2 | Grok ×2 (one turn-1 stall) | 2 | [#466](https://github.com/TRO-Wolf/repark/pull/466) | **merged** `0eddabfe` |
| BALLISTA-M1-C | 1–2 | Grok ×2 (one turn-1 stall) | 2 | [#469](https://github.com/TRO-Wolf/repark/pull/469) | open, auto-merge armed on green |
| BALLISTA-M1-D | 1–2 | Grok ×2 | 2 | [#470](https://github.com/TRO-Wolf/repark/pull/470) | open, auto-merge armed on green |
| REVIEW-1 | 24 critic rounds | Grok critic | 24 (+1 discarded) | this PR | findings document, 11 fix cards, 8 owner questions |

Every merge was checked for squash tree-equality against the branch head before the Slack note.

## 2. The Grok lane — Ballista Milestone 1

The seed (step 0 of M1-A, under G-5) is the orchestrator's: the `repark-distributed` crate
skeleton, the workspace member, the four Ballista crates pinned `=54.1.0` (verified on crates.io
against the workspace's DataFusion 54.1 pin), the dependency-policy rows and the manifest row.
`make verify` and `cargo build -p repark-distributed --features cluster` were both green before
the first Grok round opened.

By the end of the run the crate carries the `DistributedExecutor` seam, the local reference
executor, an in-process Ballista cluster (one scheduler, N executors on ephemeral ports), the
session-provider and codec seats, three multi-stage shapes checked against the local executor,
per-stage metrics on `Completed`, Iceberg reads rebuilt on each executor from the session catalog,
and a Milestone 1 design document whose success list is checked line by line against pins. Nineteen pins, all running, none `#[ignore]`d: three local, four cluster, five multi-stage, four Iceberg, plus the crate's own.

**Two clauses are OPEN on purpose, and both are dependency walls the runbook says to park:**

- M1-B **C-004** — the codec round-trip serde pin needs `PhysicalExtensionCodec` from
  `datafusion-proto`, which is not a dependency of the crate. The wrapper today is a newtype that
  adds no behaviour. The owner's two options are written into
  `crates/repark-distributed/src/map.md` with no choice taken.
- M1-C **C-002** — deterministic executor-kill retry. Ballista 54.1.0's `ChaosExec` is not
  fail-once, and stopping one executor between stages needs `arrow_flight`. Recorded as residue;
  the design document lists the success line as missing, not done.

Three API residues against the 2026-09-08 audit were recorded rather than worked around: the
session seat is `SessionBuilder` (not `SessionProvider`), `PhysicalExtensionCodec` is not
re-exported, and Ballista's standalone `work_dir` is its own `TempDir` — shuffle files never land
in the session spill directory the card expected. The REVIEW-1 round over the audit document found
the first of those independently.

## 3. The Grok critic sweep (REVIEW-1)

Twenty-four rounds over eleven units, ~$9.80 in worker cost, in
[review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md): 51 numbered findings — 39
CONFIRMED, 4 SUSPECTED, 8 owner questions — 12 fix cards, and two units that survived with nothing
(PREFLIGHT-PARITY-1 in both roles, DISPLAY-BRIDGE-1). The orchestrator re-ran nine reproductions
itself; all nine held. Highlights:

- **A data-loss path in DF-EAGER-1** (raised to high by the second round): `count()` on an eager
  frame skips the pending-checkpoint materialize, so a later `catalog.clearCache()` restores
  source lineage and a frame whose source file is gone collects as `[]`. Its sibling — `lazy()`
  after `localCheckpoint()` interpolating `SELECT * FROM None` — was filed by all three roles
  independently, and the security round showed it silently reads a temp view named `none` if one
  exists. That fix card is the one to work first.
- **A TOML injection in the CFG-1 config mirror**: an unsanitized profile name renders extra
  tables, including a Glue catalog block, into a document the loader accepts.
- **A credential echo**: a config parse error forwards the `toml` crate's `Display`, which reprints
  the offending source line — `REPARK_CONFIG` pointed at an AWS credentials file puts the access
  key into the raised exception.
- **A redaction gap in `DESCRIBE TABLE EXTENDED`**: it redacts with Spark's predicate, not the
  `prop_key_is_secret` predicate D-5's text names, so `s3.access-key-id`, `credential` and
  `basic.auth.user.info` print in plaintext where `DESCRIBE NAMESPACE EXTENDED` would redact them.
- **A gate that fail-opens on a substring**: `check_ledger_grammar.py`'s READING exemption matches
  a quoted mention in prose, so a `STANDARD` ledger can ship unpinned PROVEN clauses.
- **A dead link the docs-links gate cannot see**: same-file `#anchor` targets are never checked,
  and `docs/spark-sql-iceberg-parity.md:3053` already points at a slug its heading no longer
  produces.

## 4. Parked, and what the next run should pick up

- **Both remaining Ballista PRs (#469 M1-C, #470 M1-D) are open with auto-merge armed** and will
  land on green without further attention; #470 is stacked on #469, so its diff collapses once
  #469 lands. Neither ledger has departed to `completed/` — that is the owner's call together with
  whatever STATUS.md should say about Milestone 1, and the PRs say so.
- **The whole of Ballista Milestone 1 (A, B, C, D) was carded, built, gated and PR'd in this
  window**, against a card family that had never been started. Two clauses are OPEN and one is
  narrowed, all three named in `docs/design/distributed-m1.md`'s open-questions section, and all
  three trace to the same cause: RePark plan nodes cannot cross to an executor without
  `datafusion-proto`. **That is the first question Milestone 2 has to answer**, and it is the one
  thing in this run the owner should look at before scheduling more distributed work.
- **REVIEW-1 is not done.** The card wants both roles on every unit in its D-1 list plus two
  security rounds; this run did twenty-four of about twenty-six. The findings document names exactly
  what is missing.
- **Twelve fix cards** (REVIEW-FIX-1…12) are written but unopened. None
  was worked this run: REVIEW-1 D-4 says a critic never patches, and the fix rounds belong to a
  scheduled slate, not to the sweep.
- **Eight owner questions** are collected in §4 of the findings document, each with the
  orchestrator's lean. The two that block a fix card are Q-25 (narrow R-16's key list) and Q-30
  (which redaction predicate `DESCRIBE` owes).

## 5. Decisions taken under G-2

| # | Decision | Why it was inside the grant |
|---|---|---|
| D-b1 | The `repark-distributed` manifest row spells its layer `surface crates`, not the card's `layer = "runtime"`. | The card's word is the crate's ROLE; `layer` is mechanically checked against the dependency policy's tier names. A measurement disagreeing with a card's expectation is a residue row, and it is recorded in the crate's `map.md`. |
| D-b2 | `runtime` was added to the role vocabulary in `scripts/check_crate_dag.py`. | Implementing the card's own D-1, which names the role. No structural rule quantifies over it. |
| D-b3 | M1-B's C-004 parked rather than adding `datafusion-proto`. | §6 parks a lane on "a dependency beyond the card's seed commit". Parking one clause instead of the lane kept the rest of the card moving. |
| D-b4 | M1-C's C-002 left OPEN with its residue rather than a weakened pin. | Same rule; the brief said an honest OPEN beats a pin that proves nothing. |
| D-b5 | The FACADE-AUDIT-0 ledger's header became `**Path:** READING`. | R-10's own marker for a reading unit; the ledger already described itself as one in prose. |
| D-b6 | DISPLAY-BRIDGE-1 was reviewed although REVIEW-1 D-1 does not list it. | It merged mid-sweep into the display path three other findings live in; D-1's scope is "every PR merged from #426 through the newest at launch". |
| D-b7 | The critic clones were given the owner's prebuilt `_native.abi3.so` and the owner's venv interpreter, with the clone's Python source shadowing the editable install. | Fixture layout inside the review lanes. Its cost is disclosed in the findings document: every Python reproduction ran against a native that predates CFG-1, with per-round shims. |

## 6. Process notes for the runbook

- **The turn-1 stall fired on four of seven Grok actor rounds** and once as the fabrication
  pattern (a `CONCLUDED` naming eight pytest files that do not exist, `num_turns` 1, no report on
  disk). The runbook's two checks caught all four. Resuming the same session with a one-paragraph
  proceed mandate worked every time; no round needed a second resume. Worth folding into the
  launcher: the mandate could be prepended to every Grok brief by default.
- **A squash merge makes the next stacked branch conflict add/add.** M1-B and M1-C each hit
  three-file add/add conflicts against `main` because their parent landed squashed. Resolution is
  mechanical (the branch's side for the crate, both sides for `task/ledgers/staging/map.md`), but
  the first attempt committed conflict markers in five source files — `git checkout --ours` was
  given four paths while ten were conflicted. **Check `git diff --diff-filter=U --name-only` is
  empty before committing a merge, and compile before pushing.**
- **Never `git checkout` in a lane clone while its worker is running.** One branch switch happened
  with a worker mid-round; the worker's uncommitted files survived, but only by luck.
- **A critic reached into another run's lane clone** (`/tmp/oc-dp4`) to run pins against a unit
  that had merged after its own clone was made. It left that clone clean and disclosed it, but the
  brief should say the lane's own clone is the only tree it may read, and the clone should be
  refreshed to `origin/main` between rounds — which this run started doing after the sixth pair.
- **Auto-merge (`gh pr merge --auto --squash`) plus `gh pr update-branch` is the right shape** for
  a run with two lanes: the orchestrator never waits on CI. `update-branch` fails on conflicts, so
  the local resolution above is still needed.

## 7. Cost

Worker cost from `runs.tsv`: Grok actor rounds ≈ $6.30 (M1-A $0.86, M1-B $2.58, M1-C $2.86),
Grok critic rounds ≈ $9.80 over 24 rounds plus $0.02 of discarded stalls, Muse ≈ two rounds on
`muse-spark-1.3-contributor`. The orchestrator's own spend is one audit per round plus the merge
chains.

## Pointers

- Up: [map.md](map.md) · The procedure: [overnight-orchestrator-runbook-2026-09-08.md](overnight-orchestrator-runbook-2026-09-08.md)
- The slate: [cheap-tier-slate-2-2026-09-09.md](cheap-tier-slate-2-2026-09-09.md)
- The sweep: [review-1-findings-2026-09-10.md](review-1-findings-2026-09-10.md)
