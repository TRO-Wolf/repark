# Phase-3 execution brief — Python binding, facade, parity = milestone one

> **ARCHIVED 2026-08-09** (Front-Door FD-4) — a historical record of the v1 → v2 port, kept for
> provenance and **not a source of live rules**: every rule still in force was promoted to a
> current document first ([promotion-ledger.md](promotion-ledger.md)). Relative links were
> repaired for this location on the same date; nothing else changed. Current state:
> [STATUS.md](../../../STATUS.md).

Status: **DELIVERED** — phase 3 closed 2026-08-08 = milestone one
(PRs #16, #18-#23); archived 2026-08-09. Slate settled 2026-08-08. Companion to the settled design (`DESIGN-python-facade.md`; lands
in-repo as `docs/design/python-facade.md` via PR-1 alongside this brief as
`briefs/phase-3-python-facade.md`). Port source: the private v1 engine repository at the frozen pin
`fc3f48102` (== that repository's `main`, zero drift, re-verified 2026-08-08). Base: public `main`
at the phase-2 close.

Operator confirmations on record: phase-3 oversight delegated to the orchestrator (2026-08-08);
delegated **actor/builder agents run Opus 5 medium; critic/judge/verifier agents run Opus 5 high**
(standing rule, 2026-08-08); the operator merges every PR.

Census ground truth: every gating count is **generated** (`cargo test --workspace -- --list`;
`pytest --collect-only -q`; JUnit XML), never hand-written. Baseline numbers recorded at the pin by
the PR-4 procedure; the stale PLAN.md table is replaced at phase close. Static orientation counts:
binding ~6,653 lines Rust; facade 53 modules / ~46 KLOC / 127 test files (~2,350 static `def
test_`); parity core 85 lines + 64 generated test names (corrected from the static 58 at PR-4) + census machinery; `repark-ml` 1,703 lines.

## 0. Deliverables

- `crates/repark-ml` (tier 3, verbatim) and `crates/repark-python` (tier 4 "bindings"; edit classes
  EC-1/2/3/5/6/10) — tier rows pre-declared in PR-1.
- `python/repark` (the wheel; verbatim + EC-4 deferral ledger + EC-7 map regen + EC-9 hygiene
  scrub) and `python/repark-parity` (verbatim + additive `--classic` + the NEW report comparator).
- uv workspace (members staged: parity in PR-4, facade in PR-5; `uv.lock` checked in), ruff
  per-file-ignores verbatim, `check_lib_py` + eight Makefile targets dual-wired.
- CI: rust job split (PR-1), python job extension/rename (PR-4), `pip-audit.yml` (PR-4),
  `wheels.yml` (PR-5, smoke → required), `parity-live.yml` + net-new `aws-acceptance.yml` (PR-6).
- Census artifacts: v1-pin baseline (four cohorts + stability self-diff + environment manifests,
  PR-4), v2 acceptance run + comparator outputs + reconciliation (PR-7).

## 1. PR slate

| PR | Scope | Verify | Ledger |
|---|---|---|---|
| 1 arming | design+brief in-repo; DAG tier rows + provocations; rust split + contexts; testing.md row-2 note; dialect doc rider | slim | `task/p3a-arming-ledger.md` |
| 2 repark-ml | verbatim crate, identity census (empty `--list` diff); `diff -r` empty **except crate `map.md`** (EC-7, five dead v1 links) | slim | `task/p3b-ml-ledger.md` |
| 3 repark-python | whole crate; door wiring pin FIRST; dep collapse; refuse-arms; EngineRuntime (type→core, instance→binding); check_lib_rs row; type-identity test; panic-ban carve-out; **discharge the PR-2 `docs/ml-design.md` dead-pointer rider (EC-6, 4 sites incl. a runtime error string)** | FULL panel | `task/p3c-binding-ledger.md` |
| 4 parity+census | parity pkg verbatim; `--classic` additive + `--stretch` pin; comparator+tests; census.md; uv root (parity member only); python job extend+rename; pip-audit; **v1 baseline + stability self-diff committed** | slim port / full lens on comparator+flag+baseline | `task/p3d-parity-ledger.md` |
| 5 facade+wheel | 53 modules verbatim; tests minus generated deferral ledger; EC-9 hygiene ledger; uv member+lock; check_lib_py; map regen; wheels.yml (smoke→required) | FULL on edit classes; census lens (collect-only identity) | `task/p3e-facade-ledger.md` |
| 6 tier-2 CI | parity-live armed (nightly+dispatch only); aws-acceptance net-new (OIDC, env-gated, no-delete IAM) | FULL + security lens | `task/p3f-tier2-ledger.md` |
| 7 phase close | v2 census run; comparator ×4; reconciliation append; PLAN re-baseline; retrospective; cutover-note link | FULL | `task/p3g-close-ledger.md` |

Order: **1 → 2 → (3 ∥ 4) → 5 → 6 → 7.** PR-3∥PR-4 have disjoint code footprints; both carry
carve-out edits, so second-to-merge takes the phase-2 union-merge + full re-gate recipe.
Load-bearing ordering rules: parity BEFORE facade (nine facade test files import it);
wheels.yml WITH the wheel package, never before.

Carve-outs (`.github/`, `AGENTS.md`, `CLAUDE.md`, `Makefile`, branch protection) are
orchestrator-only, committed on the owning PR's branch by the orchestrator.

## 2. Execution pattern

Per PR: staged delegated workstreams (builders **Opus 5 medium**) in isolated worktrees under the
session scratchpad → orchestrator assembly + carve-outs → verification per the tier column
(slim = one adversarial verifier, **Opus 5 high**; FULL = four-lens panel — port-fidelity/census,
design-conformance, testing-discipline, public-hygiene — all **Opus 5 high**, plus the named extra
lenses) → fixer (Opus 5 medium) with reproduction pins for every confirmed finding → orchestrator
hygiene passes (BOTH greps, diff + log metadata, zero hits) → push through the local pre-push hook →
PR. The operator merges.

Standing rules bind (AGENTS.md "Delegated-agent standing rules"): delegated agents never call AWS,
never set `REPARK_AWS_ACCEPTANCE` / `REPARK_ACCEPT_*` / `TABLE_BUCKET_ARN` / `REPARK_PG_DSN` or any
gate var; v1 is read-only at the pin (worktree under scratchpad); `cargo test --workspace`, never
`--all-features`; never `--no-verify`; commits authored `TRO-Wolf
<64240326+TRO-Wolf@users.noreply.github.com>` with trailer `Authored-By: Claude (<authoring-model>)`
(`claude-opus-5` for delegated-built PRs, `claude-fable-5` for orchestrator commits); no session
IDs/URLs anywhere.

Census baseline generation (PR-4) and the v2 acceptance run (PR-7) are **orchestrator-run local
procedures** (scratch venvs, network sparse clone of the Apache tree, hours of wall clock) — never
CI, never delegated to agents with env-var access, artifacts committed as evidence.

## 3. Acceptance (phase close = milestone one)

1. Comparator: four cohorts with environment manifests equal and matching denominators —
   either empty diffs (exit 0) or, per design §6.4/§6.6, an attributed-movement table where
   every row names a deferred surface (the comparator exits non-zero on any movement; the
   attribution table is the phase-close evidence that the movement is enumerated, not waved).
2. Identity diffs: `--list` empty for repark-ml + repark-python; `--collect-only` empty for the
   facade after the declared deferral subtraction.
3. Reconciliation append foots: ported ∪ deferred = v1 pin totals, all three populations.
4. `make preflight` green; nine required checks green; every map.md in lockstep; zero
   ignore/skip-in-CI violations; hygiene greps zero across the whole phase's range.
5. PLAN.md baseline table → recorded-run pointer; target-map dated notes landed.
6. User-side items formally listed open: AWS role/OIDC/lifecycle setup, first dispatch runs,
   trusted publishers, cutover-sequencing note (blocks the milestone declaration, not the merge).
7. Retrospective + lessons appended; v1 declared bugfix-only by the operator.

## 4. Decisions record

- 2026-08-08 — competition synthesis: census-first architecture (A) + delivery grafts (C) + four
  B rulings; all judge fatal-flaws fixed in the settled text. Q1 verbatim-layout / re-home
  post-milestone with release-prep gate; Q2 zero ANSI-from-Python; Q3 repark-ml IN, excel/postgres
  refuse-armed; Q7 type-in-core/instance-in-binding; Q8 TT-leak fix AFTER acceptance, paired with
  v1; TT-leak PR-2 variant (C) rejected — a ported test pins the leak's presence.
- 2026-08-08 — operator: phase-3 oversight delegated; actor=Opus medium, critic=Opus high.
- Pending operator decisions carried in the design §11: cutover sequencing (Q10); AWS-side setup;
  first dispatch acceptance runs; registry publishers (first release).
