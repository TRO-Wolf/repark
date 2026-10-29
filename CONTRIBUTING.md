# Contributing to RePark

RePark is **source-open, contribution-gated**: the engine has shipped releases since
2026-09-03 (current release in [STATUS.md](STATUS.md)), and external code contributions are
still gated by the maintainer-side process below.
Current state — what is delivered, what is deferred, and release state — is
[STATUS.md](STATUS.md); it is the source of truth, and this file does not restate it.

## What is welcome now

- **Issues and bug reports** — very welcome. Include the smallest reproduction you can, the RePark
  version/commit, and (for engine behavior) the expected result from the engine you are comparing
  against (Spark, Trino, DuckDB).
- **Discussion and design feedback** via issues.
- **Security reports** — privately, per [SECURITY.md](SECURITY.md), never as a public issue.

## What is not accepted yet

**External code pull requests are not accepted yet.** Work here runs under a strict internal
process (see below); unsolicited PRs will be closed with thanks. The port that stood this
repository up is complete and releases are tagged, so the gate is the process itself: every
change lands through the maintainer's lane-and-verifier pipeline, which has no seat for
external branches yet. The maintainer revisits the policy as that process matures;
[STATUS.md](STATUS.md) "Current milestone" records the current state.

## Maintainer-side process (for context)

Work in this repo — by the maintainer and delegated agents — runs under an enforced discipline:

- **SEPMO** ([.agents/skills/sepmo/SKILL.md](.agents/skills/sepmo/SKILL.md)) governs how work flows: scope audit →
  adversarial Actor–Critic → per-PR delivery → retrospective.
- **Briefs** ([briefs/](briefs/)) define delegated work units; standing rules live in
  [AGENTS.md](AGENTS.md) "Delegated-agent standing rules".
- **map.md**: a map where the architecture needs one (AGENTS.md "Markdown document lifecycle"). Its
  links and size are gated (`make check-map-sync`, `make check-map-size`); lockstep with the code
  only warns (`scripts/check_map_md.sh`).
- **Tests-with-code**: tests land in the same commit as the code being tested — a hard block, per
  [docs/testing.md](docs/testing.md).
- `make ci` is the canonical gate; `make preflight` mirrors the full CI surface.

A PR that ignores this discipline fails CI red — the gates are mechanical.

## Security

See [SECURITY.md](SECURITY.md) for how to report a vulnerability privately.
