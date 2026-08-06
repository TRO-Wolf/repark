# Contributing to RePark

RePark is **source-open, contribution-gated** while the port from the private v1 repository is in
progress.

## What is welcome now

- **Issues and bug reports** — very welcome. Include the smallest reproduction you can, the RePark
  version/commit, and (for engine behavior) the expected result from the engine you are comparing
  against (Spark, Trino, DuckDB).
- **Discussion and design feedback** via issues.
- **Security reports** — privately, per [SECURITY.md](SECURITY.md), never as a public issue.

## What is not accepted yet

**External code pull requests are not accepted during the port.** The project is being ported phase
by phase from a private v1 codebase under a strict internal process; unsolicited PRs will be closed
with thanks. This policy will be revisited after the port's milestone one (the full v1 test suite
green on the new skeleton).

## Maintainer-side process (for context)

Work in this repo — by the maintainer and delegated agents — runs under an enforced discipline:

- **SEPMO** ([skills/sepmo/SKILL.md](skills/sepmo/SKILL.md)) governs how work flows: scope audit →
  adversarial Actor–Critic → per-PR delivery → retrospective.
- **Briefs** ([briefs/](briefs/)) define delegated work units; standing rules live in
  [AGENTS.md](AGENTS.md) "Delegated-agent standing rules".
- **map.md lockstep**: every directory carries a `map.md`, updated in the same change as the code
  it describes (`scripts/check_map_md.sh` enforces this).
- **Tests-with-code**: tests land in the same commit as the code being tested — a hard block, per
  [docs/testing.md](docs/testing.md).
- `make ci` is the canonical gate; `make preflight` mirrors the full CI surface.

A PR that ignores this discipline fails CI red — the gates are mechanical.

## Security

See [SECURITY.md](SECURITY.md) for how to report a vulnerability privately.
