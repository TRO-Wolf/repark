# map — .github/

## Purpose

CI/CD, dependency automation, and the rust-cache pre-warmer that keeps PR jobs hot off the
critical path. Phase 1 carries tier 1 (every PR, no secrets, GitHub-hosted, read-only token);
tier 2 (live AWS, merged code only) and tier 3 (benchmarks) land in later phases.

## Contents

- [workflows/](workflows/map.md) — GitHub Actions (the gates; includes `audit.yml` cargo-audit
  CVE scanning and `cache-warm.yml`, the rust-cache pre-warmer paired with the `ci.yml`
  restore keys).
- `dependabot.yml` — weekly grouped dependency PRs (cargo + uv + github-actions; docker added
  later). Carries the DataFusion-family rule: never merge a bundled DF/Arrow major bump — split
  it.
- `zizmor.yml` — zizmor config: accepted-risk suppressions (if any); currently empty. The gate
  is otherwise blocking.

## I want to...

| ...do this | go to |
|---|---|
| Change a CI gate | [workflows/map.md](workflows/map.md) |
| Adjust dependency automation | `dependabot.yml` |
| Suppress a zizmor finding (with rationale) | `zizmor.yml` |

## Pointers

- Up: [../map.md](../map.md)
- Related: gates mirror [../Makefile](../Makefile); supply-chain policy in
  [../SECURITY.md](../SECURITY.md); release engineering in [../docs/release.md](../docs/release.md).

## Debug

First checks: workflows are validated by their first run; `make ci` mirrors the core gate
locally, `make preflight` the full CI surface.
Escalate to: [../map.md#debug](../map.md).
