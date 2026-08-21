# Release engineering

`release.yml` is **wired and proven**: it fires on `v*` tags and publishes the wheel via trusted
publishing (OIDC), so no long-lived upload token lives in the repo or its secrets. Which versions
have shipped is [STATUS.md](../STATUS.md) "Release state" — not restated here. The agent-facing
runbook for cutting a tag is [`.agent/skills/publish-pypi/SKILL.md`](../.agent/skills/publish-pypi/SKILL.md);
this page is the registry-side contract and the standing deferrals.

Public ≠ released: the API-forever clock started at the first tagged PyPI release (2026-08-15),
not when the repo went public. Pre-1.0, the API can still move between tags.

## PyPI — trusted publishing (OIDC)

The `repark` project on PyPI publishes via [trusted
publishing](https://docs.pypi.org/trusted-publishers/): PyPI trusts short-lived OIDC tokens
minted by GitHub Actions for a specific repo + workflow, so no API token is stored anywhere.

Maintainer setup (registry-side, one-time — **done 2026-08-15**, recorded here so the wiring
stays auditable):

1. Log in to PyPI as the project owner and open the `repark` project → **Publishing**.
2. **Add a new publisher** with:
   - Publisher: **GitHub**
   - Owner: `TRO-Wolf`
   - Repository name: `repark`
   - Workflow name: `release.yml`
   - Environment: `release` (recommended — bind the publisher to a GitHub environment so
     publishing can carry required reviewers / deployment protection)
3. The `repark` project **already exists** on PyPI (the 0.0.1 name reservation — see
   [STATUS.md](../STATUS.md) "Release state"), so the publisher is added on the **existing
   project** (project → Settings → Publishing), not as a pending publisher — pending publishers
   are only for names that do not exist yet. *(Corrected 2026-08-09; the earlier wording
   described the pending-publisher flow.)*

The publishing job in `release.yml` needs `permissions: id-token: write` on the publish job only, and uses `pypa/gh-action-pypi-publish` (SHA-pinned, like every action here).

## crates.io — Trusted Publishing (DEFERRED — structurally blocked)

**Discovered 2026-08-14 at release-PR drafting:** the workspace sources the whole `iceberg*`
family from the owned fork via `[patch.crates-io]` (rev-pinned). Cargo does not publish
patches: a crate uploaded to crates.io would resolve its `iceberg 0.9.1` dependency against
the REGISTRY release, which lacks the fork's write/commit surface — the published crates
would not compile the engine that repark actually is. PyPI is unaffected (the cdylib links
everything statically).

Unblock path (a later, deliberate decision — not release-blocking): publish the fork's
crates under owned names (e.g. `repark-iceberg-core`) and depend on those directly, or
upstream the fork surface. Until then, `release.yml` publishes the wheel only. The original
trusted-publishing steps below stand for whenever the unblock lands.

## Bootstrap tokens — revoke

Any upload tokens created to bootstrap the package names (one PyPI, one crates.io) are
**temporary**. Once trusted publishing is configured on both registries, revoke both tokens.
No registry token is ever stored as a GitHub Actions secret.

## Hard blockers (the first tag FAILS while any of these holds)

- **RESOLVED 2026-08-14 (#95):** `repark.sql` is no longer a module. The pyspark-alias package
  re-homed to `repark.spark` (with `repark.spark.sql` keeping same-object identity for the
  mechanical `pyspark` → `repark.spark` swap), and `repark.sql()` is the top-level ANSI-door
  callable. `python -c "import repark.sql"` fails; `release.yml` re-verifies this on every
  tag build (the import smoke fails the release if the alias package ever returns).

No hard blocker remains, and none has since the first tag. Cutting a tag is an owner action;
the prepared-and-verified sequence is [`.agent/skills/publish-pypi/SKILL.md`](../.agent/skills/publish-pypi/SKILL.md).

## Settled at the first tags

Each of these was an open question before 2026-08-15 and is now decided by what actually ships.
They are recorded — not re-opened — so a later change is a deliberate one.

- **abi3 — settled.** One `cp312-abi3` wheel per platform, from the `abi3-py312` PyO3 pin
  (design §4 Q6). Every shipped tag has produced exactly this.
- **Python floor — settled at ≥ 3.12**, the direct consequence of the `abi3-py312` pin.
- **Version SSOT — settled.** `[workspace.package] version` in the root `Cargo.toml`, injected
  into the wheel by maturin (`dynamic = ["version"]`), with the tag/version consistency check in
  `release.yml` as the mechanical gate. The bump rides the release PR.

## Open items

- **Wheel matrix** — which platforms/architectures get prebuilt wheels beyond the manylinux
  x86_64 floor that ships today (macOS arm64, Windows, musllinux TBD).
- **Cadence** — release cadence and versioning policy remain **unwritten** (pre-1.0 semantics).
  Practice so far: feature work cuts a minor, fixes cut a patch. Not a rule until it is written
  here.
- **Signing / attestation** — PyPI attestations already come free with trusted publishing and
  are present on the shipped wheels; whether to add Sigstore signing for the crates and GitHub
  release artifacts is still open.
