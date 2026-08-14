# Release engineering

Documentation only in phase 0: nothing here is wired yet. The release workflow (`release.yml`)
lands at the **first tagged release** — not before — and publishes via trusted publishing (OIDC)
on both registries, so no long-lived upload tokens live in the repo or its secrets.

Public ≠ released: the API-forever clock starts at the first tagged PyPI release, not when the
repo went public.

## PyPI — trusted publishing (OIDC)

The `repark` project on PyPI publishes via [trusted
publishing](https://docs.pypi.org/trusted-publishers/): PyPI trusts short-lived OIDC tokens
minted by GitHub Actions for a specific repo + workflow, so no API token is stored anywhere.

Maintainer setup (registry-side, one-time, done when `release.yml` lands):

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

The publishing job in `release.yml` (when it exists) needs `permissions: id-token: write` on the
publish job only, and uses `pypa/gh-action-pypi-publish` (SHA-pinned, like every action here).

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

No hard blocker remains. The first tag is an owner action: registry-side trusted-publisher
setup below, then `git tag v<version> && git push origin v<version>`.

## Open items (decide before the first tagged release)

- **Wheel matrix** — which platforms/architectures get prebuilt wheels (manylinux x86_64 is the
  floor; macOS arm64, Windows, musllinux TBD).
- **abi3** — whether the PyO3 cdylib builds abi3 wheels (one wheel per platform) or per-Python
  wheels. *(Effectively settled by the v1 pin: `abi3-py312`, one `cp312-abi3` wheel per
  platform — confirm and close at release; design §4 Q6.)*
- **Python floor** — the minimum supported Python version at release (≥ 3.12 is the working
  assumption; confirm at release).
- **Cadence** — release cadence and versioning policy (pre-1.0 semantics). Version SSOT shape is
  recorded (design §4 Q6): `dynamic = ["version"]` from the crate version + a wheel-path test —
  the edit rides the release PR.
- **Signing / attestation** — PyPI attestations come free with trusted publishing; decide
  whether to add Sigstore signing for the crates and GitHub release artifacts.
