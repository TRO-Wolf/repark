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
3. If the project does not exist yet, use **pending publishers** (PyPI account → Publishing →
   "Add a pending publisher") with the same values; the first trusted-publish then creates the
   project.

The publishing job in `release.yml` (when it exists) needs `permissions: id-token: write` on the
publish job only, and uses `pypa/gh-action-pypi-publish` (SHA-pinned, like every action here).

## crates.io — Trusted Publishing

crates.io supports the equivalent [Trusted
Publishing](https://crates.io/docs/trusted-publishing) flow for GitHub Actions:

1. As the crate owner, open each `repark-*` crate on crates.io → **Settings** → **Trusted
   Publishing** → **Add**.
2. Configure: repository owner `TRO-Wolf`, repository `repark`, workflow file `release.yml`,
   and (recommended) the `release` environment.
3. First-time publishes of a new crate name still require a classic token once (crates.io
   trusted publishing cannot create a crate that does not exist); do that manually from a
   maintainer machine with a scoped, short-expiry token, then configure the trusted publisher
   and revoke the token.

## Bootstrap tokens — revoke

Any upload tokens created to bootstrap the package names (one PyPI, one crates.io) are
**temporary**. Once trusted publishing is configured on both registries, revoke both tokens.
No registry token is ever stored as a GitHub Actions secret.

## Open items (decide before the first tagged release)

- **Wheel matrix** — which platforms/architectures get prebuilt wheels (manylinux x86_64 is the
  floor; macOS arm64, Windows, musllinux TBD).
- **abi3** — whether the PyO3 cdylib builds abi3 wheels (one wheel per platform) or per-Python
  wheels.
- **Python floor** — the minimum supported Python version at release (≥ 3.12 is the working
  assumption; confirm at release).
- **Cadence** — release cadence and versioning policy (pre-1.0 semantics).
- **Signing / attestation** — PyPI attestations come free with trusted publishing; decide
  whether to add Sigstore signing for the crates and GitHub release artifacts.
