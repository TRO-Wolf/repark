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

## Hard blockers (the first tag FAILS while any of these holds)

- **`repark.sql` is still a module.** The phase-3 design
  ([design/python-facade.md](design/python-facade.md) Q1) ships the ported pyspark-alias
  *package* at `repark.sql` and defers the re-home (`repark.spark` facade; `repark.sql()` the
  ANSI-door *function*) to its own post-milestone-one design pass. That deferral is legal only
  pre-release — a tag with the alias package in place would make it an API-forever promise.
  The release PR's checklist verifies `python -c "import repark.sql"` fails (or imports a
  callable's owner, not the alias package) before tagging; while it succeeds, there is no tag.
  Added 2026-08-08 with the phase-3 arming PR; the settled rulings live in the design doc.

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
