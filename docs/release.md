# Release engineering

`release.yml` is **wired and proven**: it fires on `v*` tags and publishes the wheel via trusted
publishing (OIDC), so no long-lived upload token lives in the repo or its secrets. Which versions
have shipped is [STATUS.md](../STATUS.md) "Release state" — not restated here. The agent-facing
runbook for cutting a tag is [`.agent/skills/publish-pypi.md`](../.agent/skills/publish-pypi.md);
this page is the registry-side contract and the standing deferrals.

Public ≠ released: the API-forever clock started at the first tagged PyPI release (2026-08-15),
not when the repo went public. Pre-1.0, the API can still move between **minors** — never within
a patch. The rule is "Cadence and versioning" below.

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
the prepared-and-verified sequence is [`.agent/skills/publish-pypi.md`](../.agent/skills/publish-pypi.md).

## Settled at the first tags

Each of these was an open question before 2026-08-15 and is now decided by what actually ships.
They are recorded — not re-opened — so a later change is a deliberate one.

- **abi3 — settled.** One `cp312-abi3` wheel per platform, from the `abi3-py312` PyO3 pin
  (design §4 Q6). Every shipped tag has produced exactly this.
- **Python floor — settled at ≥ 3.12**, the direct consequence of the `abi3-py312` pin.
- **Version SSOT — settled.** `[workspace.package] version` in the root `Cargo.toml`, injected
  into the wheel by maturin (`dynamic = ["version"]`), with the tag/version consistency check in
  `release.yml` as the mechanical gate. The bump rides the release PR.

## Cadence and versioning (pre-1.0)

**Settled 2026-08-20.** This promotes the practice of v0.1.0–v0.5.0 into a rule; it describes
what those seven tags already did, and from here it binds. This section is the single home for
the policy — [`.agent/skills/publish-pypi.md`](../.agent/skills/publish-pypi.md) points here for
"the version to cut is decided" and states no version rule of its own.

### When a tag is cut

**On demand, by the owner. There is no schedule and no release train.** A tag is cut when the
owner calls for one and [STATUS.md](../STATUS.md) shows no release blocker — not because time
passed and not because a PR merged. `main` is expected to be releasable at all times, which is
what makes on-demand safe; it is not a standing obligation to release.

Nothing accumulates a "pending release" state. A merge that ships no tag is simply unreleased
until the owner decides otherwise, and STATUS.md is where that truth lives.

### Which number moves

| The tag carries | Bump | Precedent |
|---|---|---|
| New capability, a behavior change, or any move in the public API surface | **minor** (`0.N.0`) | v0.4.0 (functions surface), v0.5.0 (native `dynamicFlatten`) |
| Fixes and performance only, with the public API surface unmoved | **patch** (`0.N.M`) | v0.3.1 (TA-kernel perf), v0.3.2 (mixed-case bind fix) |

Pre-1.0, the leading `0.` means the API can still move between **minors** — that is the whole
content of the pre-1.0 promise, and it does not weaken the patch rule below.

### The one hard rule: a patch never breaks

**A patch tag is strictly non-breaking.** Any of the following forces a minor, however small the
diff:

- a public door removed, renamed, or given a different signature;
- a default value or default behavior changed;
- a refuse that **stops** firing, or that changes its error token or shape for input that was
  already accepted or already refused.

The single deliberate carve-out: a **new fail-closed refuse that tightens behavior which was
previously wrong** may ride a patch, because that is a bug fix — v0.3.2's shape. It is a
carve-out, not a loophole; if the previous behavior was defensible, the tightening is a minor.

The point of the rule is that a user upgrading `0.N.M → 0.N.M+1` needs no reading. Upgrading a
minor pre-1.0 means reading STATUS.md.

### Published versions are immutable

A published version is never re-tagged, re-uploaded, or deleted — PyPI filenames are
permanently claimed, and `release.yml` would fail the upload anyway. **A bad release is
superseded by the next patch, never patched in place.** Yanking is reserved for an artifact that
is actively harmful to install (it hides the version from resolvers without freeing the name)
and is an owner decision, recorded in STATUS.md when it happens.

### What ends pre-1.0

No date. 1.0 is the point where the API stops moving without a major bump, so it is gated on
these being true and stated — each is currently open:

1. **The frozen surface is named.** An explicit list of what 1.0 freezes versus what stays
   internal and free to move. Today no such list exists; `SessionExtension` is one known example
   of a seam deliberately kept out of the wheel
   ([session-extension-conf-seam.md](design/session-extension-conf-seam.md)).
2. **The crates.io deferral is resolved or accepted as permanent.** Shipping 1.0 wheel-only is a
   legitimate answer — but a chosen one, not an inherited blocker (see the crates.io section
   above).
3. **The `planned` components have landed or been dropped.** `repark-exec`, `repark-io`, and
   `repark-connect` are `planned` rows in `repo-manifest.toml`; 1.0 should not carry named homes
   for code nobody intends to write.
4. **The wheel matrix is a decision.** 1.0 on a manylinux-x86_64-only floor is defensible;
   arriving there by never deciding is not.
5. **Parity claims are measured, not argued.** The Spark-door divergences a 1.0 user would hit
   are documented where they can find them, not only in unit ledgers.

## Open items

- **Wheel matrix** — which platforms/architectures get prebuilt wheels beyond the manylinux
  x86_64 floor that ships today (macOS arm64, Windows, musllinux TBD). Also 1.0 criterion 4.
- **Signing / attestation** — PyPI attestations already come free with trusted publishing and
  are present on the shipped wheels; whether to add Sigstore signing for the crates and GitHub
  release artifacts is still open.
