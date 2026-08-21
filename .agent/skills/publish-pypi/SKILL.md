---
name: publish-pypi
description: >-
  Cut a versioned RePark release and publish the wheel to PyPI. Records the
  proven sequence every shipped tag has followed: the four-file release PR
  (Cargo.toml, Cargo.lock, STATUS.md, root map.md), the squash tree-equality
  check that is the only guard against unreviewed content reaching a tag, the
  annotated tag, the release.yml pipeline and its owner-approved deployment
  gate, and registry verification. Use this skill when the user asks to cut a
  release, tag a version, ship a wheel, or publish to PyPI. Do NOT use it to
  decide WHETHER to release — that gate lives in STATUS.md and the go/no-go is
  the owner's. The merge, the deployment approval, and the go/no-go are owner
  actions; an agent prepares and verifies.
---

# Skill: publish-pypi — tag a release and publish the wheel

An agent-facing runbook for cutting a versioned release to PyPI. It records the **proven
sequence** (the shape every shipped tag has followed); it defines no policy. On any conflict,
the spine wins: [AGENTS.md](../../../AGENTS.md) (precedence, hygiene, gates),
[docs/release.md](../../../docs/release.md) (registry setup, crates.io deferral),
[.github/workflows/release.yml](../../../.github/workflows/release.yml) (the pipeline itself),
and [STATUS.md](../../../STATUS.md) (whether a release gate is even open).

**Owner actions are owner actions.** The squash-merge of the release PR, the approval of the
`release` deployment environment, and the go/no-go itself belong to the repository owner. An
agent prepares and verifies; it does not merge and it does not self-approve.

## 0. Preconditions

- The release gate in [STATUS.md](../../../STATUS.md) is satisfied and the owner has said "release".
- `main` is green and your working tree sits on a fresh `origin/main`.
- The version to cut is decided (pre-1.0 semantics; see docs/release.md "Cadence").

## 1. The release PR (precedent shape — four files, nothing else)

1. **`Cargo.toml`** — bump `[workspace.package] version`. This is the single version SSOT;
   maturin injects it into the wheel (pyproject `dynamic = ["version"]`). Bump nowhere else.
2. **`Cargo.lock`** — regenerate with
   `cargo update --workspace --offline`. The diff must be exactly the nine `repark-*`
   workspace members' `version` lines and nothing more. (This is the one sanctioned lockfile
   edit; outside a release PR the lockfiles are untouchable — AGENTS.md.)
3. **`STATUS.md`** — rewrite "Release state" and "Release blockers" to the new truth; update
   the last-updated stamp.
4. **root `map.md`** — the one SSOT sentence that names the version number.

Then: `make preflight` locally (must exit 0), the standard two-pass content hygiene check,
and a PR whose body states the version, the payload summary, and the lock-delta line count.
All required checks must be green. The owner squash-merges.

## 2. Verify the squash, then tag

1. Confirm the squash commit's tree equals the reviewed branch head's tree:
   `git rev-parse <reviewed-sha>^{tree}` must equal the merge commit's `tree.sha`
   (`gh api repos/<owner>/<repo>/commits/<squash-sha>`). A mismatch means the merge picked up
   something unreviewed — stop and re-review.
2. Create an **annotated** tag `v<version>` on the squash commit and push it. If the tag is
   created through the GitHub API (`POST /repos/…/git/tags` + `/git/refs`) rather than a local
   `git push`, note that API-created tags **bypass local pre-push hooks** — apply the content
   hygiene check to the tag message by hand before creating it.

## 3. The pipeline (release.yml, trigger: `v*` tags)

The tag fires `release.yml`, which in order:

1. Builds the manylinux wheel with maturin (one `cp312-abi3` wheel).
2. Checks tag/version consistency — the wheel filename must carry the tag's version.
3. Runs the import smoke: `import repark.sql` must **fail** (the alias package must never
   return) and the top-level `repark.sql` callable must exist.
4. Publishes via `pypa/gh-action-pypi-publish` under the **`release` environment** using OIDC
   trusted publishing — no stored tokens anywhere. The environment carries a required
   reviewer: the run **pauses until the owner approves the deployment** in the GitHub UI.
   That pause is the designed human gate, not a failure.

crates.io publishing is structurally deferred (the `[patch.crates-io]` iceberg fork) — see
docs/release.md. The pipeline publishes the wheel only.

## 4. Verify on the registry, then close out

1. `https://pypi.org/pypi/repark/json` — the latest version equals the tag and the wheel
   filename is `repark-<version>-cp312-abi3-manylinux…`.
2. Run the [compact-context-docs](../compact-context-docs/SKILL.md) ritual so STATUS.md and every doc
   that names release state reflect the shipped tag.

## Gotchas (each has bitten a real release)

- Skipping the tree-equality check (step 2.1) is the only gap through which unreviewed content
  can reach a tag. Never skip it.
- The `release` environment approval can race: if the owner approves in the UI while an agent
  is polling, the agent's approval call 422s ("no pending deployment"). Harmless — confirm the
  run finished instead of retrying.
- The lock delta drifting beyond the nine member lines means `cargo update` pulled dependency
  churn — regenerate offline, never ship extra churn in a release PR.
