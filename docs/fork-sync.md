# Fork sync — keeping repark and TRO-Wolf/iceberg-rust aligned

RePark consumes the whole `iceberg*` family from the owned fork via `[patch.crates-io]`,
**rev-pinned**: five `rev = "…"` lines in the root [Cargo.toml](../Cargo.toml) that must stay
byte-identical (the **single-writer-per-pin** invariant). This page is the *sync contract* —
the structural decision that the fork stays a sibling repo is
[ADR-0002](adr/0002-own-iceberg-fork.md) /
[ADR-0003](adr/0003-consume-fork-datafusion-integration.md) and is not restated here.

## The three rules

1. **The pin moves only via its own PR.** `make bump-fork-pin REV=<sha|branch>` rewrites all
   five rev lines + `Cargo.lock` and prints the fork changelog URL for the PR body;
   `make preflight` gates the PR like any other. Never bump the pin as a side effect of an
   unrelated change.
2. **Fork main must be green before it is pinnable.** The fork's own CI decides; a red fork
   main is not a valid `REV`.
3. **Upstream flows through the fork, never directly.** `apache/iceberg-rust` improvements
   merge fork-side first, then reach repark as an ordinary pin bump (rule 1). RePark never
   patches Iceberg semantics locally — engine-agnostic table-format work lives in the fork
   (CLAUDE.md / AGENTS.md invariant).

## Drift visibility

The weekly **`fork-sync-drift`** workflow
([.github/workflows/fork-sync-drift.yml](../.github/workflows/fork-sync-drift.yml); also
manually dispatchable) reports three numbers in its run summary, so nobody has to remember
to check:

| Number | Meaning |
|---|---|
| Fork main ahead of pin | commits on the fork's `main` that repark does not consume yet |
| Upstream ahead of fork | commits on `apache/iceberg-rust` `main` past the fork's merge-base |
| Pin reachability | whether the pinned rev still exists on the fork (force-push guard) |

**Soft thresholds:** fork **>10** commits ahead of the pin → schedule a pin bump; upstream
**>50** ahead of the fork → schedule a fork-side upstream merge in the next planning pass.
**Hard stop:** the pin going unreachable — investigate before any other fork action.

## Debug

First checks: dispatch `fork-sync-drift` manually and read the run summary; locally,
`grep -oE 'rev = "[0-9a-f]{40}"' Cargo.toml | sort -u` must print exactly one line.
Escalate to: [map.md](map.md#debug).
