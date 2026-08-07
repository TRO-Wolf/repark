#!/usr/bin/env python3
"""Enforce the internal crate-layering rule over `cargo metadata` on stdin (BH4).

This module is the SSOT for RePark's crate layering: `TIERS` below is the tier map, and prose
(`crates/map.md`, `AGENTS.md`) points here rather than restating it.

The rule is an **intent** rule — what each crate is FOR, not how deep it currently sits:

    no `repark-*` crate may depend on a crate in a STRICTLY HIGHER tier;
    same-tier edges are allowed.

Same-tier edges are legitimate (a future sibling capability crate may sit beside another at
the same tier). A strict "must be strictly lower" rule would
red-flag those, and over *measured* depth it degenerates into a cycle check that
Cargo already enforces as a hard error. What this guard catches is the acyclic-but-INVERTED
edge — a foundation crate reaching up into an orchestration crate — which Cargo accepts
happily and which no prose invariant can prevent.

Scope: NORMAL dependency edges only. dev-dependencies and build-dependencies are excluded (a
test-only edge is not a layering statement); third-party crates are out of scope.
"""

from __future__ import annotations

import json
import sys

INTERNAL_PREFIX = "repark-"

# tier number -> human-readable role of that layer.
TIER_NAMES: dict[int, str] = {
    0: "foundation",
    1: "table service",
    2: "engine session",
    3: "spark surface",
}

# The tier map. A new `repark-*` crate MUST be added here or this guard fails loudly —
# an unclassified crate is an unstated layering claim. Crates listed here that do not exist
# in the workspace yet are simply not inspected (the guard reads `cargo metadata`), so the
# map may carry the phase-1 target shape before every crate lands.
#
# `repark-iceberg`'s catalog half deliberately speaks `datafusion::error::Result` (no
# error-seed dependency on that surface); the conversion happens in `repark-core`. That is a
# documented choice, not a missing edge — this guard only bans edges, it never requires one.
TIERS: dict[str, int] = {
    "repark-common": 0,
    "repark-iceberg": 1,
    "repark-core": 2,
    # Phase 2 (spark surface) — pre-declared; crates not yet in the workspace are skipped.
    "repark-functions": 3,
    "repark-ta": 3,
    "repark-spark": 3,
    "repark-sql": 3,
}


def describe_tier(tier: int) -> str:
    """Render a tier as `N (role)` for use in an error message."""
    return f"tier {tier} ({TIER_NAMES.get(tier, 'unnamed')})"


def collect_violations(metadata: dict) -> tuple[list[str], int]:
    """Return (error messages, number of internal normal edges inspected).

    Errors cover both unclassified `repark-*` crates and upward edges. The edge count is
    reported on success so a silently-empty run (wrong metadata, wrong cwd) is visible.
    """
    errors: list[str] = []
    edge_count = 0
    packages = [p for p in metadata.get("packages", []) if p["name"].startswith(INTERNAL_PREFIX)]

    for package in sorted(packages, key=lambda p: p["name"]):
        if package["name"] not in TIERS:
            errors.append(
                f"ERROR: {package['name']} is not in the tier map "
                f"(scripts/check_crate_dag.py TIERS) — classify it before it can be depended on."
            )
    if errors:
        return errors, 0

    for package in sorted(packages, key=lambda p: p["name"]):
        source_tier = TIERS[package["name"]]
        for dependency in package.get("dependencies", []):
            # kind is null for a normal dependency, "dev" / "build" otherwise.
            if dependency.get("kind") is not None:
                continue
            if not dependency["name"].startswith(INTERNAL_PREFIX):
                continue
            if dependency["name"] not in TIERS:
                errors.append(
                    f"ERROR: {package['name']} depends on {dependency['name']}, which is not in "
                    f"the tier map (scripts/check_crate_dag.py TIERS)."
                )
                continue
            edge_count += 1
            target_tier = TIERS[dependency["name"]]
            if target_tier > source_tier:
                errors.append(
                    f"ERROR: layering inversion — {package['name']} "
                    f"[{describe_tier(source_tier)}] depends on {dependency['name']} "
                    f"[{describe_tier(target_tier)}]. A crate may not depend on a strictly "
                    f"higher tier; same-tier and downward edges are allowed."
                )
    return errors, edge_count


def main() -> int:
    """Read `cargo metadata --format-version 1 --no-deps` JSON from stdin and apply the rule."""
    try:
        metadata = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        print(f"ERROR: could not parse cargo metadata JSON — {error}", file=sys.stderr)
        return 1

    errors, edge_count = collect_violations(metadata)
    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        print(
            "crate-dag: layering rule violated (see scripts/check_crate_dag.py for the map).",
            file=sys.stderr,
        )
        return 1
    internal_count = sum(
        1 for p in metadata.get("packages", []) if p["name"].startswith(INTERNAL_PREFIX)
    )
    if internal_count == 0:
        print(
            "ERROR: crate-dag inspected zero internal crates — metadata looks wrong.",
            file=sys.stderr,
        )
        return 1
    if edge_count == 0 and internal_count > 1:
        # With 2+ internal crates present the workspace must have at least one internal edge;
        # zero means the metadata (or cwd) is wrong. A single-crate workspace legitimately
        # has zero internal edges (the phase-1 PR-A state), so it is not an error.
        print(
            "ERROR: crate-dag inspected zero internal edges — metadata looks wrong.",
            file=sys.stderr,
        )
        return 1
    print(
        f"crate-dag: {edge_count} internal edges clean across "
        f"{internal_count} of {len(TIERS)} mapped crates"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
