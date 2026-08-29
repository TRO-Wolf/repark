#!/usr/bin/env python3
"""Enforce the internal crate dependency policy over `cargo metadata` on stdin (BH4).

This module is the SSOT for RePark's crate structure: `TIERS` is the tier map, `ROLES` names
what each crate IS architecturally, and `ALLOWED_EDGES` is the explicit table of every internal
dependency edge with the dependency KINDS it may take. Prose points here rather than restating
any of the three.

Dependency kinds: `normal` — a product edge; `optional` — a product edge behind a cargo
feature; `dev` — a test/bench-only edge, invisible to the product graph; `build` — a
build-script edge.

Four rules, checked in this order:

1. Declaration audit. The policy table itself must obey the structural rules below — a
   forbidden edge cannot be legalized by writing it down.
2. Explicit allowed edges. Every observed internal edge must appear in `ALLOWED_EDGES` with its
   kind permitted for that pair; a new edge, including a SAME-TIER one, fails until declared
   with a reason.
3. Structural rules (over `ROLES`, applied to declared AND observed edges): no door -> door
   edge outside `dev`; nothing may depend on the bindings adapter; the foundation crate depends
   on nothing internal; a capability (kernel/function) crate never depends on a user-facing
   door.
4. Layering (the original BH4 rule, unchanged): no `repark-*` crate may depend on a STRICTLY
   HIGHER tier over a PRODUCT edge (`normal` / `optional`); same-tier edges are allowed.

Rule 4 stays an intent rule (what each crate is FOR, not measured depth): what it catches is
the acyclic-but-INVERTED edge Cargo accepts and no prose invariant can prevent. Scope:
INTERNAL means any Cargo workspace member — membership, not the `repark-` name, is the test.
All four kinds are in scope for the edge table; only PRODUCT kinds are subject to the tier
rule; third-party crates are out of scope.
"""

from __future__ import annotations

import json
import sys

# Internal = any Cargo workspace member: membership, not the name, makes a crate internal. The
# prefix is belt-and-braces on the TARGET side only: a third-party dep carrying the family name
# is inspected (and reds as unclassified), never silently skipped.
INTERNAL_PREFIX = "repark-"

# The dependency-kind vocabulary. `normal` / `optional` are PRODUCT kinds: they ship.
KINDS: frozenset[str] = frozenset({"normal", "optional", "dev", "build"})
PRODUCT_KINDS: frozenset[str] = frozenset({"normal", "optional"})

# tier number -> human-readable role of that layer.
TIER_NAMES: dict[int, str] = {
    0: "foundation",
    1: "table service",
    2: "engine session",
    3: "surface crates",
    # Tier 4 states the real rule: nothing may ever depend on the bindings adapter.
    4: "bindings",
}

# A new `repark-*` crate MUST be added here or this guard fails loudly — an unclassified crate
# is an unstated layering claim. Rows for crates not yet in the workspace are simply not
# inspected, so the map may carry the target shape. `repark-iceberg`'s catalog half
# deliberately speaks `datafusion::error::Result` (converted in `repark-core`): a documented
# choice, not a missing edge — this guard only bans edges, it never requires one.
TIERS: dict[str, int] = {
    "repark-common": 0,
    "repark-iceberg": 1,
    "repark-core": 2,
    "repark-functions": 3,
    "repark-ta": 3,
    "repark-spark": 3,
    "repark-sql": 3,
    "repark-ml": 3,
    "repark-python": 4,
}

# The role vocabulary. The structural rules quantify over exactly these; `audit_policy`
# rejects any ROLES value outside this set, because an unrecognized role matches no rule and
# must fail loudly, never pass quietly.
ROLE_NAMES: frozenset[str] = frozenset(
    {"foundation", "table service", "engine", "capability", "door", "bindings"}
)

# What each crate IS, architecturally — the vocabulary the structural rules quantify over. A
# tier says how deep a crate sits; a role says what it is FOR. Every crate in `TIERS` needs a
# row here (and vice versa) or the declaration audit fails.
#
#   foundation     the bottom: shared seed types, no internal deps
#   table service  the Iceberg surface (catalogs + the write adapter over the owned fork)
#   engine         the session/engine API the doors and bindings plug into
#   capability     a kernel / function leaf (Spark fns, TA kernels, ML estimators)
#   door           a USER-FACING SQL dialect surface; each keeps its own grammar
#   bindings       the FFI adapter; reaches down, is never depended upon
ROLES: dict[str, str] = {
    "repark-common": "foundation",
    "repark-iceberg": "table service",
    "repark-core": "engine",
    "repark-functions": "capability",
    "repark-ta": "capability",
    "repark-ml": "capability",
    "repark-spark": "door",
    "repark-sql": "door",
    "repark-python": "bindings",
}

# The explicit edge table: (source, target) -> (permitted kinds, why the edge exists). EVERY
# internal edge must appear here, so adding a dependency is a two-file change — the manifest
# and this table. Rows whose endpoints both exist but whose edge is gone are reported as stale.
ALLOWED_EDGES: dict[tuple[str, str], tuple[frozenset[str], str]] = {
    ("repark-core", "repark-common"): (
        frozenset({"normal"}),
        "the error seed (Error / ErrorClass / Result), re-exported at the engine root so doors "
        "and bindings import one crate",
    ),
    ("repark-core", "repark-iceberg"): (
        frozenset({"normal"}),
        "catalog builders + provider registration and the write-knob ConfigExtension installers "
        "that `build()` threads",
    ),
    ("repark-iceberg", "repark-common"): (
        frozenset({"normal"}),
        "the write half re-exports `repark_common::{Error, Result}`; the catalog half stays "
        "DataFusion/iceberg-native and folds one layer up",
    ),
    ("repark-ta", "repark-core"): (
        frozenset({"optional"}),
        "`TaExtension` implements the `SessionExtension` seam — OPTIONAL and feature-tied "
        "(`datafusion`) so the kernel core stays dependency-light and independently publishable",
    ),
    ("repark-spark", "repark-core"): (
        frozenset({"normal"}),
        "the phase-1 seams this door plugs into: SqlDialect/EngineContext, SessionExtension, the "
        "CatalogRegistry and the time-travel pin half",
    ),
    ("repark-spark", "repark-iceberg"): (
        frozenset({"normal"}),
        "the catalog wiring + the table-mutating primitives (ALTER, MERGE, namespace "
        "invalidation) the statement router delegates to",
    ),
    ("repark-spark", "repark-functions"): (
        frozenset({"normal"}),
        "`analyze_eagerly` — the one blessed way to analyze a plan before its schema crosses a "
        "boundary — plus the SEC-02 `repark.sql.*` settings reader. Same-tier, DAG-legal",
    ),
    ("repark-spark", "repark-ta"): (
        frozenset({"normal"}),
        "`SparkExtension` composes `TaExtension` at v1's registration position so a Spark session "
        "keeps the TA window UDFs callable. The TA set is door-neutral; this door is a consumer",
    ),
    ("repark-spark", "repark-common"): (
        frozenset({"dev"}),
        "DEV-ONLY: `repark_common::surfaces`, the dialect-neutral registry this door's "
        "`matrix.rs` audit compiles against. Nothing in `src/` reads it; it graduates to a "
        "product edge the day product code does",
    ),
    ("repark-sql", "repark-core"): (
        frozenset({"normal"}),
        "the frozen `SqlDialect` / `EngineContext` seam this door implements, plus "
        "CatalogRegistry + LocationPolicy routing and the error fold",
    ),
    ("repark-sql", "repark-iceberg"): (
        frozenset({"normal"}),
        "the tier-1 Iceberg surface the handlers delegate to: provider invalidation, namespace "
        "location resolution, scheme-selected FileIO, the staged-write helpers",
    ),
    ("repark-sql", "repark-common"): (
        frozenset({"normal"}),
        "kept explicit so a handler can name `repark_common::Error` at a seam",
    ),
    ("repark-sql", "repark-spark"): (
        frozenset({"dev"}),
        "DEV-ONLY, and the reason this table carries kinds at all: the cross-door two-session "
        "protocol needs the OTHER door's dialect and extension in one TEST binary. A `normal` "
        "edge here is the forbidden door -> door product edge — nothing in `src/` may name it",
    ),
    ("repark-sql", "repark-ta"): (
        frozenset({"dev"}),
        "DEV-ONLY: the ANSI TA toll — `TaExtension` on a NATIVE session, one kernel driven "
        "through THIS door's SQL and compared bit-exact against the golden-gated kernel",
    ),
    ("repark-python", "repark-core"): (
        frozenset({"normal"}),
        "the engine API the binding adapts; v1's error-seed and session crates collapse into "
        "this one entry because `repark-core` re-exports the taxonomy (EC-1)",
    ),
    ("repark-python", "repark-functions"): (
        frozenset({"normal"}),
        "the Spark date-function `Expr` builders `PyColumn` composes, so a standalone column "
        "expression carries the exact UDF the session registers for the SQL path",
    ),
    ("repark-python", "repark-ta"): (
        frozenset({"normal"}),
        "the TA window UDFs `PyColumn.ta_window` builds (feature `datafusion`)",
    ),
    ("repark-python", "repark-ml"): (
        frozenset({"normal"}),
        "the native estimator kernels the fit binder streams Arrow batches into",
    ),
    ("repark-python", "repark-spark"): (
        frozenset({"normal"}),
        "the Spark DOOR the constructor installs (EC-2). Deliberate NON-edges, enforced by "
        "review because this guard bans edges and never requires one: `repark-sql` (no ANSI "
        "surface from Python) and `repark-iceberg` (reached only through the session + SQL text)",
    ),
    ("repark-python", "repark-common"): (
        frozenset({"dev"}),
        "DEV-ONLY: the EC-1 type-identity guard names `repark_common::Error` alongside "
        "`repark_core::Error`, which is what makes 'the same type, re-exported' a compile error "
        "to break. The binding's product dep list stays the five crates the design names",
    ),
}


def describe_tier(tier: int) -> str:
    """Render a tier as `N (role)` for use in an error message."""
    return f"tier {tier} ({TIER_NAMES.get(tier, 'unnamed')})"


def describe_kinds(kinds: frozenset[str]) -> str:
    """Render a permitted-kind set in a stable order for an error message."""
    return ", ".join(sorted(kinds))


def edge_kind(dependency: dict) -> str:
    """Map a `cargo metadata` dependency entry onto this policy's kind vocabulary."""
    # `kind` is null for a normal dependency, "dev" / "build" otherwise; `optional` splits a
    # feature-gated product edge out of `normal` so the two can be permitted separately.
    kind = dependency.get("kind")
    if kind in ("dev", "build"):
        return str(kind)
    return "optional" if dependency.get("optional") else "normal"


def forbidden_reason(source: str, target: str, kind: str) -> str | None:
    """Return why this edge SHAPE is structurally forbidden, or None if it is permitted.

    Role-based, so it holds for edges that do not exist yet: it is applied to the declared
    policy table as well as to the observed workspace.
    """
    source_role = ROLES.get(source, "unclassified")
    target_role = ROLES.get(target, "unclassified")
    if target_role == "bindings":
        return (
            "nothing may depend on the bindings adapter — it reaches DOWN into the engine and is "
            "never depended upon (no kind is permitted)"
        )
    if source_role == "foundation":
        return (
            "the foundation crate depends on nothing internal — it is the bottom that keeps the "
            "DAG acyclic (no kind is permitted)"
        )
    if source_role == "door" and target_role == "door" and kind in PRODUCT_KINDS:
        return (
            "no door -> door product edge, ever: each door keeps its own grammar and they meet "
            "only at tiers 0-1. A `dev` edge for the cross-door test protocol is the only "
            "permitted form"
        )
    if source_role == "capability" and target_role == "door":
        return (
            "a capability crate (kernel / function leaf) may not depend on a user-facing door — "
            "kernels are door-neutral and doors consume them, never the reverse (no kind is "
            "permitted)"
        )
    return None


def audit_policy() -> list[str]:
    """Audit the policy tables themselves, before any workspace edge is inspected."""
    errors: list[str] = []
    for crate, role in sorted(ROLES.items()):
        if role not in ROLE_NAMES:
            errors.append(
                f"ERROR: {crate} has unrecognized role {role!r} (scripts/check_crate_dag.py "
                f"ROLES) — roles are: {', '.join(sorted(ROLE_NAMES))}. An unknown role matches "
                f"no structural rule and would silently disable them all."
            )
    for crate, tier in sorted(TIERS.items()):
        if tier not in TIER_NAMES:
            errors.append(
                f"ERROR: {crate} has unrecognized tier {tier} (scripts/check_crate_dag.py "
                f"TIERS) — tiers are: {', '.join(str(t) for t in sorted(TIER_NAMES))}."
            )
    for crate in sorted(set(TIERS) | set(ROLES)):
        if crate not in TIERS:
            errors.append(
                f"ERROR: {crate} has a ROLES entry but no TIERS entry "
                f"(scripts/check_crate_dag.py) — every crate needs both."
            )
        if crate not in ROLES:
            errors.append(
                f"ERROR: {crate} has a TIERS entry but no ROLES entry "
                f"(scripts/check_crate_dag.py) — every crate needs both."
            )

    for (source, target), (kinds, _reason) in sorted(ALLOWED_EDGES.items()):
        for endpoint in (source, target):
            if endpoint not in TIERS:
                errors.append(
                    f"ERROR: ALLOWED_EDGES names {endpoint} ({source} -> {target}), which is not "
                    f"in the tier map (scripts/check_crate_dag.py TIERS)."
                )
        if not kinds:
            errors.append(
                f"ERROR: ALLOWED_EDGES row {source} -> {target} permits no kinds — "
                f"delete the row instead."
            )
        for kind in sorted(kinds):
            if kind not in KINDS:
                errors.append(
                    f"ERROR: ALLOWED_EDGES row {source} -> {target} names unknown kind "
                    f"'{kind}' — kinds are: {describe_kinds(KINDS)}."
                )
                continue
            reason = forbidden_reason(source, target, kind)
            if reason is not None:
                errors.append(
                    f"ERROR: the policy DECLARES a forbidden edge — {source} -> {target} "
                    f"(kind: {kind}): {reason}."
                )
            if (
                kind in PRODUCT_KINDS
                and source in TIERS
                and target in TIERS
                and TIERS[target] > TIERS[source]
            ):
                errors.append(
                    f"ERROR: the policy DECLARES a layering inversion — {source} "
                    f"[{describe_tier(TIERS[source])}] -> {target} "
                    f"[{describe_tier(TIERS[target])}] (kind: {kind})."
                )
    return errors


def check_edge(source: str, target: str, kind: str) -> list[str]:
    """Check one observed edge against the structural rules, the edge table, and the tiers."""
    errors: list[str] = []
    reason = forbidden_reason(source, target, kind)
    if reason is not None:
        errors.append(f"ERROR: forbidden edge — {source} -> {target} (kind: {kind}): {reason}.")

    policy = ALLOWED_EDGES.get((source, target))
    if policy is None:
        errors.append(
            f"ERROR: undeclared dependency edge — {source} -> {target} (kind: {kind}). Every "
            f"internal edge must be declared: add it to ALLOWED_EDGES in "
            f"scripts/check_crate_dag.py with the kind and a reason, or drop the dependency."
        )
    elif kind not in policy[0]:
        errors.append(
            f"ERROR: dependency kind not permitted — {source} -> {target} (kind: {kind}); the "
            f"policy allows this edge only as: {describe_kinds(policy[0])}. Reason on file: "
            f"{policy[1]}."
        )

    if kind in PRODUCT_KINDS and TIERS[target] > TIERS[source]:
        errors.append(
            f"ERROR: layering inversion — {source} [{describe_tier(TIERS[source])}] depends on "
            f"{target} [{describe_tier(TIERS[target])}] (kind: {kind}). A crate may not depend "
            f"on a strictly higher tier over a product edge; same-tier and downward are allowed."
        )
    return errors


def collect_violations(metadata: dict) -> tuple[list[str], dict[str, int]]:
    """Return (error messages, edge counts by kind).

    Counts are reported on success so a silently-empty run (wrong metadata, wrong cwd) is
    visible.
    """
    errors: list[str] = []
    counts: dict[str, int] = dict.fromkeys(sorted(KINDS), 0)
    # With `--no-deps`, `packages` IS the workspace-member set: every member is internal and
    # policed, whatever it is named.
    packages = list(metadata.get("packages", []))
    present = {p["name"] for p in packages}

    for package in sorted(packages, key=lambda p: p["name"]):
        if package["name"] not in TIERS:
            errors.append(
                f"ERROR: {package['name']} is not in the tier map "
                f"(scripts/check_crate_dag.py TIERS) — classify it before it can be depended on."
            )
    if errors:
        return errors, counts

    observed: set[tuple[str, str]] = set()
    for package in sorted(packages, key=lambda p: p["name"]):
        source = package["name"]
        for dependency in package.get("dependencies", []):
            target = dependency["name"]
            if target not in present and not target.startswith(INTERNAL_PREFIX):
                continue
            if target not in TIERS:
                errors.append(
                    f"ERROR: {source} depends on {target}, which is not in the tier map "
                    f"(scripts/check_crate_dag.py TIERS)."
                )
                continue
            kind = edge_kind(dependency)
            counts[kind] += 1
            observed.add((source, target))
            errors.extend(check_edge(source, target, kind))

    # Drift: a declared edge that no longer exists. Only reported when BOTH endpoints are
    # present in the workspace, so rows for pre-declared crates stay legal.
    for source, target in sorted(ALLOWED_EDGES):
        if source in present and target in present and (source, target) not in observed:
            errors.append(
                f"ERROR: stale policy row — ALLOWED_EDGES declares {source} -> {target} but no "
                f"such dependency exists; remove the row (the table describes the workspace)."
            )
    return errors, counts


def main() -> int:
    """Read `cargo metadata --format-version 1 --no-deps` JSON from stdin and apply the rules."""
    try:
        metadata = json.load(sys.stdin)
    except json.JSONDecodeError as error:
        print(f"ERROR: could not parse cargo metadata JSON — {error}", file=sys.stderr)
        return 1

    errors = audit_policy()
    counts: dict[str, int] = dict.fromkeys(sorted(KINDS), 0)
    if not errors:
        errors, counts = collect_violations(metadata)
    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        print(
            "crate-dag: dependency policy violated (see scripts/check_crate_dag.py for the "
            "tier map, the roles, and the allowed-edge table).",
            file=sys.stderr,
        )
        return 1
    internal_count = len(metadata.get("packages", []))
    if internal_count == 0:
        print(
            "ERROR: crate-dag inspected zero internal crates — metadata looks wrong.",
            file=sys.stderr,
        )
        return 1
    edge_count = sum(counts.values())
    if edge_count == 0 and internal_count > 1:
        # With 2+ internal crates the workspace must have at least one internal edge; a
        # single-crate workspace legitimately has zero, so it is not an error.
        print(
            "ERROR: crate-dag inspected zero internal edges — metadata looks wrong.",
            file=sys.stderr,
        )
        return 1
    breakdown = ", ".join(f"{counts[kind]} {kind}" for kind in sorted(KINDS) if counts[kind])
    print(
        f"crate-dag: {edge_count} internal edges clean ({breakdown}) across "
        f"{internal_count} of {len(TIERS)} mapped crates"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
