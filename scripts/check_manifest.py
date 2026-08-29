#!/usr/bin/env python3
"""Validate `repo-manifest.toml` against the workspace, the docs, the gates and the crate maps.

`repo-manifest.toml` is the machine-readable structural claim this repository makes about
itself. This guard is what makes the claim true: every field is cross-checked against a real
artifact, so structural documentation drift is a CI failure rather than a slow rot.

The rules:

1. **Inventory, both directions.** Every Cargo workspace member is declared as a component, and
   every `delivered` component is a Cargo workspace member.
2. **Delivered means it exists.** A `delivered` component has a directory at its declared path
   with a `Cargo.toml` in it.
3. **planned != delivered.** A `planned` component's path must NOT exist. Code that has arrived
   is delivered, and saying otherwise (in either direction) is the drift this catches.
4. **Layers are recognized, and they are not a second source of truth.** A delivered
   component's `layer` must equal the tier name that `scripts/check_crate_dag.py` — the
   dependency-policy SSOT — assigns that crate. `planned` components carry no layer.
5. **Dependency-policy coverage.** Every delivered component appears in that script's `TIERS`
   and `ROLES` maps, so no crate is outside the layering + allowed-edge policy.
6. **The gates exist.** Each `[project.gates]` command is `make <target>` for a target the
   Makefile actually defines.
7. **The declared documents exist.** Every `[documentation]` path resolves to a file.
8. **STATUS.md agrees.** STATUS.md is the status SSOT; the manifest's phase / release words
   must be found in it, so a milestone that moves in one file and not the other is red.
9. **Manifest <-> map consistency.** Every delivered component has a hand-written `map.md` at
   its declared path (its LOCATION is the path agreement), whose heading names the component
   and whose body names the same tier (the number the dependency-policy SSOT assigns its
   layer). This is the only
   `map.md` automation in the repository: it CHECKS a hand-written file and never generates,
   rewrites, or scaffolds one.

Exit 0 on clean, naming what was checked; non-zero with the offending field, path or command.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import tomllib
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = REPO_ROOT / "repo-manifest.toml"
DAG_SCRIPT = REPO_ROOT / "scripts" / "check_crate_dag.py"
CARGO_MANIFEST = REPO_ROOT / "Cargo.toml"
MAKEFILE = REPO_ROOT / "Makefile"

# Schema. `[project]` and `[components.*]` are STRICT — every key below is required and an
# unknown one is a typo that would otherwise go unread. `[project.gates]` and `[documentation]`
# require the keys below and accept extra rows, each validated the same way as the required ones.
PROJECT_KEYS = frozenset({"name", "phase", "phase_state", "release_status", "gates"})
REQUIRED_GATE_KEYS = frozenset({"canonical", "completion", "pre_pr"})
REQUIRED_DOCUMENTATION_KEYS = frozenset(
    {"contract", "architecture", "development", "status", "testing"}
)
COMPONENT_KEYS = frozenset({"path", "layer", "status"})
DELIVERED = "delivered"
PLANNED = "planned"
STATUSES = frozenset({DELIVERED, PLANNED})

# STATUS.md sections the phase / release words are matched against. Named here so a red gate
# after a STATUS.md section rename says which heading it could not find.
MILESTONE_SECTION = "Current milestone"
RELEASE_SECTION = "Release state"

MAKE_TARGET_RE = re.compile(r"^([A-Za-z0-9_.-]+)[ \t]*:(?!=)")


def read_toml(path: Path) -> tuple[dict, list[str]]:
    """Parse a TOML file, returning ({}, [error]) rather than raising on a bad or missing file."""
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle), []
    except FileNotFoundError:
        return {}, [f"ERROR: {display(path)} not found."]
    except OSError as error:
        return {}, [f"ERROR: {display(path)} could not be read — {error}."]
    except tomllib.TOMLDecodeError as error:
        return {}, [f"ERROR: {display(path)} is not parseable TOML — {error}."]


def read_text(path: Path) -> tuple[str, list[str]]:
    """Read a UTF-8 text file, returning ("", [error]) rather than raising."""
    try:
        return path.read_text(encoding="utf-8"), []
    except FileNotFoundError:
        return "", [f"ERROR: {display(path)} not found."]
    except OSError as error:
        return "", [f"ERROR: {display(path)} could not be read — {error}."]


def display(path: Path) -> str:
    """Render a path relative to the repository root for error messages."""
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def load_dependency_policy() -> tuple[ModuleType | None, list[str]]:
    """Import `scripts/check_crate_dag.py` — the SSOT for tiers, roles and allowed edges.

    Imported by path (scripts/ is not a package) so the manifest is checked against the
    enforcing guard, never a copy of the map kept here.
    """
    specification = importlib.util.spec_from_file_location("check_crate_dag", DAG_SCRIPT)
    if specification is None or specification.loader is None:
        return None, [f"ERROR: could not load the dependency policy from {display(DAG_SCRIPT)}."]
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module, []


def workspace_members() -> tuple[dict[str, str], list[str]]:
    """Return {crate name: member path} for every Cargo workspace member."""
    root, errors = read_toml(CARGO_MANIFEST)
    if errors:
        return {}, errors
    members = root.get("workspace", {}).get("members", [])
    if not members:
        return {}, [f"ERROR: {display(CARGO_MANIFEST)} declares no workspace members."]

    resolved: dict[str, str] = {}
    for member in members:
        paths = (
            # Only directories: a glob member like `crates/*` also matches `crates/map.md`.
            [path for path in sorted(REPO_ROOT.glob(member)) if path.is_dir()]
            if any(character in member for character in "*?[")
            else [REPO_ROOT / member]
        )
        for path in paths:
            package, read_errors = read_toml(path / "Cargo.toml")
            if read_errors:
                errors.extend(read_errors)
                continue
            name = package.get("package", {}).get("name")
            if not isinstance(name, str):
                errors.append(f"ERROR: {display(path)}/Cargo.toml declares no [package] name.")
                continue
            resolved[name] = display(path)
    return resolved, errors


def make_targets() -> tuple[set[str], list[str]]:
    """Return every target the Makefile defines."""
    text, errors = read_text(MAKEFILE)
    if errors:
        return set(), errors
    targets = {
        match.group(1)
        for match in (MAKE_TARGET_RE.match(line) for line in text.splitlines())
        if match is not None
    }
    targets.discard(".PHONY")
    targets.discard(".DEFAULT_GOAL")
    return targets, []


def markdown_section(text: str, title: str) -> str | None:
    """Return the body of a `## <title>` section, or None if the heading is absent."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.strip().lower() == f"## {title}".lower():
            body: list[str] = []
            for following in lines[index + 1 :]:
                if following.startswith("## "):
                    break
                body.append(following)
            return "\n".join(body)
    return None


def normalize(text: str) -> str:
    """Lower-case and strip markdown emphasis so a prose claim can be matched literally."""
    return re.sub(r"\s+", " ", re.sub(r"[*_`]", "", text)).strip().lower()


def require_string(table: dict, key: str, where: str, errors: list[str]) -> str | None:
    """Fetch a required string field, appending an error when it is missing or typed wrong."""
    value = table.get(key)
    if value is None:
        errors.append(f"ERROR: {where} is missing the required `{key}` field.")
        return None
    if not isinstance(value, str) or not value.strip():
        errors.append(f"ERROR: {where} `{key}` must be a non-empty string (found {value!r}).")
        return None
    return value


def check_project(manifest: dict, targets: set[str], status_doc: Path) -> list[str]:
    """Validate `[project]`: the schema, the gate commands, and agreement with the status doc."""
    errors: list[str] = []
    project = manifest.get("project")
    if not isinstance(project, dict):
        return ["ERROR: repo-manifest.toml has no [project] table."]

    for key in sorted(set(project) - PROJECT_KEYS):
        errors.append(f"ERROR: [project] carries unknown key `{key}` — remove it or fix the typo.")
    for key in sorted(PROJECT_KEYS - set(project)):
        errors.append(f"ERROR: [project] is missing the required `{key}` field.")
    # Type-check: a present-but-not-a-string field would silently skip the STATUS.md
    # agreement rule while the success line still claims agreement.
    for key in ("name", "phase", "phase_state", "release_status"):
        if key in project:
            require_string(project, key, "[project]", errors)

    gates = project.get("gates")
    if not isinstance(gates, dict):
        errors.append("ERROR: repo-manifest.toml has no [project.gates] table.")
    else:
        for key in sorted(REQUIRED_GATE_KEYS - set(gates)):
            errors.append(f"ERROR: [project.gates] is missing the required `{key}` gate.")
        for key in sorted(gates):
            command = require_string(gates, key, f"[project.gates] `{key}`", errors)
            if command is None:
                continue
            words = command.split()
            if len(words) != 2 or words[0] != "make":
                errors.append(
                    f"ERROR: [project.gates] `{key}` = {command!r} is not a `make <target>` "
                    f"command; the gates are Makefile targets."
                )
            elif words[1] not in targets:
                errors.append(
                    f"ERROR: [project.gates] `{key}` names `{words[1]}`, which the Makefile does "
                    f"not define — the command is dead."
                )

    errors.extend(check_status_agreement(project, status_doc))
    return errors


def check_status_agreement(project: dict, status_doc: Path) -> list[str]:
    """Cross-check the phase / release words against the declared status document, the SSOT."""
    errors: list[str] = []
    phase = project.get("phase")
    phase_state = project.get("phase_state")
    release_status = project.get("release_status")
    text, read_errors = read_text(status_doc)
    if read_errors:
        return read_errors

    document = display(status_doc)
    if isinstance(phase, str) and isinstance(phase_state, str):
        section = markdown_section(text, MILESTONE_SECTION)
        claim = f"{phase} is {phase_state}"
        if section is None:
            errors.append(
                f"ERROR: {document} has no `## {MILESTONE_SECTION}` section — the manifest's "
                f"phase cannot be checked against the status SSOT."
            )
        elif normalize(claim) not in normalize(section):
            errors.append(
                f"ERROR: [project] claims phase {phase!r} / phase_state {phase_state!r}, but "
                f"{document}'s `## {MILESTONE_SECTION}` section does not state "
                f"{claim!r} — one of the two is stale ({document} is the status SSOT)."
            )

    if isinstance(release_status, str):
        section = markdown_section(text, RELEASE_SECTION)
        if section is None:
            errors.append(f"ERROR: {document} has no `## {RELEASE_SECTION}` section.")
        elif normalize(release_status) not in normalize(section):
            errors.append(
                f"ERROR: [project] claims release_status {release_status!r}, which {document}'s "
                f"`## {RELEASE_SECTION}` section does not state."
            )
    return errors


def check_documentation(manifest: dict) -> list[str]:
    """Validate `[documentation]`: the required index keys are present and every path exists."""
    errors: list[str] = []
    documentation = manifest.get("documentation")
    if not isinstance(documentation, dict):
        return ["ERROR: repo-manifest.toml has no [documentation] table."]

    for key in sorted(REQUIRED_DOCUMENTATION_KEYS - set(documentation)):
        errors.append(f"ERROR: [documentation] is missing the required `{key}` entry.")
    for key in sorted(documentation):
        path = require_string(documentation, key, f"[documentation] `{key}`", errors)
        if path is None:
            continue
        if not (REPO_ROOT / path).is_file():
            errors.append(
                f"ERROR: [documentation] `{key}` points at {path}, which does not exist — "
                f"the document moved or was archived without updating the index."
            )
    return errors


def check_components(manifest: dict, members: dict[str, str], policy: ModuleType) -> list[str]:
    """Validate `[components.*]`: the schema, the inventory both ways, the paths, and the layers."""
    errors: list[str] = []
    components = manifest.get("components")
    if not isinstance(components, dict) or not components:
        return ["ERROR: repo-manifest.toml declares no [components.*] tables."]

    for name in sorted(components):
        component = components[name]
        if not isinstance(component, dict):
            errors.append(f"ERROR: [components.{name}] is not a table.")
            continue
        for key in sorted(set(component) - COMPONENT_KEYS):
            errors.append(
                f"ERROR: [components.{name}] carries unknown key `{key}` — "
                f"the fields are: {', '.join(sorted(COMPONENT_KEYS))}."
            )
        path = require_string(component, "path", f"[components.{name}]", errors)
        status = require_string(component, "status", f"[components.{name}]", errors)
        if path is None or status is None:
            continue
        if status not in STATUSES:
            errors.append(
                f"ERROR: [components.{name}] status {status!r} is not recognized "
                f"(one of: {', '.join(sorted(STATUSES))})."
            )
            continue
        errors.extend(check_component_status(name, path, status, component, members, policy))

    for crate, member_path in sorted(members.items()):
        if crate not in components:
            errors.append(
                f"ERROR: Cargo workspace member {crate} ({member_path}) is not declared in "
                f"repo-manifest.toml — add a [components.{crate}] entry (path, layer, status)."
            )
    return errors


def check_component_status(
    name: str,
    path: str,
    status: str,
    component: dict,
    members: dict[str, str],
    policy: ModuleType,
) -> list[str]:
    """Validate one component's status-dependent obligations (existence, membership, layer)."""
    errors: list[str] = []
    directory = REPO_ROOT / path

    if status == PLANNED:
        if "layer" in component:
            errors.append(
                f"ERROR: [components.{name}] is planned but declares a layer — a layer claim is "
                f"a delivered-crate fact assigned by scripts/check_crate_dag.py when it lands."
            )
        if directory.exists():
            errors.append(
                f"ERROR: [components.{name}] is declared planned, but {path} exists — code that "
                f"has arrived is delivered; flip the status (and declare its layer)."
            )
        return errors

    if not (directory / "Cargo.toml").is_file():
        errors.append(
            f"ERROR: [components.{name}] is declared delivered, but {path}/Cargo.toml does not "
            f"exist — a delivered component is one that is actually there."
        )
    if name not in members:
        errors.append(
            f"ERROR: [components.{name}] is declared delivered, but it is not a Cargo workspace "
            f"member — add it to [workspace] members in Cargo.toml or mark it planned."
        )
    elif members[name] != path:
        errors.append(
            f"ERROR: [components.{name}] declares path {path}, but the Cargo workspace member "
            f"lives at {members[name]}."
        )

    layer = require_string(component, "layer", f"[components.{name}]", errors)
    if layer is None:
        return errors
    if layer not in set(policy.TIER_NAMES.values()):
        errors.append(
            f"ERROR: [components.{name}] layer {layer!r} is not recognized — the layers are "
            f"named by scripts/check_crate_dag.py TIER_NAMES: "
            f"{', '.join(sorted(set(policy.TIER_NAMES.values())))}."
        )
    if name not in policy.TIERS or name not in policy.ROLES:
        errors.append(
            f"ERROR: [components.{name}] is delivered but is not covered by the dependency "
            f"policy — classify it in scripts/check_crate_dag.py (TIERS and ROLES) before it "
            f"can be depended on."
        )
        return errors
    expected = policy.TIER_NAMES[policy.TIERS[name]]
    if layer != expected:
        errors.append(
            f"ERROR: [components.{name}] layer {layer!r} disagrees with the dependency-policy "
            f"SSOT, which puts it at {policy.describe_tier(policy.TIERS[name])} — the manifest "
            f"mirrors scripts/check_crate_dag.py, it does not override it."
        )
    return errors


def check_component_maps(manifest: dict, policy: ModuleType) -> list[str]:
    """Check each delivered component's hand-written crate-root `map.md` agrees with the manifest.

    Existence AT the declared path is the path agreement; the heading must name the component
    and the body must name its crate-DAG tier number. Nothing is generated or rewritten.
    """
    errors: list[str] = []
    components = manifest.get("components")
    if not isinstance(components, dict):
        return errors

    for name in sorted(components):
        component = components[name]
        if not isinstance(component, dict) or component.get("status") != DELIVERED:
            continue
        path = component.get("path")
        if not isinstance(path, str):
            continue
        map_path = REPO_ROOT / path / "map.md"
        if not map_path.is_file():
            errors.append(
                f"ERROR: [components.{name}] has no map.md at {path}/map.md — every directory "
                f"carries one, hand-written (write it; this guard never generates one)."
            )
            continue
        text, read_errors = read_text(map_path)
        if read_errors:
            errors.extend(read_errors)
            continue
        heading = next((line for line in text.splitlines() if line.startswith("# ")), "")
        if name not in heading:
            errors.append(
                f"ERROR: {path}/map.md heading {heading.strip()!r} does not name {name} — "
                f"the map must identify the component the manifest declares at this path."
            )
        tier = policy.TIERS.get(name)
        if tier is not None and not re.search(rf"tier[-\s]*{tier}\b", text, re.IGNORECASE):
            errors.append(
                f"ERROR: {path}/map.md never names its tier — [components.{name}] declares "
                f"{component.get('layer')!r}, which is {policy.describe_tier(tier)}; say "
                f"`tier {tier}` in the map so the manifest and the map cannot drift."
            )
    return errors


def main() -> int:
    """Load the manifest and apply every rule; report the counts checked on success."""
    manifest, errors = read_toml(MANIFEST_PATH)
    policy: ModuleType | None = None
    if not errors:
        policy, errors = load_dependency_policy()
    if not errors and policy is not None:
        members, member_errors = workspace_members()
        targets, target_errors = make_targets()
        # The status doc is whatever the manifest's own doc index declares, so a rename moves
        # the phase check with it rather than silently unhooking it.
        declared_status = manifest.get("documentation", {}).get("status")
        if not isinstance(declared_status, str):
            declared_status = "STATUS.md"
        status_doc = REPO_ROOT / declared_status
        errors = (
            member_errors
            + target_errors
            + check_project(manifest, targets, status_doc)
            + check_documentation(manifest)
            + check_components(manifest, members, policy)
            + check_component_maps(manifest, policy)
        )

    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        print(
            f"manifest: FAIL — {len(errors)} violation(s) "
            f"(the structural SSOT is repo-manifest.toml; the rules are "
            f"scripts/check_manifest.py).",
            file=sys.stderr,
        )
        return 1

    components = manifest.get("components", {})
    delivered = sum(1 for entry in components.values() if entry.get("status") == DELIVERED)
    planned = sum(1 for entry in components.values() if entry.get("status") == PLANNED)
    if delivered == 0:
        print(
            "ERROR: manifest declares zero delivered components — it looks wrong.",
            file=sys.stderr,
        )
        return 1
    print(
        f"manifest: {len(components)} components ({delivered} delivered, {planned} planned) "
        f"agree with the workspace, the gates, the doc index, the status document and the "
        f"crate maps"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
