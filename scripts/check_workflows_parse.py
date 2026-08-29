#!/usr/bin/env python3
"""Fail if any GitHub Actions workflow is not parseable YAML.

zizmor — the blocking workflow lint — *skips* files it cannot parse, reporting "collection
yielded no auditable inputs" and exiting 0: a broken workflow passes the security gate while
GitHub silently never runs it. This check closes that hole.
"""

from __future__ import annotations

import pathlib
import sys

import yaml

WORKFLOW_DIR = pathlib.Path(".github/workflows")


def main() -> int:
    """Parse every workflow; report each failure with its YAML error."""
    failures: list[tuple[pathlib.Path, str]] = []
    workflows = sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml"))
    if not workflows:
        print(f"ERROR: no workflows found under {WORKFLOW_DIR}", file=sys.stderr)
        return 1
    for path in workflows:
        try:
            if yaml.safe_load(path.read_text(encoding="utf-8")) is None:
                failures.append((path, "parsed as empty"))
        except yaml.YAMLError as error:
            failures.append((path, str(error).splitlines()[0]))
    for path, reason in failures:
        print(f"ERROR: {path} is not parseable YAML — {reason}", file=sys.stderr)
    if failures:
        print(
            f"{len(failures)} unparsable workflow(s); GitHub would never run them.",
            file=sys.stderr,
        )
        return 1
    print(f"workflows-parse: {len(workflows)} workflows parse cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
