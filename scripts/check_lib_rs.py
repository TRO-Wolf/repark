#!/usr/bin/env python3
"""Enforce crate-root thinness: no inline #[cfg(test)] test modules; non-test line ceilings.

SSOT for lib.rs hygiene (r26 LR2). Prose (AGENTS.md / CLAUDE.md / crates/map.md) points
here and never restates the ceilings. Mirrors BH4 dual-wire shape (py = logic + SSOT,
sh = wrapper).

Rules over every crates/*/src/lib.rs:
1. No inline test block: an inline `#[cfg(test)] mod NAME {` fails — both the
   multi-line form and the same-line form. File-backed `#[cfg(test)] mod NAME;`
   is the only sanctioned form (any test module name).
2. Non-test line ceiling: default 150 lines on the whole root file (docs count —
   what a reader scrolls past). EXCEPTIONS table overrides with reason + ratchet note.

Exit 0 on clean; non-zero with named crate, measured count, ceiling, and sanctioned outs.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DEFAULT_CEILING = 150

# crate directory name -> (ceiling, reason). Ceilings only go DOWN as follow-ups land;
# never up without a stated reason in the commit that raises them.
EXCEPTIONS: dict[str, tuple[int, str]] = {
    # Measured line counts at the ported tip are noted in each reason; ceilings include slack.
    # Keys sorted alphabetically. Ceilings ratchet DOWN only. Empty at phase-1 PR-A (only
    # repark-common exists, well under the default); entries are added with a measured count
    # and reason in the same change that makes a crate root exceed the default.
    "repark-functions": (
        176,  # measured 176 after Q10 `pub mod timestamp_type;`
        "register_all / analyzer_rules registration glue is root-legitimate; "
        "U5 added the ANSI ConfigExtension module decl; Q10 added the "
        "spark.sql.timestampType carrier module decl; "
        "RATCHET: if registration moves",
    ),
    "repark-python": (
        190,  # measured 180 (EC-10; ratcheted 230 -> 190 when the taxonomy moved out)
        "the PyO3 crate root is a MANIFEST, not logic: doc lines, six `mod` decls, the "
        "`pub use` re-exports (incl. the exception taxonomy re-export), the two `to_py_err` "
        "folds, the env-gated tracing init, and the `#[pymodule]` registration. The "
        "`create_exception!` taxonomy lives file-backed in src/exceptions.rs (the recorded "
        "RATCHET fired at PR-3: the module-scoped disallowed_methods expectation needed a "
        "module, so it became a file and the ceiling ratcheted 230 -> 190). Already uses the "
        "sanctioned file-backed test module (`#[cfg(test)] mod tests;` -> src/tests.rs)",
    ),
    "repark-ta": (
        260,  # measured 249 (49 doc lines + the `TaError` enum + the kernel re-export block)
        "verbatim port of the v1 kernel crate root: the crate-wide `TaError` enum (the kernel "
        "contract every module returns) plus the flat `pub use` re-export surface — splitting "
        "either would break the port's identity diff against the pin; "
        "RATCHET: if `TaError` moves to its own module",
    ),
}

# Matches both:
#   #[cfg(test)]
#   mod tests { … }
# and the single-line form:
#   #[cfg(test)] mod tests { … }
INLINE_MOD_RE = re.compile(
    r"(?m)^#\[cfg\(test\)\](?:\s*\n(?:[ \t]*\n)*|\s+)mod[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]*\{"
)


def check_lib(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    crate = path.parent.parent.name  # crates/<crate>/src/lib.rs
    # wc-style: number of lines as splitlines length (docs count toward ceiling).
    line_count = len(text.splitlines())

    for match in INLINE_MOD_RE.finditer(text):
        name = match.group(1)
        line_no = text[: match.start()].count("\n") + 1
        errors.append(
            f"ERROR: {crate} src/lib.rs:{line_no}: inline #[cfg(test)] mod {name} {{ … }} "
            f"is forbidden — move the body to src/{name}.rs and leave "
            f"`#[cfg(test)] mod {name};` (file-backed only)."
        )

    ceiling, reason = EXCEPTIONS.get(crate, (DEFAULT_CEILING, "default ceiling"))
    if line_count > ceiling:
        errors.append(
            f"ERROR: {crate} src/lib.rs is {line_count} lines (ceiling {ceiling}). "
            f"Reason on file: {reason}. "
            f"Sanctioned outs: (1) move production code into a named module with pub use "
            f"re-exports, or (2) edit EXCEPTIONS in scripts/check_lib_rs.py with a reason "
            f"(ceilings ratchet down only)."
        )
    return errors


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    crates_root = repo / "crates"
    if not crates_root.is_dir():
        print("ERROR: crates/ not found", file=sys.stderr)
        return 2

    # Stale EXCEPTIONS rows: a crate-name key whose crates/<key>/src/lib.rs no
    # longer exists is fail-closed (exception for a deleted/renamed crate would
    # silently grandfather nothing). Mirrors check_rust_file_size's path-key
    # stale check; keys here are crate directory names, not repo-relative paths.
    all_errors: list[str] = []
    for crate in sorted(EXCEPTIONS):
        lib = crates_root / crate / "src" / "lib.rs"
        if not lib.is_file():
            all_errors.append(
                f"ERROR: EXCEPTIONS key has no crate root on disk: {crate} "
                f"(remove the row or restore crates/{crate}/src/lib.rs)"
            )

    checked = 0
    for lib in sorted(crates_root.glob("*/src/lib.rs")):
        checked += 1
        all_errors.extend(check_lib(lib))

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        print(
            f"lib-rs: FAIL — {len(all_errors)} violation(s) across {checked} crate roots",
            file=sys.stderr,
        )
        return 1

    print(f"lib-rs: {checked} crate roots clean (no inline test modules; ceilings held)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
