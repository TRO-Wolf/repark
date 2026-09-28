#!/usr/bin/env python3
"""Pin the staged Python hook fallback when Cargo is unavailable."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parent.parent


class HookFallbackTests(unittest.TestCase):
    """Exercise wrapper failures against a temporary Git index."""

    def setUp(self) -> None:
        """Create one committed map and copy the hook entry points."""
        self.temporary = tempfile.TemporaryDirectory(prefix="repo-tool-fallback-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        scripts = self.root / "scripts"
        scripts.mkdir()
        for name in (
            "repo-tool.sh",
            "repo-tool-fallback.py",
            "repo-hook-cargo.sh",
            "sync_map_md.py",
        ):
            shutil.copy2(SOURCE_ROOT / "scripts" / name, scripts / name)
        shutil.copy2(SOURCE_ROOT / "Makefile", self.root / "Makefile")
        self.git("init", "-q")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "user.name", "Test")
        (self.root / "target.txt").write_text("target\n", encoding="utf-8")
        self.good_map = "# map\n\n- [target](target.txt)\n"
        self.bad_map = "# map\n\n- [bad](absent.txt)\n"
        (self.root / "map.md").write_text(self.good_map, encoding="utf-8")
        self.git("add", "map.md", "target.txt")
        self.git("commit", "-qm", "fixture")
        self.no_cargo = self.root / "no-cargo-bin"
        self.no_cargo.mkdir()
        for name in ("git", "python3", "dirname", "bash"):
            (self.no_cargo / name).symlink_to(shutil.which(name))
        fake_uvx = self.no_cargo / "uvx"
        fake_uvx.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        fake_uvx.chmod(0o755)

    def git(
        self, *arguments: str, environment: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        """Run Git in the fixture with a checked exit status."""
        return subprocess.run(
            ["git", "-C", str(self.root), *arguments],
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )

    def wrapper(
        self, environment: dict[str, str], *arguments: str
    ) -> subprocess.CompletedProcess[str]:
        """Call the wrapper with the selected Cargo environment."""
        return subprocess.run(
            ["/bin/bash", str(self.root / "scripts" / "repo-tool.sh"), *arguments],
            capture_output=True,
            text=True,
            cwd=self.root,
            env=environment,
            check=False,
        )

    def staged_arguments(self) -> tuple[str, ...]:
        """Return the hook's staged map check arguments."""
        return ("--snapshot", "index", "maps", "--check")

    def test_missing_cargo_checks_staged_bad_link_not_good_worktree(self) -> None:
        """A missing Rust build cannot pass a broken staged map."""
        (self.root / "map.md").write_text(self.bad_map, encoding="utf-8")
        self.git("add", "map.md")
        (self.root / "map.md").write_text(self.good_map, encoding="utf-8")
        environment = {**os.environ, "PATH": str(self.no_cargo)}
        result = self.wrapper(environment, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("absent.txt", result.stderr)
        self.assertIn("Rust build unavailable", result.stderr)

    def test_failed_build_uses_staged_good_map_not_bad_worktree(self) -> None:
        """A Cargo build error invokes the staged checker and keeps its result."""
        (self.root / "map.md").write_text(self.bad_map, encoding="utf-8")
        fake = self.root / "fake-bin"
        fake.mkdir()
        cargo = fake / "cargo"
        cargo.write_text("#!/bin/sh\nexit 42\n", encoding="utf-8")
        cargo.chmod(0o755)
        environment = {**os.environ, "PATH": f"{fake}:{os.environ['PATH']}"}
        result = self.wrapper(environment, *self.staged_arguments())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("staged maps clean", result.stdout)

    def test_rust_checker_finding_never_uses_fallback(self) -> None:
        """A built Rust checker owns its own failure result."""
        fake = self.root / "fake-bin"
        fake.mkdir()
        cargo = fake / "cargo"
        cargo.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        cargo.chmod(0o755)
        binary = self.root / "scripts" / "repo-tool" / "target" / "release" / "repark-repo"
        binary.parent.mkdir(parents=True)
        binary.write_text("#!/bin/sh\necho rust-defect >&2\nexit 1\n", encoding="utf-8")
        binary.chmod(0o755)
        environment = {**os.environ, "PATH": f"{fake}:{os.environ['PATH']}"}
        result = self.wrapper(environment, *self.staged_arguments())
        self.assertEqual(result.returncode, 1)
        self.assertIn("rust-defect", result.stderr)
        self.assertNotIn("Rust build unavailable", result.stderr)

    def test_custom_index_controls_fallback(self) -> None:
        """A Git temporary index wins over the ordinary index and worktree."""
        alternate = self.root / "alternate.index"
        shutil.copy2(self.root / ".git" / "index", alternate)
        (self.root / "map.md").write_text(self.bad_map, encoding="utf-8")
        alternate_environment = {**os.environ, "GIT_INDEX_FILE": str(alternate)}
        self.git("add", "map.md", environment=alternate_environment)
        (self.root / "map.md").write_text(self.good_map, encoding="utf-8")
        environment = {**alternate_environment, "PATH": str(self.no_cargo)}
        bad = self.wrapper(environment, *self.staged_arguments())
        self.assertEqual(bad.returncode, 1, bad.stderr)
        good = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(good.returncode, 0, good.stderr)

    def test_managed_inventory_stays_checked_without_cargo(self) -> None:
        """The fallback rejects a stale generated contents block."""
        (self.root / "code.py").write_text("value = 1\n", encoding="utf-8")
        (self.root / "map.md").write_text(
            "# map\n\n<!-- repo-tool:contents:start -->\n<!-- repo-tool:contents:end -->\n",
            encoding="utf-8",
        )
        self.git("add", "code.py", "map.md")
        environment = {**os.environ, "PATH": str(self.no_cargo)}
        result = self.wrapper(environment, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("managed contents differ", result.stderr)

    def test_fenced_managed_markers_cannot_satisfy_required_block(self) -> None:
        """Managed markers inside a code fence cannot satisfy the staged policy."""
        (self.root / "code.py").write_text("value = 1\n", encoding="utf-8")
        (self.root / "map.md").write_text(
            "# map\n\n```md\n<!-- repo-tool:contents:start -->\n"
            "- [code.py](code.py)\n<!-- repo-tool:contents:end -->\n```\n",
            encoding="utf-8",
        )
        self.git("add", "code.py", "map.md")
        result = self.wrapper(
            {**os.environ, "PATH": str(self.no_cargo)},
            *self.staged_arguments(),
            "--require-managed",
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("inside a code fence", result.stderr)

    def test_reference_definition_fails_closed_without_rust_parser(self) -> None:
        """The legacy inline scanner cannot approve a defined reference link."""
        (self.root / "map.md").write_text(
            "# map\n\n- [target][t]\n\n[t]: absent.txt\n", encoding="utf-8"
        )
        self.git("add", "map.md")
        result = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("reference links require", result.stderr)

    def test_mixed_fences_cannot_hide_reference_definition(self) -> None:
        """A fence delimiter of another type cannot conceal a dead reference."""
        (self.root / "map.md").write_text(
            "# map\n\n```md\n~~~\n```\n\n- [bad][ref]\n\n[ref]: missing.md\n",
            encoding="utf-8",
        )
        self.git("add", "map.md")
        result = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("reference links require", result.stderr)

    def test_multiline_reference_definition_fails_closed(self) -> None:
        """A destination on the next line still requires the Rust parser."""
        (self.root / "map.md").write_text(
            "# map\n\n- [bad][ref]\n\n[ref]:\n  missing.md\n", encoding="utf-8"
        )
        self.git("add", "map.md")
        result = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("reference links require", result.stderr)

    def test_container_and_escaped_reference_headers_fail_closed(self) -> None:
        """Quoted, listed, and escaped-label definitions need the Rust parser."""
        for header in (
            "> [ref]: missing.md",
            "- [ref]: missing.md",
            "[r\\]ef]:\n  missing.md",
            "[one\ntwo\nthree\nfour\nfive]: missing.md",
        ):
            with self.subTest(header=header):
                (self.root / "map.md").write_text(
                    f"# map\n\n- [bad][ref]\n\n{header}\n", encoding="utf-8"
                )
                self.git("add", "map.md")
                result = self.wrapper(
                    {**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments()
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("reference links require", result.stderr)

    def test_prose_severity_label_is_not_reference_definition(self) -> None:
        """A severity label in ordinary prose does not block a docs-only commit."""
        (self.root / "map.md").write_text(
            "# map\n\n- `pin`: Critic r3 [S2]: detail.\n", encoding="utf-8"
        )
        self.git("add", "map.md")
        result = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_inline_link_must_stay_inside_repository(self) -> None:
        """Raw, encoded, and angle-form parent links cannot escape the root."""
        for target in ("../", "%2e%2e/", "<../>"):
            with self.subTest(target=target):
                (self.root / "map.md").write_text(
                    f"# map\n\n- [parent]({target})\n", encoding="utf-8"
                )
                self.git("add", "map.md")
                result = self.wrapper(
                    {**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments()
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("escapes repository", result.stderr)

    def test_parser_only_link_forms_fail_closed(self) -> None:
        """Unmatched CommonMark link forms cannot bypass the legacy scanner."""
        for link in (
            "[x\\]](../)",
            "[x](\n../\n)",
            "[a [b] c](../)",
            "[good](target.txt) [x\\]](../)",
        ):
            with self.subTest(link=link):
                (self.root / "map.md").write_text(f"# map\n\n- {link}\n", encoding="utf-8")
                self.git("add", "map.md")
                result = self.wrapper(
                    {**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments()
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("link syntax requires", result.stderr)

    def test_escaped_local_destinations_need_rust_parser(self) -> None:
        """Entity and backslash paths cannot be approved by raw file names."""
        for target in ("&#46;&#46;/", r"\.\./"):
            with self.subTest(target=target):
                (self.root / "map.md").write_text(
                    f"# map\n\n- [parent]({target})\n", encoding="utf-8"
                )
                self.git("add", "map.md")
                result = self.wrapper(
                    {**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments()
                )
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("link escapes need", result.stderr)

    def test_staged_map_symlink_cannot_borrow_target_text(self) -> None:
        """A map symlink is invalid even when its target has valid map text."""
        (self.root / "other.md").write_text(self.good_map, encoding="utf-8")
        (self.root / "map.md").unlink()
        (self.root / "map.md").symlink_to("other.md")
        self.git("add", "map.md", "other.md")
        result = self.wrapper({**os.environ, "PATH": str(self.no_cargo)}, *self.staged_arguments())
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("not a regular file", result.stderr)

    def test_docs_only_skip_cargo_gates_but_staged_code_requires_them(self) -> None:
        """The hook skips Cargo only when the index has no relevant change."""
        (self.root / "map.md").write_text(self.good_map + "\nDocs edit.\n", encoding="utf-8")
        self.git("add", "map.md")
        environment = {**os.environ, "PATH": str(self.no_cargo)}
        helper = self.root / "scripts" / "repo-hook-cargo.sh"
        for mode in ("dag", "fmt"):
            clean = subprocess.run(
                ["/bin/bash", str(helper), mode],
                cwd=self.root,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(clean.returncode, 0, clean.stderr)
        (self.root / "code.rs").write_text("fn main() {}\n", encoding="utf-8")
        self.git("add", "code.rs")
        rust = subprocess.run(
            ["/bin/bash", str(helper), "fmt"],
            cwd=self.root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(rust.returncode, 2)
        self.assertIn("staged Rust", rust.stderr)
        (self.root / "Cargo.toml").write_text(
            "[package]\nname='x'\nversion='0.1.0'\n", encoding="utf-8"
        )
        self.git("add", "Cargo.toml")
        manifest = subprocess.run(
            ["/bin/bash", str(helper), "dag"],
            cwd=self.root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(manifest.returncode, 2)
        self.assertIn("staged Cargo.toml", manifest.stderr)

    def test_deleted_manifest_still_requires_cargo(self) -> None:
        """Removing a crate manifest still changes the dependency policy."""
        manifest = self.root / "Cargo.toml"
        manifest.write_text("[workspace]\n", encoding="utf-8")
        self.git("add", "Cargo.toml")
        self.git("commit", "-qm", "manifest")
        manifest.unlink()
        self.git("add", "-u", "Cargo.toml")
        result = subprocess.run(
            ["/bin/bash", str(self.root / "scripts" / "repo-hook-cargo.sh"), "dag"],
            cwd=self.root,
            env={**os.environ, "PATH": str(self.no_cargo)},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("staged Cargo.toml", result.stderr)

    def test_commit_a_temporary_index_rejects_bad_link(self) -> None:
        """The installed hook passes docs without Cargo but rejects bad commit -a maps."""
        for name in (
            "check_map_md.sh",
            "check_lib_rs.sh",
            "check_rust_file_size.sh",
            "check_lib_py.sh",
            "check_docstring_presence.sh",
            "check_manifest.sh",
        ):
            stub = self.root / "scripts" / name
            stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            stub.chmod(0o755)
        (self.root / "scripts" / "check_docs_compaction.py").write_text("pass\n", encoding="utf-8")
        subprocess.run(
            ["make", "install-hooks"], cwd=self.root, check=True, capture_output=True, text=True
        )
        environment = {**os.environ, "PATH": str(self.no_cargo)}
        (self.root / "map.md").write_text(self.good_map + "\nDocs edit.\n", encoding="utf-8")
        good = subprocess.run(
            [str(self.no_cargo / "git"), "-C", str(self.root), "commit", "-am", "docs"],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertEqual(good.returncode, 0, good.stderr)
        before = self.git("rev-parse", "HEAD").stdout.strip()
        (self.root / "map.md").write_text(self.bad_map, encoding="utf-8")
        result = subprocess.run(
            [str(self.no_cargo / "git"), "-C", str(self.root), "commit", "-am", "bad"],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("absent.txt", result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD").stdout.strip(), before)


if __name__ == "__main__":
    unittest.main()
