"""Fetch Apache Spark sources (tests only) into a gitignored user cache.

Charter source option (A): runtime clone of the Spark git tag matching the
installed ``pyspark`` version. Nothing from the Apache tree is committed.

Cache layout::

    ~/.cache/repark-pyspark-tests/<tag>/
        python/pyspark/sql/tests/...
        python/pyspark/testing/...   # present for reference; installed wheel used at runtime
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

LOGGER = logging.getLogger(__name__)

DEFAULT_CACHE_ROOT = Path(
    os.environ.get("REPARK_PYSPARK_TESTS_CACHE", Path.home() / ".cache" / "repark-pyspark-tests")
)
SPARK_GIT_URL = "https://github.com/apache/spark.git"
# Sparse paths required for night-1 modules + import scaffolding.
SPARSE_PATHS: tuple[str, ...] = (
    "python/pyspark/sql/tests",
    "python/pyspark/testing",
)
# Wall for network git clone + sparse-checkout (parent census does not wall this step).
CLONE_TIMEOUT_S = 600
# Spark release tags only: v4.1.2 / 4.1.2 — rejects path traversal and junk.
_TAG_RE = re.compile(r"^v?\d+(?:\.\d+)*$")


@dataclass(frozen=True)
class SparkTestsProvenance:
    """Where the Apache test sources came from (report provenance)."""

    tag: str
    commit_sha: str
    cache_dir: Path
    pyspark_version: str


def tag_for_pyspark_version(pyspark_version: str) -> str:
    """Map an installed pyspark version string to a Spark git tag.

    ``4.1.2`` → ``v4.1.2``. Pre/dev/local suffixes (``+dev``, ``.dev0``, ``rc1``,
    ``a1``, ``b2``) are stripped; the numeric core is validated.
    """
    cleaned = pyspark_version.strip()
    if cleaned.startswith("v") or cleaned.startswith("V"):
        cleaned = cleaned[1:]
    # Take the leading numeric dotted core only (stops at .dev / rc / a / b / + / _).
    match = re.match(r"^(\d+(?:\.\d+)*)", cleaned)
    if match is None:
        raise ValueError(
            f"cannot map pyspark version {pyspark_version!r} to a Spark tag "
            f"(expected a leading dotted version like '4.1.2')"
        )
    core = match.group(1).rstrip(".")
    # Normalize zero-padded segments (04.1.2 → 4.1.2) without accepting empty segments.
    segments = [str(int(part)) for part in core.split(".") if part != ""]
    if not segments:
        raise ValueError(f"cannot map pyspark version {pyspark_version!r} to a Spark tag")
    core = ".".join(segments)
    tag = f"v{core}"
    if not _TAG_RE.match(tag):
        raise ValueError(f"refusing non-release Spark tag {tag!r} from version {pyspark_version!r}")
    return tag


def ensure_spark_tests(
    *,
    pyspark_version: str | None = None,
    cache_root: Path | None = None,
    git_url: str = SPARK_GIT_URL,
    force: bool = False,
) -> SparkTestsProvenance:
    """Ensure Apache sql tests for ``pyspark_version`` exist under the cache root.

    Uses a shallow sparse clone of ``python/pyspark/sql/tests`` (+ testing helpers)
    at the matching tag. Reuses an existing cache when the tests tree is present
    unless ``force=True``.
    """
    if pyspark_version is None:
        import pyspark

        pyspark_version = pyspark.__version__
    tag = tag_for_pyspark_version(pyspark_version)
    root = Path(cache_root) if cache_root is not None else DEFAULT_CACHE_ROOT
    root_resolved = root.expanduser().resolve()
    cache_dir = (root_resolved / tag).resolve()
    if not _is_relative_to(cache_dir, root_resolved):
        raise ValueError(
            f"refusing cache_dir {cache_dir} outside cache root {root_resolved} (tag={tag!r})"
        )
    tests_marker = cache_dir / "python" / "pyspark" / "sql" / "tests" / "test_functions.py"

    if force and cache_dir.exists():
        LOGGER.info("force-refresh: removing %s", cache_dir)
        shutil.rmtree(cache_dir)

    if tests_marker.is_file():
        commit_sha = _read_git_sha(cache_dir)
        LOGGER.info("using cached Spark tests tag=%s sha=%s at %s", tag, commit_sha, cache_dir)
        return SparkTestsProvenance(
            tag=tag,
            commit_sha=commit_sha,
            cache_dir=cache_dir,
            pyspark_version=pyspark_version,
        )

    root_resolved.mkdir(parents=True, exist_ok=True)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)

    LOGGER.info("cloning Spark %s (sparse) into %s", tag, cache_dir)
    _sparse_clone(git_url=git_url, tag=tag, destination=cache_dir)
    if not tests_marker.is_file():
        raise RuntimeError(
            f"Spark sparse clone at {cache_dir} is missing {tests_marker.name}; "
            f"sparse paths={SPARSE_PATHS}"
        )
    commit_sha = _read_git_sha(cache_dir)
    return SparkTestsProvenance(
        tag=tag,
        commit_sha=commit_sha,
        cache_dir=cache_dir,
        pyspark_version=pyspark_version,
    )


def _is_relative_to(path: Path, root: Path) -> bool:
    """Path.is_relative_to backport-friendly check."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _sparse_clone(*, git_url: str, tag: str, destination: Path) -> None:
    """Shallow sparse clone of the required test paths at ``tag``."""
    if not _TAG_RE.match(tag):
        raise ValueError(f"refusing to clone non-release Spark tag {tag!r}")
    if not (git_url.startswith("https://") or git_url.startswith("git@")):
        raise ValueError(f"refusing non-git URL for Spark clone: {git_url!r}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    clone_cmd = [
        "git",
        "clone",
        "--depth",
        "1",
        "--filter=blob:none",
        "--sparse",
        "--branch",
        tag,
        git_url,
        str(destination),
    ]
    try:
        subprocess.run(
            clone_cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=CLONE_TIMEOUT_S,
        )
        sparse_cmd = ["git", "-C", str(destination), "sparse-checkout", "set", *SPARSE_PATHS]
        subprocess.run(
            sparse_cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=CLONE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as error:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        raise RuntimeError(
            f"Spark sparse clone timed out after {CLONE_TIMEOUT_S}s (tag={tag})"
        ) from error
    except subprocess.CalledProcessError as error:
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        stderr_tail = (error.stderr or "")[-500:]
        raise RuntimeError(
            f"Spark sparse clone failed for tag={tag}: {stderr_tail or error}"
        ) from error


def _read_git_sha(repo: Path) -> str:
    """Return HEAD commit SHA for a cache clone (or ``unknown`` if not a git dir)."""
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return completed.stdout.strip()
