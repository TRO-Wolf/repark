"""SparkSession surface: tags, interrupts, Connect refusals, artifacts, profile, tvf.

pins: session-surface-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

import filecmp
import re
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkNotImplementedError,
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
)
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame
    from repark.spark.session.session_core import ReparkSession

_EXECUTION_ID_RE = re.compile(r"[+-]?\d+")

_MEMORY_PROFILER_WARNING = (
    "Install the 'memory_profiler' library in the cluster to enable memory profiling"
)


def _job_tags(session: ReparkSession) -> set[str]:
    tags = session._alive_token.get("job_tags")
    if tags is None:
        tags = set()
        session._alive_token["job_tags"] = tags
    return tags


def _require_str_tag(tag: object) -> None:
    if not isinstance(tag, str):
        raise PySparkTypeError(
            f"[NOT_STR] Argument `tag` should be a str, got {type(tag).__name__}.",
            errorClass="NOT_STR",
            messageParameters={"arg_name": "tag", "arg_type": type(tag).__name__},
        )


def _connect_only(feature: str) -> NoReturn:
    raise PySparkRuntimeError(
        f"[ONLY_SUPPORTED_WITH_SPARK_CONNECT] {feature} is only supported with Spark Connect; "
        "however, the current Spark session does not use Spark Connect.",
        errorClass="ONLY_SUPPORTED_WITH_SPARK_CONNECT",
        messageParameters={"feature": feature},
    )


def _not_implemented(feature: str) -> NoReturn:
    raise PySparkNotImplementedError(
        f"[NOT_IMPLEMENTED] {feature} is not implemented.",
        errorClass="NOT_IMPLEMENTED",
        messageParameters={"feature": feature},
    )


def _require_column_arg(arg: object, arg_name: str) -> None:
    if not isinstance(arg, (Column, str)):
        raise PySparkTypeError(
            f"[NOT_COLUMN] Argument `{arg_name}` should be a Column, got {type(arg).__name__}.",
            errorClass="NOT_COLUMN",
            messageParameters={"arg_name": arg_name, "arg_type": type(arg).__name__},
        )


def _refuse_profile_type() -> NoReturn:
    raise PySparkValueError(
        "[VALUE_NOT_ALLOWED] Value for `type` has to be amongst the following values: "
        "['perf', 'memory'].",
        errorClass="VALUE_NOT_ALLOWED",
        messageParameters={"arg_name": "type", "allowed_values": str(["perf", "memory"])},
    )


def add_tag(session: ReparkSession, tag: str) -> None:
    """PySpark ``addTag`` — validate then record a job tag. pins: session-surface-1/C-001"""
    session._ensure_alive()
    _require_str_tag(tag)
    if tag == "":
        raise IllegalArgumentException("Spark job tag cannot be an empty string.")
    if "," in tag:
        raise IllegalArgumentException("Spark job tag cannot contain ','.")
    _job_tags(session).add(tag)


def remove_tag(session: ReparkSession, tag: str) -> None:
    """PySpark ``removeTag`` — drop one tag; absent and empty are no-ops.

    pins: session-surface-1/C-001
    """
    session._ensure_alive()
    _require_str_tag(tag)
    if not tag:
        return
    _job_tags(session).discard(tag)


def get_tags(session: ReparkSession) -> set[str]:
    """PySpark ``getTags`` — a copy so callers cannot mutate session state.

    pins: session-surface-1/C-001
    """
    session._ensure_alive()
    return set(_job_tags(session))


def clear_tags(session: ReparkSession) -> None:
    """PySpark ``clearTags`` — drop every tag. pins: session-surface-1/C-001"""
    session._ensure_alive()
    _job_tags(session).clear()


def interrupt_all(session: ReparkSession) -> list[str]:
    """PySpark ``interruptAll`` — repark runs actions synchronously on the calling thread.

    pins: session-surface-1/C-002
    """
    session._ensure_alive()
    return []


def interrupt_tag(session: ReparkSession, tag: str) -> list[str]:
    """PySpark ``interruptTag`` — no cancellable operation registry, always ``[]``.

    pins: session-surface-1/C-002
    """
    session._ensure_alive()
    _require_str_tag(tag)
    return []


def interrupt_operation(session: ReparkSession, op_id: str) -> list[str]:
    """PySpark ``interruptOperation`` — numeric ids only, always ``[]``.

    pins: session-surface-1/C-002
    """
    session._ensure_alive()
    if not isinstance(op_id, str) or _EXECUTION_ID_RE.fullmatch(op_id) is None:
        raise IllegalArgumentException("executionId must be a number in string form.")
    return []


def session_client(session: ReparkSession) -> NoReturn:
    """PySpark ``client`` — Spark Connect only; classic refuses identically.

    pins: session-surface-1/C-003
    """
    _ = session
    _connect_only("SparkSession.client")


def copy_from_local_to_fs(session: ReparkSession, local_path: str, dest_path: str) -> NoReturn:
    """PySpark ``copyFromLocalToFs`` — Spark Connect only. pins: session-surface-1/C-003"""
    _ = (session, local_path, dest_path)
    _connect_only("SparkSession.copyFromLocalToFs")


def register_progress_handler(session: ReparkSession, handler: Any) -> NoReturn:
    """PySpark ``registerProgressHandler`` — Spark Connect only.

    pins: session-surface-1/C-003
    """
    _ = (session, handler)
    _connect_only("SparkSession.registerProgressHandler")


def remove_progress_handler(session: ReparkSession, handler: Any) -> NoReturn:
    """PySpark ``removeProgressHandler`` — Spark Connect only.

    pins: session-surface-1/C-003
    """
    _ = (session, handler)
    _connect_only("SparkSession.removeProgressHandler")


def clear_progress_handlers(session: ReparkSession) -> NoReturn:
    """PySpark ``clearProgressHandlers`` — Spark Connect only.

    pins: session-surface-1/C-003
    """
    _ = session
    _connect_only("SparkSession.clearProgressHandlers")


def session_read_stream(session: ReparkSession) -> NoReturn:
    """PySpark ``readStream`` — declared: no Structured Streaming engine.

    pins: session-surface-1/C-004
    """
    session._ensure_alive()
    _not_implemented("readStream")


def session_streams(session: ReparkSession) -> NoReturn:
    """PySpark ``streams`` — declared: no StreamingQueryManager without streaming.

    pins: session-surface-1/C-004
    """
    session._ensure_alive()
    _not_implemented("streams")


def session_data_source(session: ReparkSession) -> NoReturn:
    """PySpark ``dataSource`` — declared: the Python data source API is deferred.

    pins: session-surface-1/C-004
    """
    session._ensure_alive()
    _not_implemented("dataSource")


def _artifact_conflicts(root: str | None, paths: tuple[str, ...]) -> set[str]:
    unchanged: set[str] = set()
    if root is None:
        return unchanged
    for artifact_path in paths:
        normalized = str(Path(artifact_path).resolve())
        target = Path(root) / Path(normalized).name
        if not target.exists():
            continue
        if filecmp.cmp(normalized, target, shallow=False):
            unchanged.add(normalized)
            continue
        raise PySparkRuntimeError(
            f"[DUPLICATED_ARTIFACT] Duplicate Artifact: {normalized}. "
            "Artifacts cannot be overwritten.",
            errorClass="DUPLICATED_ARTIFACT",
            messageParameters={"normalized_path": normalized},
        )
    return unchanged


def _artifact_dir(session: ReparkSession) -> str:
    root = session._alive_token.get("artifact_dir")
    if root is None:
        root = tempfile.mkdtemp(prefix="repark-artifacts-")
        session._alive_token["artifact_dir"] = root
        sys.path.insert(0, root)
    return str(root)


def add_artifacts(
    session: ReparkSession,
    *path: str,
    pyfile: bool = False,
    archive: bool = False,
    file: bool = False,
) -> None:
    """PySpark ``addArtifacts`` / ``addArtifact`` — driver-local pyfile copy only.

    pins: session-surface-1/C-005
    """
    session._ensure_alive()
    if sum([file, pyfile, archive]) > 1:
        raise PySparkValueError(
            "[INVALID_MULTIPLE_ARGUMENT_CONDITIONS] "
            "['pyfile', 'archive' and/or 'file'] cannot be True together.",
            errorClass="INVALID_MULTIPLE_ARGUMENT_CONDITIONS",
            messageParameters={
                "arg_names": "'pyfile', 'archive' and/or 'file'",
                "condition": "True together",
            },
        )
    unchanged = _artifact_conflicts(session._alive_token.get("artifact_dir"), path)
    if archive:
        _not_implemented("addArtifacts(archive=True)")
    if file:
        _not_implemented("addArtifacts(file=True)")
    if not pyfile:
        return
    root = _artifact_dir(session)
    for artifact_path in path:
        normalized = str(Path(artifact_path).resolve())
        if normalized in unchanged:
            continue
        shutil.copyfile(normalized, Path(root) / Path(normalized).name)


def _profiler_collector(session: ReparkSession) -> _ProfilerCollector:
    collector = session._alive_token.get("profiler_collector")
    if collector is None:
        collector = _ProfilerCollector()
        session._alive_token["profiler_collector"] = collector
    return collector


def session_profile(session: ReparkSession) -> Profile:
    """PySpark ``profile`` — a :class:`Profile` over the session's collector.

    pins: session-surface-1/C-006
    """
    session._ensure_alive()
    return Profile(_profiler_collector(session))


def session_tvf(session: ReparkSession) -> TableValuedFunction:
    """PySpark ``tvf`` — a fresh :class:`TableValuedFunction` per access.

    pins: session-surface-1/C-007
    """
    session._ensure_alive()
    return TableValuedFunction(session)


class _ProfilerCollector:
    """Session UDF profile store; repark collects no profiles, so it stays empty."""

    def __init__(self) -> None:
        """Initialize empty perf and memory profile maps."""
        self._perf_profiles: dict[int, Any] = {}
        self._memory_profiles: dict[int, Any] = {}

    def add_perf_profile(self, id: int, profile: Any) -> None:
        """Record a perf profile for a UDF id (repark never calls this)."""
        self._perf_profiles[id] = profile

    def add_memory_profile(self, id: int, profile: Any) -> None:
        """Record a memory profile for a UDF id (repark never calls this)."""
        self._memory_profiles[id] = profile

    def show_perf_profiles(self, id: int | None = None) -> None:
        """Print stored perf profiles (empty store prints nothing)."""
        ids = [id] if id is not None else sorted(self._perf_profiles)
        for profile_id in ids:
            stats = self._perf_profiles.get(profile_id)
            if stats is not None:
                print("=" * 60)
                print(f"Profile of UDF<id={profile_id}>")
                print("=" * 60)
                print(stats)

    def show_memory_profiles(self, id: int | None = None) -> None:
        """Print stored memory profiles; warns while the profiler lib is absent."""
        if not self._memory_profiles:
            warnings.warn(_MEMORY_PROFILER_WARNING, UserWarning, stacklevel=2)
        ids = [id] if id is not None else sorted(self._memory_profiles)
        for profile_id in ids:
            stats = self._memory_profiles.get(profile_id)
            if stats is not None:
                print("=" * 60)
                print(f"Profile of UDF<id={profile_id}>")
                print("=" * 60)
                print(stats)

    def dump_perf_profiles(self, path: str, id: int | None = None) -> None:
        """Write stored perf profiles under ``path`` (empty store writes nothing)."""
        ids = [id] if id is not None else sorted(self._perf_profiles)
        for profile_id in ids:
            stats = self._perf_profiles.get(profile_id)
            if stats is not None:
                Path(path).mkdir(parents=True, exist_ok=True)
                stats.dump_stats(str(Path(path) / f"udf_{profile_id}_perf.pstats"))

    def dump_memory_profiles(self, path: str, id: int | None = None) -> None:
        """Write stored memory profiles under ``path``; warns while the lib is absent."""
        if not self._memory_profiles:
            warnings.warn(_MEMORY_PROFILER_WARNING, UserWarning, stacklevel=2)
        ids = [id] if id is not None else sorted(self._memory_profiles)
        for profile_id in ids:
            stats = self._memory_profiles.get(profile_id)
            if stats is not None:
                Path(path).mkdir(parents=True, exist_ok=True)
                Path(path, f"udf_{profile_id}_memory.txt").write_text(str(stats))

    def clear_perf_profiles(self, id: int | None = None) -> None:
        """Drop perf profiles for ``id`` or every id."""
        if id is not None:
            self._perf_profiles.pop(id, None)
        else:
            self._perf_profiles.clear()

    def clear_memory_profiles(self, id: int | None = None) -> None:
        """Drop memory profiles for ``id`` or every id."""
        if id is not None:
            self._memory_profiles.pop(id, None)
        else:
            self._memory_profiles.clear()


class Profile:
    """PySpark ``spark.profile`` user-facing API; repark collects no UDF profiles."""

    def __init__(self, profiler_collector: _ProfilerCollector) -> None:
        """Bind the session's profiler collector."""
        self.profiler_collector = profiler_collector

    def show(self, id: int | None = None, *, type: str | None = None) -> None:
        """Show profiles; prints nothing when none were collected.

        pins: session-surface-1/C-006
        """
        if type == "memory":
            self.profiler_collector.show_memory_profiles(id)
        elif type == "perf" or type is None:
            self.profiler_collector.show_perf_profiles(id)
            if type is None:
                self.profiler_collector.show_memory_profiles(id)
        else:
            _refuse_profile_type()

    def dump(self, path: str, id: int | None = None, *, type: str | None = None) -> None:
        """Dump profiles under ``path``; empty state writes nothing.

        pins: session-surface-1/C-006
        """
        if type == "memory":
            self.profiler_collector.dump_memory_profiles(path, id)
        elif type == "perf" or type is None:
            self.profiler_collector.dump_perf_profiles(path, id)
            if type is None:
                self.profiler_collector.dump_memory_profiles(path, id)
        else:
            _refuse_profile_type()

    def clear(self, id: int | None = None, *, type: str | None = None) -> None:
        """Clear profiles; a no-op on empty state. pins: session-surface-1/C-006"""
        if type == "memory":
            self.profiler_collector.clear_memory_profiles(id)
        elif type == "perf" or type is None:
            self.profiler_collector.clear_perf_profiles(id)
            if type is None:
                self.profiler_collector.clear_memory_profiles(id)
        else:
            _refuse_profile_type()

    def render(self, id: int, *, type: str | None = None, renderer: Any = None) -> Any:
        """Declared: no UDF profiles exist to render. pins: session-surface-1/C-006"""
        _ = (id, type, renderer)
        _not_implemented("profile.render")


class TableValuedFunction:
    """PySpark ``spark.tvf`` — table-valued functions over existing select/sql paths."""

    def __init__(self, session: ReparkSession) -> None:
        """Bind the owning session."""
        self._session = session

    def range(
        self,
        start: int,
        end: int | None = None,
        step: int = 1,
        numPartitions: int | None = None,  # noqa: N803 — PySpark arg name
    ) -> DataFrame:
        """PySpark ``tvf.range`` — delegates to ``spark.range``.

        pins: session-surface-1/C-007
        """
        return self._session.range(start, end, step, numPartitions)

    def explode(self, collection: Column) -> DataFrame:
        """PySpark ``tvf.explode`` — one ``range(1)`` row through ``F.explode``.

        pins: session-surface-1/C-007
        """
        return self._generator("explode", "collection", collection)

    def explode_outer(self, collection: Column) -> DataFrame:
        """PySpark ``tvf.explode_outer`` — null/empty arrays yield one null row.

        pins: session-surface-1/C-007
        """
        return self._generator("explode_outer", "collection", collection)

    def posexplode(self, collection: Column) -> DataFrame:
        """PySpark ``tvf.posexplode`` — keeps ``F.posexplode``'s own refusal today.

        pins: session-surface-1/C-007
        """
        return self._generator("posexplode", "collection", collection)

    def posexplode_outer(self, collection: Column) -> DataFrame:
        """PySpark ``tvf.posexplode_outer`` — keeps the ordinal refusal today.

        pins: session-surface-1/C-007
        """
        return self._generator("posexplode_outer", "collection", collection)

    def inline(self, input: Column) -> DataFrame:
        """PySpark ``tvf.inline`` — declared until ``F.inline`` exists.

        pins: session-surface-1/C-007
        """
        return self._generator("inline", "input", input)

    def inline_outer(self, input: Column) -> DataFrame:
        """PySpark ``tvf.inline_outer`` — declared until ``F.inline_outer`` exists.

        pins: session-surface-1/C-007
        """
        return self._generator("inline_outer", "input", input)

    def variant_explode(self, input: Column) -> DataFrame:
        """PySpark ``tvf.variant_explode`` — declared until the function exists.

        pins: session-surface-1/C-007
        """
        return self._generator("variant_explode", "input", input)

    def variant_explode_outer(self, input: Column) -> DataFrame:
        """PySpark ``tvf.variant_explode_outer`` — declared until the function exists.

        pins: session-surface-1/C-007
        """
        return self._generator("variant_explode_outer", "input", input)

    def json_tuple(self, input: Column, *fields: Column) -> DataFrame:
        """PySpark ``tvf.json_tuple`` — ``CANNOT_BE_EMPTY`` precedes the type checks.

        pins: session-surface-1/C-007
        """
        if len(fields) == 0:
            raise PySparkValueError(
                "[CANNOT_BE_EMPTY] At least one field must be specified.",
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "field"},
            )
        _require_column_arg(input, "input")
        for field in fields:
            _require_column_arg(field, "fields")
        return self._generator_variadic("json_tuple", input, *fields)

    def stack(self, n: Column, *fields: Column) -> DataFrame:
        """PySpark ``tvf.stack`` — the ``StackCall`` lowers inside ``select``.

        pins: session-surface-1/C-007
        """
        _require_column_arg(n, "n")
        for field in fields:
            _require_column_arg(field, "fields")
        return self._generator_variadic("stack", n, *fields)

    def sql_keywords(self) -> DataFrame:
        """PySpark ``tvf.sql_keywords`` — declared until the SQL door answers it.

        pins: session-surface-1/C-007
        """
        return self._sql_table_function("sql_keywords")

    def collations(self) -> DataFrame:
        """PySpark ``tvf.collations`` — declared until the SQL door answers it.

        pins: session-surface-1/C-007
        """
        return self._sql_table_function("collations")

    def python_worker_logs(self) -> DataFrame:
        """PySpark ``tvf.python_worker_logs`` — declared: no Python worker plumbing.

        pins: session-surface-1/C-007
        """
        _not_implemented("tvf.python_worker_logs")

    def _generator(self, name: str, arg_name: str, arg: Column) -> DataFrame:
        _require_column_arg(arg, arg_name)
        return self._generator_variadic(name, arg)

    def _generator_variadic(self, name: str, *args: Column) -> DataFrame:
        import repark.spark.functions as functions

        generator = getattr(functions, name, None)
        if generator is None:
            _not_implemented(f"tvf.{name}")
        return self._session.range(1).select(generator(*args))

    def _sql_table_function(self, name: str) -> DataFrame:
        try:
            return self._session.sql(f"SELECT * FROM {name}()")
        except (AnalysisException, ParseException):
            _not_implemented(f"tvf.{name}")
