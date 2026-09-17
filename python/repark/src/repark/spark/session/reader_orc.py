"""ORC reader: Spark-typed columns over files, directories, globs, and path lists."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from repark.spark.dataframe import DataFrame
from repark.spark.session.reader_support import _schema_fields

if TYPE_CHECKING:
    from repark.spark.session.reader import DataFrameReader


def orc(
    self: DataFrameReader,
    path: str | Path | list[str | Path] | None = None,
    mergeSchema: bool | None = None,  # noqa: N803 — PySpark param name
    pathGlobFilter: str | None = None,  # noqa: N803 — PySpark param name
    recursiveFileLookup: str | bool | None = None,  # noqa: N803 — PySpark param name
    modifiedBefore: str | None = None,  # noqa: N803 — PySpark param name
    modifiedAfter: str | None = None,  # noqa: N803 — PySpark param name
) -> DataFrame:
    """Read ORC files (PySpark spark.read.orc). pins: io-orc-1/C-002"""
    from repark.errors import IllegalArgumentException

    if mergeSchema is not None and not isinstance(mergeSchema, bool):
        raise IllegalArgumentException(f'For input string: "{mergeSchema}"')
    for key, value in (
        ("mergeSchema", mergeSchema),
        ("pathGlobFilter", pathGlobFilter),
        ("recursiveFileLookup", recursiveFileLookup),
        ("modifiedBefore", modifiedBefore),
        ("modifiedAfter", modifiedAfter),
    ):
        if value is not None:
            self.option(key, value)
    if path is None:
        path = self._option_path()
    if path is None:
        from repark.errors import AnalysisException

        raise AnalysisException("ORC load requires a path argument")
    _check_orc_path_type(path)
    self._format = "orc"
    return load_orc(self, path)


def load_orc(reader: Any, path: str | Path | list[str | Path] | None) -> DataFrame:
    """Run an ORC load for the reader option map (format(orc).load). pins: io-orc-1/C-002"""
    from repark import _native
    from repark.errors import AnalysisException

    if path is None:
        raise AnalysisException("ORC load requires a path argument")
    _check_orc_path_type(path)
    if isinstance(path, list) and not path:
        raise AnalysisException("ORC load requires a non-empty path list")
    raw_paths: list[str | Path] = path if isinstance(path, list) else [path]
    paths: list[str] = [str(item) for item in raw_paths]
    inner = reader._session._ensure_alive()
    token = reader._session._alive_token
    try:
        native = _native.read_orc(
            inner,
            paths,
            merge_schema=reader._option_bool("mergeschema", default=False),
            path_glob_filter=reader._option_str("pathglobfilter"),
            recursive_file_lookup=reader._option_bool("recursivefilelookup", default=False),
            modified_before=reader._option_str("modifiedbefore"),
            modified_after=reader._option_str("modifiedafter"),
            base_path=_orc_base_path(reader),
            ignore_corrupt_files=reader._option_bool("ignorecorruptfiles", default=False),
            user_schema=_orc_user_schema(reader),
        )
    except AnalysisException as error:
        from repark.spark.dataframe.surface_a import _bind_native_condition

        _bind_native_condition(error)
        raise
    return DataFrame(native, inner, token)


def _check_orc_path_type(path: Any) -> None:
    """Reject a non-str/non-list ORC path with Spark's NOT_STR_OR_LIST. pins: io-orc-1/C-005"""
    from repark.errors import PySparkTypeError

    if isinstance(path, (str, Path)):
        return
    if isinstance(path, list) and all(isinstance(item, (str, Path)) for item in path):
        return
    if isinstance(path, list):
        bad = next(item for item in path if not isinstance(item, (str, Path)))
        got = type(bad).__name__
    else:
        got = type(path).__name__
    raise PySparkTypeError(
        f"[NOT_STR_OR_LIST] Argument `path` should be a str or list, got {got}.",
        errorClass="NOT_STR_OR_LIST",
        messageParameters={"arg_name": "path", "arg_type": got},
    )


def _orc_base_path(reader: Any) -> str | None:
    """Return the basePath option for partition discovery under a glob. pins: io-orc-1/C-006"""
    return reader._option_str("basepath")


def _orc_user_schema(reader: Any) -> list[tuple[str, str]] | None:
    """Return the user schema as data-schema name/type pairs for the engine. pins: io-orc-1/C-007"""
    if reader._schema is None:
        return None
    return [
        (str(field["name"]), str(field["dataType"].simpleString()))
        for field in _schema_fields(reader._schema)
    ]
