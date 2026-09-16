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
    if path is None:
        from repark.errors import AnalysisException

        raise AnalysisException("ORC load requires a path argument")
    _check_orc_path_type(path)
    merge_schema = reader._option_bool("mergeschema", default=False)
    recursive_lookup = reader._option_bool("recursivefilelookup", default=False)
    ignore_corrupt = reader._option_bool("ignorecorruptfiles", default=False)
    base_path = _orc_base_path(reader)
    user_schema = _orc_user_schema(reader)
    inner = reader._session._ensure_alive()
    token = reader._session._alive_token
    if isinstance(path, list):
        if not path:
            from repark.errors import AnalysisException

            raise AnalysisException("ORC load requires a non-empty path list")
        frames = [
            _read_one(
                inner,
                token,
                str(item),
                merge_schema,
                reader._option_str("pathglobfilter"),
                recursive_lookup,
                reader._option_str("modifiedbefore"),
                reader._option_str("modifiedafter"),
                base_path,
                ignore_corrupt,
                user_schema,
            )
            for item in path
        ]
        frame = frames[0]
        for extra in frames[1:]:
            if merge_schema:
                frame = frame.unionByName(extra, allowMissingColumns=True)
            else:
                frame = frame.union(extra)
    else:
        frame = _read_one(
            inner,
            token,
            str(path),
            merge_schema,
            reader._option_str("pathglobfilter"),
            recursive_lookup,
            reader._option_str("modifiedbefore"),
            reader._option_str("modifiedafter"),
            base_path,
            ignore_corrupt,
            user_schema,
        )
    return frame


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


def _read_one(
    inner: Any,
    token: Any,
    path: str,
    merge_schema: bool,
    path_glob_filter: str | None,
    recursive_lookup: bool,
    modified_before: str | None,
    modified_after: str | None,
    base_path: str | None,
    ignore_corrupt: bool,
    user_schema: list[tuple[str, str]] | None,
) -> DataFrame:
    """Read one file, directory, or glob through the Rust ORC scan. pins: io-orc-1/C-002"""
    from repark import _native
    from repark.errors import AnalysisException

    try:
        return DataFrame(
            _native.read_orc(
                inner,
                path,
                merge_schema=merge_schema,
                path_glob_filter=path_glob_filter,
                recursive_file_lookup=recursive_lookup,
                modified_before=modified_before,
                modified_after=modified_after,
                base_path=base_path,
                ignore_corrupt_files=ignore_corrupt,
                user_schema=user_schema,
            ),
            inner,
            token,
        )
    except AnalysisException as error:
        _attach_orc_condition(error, path)
        raise


def _attach_orc_condition(error: Any, path: str) -> None:
    """Attach Spark's error class, params, and SQLSTATE to a native ORC error."""
    from repark.spark._integral import attach_error_condition

    text = str(error)
    if text.startswith("[PATH_NOT_FOUND]"):
        attach_error_condition(error, "PATH_NOT_FOUND", "42K03")
        error._spark_message_parameters = {"path": f"file:{path}"}
    elif text.startswith("[UNABLE_TO_INFER_SCHEMA]"):
        attach_error_condition(error, "UNABLE_TO_INFER_SCHEMA", "42KD9")
        error._spark_message_parameters = {"format": "ORC"}
    elif text.startswith("[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]"):
        attach_error_condition(error, "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER", "KD001")
    elif text.startswith("[CANNOT_MERGE_SCHEMAS]"):
        attach_error_condition(error, "CANNOT_MERGE_SCHEMAS")


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
