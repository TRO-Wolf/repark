"""Text reader: one string column over files, directories, and path lists."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from repark.spark.dataframe import DataFrame
from repark.spark.session.reader_support import _schema_fields

if TYPE_CHECKING:
    from repark.spark.session.reader import DataFrameReader


def text(
    self: DataFrameReader,
    paths: str | Path | list[str] | None = None,
    wholetext: bool = False,
    lineSep: str | None = None,  # noqa: N803 — PySpark param name
    pathGlobFilter: str | None = None,  # noqa: N803 — PySpark param name
    recursiveFileLookup: str | bool | None = None,  # noqa: N803 — PySpark param name
    modifiedBefore: str | None = None,  # noqa: N803 — PySpark param name
    modifiedAfter: str | None = None,  # noqa: N803 — PySpark param name
) -> DataFrame:
    """Read text files as one string column (PySpark spark.read.text). pins: io-text-1/C-001"""
    self.option("wholetext", wholetext)
    for key, value in (
        ("lineSep", lineSep),
        ("pathGlobFilter", pathGlobFilter),
        ("recursiveFileLookup", recursiveFileLookup),
        ("modifiedBefore", modifiedBefore),
        ("modifiedAfter", modifiedAfter),
    ):
        if value is not None:
            self.option(key, value)
    if paths is None:
        paths = self._option_path()
    if paths is None:
        from repark.errors import AnalysisException

        raise AnalysisException("Text load requires a path argument")
    self._format = "text"
    return load_text(self, paths)


def load_text(reader: Any, path: str | Path | list[str] | None) -> DataFrame:
    """Run a text load for the reader option map (format(text).load). pins: io-text-1/C-001"""
    if path is None:
        from repark.errors import AnalysisException

        raise AnalysisException("Text load requires a path argument")
    _drop_falsy_recursive_lookup(reader)
    reader._reject_unsupported_semantic_options()
    _reject_text_encoding(reader)
    wholetext = reader._option_bool("wholetext", default=False)
    linesep = reader._option_str("linesep")
    inner = reader._session._ensure_alive()
    token = reader._session._alive_token
    if isinstance(path, list):
        if not path:
            from repark.errors import AnalysisException

            raise AnalysisException("Text load requires a non-empty path list")
        frames = [_read_one(inner, token, str(item), wholetext, linesep) for item in path]
        frame = frames[0]
        for extra in frames[1:]:
            frame = frame.union(extra)
    else:
        frame = _read_one(inner, token, str(path), wholetext, linesep)
    return _apply_text_schema(reader, frame)


def _read_one(inner: Any, token: Any, path: str, wholetext: bool, linesep: str | None) -> DataFrame:
    """Read one file or directory through the Rust text scan. pins: io-text-1/C-001"""
    from repark import _native
    from repark.errors import AnalysisException
    from repark.spark._integral import attach_error_condition

    try:
        return DataFrame(_native.read_text(inner, path, wholetext, linesep), inner, token)
    except AnalysisException as error:
        if str(error).startswith("[PATH_NOT_FOUND]"):
            attach_error_condition(error, "PATH_NOT_FOUND", "42K03")
        raise


def _drop_falsy_recursive_lookup(reader: Any) -> None:
    """Drop an explicit non-recursive lookup flag (the scan never recurses). pins: io-text-1/T-1"""
    for key in [key for key in reader._options if key.lower() == "recursivefilelookup"]:
        if str(reader._options[key]).strip().lower() == "false":
            del reader._options[key]


def _reject_text_encoding(reader: Any) -> None:
    """Refuse non-UTF-8 text encodings loud (the scan reads UTF-8). pins: io-text-1/C-001"""
    encoding = reader._option_str("encoding")
    if encoding is not None and encoding.strip().lower().replace("-", "") not in {
        "",
        "utf8",
        "utf_8",
    }:
        from repark.errors import AnalysisException

        raise AnalysisException(
            f"reader option encoding={encoding!r} is not supported (only UTF-8)"
        )


def _apply_text_schema(reader: Any, frame: DataFrame) -> DataFrame:
    """Project a single-string user schema, or refuse anything wider. pins: io-text-1/C-001"""
    from repark.spark import functions as F  # noqa: N812 — local import avoids cycle at module load
    from repark.spark.types import StringType

    if reader._schema is None:
        return frame
    fields = _schema_fields(reader._schema)
    if len(fields) != 1 or not isinstance(fields[0]["dataType"], StringType):
        from repark.errors import AnalysisException

        raise AnalysisException(
            "text schema must be a single string field (the scan only serves struct<value:string>)"
        )
    return frame.select(F.col("value").cast("string").alias(fields[0]["name"]))
