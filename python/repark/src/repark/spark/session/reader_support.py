"""Reader option and schema support."""

from __future__ import annotations

from pathlib import Path

from typing import Any

from repark.spark.dataframe import DataFrame

from repark.errors import AnalysisException, IllegalArgumentException, PySparkException


_UNSUPPORTED_SEMANTIC_READER_OPTIONS: frozenset[str] = frozenset(
    {
        "pathglobfilter",
        "recursivefilelookup",
        "mergeschema",
        "basepath",
        "modifiedbefore",
        "modifiedafter",
        "ignorecorruptfiles",
        "ignoremissingfiles",
        "encryption",
        # Parquet value-semantic read options.
        "datetimerebasemode",
        "datetimerebasemodeinread",
        "int96rebasemode",
        "int96rebasemodeinread",
        # Iceberg incremental-read window (future seed) — not time-travel pins.
        "start-snapshot-id",
        "end-snapshot-id",
    }
)


_CSV_UNSUPPORTED_PARSE_OPTIONS: frozenset[str] = frozenset(
    {
        "encoding",
        "dateformat",
        "timestampformat",
        "timestampntzformat",
        "locale",
        "chartoescapequoteescaping",
        "samplingratio",
        "enforceschema",
        "columnnameofcorruptrecord",
        "nanvalue",
        "positiveinf",
        "negativeinf",
        "maxcolumns",
        "maxcharspercolumn",
        "preferdate",
        "ignoreleadingwhitespace",
        "ignoretrailingwhitespace",
        "linesep",
        "unescapedquotehandling",
    }
)


_JSON_UNSUPPORTED_PARSE_OPTIONS: frozenset[str] = frozenset(
    {
        "primitivesasstring",
        "prefersdecimal",
        "allowcomments",
        "allowunquotedfieldnames",
        "allowsinglequotes",
        "allownumericleadingzeros",
        "allowbackslashescapinganycharacter",
        "allowunquotedcontrolchars",
        "columnnameofcorruptrecord",
        "dateformat",
        "timestampformat",
        "timestampntzformat",
        "encoding",
        "linesep",
        "samplingratio",
        "dropfieldifallnull",
        "locale",
        "timezone",
        "timeZone",
    }
)


_CSV_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "header",
        "sep",
        "delimiter",
        "quote",
        "escape",
        "comment",
        "nullvalue",
        "inferschema",
        "multiline",
        "compression",
    }
)


_JSON_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "multiline",
        "compression",
    }
)


_EXCEL_NATIVE_OPTION_KEYS: frozenset[str] = frozenset(
    {
        "sheet_name",
        "sheetname",
        "header",
        "skip_rows",
        "skiprows",
        "schema",
        "column_names",
        "names",
    }
)


_ICEBERG_TIME_TRAVEL_OPTIONS: frozenset[str] = frozenset(
    {
        "snapshot-id",
        "as-of-timestamp",
        "branch",
        "tag",
    }
)


_I64_MIN = -(2**63)


_I64_MAX = 2**63 - 1


def _parse_jdbc_int_option(name: str, raw: str | None) -> int | None:
    """Parse a JDBC partition integer option; map bad text to IllegalArgumentException."""

    if raw is None:
        return None

    try:
        return int(raw)

    except (TypeError, ValueError) as exc:
        from repark.errors import IllegalArgumentException

        raise IllegalArgumentException(
            f"jdbc option {name} must be an integer, got {raw!r}"
        ) from exc


def _reader_path_to_str(path: str | Path | list[str]) -> str:
    """Normalize a reader path argument to a single filesystem path string.



    Multi-path lists: Spark unions them; repark v1 accepts a single path or a one-element list

    and fails loud on multi-path (no silent partial read).

    """

    if isinstance(path, list):
        if len(path) == 0:
            from repark.errors import AnalysisException

            raise AnalysisException("CSV/JSON load requires a non-empty path")

        if len(path) > 1:
            from repark.errors import AnalysisException

            raise AnalysisException(
                "reading multiple paths in one load() is not supported by repark yet "
                f"(got {len(path)} paths)"
            )

        return str(path[0])

    return str(path)


def _json_input_nonempty(path_str: str) -> bool:
    """True when a local JSON path (file or dir) has non-zero content to parse."""

    path = Path(path_str)

    if path.is_file():
        try:
            return path.stat().st_size > 0

        except OSError:
            return False

    if path.is_dir():
        for child in path.rglob("*"):
            if child.is_file():
                try:
                    if child.stat().st_size > 0:
                        return True

                except OSError:
                    continue

    return False


def _json_multiline_empty_schema_is_mismatch(path_str: str) -> bool:
    """True when multiLine empty schema likely means wrong shape (not empty ``[]``).



    Empty array files are valid zero-row inputs (octo R1-C5-001). Pretty single objects and

    NDJSON under multiLine still fail loud (R1-C1-005).

    """

    if not _json_input_nonempty(path_str):
        return False

    path = Path(path_str)

    sample = b""

    try:
        if path.is_file():
            with path.open("rb") as handle:
                sample = handle.read(4096)

        elif path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file() and child.stat().st_size > 0:
                    with child.open("rb") as handle:
                        sample = handle.read(4096)

                    break

    except OSError:
        return True

    stripped = sample.lstrip()

    if not stripped:
        return False

    # Top-level array (including ``[]``) is the multiLine contract — empty schema is OK.

    return not stripped.startswith(b"[")


def _csv_string_column_has_clock(frame: DataFrame, name: str) -> bool:
    """True when a non-null string cell contains ``:`` (a clock, not a date-only day)."""

    from repark.spark import functions as F  # noqa: N812

    marker = frame.select(F.max(F.col(name).cast("string").contains(":")).alias("has_clock"))
    values = marker.to_arrow().column(0).to_pylist()
    return bool(values) and values[0] is True


def _promote_csv_string_types(frame: DataFrame) -> DataFrame:
    """Spark-like type promotion on an all-string CSV frame after nullValue application.

    Tries bigint → double → boolean → timestamp → date per column via engine CAST; keeps
    string on failure. Timestamp requires a ``:``; date requires its absence.
    Validates each trial by materializing so a late bad value rejects the type.
    """

    from repark.spark import functions as F  # noqa: N812

    columns = list(frame.columns)

    if not columns:
        return frame

    candidates = ("bigint", "double", "boolean", "timestamp", "date")
    dtypes = dict(frame.dtypes)
    selects: list[Any] = []

    for name in columns:
        if dtypes.get(name) != "string":
            selects.append(F.col(name))
            continue

        promoted: Any | None = None
        has_clock = _csv_string_column_has_clock(frame, name)

        for type_name in candidates:
            if type_name == "timestamp" and not has_clock:
                continue
            if type_name == "date" and has_clock:
                continue

            trial = frame.select(F.col(name).cast(type_name).alias(name))

            try:
                trial.to_arrow()
                promoted = type_name
                break
            except (AnalysisException, PySparkException, RuntimeError, ValueError, TypeError):
                continue

        if promoted is not None:
            selects.append(F.col(name).cast(promoted).alias(name))
        else:
            selects.append(F.col(name))

    return frame.select(*selects)


def _cast_inferred_naive_timestamps(frame: DataFrame) -> DataFrame:
    """Cast inferred tz-naive timestamps to instant ``timestamp`` (Spark inferSchema).

    Spark infers timestamps as ``timestamp`` on every text source; the engine infers
    tz-naive Arrow, which the facade otherwise reports ``timestamp_ntz`` (correct for
    parquet/NTZ sources). CAST goes through string so the session zone localizes the
    wall clock the way CAST(str AS TIMESTAMP) does. Runs only past the user-schema
    and no-infer returns, so an explicit user schema never reaches it.
    """

    from repark.spark import functions as F  # noqa: N812

    selects = [
        F.col(name).cast("string").cast("timestamp").alias(name)
        if dtype == "timestamp_ntz"
        else F.col(name)
        for name, dtype in frame.dtypes
    ]
    if not selects:
        return frame
    return frame.select(*selects)


def _schema_fields(schema: Any) -> list[dict[str, Any]]:
    """Normalize StructType / DDL / field-list into ``[{name, dataType}, …]`` for reader casts."""

    from repark.spark.types import DataType, StructField, StructType

    if isinstance(schema, StructType):
        return [{"name": field.name, "dataType": field.dataType} for field in schema.fields]

    if isinstance(schema, str):
        parsed = DataType.fromDDL(schema)

        if isinstance(parsed, StructType):
            return [{"name": field.name, "dataType": field.dataType} for field in parsed.fields]

        return [{"name": "value", "dataType": parsed}]

    if isinstance(schema, (list, tuple)) and schema and isinstance(schema[0], StructField):
        return [{"name": field.name, "dataType": field.dataType} for field in schema]

    if isinstance(schema, DataType) and not isinstance(schema, StructType):
        return [{"name": "value", "dataType": schema}]

    from repark.errors import AnalysisException

    raise AnalysisException(
        f"DataFrameReader.schema expects StructType, DDL string, or StructField list; "
        f"got {type(schema).__name__}"
    )
