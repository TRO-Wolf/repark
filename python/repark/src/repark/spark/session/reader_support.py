"""Reader option and schema support."""

from __future__ import annotations

from pathlib import Path

from typing import Any

from repark.spark.dataframe import DataFrame


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
        "utf8_columns",
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


_CSV_PROMOTE_CANDIDATES: tuple[str, ...] = ("bigint", "double", "boolean", "timestamp", "date")


def _finish_csv_infer_schema(
    frame: DataFrame,
    reader: Any,
    infer_schema: bool,
    null_token: str | None,
) -> DataFrame:
    """Finish CSV inferSchema: native types plus Utf8 timestamp text, or nullValue promotion."""

    from repark.spark import functions as F  # noqa: N812

    if not infer_schema:
        return frame
    if null_token is not None:
        return _promote_csv_string_types(frame)
    is_csv = (reader._format or "").lower() == "csv"
    csv_path = reader._option_path()
    if not is_csv or csv_path is None:
        return frame
    timestamp_names = [
        name for name, dtype in frame.dtypes if dtype in {"timestamp", "timestamp_ntz"}
    ]
    if not timestamp_names:
        return frame
    options = reader._native_options_for(_CSV_NATIVE_OPTION_KEYS)
    if "header" not in {key.lower() for key in options}:
        options["header"] = "false"
    options["utf8_columns"] = ",".join(timestamp_names)
    reread = reader._session.read_csv(csv_path, options)
    timestamp_name_set = set(timestamp_names)
    selects = [
        F.col(name).cast("timestamp").alias(name) if name in timestamp_name_set else F.col(name)
        for name in reread.columns
    ]
    return reread.select(*selects)


def _promote_csv_string_types(frame: DataFrame) -> DataFrame:
    """Spark-like type promotion on an all-string CSV frame after nullValue application.

    Tries bigint → double → boolean → timestamp → date per column via engine CAST; keeps
    string on failure. Timestamp requires a ``:``; date requires its absence.
    One aggregation of try_cast failure counts rejects a type when any non-null cell fails.
    """

    from repark.spark import functions as F  # noqa: N812

    columns = list(frame.columns)

    if not columns:
        return frame

    dtypes = dict(frame.dtypes)
    string_names = [name for name in columns if dtypes.get(name) == "string"]
    if not string_names:
        return frame

    aggregates: list[Any] = []
    clock_aliases: list[str] = []
    fail_aliases: list[tuple[str, str, str]] = []
    for column_index, name in enumerate(string_names):
        clock_alias = f"c{column_index}_clock"
        clock_aliases.append(clock_alias)
        aggregates.append(F.max(F.col(name).cast("string").contains(":")).alias(clock_alias))
        for type_name in _CSV_PROMOTE_CANDIDATES:
            fail_alias = f"c{column_index}_fail_{type_name}"
            fail_aliases.append((name, type_name, fail_alias))
            failed = (F.col(name).isNotNull()) & (F.col(name).try_cast(type_name).isNull())
            aggregates.append(F.sum(F.when(failed, 1).otherwise(0)).alias(fail_alias))

    stats = frame.agg(*aggregates).to_arrow()
    values: dict[str, Any] = {
        field.name: stats.column(field.name)[0].as_py() for field in stats.schema
    }
    fail_counts: dict[tuple[str, str], int] = {}
    for name, type_name, fail_alias in fail_aliases:
        raw = values.get(fail_alias)
        fail_counts[(name, type_name)] = 0 if raw is None else int(raw)
    clock_by_name = {
        name: values.get(clock_aliases[column_index]) is True
        for column_index, name in enumerate(string_names)
    }

    selects: list[Any] = []
    for name in columns:
        if dtypes.get(name) != "string":
            selects.append(F.col(name))
            continue
        has_clock = clock_by_name[name]
        promoted: str | None = None
        for type_name in _CSV_PROMOTE_CANDIDATES:
            if type_name == "timestamp" and not has_clock:
                continue
            if type_name == "date" and has_clock:
                continue
            if fail_counts[(name, type_name)] == 0:
                promoted = type_name
                break
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
