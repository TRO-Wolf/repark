"""Writer region — DataFrameWriter, WriterV2, Stat + write helpers."""

from __future__ import annotations

import contextlib
import logging
import re
import uuid
import warnings
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, overload

from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkAttributeError,
    PySparkException,
    PySparkNotImplementedError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark._idents import escape_sql_single_quotes
from repark.spark._idents import quote_ident as _quote_ident_sql
from repark.spark._temp_views import scratch_view_name
from repark.spark.column import Column
from repark.spark.dataframe.core import (
    DataFrame,
    _by_name_casefold_map,
    _sql_string_literal,
    _warn_writer_v2_option_once,
)
from repark.spark.row import Row
from repark.spark.types import DataType, StructField, StructType

logger = logging.getLogger("repark.spark.dataframe")


def _resolve_writer_table(dataframe: DataFrame, name: str) -> tuple[str, str]:
    """Return the action-time qualified name and quoted SQL reference for a writer target."""
    from repark.spark.catalog import DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME
    from repark.spark.session import _sql_table_ref, resolve_table_name

    token = getattr(dataframe, "_alive_token", {}) or {}
    state = token.get("catalog_state") if isinstance(token, dict) else None
    if not isinstance(state, dict):
        state = {
            "current_catalog": DEFAULT_CATALOG_NAME,
            "current_database": DEFAULT_DATABASE_NAME,
            "known_catalogs": set(),
        }
    known = state.get("known_catalogs") or set()
    if not isinstance(known, set):
        known = set(known)
    qualified = resolve_table_name(
        name,
        current_catalog=str(state.get("current_catalog", DEFAULT_CATALOG_NAME)),
        current_database=str(state.get("current_database", DEFAULT_DATABASE_NAME)),
        known_catalogs=known,
        prefer_temp_view=False,
        default_catalog_is_auto=bool(state.get("auto_default_catalog")),
    )
    return qualified, _sql_table_ref(qualified)


class DataFrameWriter:
    """Build Iceberg table writes and Parquet, CSV, or JSON path writes through SQL.

    Table writes use CTAS or INSERT paths, and creation rejects tightened frames. Path overwrite
    stages and swaps output safely.
    """

    __slots__ = ("_dataframe", "_format", "_mode", "_options", "_partition_columns")

    _VALID_MODES = ("append", "overwrite", "error", "errorifexists", "ignore")
    _PATH_MODES = ("append", "overwrite", "error", "errorifexists", "ignore")
    _PATH_FORMATS = frozenset({"parquet", "csv", "json"})
    _CSV_WRITE_UNSUPPORTED_OPTIONS: frozenset[str] = frozenset(
        {
            "dateformat",
            "timestampformat",
            "timestampntzformat",
            "encoding",
            "linesep",
            "chartoescapequoteescaping",
            "ignoreleadingwhitespace",
            "ignoretrailingwhitespace",
            "maxrecordsperfile",
            "emptyvalue",
        }
    )
    _JSON_WRITE_UNSUPPORTED_OPTIONS: frozenset[str] = frozenset(
        {
            "dateformat",
            "timestampformat",
            "timestampntzformat",
            "encoding",
            "linesep",
            "ignorenullfields",
        }
    )

    def __init__(self, dataframe: DataFrame) -> None:
        """Start with PySpark defaults: iceberg format, ``errorifexists`` mode, no partitioning."""
        self._dataframe = dataframe
        self._format = "iceberg"
        self._mode = "error"
        self._partition_columns: list[str] = []
        self._options: dict[str, str] = {}

    def format(self, source: str) -> DataFrameWriter:
        """Set the format for table or path writes; unsupported formats fail at the action."""
        if not isinstance(source, str):
            raise PySparkTypeError(f"format expects a str, got {type(source).__name__}")
        self._format = source.lower()
        return self

    def option(self, key: str, value: Any) -> DataFrameWriter:
        """Set a single writer option (PySpark ``DataFrameWriter.option``); chains."""
        key_str = str(key)
        for existing in list(self._options):
            if existing.lower() == key_str.lower():
                del self._options[existing]
        self._options[key_str] = str(value)
        return self

    def options(self, **options: Any) -> DataFrameWriter:
        """Set multiple writer options (PySpark ``DataFrameWriter.options``); chains."""
        for key, value in options.items():
            self.option(key, value)
        return self

    def mode(self, save_mode: str) -> DataFrameWriter:
        """Set append, overwrite, error, errorifexists, or ignore mode."""
        if save_mode not in self._VALID_MODES:
            raise AnalysisException(
                f"[INVALID_SAVE_MODE] The specified save mode {save_mode!r} is invalid; "
                f"mode must be one of {self._VALID_MODES}"
            )
        self._mode = save_mode
        return self

    def partitionBy(self, *cols: str | list[str]) -> DataFrameWriter:  # noqa: N802 — PySpark method name
        """Set identity partition columns for CTAS or path writes; duplicates are rejected."""
        columns: list[str] = []
        for item in cols:
            if isinstance(item, (list, tuple)):
                columns.extend(item)
            else:
                columns.append(item)
        seen_casefold: set[str] = set()
        for column in columns:
            key = str(column).casefold()
            if key in seen_casefold:
                raise AnalysisException(
                    f"duplicate partitionBy column {column!r}; "
                    "path/table partitionBy requires unique column names "
                    "(duplicate names nest identical hive dirs)"
                )
            seen_casefold.add(key)
        self._partition_columns = columns
        return self

    partition_by = partitionBy

    def saveAsTable(self, name: str) -> None:  # noqa: N802 — PySpark method name
        """Persist an Iceberg table using CTAS or by-name insert/overwrite semantics.

        Bare names resolve and quote at action time. Existing-table source columns follow target
        order. Empty overwrite validates schema through ``INSERT OVERWRITE`` before wiping.
        """
        self._dataframe._ensure_alive()
        if self._format != "iceberg":
            raise PySparkValueError(
                "repark.write supports only format('iceberg') for saveAsTable, "
                f"got {self._format!r}"
            )
        qualified, table_ref = _resolve_writer_table(self._dataframe, name)
        session = self._dataframe._session
        normalized_mode = "error" if self._mode == "errorifexists" else self._mode
        if not session.table_exists(qualified):
            self._dataframe._refuse_tightened_iceberg_create()
            self._run_through_temp_view(lambda view: self._ctas_sql(table_ref, view=view))
            return
        if normalized_mode == "error":
            raise AnalysisException(
                f"table {name!r} already exists; use mode('append'|'overwrite'|'ignore') to write "
                "into an existing table"
            )
        if normalized_mode == "ignore":
            return
        projection = self._by_name_projection(session, table_ref, display_name=name)
        verb = "INSERT OVERWRITE" if normalized_mode == "overwrite" else "INSERT INTO"
        self._run_through_temp_view(
            lambda view: f"{verb} {table_ref} SELECT {projection} FROM {view}"
        )

    save_as_table = saveAsTable

    def insertInto(self, name: str, overwrite: bool | None = None) -> None:  # noqa: N802 — PySpark method name
        """Insert into an existing Iceberg table by position.

        ``overwrite`` selects whole-table overwrite.
        """
        self._dataframe._ensure_alive()
        if self._format != "iceberg":
            raise PySparkValueError(
                f"repark.write supports only format('iceberg') for insertInto, got {self._format!r}"
            )
        _qualified, table_ref = _resolve_writer_table(self._dataframe, name)
        if overwrite is None:
            overwrite = self._mode == "overwrite"
        verb = "INSERT OVERWRITE" if overwrite else "INSERT INTO"
        self._run_through_temp_view(lambda view: f"{verb} {table_ref} SELECT * FROM {view}")

    insert_into = insertInto

    def parquet(
        self,
        path: str,
        mode: str | None = None,
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark param name
        compression: str | None = None,
    ) -> None:
        """Write Parquet through ``COPY`` with optional partitioning, compression, and path mode."""
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        if mode is not None:
            self.mode(mode)
        self._format = "parquet"
        self.save(path)

    def csv(
        self,
        path: str,
        mode: str | None = None,
        compression: str | None = None,
        sep: str | None = None,
        quote: str | None = None,
        escape: str | None = None,
        header: bool | str | None = None,
        nullValue: str | None = None,  # noqa: N803 — PySpark param name
        quoteAll: bool | str | None = None,  # noqa: N803 — PySpark param name
        escapeQuotes: bool | str | None = None,  # noqa: N803 — PySpark param name
        dateFormat: str | None = None,  # noqa: N803 — PySpark param name
        timestampFormat: str | None = None,  # noqa: N803 — PySpark param name
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark param name
        **extra: Any,
    ) -> None:
        """Write CSV through ``COPY``.

        The shorthand defaults ``header`` to true and refuses unsupported options.
        """
        if mode is not None:
            self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        if sep is not None:
            self.option("sep", sep)
        if quote is not None:
            self.option("quote", quote)
        if escape is not None:
            self.option("escape", escape)
        if header is not None:
            self.option("header", header)
        elif not any(key.lower() == "header" for key in self._options):
            self.option("header", "true")
        if nullValue is not None:
            self.option("nullValue", nullValue)
        if quoteAll is not None:
            self.option("quoteAll", quoteAll)
        if escapeQuotes is not None:
            self.option("escapeQuotes", escapeQuotes)
        if dateFormat is not None:
            self.option("dateFormat", dateFormat)
        if timestampFormat is not None:
            self.option("timestampFormat", timestampFormat)
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)
        self._format = "csv"
        self.save(path)

    def json(
        self,
        path: str,
        mode: str | None = None,
        compression: str | None = None,
        dateFormat: str | None = None,  # noqa: N803 — PySpark param name
        timestampFormat: str | None = None,  # noqa: N803 — PySpark param name
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark param name
        **extra: Any,
    ) -> None:
        """Write newline-delimited JSON through ``COPY`` with configured path mode."""
        if mode is not None:
            self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        if dateFormat is not None:
            self.option("dateFormat", dateFormat)
        if timestampFormat is not None:
            self.option("timestampFormat", timestampFormat)
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)
        self._format = "json"
        self.save(path)

    def save(
        self,
        path: str | None = None,
        format: str | None = None,
        mode: str | None = None,
        partitionBy: str | list[str] | None = None,  # noqa: N803 — PySpark param name
        **options: Any,
    ) -> None:
        """Write to a Parquet, CSV, or JSON path using the configured save mode."""
        self._dataframe._ensure_alive()
        if format is not None:
            self.format(format)
        if mode is not None:
            self.mode(mode)
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        for key, value in options.items():
            if value is not None:
                self.option(key, value)
        if path is None:
            for key, value in self._options.items():
                if key.lower() == "path" and value:
                    path = value
                    break
        if path is None:
            raise AnalysisException("'path' is not specified.")
        if self._format not in self._PATH_FORMATS:
            if self._format == "iceberg":
                raise AnalysisException(
                    "DataFrameWriter.save(path) requires format('parquet'|'csv'|'json'); "
                    "use saveAsTable for Iceberg tables"
                )
            shown = (self._format or "")[:64]
            raise AnalysisException(
                f"DATA_SOURCE_NOT_FOUND: Failed to find the data source: {shown!r}. "
                "repark path writes support format('parquet'|'csv'|'json') via COPY TO "
                "(orc/other formats are not supported)."
            )
        self._apply_path_write(path, stored_as=self._format.upper())

    def _apply_path_write(self, path: str, *, stored_as: str) -> None:
        """Apply a staged ``COPY`` write with path modes and schema checks.

        Failed COPY leaves the destination unchanged; append requires compatible columns and types.
        """
        import shutil

        normalized_mode = "error" if self._mode == "errorifexists" else self._mode
        if normalized_mode not in self._PATH_MODES:
            raise AnalysisException(
                f"path write mode must be one of {self._PATH_MODES}, got {self._mode!r}"
            )
        destination = Path(path)
        if destination.exists() and normalized_mode == "error":
            raise AnalysisException(
                f"[PATH_ALREADY_EXISTS] Path {path} already exists. "
                'Set mode as "overwrite" to overwrite the existing path.'
            )
        if destination.exists() and normalized_mode == "ignore":
            return
        if normalized_mode == "append" and destination.exists():
            if destination.is_file() or (destination.is_symlink() and not destination.is_dir()):
                raise AnalysisException(
                    f"[PATH_ALREADY_EXISTS] Path {path} is a file (or non-directory symlink); "
                    "path mode('append') requires a directory of part files. "
                    'Use mode("overwrite") to replace the path, or write to a directory path.'
                )
            self._validate_path_append_schema(destination, stored_as=stored_as)
        partition_clause = self._partitioned_by_sql_clause()
        staging = destination.parent / (
            f"repark-staging-{uuid.uuid4().hex}-{destination.name or 'out'}"
        )
        escaped_staging = escape_sql_single_quotes(str(staging))
        options_clause = self._copy_options_sql(stored_as)
        self._dataframe._session.note_local_write_root(escaped_staging)
        try:
            self._run_through_temp_view(
                lambda view: (
                    f"COPY (SELECT * FROM {view}) TO '{escaped_staging}' "
                    f"STORED AS {stored_as}{partition_clause}{options_clause}"
                )
            )
            if not staging.exists():
                staging.mkdir(parents=True, exist_ok=True)
            self._materialize_empty_path_write(staging, stored_as=stored_as)
            if normalized_mode == "append" and destination.exists():
                try:
                    _merge_path_write_tree(staging, destination)
                except (FileExistsError, OSError, shutil.Error) as exc:
                    raise AnalysisException(
                        f"path mode('append') failed for {path!r}: {exc}"
                    ) from exc
                if staging.exists():
                    if staging.is_dir():
                        shutil.rmtree(staging)
                    else:
                        staging.unlink()
                return
            if destination.exists():
                if destination.is_symlink():
                    raise AnalysisException(
                        f"cannot overwrite path {path!r}: destination is a symbolic link "
                        "(refuse-loud; repark will not rmtree/unlink a symlink destination)"
                    )
                try:
                    if destination.is_dir():
                        shutil.rmtree(destination)
                    else:
                        destination.unlink()
                except OSError as exc:
                    raise AnalysisException(f"cannot overwrite path {path!r}: {exc}") from exc
            staging.rename(destination)
        except AnalysisException:
            if staging.exists() and destination.exists():
                if staging.is_dir() and not staging.is_symlink():
                    shutil.rmtree(staging)
                elif staging.is_file() or staging.is_symlink():
                    staging.unlink()
            raise
        except Exception:
            if staging.exists() and destination.exists():
                if staging.is_dir() and not staging.is_symlink():
                    shutil.rmtree(staging)
                elif staging.is_file() or staging.is_symlink():
                    staging.unlink()
            raise

    def _partitioned_by_sql_clause(self) -> str:
        """Build the path ``PARTITIONED BY`` clause for configured identity columns."""
        if not self._partition_columns:
            return ""
        frame_columns = list(self._dataframe.columns)
        frame_by_case = {column.casefold(): column for column in frame_columns}
        resolved: list[str] = []
        seen_casefold: set[str] = set()
        for column in self._partition_columns:
            name = str(column)
            matched = frame_by_case.get(name.casefold())
            if matched is None:
                raise AnalysisException(
                    f"partitionBy column {name!r} is not in the DataFrame columns "
                    f"{frame_columns}; path partitionBy requires identity columns present "
                    "on the frame (Spark-shaped)"
                )
            if not matched.isidentifier():
                raise AnalysisException(
                    f"partitionBy column {matched!r} is not a simple SQL identifier; "
                    "repark path partitionBy supports simple column names only"
                )
            key = matched.casefold()
            if key in seen_casefold:
                raise AnalysisException(
                    f"duplicate partitionBy column {matched!r}; "
                    "path partitionBy requires unique column names"
                )
            seen_casefold.add(key)
            resolved.append(matched)
        return " PARTITIONED BY (" + ", ".join(resolved) + ")"

    def _validate_path_append_schema(self, destination: Any, *, stored_as: str) -> None:
        """Reject path append when source and destination schemas cannot be merged safely."""

        destination_path = Path(destination)
        source_table = self._dataframe.limit(0).to_arrow()
        source_names = list(source_table.schema.names)
        partition_casefold = {str(column).casefold() for column in self._partition_columns}
        expected_names = [
            name for name in source_names if name.casefold() not in partition_casefold
        ]
        expected_by_case = {name.casefold(): name for name in expected_names}
        source_type_by_case = {
            field.name.casefold(): field.type
            for field in source_table.schema
            if field.name.casefold() in expected_by_case
        }

        if stored_as == "PARQUET":
            import pyarrow.parquet as pa_pq

            if destination_path.is_file():
                part_files = [destination_path]
            else:
                part_files = [
                    item for item in destination_path.rglob("*.parquet") if item.is_file()
                ]
            if not part_files:
                return
            dest_type_by_case: dict[str, Any] = {}
            dest_name_by_case: dict[str, str] = {}
            for part_file in part_files:
                schema = pa_pq.read_schema(part_file)
                for field in schema:
                    key = field.name.casefold()
                    if key not in dest_type_by_case:
                        dest_type_by_case[key] = field.type
                        dest_name_by_case[key] = field.name
            dest_keys = set(dest_type_by_case)
            expected_keys = set(expected_by_case)
            if dest_keys != expected_keys:
                missing_from_source = sorted(
                    dest_name_by_case[key] for key in dest_keys - expected_keys
                )
                extra_in_source = sorted(expected_by_case[key] for key in expected_keys - dest_keys)
                raise AnalysisException(
                    f"cannot append DataFrame columns {source_names} to path parquet schema "
                    f"{sorted(dest_name_by_case.values())}: column sets differ"
                    + (
                        f"; missing from the DataFrame: {missing_from_source}"
                        if missing_from_source
                        else ""
                    )
                    + (f"; extra in the DataFrame: {extra_in_source}" if extra_in_source else "")
                    + " (path mode('append') refuses silent null-fill / schema drift)"
                )
            for key, expected_type in source_type_by_case.items():
                dest_type = dest_type_by_case[key]
                if dest_type != expected_type:
                    raise AnalysisException(
                        f"cannot append column {expected_by_case[key]!r}: type mismatch "
                        f"source={expected_type} vs path={dest_type} "
                        "(path mode('append') refuses type-incompatible merge)"
                    )
            return

        if stored_as == "CSV":
            if destination_path.is_file():
                candidates = [destination_path]
            else:
                candidates = [item for item in destination_path.rglob("*.csv") if item.is_file()]
            if not candidates:
                return
            header_on = True
            for key, value in self._options.items():
                if key.lower() == "header":
                    header_on = str(value).strip().lower() in {"true", "1", "yes", "t", "y"}
            if not header_on:
                return
            header_line = candidates[0].read_text(encoding="utf-8").splitlines()
            if not header_line:
                return
            separator = ","
            for key, value in self._options.items():
                if key.lower() in {"sep", "delimiter"}:
                    separator = str(value)
            dest_names = [part.strip() for part in header_line[0].split(separator) if part.strip()]
            dest_keys = {name.casefold() for name in dest_names}
            expected_keys = set(expected_by_case)
            if dest_keys != expected_keys:
                raise AnalysisException(
                    f"cannot append DataFrame columns {source_names} to path csv header "
                    f"{dest_names}: column sets differ "
                    "(path mode('append') refuses silent null-fill / schema drift)"
                )
            return

        if stored_as == "JSON":
            import json as json_module

            if destination_path.is_file():
                candidates = [destination_path]
            else:
                candidates = [item for item in destination_path.rglob("*.json") if item.is_file()]
            for part_file in candidates:
                for line in part_file.read_text(encoding="utf-8").splitlines():
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        payload = json_module.loads(stripped)
                    except json_module.JSONDecodeError:
                        break
                    if not isinstance(payload, dict):
                        break
                    dest_keys = {str(key).casefold() for key in payload}
                    expected_keys = set(expected_by_case)
                    if dest_keys != expected_keys:
                        raise AnalysisException(
                            f"cannot append DataFrame columns {source_names} to path json keys "
                            f"{sorted(payload)}: column sets differ "
                            "(path mode('append') refuses silent null-fill / schema drift)"
                        )
                    return
            return

    def _copy_options_sql(self, stored_as: str) -> str:
        """Build format-specific ``COPY`` options or reject unsupported options."""
        if not self._options:
            return ""
        pairs: list[str] = []
        for key, value in self._options.items():
            lowered = key.lower()
            if lowered == "path":
                continue
            if stored_as == "CSV":
                if lowered in self._CSV_WRITE_UNSUPPORTED_OPTIONS:
                    raise AnalysisException(
                        f"DataFrameWriter.csv option {key!r} is not supported yet "
                        "(Spark SimpleDateFormat / encoding / lineSep knobs would silently "
                        "diverge from DataFusion strftime writers if ignored or passed raw — "
                        "refuse-loud; see task/r2-read-formats2-ledger.md)"
                    )
                if lowered in {"header"}:
                    pairs.append(f"'format.has_header' '{_sql_option_escape(value)}'")
                elif lowered in {"sep", "delimiter"}:
                    pairs.append(f"'format.delimiter' '{_sql_option_escape(value)}'")
                elif lowered == "quote":
                    pairs.append(f"'format.quote' '{_sql_option_escape(value)}'")
                elif lowered == "escape":
                    pairs.append(f"'format.escape' '{_sql_option_escape(value)}'")
                elif lowered == "nullvalue":
                    pairs.append(f"'format.null_value' '{_sql_option_escape(value)}'")
                elif lowered == "quoteall":
                    quote_all_on = str(value).strip().lower() in {
                        "true",
                        "1",
                        "yes",
                        "t",
                        "y",
                    }
                    style = "Always" if quote_all_on else "Necessary"
                    pairs.append(f"'format.quote_style' '{style}'")
                elif lowered == "escapequotes":
                    escape_quotes_on = str(value).strip().lower() in {
                        "true",
                        "1",
                        "yes",
                        "t",
                        "y",
                    }
                    pairs.append(f"'format.double_quote' '{str(escape_quotes_on).lower()}'")
                elif lowered == "compression":
                    compression = _sql_option_escape(_normalize_write_compression(value))
                    pairs.append(f"'format.compression' '{compression}'")
                else:
                    raise AnalysisException(
                        f"DataFrameWriter.csv option {key!r} is not supported yet "
                        "(would silently change write semantics if ignored)"
                    )
            elif stored_as == "JSON":
                if lowered in self._JSON_WRITE_UNSUPPORTED_OPTIONS:
                    raise AnalysisException(
                        f"DataFrameWriter.json option {key!r} is not supported yet "
                        "(would silently change write semantics if ignored — refuse-loud; "
                        "see task/r2-read-formats2-ledger.md)"
                    )
                if lowered == "compression":
                    compression = _sql_option_escape(_normalize_write_compression(value))
                    pairs.append(f"'format.compression' '{compression}'")
                else:
                    raise AnalysisException(
                        f"DataFrameWriter.json option {key!r} is not supported yet "
                        "(would silently change write semantics if ignored)"
                    )
            elif stored_as == "PARQUET":
                if lowered == "compression":
                    compression = _sql_option_escape(_normalize_parquet_write_compression(value))
                    pairs.append(f"'format.compression' '{compression}'")
                else:
                    raise AnalysisException(
                        f"DataFrameWriter.parquet/save option {key!r} is not supported yet "
                        "(would silently change write semantics if ignored)"
                    )
        if not pairs:
            return ""
        return " OPTIONS (" + ", ".join(pairs) + ")"

    def _materialize_empty_path_write(self, staging: Any, *, stored_as: str) -> None:
        """Create a schema-carrying part file when empty ``COPY`` creates no output."""

        staging_path = Path(staging)
        if not staging_path.is_dir():
            return
        if stored_as == "PARQUET":
            if any(staging_path.rglob("*.parquet")):
                return
            if self._partition_columns and any(staging_path.iterdir()):
                return
            import pyarrow.parquet as pa_pq

            empty_table = self._dataframe.limit(0).to_arrow()
            pa_pq.write_table(empty_table, staging_path / "part-00000.parquet")
            return
        if stored_as == "CSV":
            if any(staging_path.rglob("*.csv")) or any(staging_path.iterdir()):
                return
            header_on = True
            separator = ","
            for key, value in self._options.items():
                lowered = key.lower()
                if lowered == "header":
                    header_on = str(value).strip().lower() in {"true", "1", "yes", "t", "y"}
                elif lowered in {"sep", "delimiter"}:
                    separator = str(value)
            columns = list(self._dataframe.columns)
            content = (separator.join(columns) + "\n") if header_on and columns else ""
            (staging_path / "part-00000.csv").write_text(content, encoding="utf-8")
            return
        if stored_as == "JSON":
            if any(staging_path.rglob("*.json")) or any(staging_path.iterdir()):
                return
            (staging_path / "part-00000.json").write_text("", encoding="utf-8")
            return

    def _by_name_projection(self, session: Any, table_ref: str, *, display_name: str) -> str:
        """Return a quoted source projection in target-table order, rejecting schema drift."""
        from repark.spark._idents import quote_ident as _quote_ident

        target_columns = list(session.sql(f"SELECT * FROM {table_ref} LIMIT 0").column_names())
        source_columns = self._dataframe.columns
        source_by_case = _by_name_casefold_map(source_columns, surface="DataFrame")
        target_by_case = _by_name_casefold_map(target_columns, surface="table")
        missing = [column for column in target_columns if column.casefold() not in source_by_case]
        extra = [column for column in source_columns if column.casefold() not in target_by_case]
        if missing or extra:
            detail = ""
            if missing:
                detail += f"; missing from the DataFrame: {missing}"
            if extra:
                detail += f"; extra in the DataFrame: {extra}"
            raise AnalysisException(
                f"cannot write DataFrame columns {source_columns} into table {display_name!r} "
                f"columns {target_columns} by name for saveAsTable{detail}"
            )
        return ", ".join(
            _quote_ident(source_by_case[column.casefold()]) for column in target_columns
        )

    def _ctas_sql(self, table_ref: str, *, view: str) -> str:
        """Build a quoted Iceberg CTAS statement."""
        from repark.spark._idents import quote_ident as _quote_ident

        partition_clause = ""
        if self._partition_columns:
            quoted_parts = ", ".join(_quote_ident(column) for column in self._partition_columns)
            partition_clause = f" PARTITIONED BY ({quoted_parts})"
        return f"CREATE TABLE {table_ref} USING iceberg{partition_clause} AS SELECT * FROM {view}"

    def _run_through_temp_view(self, build_sql: Callable[[str], str]) -> None:
        """Register a temporary view, execute the generated write SQL, and drop the view."""
        self._dataframe._ensure_alive()
        session = self._dataframe._session
        view_name = scratch_view_name(session, "_repark_writer_")
        session.create_or_replace_temp_view(view_name, self._dataframe._native_for_registration())
        try:
            session.sql(build_sql(view_name))
        finally:
            session.drop_temp_view(view_name)


def _sql_option_escape(value: str) -> str:
    """Escape a single-quoted COPY OPTIONS value."""
    return escape_sql_single_quotes(str(value))


def _normalize_write_compression(raw: str) -> str:
    """Map Spark compression names to DataFusion CSV or JSON compression tokens."""
    lowered = str(raw).strip().lower()
    if lowered in {"", "none", "uncompressed"}:
        return "uncompressed"
    if lowered in {"gzip", "gz"}:
        return "gzip"
    if lowered in {"bzip2", "bz2"}:
        return "bzip2"
    if lowered == "xz":
        return "xz"
    if lowered in {"zstd", "zst"}:
        return "zstd"
    raise AnalysisException(
        f"unsupported write compression {raw!r}; "
        "repark supports gzip, bzip2, xz, zstd, none/uncompressed"
    )


def _normalize_parquet_write_compression(raw: str) -> str:
    """Map Spark Parquet compression names to DataFusion tokens."""
    lowered = str(raw).strip().lower()
    if lowered in {"", "none", "uncompressed"}:
        return "uncompressed"
    if lowered == "snappy":
        return "snappy"
    if lowered in {"gzip", "gz"}:
        return "gzip(6)"
    if lowered in {"zstd", "zst"}:
        return "zstd(3)"
    if lowered == "lz4":
        return "lz4"
    if lowered.startswith("gzip(") or lowered.startswith("zstd(") or lowered.startswith("brotli("):
        return lowered
    raise AnalysisException(
        f"unsupported parquet write compression {raw!r}; "
        "repark supports snappy, gzip, zstd, lz4, none/uncompressed"
    )


def _merge_path_write_tree(staging: Any, destination: Any) -> None:
    """Merge a staged COPY tree into a destination without replacing existing parts."""
    import shutil

    staging_path = Path(staging)
    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)
    if not staging_path.is_dir():
        target = destination_path / staging_path.name
        if target.exists():
            target = destination_path / f"part-append-{uuid.uuid4().hex[:12]}{staging_path.suffix}"
        shutil.move(str(staging_path), str(target))
        return
    for item in staging_path.iterdir():
        target = destination_path / item.name
        if item.is_dir():
            _merge_path_write_tree(item, target)
            continue
        if target.exists():
            target = destination_path / f"part-append-{uuid.uuid4().hex[:12]}{item.suffix}"
        shutil.move(str(item), str(target))


class DataFrameWriterV2:
    """Build V2 Iceberg CTAS, append, and partition-transform writes.

    V2 append is by name. Dynamic and conditional overwrite are loud unsupported errors.
    """

    __slots__ = (
        "_dataframe",
        "_partition_exprs",
        "_properties",
        "_provider",
        "_table",
    )

    def __init__(self, dataframe: DataFrame, table: str) -> None:
        """Bind a source DataFrame and validate its target name; resolution occurs per action."""
        self._dataframe = dataframe
        self._table = table
        _resolve_writer_table(dataframe, table)
        self._provider = "iceberg"
        self._properties: dict[str, str] = {}
        self._partition_exprs: list[str] = []

    def _resolved_table(self) -> tuple[str, str]:
        """Qualify + quote under the session's **current** catalog/NS (action-time)."""
        return _resolve_writer_table(self._dataframe, self._table)

    def using(self, provider: str) -> DataFrameWriterV2:
        """Set the table provider; only ``iceberg`` is supported."""
        if not isinstance(provider, str):
            raise PySparkTypeError(f"using provider must be str, got {type(provider).__name__}")
        if provider.lower() != "iceberg":
            raise PySparkValueError(
                f"repark.writeTo supports only using('iceberg'), got {provider!r}"
            )
        self._provider = provider.lower()
        return self

    def tableProperty(  # noqa: N802 — PySpark method name
        self,
        property: str,
        value: str,
    ) -> DataFrameWriterV2:
        """Set a table property used by V2 create and replace operations."""
        if not isinstance(property, str) or not isinstance(value, str):
            raise PySparkTypeError("tableProperty expects (str, str) key and value")
        self._properties[property] = value
        return self

    table_property = tableProperty

    def partitionedBy(  # noqa: N802 — PySpark method name
        self,
        col: Column | str,
        *cols: Column | str,
    ) -> DataFrameWriterV2:
        """Set identity columns or supported partition transforms for CTAS."""
        for item in (col, *cols):
            self._partition_exprs.append(self._partition_sql_fragment(item))
        return self

    partitioned_by = partitionedBy

    def create(self) -> None:
        """Create the table, failing if it already exists and rejecting tightened frames."""
        self._dataframe._ensure_alive()
        session = self._dataframe._session
        qualified, _table_ref = self._resolved_table()
        if session.table_exists(qualified):
            raise AnalysisException(
                f"[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view {self._table!r} "
                "because it already exists. Choose a different name, drop or replace the existing "
                "object, or use createOrReplace()."
            )
        self._dataframe._refuse_tightened_iceberg_create()
        self._run_ctas(or_replace=False)

    def createOrReplace(self) -> None:  # noqa: N802 — PySpark method name
        """Create or replace the table, rejecting tightened frames."""
        self._dataframe._ensure_alive()
        self._dataframe._refuse_tightened_iceberg_create()
        self._run_ctas(or_replace=True)

    create_or_replace = createOrReplace

    def replace(self) -> None:
        """Replace an existing table, failing if it does not exist or the frame is tightened."""
        self._dataframe._ensure_alive()
        session = self._dataframe._session
        qualified, _table_ref = self._resolved_table()
        if not session.table_exists(qualified):
            raise AnalysisException(
                f"[TABLE_OR_VIEW_NOT_FOUND] Cannot replace table {self._table!r} because it does "
                "not exist. Use create() or createOrReplace() to create it."
            )
        self._dataframe._refuse_tightened_iceberg_create()
        self._run_ctas(or_replace=True)

    def append(self) -> None:
        """Append rows to an existing table by column name."""
        self._dataframe._ensure_alive()
        session = self._dataframe._session
        qualified, table_ref = self._resolved_table()
        if not session.table_exists(qualified):
            raise AnalysisException(
                f"Cannot append into table {self._table!r} because it does not exist. "
                "Use create() or createOrReplace() first."
            )
        projection = self._by_name_projection(session, table_ref=table_ref)
        self._run_through_temp_view(
            lambda view: f"INSERT INTO {table_ref} SELECT {projection} FROM {view}"
        )

    def overwritePartitions(self) -> None:  # noqa: N802 — PySpark method name
        """Reject dynamic partition overwrite because static overwrite could lose unrelated rows."""

        raise UnsupportedOperationException(
            "overwritePartitions: Spark's dynamic partition overwrite (partition-scoped "
            "replace) is not supported by the repark engine yet — a static INSERT OVERWRITE "
            "would silently replace ALL rows, not just the source's partitions. Use "
            "createOrReplace() for a deliberate full rebuild, or append(). "
            "(Engine path: iceberg-rust fork ReplacePartitions, not yet wired.)"
        )

    overwrite_partitions = overwritePartitions

    def overwrite(self, condition: Column | str) -> None:
        """Reject conditional overwrite because no engine path supports it."""
        _ = condition  # signature parity

        raise UnsupportedOperationException(
            "DataFrameWriterV2.overwrite(condition) is not supported — no engine path for "
            "conditional overwrite (Group I disclosure). Use createOrReplace() for a "
            "deliberate full rebuild, or DELETE + append."
        )

    def option(self, key: str, value: Any) -> DataFrameWriterV2:
        """Set an option; branch and tag writes are rejected, others warn once."""
        _ = value
        key_lower = str(key).lower()
        if key_lower in {"branch", "tag"}:
            raise UnsupportedOperationException(
                f"writing to an Iceberg {key_lower} is not supported — "
                "repark write path is current-snapshot only (I1 / R-TIME-TRAVEL)"
            )
        _warn_writer_v2_option_once(stacklevel=2)
        return self

    def options(self, **options: Any) -> DataFrameWriterV2:
        """Set multiple options using the same rules as :meth:`option`."""
        for key, value in options.items():
            self.option(key, value)
        return self

    @staticmethod
    def _partition_sql_fragment(item: Column | str) -> str:
        """Render one partition expression, quoting identity names."""
        from repark.spark._idents import quote_ident as _quote_ident

        if isinstance(item, str):
            return _quote_ident(item)
        if isinstance(item, Column):
            transform = getattr(item, "_partition_transform", None)
            if transform is not None:
                return transform
            return _quote_ident(item.spark_display_part())
        raise PySparkTypeError(
            f"partitionedBy expects column names (str) or Column, got {type(item).__name__}"
        )

    def _by_name_projection(self, session: Any, *, table_ref: str | None = None) -> str:
        """Return a quoted source projection in table order, rejecting schema drift."""
        from repark.spark._idents import quote_ident as _quote_ident

        name = self._table
        if table_ref is None:
            _qualified, table_ref = self._resolved_table()
        target_columns = list(session.sql(f"SELECT * FROM {table_ref} LIMIT 0").column_names())
        source_columns = self._dataframe.columns
        source_by_case = _by_name_casefold_map(source_columns, surface="DataFrame")
        target_by_case = _by_name_casefold_map(target_columns, surface="table")
        missing = [column for column in target_columns if column.casefold() not in source_by_case]
        extra = [column for column in source_columns if column.casefold() not in target_by_case]
        if missing or extra:
            detail = ""
            if missing:
                detail += f"; missing from the DataFrame: {missing}"
            if extra:
                detail += f"; extra in the DataFrame: {extra}"
            raise AnalysisException(
                f"cannot write DataFrame columns {source_columns} into table {name!r} columns "
                f"{target_columns} by name for writeTo{detail}"
            )
        return ", ".join(
            _quote_ident(source_by_case[column.casefold()]) for column in target_columns
        )

    def _ctas_sql(self, *, or_replace: bool, view: str) -> str:
        """Build ``CREATE [OR REPLACE] TABLE … USING iceberg … AS SELECT``."""
        if self._provider != "iceberg":
            raise PySparkValueError(
                f"repark.writeTo supports only using('iceberg'), got {self._provider!r}"
            )
        _qualified, table_ref = self._resolved_table()
        verb = "CREATE OR REPLACE TABLE" if or_replace else "CREATE TABLE"
        partition_clause = ""
        if self._partition_exprs:
            partition_clause = f" PARTITIONED BY ({', '.join(self._partition_exprs)})"
        properties_clause = ""
        if self._properties:
            pairs = ", ".join(
                f"{_sql_string_literal(key)}={_sql_string_literal(value)}"
                for key, value in self._properties.items()
            )
            properties_clause = f" TBLPROPERTIES ({pairs})"
        return (
            f"{verb} {table_ref} USING iceberg{partition_clause}{properties_clause} "
            f"AS SELECT * FROM {view}"
        )

    def _run_ctas(self, *, or_replace: bool) -> None:
        """Execute the CTAS / CREATE OR REPLACE path through a throwaway temp view."""
        self._run_through_temp_view(lambda view: self._ctas_sql(or_replace=or_replace, view=view))

    def _run_through_temp_view(self, build_sql: Callable[[str], str]) -> None:
        """Register the DataFrame as a temp view, run ``build_sql(view_name)``, drop the view.

        Builds SQL from the bound view name after registration so user-controlled path /
        ``tableProperty`` text cannot be reinterpreted by ``str.format``.
        Materializes pending mapInArrow / cache first.
        """
        self._dataframe._ensure_alive()
        session = self._dataframe._session
        view_name = scratch_view_name(session, "_repark_writer_v2_")
        session.create_or_replace_temp_view(view_name, self._dataframe._native_for_registration())
        try:
            session.sql(build_sql(view_name))
        finally:
            session.drop_temp_view(view_name)


class DataFrameStatFunctions:
    """Expose PySpark ``DataFrame.stat`` methods through the owning DataFrame."""

    __slots__ = ("_dataframe",)

    def __init__(self, dataframe: DataFrame) -> None:
        self._dataframe = dataframe

    def approxQuantile(  # noqa: N802
        self,
        col: str | list[str] | tuple[str, ...],
        probabilities: list[float] | tuple[float, ...],
        relativeError: float,  # noqa: N803
    ) -> list[float] | list[list[float]]:
        """Return approximate quantiles by delegating to the DataFrame."""
        return self._dataframe.approxQuantile(col, probabilities, relativeError)

    def corr(self, col1: str, col2: str, method: str | None = None) -> float:
        """Return Pearson correlation by delegating to the DataFrame."""
        return self._dataframe.corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        """Return sample covariance by delegating to the DataFrame."""
        return self._dataframe.cov(col1, col2)

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        """Return a pair-wise frequency table by delegating to the DataFrame."""
        return self._dataframe.crosstab(col1, col2)

    def freqItems(self, cols: list[str], support: float | None = None) -> DataFrame:  # noqa: N802
        """Reject frequent-item discovery because it is not implemented."""
        del cols, support
        raise UnsupportedOperationException(
            "DataFrame.stat.freqItems is not supported yet (disclosed R-DF-BATCH2)"
        )

    def sampleBy(  # noqa: N802 — PySpark camelCase
        self,
        col: Column | str,
        fractions: dict[Any, float],
        seed: int | None = None,
    ) -> DataFrame:
        """Return a stratified sample by delegating to the DataFrame."""
        return self._dataframe.sampleBy(col, fractions, seed)
