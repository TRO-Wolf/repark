"""Writer region — DataFrameWriter, WriterV2, Stat + write helpers (r27 T0; technique A)."""

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
    """Qualify then quote a writer target table under session catalog state (E2).

    Returns ``(qualified_unquoted, quoted_sql_ref)``. Shared by V1/V2 writers so bare
    ``saveAsTable("t")`` / ``writeTo("t")`` land in ``currentCatalog.currentDatabase.t``.
    """
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
    """The write surface (PySpark ``DataFrame.write``).

    A small builder that routes writes through the engine's **existing sanctioned SQL paths** — CTAS
    (``CREATE TABLE … USING iceberg AS SELECT``), ``INSERT INTO``, ``INSERT OVERWRITE``, and path
    ``COPY … TO … STORED AS PARQUET|CSV|JSON`` — via a throwaway temp view. It adds **no** commit or
    transaction machinery of its own. Table writes support only Iceberg; path writes support
    Parquet / CSV / JSON (R1 + R2 option/mode/partitionBy honesty).
    """

    # === r20 R1: read-formats ===
    # === r22 R2: writer option matrix / path modes / partitionBy ===
    __slots__ = ("_dataframe", "_format", "_mode", "_options", "_partition_columns")

    _VALID_MODES = ("append", "overwrite", "error", "errorifexists", "ignore")
    # Path modes: overwrite/error/errorifexists (R1) + append/ignore (R2 pure-Python).
    _PATH_MODES = ("append", "overwrite", "error", "errorifexists", "ignore")
    _PATH_FORMATS = frozenset({"parquet", "csv", "json"})
    # Writer options that would silently mis-format if ignored or passed raw to DataFusion
    # (strftime tokens) — refuse-loud. Full matrix in task/r2-read-formats2-ledger.md.
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
        # === r20 R1: read-formats ===
        self._options: dict[str, str] = {}

    def format(self, source: str) -> DataFrameWriter:
        """Set the write format (PySpark ``DataFrameWriter.format``).

        ``iceberg`` is accepted for :meth:`saveAsTable` / :meth:`insertInto`; ``parquet`` /
        ``csv`` / ``json`` for path :meth:`save` / shorthand methods. Other formats are rejected
        at the action that needs them (not silently mis-written).
        """
        if not isinstance(source, str):
            raise PySparkTypeError(f"format expects a str, got {type(source).__name__}")
        self._format = source.lower()
        return self

    # === r20 R1: read-formats ===
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
        """Set the save mode (PySpark ``DataFrameWriter.mode``):
        ``append`` / ``overwrite`` / ``error`` / ``errorifexists`` / ``ignore``.

        An unrecognized mode raises :class:`~repark.errors.AnalysisException` — Group X live
        oracle: Spark 4.0 rejects this JVM-side with ``[INVALID_SAVE_MODE]``, an
        ``AnalysisException`` (it is NOT Python-side argument validation, so it is deliberately
        not one of the ``PySpark*Error`` wrappers). Matches the sibling path-write mode check
        below, which already raises ``AnalysisException``.
        """
        if save_mode not in self._VALID_MODES:
            raise AnalysisException(
                f"[INVALID_SAVE_MODE] The specified save mode {save_mode!r} is invalid; "
                f"mode must be one of {self._VALID_MODES}"
            )
        self._mode = save_mode
        return self

    def partitionBy(self, *cols: str | list[str]) -> DataFrameWriter:  # noqa: N802 — PySpark method name
        """Set identity partition columns for CTAS (PySpark ``DataFrameWriter.partitionBy``).

        Only identity partitioning is supported (the existing CTAS partition surface). Accepts
        varargs or a single list. Duplicate column names are refused (nested ``col=v/col=v/``
        would be silently surprising — R2 octo C3-002).
        """
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

    # PySpark spells these camelCase; keep snake_case too for repark-convention consistency.
    partition_by = partitionBy

    def saveAsTable(self, name: str) -> None:  # noqa: N802 — PySpark method name
        """Persist the DataFrame as an Iceberg table (PySpark ``DataFrameWriter.saveAsTable``).

        Creates the table (CTAS) when it does not exist; when it does, the mode decides: ``error`` /
        ``errorifexists`` raises, ``ignore`` is a no-op, ``append`` runs ``INSERT INTO``, and
        ``overwrite`` runs ``INSERT OVERWRITE``. Into an **existing** table, columns resolve **by
        NAME** — unlike positional :meth:`insertInto` — per the PySpark ``saveAsTable`` contract:
        the source is projected in the target table's column order, so a reordered same-named frame
        lands correctly instead of silently transposing. An extra or missing source column (the
        column *sets* differ) raises an :class:`~repark.errors.AnalysisException` (Spark parity —
        never a silent drop).

        Bare / two-part names expand under the session default catalog + namespace (E2 shared
        resolution). Table names are validated and quoted so SQL fragments cannot be injected
        through the identifier (C1-SEC-001).
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
        # Empty overwrite uses the INSERT OVERWRITE SQL path (engine probes + schema-validates
        # then provider-wipes — C1-Q-001 / C5-Q-001 / audit BUG-003). Do not short-circuit to bare
        # DELETE: that skipped plan-time column checks and mismatched MoR isolation/shape.
        # by_name_projection already rejected set mismatches; the engine pin covers residual.
        verb = "INSERT OVERWRITE" if normalized_mode == "overwrite" else "INSERT INTO"
        self._run_through_temp_view(
            lambda view: f"{verb} {table_ref} SELECT {projection} FROM {view}"
        )

    save_as_table = saveAsTable

    def insertInto(self, name: str, overwrite: bool | None = None) -> None:  # noqa: N802 — PySpark method name
        """Insert into an existing table by **position** (PySpark ``DataFrameWriter.insertInto``).

        Column names and ``partitionBy`` are ignored (position-based, like Spark). ``overwrite``
        (or ``mode('overwrite')``) selects ``INSERT OVERWRITE`` over ``INSERT INTO``. The target
        table must already exist (the engine raises otherwise).

        Bare / two-part names expand under the session default catalog + namespace (E2). Table
        names are validated and quoted so SQL fragments cannot be injected (C1-SEC-001).

        Fails if the owning session was stopped (octo r3 C2-L-001).
        """
        self._dataframe._ensure_alive()
        if self._format != "iceberg":
            raise PySparkValueError(
                f"repark.write supports only format('iceberg') for insertInto, got {self._format!r}"
            )
        _qualified, table_ref = _resolve_writer_table(self._dataframe, name)
        if overwrite is None:
            overwrite = self._mode == "overwrite"
        # Empty overwrite routes through INSERT OVERWRITE SQL (not a bare DELETE) so the
        # repark-sql empty-OW intercept can schema-validate before wipe (C5-Q-001 — an empty
        # frame with the wrong column count must not wipe). Non-empty uses the same verb path.
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
        """Write Parquet to a path (PySpark ``DataFrameWriter.parquet``).

        Routes through :meth:`_apply_path_write` → ``COPY … STORED AS PARQUET``. ``mode`` defaults
        to the writer's mode. **Shape disclosure:** Spark writes ``part-*.parquet`` + ``_SUCCESS``;
        repark's COPY TO writes a directory of engine-named ``*.parquet`` files (no ``_SUCCESS``).
        Values round-trip via ``spark.read.parquet``. ``partitionBy`` is wired via DataFusion
        ``COPY … PARTITIONED BY`` (hive-style dirs; R2). ``compression`` maps to
        ``format.compression`` (snappy / gzip / zstd / lz4 / none).
        """
        if partitionBy is not None:
            self.partitionBy(partitionBy)
        if compression is not None:
            self.option("compression", compression)
        if mode is not None:
            self.mode(mode)
        self._format = "parquet"
        self.save(path)

    # === r20 R1: read-formats ===
    # === r22 R2: writer option matrix / path modes / partitionBy ===
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
        """Write CSV to a path (PySpark ``DataFrameWriter.csv``).

        Routes through :meth:`_apply_path_write` → ``COPY … STORED AS CSV`` with Spark-ish
        options (``header`` default true on this shorthand). R2 wires ``quoteAll`` /
        ``escapeQuotes``; ``dateFormat`` / ``timestampFormat`` refuse-loud (strftime vs
        SimpleDateFormat mismatch — see ledger).
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
            # Spark write.csv default header=true
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
        """Write JSON (NDJSON) to a path (PySpark ``DataFrameWriter.json``).

        Routes through :meth:`_apply_path_write` → ``COPY … STORED AS JSON``.
        """
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
        """Write to a path (PySpark ``DataFrameWriter.save``).

        Supported path formats: ``parquet`` / ``csv`` / ``json`` (R1) via :meth:`_apply_path_write`
        and ``COPY TO … STORED AS …``. ``path`` is required. ``partitionBy`` wires hive-style
        dirs via DataFusion ``PARTITIONED BY`` (R2). Path modes: overwrite / append / error /
        errorifexists / ignore.
        """
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
            # Spark also accepts path via option("path"); honor it for builder parity.
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
            # Loud unsupported with DATA_SOURCE_NOT_FOUND shape (E1 machinery / E2 format forms).
            shown = (self._format or "")[:64]
            raise AnalysisException(
                f"DATA_SOURCE_NOT_FOUND: Failed to find the data source: {shown!r}. "
                "repark path writes support format('parquet'|'csv'|'json') via COPY TO "
                "(orc/other formats are not supported)."
            )
        self._apply_path_write(path, stored_as=self._format.upper())

    def _apply_path_write(self, path: str, *, stored_as: str) -> None:
        """Apply ``COPY TO … STORED AS <format>`` with Spark-ish path mode semantics.

        # === r20 R1: read-formats ===
        # === r22 R2: append/ignore + PARTITIONED BY ===
        Shared staging machinery for parquet / csv / json (do not fork per format). Overwrite is
        **stage-then-swap**: COPY lands in a sibling staging path first; only after staging is
        known to exist is the prior destination removed and the staging path renamed into place.
        A failed COPY therefore never destroys an existing path (Critic C1-Q-003 / C2-S-002).
        Empty frames: DataFusion may succeed without materializing the path — we create an empty
        staging directory and a schema-carrying part file so empty overwrite cannot wipe prior
        data then fail the rename (octo r1 C1-Q-001 / DF54 empty-write lesson).

        Path modes (R2 oracle): ``error``/``errorifexists`` raise if path exists; ``ignore`` is a
        no-op when it exists; ``append`` merges staged part files into an existing tree (unique
        names on collision) after **schema column-set + type checks** (refuse-loud; R2 octo);
        ``overwrite`` replaces. ``partitionBy`` becomes ``COPY … PARTITIONED BY (cols)``
        (hive-style dirs; partition cols omitted from data files — Spark shape). Hive partition
        *discovery on read* is residual (not R2). OS path failures (file-vs-dir append, symlink
        overwrite) surface as :class:`~repark.errors.AnalysisException`, not raw OSError.
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
                # Plain file (or non-dir symlink): mkdir/merge would raise FileExistsError.
                raise AnalysisException(
                    f"[PATH_ALREADY_EXISTS] Path {path} is a file (or non-directory symlink); "
                    "path mode('append') requires a directory of part files. "
                    'Use mode("overwrite") to replace the path, or write to a directory path.'
                )
            # Schema honesty before COPY (C2-001 / C7-002): refuse col-set or type mismatch.
            self._validate_path_append_schema(destination, stored_as=stored_as)
        partition_clause = self._partitioned_by_sql_clause()
        # Stage beside the destination so braces/`{view}` in the user path cannot collide with a
        # template placeholder, and so a failed COPY cannot wipe existing data.
        # Do **not** prefix with '.' — DataFusion COPY TO treats dot-prefixed targets as a single
        # file rather than a Spark-style directory of part files.
        staging = destination.parent / (
            f"repark-staging-{uuid.uuid4().hex}-{destination.name or 'out'}"
        )
        escaped_staging = escape_sql_single_quotes(str(staging))
        options_clause = self._copy_options_sql(stored_as)
        # SEC-02: the generated COPY TO runs through the ordinary SQL path, which carries the
        # local-filesystem DDL gate. That gate is scoped to *free* SQL (SECURITY.md "Input
        # surfaces"); a typed `df.write.<fmt>(path)` is the caller naming their own destination.
        # Trust the uuid-unique staging target ONLY — never `destination.parent`, which would
        # trust every sibling path (writing to `/tmp/out` would open all of `/tmp` to free SQL).
        # The destination itself is reached by plain filesystem ops below, not by SQL.
        self._dataframe._session.note_local_write_root(escaped_staging)
        try:
            self._run_through_temp_view(
                lambda view: (
                    f"COPY (SELECT * FROM {view}) TO '{escaped_staging}' "
                    f"STORED AS {stored_as}{partition_clause}{options_clause}"
                )
            )
            # Never mutate the destination until staging is present. Empty COPY can report success
            # without creating a path — materialize an empty directory so the swap is still safe.
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
                # Symlink dest: rmtree raises OSError ("Cannot call rmtree on a symbolic link").
                # Refuse loud as AnalysisException (R2 octo C2-003).
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
            # If the destination was already removed but rename failed, leave staging for recovery.
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
        """Build `` PARTITIONED BY (c1, c2)`` for path COPY when ``partitionBy`` was set (R2).

        Columns must be simple frame column names (identity partitions). Unknown columns fail
        loud. Duplicate partition columns are refused (already gated in :meth:`partitionBy`;
        re-checked here for save-time kwarg paths). DataFusion emits hive-style ``col=value/``
        dirs and omits partition cols from data files (Spark shape).
        """
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
            # DataFusion PARTITIONED BY expects bare identifiers; reject anything that would
            # need quoting (spaces / punctuation) rather than invent SQL-escape rules.
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
        """Refuse path ``append`` when source columns/types cannot honest-merge with dest (R2).

        Spark path/table append rejects schema drift; repark previously only merged filesystem
        trees (null-fill / Arrow merge fail on read — octo C2-001 / C7-002). Parquet: compare
        on-disk part-file schemas (partition columns omitted from data files when
        ``partitionBy`` was used). CSV: header row column-set when present. JSON: first-object
        keys when a non-empty part exists.
        """

        destination_path = Path(destination)
        source_table = self._dataframe.limit(0).to_arrow()
        source_names = list(source_table.schema.names)
        partition_casefold = {str(column).casefold() for column in self._partition_columns}
        # On-disk data schema omits partition columns under PARTITIONED BY (Spark shape).
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
                    # Prefer non-null-only / first-seen type; conflict among dest parts is rare.
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
                # Without headers we cannot honest-compare column sets; skip (type/name residual).
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
            # Best-effort: first non-empty NDJSON object keys (when present).
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
        """Build ``OPTIONS (…)`` for COPY TO from writer options (format-specific keys)."""
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
                    # Spark quoteAll=true → quote every field; DF quote_style Always/Necessary.
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
                    # Best-effort: true → Arrow double_quote (RFC "" escaping); false → escape char.
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
                    # Unknown option: Spark tolerates many; repark fails loud for path writes so
                    # we do not silently drop compression/sep under an alias.
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
        """Ensure empty COPY leaves a schema-carrying part file (DF54 empty-write lesson).

        # === r22 R2: writer option matrix / path modes / partitionBy ===
        **Parquet + partitionBy (octo F-R2-C3-001):** DataFusion ``COPY … PARTITIONED BY``
        places data only under hive ``col=value/`` children. A **non-recursive** root
        ``glob("*.parquet")`` missed those children and injected an empty root
        ``part-00000.parquet`` with the **full** frame schema (partition cols included).
        ``spark.read.parquet(root)`` then merged schemas and null-filled every data row's
        partition keys — silent wrong. Fix: treat any nested ``*.parquet`` (``rglob``) or
        existing hive-style children as non-empty and skip the root empty materialize.
        """

        staging_path = Path(staging)
        if not staging_path.is_dir():
            return
        if stored_as == "PARQUET":
            # Nested hive part files count as non-empty (partitionBy COPY layout).
            if any(staging_path.rglob("*.parquet")):
                return
            # Hive-style dirs present (col=value) without files yet — still do not invent a
            # full-schema root part that would poison schema merge on read.
            if self._partition_columns and any(staging_path.iterdir()):
                return
            import pyarrow.parquet as pa_pq

            empty_table = self._dataframe.limit(0).to_arrow()
            pa_pq.write_table(empty_table, staging_path / "part-00000.parquet")
            return
        if stored_as == "CSV":
            if any(staging_path.rglob("*.csv")) or any(staging_path.iterdir()):
                return
            # Schema-only header row when header=true; empty file otherwise (zero data rows).
            # Honor sep/delimiter (octo R1-C1-003) — non-empty COPY already does via OPTIONS.
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
            # Empty NDJSON file — zero rows, schema recovered on read only with user schema.
            (staging_path / "part-00000.json").write_text("", encoding="utf-8")
            return

    def _by_name_projection(self, session: Any, table_ref: str, *, display_name: str) -> str:
        """Project the source columns onto the existing table's columns **by name**.

        Returns the quoted column list, in the target table's column order, for the
        ``INSERT INTO … SELECT <projection> FROM {view}`` — the Spark ``saveAsTable``
        append/overwrite contract (unlike positional ``insertInto``). Reads the target columns from
        the engine's analyzed schema (``SELECT * … LIMIT 0`` — metadata only). Raises
        :class:`~repark.errors.AnalysisException` when the two frames' column *sets* differ (an
        extra or missing source column), rather than silently dropping data or transposing.

        ``table_ref`` must already be a validated, quoted SQL identifier
        (:func:`repark.session._sql_table_ref`); ``display_name`` is the user-facing name in errors.
        """
        from repark.spark._idents import quote_ident as _quote_ident

        target_columns = list(session.sql(f"SELECT * FROM {table_ref} LIMIT 0").column_names())
        source_columns = self._dataframe.columns
        # Spark default caseSensitive=false: match by casefold, project source names in target
        # order so INSERT … SELECT is positional under the target schema (audit BUG-007).
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
        """Build the ``CREATE TABLE … USING iceberg [PARTITIONED BY …] AS SELECT`` statement.

        ``table_ref`` must already be a validated, quoted SQL identifier. Partition column names
        are quoted via :func:`repark._idents.quote_ident` (C1-SEC-001).
        """
        from repark.spark._idents import quote_ident as _quote_ident

        partition_clause = ""
        if self._partition_columns:
            quoted_parts = ", ".join(_quote_ident(column) for column in self._partition_columns)
            partition_clause = f" PARTITIONED BY ({quoted_parts})"
        return f"CREATE TABLE {table_ref} USING iceberg{partition_clause} AS SELECT * FROM {view}"

    def _run_through_temp_view(self, build_sql: Callable[[str], str]) -> None:
        """Register the DataFrame as a unique temp view, run SQL built from the view name, drop it.

        ``build_sql(view_name)`` must embed the view identifier by ordinary string formatting of the
        bound argument — **never** ``str.format`` over a template that already contains
        user-controlled path / property text (braces in those strings must not be interpreted).
        Routes the write through the engine's eager SQL path — no new commit machinery.
        """
        self._dataframe._ensure_alive()
        # Materialize pending mapInArrow / cache so uncached MIA is not an empty wipe
        # (octo C2-Q-001 / C2-SAF-001 / C2-L-001).
        session = self._dataframe._session
        view_name = scratch_view_name(session, "_repark_writer_")
        session.create_or_replace_temp_view(view_name, self._dataframe._native_for_registration())
        try:
            # CTAS / INSERT / COPY execute eagerly at `sql()` (the engine's eager-command
            # contract), so the write commits here; the returned handle is intentionally discarded.
            session.sql(build_sql(view_name))
        finally:
            session.drop_temp_view(view_name)


def _sql_option_escape(value: str) -> str:
    """Escape a single-quoted COPY OPTIONS value."""
    return escape_sql_single_quotes(str(value))


def _normalize_write_compression(raw: str) -> str:
    """Map Spark compression names to DataFusion COPY format.compression tokens (CSV/JSON)."""
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
    """Map Spark parquet compression names to DataFusion ``format.compression`` tokens.

    # === r22 R2: writer option matrix ===
    DataFusion requires an explicit level for gzip/zstd (e.g. ``gzip(6)``, ``zstd(3)``);
    bare ``gzip`` / ``zstd`` are rejected by the engine. Spark names (snappy/gzip/lz4/…)
    map to those forms.
    """
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
    # Pass through already-leveled forms (gzip(4), zstd(3), …).
    if lowered.startswith("gzip(") or lowered.startswith("zstd(") or lowered.startswith("brotli("):
        return lowered
    raise AnalysisException(
        f"unsupported parquet write compression {raw!r}; "
        "repark supports snappy, gzip, zstd, lz4, none/uncompressed"
    )


def _merge_path_write_tree(staging: Any, destination: Any) -> None:
    """Merge a staged COPY tree into an existing destination (path-write ``mode('append')``).

    # === r22 R2: path append ===
    Recurses hive-style partition dirs; on file-name collision, renames the incoming file
    with a unique ``part-append-<hex>`` prefix so prior part files are preserved. Pure
    filesystem — no new plan machinery.
    """
    import shutil

    staging_path = Path(staging)
    destination_path = Path(destination)
    destination_path.mkdir(parents=True, exist_ok=True)
    if not staging_path.is_dir():
        # Single-file staging (unusual for COPY dir mode) — drop beside destination.
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
    """V2 table writer (PySpark ``DataFrame.writeTo`` → ``DataFrameWriterV2``).

    Routes **only** over the engine's existing sanctioned paths:

    * :meth:`create` / :meth:`createOrReplace` / :meth:`replace` →
      ``CREATE [OR REPLACE] TABLE … USING iceberg [PARTITIONED BY …] [TBLPROPERTIES …] AS SELECT``
    * :meth:`append` → ``INSERT INTO … SELECT <by-name projection>`` (columns resolve **by NAME**,
      unlike V1 :meth:`DataFrameWriter.insertInto` which is positional — recorded vs live
      PySpark 4.1.2 / Iceberg)
    * :meth:`overwritePartitions` → loud :class:`~repark.errors.UnsupportedOperationException`
      (Spark Iceberg semantics are DYNAMIC partition overwrite; the engine's static whole-table
      overwrite would silently replace all rows — refused until fork ``ReplacePartitions`` is
      wired; 2026-07-22 review)
    * :meth:`overwrite` (condition) → loud :class:`~repark.errors.UnsupportedOperationException`
      (no engine conditional-overwrite path)

    Identity ``partitionedBy`` AND non-identity transforms
    (``F.bucket``/``F.years``/``F.months``/``F.days``/``F.hours``, plus SQL ``truncate``) work
    end-to-end: they render into CTAS ``PARTITIONED BY`` and the engine builds the real Iceberg
    transform, computing each partition value from the source column via the iceberg-rust fork
    (Group P). A non-positive bucket/truncate width is rejected loudly at parse time.
    """

    __slots__ = (
        "_dataframe",
        "_partition_exprs",
        "_properties",
        "_provider",
        "_table",
    )

    def __init__(self, dataframe: DataFrame, table: str) -> None:
        """Bind the source DataFrame and the user-facing target table name.

        Bare / two-part names expand under the session **current** catalog + namespace at each
        action (``create`` / ``append`` / …), matching V1 ``saveAsTable`` — not frozen at
        ``writeTo`` construction (E2 / octo C1-L-002). Validates the identifier immediately so
        a malicious fragment fails at ``writeTo`` rather than at a later SQL action (C1-SEC-001).
        """
        self._dataframe = dataframe
        # Keep the user-facing name; qualify at action time under live catalog state.
        self._table = table
        # Early injection / identifier gate (discard resolved form — action re-resolves).
        _resolve_writer_table(dataframe, table)
        self._provider = "iceberg"
        self._properties: dict[str, str] = {}
        self._partition_exprs: list[str] = []

    def _resolved_table(self) -> tuple[str, str]:
        """Qualify + quote under the session's **current** catalog/NS (action-time)."""
        return _resolve_writer_table(self._dataframe, self._table)

    def using(self, provider: str) -> DataFrameWriterV2:
        """Set the table provider (PySpark ``DataFrameWriterV2.using``).

        Only ``"iceberg"`` is accepted (default). Other providers fail loud.
        """
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
        """Set a table property (PySpark ``DataFrameWriterV2.tableProperty``); chains.

        Properties land as CTAS ``TBLPROPERTIES ('k'='v', …)`` on :meth:`create` /
        :meth:`createOrReplace` / :meth:`replace`.
        """
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
        """Set partition expressions (PySpark ``DataFrameWriterV2.partitionedBy``).

        Accepts identity columns (``"cat"`` / ``F.col("cat")``) and partition transforms
        (``F.bucket(4, "id")``, ``F.years(F.col("event_date"))``, …). Both work end-to-end on
        CTAS: transform partitions route into SQL ``PARTITIONED BY (bucket(4, "id"), …)`` and the
        engine builds the real Iceberg transform, computing each partition value from the source
        column via the iceberg-rust fork (Group P).
        """
        for item in (col, *cols):
            self._partition_exprs.append(self._partition_sql_fragment(item))
        return self

    partitioned_by = partitionedBy

    def create(self) -> None:
        """Create the table (PySpark ``DataFrameWriterV2.create``).

        Fails with :class:`~repark.errors.AnalysisException` if the table already exists
        (Spark ``TABLE_OR_VIEW_ALREADY_EXISTS`` class).
        """
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
        """Create or replace the table (PySpark ``DataFrameWriterV2.createOrReplace``).

        Routes to ``CREATE OR REPLACE TABLE … AS SELECT`` (engine path already supported).
        """
        self._dataframe._ensure_alive()
        self._dataframe._refuse_tightened_iceberg_create()
        self._run_ctas(or_replace=True)

    create_or_replace = createOrReplace

    def replace(self) -> None:
        """Replace an **existing** table (PySpark ``DataFrameWriterV2.replace``).

        Raises :class:`~repark.errors.AnalysisException` if the table does not exist (Spark
        rejects replace on a missing table). When present, routes to ``CREATE OR REPLACE TABLE``.
        """
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
        """Append rows into an existing table **by name** (PySpark ``DataFrameWriterV2.append``).

        Columns resolve by NAME (Spark V2 / Iceberg contract) — a reordered same-named frame
        lands correctly, unlike positional V1 :meth:`DataFrameWriter.insertInto`.
        """
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
        """Refuse loud (2026-07-22 review): dynamic partition overwrite is unavailable.

        Spark Iceberg implements this as *dynamic* partition overwrite — only the partitions
        present in the source are replaced, and an empty source replaces nothing. repark's
        engine exposes only **static** whole-table ``INSERT OVERWRITE``, so honoring the call
        would silently replace ALL rows (and the empty-source case would wipe the table) —
        the silent-data-loss class for any caller relying on Spark's partition-scoped
        semantics (e.g. per-key audit-table refresh). The facade cannot even detect a
        partitioned target to carve out the unpartitioned-equivalence case. Until the fork's
        ``ReplacePartitions`` action is wired, this raises
        :class:`~repark.errors.UnsupportedOperationException`; use ``createOrReplace()`` for
        an intentional full rebuild or ``append()`` for additive writes.
        """

        raise UnsupportedOperationException(
            "overwritePartitions: Spark's dynamic partition overwrite (partition-scoped "
            "replace) is not supported by the repark engine yet — a static INSERT OVERWRITE "
            "would silently replace ALL rows, not just the source's partitions. Use "
            "createOrReplace() for a deliberate full rebuild, or append(). "
            "(Engine path: iceberg-rust fork ReplacePartitions, not yet wired.)"
        )

    overwrite_partitions = overwritePartitions

    def overwrite(self, condition: Column | str) -> None:
        """Conditional overwrite (PySpark ``DataFrameWriterV2.overwrite``).

        **Loud reject:** repark has no engine path for condition-scoped overwrite. Use
        :meth:`createOrReplace` for a deliberate full rebuild or a ``DELETE`` + ``append``
        pattern until a conditional path lands.
        """
        _ = condition  # signature parity

        raise UnsupportedOperationException(
            "DataFrameWriterV2.overwrite(condition) is not supported — no engine path for "
            "conditional overwrite (Group I disclosure). Use createOrReplace() for a "
            "deliberate full rebuild, or DELETE + append."
        )

    def option(self, key: str, value: Any) -> DataFrameWriterV2:
        """Writer option (PySpark ``DataFrameWriterV2.option``).

        Write path is **current-snapshot only** (I1): ``branch`` / ``tag`` fail loud. Other
        storage options are accepted then ignored with a process-once :class:`UserWarning`
        (C1-Q-005 / Group I disclosure).
        """
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
        """Writer options (PySpark ``DataFrameWriterV2.options``).

        Same rules as :meth:`option` — ``branch`` / ``tag`` fail loud; others warn-once.
        """
        for key, value in options.items():
            self.option(key, value)
        return self

    # ---- internals --------------------------------------------------------------------------

    @staticmethod
    def _partition_sql_fragment(item: Column | str) -> str:
        """Render one ``partitionedBy`` argument as a SQL ``PARTITIONED BY`` element.

        Bare identity column names (``str``) and identity :class:`Column` display names are
        double-quoted via :func:`repark._idents.quote_ident` so reserved words / hostile text
        cannot break out of the identifier (C1-SEC-001). Transform call forms
        (``years("col")`` / …) already embed a quoted identity arg from
        :func:`repark.functions._partition_transform` (C3-SEC-001) and are returned as-is.
        """
        from repark.spark._idents import quote_ident as _quote_ident

        if isinstance(item, str):
            return _quote_ident(item)
        if isinstance(item, Column):
            transform = getattr(item, "_partition_transform", None)
            if transform is not None:
                # Fragment already carries years("ident") / months("ident") / … (quoted).
                return transform
            # Identity column: F.col("x") / bare Column — quote the display name.
            return _quote_ident(item.spark_display_part())
        raise PySparkTypeError(
            f"partitionedBy expects column names (str) or Column, got {type(item).__name__}"
        )

    def _by_name_projection(self, session: Any, *, table_ref: str | None = None) -> str:
        """Quoted source-column list in table order (by-name conform; extra/missing → error)."""
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
        ``tableProperty`` text cannot be reinterpreted by ``str.format`` (Critic C2-S-001).
        Materializes pending mapInArrow / cache first (octo C2-Q-001 / C2-SAF-001 / C2-L-001).
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
    """PySpark ``DataFrame.stat`` surface — aliases of :class:`DataFrame` stat methods (G1).

    ``corr`` / ``cov`` / ``crosstab`` / ``sampleBy`` / ``approxQuantile`` delegate to the
    same implementations on :class:`DataFrame`. ``freqItems`` stays loud-unsupported.
    """

    __slots__ = ("_dataframe",)

    def __init__(self, dataframe: DataFrame) -> None:
        self._dataframe = dataframe

    def approxQuantile(  # noqa: N802
        self,
        col: str | list[str] | tuple[str, ...],
        probabilities: list[float] | tuple[float, ...],
        relativeError: float,  # noqa: N803
    ) -> list[float] | list[list[float]]:
        """Approximate quantiles of numeric columns — see :meth:`DataFrame.approxQuantile`."""
        return self._dataframe.approxQuantile(col, probabilities, relativeError)

    def corr(self, col1: str, col2: str, method: str | None = None) -> float:
        """Pearson correlation of two columns — see :meth:`DataFrame.corr`."""
        return self._dataframe.corr(col1, col2, method)

    def cov(self, col1: str, col2: str) -> float:
        """Sample covariance of two columns — see :meth:`DataFrame.cov`."""
        return self._dataframe.cov(col1, col2)

    def crosstab(self, col1: str, col2: str) -> DataFrame:
        """Pair-wise frequency table of two columns — see :meth:`DataFrame.crosstab`."""
        return self._dataframe.crosstab(col1, col2)

    def freqItems(self, cols: list[str], support: float | None = None) -> DataFrame:  # noqa: N802
        """Refuse — frequent-item discovery is not implemented; raises loudly today."""
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
        """Stratified sample without replacement — see :meth:`DataFrame.sampleBy`."""
        return self._dataframe.sampleBy(col, fractions, seed)
