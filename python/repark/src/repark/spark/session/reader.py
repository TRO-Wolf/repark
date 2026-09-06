"""DataFrameReader."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from typing import Any

from repark.spark.session import _funcs as _session_funcs
from repark.spark.session.session_core import ReparkSession

for _name in dir(_session_funcs):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_session_funcs, _name)
del _name, _session_funcs


class DataFrameReader:
    """PySpark ``DataFrameReader``: parquet, csv, json, excel, table, format/load, option(s).

    CSV/JSON route through DataFusion native readers (``read_csv`` / ``read_json``).
    :meth:`schema` stores a user StructType/DDL for those formats. Unknown formats raise
    :class:`~repark.errors.AnalysisException` (Spark's ``DATA_SOURCE_NOT_FOUND`` class shape).
    Unknown options are accepted and stored (Spark silently tolerates them) except the semantic
    denylist which fails loud. :meth:`excel` is a disclosed RePark extension (PySpark has no
    excel reader); single-sheet v1 — use :meth:`sheet_names` to discover sheets.
    """

    __slots__ = ("_format", "_options", "_schema", "_session")

    def __init__(self, session: ReparkSession) -> None:
        """Bind to the owning session; each ``spark.read`` access returns a fresh reader."""
        self._session = session
        self._format: str | None = None
        self._options: dict[str, str] = {}
        self._schema: Any | None = None

    def parquet(self, path: str | Path) -> DataFrame:
        """Read Parquet (PySpark ``spark.read.parquet``) via the session's native path.

        Semantic reader options set via :meth:`option` fail loud here (same gate as :meth:`load`).
        """
        self._reject_unsupported_semantic_options()
        if self._schema is not None:
            from repark.errors import AnalysisException

            raise AnalysisException(
                "DataFrameReader.schema(...) is not applied on parquet reads yet "
                "(use csv/json schema, or cast after read)"
            )
        return self._session.read_parquet(path)

    def csv(
        self,
        path: str | Path | list[str] | None = None,
        schema: Any | None = None,
        sep: str | None = None,
        encoding: str | None = None,
        quote: str | None = None,
        escape: str | None = None,
        comment: str | None = None,
        header: bool | str | None = None,
        inferSchema: bool | str | None = None,  # noqa: N803 — PySpark param name
        nullValue: str | None = None,  # noqa: N803 — PySpark param name
        multiLine: bool | str | None = None,  # noqa: N803 — PySpark param name
        mode: str | None = None,
        compression: str | None = None,
        **extra: Any,
    ) -> DataFrame:
        """Read CSV (PySpark ``spark.read.csv``).

        Everyday options are wired; see ``task/r1-read-formats-ledger.md`` for the full matrix
        (engine-default-differs for ``inferSchema`` when no user schema is supplied). Default
        semantics are **frozen**. For preamble junk, delimiter auto-detect,
        protocol type inference, and surfaceable diagnostics use :meth:`smartCsv` (repark
        extension).
        """
        if schema is not None:
            self.schema(schema)
        if path is None:
            path = self._option_path()
        if path is None:
            from repark.errors import AnalysisException

            raise AnalysisException("CSV load requires a path argument")
        # Keyword options overlay the builder options map (last-write-wins, Spark shape).
        for key, value in (
            ("sep", sep),
            ("encoding", encoding),
            ("quote", quote),
            ("escape", escape),
            ("comment", comment),
            ("header", header),
            ("inferSchema", inferSchema),
            ("nullValue", nullValue),
            ("multiLine", multiLine),
            ("mode", mode),
            ("compression", compression),
        ):
            if value is not None:
                self.option(key, value)
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)
        self._format = "csv"
        return self._load_csv(path)

    def smartCsv(  # noqa: N802 — repark extension camelCase (Q5 contract)
        self,
        path: str | Path | None = None,
        sep: str | None = None,
        header: bool | str | None = None,
        nullValue: str | None = None,  # noqa: N803 — PySpark-shaped option name
        normalizeHeaderCase: str | None = None,  # noqa: N803 — repark extension
        samplingRows: int | None = None,  # noqa: N803 — repark extension (r26 Q4)
        **extra: Any,
    ) -> DataFrame:
        """Smart CSV reader (repark extension) — protocol inference + messy-file heuristics.

        **Not** a Spark API. Default :meth:`csv` is unchanged (byte-identical pins).

        * Delimiter auto-detect (unless ``sep`` given; ``sep`` must be one character)
        * Leading junk/preamble skip (delimiter-consistency scan)
        * Header auto-detect (override with ``header=True/False``)
        * Ragged rows null-padded (count in :meth:`~repark.dataframe.DataFrame.describe_ingest`)
        * Type inference via the Q1 protocol ladder
          (bool→int32→int64→decimal128→float64→date→timestamp→string)
        * Opt-in header case normalization via ``normalizeHeaderCase``
          (``lower`` / ``upper`` / ``snake``) — never silent by default
        * Inference sampling: full scan when ≤ 10_000 data rows, else first
          10_000 only (override with ``samplingRows``). Cap is **inference-only** —
          the full file is always read for data. A value class appearing only past
          the cap can under-widen the schema; the subsequent cast fails loud
          (e.g. decimal overflow) rather than corrupting — raise ``samplingRows``.
          ``samplingRows`` ≤ 0 raises :class:`~repark.errors.IllegalArgumentException`.

        Diagnostics: ``df.describe_ingest()`` includes ``inference_rows_scanned``,
        ``inference_capped``, ``sampling_rows_limit`` (never silent magic).
        """
        from repark.spark._csv_smart import load_smart_csv
        from repark.errors import AnalysisException, IllegalArgumentException

        if path is None:
            path = self._option_path()
        if path is None:
            raise AnalysisException("smartCsv requires a path argument")
        if isinstance(path, list):
            raise AnalysisException(
                "smartCsv does not accept multi-path lists yet (pass a single path)"
            )
        # Snake-case locals (N806); kwargs keep PySpark-shaped camelCase names.
        if sep is not None:
            resolved_sep = sep
        else:
            sep_option = self._option_str("sep")
            resolved_sep = sep_option if sep_option is not None else self._option_str("delimiter")
        resolved_null = nullValue if nullValue is not None else self._option_str("nullvalue")
        resolved_case = (
            normalizeHeaderCase
            if normalizeHeaderCase is not None
            else self._option_str("normalizeheadercase")
        )
        header_raw: bool | str | None = header
        if header_raw is None and self._option_str("header") is not None:
            header_raw = self._option_bool("header", default=True)
        resolved_sampling = samplingRows
        if resolved_sampling is None and self._option_str("samplingrows") is not None:
            resolved_sampling = self._option_str("samplingrows")
        for key, value in extra.items():
            lowered = str(key).lower()
            if lowered in {"sep", "delimiter"} and value is not None:
                resolved_sep = str(value)
            elif lowered == "header" and value is not None:
                header_raw = value  # type: ignore[assignment]
            elif lowered == "nullvalue" and value is not None:
                resolved_null = str(value)
            elif lowered == "normalizeheadercase" and value is not None:
                resolved_case = str(value)
            elif lowered == "samplingrows" and value is not None:
                resolved_sampling = value  # type: ignore[assignment]

        header_flag: bool | None
        if header_raw is None:
            header_flag = None
        elif isinstance(header_raw, bool):
            header_flag = header_raw
        else:
            normalized = str(header_raw).strip().lower()
            if normalized in {"true", "1", "yes", "t", "y"}:
                header_flag = True
            elif normalized in {"false", "0", "no", "f", "n"}:
                header_flag = False
            else:
                raise AnalysisException(f"smartCsv header expects a boolean, got {header_raw!r}")

        if resolved_case is not None:
            case_token = str(resolved_case).strip().lower()
            if case_token not in {"lower", "upper", "snake"}:
                raise AnalysisException(
                    "smartCsv normalizeHeaderCase must be one of "
                    f"'lower', 'upper', 'snake'; got {resolved_case!r}"
                )
            resolved_case = case_token

        if resolved_sampling is not None:
            try:
                if isinstance(resolved_sampling, bool):
                    raise TypeError("bool is not a valid samplingRows")
                if isinstance(resolved_sampling, float):
                    if not resolved_sampling.is_integer():
                        raise ValueError("non-integral float")
                    sampling_int = int(resolved_sampling)
                else:
                    # str / int — reject "2.5" via float check after int fail path
                    text = str(resolved_sampling).strip()
                    if "." in text or "e" in text.lower():
                        as_float = float(text)
                        if not as_float.is_integer():
                            raise ValueError("non-integral")
                        sampling_int = int(as_float)
                    else:
                        sampling_int = int(text, 10)
            except (TypeError, ValueError) as exc:
                raise IllegalArgumentException(
                    f"smartCsv samplingRows must be an integer, got {resolved_sampling!r}"
                ) from exc
            if sampling_int <= 0:
                raise IllegalArgumentException(
                    f"smartCsv samplingRows must be > 0, got {sampling_int}"
                )
            resolved_sampling = sampling_int

        if resolved_sep is not None:
            from repark.spark._csv_smart import _require_single_char_delimiter

            if not isinstance(resolved_sep, str):
                raise IllegalArgumentException(
                    "smartCsv sep must be a single character other than newline, "
                    f"carriage return, or quote, got {resolved_sep!r}"
                )
            try:
                resolved_sep = _require_single_char_delimiter(resolved_sep, what="smartCsv sep")
            except ValueError as error:
                raise IllegalArgumentException(str(error)) from error

        frame, report = load_smart_csv(
            self._session,
            path,
            sep=resolved_sep,
            header=header_flag,
            null_value=resolved_null,
            normalize_header_case=resolved_case,
            sampling_rows=resolved_sampling,
        )
        # Sticky diagnostics meta for describe_ingest (slots attribute).
        frame._ingest_report = report.to_dict()
        return frame

    def json(
        self,
        path: str | Path | list[str] | None = None,
        schema: Any | None = None,
        multiLine: bool | str | None = None,  # noqa: N803 — PySpark param name
        mode: str | None = None,
        compression: str | None = None,
        **extra: Any,
    ) -> DataFrame:
        """Read JSON / NDJSON (PySpark ``spark.read.json``)."""
        if schema is not None:
            self.schema(schema)
        if path is None:
            path = self._option_path()
        if path is None:
            from repark.errors import AnalysisException

            raise AnalysisException("JSON load requires a path argument")
        for key, value in (
            ("multiLine", multiLine),
            ("mode", mode),
            ("compression", compression),
        ):
            if value is not None:
                self.option(key, value)
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)
        self._format = "json"
        return self._load_json(path)

    def excel(
        self,
        path: str | Path | None = None,
        sheet_name: str | None = None,
        header: bool | str | None = None,
        skip_rows: int | str | None = None,
        schema: Any | None = None,
        **extra: Any,
    ) -> DataFrame:
        """Read one Excel sheet (disclosed extension — polars-adjacent naming).

        Single-sheet v1. Default sheet is the first workbook sheet. Use
        :meth:`sheet_names` to list sheets. Types resolve via the Rust Q1 lattice
        (bool → int32 → int64 → decimal128 → float64 → date → timestamp → string).

        Parameters
        ----------
        path:
            Path to ``.xlsx`` / ``.xls``.
        sheet_name:
            Sheet to read; ``None`` selects the first sheet.
        header:
            When true (default), first non-skipped row is column names.
        skip_rows:
            Leading rows to skip before the header (or first data row).
        schema:
            Optional type override: comma-separated lattice names, a list/tuple of type
            name strings matching column order, or a list of column name strings (names
            only — types still inferred). StructType is accepted as names + simple
            type names when fields are scalar.
        """
        if path is None:
            path = self._option_path()
        if path is None:
            from repark.errors import AnalysisException

            raise AnalysisException("excel load requires a path argument")
        if sheet_name is not None:
            self.option("sheet_name", sheet_name)
        if header is not None:
            self.option("header", header)
        if skip_rows is not None:
            self.option("skip_rows", skip_rows)
        if schema is not None:
            self._apply_excel_schema_option(schema)
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)
        self._format = "excel"
        return self._load_excel(path)

    def sheet_names(self, path: str | Path) -> list[str]:
        """List Excel workbook sheet names in workbook order (first is the v1 default)."""
        return self._session.excel_sheet_names(path)

    def table(self, table_name: str) -> DataFrame:
        """Load a catalog/temp table by name (PySpark ``spark.read.table``).

        Same semantics as :meth:`ReparkSession.table` / ``SELECT * FROM <name>``.
        Iceberg time-travel options (``snapshot-id`` / ``as-of-timestamp`` / ``branch`` / ``tag``)
        pin a snapshot-static scan. Other semantic reader options fail loud.
        """
        self._reject_unsupported_semantic_options()
        travel = self._iceberg_time_travel_opts()
        if travel is not None:
            # read_iceberg_table resolves bare/two-part/spark_catalog; do not
            # bypass the shared layer by forwarding the raw user string only.
            return self._session.read_iceberg_table(table_name, **travel)
        return self._session.table(table_name)

    def jdbc(
        self,
        url: str,
        table: str | None = None,
        column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
        num_partitions: int | None = None,
        predicates: list[str] | None = None,
        properties: dict[str, str] | None = None,
        *,
        connection_properties: dict[str, str] | None = None,
    ) -> DataFrame:
        """Read PostgreSQL via JDBC-compatible options (PySpark ``spark.read.jdbc``).

        Three Spark overload shapes are supported:

        1. ``jdbc(url, table, properties=...)`` — single partition
        2. ``jdbc(url, table, column, lower_bound, upper_bound, num_partitions, properties)`` —
           range partitions (Spark stride; first/last unbounded)
        3. ``jdbc(url, table, predicates=[...], properties=...)`` — one partition per predicate
           (Spark does no overlap checking; duplicates are contractual)

        Only PostgreSQL URLs are supported in v1 (``jdbc:postgresql://`` or ``postgresql://``).
        ``driver`` is accepted and ignored (disclosed). TLS (SEC-001): default
        ``sslmode=prefer`` when omitted; prefer attempts verified TLS and may fall back to
        plaintext **with a downgrade warning** (never silent). Explicit ``disable`` is silent
        plaintext. ``require`` / ``verify-full`` encrypt + verify and never fall back;
        ``verify-ca`` is refused loud.
        """
        from repark.errors import IllegalArgumentException

        props = dict(properties or connection_properties or {})
        # Never log props (may contain password). Resolve dbtable from the positional table
        # arg OR properties["dbtable"] (case-insensitive).
        dbtable = table
        if dbtable is None:
            for key, value in props.items():
                if key.lower() == "dbtable" and value:
                    dbtable = value
                    break
        if dbtable is None:
            raise IllegalArgumentException(
                "jdbc requires a table name (or dbtable option) — "
                "use spark.read.jdbc(url, table, properties=...) "
                "or format('postgres').option('dbtable', ...).load()"
            )
        # Detect partial range bags early for a clear facade error.
        range_parts = [column, lower_bound, upper_bound, num_partitions]
        range_set = sum(part is not None for part in range_parts)
        if predicates is not None and range_set > 0:
            raise IllegalArgumentException(
                "jdbc predicates[] cannot be combined with partitionColumn/lowerBound/"
                "upperBound/numPartitions (Spark JDBC mutual exclusion)"
            )
        if range_set not in (0, 4):
            raise IllegalArgumentException(
                "jdbc range partitioning requires column, lower_bound, upper_bound, and "
                "num_partitions together (Spark JDBC parity)"
            )
        if predicates is not None and len(predicates) == 0:
            raise IllegalArgumentException("jdbc predicates[] must be non-empty when supplied")

        return self._session.read_postgres(
            url=url,
            dbtable=dbtable,
            query=None,
            properties=props,
            partition_column=column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            predicates=predicates,
        )

    def format(self, source: str) -> DataFrameReader:
        """Set the input format (PySpark ``DataFrameReader.format``); returns self for chaining."""
        self._format = source
        return self

    def option(self, key: str, value: Any) -> DataFrameReader:
        """Set a single reader option (PySpark ``DataFrameReader.option``).

        Keys are case-insensitive with last-write-wins (Spark map semantics). Unknown keys are
        stored and tolerated. The ``path`` key is applied by :meth:`load` when the path argument
        is omitted. Options that would change which files/rows/snapshots Spark would load raise
        at materializing methods (``load`` / ``parquet`` / ``table``).
        """
        key_str = str(key)
        for existing in list(self._options):
            if existing.lower() == key_str.lower():
                del self._options[existing]
        self._options[key_str] = str(value)
        return self

    def options(self, **options: Any) -> DataFrameReader:
        """Set multiple reader options (PySpark ``DataFrameReader.options``)."""
        for key, value in options.items():
            self.option(key, value)
        return self

    def load(self, path: str | Path | None = None, **extra: Any) -> DataFrame:
        """Load data for the configured format (PySpark ``DataFrameReader.load``).

        * ``format("parquet").load(path)`` ≡ ``.parquet(path)``
        * ``format("csv"|"json").load(path)`` ≡ ``.csv`` / ``.json``
        * ``format("parquet").option("path", p).load()`` uses the option when ``path`` is omitted
        * ``format("iceberg").load(table_identifier)`` reads the **catalog** Iceberg table
          (PySpark Iceberg convention: ``load`` takes the table name, not a filesystem path).
          Bare names resolve under current catalog/NS with **no** temp-view prefer
          (unlike :meth:`table` / ``spark.table``)
        * missing/unknown format → :class:`~repark.errors.AnalysisException`
        * empty format is **not** Spark's default-parquet: call ``format(...)`` first
          (disclosed divergence — Spark uses ``spark.sql.sources.default``)
        * semantic options repark does not implement (e.g. ``pathGlobFilter``) → AnalysisException
        """
        from repark.errors import AnalysisException

        # Spark load(**options) merges kwargs into the reader option map.
        for key, value in extra.items():
            if value is not None:
                self.option(key, value)

        fmt = (self._format or "").strip().lower()
        # Postgres/JDBC options are intentional; skip the parquet/iceberg semantic gate for them.
        if fmt not in {"postgres", "postgresql", "jdbc"}:
            self._reject_unsupported_semantic_options()
        if not fmt:
            raise AnalysisException(
                "DATA_SOURCE_NOT_FOUND: Failed to find the data source: (empty). "
                "Call format(...) before load(...) "
                "(repark does not default empty format to parquet)."
            )
        effective_path = path if path is not None else self._option_path()
        if fmt == "parquet":
            if effective_path is None:
                raise AnalysisException("Parquet load requires a path argument")
            return self.parquet(effective_path)
        if fmt == "csv":
            if effective_path is None:
                raise AnalysisException("CSV load requires a path argument")
            return self._load_csv(effective_path)
        if fmt == "json":
            if effective_path is None:
                raise AnalysisException("JSON load requires a path argument")
            return self._load_json(effective_path)
        if fmt == "iceberg":
            if effective_path is None:
                raise AnalysisException("Iceberg load requires a table identifier argument")
            # spark.table() / read.table() still prefer temp views; format("iceberg").load
            # must not silent-shadow a catalog table with a same-name temp view.
            # Time-travel options + residual denylist already applied above.
            travel = self._iceberg_time_travel_opts()
            if travel is not None:
                return self._session.read_iceberg_table(str(effective_path), **travel)
            return self._session.read_iceberg_table(str(effective_path))
        if fmt in {"postgres", "postgresql", "jdbc"}:
            return self._load_postgres()
        # Truncate hostile/long format strings in the error.
        shown = (self._format or "")[:64]
        raise AnalysisException(
            f"DATA_SOURCE_NOT_FOUND: Failed to find the data source: {shown!r}. "
            "Make sure the provider name is correct and the package is properly registered "
            "and compatible with your Spark version."
        )

    def _load_csv(self, path: str | Path | list[str]) -> DataFrame:
        """Materialize CSV via the session native reader + Spark-semantics post-steps."""
        # Semantic I/O options (pathGlobFilter / ignoreCorruptFiles / mergeSchema / …) must fail
        # loud on the `.csv()` shorthand too — not only format().load.
        self._reject_unsupported_semantic_options()
        self._reject_csv_json_parse_options(is_csv=True)
        path_str = _reader_path_to_str(path)
        native_options = self._native_options_for(_CSV_NATIVE_OPTION_KEYS)
        # Spark default header=false when unset — force it so DF does not default true.
        if "header" not in {key.lower() for key in native_options}:
            native_options["header"] = "false"
        return self._apply_reader_schema_semantics(
            self._session.read_csv(path_str, native_options),
            infer_schema=self._option_bool("inferschema", default=False),
            header=self._option_bool("header", default=False),
            path=path_str,
        )

    def _load_json(self, path: str | Path | list[str]) -> DataFrame:
        """Materialize JSON via the session native reader + optional schema projection."""
        self._reject_unsupported_semantic_options()
        self._reject_csv_json_parse_options(is_csv=False)
        path_str = _reader_path_to_str(path)
        native_options = self._native_options_for(_JSON_NATIVE_OPTION_KEYS)
        frame = self._session.read_json(path_str, native_options)
        # multiLine=true maps to DF newline_delimited=false, which accepts a **JSON array** only.
        # A pretty single object or NDJSON under multiLine yields an empty schema — silent wrong
        # vs Spark. Empty `[]` is allowed (R1-C5-001).
        if (
            self._option_bool("multiline", default=False)
            and not list(frame.columns)
            and _json_multiline_empty_schema_is_mismatch(path_str)
        ):
            from repark.errors import AnalysisException

            raise AnalysisException(
                "multiLine JSON produced an empty schema; repark (DataFusion) multiLine "
                "expects a top-level JSON array, not a single multi-line object or NDJSON. "
                "Use multiLine=false for NDJSON, or wrap records in [ ... ]. "
                'For a single object wrapper such as {"Orders":[...]}, use '
                "json.load + spark.createDataFrame(payload['Orders']) (dict key-union path)."
            )
        # JSON always has field names; inferSchema is not a Spark JSON option in the same way.
        return self._apply_reader_schema_semantics(frame, infer_schema=True, header=True)

    def _load_excel(self, path: str | Path) -> DataFrame:
        """Materialize one Excel sheet via the pure-Rust calamine reader."""
        # Semantic I/O / Iceberg TT options do not apply to local Excel files.
        self._reject_unsupported_semantic_options()
        path_str = str(path)
        native_options: dict[str, str] = {}
        for key, value in self._options.items():
            lowered = key.lower()
            if lowered in _EXCEL_NATIVE_OPTION_KEYS:
                native_options[lowered] = str(value)
        # Spark-style schema() store: if set and not already pushed as type list, apply names.
        if self._schema is not None and "schema" not in native_options:
            self._apply_excel_schema_option(self._schema)
            for key, value in self._options.items():
                lowered = key.lower()
                if lowered in _EXCEL_NATIVE_OPTION_KEYS:
                    native_options[lowered] = str(value)
        if "header" not in native_options:
            native_options["header"] = "true"
        return self._session.read_excel(path_str, native_options)

    def _apply_excel_schema_option(self, schema: Any) -> None:
        """Map facade schema forms onto excel option keys (names and/or lattice types)."""
        if isinstance(schema, str):
            # Comma-separated lattice type names OR a single DDL-ish blob treated as types.
            self.option("schema", schema)
            return
        if isinstance(schema, (list, tuple)):
            if not schema:
                return
            if all(isinstance(item, str) for item in schema):
                # Ambiguous: could be column names or type names. Prefer types when every
                # token is a known lattice name; otherwise treat as column names.
                known = {
                    "bool",
                    "boolean",
                    "int",
                    "integer",
                    "int32",
                    "long",
                    "bigint",
                    "int64",
                    "decimal",
                    "decimal128",
                    "numeric",
                    "float",
                    "double",
                    "float64",
                    "real",
                    "date",
                    "timestamp",
                    "datetime",
                    "string",
                    "str",
                    "utf8",
                    "text",
                    "varchar",
                }
                if all(str(item).strip().lower() in known for item in schema):
                    self.option("schema", ",".join(str(item).strip() for item in schema))
                else:
                    self.option("column_names", ",".join(str(item) for item in schema))
                return
        # StructType / list of StructField — extract names + simple type names when possible.
        fields = getattr(schema, "fields", None)
        if fields is not None:
            names: list[str] = []
            types: list[str] = []
            for field in fields:
                name = getattr(field, "name", None)
                data_type = getattr(field, "dataType", None) or getattr(field, "data_type", None)
                if name is None:
                    continue
                names.append(str(name))
                type_name = getattr(data_type, "typeName", None)
                if callable(type_name):
                    types.append(str(type_name()))
                elif data_type is not None:
                    types.append(str(data_type).lower())
                else:
                    types.append("string")
            if names:
                self.option("column_names", ",".join(names))
            if types and len(types) == len(names):
                self.option("schema", ",".join(types))
            return
        from repark.errors import AnalysisException

        raise AnalysisException(
            "excel schema must be a lattice type list/string, column-name list, or StructType"
        )

    def _native_options_for(self, allowed: frozenset[str]) -> dict[str, str]:
        """Lowercase keys for native option bags; keep only keys the engine understands."""
        out: dict[str, str] = {}
        for key, value in self._options.items():
            lowered = key.lower()
            if lowered in allowed:
                out[lowered] = value
        return out

    def _option_bool(self, key_lower: str, *, default: bool) -> bool:
        """Read a boolean reader option (case-insensitive key); default when unset.

        Invalid spellings fail loud — never silent-false on ``maybe``.
        """
        for key, value in self._options.items():
            if key.lower() == key_lower:
                normalized = str(value).strip().lower()
                if normalized in {"true", "1", "yes", "t", "y"}:
                    return True
                if normalized in {"false", "0", "no", "f", "n"}:
                    return False
                from repark.errors import AnalysisException

                raise AnalysisException(f"reader option {key!r} expects a boolean, got {value!r}")
        return default

    def _reject_csv_json_parse_options(self, *, is_csv: bool) -> None:
        """Fail loud on CSV/JSON parse options repark does not honor (no silent default lies)."""
        from repark.errors import AnalysisException

        denylist = _CSV_UNSUPPORTED_PARSE_OPTIONS if is_csv else _JSON_UNSUPPORTED_PARSE_OPTIONS
        for key in self._options:
            lowered = key.lower()
            if lowered == "path":
                continue
            if lowered == "mode":
                mode = str(self._options[key]).strip().upper()
                if mode in {"", "PERMISSIVE"}:
                    continue
                raise AnalysisException(
                    f"reader option mode={self._options[key]!r} is not supported by repark yet "
                    f"(only PERMISSIVE / default; FAILFAST/DROPMALFORMED are unsupported-loud)"
                )
            if lowered == "encoding":
                encoding = str(self._options[key]).strip().lower().replace("-", "")
                if encoding in {"", "utf8", "utf_8"}:
                    continue
                raise AnalysisException(
                    f"reader option encoding={self._options[key]!r} is not supported (only UTF-8)"
                )
            if lowered in denylist:
                raise AnalysisException(
                    f"reader option {key!r} is not supported by repark yet "
                    "(would silently change load semantics if ignored)"
                )
            if lowered == "timezone":
                raise AnalysisException(
                    f"reader option {key!r} is not supported by repark yet "
                    "(would silently change load semantics if ignored)"
                )

    def _apply_reader_schema_semantics(
        self, frame: DataFrame, *, infer_schema: bool, header: bool, path: str | None = None
    ) -> DataFrame:
        """Apply user schema / Spark inferSchema=false / no-header column naming.

        * User ``schema`` → project **by name** when source has matching field names; otherwise
          by position (CSV no-header).
        * ``inferSchema=false`` without user schema → cast all columns to string (Spark default).
        * ``nullValue`` → engine-side ``when``→NULL (DataFusion ``null_regex`` is not applied on
          the scan path reliably; facade maps Spark nullValue honestly).
        * No header and no user schema → rename DF ``column_N`` names to Spark ``_c0``, ``_c1``.
        """
        from repark.spark import functions as F  # noqa: N812 — local import avoids cycle at module load

        schema = self._schema
        columns = list(frame.columns)
        null_token = self._option_str("nullvalue")

        # Apply nullValue on the string form of every column before casts (Spark parse nulls).
        # Engine may already have Utf8-forced the scan when nullValue is set (R1-C1-001).
        if null_token is not None:
            selects = [
                F.when(F.col(name).cast("string") == F.lit(null_token), F.lit(None))
                .otherwise(F.col(name))
                .alias(name)
                for name in columns
            ]
            frame = frame.select(*selects)
            columns = list(frame.columns)

        if schema is not None:
            fields = _schema_fields(schema)
            if not fields:
                return frame
            column_by_case = {name.casefold(): name for name in columns}
            # Prefer name-based projection when every schema field exists on the source
            # (JSON / CSV-with-header). Fall back to position for CSV no-header generic names.
            name_based = all(field["name"].casefold() in column_by_case for field in fields)
            selects = []
            if name_based:
                for field in fields:
                    source_name = column_by_case[field["name"].casefold()]
                    selects.append(F.col(source_name).cast(field["dataType"]).alias(field["name"]))
            else:
                # Partial name match (JSON / header CSV with user schema listing extra fields):
                # null-fill missing names — Spark shape. Skip when source names
                # look like DF generic no-header columns so positional CSV still works.
                generic_source = bool(columns) and all(
                    name.lower().startswith("column") or name.startswith("_c") for name in columns
                )
                any_name_hit = any(field["name"].casefold() in column_by_case for field in fields)
                if any_name_hit and not generic_source:
                    for field in fields:
                        key = field["name"].casefold()
                        if key in column_by_case:
                            source_name = column_by_case[key]
                            selects.append(
                                F.col(source_name).cast(field["dataType"]).alias(field["name"])
                            )
                        else:
                            selects.append(F.lit(None).cast(field["dataType"]).alias(field["name"]))
                elif len(fields) <= len(columns):
                    for index, field in enumerate(fields):
                        source_name = columns[index]
                        selects.append(
                            F.col(source_name).cast(field["dataType"]).alias(field["name"])
                        )
                else:
                    from repark.errors import AnalysisException

                    raise AnalysisException(
                        f"reader schema has {len(fields)} fields but source has "
                        f"{len(columns)} columns"
                    )
            return frame.select(*selects)

        frame = _finish_csv_infer_schema(frame, self, infer_schema, null_token, path)
        # Align generic DF names with Spark `_cN` when no header and no user schema —
        # regardless of inferSchema.
        if not header and columns:
            renames = []
            needs_rename = False
            for index, name in enumerate(columns):
                lowered = name.lower()
                if lowered.startswith("column") or lowered == f"column_{index + 1}":
                    renames.append(F.col(name).alias(f"_c{index}"))
                    needs_rename = True
                else:
                    renames.append(F.col(name))
            if needs_rename:
                frame = frame.select(*renames)
                columns = list(frame.columns)

        if not infer_schema:
            # Spark CSV default: all string columns.
            return frame.select(*[F.col(name).cast("string").alias(name) for name in columns])

        # inferSchema=true after nullValue Utf8 force: re-promote integer/double/boolean columns
        # via engine casts (Python only builds the plan — no row loops).
        return _cast_inferred_naive_timestamps(frame)

    def _option_str(self, key_lower: str) -> str | None:
        """Return a string reader option value if set (case-insensitive key)."""
        for key, value in self._options.items():
            if key.lower() == key_lower:
                return value
        return None

    def _load_postgres(self) -> DataFrame:
        """Materialize format('postgres'|'jdbc'|'postgresql') via the session connector."""
        from repark.errors import IllegalArgumentException

        options = {key.lower(): value for key, value in self._options.items()}
        url = options.get("url")
        if not url:
            raise IllegalArgumentException(
                "format('postgres') requires option('url', ...) "
                "(jdbc is a compatibility alias; docs lead with postgres)"
            )
        dbtable = options.get("dbtable")
        query = options.get("query")
        if dbtable and query:
            raise IllegalArgumentException(
                "options `dbtable` and `query` are mutually exclusive (Spark JDBC parity)"
            )
        if not dbtable and not query:
            raise IllegalArgumentException(
                "format('postgres') requires option('dbtable', ...) or option('query', ...)"
            )

        # Partition options (optional all-four bag).
        partition_column = options.get("partitioncolumn")
        lower_raw = options.get("lowerbound")
        upper_raw = options.get("upperbound")
        num_raw = options.get("numpartitions")
        range_set = sum(
            value is not None for value in (partition_column, lower_raw, upper_raw, num_raw)
        )
        if range_set not in (0, 4):
            raise IllegalArgumentException(
                "partitionColumn, lowerBound, upperBound, and numPartitions must all be set "
                "together (Spark JDBC parity)"
            )
        lower_bound = _parse_jdbc_int_option("lowerBound", lower_raw)
        upper_bound = _parse_jdbc_int_option("upperBound", upper_raw)
        num_partitions = _parse_jdbc_int_option("numPartitions", num_raw)

        # Connection property bag (pass-through; driver ignored by engine).
        connection_keys = {
            "user",
            "username",
            "password",
            "passwd",
            "pwd",
            "fetchsize",
            "fetch_size",
            "sslmode",
            "ssl_mode",
            "driver",
        }
        properties = {
            key: value
            for key, value in self._options.items()
            if key.lower() in connection_keys
            or key.lower()
            not in {
                "url",
                "dbtable",
                "query",
                "partitioncolumn",
                "lowerbound",
                "upperbound",
                "numpartitions",
                "path",
            }
        }

        return self._session.read_postgres(
            url=url,
            dbtable=dbtable,
            query=query,
            properties=properties,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            predicates=None,
        )

    def _option_path(self) -> str | None:
        """Return the ``path`` reader option if set (case-insensitive; last-write-wins)."""
        # Dict preserves insertion order; option() replaces same-key-insensitive so the sole
        # path entry is the latest write.
        for key, value in self._options.items():
            if key.lower() == "path" and value:
                return value
        return None

    def _reject_unsupported_semantic_options(self) -> None:
        """Fail loud on options that would constrain I/O in Spark but are not applied here."""
        from repark.errors import AnalysisException

        fmt = (self._format or "").strip().lower()
        for key in self._options:
            lowered = key.lower()
            if lowered == "path":
                continue
            # compression is wired for csv/json; still loud on parquet/iceberg/empty.
            if lowered == "compression":
                if fmt in {"csv", "json"}:
                    continue
                raise AnalysisException(
                    f"reader option {key!r} is not supported by repark yet "
                    "(would silently change load semantics if ignored)"
                )
            # Time-travel pins are applied on the Iceberg path only; on parquet they stay loud.
            if lowered in _ICEBERG_TIME_TRAVEL_OPTIONS:
                if fmt in {"", "iceberg"}:
                    continue
                raise AnalysisException(
                    f"reader option {key!r} is only supported for format('iceberg') "
                    "(Iceberg time travel)"
                )
            if lowered in _UNSUPPORTED_SEMANTIC_READER_OPTIONS:
                # Incremental-read bounds get a targeted message (future seed).
                if lowered in {"start-snapshot-id", "end-snapshot-id"}:
                    raise AnalysisException(
                        f"reader option {key!r} is not supported by repark yet "
                        "(Iceberg incremental read / start-end snapshot window is a future seed; "
                        "use snapshot-id / as-of-timestamp / branch / tag for time travel)"
                    )
                raise AnalysisException(
                    f"reader option {key!r} is not supported by repark yet "
                    "(would silently change load semantics if ignored)"
                )

    def _iceberg_time_travel_opts(self) -> dict[str, Any] | None:
        """Collect supported Iceberg time-travel options; mutual exclusion fails loud.

        Returns ``None`` when no pin is set; otherwise a kwargs dict for
        :meth:`ReparkSession.read_iceberg_table`.
        """
        from repark.errors import AnalysisException

        found: dict[str, str] = {}
        for key, value in self._options.items():
            lowered = key.lower()
            if lowered in _ICEBERG_TIME_TRAVEL_OPTIONS:
                found[lowered] = value
        if not found:
            return None
        if len(found) > 1:
            names = " and ".join(sorted(found))
            raise AnalysisException(
                f"Iceberg time-travel reader options are mutually exclusive; got {names}"
            )
        key, raw = next(iter(found.items()))
        if key == "snapshot-id":
            try:
                parsed = int(raw)
            except ValueError as error:
                raise AnalysisException(
                    f"snapshot-id must be an integer snapshot id, got {raw!r}"
                ) from error
            # Python int is unbounded; PyO3 i64 conversion would raise OverflowError
            # Gate here so callers see AnalysisException consistently.
            if parsed < _I64_MIN or parsed > _I64_MAX:
                raise AnalysisException(
                    f"snapshot-id must fit a signed 64-bit integer, got {raw!r}"
                )
            return {"snapshot_id": parsed}
        if key == "as-of-timestamp":
            try:
                parsed = int(raw)
            except ValueError as error:
                raise AnalysisException(
                    f"as-of-timestamp must be epoch milliseconds (int), got {raw!r}"
                ) from error
            if parsed < _I64_MIN or parsed > _I64_MAX:
                raise AnalysisException(
                    f"as-of-timestamp must fit a signed 64-bit integer epoch ms, got {raw!r}"
                )
            return {"as_of_timestamp_ms": parsed}
        if key == "branch":
            return {"branch": raw}
        if key == "tag":
            return {"tag": raw}
        return None

    def schema(self, schema: Any) -> DataFrameReader:
        """Set a user schema for the next load (PySpark ``DataFrameReader.schema``).

        Applied on CSV/JSON reads (cast + rename). Parquet still rejects a set schema at load.
        Accepts :class:`~repark.types.StructType`, a DDL field-list string, or a list of
        :class:`~repark.types.StructField`.
        """
        self._schema = schema
        return self
