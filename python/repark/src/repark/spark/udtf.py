"""User-defined table functions (PySpark ``pyspark.sql.udtf`` / ``functions.udtf``).

**r23 C6 / U12 — scalar-arg UDTF phase-2 core** (U11 seed):

* ``UserDefinedTableFunction(lit_args…)`` builds a one-shot relation via a synthetic
  one-row arg frame + :meth:`~repark.dataframe.DataFrame.mapInArrow` expansion of
  ``handler.eval(*args)`` iterators.
* ``spark.udtf.register(name, udtf)`` stores the handler on the session; SQL
  ``SELECT * FROM name(lit_args)`` rewrites to the same relation constructor.
* **LATERAL** / non-literal / table-arg UDTFs stay blocked (DataFusion
  ``OuterReferenceColumn`` has no physical support; U11 hour-0 evidence).
* Construction still validates Spark ``INVALID_UDTF_*`` error classes.
* Never surface ``__repark_sql_udf_*`` internal names.
"""

from __future__ import annotations

import functools
import inspect
import re
from collections.abc import Callable, Iterator
from typing import Any

from repark.errors import (
    PySparkAttributeError,
    PySparkException,
    PySparkTypeError,
    UnsupportedOperationException,
)

# === r23 C6: udtf-phase2-core ===

_LATERAL_BLOCKED_MESSAGE = (
    "LATERAL / correlated UDTF is not supported in repark v1: DataFusion has no "
    "physical OuterReferenceColumn support for LATERAL TVF. Use scalar-arg "
    "UserDefinedTableFunction(lit(...)) or SELECT * FROM registered_udtf(lit_args) "
    "(U12 phase-2 core); table-arg and LATERAL stay blocked."
)

_TABLE_ARG_BLOCKED_MESSAGE = (
    "table-argument UDTF is not supported in repark v1 (U12 phase-2 is scalar-arg "
    "only). Pass foldable lit(...) / Python scalars, or register + "
    "SELECT * FROM name(lit_args). LATERAL / TABLE args stay blocked."
)

_NON_LITERAL_BLOCKED_MESSAGE = (
    "UDTF arguments must be foldable scalar literals (lit(...)/Python scalars) in "
    "repark v1 U12; non-literal Column args require LATERAL which is blocked "
    f"({_LATERAL_BLOCKED_MESSAGE})"
)


def _validate_udtf_handler(cls: Any, return_type: Any) -> None:
    """Validate handler class with Spark error classes (INVALID_UDTF_*)."""
    if not isinstance(cls, type):
        raise PySparkTypeError(
            errorClass="INVALID_UDTF_HANDLER_TYPE",
            messageParameters={"type": type(cls).__name__},
        )
    if not hasattr(cls, "eval"):
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_NO_EVAL",
            messageParameters={"name": cls.__name__},
        )
    has_analyze = hasattr(cls, "analyze")
    has_analyze_staticmethod = has_analyze and isinstance(
        inspect.getattr_static(cls, "analyze"), staticmethod
    )
    if return_type is None and not has_analyze_staticmethod:
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_RETURN_TYPE",
            messageParameters={"name": cls.__name__},
        )
    if return_type is not None and has_analyze:
        raise PySparkAttributeError(
            errorClass="INVALID_UDTF_BOTH_RETURN_TYPE_AND_ANALYZE",
            messageParameters={"name": cls.__name__},
        )


def _resolve_return_struct(return_type: Any, *, name: str) -> Any:
    """Parse UDTF returnType into a :class:`~repark.types.StructType` (Spark shape)."""
    from repark.spark.types import StructType, _parse_datatype_string

    if return_type is None:
        raise UnsupportedOperationException(
            f"UserDefinedTableFunction({name!r}): analyze()-only UDTFs (no returnType) "
            "are not supported in repark v1 U12; declare returnType as a StructType or "
            "DDL field list (e.g. 'c1: int, c2: int')."
        )
    if isinstance(return_type, StructType):
        parsed: Any = return_type
    elif isinstance(return_type, str):
        parsed = _parse_datatype_string(return_type)
    elif hasattr(return_type, "simpleString") and hasattr(return_type, "fields"):
        # Duck-typed StructType (e.g. pyspark leftover after overlay).
        parsed = return_type
    else:
        raise PySparkTypeError(
            errorClass="UDTF_RETURN_TYPE_MISMATCH",
            messageParameters={
                "name": name,
                "return_type": f"{return_type}",
            },
        )
    if not isinstance(parsed, StructType) and not (
        hasattr(parsed, "fields") and hasattr(parsed, "simpleString")
    ):
        raise PySparkTypeError(
            errorClass="UDTF_RETURN_TYPE_MISMATCH",
            messageParameters={
                "name": name,
                "return_type": f"{parsed}",
            },
        )
    if not list(getattr(parsed, "fields", []) or []):
        raise PySparkTypeError(
            errorClass="UDTF_RETURN_TYPE_MISMATCH",
            messageParameters={
                "name": name,
                "return_type": "empty StructType",
            },
        )
    return parsed


def _return_type_to_map_schema(struct_type: Any) -> str:
    """DDL field list for mapInArrow (``name type, …``), not nested ``struct<…>``."""
    from repark.spark.types import DataType

    pieces: list[str] = []
    for field in struct_type.fields:
        data_type = field.dataType
        if isinstance(data_type, DataType) or hasattr(data_type, "simpleString"):
            token = data_type.simpleString()
        else:
            token = str(data_type)
        pieces.append(f"{field.name} {token}")
    return ", ".join(pieces)


def _is_table_arg(value: Any) -> bool:
    """Whether ``value`` is a table / TableArg-shaped UDTF input (blocked in U12)."""
    type_name = type(value).__name__
    if type_name in {"DataFrame", "TableArg"}:
        return True
    module = type(value).__module__ or ""
    if module.startswith("repark.spark.dataframe") and type_name == "DataFrame":
        return True
    return "table_arg" in module.lower()


def _coerce_scalar_arg(arg: Any, *, surface: str) -> Any:
    """Accept a foldable Column or Python scalar; refuse table / non-literal Column."""
    from repark.spark.column import Column
    from repark.spark.functions import lit

    if _is_table_arg(arg):
        raise UnsupportedOperationException(f"{surface}: {_TABLE_ARG_BLOCKED_MESSAGE}")
    if isinstance(arg, Column):
        if not bool(getattr(arg, "_is_foldable", False)):
            raise UnsupportedOperationException(f"{surface}: {_NON_LITERAL_BLOCKED_MESSAGE}")
        return arg
    # Python scalar / None / list / … → lit (foldable).
    return lit(arg)


def _normalize_eval_rows(
    result: Any,
    *,
    expected_width: int,
    surface: str,
) -> list[tuple[Any, ...]]:
    """Normalize ``eval`` return (iterator / list / single tuple) to a list of tuples.

    Each yielded row must match ``expected_width`` (declared returnType field count) —
    short/long rows refuse loud (no silent Null-pad / tail-drop; octo C1-L-003).
    """
    if result is None:
        return []
    if inspect.isgenerator(result) or isinstance(result, Iterator):
        items = list(result)
    elif isinstance(result, list):
        items = result
    else:
        items = [result]
    rows: list[tuple[Any, ...]] = []
    for item in items:
        if item is None:
            # Bare None is not a row (octo C2-Q-002) — yield (None,) / (None, None, …)
            # for null cells; refuse silent multiset shrink.
            raise PySparkException(
                f"UDTF {surface} eval() yielded None; yield a tuple of length "
                f"{expected_width} (use None cells for nulls)"
            )
        row = item if isinstance(item, tuple) else (item,)
        if len(row) != expected_width:
            raise PySparkException(
                f"UDTF {surface} eval() yielded {len(row)} column(s); "
                f"returnType declares {expected_width} field(s)"
            )
        rows.append(row)
    return rows


def _build_output_batch(
    rows: list[tuple[Any, ...]],
    field_names: list[str],
    arrow_schema: Any,
) -> Any:
    """Build one ``pyarrow.RecordBatch`` matching ``arrow_schema`` (empty-ok).

    Rows are pre-validated to ``len(field_names)`` width by :func:`_normalize_eval_rows`.
    """
    import pyarrow as pa

    _ = field_names  # width already enforced; names live on arrow_schema
    columns: list[Any] = []
    for column_index, field in enumerate(arrow_schema):
        values = [row[column_index] for row in rows]
        columns.append(pa.array(values, type=field.type))
    return pa.record_batch(columns, schema=arrow_schema)


def _map_udtf_batches(
    batches: Iterator[Any],
    *,
    handler_cls: type[Any],
    arg_count: int,
    field_names: list[str],
    arrow_schema: Any,
    surface: str,
) -> Iterator[Any]:
    """Expand one UDTF handler across streamed argument batches (mapInArrow body)."""
    import traceback

    handler = handler_cls()
    out_rows: list[tuple[Any, ...]] = []
    # start + eval share the same finally so terminate always runs after
    # construction (octo C2-Q-001) — including when start() raises.
    try:
        start = getattr(handler, "start", None)
        if callable(start):
            try:
                start()
            except Exception as error:
                detail = traceback.format_exc()
                raise PySparkException(
                    f"UDTF {surface} start() raised {type(error).__name__}: {error}\n{detail}"
                ) from error

        for batch in batches:
            for row_index in range(batch.num_rows):
                if arg_count == 0:
                    python_args: tuple[Any, ...] = ()
                else:
                    python_args = tuple(
                        batch.column(column_index)[row_index].as_py()
                        for column_index in range(arg_count)
                    )
                try:
                    result = handler.eval(*python_args)
                except PySparkException:
                    raise
                except Exception as error:
                    detail = traceback.format_exc()
                    raise PySparkException(
                        f"UDTF {surface} eval() raised {type(error).__name__}: {error}\n{detail}"
                    ) from error
                out_rows.extend(
                    _normalize_eval_rows(
                        result,
                        expected_width=len(field_names),
                        surface=surface,
                    )
                )
    finally:
        terminate = getattr(handler, "terminate", None)
        if callable(terminate):
            try:
                terminate()
            except Exception as error:
                detail = traceback.format_exc()
                raise PySparkException(
                    f"UDTF {surface} terminate() raised {type(error).__name__}: {error}\n{detail}"
                ) from error

    yield _build_output_batch(out_rows, field_names, arrow_schema)


def _execute_scalar_udtf(
    *,
    session: Any,
    handler_cls: type[Any],
    return_struct: Any,
    scalar_columns: list[Any],
    surface: str,
) -> Any:
    """Run scalar-arg UDTF via synthetic one-row frame + mapInArrow expansion."""
    from repark.spark.functions import lit

    schema_ddl = _return_type_to_map_schema(return_struct)
    # Synthetic one-row arg frame (or dummy when zero-arg).
    if scalar_columns:
        projections = [
            column.alias(f"__repark_udtf_arg_{index}")
            for index, column in enumerate(scalar_columns)
        ]
        arg_frame = session.range(1).select(*projections)
        arg_count = len(scalar_columns)
    else:
        arg_frame = session.range(1).select(lit(0).alias("__repark_udtf_dummy"))
        arg_count = 0

    # Pre-resolve arrow schema so the map bridge can type-check yields.
    from repark.spark.dataframe import _coerce_map_in_arrow_schema

    _declared, arrow_schema = _coerce_map_in_arrow_schema(schema_ddl)
    field_names = [field.name for field in return_struct.fields]
    return arg_frame.mapInArrow(
        functools.partial(
            _map_udtf_batches,
            handler_cls=handler_cls,
            arg_count=arg_count,
            field_names=field_names,
            arrow_schema=arrow_schema,
            surface=surface,
        ),
        schema_ddl,
    )


class UserDefinedTableFunction:
    """Python UDTF wrapper (PySpark ``UserDefinedTableFunction``).

    Construction validates handlers (Spark ``INVALID_UDTF_*``). Call with foldable
    scalar args produces a :class:`~repark.dataframe.DataFrame` via mapInArrow (U12).
    LATERAL / table-arg forms refuse loud.
    """

    __slots__ = ("_name", "_return_type", "deterministic", "func")

    def __init__(
        self,
        func: type[Any],
        returnType: Any = None,  # noqa: N803 — PySpark camelCase
        name: str | None = None,
        deterministic: bool = False,
    ) -> None:
        """Wrap a UDTF handler class after Spark-shaped validation."""
        _validate_udtf_handler(func, returnType)
        self.func = func
        self._return_type = returnType
        self._name = name if name is not None else getattr(func, "__name__", "udtf")
        self.deterministic = deterministic

    @property
    def returnType(self) -> Any:  # noqa: N802 — PySpark property name
        """Declared return type (StructType DDL string or object), or ``None`` for analyze."""
        return self._return_type

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke scalar-arg UDTF → DataFrame (U12 mapInArrow relation constructor).

        Keyword arguments and non-literal / table args refuse loud.
        """
        from repark.spark.session import ReparkSession

        if kwargs:
            raise UnsupportedOperationException(
                f"UserDefinedTableFunction({self._name!r}).__call__: named UDTF "
                "arguments are not supported in repark v1 U12 (scalar positional "
                f"lit args only). {_TABLE_ARG_BLOCKED_MESSAGE}"
            )
        session = ReparkSession.getActiveSession()
        if session is None or getattr(session, "_inner", None) is None:
            raise PySparkException(
                f"UserDefinedTableFunction({self._name!r}) requires an active "
                "SparkSession (call builder.getOrCreate() first)"
            )
        surface = f"UserDefinedTableFunction({self._name!r})"
        scalar_columns = [_coerce_scalar_arg(arg, surface=surface) for arg in args]
        return_struct = _resolve_return_struct(self._return_type, name=self._name)
        return _execute_scalar_udtf(
            session=session,
            handler_cls=self.func,
            return_struct=return_struct,
            scalar_columns=scalar_columns,
            surface=surface,
        )

    def asDeterministic(self) -> UserDefinedTableFunction:  # noqa: N802 — PySpark camelCase
        """Mark deterministic (accepted; no Spark codegen path)."""
        self.deterministic = True
        return self


class UDTFRegistration:
    """``spark.udtf`` namespace (PySpark ``UDTFRegistration``).

    :meth:`register` stores a :class:`UserDefinedTableFunction` for SQL
    ``SELECT * FROM name(lit_args)`` rewrite (U12). LATERAL stays blocked.
    """

    __slots__ = ("_session",)

    def __init__(self, session: Any) -> None:
        """Bind to a live session (type is duck-typed to avoid circular imports)."""
        self._session = session

    def register(self, name: str, f: Any) -> UserDefinedTableFunction:
        """Register a UDTF for SQL ``FROM name(lit_args)`` (U12).

        Validates that ``f`` is a :class:`UserDefinedTableFunction` with Spark
        ``CANNOT_REGISTER_UDTF`` when the type is wrong (handler class must be wrapped
        via :func:`udtf` / ``@udtf`` first). Overwrites an existing name (case-
        insensitive key collapse).
        """
        self._session._ensure_alive()
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("udtf register name must be a non-empty str")
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None:
            raise PySparkTypeError(
                "udtf register name must be a simple SQL identifier "
                f"([A-Za-z_][A-Za-z0-9_]*); got {name!r}"
            )
        # Never accept internal materialization namespace as a user-facing UDTF name.
        if "__repark_sql_udf" in name.lower() or "__repark_udtf" in name.lower():
            raise PySparkTypeError(
                "udtf register name must not use a reserved repark materialization "
                f"prefix (__repark_sql_udf_* / __repark_udtf_*); got {name!r}"
            )
        if not isinstance(f, UserDefinedTableFunction):
            raise PySparkTypeError(
                errorClass="CANNOT_REGISTER_UDTF",
                messageParameters={"name": name},
            )
        # Ensure returnType is resolvable at register time (fail-loud, not at SQL).
        _resolve_return_struct(f._return_type, name=name)

        registry = self._session._udtf_registry()
        name_lower = name.lower()
        for existing in list(registry.keys()):
            if existing.lower() == name_lower and existing != name:
                del registry[existing]
        registry[name] = f
        if f._name is None or f._name == getattr(f.func, "__name__", "udtf"):
            f._name = name
        return f


def udtf(
    cls: type[Any] | None = None,
    *,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    useArrow: bool | None = None,  # noqa: N803 — PySpark camelCase (accepted, ignored)
) -> UserDefinedTableFunction | Callable[[type[Any]], UserDefinedTableFunction]:
    """Create a Python UDTF (PySpark ``functions.udtf``) — decorator / direct form.

    Construction validates the handler (Spark ``INVALID_UDTF_*``). Invocation with
    scalar lit args produces a DataFrame (U12 mapInArrow core). ``spark.udtf.register``
    enables ``SELECT * FROM name(lit_args)``. LATERAL / table-arg stay blocked.

    Forms::

        @udtf(returnType="c1: int, c2: int")
        class PlusOne:
            def eval(self, x: int):
                yield x, x + 1

        PlusOne = udtf(PlusOne, returnType="c1: int, c2: int")
        PlusOne(lit(1)).show()

    ``useArrow`` is accepted for signature parity and ignored (Arrow path is always
    via mapInArrow).
    """
    _ = useArrow

    def _build(  # nested-def: decorator factory closes over returnType
        handler: type[Any],
    ) -> UserDefinedTableFunction:
        return UserDefinedTableFunction(handler, returnType=returnType)

    # @udtf / @udtf(returnType=...) — decorator form (no positional handler yet).
    if cls is None:
        return _build

    # Direct: udtf(Handler, returnType=...) — non-class raises INVALID_UDTF_HANDLER_TYPE
    # inside UserDefinedTableFunction (Spark errorClass parity).
    return _build(cls)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# SQL rewrite helpers (session.sql entry) — SELECT * FROM registered_udtf(lit_args)
# ---------------------------------------------------------------------------

# Top-level SELECT … FROM name(args) [AS alias] — no JOIN / LATERAL / multi-FROM.
_FROM_UDTF_SQL_RE = re.compile(
    r"(?is)^\s*"
    r"(SELECT\s+(?P<select>.+?)\s+)"
    r"FROM\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*"
    r"\((?P<args>.*)\)\s*"
    r"(?:(?:AS\s+)?(?P<alias>[A-Za-z_][A-Za-z0-9_]*))?\s*"
    r";?\s*$"
)


def _split_sql_literal_args(args_blob: str) -> list[Any]:
    """Parse a comma-separated list of SQL scalar literals into Python values."""
    text = args_blob.strip()
    if not text:
        return []
    values: list[Any] = []
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index] in " \t\n\r":
            index += 1
        if index >= length:
            break
        char = text[index]
        if char in ("'", '"'):
            quote = char
            index += 1
            pieces: list[str] = []
            closed = False
            while index < length:
                if text[index] == quote:
                    if index + 1 < length and text[index + 1] == quote:
                        pieces.append(quote)
                        index += 2
                        continue
                    index += 1
                    closed = True
                    break
                pieces.append(text[index])
                index += 1
            if not closed:
                # octo C1-SEC-002 / C1-L-002: refuse unclosed quotes (no silent value)
                raise PySparkTypeError(
                    f"unclosed string literal in UDTF SQL arguments near {args_blob!r}"
                )
            values.append("".join(pieces))
        else:
            start = index
            while index < length and text[index] != ",":
                index += 1
            token = text[start:index].strip()
            if not token:
                raise PySparkTypeError(f"empty UDTF SQL argument near {args_blob!r}")
            upper = token.upper()
            if upper == "NULL":
                values.append(None)
            elif upper == "TRUE":
                values.append(True)
            elif upper == "FALSE":
                values.append(False)
            else:
                try:
                    if re.fullmatch(r"[+-]?\d+", token):
                        values.append(int(token))
                    elif re.fullmatch(
                        r"[+-]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][+-]?\d+)?",
                        token,
                    ) and re.search(r"[.eE]", token):
                        # Floats: decimal and/or exponent (1.5, .5, 1e2, 1.0e-3).
                        # Pure ints already handled above; require . or e so we
                        # do not re-parse ints as float (octo C2-L-001: 1e2).
                        values.append(float(token))
                    else:
                        raise PySparkTypeError(
                            f"UDTF SQL argument must be a scalar literal "
                            f"(string/number/NULL/TRUE/FALSE); got {token!r}"
                        )
                except ValueError as error:
                    raise PySparkTypeError(
                        f"UDTF SQL argument must be a scalar literal; got {token!r}"
                    ) from error
        while index < length and text[index] in " \t\n\r":
            index += 1
        if index < length:
            if text[index] != ",":
                raise PySparkTypeError(
                    f"expected comma between UDTF SQL arguments near {text[index:]!r}"
                )
            index += 1
            # Trailing comma with no following argument (octo C2-SEC-001).
            rest = text[index:].strip()
            if rest == "":
                raise PySparkTypeError(f"trailing comma in UDTF SQL arguments near {args_blob!r}")
    return values


def _strip_sql_comments(text: str) -> str:
    """Remove ``--`` line comments and ``/* */`` block comments (best-effort)."""
    without_block = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", without_block)


def _sql_from_clause_region(body: str) -> str:
    """Return the text after the first top-level FROM until a clause keyword / end.

    Used to detect registered UDTF **table factors** without matching SELECT-list
    calls like ``SELECT id, max(1) FROM t`` (octo C5-SEC-001). Not a full SQL
    parser — good enough to scope name( hits to the FROM region. Comments are
    stripped so ``FROM t -- name(\\n`` does not false-positive.
    """
    from_match = re.search(r"(?is)\bFROM\b", body)
    if from_match is None:
        return ""
    tail = body[from_match.end() :]
    end_match = re.search(
        r"(?is)\b(?:WHERE|GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|UNION|EXCEPT|INTERSECT)\b",
        tail,
    )
    region = tail if end_match is None else tail[: end_match.start()]
    return _strip_sql_comments(region)


def _registered_udtf_in_from_region(name: str, from_region: str) -> bool:
    """Whether ``name(`` appears as a call token inside the FROM region (case-insensitive)."""
    return re.search(rf"(?is)(?<![.\w]){re.escape(name)}\s*\(", from_region) is not None


def try_sql_registered_udtf(session: Any, query: str) -> Any | None:
    """If ``query`` is ``SELECT … FROM registered_udtf(lit_args)``, return the DataFrame.

    Returns ``None`` when no registered UDTF appears as a **table factor** in the
    FROM region. LATERAL / JOIN / multi-FROM table-factor hits refuse loud (not
    silent engine-parse failure). SELECT-list calls and name mentions only inside
    string literals / comments do **not** hijack planning (octo C1-SEC-001,
    C5-SEC-001).
    """
    if not isinstance(query, str):
        return None
    registry = session._udtf_registry()
    if not registry:
        return None

    # Leading trivia strip (comments / whitespace) — match session SQL helpers.
    body = query
    trivia_match = re.match(r"(?s)^(\s*(?:--[^\n]*\n|/\*.*?\*/\s*)*)", body)
    trivia = trivia_match.group(1) if trivia_match else ""
    body = body[len(trivia) :]
    if not body.strip():
        return None

    # Case-insensitive registry keys for table-factor scans.
    registry_by_lower = {key.lower(): (key, value) for key, value in registry.items()}

    # Supported U12 form first (whole-statement match).
    match = _FROM_UDTF_SQL_RE.match(body)
    if match is not None:
        registered_name = match.group("name")
        entry_pair = registry_by_lower.get(registered_name.lower())
        if entry_pair is not None:
            _key, entry = entry_pair
            select_list = match.group("select").strip()
            args_blob = match.group("args")
            try:
                python_args = _split_sql_literal_args(args_blob)
            except PySparkTypeError as error:
                raise UnsupportedOperationException(
                    f"UDTF SQL {registered_name}(…): {error}. Only scalar SQL literals "
                    "are supported in U12 FROM-udtf rewrite."
                ) from error

            frame = entry(*python_args)
            # Optional projection when not SELECT *.
            if select_list != "*":
                # Simple comma list of identifiers only (no expressions).
                columns = [part.strip().strip('`"') for part in select_list.split(",")]
                if not columns or any(
                    not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col) for col in columns
                ):
                    raise UnsupportedOperationException(
                        f"UDTF SQL SELECT list must be * or simple column names in U12; "
                        f"got {select_list!r}"
                    )
                frame = frame.select(*columns)
            return frame
        # FROM other_name(...) — not our UDTF; fall through.

    # LATERAL … name( anywhere (lateral is always a table-factor construct).
    for name in registry:
        if re.search(rf"(?is)\bLATERAL\s+{re.escape(name)}\s*\(", body):
            raise UnsupportedOperationException(
                f"spark.sql LATERAL … {name}(…): {_LATERAL_BLOCKED_MESSAGE}"
            )

    # Unsupported shapes: registered name as a call inside the FROM region only
    # (JOIN / multi-FROM / trailing clauses). Do **not** match SELECT-list
    # ``, name(`` calls (octo C5-SEC-001).
    from_region = _sql_from_clause_region(body)
    if from_region:
        for name in registry:
            if not _registered_udtf_in_from_region(name, from_region):
                continue
            raise UnsupportedOperationException(
                f"registered UDTF {name!r} appears as a table factor but the statement "
                "is not a supported U12 form. Supported: SELECT <cols|*> FROM "
                f"name(lit_args) [AS alias]. LATERAL / JOIN / multi-FROM stay blocked. "
                f"({_LATERAL_BLOCKED_MESSAGE})"
            )

    return None


__all__ = [
    "UDTFRegistration",
    "UserDefinedTableFunction",
    "try_sql_registered_udtf",
    "udtf",
]
