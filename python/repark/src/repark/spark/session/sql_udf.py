"""UDFRegistration."""

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


class UDFRegistration:
    """``spark.udf`` namespace (PySpark ``UDFRegistration``).

    :meth:`register` stores a classic scalar Python UDF on the session and returns the
    :class:`~repark.functions.UserDefinedFunction` callable (PySpark contract).
    :meth:`registerJavaFunction` is loud-unsupported (no JVM).
    """

    __slots__ = ("_session",)

    def __init__(self, session: ReparkSession) -> None:
        """Bind to a live :class:`ReparkSession`."""
        self._session = session

    def register(
        self,
        name: str,
        f: Any,
        returnType: Any = None,  # noqa: N803 — PySpark camelCase
    ) -> Any:
        """Register a classic scalar Python UDF for SQL + DataFrame use.

        Returns the :class:`~repark.functions.UserDefinedFunction` (PySpark returns the
        callable). ``returnType`` defaults to ``string``. Overwrites an existing name.
        """
        from repark.errors import PySparkTypeError, UnsupportedOperationException
        from repark.spark.functions import UserDefinedFunction, _build_python_udf, udf
        from repark.spark.types import StringType

        self._session._ensure_alive()
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("udf register name must be a non-empty str")
        # Simple SQL identifier only — SQL rewrite + registry scan require bare idents.
        # DF use of the returned callable does not need the name.
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) is None:
            raise PySparkTypeError(
                "udf register name must be a simple SQL identifier "
                f"([A-Za-z_][A-Za-z0-9_]*); got {name!r}"
            )
        # Reserved internal SQL-UDF materialization namespace. User-chosen names containing
        # this prefix would surface as schema columns and defeat the never-leak
        # ``__repark_sql_udf_*`` bar.
        if "__repark_sql_udf" in name.lower():
            raise PySparkTypeError(
                "udf register name must not use the reserved repark SQL UDF "
                f"materialization prefix (__repark_sql_udf_*); got {name!r}"
            )
        if not callable(f):
            raise PySparkTypeError(f"udf register f must be callable, got {type(f).__name__}")
        # Reject pandas_udf wrappers — SQL registry is classic scalar only.
        if type(f).__name__ == "PandasUDFFunction":
            raise UnsupportedOperationException(
                "spark.udf.register does not accept pandas_udf callables in repark v1; "
                "register a classic scalar Python function (or use F.udf). "
                "pandas_udf stays on the DataFrame path (M6)."
            )
        # Reject table UDTF wrappers — callable but not a scalar UDF. Without this, register
        # appears to succeed and only fails at invocation.
        from repark.spark.udtf import UserDefinedTableFunction

        if isinstance(f, UserDefinedTableFunction):
            raise PySparkTypeError(
                f"spark.udf.register({name!r}) does not accept UserDefinedTableFunction "
                "(table UDTF). Use spark.udtf.register / @udtf for table functions "
                "(U12 scalar-arg core), or pass a scalar Python callable."
            )
        if isinstance(f, UserDefinedFunction):
            user_defined = f
            if returnType is not None:
                # Rebuild with the explicit returnType (Spark allows override on register).
                user_defined = _build_python_udf(
                    f._user_func,
                    returnType,
                    name=name,
                )
                # Preserve the nondeterministic mark across the returnType rebuild.
                if not f.deterministic:
                    user_defined.asNondeterministic()
            else:
                user_defined = UserDefinedFunction(
                    f._user_func,
                    f._return_type_sql,
                    name=name,
                    deterministic=f.deterministic,
                )
        else:
            resolved = returnType if returnType is not None else StringType()
            user_defined = _build_python_udf(f, resolved, name=name)

        registry = self._session._udf_registry()
        # Case-insensitive overwrite: SQL resolution is case-insensitive, so two keys
        # differing only by case would silently pick dict-iteration order.
        name_lower = name.lower()
        for existing in list(registry.keys()):
            if existing.lower() == name_lower and existing != name:
                del registry[existing]
        registry[name] = {
            "func": user_defined._user_func,
            "return_type_sql": user_defined._return_type_sql,
            "udf": user_defined,
        }
        # Return callable — PySpark contract (also usable as F.udf-style on DataFrames).
        _ = udf  # keep import used for type symmetry / future dual-path
        return user_defined

    def registerJavaFunction(  # noqa: N802 — PySpark camelCase
        self,
        name: str,
        javaClassName: str,  # noqa: N803 — PySpark camelCase
        returnType: Any = None,  # noqa: N803 — PySpark camelCase
    ) -> None:
        """Loud refuse — repark has no JVM (PySpark ``registerJavaFunction``)."""
        from repark.errors import UnsupportedOperationException

        _ = (name, javaClassName, returnType)
        raise UnsupportedOperationException(
            "spark.udf.registerJavaFunction is not supported in repark "
            "(no JVM; register a Python callable via spark.udf.register / F.udf instead)"
        )

    def registerJavaUDAF(  # noqa: N802 — PySpark camelCase
        self,
        name: str,
        javaClassName: str,  # noqa: N803 — PySpark camelCase
    ) -> None:
        """Loud refuse — repark has no JVM (PySpark ``registerJavaUDAF``)."""
        from repark.errors import UnsupportedOperationException

        _ = (name, javaClassName)
        raise UnsupportedOperationException(
            "spark.udf.registerJavaUDAF is not supported in repark "
            "(no JVM; aggregate pandas_udf / engine aggregates only)"
        )
