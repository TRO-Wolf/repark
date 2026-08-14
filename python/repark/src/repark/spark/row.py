"""PySpark-compatible :class:`Row` — attribute / index access over a named tuple of values."""

from __future__ import annotations

from typing import Any

from repark.errors import PySparkAttributeError, PySparkValueError


class Row:
    """A single collected row (near-drop-in for ``pyspark.sql.Row``).

    Supports attribute access (``row.col``), index access (``row[0]`` / ``row["col"]`` /
    slices), membership on field names (``"col" in row``), :meth:`asDict`, value equality
    (incl. vs plain tuples of the same values — live PySpark 4.1.2; ``Row`` is a ``tuple``
    subclass there), iteration over values, :attr:`__fields__`, and a readable :func:`repr`.

    **Factory form (R-PARITY3):** ``Row("name", "age")`` when every positional argument is a
    ``str`` (and no kwargs) returns a **callable factory** (repr ``<Row('name', 'age')>``).
    Calling the factory builds a value row: ``Row("name", "age")("alice", 1)`` →
    ``Row(name='alice', age=1)``. Factories pickle/unpickle and re-call. Mixed
    ``Row("x", 1)`` is a normal positional value row (not a factory).

    Oracle basis (live PySpark 4.1.2, zulu-17): see ``tests/test_row.py`` and G-ROW /
    R-PARITY3 ledgers in ``task/todo.md``.

    Internal storage uses name-mangled slots (``__field_names`` / ``__field_values`` →
    ``_Row__…``) so user fields literally named ``_fields`` or ``_values`` still work via
    attribute access (octo C1-L-001).
    """

    __slots__ = ("__factory", "__field_names", "__field_values")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Build from kwargs, all-str factory args, empty factory, or ordered values.

        Spark 4.1.2: ``Row()`` and ``Row("a","b")`` (all-str positionals) are factories;
        ``Row("Alice", 11)`` is an unnamed value row; kwargs build named value rows.
        """
        if kwargs and args:
            raise PySparkValueError(
                "[CANNOT_SET_TOGETHER] args and kwargs should not be set together."
            )
        if kwargs:
            self.__factory = False
            self.__field_names = tuple(kwargs.keys())
            self.__field_values = tuple(kwargs.values())
            return
        # Factory: zero args OR every positional is str (Spark Row class factory form).
        if not args or all(isinstance(argument, str) for argument in args):
            self.__factory = True
            self.__field_names = tuple(args)
            self.__field_values = ()
            return
        self.__factory = False
        # Unnamed value row — synthetic names for index/asDict; repr uses angle brackets.
        self.__field_names = tuple(f"_{index}" for index in range(len(args)))
        self.__field_values = args

    def __call__(self, *values: Any, **kwargs: Any) -> Row:
        """Invoke a field-name factory: ``Row("a","b")(1, 2)`` → ``Row(a=1, b=2)``."""
        if not self.__factory:
            raise TypeError("'Row' object is not callable")
        if kwargs:
            raise TypeError("Row.__call__() got an unexpected keyword argument")
        if len(values) != len(self.__field_names):
            raise ValueError(
                f"Row factory expects {len(self.__field_names)} value(s), got {len(values)}"
            )
        if not self.__field_names:
            # Empty factory → empty named value row (repr ``Row()``).
            empty = object.__new__(Row)
            empty._Row__factory = False  # type: ignore[attr-defined]
            empty._Row__field_names = ()  # type: ignore[attr-defined]
            empty._Row__field_values = ()  # type: ignore[attr-defined]
            return empty
        bound = dict(zip(self.__field_names, values, strict=True))
        return Row(**bound)

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any]) -> Row:
        """Build a row from an ordered field→value mapping (preserves insertion order)."""
        return cls(**mapping)

    @classmethod
    def from_ordered_fields(
        cls, names: list[str] | tuple[str, ...], values: list[Any] | tuple[Any, ...]
    ) -> Row:
        """Build a value row from parallel name/value sequences (duplicate names allowed).

        # === r21 T3: ux-polish ===
        Used by export/collect when H1 multi-name display maps rename engine fields to
        Spark-legal duplicate display names — kwargs / dict paths cannot preserve dups.
        """
        if len(names) != len(values):
            raise PySparkValueError(
                f"from_ordered_fields expects len(names)==len(values), "
                f"got {len(names)} names and {len(values)} values"
            )
        row = object.__new__(cls)
        row._Row__factory = False  # type: ignore[attr-defined]
        row._Row__field_names = tuple(names)  # type: ignore[attr-defined]
        row._Row__field_values = tuple(values)  # type: ignore[attr-defined]
        return row

    @property
    def __fields__(self) -> list[str]:
        """Field names in order (live PySpark ``row.__fields__`` — a ``list`` on value rows)."""
        return list(self.__field_names)

    def __getattr__(self, name: str) -> Any:
        """Attribute access by field name (PySpark ``row.col``)."""
        try:
            index = self.__field_names.index(name)
        except ValueError as error:
            raise PySparkAttributeError(
                f"[ATTRIBUTE_NOT_SUPPORTED] Attribute `{name}` is not supported."
            ) from error
        if self.__factory:
            raise PySparkAttributeError(
                f"[ATTRIBUTE_NOT_SUPPORTED] Attribute `{name}` is not supported."
            )
        return self.__field_values[index]

    def __getitem__(self, key: Any) -> Any:
        """Index, slice, or field-name access (PySpark ``row[0]`` / ``row['col']`` / ``row[1:3]``).

        Live PySpark 4.1.2 routes non-``int``/``slice`` keys through ``__fields__.index`` and
        re-raises the resulting ``ValueError`` as :class:`~repark.errors.PySparkValueError`.
        Out-of-range int indices stay bare ``IndexError``. Slices return a plain ``tuple``.
        """
        if self.__factory:
            if isinstance(key, (int, slice)):
                return self.__field_names[key]
            try:
                return self.__field_names.index(key)
            except ValueError as error:
                raise PySparkValueError(key) from error
        if isinstance(key, (int, slice)):
            return self.__field_values[key]
        try:
            index = self.__field_names.index(key)
        except ValueError as error:
            raise PySparkValueError(key) from error
        return self.__field_values[index]

    def __contains__(self, item: Any) -> bool:
        """Field-name membership (``"col" in row``); values are NOT searched (live PySpark)."""
        return item in self.__field_names

    def asDict(self, recursive: bool = False) -> dict[str, Any]:  # noqa: N802 — PySpark name
        """Return a ``dict`` of field names to values (PySpark ``Row.asDict``).

        When ``recursive=True``, nested :class:`Row` values (and Rows inside lists/dicts) are
        converted to dicts — live PySpark 4.1.2. Factories have no values → empty dict.
        """
        if self.__factory:
            return {}
        if not recursive:
            return dict(zip(self.__field_names, self.__field_values, strict=True))

        def convert(obj: Any) -> Any:
            if isinstance(obj, Row):
                return obj.asDict(recursive=True)
            if isinstance(obj, list):
                return [convert(item) for item in obj]
            if isinstance(obj, dict):
                return {key: convert(value) for key, value in obj.items()}
            return obj

        return dict(
            zip(
                self.__field_names,
                (convert(value) for value in self.__field_values),
                strict=True,
            )
        )

    as_dict = asDict

    def __iter__(self) -> Any:
        """Iterate values in field order; factories iterate field names (live ``list(RF)``)."""
        if self.__factory:
            return iter(self.__field_names)
        return iter(self.__field_values)

    def __eq__(self, other: object) -> bool:
        """Value equality — field names ignored (live PySpark: ``Row`` compares as a tuple).

        ``Row(a=1, b=2) == Row(x=1, y=2) == (1, 2)`` are all True on PySpark 4.1.2.
        Two factories compare equal when their field-name tuples match.
        """
        if isinstance(other, Row):
            if self.__factory or other.__factory:
                return (
                    self.__factory and other.__factory and self.__field_names == other.__field_names
                )
            return self.__field_values == other.__field_values
        if self.__factory:
            return NotImplemented
        if isinstance(other, tuple):
            return self.__field_values == other
        return NotImplemented

    def __hash__(self) -> int:
        """Hash on values only (matches :meth:`__eq__`); factories hash field names."""
        if self.__factory:
            return hash(("__factory__", self.__field_names))
        return hash(self.__field_values)

    def _is_unnamed_value_row(self) -> bool:
        """True when fields are synthetic ``_0``… names (positional value row)."""
        if self.__factory:
            return False
        names = self.__field_names
        if not names:
            return False
        return all(name == f"_{index}" for index, name in enumerate(names))

    def __repr__(self) -> str:
        """Render Spark-style: named ``Row(a=1)``, factory/unnamed ``<Row(…)>``.

        Empty named value rows (from empty factory call) render ``Row()``. Nested factories
        and unnamed rows use angle brackets (Apache ``test_row_repr_with_empty_row``).
        """
        if self.__factory:
            body = ", ".join(repr(name) for name in self.__field_names)
            return f"<Row({body})>"
        if self._is_unnamed_value_row():
            body = ", ".join(repr(value) for value in self.__field_values)
            return f"<Row({body})>"
        body = ", ".join(
            f"{name}={value!r}"
            for name, value in zip(self.__field_names, self.__field_values, strict=True)
        )
        return f"Row({body})"

    def __len__(self) -> int:
        """Number of fields (factory) or values (row)."""
        if self.__factory:
            return len(self.__field_names)
        return len(self.__field_values)

    def __reduce__(self) -> Any:
        """Pickle support: factories rebuild as ``Row(*names)``; value rows preserve order.

        # === r21 T3: ux-polish ===
        Value rows use :meth:`from_ordered_fields` (not :meth:`asDict` / :meth:`from_mapping`)
        so Spark-legal duplicate display names from H1 multi-name collect rows round-trip
        without silent value loss (F-T3-002).
        """
        if self.__factory:
            return (Row, tuple(self.__field_names))
        return (
            Row.from_ordered_fields,
            (list(self.__field_names), list(self.__field_values)),
        )
