"""The :class:`Column` facade — a thin, typed wrapper over the native ``PyColumn``.

A :class:`Column` holds a native ``repark._native.PyColumn`` (a DataFusion expression) and presents
PySpark's operator surface: arithmetic (``+ - * /``), comparison (``== != < > <= >=``), logical
(``& | ~``), plus :meth:`alias`, :meth:`cast`, and the ordering markers :meth:`asc` / :meth:`desc`.
Columns are immutable — every operator returns a new :class:`Column`, matching PySpark. Build one
with :func:`repark.functions.col` / :func:`repark.functions.lit` / :func:`repark.functions.expr`.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from repark import _native

# === r23 QI1: idents ===
from repark._idents import quote_ident as _quote_sql_field_ident
from repark.errors import AnalysisException, ParseException, PySparkTypeError, PySparkValueError

if TYPE_CHECKING:
    from repark.types import DataType
    from repark.window import WindowSpec

# A scalar that may stand in for a Column on the right of an operator (``col("a") + 1``); it is
# wrapped with ``lit`` before crossing the boundary.
Scalar = int | float | str | bool | None

# Exact ``decimal(p,s)`` shape after :func:`_normalize_type_string` (digits only). Used to
# reject hostile cast suffixes before they are embedded into generator unnest SQL
# (octo C4-SEC-001 / C4-L-002).
_DECIMAL_CAST_TYPE_RE = re.compile(r"^decimal\(\d+,\d+\)$")


class Column:
    """A column expression (near-drop-in for ``pyspark.sql.column.Column``).

    Wrap a native ``PyColumn``. The optional sort markers are set only by :meth:`asc` / :meth:`desc`
    and are read by :meth:`repark.dataframe.DataFrame.orderBy`; on a plain column they are ``None``
    (interpreted as ascending, PySpark's default).
    """

    __slots__ = (
        "_agg_name",
        "_g2_range_order_names",
        "_generator",
        "_generator_cast",
        "_has_free_attribute",
        "_has_ungroupable",
        "_inner",
        "_is_aggregate",
        "_is_aggregate_function",
        "_is_foldable",
        # === r20 H1: join/identity ===
        "_join_sql_expr",
        "_origin_field",
        "_origin_plan_id",
        "_partition_transform",
        "_projection_name",
        "_sort_ascending",
        "_sort_nulls_first",
        "_spark_display",
        "_sql_expr",
        "_stable_name",
        "_when_pairs",
        # === r23b N2: plan-collapse ===
        # Retained after ``.over(WindowSpec)`` for adjacent same-spec withColumn(s) merge.
        # Alias / for_select / ``round`` wrappers preserve it (Q15 same-layer wraps).
        "_window_spec",
    )

    def __init__(
        self,
        inner: Any,
        *,
        sort_ascending: bool | None = None,
        sort_nulls_first: bool | None = None,
        when_pairs: list[tuple[Column, Column]] | None = None,
        agg_name: str | None = None,
        is_aggregate: bool | None = None,
        is_foldable: bool = False,
        has_free_attribute: bool = False,
        has_ungroupable: bool = False,
        is_aggregate_function: bool | None = None,
        generator: str | None = None,
        generator_cast: str | tuple[str, ...] | None = None,
        spark_display: str | None = None,
        projection_name: str | None = None,
        stable_name: bool = False,
        partition_transform: str | None = None,
        sql_expr: str | None = None,
        origin_plan_id: str | None = None,
        origin_field: str | None = None,
        join_sql_expr: str | None = None,
        g2_range_order_names: list[str] | None = None,
        window_spec: WindowSpec | None = None,
    ) -> None:
        """Wrap a native ``PyColumn`` (a Rust pyclass, hence untyped), carrying sort markers.

        ``agg_name`` is the PySpark default output name for an aggregate column (``"sum(x)"``,
        ``"count"``, …), set by the :mod:`repark.functions` aggregate builders and read by
        :meth:`repark.dataframe.GroupedData.agg` to alias each aggregate to its Spark name. Any
        other operator (:meth:`alias`, arithmetic, …) returns a fresh :class:`Column` with
        ``agg_name`` reset to ``None`` — so an explicit ``.alias(...)`` overrides the default.

        ``is_aggregate`` is sticky aggregate identity for :meth:`~repark.dataframe.DataFrame.select`
        global-agg routing (R-SELECT-GLOBAL-AGG / octo C1-Q-001). True for ``F.sum``/… builders
        (via ``agg_name``) and preserved across ``.alias``, ``cast``, arithmetic, unary ops, and
        null checks so ``df.select(F.sum("x") + 1)`` still routes as a global aggregate. Cleared
        by :meth:`over` (window aggregates are not GROUP BY aggregates).

        ``is_foldable`` marks constant/literal expressions (``F.lit``, pure-literal arithmetic).
        Spark allows foldables beside aggregates in an ungrouped ``select``; the select
        classifier treats aggregate-or-foldable lists as global agg, not
        ``[MISSING_GROUP_BY]`` (octo C1-Q-002).

        ``has_free_attribute`` is sticky free (non-aggregated) column-ref identity for
        select routing (octo C2-Q-001 / C2-L-001). True for bare ``F.col`` / ``df["x"]``
        and OR-propagated across binary / when / coalesce / scalar wrappers. Aggregate
        builders absorb their arguments (result leaves this False). An expression that is
        both aggregate and free-attribute (``sum(x) + id``) must raise
        ``[MISSING_GROUP_BY]`` — sticky ``_is_aggregate`` alone is not enough.

        ``has_ungroupable`` is sticky non-groupable identity for analytics / generators
        (window ``.over(...)``, ``F.rand()``) that are neither foldable nor free attrs
        (octo C7-L-002). OR-propagated like free so nested ``sum(x)+row_number().over(...)``
        / ``coalesce(sum, window)`` raise ``[MISSING_GROUP_BY]`` instead of pure_global
        (list-level window companions were fixed in C6-L-001; nested composition needs
        this bit).

        ``is_aggregate_function`` marks a bare AggregateFunction (``F.sum``/… builders)
        acceptable to native ``DataFrame.aggregate``. Preserved only across ``.alias`` /
        ``for_select`` / sort markers — cleared by cast / arithmetic / scalar wrappers so
        those take the SQL global-agg path (octo C2-Q-002 fallout).

        ``spark_display`` is the PySpark-style expression string embedded in aggregate output
        names (``sum((x + 1))``, ``sum(CAST(x AS DOUBLE))``, ``sum(x AS y)``). Tracked on the
        facade so compound expressions never leak DataFusion's ``Int64(1)`` rendering. When
        unset, :meth:`spark_display_part` falls back to the native ``display_name()``.

        ``projection_name`` is the name applied at the :meth:`~repark.dataframe.DataFrame.select`
        boundary when the user gave no explicit alias (Group H). It matches live PySpark's
        projection display (``(x + 1)``, ``negative(x)``, …) and is **not** always identical
        to ``spark_display`` — a plain cast of a named attribute keeps the child name
        (``df.x.cast("double")`` → ``"x"``) even though the agg embed is
        ``CAST(x AS DOUBLE)``.

        ``stable_name`` is true for bare column references and user ``.alias(...)`` results
        (Spark ``NamedExpression``). :meth:`cast` preserves that name; compound ops clear it.

        ``partition_transform`` is set only by ``F.years`` / ``F.months`` / ``F.days`` /
        ``F.hours`` (Group I) — the SQL fragment for ``writeTo(...).partitionedBy(...)``.
        Sticky across derived Columns so the transform still fails loud outside
        ``partitionedBy`` (Spark ``PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY``).

        ``origin_plan_id`` / ``origin_field`` (H1 Group H): set when this Column is a pure
        schema bind from a DataFrame (``df["x"]`` / ``df.x``). Join conditions and
        post-join ``select``/``drop`` resolve the correct side via these tokens. Cleared
        on compound ops (binary/arithmetic); preserved across ``.alias`` / ``for_select``.

        ``join_sql_expr`` (H1): composed join-ON SQL with ``__REPARK_QCOL_*`` tokens so
        ``df1.b == df2.b`` stays side-qualified through binary ops without polluting
        free-SQL ``sql_expr`` (groupBy / MERGE / global-agg).
        """
        self._inner = inner
        self._sort_ascending = sort_ascending
        self._sort_nulls_first = sort_nulls_first
        self._when_pairs = when_pairs
        self._agg_name = agg_name
        # True for aggregate expressions even after .alias() clears _agg_name
        # (alias must not re-apply default agg output names — GroupedData contract).
        self._is_aggregate = bool(agg_name is not None if is_aggregate is None else is_aggregate)
        # Aggregates are never foldable constants; OR-propagation on ops clears foldable.
        self._is_foldable = bool(is_foldable) and not self._is_aggregate
        # Free attrs and aggregates can both be True on composed exprs (sum(x)+id).
        self._has_free_attribute = bool(has_free_attribute)
        # Window / non-deterministic generators: sticky across composition (octo C7-L-002).
        self._has_ungroupable = bool(has_ungroupable)
        # Bare AggregateFunction only (builders); default True when agg_name is set.
        if is_aggregate_function is None:
            self._is_aggregate_function = bool(agg_name is not None)
        else:
            self._is_aggregate_function = bool(is_aggregate_function) and self._is_aggregate
        self._generator = generator  # explode / explode_outer / posexplode
        # Optional SQL cast type(s) applied *after* unnest (``explode(...).cast("string")``).
        # Sticky across further casts; rewrite wraps ``CAST(unnest(...) AS <type>)``.
        # A tuple is an ordered cast chain (innermost first) for ``.cast().cast()``
        # composition — overwriting would drop intermediate types (octo C5-L-003).
        self._generator_cast = generator_cast if generator is not None else None
        self._spark_display = spark_display
        # Default projection_name to spark_display when the caller only set the latter
        # (binary ops, lit, coalesce, …) so select aliases without duplicating every call site.
        if projection_name is None and spark_display is not None:
            self._projection_name = spark_display
        else:
            self._projection_name = projection_name
        self._stable_name = stable_name
        self._partition_transform = partition_transform
        # Optional SQL fragment for surfaces that embed Columns into SQL text (e.g. MERGE
        # assignments). Distinct from ``spark_display``: string literals are unquoted in
        # display names but must be quoted in SQL. Unset → fall back to spark_display_part().
        self._sql_expr = sql_expr
        # === r20 H1: join/identity ===
        self._origin_plan_id = origin_plan_id
        self._origin_field = origin_field
        self._join_sql_expr = join_sql_expr
        # === r20 G2: window/rand/sampleBy ===
        # Simple ORDER BY column names for value-offset RANGE numeric-type check at select.
        self._g2_range_order_names = list(g2_range_order_names) if g2_range_order_names else None
        # === r23b N2: plan-collapse ===
        self._window_spec = window_spec

    def sql_expr_part(self) -> str:
        """SQL fragment for embedding this column into a generated SQL statement."""
        if self._sql_expr is not None:
            return self._sql_expr
        return self.spark_display_part()

    def join_sql_part(self) -> str:
        """SQL fragment for join ON rewrite (H1) — origin-qualified tokens when present."""
        if self._join_sql_expr is not None:
            return self._join_sql_expr
        if self._origin_plan_id is not None and self._origin_field is not None:
            # Local import-free token: plan_id is hex; field encoded length-safe.
            field_enc = self._origin_field.replace("\\", "\\\\").replace("\n", "\\n")
            field_enc = field_enc.replace("__", "\\_\\_")
            return f"__REPARK_QCOL_{self._origin_plan_id}__{field_enc}__"
        return self.sql_expr_part()

    def sql_expr_without_alias(self) -> str:
        """SQL fragment with a trailing NamedExpression ``AS name`` stripped (if present).

        ``Column.alias`` embeds ``… AS name`` into ``sql_expr`` for MERGE/select surfaces.
        Generator rewrites (``unnest`` / ``WHERE array_length(…)``) need the bare array
        expression only — never an illegal ``AS`` inside ``unnest(...)`` (octo C1-Q-005).
        """
        text = self.sql_expr_part()
        if not self._stable_name or self._projection_name is None:
            return text
        suffix = f" AS {self._projection_name}"
        if text.endswith(suffix):
            return text[: -len(suffix)]
        return text

    @staticmethod
    def _to_column(value: Column | Scalar) -> Column:
        """Coerce a Column-or-scalar operand into a :class:`Column` (scalars via ``lit``)."""
        if isinstance(value, Column):
            return value
        from repark.functions import lit  # local import avoids circular at module load

        return lit(value)

    def spark_display_part(self) -> str:
        """PySpark-style name fragment for this expression (aggregate output-name building)."""
        if self._spark_display is not None:
            return self._spark_display
        return self._inner.display_name()

    def spark_wrap_display_part(self) -> str:
        """Child fragment when this column is embedded inside an outer expression display.

        User ``.alias("v")`` stores ``spark_display`` as ``… AS v`` so aggregate arguments
        keep Spark's ``sum(x AS y)`` form via :meth:`spark_display_part`. Outer wrappers
        (``round`` / ``abs`` / arithmetic / cast / ``_scalar``) collapse that NamedExpression
        to the projection name so ``.alias("v").round(2)`` displays ``round(v, 2)`` rather
        than ``round((id * 1.234) AS v, 2)`` (H2 Group H naming polish).
        """
        if (
            self._stable_name
            and self._projection_name is not None
            and self._spark_display is not None
            and self._spark_display.endswith(f" AS {self._projection_name}")
        ):
            return self._projection_name
        return self.spark_display_part()

    def _reject_nested_generator(self, operation: str) -> None:
        """Refuse ops that would drop ``_generator`` and silently skip unnest (octo C4-L-001).

        Spark rejects nested generators (``UNSUPPORTED_GENERATOR``). Alias / cast / asc / desc
        keep the generator sticky on purpose; every other Column operator must fail loud rather
        than return a plain Column that ``select`` no longer rewrites.
        """
        if self._generator is not None:
            raise AnalysisException(
                f"[UNSUPPORTED_GENERATOR] The generator expression {self._generator!r} "
                f"cannot be nested inside {operation} "
                "(Spark: only one explode/explode_outer as a top-level select projection; "
                "posexplode is unsupported)."
            )

    def _binary(
        self,
        other: Column | Scalar,
        op_method: str,
        spark_op: str,
    ) -> Column:
        """Apply a binary native op and parenthesize the Spark display like PySpark does."""
        self._reject_nested_generator(f"binary op {spark_op!r}")
        right = self._to_column(other)
        right._reject_nested_generator(f"binary op {spark_op!r}")
        native = getattr(self._inner, op_method)(right._inner)
        # H2: collapse aliased children so ``.alias("v") + 1`` displays ``(v + 1)``.
        display = f"({self.spark_wrap_display_part()} {spark_op} {right.spark_wrap_display_part()})"
        # Track SQL embedding so MERGE / free-SQL surfaces quote lit operands (octo C1-Q-005).
        sql_expr = f"({self.sql_expr_part()} {spark_op} {right.sql_expr_part()})"
        # H1: parallel join-ON fragment with origin tokens (df1.b == df2.b stays sided).
        join_sql_expr = f"({self.join_sql_part()} {spark_op} {right.join_sql_part()})"
        # Sticky aggregate identity (OR): ``sum(x) + 1`` remains an aggregate for select routing.
        # Sticky free attribute (OR): ``sum(x) + id`` is both aggregate and free → MISSING_GROUP_BY.
        # Sticky ungroupable (OR): ``sum + row_number().over(...)`` → MISSING_GROUP_BY (C7-L-002).
        is_aggregate = self._is_aggregate or right._is_aggregate
        is_foldable = self._is_foldable and right._is_foldable and not is_aggregate
        has_free_attribute = self._has_free_attribute or right._has_free_attribute
        has_ungroupable = self._has_ungroupable or right._has_ungroupable
        return Column(
            native,
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
            partition_transform=self._partition_transform or right._partition_transform,
        )

    # ---- arithmetic -------------------------------------------------------------------------

    def __add__(self, other: Column | Scalar) -> Column:
        """``self + other`` (PySpark ``Column.__add__``)."""
        return self._binary(other, "add", "+")

    def __sub__(self, other: Column | Scalar) -> Column:
        """``self - other`` (PySpark ``Column.__sub__``)."""
        return self._binary(other, "sub", "-")

    def __mul__(self, other: Column | Scalar) -> Column:
        """``self * other`` (PySpark ``Column.__mul__``)."""
        return self._binary(other, "mul", "*")

    def __truediv__(self, other: Column | Scalar) -> Column:
        """``self / other`` (PySpark ``Column.__truediv__``)."""
        return self._binary(other, "div", "/")

    def __radd__(self, other: Scalar) -> Column:
        """``other + self`` — scalar on the left (PySpark ``Column.__radd__``).

        PySpark commutes reflected ``+`` (``2 + x`` names as ``(x + 2)``, live 4.1.2); the
        value is unchanged by commutativity, so mirror the name too.
        """
        return self._binary(other, "add", "+")

    def __rsub__(self, other: Scalar) -> Column:
        """``other - self`` — scalar on the left (PySpark ``Column.__rsub__``)."""
        return self._to_column(other)._binary(self, "sub", "-")

    def __rmul__(self, other: Scalar) -> Column:
        """``other * self`` — scalar on the left (PySpark ``Column.__rmul__``).

        PySpark commutes reflected ``*`` (``2 * x`` names as ``(x * 2)``, live 4.1.2) —
        same rule as :meth:`__radd__`; reflected ``-`` and ``/`` do NOT commute.
        """
        return self._binary(other, "mul", "*")

    def __rtruediv__(self, other: Scalar) -> Column:
        """``other / self`` — scalar on the left (PySpark ``Column.__rtruediv__``)."""
        return self._to_column(other)._binary(self, "div", "/")

    def __mod__(self, other: Column | Scalar) -> Column:
        """``self % other`` — modulo (PySpark ``Column.__mod__``)."""
        return self._binary(other, "modulo", "%")

    def __rmod__(self, other: Scalar) -> Column:
        """``other % self`` — scalar on the left (PySpark ``Column.__rmod__``)."""
        return self._to_column(other)._binary(self, "modulo", "%")

    def __neg__(self) -> Column:
        """Unary minus (PySpark ``Column.__neg__`` → ``-col``).

        Value via ``lit(0) - self`` (existing binary ``sub``; no native unary-minus API).
        Display name is the live-recorded PySpark form ``negative(x)`` (not ``(- x)`` /
        ``(0 - x)``). The native expression is **aliased** to that display string so
        ``df.select(-df.x).columns == ['negative(x)']`` and nested forms compose
        (``-(-x)`` → ``negative(negative(x))``, ``F.sum(-df.x)`` → ``sum(negative(x))``).
        """
        from repark.functions import lit  # local import avoids circular at module load

        self._reject_nested_generator("unary minus")
        native = lit(0)._inner.sub(self._inner)
        display = f"negative({self.spark_wrap_display_part()})"
        sql_expr = f"(-({self.sql_expr_part()}))"
        # Alias native so even pre-Group-H paths see the Spark name; projection_name too.
        return Column(
            native.alias(display),
            spark_display=display,
            projection_name=display,
            sql_expr=sql_expr,
            stable_name=False,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    def __repr__(self) -> str:
        """Render as ``Column<'expr'>`` (PySpark ``Column.__repr__``)."""
        return f"Column<'{self.spark_display_part()}'>"

    # ---- comparison -------------------------------------------------------------------------

    def __eq__(self, other: object) -> Column:  # type: ignore[override]
        """``self == other`` — a boolean :class:`Column` (PySpark ``Column.__eq__``), not a bool."""
        # Spark display uses SQL ``=`` (not Python ``==``) — live-recorded PySpark 4.1.2.
        return self._binary(other, "eq", "=")  # type: ignore[arg-type]

    def __ne__(self, other: object) -> Column:  # type: ignore[override]
        """``self != other`` — a boolean :class:`Column` (PySpark ``Column.__ne__``)."""
        # Live PySpark renders ``!=`` as ``NOT (left = right)``, not ``<>`` / ``!=``.
        self._reject_nested_generator("!=")
        right = self._to_column(other)  # type: ignore[arg-type]
        right._reject_nested_generator("!=")
        native = self._inner.ne(right._inner)
        display = f"(NOT ({self.spark_wrap_display_part()} = {right.spark_wrap_display_part()}))"
        sql_expr = f"(NOT ({self.sql_expr_part()} = {right.sql_expr_part()}))"
        join_sql_expr = f"(NOT ({self.join_sql_part()} = {right.join_sql_part()}))"
        is_aggregate = self._is_aggregate or right._is_aggregate
        is_foldable = self._is_foldable and right._is_foldable and not is_aggregate
        has_free_attribute = self._has_free_attribute or right._has_free_attribute
        has_ungroupable = self._has_ungroupable or right._has_ungroupable
        return Column(
            native,
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
            partition_transform=self._partition_transform or right._partition_transform,
        )

    def __lt__(self, other: Column | Scalar) -> Column:
        """``self < other`` (PySpark ``Column.__lt__``)."""
        return self._binary(other, "lt", "<")

    def __gt__(self, other: Column | Scalar) -> Column:
        """``self > other`` (PySpark ``Column.__gt__``)."""
        return self._binary(other, "gt", ">")

    def __le__(self, other: Column | Scalar) -> Column:
        """``self <= other`` (PySpark ``Column.__le__``)."""
        return self._binary(other, "le", "<=")

    def __ge__(self, other: Column | Scalar) -> Column:
        """``self >= other`` (PySpark ``Column.__ge__``)."""
        return self._binary(other, "ge", ">=")

    # ---- logical ----------------------------------------------------------------------------

    def __and__(self, other: Column | Scalar) -> Column:
        """``self & other`` — boolean AND (PySpark ``Column.__and__``)."""
        # Spark uppercase AND/OR (live-recorded).
        return self._binary(other, "and_", "AND")

    def __rand__(self, other: Column | Scalar) -> Column:
        """``other & self`` — reflected boolean AND (``True & col``)."""
        return self._to_column(other)._binary(self, "and_", "AND")

    def __or__(self, other: Column | Scalar) -> Column:
        """``self | other`` — boolean OR (PySpark ``Column.__or__``)."""
        return self._binary(other, "or_", "OR")

    def __ror__(self, other: Column | Scalar) -> Column:
        """``other | self`` — reflected boolean OR (``True | col``)."""
        return self._to_column(other)._binary(self, "or_", "OR")

    def __pow__(self, other: Column | Scalar) -> Column:
        """``self ** other`` — power (PySpark ``Column.__pow__``)."""
        from repark.functions import pow as spark_pow

        self._reject_nested_generator("power")
        right = self._to_column(other)
        right._reject_nested_generator("power")
        return spark_pow(self, right)

    def __rpow__(self, other: Column | Scalar) -> Column:
        """``other ** self`` — reflected power (``2 ** col``)."""
        from repark.functions import pow as spark_pow

        self._reject_nested_generator("power")
        left = self._to_column(other)
        left._reject_nested_generator("power")
        return spark_pow(left, self)

    def between(
        self,
        lowerBound: Column | Scalar,  # noqa: N803 — PySpark arg name
        upperBound: Column | Scalar,  # noqa: N803 — PySpark arg name
    ) -> Column:
        """``lowerBound <= self <= upperBound`` (PySpark ``Column.between``)."""
        self._reject_nested_generator("between")
        lower = self._to_column(lowerBound)
        upper = self._to_column(upperBound)
        lower._reject_nested_generator("between")
        upper._reject_nested_generator("between")
        return (self >= lower) & (self <= upper)

    def eqNullSafe(self, other: Column | Scalar) -> Column:  # noqa: N802 — PySpark camelCase
        """Null-safe equality (``IS NOT DISTINCT FROM``; PySpark ``Column.eqNullSafe``)."""
        self._reject_nested_generator("eqNullSafe")
        right = self._to_column(other)
        right._reject_nested_generator("eqNullSafe")
        display = f"({self.spark_wrap_display_part()} <=> {right.spark_wrap_display_part()})"
        sql_expr = f"({self.sql_expr_part()} IS NOT DISTINCT FROM {right.sql_expr_part()})"
        join_sql_expr = f"({self.join_sql_part()} IS NOT DISTINCT FROM {right.join_sql_part()})"
        is_aggregate = self._is_aggregate or right._is_aggregate
        is_foldable = self._is_foldable and right._is_foldable and not is_aggregate
        has_free_attribute = self._has_free_attribute or right._has_free_attribute
        has_ungroupable = self._has_ungroupable or right._has_ungroupable
        return Column(
            _native.PyColumn.call_scalar("eq_null_safe", [self._inner, right._inner]),
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
            partition_transform=self._partition_transform or right._partition_transform,
        )

    def contains(self, other: Column | Scalar) -> Column:
        """Substring containment (PySpark ``Column.contains``)."""
        return self._string_predicate("contains", other)

    def substr(self, startPos: Column | int, length: Column | int) -> Column:  # noqa: N803
        """Substring slice (PySpark ``Column.substr``).

        # === r21 T7: census-r6 ===
        Spark 1-based positions; ``startPos=0`` is treated as 1 (owned substring UDF).
        ``startPos`` and ``length`` must share a type (both int or both Column) — same
        checks as classic ``Column.__getitem__`` slice path.
        """
        self._reject_nested_generator("substr")
        from repark.functions import lit

        start = startPos
        stop = length
        if type(start) is not type(stop):
            raise PySparkTypeError(
                errorClass="NOT_SAME_TYPE",
                messageParameters={
                    "arg_name1": "startPos",
                    "arg_name2": "length",
                    "arg_type1": type(start).__name__,
                    "arg_type2": type(stop).__name__,
                },
            )
        if isinstance(start, int):
            start_col = lit(int(start))
            length_col = lit(int(stop))
            start_display: Any = start
            length_display: Any = stop
        elif isinstance(start, Column):
            start_col = start
            length_col = stop  # type: ignore[assignment]
            start_display = start.spark_wrap_display_part()
            length_display = length_col.spark_wrap_display_part()
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_INT",
                messageParameters={
                    "arg_name": "startPos",
                    "arg_type": type(start).__name__,
                },
            )
        display = f"substr({self.spark_wrap_display_part()}, {start_display}, {length_display})"
        return Column(
            _native.PyColumn.call_scalar(
                "substr",
                [self._inner, start_col._inner, length_col._inner],
            ),
            spark_display=display,
            sql_expr=(
                f"substr({self.sql_expr_part()}, {start_col.sql_expr_part()}, "
                f"{length_col.sql_expr_part()})"
            ),
            has_free_attribute=self._has_free_attribute,
            is_foldable=self._is_foldable and not self._is_aggregate,
            is_aggregate=self._is_aggregate,
            has_ungroupable=self._has_ungroupable,
        )

    def startswith(self, other: Column | Scalar) -> Column:
        """Prefix test (PySpark ``Column.startswith``)."""
        return self._string_predicate("starts_with", other, display_name="startswith")

    def endswith(self, other: Column | Scalar) -> Column:
        """Suffix test (PySpark ``Column.endswith``)."""
        return self._string_predicate("ends_with", other, display_name="endswith")

    def like(self, other: Column | Scalar) -> Column:
        """SQL ``LIKE`` (PySpark ``Column.like``)."""
        return self._string_predicate("like", other)

    def ilike(self, other: Column | Scalar) -> Column:
        """Case-insensitive ``LIKE`` (PySpark ``Column.ilike``)."""
        return self._string_predicate("ilike", other)

    def rlike(self, other: Column | Scalar) -> Column:
        """Regex match (PySpark ``Column.rlike``)."""
        return self._string_predicate("rlike", other)

    def _string_predicate(
        self,
        call_name: str,
        other: Column | Scalar,
        *,
        display_name: str | None = None,
    ) -> Column:
        """Unary string predicate against a pattern/substring Column-or-scalar."""
        shown = display_name or call_name
        self._reject_nested_generator(shown)
        right = self._to_column(other)
        right._reject_nested_generator(shown)
        display = f"{self.spark_wrap_display_part()}.{shown}({right.spark_wrap_display_part()})"
        sql_expr = f"{call_name}({self.sql_expr_part()}, {right.sql_expr_part()})"
        is_aggregate = self._is_aggregate or right._is_aggregate
        is_foldable = self._is_foldable and right._is_foldable and not is_aggregate
        has_free_attribute = self._has_free_attribute or right._has_free_attribute
        has_ungroupable = self._has_ungroupable or right._has_ungroupable
        return Column(
            _native.PyColumn.call_scalar(call_name, [self._inner, right._inner]),
            spark_display=display,
            sql_expr=sql_expr,
            stable_name=False,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
            partition_transform=self._partition_transform or right._partition_transform,
        )

    def bitwiseAND(self, other: Column | Scalar) -> Column:  # noqa: N802 — PySpark camelCase
        """Bitwise AND (PySpark ``Column.bitwiseAND``)."""
        return self._bitwise("bitwise_and", other, "&")

    def bitwiseOR(self, other: Column | Scalar) -> Column:  # noqa: N802 — PySpark camelCase
        """Bitwise OR (PySpark ``Column.bitwiseOR``)."""
        return self._bitwise("bitwise_or", other, "|")

    def bitwiseXOR(self, other: Column | Scalar) -> Column:  # noqa: N802 — PySpark camelCase
        """Bitwise XOR (PySpark ``Column.bitwiseXOR``)."""
        return self._bitwise("bitwise_xor", other, "^")

    def _bitwise(self, call_name: str, other: Column | Scalar, spark_op: str) -> Column:
        """Bitwise binary op lowered through native ``call_scalar``."""
        self._reject_nested_generator(f"bitwise {spark_op}")
        right = self._to_column(other)
        right._reject_nested_generator(f"bitwise {spark_op}")
        display = f"({self.spark_wrap_display_part()} {spark_op} {right.spark_wrap_display_part()})"
        sql_expr = f"({self.sql_expr_part()} {spark_op} {right.sql_expr_part()})"
        is_aggregate = self._is_aggregate or right._is_aggregate
        is_foldable = self._is_foldable and right._is_foldable and not is_aggregate
        has_free_attribute = self._has_free_attribute or right._has_free_attribute
        has_ungroupable = self._has_ungroupable or right._has_ungroupable
        return Column(
            _native.PyColumn.call_scalar(call_name, [self._inner, right._inner]),
            spark_display=display,
            sql_expr=sql_expr,
            stable_name=False,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
            partition_transform=self._partition_transform or right._partition_transform,
        )

    def __invert__(self) -> Column:
        """``~self`` — boolean NOT (PySpark ``Column.__invert__``)."""
        self._reject_nested_generator("boolean NOT")
        display = f"(NOT {self.spark_wrap_display_part()})"
        sql_expr = f"(NOT {self.sql_expr_part()})"
        join_sql_expr = f"(NOT {self.join_sql_part()})"
        return Column(
            self._inner.not_(),
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    # ---- Python-boolean misuse guards ---------------------------------------------------------

    def __bool__(self) -> bool:
        """Raise — a Column has no truth value (PySpark ``Column.__bool__``).

        Without this guard, Python's ``and`` / ``or`` / ``not`` / ``if`` fall back to object
        truthiness (always ``True``) and silently drop predicates —
        ``(col("a") > 1) and (col("b") > 2)`` would evaluate to just ``col("b") > 2``.
        """
        raise PySparkValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __nonzero__ = __bool__

    def __contains__(self, item: object) -> bool:
        """Raise — ``x in column`` is not supported (PySpark ``Column.__contains__``)."""
        raise PySparkValueError(
            "Cannot apply 'in' operator against a column: please use 'contains' in a join "
            "column expression, a WHERE clause, or a filter() call instead."
        )

    def is_null(self) -> Column:
        """``IS NULL`` (PySpark ``Column.isNull``)."""
        self._reject_nested_generator("isNull")
        display = f"({self.spark_wrap_display_part()} IS NULL)"
        sql_expr = f"({self.sql_expr_part()} IS NULL)"
        # H1: keep join_sql QCOL tokens so post-join filter can rewrite to engine fields.
        join_sql_expr = f"({self.join_sql_part()} IS NULL)"
        return Column(
            self._inner.is_null(),
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    isNull = is_null  # noqa: N815 — PySpark camelCase alias

    def is_not_null(self) -> Column:
        """``IS NOT NULL`` (PySpark ``Column.isNotNull``)."""
        self._reject_nested_generator("isNotNull")
        display = f"({self.spark_wrap_display_part()} IS NOT NULL)"
        sql_expr = f"({self.sql_expr_part()} IS NOT NULL)"
        # H1: keep join_sql QCOL tokens so post-join filter can rewrite to engine fields.
        join_sql_expr = f"({self.join_sql_part()} IS NOT NULL)"
        return Column(
            self._inner.is_not_null(),
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    isNotNull = is_not_null  # noqa: N815 — PySpark camelCase alias

    @classmethod
    def _from_when_pairs(
        cls,
        pairs: list[tuple[Column, Column]],
        *,
        otherwise: Column | None,
    ) -> Column:
        """Build a searched CASE column from ordered WHEN arms (used by ``F.when``)."""
        # Generators inside CASE arms strip ``_generator`` and skip unnest rewrite
        # (octo C5-L-001 / C5-Q-001). Spark: UNSUPPORTED_GENERATOR.
        for condition, value in pairs:
            condition._reject_nested_generator("when/CASE")
            value._reject_nested_generator("when/CASE")
        if otherwise is not None:
            otherwise._reject_nested_generator("when/CASE otherwise")
        when_thens = [(condition._inner, value._inner) for condition, value in pairs]
        else_inner = None if otherwise is None else otherwise._inner
        # Track Spark-style CASE text so agg names do not leak native DF CASE rendering.
        arms = " ".join(
            f"WHEN {condition.spark_wrap_display_part()} THEN {value.spark_wrap_display_part()}"
            for condition, value in pairs
        )
        sql_arms = " ".join(
            f"WHEN {condition.sql_expr_part()} THEN {value.sql_expr_part()}"
            for condition, value in pairs
        )
        # H1: CASE join_sql keeps QCOL tokens from origin-bearing arms (post-join when).
        join_arms = " ".join(
            f"WHEN {condition.join_sql_part()} THEN {value.join_sql_part()}"
            for condition, value in pairs
        )
        if otherwise is None:
            display = f"CASE {arms} END"
            sql_expr = f"CASE {sql_arms} END"
            join_sql_expr = f"CASE {join_arms} END"
            # Open when-chain: further .when() allowed.
            retained_pairs: list[tuple[Column, Column]] | None = list(pairs)
        else:
            display = f"CASE {arms} ELSE {otherwise.spark_wrap_display_part()} END"
            sql_expr = f"CASE {sql_arms} ELSE {otherwise.sql_expr_part()} END"
            join_sql_expr = f"CASE {join_arms} ELSE {otherwise.join_sql_part()} END"
            # Closed with ELSE — further .when() must fail (Spark; octo C2-L-003).
            retained_pairs = None
        # Sticky partition-transform (Group I): CASE arms embedding F.years/... still fail
        # loud outside partitionedBy (octo r1 C1-Q-003).
        arm_columns = [col for pair in pairs for col in pair] + (
            [otherwise] if otherwise is not None else []
        )
        transform = next(
            (col._partition_transform for col in arm_columns if col._partition_transform),
            None,
        )
        is_aggregate = any(col._is_aggregate for col in arm_columns)
        is_foldable = (not is_aggregate) and all(col._is_foldable for col in arm_columns)
        has_free_attribute = any(col._has_free_attribute for col in arm_columns)
        has_ungroupable = any(col._has_ungroupable for col in arm_columns)
        return cls(
            _native.PyColumn.case_when(when_thens, else_inner),
            when_pairs=retained_pairs,
            partition_transform=transform,
            spark_display=display,
            sql_expr=sql_expr,
            join_sql_expr=join_sql_expr,
            stable_name=False,
            is_aggregate=is_aggregate,
            is_foldable=is_foldable,
            has_free_attribute=has_free_attribute,
            has_ungroupable=has_ungroupable,
        )

    def when(self, condition: Column, value: Column | Scalar) -> Column:
        """Chain another ``WHEN`` arm (PySpark ``Column.when`` on a when-column)."""
        if self._when_pairs is None:
            raise PySparkTypeError(
                "when() can only be chained on a column produced by repark.functions.when "
                "(not after otherwise())"
            )
        if not isinstance(condition, Column):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN",
                messageParameters={
                    "arg_name": "condition",
                    "arg_type": type(condition).__name__,
                },
            )
        pair = (condition, self._to_column(value))
        return Column._from_when_pairs([*self._when_pairs, pair], otherwise=None)

    def otherwise(self, value: Column | Scalar) -> Column:
        """Close a ``when`` chain with an ``ELSE`` arm (PySpark ``Column.otherwise``)."""
        if self._when_pairs is None:
            raise PySparkTypeError(
                "otherwise() can only be used on a column produced by repark.functions.when"
            )
        return Column._from_when_pairs(self._when_pairs, otherwise=self._to_column(value))

    # ---- naming / typing / ordering ---------------------------------------------------------

    @property
    def str(self) -> Any:
        """Polars-style string namespace (``col.str.to_uppercase()``) — R-POLARS-NS."""
        from repark.polars import StringNameSpace

        return StringNameSpace(self)

    @property
    def dt(self) -> Any:
        """Polars-style datetime namespace (``col.dt.year()``) — R-POLARS-NS."""
        from repark.polars import DatetimeNameSpace

        return DatetimeNameSpace(self)

    # === r21 T3: ux-polish ===
    def round(self, scale: int = 0) -> Column:
        """Round this column to ``scale`` decimal places (repark extension; not PySpark).

        Delegates to :func:`repark.functions.round`. Documented divergence-extension (same
        bucket as ``to_polars()`` / TA): works on windowed and TA outputs, e.g.
        ``ta.sma(\"close\", 10).over(window).round(4)``.

        See ``docs/divergences.md`` row for ``Column.round``.
        """
        from repark import functions as functions_mod

        return functions_mod.round(self, scale)

    def alias(self, *alias: str, metadata: dict[str, Any] | None = None) -> Column:
        """Rename the column (PySpark ``Column.alias``).

        When the aliased column is later used as an aggregate *argument*
        (``F.sum(col("x").alias("y"))``), PySpark embeds ``x AS y`` in the default output name.
        An explicit ``F.sum(...).alias("total")`` still wins because the outer alias clears
        ``agg_name`` (GroupedData does not re-apply a default name).

        ``sql_expr`` keeps the child expression **without** embedding ``AS name`` so
        post-alias composition (``sum(x).alias("t") + 1``, ``.cast(...)``) and free-SQL
        global-agg select stay valid; the output name is ``projection_name`` (quoted at
        the SELECT boundary — octo C3-002 / C3-SEC-001 / C3-SAF-001).

        E1: accepts ``*alias`` and optional ``metadata=``. Multi-name + ``metadata`` raises
        ``ONLY_ALLOWED_FOR_SINGLE_COLUMN`` (Apache ``test_alias_negative``). Schema metadata
        is accepted and ignored on the engine path (no StructField metadata plumbing yet).
        Multi-name without metadata uses the first name (generator multi-output residual).
        """
        if not alias:
            raise PySparkTypeError(
                errorClass="CANNOT_BE_EMPTY",
                messageParameters={"item": "alias"},
            )
        if metadata is not None and len(alias) != 1:
            raise PySparkValueError(
                errorClass="ONLY_ALLOWED_FOR_SINGLE_COLUMN",
                messageParameters={"arg_name": "metadata"},
            )
        # metadata accepted on single-name alias (schema store deferred — error-surface only).
        _ = metadata
        name = alias[0]
        if self._generator is not None:
            # Generators keep the *array* SQL in sql_expr; only the output name changes.
            # Sticky aggregate / free / ungroupable / AF must survive ``.alias`` so select
            # generator+agg still raises ``[MISSING_GROUP_BY]`` (combine octo C5-Q-002) —
            # pre-fix dropped those bits and ``select(synth.alias("e"))`` bypassed the gate.
            return Column(
                self._inner.alias(name),
                # H2: collapse prior alias so re-alias is ``a AS b`` not ``x AS a AS b``.
                spark_display=f"{self.spark_wrap_display_part()} AS {name}",
                sql_expr=self.sql_expr_part(),
                projection_name=name,
                stable_name=True,
                partition_transform=self._partition_transform,
                is_aggregate=self._is_aggregate,
                is_foldable=self._is_foldable and not self._is_aggregate,
                has_free_attribute=self._has_free_attribute,
                has_ungroupable=self._has_ungroupable,
                is_aggregate_function=self._is_aggregate_function,
                generator=self._generator,
                generator_cast=self._generator_cast,
                origin_plan_id=self._origin_plan_id,
                origin_field=self._origin_field,
                join_sql_expr=self._join_sql_expr,
                g2_range_order_names=self._g2_range_order_names,
                window_spec=self._window_spec,
            )
        return Column(
            self._inner.alias(name),
            # H2: collapse prior alias so re-alias is ``a AS b`` not ``x AS a AS b``.
            spark_display=f"{self.spark_wrap_display_part()} AS {name}",
            # Do not embed ``AS name`` into sql_expr — composition and CAST would break.
            sql_expr=self.sql_expr_part(),
            projection_name=name,
            stable_name=True,
            partition_transform=self._partition_transform,
            # Keep aggregate identity; clear default name via omitting agg_name.
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            # Alias alone keeps native AggregateFunction purity (GroupedData / global agg).
            is_aggregate_function=self._is_aggregate_function,
            generator=self._generator,
            generator_cast=self._generator_cast,
            # H1: alias renames the projection but the leaf still came from the same DF field.
            origin_plan_id=self._origin_plan_id,
            origin_field=self._origin_field,
            join_sql_expr=self._join_sql_expr,
            # Sticky RANGE order-name check (r20 G2 octo C1) across .alias.
            g2_range_order_names=self._g2_range_order_names,
            # === r23b N2: plan-collapse ===
            window_spec=self._window_spec,
        )

    def __iter__(self) -> None:
        """Refuse iteration — defining :meth:`__getitem__` would otherwise make Columns iterable.

        PySpark raises ``NOT_ITERABLE`` (Apache ``test_column_iterator``). Without this, Python
        falls back to ``__getitem__(0)``, ``__getitem__(1)``, … and silently fails to TypeError.
        """
        raise PySparkTypeError(
            errorClass="NOT_ITERABLE",
            messageParameters={"objectName": "Column"},
        )

    def __getitem__(self, key: Any) -> Column:
        """Item / field / slice access (PySpark ``Column.__getitem__``).

        E1 surface: returns a :class:`Column`. A slice with ``step`` raises
        ``PySparkValueError`` / ``SLICE_WITH_STEP`` (Apache classic). Open-bound slices
        (``None`` start and/or stop) raise the same ``substr`` type errors classic raises
        — never invent defaults (octo C3-L-001). Index/key paths lower via native
        ``array_element`` / ``get_field`` / ``getitem`` where possible.
        """
        self._reject_nested_generator("__getitem__")
        from repark.functions import lit

        if isinstance(key, slice):
            # Apache classic (4.1.2 classic/column.py): step → SLICE_WITH_STEP; else
            # return self.substr(k.start, k.stop) with *no* open-bound defaults
            # (octo C3-L-001 / C3-L-002). Inventing start=1 / length=start made
            # col[:n]/col[i:]/col[:] silently evaluate wrong substr.
            if key.step is not None:
                raise PySparkValueError(
                    errorClass="SLICE_WITH_STEP",
                    messageParameters={},
                )
            start = key.start
            length = key.stop
            # Mirror classic Column.substr type checks exactly.
            if type(start) is not type(length):
                raise PySparkTypeError(
                    errorClass="NOT_SAME_TYPE",
                    messageParameters={
                        "arg_name1": "startPos",
                        "arg_name2": "length",
                        "arg_type1": type(start).__name__,
                        "arg_type2": type(length).__name__,
                    },
                )
            if isinstance(start, int):
                start_col = lit(int(start))
                length_col = lit(int(length))
                start_display: Any = start
                length_display: Any = length
            elif isinstance(start, Column):
                start_col = start
                # type(start) is type(length) and start is Column ⇒ length is Column.
                length_col = length  # type: ignore[assignment]
                start_display = start.spark_wrap_display_part()
                length_display = length_col.spark_wrap_display_part()
            else:
                raise PySparkTypeError(
                    errorClass="NOT_COLUMN_OR_INT",
                    messageParameters={
                        "arg_name": "startPos",
                        "arg_type": type(start).__name__,
                    },
                )
            # call_scalar("substr") embeds owned Spark substring_udf (pos 0 ≡ 1), not DF
            # built-in — see crates/repark-python call_scalar arm (octo C7-L-001).
            display = f"substr({self.spark_wrap_display_part()}, {start_display}, {length_display})"
            return Column(
                _native.PyColumn.call_scalar(
                    "substr",
                    [self._inner, start_col._inner, length_col._inner],
                ),
                spark_display=display,
                sql_expr=(
                    f"substr({self.sql_expr_part()}, {start_col.sql_expr_part()}, "
                    f"{length_col.sql_expr_part()})"
                ),
                has_free_attribute=self._has_free_attribute,
                is_foldable=self._is_foldable and not self._is_aggregate,
                is_aggregate=self._is_aggregate,
                has_ungroupable=self._has_ungroupable,
            )

        key_column = key if isinstance(key, Column) else lit(key)
        # Integer: Spark Python ``Column[i]`` is 0-based element extract (not a 1-element slice).
        # call_scalar ``array_element`` embeds owned ``__repark_array_get__`` (octo C1-L-001 /
        # C1-Q-002 — no fail-open to parent ``_inner``).
        if isinstance(key, int) and not isinstance(key, bool):
            display = f"{self.spark_wrap_display_part()}[{key}]"
            index_lit = lit(key)
            native = _native.PyColumn.call_scalar(
                "array_element",
                [self._inner, index_lit._inner],
            )
            return Column(
                native,
                spark_display=display,
                sql_expr=f"({self.sql_expr_part()})[{key}]",
                has_free_attribute=self._has_free_attribute,
                is_foldable=self._is_foldable and not self._is_aggregate,
                is_aggregate=self._is_aggregate,
                has_ungroupable=self._has_ungroupable,
            )

        # String key — struct field via native get_field (octo C1-L-002); free-SQL always
        # double-quotes the ident so hostile fragments cannot widen ON/GROUP BY (C1-SEC-001).
        # get_field also resolves map[str] (Apache field_accessor / access_nested_types);
        # pinned by test_column_getitem_map_str_key_extracts_value (octo C4-Q-001).
        display = f"{self.spark_wrap_display_part()}[{key!r}]"
        if isinstance(key, str):
            key_lit = lit(key)
            native = _native.PyColumn.call_scalar(
                "get_field",
                [self._inner, key_lit._inner],
            )
            sql = f"({self.sql_expr_part()}).{_quote_sql_field_ident(key)}"
            return Column(
                native,
                spark_display=display,
                sql_expr=sql,
                projection_name=display,
                has_free_attribute=self._has_free_attribute,
                is_foldable=self._is_foldable and not self._is_aggregate,
                is_aggregate=self._is_aggregate,
                has_ungroupable=self._has_ungroupable,
            )

        # Column / other key — polymorphic GetItem (array 0-based or map-by-key). Never
        # fail-open to parent ``_inner`` (octo C2-L-001 / C2-SAF-001 residual).
        if isinstance(key, Column):
            display = f"{self.spark_wrap_display_part()}[{key.spark_wrap_display_part()}]"
        native = _native.PyColumn.call_scalar(
            "getitem",
            [self._inner, key_column._inner],
        )
        sql = f"({self.sql_expr_part()})[{key_column.sql_expr_part()}]"
        return Column(
            native,
            spark_display=display,
            sql_expr=sql,
            projection_name=display,
            has_free_attribute=self._has_free_attribute,
            is_foldable=self._is_foldable and not self._is_aggregate,
            is_aggregate=self._is_aggregate,
            has_ungroupable=self._has_ungroupable,
        )

    # === r22 C5: census-r7 Column access surface ===
    def getItem(self, key: Any) -> Column:  # noqa: N802 — PySpark camelCase
        """Get an array element or map value (PySpark ``Column.getItem``).

        Thin wrapper over :meth:`__getitem__` — same 0-based array / map-key / field paths
        (Apache ``test_access_nested_types``).
        """
        return self[key]

    def getField(self, name: Any) -> Column:  # noqa: N802 — PySpark camelCase
        """Get a struct field by name (PySpark ``Column.getField``).

        Thin wrapper over :meth:`__getitem__` string/Column field resolution.
        """
        return self[name]

    def __getattr__(self, item: str) -> Column:
        """Struct field access via attribute syntax (PySpark ``Column.__getattr__``).

        ``df.select(df.r.a)`` resolves as ``getField("a")`` after ``df.r`` returns a Column.
        Double-underscore names raise :class:`AttributeError` so dunder protocol is untouched.
        """
        if item.startswith("__"):
            raise AttributeError(item)
        return self.getField(item)

    def transform(self, f: Callable[[Column], Column]) -> Column:
        """Apply a Column→Column function (PySpark ``Column.transform``, Spark 4.1+).

        Enables method chaining: ``col.transform(F.trim).transform(F.upper)``.
        """
        if not callable(f):
            raise PySparkTypeError(
                errorClass="NOT_CALLABLE",
                messageParameters={
                    "arg_name": "f",
                    "arg_type": type(f).__name__,
                },
            )
        result = f(self)
        if not isinstance(result, Column):
            raise PySparkTypeError(
                errorClass="NOT_COLUMN",
                messageParameters={
                    "arg_name": "f(column)",
                    "arg_type": type(result).__name__,
                },
            )
        return result

    def cast(self, data_type: DataType | str) -> Column:
        """Cast to ``data_type`` (PySpark ``Column.cast``).

        Accepts a :mod:`repark.types` type object (e.g. ``StringType()``, ``DecimalType(10, 4)``) or
        a canonical type string (``"string"``, ``"int"``, ``"decimal(10,4)"``, …).
        """
        engine_type = _engine_type_from_cast_arg(data_type)
        normalized = _normalize_type_string(engine_type)
        # Allowlist Spark CAST tokens — never fail-open unknown/hostile type text into SQL
        # (generator path embeds ``CAST(unnest(...) AS {type})``; octo C4-SEC-001 / C4-L-002).
        spark_type = _spark_cast_type_name(normalized)
        cast_display = f"CAST({self.spark_wrap_display_part()} AS {spark_type})"
        # Live PySpark 4.1.2: cast of a NamedExpression (bare col / alias) keeps the child
        # name in a plain select; cast of a compound expression uses the CAST(...) text.
        if self._stable_name and self._projection_name is not None:
            projection = self._projection_name
            stable = True
        else:
            projection = cast_display
            stable = False
        if self._generator is not None:
            # Element cast after unnest: keep the *array* native + sql_expr + generator
            # sticky so select still rewrites (octo C1-Q-003 / C1-L-003). Do not cast the
            # array itself — the two-phase rewrite projects the array then
            # ``CAST(unnest(...) AS <type>)`` via ``_generator_cast`` (octo C3).
            # spark_type is allowlisted above so phase-2 SQL cannot reshape SELECT.
            # Chained ``.cast().cast()`` *composes* (innermost first), never overwrites —
            # float→int→string must apply every CAST (octo C5-L-003).
            previous = self._generator_cast
            if previous is None:
                composed_cast: str | tuple[str, ...] = spark_type
            elif isinstance(previous, str):
                composed_cast = (previous, spark_type)
            else:
                composed_cast = (*previous, spark_type)
            return Column(
                self._inner,
                spark_display=cast_display,
                sql_expr=self.sql_expr_part(),
                projection_name=projection,
                stable_name=stable,
                partition_transform=self._partition_transform,
                # Sticky aggregate / free / ungroupable across generator cast (combine
                # octo C5-Q-002) — same contract as non-generator cast below. AF purity
                # is cleared by cast (SQL path), matching the non-generator branch.
                is_aggregate=self._is_aggregate,
                is_foldable=self._is_foldable and not self._is_aggregate,
                has_free_attribute=self._has_free_attribute,
                has_ungroupable=self._has_ungroupable,
                generator=self._generator,
                generator_cast=composed_cast,
            )
        cast_sql = f"CAST({self.sql_expr_part()} AS {spark_type})"
        # H1: keep join_sql QCOL tokens so post-join filter / SQL select can rewrite the
        # cast to the correct engine field. Origin is *not* preserved — pure origin rebind
        # would drop the cast and project the uncast leaf.
        join_sql_expr = f"CAST({self.join_sql_part()} AS {spark_type})"
        return Column(
            self._inner.cast(normalized),
            spark_display=cast_display,
            sql_expr=cast_sql,
            join_sql_expr=join_sql_expr,
            projection_name=projection,
            stable_name=stable,
            # Sticky aggregate identity: ``F.sum("x").cast("double")`` is still global-agg.
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    def try_cast(self, data_type: DataType | str) -> Column:
        """Try-cast to ``data_type`` (PySpark ``Column.try_cast`` / SQL ``TRY_CAST``).

        Same type grammar as :meth:`cast`. On conversion failure the engine yields NULL
        instead of raising (DataFusion ``TryCast``). Display form is ``TRY_CAST(... AS …)``
        (Apache ``test_cast_str_representation``). Non-str / non-DataType args raise
        ``NOT_DATATYPE_OR_STR`` (Apache ``test_cast_negative`` grammar; shared with cast).
        """
        self._reject_nested_generator("try_cast")
        engine_type = _engine_type_from_cast_arg(data_type)
        normalized = _normalize_type_string(engine_type)
        spark_type = _spark_cast_type_name(normalized)
        cast_display = f"TRY_CAST({self.spark_display_part()} AS {spark_type})"
        if self._stable_name and self._projection_name is not None:
            projection = self._projection_name
            stable = True
        else:
            projection = cast_display
            stable = False
        cast_sql = f"TRY_CAST({self.sql_expr_part()} AS {spark_type})"
        join_sql_expr = f"TRY_CAST({self.join_sql_part()} AS {spark_type})"
        return Column(
            self._inner.try_cast(normalized),
            spark_display=cast_display,
            sql_expr=cast_sql,
            join_sql_expr=join_sql_expr,
            projection_name=projection,
            stable_name=stable,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable and not self._is_aggregate,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            partition_transform=self._partition_transform,
        )

    def over(self, window: WindowSpec) -> Column:
        """Apply a window specification to this window/aggregate column (PySpark ``Column.over``).

        Used on a window function such as :func:`repark.functions.row_number` or an aggregate
        with a frame (r20 G2)::

            F.row_number().over(Window.partitionBy("g").orderBy("ts"))
            F.max("k").over(Window.orderBy("k").rowsBetween(0, 1))

        Clears aggregate / free-attribute / foldable markers — a window expression is not a
        GROUP BY aggregate and must not classify as a foldable global-agg companion
        (``select(sum(x), row_number().over(...))`` → ``[MISSING_GROUP_BY]`` — octo C6-L-001).
        Sets sticky ``_has_ungroupable`` so nested compositions (``sum+over``,
        ``coalesce(sum, window)``, ``when(...).otherwise(window)``) also raise
        ``[MISSING_GROUP_BY]`` rather than pure_global (octo C7-L-002).

        Raises (from the engine) if this column is not a window or aggregate function.
        """
        self._reject_nested_generator("over")
        # === r20 G2: window/rand/sampleBy ===
        # RANGE without ORDER BY / multi-order value-offset + mark numeric ORDER BY names.
        window._validate_at_over()
        # Ranking / row_number / ntile require ORDER BY (Spark AnalysisException; DF Internal).
        if not window._order_columns:
            display = (self._spark_display or self._projection_name or "").strip()
            ranking = display in {"rank()", "dense_rank()", "row_number()"} or display.startswith(
                "ntile("
            )
            if ranking:
                raise AnalysisException(
                    f"Window function {display or 'rank()'} requires window to be ordered, "
                    "please add ORDER BY clause. For example SELECT rank() OVER "
                    "(PARTITION BY ... ORDER BY ...)"
                )
        partitions = window._partition_natives()
        order_natives, ascending, nulls_first = window._order_specs()
        frame_units, frame_start, frame_end = window._frame_args()
        range_order_names: list[str] | None = None
        if window._range_needs_numeric_order():
            names: list[str] = []
            for order_column in window._order_columns:
                # Bare col / alias: stable projection name; else spark display (best-effort).
                #
                # "Bare" must mean bare. A CAST chain keeps the BASE column's projection name
                # (`col("d").cast("timestamp").cast("long")` still projects as `d`), so naming it
                # here made the guard below read the SOURCE column's dtype and refuse a perfectly
                # numeric order key — Spark accepts `CAST(ts AS BIGINT)` as a RANGE key, and so
                # does repark since the TZ-5 cast fix made that expression epoch seconds
                # (task/tz5-cast-seconds-ledger.md; the arithmetic wrapper the moving-average pin
                # used to carry had been hiding this). Requiring the display to EQUAL the
                # projection name is what separates a bare reference from an expression over one;
                # an expression falls to the display branch, whose name matches no schema field,
                # so the guard skips it and the engine remains the authority on its type.
                if (
                    order_column._stable_name
                    and order_column._projection_name is not None
                    and order_column._spark_display == order_column._projection_name
                ):
                    names.append(order_column._projection_name)
                elif order_column._spark_display is not None:
                    names.append(order_column._spark_display)
            range_order_names = names
        # Explicit non-agg / non-foldable / non-free so select's pure_global predicate
        # rejects window companions beside aggregates (defaults already False — keep
        # intentional so a future sticky copy cannot reintroduce C6-L-001). Sticky
        # ungroupable so nested sum∘window composition cannot OR-aggregate away the
        # window leaf (octo C7-L-002).
        return Column(
            self._inner.over(
                partitions,
                order_natives,
                ascending,
                nulls_first,
                frame_units,
                frame_start,
                frame_end,
            ),
            is_aggregate=False,
            is_foldable=False,
            has_free_attribute=False,
            has_ungroupable=True,
            is_aggregate_function=False,
            g2_range_order_names=range_order_names,
            # === r23b N2: plan-collapse ===
            # Retain the facade WindowSpec so adjacent withColumn(s) can merge same-spec
            # windows (structural equality). Native expr alone does not expose the spec.
            window_spec=window,
        )

    def asc(self) -> Column:
        """Mark this column for ascending order (PySpark ``Column.asc``; nulls first)."""
        return Column(
            self._inner,
            sort_ascending=True,
            sort_nulls_first=True,
            spark_display=self._spark_display,
            projection_name=self._projection_name,
            stable_name=self._stable_name,
            agg_name=self._agg_name,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            is_aggregate_function=self._is_aggregate_function,
            partition_transform=self._partition_transform,
            # Keep structural sql_expr so free-SQL global-agg does not fall back to
            # unquoted display after sort markers (octo C4-SEC-002).
            sql_expr=self._sql_expr,
            # Keep generators sticky — orderBy markers must not drop explode rewrite
            # (octo C2-Q-005 / C2-L-003).
            generator=self._generator,
            generator_cast=self._generator_cast,
            when_pairs=self._when_pairs,
            # H1: sort markers must not drop join identity (orderBy(parent.col.desc())).
            origin_plan_id=self._origin_plan_id,
            origin_field=self._origin_field,
            join_sql_expr=self._join_sql_expr,
        )

    def desc(self) -> Column:
        """Mark this column for descending order (PySpark ``Column.desc``; nulls last)."""
        return Column(
            self._inner,
            sort_ascending=False,
            sort_nulls_first=False,
            spark_display=self._spark_display,
            projection_name=self._projection_name,
            stable_name=self._stable_name,
            agg_name=self._agg_name,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            is_aggregate_function=self._is_aggregate_function,
            partition_transform=self._partition_transform,
            # Keep structural sql_expr so free-SQL global-agg does not fall back to
            # unquoted display after sort markers (octo C4-SEC-002).
            sql_expr=self._sql_expr,
            generator=self._generator,
            generator_cast=self._generator_cast,
            when_pairs=self._when_pairs,
            # H1: sort markers must not drop join identity (orderBy(parent.col.desc())).
            origin_plan_id=self._origin_plan_id,
            origin_field=self._origin_field,
            join_sql_expr=self._join_sql_expr,
        )

    def for_select(self) -> Column:
        """Return this column with the native expression aliased to the Spark projection name.

        Used by :meth:`repark.dataframe.DataFrame.select` (Group H) so
        ``df.select(df.x + 1).columns`` is ``['(x + 1)']`` rather than DataFusion's
        ``t.x + Int64(1)``. An explicit :meth:`alias` already names the native; re-aliasing
        to ``projection_name`` is idempotent. When no projection name is tracked, the
        column is returned unchanged.

        Bare ``col("name")`` / string select refs are **also** aliased to the requested
        projection spelling (live PySpark 4.1.2 under ``spark.sql.caseSensitive=false``:
        ``df.select("X")`` and ``df.select(F.col("X"))`` keep ``"X"`` when the schema
        column is ``x`` — not the engine's canonical field name). Skipping the alias
        collapsed CI-equivalent projections (``select("X", "x")``) into a single
        DataFusion name and bypassed the facade duplicate preflight (octo C3-L-001/002).
        Cast-of-attribute still differs: ``spark_display`` is ``CAST(...)`` while
        ``projection_name`` stays the child name, so the child name is applied here.
        """
        if self._projection_name is None:
            return self
        return Column(
            self._inner.alias(self._projection_name),
            sort_ascending=self._sort_ascending,
            sort_nulls_first=self._sort_nulls_first,
            when_pairs=self._when_pairs,
            agg_name=self._agg_name,
            is_aggregate=self._is_aggregate,
            is_foldable=self._is_foldable,
            has_free_attribute=self._has_free_attribute,
            has_ungroupable=self._has_ungroupable,
            is_aggregate_function=self._is_aggregate_function,
            generator=self._generator,
            generator_cast=self._generator_cast,
            spark_display=self._spark_display,
            projection_name=self._projection_name,
            stable_name=self._stable_name,
            partition_transform=self._partition_transform,
            sql_expr=self._sql_expr,
            origin_plan_id=self._origin_plan_id,
            origin_field=self._origin_field,
            join_sql_expr=self._join_sql_expr,
            g2_range_order_names=self._g2_range_order_names,
            window_spec=self._window_spec,
        )


def _engine_type_from_cast_arg(data_type: Any) -> str:
    """Resolve a cast/try_cast argument to an engine type string (Apache type gate).

    Accepts ``str`` or a :class:`~repark.types.DataType` instance. Any other Python type raises
    :class:`PySparkTypeError` with ``NOT_DATATYPE_OR_STR`` (Apache ``test_cast_negative``) —
    never bare ``AttributeError`` on ``_engine_type`` (octo C5-C1-001).
    """
    if isinstance(data_type, str):
        return data_type
    # Lazy import: types ↔ column would cycle if DataType were a top-level import.
    from repark.types import DataType

    if isinstance(data_type, DataType):
        return data_type._engine_type()
    raise PySparkTypeError(
        errorClass="NOT_DATATYPE_OR_STR",
        messageParameters={
            "arg_name": "dataType",
            "arg_type": type(data_type).__name__,
        },
    )


def _normalize_type_string(text: str) -> str:
    """Lower-case and strip whitespace from a type string so ``"Decimal(10, 4)"`` reaches the native
    parser as ``"decimal(10,4)"``."""
    return "".join(text.split()).lower()


# Spark CAST tokens produced by :func:`_spark_cast_type_name` (simple + DECIMAL(p,s)).
_SPARK_CAST_SIMPLE_TOKENS = frozenset(
    {
        "BOOLEAN",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "STRING",
        "BINARY",
        "DATE",
        "TIMESTAMP",
    }
)
_SPARK_DECIMAL_CAST_TOKEN_RE = re.compile(r"^DECIMAL\(\d+,\d+\)$")


def _spark_cast_type_name(engine_type: str) -> str:
    """Map a repark engine type string to PySpark's CAST type token (live-recorded).

    PySpark 4.1.2 emits uppercase simple names (``DOUBLE``, ``INT``) and ``DECIMAL(p,s)``.

    Unknown / hostile type text raises :class:`ParseException` — never fail-open via
    ``.upper()`` or a loose decimal suffix. Generator unnest SQL embeds this token as
    ``CAST(... AS {type})`` (octo C4-SEC-001 / C4-L-002); non-generator casts still go
    through native ``parse_data_type`` as a second gate.

    The allowlist is the security control; the *exception class* is parity. Live PySpark
    4.1.2 raises ``ParseException`` for an unparsable cast token — and ``ParseException``
    subclasses :class:`AnalysisException` (see :mod:`repark.errors`), so user code written
    as ``except AnalysisException`` catches a bad cast on both engines. A bare
    ``ValueError`` would not be caught by that idiom (r24 morning rider; oracle-recorded
    ``notatype``/``varchar`` → ``ParseException``, ``ValueError=False``).
    """
    if engine_type.startswith("decimal"):
        if _DECIMAL_CAST_TYPE_RE.fullmatch(engine_type) is None:
            raise ParseException(f"unknown cast type {engine_type!r}")
        # engine: decimal(10,4) → Spark: DECIMAL(10,4)
        return "DECIMAL" + engine_type[len("decimal") :].upper()
    # Lockstep with native ``parse_data_type`` (r24 QUAL-03). Aliases ``tinyint``/``smallint``
    # match Spark short forms; ``byte``/``short`` are the ``types.py`` ``_engine_type`` tokens.
    mapping = {
        "boolean": "BOOLEAN",
        "byte": "TINYINT",
        "tinyint": "TINYINT",
        "short": "SMALLINT",
        "smallint": "SMALLINT",
        "int": "INT",
        "integer": "INT",
        "long": "BIGINT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "string": "STRING",
        "binary": "BINARY",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }
    spark_type = mapping.get(engine_type)
    if spark_type is None:
        raise ParseException(f"unknown cast type {engine_type!r}")
    return spark_type


def _require_allowlisted_spark_cast_token(spark_type: str) -> str:
    """Return ``spark_type`` if it is a safe CAST SQL token; else raise ``ValueError``.

    Used at generator unnest embed time so a poisoned ``_generator_cast`` cannot reshape
    ``SELECT`` (octo C4-SEC-001). Accepts only tokens emitted by :func:`_spark_cast_type_name`.
    """
    if spark_type in _SPARK_CAST_SIMPLE_TOKENS:
        return spark_type
    if _SPARK_DECIMAL_CAST_TOKEN_RE.fullmatch(spark_type) is not None:
        return spark_type
    raise ValueError(f"unknown cast type {spark_type!r}")
