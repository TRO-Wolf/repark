"""PySpark 4.0+ ``DataFrame.mergeInto`` builder — lowers to engine SQL ``MERGE INTO``.

Oracle: live PySpark 4.1.2 ``pyspark.sql.merge.MergeIntoWriter`` (surface shapes). Execution
delegates to the existing ``spark.sql("MERGE INTO …")`` path (zero engine code in this unit).

``whenNotMatchedBySource`` is accepted on the builder and rendered into SQL; the engine rejects
``WHEN NOT MATCHED BY SOURCE`` today (``not_matched_by_source_rejected`` pin) — the loud engine
error is intentional and disclosed in the unit ledger.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from repark.errors import (
    AnalysisException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)

# === r23 QI1: idents ===
from repark.spark._idents import is_plain_ident
from repark.spark._idents import quote_ident as _quote_ident
from repark.spark._idents import quote_ident_if_needed as _quote_assign_target
from repark.spark._temp_views import scratch_view_name
from repark.spark.column import Column

if TYPE_CHECKING:
    from repark.spark.dataframe import DataFrame

# Bare identifier for equi-join sugar: mergeInto(table, "id") → target.id = source.id


def _column_sql(column: Column | str) -> str:
    """Render a merge condition / assignment expression as SQL text.

    :class:`Column` prefers :meth:`Column.sql_expr_part` (SQL-quoted string literals). Free
    ``str`` fragments are refused for assignments/predicates (octo C2-SEC-001) — use Column.
    """
    if isinstance(column, Column):
        return column.sql_expr_part()
    raise PySparkTypeError(
        f"merge expression must be Column (not free SQL str), got {type(column).__name__}"
    )


def _on_sql(condition: Column | str) -> str:
    """Lower the merge match condition.

    * ``Column`` → ``sql_expr_part()`` (callers should qualify source/target when needed).
    * bare identifier str (``\"id\"``) → equi-join sugar ``target.id = source.id`` (PySpark
      doctest shape; aliases match the SQL we emit).
    * other str → SQL fragment as-is.
    """
    if isinstance(condition, Column):
        return condition.sql_expr_part()
    if isinstance(condition, str):
        stripped = condition.strip()
        # Only bare-ident equi sugar — free SQL ON fragments refused (octo C2-SEC-001).
        if is_plain_ident(stripped):
            quoted = _quote_ident(stripped)
            return f"target.{quoted} = source.{quoted}"
        raise PySparkTypeError(
            "mergeInto condition str must be a bare column name for equi-join sugar; "
            "use a Column for general predicates (free SQL ON fragments are refused)"
        )
    raise PySparkTypeError(
        f"mergeInto condition must be Column or str, got {type(condition).__name__}"
    )


class _Clause(BaseModel):
    """One WHEN clause accumulated before ``merge()`` renders SQL."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    kind: str  # matched | not_matched | not_matched_by_source
    action: str  # update_all | update | delete | insert_all | insert
    predicate_sql: str | None = None
    assignments: dict[str, str] = Field(default_factory=dict)


class MergeIntoWriter:
    """Builder for ``DataFrame.mergeInto(table, condition)`` (PySpark ``MergeIntoWriter``).

    Accumulates ``whenMatched`` / ``whenNotMatched`` / ``whenNotMatchedBySource`` actions, then
    :meth:`merge` registers the source frame as a generated temp view, runs the rendered
    ``MERGE INTO`` SQL via the session, and drops the view.
    """

    def __init__(self, dataframe: DataFrame, table: str, condition: Column | str) -> None:
        if not isinstance(table, str) or table.strip() == "":
            raise PySparkTypeError(f"mergeInto table must be a non-empty str, got {table!r}")
        if not isinstance(condition, (Column, str)):
            raise PySparkTypeError(
                f"mergeInto condition must be Column or str, got {type(condition).__name__}"
            )
        self._dataframe = dataframe
        # Same identifier validation as writeTo / spark.table (E2). Store the raw name and
        # re-resolve under the session's *current* catalog/NS at merge() — not freeze at
        # construction (octo C1-L-002; matches V1 saveAsTable action-time resolve).
        from repark.spark.dataframe import _resolve_writer_table

        self._table_name = table.strip()
        # Early injection / identifier gate (discard resolved form — merge re-resolves).
        _resolve_writer_table(dataframe, self._table_name)
        self._on_sql = _on_sql(condition)
        self._clauses: list[_Clause] = []
        self._schema_evolution = False

    # ---- clause openers ------------------------------------------------------------------

    def whenMatched(  # noqa: N802 — PySpark method name
        self, condition: Column | str | None = None
    ) -> MergeIntoWriter.WhenMatched:
        """Open a ``WHEN MATCHED`` action (optional extra predicate)."""
        return MergeIntoWriter.WhenMatched(self, condition)

    def whenNotMatched(  # noqa: N802 — PySpark method name
        self, condition: Column | str | None = None
    ) -> MergeIntoWriter.WhenNotMatched:
        """Open a ``WHEN NOT MATCHED`` (source-only) action."""
        return MergeIntoWriter.WhenNotMatched(self, condition)

    def whenNotMatchedBySource(  # noqa: N802 — PySpark method name
        self, condition: Column | str | None = None
    ) -> MergeIntoWriter.WhenNotMatchedBySource:
        """Open a ``WHEN NOT MATCHED BY SOURCE`` action.

        The clause is rendered into SQL. The engine rejects this form today — callers see the
        engine's loud error (not a silent no-op).
        """
        return MergeIntoWriter.WhenNotMatchedBySource(self, condition)

    def withSchemaEvolution(self) -> MergeIntoWriter:  # noqa: N802 — PySpark method name
        """Schema evolution is not supported on repark's MERGE path — fail loud (octo C1-L-010)."""
        raise UnsupportedOperationException(
            "MergeIntoWriter.withSchemaEvolution() is not supported yet "
            "(repark MERGE SQL path has no schema-evolution flag; refuse rather than silent no-op)"
        )

    # ---- execution -----------------------------------------------------------------------

    def merge(self) -> None:
        """Execute the accumulated merge (PySpark ``MergeIntoWriter.merge`` → ``None``)."""
        self._dataframe._ensure_alive()
        if not self._clauses:
            raise AnalysisException(
                "[NO_MERGE_ACTION_SPECIFIED] df.mergeInto needs to be followed by at least "
                "one of whenMatched/whenNotMatched/whenNotMatchedBySource. SQLSTATE: 42K0E"
            )
        session = self._dataframe._session
        # Fill pending mapInArrow + cache so MERGE source is real rows, not the empty
        # schema-only MIA placeholder (octo C2-Q-001 / C2-SAF-001 / C2-L-001 / C2-L-005).
        view_name = scratch_view_name(session, "__repark_merge_src_")
        session.create_or_replace_temp_view(view_name, self._dataframe._native_for_registration())
        try:
            sql = self._render_sql(view_name)
            # MERGE is eager at sql(); discard the returned handle (same as CTAS writers).
            session.sql(sql)
        finally:
            session.drop_temp_view(view_name)

    def _render_sql(self, source_view: str) -> str:
        """Build the full ``MERGE INTO … USING … ON … WHEN …`` statement."""
        from repark.spark.dataframe import _resolve_writer_table

        _qualified, table_ref = _resolve_writer_table(self._dataframe, self._table_name)
        parts = [
            f"MERGE INTO {table_ref} AS target USING {source_view} AS source ON {self._on_sql}"
        ]
        for clause in self._clauses:
            parts.append(self._render_clause(clause))
        return " ".join(parts)

    def _render_clause(self, clause: _Clause) -> str:
        if clause.kind == "matched":
            head = "WHEN MATCHED"
        elif clause.kind == "not_matched":
            head = "WHEN NOT MATCHED"
        else:
            head = "WHEN NOT MATCHED BY SOURCE"
        if clause.predicate_sql:
            head = f"{head} AND {clause.predicate_sql}"
        if clause.action == "update_all":
            return f"{head} THEN UPDATE SET *"
        if clause.action == "delete":
            return f"{head} THEN DELETE"
        if clause.action == "insert_all":
            return f"{head} THEN INSERT *"
        if clause.action == "update":
            assigns = ", ".join(
                f"{_quote_assign_target(name)} = {sql}" for name, sql in clause.assignments.items()
            )
            return f"{head} THEN UPDATE SET {assigns}"
        if clause.action == "insert":
            columns = ", ".join(_quote_assign_target(name) for name in clause.assignments)
            values = ", ".join(clause.assignments.values())
            return f"{head} THEN INSERT ({columns}) VALUES ({values})"
        raise PySparkValueError(f"internal merge clause action {clause.action!r}")

    def _predicate_sql(self, condition: Column | str | None) -> str | None:
        if condition is None:
            return None
        return _column_sql(condition)

    def _require_assignment_map(self, assignments: object, *, surface: str) -> dict[str, str]:
        if not isinstance(assignments, dict):
            raise PySparkTypeError(
                f"{surface} assignments must be a dict of str to Column, "
                f"got {type(assignments).__name__}"
            )
        if not assignments:
            raise PySparkValueError(f"{surface} assignments dict must not be empty")
        out: dict[str, str] = {}
        for key, value in assignments.items():
            if not isinstance(key, str) or key.strip() == "":
                raise PySparkTypeError(
                    f"{surface} assignment keys must be non-empty str, got {key!r}"
                )
            if not isinstance(value, Column):
                raise PySparkTypeError(
                    f"{surface} assignment values must be Column, got {type(value).__name__}"
                )
            out[key] = _column_sql(value)
        return out

    # ---- nested action classes -----------------------------------------------------------

    class WhenMatched:
        """``WHEN MATCHED`` terminal actions: ``updateAll`` / ``update`` / ``delete``."""

        def __init__(self, writer: MergeIntoWriter, condition: Column | str | None) -> None:
            self._writer = writer
            self._predicate_sql = writer._predicate_sql(condition)

        def updateAll(self) -> MergeIntoWriter:  # noqa: N802 — PySpark method name
            """Update all columns of matched target rows from the source (``UPDATE SET *``)."""
            self._writer._clauses.append(
                _Clause(
                    kind="matched",
                    action="update_all",
                    predicate_sql=self._predicate_sql,
                )
            )
            return self._writer

        def update(self, assignments: dict[str, Column | str]) -> MergeIntoWriter:
            """Update matched rows with the given column assignments."""
            rendered = self._writer._require_assignment_map(assignments, surface="update")
            self._writer._clauses.append(
                _Clause(
                    kind="matched",
                    action="update",
                    predicate_sql=self._predicate_sql,
                    assignments=rendered,
                )
            )
            return self._writer

        def delete(self) -> MergeIntoWriter:
            """Delete matched target rows."""
            self._writer._clauses.append(
                _Clause(
                    kind="matched",
                    action="delete",
                    predicate_sql=self._predicate_sql,
                )
            )
            return self._writer

    class WhenNotMatched:
        """``WHEN NOT MATCHED`` terminal actions: ``insertAll`` / ``insert``."""

        def __init__(self, writer: MergeIntoWriter, condition: Column | str | None) -> None:
            self._writer = writer
            self._predicate_sql = writer._predicate_sql(condition)

        def insertAll(self) -> MergeIntoWriter:  # noqa: N802 — PySpark method name
            """Insert all non-matched source rows (``INSERT *``)."""
            self._writer._clauses.append(
                _Clause(
                    kind="not_matched",
                    action="insert_all",
                    predicate_sql=self._predicate_sql,
                )
            )
            return self._writer

        def insert(self, assignments: dict[str, Column | str]) -> MergeIntoWriter:
            """Insert non-matched rows with explicit column assignments."""
            rendered = self._writer._require_assignment_map(assignments, surface="insert")
            self._writer._clauses.append(
                _Clause(
                    kind="not_matched",
                    action="insert",
                    predicate_sql=self._predicate_sql,
                    assignments=rendered,
                )
            )
            return self._writer

    class WhenNotMatchedBySource:
        """``WHEN NOT MATCHED BY SOURCE`` terminals (engine rejects at execute today)."""

        def __init__(self, writer: MergeIntoWriter, condition: Column | str | None) -> None:
            self._writer = writer
            self._predicate_sql = writer._predicate_sql(condition)

        def updateAll(self) -> MergeIntoWriter:  # noqa: N802 — PySpark method name
            """Update all columns of source-unmatched target rows (``UPDATE SET *``)."""
            self._writer._clauses.append(
                _Clause(
                    kind="not_matched_by_source",
                    action="update_all",
                    predicate_sql=self._predicate_sql,
                )
            )
            return self._writer

        def update(self, assignments: dict[str, Column | str]) -> MergeIntoWriter:
            """Update source-unmatched target rows with the given column assignments."""
            rendered = self._writer._require_assignment_map(
                assignments, surface="whenNotMatchedBySource.update"
            )
            self._writer._clauses.append(
                _Clause(
                    kind="not_matched_by_source",
                    action="update",
                    predicate_sql=self._predicate_sql,
                    assignments=rendered,
                )
            )
            return self._writer

        def delete(self) -> MergeIntoWriter:
            """Delete target rows that no source row matched."""
            self._writer._clauses.append(
                _Clause(
                    kind="not_matched_by_source",
                    action="delete",
                    predicate_sql=self._predicate_sql,
                )
            )
            return self._writer
