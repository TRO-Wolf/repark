"""ReparkSession."""

from __future__ import annotations

import functools
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from typing import Any

import repark.spark.session._funcs as _sf
from repark.spark.session._coerce import range_bound_as_int as _range_bound_as_int
from repark.spark.session._coerce import sql_clause_end_after as _sql_clause_end_after
from repark.spark.session.builder_conf import RuntimeConfig, SparkContext
from repark.spark.session.session_time_zone import (
    SESSION_TIME_ZONE_KEYS,
    normalize_session_time_zone_config,
)
from repark.spark.session.timestamp_type import (
    TIMESTAMP_TYPE_KEYS,
    normalize_timestamp_type_config,
)

for _name in dir(_sf):
    if _name.startswith("__"):
        continue
    globals()[_name] = getattr(_sf, _name)
# keep module ref for active-session mutations (do not del _sf)
del _name


def _temp_view_home_ref(inner: Any, name: str) -> list[str] | None:
    """The temp view's home segments, or ``None`` when it is not a temp view."""
    try:
        return inner.resolve_temp_view_home_ref(name)
    except Exception:
        return None


class ReparkSession:
    """The repark session — near-drop-in for ``pyspark.sql.SparkSession``.

    Construct via :attr:`ReparkSession.builder`. :meth:`Builder.getOrCreate` returns the
    process-wide active session when one is already live (PySpark semantics).
    """

    __slots__ = ("_alive_token", "_builder_config", "_inner", "_spark_context")

    def __init__(
        self,
        inner: _native.PyReparkSession,
        builder_config: dict[str, str | None] | None = None,
        *,
        display_style: str = _DEFAULT_DISPLAY_STYLE,
    ) -> None:
        """Wrap a native ``PyReparkSession``. Prefer :attr:`ReparkSession.builder`."""
        self._inner: _native.PyReparkSession | None = inner
        # Builder-config snapshot for getOrCreate warn-on-diff; values may be None (Spark _to_str).
        self._builder_config: dict[str, str | None] = dict(builder_config or {})
        # Shared with every DataFrame this session mints; stop() flips alive→False;
        # display_style rides the same box so show() sees runtime updates (R-DISPLAY).
        # R-CURCAT-FACADE: catalog state is facade-only; dies with stop(), never engine USE state.
        known_catalogs = _catalog_names_from_builder_config(self._builder_config)
        default_catalog = _default_catalog_from_builder_config(self._builder_config)
        if default_catalog is None and len(known_catalogs) == 1:
            # Single configured catalog → start currentCatalog there (dogfood-friendly).
            default_catalog = next(iter(known_catalogs))
        if default_catalog is None:
            default_catalog = DEFAULT_CATALOG_NAME
        # spark.sql.defaultNamespace seeds currentDatabase (else Spark's ``default``).
        default_namespace = _default_namespace_from_builder_config(self._builder_config)
        if default_namespace is None:
            default_namespace = DEFAULT_DATABASE_NAME
        self._alive_token: dict[str, Any] = {
            "alive": True,
            "display_style": normalize_display_style(display_style),
            # Shared conf map for facade reads (pivotMaxValues, …) without a Python session handle.
            "builder_config": self._builder_config,
            "catalog_state": {
                "current_catalog": default_catalog,
                "current_database": default_namespace,
                "known_catalogs": known_catalogs,
                "information_schema_enabled": False,
            },
            # Classic scalar Python UDF registry (name → entry); dies with stop().
            "udf_registry": {},
        }
        master = self._builder_config_get_master()
        # Stable per-session id (PySpark uses local-<epochms><seq>); repark uses a uuid suffix.
        application_id = f"local-repark-{uuid.uuid4().hex[:12]}"
        self._spark_context = SparkContext(application_id=application_id, master=master)

    @property
    def display_style(self) -> str:
        """Opt-in ``DataFrame.show()`` render style: ``spark`` (default), ``polars``, or ``duckdb``.

        Default ``spark`` keeps PySpark-parity ASCII-grid output byte-identical. Set via
        ``ReparkSession.builder.config("repark.display.style", …)`` at build time,
        ``session.conf.set("repark.display.style", …)``, or assign this attribute at runtime
        (facade-only; not an engine knob). Values are case-insensitive.
        """
        self._ensure_alive()
        style = self._alive_token.get("display_style", _DEFAULT_DISPLAY_STYLE)
        return str(style)

    @display_style.setter
    def display_style(self, value: str) -> None:
        """Set the session display style (see :attr:`display_style`).

        Keeps ``spark.conf`` / builder snapshot in sync so conf.get and show() agree.
        """
        self._ensure_alive()
        style = normalize_display_style(value)
        self._alive_token["display_style"] = style
        _sync_display_style_into_builder_config(self._builder_config, style)
        store = self._alive_token.get("runtime_conf")
        if isinstance(store, dict):
            for existing in list(store):
                if existing.lower() == _DISPLAY_STYLE_KEY:
                    del store[existing]
            store[_DISPLAY_STYLE_KEY] = style
        tomb = self._alive_token.get("runtime_conf_unset")
        if isinstance(tomb, set):
            tomb.discard(_DISPLAY_STYLE_KEY)
            tomb.discard("repark.display.style")

    def _builder_config_get_master(self) -> str:
        """Return spark.master from builder config (case-insensitive key), default local[repark]."""
        for key, value in self._builder_config.items():
            if key.lower() == "spark.master" and value is not None:
                return value
        return "local[repark]"

    def _ensure_alive(self) -> _native.PyReparkSession:
        """Return the native handle, or raise if :meth:`stop` has already run."""
        if self._inner is None:
            raise RuntimeError(_STOPPED_MESSAGE)
        return self._inner

    @property
    def sparkContext(self) -> SparkContext:  # noqa: N802 — PySpark camelCase
        """Minimal SparkContext surface (``setLogLevel`` / ``applicationId`` / ``master``)."""
        self._ensure_alive()
        return self._spark_context

    @property
    def _sc(self) -> SparkContext:
        """PySpark private alias for :attr:`sparkContext` (Apache suite uses ``session._sc``)."""
        return self.sparkContext

    @property
    def version(self) -> str:
        """Engine version string (PySpark ``spark.version``).

        Returns repark's distribution version (``repark-<version>``), **not** Spark's
        ``"4.1.2"``. Scripts log this; they must not parse it as a Spark release. Disclosed in
        ``docs/spark-sql-iceberg-parity.md`` §8 and ``test_dropin_disclosure.py``.
        """
        self._ensure_alive()
        from repark import __version__

        return f"repark-{__version__}"

    def sql(self, query: str) -> DataFrame:
        """Run a Spark-SQL string and return a :class:`DataFrame` (PySpark ``spark.sql``).

        Bare / two-part table names in load-bearing free-SQL forms (``DROP TABLE``,
        ``INSERT``, ``CREATE TABLE``, ``MERGE INTO``, ``UPDATE``, ``DELETE FROM``,
        ``SELECT``/``WITH`` FROM/JOIN) expand under the session default catalog +
        namespace at this entry point via :meth:`resolve_table_name` — not by call-site
        string surgery. Auto-memory-catalog sticky alias semantics apply unchanged.

        Only registered Python UDFs (via :meth:`spark.udf.register`) are considered —
        never a generic ``ident(`` scan. SELECT-list forms (simple, expression-wrapped,
        DISTINCT, WITH/CTE bodies) and scalar UDF use in WHERE / GROUP BY / HAVING
        rewrite through the DataFrame ``udf`` bridge; JOIN ON / nested ``(SELECT``
        subqueries / ORDER BY *expressions* still refuse loud. ``ORDER BY`` on SELECT
        aliases is applied post-materialization (no internal-name leak). See
        :meth:`_sql_with_registered_udfs`.

        Registered Python UDTFs (via :meth:`spark.udtf.register`) rewrite
        ``SELECT * FROM name(lit_args)``; LATERAL stays blocked.
        """
        inner = self._ensure_alive()
        self._promote_active()
        # UDTF FROM-name(lit_args) before scalar UDF rewrite (distinct registries).
        from repark.spark.udtf import try_sql_registered_udtf

        udtf_frame = try_sql_registered_udtf(self, query)
        if udtf_frame is not None:
            return udtf_frame
        # Registry-name scan before bare-table expand (rewrite may re-enter sql).
        udf_frame = self._sql_with_registered_udfs(query)
        if udf_frame is not None:
            return udf_frame
        expanded = self._expand_bare_table_names_in_sql(query)
        return DataFrame(inner.sql(expanded), inner, self._alive_token)

    def resolve_table_name(
        self,
        table_name: str,
        *,
        prefer_temp_view: bool = False,
    ) -> str:
        """Qualify ``table_name`` under current catalog/database (shared resolution).

        Returns an unquoted multipart identifier. Use :func:`_sql_table_ref` for SQL embeds.
        With ``prefer_temp_view=True``, a one-part name that exists as a temp view resolves
        to that view's session-local HOME, **not** the bare name (R7-1): a bare reference
        follows the LIVE ``datafusion.catalog.default_catalog``, so under a raw ``SET`` the
        product read paths missed a view ``tableExists`` reported present.
        """
        state = self._catalog_state()
        known: set[str] = state.get("known_catalogs") or set()
        probe = None
        if prefer_temp_view:
            probe = functools.partial(_temp_view_home_ref, self._ensure_alive())

        return resolve_table_name(
            table_name,
            current_catalog=str(state["current_catalog"]),
            current_database=str(state["current_database"]),
            known_catalogs=known,
            prefer_temp_view=prefer_temp_view,
            temp_view_home_ref=probe,
            default_catalog_is_auto=bool(state.get("auto_default_catalog")),
        )

    def _sql_table_ref_resolved(
        self,
        table_name: str,
        *,
        prefer_temp_view: bool = False,
    ) -> str:
        """Resolve then quote a table identifier for SQL (writer paths)."""
        return _sql_table_ref(
            self.resolve_table_name(table_name, prefer_temp_view=prefer_temp_view)
        )

    def _expand_bare_table_names_in_sql(self, query: str) -> str:
        """Expand bare / two-part names in load-bearing free SQL at the SQL entry point.

        Statement-prefix / parsed-bounded shapes only (no freestyle body regex). Shared SSOT:
        :meth:`resolve_table_name`. Forms:

        * ``DROP TABLE [IF EXISTS] name [, …]`` (sqlutils cleanup)
        * ``INSERT [OVERWRITE [TABLE] | INTO [TABLE]] name …`` (target; not DIRECTORY)
        * ``CREATE [OR REPLACE] TABLE [IF NOT EXISTS] name …`` (durable target; not TEMP)
        * ``MERGE INTO target [AS a] USING source [AS b] ON …`` (target + source)
        * ``UPDATE name [alias] SET … [WHERE …]`` (target; SET body never regexed)
        * ``DELETE FROM name [alias] [WHERE …]`` (target; WHERE-subquery FROM via walker)
        * ``SELECT`` / ``WITH`` … (FROM/JOIN/comma refs; a one-part temp view = its HOME)

        Does **not** rewrite multi-statement scripts. Leading SQL comments / whitespace are
        stripped for classification then re-prefixed.
        """
        trivia, body = _split_leading_sql_trivia(query)
        if not body:
            return query
        expanded = self._expand_bare_table_names_in_sql_body(body)
        return trivia + expanded if expanded is not None else query

    def _expand_bare_table_names_in_sql_body(self, query: str) -> str:
        """Statement-form dispatch after leading trivia has been stripped."""
        # DROP TABLE [IF EXISTS] — sqlutils.table() context manager path.
        drop_match = _DROP_TABLE_SQL_RE.match(query)
        if drop_match is not None:
            if_exists = drop_match.group(1) or ""
            names_blob = drop_match.group(2).strip().rstrip(";").strip()
            if not names_blob:
                return query
            qualified: list[str] = []
            for raw_name in _split_sql_table_name_list(names_blob):
                # DROP never prefers temp-view short-circuit for one-part Iceberg tables; qualify.
                # Temp views use dropTempView, not DROP TABLE.
                resolved = self.resolve_table_name(raw_name, prefer_temp_view=False)
                qualified.append(_sql_table_ref(resolved))
            return f"DROP TABLE {if_exists}{', '.join(qualified)}"

        # MERGE INTO — target + source.
        merge_match = _MERGE_INTO_SQL_RE.match(query)
        if merge_match is not None:
            return self._expand_merge_into_sql(query, merge_match)

        # CREATE VIEW / TEMP VIEW — leave the view name alone; expand the AS body only.
        # Durable CREATE TABLE handled below.
        view_expanded = self._try_expand_create_view_body_sql(query)
        if view_expanded is not None:
            return view_expanded

        # Durable CREATE TABLE (not VIEW / TEMP TABLE).
        if _CREATE_TEMP_TABLE_SQL_RE.match(query) is None:
            create_expanded = self._try_expand_create_table_sql(query)
            if create_expanded is not None:
                return create_expanded

        # INSERT OVERWRITE / INSERT INTO — target.
        insert_expanded = self._try_expand_insert_sql(query)
        if insert_expanded is not None:
            return insert_expanded

        update_expanded = self._try_expand_update_sql(query)
        if update_expanded is not None:
            return update_expanded
        delete_expanded = self._try_expand_delete_sql(query)
        if delete_expanded is not None:
            return delete_expanded

        # SELECT / WITH — structural FROM/JOIN expansion (prefer temp views).
        if _SELECT_OR_WITH_HEAD_RE.match(query) is not None:
            return self._expand_from_join_table_refs_in_sql(query)

        return query

    def _try_expand_create_view_body_sql(self, query: str) -> str | None:
        """Expand FROM/JOIN inside ``CREATE [TEMP] VIEW … AS <query>`` bodies.

        View *names* stay as written (temp views are session-local; durable view catalog
        qualification is residual). Returns ``None`` when not a CREATE VIEW shape.
        """
        if _CREATE_VIEW_SQL_RE.match(query) is None:
            return None
        # Find the AS that introduces the view query (last structural AS before SELECT/WITH).
        match = re.search(r"(?is)\bAS\b", query)
        if match is None:
            return query
        prefix = query[: match.end()]
        body = query[match.end() :]
        body_trivia, body_sql = _split_leading_sql_trivia(body)
        if _SELECT_OR_WITH_HEAD_RE.match(body_sql) is None:
            return query
        expanded_body = self._expand_from_join_table_refs_in_sql(body_sql)
        return f"{prefix}{body_trivia}{expanded_body}"

    def _qualify_sql_table_ref(self, raw_name: str, *, prefer_temp_view: bool) -> str:
        """Resolve + quote a single table identifier for free-SQL expansion."""
        resolved = self.resolve_table_name(raw_name.strip(), prefer_temp_view=prefer_temp_view)
        return _sql_table_ref(resolved)

    def _try_expand_insert_sql(self, query: str) -> str | None:
        """Rewrite ``INSERT … name …`` so the target is fully qualified.

        Returns ``None`` when not an INSERT shape; also expands FROM/JOIN table refs in
        the trailing body (``INSERT … SELECT … FROM t``). ``INSERT OVERWRITE [LOCAL]
        DIRECTORY`` is not a catalog table target: leave the target path
        intact and only expand the statement body.
        """
        prefix_match = _INSERT_PREFIX_RE.match(query)
        if prefix_match is None:
            return None
        prefix = prefix_match.group(1)
        name_start = prefix_match.end()
        while name_start < len(query) and query[name_start].isspace():
            name_start += 1
        # Path-insert dialect — not a table identifier.
        if _INSERT_DIRECTORY_HEAD_RE.match(query[name_start:]) is not None:
            return self._expand_from_join_table_refs_in_sql(query)
        name_end = _scan_sql_table_identifier_end(query, name_start)
        if name_end is None or name_end == name_start:
            return None
        raw_table = query[name_start:name_end]
        rest = query[name_end:]
        try:
            qualified = self._qualify_sql_table_ref(raw_table, prefer_temp_view=False)
        except Exception:
            # Invalid identifier — leave the statement unchanged so the engine/plan error wins.
            return query
        rest_expanded = self._expand_from_join_table_refs_in_sql(rest)
        return f"{prefix}{qualified}{rest_expanded}"

    def _try_expand_create_table_sql(self, query: str) -> str | None:
        """Rewrite durable ``CREATE TABLE name …`` target to three-part form.

        Returns ``None`` when the statement is not a CREATE TABLE shape (caller falls through).
        Table name is scanned as a multipart identifier (not regex-to-AS — avoids eating
        trailing ``as`` in names like ``bare_ctas``). CTAS body FROM/JOIN refs expand too.
        """
        prefix_match = _CREATE_TABLE_PREFIX_RE.match(query)
        if prefix_match is None:
            return None
        prefix = prefix_match.group(1)
        name_start = prefix_match.end()
        while name_start < len(query) and query[name_start].isspace():
            name_start += 1
        name_end = _scan_sql_table_identifier_end(query, name_start)
        if name_end is None or name_end == name_start:
            return None
        raw_table = query[name_start:name_end]
        rest = query[name_end:]
        try:
            qualified = self._qualify_sql_table_ref(raw_table, prefer_temp_view=False)
        except Exception:
            return query
        rest_expanded = self._expand_from_join_table_refs_in_sql(rest)
        return f"{prefix}{qualified}{rest_expanded}"

    def _try_expand_update_sql(self, query: str) -> str | None:
        """Rewrite ``UPDATE name [alias] SET … [WHERE …]`` target to three-part form.

        Returns ``None`` when not an UPDATE shape. Target is identifier-scanned (never
        regex-to-SET — table names may end in ``set``); the SET list is never regexed.
        Optional alias + SET/WHERE body stay in the rest; WHERE-subquery FROM/JOIN refs
        expand via the existing region walker.
        """
        prefix_match = _UPDATE_PREFIX_RE.match(query)
        if prefix_match is None:
            return None
        prefix = prefix_match.group(1)
        name_start = prefix_match.end()
        while name_start < len(query) and query[name_start].isspace():
            name_start += 1
        name_end = _scan_sql_table_identifier_end(query, name_start)
        if name_end is None or name_end == name_start:
            return None
        raw_table = query[name_start:name_end]
        rest = query[name_end:]
        # Identifier scan can eat the SET keyword when the table name is
        # missing (``UPDATE SET x = 1`` → table ``SET``). Require a SET clause after the
        # optional alias; otherwise leave the statement unchanged for the engine parser.
        if not _update_rest_has_set_clause(rest):
            return query
        try:
            qualified = self._qualify_sql_table_ref(raw_table, prefer_temp_view=False)
        except Exception:
            return query
        rest_expanded = self._expand_from_join_table_refs_in_sql(rest)
        return f"{prefix}{qualified}{rest_expanded}"

    def _try_expand_delete_sql(self, query: str) -> str | None:
        """Rewrite ``DELETE FROM name [alias] [WHERE …]`` target to three-part form.

        Returns ``None`` when not a ``DELETE FROM`` shape. Target is identifier-scanned;
        optional alias + WHERE body stay in the rest. WHERE-subquery FROM/JOIN refs expand
        via the existing region walker (same never-regex-a-body rule as SELECT).
        """
        prefix_match = _DELETE_FROM_PREFIX_RE.match(query)
        if prefix_match is None:
            return None
        prefix = prefix_match.group(1)
        name_start = prefix_match.end()
        while name_start < len(query) and query[name_start].isspace():
            name_start += 1
        name_end = _scan_sql_table_identifier_end(query, name_start)
        if name_end is None or name_end == name_start:
            return None
        raw_table = query[name_start:name_end]
        rest = query[name_end:]
        try:
            qualified = self._qualify_sql_table_ref(raw_table, prefer_temp_view=False)
        except Exception:
            return query
        rest_expanded = self._expand_from_join_table_refs_in_sql(rest)
        return f"{prefix}{qualified}{rest_expanded}"

    def _expand_merge_into_sql(self, query: str, match: re.Match[str]) -> str:
        """Rewrite ``MERGE INTO target … USING source … ON …`` table refs.

        Target/source blobs may carry trailing aliases (``t AS target`` / ``s source``).
        Only the leading multipart identifier is expanded; alias text is preserved.
        """
        target_blob = match.group(1).strip()
        source_blob = match.group(2).strip()
        on_rest = match.group(3)
        target_name, target_suffix = _split_leading_table_ident(target_blob)
        source_name, source_suffix = _split_leading_table_ident(source_blob)
        if target_name is None:
            return query
        try:
            target_qualified = self._qualify_sql_table_ref(target_name, prefer_temp_view=False)
            if source_name is None:
                source_qualified = source_blob
            elif source_name.startswith("("):
                # Expand FROM/JOIN inside parenthesized USING source.
                # ``source_name`` may include a trailing alias (``(SELECT …) s``) because
                # ``_split_leading_table_ident`` returns the whole blob for ``(`` heads.
                close = _find_matching_paren(source_name, 0)
                if close is None:
                    source_qualified = source_name + source_suffix
                else:
                    inner = source_name[1:close]
                    tail = source_name[close + 1 :]
                    expanded_inner = self._expand_from_join_table_refs_in_sql(inner)
                    source_qualified = f"({expanded_inner}){tail}{source_suffix}"
            else:
                source_qualified = (
                    self._qualify_sql_table_ref(source_name, prefer_temp_view=True) + source_suffix
                )
        except Exception:
            return query
        return f"MERGE INTO {target_qualified}{target_suffix} USING {source_qualified} ON{on_rest}"

    def _expand_from_join_table_refs_in_sql(self, query: str) -> str:
        """Expand bare / two-part table refs after FROM / JOIN / comma lists.

        Structural scan (quote-aware, paren-aware, comment-aware), not freestyle body regex.
        Skips:

        * non-subquery paren groups (``EXTRACT(YEAR FROM col)``, function args) — FROM
          inside those must not be treated as a table clause (TPC-H Q7 regression)
        * subqueries ``FROM (SELECT …)`` — content is still walked for nested FROM/JOIN
        * nested ``WITH`` CTE names recollected per region
        * table functions ``FROM range(…)`` (ident immediately followed by ``(``)
        * CTE names from ``WITH`` lists (``WITH q AS (…) SELECT … FROM q``)
        * keywords in :data:`_FROM_JOIN_NON_TABLE`
        * SQL comments ``--`` / ``/* … */``

        Comma-separated FROM lists (``FROM a, b``) expand each relation.
        One-part temp views expand to their session-local HOME, never bare (R7-1, MEASURED).

        Non-recursive ``WITH`` bodies only see *prior* CTE names so
        ``WITH t AS (SELECT * FROM t)`` expands the body table.
        ``WITH RECURSIVE`` keeps self-reference bare.
        """
        if re.match(r"(?is)^\s*WITH\b", query) is not None:
            return self._expand_with_statement(query, outer_ctes=set())
        return self._expand_from_join_region(query, cte_names=set())

    def _expand_with_statement(self, query: str, *, outer_ctes: set[str] | None = None) -> str:
        """Expand a ``WITH …`` statement with correct CTE scope.

        Non-recursive bodies see ``outer_ctes`` plus *prior* CTEs in this list only.
        ``WITH RECURSIVE`` bodies see outer + all names from this WITH list.
        """
        match = re.match(r"(?is)^\s*WITH\b", query)
        if match is None:
            return self._expand_from_join_region(query, cte_names=set(outer_ctes or ()))
        pieces: list[str] = [query[: match.end()]]
        index = match.end()
        length = len(query)
        while index < length and query[index].isspace():
            pieces.append(query[index])
            index += 1
        recursive = False
        if index + 9 <= length and query[index : index + 9].upper() == "RECURSIVE":
            recursive = True
            pieces.append(query[index : index + 9])
            index += 9
            while index < length and query[index].isspace():
                pieces.append(query[index])
                index += 1

        # Pre-collect all names for RECURSIVE bodies (self + siblings in scope).
        local_names = _collect_cte_names(query)
        all_cte_names = set(outer_ctes or ()) | local_names
        prior: set[str] = set(outer_ctes or ())

        while index < length:
            name_end = _scan_sql_table_identifier_end(query, index)
            if name_end is None or name_end == index:
                break
            raw_name = query[index:name_end]
            segment = raw_name.split(".")[-1].strip().strip('"').strip("`").lower()
            pieces.append(raw_name)
            index = name_end
            while index < length and query[index].isspace():
                pieces.append(query[index])
                index += 1
            if index < length and query[index] == "(":
                close = _find_matching_paren(query, index)
                if close is None:
                    break
                pieces.append(query[index : close + 1])
                index = close + 1
                while index < length and query[index].isspace():
                    pieces.append(query[index])
                    index += 1
            if index + 2 > length or query[index : index + 2].upper() != "AS":
                break
            pieces.append(query[index : index + 2])
            index += 2
            while index < length and query[index].isspace():
                pieces.append(query[index])
                index += 1
            if index >= length or query[index] != "(":
                break
            close = _find_matching_paren(query, index)
            if close is None:
                break
            body = query[index + 1 : close]
            body_ctes = set(all_cte_names) if recursive else set(prior)
            expanded_body = self._expand_from_join_region(body, cte_names=body_ctes)
            pieces.append("(")
            pieces.append(expanded_body)
            pieces.append(")")
            index = close + 1
            if segment:
                prior.add(segment)
            while index < length and query[index].isspace():
                pieces.append(query[index])
                index += 1
            if index < length and query[index] == ",":
                pieces.append(",")
                index += 1
                while index < length and query[index].isspace():
                    pieces.append(query[index])
                    index += 1
                continue
            break

        # Final query after CTE list (SELECT …) — all CTE names in scope.
        if index < length:
            pieces.append(self._expand_from_join_region(query[index:], cte_names=set(prior)))
        return "".join(pieces)

    def _expand_from_join_region(self, query: str, *, cte_names: set[str]) -> str:
        """Walk one SQL region expanding FROM/JOIN/comma table refs; skip non-subquery parens."""
        result: list[str] = []
        index = 0
        length = len(query)
        while index < length:
            char = query[index]
            # Line / block comments — copy opaque so FROM inside comments is not rewritten.
            if char == "-" and index + 1 < length and query[index + 1] == "-":
                end = query.find("\n", index)
                if end < 0:
                    result.append(query[index:])
                    break
                result.append(query[index : end + 1])
                index = end + 1
                continue
            if char == "/" and index + 1 < length and query[index + 1] == "*":
                end = query.find("*/", index + 2)
                if end < 0:
                    result.append(query[index:])
                    break
                result.append(query[index : end + 2])
                index = end + 2
                continue
            if char in {'"', "'", "`"}:
                quote = char
                result.append(char)
                index += 1
                while index < length:
                    current = query[index]
                    result.append(current)
                    index += 1
                    if current == quote:
                        if quote == '"' and index < length and query[index] == '"':
                            result.append(query[index])
                            index += 1
                            continue
                        break
                continue
            if char == "(":
                # Subquery ``(SELECT|WITH …)`` → expand inside; else copy opaque (EXTRACT FROM…).
                close = _find_matching_paren(query, index)
                if close is None:
                    result.append(char)
                    index += 1
                    continue
                inner = query[index + 1 : close]
                inner_stripped = inner.lstrip()
                if _SELECT_OR_WITH_HEAD_RE.match(inner_stripped) is not None:
                    result.append("(")
                    lead_len = len(inner) - len(inner.lstrip())
                    lead = inner[:lead_len]
                    body = inner[lead_len:]
                    if re.match(r"(?is)^WITH\b", body) is not None:
                        result.append(lead)
                        result.append(self._expand_with_statement(body, outer_ctes=set(cte_names)))
                    else:
                        result.append(self._expand_from_join_region(inner, cte_names=cte_names))
                    result.append(")")
                else:
                    result.append(query[index : close + 1])
                index = close + 1
                continue
            keyword = _match_from_or_join_keyword(query, index)
            if keyword is None:
                result.append(char)
                index += 1
                continue
            result.append(query[index : index + len(keyword)])
            index += len(keyword)
            while index < length and query[index].isspace():
                result.append(query[index])
                index += 1
            if index >= length:
                break
            # Relation list: table or (subquery), optional alias / TABLESAMPLE, comma siblings
            # (``FROM (subq) a, bare_t`` must expand bare_t).
            index = self._append_from_relation_list(query, index, result, cte_names=cte_names)
        return "".join(result)

    def _append_from_relation_list(
        self,
        query: str,
        index: int,
        result: list[str],
        *,
        cte_names: set[str],
    ) -> int:
        """Expand a comma-separated FROM/JOIN relation list starting at ``index``."""
        length = len(query)
        first = True
        while index < length:
            if not first:
                # Skip whitespace *and* comments so ``t/*c*/, u`` still expands u.
                cursor = _skip_sql_ws_and_comments(query, index)
                if cursor >= length or query[cursor] != ",":
                    break
                while index < cursor:
                    result.append(query[index])
                    index += 1
                result.append(",")
                index = cursor + 1
                cursor = _skip_sql_ws_and_comments(query, index)
                while index < cursor:
                    result.append(query[index])
                    index += 1
                if index >= length:
                    break
            first = False
            if query[index] == "(":
                index = self._append_from_subquery_relation(
                    query, index, result, cte_names=cte_names
                )
            else:
                index = self._append_from_join_relation(query, index, result, cte_names=cte_names)
            index = self._append_optional_tablesample(query, index, result)
        return index

    def _append_from_subquery_relation(
        self,
        query: str,
        index: int,
        result: list[str],
        *,
        cte_names: set[str],
    ) -> int:
        """Copy/expand ``(SELECT|WITH …)`` after FROM/JOIN and optional alias."""
        close = _find_matching_paren(query, index)
        if close is None:
            result.append(query[index])
            return index + 1
        inner = query[index + 1 : close]
        inner_stripped = inner.lstrip()
        result.append("(")
        if _SELECT_OR_WITH_HEAD_RE.match(inner_stripped) is not None:
            lead_len = len(inner) - len(inner.lstrip())
            lead = inner[:lead_len]
            body = inner[lead_len:]
            if re.match(r"(?is)^WITH\b", body) is not None:
                result.append(lead)
                result.append(self._expand_with_statement(body, outer_ctes=set(cte_names)))
            else:
                result.append(self._expand_from_join_region(inner, cte_names=cte_names))
        else:
            result.append(inner)
        result.append(")")
        return self._append_optional_relation_alias(query, close + 1, result)

    def _append_optional_tablesample(self, query: str, index: int, result: list[str]) -> int:
        """Copy optional ``TABLESAMPLE (…)`` after a relation."""
        length = len(query)
        cursor = index
        while cursor < length and query[cursor].isspace():
            cursor += 1
        keyword = "TABLESAMPLE"
        if cursor + len(keyword) > length:
            return index
        if query[cursor : cursor + len(keyword)].upper() != keyword:
            return index
        after = cursor + len(keyword)
        if after < length and (query[after].isalnum() or query[after] == "_"):
            return index
        while index < cursor:
            result.append(query[index])
            index += 1
        result.append(query[index:after])
        index = after
        while index < length and query[index].isspace():
            result.append(query[index])
            index += 1
        # Optional sampling method: BERNOULLI / SYSTEM.
        for method in ("BERNOULLI", "SYSTEM"):
            if (
                index + len(method) <= length
                and query[index : index + len(method)].upper() == method
            ):
                end_method = index + len(method)
                if end_method < length and (
                    query[end_method].isalnum() or query[end_method] == "_"
                ):
                    continue
                result.append(query[index:end_method])
                index = end_method
                while index < length and query[index].isspace():
                    result.append(query[index])
                    index += 1
                break
        if index < length and query[index] == "(":
            close = _find_matching_paren(query, index)
            if close is None:
                return index
            result.append(query[index : close + 1])
            return close + 1
        return index

    def _append_from_join_relation(
        self,
        query: str,
        index: int,
        result: list[str],
        *,
        cte_names: set[str],
    ) -> int:
        """Expand or copy one relation starting at ``index``; return the new index.

        Also copies an optional trailing relation alias (``AS a`` / bare ``a``) so the
        comma-list scanner can find the next ``,``.
        """
        length = len(query)
        if index >= length:
            return index
        if query[index] == "(":
            return index
        table_end = _scan_sql_table_identifier_end(query, index)
        if table_end is None or table_end == index:
            return index
        raw_table = query[index:table_end]
        cursor = table_end
        while cursor < length and query[cursor].isspace():
            cursor += 1
        if cursor < length and query[cursor] == "(":
            # Table function ``range(…)`` / ``UNNEST(…)`` — leave unexpanded.
            result.append(raw_table)
            return table_end
        first_segment = raw_table.split(".", 1)[0].strip().strip('"').strip("`")
        # ONLY is a relation prefix (``FROM ONLY t``), not a full relation.
        if first_segment.upper() == "ONLY":
            result.append(raw_table)
            cursor = table_end
            while cursor < length and query[cursor].isspace():
                result.append(query[cursor])
                cursor += 1
            if cursor < length and query[cursor] != "(":
                return self._append_from_join_relation(query, cursor, result, cte_names=cte_names)
            return cursor
        if first_segment.upper() in _FROM_JOIN_NON_TABLE:
            result.append(raw_table)
            return self._append_optional_relation_alias(query, table_end, result)
        if first_segment.lower() in cte_names:
            result.append(raw_table)
            return self._append_optional_relation_alias(query, table_end, result)
        try:
            qualified = self._qualify_sql_table_ref(raw_table, prefer_temp_view=True)
        except Exception:
            result.append(raw_table)
            return self._append_optional_relation_alias(query, table_end, result)
        result.append(qualified)
        return self._append_optional_relation_alias(query, table_end, result)

    def _append_optional_relation_alias(self, query: str, index: int, result: list[str]) -> int:
        """Copy optional ``AS alias`` / bare alias after a relation; return new index."""
        length = len(query)
        cursor = index
        while cursor < length and query[cursor].isspace():
            cursor += 1
        if cursor >= length:
            while index < cursor:
                result.append(query[index])
                index += 1
            return index
        # Explicit AS alias.
        as_word = (
            cursor + 2 <= length
            and query[cursor : cursor + 2].upper() == "AS"
            and (
                cursor + 2 == length
                or not (query[cursor + 2].isalnum() or query[cursor + 2] == "_")
            )
        )
        if as_word:
            while index < cursor:
                result.append(query[index])
                index += 1
            result.append(query[index : index + 2])
            index += 2
            while index < length and query[index].isspace():
                result.append(query[index])
                index += 1
            alias_end = _scan_sql_table_identifier_end(query, index)
            if alias_end is not None and alias_end > index:
                result.append(query[index:alias_end])
                return alias_end
            return index
        # Bare alias — single identifier not a relation-follow keyword.
        if query[cursor] in {'"', "`"} or query[cursor].isalpha() or query[cursor] == "_":
            alias_end = _scan_sql_table_identifier_end(query, cursor)
            if alias_end is not None and alias_end > cursor:
                # Only one-part bare aliases (multi-part would be another table — not valid alias).
                raw_alias = query[cursor:alias_end]
                if "." not in raw_alias:
                    token = raw_alias.strip().strip('"').strip("`").upper()
                    is_alias = (
                        bool(token)
                        and token not in _RELATION_FOLLOW_KEYWORDS
                        and token not in _FROM_JOIN_NON_TABLE
                    )
                    if is_alias:
                        while index < cursor:
                            result.append(query[index])
                            index += 1
                        result.append(raw_alias)
                        return alias_end
        while index < cursor:
            result.append(query[index])
            index += 1
        return index

    def read_parquet(self, path: str | Path) -> DataFrame:
        """Read a Parquet file or directory into a :class:`DataFrame` (``spark.read.parquet``)."""
        inner = self._ensure_alive()
        return DataFrame(inner.read_parquet(str(path)), inner, self._alive_token)

    def read_csv(
        self,
        path: str | Path,
        options: dict[str, str] | None = None,
    ) -> DataFrame:
        """Engine entry for CSV reads (``spark.read.csv`` / ``format("csv").load``).

        Options are a string map (Spark keys). Rows never cross the Python boundary as Python
        objects — only Arrow via collect/to_arrow.
        """
        inner = self._ensure_alive()
        frame = inner.read_csv(str(path), options)
        return DataFrame(frame, inner, self._alive_token)

    def read_json(
        self,
        path: str | Path,
        options: dict[str, str] | None = None,
    ) -> DataFrame:
        """Engine entry for JSON reads (``spark.read.json`` / ``format("json").load``)."""
        inner = self._ensure_alive()
        frame = inner.read_json(str(path), options)
        return DataFrame(frame, inner, self._alive_token)

    def read_postgres(
        self,
        url: str,
        dbtable: str | None = None,
        query: str | None = None,
        properties: dict[str, str] | None = None,
        partition_column: str | None = None,
        lower_bound: int | None = None,
        upper_bound: int | None = None,
        num_partitions: int | None = None,
        predicates: list[str] | None = None,
    ) -> DataFrame:
        """Engine entry for PostgreSQL reads (used by :meth:`DataFrameReader.jdbc` / format load).

        Rows never cross the Python boundary as Python objects — only Arrow via collect/to_arrow.
        """
        inner = self._ensure_alive()
        frame = inner.read_postgres(
            url,
            dbtable,
            query,
            properties,
            partition_column,
            lower_bound,
            upper_bound,
            num_partitions,
            predicates,
        )
        return DataFrame(frame, inner, self._alive_token)

    def read_excel(
        self,
        path: str | Path,
        options: dict[str, str] | None = None,
    ) -> DataFrame:
        """Engine entry for Excel reads (``spark.read.excel`` disclosed extension).

        Pure-Rust calamine path; rows never cross the Python boundary as Python objects —
        only Arrow via collect/to_arrow. See ``task/t5-excel-ledger.md``.
        """
        inner = self._ensure_alive()
        frame = inner.read_excel(str(path), options)
        return DataFrame(frame, inner, self._alive_token)

    def excel_sheet_names(self, path: str | Path) -> list[str]:
        """List Excel workbook sheet names in workbook order (first is the v1 default)."""
        inner = self._ensure_alive()
        return list(inner.excel_sheet_names(str(path)))

    def table(self, table_name: str) -> DataFrame:
        """Load a catalog or temp-view table by name (PySpark ``spark.table``).

        Implemented as ``SELECT * FROM <quoted multipart identifier>`` through the SQL path so
        Iceberg catalog tables and temporary views both resolve. Bare and two-part names expand
        under the session default catalog + namespace; a one-part temp view resolves to its
        HOME, never bare (R7-1). Only identifier segments are accepted (``catalog.db.table``);
        SQL fragments (``UNION``, ``JOIN``, subqueries) raise
        :class:`~repark.errors.AnalysisException` — this is an identifier API, not free SQL.
        """
        if not isinstance(table_name, str):
            from repark.errors import PySparkTypeError

            raise PySparkTypeError(
                errorClass="NOT_STR",
                messageParameters={
                    "arg_name": "tableName",
                    "arg_type": type(table_name).__name__,
                },
            )
        # Prefer temp-view for one-part names (Spark: temps before current db). Use the native
        # SQL path directly so the free-SQL DROP rewriter cannot re-qualify a temp view.
        table_ref = self._sql_table_ref_resolved(table_name, prefer_temp_view=True)
        inner = self._ensure_alive()
        return DataFrame(inner.sql(f"SELECT * FROM {table_ref}"), inner, self._alive_token)

    def registerTempTable(self, name: str, table: Any) -> None:  # noqa: N802
        """Unsupported legacy alias of createOrReplaceTempView (R-FACADE-HYGIENE W7)."""
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "SparkSession.registerTempTable is not supported "
            "(removed in modern PySpark; use DataFrame.createOrReplaceTempView)"
        )

    @property
    def pandas_api(self) -> Any:
        """Unsupported pandas-on-Spark API (R-FACADE-HYGIENE W7)."""
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "SparkSession.pandas_api is not supported "
            "(use DataFrame.to_pandas / to_polars; disclosed R-FACADE-HYGIENE)"
        )

    @property
    def read(self) -> DataFrameReader:
        """The DataFrame reader (PySpark ``spark.read``): parquet, table, format/load, option."""
        return DataFrameReader(self)

    def range(
        self,
        start: int | float,
        end: int | float | None = None,
        step: int | float = 1,
        numPartitions: int | None = None,  # noqa: N803 — PySpark camelCase
    ) -> DataFrame:
        """Create a single-column ``id`` DataFrame over ``[start, end)`` (PySpark ``range``).

        ``range(end)`` → ids ``0 .. end-1``; ``range(start, end, step, numPartitions)`` →
        arithmetic sequence exclusive of ``end``. Engine path: DataFusion
        ``generate_series`` (inclusive stop rewritten to Spark's exclusive end); values are
        BIGINT / Arrow ``int64`` (Spark ``LongType``). Facade ``schema``/``dtypes`` currently
        collapse Arrow Int64 → ``IntegerType`` / ``"int"`` for *all* bigint columns (shared
        mapping; remapping is deferred types work) — the physical type remains int64 on
        ``to_arrow()``. ``numPartitions`` is accepted for API parity and ignored on the
        single-node backend. Bounds/step are coerced with ``int(...)`` (bool rejected).
        Plan construction is lazy; a full ``count()``/``collect()`` of a huge range still scans
        (no Spark Range stats shortcut on the single-node backend).
        """
        self._ensure_alive()
        from repark.errors import IllegalArgumentException

        if end is None:
            range_start = 0
            range_end = _range_bound_as_int("end", start)
        else:
            range_start = _range_bound_as_int("start", start)
            range_end = _range_bound_as_int("end", end)
        range_step = _range_bound_as_int("step", step)
        if range_step == 0:
            raise IllegalArgumentException("range step must not be zero")
        # Accepted for Spark API parity; single-node backend has no partition fan-out.
        if numPartitions is not None:
            _ = _range_bound_as_int("numPartitions", numPartitions)
            if int(numPartitions) < 1:
                raise IllegalArgumentException("numPartitions must be >= 1 when set")

        # Spark end is exclusive; generate_series stop is inclusive → back up one step unit.
        inclusive_stop = range_end - (1 if range_step > 0 else -1)
        empty = (range_step > 0 and range_start >= range_end) or (
            range_step < 0 and range_start <= range_end
        )
        if empty:
            query = "SELECT CAST(value AS BIGINT) AS id FROM generate_series(0, -1, 1) WHERE false"
        else:
            query = (
                "SELECT CAST(value AS BIGINT) AS id FROM generate_series("
                f"{range_start}, {inclusive_stop}, {range_step})"
            )
        return self.sql(query)

    def create_dataframe(
        self,
        data: Any,
        schema: Any = None,
    ) -> DataFrame:
        """Build a :class:`DataFrame` from rows (PySpark ``createDataFrame``).

        Promotes this session to process-active (Spark parity). Accepts:

        * a list of tuples/lists (optional ``schema`` column-name list; positional bind)
        * a list of dicts — **Spark key-union** across rows: column order is first-row keys
          then newly seen keys; missing fields null-fill; later-row extra keys become columns
        * a list of :class:`~repark.row.Row` — fail-loud name bind, no key-union (Spark
          ``STRUCT_ARRAY_LENGTH_MISMATCH`` class on key mismatch)
        * a pandas ``DataFrame`` (optional; empty frames fail with CANNOT_INFER_EMPTY_SCHEMA;
          nested list/struct Arrow columns accepted via the native Arrow path)
        * a polars ``DataFrame`` (optional; same empty rules; nested ``List``/``Struct``
          accepted via ``.to_arrow()``; Binary/Time/Duration still refuse)

        Row-as-dict ingestion is **not** governed by
        ``spark.sql.pyspark.inferNestedDictAsStruct.enabled`` (SPARK-35929) — that conf only
        affects **dict-valued cells** (column values at any nesting depth). When the conf is
        ``true`` (**the repark default** — a declared divergence from PySpark's ``false``), a
        dict cell infers as StructType with field union + null-fill; when ``false``, dict
        cells stay MapType, byte-identical to PySpark's default. Explicit ``schema=`` wins
        either way.

        **Wrapped JSON objects** (``{"Orders":[...]}``): ``spark.read.json`` is NDJSON /
        top-level-array shaped. Load a single object wrapper with ``json.load`` →
        ``spark.createDataFrame(payload["Orders"])`` (dict key-union path).

        ``schema=`` forms:

        * ``None`` — infer names (Python-int → BIGINT widening, disclosed)
        * ``list``/``tuple`` of ``str`` names — name bind only (types still inferred)
        * :class:`~repark.types.StructType` — names **and** types (``IntegerType`` →
          INT/int32, preserving the int32 path that bare VALUES widens)
        * DDL string ``"a INT, b STRING"`` — parsed to StructType-equivalent (live Spark form)

        ``schema=[names]`` bind (named sources — dict / Row / namedtuple / ``NamedTuple`` /
        pandas / polars): a **pure reorder** (same names, different order) binds **by name**
        so values follow names; a **pure rename** (same length, no shared names) binds
        **positionally**; partial overlap or length mismatch fails loud (no silent
        project/drop/swap). Plain tuple/list rows bind positionally;
        ``collections.namedtuple`` / ``typing.NamedTuple`` use ``_fields`` as source names
        (``schema=[names]`` reorders by name like dict/Row).

        Materializes into a MemTable once via a ``pyarrow.Table`` registered with an Arrow
        **C Stream** (no IPC encode intermediate; IPC remains the version-skew fallback).
        Cell types: ``None``, ``bool``, ``int``, ``float``, ``str``, ``datetime.date``,
        ``datetime.datetime`` (tz-aware → UTC then naive; pandas ``Timestamp`` /
        ``datetime64`` / ``numpy.datetime64`` included — ns units do not collapse to epoch
        int; calendar units ``D``/``W``/``M``/``Y`` stay DATE including all-null NaT
        witnesses), and ``decimal.Decimal`` (fixed-point into
        ``DECIMAL(38,18)``; values outside that envelope fail loud — no silent zero/round).

        All-null columns: empty list + name schema emit ``CAST(NULL AS VARCHAR)``;
        pure-``None`` columns without a typed witness do the same. All-null pandas/polars
        typed columns keep a dtype-matched ``CAST(NULL AS …)`` (integers always ``BIGINT`` so
        null occupancy cannot flip int32↔int64); columns of typed missing markers
        (``float('nan')``, ``NaT``, ``numpy.datetime64('NaT')``, …) keep
        DOUBLE/TIMESTAMP/DATE from those witnesses rather than collapsing to VARCHAR.
        Timedelta / duration / ``numpy.timedelta64`` refuse even when all-null (no silent
        duration→int); pandas Interval / Period and polars Binary/Time/Duration
        refuse rather than fail-open as BIGINT/DOUBLE/VARCHAR); nested List/Struct
        (polars + pandas ArrowDtype) land via the Arrow path;
        pandas categorical all-null follows ``categories.dtype``; pandas ArrowDtype
        time/binary still refuse.
        """
        self._ensure_alive()
        self._promote_active()
        return _create_dataframe_from_rows(self, data, schema)

    createDataFrame = create_dataframe  # noqa: N815 — deliberate PySpark-compatible camelCase alias

    @property
    def catalog(self) -> Catalog:
        """The metadata surface (PySpark ``spark.catalog``): tableExists, listDatabases,
        currentCatalog, … (R-CURCAT-FACADE)."""
        self._ensure_alive()
        return Catalog(self)

    @property
    def conf(self) -> RuntimeConfig:
        """Runtime configuration map (PySpark ``spark.conf``) — set/get/unset.

        Backs Apache ``sql_conf`` context managers and a few SQL-flag gates (e.g.
        ``spark.sql.crossJoin.enabled``). Most values live on the session alive token
        (facade-local). Keys under ``datafusion.`` are **forwarded** to the live DataFusion
        session — see :class:`RuntimeConfig` for the memory-pool one-truth contract
        (``repark.memory.limit.gb`` build-time vs ``datafusion.runtime.memory_limit`` runtime).
        """
        self._ensure_alive()
        return RuntimeConfig(self)

    # Active session + context manager + newSession
    @classmethod
    def getActiveSession(cls) -> ReparkSession | None:  # noqa: N802 — PySpark camelCase
        """Return the process-wide active session, or ``None`` when none is live.

        PySpark ``SparkSession.getActiveSession`` parity for the single-node facade
        (one active session per process).
        """
        if _sf._active_session is not None and _sf._active_session._inner is not None:
            return _sf._active_session
        return None

    @classmethod
    def active(cls) -> ReparkSession:
        """Return the active session, or raise when none exists.

        Raises :class:`~repark.errors.PySparkRuntimeError` with
        ``NO_ACTIVE_OR_DEFAULT_SESSION`` when no session is live.
        """
        session = cls.getActiveSession()
        if session is None:
            raise PySparkRuntimeError(
                errorClass="NO_ACTIVE_OR_DEFAULT_SESSION",
                messageParameters={},
            )
        return session

    def newSession(self) -> ReparkSession:  # noqa: N802 — PySpark camelCase
        """Create a distinct session with the same builder config snapshot.

        Always builds a **new** engine handle (unlike :meth:`Builder.getOrCreate`) and does
        **not** steal the process active session — the caller stays active until an action
        on the new session promotes it (Spark parity).

        Active-session restore uses ``try``/``finally`` so ``BaseException``
        (``KeyboardInterrupt`` / ``SystemExit``) cannot leave the process pointer on the
        half-built session after ``getOrCreate`` cleared it.
        """
        self._ensure_alive()
        # Force a fresh build: clear active so getOrCreate does not reuse self.
        previous = _sf._active_session
        _sf._active_session = None
        new: ReparkSession | None = None
        try:
            builder = ReparkSession.builder
            for key, value in self._builder_config.items():
                builder = builder.config(key, value)
            new = builder.getOrCreate()
            return new
        finally:
            # Always restore prior active — including BaseException paths. getOrCreate
            # may have registered ``new`` as active; newSession must not steal it.
            _sf._active_session = previous

    def _promote_active(self) -> None:
        """Mark this session as the process-wide active session (Spark action promotion)."""
        if self._inner is not None:
            _sf._active_session = self

    def __enter__(self) -> ReparkSession:
        """Context-manager enter (PySpark ``with SparkSession… as spark``)."""
        self._ensure_alive()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        """Context-manager exit — always :meth:`stop` the session."""
        self.stop()

    def _catalog_state(self) -> dict[str, Any]:
        """Facade-only current-catalog box (R-CURCAT); raises if the session is stopped."""
        self._ensure_alive()
        state = self._alive_token.get("catalog_state")
        if not isinstance(state, dict):
            raise RuntimeError("catalog state missing on session (internal error)")
        return state

    def _ensure_information_schema(self) -> None:
        """Enable DataFusion ``information_schema`` once (powers Catalog.listTables)."""
        state = self._catalog_state()
        if state.get("information_schema_enabled"):
            return
        self.sql("SET datafusion.catalog.information_schema = true")
        state["information_schema_enabled"] = True

    def _note_registered_catalog(self, name: str) -> None:
        """Track a newly registered catalog on the facade current-catalog state."""
        state = self._catalog_state()
        known: set[str] = state["known_catalogs"]
        known.add(name)
        # If current catalog is still the default and spark_catalog is unregistered or only
        # the AUTO-registered fallback (R-AUTO-MEMCAT — not a user choice), flip to the first
        # user-registered catalog so listDatabases works without setCurrentCatalog.
        current = str(state["current_catalog"])
        default_is_auto_only = bool(state.get("auto_default_catalog"))
        if (
            name != DEFAULT_CATALOG_NAME
            and current == DEFAULT_CATALOG_NAME
            and (
                DEFAULT_CATALOG_NAME not in known
                or (default_is_auto_only and not state.get("auto_flip_done"))
            )
        ):
            state["current_catalog"] = name
            # One flip only; `auto_default_catalog` itself stays sticky so spark_catalog
            # refs keep aliasing to the user catalog (the auto catalog never blocks).
            state["auto_flip_done"] = True

    def register_memory_catalog(self, name: str, warehouse: str | Path) -> None:
        """Register the AWS-free in-memory Iceberg catalog under ``name`` (a RePark extension).

        Table metadata lives in process memory; data files are written under ``warehouse`` on
        the local filesystem — the local-development/test analogue of a Glue catalog.
        Production Glue / S3 Tables catalogs register via ``spark.sql.catalog.<name>.*`` /
        ``repark.sql.catalog.<name>.*`` builder config (or the Rust ``register_iceberg_catalog``
        path). Create namespaces before use::

            spark.register_memory_catalog("glue_catalog", "/tmp/warehouse")
            spark.sql("CREATE NAMESPACE glue_catalog.example_silver")

        Bare-name resolution still qualifies to ``name.currentDatabase.t``; the namespace must
        exist for writes.
        """
        self._ensure_alive().register_memory_catalog(name, str(warehouse))
        self._note_registered_catalog(name)

    def _register_auto_memory_catalog(self) -> None:
        """Auto-register the session-scoped ``spark_catalog`` memory catalog (R-AUTO-MEMCAT).

        The ``duckdb.connect(":memory:")`` analogue: a bare ``builder.getOrCreate()`` gets a
        working default catalog + ``default`` namespace so first-session bare-name flows
        work with zero config. Data files live in a session-scoped temp warehouse removed on
        :meth:`stop`; the catalog's table *metadata* is process-memory already. Registration
        failure is non-fatal (warn + continue): a session without a default catalog is the
        pre-existing behavior, not a broken session.
        """
        import tempfile

        try:
            tmpdir = tempfile.TemporaryDirectory(prefix="repark-spark-catalog-")
            self.register_memory_catalog(DEFAULT_CATALOG_NAME, tmpdir.name)
            # Tie warehouse lifetime to the session; stop() cleans it (R-AUTO-MEMCAT).
            self._alive_token["auto_catalog_warehouse"] = tmpdir
            # Auto ≠ user choice: the first USER-registered catalog may still take
            # currentCatalog (see _note_registered_catalog).
            self._catalog_state()["auto_default_catalog"] = True
            # Spark's `default` database always exists — seed it so first writes work.
            self.create_namespace(DEFAULT_CATALOG_NAME, DEFAULT_DATABASE_NAME)
        except Exception as error:  # pragma: no cover — engine/filesystem edge
            warnings.warn(
                f"repark could not auto-register the default memory catalog: {error}; "
                "register one explicitly (register_memory_catalog / spark.sql.catalog.*)",
                UserWarning,
                stacklevel=2,
            )

    def read_iceberg_table(
        self,
        table_name: str,
        *,
        snapshot_id: int | None = None,
        as_of_timestamp_ms: int | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> DataFrame:
        """Read an Iceberg catalog table, optionally time-travel pinned (R-TIME-TRAVEL).

        At most one of ``snapshot_id`` / ``as_of_timestamp_ms`` / ``branch`` / ``tag`` may be
        set. Engine path: fork ``IcebergStaticTableProvider::try_new_from_table_snapshot`` —
        never a post-hoc filter.
        """
        from repark.errors import AnalysisException

        # Gate i64 domain before PyO3 — Python ints are unbounded.
        if snapshot_id is not None and (snapshot_id < _I64_MIN or snapshot_id > _I64_MAX):
            raise AnalysisException(
                f"snapshot_id must fit a signed 64-bit integer, got {snapshot_id!r}"
            )
        if as_of_timestamp_ms is not None and (
            as_of_timestamp_ms < _I64_MIN or as_of_timestamp_ms > _I64_MAX
        ):
            raise AnalysisException(
                f"as_of_timestamp_ms must fit a signed 64-bit integer, got {as_of_timestamp_ms!r}"
            )
        # Qualify bare / two-part / spark_catalog names like table() / writers;
        # TT targets Iceberg catalog tables, not temp views.
        resolved = self.resolve_table_name(table_name, prefer_temp_view=False)
        inner = self._ensure_alive()
        frame = inner.read_iceberg_table(
            resolved,
            snapshot_id,
            as_of_timestamp_ms,
            branch,
            tag,
        )
        return DataFrame(frame, inner, self._alive_token)

    def _testing_create_ref(
        self,
        table_name: str,
        kind: str,
        name: str,
        snapshot_id: int,
    ) -> None:
        """Test-support only: create a branch/tag via fork ManageSnapshots.

        Public SQL ``ALTER TABLE … CREATE BRANCH|TAG`` remains loud-unsupported (P6); this
        seam stays for fixtures only (see map.md).
        """
        self._ensure_alive().testing_create_ref(table_name, kind, name, snapshot_id)

    def _testing_list_snapshots(self, table_name: str) -> list[tuple[int, int]]:
        """Test-support only: ``(snapshot_id, timestamp_ms)`` pairs in history order."""
        return list(self._ensure_alive().testing_list_snapshots(table_name))

    def list_iceberg_table_names(self, catalog: str, namespace: str) -> list[str]:
        """Live Iceberg table names in ``namespace`` (list-on-access; no DF provider snapshot).

        Used by :meth:`repark.catalog.Catalog.list_tables` so out-of-band creates/drops are
        visible (T6 / CQ-008 / BUG-007). Non-Iceberg catalogs raise.
        """
        return list(self._ensure_alive().list_iceberg_table_names(catalog, namespace))

    def list_temp_view_names(self) -> list[str]:
        """Session temp-view names from the default catalog/schema (no ``information_schema``).

        Used by :meth:`repark.catalog.Catalog.list_tables` so temp listing never loads phantom
        Iceberg provider names after an out-of-band drop (T6 F-T6-PHANTOM-A / F-T6-TEMP-A).
        """
        return list(self._ensure_alive().list_temp_view_names())

    def list_df_schema_table_names(self, catalog: str, schema: str) -> list[str]:
        """DataFusion provider name directory for ``catalog.schema`` (no table load).

        Non-Iceberg permanent fallback for :meth:`repark.catalog.Catalog.list_tables`. Iceberg
        catalogs should use :meth:`list_iceberg_table_names` (live); this path is snapshot-stale.
        """
        return list(self._ensure_alive().list_df_schema_table_names(catalog, schema))

    def refresh_catalog_provider(self, catalog: str) -> None:
        """Rebuild the DataFusion catalog provider from the live Iceberg handle.

        Product SQL re-registers after owned DDL. Call after out-of-band mutations when free
        SQL / ``information_schema`` must see the new name directory. Facade ``listTables``
        does not need this (it lists live for permanents + native temp names).
        """
        self._ensure_alive().refresh_catalog_provider(catalog)

    def _testing_oob_create_table(
        self,
        catalog_name: str,
        namespace: str,
        table: str,
        warehouse_location: str,
    ) -> None:
        """Test-support only: Catalog-API create without DF provider re-register (OOB create)."""
        self._ensure_alive().testing_oob_create_table(
            catalog_name, namespace, table, warehouse_location
        )

    def _testing_oob_drop_table(self, catalog_name: str, namespace: str, table: str) -> None:
        """Test-support only: Catalog-API drop without DF provider re-register (OOB drop)."""
        self._ensure_alive().testing_oob_drop_table(catalog_name, namespace, table)

    def create_namespace(
        self,
        catalog: str,
        namespace: str,
        location: str | None = None,
    ) -> None:
        """Create a namespace in a registered catalog, optionally with a ``location`` property.

        SQL ``CREATE NAMESPACE … LOCATION`` / ``WITH DBPROPERTIES`` can also set properties
        (WG-5); either way, a namespace destined for a Glue / S3
        Tables catalog must be created here with its warehouse ``location`` — otherwise a later CTAS
        into it fails loud (it has no path to write to). The engine stores ``location`` under BOTH
        the ``location`` and ``location_uri`` namespace property keys, so the canonical Glue
        database ``locationUri`` is set whichever key the catalog maps (audit BUG-001); reads fall
        back to ``location_uri``, so CTAS into a pre-existing Glue database also works.
        ``location=None`` creates a property-less namespace (fine for the in-memory / local
        catalog)::

            spark.create_namespace(
                "glue_catalog", "silver", location="s3://bucket/warehouse/silver"
            )
        """
        self._ensure_alive().create_namespace(catalog, namespace, location)

    # spark.udf namespace (sole-writer region)
    def _udf_registry(self) -> dict[str, dict[str, Any]]:
        """Session-scoped Python UDF registry (name → entry). Dies with :meth:`stop`."""
        self._ensure_alive()
        registry = self._alive_token.get("udf_registry")
        if registry is None:
            registry = {}
            self._alive_token["udf_registry"] = registry
        return registry  # type: ignore[return-value]

    def _udtf_registry(self) -> dict[str, Any]:
        """Session-scoped Python UDTF registry (name → UserDefinedTableFunction). Dies with stop."""
        self._ensure_alive()
        registry = self._alive_token.get("udtf_registry")
        if registry is None:
            registry = {}
            self._alive_token["udtf_registry"] = registry
        return registry  # type: ignore[return-value]

    @property
    def udf(self) -> UDFRegistration:
        """UDF registration namespace (PySpark ``spark.udf``).

        Provides :meth:`UDFRegistration.register` and
        :meth:`UDFRegistration.registerJavaFunction` (Java is loud-unsupported).
        """
        self._ensure_alive()
        return UDFRegistration(self)

    @property
    def udtf(self) -> Any:
        """UDTF registration namespace (PySpark ``spark.udtf``).

        Construction validates handlers (Spark ``INVALID_UDTF_*``).
        :meth:`~repark.udtf.UDTFRegistration.register` stores the UDTF for SQL
        ``SELECT * FROM name(lit_args)``. Call with foldable lit args builds a
        DataFrame via mapInArrow. LATERAL / table-arg stay blocked.
        """
        from repark.spark.udtf import UDTFRegistration

        self._ensure_alive()
        return UDTFRegistration(self)

    def _sql_with_registered_udfs(self, query: str) -> DataFrame | None:
        """Structural scan for **registered** UDF names; rewrite SELECT-list or refuse.

        Returns ``None`` when no registered UDF name appears as ``name(`` (not preceded
        by ``.``). Never scans generic identifiers — only the session registry. Bounds:

        * UDF in ORDER BY *expression* / JOIN ON / nested ``(SELECT`` / ``(WITH`` →
          :class:`~repark.errors.UnsupportedOperationException`.
        * SELECT-list: simple ``udf(col|lit)`` **and** expression-wrapped forms
          (``udf(x)+1``, ``CAST(udf(x) AS …)``, nested ``f(g(x))``) — UDF calls materialize
          via the DataFrame bridge; residual expression is engine-side.
        * WHERE / GROUP BY / HAVING scalar UDF forms rewrite similarly — residual
          predicates / keys applied post-materialization; never leak
          ``__repark_sql_udf_*`` internal names.
        * WITH/CTE bodies and ``SELECT DISTINCT`` projections.
        * ``ORDER BY`` on SELECT aliases of rewritten UDF outputs — post-materialization
          ``orderBy`` (never leak ``__repark_sql_udf_*`` internal names).
        """
        from repark.errors import UnsupportedOperationException

        if not isinstance(query, str):
            return None
        registry = self._udf_registry()
        if not registry:
            return None

        trivia, body = _split_leading_sql_trivia(query)
        if not body:
            return None

        hits = _sql_collect_registry_udf_hits(body, registry)
        if not hits:
            return None

        # WITH / CTE: rewrite each CTE body region (and outer) that hits the registry.
        if re.match(r"(?is)^WITH\b", body.strip()):
            return self._sql_with_udfs_in_with_statement(trivia, body, registry)

        try:
            return self._sql_rewrite_select_with_udfs(trivia, body, registry, hits)
        except UnsupportedOperationException:
            raise
        except Exception as error:
            raise _sql_udf_clean_exception(error) from error

    def _sql_with_udfs_in_with_statement(
        self,
        trivia: str,
        body: str,
        registry: dict[str, dict[str, Any]],
    ) -> DataFrame:
        """Rewrite registered UDFs inside WITH/CTE bodies (F1-style regions)."""
        from repark.errors import UnsupportedOperationException

        match = re.match(r"(?is)^WITH\b", body)
        if match is None:
            raise UnsupportedOperationException(
                "internal WITH UDF rewrite expected a WITH statement"
            )
        index = match.end()
        length = len(body)
        while index < length and body[index].isspace():
            index += 1
        if index + 9 <= length and body[index : index + 9].upper() == "RECURSIVE":
            raise UnsupportedOperationException(
                "registered Python UDF inside WITH RECURSIVE is not supported in repark v1; "
                "flatten the CTE or use the DataFrame udf path"
            )
            # unreachable — keep branch explicit for future RECURSIVE support
        # (name, prior_frame_or_None) — restore the session catalog after the outer SELECT
        # is planned; WITH must not permanently overwrite/leave user temp views.
        view_snapshots: list[tuple[str, Any | None]] = []
        pieces_prefix = body[: match.end()]
        _ = pieces_prefix  # WITH keyword retained only for diagnostics

        try:
            while index < length:
                name_end = _scan_sql_table_identifier_end(body, index)
                if name_end is None or name_end == index:
                    break
                raw_name = body[index:name_end]
                cte_name = raw_name.split(".")[-1].strip().strip('"').strip("`")
                index = name_end
                while index < length and body[index].isspace():
                    index += 1
                # Optional column list: ``WITH c(z, y) AS (…)``.
                cte_column_names: list[str] | None = None
                if index < length and body[index] == "(":
                    close = _find_matching_paren(body, index)
                    if close is None:
                        break
                    col_blob = body[index + 1 : close].strip()
                    index = close + 1
                    while index < length and body[index].isspace():
                        index += 1
                    if col_blob:
                        col_parts = _split_sql_select_list(col_blob)
                        parsed_cols: list[str] = []
                        for part in col_parts:
                            token = part.strip()
                            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
                                parsed_cols.append(token)
                            elif re.fullmatch(r'"([^"]|"")*"', token):
                                parsed_cols.append(token[1:-1].replace('""', '"'))
                            elif re.fullmatch(r"`([^`]|``)*`", token):
                                parsed_cols.append(token[1:-1].replace("``", "`"))
                            else:
                                raise UnsupportedOperationException(
                                    f"WITH CTE column list entry {token!r} must be a "
                                    "simple SQL identifier for UDF rewrite in repark v1"
                                )
                        cte_column_names = parsed_cols
                if index + 2 > length or body[index : index + 2].upper() != "AS":
                    break
                index += 2
                while index < length and body[index].isspace():
                    index += 1
                if index >= length or body[index] != "(":
                    break
                close = _find_matching_paren(body, index)
                if close is None:
                    break
                cte_body = body[index + 1 : close].strip()
                index = close + 1
                body_hits = _sql_collect_registry_udf_hits(cte_body, registry)
                if body_hits:
                    cte_frame = self._sql_rewrite_select_with_udfs(
                        "", cte_body, registry, body_hits
                    )
                else:
                    cte_frame = self.sql(cte_body)
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cte_name):
                    raise UnsupportedOperationException(
                        f"WITH CTE name {cte_name!r} must be a simple SQL identifier for "
                        "UDF rewrite in repark v1"
                    )
                if cte_column_names is not None:
                    if len(cte_column_names) != len(cte_frame.columns):
                        raise UnsupportedOperationException(
                            f"WITH CTE {cte_name!r} column list length "
                            f"({len(cte_column_names)}) does not match SELECT list "
                            f"({len(cte_frame.columns)}) for UDF rewrite in repark v1"
                        )
                    cte_frame = cte_frame.toDF(*cte_column_names)
                # Snapshot any pre-existing same-named relation so we can restore after
                # the outer SELECT is planned (engine captures the plan at sql() time).
                prior_frame: Any | None = None
                try:
                    if self.catalog.tableExists(cte_name):
                        prior_frame = self.table(cte_name)
                except Exception:
                    prior_frame = None
                view_snapshots.append((cte_name, prior_frame))
                # Plan-stable temp view (same seam as selectExpr) via the DataFrame API.
                cte_frame.createOrReplaceTempView(cte_name)
                while index < length and body[index].isspace():
                    index += 1
                if index < length and body[index] == ",":
                    index += 1
                    while index < length and body[index].isspace():
                        index += 1
                    continue
                break

            if index >= length:
                raise UnsupportedOperationException(
                    "WITH statement with registered Python UDF is missing a final SELECT "
                    "in repark v1"
                )
            outer = body[index:].strip()
            outer_hits = _sql_collect_registry_udf_hits(outer, registry)
            if outer_hits:
                result = self._sql_rewrite_select_with_udfs(trivia, outer, registry, outer_hits)
            else:
                result = self.sql(trivia + outer if trivia else outer)
            return result
        finally:
            # Query-scoped CTE isolation: restore prior relation or drop the materialization.
            # Outer DataFrame already holds the planned scan (drop/restore-safe).
            for cte_name, prior_frame in reversed(view_snapshots):
                try:
                    if prior_frame is not None:
                        prior_frame.createOrReplaceTempView(cte_name)
                    else:
                        self.catalog.dropTempView(cte_name)
                except Exception:
                    # Best-effort cleanup — never mask the primary result/error.
                    pass

    def _sql_rewrite_select_with_udfs(
        self,
        trivia: str,
        body: str,
        registry: dict[str, dict[str, Any]],
        hits: list[tuple[str, str, int]],
    ) -> DataFrame:
        """Rewrite one SELECT (not WITH) that contains registered UDF hits."""
        from repark.errors import AnalysisException, UnsupportedOperationException
        from repark.spark.functions import UserDefinedFunction

        body_for_scan = body

        select_end = len(body_for_scan)
        from_index = _sql_top_level_keyword_index(body_for_scan, "FROM")
        if from_index is not None:
            select_end = from_index

        # Top-level clause starts (paren depth 0) for hit classification.
        where_index = _sql_top_level_keyword_index(body_for_scan, "WHERE")
        group_index = _sql_top_level_keyword_index(body_for_scan, "GROUP BY")
        having_index = _sql_top_level_keyword_index(body_for_scan, "HAVING")
        order_index = _sql_top_level_keyword_index(body_for_scan, "ORDER BY")
        limit_index = _sql_top_level_keyword_index(body_for_scan, "LIMIT")
        clause_end = functools.partial(
            _sql_clause_end_after,
            later=(where_index, group_index, having_index, order_index, limit_index),
            body_length=len(body_for_scan),
        )

        # Set operations cannot be rewritten as a single SELECT-list materialization
        # (refuse with an accurate shape message, not "outside SELECT list").
        body_upper_for_set = _sql_mask_strings_and_comments(body_for_scan).upper()
        padded_upper = f" {body_upper_for_set} "
        for set_keyword in (" UNION ", " INTERSECT ", " EXCEPT "):
            if set_keyword in padded_upper:
                raise UnsupportedOperationException(
                    "registered Python UDF with UNION / INTERSECT / EXCEPT is not "
                    "supported in repark v1 (SELECT-list rewrite is single-SELECT only). "
                    "Materialize each branch via DataFrame.select / withColumn, then set-op."
                )
        # Hive-style / Spark post-SELECT clauses not peeled by the UDF planner.
        for hive_keyword in (
            " CLUSTER BY ",
            " DISTRIBUTE BY ",
            " SORT BY ",
            " QUALIFY ",
        ):
            if hive_keyword in padded_upper:
                raise UnsupportedOperationException(
                    "registered Python UDF with CLUSTER BY / DISTRIBUTE BY / SORT BY / "
                    "QUALIFY is not supported in repark v1 (SELECT-list rewrite peels "
                    "ORDER BY/LIMIT only). Use DataFrame.orderBy / filter after select."
                )

        for registered_name, matched_text, index in hits:
            if _sql_udf_in_nested_subquery(body_for_scan, index):
                raise UnsupportedOperationException(
                    f"registered Python UDF {registered_name!r} in a nested subquery is "
                    "not supported in repark v1. Flatten the query or use the DataFrame "
                    f"udf path. Matched as {matched_text!r}."
                )
            if index < select_end:
                continue  # SELECT-list hit
            # WHERE / GROUP BY / HAVING spans (top-level keywords only).
            if where_index is not None and where_index <= index < clause_end(where_index):
                continue
            if group_index is not None and group_index <= index < clause_end(group_index):
                continue
            if having_index is not None and having_index <= index < clause_end(having_index):
                continue
            if order_index is not None and order_index <= index < clause_end(order_index):
                raise UnsupportedOperationException(
                    f"registered Python UDF {registered_name!r} in ORDER BY expressions "
                    "is not supported in repark v1 (order by the SELECT-list UDF output "
                    f"alias only). Matched as {matched_text!r}."
                )
            # FROM / JOIN ON / other post-FROM positions (not WHERE/GROUP/HAVING).
            raise UnsupportedOperationException(
                f"registered Python UDF {registered_name!r} cannot be used in this "
                "statement position in repark v1 (supported: SELECT list, WHERE, GROUP BY, "
                "HAVING; not JOIN ON / nested subquery / ORDER BY expression). Apply the "
                f"UDF via DataFrame.select / withColumn. Matched as {matched_text!r}."
            )

        try:
            rewritten = _try_rewrite_select_list_python_udfs(
                body_for_scan,
                registry=registry,
                hits=hits,
            )
        except UnsupportedOperationException:
            raise
        except Exception as error:
            raise UnsupportedOperationException(
                "registered Python UDF in SQL could not be rewritten in repark v1 "
                f"({type(error).__name__}: {_sql_udf_public_error_text(error)}). "
                "Use DataFrame F.udf / spark.udf.register + select/withColumn. "
                "SQL-embedded UDF rewrite supports SELECT-list, WHERE, GROUP BY, and "
                "HAVING scalar forms (U9/U10)."
            ) from error
        if rewritten is None:
            raise UnsupportedOperationException(
                "registered Python UDF in SQL is not supported for this statement shape "
                "in repark v1 (SELECT-list udf forms — simple calls, expression wraps "
                "like udf(x)+1 / CAST(udf(x) AS …) / nested f(g(x)), WITH bodies, "
                "DISTINCT, and scalar WHERE/GROUP BY/HAVING — U9/U10). Use DataFrame "
                "F.udf / spark.udf.register + select/withColumn."
            )

        base_sql, materialize_plan = rewritten
        try:
            base_frame = self.sql(trivia + base_sql if trivia else base_sql)
        except Exception as error:
            raise _sql_udf_clean_exception(error) from error

        from repark.spark.functions import col as f_col

        frame = base_frame
        # Multi-stage UDF materialization (inner nested calls first).
        for stage in materialize_plan["stages"]:
            select_items: list[Any] = []
            # Pass through every column currently on the frame (hidden inputs + prior outs).
            for column_name in frame.columns:
                select_items.append(f_col(column_name))
            for projection in stage:
                if projection["kind"] != "udf":
                    continue
                entry = registry[projection["registered_name"]]
                user_defined = entry["udf"]
                if not isinstance(user_defined, UserDefinedFunction):
                    user_defined = UserDefinedFunction(
                        entry["func"],
                        entry["return_type_sql"],
                        name=projection["registered_name"],
                    )
                args = [f_col(input_name) for input_name in projection["input_names"]]
                select_items.append(user_defined(*args).alias(projection["out_name"]))
            try:
                frame = frame.select(*select_items)
            except Exception as error:
                raise _sql_udf_clean_exception(error) from error

        # WHERE residual filter before user projection (may reference base + UDF temps).
        where_sql = materialize_plan.get("where_sql")
        if where_sql:
            try:
                frame = frame.filter(where_sql)
            except Exception as error:
                raise _sql_udf_clean_exception(error) from error

        # Final user-visible projection (residual expressions + aliases).
        final_exprs: list[str] = materialize_plan["final_exprs"]
        try:
            if final_exprs:
                frame = frame.selectExpr(*final_exprs)
        except Exception as error:
            raise _sql_udf_clean_exception(error) from error

        # GROUP BY on SELECT-list aliases / planned keys (post user projection).
        group_by_keys = materialize_plan.get("group_by_keys")
        if group_by_keys:
            try:
                grouped = frame.groupBy(*group_by_keys)
                # Keys-only GROUP BY (no aggregates in SELECT) ≡ distinct on keys.
                # Preserve SELECT-list order via user_out_names (subset of group keys).
                project_names = materialize_plan.get("user_out_names") or group_by_keys
                frame = grouped.count().select(*project_names)
            except Exception as error:
                raise _sql_udf_clean_exception(error) from error

        # HAVING residual (post-group filter on user-visible names).
        having_sql = materialize_plan.get("having_sql")
        if having_sql:
            try:
                frame = frame.filter(having_sql)
            except Exception as error:
                raise _sql_udf_clean_exception(error) from error

        if materialize_plan.get("distinct"):
            frame = frame.distinct()

        order_by = materialize_plan.get("order_by")
        if order_by:
            try:
                from repark.spark.functions import col as order_col

                order_items: list[Any] = []
                for column_name, ascending in order_by:
                    column = order_col(column_name)
                    order_items.append(column.asc() if ascending else column.desc())
                frame = frame.orderBy(*order_items)
            except Exception as error:
                raise UnsupportedOperationException(
                    "registered Python UDF SELECT with ORDER BY could not be applied "
                    "after materialization in repark v1 "
                    f"({_sql_udf_public_error_text(error)}). Order by the SELECT-list "
                    "output alias only, or use DataFrame.orderBy after select."
                ) from error

        limit_n = materialize_plan.get("limit")
        if limit_n is not None:
            frame = frame.limit(int(limit_n))

        _ = AnalysisException  # used by clean_exception path callers
        return frame

    def stop(self) -> None:
        """Release the engine handle and clear the active-session registry (PySpark ``stop``).

        Subsequent operations on this instance raise :class:`RuntimeError` naming the stopped
        state. Held :class:`SparkContext` and :class:`DataFrame` references are invalidated too.
        Idempotent: a second :meth:`stop` is a no-op.
        A later :meth:`Builder.getOrCreate` builds a fresh session.
        """
        if _sf._active_session is self:
            _sf._active_session = None
        self._spark_context._mark_stopped()
        self._alive_token["alive"] = False
        # R-AUTO-MEMCAT: the auto catalog's temp warehouse dies with the session (:memory:).
        auto_warehouse = self._alive_token.pop("auto_catalog_warehouse", None)
        if auto_warehouse is not None:
            with contextlib.suppress(Exception):
                auto_warehouse.cleanup()
        self._inner = None

    class Builder:
        """The ``ReparkSession.builder`` chain (``.config(...).appName(...).getOrCreate()``)."""

        __slots__ = ("_config",)

        def __init__(self) -> None:
            """Start with an empty config map."""
            # Values may be ``None`` (Spark ``to_str(None)``); native path strips them.
            self._config: dict[str, str | None] = {}

        def config(
            self,
            key: str | None = None,
            value: str | int | bool | None = None,
            conf: Any | None = None,
            *,
            map: dict[str, Any] | None = None,
        ) -> Self:
            """Set a config key (PySpark ``.config``).

            Signature mirrors live PySpark 4.1.2
            (``config(key=None, value=None, conf=None, *, map=None)``):

            * ``.config(key, value)`` / ``.config(key=…, value=…)`` — single pair (values coerced
              with Spark's ``to_str``: bool → ``"true"``/``"false"``, ``None`` stays ``None``,
              other types via ``str(...)``).
            * ``.config(map={...})`` — set many keys from a dict (PySpark 3.4+). This is the
              PySpark form; ``**dict`` unpacking is **not** the API and is not supported.
            * ``.config(conf=…)`` — duck-typed object with ``.getAll() → Iterable[(k, v)]``
              (live ``SparkConf``). repark has no ``SparkConf`` type of its own.

            Precedence when several forms are combined: ``conf is not None`` wins and ignores
            ``map``/key/value; else ``map is not None`` wins and ignores key/value (no error —
            Spark does **not** refuse the combination); else the key/value pair is stored. A
            non-mapping ``map`` raises ``AttributeError`` on ``.items()`` exactly as live Spark
            does.

            Engine knobs (memory / batch / partitions) and ``spark.sql.catalog.<name>.*`` catalog
            blocks are load-bearing; a catalog block registers its catalog at ``getOrCreate`` (a
            malformed block raises there). All other keys are tolerated and ignored, so an existing
            script's ``.config(...)`` calls do not raise.

            Out-of-range engine-knob values follow **Spark's own per-key rule** (SAF-006), checked
            at ``getOrCreate`` — on the build path *and* on the reuse path, as PySpark does:

            * ``spark.sql.execution.arrow.maxRecordsPerBatch`` (and ``repark.batch.size``) — ``0``
              or negative is Spark's documented "no limit" sentinel and is **accepted**. repark
              cannot emit unbounded Arrow batches, so the engine default batch size is used and a
              one-time :class:`UserWarning` discloses that.
            * ``spark.sql.shuffle.partitions`` (and ``repark.target.partitions``) — Spark requires a
              positive value, so ``0`` or negative raises
              :class:`~repark.errors.IllegalArgumentException`. The exception **class** and message
              match live PySpark 4.1.2 (``[INVALID_CONF_VALUE.REQUIREMENT] …``, minus SQLSTATE).
            * ``repark.memory.limit.gb`` — ``0`` opts out of the bounded memory pool; negative
              raises :class:`~repark.errors.IllegalArgumentException`.

            **Disclosed TIMING divergence — the class matches PySpark, the moment does not.** On a
            FRESH process PySpark validates lazily: ``.config(...)`` and ``getOrCreate()`` both
            succeed and the ``IllegalArgumentException`` only surfaces at the first
            ``sessionState`` touch. repark validates eagerly, inside ``getOrCreate()``, so a
            misconfigured session is never handed out and the traceback points at the offending
            ``.config(...)`` chain. **Code that wraps its ``try``/``except`` around the first query
            rather than around ``getOrCreate()`` must move it.** (On the *reuse* path the two
            agree: PySpark applies builder options to the live session and raises right there, and
            so does repark.)

            ``repark.display.style`` (any casing) is stored under the canonical key and collapses
            prior case-variant aliases so dual-cased chains are last-write-wins
            — matching the reuse snapshot collapse in
            :func:`_sync_display_style_into_builder_config`.
            """
            # Classic Builder.config order: conf branch first, then map, else kv. Map/kv
            # values use Spark ``to_str``; conf values from live ``SparkConf.getAll()`` are
            # already strings. Every write routes through ``_set_config_entry`` so the
            # R-DISPLAY key canonicalization (case-insensitive last-wins for
            # ``repark.display.style``) applies uniformly to conf/map/kv forms.
            if conf is not None:
                for conf_key, conf_value in conf.getAll():
                    self._set_config_entry(str(conf_key), _to_str(conf_value))
            elif map is not None:
                for map_key, map_value in map.items():
                    self._set_config_entry(str(map_key), _to_str(map_value))
            else:
                self._set_config_entry(str(key), _to_str(value))
            return self

        def _set_config_entry(self, key: str, value: str | None) -> None:
            """Store one config entry; ``repark.display.style`` aliases canonicalize last-wins
            under the canonical key, case-insensitively (R-DISPLAY harden)."""
            from repark.spark.types import refuse_collation_session_key

            refuse_collation_session_key(key)
            if key.lower() == _DISPLAY_STYLE_KEY:
                for existing in list(self._config):
                    if existing.lower() == _DISPLAY_STYLE_KEY:
                        del self._config[existing]
                self._config[_DISPLAY_STYLE_KEY] = value
                return
            self._config[key] = value

        def app_name(self, name: str) -> Self:
            """Set the application name (PySpark ``.appName``).

            Surfaced via ``session.conf.get("spark.app.name")``. When omitted, the default is
            ``"repark"``.
            """
            self._config["spark.app.name"] = name
            return self

        # PySpark spells this ``appName``; expose both so the one-line import swap just works.
        appName = app_name  # noqa: N815 — deliberate PySpark-compatible camelCase alias

        def master(self, master: str) -> Self:
            """Accept a ``.master(...)`` call for source compatibility (single-node; ignored).

            repark runs single-node (distribution is deferred behind the ``ExecutionBackend``
            seam), so the master URL is recorded but has no effect. The value is warned about ONCE
            per process (OTH-010) so a script pointed at a real cluster URL learns it will run
            single-node instead of silently doing so. The same warn fires if ``spark.master`` is
            set only via :meth:`config`.
            """
            _warn_master_once(stacklevel=2)
            self._config["spark.master"] = master
            return self

        def get_or_create(self) -> ReparkSession:
            """Return the active session, or build and register one (PySpark ``.getOrCreate``).

            If a live session already exists in this process, it is returned. Engine-knob values
            whose validation is FACADE-side are still **validated** first, so an out-of-range knob
            raises on the reuse path exactly as it does on the build path — live PySpark 4.1.2
            applies builder options to the existing session and raises there, and repark must not
            be the laxer of the two on the ubiquitous notebook / long-lived-process path (audit G3).

            **One knob is carved out of that promise, deliberately (H-1a, D-A1):**
            ``spark.sql.session.timeZone``. Its validity is not a range check — it needs the
            engine's zone database — and the repo keeps exactly one validator, in the engine, at
            session build. On the reuse path no session is built, so an invalid zone is neither
            validated nor applied: the engine-knob warning below fires and ``conf.get`` keeps
            reporting the live session's real zone. repark is knowingly laxer than PySpark here,
            on this key only; pinned by
            ``test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise``.

            **Disclosed divergence on reuse:** PySpark *applies* the builder's options to the live
            session (``.config("spark.sql.shuffle.partitions", "7")`` really changes it to 7);
            repark's engine knobs are fixed at session build, so a differing config is only
            reported with a :class:`UserWarning` and the active session is returned unchanged.
            After :meth:`ReparkSession.stop`, the next call builds a fresh session.

            When ``spark.master`` is present in the builder config (including via ``.config`` only),
            the OTH-010 single-node warning is emitted once per process if not already warned.
            """
            # OTH-010: any builder that carries spark.master (create OR reuse) warns once
            # (reuse path must not skip disclosure). Key match is
            # case-insensitive.
            if any(key.lower() == "spark.master" for key in self._config):
                _warn_master_once(stacklevel=2)

            # Resolve + range-check BEFORE the reuse short-circuit (audit G3): live PySpark 4.1.2
            # raises `IllegalArgumentException` for `spark.sql.shuffle.partitions=0` against an
            # ALREADY-ACTIVE session, so short-circuiting first would silently accept a value Spark
            # refuses — and would swallow the SAF-006 batch-sentinel disclosure too.
            # One-truth gate before either reuse or build.
            _refuse_dual_memory_pool_knobs(self._config)
            # Whitespace-normalize the zone BEFORE it is stored or forwarded, because the engine
            # trims before parsing and would otherwise build a session holding a zone string the
            # facade's own `conf.get` does not report. Normalization only — the engine stays the
            # SOLE validator of what a zone is.
            normalize_session_time_zone_config(self._config)
            normalize_timestamp_type_config(self._config)
            memory_limit_gb = self._resolve_memory_limit_gb()
            batch_size = self._resolve_batch_size()
            target_partitions = self._resolve_shuffle_partitions()

            # Facade-only knob: resolve + validate before reuse so a typo still fails loud, and
            # apply on reuse (display style is runtime-mutable, not fixed at engine build).
            display_style = self._resolve_display_style()

            if _sf._active_session is not None and _sf._active_session._inner is not None:
                # PySpark parity on the notebook path: Spark instantiates catalogs lazily per
                # name, so a catalog configured by a LATER builder works against the
                # already-active session. Register NEW `spark.sql.catalog.*` names onto the live
                # session; already-registered names keep their registration. A malformed block
                # or a failing catalog raises here exactly as it would at build.
                config = dict(self._config)
                added, skipped = _sf._active_session._inner.register_late_catalogs(config)
                added_set = set(added)
                for catalog_name in added:
                    _sf._active_session._note_registered_catalog(catalog_name)
                catalog_prefix = "spark.sql.catalog."
                for key, value in config.items():
                    if key.lower().startswith(catalog_prefix):
                        name = key[len(catalog_prefix) :].split(".", 1)[0]
                        if name in added_set:
                            # Fold the newly-applied catalog block into the recorded builder
                            # config so a repeat getOrCreate with this builder does not re-warn.
                            _sf._active_session._builder_config[key] = value
                # Fold facade-only config keys into the live session's RuntimeConfig +
                # builder snapshot (PySpark setConfString on reuse). Engine knobs stay fixed
                # at build and still warn when they differ.
                engine_key_set = {
                    key.lower()
                    for key in (
                        *_MEMORY_LIMIT_KEYS,
                        *_BATCH_SIZE_KEYS,
                        *_TARGET_PARTITIONS_KEYS,
                        # The session zone is fixed at build like the other engine knobs, so
                        # reuse must NOT fold a new value into the facade conf — it would
                        # report a zone the live engine session does not have. It falls into
                        # `unapplied` below and rides the existing engine-knob warning.
                        *SESSION_TIME_ZONE_KEYS,
                        *TIMESTAMP_TYPE_KEYS,
                    )
                }
                runtime_conf = RuntimeConfig(_sf._active_session)._store()
                for key, value in config.items():
                    key_lower = key.lower()
                    if key_lower == _DISPLAY_STYLE_KEY:
                        continue
                    if key_lower in engine_key_set:
                        continue
                    if key.lower().startswith("spark.sql.catalog."):
                        # Catalog blocks handled above via register_late_catalogs.
                        continue
                    if key in _SQLCONF_STATIC_KEYS:
                        # Static conf is not runtime-modifiable.
                        continue
                    if value is None:
                        continue
                    text = value if isinstance(value, str) else str(value)
                    # Reuse fold writes the store without RuntimeConfig.set —
                    # refuse a planted collation key here (SEC-003).
                    from repark.spark.types import refuse_collation_session_key

                    refuse_collation_session_key(key)
                    # datafusion.* is runtime-mutable on the live engine — fold via
                    # RuntimeConfig.set so SQL SET forwards (not store-only). Lookalike
                    # mixed-case / padded keys refuse-loud inside set.
                    if _looks_like_datafusion_conf_key(key):
                        RuntimeConfig(_sf._active_session).set(key, text)
                        _sf._active_session._builder_config[key] = text
                        continue
                    runtime_conf[key] = text
                    _sf._active_session._builder_config[key] = text
                    # Soft fold is a set — clear any prior conf.unset tombstone.
                    RuntimeConfig(_sf._active_session)._unset_keys().discard(key)
                unapplied = {
                    key
                    for key, value in config.items()
                    if _sf._active_session._builder_config.get(key) != value
                    # repark.display.style is always applied on the reuse path below
                    # (facade-only, runtime-mutable) — excluded so a pure style delta does
                    # not false-warn (R-DISPLAY); engine knobs still warn as before.
                    and key.lower() != _DISPLAY_STYLE_KEY
                    # Static conf is deliberately not folded — exclude so
                    # the warn does not claim a failed apply for an intentional refuse.
                    and key not in _SQLCONF_STATIC_KEYS
                }
                if unapplied:
                    skipped_note = (
                        f" already-registered catalogs keep their configuration: "
                        f"{sorted(set(skipped) & _late_catalog_names(unapplied))};"
                        if skipped
                        else ""
                    )
                    warnings.warn(
                        "Using an existing ReparkSession; some configuration may not apply "
                        f"(engine knobs are fixed at session build;{skipped_note} "
                        f"unapplied keys: {sorted(unapplied)}).",
                        UserWarning,
                        stacklevel=2,
                    )
                # Always honor an explicit display-style on the reuse path (facade-only).
                # Key match is case-insensitive (Repark.Display.Style etc.).
                if any(key.lower() == _DISPLAY_STYLE_KEY for key in self._config):
                    _sf._active_session.display_style = display_style
                    # Keep the session snapshot in sync so pure-style reuse stays silent on
                    # repeats (otherwise _builder_config still lacked the applied key).
                    _sync_display_style_into_builder_config(
                        _sf._active_session._builder_config, display_style
                    )
                return _sf._active_session

            # Native HashMap<String, String> cannot hold None; Spark keeps None in the facade
            # options map (``to_str(None)``) and treats those keys as unset for knobs — strip
            # only at the FFI boundary.
            native_config = {key: value for key, value in self._config.items() if value is not None}
            inner = _native.PyReparkSession(
                memory_limit_gb=memory_limit_gb,
                batch_size=batch_size,
                target_partitions=target_partitions,
                config=native_config,
            )
            logger.debug(
                "created repark ReparkSession (memory_limit_gb=%s, batch_size=%s, "
                "target_partitions=%s)",
                memory_limit_gb,
                batch_size,
                target_partitions,
            )
            session = ReparkSession(
                inner,
                builder_config=dict(self._config),
                display_style=display_style,
            )
            # Forward builder ``datafusion.*`` (incl. runtime.memory_limit) onto the live
            # DataFusion session after the native handle exists. Builder repark.memory.limit.gb
            # already sized the FairSpillPool; datafusion.runtime.memory_limit alone (no
            # repark twin — dual-set refused above) re-sizes that same pool here.
            _apply_builder_datafusion_conf(session, self._config)
            # R-AUTO-MEMCAT: a bare session gets a working spark_catalog + default namespace
            # (the duckdb :memory: analogue) — skipped when the builder configured catalogs,
            # named a different defaultCatalog, or set repark.sql.autoMemoryCatalog=false.
            if _auto_memory_catalog_wanted(session._builder_config):
                session._register_auto_memory_catalog()
            _sf._active_session = session
            return session

        # PySpark spells this ``getOrCreate``; expose both.
        getOrCreate = get_or_create  # noqa: N815 — deliberate PySpark-compatible camelCase alias

        def _resolve_display_style(self) -> str:
            """Resolve ``repark.display.style`` (facade-only; default ``spark``).

            Case-insensitive **last-write-wins** among all key aliases present in the builder map
            (deliberate). ``Builder.config`` normally collapses aliases to the canonical
            key on write; this scan still prefers the last insertion-order match so a dual-cased
            map (e.g. after direct mutation or an older snapshot) cannot silently keep an earlier
            exact-key value over a later mixed-case override. Every resolved value is validated
            via :func:`normalize_display_style` (invalid last alias refuses loud).
            """
            raw: str | None = None
            for key, value in self._config.items():
                if key.lower() == _DISPLAY_STYLE_KEY:
                    # Dict preserves insertion order: last matching alias wins.
                    raw = value
            if raw is None:
                return _DEFAULT_DISPLAY_STYLE
            return normalize_display_style(raw)

        def _resolve_memory_limit_gb(self) -> int | None:
            """Resolve ``repark.memory.limit.gb`` (repark-only knob; no Spark counterpart).

            ``0`` is meaningful — it opts the session out of the bounded memory pool entirely —
            so only a NEGATIVE budget is a config error. Raising here (rather than letting a
            negative reach the native ``Option<usize>`` argument) keeps the class the facade
            contracts: :class:`~repark.errors.IllegalArgumentException`, not PyO3's
            ``OverflowError``.
            """
            from repark.errors import IllegalArgumentException

            entry = self._lookup_int(_MEMORY_LIMIT_KEYS)
            if entry is None:
                return None
            key, value = entry
            if value < 0:
                raise IllegalArgumentException(
                    _config_value_error(
                        key,
                        value,
                        f"The value of {key} must not be negative "
                        "(0 opts out of the bounded memory pool)",
                    )
                )
            return value

        def _resolve_batch_size(self) -> int | None:
            """Resolve the Arrow batch-size knob, honoring Spark's "no limit" sentinel (SAF-006).

            ``spark.sql.execution.arrow.maxRecordsPerBatch`` carries no ``checkValue`` in Spark's
            ``SQLConf`` and is documented "If set to zero or negative there is no limit" — a legal,
            commonly used PySpark value. repark therefore **accepts** ``<= 0`` (returning ``None``,
            i.e. the knob is left unset) rather than refusing it, which is what
            ``spark.sql.shuffle.partitions`` gets — the two keys are validated per Spark's own
            per-key rules, never by one blanket rule.

            **Disclosed divergence:** Spark's sentinel asks for *unbounded* Arrow batches, and
            DataFusion has no unbounded-batch mode, so the default batch size stays in force.
            Values are unaffected (batching is not observable in results, only in batch
            boundaries), so this is an accepted-but-not-honored knob, warned once per process like
            ``.master(...)`` (OTH-010). The repark-native spelling ``repark.batch.size`` shares the
            sentinel so both spellings of one knob cannot diverge.
            """
            entry = self._lookup_int(_BATCH_SIZE_KEYS)
            if entry is None:
                return None
            key, value = entry
            if value <= 0:
                _warn_unbounded_batch_once(key, value, stacklevel=4)
                return None
            return value

        def _resolve_shuffle_partitions(self) -> int | None:
            """Resolve the partition-count knob with Spark's positive-only rule (SAF-006).

            ``spark.sql.shuffle.partitions`` is declared in Spark's ``SQLConf`` with
            ``checkValue(_ > 0, …)``, so ``0`` / negative raise ``IllegalArgumentException`` in
            real PySpark. repark mirrors the class AND Spark 4.1.2's
            ``[INVALID_CONF_VALUE.REQUIREMENT]`` message verbatim — see :func:`_config_value_error`
            for the live capture and the two recorded deltas (no ``SQLSTATE`` suffix; the
            repark-native spelling has no Spark counterpart). Contrast
            :meth:`_resolve_batch_size`, whose key is documented to accept ``0``.
            """
            from repark.errors import IllegalArgumentException

            entry = self._lookup_int(_TARGET_PARTITIONS_KEYS)
            if entry is None:
                return None
            key, value = entry
            if value <= 0:
                raise IllegalArgumentException(
                    _config_value_error(key, value, f"The value of {key} must be positive")
                )
            return value

        def _lookup_int(self, keys: tuple[str, ...]) -> tuple[str, int] | None:
            """Return the winning ``(key, integer)`` among ``keys``, or ``None`` if none are set.

            The key is returned alongside the value because range validation is **per key family**
            (SAF-006) and the error messages name the spelling the user actually set.

            Keys are tried in order (repark-native first). If several spellings are set:
            identical values collapse; different values raise
            :class:`~repark.errors.IllegalArgumentException` naming both keys. Non-integer values
            raise naming the key (never warn-and-default).

            This is the FACADE twin of the engine's ``repark_core::Error::Config``: both raise the
            SAME class live PySpark raises for an invalid ``SQLConf`` value
            (``IllegalArgumentException``) — a deliberate break from repark's former
            ``ValueError``, which ``except ValueError`` never caught either.

            **TIMING divergence (deliberate):** the CLASS matches PySpark but the
            MOMENT does not — repark validates eagerly inside ``getOrCreate()`` where a fresh
            PySpark process validates at the first ``sessionState`` touch. The user-readable copy
            of this disclosure lives on :meth:`Builder.config` (the ``help()`` surface); keep the
            two in sync.
            """
            from repark.errors import IllegalArgumentException

            found: list[tuple[str, int]] = []
            for key in keys:
                value = self._config.get(key)
                if value is None:
                    continue
                try:
                    parsed = int(value)
                except ValueError as error:
                    raise IllegalArgumentException(
                        f"config key {key!r} must be an integer, got {value!r}"
                    ) from error
                found.append((key, parsed))
            if not found:
                return None
            first_key, first_value = found[0]
            for key, value in found[1:]:
                if value != first_value:
                    raise IllegalArgumentException(
                        f"conflicting config: {first_key!r} and {key!r} set different values"
                    )
            return first_key, first_value

    class _BuilderAccessor:
        """Descriptor yielding a *fresh* :class:`ReparkSession.Builder` per ``.builder`` access.

        Returning a new builder per access keeps independent ``.config(...)`` chains from
        leaking config into one another (PySpark exposes ``.builder`` as a chain start).
        """

        def __get__(
            self,
            _obj: ReparkSession | None,
            _owner: type[ReparkSession],
        ) -> ReparkSession.Builder:
            return ReparkSession.Builder()

    #: The PySpark-style entry point: ``ReparkSession.builder.getOrCreate()``.
    builder = _BuilderAccessor()
