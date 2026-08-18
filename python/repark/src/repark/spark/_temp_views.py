"""The temp-view SPELLING seam — how the facade names a session-local view in SQL (R7-1).

The engine has no temp-view namespace: a session-local view is a table in the session's
build-time home (``datafusion.public`` unless the session was built with other
``datafusion.catalog.*`` defaults), and the WRITE side is pinned to that home by the native
``crates/repark-core/src/temp_view.rs`` seam (R6-1). The READ side is not: a **bare** reference
inside a SQL body is resolved by DataFusion against the **live**
``datafusion.catalog.default_catalog``. So after a raw
``spark.sql("SET datafusion.catalog.default_catalog = 'other'")`` every facade path that minted a
view and then scanned it by bare name looked in ``other`` and missed — MEASURED on `3910ac7`:
``spark.table("tv")``, ``cache()``/``persist()``/``checkpoint()``, ``selectExpr``, ``alias`` and
the ML scratch views all raised ``table 'other.public.… not found`` while
``spark.catalog.tableExists("tv")`` (which asks the home) said ``True``.

This module is the one place that answers "what do I write in SQL for a session-local view":

* :func:`scratch_view_name` — mint an INTERNAL scratch view name already spelled against the
  home, so the mint call, every ``FROM``/qualifier use of it, and the drop all carry the same
  home-pinned string and no read site needs to know about any of this.
* :func:`home_view_ref` — the home-qualified spelling of an EXISTING user-visible one-part view
  (``alias``, ``spark.table``-shaped internal re-reads), whose NAME must stay one-part because
  the user chose it.
* :func:`local_view_name` — back to the one-part name, for the callers that compare against a
  ``__repark_*`` prefix or hand the name to a name-only API.

Out of scope by decision (round 7): raw user-typed SQL bodies on the NATIVE door
(``repark-core``'s ``Session::sql``) keep DataFusion's live-default resolution — pinned by
``set_to_a_plain_catalog_keeps_the_write_home_and_moves_only_the_read``. The facade's own
``spark.sql`` bare-name expander resolves through :meth:`ReparkSession.resolve_table_name`, which
now emits the home spelling too.

Also out of reach from HERE (round-8 item, MEASURED red on the round-7 BASE as well, so this
module neither caused nor cured it): the scratch relations the ENGINE crates register for
themselves with a bare name — ``repark-iceberg``'s MERGE and identity-``UPDATE``/``DELETE``
tables, and the ``__repark_tt_*`` time-travel view. They never pass through this module, and
under a ``SET`` to an Iceberg catalog their bare registration is handed to the Iceberg schema
provider, which refuses it. See ``task/se1-declared-sorted-ledger.md`` (round 7, "NOT-RUN").
"""

from __future__ import annotations

import uuid
from typing import Any

from repark.spark._idents import quote_ident


def _home_segments(session: Any) -> list[str] | None:
    """``[catalog, schema]`` of the session's temp-view home, or ``None`` when it has none.

    ``None`` is the "a catalog was registered over the home" case (native
    ``assert_home_intact``): there is nothing session-local to name, and the caller falls back to
    the bare name so the native temp-view API raises its own loud refusal instead of this module
    inventing one.
    """
    try:
        home = session.temp_view_home()
    except Exception:
        return None
    if not home or len(home) < 2:
        return None
    return [str(home[0]), str(home[1])]


def scratch_view_name(session: Any, prefix: str) -> str:
    """A fresh INTERNAL scratch-view name, spelled against the session's temp-view home.

    Returns ``"catalog"."schema"."<prefix><uuid>"`` — ALWAYS quoted (the home-less fallback is the
    quoted bare name), so the returned string is a ready-to-embed SQL reference and a call site
    must never wrap it in :func:`~repark.spark._idents.quote_ident` again. It is a single string
    that is simultaneously the name the native temp-view API registers (the seam accepts the
    session's OWN home spelling as the home, R7-1) and the reference a SQL body reads, so a
    ``SET`` of the live default catalog cannot separate the mint from the read.
    """
    name = f"{prefix}{uuid.uuid4().hex}"
    home = _home_segments(session)
    if home is None:
        return quote_ident(name)
    return ".".join(quote_ident(segment) for segment in (*home, name))


def home_view_ref(session: Any, name: str) -> str:
    """The home-qualified SQL reference for the EXISTING one-part temp view ``name``.

    For the paths that must keep a user-chosen one-part NAME (``DataFrame.alias``) but still read
    it home-pinned. The segments come from the native resolver, so the identifier normalization
    (unquoted names fold ASCII-lowercase, quoted stay verbatim) is the engine's own — spelling the
    home by hand around a raw ``name`` would miss a view registered under a folded name. Falls
    back to the quoted bare name when the session has no home left or no such view exists.
    """
    try:
        segments = session.resolve_temp_view_home_ref(name)
    except Exception:
        segments = None
    if not segments:
        return quote_ident(name)
    return ".".join(quote_ident(str(segment)) for segment in segments)


def local_view_name(view: str) -> str:
    """The one-part name inside a (possibly home-qualified) temp-view spelling.

    :func:`scratch_view_name` returns a quoted three-part string; callers that compare against a
    ``__repark_*`` prefix, or feed a name-only API, want the last segment unquoted.
    """
    tail = view.rpartition(".")[2] or view
    if len(tail) >= 2 and tail.startswith('"') and tail.endswith('"'):
        return tail[1:-1].replace('""', '"')
    return tail
