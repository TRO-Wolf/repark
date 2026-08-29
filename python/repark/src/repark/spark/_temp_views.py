"""Temporary-view naming and cleanup helpers for the Spark facade.

Scratch names and references stay qualified to the session's write home so registration,
reads, and drops use the same namespace. These helpers cover facade-owned views only.
Engine-owned scratch registrations have a separate lifecycle and can fail if the DataFusion
default catalog changes. User-visible one-part names remain one-part.
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
    session's own home spelling as the home and the reference a SQL body reads, so a
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
