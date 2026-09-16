"""The session-timezone conf — ``spark.sql.session.timeZone``, the facade half of one engine knob.

The key is spelled **exactly once** on each side of the boundary: :data:`SESSION_TIME_ZONE_KEY`
here and ``repark_core::SESSION_TIME_ZONE_KEY`` in the engine. There is deliberately no alternate
spelling — no ``repark.``-namespaced twin, no ``snake_case`` alias, no case-insensitive lookalike.
A lookalike is an unknown ``.config(...)`` key and is tolerated the way PySpark tolerates any
unknown key: it configures nothing.

**One truth, not two knobs.** The zone is resolved and validated ONCE, by the engine, at session
construction. It follows that:

* ``ReparkSession.builder.config(SESSION_TIME_ZONE_KEY, "America/New_York")`` is the way to set
  it; an unknown zone fails loud (``IllegalArgumentException``) at ``getOrCreate``.
* ``spark.conf.get(SESSION_TIME_ZONE_KEY)`` reports the zone the live engine session actually
  has, defaulting to :data:`DEFAULT_SESSION_TIME_ZONE`; the builder value is
  whitespace-normalized (:func:`normalize_session_time_zone_config`) so a
  padded ``.config(...)`` value cannot make the facade report a string the engine trimmed away.
* ``spark.conf.set(SESSION_TIME_ZONE_KEY, …)`` at runtime **validates and applies
  immediately** through the native setter: an unknown zone refuses
  (``IllegalArgumentException``) before anything is stored, and a valid zone moves fresh
  queries while frames built before the set keep the snapshot they were analysed under.

**Declared divergence on the default.** PySpark defaults this key to the JVM's local zone, so a
job produces different wall clocks on two hosts. repark defaults to ``UTC`` for reproducibility
(and because reading the host zone would be an environment read the server-prep discipline
forbids).

**What the zone reaches.** Timestamp **extraction** honors it over an INSTANT-typed (tz-aware)
TIMESTAMP. Zoneless LTZ inputs — ``TIMESTAMP '…'``, zoneless ``to_timestamp``,
``CAST(str AS TIMESTAMP)``, a naive-``datetime`` column declared as default ``TIMESTAMP`` /
``TimestampType`` — localize in this zone then store µs+UTC. ``TIMESTAMP_NTZ`` stays naive and
is **not** shifted. ``CAST(ts AS DATE)`` / ``to_date(ts)`` take the date in this zone for LTZ
(NTZ stays the stored wall). ``datediff`` of a TIMESTAMP rides that CAST. ``last_day`` /
``date_add`` over a TIMESTAMP stay residual. ``CAST(TIMESTAMP AS STRING)`` rendering is B-TZ-4.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from collections.abc import MutableMapping

# The ONE authoritative spelling of the session-timezone conf key (PySpark's own).
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The session zone when the key is unset. Mirrors ``repark_core::DEFAULT_SESSION_TIME_ZONE``.
DEFAULT_SESSION_TIME_ZONE = "UTC"

# Exactly one member by construction — the tuple joins the engine-knob set the ``getOrCreate``
# reuse path excludes from its runtime-conf fold, not to invite a second spelling.
SESSION_TIME_ZONE_KEYS: tuple[str, ...] = (SESSION_TIME_ZONE_KEY,)


def normalize_session_time_zone_config(config: MutableMapping[str, str | None]) -> None:
    """Strip surrounding whitespace from the builder's session-zone value, in place.

    **Whitespace normalization only — the ENGINE remains the sole validator.** Nothing here
    decides whether a value names a real zone; ``repark_core::SessionTimeZone::parse`` does,
    once, at session build. This matches the engine's own ``raw.trim()`` before the value is
    stored on the facade, because the engine builds the session with the TRIMMED zone: without
    it ``.config(KEY, "  Asia/Tokyo  ")`` would leave ``spark.conf.get`` reporting the padded
    string while the live session holds ``Asia/Tokyo`` — the facade/engine split-brain this
    surface exists to prevent. A value that trims to empty is left empty and the engine refuses
    it, so normalizing never turns a refusal into a silent default.
    """
    raw = config.get(SESSION_TIME_ZONE_KEY)
    if isinstance(raw, str):
        config[SESSION_TIME_ZONE_KEY] = raw.strip()


def active_session_time_zone() -> str:
    """The live session's zone id, or :data:`DEFAULT_SESSION_TIME_ZONE` if none is active.

    Late-imports the session class so this module stays importable from ``types.py``.
    """
    try:
        from repark.spark.session.session_core import ReparkSession

        session = ReparkSession.getActiveSession()
        if session is None:
            return DEFAULT_SESSION_TIME_ZONE
        value = session.conf.get(SESSION_TIME_ZONE_KEY)
        if isinstance(value, str) and value.strip():
            return value.strip()
    except Exception:
        return DEFAULT_SESSION_TIME_ZONE
    return DEFAULT_SESSION_TIME_ZONE


def collect_timestamp_as_session_wall(value: datetime.datetime) -> datetime.datetime:
    """Spark ``collect``: tz-aware instant → naive wall in the session zone."""
    return value.astimezone(ZoneInfo(active_session_time_zone())).replace(tzinfo=None)


def localize_naive_datetime_to_utc(value: datetime.datetime) -> datetime.datetime:
    """Naive wall → instant in the session zone (UTC-aware). Aware values convert to UTC.

    Session zone, never the host TZ. Gap/fold uses ``fold=0`` (earlier offset), matching
    Spark's ``ofLocal`` earlier-preferred arm when no source offset is supplied.
    """
    if value.tzinfo is not None:
        return value.astimezone(datetime.UTC)
    zone = ZoneInfo(active_session_time_zone())
    localized = value.replace(tzinfo=zone, fold=0)
    return localized.astimezone(datetime.UTC)
