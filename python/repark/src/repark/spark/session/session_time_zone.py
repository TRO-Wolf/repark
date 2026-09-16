"""The session-timezone conf — ``spark.sql.session.timeZone``, the facade half of one engine knob.

The key is spelled **exactly once** on each side of the boundary: :data:`SESSION_TIME_ZONE_KEY`
here and ``repark_core::SESSION_TIME_ZONE_KEY`` in the engine. There is deliberately no alternate
spelling — no ``repark.``-namespaced twin, no ``snake_case`` alias, no case-insensitive lookalike.
A lookalike is an unknown ``.config(...)`` key and is tolerated the way PySpark tolerates any
unknown key: it configures nothing.

**Two values, one knob (R-17c-4).** The live zone snapshot carries the **raw Spark-visible
text** exactly as set — what ``conf.get``, ``SET -v`` and ``current_timezone()`` report — and a
**canonical companion** every value-bearing consumer uses (an id Arrow ``Tz`` parses and this
module converts to a ``tzinfo``). The canonicalisation happens once, in Rust, at the gate; no
consumer re-parses the raw text. It follows that:

* ``ReparkSession.builder.config(SESSION_TIME_ZONE_KEY, "America/New_York")`` seeds the zone;
  an unknown zone fails loud (``IllegalArgumentException``) at ``getOrCreate``.
* ``spark.conf.get(SESSION_TIME_ZONE_KEY)`` reports the raw text the live engine session was
  given, defaulting to :data:`DEFAULT_SESSION_TIME_ZONE`; builder and runtime values are
  whitespace-normalized so a padded set cannot leave the facade and the engine disagreeing.
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
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from repark.spark.session.session_core import ReparkSession

# The ONE authoritative spelling of the session-timezone conf key (PySpark's own).
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The session zone when the key is unset. Mirrors ``repark_core::DEFAULT_SESSION_TIME_ZONE``.
DEFAULT_SESSION_TIME_ZONE = "UTC"

# Exactly one member by construction — the tuple joins the engine-knob set the ``getOrCreate``
# reuse path excludes from its runtime-conf fold, not to invite a second spelling.
SESSION_TIME_ZONE_KEYS: tuple[str, ...] = (SESSION_TIME_ZONE_KEY,)

SESSION_ZONE_CANONICAL_TOKEN = "session_zone_canonical"


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


def active_session_time_zone_canonical() -> str:
    """The snapshot's canonical companion id, cached on the session's alive token.

    Arrow ``Tz`` parses it, and :func:`_session_tzinfo` converts it; the raw text from
    :func:`active_session_time_zone` is only for display. Falls back to the raw text when
    the native reader is unreachable, matching the pre-R-17c-4 behaviour.
    """
    try:
        from repark import _native
        from repark.spark.session.session_core import ReparkSession

        session = ReparkSession.getActiveSession()
        if session is None:
            return DEFAULT_SESSION_TIME_ZONE
        cached = session._alive_token.get(SESSION_ZONE_CANONICAL_TOKEN)
        if isinstance(cached, str) and cached:
            return cached
        canonical = _native.session_zone_canonical(session._ensure_alive())
        if isinstance(canonical, str) and canonical:
            session._alive_token[SESSION_ZONE_CANONICAL_TOKEN] = canonical
            return canonical
    except Exception:
        return DEFAULT_SESSION_TIME_ZONE
    return active_session_time_zone()


def refresh_session_zone_canonical(session: ReparkSession) -> None:
    """Re-read the snapshot canonical id after a runtime zone mutation.

    A failure leaves the cached value in place; the next wall conversion re-seeds it
    through :func:`active_session_time_zone_canonical`.
    """
    try:
        from repark import _native

        canonical = _native.session_zone_canonical(session._ensure_alive())
        if isinstance(canonical, str) and canonical:
            session._alive_token[SESSION_ZONE_CANONICAL_TOKEN] = canonical
    except Exception:
        return


def _offset_tzinfo(canonical: str) -> datetime.tzinfo:
    """A ``±H`` / ``±HH`` / ``±HHMM`` / ``±HH:MM`` companion id as a fixed offset."""
    text = canonical.strip()
    negative = text.startswith("-")
    body = text[1:] if text[:1] in ("+", "-") else text
    digits = body.replace(":", "")
    hours = 0
    minutes = 0
    seconds = 0
    if digits.isdigit() and len(digits) in (1, 2):
        hours = int(digits)
    elif digits.isdigit() and len(digits) == 4:
        hours = int(digits[:2])
        minutes = int(digits[2:])
    elif digits.isdigit() and len(digits) == 6:
        hours = int(digits[:2])
        minutes = int(digits[2:4])
        seconds = int(digits[4:])
    else:
        raise ZoneInfoNotFoundError(canonical)
    if minutes > 59 or seconds > 59 or hours > 23:
        raise ZoneInfoNotFoundError(canonical)
    delta = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return datetime.timezone(-delta if negative else delta)


def _session_tzinfo() -> datetime.tzinfo:
    """The live session zone as a tzinfo: IANA names via ZoneInfo, offsets fixed."""
    canonical = active_session_time_zone_canonical()
    try:
        return ZoneInfo(canonical)
    except ZoneInfoNotFoundError:
        return _offset_tzinfo(canonical)


def collect_timestamp_as_session_wall(value: datetime.datetime) -> datetime.datetime:
    """Spark ``collect``: tz-aware instant → naive wall in the session zone."""
    return value.astimezone(_session_tzinfo()).replace(tzinfo=None)


def localize_naive_datetime_to_utc(value: datetime.datetime) -> datetime.datetime:
    """Naive wall → instant in the session zone (UTC-aware). Aware values convert to UTC.

    Session zone, never the host TZ. Gap/fold uses ``fold=0`` (earlier offset), matching
    Spark's ``ofLocal`` earlier-preferred arm when no source offset is supplied.
    """
    if value.tzinfo is not None:
        return value.astimezone(datetime.UTC)
    zone = _session_tzinfo()
    localized = value.replace(tzinfo=zone, fold=0)
    return localized.astimezone(datetime.UTC)
