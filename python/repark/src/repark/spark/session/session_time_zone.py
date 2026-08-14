"""The session-timezone conf — ``spark.sql.session.timeZone``, the facade half of one engine knob.

The key is spelled **exactly once** on each side of the boundary: :data:`SESSION_TIME_ZONE_KEY`
here and ``repark_core::SESSION_TIME_ZONE_KEY`` in the engine. There is deliberately no alternate
spelling — no ``repark.``-namespaced twin, no ``snake_case`` alias, no case-insensitive lookalike.
A lookalike is an unknown ``.config(...)`` key and is tolerated the way PySpark tolerates any
unknown key: it configures nothing.

**One truth, not two knobs.** The zone is resolved and validated ONCE, by the engine, at session
construction — the same shape as the build-time memory-pool knob. It follows that:

* ``ReparkSession.builder.config(SESSION_TIME_ZONE_KEY, "America/New_York")`` is the way to set it,
  and an unknown zone fails loud (``IllegalArgumentException``) at ``getOrCreate``;
* ``spark.conf.get(SESSION_TIME_ZONE_KEY)`` reads it back, defaulting to
  :data:`DEFAULT_SESSION_TIME_ZONE`, and reports the zone the live engine session actually has —
  a runtime ``set`` never moves it, and the builder value is whitespace-normalized on the way in
  (:func:`normalize_session_time_zone_config`) so a padded ``.config(...)`` value cannot make the
  facade report a string the engine trimmed away;
* ``spark.conf.set(SESSION_TIME_ZONE_KEY, …)`` at runtime is **accepted for source compatibility,
  warned once, and NEITHER VALIDATED NOR APPLIED**
  (:func:`warn_runtime_session_time_zone_not_applied`) — the ``.master(...)`` /
  arrow-batch-sentinel shape (OTH-010), not the memory-pool refusal shape. Validation happens
  exactly once, at session build, so a garbage zone handed to the runtime setter is not refused
  either; it simply configures nothing, and the warning says so.

  *Why accepted rather than refused (evidence, not taste).* Refusing was tried first and is the
  stricter, more obviously honest option — but the Apache pinned test
  ``pyspark.sql.tests.test_creation.DataFrameCreationTests.test_create_dataframe_from_pandas_with_dst``
  sets this key through PySpark's own ``sql_conf`` context manager, so a raise turns a passing
  drop-in test red. The drop-in promise is "change the import line"; a migrated script must not
  explode on a conf call PySpark accepts. What the repo refuses is a *lying* conf, so the value is
  deliberately **not stored**: ``conf.get`` keeps reporting the engine's real zone rather than the
  one the caller asked for, and the divergence is a visible warning instead of a silent
  split-brain.

**Declared divergence on the default.** PySpark defaults this key to the JVM's local zone, so a job
produces different wall clocks on two hosts. repark defaults to ``UTC`` for reproducibility (and
because reading the host zone would be an environment read the server-prep discipline forbids).

**What the zone reaches, as of 2026-08-14 (TZ-8).** Timestamp **extraction** honors it over
an INSTANT-typed (tz-aware) TIMESTAMP. Zoneless LTZ inputs — ``TIMESTAMP '…'``, zoneless
``to_timestamp``, ``CAST(str AS TIMESTAMP)``, a naive-``datetime`` column declared as default
``TIMESTAMP`` / ``TimestampType`` — localize in this zone then store µs+UTC. ``TIMESTAMP_NTZ``
stays naive and is **not** shifted. ``CAST(ts AS DATE)`` / ``to_date(ts)`` take the date in
this zone for LTZ (NTZ stays the stored wall). ``datediff`` of a TIMESTAMP rides that
CAST. ``last_day`` / ``date_add`` over a TIMESTAMP stay residual. ``CAST(TIMESTAMP AS
STRING)`` rendering is B-TZ-4.
"""

from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    from collections.abc import MutableMapping

# The ONE authoritative spelling of the session-timezone conf key (PySpark's own).
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The session zone when the key is unset. Mirrors ``repark_core::DEFAULT_SESSION_TIME_ZONE``.
DEFAULT_SESSION_TIME_ZONE = "UTC"

# The key family, in the shape the builder's engine-knob machinery consumes. Exactly one member
# by construction — the tuple exists so the session-timezone key joins the engine-knob set the
# ``getOrCreate`` reuse path excludes from its runtime-conf fold, not to invite a second spelling.
SESSION_TIME_ZONE_KEYS: tuple[str, ...] = (SESSION_TIME_ZONE_KEY,)


def normalize_session_time_zone_config(config: MutableMapping[str, str | None]) -> None:
    """Strip surrounding whitespace from the builder's session-zone value, in place.

    **Whitespace normalization only — the ENGINE remains the sole validator.** Nothing here
    decides whether a value names a real zone; ``repark_core::SessionTimeZone::parse`` does, once,
    at session build. What this does is match the engine's own ``raw.trim()`` before the value is
    stored on the facade, because the engine builds the session with the TRIMMED zone: without it
    ``.config(KEY, "  Asia/Tokyo  ")`` would leave ``spark.conf.get`` reporting the padded string
    while the live session holds ``Asia/Tokyo`` — the facade/engine split-brain this whole surface
    exists to prevent. A value that trims to empty is left empty and the engine refuses it, so
    normalizing never turns a refusal into a silent default.
    """
    raw = config.get(SESSION_TIME_ZONE_KEY)
    if isinstance(raw, str):
        config[SESSION_TIME_ZONE_KEY] = raw.strip()


_runtime_session_time_zone_warned = False


def warn_runtime_session_time_zone_not_applied(key: str, *, stacklevel: int = 2) -> bool:
    """Handle a runtime ``conf.set`` of the build-time session-timezone key.

    Returns ``True`` when ``key`` IS that knob — the caller must then accept the call and store
    nothing, so ``conf.get`` keeps reporting the zone the live engine session actually has.
    Returns ``False`` for every other key (including a differently-cased lookalike, which is not
    this knob and is left to the ordinary unknown-key path).

    The value is **neither validated nor applied**: validation is the engine's, and it happens
    exactly once, at session build. An unknown zone passed to the runtime setter is therefore not
    refused here — repark is knowingly laxer than PySpark 4.1.2 on that one point (Spark raises
    ``[INVALID_CONF_VALUE.TIME_ZONE]``), because refusing reds the pinned Apache drop-in test that
    drives this whole shape. The warning says so in as many words, so a caller who typo'd a zone
    reads it in the message rather than inferring it from a silent no-op.

    Emits that disclosure at most once **per process** — not per session — the same shape as the
    ``.master(...)`` and arrow-batch-sentinel disclosures (OTH-010): a migrated PySpark script
    calls this setter and must not explode, but it must also not be told the zone changed. The
    once-per-process scope means a second session in the same interpreter gets a silent no-op;
    that is the deliberate cost of the OTH-010 idiom, recorded in the divergence registry (TZ-3).
    """
    if key != SESSION_TIME_ZONE_KEY:
        return False
    global _runtime_session_time_zone_warned
    if not _runtime_session_time_zone_warned:
        warnings.warn(
            f"config {key!r} is accepted for source compatibility but NOT applied at runtime, and "
            f"its value is NOT validated: the session timezone is resolved AND validated exactly "
            f"once, at getOrCreate (default {DEFAULT_SESSION_TIME_ZONE!r}), so this session keeps "
            f"the zone it was built with, conf.get keeps reporting it, and an unknown zone passed "
            f"here is neither refused nor stored. To change the zone — and to have the value "
            f"checked against the engine's zone database — build a session with "
            f"ReparkSession.builder.config({key!r}, 'America/New_York'). This disclosure is "
            f"emitted once per process.",
            UserWarning,
            stacklevel=stacklevel,
        )
        _runtime_session_time_zone_warned = True
    return True


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

    Q12: session zone, never the host TZ. Gap/fold uses ``fold=0`` (earlier offset), matching
    Spark's ``ofLocal`` earlier-preferred arm when no source offset is supplied.
    """
    if value.tzinfo is not None:
        return value.astimezone(datetime.UTC)
    zone = ZoneInfo(active_session_time_zone())
    localized = value.replace(tzinfo=zone, fold=0)
    return localized.astimezone(datetime.UTC)
