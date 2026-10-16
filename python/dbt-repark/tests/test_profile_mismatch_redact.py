"""The profile-mismatch refusal, the dbt debug display and the Jinja target carry no value.

pins: pa-dbt-mismatch-redact-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

from argparse import Namespace
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from dbt_common.exceptions import DbtRuntimeError

CATALOG = "ice"
OTHER_CATALOG = "other"
NAMESPACE = "gold"
PROFILE = "repark_redact"

CATALOG_SECRET_KEY = "spark.sql.catalog.ice.s3.secret-access-key"
CATALOG_URL_KEY = "spark.sql.catalog.ice.uri"
SESSION_SECRET_KEY = "spark.hadoop.fs.s3a.secret.key"
SESSION_URL_KEY = "repark.source.pg.url"

SOURCE_URL = "postgresql://u:SENTINEL_URL_PW@h/db"
ROTATED_URL = "postgresql://u:SENTINEL_ROTATED_URL_PW@h/db"
WAREHOUSE_URL = "s3://u:SENTINEL_WH_PW@bucket/warehouse"

SENTINELS = (
    "SENTINEL_CAT_SECRET",
    "SENTINEL_SESSION_SECRET",
    "SENTINEL_URL_PW",
    "SENTINEL_ROTATED_CAT_SECRET",
    "SENTINEL_ROTATED_SESSION_SECRET",
    "SENTINEL_ROTATED_URL_PW",
    "SENTINEL_WH_PW",
)


class _StandInSession:
    """A live session the refusal must leave in place; nothing is built or reached."""

    def stop(self) -> None:
        """``release_session`` stops what it forgets."""


def _catalog_properties() -> dict[str, str]:
    """A catalog block with one secret and one source URL carrying a password."""
    return {CATALOG_SECRET_KEY: "SENTINEL_CAT_SECRET", CATALOG_URL_KEY: SOURCE_URL}


def _session_properties() -> dict[str, str]:
    """Session properties with one secret and one source URL carrying a password."""
    return {SESSION_SECRET_KEY: "SENTINEL_SESSION_SECRET", SESSION_URL_KEY: SOURCE_URL}


def _credentials(
    database: str = CATALOG,
    catalog_properties: dict[str, str] | None = None,
    session_properties: dict[str, str] | None = None,
) -> Any:
    """A profile carrying the sentinel secrets in both property dictionaries."""
    from dbt.adapters.repark.connections import ReparkCredentials

    return ReparkCredentials(
        database=database,
        schema=NAMESPACE,
        catalog_properties=_catalog_properties()
        if catalog_properties is None
        else catalog_properties,
        session_properties=_session_properties()
        if session_properties is None
        else session_properties,
    )


@pytest.fixture
def live() -> Iterator[_StandInSession]:
    """A session already live for the sentinel profile, released after the test."""
    from dbt.adapters.repark import session

    session.release_session()
    stand_in = _StandInSession()
    session._ACTIVE["session"] = stand_in
    session._ACTIVE["key"] = session.session_key(_credentials())
    try:
        yield stand_in
    finally:
        session.release_session()


def _refusal(requested: Any) -> str:
    """The refusal text a second, differing profile meets."""
    from dbt.adapters.repark.session import acquire_session

    with pytest.raises(DbtRuntimeError) as refused:
        acquire_session(requested)
    return str(refused.value)


def _assert_no_sentinel(text: str) -> None:
    """No sentinel value, original or rotated, appears in the text."""
    assert [sentinel for sentinel in SENTINELS if sentinel in text] == []


def test_only_database_differs_names_both_and_no_key(live: _StandInSession) -> None:
    """A changed database alone refuses, names both databases, and lists no changed key."""
    message = _refusal(_credentials(database=OTHER_CATALOG))
    _assert_no_sentinel(message)
    assert f"live database '{CATALOG}'" in message
    assert f"requested database '{OTHER_CATALOG}'" in message
    assert "changed keys: none" in message


def test_catalog_secret_change_names_the_catalog_key(live: _StandInSession) -> None:
    """A rotated catalog secret refuses and names only its prefixed key."""
    rotated = {**_catalog_properties(), CATALOG_SECRET_KEY: "SENTINEL_ROTATED_CAT_SECRET"}
    message = _refusal(_credentials(catalog_properties=rotated))
    _assert_no_sentinel(message)
    assert f"changed keys: catalog_properties.{CATALOG_SECRET_KEY})" in message


def test_session_secret_change_names_the_session_key(live: _StandInSession) -> None:
    """A rotated session secret refuses and names only its prefixed key."""
    rotated = {**_session_properties(), SESSION_SECRET_KEY: "SENTINEL_ROTATED_SESSION_SECRET"}
    message = _refusal(_credentials(session_properties=rotated))
    _assert_no_sentinel(message)
    assert f"changed keys: session_properties.{SESSION_SECRET_KEY})" in message


def test_source_url_change_names_both_url_keys_sorted(live: _StandInSession) -> None:
    """A rotated URL password in both dictionaries names both keys, sorted, and no URL."""
    message = _refusal(
        _credentials(
            catalog_properties={**_catalog_properties(), CATALOG_URL_KEY: ROTATED_URL},
            session_properties={**_session_properties(), SESSION_URL_KEY: ROTATED_URL},
        )
    )
    _assert_no_sentinel(message)
    assert "postgresql://" not in message
    assert (
        f"changed keys: catalog_properties.{CATALOG_URL_KEY}, session_properties.{SESSION_URL_KEY})"
    ) in message


def test_added_and_removed_keys_are_named(live: _StandInSession) -> None:
    """A key present on one side only is a changed key, whichever side holds it."""
    message = _refusal(
        _credentials(
            catalog_properties={CATALOG_URL_KEY: SOURCE_URL},
            session_properties={**_session_properties(), "spark.sql.shuffle.partitions": "4"},
        )
    )
    _assert_no_sentinel(message)
    assert (
        f"changed keys: catalog_properties.{CATALOG_SECRET_KEY}, "
        "session_properties.spark.sql.shuffle.partitions)"
    ) in message


def test_warehouse_url_credentials_are_masked(live: _StandInSession) -> None:
    """A warehouse is named through RePark's value redaction, so its userinfo is masked."""
    from dbt.adapters.repark.connections import ReparkCredentials

    requested = ReparkCredentials(
        database=CATALOG,
        schema=NAMESPACE,
        warehouse="s3://u:SENTINEL_URL_PW@bucket/warehouse",
        catalog_properties=_catalog_properties(),
        session_properties=_session_properties(),
    )
    message = _refusal(requested)
    _assert_no_sentinel(message)
    assert "live database 'ice', warehouse None" in message
    assert "warehouse 's3://u:***@bucket/warehouse'" in message


def test_refusal_still_raises_and_keeps_the_live_session(live: _StandInSession) -> None:
    """A differing secret still refuses, and the live session and its key stay as they were."""
    from dbt.adapters.contracts.connection import Connection, ConnectionState
    from dbt.adapters.repark import session
    from dbt.adapters.repark.connections import ReparkConnectionManager

    live_key = session._ACTIVE["key"]
    rotated = {**_catalog_properties(), CATALOG_SECRET_KEY: "SENTINEL_ROTATED_CAT_SECRET"}
    connection = Connection(
        type="repark",
        name="redact",
        credentials=_credentials(catalog_properties=rotated),
    )
    with pytest.raises(DbtRuntimeError):
        ReparkConnectionManager.open(connection)
    assert connection.state == ConnectionState.FAIL
    assert session._ACTIVE["session"] is live
    assert session._ACTIVE["key"] == live_key
    assert session.acquire_session(_credentials()) is live


def test_connection_info_renders_catalog_properties_redacted() -> None:
    """dbt's connection report keeps the catalog keys and drops every secret value."""
    info = dict(_credentials().connection_info())
    _assert_no_sentinel(repr(info))
    assert info["catalog_properties"] == {
        CATALOG_SECRET_KEY: "***",
        CATALOG_URL_KEY: "postgresql://u:***@h/db",
    }
    assert info["database"] == CATALOG


def test_jinja_target_renders_identifiers_and_catalog_properties_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Jinja ``target`` dbt builds from the profile carries no sentinel."""
    from dbt.adapters.repark.connections import ReparkCredentials
    from dbt.config.profile import Profile

    monkeypatch.setattr("dbt.flags.GLOBAL_FLAGS", Namespace(LOG_CACHE_EVENTS=False))

    credentials = ReparkCredentials(
        database=CATALOG,
        schema=NAMESPACE,
        warehouse=WAREHOUSE_URL,
        catalog_properties=_catalog_properties(),
        session_properties=_session_properties(),
    )
    target = Profile(
        profile_name=PROFILE, target_name="local", threads=1, credentials=credentials
    ).to_target_dict()
    _assert_no_sentinel(repr(target))
    assert target["catalog_properties"] == {
        CATALOG_SECRET_KEY: "***",
        CATALOG_URL_KEY: "postgresql://u:***@h/db",
    }
    assert target["warehouse"] == "s3://u:***@bucket/warehouse"
    assert target["database"] == CATALOG
    assert target["catalog"] == CATALOG


def _write_project(root: Path) -> None:
    """A dbt project whose profile asks for the other catalog with the sentinel properties."""
    (root / "dbt_project.yml").write_text(
        yaml.safe_dump(
            {"name": PROFILE, "version": "1.0", "config-version": 2, "profile": PROFILE}
        ),
        encoding="utf-8",
    )
    (root / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                PROFILE: {
                    "target": "local",
                    "outputs": {
                        "local": {
                            "type": "repark",
                            "catalog": OTHER_CATALOG,
                            "schema": NAMESPACE,
                            "threads": 1,
                            "warehouse": WAREHOUSE_URL,
                            "catalog_properties": _catalog_properties(),
                            "session_properties": _session_properties(),
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )


def test_dbt_debug_reports_the_refusal_without_a_value(
    live: _StandInSession, tmp_path: Path
) -> None:
    """``dbt debug`` prints the connection and the refusal; no sentinel reaches either sink."""
    from dbt.cli.main import dbtRunner

    _write_project(tmp_path)
    messages: list[str] = []
    result = dbtRunner(callbacks=[lambda event: messages.append(event.info.msg)]).invoke(
        [
            "debug",
            "--no-send-anonymous-usage-stats",
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
        ]
    )
    printed = "\n".join(messages)
    logged = (tmp_path / "logs" / "dbt.log").read_text(encoding="utf-8")
    assert result.success is False
    assert f"  catalog_properties: {{'{CATALOG_SECRET_KEY}': '***'" in printed
    assert "a RePark session is already live for a different profile" in printed
    assert f"requested database '{OTHER_CATALOG}'" in printed
    _assert_no_sentinel(printed)
    _assert_no_sentinel(logged)
