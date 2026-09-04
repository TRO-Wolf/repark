"""The dbt-repark adapter plugin."""

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.repark.connections import ReparkConnectionManager, ReparkCredentials
from dbt.adapters.repark.impl import ReparkAdapter
from dbt.adapters.repark.relation import ReparkRelation
from dbt.adapters.repark.session import acquire_session, release_session
from dbt.include import repark

Plugin = AdapterPlugin(
    adapter=ReparkAdapter,
    credentials=ReparkCredentials,
    include_path=repark.PACKAGE_PATH,
    dependencies=["spark"],
)

__all__ = [
    "Plugin",
    "ReparkAdapter",
    "ReparkConnectionManager",
    "ReparkCredentials",
    "ReparkRelation",
    "acquire_session",
    "release_session",
]
