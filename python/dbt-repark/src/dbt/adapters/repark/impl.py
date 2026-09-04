"""The adapter: dbt-spark's, with every surface RePark refuses replaced."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.repark.connections import ReparkConnectionManager
from dbt.adapters.repark.relation import ReparkRelation
from dbt.adapters.spark.column import SparkColumn
from dbt.adapters.spark.impl import SparkAdapter, SparkConfig

from repark.errors import PySparkException

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = AdapterLogger("RePark")

ICEBERG_INFORMATION = "Provider: iceberg\nType: TABLE\n"


class ReparkAdapter(SparkAdapter):
    """dbt over the RePark engine, in process.

    Inherited from dbt-spark: the ``table`` materialization and the ``create_table_as``
    clause macros, which emit the one CTAS shape the RePark SQL door serves, and the
    ``SparkConfig`` keys the production gold project already sets. Replaced here: every
    listing, description and naming path, because RePark has no SQL listing surface and
    no current catalog.
    """

    Relation = ReparkRelation
    Column = SparkColumn
    ConnectionManager = ReparkConnectionManager
    AdapterSpecificConfigs = SparkConfig

    @classmethod
    def date_function(cls) -> str:
        """RePark spells the wall clock the way Spark does."""
        return "current_timestamp()"

    def _session(self) -> Any:
        """The shared RePark session behind this thread's connection."""
        return self.connections.repark_session()

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> list[BaseRelation]:
        """Live table names from the RePark catalog.

        ``SHOW TABLES IN`` is unimplemented and ``SHOW TABLE EXTENDED`` needs
        ``information_schema`` (registry ST-1), so the facade ``Catalog`` is the listing
        surface. A namespace that does not exist lists empty, which is what dbt asks of
        this method before it creates one.
        """
        database = schema_relation.database
        schema = schema_relation.schema
        try:
            names = self._session().list_iceberg_table_names(database, schema)
        except PySparkException as error:
            logger.debug(f"listing {database}.{schema} found nothing: {error}")
            return []
        return [
            self.Relation.create(
                database=database,
                schema=schema,
                identifier=name,
                type=RelationType.Table,
                is_iceberg=True,
                is_delta=False,
                is_hudi=False,
                information=ICEBERG_INFORMATION,
            )
            for name in names
        ]

    def get_relation(self, database: str, schema: str, identifier: str) -> BaseRelation | None:
        """Keep the catalog part dbt-spark drops; RePark needs all three."""
        return super(SparkAdapter, self).get_relation(database, schema, identifier)

    def get_columns_in_relation(self, relation: BaseRelation) -> list[SparkColumn]:
        """Columns from the facade schema.

        ``DESCRIBE EXTENDED`` runs but answers Arrow spellings (``Utf8``, ``Int32``) and no
        table-detail block, so its output cannot fill a dbt column (registry DBT-DESC-1).
        The facade schema answers Spark spellings.
        """
        try:
            schema = self._session().table(relation.render()).schema
        except PySparkException as error:
            logger.debug(f"no columns for {relation.render()}: {error}")
            return []
        return [
            SparkColumn(
                table_database=relation.database,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=None,
                table_stats=None,
                column=field.name,
                column_index=index,
                dtype=field.dataType.simpleString(),
            )
            for index, field in enumerate(schema.fields)
        ]

    def parse_columns_from_information(self, relation: BaseRelation) -> list[SparkColumn]:
        """The catalog path reads real columns; there is no describe-extended text to parse."""
        return self.get_columns_in_relation(relation)

    def _get_columns_for_catalog(self, relation: BaseRelation) -> Iterable[dict[str, Any]]:
        """Catalog rows built from the facade schema, in dbt's column-dict shape."""
        for column in self.get_columns_in_relation(relation):
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = relation.database
            yield as_dict
