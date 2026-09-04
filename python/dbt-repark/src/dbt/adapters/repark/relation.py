"""Relations that render the catalog part the RePark SQL door requires."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class ReparkQuotePolicy(Policy):
    """RePark identifiers are unquoted, as they are on Spark."""

    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class ReparkIncludePolicy(Policy):
    """Three parts always: DESCRIBE, ALTER TABLE and namespace DDL refuse two-part names."""

    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class ReparkRelation(BaseRelation):
    """A ``catalog.namespace.table`` relation carrying the flags dbt-spark's macros read."""

    quote_policy: Policy = field(default_factory=ReparkQuotePolicy)
    include_policy: Policy = field(default_factory=ReparkIncludePolicy)
    quote_character: str = "`"
    is_delta: bool | None = None
    is_hudi: bool | None = None
    is_iceberg: bool | None = None
    information: str | None = None
    require_alias: bool = False
