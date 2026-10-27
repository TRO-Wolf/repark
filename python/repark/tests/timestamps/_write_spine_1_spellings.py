"""WRITE-SPINE-1 step 0: one write spelling per name, keyed to the door it enters.

NOT a ``test_`` module. Each entry is one way a user writes a value into an Iceberg table:
a Spark SQL statement, a DataFrame writer call, the same SQL text sent through
``repark.sql()``, or a door no Python spelling reaches. The 28 names of the U-8 probe
(``task/wo/ice-ntz-nested-refuse-1/evidence/probe.py``) keep the probe's statements; the
others add the D2 options path, the D6 table-creating family and the native doors D14 to D21.

A template is SQL text with named holes that ``_write_spine_1_shapes.py`` fills per cell:

- ``{table}``: the target table; ``{view}``: the one-row source view;
- ``{select}``: ``SELECT 1 AS id, <value> AS v, 0 AS k`` over the view when the source is a
  column; ``{select_cast}`` casts the value to ``{target_type}``; ``{select_part}`` omits
  the partition column ``k``;
- ``{value_bare}``, ``{value_s}``, ``{value_c}``: the value packed into the target shape, with
  the source as a literal or a scalar subquery, as ``s.<column>`` of a MERGE source, or from
  the sibling column ``c``;
- ``{bare}``, ``{s}``, ``{src}``, ``{from}``: the unpacked source in those places;
- ``{using}``, ``{using_literal}``: the MERGE source relation; ``{field}``: the leaf a
  field assignment writes; ``{null}``: a typed NULL of the target column.

A ``dataframe`` template names the writer call the recorder makes on ``spark.sql({select})``.
A ``seed`` runs before the template so an update or an overwrite has a row to change.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

SPARK_SQL = "spark_sql"
DATAFRAME = "dataframe"
NATIVE_SQL = "native_sql"
RUST_API = "rust_api"
NONE = "none"
REACHES: tuple[str, ...] = (SPARK_SQL, DATAFRAME, NATIVE_SQL, RUST_API, NONE)
ALL_TARGETS = "all"
FIELD_TARGETS = "field"
KEY_TARGETS = "key"
SEED_NULL = "INSERT INTO {table} (id, v, k) VALUES (1, {null}, 0)"
SEED_SIBLING = "INSERT INTO {table} SELECT 1, {null}, 0, {src}{from}"
SEED_TWO_ROWS = "INSERT INTO {table} SELECT 1, {value_src}, 0{from} UNION ALL SELECT 2, {null}, 0"
SESSION_CODEC: tuple[tuple[str, str], ...] = (("spark.sql.iceberg.compression-codec", "zstd"),)
WRITE_OPTION = ("snapshot-property.write-spine-1", "census")


class Spelling(BaseModel):
    """One write spelling: its name, its door, how it is reached and how a cell runs it."""

    model_config = ConfigDict(frozen=True)

    name: str
    door: str
    template: str
    reach: str
    row_level_mode: bool
    seed: str = ""
    targets: str = ALL_TARGETS
    creates_table: bool = False
    partitioned: bool = False
    sibling: bool = False
    session_conf: tuple[tuple[str, str], ...] = ()


def _sql(name: str, door: str, template: str, **extra: object) -> Spelling:
    """Return a Spark SQL spelling."""
    return Spelling(name=name, door=door, template=template, reach=SPARK_SQL, **extra)


def _frame(name: str, door: str, template: str, **extra: object) -> Spelling:
    """Return a DataFrame-writer spelling."""
    return Spelling(name=name, door=door, template=template, reach=DATAFRAME, **extra)


def _native(name: str, door: str, template: str, **extra: object) -> Spelling:
    """Return a spelling sent through ``repark.sql()``."""
    return Spelling(name=name, door=door, template=template, reach=NATIVE_SQL, **extra)


PROBE_SPELLINGS: tuple[Spelling, ...] = (
    _sql(
        "insert_values",
        "D1",
        "INSERT INTO {table} VALUES (1, {value_bare}, 0)",
        row_level_mode=False,
    ),
    _sql("insert_select", "D1", "INSERT INTO {table} {select}", row_level_mode=False),
    _sql(
        "insert_by_name",
        "D3",
        "INSERT INTO {table} BY NAME SELECT 0 AS k, {value_src} AS v, 1 AS id{from}",
        row_level_mode=False,
    ),
    _sql(
        "insert_overwrite",
        "D4",
        "INSERT OVERWRITE {table} {select}",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _sql(
        "insert_overwrite_values",
        "D4",
        "INSERT OVERWRITE {table} VALUES (1, {value_bare}, 0)",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _sql(
        "insert_replace_where",
        "D5",
        "INSERT INTO {table} REPLACE WHERE id >= 0 {select}",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _sql(
        "merge_insert",
        "D7",
        "MERGE INTO {table} t USING {using} ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {value_s}, 0)",
        row_level_mode=True,
    ),
    _sql(
        "merge_insert_star",
        "D7",
        "MERGE INTO {table} t USING ({select}) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
        row_level_mode=True,
    ),
    _sql(
        "merge_insert_literal",
        "D7",
        "MERGE INTO {table} t USING {using_literal} ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {value_s}, 0)",
        row_level_mode=True,
    ),
    _sql(
        "merge_update_whole",
        "D8",
        "MERGE INTO {table} t USING {using} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = {value_s}",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _sql(
        "merge_update_star",
        "D8",
        "MERGE INTO {table} t USING ({select}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _sql(
        "merge_update_whole_literal",
        "D8",
        "MERGE INTO {table} t USING {using_literal} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.v = {value_s}",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _sql(
        "update_whole_where",
        "D10",
        "UPDATE {table} SET v = {value_bare} WHERE id >= 0",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _sql(
        "update_whole_nowhere",
        "D10",
        "UPDATE {table} SET v = {value_bare}",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _sql(
        "update_col_where",
        "D10",
        "UPDATE {table} SET v = {value_c} WHERE id >= 0",
        row_level_mode=True,
        seed=SEED_SIBLING,
        sibling=True,
    ),
    _sql(
        "update_col_nowhere",
        "D10",
        "UPDATE {table} SET v = {value_c}",
        row_level_mode=True,
        seed=SEED_SIBLING,
        sibling=True,
    ),
    _frame(
        "df_append",
        "D1",
        "spark.sql({select}).writeTo({table}).append()",
        row_level_mode=False,
    ),
    _frame(
        "df_overwrite_partitions",
        "D4",
        "spark.sql({select}).writeTo({table}).overwritePartitions()",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _frame(
        "df_overwrite_condition",
        "D5",
        "spark.sql({select}).writeTo({table}).overwrite(col('id') >= 0)",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _frame(
        "df_insert_into",
        "D1",
        "spark.sql({select}).write.insertInto({table})",
        row_level_mode=False,
    ),
    _frame(
        "df_insert_into_overwrite",
        "D4",
        "spark.sql({select}).write.insertInto({table}, overwrite=True)",
        row_level_mode=False,
        seed=SEED_NULL,
    ),
    _frame(
        "df_save_as_table",
        "D1",
        "spark.sql({select}).write.mode('append').saveAsTable({table})",
        row_level_mode=False,
    ),
    _sql(
        "merge_update_field",
        "D9",
        "MERGE INTO {table} t USING {using} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.{field} = {s}",
        row_level_mode=True,
        seed=SEED_NULL,
        targets=FIELD_TARGETS,
    ),
    _sql(
        "merge_update_field_literal",
        "D9",
        "MERGE INTO {table} t USING {using_literal} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.{field} = {s}",
        row_level_mode=True,
        seed=SEED_NULL,
        targets=FIELD_TARGETS,
    ),
    _sql(
        "update_field_where",
        "D10",
        "UPDATE {table} SET {field} = {bare} WHERE id >= 0",
        row_level_mode=True,
        seed=SEED_NULL,
        targets=FIELD_TARGETS,
    ),
    _sql(
        "update_field_nowhere",
        "D10",
        "UPDATE {table} SET {field} = {bare}",
        row_level_mode=True,
        seed=SEED_NULL,
        targets=FIELD_TARGETS,
    ),
    _sql(
        "update_field_col_where",
        "D10",
        "UPDATE {table} SET {field} = c WHERE id >= 0",
        row_level_mode=True,
        seed=SEED_SIBLING,
        targets=FIELD_TARGETS,
        sibling=True,
    ),
    _sql(
        "update_field_col_nowhere",
        "D10",
        "UPDATE {table} SET {field} = c",
        row_level_mode=True,
        seed=SEED_SIBLING,
        targets=FIELD_TARGETS,
        sibling=True,
    ),
)

ADDED_SPELLINGS: tuple[Spelling, ...] = (
    _frame(
        "insert_options",
        "D2",
        "spark.sql({select}).writeTo({table})"
        f".option('{WRITE_OPTION[0]}', '{WRITE_OPTION[1]}').append()",
        row_level_mode=False,
    ),
    _sql(
        "ctas",
        "D6",
        "CREATE TABLE {table} USING iceberg AS {select_cast}",
        row_level_mode=False,
        creates_table=True,
    ),
    _sql(
        "rtas",
        "D6",
        "REPLACE TABLE {table} USING iceberg AS {select_cast}",
        row_level_mode=False,
    ),
    _sql(
        "create_or_replace_as",
        "D6",
        "CREATE OR REPLACE TABLE {table} USING iceberg AS {select_cast}",
        row_level_mode=False,
    ),
    _native("native_insert", "D14", "INSERT INTO {table} {select}", row_level_mode=False),
    _native(
        "native_insert_conf",
        "D15",
        "INSERT INTO {table} {select}",
        row_level_mode=False,
        session_conf=SESSION_CODEC,
    ),
    _native(
        "native_insert_overwrite_partition",
        "D16",
        "INSERT OVERWRITE {table} PARTITION (k = 0) {select_part}",
        row_level_mode=False,
        seed=SEED_NULL,
        partitioned=True,
    ),
    _native(
        "native_ctas",
        "D17",
        "CREATE TABLE {table} AS {select_cast}",
        row_level_mode=False,
        creates_table=True,
    ),
    Spelling(
        name="native_append_api", door="D18", template="", reach=RUST_API, row_level_mode=False
    ),
    _native(
        "native_merge",
        "D19",
        "MERGE INTO {table} t USING {using} ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET v = {value_s} "
        "WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {value_s}, 0)",
        row_level_mode=True,
    ),
    _native(
        "native_update",
        "D20",
        "UPDATE {table} SET v = {value_bare} WHERE id >= 0",
        row_level_mode=True,
        seed=SEED_NULL,
    ),
    _native(
        "native_delete",
        "D20",
        "DELETE FROM {table} WHERE id = 2",
        row_level_mode=True,
        seed=SEED_TWO_ROWS,
    ),
    Spelling(name="microbatch_sink", door="D21", template="", reach=NONE, row_level_mode=False),
    _sql(
        "update_whole_where_part",
        "D10",
        "UPDATE {table} SET v = {value_bare} WHERE k = 0",
        row_level_mode=True,
        seed=SEED_NULL,
        partitioned=True,
    ),
    _sql(
        "insert_map_key",
        "D3",
        "INSERT INTO {table} BY NAME SELECT 0 AS k, {value_src} AS v, 1 AS id{from}",
        row_level_mode=False,
        targets=KEY_TARGETS,
    ),
)

SPELLINGS: tuple[Spelling, ...] = (*PROBE_SPELLINGS, *ADDED_SPELLINGS)
BY_NAME: dict[str, Spelling] = {spelling.name: spelling for spelling in SPELLINGS}
UNREACHED_REASONS: dict[str, str] = {
    "native_append_api": (
        "D18 is reached only through the Rust append API; its one product caller is the native "
        "v3 CREATE path, and no Python call writes a chosen value through it"
    ),
    "microbatch_sink": (
        "D21 has no streaming facade on main: `readStream` and `writeStream` are declared "
        "not implemented, so no Python call reaches the micro-batch sink"
    ),
}


def measured_spellings() -> tuple[Spelling, ...]:
    """Return the spellings a Python cell can run, in table order."""
    return tuple(spelling for spelling in SPELLINGS if spelling.reach not in (RUST_API, NONE))


def check_table() -> None:
    """Raise when two spellings share a name or a spelling names an unknown reach."""
    if len(BY_NAME) != len(SPELLINGS):
        raise ValueError("duplicate spelling names in the WRITE-SPINE-1 table")
    unknown = [spelling.name for spelling in SPELLINGS if spelling.reach not in REACHES]
    if unknown:
        raise ValueError(f"unknown reach on {unknown}")
    unreached = {spelling.name for spelling in SPELLINGS if spelling.reach in (RUST_API, NONE)}
    if unreached != set(UNREACHED_REASONS):
        raise ValueError("every unreached spelling carries exactly one reason")


check_table()
