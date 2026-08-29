"""F1 free-SQL bare-name expander — Path A statement forms via resolve_table_name SSOT.

Covers INSERT / SELECT / CTAS / MERGE expansion, temp-view prefer on FROM, and non-rewrite
residuals (VIEW, TEMP TABLE, multi-statement scripts).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark.spark.session import ReparkSession, _reset_active_session_for_tests


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("pytest-f1-sql-expander")
        .config("repark.sql.autoMemoryCatalog", "false")
        .getOrCreate()
    )
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.create_namespace("glue_catalog", "default")
    yield session
    session.stop()
    _reset_active_session_for_tests()


def test_expand_insert_into_bare_target(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("INSERT INTO bare_t VALUES (1)")
    assert expanded == 'INSERT INTO "glue_catalog"."default"."bare_t" VALUES (1)'


def test_expand_insert_overwrite_table_bare_target(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("INSERT OVERWRITE TABLE bare_t SELECT 1 AS id")
    assert expanded.startswith('INSERT OVERWRITE TABLE "glue_catalog"."default"."bare_t"')
    assert "SELECT 1 AS id" in expanded


def test_expand_insert_select_from_also_qualifies_source(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "INSERT INTO dest SELECT * FROM src WHERE id > 0"
    )
    assert expanded == (
        'INSERT INTO "glue_catalog"."default"."dest" '
        'SELECT * FROM "glue_catalog"."default"."src" WHERE id > 0'
    )


def test_expand_create_table_as_select_target(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("CREATE TABLE bare_ctas AS SELECT 1 AS id")
    assert expanded == ('CREATE TABLE "glue_catalog"."default"."bare_ctas" AS SELECT 1 AS id')


def test_expand_create_table_if_not_exists(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "CREATE TABLE IF NOT EXISTS bare_t (id INT) USING iceberg"
    )
    assert expanded.startswith('CREATE TABLE IF NOT EXISTS "glue_catalog"."default"."bare_t"')


def test_create_view_name_not_qualified(spark: ReparkSession) -> None:
    """View *name* stays bare; body without FROM is unchanged."""
    sql = "CREATE VIEW bare_v AS SELECT 1 AS id"
    assert spark._expand_bare_table_names_in_sql(sql) == sql


def test_create_view_body_from_expands(spark: ReparkSession) -> None:
    """octo C5-Q-002: CREATE VIEW AS SELECT … FROM bare expands the body only."""
    expanded = spark._expand_bare_table_names_in_sql("CREATE VIEW bare_v AS SELECT * FROM bare_t")
    assert expanded == ('CREATE VIEW bare_v AS SELECT * FROM "glue_catalog"."default"."bare_t"')


def test_create_temp_table_not_rewritten(spark: ReparkSession) -> None:
    sql = "CREATE TEMPORARY TABLE bare_tmp AS SELECT 1 AS id"
    assert spark._expand_bare_table_names_in_sql(sql) == sql


def test_expand_select_from_bare(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM bare_t WHERE id = 1")
    assert expanded == ('SELECT * FROM "glue_catalog"."default"."bare_t" WHERE id = 1')


def test_expand_select_join_bare(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "SELECT a.id FROM left_t a JOIN right_t b ON a.id = b.id"
    )
    assert expanded == (
        'SELECT a.id FROM "glue_catalog"."default"."left_t" a '
        'JOIN "glue_catalog"."default"."right_t" b ON a.id = b.id'
    )


def test_select_from_temp_view_stays_bare(spark: ReparkSession) -> None:
    spark.range(3).createOrReplaceTempView("tv_f1")
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM tv_f1")
    # One-part temp view prefer — still bare (quoted only if resolve returned bare).
    assert "tv_f1" in expanded
    assert "glue_catalog" not in expanded


def test_select_from_subquery_not_mangled(spark: ReparkSession) -> None:
    sql = "SELECT * FROM (SELECT 1 AS id) AS sub"
    assert spark._expand_bare_table_names_in_sql(sql) == sql


def test_select_from_table_function_not_expanded(spark: ReparkSession) -> None:
    sql = "SELECT * FROM range(10)"
    assert spark._expand_bare_table_names_in_sql(sql) == sql


def test_expand_merge_into_bare_target_and_source(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "MERGE INTO tgt AS t USING src AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
    )
    assert expanded.startswith('MERGE INTO "glue_catalog"."default"."tgt" AS t USING')
    assert '"glue_catalog"."default"."src" AS s ON' in expanded


def test_expand_merge_into_subquery_source(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "MERGE INTO tgt USING (SELECT 1 AS id) s ON tgt.id = s.id WHEN NOT MATCHED THEN INSERT *"
    )
    assert expanded.startswith('MERGE INTO "glue_catalog"."default"."tgt" USING (SELECT 1 AS id)')


def test_two_part_name_gets_current_catalog(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM analytics.orders")
    assert expanded == 'SELECT * FROM "glue_catalog"."analytics"."orders"'


def test_three_part_name_unchanged_modulo_quoting(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM glue_catalog.default.orders")
    assert expanded == 'SELECT * FROM "glue_catalog"."default"."orders"'


def test_bare_session_auto_memory_catalog_select(tmp_path: Path) -> None:
    """Auto-memory sticky flag threads: bare getOrCreate qualifies to spark_catalog.default."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("f1-automem").getOrCreate()
    try:
        expanded = session._expand_bare_table_names_in_sql("SELECT * FROM bare_auto")
        assert expanded == 'SELECT * FROM "spark_catalog"."default"."bare_auto"'
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_e2e_bare_select_after_save_as_table(spark: ReparkSession) -> None:
    spark.range(4).write.saveAsTable("e2e_bare_sel")
    rows = spark.sql("SELECT id FROM e2e_bare_sel ORDER BY id").to_arrow().to_pylist()
    assert [row["id"] for row in rows] == [0, 1, 2, 3]


def test_e2e_bare_insert_into(spark: ReparkSession) -> None:
    spark.createDataFrame([(1,)], ["id"]).write.saveAsTable("e2e_bare_ins")
    spark.sql("INSERT INTO e2e_bare_ins VALUES (2)")
    rows = spark.sql("SELECT id FROM e2e_bare_ins ORDER BY id").to_arrow().to_pylist()
    assert [row["id"] for row in rows] == [1, 2]


def test_e2e_bare_ctas(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE e2e_bare_ctas AS SELECT 7 AS id")
    rows = spark.table("e2e_bare_ctas").to_arrow().to_pylist()
    assert rows == [{"id": 7}]


def test_extract_from_not_treated_as_table_ref(spark: ReparkSession) -> None:
    """EXTRACT(YEAR FROM col) must not expand ``col`` as a table (TPC-H Q7 class)."""
    sql = "SELECT extract(year FROM l_shipdate) FROM lineitem"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    # Only the FROM lineitem table ref is qualified — not the EXTRACT argument.
    assert "extract(year FROM l_shipdate)" in expanded
    assert 'FROM "glue_catalog"."default"."lineitem"' in expanded
    assert '."l_shipdate"' not in expanded


def test_cte_name_not_expanded(spark: ReparkSession) -> None:
    sql = "WITH q AS (SELECT 1 AS id) SELECT id FROM q"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert expanded.endswith("FROM q") or "FROM q" in expanded
    assert 'default"."q"' not in expanded


def test_nested_with_cte_name_not_expanded(spark: ReparkSession) -> None:
    """octo C1-Q-001: nested WITH CTE must stay bare (not catalog-qualified)."""
    sql = "SELECT * FROM (WITH q AS (SELECT 1 AS id) SELECT * FROM q) t"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert "FROM q" in expanded
    assert 'default"."q"' not in expanded
    assert '."q"' not in expanded


def test_e2e_nested_with_cte(spark: ReparkSession) -> None:
    """octo C1-L-001: nested WITH plans and returns rows (public sql entry)."""
    rows = (
        spark.sql("SELECT * FROM (WITH q AS (SELECT 7 AS id) SELECT * FROM q) t")
        .to_arrow()
        .to_pylist()
    )
    assert rows == [{"id": 7}]


def test_expand_comma_join_both_relations(spark: ReparkSession) -> None:
    """octo C1-Q-002: FROM a, b qualifies every relation in the list."""
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM a, b WHERE a.id = b.id")
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."a", "glue_catalog"."default"."b" WHERE a.id = b.id'
    )


def test_expand_comma_join_with_aliases(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "SELECT * FROM left_t AS l, right_t r WHERE l.id = r.id"
    )
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."left_t" AS l, '
        '"glue_catalog"."default"."right_t" r WHERE l.id = r.id'
    )


def test_cte_plus_comma_real_table_expands(spark: ReparkSession) -> None:
    sql = "WITH q AS (SELECT 1 AS id) SELECT * FROM q, real_t"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert "FROM q," in expanded or "FROM q ," in expanded
    assert '"glue_catalog"."default"."real_t"' in expanded
    assert 'default"."q"' not in expanded


def test_insert_overwrite_directory_not_table_target(spark: ReparkSession) -> None:
    """octo C1-Q-003: DIRECTORY path-insert is not rewritten as a catalog table."""
    sql = "INSERT OVERWRITE DIRECTORY '/tmp/x' SELECT * FROM src"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert "DIRECTORY" in expanded
    assert '."DIRECTORY"' not in expanded
    assert '"glue_catalog"."default"."src"' in expanded


def test_insert_overwrite_local_directory_not_table_target(spark: ReparkSession) -> None:
    sql = "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/x' SELECT 1"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert expanded.startswith("INSERT OVERWRITE LOCAL DIRECTORY")
    assert '."LOCAL"' not in expanded
    assert '."DIRECTORY"' not in expanded


def test_insert_into_table_keyword_optional(spark: ReparkSession) -> None:
    """octo C1-Q-004: INSERT INTO TABLE t — TABLE is keyword, not the table name."""
    expanded = spark._expand_bare_table_names_in_sql("INSERT INTO TABLE bare_t VALUES (1)")
    assert expanded == 'INSERT INTO TABLE "glue_catalog"."default"."bare_t" VALUES (1)'


def test_sql_comment_from_not_expanded(spark: ReparkSession) -> None:
    """octo C1-Q-005: FROM inside -- / /* */ comments is left alone."""
    line_comment = "SELECT 1 -- FROM bare\nFROM real_t"
    expanded_line = spark._expand_bare_table_names_in_sql(line_comment)
    assert "-- FROM bare" in expanded_line
    assert '"glue_catalog"."default"."real_t"' in expanded_line
    assert '."bare"' not in expanded_line

    block = "SELECT /* FROM bare */ 1 FROM real_t"
    expanded_block = spark._expand_bare_table_names_in_sql(block)
    assert "/* FROM bare */" in expanded_block
    assert '"glue_catalog"."default"."real_t"' in expanded_block


def test_leading_trivia_still_expands_select(spark: ReparkSession) -> None:
    """octo C1-Q-006: leading comments do not prevent SELECT classification."""
    sql = "/* lead */ SELECT * FROM bare"
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert expanded.startswith("/* lead */")
    assert '"glue_catalog"."default"."bare"' in expanded


def test_from_subquery_then_comma_table_expands(spark: ReparkSession) -> None:
    """octo C2-Q-001: FROM (subq) a, bare_t expands the bare sibling."""
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM (SELECT 1 AS id) a, bare_t b")
    assert expanded == ('SELECT * FROM (SELECT 1 AS id) a, "glue_catalog"."default"."bare_t" b')


def test_from_comma_then_subquery_then_table(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "SELECT * FROM left_t, (SELECT 1 AS id) mid, right_t"
    )
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."left_t", (SELECT 1 AS id) mid, '
        '"glue_catalog"."default"."right_t"'
    )


def test_merge_using_subquery_expands_inner_from(spark: ReparkSession) -> None:
    """octo C2-Q-002: MERGE USING (SELECT … FROM bare) qualifies the inner table."""
    expanded = spark._expand_bare_table_names_in_sql(
        "MERGE INTO tgt USING (SELECT * FROM bare_src) s ON tgt.id = s.id WHEN MATCHED THEN DELETE"
    )
    assert expanded.startswith('MERGE INTO "glue_catalog"."default"."tgt" USING')
    assert '"glue_catalog"."default"."bare_src"' in expanded
    assert "FROM bare_src" not in expanded


def test_merge_using_with_cte_stays_bare(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "MERGE INTO tgt USING (WITH q AS (SELECT 1 AS id) SELECT * FROM q) s "
        "ON tgt.id = s.id WHEN MATCHED THEN DELETE"
    )
    assert "FROM q" in expanded
    assert 'default"."q"' not in expanded


def test_tablesample_then_comma_expands_sibling(spark: ReparkSession) -> None:
    """octo C2-Q-003: TABLESAMPLE does not stop comma-list expansion."""
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM a TABLESAMPLE (10 PERCENT), b")
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."a" TABLESAMPLE (10 PERCENT), '
        '"glue_catalog"."default"."b"'
    )


def test_from_only_prefix_expands_table(spark: ReparkSession) -> None:
    """octo C3-Q-001: ONLY is a prefix, not a relation name."""
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM ONLY bare_t")
    assert expanded == 'SELECT * FROM ONLY "glue_catalog"."default"."bare_t"'


def test_nonrecursive_cte_body_expands_same_name_table(spark: ReparkSession) -> None:
    """octo C4-Q-001: non-recursive WITH body does not treat its own name as CTE."""
    expanded = spark._expand_bare_table_names_in_sql("WITH t AS (SELECT * FROM t) SELECT * FROM t")
    assert expanded == ('WITH t AS (SELECT * FROM "glue_catalog"."default"."t") SELECT * FROM t')


def test_cte_later_sees_earlier_not_expanded(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "WITH a AS (SELECT * FROM src), b AS (SELECT * FROM a) SELECT * FROM b"
    )
    assert expanded == (
        'WITH a AS (SELECT * FROM "glue_catalog"."default"."src"), '
        "b AS (SELECT * FROM a) SELECT * FROM b"
    )


def test_recursive_cte_self_ref_stays_bare(spark: ReparkSession) -> None:
    sql = (
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
        "SELECT * FROM r"
    )
    expanded = spark._expand_bare_table_names_in_sql(sql)
    assert "FROM r" in expanded
    assert 'default"."r"' not in expanded


def test_comment_between_relation_and_comma_expands_sibling(spark: ReparkSession) -> None:
    """octo C5-Q-001: comments must not break comma-list expansion."""
    expanded = spark._expand_bare_table_names_in_sql("SELECT * FROM t/*c*/ , u")
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."t"/*c*/ , "glue_catalog"."default"."u"'
    )


def test_tablesample_bernoulli_then_comma(spark: ReparkSession) -> None:
    """octo C6-Q-001: TABLESAMPLE BERNOULLI (n) does not stop comma expansion."""
    expanded = spark._expand_bare_table_names_in_sql(
        "SELECT * FROM t TABLESAMPLE BERNOULLI (10), u"
    )
    assert expanded == (
        'SELECT * FROM "glue_catalog"."default"."t" TABLESAMPLE BERNOULLI (10), '
        '"glue_catalog"."default"."u"'
    )


# G1 — UPDATE / DELETE bare targets (F1 residual)


def test_expand_update_bare_target(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("UPDATE bare_t SET name = 'x' WHERE id = 1")
    assert expanded == ('UPDATE "glue_catalog"."default"."bare_t" SET name = \'x\' WHERE id = 1')


def test_expand_update_with_alias(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "UPDATE bare_t AS t SET t.name = 'y' WHERE t.id = 2"
    )
    assert expanded == (
        'UPDATE "glue_catalog"."default"."bare_t" AS t SET t.name = \'y\' WHERE t.id = 2'
    )


def test_expand_update_two_part_gets_catalog(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "UPDATE analytics.orders SET status = 1 WHERE id = 0"
    )
    assert expanded == ('UPDATE "glue_catalog"."analytics"."orders" SET status = 1 WHERE id = 0')


def test_expand_update_where_subquery_from(spark: ReparkSession) -> None:
    """WHERE-subquery FROM expands via the existing region walker (never-regex-a-body)."""
    expanded = spark._expand_bare_table_names_in_sql(
        "UPDATE dest SET flag = 1 WHERE id IN (SELECT id FROM src WHERE active)"
    )
    assert expanded == (
        'UPDATE "glue_catalog"."default"."dest" SET flag = 1 '
        'WHERE id IN (SELECT id FROM "glue_catalog"."default"."src" WHERE active)'
    )


def test_expand_update_set_body_not_regexed(spark: ReparkSession) -> None:
    """SET assignments must not be freestyle-regexed (column names stay as written)."""
    expanded = spark._expand_bare_table_names_in_sql(
        "UPDATE bare_t SET col_set = (SELECT max(v) FROM other) WHERE k = 1"
    )
    assert expanded.startswith('UPDATE "glue_catalog"."default"."bare_t" SET col_set =')
    assert 'FROM "glue_catalog"."default"."other"' in expanded
    assert '."col_set"' not in expanded


def test_expand_update_table_name_ending_in_set(spark: ReparkSession) -> None:
    """octo C1-Q-004: table identifiers ending in ``set`` are not truncated at SET."""
    expanded = spark._expand_bare_table_names_in_sql("UPDATE bare_set SET x = 1 WHERE id = 0")
    assert expanded == ('UPDATE "glue_catalog"."default"."bare_set" SET x = 1 WHERE id = 0')


def test_expand_update_missing_table_does_not_eat_set_keyword(spark: ReparkSession) -> None:
    """octo C1-Q-005: ``UPDATE SET x = 1`` must not treat SET as a table name."""
    sql = "UPDATE SET x = 1"
    assert spark._expand_bare_table_names_in_sql(sql) == sql


def test_expand_delete_from_bare_target(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("DELETE FROM bare_t WHERE id = 1")
    assert expanded == 'DELETE FROM "glue_catalog"."default"."bare_t" WHERE id = 1'


def test_expand_delete_from_with_alias(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("DELETE FROM bare_t t WHERE t.id < 0")
    assert expanded == 'DELETE FROM "glue_catalog"."default"."bare_t" t WHERE t.id < 0'


def test_expand_delete_where_subquery_from(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql(
        "DELETE FROM dest WHERE id IN (SELECT id FROM src)"
    )
    assert expanded == (
        'DELETE FROM "glue_catalog"."default"."dest" '
        'WHERE id IN (SELECT id FROM "glue_catalog"."default"."src")'
    )


def test_expand_delete_two_part_gets_catalog(spark: ReparkSession) -> None:
    expanded = spark._expand_bare_table_names_in_sql("DELETE FROM analytics.orders WHERE id = 9")
    assert expanded == 'DELETE FROM "glue_catalog"."analytics"."orders" WHERE id = 9'


def test_update_delete_leading_trivia(spark: ReparkSession) -> None:
    """Leading comments still classify UPDATE/DELETE (same trivia split as SELECT)."""
    updated = spark._expand_bare_table_names_in_sql("/* lead */ UPDATE bare SET x = 1")
    assert updated.startswith("/* lead */")
    assert '"glue_catalog"."default"."bare"' in updated
    deleted = spark._expand_bare_table_names_in_sql("-- c\nDELETE FROM bare WHERE 1=1")
    assert deleted.startswith("-- c\n")
    assert '"glue_catalog"."default"."bare"' in deleted


def test_e2e_bare_update_and_delete(spark: ReparkSession) -> None:
    """Public sql() entry: bare UPDATE + DELETE hit Iceberg DML after expansion."""
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).write.saveAsTable("e2e_bare_dml")
    spark.sql("UPDATE e2e_bare_dml SET name = 'z' WHERE id = 1")
    spark.sql("DELETE FROM e2e_bare_dml WHERE id = 2")
    rows = spark.sql("SELECT id, name FROM e2e_bare_dml ORDER BY id").to_arrow().to_pylist()
    assert rows == [{"id": 1, "name": "z"}]
