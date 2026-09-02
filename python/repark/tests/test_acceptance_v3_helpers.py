"""AWS-free structural pins for the LIVE-v3 leg body and its two live legs.

Sibling of `test_acceptance_helpers.py` for the `_acceptance_v3` module: the never-teardown
guard over the one row-scoped delete, and the AST twin pin over the Glue / S3 Tables v3 legs.

pins: live-v3-aws-legs/C-001, C-003
"""

from __future__ import annotations

import ast
import pathlib

from _acceptance import ACCEPTANCE_TABLE_PREFIX
from _acceptance_v3 import v3_row_delete_sql

_TESTS_DIR = pathlib.Path(__file__).resolve().parent


def _module(filename: str) -> tuple[str, ast.Module]:
    """Source text and parsed tree for a harness module."""
    source = (_TESTS_DIR / filename).read_text(encoding="utf-8")
    return source, ast.parse(source)


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    """The top-level function ``name``."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name} not found")


def _call_names(fn: ast.FunctionDef) -> set[str]:
    """Every called name or attribute inside ``fn``."""
    names: set[str] = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                names.add(func.id)
            elif isinstance(func, ast.Attribute):
                names.add(func.attr)
    return names


def _table_name_is_scratch_uuid(fn: ast.FunctionDef) -> bool:
    """True when ``table_name`` is the scratch prefix plus a ``v3_dv_`` stem plus ``uuid4``."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.Assign):
            continue
        if "table_name" not in [t.id for t in node.targets if isinstance(t, ast.Name)]:
            continue
        children = list(ast.walk(node.value))
        has_uuid4 = any(
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and child.func.attr == "uuid4"
            for child in children
        )
        has_prefix = any(
            isinstance(child, ast.Name) and child.id == "ACCEPTANCE_TABLE_PREFIX"
            for child in children
        )
        has_stem = any(
            isinstance(child, ast.Constant)
            and isinstance(child.value, str)
            and "v3_dv_" in child.value
            for child in children
        )
        if has_uuid4 and has_prefix and has_stem:
            return True
    return False


def _run_v3_keywords(fn: ast.FunctionDef) -> set[str]:
    """Keyword argument names passed to ``run_v3_acceptance`` inside ``fn``."""
    return {
        keyword.arg
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "run_v3_acceptance"
        for keyword in node.keywords
        if keyword.arg
    }


def _configured_constants(fn: ast.FunctionDef) -> set[str]:
    """Names passed as the first argument of a ``.config(...)`` call."""
    return {
        node.args[0].id
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "config"
        and node.args
        and isinstance(node.args[0], ast.Name)
    }


def test_the_v3_leg_body_has_one_row_scoped_delete_and_no_drop() -> None:
    """The only DELETE FROM in `_acceptance_v3` is single-key, and nothing drops."""
    source, tree = _module("_acceptance_v3.py")
    upper = source.upper()
    assert "DROP TABLE" not in upper
    assert "DROP NAMESPACE" not in upper
    assert "DROP_TABLE" not in upper
    assert upper.count("DELETE FROM") == 1
    builder = _function(tree, "v3_row_delete_sql")
    builder_source = ast.get_source_segment(source, builder) or ""
    assert builder_source.upper().count("DELETE FROM") == 1
    scoped = v3_row_delete_sql("glue_catalog.testing_repark_acceptance.testing_v3", "id", 3)
    assert scoped == "DELETE FROM glue_catalog.testing_repark_acceptance.testing_v3 WHERE id = 3"


def test_the_v3_leg_body_never_registers_without_a_second_session() -> None:
    """`register_table` is reached only through the optional `adopt_with` factory."""
    source, tree = _module("_acceptance_v3.py")
    runner = _function(tree, "run_v3_acceptance")
    runner_source = ast.get_source_segment(source, runner) or ""
    assert "_register_table_sql" in runner_source
    assert "if adopt_with is not None:" in runner_source
    register = _function(tree, "_register_table_sql")
    register_source = ast.get_source_segment(source, register) or ""
    assert "register_table" in register_source
    assert "metadata_file" in register_source


def test_v3_legs_are_twins_of_the_mor_legs() -> None:
    """Both v3 legs are scratch-named, gated, and drive the shared helper and asserter."""
    source, tree = _module("test_aws_acceptance.py")

    glue = _function(tree, "test_v3_dv_dml_maintenance_against_glue")
    glue_names = _call_names(glue)
    assert "run_v3_acceptance" in glue_names
    assert "assert_v3_acceptance_outcome" in glue_names
    assert "assert_glue_scratch_namespace_location" in glue_names
    assert "assert_real_buckets_configured" in glue_names
    assert _table_name_is_scratch_uuid(glue)
    assert "adopt_with" in _run_v3_keywords(glue)
    assert "V3_ALLOW_CREATE_KEY" in _configured_constants(glue)
    assert ACCEPTANCE_TABLE_PREFIX == "testing_"

    s3 = _function(tree, "test_v3_dv_dml_maintenance_against_s3tables")
    s3_names = _call_names(s3)
    assert "run_v3_acceptance" in s3_names
    assert "assert_v3_acceptance_outcome" in s3_names
    assert "classify_v3_create_outcome" in s3_names
    assert "format_v3_refusal_record" in s3_names
    assert "s3tables_catalog_config" in s3_names
    assert "skip" in s3_names
    assert "assert_glue_scratch_namespace_location" not in s3_names
    assert _table_name_is_scratch_uuid(s3)
    assert "adopt_with" not in _run_v3_keywords(s3)
    assert "V3_ALLOW_CREATE_KEY" in _configured_constants(s3)
    s3_source = ast.get_source_segment(source, s3) or ""
    assert "TABLE_BUCKET_ARN" in s3_source

    relaxed = [
        keyword
        for node in ast.walk(s3)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "assert_v3_acceptance_outcome"
        for keyword in node.keywords
        if keyword.arg == "exact_commit_counts"
    ]
    assert len(relaxed) == 1
    assert isinstance(relaxed[0].value, ast.Constant) and relaxed[0].value.value is False

    create_calls = [
        node
        for node in ast.walk(s3)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_namespace"
    ]
    assert create_calls
    for create_call in create_calls:
        assert {keyword.arg for keyword in create_call.keywords if keyword.arg} == set()
        assert len(create_call.args) == 2

    denial_uses_format = False
    for node in ast.walk(s3):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "fail" and node.args:
            argument = node.args[0]
            if (
                isinstance(argument, ast.Call)
                and isinstance(argument.func, ast.Name)
                and argument.func.id == "format_denial_failure"
            ):
                denial_uses_format = True
    assert denial_uses_format, "denial path must pytest.fail(format_denial_failure(...))"
