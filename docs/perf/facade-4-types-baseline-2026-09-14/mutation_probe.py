"""Mutation proof for the FACADE-4 goldens: each counterexample must go red.

Loads the committed test module and golden, applies each critic mutation in
process (no product edits), rebuilds the payload, and reports whether the
canonical bytes differ from the committed golden plus which case ids moved.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

TEST_PATH = (
    Path(__file__).resolve().parents[3] / "python/repark/tests/test_facade_4_ddl_round_trip.py"
)
GOLDEN_PATH = TEST_PATH.with_name("facade_4_type_goldens.json")


def _load_test_module() -> Any:
    spec = importlib.util.spec_from_file_location("facade4_test", TEST_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _diff_ids(live: str, expected: str) -> list[str]:
    live_cases = json.loads(live)
    want_cases = json.loads(expected)
    return sorted(
        case_id
        for case_id in live_cases.keys() & want_cases.keys()
        if live_cases[case_id] != want_cases[case_id]
    )


def _mutate_fromjson_forces_flags_true(types_module: Any) -> dict[str, Any]:
    original_array = types_module.ArrayType.__dict__["fromJson"]
    original_map = types_module.MapType.__dict__["fromJson"]

    def array_from(cls: Any, json_value: Any, *args: Any, **kwargs: Any) -> Any:
        parsed = original_array.__func__(cls, json_value, *args, **kwargs)
        parsed.containsNull = True
        return parsed

    def map_from(cls: Any, json_value: Any, *args: Any, **kwargs: Any) -> Any:
        parsed = original_map.__func__(cls, json_value, *args, **kwargs)
        parsed.valueContainsNull = True
        return parsed

    types_module.ArrayType.fromJson = classmethod(array_from)
    types_module.MapType.fromJson = classmethod(map_from)
    return {
        "restore": lambda: (
            setattr(types_module.ArrayType, "fromJson", original_array),
            setattr(types_module.MapType, "fromJson", original_map),
        )
    }


def _mutate_inbound_small_ints(types_module: Any) -> dict[str, Any]:
    original = types_module._arrow_type_to_repark

    def swapped(arrow_type: Any) -> Any:
        import pyarrow as pa

        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
            return types_module.IntegerType()
        return original(arrow_type)

    types_module._arrow_type_to_repark = swapped
    return {"restore": lambda: setattr(types_module, "_arrow_type_to_repark", original)}


def _mutate_preserve_item_nullability(types_module: Any) -> dict[str, Any]:
    original = types_module._arrow_type_to_repark

    def preserved(arrow_type: Any) -> Any:
        import pyarrow as pa

        result = original(arrow_type)
        if isinstance(result, types_module.ArrayType) and (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            result.containsNull = arrow_type.value_field.nullable
        if isinstance(result, types_module.MapType) and pa.types.is_map(arrow_type):
            result.valueContainsNull = arrow_type.item_field.nullable
        return result

    types_module._arrow_type_to_repark = preserved
    return {"restore": lambda: setattr(types_module, "_arrow_type_to_repark", original)}


def _mutate_drop_inner_struct_nullability(types_module: Any) -> dict[str, Any]:
    original = types_module._arrow_type_to_repark

    def dropped(arrow_type: Any) -> Any:
        import pyarrow as pa

        if pa.types.is_struct(arrow_type):
            fields = [
                types_module.StructField(field.name, original(field.type), True)
                for field in arrow_type
            ]
            return types_module.StructType(fields)
        return original(arrow_type)

    types_module._arrow_type_to_repark = dropped
    return {"restore": lambda: setattr(types_module, "_arrow_type_to_repark", original)}


def main() -> None:
    from repark.spark import types as types_module

    test_module = _load_test_module()
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    baseline = test_module._canonical_json(test_module._build_payload())
    out: dict[str, Any] = {
        "python": sys.executable,
        "baseline_matches_golden": baseline == expected,
    }
    mutations = {
        "fromjson_forces_flags_true": _mutate_fromjson_forces_flags_true,
        "inbound_int8_int16_to_integer": _mutate_inbound_small_ints,
        "preserve_arrow_item_nullability": _mutate_preserve_item_nullability,
        "drop_inner_struct_nullability": _mutate_drop_inner_struct_nullability,
    }
    for name, apply_mutation in mutations.items():
        handle = apply_mutation(types_module)
        live = test_module._canonical_json(test_module._build_payload())
        handle["restore"]()
        out[f"{name}_reds_golden"] = live != expected
        if live != expected:
            out[f"{name}_changed_ids"] = _diff_ids(live, expected)[:25]
    out["restored_matches_golden"] = (
        test_module._canonical_json(test_module._build_payload()) == expected
    )
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
