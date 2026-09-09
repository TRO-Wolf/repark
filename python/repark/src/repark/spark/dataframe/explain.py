"""Facade explain rendering: Spark section headers over verbatim DataFusion plan text."""

from __future__ import annotations

_LOGICAL_PLAN_HEADER: str = "== Optimized Logical Plan =="
_PHYSICAL_PLAN_HEADER: str = "== Physical Plan =="
_EXPLAIN_CODEGEN_NOTE: str = "codegen: not applicable (RePark has no generated code)"
_EXPLAIN_SECTION_PLAN: dict[str, tuple[str, tuple[str | None, ...]]] = {
    "simple": ("EXPLAIN", ("physical_plan",)),
    "extended": ("EXPLAIN", ("logical_plan", "physical_plan")),
    "formatted": ("EXPLAIN FORMAT TREE", (None,)),
    "cost": ("EXPLAIN ANALYZE", (None,)),
    "codegen": ("EXPLAIN", ("physical_plan",)),
}


def _render_explain_sections(
    selected: str,
    keys: tuple[str | None, ...],
    rows: list[tuple[str, str]],
) -> str:
    """Render the explain sections: header plus verbatim plan text, blank line between."""
    rendered = ""
    for key in keys:
        texts = [text for plan_type, text in rows if key is None or plan_type == key]
        if not texts:
            continue
        if rendered:
            rendered += "\n" if rendered.endswith("\n") else "\n\n"
        header = _LOGICAL_PLAN_HEADER if key == "logical_plan" else _PHYSICAL_PLAN_HEADER
        rendered += f"{header}\n{''.join(texts)}"
    return rendered + (f"{_EXPLAIN_CODEGEN_NOTE}\n" if selected == "codegen" else "")
