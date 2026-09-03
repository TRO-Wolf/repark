"""Pins for the v1.0 API freeze: the decided rows, the registered names, the additive rule."""

from __future__ import annotations

import importlib.util
import json
import shutil
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_BUILDER = _REPO / "scripts" / "build_api_freeze.py"


def _load_builder() -> ModuleType:
    """Load the freeze-inventory builder as a module."""
    spec = importlib.util.spec_from_file_location("build_api_freeze", _BUILDER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _scratch_tree(builder: ModuleType, destination: Path) -> Path:
    """Copy every source the freeze enumerators read into a scratch root."""
    root = destination / "tree"
    for relative in builder.source_paths(_REPO):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_REPO / relative, target)
    return root


def _rewrite(path: Path, old: str, new: str) -> None:
    """Replace one occurrence in a scratch source file."""
    text = path.read_text(encoding="utf-8")
    assert old in text, old
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def test_freeze_inventory_matches_the_tree() -> None:
    """C-001: the checked-in frozen surface is exactly what the tree ships."""
    builder = _load_builder()
    inventory = builder.load_inventory(_REPO)
    assert builder.findings(_REPO, inventory) == []


def test_freeze_inventory_is_the_generated_file() -> None:
    """C-001: the checked-in file equals a fresh build, so regeneration is a no-op."""
    builder = _load_builder()
    assert builder.build(_REPO) == builder.load_inventory(_REPO)


def test_every_decision_equals_its_recommendation() -> None:
    """C-002: the owner answered every row with its recommendation on 2026-09-02."""
    builder = _load_builder()
    packet = json.loads((_REPO / builder.PACKET_RELATIVE).read_text(encoding="utf-8"))
    assert packet["decision_date"] == "2026-09-02"
    assert packet["decision_rule"] == builder.POLICY
    assert len(packet["rows"]) == 35
    assert all(row["decision"] == row["recommend"] for row in packet["rows"])
    counts = {"YES": 0, "YES except": 0, "NO": 0}
    for row in packet["rows"]:
        key = "NO" if row["decision"] == "NO" else row["decision"][:10].strip()
        counts[key] += 1
    assert counts == {"YES": 15, "YES except": 15, "NO": 5}


def test_no_rows_are_recorded_unfrozen_and_unchecked() -> None:
    """C-002: the five NO rows carry frozen false and no registered member."""
    builder = _load_builder()
    inventory = builder.load_inventory(_REPO)
    unfrozen = {row["id"] for row in inventory["rows"] if not row["frozen"]}
    assert unfrozen == {"B2", "J2", "K5", "K7", "M1"}
    assert all(row["members"] == [] for row in inventory["rows"] if not row["frozen"])
    assert inventory["counts"]["frozen_rows"] == 30
    assert inventory["counts"]["frozen_names"] == 888


def test_excepted_members_are_not_registered() -> None:
    """C-002: a `YES except` row registers the surface minus the named members."""
    builder = _load_builder()
    inventory = builder.load_inventory(_REPO)
    rows = {row["id"]: row for row in inventory["rows"]}
    registered = {member["name"] for member in rows["B1"]["members"]}
    assert {"DataFrame.filter", "DataFrame.where", "DataFrame.join"}.isdisjoint(registered)
    assert "DataFrame.select" in registered
    assert "MERGE" not in {member["name"] for member in rows["K2"]["members"]}
    assert all(not member["name"].startswith("repark.merge.") for member in rows["L1"]["members"])


def test_every_surface_id_is_claimed_by_exactly_one_row() -> None:
    """C-003: a new engine surface with no packet row is red."""
    builder = _load_builder()
    ids = builder.surface_ids(_REPO)
    assert len(ids) == 50
    assert builder.partition_findings(ids) == []
    assert builder.partition_findings([name for name in ids if name != "MERGE"]) != []


def test_a_removed_frozen_name_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: deleting a frozen member from its module REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    inventory = builder.load_inventory(_REPO)
    assert builder.findings(root, inventory) == []
    _rewrite(
        root / "python/repark/src/repark/spark/catalog.py",
        "    def list_tables(",
        "    def _list_tables(",
    )
    found = builder.findings(root, inventory)
    assert any("Catalog.list_tables" in finding and "gone" in finding for finding in found)


def test_a_renamed_required_parameter_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: renaming a frozen callable's required parameter REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(
        root / "python/repark/src/repark/spark/session/reader.py",
        "def format(self, source: str)",
        "def format(self, fmt: str)",
    )
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("DataFrameReader.format" in finding for finding in found)


def test_an_added_optional_parameter_stays_green(tmp_path: Path) -> None:
    """C-003 MUTATION: the additive rule — a new optional parameter is not a break."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(
        root / "python/repark/src/repark/spark/session/reader.py",
        "def format(self, source: str)",
        "def format(self, source: str, hint: str | None = None)",
    )
    assert builder.findings(root, builder.load_inventory(_REPO)) == []


def test_an_added_public_name_stays_green(tmp_path: Path) -> None:
    """C-003 MUTATION: the additive rule — a new public member is not a break."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(
        root / "python/repark/src/repark/spark/catalog.py",
        "    def list_tables(",
        "    def list_views(self) -> list[Any]:\n        return []\n\n    def list_tables(",
    )
    assert builder.findings(root, builder.load_inventory(_REPO)) == []


def test_a_flipped_door_disposition_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: a frozen door surface changing Tested to absent REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    dispositions = builder.door_dispositions(root, builder.DOOR_ANSI)
    assert dispositions["CTAS"] == builder.DISPOSITION_TESTED
    _rewrite(
        root / builder.ANSI_MATRIX_SOURCE,
        "surfaces::CTAS,\n        t(",
        "surfaces::CTAS,\n        absent(",
    )
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("CTAS" in finding for finding in found)


def test_a_dropped_conf_key_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: losing a frozen session conf key REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(
        root / builder.CARDINALITY_SOURCE,
        "repark.sql.maxArrayElements",
        "repark.sql.maxArrayElems",
    )
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("maxArrayElements" in finding for finding in found)


def test_a_dropped_error_class_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: losing a frozen error class REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(root / builder.ERRORS_SOURCE, '"ParseException",\n', "")
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("ParseException" in finding for finding in found)


def test_a_dropped_packaging_fact_is_red(tmp_path: Path) -> None:
    """C-003 MUTATION: losing a frozen packaging fact REDs the pin."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(root / builder.FACADE_PYPROJECT_SOURCE, 'requires-python = ">=3.12"', "")
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("requires-python" in finding for finding in found)


def test_a_changed_decision_is_red(tmp_path: Path) -> None:
    """C-002 MUTATION: a packet decision that stops equalling its recommendation REDs."""
    builder = _load_builder()
    root = _scratch_tree(builder, tmp_path)
    _rewrite(
        root / builder.PACKET_RELATIVE,
        '"recommend": "YES",\n   "decision": "YES",\n   "why": "builder/appName',
        '"recommend": "YES",\n   "decision": "NO",\n   "why": "builder/appName',
    )
    found = builder.findings(root, builder.load_inventory(_REPO))
    assert any("A1" in finding for finding in found)


def test_the_policy_sentence_is_the_owners_wording() -> None:
    """C-004: the release policy, the packet and the inventory carry one sentence."""
    builder = _load_builder()
    inventory = builder.load_inventory(_REPO)
    assert inventory["policy"] == builder.POLICY
    assert inventory["date"] == "2026-09-02"
    release = (_REPO / "docs" / "release.md").read_text(encoding="utf-8")
    assert "## Versioning policy" in release
    assert "docs/design/v1-0-api-freeze.json" in release
    assert "release cadence and versioning policy remain **unwritten**" not in release


def test_the_freeze_pointers_are_in_lockstep() -> None:
    """C-004: the facade ruling and the north-star gate point at the answered review."""
    facade = (_REPO / "docs" / "design" / "python-facade.md").read_text(encoding="utf-8")
    northstar = (
        _REPO / "task" / "roadmap" / "epic-term" / "v1-0-iceberg-v3-northstar.md"
    ).read_text(encoding="utf-8")
    assert "v1-0-api-review-2026-09-02.md" in facade
    assert "API review answered 2026-09-02" in northstar
    status = (_REPO / "STATUS.md").read_text(encoding="utf-8")
    assert "docs/design/v1-0-api-freeze.json" in status
    assert "the tag waits on the north-star gate" in status


@pytest.mark.parametrize("row_id", ["A1", "K6", "L1", "N1", "O1"])
def test_each_member_kind_registers_names(row_id: str) -> None:
    """C-001: every frozen member kind reaches the inventory with real names."""
    builder = _load_builder()
    rows = {row["id"]: row for row in builder.load_inventory(_REPO)["rows"]}
    row = rows[row_id]
    assert row["frozen"] is True
    assert row["members"]
    assert all(member["name"] for member in row["members"])
