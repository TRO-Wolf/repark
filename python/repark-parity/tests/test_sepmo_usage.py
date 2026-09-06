"""SEPMO usage collector pins.

pins: sepmo-e0-e1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import sys
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_SCRIPT = _REPO / "scripts" / "sepmo_usage.py"
_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sepmo_usage"
_INVENTORY = _REPO / "docs" / "sepmo" / "telemetry" / "inventory.md"
_SCHEMA = _REPO / "docs" / "sepmo" / "telemetry" / "usage-record.schema.json"

_LIVE_ROOT = Path(os.environ.get("SEPMO_USAGE_BASELINE_ROOT", "/tmp/muse-worker"))
_LIVE_SAMPLES: tuple[tuple[str, str, int, int, int], ...] = (
    ("ex25", "20260905T214117Z", 0, 408, 163),
    ("cdf1", "20260905T113405Z", 0, 883, 325),
    ("icescanfr2", "20260905T225134Z", 143, 221, 105),
)


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("sepmo_usage", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _collect(path: Path) -> dict[str, object]:
    module = _load()
    return module.collect_run(path)


def _prepared(source: Path, destination: Path) -> Path:
    shutil.copytree(source, destination)
    sample = destination / "handback.sample.json"
    if sample.is_file():
        sample.replace(destination / "handback.json")
    return destination


def _stamp_mtimes(path: Path, started: float, finished: float) -> None:
    os.utime(path / "cmd.txt", (started, started))
    exit_path = path / "exit"
    if exit_path.exists():
        os.utime(exit_path, (finished, finished))


def test_muse_fixture_counts_steps_and_tools_and_keeps_tokens_null(
    tmp_path: Path,
) -> None:
    """Muse collect counts task.started and tool.result; no session store.

    pins: sepmo-e0-e1/C-004, C-007
    """
    run_dir = _prepared(_FIXTURES / "muse" / "happy", tmp_path / "run")
    _stamp_mtimes(run_dir, 1_000.0, 1_130.5)
    record = _collect(run_dir)
    assert record["adapter"] == "muse"
    assert record["model"] == "muse-spark-1.3"
    assert record["effort"] == "max"
    assert record["role"] is None
    assert "does not guess" in str(record["missing_reason"]["role"])
    assert record["steps"] == 3
    assert record["tool_calls"] == 3
    assert record["commits"] == 2
    assert record["exit"] == 0
    assert record["wall_s"] == pytest.approx(130.5)
    assert record["tokens_in"] is None
    assert record["tokens_out"] is None
    assert record["tokens_cached"] is None
    assert record["cost_usd"] is None
    assert "tokens_in" in record["missing_reason"]
    assert "session" in str(record["missing_reason"]["tokens_in"])
    assert "cost field" in str(record["missing_reason"]["cost_usd"])
    assert record["units"]["wall_s"] == "seconds"
    assert record["units"]["tokens_in"] == "provider_tokens"
    assert record["truncated"] is False


def test_grok_fixture_reads_turns_and_cost_without_inventing_tokens() -> None:
    """Grok collect reads num_turns and total_cost_usd; missing tokens are per-field.

    pins: sepmo-e0-e1/C-004, C-007
    """
    record = _collect(_FIXTURES / "grok" / "happy")
    assert record["adapter"] == "grok"
    assert record["model"] == "grok-4.6"
    assert record["effort"] == "high"
    assert record["role"] == "sepmo-actor"
    assert record["steps"] == 4
    assert record["cost_usd"] == pytest.approx(0.0123)
    assert record["tokens_in"] is None
    assert record["tokens_cached"] is None
    assert record["tool_calls"] is None
    assert "usage.input_tokens" in str(record["missing_reason"]["tokens_in"])
    assert "cache_read_input_tokens" in str(record["missing_reason"]["tokens_cached"])
    assert "token keys were not present" not in str(record["missing_reason"])
    assert "tool_calls" in record["missing_reason"]


def test_grok_usage_object_is_read_when_the_tool_reports_it() -> None:
    """Grok collect reads the live usage key names, including cache_read_input_tokens.

    pins: sepmo-e0-e1/C-004, C-007
    """
    record = _collect(_FIXTURES / "grok" / "with-tokens")
    assert record["tokens_in"] == 275354
    assert record["tokens_out"] == 69313
    assert record["tokens_cached"] == 10007936
    assert record["tokens_cache_write"] == 0
    assert record["tokens_reasoning"] == 53150
    assert record["cost_usd"] == pytest.approx(1.7826183)
    assert record["steps"] == 49
    assert "tokens_in" not in record["missing_reason"]
    assert "tokens_cached" not in record["missing_reason"]


def test_opencode_fixture_sums_step_finish_usage(tmp_path: Path) -> None:
    """OpenCode collect sums step-finish tokens and cost; counts tool events.

    pins: sepmo-e0-e1/C-004, C-007
    """
    record = _collect(_prepared(_FIXTURES / "opencode" / "happy", tmp_path / "run"))
    assert record["adapter"] == "opencode"
    assert record["model"] == "zai/glm-5.3-flash"
    assert record["effort"] == "high"
    assert record["role"] == "worker"
    assert record["steps"] == 2
    assert record["tool_calls"] == 2
    assert record["tokens_in"] == 250
    assert record["tokens_out"] == 50
    assert record["tokens_cached"] == 50
    assert record["tokens_cache_write"] == 0
    assert record["tokens_reasoning"] == 12
    assert record["cost_usd"] == pytest.approx(0.03)
    assert record["commits"] == 1


def test_claude_fixture_records_unavailable_usage() -> None:
    """Claude collect keeps usage null with an unavailable missing_reason.

    pins: sepmo-e0-e1/C-004, C-007
    """
    record = _collect(_FIXTURES / "claude" / "unavailable")
    assert record["adapter"] == "claude"
    assert record["tokens_in"] is None
    assert record["steps"] is None
    assert "not accessible" in str(record["missing_reason"]["tokens_in"])


def test_empty_grok_out_json_does_not_become_zero() -> None:
    """An empty Grok out.json leaves token and step fields null, not zero.

    pins: sepmo-e0-e1/C-004, C-007
    """
    record = _collect(_FIXTURES / "grok" / "empty-out")
    assert record["exit"] == 1
    assert record["steps"] is None
    assert record["tokens_in"] is None
    assert record["cost_usd"] is None
    assert record["tokens_in"] != 0
    assert "empty" in str(record["missing_reason"]["steps"])


def test_missing_cmd_txt_fails_loudly() -> None:
    """Collect fails when cmd.txt is absent.

    pins: sepmo-e0-e1/C-006
    """
    module = _load()
    with pytest.raises(module.UsageError, match=re.escape("missing cmd.txt")):
        module.collect_run(_FIXTURES)


def test_malformed_grok_json_fails_loudly() -> None:
    """Collect fails on truncated Grok out.json.

    pins: sepmo-e0-e1/C-006
    """
    module = _load()
    with pytest.raises(module.UsageError, match=re.escape("malformed out.json")):
        module.collect_run(_FIXTURES / "malformed" / "bad-json")


def test_malformed_muse_jsonl_fails_loudly() -> None:
    """Collect fails when a majority of Muse JSONL lines do not parse.

    pins: sepmo-e0-e1/C-006
    """
    module = _load()
    with pytest.raises(module.UsageError, match="malformed JSONL"):
        module.collect_run(_FIXTURES / "malformed" / "bad-jsonl")


def test_truncated_muse_jsonl_fails_when_exit_record_absent() -> None:
    """A cut last JSONL line with no run.terminal.completed is a loud failure.

    pins: sepmo-e0-e1/C-006
    """
    module = _load()
    with pytest.raises(module.UsageError, match="truncated JSONL with no exit record"):
        module.collect_run(_FIXTURES / "malformed" / "truncated-tail")


def test_remote_url_is_rejected_before_resolve() -> None:
    """Collect refuses a remote URL on the raw argument, not after Path.resolve.

    pins: sepmo-e0-e1/C-006
    """
    module = _load()
    with pytest.raises(module.UsageError, match="refusing a remote path"):
        module.collect_run(Path("https://example.invalid/run"))


def test_muse_session_store_tokens_from_runs_tsv_join(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Muse collect joins runs.tsv to the session store and splits uncached input.

    pins: sepmo-e0-e1/C-004, C-007
    """
    fixture = _FIXTURES / "muse" / "with-tokens"
    root = tmp_path / "muse-worker"
    run_dir = root / "ex25" / "20260905T214117Z"
    run_dir.mkdir(parents=True)
    for name in ("cmd.txt", "prompt.md", "out.jsonl", "exit"):
        (run_dir / name).write_bytes((fixture / name).read_bytes())
    (root / "runs.tsv").write_bytes((fixture / "runs.tsv").read_bytes())
    sessions = tmp_path / "sessions"
    monkeypatch.setenv("SEPMO_MUSE_SESSIONS_ROOT", str(sessions))
    session_dir = sessions / "sess-muse-tokens"
    session_dir.mkdir(parents=True)
    (session_dir / "session.jsonl").write_bytes(
        (fixture / "sessions" / "sess-muse-tokens" / "session.jsonl").read_bytes()
    )
    snap_dir = sessions / "msp-view-v1" / "sess-muse-tokens"
    snap_dir.mkdir(parents=True)
    (snap_dir / "snapshot-1.json").write_bytes(
        (fixture / "sessions" / "msp-view-v1" / "sess-muse-tokens" / "snapshot-1.json").read_bytes()
    )
    record = _collect(run_dir)
    assert record["adapter"] == "muse"
    assert record["steps"] == 2
    assert record["tool_calls"] == 2
    assert record["tokens_in"] == 170
    assert record["tokens_out"] == 30
    assert record["tokens_cached"] == 80
    assert record["tokens_cache_write"] == 5
    assert record["tokens_reasoning"] == 5
    assert record["cost_usd"] is None
    assert "cost field" in str(record["missing_reason"]["cost_usd"])
    assert record["role"] is None
    assert "session.jsonl" in record["source"]
    assert any("matched" in str(item) for item in record["source"])


def test_index_writes_inventory_table_shape(tmp_path: Path) -> None:
    """Index emits the inventory table columns over a directory of run dirs.

    pins: sepmo-e0-e1/C-005
    """
    module = _load()
    root = tmp_path / "runs"
    shutil.copytree(_FIXTURES / "muse" / "happy", root / "ex25" / "20260905T214117Z")
    shutil.copytree(_FIXTURES / "grok" / "happy", root / "sepmoe0" / "20260906T010334Z")
    table = module.index_runs(root, as_jsonl=False)
    assert table.startswith("| unit | lane | adapter |")
    assert "| muse |" in table
    assert "| grok |" in table
    jsonl = module.index_runs(root, as_jsonl=True)
    lines = [line for line in jsonl.splitlines() if line.strip()]
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    adapters = {row["adapter"] for row in parsed}
    assert adapters == {"muse", "grok"}


def test_cli_collect_and_index_round_trip(tmp_path: Path) -> None:
    """The CLI collect and index entry points exit 0 on fixtures.

    pins: sepmo-e0-e1/C-005, C-006
    """
    module = _load()
    happy = _prepared(_FIXTURES / "muse" / "happy", tmp_path / "happy")
    collect_exit = module.main(["collect", str(happy)])
    assert collect_exit == 0
    index_root = tmp_path / "index-root"
    shutil.copytree(happy, index_root / "ex25" / "20260905T214117Z")
    index_exit = module.main(["index", str(index_root)])
    assert index_exit == 0
    bad_exit = module.main(["collect", str(_FIXTURES / "malformed" / "bad-json")])
    assert bad_exit == 1


def test_schema_file_lists_every_record_field() -> None:
    """The checked-in schema names every collector field, all nullable.

    pins: sepmo-e0-e1/C-004
    """
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    module = _load()
    required = set(schema["required"])
    expected = set(module.RECORD_FIELDS) | {"missing_reason", "units", "source"}
    assert required == expected
    for field_name in module.RECORD_FIELDS:
        types = schema["properties"][field_name]["type"]
        assert "null" in types


def test_inventory_covers_four_adapters_and_pilot_strata() -> None:
    """The inventory names each adapter, missing fields, baseline, and strata.

    pins: sepmo-e0-e1/C-001, C-002, C-003
    """
    text = _INVENTORY.read_text(encoding="utf-8")
    for adapter in ("Muse", "opencode", "Grok", "Claude"):
        assert adapter in text
    assert "session.jsonl" in text
    assert "cache_read_input_tokens" in text
    assert "tokens_input" in text
    assert "num_turns" in text
    assert "total_cost_usd" in text
    assert "not accessible" in text
    assert "20260905T214117Z" in text
    assert "ex25" in text
    assert "ex26" in text
    assert "approxpctr2" in text
    assert "cdf1" in text
    assert "icescanfr2" in text
    assert "Mechanical edits" in text
    assert "Rust semantic" in text
    assert "Sensitive write" in text
    assert "critic round" in text
    assert "remediation" in text
    assert "EX-26" not in text
    assert "no cost field" in text or "cost is genuinely absent" in text


def test_reconciles_three_live_muse_run_dirs_when_present() -> None:
    """Collector numbers for three frozen Muse run dirs match the raw files.

    pins: sepmo-e0-e1/C-008
    """
    module = _load()
    present = []
    for lane, stamp, _exit, _steps, _tools in _LIVE_SAMPLES:
        path = _LIVE_ROOT / lane / stamp
        if path.is_dir() and (path / "cmd.txt").is_file() and (path / "out.jsonl").is_file():
            present.append(path)
    if len(present) < 3:
        pytest.skip(f"frozen Muse baseline absent under {_LIVE_ROOT}")
    for lane, stamp, expected_exit, expected_steps, expected_tools in _LIVE_SAMPLES:
        path = _LIVE_ROOT / lane / stamp
        raw_tools = 0
        raw_tasks: set[str] = set()
        with (path / "out.jsonl").open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = event.get("payload") or {}
                if event.get("payload_type") == "tool.result":
                    raw_tools += 1
                if event.get("payload_type") == "task.lifecycle.started":
                    task_id = payload.get("task_id")
                    if isinstance(task_id, str):
                        raw_tasks.add(task_id)
        record = module.collect_run(path)
        assert record["adapter"] == "muse"
        assert record["exit"] == expected_exit
        assert record["steps"] == expected_steps == len(raw_tasks)
        assert record["tool_calls"] == expected_tools == raw_tools
        assert record["cost_usd"] is None
        started = (path / "cmd.txt").stat().st_mtime
        finished = (path / "exit").stat().st_mtime
        assert record["wall_s"] == pytest.approx(finished - started, abs=0.05)
        session_id = None
        with (path / "out.jsonl").open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                stream = event.get("stream")
                if isinstance(stream, dict) and isinstance(stream.get("id"), str):
                    session_id = stream["id"]
                    break
        if session_id is None:
            assert record["tokens_in"] is None
            continue
        sessions = Path.home() / ".local" / "share" / "muse" / "sessions"
        matches = list(sessions.glob(f"*/*/*/{session_id}/session.jsonl"))
        if not matches:
            assert record["tokens_in"] is None
            continue
        raw_in = 0
        raw_out = 0
        raw_cached = 0
        raw_reason = 0
        with matches[0].open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                payload = event.get("payload") or {}
                inner = payload.get("event") or {}
                if inner.get("kind") != "model_completed":
                    continue
                usage = inner.get("usage") or {}
                if "input_tokens" not in usage:
                    continue
                prompt = int(usage.get("input_tokens") or 0)
                cached = int(usage.get("cached_tokens") or 0)
                raw_in += prompt - cached if prompt >= cached else prompt
                raw_out += int(usage.get("output_tokens") or 0)
                raw_cached += cached
                raw_reason += int(usage.get("reasoning_tokens") or 0)
        assert record["tokens_in"] == raw_in
        assert record["tokens_out"] == raw_out
        assert record["tokens_cached"] == raw_cached
        assert record["tokens_reasoning"] == raw_reason
