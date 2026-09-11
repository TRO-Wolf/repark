"""PROFILES-1 probe re-runnability pins — REVIEW-FIX-8 (Q-21, Q-55, Q-56).

The probe script lives beside its document under ``docs/perf/profiles-1-probe/``;
these pins run it as a subprocess exactly as the document's reproduce block does.

pins: review-fix-8/C-001, C-002, C-003, C-005
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROBE_DIR = Path(__file__).resolve().parents[3] / "docs" / "perf" / "profiles-1-probe"
PROBE = PROBE_DIR / "profiles1_probe.py"

POISON_TOML = '[default.conf]\ndatafusion.execution.batch_size = "notanumber"\n'


def _clean_env() -> dict[str, str]:
    env = dict(os.environ)
    env.pop("REPARK_CONFIG", None)
    return env


def _run_probe(env: dict[str, str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(PROBE)],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
        cwd=str(cwd) if cwd is not None else None,
    )


def test_probe_runs_twice_with_identical_output() -> None:
    """D-1: two runs in a row exit 0, agree byte for byte, stage nothing tracked."""
    before = sorted(path.name for path in PROBE_DIR.iterdir())
    first = _run_probe(_clean_env())
    assert first.returncode == 0, first.stderr[-2000:]
    second = _run_probe(_clean_env())
    assert second.returncode == 0, second.stderr[-2000:]
    assert second.stdout == first.stdout
    assert sorted(path.name for path in PROBE_DIR.iterdir()) == before


def test_probe_ignores_a_poisoned_discovered_config(tmp_path: Path) -> None:
    """D-1a: a discovered file with an invalid value cannot break the probe."""
    home = tmp_path / "home"
    (home / ".config" / "repark").mkdir(parents=True)
    (home / ".config" / "repark" / "repark.toml").write_text(POISON_TOML)
    work = tmp_path / "work"
    work.mkdir()
    (work / "repark.toml").write_text(POISON_TOML)
    env = _clean_env()
    env["HOME"] = str(home)
    run = _run_probe(env, cwd=work)
    assert run.returncode == 0, run.stderr[-2000:]


def test_probe_output_holds_the_table_inputs() -> None:
    """CONF-UNREAD-1 D-2: nineteen accepted keys, one loud refusal, nine validation refusals."""
    run = _run_probe(_clean_env())
    assert run.returncode == 0, run.stderr[-2000:]
    report = json.loads(run.stdout)
    assert len(report["keys"]) == 20
    refused = report["keys"]["datafusion.execution.coalesce_batches"]
    assert refused["outcome"] == "refused"
    assert "datafusion.execution.coalesce_batches" in refused["refusal"]
    assert refused["runtime_set"]["set"] == "refused"
    for key, entry in report["keys"].items():
        if entry["outcome"] == "refused":
            continue
        assert entry["conf_get"] == entry["set_value"], key
        assert entry["runtime_set"]["set"] == "accepted", key
    written = report["keys"]["datafusion.execution.parquet.write_batch_size"]["measured"]["write"]
    baseline_write = report["baseline"]["write"]
    assert written["part_files"] == baseline_write["part_files"]
    assert written["row_groups"] == baseline_write["row_groups"]
    assert written["bytes"] != baseline_write["bytes"]
    assert len(report["validation"]) == 9
    for name, probe in report["validation"].items():
        assert probe["outcome"] == "refused", name
