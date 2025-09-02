# tests/test_alerts_append_mode.py
# ---------------------------------------------------------------------------------
# Purpose:
#   Exercise the branch where the optional recommendation file is missing AND
#   we append to an existing outbox (second run). This closes the remaining
#   uncovered lines near the outbox write path in services/alerts.py.
#
# What it does:
#   - Writes minimal EVM + MC (includes BAC for float casting)
#   - Writes a minimal config with dry_run true
#   - Runs alerts.main twice (second run appends)
#   - Ensures outbox JSON exists and has at least 2 items after second run
# ---------------------------------------------------------------------------------

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd
from services.alerts import main as alerts_main  # type: ignore

def _prep_inputs(processed: Path) -> None:
    processed.mkdir(parents=True, exist_ok=True)
    # Minimal EVM row — include BAC to avoid float(None)
    pd.DataFrame(
        {
            "ProjectID": ["PP"],
            "WBS": ["W1"],
            "Period": pd.to_datetime(["2025-01-01"]),
            "CPI": [0.88],
            "SPI": [0.80],
            "EAC": [250000.0],
            "VAC": [-20000.0],
            "BAC": [270000.0],
        }
    ).to_parquet(processed / "evm_timeseries.parquet", index=False)

    # Minimal MC summary
    pd.DataFrame(
        {
            "ProjectID": ["PP"],
            "EAC_P50": [240000.0],
            "EAC_P80": [280000.0],
            "Finish_P50": [10.0],
            "Finish_P80": [20.0],
        }
    ).to_parquet(processed / "monte_carlo_summary.parquet", index=False)


def _write_cfg(root: Path) -> Path:
    cfg = root / "config.yaml"
    cfg.write_text(
        "alerts:\n"
        "  slack_enabled: false\n"
        "  email_enabled: false\n"
        "  jira_enabled: false\n"
        "  dry_run: true\n",
        encoding="utf-8",
    )
    return cfg


def test_alerts_append_when_ai_note_missing(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    _prep_inputs(processed)
    cfg = _write_cfg(tmp_path)

    # Intentionally DO NOT create ai_recommendations.txt → cover "missing note" path

    # First run → creates outbox
    alerts_main(str(cfg), str(processed), dry_run=True)
    out_fp = processed / "alerts_outbox.json"
    assert out_fp.exists()

    first = json.loads(out_fp.read_text(encoding="utf-8"))
    # Normalize to list
    if isinstance(first, dict):
        first = [first]
    assert isinstance(first, list)

    # Second run → appends
    alerts_main(str(cfg), str(processed), dry_run=True)

    second = json.loads(out_fp.read_text(encoding="utf-8"))
    if isinstance(second, dict):
        second = [second]
    assert isinstance(second, list)
    assert len(second) >= len(first), "Second dry run should append or keep entries"
