# tests/test_alerts_outbox_path.py
# --------------------------------------------------------------------
# Purpose:
#   Hit the outbox write path in services/alerts.py with dry_run=True.
#   This covers the remaining uncovered lines (around 60–63).
#
# What this tests:
#   - Builds minimal processed inputs (EVM + MC summary + AI note)
#   - Builds a minimal config with alerts.dry_run = true
#   - Calls main(config, processed, dry_run=True) directly (no argparse)
#   - Asserts alerts_outbox.json exists and is valid JSON
# --------------------------------------------------------------------

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

# Import the module entrypoint
from services.alerts import main as alerts_main  # type: ignore


def _write_minimal_processed_for_alerts(root: Path) -> Path:
    """
    Create the minimal set of processed inputs that services.alerts expects:
      - evm_timeseries.parquet   (must include BAC or float(None) will fail)
      - monte_carlo_summary.parquet
      - ai_recommendations.txt   (optional narrative snippet consumed by alerts)
    """
    processed = root / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # Minimal EVM needed by alerts flow
    # NOTE: Include BAC so build_alerts can safely float(bac)
    pd.DataFrame(
        {
            "ProjectID": ["PA"],
            "WBS": ["W1"],
            "Period": pd.to_datetime(["2025-01-01"]),
            "CPI": [0.9],
            "SPI": [0.85],
            "EAC": [500_000.0],
            "VAC": [-10_000.0],
            "BAC": [520_000.0],  # <- important: alerts casts this to float
        }
    ).to_parquet(processed / "evm_timeseries.parquet", index=False)

    # Minimal MC summary (alerts may read this)
    pd.DataFrame(
        {
            "ProjectID": ["PA"],
            "EAC_P50": [490_000.0],
            "EAC_P80": [520_000.0],
            "Finish_P50": [15.0],
            "Finish_P80": [25.0],
        }
    ).to_parquet(processed / "monte_carlo_summary.parquet", index=False)

    # Optional note referenced by alerts
    (processed / "ai_recommendations.txt").write_text("Stub recommendations for test.", encoding="utf-8")

    return processed


def _write_minimal_config(root: Path) -> Path:
    """
    Write a minimal YAML config that keeps all external integrations disabled
    and runs alerts in dry_run mode so no real sends occur.
    """
    cfg = root / "config.yaml"
    cfg.write_text(
        "alerts:\n  slack_enabled: false\n  email_enabled: false\n  jira_enabled: false\n  dry_run: true\n",
        encoding="utf-8",
    )
    return cfg


def test_alerts_main_writes_outbox_in_dry_run(tmp_path: Path) -> None:
    """
    Arrange:
      - Minimal processed inputs + dry-run config
    Act:
      - Call services.alerts.main directly with dry_run=True
    Assert:
      - alerts_outbox.json is created and contains valid JSON
    """
    processed = _write_minimal_processed_for_alerts(tmp_path)
    cfg = _write_minimal_config(tmp_path)

    # Call the entry with dry_run=True (no external sends)
    alerts_main(str(cfg), str(processed), dry_run=True)

    out_fp = processed / "alerts_outbox.json"
    assert out_fp.exists(), "alerts_outbox.json should be created in dry-run"

    data = json.loads(out_fp.read_text(encoding="utf-8"))
    assert isinstance(data, (dict, list)), "outbox JSON should be a dict or list"
