"""
tests/test_alerts_smoke.py

Purpose:
- Smoke test the alert pipeline in dry-run mode.
- Create BOTH minimal EVM and minimal Monte Carlo summary parquet files that the
  alert module expects.
- Reset sys.argv so pytest flags don't break argparse.

What it solves:
- FileNotFoundError for data/processed/monte_carlo_summary.parquet
"""

import json
import runpy
import sys
from pathlib import Path

import pandas as pd


def _write_minimal_evm(processed_dir: Path) -> None:
    """Create a tiny evm_timeseries.parquet that forces CPI/SPI/VAC triggers."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    evm = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "Period": ["2025-01-01", "2025-01-01"],
            "BAC": [1000.0, 2000.0],
            "EV": [700.0, 1400.0],  # low EV -> CPI/SPI below thresholds
            "AC": [900.0, 1800.0],
            "PV": [1000.0, 2000.0],
            "EAC": [1200.0, 2200.0],
            "VAC": [-200.0, -100.0],
            "CPI": [0.78, 0.78],
            "SPI": [0.70, 0.70],
        }
    )
    (processed_dir / "evm_timeseries.parquet").write_bytes(evm.to_parquet(index=False) or b"")


def _write_minimal_mc_summary(processed_dir: Path) -> None:
    """
    Create the minimal monte_carlo_summary.parquet expected by services.alerts.
    Columns typically referenced: ProjectID, EAC_P50/EAC_P80, Finish_P50/Finish_P80.
    """
    summary = pd.DataFrame(
        {
            "ProjectID": ["P1"],
            "EAC_P10": [1100.0],
            "EAC_P50": [1200.0],
            "EAC_P80": [1300.0],
            "Finish_P10": [5.0],
            "Finish_P50": [10.0],
            "Finish_P80": [20.0],
        }
    )
    (processed_dir / "monte_carlo_summary.parquet").write_bytes(summary.to_parquet(index=False) or b"")


def _write_minimal_config(root: Path) -> None:
    """Write a small config.yaml enabling dry-run and standard thresholds."""
    cfg = """\
thresholds:
  cpi_red: 0.90
  spi_red: 0.85
  vac_negative: 0.0
alerts:
  slack_enabled: true
  email_enabled: true
  jira_enabled: true
  dry_run: true
paths:
  processed_dir: data/processed
"""
    (root / "config.yaml").write_text(cfg, encoding="utf-8")


def test_services_alerts_smoke(tmp_path, monkeypatch):
    """
    End-to-end dry-run smoke:
    - Create config + processed data (EVM + MC summary)
    - Reset sys.argv so argparse won't see pytest flags
    - Run `services.alerts` as __main__
    - Assert alerts_outbox.json exists and is parseable
    """
    root = tmp_path
    processed = root / "data" / "processed"
    _write_minimal_evm(processed)
    _write_minimal_mc_summary(processed)
    _write_minimal_config(root)

    # Run from temp root so relative paths resolve
    monkeypatch.chdir(root)

    # Remove pytest's flags for argparse in the module
    monkeypatch.setattr(sys, "argv", ["services.alerts"])

    # Execute like: python -m services.alerts
    runpy.run_module("services.alerts", run_name="__main__")

    out_fp = processed / "alerts_outbox.json"
    assert out_fp.exists(), "alerts_outbox.json should be written in dry-run mode"
    _ = json.loads(out_fp.read_text(encoding="utf-8"))  # ensure valid JSON
