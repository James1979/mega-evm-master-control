"""
tests/test_evm_calculator_cli.py

Goal:
- Exercise the CLI (__main__) path of etl.evm_calculator to cover argparse + main().
- Ensures the module writes its expected parquet output when run as a script.

What this tests:
- We provide tiny CSV inputs in a temp workspace.
- We sanitize sys.argv so pytest's flags (like --cov) don't confuse argparse.
- We assert that the expected output parquet exists and has the essential columns.

Why it matters:
- CLI path is often uncovered in unit tests, but it's critical for CI/CD pipelines
  and hiring-manager demos.
"""

import runpy
import sys
from pathlib import Path

import pandas as pd


def _write_inputs(root: Path) -> None:
    """
    Create the minimal CSVs the CLI expects:
    - data/samples/schedule_activities.csv
    - data/samples/cost_erp.csv
    """
    samples = root / "data" / "samples"
    processed = root / "data" / "processed"
    samples.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)

    # Schedule inputs
    sched = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "PercentComplete": [0.5, 0.25],
            "BAC": [1000.0, 2000.0],
        }
    )
    sched.to_csv(samples / "schedule_activities.csv", index=False)

    # Cost inputs across 2 periods
    cost = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "Period": ["2025-01", "2025-02"],
            "ActualCost": [400.0, 250.0],
            "Budget": [500.0, 200.0],
        }
    )
    cost.to_csv(samples / "cost_erp.csv", index=False)


def test_evm_calculator_cli_writes_parquet(tmp_path, monkeypatch):
    """
    Arrange:
      - Temp repo root; write minimal input files.
    Act:
      - Change CWD to the temp root.
      - Reset sys.argv to a barebones set the CLI expects (no pytest flags).
      - Run the module as __main__ via runpy.
    Assert:
      - evm_timeseries.parquet exists with essential columns.
    """
    root = tmp_path
    _write_inputs(root)
    monkeypatch.chdir(root)

    # Reset argv for argparse in the module:
    # If your CLI supports custom args (--samples, --processed, --out), add them here.
    # We keep defaults so it reads from data/samples and writes to data/processed.
    monkeypatch.setattr(sys, "argv", ["etl.evm_calculator"])

    # Execute like: python -m etl.evm_calculator
    runpy.run_module("etl.evm_calculator", run_name="__main__")

    # Verify output parquet
    out_fp = root / "data" / "processed" / "evm_timeseries.parquet"
    assert out_fp.exists(), "Expected evm_timeseries.parquet to be created by the CLI run"

    df = pd.read_parquet(out_fp)
    assert not df.empty
    for col in ["ProjectID", "WBS", "Period", "EV", "PV", "AC", "BAC", "CPI", "SPI", "EAC", "VAC"]:
        assert col in df.columns
