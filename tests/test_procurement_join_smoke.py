"""
tests/test_procurement_join_smoke.py

Purpose:
- Smoke test for `etl.procurement_join` run as a CLI module.
- Include Qty and UnitCost columns in procurement.csv because the module
  uses them to compute DelayCost.
- Reset sys.argv so pytest flags don't break argparse.

What it solves:
- KeyError: 'Qty'
"""

import runpy
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def _write_inputs(root: Path) -> None:
    """Create minimal EVM parquet and procurement.csv inputs (with Qty/UnitCost)."""
    processed = root / "data" / "processed"
    samples = root / "data" / "samples"
    processed.mkdir(parents=True, exist_ok=True)
    samples.mkdir(parents=True, exist_ok=True)

    evm = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "Period": ["2025-01-01", "2025-01-01"],
            "BAC": [1000.0, 2000.0],
            "EV": [700.0, 1400.0],
            "AC": [900.0, 1800.0],
            "PV": [1000.0, 2000.0],
        }
    )
    (processed / "evm_timeseries.parquet").write_bytes(evm.to_parquet(index=False) or b"")

    today = datetime(2025, 1, 1)
    proc = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "Item": ["Valve", "Cable"],
            "Vendor": ["ACME", "ACME"],    # <- add vendor so groupby(['WBS','Vendor']) is valid
            "Qty": [10, 100],                  # REQUIRED by module
            "UnitCost": [500.0, 12.5],         # REQUIRED by module
            "PlannedDelivery": [today, today + timedelta(days=5)],
            "ActualDelivery": [today + timedelta(days=3), today + timedelta(days=10)],
        }
    )
    proc.to_csv(samples / "procurement.csv", index=False)


def test_procurement_join_cli_runs(tmp_path, monkeypatch):
    """Run `etl.procurement_join` in a temp workspace and verify output parquet exists."""
    root = tmp_path
    _write_inputs(root)
    monkeypatch.chdir(root)

    # Remove pytest's flags for argparse in the module
    monkeypatch.setattr(sys, "argv", ["etl.procurement_join"])

    # Execute like: python -m etl.procurement_join
    runpy.run_module("etl.procurement_join", run_name="__main__")

    out_fp = root / "data" / "processed" / "procurement_impacts.parquet"
    assert out_fp.exists(), "Expected procurement_impacts.parquet to be generated"
    df = pd.read_parquet(out_fp)
    assert not df.empty
