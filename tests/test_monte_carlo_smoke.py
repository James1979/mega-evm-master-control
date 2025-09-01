"""
tests/test_monte_carlo_smoke.py

Tiny, fast smoke test for the Monte Carlo engine with schema + sanity checks.
This version clears sys.modules to avoid the runpy RuntimeWarning.
"""

import runpy            # Execute a module as if via `python -m`
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def _write_minimal_inputs(processed: Path, samples: Path) -> None:
    """
    Create the smallest viable input files so etl.monte_carlo can run.
    """
    processed.mkdir(parents=True, exist_ok=True)
    samples.mkdir(parents=True, exist_ok=True)

    # Baseline EVM parquet: minimal data for mean() to work
    evm = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "EAC": [1_000_000.0, 1_020_000.0],
            "BAC": [1_100_000.0, 1_100_000.0],
        }
    )
    (processed / "evm_timeseries.parquet").write_bytes(evm.to_parquet(index=False) or b"")

    # Risk register CSV: minimal numeric PERT + Probability
    risks = pd.DataFrame(
        {
            "RiskID": ["R1", "R2"],
            "Probability": [0.6, 0.4],
            "CostLow": [5_000.0, 10_000.0],
            "CostML": [10_000.0, 20_000.0],
            "CostHigh": [20_000.0, 40_000.0],
            "SchedDaysLow": [0.0, 1.0],
            "SchedDaysML": [2.0, 4.0],
            "SchedDaysHigh": [5.0, 10.0],
        }
    )
    risks.to_csv(samples / "risk_register.csv", index=False)

    # Procurement CSV: small artificial delays to exercise delay logic
    today = datetime(2025, 1, 1)
    procurement = pd.DataFrame(
        {
            "Item": ["Valve", "Cable"],
            "PlannedDelivery": [today, today + timedelta(days=5)],
            "ActualDelivery": [today + timedelta(days=3), today + timedelta(days=9)],
        }
    )
    procurement.to_csv(samples / "procurement.csv", index=False)


def test_monte_carlo_cli_runs_inprocess(tmp_path: Path, monkeypatch) -> None:
    """
    Smoke test: run `etl.monte_carlo` in-process with tiny params and validate outputs.
    """
    # Arrange: temp input/output dirs
    processed = tmp_path / "processed"
    samples = tmp_path / "samples"
    outdir = tmp_path / "out"
    _write_minimal_inputs(processed, samples)

    # Arrange: CLI argv for argparse in __main__
    argv = [
        "etl.monte_carlo",
        "--iters", "50",
        "--seed", "42",
        "--processed", str(processed),
        "--samples", str(samples),
        "--outdir", str(outdir),
    ]
    monkeypatch.setattr(sys, "argv", argv, raising=True)

    # NEW: ensure a clean import state so runpy doesn't warn
    sys.modules.pop("etl.monte_carlo", None)  # remove if already imported

    # Act: execute module as if `python -m etl.monte_carlo`
    runpy.run_module("etl.monte_carlo", run_name="__main__")

    # Assert: files exist
    runs_fp = outdir / "monte_carlo_runs.parquet"
    summ_fp = outdir / "monte_carlo_summary.parquet"
    scur_fp = outdir / "forecast_s_curves.parquet"
    assert runs_fp.exists()
    assert summ_fp.exists()
    assert scur_fp.exists()

    # Assert: schemas + basic sanity
    runs = pd.read_parquet(runs_fp)
    assert {"ProjectID", "EAC", "FinishDaysOverBaseline"}.issubset(runs.columns)
    assert len(runs) > 0

    summary = pd.read_parquet(summ_fp)
    expected_cols = {"ProjectID", "EAC_P10", "EAC_P50", "EAC_P80", "Finish_P10", "Finish_P50", "Finish_P80"}
    assert expected_cols.issubset(summary.columns)
    row = summary.iloc[0]
    assert row["EAC_P10"] <= row["EAC_P50"] <= row["EAC_P80"]
    assert row["Finish_P10"] <= row["Finish_P50"] <= row["Finish_P80"]

    sc = pd.read_parquet(scur_fp)
    assert {"ProjectID", "Metric", "Value", "CDF"}.issubset(sc.columns)
    assert (sc["CDF"] >= 0).all() and (sc["CDF"] <= 1).all()
    for (_, _), grp in sc.groupby(["ProjectID", "Metric"]):
        g = grp.sort_values("Value")
        assert (g["CDF"].diff().fillna(0) >= -1e-9).all()

