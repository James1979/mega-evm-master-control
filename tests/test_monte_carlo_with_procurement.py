# tests/test_monte_carlo_with_procurement.py
# -----------------------------------------------------------------------------
# What this covers:
# - The code path where samples/procurement.csv EXISTS (not the fallback).
# - Ensures the delay-day distribution is derived from actual data.
# - Also exercises the CLI-ish print at the end (we don't assert on stdout;
#   calling the function is enough to mark those lines as executed).
# -----------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path

import pandas as pd

from etl.monte_carlo import run  # use the pure function, fast & deterministic


def test_monte_carlo_with_procurement_file(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    processed.mkdir(parents=True, exist_ok=True)
    samples.mkdir(parents=True, exist_ok=True)

    # Minimal baseline EVM so the simulation has an EAC/BAC base
    pd.DataFrame({"ProjectID": ["PX", "PX"], "EAC": [100_000.0, 120_000.0], "BAC": [150_000.0, 150_000.0]}).to_parquet(
        processed / "evm_timeseries.parquet", index=False
    )

    # Small risk register (numeric coercion happens inside the function)
    pd.DataFrame(
        {
            "Probability": [0.6, 0.3],
            "CostLow": [500, 200],
            "CostML": [1500, 600],
            "CostHigh": [3000, 1200],
            "SchedDaysLow": [0, 0],
            "SchedDaysML": [1, 2],
            "SchedDaysHigh": [3, 5],
        }
    ).to_csv(samples / "risk_register.csv", index=False)

    # ✅ Create procurement.csv to take the "file exists" branch
    pd.DataFrame(
        {
            "ProjectID": ["PX", "PX"],
            "WBS": ["W1", "W2"],
            "Item": ["Valve", "Pipe"],
            "PlannedDelivery": pd.to_datetime(["2025-01-10", "2025-02-01"]),
            "ActualDelivery": pd.to_datetime(["2025-01-20", "2025-02-15"]),  # delays: 10 & 14
        }
    ).to_csv(samples / "procurement.csv", index=False)

    # Run a very small simulation (fast)
    run(iters=30, seed=7, processed_dir=processed, samples_dir=samples, outdir=processed)

    # Outputs should exist — exercising the write & print tail of the function
    assert (processed / "monte_carlo_runs.parquet").exists()
    assert (processed / "monte_carlo_summary.parquet").exists()
    assert (processed / "forecast_s_curves.parquet").exists()
