# tests/test_monte_carlo_no_procurement.py
# ---------------------------------------------------------------------------------
# Purpose:
#   Cover the branch where samples/procurement.csv does NOT exist, forcing the
#   fallback PERT defaults (pd_low, pd_ml, pd_high). This closes coverage holes
#   around the "no file" path in etl/monte_carlo.py.
#
# What it does:
#   - Writes minimal evm_timeseries.parquet in data/processed
#   - Writes a tiny risk_register.csv in data/samples
#   - Intentionally does NOT create procurement.csv
#   - Calls run(iters=30, ...) to keep it fast
#   - Asserts outputs exist and have plausible shape
# ---------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import pandas as pd
from etl.monte_carlo import run  # uses the pure function; no CLI

def test_monte_carlo_fallback_when_no_procurement(tmp_path: Path) -> None:
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    processed.mkdir(parents=True, exist_ok=True)
    samples.mkdir(parents=True, exist_ok=True)

    # Minimal evm_timeseries with EAC/BAC so base frame exists
    pd.DataFrame(
        {"ProjectID": ["P1", "P1"], "EAC": [100000.0, 120000.0], "BAC": [150000.0, 150000.0]}
    ).to_parquet(processed / "evm_timeseries.parquet", index=False)

    # Tiny risk register — columns will be coerced numeric inside the function
    pd.DataFrame(
        {
            "Probability": [0.5, 0.2],
            "CostLow": [1000, 500],
            "CostML": [2000, 800],
            "CostHigh": [5000, 1500],
            "SchedDaysLow": [0, 0],
            "SchedDaysML": [2, 1],
            "SchedDaysHigh": [5, 3],
        }
    ).to_csv(samples / "risk_register.csv", index=False)

    # NOTE: we do NOT create samples/procurement.csv → forces fallback branch

    # Run small simulation (fast)
    outdir = processed
    run(iters=30, seed=123, processed_dir=processed, samples_dir=samples, outdir=outdir)

    # Assert outputs exist
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()

    # Sanity: summary must include our project and Pxx columns
    summ = pd.read_parquet(outdir / "monte_carlo_summary.parquet")
    assert "ProjectID" in summ.columns and "EAC_P80" in summ.columns
    assert (summ["ProjectID"] == "P1").any()
