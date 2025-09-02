# tests/test_monte_carlo_proc_branch.py
# -----------------------------------------------------------------
# Purpose:
#   Cover the branch in etl/monte_carlo.py where procurement.csv
#   EXISTS (previous smoke test likely covered the fallback path).
#
# What this tests:
#   - Writes all 3 outputs with a tiny run (iters=10)
#   - Ensures S-curve generation and summary happen with proc data
# -----------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import pandas as pd
from etl.monte_carlo import run


def _write_minimal_inputs_with_proc(root: Path) -> tuple[Path, Path, Path]:
    """
    Create:
      - processed/evm_timeseries.parquet  (baseline EAC/BAC)
      - samples/risk_register.csv         (PERT-like inputs)
      - samples/procurement.csv           (so we hit the 'exists' branch)
    """
    processed = root / "data" / "processed"
    samples = root / "data" / "samples"
    processed.mkdir(parents=True, exist_ok=True)
    samples.mkdir(parents=True, exist_ok=True)

    # Baseline EVM
    evm = pd.DataFrame(
        {
            "ProjectID": ["PX"],
            "WBS": ["W1"],
            "Period": pd.to_datetime(["2025-01-01"]),
            "EAC": [1_000_000.0],
            "BAC": [950_000.0],
        }
    )
    evm.to_parquet(processed / "evm_timeseries.parquet", index=False)

    # Minimal risk register
    pd.DataFrame(
        {
            "RiskID": [1, 2],
            "Probability": [0.8, 0.4],
            "CostLow": [1000, 500],
            "CostML": [3000, 1000],
            "CostHigh": [7000, 2000],
            "SchedDaysLow": [1, 0],
            "SchedDaysML": [5, 2],
            "SchedDaysHigh": [10, 6],
        }
    ).to_csv(samples / "risk_register.csv", index=False)

    # Procurement delays present -> triggers the “exists” code path
    pd.DataFrame(
        {
            "Item": ["Valve", "Cable"],
            "PlannedDelivery": pd.to_datetime(["2025-02-01", "2025-02-05"]),
            "ActualDelivery": pd.to_datetime(["2025-02-10", "2025-02-12"]),
        }
    ).to_csv(samples / "procurement.csv", index=False)

    return processed, samples, processed  # outdir=processed for convenience


def test_monte_carlo_with_procurement_branch(tmp_path: Path) -> None:
    processed, samples, outdir = _write_minimal_inputs_with_proc(tmp_path)

    # Tiny run to keep it fast, but still generate all outputs
    run(iters=10, seed=123, processed_dir=processed, samples_dir=samples, outdir=outdir)

    # All expected files exist
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()

    # And they’re non-empty
    import pandas as pd

    assert len(pd.read_parquet(outdir / "monte_carlo_runs.parquet")) > 0
    assert len(pd.read_parquet(outdir / "monte_carlo_summary.parquet")) > 0
    assert len(pd.read_parquet(outdir / "forecast_s_curves.parquet")) > 0
