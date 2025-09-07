from pathlib import Path

import numpy as np
import pandas as pd

from etl.monte_carlo import pert, run


def test_pert_handles_high_le_low_and_clipping():
    # high <= low triggers epsilon nudge + positive clipping for a,b
    rng = np.random.default_rng(0)
    low = np.array([1.0, 2.0])
    mode = np.array([1.0, 2.0])  # equal to low to push a=1.0+4*(0)/eps
    high = np.array([1.0, 2.0])  # equal to low/mode -> eps applied
    samples = pert(low, mode, high, size=(5, 2), rng=rng)
    # Should produce finite samples within [low, high + eps]
    assert np.isfinite(samples).all()
    assert (samples >= 1.0 - 1e-6).all()


def _write_minimal_evm(processed_dir: Path):
    # Minimal EVM parquet sufficient for MC run (projects P1)
    df = pd.DataFrame(
        {
            "ProjectID": ["P1"],
            "WBS": ["W1"],
            "Period": ["2025-01"],
            "EV": [50.0],
            "PV": [60.0],
            "AC": [55.0],
            "BAC": [100.0],
            "CPI": [0.909],
            "SPI": [0.833],
            "EAC": [105.0],
            "VAC": [-5.0],
            "CV": [-5.0],
            "SV": [-10.0],
        }
    )
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "evm_timeseries.parquet").write_bytes(df.to_parquet(index=False) or b"")


def _write_minimal_risk_register(samples_dir: Path):
    # Provide numeric columns to avoid coercion issues
    risks = pd.DataFrame(
        {
            "RiskID": [1, 2],
            "Probability": [0.5, 0.3],
            "CostLow": [1000, 500],
            "CostML": [2000, 1000],
            "CostHigh": [3000, 1500],
            "SchedDaysLow": [1, 0],
            "SchedDaysML": [3, 2],
            "SchedDaysHigh": [5, 4],
        }
    )
    samples_dir.mkdir(parents=True, exist_ok=True)
    (samples_dir / "risk_register.csv").write_text(risks.to_csv(index=False), encoding="utf-8")


def _write_procurement(samples_dir: Path):
    # A tiny procurement file so we exercise the "exists()" branch
    df = pd.DataFrame(
        {
            "PlannedDelivery": pd.to_datetime(["2025-01-01", "2025-01-10"]),
            "ActualDelivery": pd.to_datetime(["2025-01-05", "2025-01-14"]),
        }
    )
    (samples_dir / "procurement.csv").write_text(df.to_csv(index=False), encoding="utf-8")


def test_run_with_missing_procurement_uses_defaults(tmp_path: Path):
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    outdir = tmp_path / "out"

    _write_minimal_evm(processed)
    _write_minimal_risk_register(samples)
    # intentionally DO NOT write procurement.csv → hits the "else" default path

    run(iters=32, seed=123, processed_dir=str(processed), samples_dir=str(samples), outdir=str(outdir))

    # Ensure files are written
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()


def test_run_with_procurement_file_branch(tmp_path: Path):
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    outdir = tmp_path / "out2"

    _write_minimal_evm(processed)
    _write_minimal_risk_register(samples)
    _write_procurement(samples)  # now exercise the "if procurement exists" branch

    run(iters=32, seed=123, processed_dir=str(processed), samples_dir=str(samples), outdir=str(outdir))

    # Ensure files are written and not empty
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()
