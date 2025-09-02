from pathlib import Path
import pandas as pd
from etl.monte_carlo import run

def _write_minimal_evm(processed_dir: Path):
    df = pd.DataFrame({
        "ProjectID": ["PZ"],
        "WBS": ["WZ"],
        "Period": ["2025-01"],
        "EV": [10.0],
        "PV": [12.0],
        "AC": [11.0],
        "BAC": [100.0],
        "CPI": [0.909],
        "SPI": [0.833],
        "EAC": [95.0],
        "VAC": [5.0],
        "CV": [-1.0],
        "SV": [-2.0],
    })
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "evm_timeseries.parquet").write_bytes(df.to_parquet(index=False) or b"")

def _write_risks_missing_cols(samples_dir: Path):
    # Deliberately omit some numeric columns (e.g., SchedDaysHigh, CostHigh)
    # so the loop in monte_carlo.py takes the `else: risks[c] = 0.0` branch.
    risks = pd.DataFrame({
        "RiskID": [1],
        "Probability": [0.4],      # present (will be coerced)
        "CostLow": [1000],         # present
        "CostML": [1500],          # present
        # "CostHigh" missing  -> should be filled with 0.0 in code
        "SchedDaysLow": [1],       # present
        "SchedDaysML": [2],        # present
        # "SchedDaysHigh" missing -> should be filled with 0.0 in code
    })
    samples_dir.mkdir(parents=True, exist_ok=True)
    (samples_dir / "risk_register.csv").write_text(risks.to_csv(index=False), encoding="utf-8")

def test_mc_handles_missing_risk_columns_defaults_to_zero(tmp_path: Path):
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    outdir = tmp_path / "out_mc_missing_cols"

    _write_minimal_evm(processed)
    _write_risks_missing_cols(samples)
    # No procurement.csv → takes the "missing file" branch (already covered, but fine)

    run(iters=8, seed=123, processed_dir=str(processed), samples_dir=str(samples), outdir=str(outdir))

    # If the missing-column defaults were applied correctly, the run completes and writes outputs
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()
