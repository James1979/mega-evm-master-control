from pathlib import Path
import pandas as pd

import etl.monte_carlo as mc  # import module to monkeypatch inside it

from etl.monte_carlo import run

def _write_evm_with_nan_eac(processed_dir: Path):
    df = pd.DataFrame({
        "ProjectID": ["PX"],
        "WBS": ["WX"],
        "Period": ["2025-01"],
        "EV": [50.0],
        "PV": [60.0],
        "AC": [55.0],
        "BAC": [100.0],
        "CPI": [0.909],
        "SPI": [0.833],
        "EAC": [float("nan")],   # forces fallback to BAC
        "VAC": [-5.0],
        "CV": [-5.0],
        "SV": [-10.0],
    })
    processed_dir.mkdir(parents=True, exist_ok=True)
    (processed_dir / "evm_timeseries.parquet").write_bytes(df.to_parquet(index=False) or b"")

def _write_risks(samples_dir: Path):
    risks = pd.DataFrame({
        "RiskID": [1],
        "Probability": [0.5],
        "CostLow": [1000],
        "CostML": [1500],
        "CostHigh": [2000],
        "SchedDaysLow": [1],
        "SchedDaysML": [2],
        "SchedDaysHigh": [3],
    })
    samples_dir.mkdir(parents=True, exist_ok=True)
    (samples_dir / "risk_register.csv").write_text(risks.to_csv(index=False), encoding="utf-8")

def test_mc_fallback_eac_to_bac_and_empty_procurement(tmp_path: Path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    samples = tmp_path / "data" / "samples"
    outdir = tmp_path / "out_mc_full"

    _write_evm_with_nan_eac(processed)
    _write_risks(samples)

    # Create an empty procurement.csv file so the exists() check is True
    samples.mkdir(parents=True, exist_ok=True)
    (samples / "procurement.csv").write_text("PlannedDelivery,ActualDelivery\n", encoding="utf-8")

    # Monkeypatch pd.read_csv inside etl.monte_carlo so that ONLY the procurement file
    # returns an *empty* DataFrame with datetime64 dtypes, making `.dt.days` valid.
    orig_read_csv = mc.pd.read_csv

    def patched_read_csv(path, *args, **kwargs):
        path = Path(path)
        if path.name == "procurement.csv":
            return pd.DataFrame({
                "PlannedDelivery": pd.to_datetime([], errors="coerce"),
                "ActualDelivery": pd.to_datetime([], errors="coerce"),
            })
        return orig_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(mc.pd, "read_csv", patched_read_csv)

    run(iters=16, seed=7, processed_dir=str(processed), samples_dir=str(samples), outdir=str(outdir))

    # Outputs exist → both branches executed (EAC fallback + empty procurement)
    assert (outdir / "monte_carlo_runs.parquet").exists()
    assert (outdir / "monte_carlo_summary.parquet").exists()
    assert (outdir / "forecast_s_curves.parquet").exists()
