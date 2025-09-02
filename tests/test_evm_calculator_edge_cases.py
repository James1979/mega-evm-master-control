import io
from pathlib import Path
import pandas as pd
import pytest

from etl.evm_calculator import (
    compute_metrics,
    _read_inputs,
    _write_output,
    _compute_metrics_timeseries,
)

def test_compute_metrics_type_error_raises():
    # Triggers the TypeError branch in compute_metrics(...)
    with pytest.raises(TypeError):
        compute_metrics(123)  # neither dict nor DataFrames

def test_timeseries_missing_schedule_columns_raises():
    # Missing required columns in schedule_df to hit ValueError branch
    schedule_df = pd.DataFrame({"ProjectID": ["P1"], "WBS": ["W1"]})  # missing PercentComplete, BAC
    cost_df = pd.DataFrame({
        "ProjectID": ["P1"],
        "WBS": ["W1"],
        "Period": ["2025-01"],
        "ActualCost": [100.0],
        "Budget": [120.0],
    })
    with pytest.raises(ValueError):
        _compute_metrics_timeseries(schedule_df, cost_df)

def test_timeseries_missing_cost_columns_raises():
    # Missing required columns in cost_df to hit the other ValueError branch
    schedule_df = pd.DataFrame({
        "ProjectID": ["P1"],
        "WBS": ["W1"],
        "PercentComplete": [0.5],
        "BAC": [1000.0],
    })
    cost_df = pd.DataFrame({"ProjectID": ["P1"], "WBS": ["W1"]})  # missing Period/ActualCost/Budget
    with pytest.raises(ValueError):
        _compute_metrics_timeseries(schedule_df, cost_df)

def test_read_inputs_missing_files_raise(tmp_path: Path):
    # No files present → hits FileNotFoundError for schedule_activities.csv
    with pytest.raises(FileNotFoundError):
        _read_inputs(tmp_path)

    # Create only schedule file so we fall through to the second FileNotFoundError
    (tmp_path / "schedule_activities.csv").write_text("ProjectID,WBS,PercentComplete,BAC\n", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        _read_inputs(tmp_path)

def test_write_output_creates_dir_and_writes_parquet(tmp_path: Path):
    df = pd.DataFrame({
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
    })
    out_fp = _write_output(df, tmp_path / "nested" / "processed")
    assert out_fp.exists()
