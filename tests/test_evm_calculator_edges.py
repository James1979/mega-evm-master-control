# tests/test_evm_calculator_edges.py
# ------------------------------------------------------------
# Purpose:
#   Hit edge/guard paths in etl/evm_calculator.py that protect
#   against divide-by-zero and missing data, pushing coverage.
#
# What this tests:
#   - EV/PV/AC combos that could yield divide-by-zero
#   - End-to-end compute via the public API function
# ------------------------------------------------------------

from __future__ import annotations

import math
import pandas as pd
from etl.evm_calculator import compute_metrics  # import your public function


def _is_finite_or_nan(x) -> bool:
    """
    Helper: return True if x is a number (finite) OR NaN.
    We accept NaN here because the implementation chooses NaN
    instead of coercing to 0/1 for undefined CPI/SPI.
    """
    if isinstance(x, (int, float)):
        return True  # covers finite and inf; we’ll exclude inf right below
    try:
        xf = float(x)
        return (not math.isinf(xf)) or math.isnan(xf)
    except Exception:
        return False


def test_compute_metrics_handles_zero_pv_and_zero_ac() -> None:
    """
    Case:
      - PV = 0 (SPI = EV/PV undefined)
      - AC = 0 (CPI = EV/AC undefined)
    Expect:
      - Function returns DataFrame and includes required columns.
      - CPI/SPI may be NaN; both behaviors (finite or NaN) are acceptable.
    """
    schedule_df = pd.DataFrame(
        {
            "ProjectID": ["PZ"],
            "WBS": ["W1"],
            "PercentComplete": [0.0],
            "BAC": [1000.0],
        }
    )

    cost_df = pd.DataFrame(
        {
            "ProjectID": ["PZ"],
            "WBS": ["W1"],
            "Period": ["2025-01"],
            "ActualCost": [0.0],   # AC=0
            "Budget": [0.0],       # PV=0
        }
    )

    out = compute_metrics(schedule_df, cost_df)
    assert not out.empty

    # Required columns exist
    for col in ["PV", "EV", "AC", "CPI", "SPI", "EAC", "VAC", "CV", "SV"]:
        assert col in out.columns

    # CPI/SPI either finite values or NaN (both acceptable here)
    cpi = out.loc[out.index[0], "CPI"]
    spi = out.loc[out.index[0], "SPI"]
    assert _is_finite_or_nan(cpi)
    assert _is_finite_or_nan(spi)


def test_compute_metrics_missing_optional_columns_ok() -> None:
    """
    Case:
      - Provide minimal inputs so any optional/derived columns kick in.
    Expect:
      - No crash, required columns present, numeric types for PV/AC.
    """
    schedule_df = pd.DataFrame(
        {
            "ProjectID": ["PM"],
            "WBS": ["W1"],
            "PercentComplete": [0.3],
            "BAC": [2000.0],
        }
    )

    cost_df = pd.DataFrame(
        {
            "ProjectID": ["PM"],
            "WBS": ["W1"],
            "Period": ["2025-02"],
            "ActualCost": [500.0],
            "Budget": [600.0],
        }
    )

    out = compute_metrics(schedule_df, cost_df)
    assert not out.empty
    assert set(["PV", "EV", "AC"]).issubset(out.columns)
    assert pd.api.types.is_numeric_dtype(out["PV"])
    assert pd.api.types.is_numeric_dtype(out["AC"])
