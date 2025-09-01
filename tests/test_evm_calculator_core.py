"""
tests/test_evm_calculator_core.py

Goal:
- Exercise the primary path of compute_metrics() with small, realistic inputs.
- Verify EVM math relationships and required columns exist.

What this test covers:
- CPI ≈ EV / AC, SPI ≈ EV / PV
- VAC = BAC - EAC
- Output shape and essential columns
"""

import math
import pandas as pd

# Import the functions under test.
# If your module exposes different names, adjust these imports.
from etl.evm_calculator import compute_metrics


def _schedule_df() -> pd.DataFrame:
    """
    Build a tiny schedule dataframe that looks like real inputs.
    Columns:
      - ProjectID, WBS, PercentComplete, BAC
    """
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P2"],
            "WBS": ["W1", "W2", "W1"],
            "PercentComplete": [0.50, 0.25, 0.40],
            "BAC": [1000.0, 2000.0, 500.0],
        }
    )


def _cost_df() -> pd.DataFrame:
    """
    Build a tiny cost dataframe across two periods.
    Columns:
      - ProjectID, WBS, Period (YYYY-MM), ActualCost, Budget
    """
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P1", "P2"],
            "WBS": ["W1", "W2", "W2", "W1"],
            "Period": ["2025-01", "2025-01", "2025-02", "2025-01"],
            "ActualCost": [400.0, 150.0, 250.0, 120.0],
            "Budget": [500.0, 100.0, 200.0, 150.0],
        }
    )


def test_compute_metrics_core_kpis() -> None:
    """
    Act:
      - Feed small schedule/cost tables to compute_metrics().
    Assert:
      - Output is non-empty and has basic EVM columns.
      - Latest-period CPI/SPI follow EV/AC and EV/PV definitions.
      - VAC = BAC - EAC (within float tolerance).
    """
    out = compute_metrics(_schedule_df(), _cost_df())

    # Basic sanity
    assert not out.empty
    for col in ["ProjectID", "WBS", "Period", "EV", "PV", "AC", "BAC", "CPI", "SPI", "EAC", "VAC"]:
        assert col in out.columns, f"Expected column {col} in compute_metrics output"

    # For each (ProjectID, WBS), take the latest Period row and validate KPI math
    latest = (
        out.assign(Period=pd.to_datetime(out["Period"], errors="coerce"))
           .sort_values("Period")
           .groupby(["ProjectID", "WBS"], as_index=False)
           .tail(1)
    )

    for _, row in latest.iterrows():
        ev = float(row["EV"])
        ac = float(row["AC"]) if row["AC"] not in (None, "") else 0.0
        pv = float(row["PV"]) if row["PV"] not in (None, "") else 0.0
        bac = float(row["BAC"])
        eac = float(row["EAC"])
        vac = float(row["VAC"])

        # Guard against divide-by-zero in assertions
        if ac > 0:
            assert math.isclose(float(row["CPI"]), ev / ac, rel_tol=1e-6, abs_tol=1e-9)
        if pv > 0:
            assert math.isclose(float(row["SPI"]), ev / pv, rel_tol=1e-6, abs_tol=1e-9)

        # Expect standard definition VAC = BAC - EAC (some pipelines may redefine — keep tolerance loose)
        assert math.isclose(vac, bac - eac, rel_tol=1e-6, abs_tol=1e-6)
