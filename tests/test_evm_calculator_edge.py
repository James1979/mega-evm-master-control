"""
tests/test_evm_calculator_edge_more.py

Goal:
- Hit edge branches in compute_metrics() that deal with:
  * non-numeric strings in Budget/ActualCost (CSV messiness)
  * missing PV/AC fallbacks and safe-divide code paths

What this tests:
- Function does not crash when numeric columns come in as strings/blank.
- PV/AC get coerced to numbers; CPI/SPI still computed or safely handled.
"""

import math
import pandas as pd
from etl.evm_calculator import compute_metrics


def test_compute_metrics_coerces_stringy_numbers_and_handles_missing() -> None:
    """
    Arrange:
      - Schedule with normal BAC/PercentComplete.
      - Cost with stringy numbers, blanks, and None to trigger to_numeric coercion.
    Act:
      - Run compute_metrics.
    Assert:
      - Output exists and contains expected columns.
      - The latest row has finite (or NaN, but not crash) CPI/SPI; VAC equals BAC - EAC when defined.
    """
    schedule = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "PercentComplete": [0.60, 0.30],
            "BAC": [1000.0, 2000.0],
        }
    )

    # Note the intentionally messy data:
    # - "400" and "100" as strings (need coercion)
    # - "" and None that should coerce to NaN -> treated as zeros in aggregations
    cost = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P1"],
            "WBS": ["W1", "W2", "W2"],
            "Period": ["2025-01", "2025-01", "2025-02"],
            "ActualCost": ["400", "", None],   # coercion path
            "Budget": ["500", "100", ""],      # coercion path
        }
    )

    out = compute_metrics(schedule, cost)
    assert not out.empty
    for col in ["EV", "PV", "AC", "BAC", "CPI", "SPI", "EAC", "VAC", "Period"]:
        assert col in out.columns

    # Validate KPI math on most recent row per WBS
    latest = (
        out.assign(Period=pd.to_datetime(out["Period"], errors="coerce"))
           .sort_values("Period")
           .groupby(["ProjectID", "WBS"], as_index=False)
           .tail(1)
    )

    for _, row in latest.iterrows():
        ev = float(row["EV"])
        bac = float(row["BAC"])
        eac = float(row["EAC"])
        vac = float(row["VAC"])

        # If denominators exist, ensure CPI/SPI relationships hold
        ac = float(row["AC"]) if pd.notna(row["AC"]) else 0.0
        pv = float(row["PV"]) if pd.notna(row["PV"]) else 0.0

        if ac > 0:
            assert math.isclose(float(row["CPI"]), ev / ac, rel_tol=1e-6, abs_tol=1e-9)
        if pv > 0:
            assert math.isclose(float(row["SPI"]), ev / pv, rel_tol=1e-6, abs_tol=1e-9)

        # VAC definition (within tolerance)
        assert math.isclose(vac, bac - eac, rel_tol=1e-6, abs_tol=1e-6)
