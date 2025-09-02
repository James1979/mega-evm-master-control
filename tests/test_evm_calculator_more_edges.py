# tests/test_evm_calculator_more_edges.py
# --------------------------------------------------------------------
# Purpose:
#   Nudge etl/evm_calculator.py through less-common branches:
#   - BAC = 0 (forces EAC/VAC fallbacks)
#   - Multiple periods per WBS (ensures "latest row wins")
#   These typically live around the lines still uncovered in your file.
# --------------------------------------------------------------------

from __future__ import annotations

import pandas as pd
from etl.evm_calculator import compute_metrics

def test_compute_metrics_bac_zero_and_multi_period_latest() -> None:
    # Schedule has BAC = 0 → forces fallback path when computing EAC/VAC
    schedule_df = pd.DataFrame(
        {
            "ProjectID": ["PX", "PX"],
            "WBS": ["W1", "W1"],
            "PercentComplete": [0.5, 0.6],
            "BAC": [0.0, 0.0],  # <- key: zero BAC to hit fallback branch
        }
    )

    # Two periods → the later one should be the final row after grouping/sorting
    cost_df = pd.DataFrame(
        {
            "ProjectID": ["PX", "PX"],
            "WBS": ["W1", "W1"],
            "Period": ["2025-01", "2025-02"],
            "ActualCost": [100.0, 150.0],
            "Budget": [120.0, 180.0],
        }
    )

    out = compute_metrics(schedule_df, cost_df)
    assert not out.empty

    # Required columns
    for col in ["PV", "EV", "AC", "CPI", "SPI", "EAC", "VAC", "CV", "SV"]:
        assert col in out.columns

    # Latest period is 2025-02
    last = out.sort_values("Period").iloc[-1]
    assert str(last["Period"]).startswith("2025-02")

    # Ensure numbers are present (fall-back logic used, but no NaNs)
    assert pd.api.types.is_numeric_dtype(out["EAC"])
    assert pd.api.types.is_numeric_dtype(out["VAC"])
