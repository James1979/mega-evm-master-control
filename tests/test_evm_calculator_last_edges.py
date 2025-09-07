# tests/test_evm_calculator_last_edges.py
# -----------------------------------------------------------------------------
# What this covers:
# - PV = 0 and AC = 0 on the latest period → CPI/SPI guards execute.
# - BAC = NaN on the latest period → EAC/VAC fallbacks execute.
# - Multiple periods to ensure "take latest" logic still returns valid numbers.
# -----------------------------------------------------------------------------

from __future__ import annotations

import numpy as np
import pandas as pd

from etl.evm_calculator import compute_metrics


def test_compute_metrics_zero_pv_ac_and_nan_bac_on_latest() -> None:
    # Schedule: two rows for the same WBS; latest has BAC = NaN to hit fallback
    schedule_df = pd.DataFrame(
        {
            "ProjectID": ["Z1", "Z1"],
            "WBS": ["W1", "W1"],
            "PercentComplete": [0.4, 0.6],
            "BAC": [10_000.0, np.nan],  # <- latest BAC NaN (forces fallback)
        }
    )

    # Cost: two periods; the latest period has zeros to trigger CPI/SPI guards
    cost_df = pd.DataFrame(
        {
            "ProjectID": ["Z1", "Z1"],
            "WBS": ["W1", "W1"],
            "Period": ["2025-01", "2025-02"],  # latest is 2025-02
            "ActualCost": [2_000.0, 0.0],  # AC=0 on latest
            "Budget": [2_500.0, 0.0],  # PV=0 on latest
        }
    )

    out = compute_metrics(schedule_df, cost_df)
    assert not out.empty

    # The latest row (2025-02) should have finite, numeric KPI values after guards
    last = out.sort_values("Period").iloc[-1]
    # Columns that must exist
    for col in ["PV", "EV", "AC", "CPI", "SPI", "EAC", "VAC", "CV", "SV"]:
        assert col in out.columns

    # Ensure numeric & not NaN after guard logic
    for col in ["CPI", "SPI", "EAC", "VAC", "CV", "SV"]:
        assert pd.notna(last[col]), f"{col} should be finite on latest row"
        assert isinstance(float(last[col]), float)
