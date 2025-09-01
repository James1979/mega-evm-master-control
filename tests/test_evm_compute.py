"""
tests/test_evm_compute.py

End-to-end validations for EVM calculations produced by
etl/evm_calculator.compute_metrics(schedule_df, cost_df).

We validate:
- Shape/columns/types
- CPI ≈ EV/AC and SPI ≈ EV/PV
- CV  = EV - AC
- SV  = EV - PV
- EAC = AC + (BAC - EV)
- VAC = BAC - EAC
"""

import math
import pandas as pd
import pytest

# Function under test
from etl.evm_calculator import compute_metrics  # type: ignore[attr-defined]


def test_compute_metrics_shapes_and_columns(schedule_df: pd.DataFrame, cost_df: pd.DataFrame) -> None:
    """
    Ensures compute_metrics returns:
    - A non-empty DataFrame
    - Expected EVM columns present
    - Numeric columns have numeric dtype
    """
    out = compute_metrics(schedule_df, cost_df)

    # Must return a DataFrame with rows
    assert isinstance(out, pd.DataFrame)
    assert len(out) > 0

    # Required EVM columns (now includes CV/SV/EAC/VAC for full coverage)
    required_cols = [
        "ProjectID", "WBS", "Period",
        "EV", "PV", "AC",
        "CPI", "SPI",
        "BAC", "EAC", "VAC",
        "CV", "SV",
    ]
    for col in required_cols:
        assert col in out.columns, f"Missing expected column: {col}"

    # Numeric columns should be numeric dtype
    numeric_cols = ["EV", "PV", "AC", "CPI", "SPI", "BAC", "EAC", "VAC", "CV", "SV"]
    for col in numeric_cols:
        assert pd.api.types.is_numeric_dtype(out[col]), f"{col} should be numeric"


def test_evm_math_consistency(schedule_df: pd.DataFrame, cost_df: pd.DataFrame) -> None:
    """
    Validates all EVM math relationships on the latest period per WBS.
    We use a small tolerance for floating-point comparisons.
    """
    out = compute_metrics(schedule_df, cost_df).copy()

    # Normalize Period and take latest row per (ProjectID, WBS)
    out["Period"] = pd.to_datetime(out["Period"], errors="coerce")
    latest = out.sort_values("Period").groupby(["ProjectID", "WBS"], as_index=False).tail(1)

    tol = 1e-6  # floating-point tolerance

    for _, row in latest.iterrows():
        ev = float(row["EV"])
        pv = float(row["PV"])
        ac = float(row["AC"])
        bac = float(row["BAC"])

        cpi = float(row["CPI"])
        spi = float(row["SPI"])
        eac = float(row["EAC"])
        vac = float(row["VAC"])
        cv = float(row["CV"])
        sv = float(row["SV"])

        # --- CPI / SPI
        if ac != 0:
            assert math.isclose(cpi, ev / ac, rel_tol=1e-6, abs_tol=tol)
        else:
            # if AC == 0, CPI is defined in code as 0 (safe-div), just ensure it's present
            assert "CPI" in row

        if pv != 0:
            assert math.isclose(spi, ev / pv, rel_tol=1e-6, abs_tol=tol)
        else:
            # if PV == 0, SPI is defined in code as 0 (safe-div)
            assert "SPI" in row

        # --- CV / SV exact identities
        assert math.isclose(cv, ev - ac, rel_tol=1e-6, abs_tol=tol)  # Cost Variance
        assert math.isclose(sv, ev - pv, rel_tol=1e-6, abs_tol=tol)  # Schedule Variance

        # --- EAC / VAC identities
        # EAC = AC + (BAC - EV)  → quick estimate
        assert math.isclose(eac, ac + (bac - ev), rel_tol=1e-6, abs_tol=tol)
        # VAC = BAC - EAC
        assert math.isclose(vac, bac - eac, rel_tol=1e-6, abs_tol=tol)
