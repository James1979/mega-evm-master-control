"""
tests/conftest.py

Shared pytest fixtures for sample schedule and cost data.
These let us test EVM calculations with small, hard-coded datasets
without requiring real project files.
"""
# --- Add this block so `import services...` works in tests & CI ---
import sys
from pathlib import Path
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
# -----------------------------------------------------------------

import pandas as pd
import pytest


@pytest.fixture
def schedule_df() -> pd.DataFrame:
    """
    Provides a minimal project schedule DataFrame.

    Columns:
    - ProjectID: project identifier
    - WBS: work breakdown structure ID
    - PercentComplete: completion % in decimal (0.5 = 50%)
    - BAC: Budget at Completion for each WBS
    """
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "PercentComplete": [0.50, 0.20],  # 50% and 20% complete
            "BAC": [1000.0, 2000.0],         # $1000 for W1, $2000 for W2
        }
    )


@pytest.fixture
def cost_df() -> pd.DataFrame:
    """
    Provides a minimal project cost ledger DataFrame.

    Columns:
    - ProjectID: project identifier
    - WBS: work breakdown structure ID
    - Period: accounting period in YYYY-MM
    - ActualCost: cost incurred that period
    - Budget: planned budget that period
    """
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P1"],
            "WBS": ["W1", "W2", "W2"],
            "Period": ["2025-01", "2025-01", "2025-02"],
            "ActualCost": [400.0, 150.0, 250.0],
            "Budget": [500.0, 100.0, 200.0],
        }
    )
