"""
tests/conftest.py

Shared pytest fixtures for sample schedule and cost data.
These let us test EVM calculations with small, hard-coded datasets
without requiring real project files.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# --- Add repo root so "import services" works in tests & CI ---
_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


@pytest.fixture
def schedule_df() -> pd.DataFrame:
    """Provides a minimal project schedule DataFrame."""
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1"],
            "WBS": ["W1", "W2"],
            "PercentComplete": [0.50, 0.20],
            "BAC": [1000.0, 2000.0],
        }
    )


@pytest.fixture
def cost_df() -> pd.DataFrame:
    """Provides a minimal project cost ledger DataFrame."""
    return pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P1"],
            "WBS": ["W1", "W2", "W2"],
            "Period": ["2025-01", "2025-01", "2025-02"],
            "ActualCost": [400.0, 150.0, 250.0],
            "Budget": [500.0, 100.0, 200.0],
        }
    )
