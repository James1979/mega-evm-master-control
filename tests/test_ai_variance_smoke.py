# tests/test_ai_variance_smoke.py
# -------------------------------------------------------------------------------------------------
# Purpose:
#   Lightweight tests to raise coverage for services/ai_variance_narratives.py without being brittle.
#   We test:
#     1) generate_stub(...) directly (pure function path)
#     2) the CLI path (module __main__) which appends to variance_narratives.jsonl and cost log
#
# Why this helps:
#   - Executes realistic code paths (parquet I/O + summary text building).
#   - Keeps assertions simple (shape/type), so refactors won’t cause false failures.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

import pandas as pd
import pytest

# Import the module under test
import services.ai_variance_narratives as ain


def _write_minimal_processed(processed_dir: Path) -> None:
    """
    Create tiny parquet inputs that ai_variance_narratives expects:
      - evm_timeseries.parquet with columns used by generate_stub
      - monte_carlo_summary.parquet with the percentile fields
    """
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Minimal EVM timeseries: include two projects so filtering is exercised
    evm = pd.DataFrame(
        {
            "ProjectID": ["P1", "P1", "P2"],
            "WBS": ["W1", "W2", "W1"],
            "Period": pd.to_datetime(["2025-01-01", "2025-02-01", "2025-01-01"]),
            "CPI": [0.88, 0.92, 1.05],
            "SPI": [0.80, 0.97, 1.02],
            "EAC": [1_020_000.0, 1_070_000.0, 300_000.0],
            "VAC": [-20_000.0, -70_000.0, 10_000.0],
        }
    )
    evm.to_parquet(processed_dir / "evm_timeseries.parquet", index=False)

    # Minimal Monte Carlo summary: one row per project with P50/P80, Finish percentiles
    mc = pd.DataFrame(
        {
            "ProjectID": ["P1", "P2"],
            "EAC_P50": [1_000_000.0, 280_000.0],
            "EAC_P80": [1_080_000.0, 320_000.0],
            "Finish_P50": [10.0, 5.0],
            "Finish_P80": [25.0, 12.0],
        }
    )
    mc.to_parquet(processed_dir / "monte_carlo_summary.parquet", index=False)


def test_generate_stub_returns_expected_shape(tmp_path: Path) -> None:
    """
    Exercise the pure function path:
      - Write tiny parquet inputs
      - Call generate_stub("P1", evm_fp, mc_fp)
      - Assert we get a dict with expected keys/types and a non-empty summary
    """
    processed = tmp_path / "data" / "processed"
    _write_minimal_processed(processed)

    out = ain.generate_stub(
        project_id="P1",
        evm_path=processed / "evm_timeseries.parquet",
        mc_path=processed / "monte_carlo_summary.parquet",
    )

    # Basic shape checks
    assert isinstance(out, dict)
    for key in ["level", "id", "kpis", "summary", "root_causes", "recommendations", "confidence", "contributors"]:
        assert key in out

    # Sanity on a few fields
    assert out["level"] == "project"
    assert out["id"] == "P1"
    assert isinstance(out["kpis"], dict)
    assert isinstance(out["summary"], str) and out["summary"].strip() != ""
    assert isinstance(out["contributors"], list)
    # The contributor impact deltas should be float-able
    contrib = out["contributors"][0]
    assert "impact_dollars" in contrib and "impact_days" in contrib


def test_cli_appends_jsonl_and_cost_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Exercise the CLI (__main__) path:
      - Create minimal processed inputs
      - cd into the temp root
      - Reset sys.argv so argparse in __main__ sees only expected args
      - run the module with runpy
      - Assert JSONL + cost log files were created and contain valid content
    """
    root = tmp_path
    processed = root / "data" / "processed"
    _write_minimal_processed(processed)

    # Run from temp repo root so default relative paths work
    monkeypatch.chdir(root)

    # Critical: remove pytest flags; present only the args that __main__ expects
    #   python -m services.ai_variance_narratives --project_id P1 --processed data/processed
    monkeypatch.setattr(
        sys, "argv", ["services.ai_variance_narratives", "--project_id", "P1", "--processed", str(processed)]
    )

    # Execute the module as if via CLI
    runpy.run_module("services.ai_variance_narratives", run_name="__main__")

    # Verify the narrative JSONL file exists and is parseable
    jsonl_fp = processed / "variance_narratives.jsonl"
    assert jsonl_fp.exists(), "variance_narratives.jsonl should be created by the CLI"
    last_line = jsonl_fp.read_text(encoding="utf-8").strip().splitlines()[-1]
    record = json.loads(last_line)
    assert record["id"] == "P1"
    assert "summary" in record and isinstance(record["summary"], str)

    # Verify the cost log is created with at least one CSV line
    cost_fp = processed / "llm_cost_log.csv"
    assert cost_fp.exists(), "llm_cost_log.csv should be created by the CLI"
    assert cost_fp.read_text(encoding="utf-8").strip() != ""
