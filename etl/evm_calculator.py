# etl/evm_calculator.py
# -----------------------------------------------------------------------------
# Purpose:
#   Core EVM math + a simple CLI to read inputs from data/samples and write a
#   timeseries parquet to data/processed. This makes it usable in CI, tests, and
#   demo runs.
#
# Backward-compatibility:
#   Some legacy tests call compute_metrics(row_dict). To support both use cases,
#   this module exposes a *polymorphic* compute_metrics() that:
#     - If called with a single dict: returns single-row KPIs (CPI/SPI/EAC/etc.)
#     - If called with two DataFrames: returns time-phased EVM over periods
#
# What this module provides:
#   - compute_metrics_row(row): one-row helper (dict → dict)
#   - compute_metrics(...): polymorphic API (dict) OR (DataFrame, DataFrame)
#   - __main__ CLI: read CSVs → compute → write data/processed/evm_timeseries.parquet
# -----------------------------------------------------------------------------

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Tuple, Union

import pandas as pd


# -----------------------------------------------------------------------------
# Helper: safe division that returns NaN instead of raising on zero
# -----------------------------------------------------------------------------
def _safediv(numer: Union[pd.Series, float], denom: Union[pd.Series, float]) -> pd.Series:
    """
    Safe divide that returns NaN when denominator is 0 or NaN.

    Why:
    - Real project data often has 0 PV/AC early in a period; we prefer NaN over crash.
    """
    return pd.Series(numer, dtype="float64") / pd.Series(denom, dtype="float64")


# -----------------------------------------------------------------------------
# Public API (dict → dict): single-row EVM calculator (used by tests and docs)
# -----------------------------------------------------------------------------
def compute_metrics_row(row: Dict[str, Any]) -> Dict[str, float]:
    """
    Given a single row dict with PV, EV, AC, BAC, compute classic EVM KPIs.

    Expected keys:
      - PV: Planned Value
      - EV: Earned Value
      - AC: Actual Cost
      - BAC: Budget At Completion

    Returns:
      dict with CPI, SPI, EAC, VAC, CV, SV (and echoes inputs for convenience)
    """
    pv = float(row.get("PV", 0.0) or 0.0)
    ev = float(row.get("EV", 0.0) or 0.0)
    ac = float(row.get("AC", 0.0) or 0.0)
    bac = float(row.get("BAC", 0.0) or 0.0)

    # Classic EVM KPIs (with safe denominators handled by simple checks)
    cpi = (ev / ac) if ac else float("nan")
    spi = (ev / pv) if pv else float("nan")
    eac = ac + (bac - ev)          # Transparent EAC = AC + ETC, with ETC ≈ (BAC - EV)
    vac = bac - eac
    cv = ev - ac
    sv = ev - pv

    return {
        "PV": pv, "EV": ev, "AC": ac, "BAC": bac,
        "CPI": cpi, "SPI": spi, "EAC": eac, "VAC": vac, "CV": cv, "SV": sv,
    }


# -----------------------------------------------------------------------------
# Public API (DataFrame, DataFrame → DataFrame): vectorized EVM over time
# -----------------------------------------------------------------------------
def _compute_metrics_timeseries(schedule_df: pd.DataFrame, cost_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute time-phased EVM metrics by (ProjectID, WBS, Period).

    Parameters
    ----------
    schedule_df :
        Columns: ProjectID, WBS, PercentComplete, BAC
    cost_df :
        Columns: ProjectID, WBS, Period (YYYY-MM), ActualCost, Budget

    Returns
    -------
    pd.DataFrame with columns:
        ProjectID, WBS, Period, EV, PV, AC, BAC, CPI, SPI, EAC, VAC, CV, SV
    """
    # --- 1) Normalize inputs and coerce numerics (handles messy CSVs) ----------
    sched = schedule_df.copy()
    cost = cost_df.copy()

    # Coerce numeric columns; blank/None become NaN then treated as 0 by groupby sum
    for col in ["PercentComplete", "BAC"]:
        if col in sched.columns:
            sched[col] = pd.to_numeric(sched[col], errors="coerce")
    for col in ["ActualCost", "Budget"]:
        if col in cost.columns:
            cost[col] = pd.to_numeric(cost[col], errors="coerce")

    # Enforce required ID columns
    required_sched = {"ProjectID", "WBS", "PercentComplete", "BAC"}
    required_cost = {"ProjectID", "WBS", "Period", "ActualCost", "Budget"}
    if not required_sched.issubset(set(sched.columns)):
        missing = required_sched - set(sched.columns)
        raise ValueError(f"schedule_df missing columns: {sorted(missing)}")
    if not required_cost.issubset(set(cost.columns)):
        missing = required_cost - set(cost.columns)
        raise ValueError(f"cost_df missing columns: {sorted(missing)}")

    # --- 2) EV/BAC per WBS -----------------------------------------------------
    # EV is independent of Period in this simple view; we’ll join it later to each Period row.
    ev_wbs = (
        sched.groupby(["ProjectID", "WBS"], as_index=False)
        .agg({"PercentComplete": "mean", "BAC": "sum"})
        .rename(columns={"PercentComplete": "Pct"})
    )
    ev_wbs["EV"] = ev_wbs["BAC"] * ev_wbs["Pct"]

    # --- 3) PV & AC per Period -------------------------------------------------
    # Aggregate Budget (PV) and ActualCost (AC) per period.
    cost["Period"] = cost["Period"].astype(str)
    pv_ac = (
        cost.groupby(["ProjectID", "WBS", "Period"], as_index=False)
        .agg({"Budget": "sum", "ActualCost": "sum"})
        .rename(columns={"Budget": "PV", "ActualCost": "AC"})
    )

    # --- 4) Join EV/BAC into time-phased table --------------------------------
    out = pv_ac.merge(ev_wbs, on=["ProjectID", "WBS"], how="left")

    # --- 5) KPIs: CPI, SPI, EAC, VAC, CV, SV ----------------------------------
    out["CPI"] = _safediv(out["EV"], out["AC"])
    out["SPI"] = _safediv(out["EV"], out["PV"])
    out["EAC"] = out["AC"] + (out["BAC"] - out["EV"])
    out["VAC"] = out["BAC"] - out["EAC"]
    out["CV"] = out["EV"] - out["AC"]
    out["SV"] = out["EV"] - out["PV"]

    # Ensure column order is friendly and deterministic
    cols = [
        "ProjectID", "WBS", "Period",
        "EV", "PV", "AC", "BAC",
        "CPI", "SPI", "EAC", "VAC", "CV", "SV",
    ]
    return out.loc[:, cols]


# -----------------------------------------------------------------------------
# Polymorphic public API: compute_metrics(...)
# -----------------------------------------------------------------------------
def compute_metrics(  # type: ignore[override]
    a: Union[Dict[str, Any], pd.DataFrame],
    b: Union[pd.DataFrame, None] = None,
) -> Union[Dict[str, float], pd.DataFrame]:
    """
    Backward-compatible *dispatcher*:

    - If called with a single dict: behaves like compute_metrics_row(row_dict).
    - If called with (schedule_df, cost_df): returns time-phased DataFrame.

    Examples
    --------
    >>> compute_metrics({"PV": 100, "EV": 90, "AC": 110, "BAC": 300})
    {'PV': 100.0, 'EV': 90.0, 'AC': 110.0, 'BAC': 300.0, 'CPI': 0.818..., ...}

    >>> compute_metrics(schedule_df, cost_df)
    DataFrame([...])
    """
    # Case 1: legacy test path — single dict
    if isinstance(a, dict) and b is None:
        return compute_metrics_row(a)

    # Case 2: full vectorized path — two DataFrames
    if isinstance(a, pd.DataFrame) and isinstance(b, pd.DataFrame):
        return _compute_metrics_timeseries(a, b)

    # Anything else: explicit error helps developers
    raise TypeError(
        "compute_metrics expects either (row_dict) or (schedule_df, cost_df) DataFrames."
    )


# -----------------------------------------------------------------------------
# CLI helpers: read/write, so tests and CI can invoke `python -m etl.evm_calculator`
# -----------------------------------------------------------------------------
def _read_inputs(samples_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read required CSVs from the samples directory.

    Expected files:
      - schedule_activities.csv
      - cost_erp.csv
    """
    sched_fp = samples_dir / "schedule_activities.csv"
    cost_fp = samples_dir / "cost_erp.csv"
    if not sched_fp.exists():
        raise FileNotFoundError(f"Missing {sched_fp}")
    if not cost_fp.exists():
        raise FileNotFoundError(f"Missing {cost_fp}")

    schedule_df = pd.read_csv(sched_fp)
    cost_df = pd.read_csv(cost_fp)
    return schedule_df, cost_df


def _write_output(df: pd.DataFrame, out_dir: Path) -> Path:
    """
    Write the EVM timeseries parquet to the processed directory.
    Returns the output path for convenience.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_fp = out_dir / "evm_timeseries.parquet"
    # Use to_parquet; if engine is missing, users can install pyarrow/fastparquet.
    df.to_parquet(out_fp, index=False)
    return out_fp


# -----------------------------------------------------------------------------
# __main__: enables `python -m etl.evm_calculator`
# -----------------------------------------------------------------------------
def _build_argparser() -> argparse.ArgumentParser:
    """
    Define CLI for local/demo/CI usage.

    Examples:
      python -m etl.evm_calculator
      python -m etl.evm_calculator --samples data/samples --processed data/processed
    """
    ap = argparse.ArgumentParser(description="Compute EVM KPIs and write a parquet timeseries.")
    ap.add_argument(
        "--samples",
        default="data/samples",
        help="Directory containing schedule_activities.csv and cost_erp.csv (default: data/samples)",
    )
    ap.add_argument(
        "--processed",
        default="data/processed",
        help="Output directory for evm_timeseries.parquet (default: data/processed)",
    )
    return ap


def main(samples: str, processed: str) -> Path:
    """
    Orchestrates the CLI:
      1) Read inputs from --samples
      2) Compute EVM KPIs
      3) Write to --processed/evm_timeseries.parquet
    Returns the path to the parquet for convenience.
    """
    samples_dir = Path(samples)
    processed_dir = Path(processed)

    schedule_df, cost_df = _read_inputs(samples_dir)
    evm = _compute_metrics_timeseries(schedule_df, cost_df)
    out_fp = _write_output(evm, processed_dir)
    print(f"[evm_calculator] wrote {out_fp}")
    return out_fp


if __name__ == "__main__":
    parser = _build_argparser()
    args = parser.parse_args()
    main(args.samples, args.processed)
