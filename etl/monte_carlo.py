# etl/monte_carlo.py
# -----------------------------------------------------------------------------
# Purpose:
#   Run a lightweight Monte Carlo simulation to produce P10/P50/P80 forecasts
#   for EAC (Estimate at Completion) and "FinishDaysOverBaseline".
#
# What it reads:
#   - data/processed/evm_timeseries.parquet   (baseline EAC/BAC computed by ETL)
#   - data/samples/risk_register.csv          (PERT/triangular-like parameters)
#   - data/samples/procurement.csv            (derive delivery-delay variability)
#
# What it writes:
#   - data/processed/monte_carlo_runs.parquet
#   - data/processed/monte_carlo_summary.parquet
#   - data/processed/forecast_s_curves.parquet
#
# Notes:
#   - This version FIXES the crash by making `pert()` support vector inputs
#     (Series/arrays) and by coercing CSV columns to numeric.
#   - It uses NumPy broadcasting to sample (iters x n_risks) matrices.
# -----------------------------------------------------------------------------

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def pert(low, mode, high, size, rng):
    """
    Vectorized PERT sampler.

    Accepts arrays/Series for low/mode/high (length = n_risks). Produces a matrix
    of samples with shape = `size` (e.g., (iters, n_risks)), using Beta distribution
    parameters derived from the PERT method.

    Parameters
    ----------
    low, mode, high : array-like or scalar
        Distribution parameters per risk. Will be coerced to float arrays.
    size : tuple
        Desired output shape, typically (iters, n_risks).
    rng : numpy.random.Generator
        Random generator (for determinism via seed).

    Returns
    -------
    np.ndarray
        Samples with shape = size.
    """
    # Coerce to numeric float arrays (handles strings/blank cells from CSV)
    low = np.asarray(pd.to_numeric(low, errors="coerce"), dtype=float)
    mode = np.asarray(pd.to_numeric(mode, errors="coerce"), dtype=float)
    high = np.asarray(pd.to_numeric(high, errors="coerce"), dtype=float)

    # Ensure high > low to avoid division by zero; nudge ties by epsilon
    eps = 1e-9
    high = np.where(high <= low, low + eps, high)

    # Classic PERT → Beta with parameters a,b computed from (low, mode, high)
    a = 1.0 + 4.0 * (mode - low) / (high - low)
    b = 1.0 + 4.0 * (high - mode) / (high - low)

    # Keep parameters strictly positive
    a = np.clip(a, eps, None)
    b = np.clip(b, eps, None)

    # rng.beta broadcasts (a,b) across the requested size
    beta_samples = rng.beta(a, b, size=size)

    # Scale Beta(0..1) back to [low, high] per risk
    return low + (high - low) * beta_samples


def run(iters, seed, processed_dir, samples_dir, outdir):
    """
    Main Monte Carlo driver.

    Steps:
      1) Read EVM (to get baseline EAC/BAC).
      2) Read Risk Register and coerce numeric columns.
      3) Read Procurement and derive simple delay distribution.
      4) For each ProjectID:
           - Sample risk cost and schedule days via PERT + probability gates.
           - Sample procurement delay days.
           - Convert schedule days → $ delta using a simple $/day factor.
           - Compute EAC distribution and FinishDaysOverBaseline per iteration.
      5) Save iteration runs, summary percentiles, and EAC CDF S-curve points.
    """
    rng = np.random.default_rng(seed)

    processed_dir = Path(processed_dir)
    samples_dir = Path(samples_dir)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # ---- 1) Baseline EVM ----
    evm = pd.read_parquet(processed_dir / "evm_timeseries.parquet")

    # Derive a baseline per project (use mean of latest EAC/BAC as a simple proxy)
    base = (
        evm.groupby("ProjectID", as_index=False)
        .agg({"EAC": "mean", "BAC": "mean"})
        .rename(columns={"EAC": "EAC_base"})
    )

    # ---- 2) Risk Register (coerce numerics) ----
    risks = pd.read_csv(samples_dir / "risk_register.csv")
    num_cols = [
        "Probability",
        "CostLow",
        "CostML",
        "CostHigh",
        "SchedDaysLow",
        "SchedDaysML",
        "SchedDaysHigh",
    ]
    for c in num_cols:
        if c in risks.columns:
            risks[c] = pd.to_numeric(risks[c], errors="coerce").fillna(0.0)
        else:
            # If a column is missing, add zeros so code still runs
            risks[c] = 0.0

    # ---- 3) Procurement delays → a simple delay-days distribution ----
    if (samples_dir / "procurement.csv").exists():
        proc = pd.read_csv(
            samples_dir / "procurement.csv",
            parse_dates=["PlannedDelivery", "ActualDelivery"],
        )
        proc["DelayDays"] = (proc["ActualDelivery"] - proc["PlannedDelivery"]).dt.days
        proc["DelayDays"] = proc["DelayDays"].fillna(0).astype(float)

        # Build a rough triangular-ish PERT from observed delays:
        # low = 0; mode ≈ mean delay (at least 1 if we have delays); high ≈ max delay (at least 2)
        pd_low = 0.0
        pd_ml = float(max(1.0, proc["DelayDays"].mean())) if len(proc) else 1.0
        pd_high = float(max(2.0, proc["DelayDays"].max())) if len(proc) else 15.0
    else:
        # Fallback defaults if no procurement file
        proc = pd.DataFrame()
        pd_low, pd_ml, pd_high = 0.0, 5.0, 15.0

    results = []

    # ---- 4) Simulate per project ----
    for proj in base["ProjectID"].unique():
        # Baselines
        eb = base.loc[base["ProjectID"] == proj, "EAC_base"].mean()
        bac = base.loc[base["ProjectID"] == proj, "BAC"].mean()
        if pd.isna(eb) or eb == 0:
            eb = bac  # if EAC not computed yet, use BAC as a baseline proxy

        # Copy risks (you might filter by WBS/project here in a real system)
        rc = risks.copy()
        n = len(rc)

        # Probability gates (True/False per iter × risk)
        prob = rc["Probability"].values
        gates = rng.random((iters, n)) <= prob

        # Sample cost and schedule-day impacts per risk (iters × n_risks)
        cost = pert(rc["CostLow"], rc["CostML"], rc["CostHigh"], (iters, n), rng)
        days = pert(
            rc["SchedDaysLow"], rc["SchedDaysML"], rc["SchedDaysHigh"], (iters, n), rng
        )

        # Sum impacts across risks per iteration
        cost_imp = (cost * gates).sum(axis=1)  # shape: (iters,)
        days_imp = (days * gates).sum(axis=1)  # shape: (iters,)

        # Sample procurement delay per iteration (shape: (iters,))
        pdays = pert(pd_low, pd_ml, pd_high, iters, rng)

        # Convert days to dollars via a simple rate (configurable in a full system)
        DAY_TO_DOLLARS = 15000.0
        eac_delta_from_days = (days_imp + pdays) * DAY_TO_DOLLARS

        # EAC distribution per iteration
        EAC = eb + cost_imp + eac_delta_from_days

        # Finish-days-over-baseline per iteration (toy proxy)
        finish_days = days_imp + pdays

        df = pd.DataFrame(
            {"ProjectID": proj, "EAC": EAC, "FinishDaysOverBaseline": finish_days}
        )
        results.append(df)

    # All-iteration runs
    runs = pd.concat(results, ignore_index=True)

    # ---- 5) Summaries & S-curves ----
    def pct(s, p):
        return float(np.percentile(s, p))

    summary = (
        runs.groupby("ProjectID", as_index=False)
        .agg(
            EAC_P10=("EAC", lambda s: pct(s, 10)),
            EAC_P50=("EAC", lambda s: pct(s, 50)),
            EAC_P80=("EAC", lambda s: pct(s, 80)),
            Finish_P10=("FinishDaysOverBaseline", lambda s: pct(s, 10)),
            Finish_P50=("FinishDaysOverBaseline", lambda s: pct(s, 50)),
            Finish_P80=("FinishDaysOverBaseline", lambda s: pct(s, 80)),
        )
        .reset_index(drop=True)
    )

    # EAC CDF points for S-curve plots (Power BI / Streamlit)
    sc_rows = []
    for proj, grp in runs.groupby("ProjectID"):
        xs = np.linspace(grp["EAC"].min(), grp["EAC"].max(), 100)
        eac_vals = grp["EAC"].to_numpy()
        for x in xs:
            sc_rows.append(
                {
                    "ProjectID": proj,
                    "Metric": "EAC",
                    "Value": float(x),
                    "CDF": float((eac_vals <= x).mean()),
                }
            )
    s_curve = pd.DataFrame(sc_rows)

    # ---- Write outputs ----
    (outdir / "monte_carlo_runs.parquet").write_bytes(
        runs.to_parquet(index=False) or b""
    )
    (outdir / "monte_carlo_summary.parquet").write_bytes(
        summary.to_parquet(index=False) or b""
    )
    (outdir / "forecast_s_curves.parquet").write_bytes(
        s_curve.to_parquet(index=False) or b""
    )

    print(f"[monte_carlo] Wrote outputs in {outdir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iters", type=int, default=5000, help="Iterations to run")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--processed", default="data/processed", help="Processed dir")
    parser.add_argument("--samples", default="data/samples", help="Samples dir")
    parser.add_argument("--outdir", default="data/processed", help="Output dir")
    args = parser.parse_args()

    run(args.iters, args.seed, args.processed, args.samples, args.outdir)
