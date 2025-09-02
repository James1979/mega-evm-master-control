# services/alerts.py
"""
Alert generation service.

- Loads EVM KPIs and Monte Carlo summary from parquet files.
- Emits breach alerts when KPI thresholds are violated.
- Always emits one Monte Carlo summary alert per project and NEVER includes a 'trigger' key on summaries.
- Writes a JSON list to data/processed/alerts_outbox.json.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import yaml


# ----------------------------
# Utilities
# ----------------------------
def _safe_float(x: Any) -> float | None:
    """Best-effort float coercion for JSON serialization; returns None if not coercible."""
    try:
        return float(x)
    except Exception:
        return None


def _first_of(d: Dict[str, Any], candidates: List[str]) -> Any:
    """
    Return the first non-None value for the provided keys from a mapping.
    """
    for k in candidates:
        if k in d and d[k] is not None:
            return d[k]
    return None


# ----------------------------
# Config loading
# ----------------------------
def load_cfg(fp: str) -> dict:
    """Load YAML config; return empty dict if the file is empty or missing keys."""
    with open(fp, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ----------------------------
# Alert builders
# ----------------------------
def _build_breach_alert(row: pd.Series, proj_id: str, wbs: str, triggers: List[str]) -> Dict:
    """
    Build an alert for KPI breaches. Breach alerts DO carry a 'trigger'.
    """
    return {
        "ts": datetime.utcnow().isoformat(),
        "project_id": proj_id,
        "wbs": wbs,
        "trigger": "|".join(triggers),  # present only on breach alerts
        "kpis": {
            "CPI": _safe_float(row.get("CPI")),
            "SPI": _safe_float(row.get("SPI")),
            "EAC": _safe_float(row.get("EAC")),
            "VAC": _safe_float(row.get("VAC")),
            "BAC": _safe_float(row.get("BAC")),
        },
        "narrative": f"EVM thresholds breached for {proj_id}/{wbs}",
        "recommendations": ["Escalate to PM", "Review critical path", "Accelerate POs"],
    }


def _build_summary_alert(proj_id: str, mc_row: Dict[str, Any]) -> Dict:
    """
    Build a Monte Carlo summary alert. NEVER include 'trigger' here.
    Normalize KPI names (prefer 'EAC_P80', but accept legacy 'P80_EAC' seamlessly).
    Always include Finish_P50 and Finish_P80 keys (may be None).
    """
    eac_p50 = _first_of(mc_row, ["EAC_P50"])
    eac_p80 = _first_of(mc_row, ["EAC_P80", "P80_EAC"])
    finish_p50 = _first_of(mc_row, ["Finish_P50"])
    finish_p80 = _first_of(mc_row, ["Finish_P80"])

    return {
        "ts": datetime.utcnow().isoformat(),
        "project_id": str(proj_id),   # <- use group key, not the row dict (which lacks ProjectID)
        "wbs": None,
        # no 'trigger'
        "kpis": {
            "EAC_P50": _safe_float(eac_p50),
            "EAC_P80": _safe_float(eac_p80),
            "Finish_P50": _safe_float(finish_p50),
            "Finish_P80": _safe_float(finish_p80),
        },
        "narrative": "Monte Carlo summary for executive view",
        "recommendations": ["Review contingency"],
    }


def build_alerts(evm: pd.DataFrame, mc: pd.DataFrame, cfg: dict) -> List[Dict]:
    """
    Generate breach and summary alerts.

    - Breach: CPI < cpi_red, SPI < spi_red, or VAC < 0 on the latest row per (ProjectID, WBS).
    - Summary: one MC summary per ProjectID (even if there are breaches).
    """
    th = (cfg or {}).get("thresholds", {})
    cpi_red = th.get("cpi_red", 0.9)
    spi_red = th.get("spi_red", 0.85)

    alerts: List[Dict] = []

    # --- BREACH DETECTION (latest EVM row per ProjectID/WBS) ---
    for (proj_id, wbs), grp in evm.groupby(["ProjectID", "WBS"]):
        row = grp.sort_values("Period").tail(1).iloc[0]
        triggers: List[str] = []

        cpi = row.get("CPI")
        spi = row.get("SPI")
        vac = row.get("VAC")

        if pd.notna(cpi) and cpi < cpi_red:
            triggers.append(f"CPI<{cpi_red:.2f}")
        if pd.notna(spi) and spi < spi_red:
            triggers.append(f"SPI<{spi_red:.2f}")
        if pd.notna(vac) and vac < 0:
            triggers.append("VAC<0")

        if triggers:
            alerts.append(_build_breach_alert(row, proj_id, wbs, triggers))

    # --- MC SUMMARY (first row per ProjectID) ---
    for proj_id, mc_row in mc.groupby("ProjectID").first().iterrows():
        alerts.append(_build_summary_alert(str(proj_id), dict(mc_row)))

    return alerts


# ----------------------------
# Main entrypoint
# ----------------------------
def main(cfg_fp: str, processed_dir: str, dry_run: bool = True) -> None:
    """
    Read parquet inputs, build alerts, and write JSON outbox.
    """
    cfg = load_cfg(cfg_fp)
    processed = Path(processed_dir)

    evm = pd.read_parquet(processed / "evm_timeseries.parquet")
    mc = pd.read_parquet(processed / "monte_carlo_summary.parquet")

    alerts = build_alerts(evm, mc, cfg)

    out_fp = processed / "alerts_outbox.json"
    out_fp.write_text(json.dumps(alerts, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[alerts] Wrote {len(alerts)} alerts to {out_fp}. dry_run={dry_run}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--processed", default="data/processed")
    parser.add_argument("--prod", action="store_true")
    args = parser.parse_args()
    main(args.config, args.processed, dry_run=(not args.prod))
