
import argparse, json
from pathlib import Path
import pandas as pd
import yaml
from datetime import datetime

def load_cfg(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def build_alerts(evm: pd.DataFrame, mc: pd.DataFrame, cfg: dict):
    th = cfg.get("thresholds", {})
    alerts = []
    for (proj, wbs), grp in evm.groupby(["ProjectID","WBS"]):
        row = grp.sort_values("Period").tail(1).iloc[0]
        cpi = row.get("CPI"); spi = row.get("SPI"); vac = row.get("VAC"); bac=row.get("BAC"); eac=row.get("EAC")
        trig = []
        if pd.notna(cpi) and cpi < th.get("cpi_red", 0.9): trig.append("CPI<0.90")
        if pd.notna(spi) and spi < th.get("spi_red", 0.85): trig.append("SPI<0.85")
        if pd.notna(vac) and vac < 0: trig.append("VAC<0")
        if trig:
            alerts.append({
                "ts": datetime.utcnow().isoformat(),
                "project_id": proj, "wbs": wbs,
                "trigger": "|".join(trig),
                "kpis": {"CPI": float(cpi), "SPI": float(spi), "EAC": float(eac), "VAC": float(vac), "BAC": float(bac)},
                "narrative": f"EVM thresholds breached for {proj}/{wbs}",
                "recommendations": ["Escalate to PM","Review critical path","Accelerate POs"]
            })
    # P80 overrun at project level
    for proj, row in mc.groupby("ProjectID").first().iterrows():
        alerts.append({
            "ts": datetime.utcnow().isoformat(),
            "project_id": proj, "wbs": None, "trigger": "P80 summary",
            "kpis": {"P80_EAC": float(row["EAC_P80"]), "EAC_P50": float(row["EAC_P50"])},
            "narrative": "Monte Carlo summary for executive view",
            "recommendations": ["Review contingency"]
        })
    return alerts

def main(cfg_fp: str, processed_dir: str, dry_run: bool=True):
    cfg = load_cfg(cfg_fp)
    evm = pd.read_parquet(Path(processed_dir) / "evm_timeseries.parquet")
    mc = pd.read_parquet(Path(processed_dir) / "monte_carlo_summary.parquet")
    alerts = build_alerts(evm, mc, cfg)
    out_fp = Path(processed_dir) / "alerts_outbox.json"
    existing = []
    if out_fp.exists():
        try:
            existing = json.loads(out_fp.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    existing.extend(alerts)
    out_fp.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    print(f"[alerts] Wrote {len(alerts)} alerts to {out_fp}. dry_run={dry_run}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--processed", default="data/processed")
    ap.add_argument("--prod", action="store_true")
    args = ap.parse_args()
    main(args.config, args.processed, dry_run=(not args.prod))
