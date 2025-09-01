
import argparse
import json
import time
from pathlib import Path

import pandas as pd

def generate_stub(project_id: str, evm_path: Path, mc_path: Path):
    evm = pd.read_parquet(evm_path)
    mc = pd.read_parquet(mc_path)
    last = evm[evm["ProjectID"]==project_id].sort_values("Period").tail(1).iloc[0].to_dict()
    summ = mc[mc["ProjectID"]==project_id].iloc[0].to_dict()
    return {
        "level": "project",
        "id": project_id,
        "kpis": {"CPI": float(last.get("CPI",1)), "SPI": float(last.get("SPI",1)),
                 "EAC": float(last.get("EAC",0)), "VAC": float(last.get("VAC",0)),
                 "P80_EAC": float(summ.get("EAC_P80",0)), "P80_FinishDate": None},
        "summary": f"CPI {last.get('CPI',1):.2f}, SPI {last.get('SPI',1):.2f}. P50 ${summ.get('EAC_P50',0):,.0f}, P80 ${summ.get('EAC_P80',0):,.0f}.",
        "root_causes": ["Procurement delays", "Productivity variance"],
        "recommendations": ["Expedite critical POs", "Reallocate resources"],
        "confidence": 0.8,
        "contributors": [{"name":"Top drivers","type":"risk","impact_dollars": float(summ.get('EAC_P80',0)-summ.get('EAC_P50',0)), "impact_days": float(summ.get('Finish_P80',0)-summ.get('Finish_P50',0))}]
    }

def main(project_id: str, processed_dir: Path):
    evm_fp = processed_dir / "evm_timeseries.parquet"
    mc_fp = processed_dir / "monte_carlo_summary.parquet"
    out_jsonl = processed_dir / "variance_narratives.jsonl"
    n = generate_stub(project_id, evm_fp, mc_fp)
    with open(out_jsonl, "a", encoding="utf-8") as f:
        f.write(json.dumps(n) + "\n")
    # cost log stub
    with open(processed_dir / "llm_cost_log.csv", "a", encoding="utf-8") as f:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{ts},stub,none,0,0,0\n")
    print(f"[ai_variance_narratives] Appended narrative to {out_jsonl}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--project_id", default="DEMO")
    ap.add_argument("--processed", default="data/processed")
    args = ap.parse_args()
    main(args.project_id, Path(args.processed))
