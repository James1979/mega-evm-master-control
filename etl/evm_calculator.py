
import pandas as pd, numpy as np
from pathlib import Path
import argparse

def compute_metrics(row):
    PV, EV, AC, BAC = row["PV"], row["EV"], row["AC"], row["BAC"]
    CPI = (EV / AC) if AC else np.nan
    SPI = (EV / PV) if PV else np.nan
    CV = EV - AC
    SV = EV - PV
    EAC = (BAC / CPI) if CPI and CPI!=0 else np.nan
    VAC = BAC - EAC if pd.notna(EAC) else np.nan
    return pd.Series(dict(PV=PV, EV=EV, AC=AC, BAC=BAC, CPI=CPI, SPI=SPI, CV=CV, SV=SV, EAC=EAC, VAC=VAC))

def main(samples_dir: Path, processed_dir: Path):
    schedule = pd.read_parquet(processed_dir / "schedule.parquet")
    cost = pd.read_csv(Path(samples_dir) / "cost_erp.csv")
    ev = schedule.groupby(["ProjectID","WBS"], as_index=False).agg({"PercentComplete":"mean","BAC":"sum"})
    ev["EV"] = ev["BAC"] * ev["PercentComplete"]
    pv = ev.copy(); pv["PV"] = ev["BAC"] * 0.75
    cost["Period"] = pd.to_datetime(cost["Period"] + "-01")
    ac = cost.groupby(["ProjectID","WBS","Period"], as_index=False).agg({"ActualCost":"sum","Budget":"sum"})
    base = ac.merge(pv[["ProjectID","WBS","PV"]], on=["ProjectID","WBS"], how="left").merge(ev[["ProjectID","WBS","EV","BAC"]], on=["ProjectID","WBS"], how="left")
    base = base.rename(columns={"ActualCost":"AC"})
    metrics = base.apply(compute_metrics, axis=1)
    out = pd.concat([base[["ProjectID","WBS","Period"]], metrics], axis=1)
    out_fp = Path(processed_dir) / "evm_timeseries.parquet"
    out.to_parquet(out_fp, index=False)
    print(f"[evm_calculator] Wrote {out_fp} rows={len(out)}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", default="data/samples")
    ap.add_argument("--out", default="data/processed")
    args = ap.parse_args()
    main(Path(args.samples), Path(args.out))
