import argparse
from pathlib import Path

import pandas as pd


def main(samples_dir: Path, processed_dir: Path):
    df = pd.read_csv(Path(samples_dir) / "procurement.csv", parse_dates=["PlannedDelivery", "ActualDelivery"])
    df["DelayDays"] = (df["ActualDelivery"] - df["PlannedDelivery"]).dt.days.fillna(0).astype(int)
    df["DelayCost"] = df["DelayDays"].clip(lower=0) * (df["Qty"] * df["UnitCost"] * 0.001)
    impacts = df.groupby(["WBS", "Vendor"], as_index=False).agg({"DelayDays": "sum", "DelayCost": "sum"})
    out_fp = Path(processed_dir) / "procurement_impacts.parquet"
    impacts.to_parquet(out_fp, index=False)
    print(f"[procurement_join] Wrote {out_fp} rows={len(impacts)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", default="data/samples")
    ap.add_argument("--out", default="data/processed")
    args = ap.parse_args()
    main(Path(args.samples), Path(args.out))
