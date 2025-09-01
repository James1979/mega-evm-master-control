
import pandas as pd
from pathlib import Path
import argparse

def main(samples_dir: Path, processed_dir: Path):
    schedule_fp = samples_dir / "schedule_activities.csv"
    df = pd.read_csv(schedule_fp, parse_dates=["BaselineStart","BaselineFinish","Start","Finish"])
    processed_dir.mkdir(parents=True, exist_ok=True)
    out = processed_dir / "schedule.parquet"
    df.to_parquet(out, index=False)
    print(f"[p6_ingest] Wrote {out} rows={len(df)}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", default="data/samples")
    ap.add_argument("--out", default="data/processed")
    args = ap.parse_args()
    main(Path(args.samples), Path(args.out))
