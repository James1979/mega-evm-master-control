"""
Smoke + output assertion for etl.p6_ingest (CLI)

What this test does:
- Creates the exact input file the CLI expects at data/samples/schedule_activities.csv
  with the date columns p6_ingest parses.
- Runs `etl.p6_ingest` as if `python -m etl.p6_ingest`.
- Asserts the CLI writes data/processed/schedule.parquet.
- (Light) Validates row count and a couple of key columns exist.
"""

import runpy
import sys

import pandas as pd


def test_p6_ingest_cli_runs_and_writes_parquet(tmp_path, monkeypatch):
    """
    Arrange:
      - Create temp repo root with `data/samples/schedule_activities.csv`
    Act:
      - chdir into temp root
      - reset sys.argv so argparse in the module doesn’t see pytest flags
      - run `etl.p6_ingest` as __main__
    Assert:
      - `data/processed/schedule.parquet` is written
      - parquet has expected rows and columns
    """
    # 1) Prepare input in the path p6_ingest expects
    samples_dir = tmp_path / "data" / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    csv_fp = samples_dir / "schedule_activities.csv"
    pd.DataFrame(
        {
            "ActivityID": [1001, 1002],
            "Name": ["Mobilization", "Civil works"],
            # p6_ingest parses these as dates via parse_dates=[...]
            "BaselineStart": ["2025-01-01", "2025-02-01"],
            "BaselineFinish": ["2025-01-10", "2025-02-20"],
            "Start": ["2025-01-02", "2025-02-02"],
            "Finish": ["2025-01-12", "2025-03-15"],
        }
    ).to_csv(csv_fp, index=False)

    # 2) Run from the temp root so relative defaults resolve:
    #    --samples=data/samples  --out=data/processed
    monkeypatch.chdir(tmp_path)

    # 3) Critical: scrub pytest flags so argparse inside the module doesn’t choke
    monkeypatch.setattr(sys, "argv", ["etl.p6_ingest"])

    # 4) Execute like: python -m etl.p6_ingest
    runpy.run_module("etl.p6_ingest", run_name="__main__")

    # 5) Assert the expected output parquet exists
    out_fp = tmp_path / "data" / "processed" / "schedule.parquet"
    assert out_fp.exists(), "p6_ingest should write data/processed/schedule.parquet"

    # 6) (Light) Validate the parquet content
    out_df = pd.read_parquet(out_fp)
    assert len(out_df) == 2, "output should preserve row count from input"
    for col in ["ActivityID", "Name", "BaselineStart", "BaselineFinish", "Start", "Finish"]:
        assert col in out_df.columns, f"missing expected column: {col}"

    # Optional: sanity check a parsed datetime column actually came through
    # (pandas writes datetimes to parquet as datetime64[ns])
    assert str(out_df["Start"].dtype).startswith("datetime64"), "Start should be datetime64 after ingest"
