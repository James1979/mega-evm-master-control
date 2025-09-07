import json
from pathlib import Path

import pandas as pd

from services.alerts import main as alerts_main


def test_alerts_empty_cfg_and_safe_float_and_first_of_none(tmp_path: Path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # EVM: no breaches (good KPIs)
    evm = pd.DataFrame(
        [
            {
                "ProjectID": "Q1",
                "WBS": "W1",
                "Period": "2025-01-01",
                "CPI": 1.01,
                "SPI": 1.02,
                "VAC": 100.0,
                "BAC": 1000.0,
                "EAC": 900.0,
            }
        ]
    )
    (processed / "evm_timeseries.parquet").write_bytes(evm.to_parquet(index=False) or b"")

    # MC: Use legacy P80_EAC and make both EAC values non-numeric strings to hit _safe_float -> None.
    # Omit Finish_* keys so _first_of(...) returns None for those candidates.
    mc = pd.DataFrame([{"ProjectID": "Q1", "EAC_P50": "not-a-number", "P80_EAC": "still-not-a-number"}])
    (processed / "monte_carlo_summary.parquet").write_bytes(mc.to_parquet(index=False) or b"")

    # Empty YAML → yaml.safe_load returns None → load_cfg(...) falls back to {}
    cfg = tmp_path / "config.yaml"
    cfg.write_text("", encoding="utf-8")

    alerts_main(str(cfg), str(processed), dry_run=True)

    data = json.loads((processed / "alerts_outbox.json").read_text(encoding="utf-8"))
    assert isinstance(data, list) and len(data) == 1
    item = data[0]

    # No 'trigger' on summary
    assert "trigger" not in item
    assert item["project_id"] == "Q1"

    # kpis should include normalized keys; values None because _safe_float failed to coerce strings
    kpis = item["kpis"]
    assert "EAC_P50" in kpis and kpis["EAC_P50"] is None
    assert "EAC_P80" in kpis and kpis["EAC_P80"] is None

    # Finish_* absent in input → _first_of returns None → keys still present with None
    assert "Finish_P50" in kpis and kpis["Finish_P50"] is None
    assert "Finish_P80" in kpis and kpis["Finish_P80"] is None
