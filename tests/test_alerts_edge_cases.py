import json
from pathlib import Path

import pandas as pd

from services.alerts import build_alerts
from services.alerts import main as alerts_main


def _make_evm_df(rows):
    return pd.DataFrame(rows)


def _make_mc_df(rows):
    return pd.DataFrame(rows)


def test_breach_alert_includes_trigger(tmp_path: Path):
    # Latest row breaches CPI and SPI; VAC negative
    evm = _make_evm_df(
        [
            {
                "ProjectID": "P1",
                "WBS": "W1",
                "Period": "2025-01-01",
                "CPI": 0.88,
                "SPI": 0.80,
                "VAC": -1.0,
                "BAC": 100.0,
                "EAC": 120.0,
            },
            {
                "ProjectID": "P1",
                "WBS": "W1",
                "Period": "2025-02-01",
                "CPI": 0.85,
                "SPI": 0.82,
                "VAC": -2.0,
                "BAC": 100.0,
                "EAC": 125.0,
            },
        ]
    )
    # Minimal MC (not used for breach checks but present)
    mc = _make_mc_df([{"ProjectID": "P1", "EAC_P50": 110.0, "EAC_P80": 130.0, "Finish_P50": 10.0, "Finish_P80": 12.0}])

    cfg = {"thresholds": {"cpi_red": 0.90, "spi_red": 0.85}}
    alerts = build_alerts(evm, mc, cfg)

    # Expect 2 items: 1 breach + 1 summary
    assert len(alerts) == 2

    breach = next(a for a in alerts if a.get("trigger"))
    assert "CPI<0.90" in breach["trigger"]
    assert "SPI<0.85" in breach["trigger"]
    assert "VAC<0" in breach["trigger"]
    assert breach["kpis"]["BAC"] == 100.0
    assert breach["kpis"]["EAC"] == 125.0


def test_summary_alert_normalizes_legacy_key_and_writes_file(tmp_path: Path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True, exist_ok=True)

    # EVM with no breaches (good KPIs)
    evm = _make_evm_df(
        [
            {
                "ProjectID": "G1",
                "WBS": "W1",
                "Period": "2025-01-01",
                "CPI": 1.05,
                "SPI": 1.02,
                "VAC": 5000.0,
                "BAC": 95000.0,
                "EAC": 100000.0,
            }
        ]
    )
    evm_fp = processed / "evm_timeseries.parquet"
    evm_fp.write_bytes(evm.to_parquet(index=False) or b"")

    # MC with legacy P80_EAC key to exercise normalization to EAC_P80
    mc = _make_mc_df(
        [{"ProjectID": "G1", "EAC_P50": 98000.0, "P80_EAC": 105000.0, "Finish_P50": 10.0, "Finish_P80": 12.0}]
    )
    mc_fp = processed / "monte_carlo_summary.parquet"
    mc_fp.write_bytes(mc.to_parquet(index=False) or b"")

    # Minimal config file
    cfg = tmp_path / "config.yaml"
    cfg.write_text("thresholds:\n  cpi_red: 0.90\n  spi_red: 0.85\n", encoding="utf-8")

    # Run main() to also cover file I/O/write path
    alerts_main(str(cfg), str(processed), dry_run=True)

    out_fp = processed / "alerts_outbox.json"
    data = json.loads(out_fp.read_text(encoding="utf-8"))

    # We expect 1 item (summary) because there are no breaches in the test setup
    # (Our implementation writes one summary per project; in this setup it's one)
    assert isinstance(data, list)
    assert len(data) == 1

    item = data[0]
    assert "trigger" not in item
    assert item["project_id"] == "G1"
    # Normalized KPI key must be present
    assert "EAC_P80" in item["kpis"]
    assert item["kpis"]["EAC_P80"] == 105000.0
    # Finish metrics included
    assert "Finish_P50" in item["kpis"]
    assert "Finish_P80" in item["kpis"]
