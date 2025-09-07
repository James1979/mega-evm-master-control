from pathlib import Path

import pandas as pd

from services.evm_metrics import compute_kpis


def test_p50_like_eac_demo():
    df = pd.read_csv(Path("data/samples/evm_sample.csv"))
    out = compute_kpis(df)
    last = out.iloc[-1]

    # Ground truth from the sample
    ev, ac, bac = 260000.0, 295000.0, 300000.0

    # Expected via the same logic as the function (CPI ~ 0.88136/0.8814 across envs)
    expected_cpi = round(ev / ac, 4)  # 0.8814 in many envs
    expected_eac = round(bac / expected_cpi, 2)  # ~340379.64

    # CPI should be very close to EV/AC rounded to 4 places
    assert abs(float(last["CPI"]) - expected_cpi) < 1e-4

    # Allow small numeric drift in EAC due to rounding modes
    assert abs(float(last["EAC_cpi"]) - expected_eac) <= 20.0  # within $20 tolerance
