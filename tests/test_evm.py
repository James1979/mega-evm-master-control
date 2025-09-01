
from etl.evm_calculator import compute_metrics
def test_evm_example():
    row = {"PV": 400000.0, "EV": 350000.0, "AC": 420000.0, "BAC": 1000000.0}
    s = compute_metrics(row)
    assert round(s["CPI"],4) == round(350000/420000,4)
    assert round(s["SPI"],4) == round(350000/400000,4)
    assert s["CV"] == -70000.0
    assert s["SV"] == -50000.0
