from __future__ import annotations
import pandas as pd

def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["CPI"] = (df["EV"] / df["AC"]).round(4)
    df["SPI"] = (df["EV"] / df["PV"]).round(4)
    # Simple EAC via CPI (demonstration): EAC = BAC / CPI
    df["BAC"] = df.groupby("ProjectID")["PV"].transform("max")
    df["EAC_cpi"] = (df["BAC"] / df["CPI"]).round(2)
    return df
