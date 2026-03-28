"""Composite score construction for multi-factor stock ranking."""

import pandas as pd


def combine_factor_scores(factor_data: pd.DataFrame) -> pd.DataFrame:
    """Combine preprocessed factor exposures into an equal-weight score."""
    if factor_data.empty:
        return pd.DataFrame(columns=["date", "ticker", "score"])

    working = factor_data.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working["ticker"] = working["ticker"].astype("string").str.upper().str.strip()
    working["value"] = pd.to_numeric(working["value"], errors="coerce")
    working = working.dropna(subset=["date", "ticker", "value"])

    scores = (
        working.groupby(["date", "ticker"], as_index=False)["value"]
        .mean()
        .rename(columns={"value": "score"})
        .sort_values(["date", "score", "ticker"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    return scores
