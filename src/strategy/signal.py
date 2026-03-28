"""Signal generation helpers based on composite factor scores."""

import pandas as pd


def select_top_names(score_data: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Select the highest-ranked names on each rebalance date."""
    if score_data.empty:
        return pd.DataFrame(columns=["rebalance_date", "ticker", "score"])

    if top_n <= 0:
        raise ValueError("top_n must be a positive integer")

    scores = score_data.copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce")
    scores["ticker"] = scores["ticker"].astype("string").str.upper().str.strip()
    scores["score"] = pd.to_numeric(scores["score"], errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "score"])

    ranked = scores.sort_values(["date", "score", "ticker"], ascending=[True, False, True]).copy()
    ranked["rank"] = ranked.groupby("date")["score"].rank(method="first", ascending=False)
    selected = ranked[ranked["rank"] <= top_n].copy()
    selected = selected.rename(columns={"date": "rebalance_date"})
    return selected[["rebalance_date", "ticker", "score"]].reset_index(drop=True)
