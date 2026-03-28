"""Grouped return analysis for factor validation."""

import pandas as pd


def compute_group_returns(score_data: pd.DataFrame, forward_returns: pd.DataFrame, groups: int = 5) -> pd.DataFrame:
    """Compute cross-sectional grouped forward returns from factor scores."""
    result_columns = ["date", "group", "group_return", "group_count", "long_short_return"]
    if score_data.empty or forward_returns.empty:
        return pd.DataFrame(columns=result_columns)

    if groups < 2:
        raise ValueError("groups must be at least 2")

    scores = score_data.copy()
    scores["date"] = pd.to_datetime(scores["date"], errors="coerce")
    scores["ticker"] = scores["ticker"].astype("string").str.upper().str.strip()
    scores["score"] = pd.to_numeric(scores["score"], errors="coerce")
    scores = scores.dropna(subset=["date", "ticker", "score"])

    returns = forward_returns.copy()
    returns["date"] = pd.to_datetime(returns["date"], errors="coerce")
    returns["ticker"] = returns["ticker"].astype("string").str.upper().str.strip()
    if "forward_return" not in returns.columns:
        if "return" in returns.columns:
            returns = returns.rename(columns={"return": "forward_return"})
        else:
            raise ValueError("forward_returns must contain 'forward_return' or 'return'")
    returns["forward_return"] = pd.to_numeric(returns["forward_return"], errors="coerce")
    returns = returns.dropna(subset=["date", "ticker", "forward_return"])

    merged = scores.merge(returns, on=["date", "ticker"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=result_columns)

    rows: list[dict[str, float | int | pd.Timestamp]] = []
    for date, group in merged.groupby("date", sort=True):
        ordered = group.sort_values(["score", "ticker"], ascending=[False, True]).reset_index(drop=True)
        if len(ordered) < groups:
            continue

        ranks = ordered["score"].rank(method="first", ascending=False, pct=True)
        ordered["group"] = ((ranks * groups).clip(upper=groups) - 1e-12).astype(int) + 1

        grouped = (
            ordered.groupby("group", as_index=False)
            .agg(group_return=("forward_return", "mean"), group_count=("ticker", "count"))
            .sort_values("group")
        )
        long_short_return = float(
            grouped.loc[grouped["group"] == 1, "group_return"].iloc[0]
            - grouped.loc[grouped["group"] == groups, "group_return"].iloc[0]
        )

        for _, row in grouped.iterrows():
            rows.append(
                {
                    "date": date,
                    "group": int(row["group"]),
                    "group_return": float(row["group_return"]),
                    "group_count": int(row["group_count"]),
                    "long_short_return": long_short_return,
                }
            )

    return pd.DataFrame(rows, columns=result_columns)
