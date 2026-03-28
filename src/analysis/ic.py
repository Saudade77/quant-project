"""Information coefficient analysis utilities."""

import pandas as pd


def _normalize_forward_returns(forward_returns: pd.DataFrame) -> pd.DataFrame:
    normalized = forward_returns.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["ticker"] = normalized["ticker"].astype("string").str.upper().str.strip()

    if "forward_return" not in normalized.columns:
        if "return" in normalized.columns:
            normalized = normalized.rename(columns={"return": "forward_return"})
        else:
            raise ValueError("forward_returns must contain 'forward_return' or 'return'")

    normalized["forward_return"] = pd.to_numeric(normalized["forward_return"], errors="coerce")
    return normalized.dropna(subset=["date", "ticker", "forward_return"])


def compute_ic_series(factor_data: pd.DataFrame, forward_returns: pd.DataFrame) -> pd.DataFrame:
    """Compute daily Pearson IC and Spearman-style RankIC by factor."""
    result_columns = ["date", "factor", "ic", "rank_ic", "n"]
    if factor_data.empty or forward_returns.empty:
        return pd.DataFrame(columns=result_columns)

    factors = factor_data.copy()
    factors["date"] = pd.to_datetime(factors["date"], errors="coerce")
    factors["ticker"] = factors["ticker"].astype("string").str.upper().str.strip()
    factors["factor"] = factors["factor"].astype("string").str.lower().str.strip()
    factors["value"] = pd.to_numeric(factors["value"], errors="coerce")
    factors = factors.dropna(subset=["date", "ticker", "factor", "value"])

    returns = _normalize_forward_returns(forward_returns)
    merged = factors.merge(returns, on=["date", "ticker"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=result_columns)

    rows: list[dict[str, float | int | pd.Timestamp | str]] = []
    for (date, factor_name), group in merged.groupby(["date", "factor"], sort=True):
        group = group.dropna(subset=["value", "forward_return"])
        if len(group) < 2:
            ic = 0.0
            rank_ic = 0.0
        else:
            ic = float(group["value"].corr(group["forward_return"], method="pearson"))
            rank_ic = float(group["value"].rank().corr(group["forward_return"].rank(), method="pearson"))
            ic = 0.0 if pd.isna(ic) else ic
            rank_ic = 0.0 if pd.isna(rank_ic) else rank_ic

        rows.append(
            {
                "date": date,
                "factor": factor_name,
                "ic": ic,
                "rank_ic": rank_ic,
                "n": int(len(group)),
            }
        )

    return pd.DataFrame(rows, columns=result_columns)
