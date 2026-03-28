"""Factor preprocessing hooks such as winsorization and z-scoring."""

from collections.abc import Sequence

import pandas as pd

from src.factors.base import FACTOR_OUTPUT_COLUMNS


def _winsorize_series(series: pd.Series, lower_quantile: float = 0.01, upper_quantile: float = 0.99) -> pd.Series:
    valid = series.dropna()
    if valid.empty:
        return series

    lower = valid.quantile(lower_quantile)
    upper = valid.quantile(upper_quantile)
    return series.clip(lower=lower, upper=upper)


def _zscore_series(series: pd.Series) -> pd.Series:
    valid = series.dropna()
    if valid.empty:
        return series

    std = valid.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)

    mean = valid.mean()
    return (series - mean) / std


def preprocess_factor_data(factor_frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Combine, winsorize, and z-score raw factor exposures."""
    if not factor_frames:
        return pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)

    combined = pd.concat(factor_frames, ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=FACTOR_OUTPUT_COLUMNS)

    combined = combined[["date", "ticker", "factor", "value"]].copy()
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined["ticker"] = combined["ticker"].astype("string").str.upper().str.strip()
    combined["factor"] = combined["factor"].astype("string").str.lower().str.strip()
    combined["value"] = pd.to_numeric(combined["value"], errors="coerce")
    combined = combined.dropna(subset=["date", "ticker", "factor", "value"])

    grouped = combined.groupby(["date", "factor"], group_keys=False)["value"]
    combined["value"] = grouped.transform(_winsorize_series)
    combined["value"] = combined.groupby(["date", "factor"], group_keys=False)["value"].transform(_zscore_series)

    return combined.sort_values(["date", "ticker", "factor"]).reset_index(drop=True)
