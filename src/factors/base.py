"""Base interfaces for factor calculation."""

from abc import ABC, abstractmethod

import pandas as pd


class BaseFactor(ABC):
    """Abstract factor interface."""

    name: str

    @abstractmethod
    def compute(self, price_data: pd.DataFrame, fundamentals_data: pd.DataFrame | None = None) -> pd.DataFrame:
        """Return factor exposures with at least date, ticker, and value columns."""


FACTOR_OUTPUT_COLUMNS = ["date", "ticker", "factor", "value"]
