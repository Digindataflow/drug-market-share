from abc import ABC, abstractmethod
from typing import List

import numpy as np
import pandas as pd

import settings


class BaseProcess(ABC):
    def __init__(self, window_size_to_weights: dict) -> None:
        self.window_size_to_weights = window_size_to_weights

    @abstractmethod
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        pass


class MarketShareProcess(BaseProcess):
    """calculate market share data from sales data"""

    @staticmethod
    def calculate_market_share(data: pd.DataFrame) -> pd.DataFrame:
        """calculate market share, assuming total sales
        consist of four products.

        :param data: sales data
        """
        data_by_product = data.groupby(["date", "product_name"]).agg(
            {"unit_sales": "sum"}
        )
        market_share = (
            data_by_product.groupby(level=0)
            .apply(lambda x: np.round(x / x.sum(), settings.DECIMAL_DIGITS))
            .rename(columns={"unit_sales": "market_share"})
            .reset_index()
        )
        return market_share

    @staticmethod
    def calculate_lagged_avg_market_share(
        data: pd.DataFrame, window_size: int
    ) -> pd.Series:
        """calculate moving average of market share

        :param data: market share data
        :param window_size: size (month) of moving window
        """
        data = data.rolling(window_size).mean().apply(lambda x: np.round(x, settings.DECIMAL_DIGITS))
        data = data.rename(f"lagged_{window_size-1}_month_avg_market_share")
        return data

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """process sales data
        calculate Snaffleflax market share and
        moving average of market share

        :param data: sales data
        """
        market_share = self.calculate_market_share(data)
        market_share = market_share.loc[
            market_share.product_name == "Snaffleflax", :
        ].set_index("date")
        market_share = market_share.loc[:, "market_share"]

        series_list = [market_share]
        for size in self.window_size_to_weights.keys():
            data = self.calculate_lagged_avg_market_share(market_share, size)
            series_list.append(data)
        return pd.concat(series_list, axis=1)


class MarketEventProcess(BaseProcess):
    """calculate event data from crm"""

    @staticmethod
    def calculate_sum_each_event(data: pd.DataFrame) -> pd.Series:
        """calculate sum of each event in each month

        :param data: event sum data based on one month
        """
        data = (
            data.groupby(["event_month", "event_type"])
            .count().loc[:, "acct_id"]
            .rename("event_count")
            .unstack()
            .fillna(0)
            .astype(int)
        )
        data.columns = [item.replace(" ", "_") for item in data.columns]
        return data

    @staticmethod
    def calculate_lagged_sum_events(data: pd.DataFrame, window_size: int) -> pd.Series:
        """calculate moving average of event sum

        :param data: event sum data based on one month
        :param window_size: size (month) of moving window
        """
        return (
            data.rolling(window_size)
            .mean()
            .apply(lambda x: np.round(x, settings.DECIMAL_DIGITS))
            .rename(f"lagged_{window_size-1}_month_sum_events")
        )

    @staticmethod
    def calculate_lagged_weighted_sum_events(
        data: pd.DataFrame, window_size: int, weights: List[int]
    ) -> pd.Series:
        """calculate moving average of event sum

        :param data: event sum data based on one month
        :param window_size: size (month) of moving window
        :param weights: weight list for each month in a moving window
        """
        sum_weights = np.sum(weights)
        data = data.rolling(window=window_size).apply(
            lambda x: np.round(np.sum(weights * x) / sum_weights, settings.DECIMAL_DIGITS), raw=False
        )
        return data.rename(f"lagged_{window_size-1}_month_weighted_sum_events")

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """process crm data
        calculate event activity sum and
        moving average of event activity sum

        :param data: crm data
        """
        data["event_month"] = data["date"].to_numpy().astype("datetime64[M]")

        sum_each_event_by_month = self.calculate_sum_each_event(data)

        sum_total_events_by_month = (
            data.groupby(["event_month"])
            .count()
            .loc[:, "acct_id"]
            .rename("event_count")
        )

        series_list = [sum_each_event_by_month, sum_total_events_by_month]
        for size in self.window_size_to_weights.keys():
            data = self.calculate_lagged_sum_events(sum_total_events_by_month, size)
            series_list.append(data)

        for size, weights in self.window_size_to_weights.items():
            data = self.calculate_lagged_weighted_sum_events(sum_total_events_by_month, size, weights)
            series_list.append(data)
        return pd.concat(series_list, axis=1)
