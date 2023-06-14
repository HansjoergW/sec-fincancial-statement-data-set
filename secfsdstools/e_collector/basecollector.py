"""
Collector interface definition
"""
import os
from abc import ABC, abstractmethod
from typing import Protocol, List, Optional

import pandas as pd

from secfsdstools.d_container.databagmodel import RawDataBag


class Collector(Protocol):
    """
    Interface for classes who collect data of one or several reports
    """

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data

        """


class BaseCollector(ABC):

    def _read_df_from_raw_parquet(self,
                                  path: str,
                                  file: str,
                                  filters=None) -> pd.DataFrame:
        return pd.read_parquet(os.path.join(path, f'{file}.parquet'),
                               filters=filters)

    def _get_pre_num_filters(self,
                             adshs: Optional[List[str]],
                             stmts: Optional[List[str]],
                             tags: Optional[List[str]]):
        pre_filter = []
        num_filter = []

        if adshs:
            adsh_filter_expression = ('adsh', 'in', adshs)
            pre_filter.append(adsh_filter_expression)
            num_filter.append(adsh_filter_expression)

        if stmts:
            pre_filter.append(('stmt', 'in', stmts))

        if tags:
            tag_filter_expression = ('tag', 'in', tags)
            pre_filter.append(tag_filter_expression)
            num_filter.append(tag_filter_expression)

        return pre_filter, num_filter

    @abstractmethod
    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag

        Returns:
            RawDataBag: the collected Data

        """
