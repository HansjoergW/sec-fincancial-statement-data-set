"""
Base class for Presenter implementations.
"""
from abc import abstractmethod

from typing import TypeVar, Generic

import pandas as pd

T = TypeVar('T')


class PresenterBase(Generic[T]):
    """
    The base class for presenter implementations.
    """

    @abstractmethod
    def present(self, databag: T) -> pd.DataFrame:
        """
        implements a presenter which reformats the bag into dataframe.
        Args:
            databag (T): the bag to transform into presentation df

        Returns:
            pd.DataFrame: the data to be presented

        """
