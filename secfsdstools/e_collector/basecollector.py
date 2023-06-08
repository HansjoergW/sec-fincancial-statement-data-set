"""
Collector interface definition
"""

from typing import Protocol

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