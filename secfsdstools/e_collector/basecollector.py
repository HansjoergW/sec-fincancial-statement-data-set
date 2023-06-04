from typing import Protocol

from secfsdstools.d_container.databagmodel import DataBag


class Collector(Protocol):
    def collect(self) -> DataBag:
        """
        collects the data and returns a Databag

        Returns:
            DataBag: the collected Data

        """