from abc import ABC, abstractmethod

from secfsdstools.d_container.databagmodel import RawDataBag, JoinedDataBag


class RawFilterBase(ABC):

    @abstractmethod
    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        implements a simple filter on the RawDataBag and produces a new databag
        Args:
            databag (RawDataBag): the RawDataBag to apply the filter to

        Returns:
            RawDataBag: the new  RawDataBag with the filtered content

        """


class JoinedFilterBase(ABC):

    @abstractmethod
    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        implements a simple filter on the RawDataBag and produces a new databag
        Args:
            databag (RawDataBag): the RawDataBag to apply the filter to

        Returns:
            RawDataBag: the new  RawDataBag with the filtered content

        """
