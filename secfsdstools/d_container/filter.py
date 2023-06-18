from abc import ABC, abstractmethod


from typing import TypeVar, Generic

T = TypeVar('T')

class FilterBase(Generic[T]):

    @abstractmethod
    def filter(self, databag: T) -> T:
        """
        implements a simple filter on the RawDataBag and produces a new databag
        Args:
            databag (RawDataBag): the RawDataBag to apply the filter to

        Returns:
            RawDataBag: the new  RawDataBag with the filtered content

        """
