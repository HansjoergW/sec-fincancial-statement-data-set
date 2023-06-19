from abc import abstractmethod

from typing import TypeVar, Generic

T = TypeVar('T')


class FilterBase(Generic[T]):

    @abstractmethod
    def filter(self, databag: T) -> T:
        """
        implements a simple filter on the RawDataBag and produces a new databag
        Args:
            databag (T): the bag to apply the filter to

        Returns:
            T: the new  bag with the filtered content

        """
