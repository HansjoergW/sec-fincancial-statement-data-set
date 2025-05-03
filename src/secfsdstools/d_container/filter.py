"""
Base class for Filter implementations.
"""
from abc import abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')


class FilterBase(Generic[T]):
    """
    Basic pathfilter definition.
    """

    @abstractmethod
    def filter(self, databag: T) -> T:
        """
        implements a simple pathfilter on the RawDataBag and produces a new databag
        Args:
            databag (T): the bag to apply the pathfilter to

        Returns:
            T: the new  bag with the filtered content

        """
