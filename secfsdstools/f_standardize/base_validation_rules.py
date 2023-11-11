"""
This module contains "Validation Rules"
"""
from abc import ABC
from abc import abstractmethod
from typing import List

import numpy as np
import pandas as pd
import pandera as pa


class ValidationRule(ABC):
    """
    Base class for all validation rules.

    A Validation Rules checks if some basic requirements are met. For instance, in a Balance Sheet,
    the equation Assets = AssetsCurrent + AssetsNoncurrent should be true if the data is valid.

    So validation rules check if such requirements are actually true. They add two columns to the
    main dataset, an "error" column, which contains the relative error, and a "cat" column which
    contains the category.

    The categories are: 0 for an exact match, 1 if the relative error is less than 1% off,
    5 if the relativ error is less than 5% off, 10 if the relative error is less than 10 % off,
    and a 100 if it is above 10%.
    """

    def __init__(self, id: str):
        """
        set the identifier of the rule. This is used as prefix when the validation columns are
        added to the dataframe.

        Args:
            id: the id of the rule
        """
        self.id = id

    @abstractmethod
    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """

        pass

    @abstractmethod
    def calculate_error(self, df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """
        pass

    def validate(self, df: pd.DataFrame):
        """
        executes the validation on the dataframe and adds the two validation columns to the
        dataframe. <id>_error containing the relative error and <id>_cat containing the category.
        """
        mask = self.mask(df)
        error = self.calculate_error(df, mask)

        error_column_name = f'{self.id}_error'
        cat_column_name = f'{self.id}_cat'

        df[error_column_name] = None
        df.loc[mask, error_column_name] = error

        df.loc[mask, cat_column_name] = 100  # gt > 0.1 / 10%
        df.loc[df[error_column_name] <= 0.1, cat_column_name] = 10  # 5-10 %
        df.loc[df[error_column_name] <= 0.05, cat_column_name] = 5  # 1-5 %
        df.loc[df[error_column_name] <= 0.01, cat_column_name] = 1  # < 1%
        df.loc[df[error_column_name] == 0.0, cat_column_name] = 0  # exact match

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        pass


class SumValidationRule(ValidationRule):
    """
    Checks whether an expected sum - equation is actually true. So for instance if the equation
    Assets = AssetsCurrent + AssetsNoncurrent is actually true.
    """

    def __init__(self, id: str, sum_tag: str, summands: List[str]):
        """

        Args:
            id: the identifier of the rule
            sum_tag: the tag that should contain the sum of all summands
            summands: the list with summands
        """
        super().__init__(id)
        self.sum_tag = sum_tag
        self.summands = summands

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = ~df[self.sum_tag].isna()
        for summand in self.summands:
            mask = mask & ~df[summand].isna()

        return mask

    def calculate_error(self, df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """

        # subtract the sum of all summands from the sum_tag and make it relatvie by dividing
        # with the sum_tag. finally make the result absolute, so that error is always positive

        return ((df[self.sum_tag] - df[self.summands].sum(axis='columns')) / df[self.sum_tag]).abs()

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """

        return f"Checks whether the sum of {self.summands} equals the value in '{self.sum_tag}'"
