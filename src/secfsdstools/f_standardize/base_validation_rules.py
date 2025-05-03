"""
This module contains "Validation Rules"
"""
from abc import ABC, abstractmethod
from typing import List

import numpy as np
import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.base_rule_framework import DescriptionEntry


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

    def __init__(self, identifier: str):
        """
        set the identifier of the rule. This is used as prefix when the validation columns are
        added to the dataframe.

        Args:
            id: the id of the rule
        """
        self.identifier = identifier

    @abstractmethod
    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            data_df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """

    @abstractmethod
    def calculate_error(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            data_df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """

    def validate(self, data_df: pd.DataFrame):
        """
        executes the validation on the dataframe and adds the two validation columns to the
        dataframe. <id>_error containing the relative error and <id>_cat containing the category.
        """
        mask = self.mask(data_df)
        error = self.calculate_error(data_df, mask)

        error_column_name = f'{self.identifier}_error'
        cat_column_name = f'{self.identifier}_cat'

        data_df[error_column_name] = np.nan
        data_df.loc[mask, error_column_name] = error

        data_df.loc[mask, cat_column_name] = 100  # gt > 0.1 / 10%
        data_df.loc[data_df[error_column_name] <= 0.1, cat_column_name] = 10  # 5-10 %
        data_df.loc[data_df[error_column_name] <= 0.05, cat_column_name] = 5  # 1-5 %
        data_df.loc[data_df[error_column_name] <= 0.01, cat_column_name] = 1  # < 1%
        data_df.loc[data_df[error_column_name] == 0.0, cat_column_name] = 0  # exact match

    @abstractmethod
    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """

    def collect_description(self, part: str) -> DescriptionEntry:
        """
        Returns the description of this rule elements and its children as a list.

        Args:
            part (str): the part to which the element belongs to

        Returns:
            DescriptionEntry
        """
        return DescriptionEntry(
            part=part,
            type="Validation",
            ruleclass=self.__class__.__name__,
            identifier=self.identifier,
            description=self.get_description())


class SumValidationRule(ValidationRule):
    """
    Checks whether an expected sum - equation is actually true. So for instance if the equation
    Assets = AssetsCurrent + AssetsNoncurrent is actually true.
    """

    def __init__(self, identifier: str, sum_tag: str, summands: List[str]):
        """

        Args:
            identifier: the identifier of the rule
            sum_tag: the tag that should contain the sum of all summands
            summands: the list with summands
        """
        super().__init__(identifier=identifier)
        self.sum_tag = sum_tag
        self.summands = summands

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = ~data_df[self.sum_tag].isna()
        for summand in self.summands:
            mask = mask & ~data_df[summand].isna()

        return mask

    def calculate_error(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            data_df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """

        # subtract the sum of all summands from the sum_tag and make it relatvie by dividing
        # with the sum_tag. finally make the result absolute, so that error is always positive

        result = ((data_df[self.sum_tag] - data_df[self.summands].sum(axis='columns'))
                  / data_df[self.sum_tag]).abs()

        # correct if we did devide by zero
        mask_zero = data_df[self.sum_tag] == 0.0
        result.loc[mask_zero & (data_df[self.summands].sum(axis='columns') == 0.0)] = 0.0

        return result

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Checks whether the sum of {self.summands} equals the value in '{self.sum_tag}'"


class ProductValidationRule(ValidationRule):
    """
    Checks whether an expected multiplication - equation is actually true. So for instance if the
    equation NetIncomeLoss = OustandingShares * EarningsPerShare is actually true.
    """

    def __init__(self, identifier: str, product_tag: str, multipliers: List[str]):
        """

        Args:
            identifier: the identifier of the rule
            product_tag: the tag that should contain the product of all multipliers
            multipliers: the list with multipliers
        """
        super().__init__(identifier=identifier)
        self.product_tag = product_tag
        self.multipliers = multipliers

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        mask = ~data_df[self.product_tag].isna()
        for multiplier in self.multipliers:
            mask = mask & ~data_df[multiplier].isna()

        return mask

    def calculate_error(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            data_df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """

        # subtract the product of all multipliers from the sum_tag and make it relative by dividing
        # with the product_tag. finally make the result absolute, so that error is always positive



        result = ((data_df[self.product_tag] - data_df[self.multipliers].prod(axis='columns'))
                  / data_df[self.product_tag]).abs()

        # correct if we did devide by zero
        mask_zero = data_df[self.product_tag] == 0.0
        result.loc[mask_zero & (data_df[self.multipliers].prod(axis='columns') == 0.0)] = 0.0

        return result

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return (f"Checks whether the product of {self.multipliers} equals"
                f" the value in '{self.product_tag}'")


class IsSetValidationRule(ValidationRule):
    """
    Checks whether a certain tag has a value and is not nan.
    """

    def __init__(self, identifier: str, tag: str):
        """

        Args:
            identifier: the identifier of the rule
            tag: the tag to check if it has a value
        """
        super().__init__(identifier=identifier)
        self.tag = tag

    def mask(self, data_df: pd.DataFrame) -> pa.typing.Series[bool]:
        """
            returns a Series[bool] which defines the rows to which this rule has to be applied.
            For validation rules this means to select the rows for which all necessary tags
            are available.

        Args:
            df: dataframe on which the rules should be applied

        Returns:
            pa.typing.Series[bool]: a boolean Series that marks which rows have to be calculated
        """
        # we have to mask all rows
        return pd.Series([True] * len(data_df), index=data_df.index)

    def calculate_error(self, data_df: pd.DataFrame, mask: pa.typing.Series[bool]) -> \
            pa.typing.Series[np.float64]:
        """
        implements the calculation logic.
        Args:
            data_df: the dataset to validate
            mask: the mask masking the rows to which the calculation should be applied to.

        Returns:
            pa.typing.Series[np.float64]: containing the relative error
        """

        # create Series with same length as dataframe and use same index
        result = pd.Series([0.0] * len(data_df), index=data_df.index)

        # mask the entries which don't have a value and set them as error = 1
        # this will lead to only two categories: 0 and 100
        result.loc[data_df[self.tag].isna()] = 1.0
        return result

    def get_description(self) -> str:
        """
        Returns the description String
        Returns:
            str: description
        """
        return f"Checks whether the {self.tag} is set."
