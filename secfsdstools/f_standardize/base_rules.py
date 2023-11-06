""" This module contains the Base Rule implementations."""
from typing import Set, List

import pandas as pd
import pandera as pa

from secfsdstools.f_standardize.rule_framework import Rule


class CopyTagRule(Rule):
    """ Copies the content of the original tag into the target tag, if the target tag is not set
    and the original tag is set"""

    def __init__(self, original: str, target: str):
        self.original = original
        self.target = target

    def mask(self, df: pd.DataFrame) -> pa.typing.Series[bool]:
        return (df[self.target].isna() &
                ~df[self.original].isna())

    def apply(self, df: pd.DataFrame, mask: pa.typing.Series[bool]):
        df.loc[mask, self.target] = df[self.original]

    def get_input_tags(self) -> Set[str]:
        return {self.target, self.original}

    def get_target_tags(self) -> List[str]:
        return [self.target]

    def get_description(self) -> str:
        return f"Copies the values from {self.original} to {self.target} " \
               f"if {self.original} is not null and {self.target} is null"
