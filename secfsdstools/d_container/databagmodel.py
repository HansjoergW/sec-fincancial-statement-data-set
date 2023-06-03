from typing import Dict, Optional, List

import pandas as pd


class DataBag:

    @classmethod
    def create(cls, sub_df: pd.DataFrame, pre_df: pd.DataFrame, num_df: pd.DataFrame):
        bag = DataBag(sub_df=sub_df, pre_df=pre_df, num_df=num_df)
        bag._init_internal_structures()
        return bag

    @staticmethod
    def _calculate_previous_period(period: int) -> int:
        # the date of period is provided as an int int the format yyyymmdd
        # so to calculate the end of the previous period (the period a year ago)
        # only 10000 has to be subtracted, e.g. 20230101 -> 20230101 - 10000 = 20220101
        previous_value = period - 10_000

        # however, if the current period or the previous period was a leap year and
        # the current period is end of February, we have to adjust

        # so get the year and the month_and_day value from the period
        period_year, period_monthday = divmod(period, 10_000)

        # is the period date on a 29th of Feb, then the previous period has to end on a 28th Feb
        # therefore, adjust the previous_value
        if period_monthday == 229:
            previous_value = previous_value - 1

        # was the previous year a leap year and is the current period for end of february
        # then the previous period ends on the 29th of Feb
        # therefore, adjust the previous value
        if (((period_year - 1) % 4) == 0) & (period_monthday == 228):
            previous_value = previous_value + 1

        return previous_value

    def __init__(self, sub_df: pd.DataFrame, pre_df: pd.DataFrame, num_df: pd.DataFrame):
        self.sub_df = sub_df
        self.pre_df = pre_df
        self.num_df = num_df

        # pandas pivot works better if coreg is not nan, so we set it here to a simple dash
        self.num_df.loc[self.num_df.coreg.isna(), 'coreg'] = '-'

        # simple dict which directly returns the 'form' (10-k, 10q, ..) of a report
        self.adsh_form_map: Optional[Dict[str, str]] = None

        # simple dict which directly returns the period date of the report as int
        self.adsh_period_map: Optional[Dict[str, int]] = None

        # simple dict which directly returns the previous (a year before) period date as int
        self.adsh_previous_period_map: Optional[Dict[str, int]] = None

    def _init_internal_structures(self):

        self.adsh_form_map = \
            self.sub_df[['adsh', 'form']].set_index('adsh').to_dict()['form']

        self.adsh_period_map = \
            self.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

        # caculate the date for the previous year
        self.adsh_previous_period_map = {adsh: DataBag._calculate_previous_period(period)
                                         for adsh, period in self.adsh_period_map.items()}


def merge(bags: List[DataBag]) -> DataBag:
    """
    Merges multiple Bags together into one bag.

    Args:
        bags: List of bags to be merged

    Returns:
        DataBag: a Bag with the merged content

    """
    return None


def save(databag: DataBag, path: str):
    """
    Stores the bag under the given directory.

    Args:
        databag: the bag to be saved
        path: the directory under which three parquet files for sub_txt, pre_text, and num_txt
              will be created

    """
    pass


def load(path: str) -> DataBag:
    """
    Loads the content of the current bag at the specified location.

    Args:
        path: the directory which contains the three parquet files for sub_txt, pre_txt, and num_txt

    Returns:
        DataBag: the loaded Databag
    """
    return None
