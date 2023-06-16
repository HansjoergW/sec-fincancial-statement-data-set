"""
Defines the container that keeps the data of sub.txt, num.txt, and  pre.txt together.
"""

import os
from dataclasses import dataclass
from typing import Dict, Optional, List, TypeVar

import pandas as pd

from secfsdstools.a_utils.basic import calculate_previous_period
from secfsdstools.a_utils.constants import SUB_TXT, PRE_TXT, NUM_TXT, PRE_NUM_TXT

RAW = TypeVar('RAW', bound='RawDataBag')
JOINED = TypeVar('JOINED', bound='JoinedDataBag')


class JoinedDataBag:

    @classmethod
    def create(cls, sub_df: pd.DataFrame, pre_num_df: pd.DataFrame) -> JOINED:
        return JoinedDataBag(sub_df=sub_df, pre_num_df=pre_num_df)

    def __init__(self, sub_df: pd.DataFrame, pre_num_df: pd.DataFrame):
        self.sub_df = sub_df
        self.pre_num_df = pre_num_df

    def get_sub_copy(self) -> pd.DataFrame:
        """
        Returns a copy of the sub dataframe.

        Returns:
            pd.DataFrame: copy of the sub dataframe.
        """
        return self.sub_df.copy()

    def get_pre_num_copy(self) -> pd.DataFrame:
        """
        Returns a copy of the joined pre_num dataframe.

        Returns:
            pd.DataFrame: copy of joined pre_num dataframe.
        """
        return self.pre_num_df.copy()

    def save(self, target_path: str):
        """
        Stores the bag under the given directory.
        The directory has to exist and must be empty.

        Args:
            databag: the bag to be saved
            target_path: the directory under which the parquet files for sub and pre_num
                  will be created

        """
        if not os.path.isdir(target_path):
            raise ValueError(f"the path {target_path} does not exist")

        if len(os.listdir(target_path)) > 0:
            raise ValueError(f"the target_path {target_path} is not empty")

        self.sub_df.to_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        self.pre_num_df.to_parquet(os.path.join(target_path, f'{PRE_NUM_TXT}.parquet'))

    @staticmethod
    def load(target_path: str) -> JOINED:
        """
        Loads the content of the current bag at the specified location.

        Args:
            target_path: the directory which contains the parquet files for sub and pre_num

        Returns:
            JoinedDataBag: the loaded Databag
        """
        sub_df = pd.read_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        pre_num_df = pd.read_parquet(os.path.join(target_path, f'{PRE_NUM_TXT}.parquet'))

        return JoinedDataBag.create(sub_df=sub_df, pre_num_df=pre_num_df)

    @staticmethod
    def concat(bags: List[JOINED]) -> JOINED:
        """
        Merges multiple Bags together into one bag.
        Note: merge does not check if DataBags with the same reports are merged together.

        Args:
            bags: List of bags to be merged

        Returns:
            JoinedDataBag: a Bag with the merged content

        """
        sub_dfs = [db.sub_df for db in bags]
        pre_num_dfs = [db.pre_num_df for db in bags]

        return JoinedDataBag.create(sub_df=pd.concat(sub_dfs),
                                    pre_num_df=pd.concat(pre_num_dfs))


@dataclass
class RawDataBagStats:
    """
    Contains simple statistics of a report.
    """
    num_entries: int
    pre_entries: int
    number_of_reports: int
    reports_per_form: Dict[str, int]
    reports_per_period_date: Dict[int, int]


class RawDataBag:
    """
    Container class to keep the data for sub.txt, pre.txt, and num.txt together.
    """

    @classmethod
    def create(cls, sub_df: pd.DataFrame, pre_df: pd.DataFrame, num_df: pd.DataFrame) -> RAW:
        bag = RawDataBag(sub_df=sub_df, pre_df=pre_df, num_df=num_df)
        bag._init_internal_structures()
        return bag

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
        self.adsh_previous_period_map = {adsh: calculate_previous_period(period)
                                         for adsh, period in self.adsh_period_map.items()}

    def get_sub_copy(self) -> pd.DataFrame:
        """
        Returns a copy of the sub.txt dataframe.

        Returns:
            pd.DataFrame: copy of the sub.txt dataframe.
        """
        return self.sub_df.copy()

    def get_pre_copy(self) -> pd.DataFrame:
        """
        Returns a copy of the pre.txt dataframe.

        Returns:
            pd.DataFrame: copy of the pre.txt dataframe.
        """
        return self.pre_df.copy()

    def get_num_copy(self) -> pd.DataFrame:
        """
        Returns a copy of the num.txt dataframe.

        Returns:
            pd.DataFrame: copy of the num.txt dataframe.
        """
        return self.num_df.copy()

    def get_joined_bag(self,
                       use_period: bool = True,
                       use_previous_period: bool = False,
                       tags: Optional[List[str]] = None) -> JoinedDataBag:
        """
        merges the raw data of pre and num together.
        depending on the parameters, it just uses the  period date and the previouis period date.
        furthermore, also the tags could be restricted.

        Note: default for use_period is True

        Args:
            use_period (bool, True): indicates that only the values are filtered which
            ddates machtes the period of the report.

            use_previous_period (bool, False): indicates that only the values  are filtered
            which ddates matches the period of the report and the previous year. If this is set
            to True, then the value of use_period is ignored

            tags (List[str], optional, None): if set, only the tags listet in this
            parameter are returned
        Returns:
        JoinedDataBag: the DataBag where pre and num are merged

        """
        num_df_filtered_for_dates = self.num_df

        ## only consider the entries in num.df that have ddates according to the set
        ## use_report and use_previoius_report

        # if use_previous_report, then use_period is ignored
        if use_period and not use_previous_period:
            mask = self.num_df['adsh'].map(self.adsh_period_map) == self.num_df['ddate']
            num_df_filtered_for_dates = num_df_filtered_for_dates[mask]

        if use_previous_period:
            mask = (self.num_df['adsh'].map(self.adsh_period_map) == self.num_df['ddate']) | \
                   (self.num_df['adsh'].map(self.adsh_previous_period_map) == self.num_df['ddate'])

            num_df_filtered_for_dates = num_df_filtered_for_dates[mask]

        ## only consider the entries in pre which tag names are defined in the tags parameter
        pre_filtered_for_tags = self.pre_df
        if tags:
            pre_filtered_for_tags = self.pre_df[self.pre_df.tag.isin(tags)]

        ## transform the data
        # merge num and pre together. only rows in num are considered for which entries in pre exist
        pre_num_df = pd.merge(num_df_filtered_for_dates,
                              pre_filtered_for_tags,
                              on=['adsh', 'tag',
                                  'version'])  # don't produce index_x and index_y columns

        return JoinedDataBag.create(sub_df=self.sub_df, pre_num_df=pre_num_df)

    def statistics(self) -> RawDataBagStats:
        """
        calculate a few simple statistics of a report.
        - number of entries in the num-file
        - number of entries in the pre-file
        - number of reports in the zip-file (equals number of entries in sub-file)
        - number of reports per form (10-K, 10-Q, ...)
        - number of reports per period date (counts per value in the period column of sub-file)

        Returns:
            RawDataBagStats: instance with basic report infos
        """

        num_entries = len(self.num_df)
        pre_entries = len(self.pre_df)
        number_of_reports = len(self.sub_df)
        reports_per_period_date: Dict[int, int] = self.sub_df.period.value_counts().to_dict()
        reports_per_form: Dict[str, int] = self.sub_df.form.value_counts().to_dict()

        return RawDataBagStats(num_entries=num_entries,
                               pre_entries=pre_entries,
                               number_of_reports=number_of_reports,
                               reports_per_form=reports_per_form,
                               reports_per_period_date=reports_per_period_date
                               )

    def save(self, target_path: str):
        """
        Stores the bag under the given directory.
        The directory has to exist and must be empty.

        Args:
            databag: the bag to be saved
            target_path: the directory under which three parquet files for sub_txt, pre_text,
                  and num_txt will be created

        """
        if not os.path.isdir(target_path):
            raise ValueError(f"the path {target_path} does not exist")

        if len(os.listdir(target_path)) > 0:
            raise ValueError(f"the target_path {target_path} is not empty")

        self.sub_df.to_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        self.pre_df.to_parquet(os.path.join(target_path, f'{PRE_TXT}.parquet'))
        self.num_df.to_parquet(os.path.join(target_path, f'{NUM_TXT}.parquet'))

    @staticmethod
    def load(target_path: str) -> RAW:
        """
        Loads the content of the current bag at the specified location.

        Args:
            target_path: the directory which contains the three parquet files for sub_txt, pre_txt,
             and num_txt

        Returns:
            RawDataBag: the loaded Databag
        """
        sub_df = pd.read_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        pre_df = pd.read_parquet(os.path.join(target_path, f'{PRE_TXT}.parquet'))
        num_df = pd.read_parquet(os.path.join(target_path, f'{NUM_TXT}.parquet'))

        return RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

    @staticmethod
    def concat(bags: List[RAW]) -> RAW:
        """
        Merges multiple Bags together into one bag.
        Note: merge does not check if DataBags with the same reports are merged together.

        Args:
            bags: List of bags to be merged

        Returns:
            RawDataBag: a Bag with the merged content

        """
        sub_dfs = [db.sub_df for db in bags]
        pre_dfs = [db.pre_df for db in bags]
        num_dfs = [db.num_df for db in bags]

        # todo: might be more efficient if the contained maps were just combined
        #       instead of being recalculated
        return RawDataBag.create(sub_df=pd.concat(sub_dfs),
                                 pre_df=pd.concat(pre_dfs),
                                 num_df=pd.concat(num_dfs))
