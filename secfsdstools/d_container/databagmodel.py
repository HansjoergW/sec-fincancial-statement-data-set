"""
Defines the container that keeps the data of sub.txt, num.txt, and  pre.txt together.
"""
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, TypeVar, Generic, Optional

import pandas as pd

from secfsdstools.a_utils.constants import SUB_TXT, PRE_TXT, NUM_TXT, PRE_NUM_TXT
from secfsdstools.a_utils.fileutils import check_dir, concat_parquet_files
from secfsdstools.d_container.filter import FilterBase
from secfsdstools.d_container.presentation import Presenter

RAW = TypeVar('RAW', bound='RawDataBag')
JOINED = TypeVar('JOINED', bound='JoinedDataBag')
T = TypeVar('T')

LOGGER = logging.getLogger(__name__)


def get_pre_num_filters(adshs: Optional[List[str]],
                        stmts: Optional[List[str]],
                        tags: Optional[List[str]]):
    """ creates filter definitions to be directly applied to num and pre files. """

    pre_filter = []
    num_filter = []

    if adshs:
        adsh_filter_expression = ('adsh', 'in', adshs)
        pre_filter.append(adsh_filter_expression)
        num_filter.append(adsh_filter_expression)

    if stmts:
        pre_filter.append(('stmt', 'in', stmts))

    if tags:
        tag_filter_expression = ('tag', 'in', tags)
        pre_filter.append(tag_filter_expression)
        num_filter.append(tag_filter_expression)

    return pre_filter, num_filter


def concat_bags_file_based_internal(paths_to_concat: List[Path],
                                    target_path: Path,
                                    file_list: List[str],
                                    drop_duplicates_sub_df: bool = False):
    """
    Helper method to concat files of multiple bags into a new bag-directory without actually
    loading the data and therefore having a low memory footprint.

    Args:
        paths_to_concat: list of paths that we want to concat
        target_path:  target path to where the concat data will be stored. the necessary directories
                      will be created
        file_list:  list of filenames to be concatenated without SUB_TXT. So this is either
                    ['pre.txt', 'num.txt'] or ['pre_num.txt'].
        drop_duplicates_sub_df: indicates whether drop duplicates has to be applied on the sub_df.
                                if true, the data for the sub.txt files must be read into memory.
                                This has to be true, for instance if you have separate bags
                                for BS, IS, and CF and want to concat them. In this case, they
                                all have the same data in sub.txt.


    """

    target_path.mkdir(parents=True, exist_ok=True)
    if not drop_duplicates_sub_df:
        file_list.append(SUB_TXT)

    for file_name in file_list:
        target_path_file = str(target_path / f'{file_name}.parquet')
        paths_to_concat_file = [str(p / f'{file_name}.parquet') for p in paths_to_concat]
        concat_parquet_files(paths_to_concat_file, target_path_file)

    # if we have to drop the duplicates, we need to read the data for the sub_df into memory
    if drop_duplicates_sub_df:
        sub_dfs: List[pd.DataFrame] = []
        for path_to_concat in paths_to_concat:
            sub_dfs.append(pd.read_parquet(path_to_concat / f'{SUB_TXT}.parquet'))

        sub_df = pd.concat(sub_dfs, ignore_index=True)
        sub_df.drop_duplicates(inplace=True)
        sub_df.to_parquet(target_path / f'{SUB_TXT}.parquet')


class DataBagBase(Generic[T]):
    """
    Base class for the DataBag types
    """

    def __getitem__(self, bagfilter: FilterBase[T]) -> T:
        """
        forwards to the pathfilter method, so that filters can be chained in a simple syntax:
        bag[filter1][filter2] is equal to bag.pathfilter(filter1).pathfilter(filter2)

        Args:
            bagfilter: the pathfilter to be applied

        Returns:
            RawDataBag: the databag with the filtered content
        """

        return self.filter(bagfilter)

    def filter(self, bagfilter: FilterBase[T]) -> T:
        """
        applies a pathfilter to the bag and produces a new bag based on the pathfilter.
        instead of using the pathfilter, you can also use the "index" syntax to apply filters:
        bag[filter1][filter2] is equal to bag.pathfilter(filter1).pathfilter(filter2)

        Args:
            bagfilter: the pathfilter to be applied

        Returns:
            RawDataBag: the databag with the filtered content
        """
        return bagfilter.filter(self)

    def present(self, presenter: Presenter[T]) -> pd.DataFrame:
        """
        apply a presenter
        """
        return presenter.present(self)

    @staticmethod
    def load_sub_df_by_filter(target_path: str,
                              adshs: Optional[List[str]] = None,
                              forms: Optional[List[str]] = None) -> pd.DataFrame:
        """
        loads the sub_txt datafrome from the target_path by directly applying the
        adshs or the froms filter.

        Args:
            target_path: root_path with the parquet files for sub, pre, and num
            forms: optional list of forms (10-K, 10-Q) to filter for during loading
            adshs: optional list of adhs to filter during the laoding

        Returns:
            pd.DataFrame the loaded sub_df content
        """

        sub_filter = None
        if adshs:
            sub_filter = ('adsh', 'in', adshs)
        elif forms:
            sub_filter = ('form', 'in', forms)

        if sub_filter:
            LOGGER.info("apply sub_df filter: %s", sub_filter)

        sub_df = pd.read_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'),
                                 filters=[sub_filter] if sub_filter else None)

        return sub_df


class JoinedDataBag(DataBagBase[JOINED]):
    """
    the DataBag in which the pre.txt and the num.txt are joined based on the
    adsh, tag, and version.
    """

    @classmethod
    def create(cls, sub_df: pd.DataFrame, pre_num_df: pd.DataFrame) -> JOINED:
        """
        create a new JoinedDataBag.

        Args:
            sub_df: sub.txt dataframe

            pre_num_df: joined pre.txt and num.txt dataframe

        Returns:
            JoinedDataBag: new instance of JoinedDataBag
        """
        return JoinedDataBag(sub_df=sub_df, pre_num_df=pre_num_df)

    def __init__(self, sub_df: pd.DataFrame, pre_num_df: pd.DataFrame):
        """
        constructor.
        Args:
            sub_df: sub.txt dataframe
            pre_num_df: joined pre.txt and num.txt dataframe
        """
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

    def copy_bag(self) -> JOINED:
        """
        creates a bag with new copies of the internal dataframes.

        Returns:
            JoinedDataBag: new instance of JoinedDataBag
        """
        return JoinedDataBag.create(sub_df=self.sub_df.copy(),
                                    pre_num_df=self.pre_num_df.copy())

    def save(self, target_path: str):
        """
        Stores the bag under the given directory.
        The directory has to exist and must be empty.

        Args:
            target_path: the directory under which the parquet files for sub and pre_num
                  will be created

        """
        check_dir(target_path)

        self.sub_df.to_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        self.pre_num_df.to_parquet(os.path.join(target_path, f'{PRE_NUM_TXT}.parquet'))

    @staticmethod
    def load(target_path: str,
             adshs_filter: Optional[List[str]] = None,
             forms_filter: Optional[List[str]] = None,
             stmt_filter: Optional[List[str]] = None,
             tag_filter: Optional[List[str]] = None) -> JOINED:
        """
            Loads the content of the current bag at the specified location.

            There are optional filters for adshs, forms, stmts and tags, that are
            applied directly during the load process and hence are more efficient and
            less memory consuming than loading the data and then applying filters.

            This makes especially sense, when you concatenated together data from different
            zip files.

            Note: the adsh are mutally exclusive and adsh has the higher precedence.

        Args:
            target_path: root_path with the parquet files for sub, pre, and num
            forms_filter: optional list of forms (10-K, 10-Q) to filter for during loading
            adshs_filter: optional list of adhs to filter during the laoding
            stmt_filter: optional list of stmts (BS, IS, CF, ..) to filter during the loading
            tag_filter: optional list of tags to filter during the loading

        Returns:
            RawDataBag: the loaded Databag
        """
        sub_df = DataBagBase.load_sub_df_by_filter(
            target_path=target_path, adshs=adshs_filter, forms=forms_filter
        )

        # if the forms filter was applied, overwrite the adshs list, since this are adshs
        # values that we should filter for in the pre_num dataframe
        if not adshs_filter and forms_filter:
            adshs_filter = sub_df.adsh.to_list()

        pre_num_filter = []

        filter_log_str: List[str] = []

        if adshs_filter:
            pre_num_filter.append(('adsh', 'in', adshs_filter))

            # the list of adshs could be quite huge, so we trim the message that we log
            # to max 100 characters
            log_part = str(('adsh', 'in', adshs_filter))
            if len(log_part) > 100:
                log_part = log_part[:100] + "...)"
            filter_log_str.append(log_part)

        if stmt_filter:
            pre_num_filter.append(('stmt', 'in', stmt_filter))
            filter_log_str.append(str(('stmt', 'in', stmt_filter)))

        if tag_filter:
            pre_num_filter.append(('tag', 'in', tag_filter))
            filter_log_str.append(str(('tag', 'in', tag_filter)))

        if len(pre_num_filter) > 0:
            LOGGER.info("apply pre_num_df filter: %s", filter_log_str)

        pre_num_df = pd.read_parquet(os.path.join(target_path, f'{PRE_NUM_TXT}.parquet'),
                                     filters=pre_num_filter if pre_num_filter else None)

        return JoinedDataBag.create(sub_df=sub_df, pre_num_df=pre_num_df)

    @staticmethod
    def concat(bags: List[JOINED], drop_duplicates_sub_df: bool = False) -> JOINED:
        """
        Merges multiple Bags together into one bag.
        Note: merge does not check if DataBags with the same reports are merged together.

        Args:
            bags: List of bags to be merged
            drop_duplicates_sub_df: set to True, if you want to remove duplicates in the sub_df

        Returns:
            JoinedDataBag: a Bag with the merged content

        """
        sub_dfs = [db.sub_df for db in bags]
        pre_num_dfs = [db.pre_num_df for db in bags]

        sub_df = pd.concat(sub_dfs, ignore_index=True)
        pre_num_df = pd.concat(pre_num_dfs, ignore_index=True)

        if drop_duplicates_sub_df:
            sub_df.drop_duplicates(inplace=True)

        return JoinedDataBag.create(sub_df=sub_df,
                                    pre_num_df=pre_num_df)

    @staticmethod
    def concat_filebased(paths_to_concat: List[Path],
                         target_path: Path,
                         drop_duplicates_sub_df: bool = False):
        """
        Concatenates all the Bags in paths_to_concatenate into the target_dir directory.

        It is directly working on the files and does not load the data into the memory.


        Args:
            paths_to_concat (List[Path]) : List with paths to read the datafrome
            target_path (Path) : path to write the concatenated data to
            drop_duplicates_sub_df (bool, False): indicates whether drop duplicates
                                has to be applied on the sub_df.
                                if true, the data for the sub.txt files must be read into memory.
                                This has to be true, for instance if you have separate bags
                                for BS, IS, and CF and want to concat them. In this case, they
                                all have the same data in sub.txt.

        Returns:
        """
        if len(paths_to_concat) == 0:
            # nothing to do
            return

        concat_bags_file_based_internal(
            paths_to_concat=paths_to_concat,
            target_path=target_path,
            file_list=[PRE_NUM_TXT],
            drop_duplicates_sub_df=drop_duplicates_sub_df
        )


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


class RawDataBag(DataBagBase[RAW]):
    """
    Container class to keep the data for sub.txt, pre.txt, and num.txt together.
    """

    @classmethod
    def create(cls, sub_df: pd.DataFrame, pre_df: pd.DataFrame, num_df: pd.DataFrame) -> RAW:
        """
        create method for RawDataBag
        Args:
            sub_df(pd.DataFrame): sub.txt dataframe
            pre_df(pd.DataFrame): pre.txt dataframe
            num_df(pd.DataFrame): num.txt dataframe

        Returns:
            RawDataBag:
        """
        return RawDataBag(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

    def __init__(self, sub_df: pd.DataFrame, pre_df: pd.DataFrame, num_df: pd.DataFrame):
        self.sub_df = sub_df
        self.pre_df = pre_df
        self.num_df = num_df

    def copy_bag(self):
        """
        creates a bag with new copies of the internal dataframes.

        Returns:
            RawDataBag: new instance of JoinedDataBag
        """

        return RawDataBag.create(sub_df=self.sub_df.copy(),
                                 pre_df=self.pre_df.copy(),
                                 num_df=self.num_df.copy())

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

    def join(self) -> JoinedDataBag:
        """
        merges the raw data of pre and num together.

        Returns:
            JoinedDataBag: the DataBag where pre and num are merged

        """

        # merge num and pre together. only rows in num are considered for which entries in pre exist
        pre_num_df = pd.merge(self.num_df,
                              self.pre_df,
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
            target_path: the directory under which three parquet files for sub_txt, pre_text,
                  and num_txt will be created
        """
        check_dir(target_path)

        self.sub_df.to_parquet(os.path.join(target_path, f'{SUB_TXT}.parquet'))
        self.pre_df.to_parquet(os.path.join(target_path, f'{PRE_TXT}.parquet'))
        self.num_df.to_parquet(os.path.join(target_path, f'{NUM_TXT}.parquet'))

    @staticmethod
    def load(target_path: str,
             adshs_filter: Optional[List[str]] = None,
             forms_filter: Optional[List[str]] = None,
             stmt_filter: Optional[List[str]] = None,
             tag_filter: Optional[List[str]] = None) -> RAW:
        """
            Loads the content of the current bag at the specified location.

            There are optional filters for adshs, forms, stmts and tags, that are
            applied directly during the load process and hence are more efficient and
            less memory consuming than loading the data and then applying filters.

            This makes especially sense, when you concatenated together data from different
            zip files.

            Note: the adsh are mutally exclusive and adsh has the higher precedence.

        Args:
            target_path: root_path with the parquet files for sub, pre, and num
            forms_filter: optional list of forms (10-K, 10-Q) to filter for during loading
            adshs_filter: optional list of adhs to filter during the laoding
            stmt_filter: optional list of stmts (BS, IS, CF, ..) to filter during the loading
            tag_filter: optional list of tags to filter during the loading

        Returns:
            RawDataBag: the loaded Databag
        """
        sub_df = DataBagBase.load_sub_df_by_filter(
            target_path=target_path, adshs=adshs_filter, forms=forms_filter
        )

        # if the forms filter was applied, overwrite the adshs list, since this is the list
        # we should then filter for
        if not adshs_filter and forms_filter:
            adshs_filter = sub_df.adsh.to_list()

        pre_filter, num_filter = get_pre_num_filters(adshs=adshs_filter,
                                                     stmts=stmt_filter,
                                                     tags=tag_filter)

        if len(num_filter) > 0:
            LOGGER.info("apply num_df filter: %s", num_filter)

        if len(pre_filter) > 0:
            LOGGER.info("apply pre_df filter: %s", pre_filter)

        pre_df = pd.read_parquet(os.path.join(target_path, f'{PRE_TXT}.parquet'),
                                 filters=pre_filter if pre_filter else None)

        num_df = pd.read_parquet(os.path.join(target_path, f'{NUM_TXT}.parquet'),
                                 filters=num_filter if num_filter else None)

        return RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

    @staticmethod
    def concat(bags: List[RAW], drop_duplicates_sub_df: bool = False) -> RAW:
        """
        Merges multiple Bags together into one bag.
        Note: merge does not check if DataBags with the same reports are merged together.

        Args:
            bags: List of bags to be merged
            drop_duplicates_sub_df: set to True, if you want to remove duplicates in the sub_df

        Returns:
            RawDataBag: a Bag with the merged content

        """
        sub_dfs = [db.sub_df for db in bags]
        pre_dfs = [db.pre_df for db in bags]
        num_dfs = [db.num_df for db in bags]

        sub_df = pd.concat(sub_dfs, ignore_index=True)
        pre_df = pd.concat(pre_dfs, ignore_index=True)
        num_df = pd.concat(num_dfs, ignore_index=True)

        if drop_duplicates_sub_df:
            sub_df.drop_duplicates(inplace=True)

        return RawDataBag.create(sub_df=sub_df,
                                 pre_df=pre_df,
                                 num_df=num_df)

    @staticmethod
    def concat_filebased(paths_to_concat: List[Path],
                         target_path: Path,
                         drop_duplicates_sub_df: bool = False):
        """
        Concatenates all the Bags in paths_to_concatenate into the target_dir directory.

        It is directly working on the files and does not load the data into the memory.


        Args:
            paths_to_concat (List[Path]) : List with paths to read the datafrome
            target_path (Path) : path to write the concatenated data to
            drop_duplicates_sub_df (bool, False): indicates whether drop duplicates
                                has to be applied on the sub_df.
                                if true, the data for the sub.txt files must be read into memory.
                                This has to be true, for instance if you have separate bags
                                for BS, IS, and CF and want to concat them. In this case, they
                                all have the same data in sub.txt.

        Returns:
        """
        if len(paths_to_concat) == 0:
            # nothing to do
            return

        concat_bags_file_based_internal(
            paths_to_concat=paths_to_concat,
            target_path=target_path,
            file_list=[PRE_TXT, NUM_TXT],
            drop_duplicates_sub_df=drop_duplicates_sub_df
        )


def is_rawbag_path(path: Path) -> bool:
    """ Check whether the provided path contains the files of a RawDatabag. """
    return (path / "num.txt.parquet").exists()


def is_joinedbag_path(path: Path) -> bool:
    """ Check whether the provided path contains the files of a JoinedDatabag. """
    return (path / "pre_num.txt.parquet").exists()
