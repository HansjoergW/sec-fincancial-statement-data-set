"""
This module contains some basic pathfilter implementations on the JoinedDataBag.

Note: the filters don't create new copies of the pandas dataset
"""
from typing import List

from secfsdstools.a_utils.basic import calculate_previous_period
from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.d_container.filter import FilterBase


class AdshJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data by a list of adshs. This pathfilter operates on the sub, pre_df and the num_df.
    """

    def __init__(self, adshs: List[str]):
        self.adshs = adshs

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only datapoints of reports defined by the adshs list
        are contained.

        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        sub_filtered_for_adshs = databag.sub_df[databag.sub_df.adsh.isin(self.adshs)]
        pre_num_filtered_for_adshs = databag.pre_num_df[databag.pre_num_df.adsh.isin(self.adshs)]

        return JoinedDataBag.create(sub_df=sub_filtered_for_adshs,
                                    pre_num_df=pre_num_filtered_for_adshs)


class StmtJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data by a list of statement type (BS, IS, CF, ...).
    This pathfilter operates on the pre_df.
    """

    def __init__(self, stmts: List[str]):
        self.stmts = stmts

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only datapoints of reports defined by the adshs list
        are contained.

        Args:
            databag(JoinedDataBag) : Joineddatabag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        pre_num_filtered_for_stmts = databag.pre_num_df[databag.pre_num_df.stmt.isin(self.stmts)]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_stmts)


class ReportPeriodJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data so that only datapoints are contained which ddate-attribute equals the
    period date of the report. Therefore, the pathfilter operates on the num_df dataframe.
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        pathfilter the databag so that only datapoints are contained which have a ddate-attribute
        that equals the period-attribute of the report.
        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """

        adsh_period_map = \
            databag.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

        mask = databag.pre_num_df['adsh'].map(adsh_period_map) == databag.pre_num_df['ddate']
        pre_num_filtered_for_ddates = databag.pre_num_df[mask]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_ddates)


class ReportPeriodAndPreviousPeriodJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data so that only datapoints are contained which ddate-attribute equals the
    period date of the report or the period date of the previous (a year ago) report.
    Therefore, the pathfilter operates on the num_df dataframe.
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        pathfilter the databag so that only datapoints are contained which have a ddate-attribute
        that equals the period-attribute of the report or the period of the previous (a year ago)
        report.
        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """

        adsh_period_map = \
            databag.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

        # caculate the dates for the previous year
        adsh_previous_period_map = {adsh: calculate_previous_period(period)
                                    for adsh, period in adsh_period_map.items()}

        mask = (databag.pre_num_df['adsh'].map(adsh_period_map) == databag.pre_num_df['ddate']) | \
               (databag.pre_num_df['adsh'].map(adsh_previous_period_map) == databag.pre_num_df[
                   'ddate'])

        pre_num_filtered_for_ddates = databag.pre_num_df[mask]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_ddates)


class TagJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data by a list of tags. This pathfilter operates on the pre_df and the num_df.
    """

    def __init__(self, tags: List[str]):
        self.tags = tags

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only datapoints are contained which have a tag-attribute
        that is in the provided list.
        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        pre_num_filtered_for_tags = databag.pre_num_df[databag.pre_num_df.tag.isin(self.tags)]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_tags)


class MainCoregJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters only for the main coreg entries (coreg == '')
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only the main coreg entries are contained
        (no data subsidiaries).
        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        pre_num_filtered_for_main_coreg = databag.pre_num_df[databag.pre_num_df.coreg == '']

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_main_coreg)


class OfficialTagsOnlyJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters only the official tags. These are the tags that contain an official XBRL version
    within the version column. "inofficial" (resp. company specific) tags are identified with
    the version column containing the value of the adsh.
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that official tags are contained.

        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        pre_num_filtered_for_tags = databag.pre_num_df[
            ~databag.pre_num_df.version.isin(databag.sub_df.adsh)]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_tags)


class USDOnlyJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Removes all entries which have a currency in the column uom that is not USD.
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        Removes all currency entries in the uom colum of the pre_num_df that are not USD.

        Args:
            databag(JoinedDataBag) : Joineddatabag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data

        """
        # currency is always in uppercase, so if it is not all uppercase, it is not a currency
        mask_has_lower = ~databag.pre_num_df.uom.str.isupper()
        mask_is_none_currency = databag.pre_num_df.uom.str.len() != 3
        mask_usd_only = databag.pre_num_df.uom == "USD"

        prenum_filtered_for_usd = databag.pre_num_df[
            mask_has_lower | mask_is_none_currency | mask_usd_only]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=prenum_filtered_for_usd)


class NoSegmentInfoJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters only for the main coreg entries (coreg == '')
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only the main coreg entries are contained
        (no data subsidiaries).
        Args:
            databag(JoinedDataBag) : databag to apply the pathfilter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """
        pre_num_filtered_for_main_coreg = databag.pre_num_df[databag.pre_num_df.segments == '']

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=pre_num_filtered_for_main_coreg)


class CIKJoinedFilter(FilterBase[JoinedDataBag]):
    """
    Filters the data by a list of ciks. This filter operates on the sub, pre_df and the num_df.
    """

    def __init__(self, ciks: List[int]):
        self.ciks = ciks

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        filters the databag so that only datapoints belonging to the provided ciks
        are contained.

        Args:
            databag(JoinedDataBag) : joineddatabag to apply the filter to

        Returns:
            JoinedDataBag: the databag with the filtered data
        """

        sub_filtered_for_adshs = databag.sub_df[databag.sub_df.cik.isin(self.ciks)]
        adshs = sub_filtered_for_adshs.adsh.tolist()

        pre_num_filtered_for_adshs = databag.pre_num_df[databag.pre_num_df.adsh.isin(adshs)]

        return JoinedDataBag.create(sub_df=sub_filtered_for_adshs,
                                    pre_num_df=pre_num_filtered_for_adshs)
