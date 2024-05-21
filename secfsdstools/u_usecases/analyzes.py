"""
Provices some helper method to analyze data in the bags.
"""
from typing import List

import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag


def find_adshs_with_all_tags(bag: JoinedDataBag, tag_list: List[str]) -> List[str]:
    """
    Returns a list with adshs, which contain all the tags mentioned in the taglist

    Args:
        bag:
        tag_list:

    Returns:
        pd.DataFrame

    """
    filtered_tags_df = bag.pre_num_df[bag.pre_num_df.tag.isin(tag_list)]
    filtered_df = filtered_tags_df[['adsh', 'tag']].unique()
    return filtered_df.groupby(['adsh']).count().reset_index().adsh().tolist()


def find_tags_containing(bag: JoinedDataBag, contains: str) -> pd.DataFrame:
    """
    returns a value counts of all tags that contain the provides string.

    Args:
        bag: the bag to check
        contains: text that should be contained in the tag name

    Returns:
        pd.DataFrame: a Dataframe with the tagname and the value_counts as columns

    """
    filtered_df = bag.pre_num_df[bag.pre_num_df.tag.str.contains(contains)]
    return filtered_df.tag.value_counts()

def count_tags(bag: JoinedDataBag) -> pd.DataFrame:
    """
    returns a value counts of all tags that are present in the bag.
    gives also a relative number to the number of unique statements in the bag
    (unique combinations of ['adsh', 'stmt', 'coreg', 'report', 'ddate', 'uom', 'qtrs']).

    Args:
        bag: the bag to check

    Returns:
        pd.DataFrame: a Dataframe with the tagname and the value_counts as columns

    """

    count_df = bag.pre_num_df.tag.value_counts().reset_index()
    count_df.columns = ['tag', 'count']
    unique_stmts = bag.pre_num_df[['adsh', 'stmt', 'coreg', 'report', 'ddate', 'uom', 'qtrs']].drop_duplicates().shape[0]

    count_df['rel'] = count_df['count'] / unique_stmts
    return count_df


def reports_using_all(bag: JoinedDataBag, used_tags: List[str]) -> List[str]:
    relevant_cols = bag.pre_num_df[['adsh', 'tag']]
    relevant_tags = relevant_cols[relevant_cols.tag.isin(used_tags)]
    unique_df = relevant_tags.drop_duplicates()
    counted_df = unique_df.groupby('adsh').count()
    filterd_df = counted_df[counted_df.tag == len(used_tags)]

    return filterd_df.index.to_list()
