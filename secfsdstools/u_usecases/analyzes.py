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
    filtered_df = bag.pre_num_df[bag.pre_num_df.tag.str.contains(contains)]
    return filtered_df.tag.value_counts()
