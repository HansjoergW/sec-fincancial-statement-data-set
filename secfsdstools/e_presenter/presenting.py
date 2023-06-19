import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.d_container.presentation import PresenterBase


class StandardStatementPresenter(PresenterBase[JoinedDataBag]):
    """
    Reformats the data so that the presentation and order reflects the way how the basic
    financial statements are presented.
    """

    def __init__(self, flatten_index: bool = False, add_form_column: bool = False):
        """

        Args:
            flatten_index: if True, the index is reset, so that the columns
                             adsh', 'coreg', 'tag', 'version', 'stmt',
                             'report', 'line', 'uom', 'negating', 'inpth'
                             appear as columns and not in the index of the dataframe.
            add_form_column: adds a "form" column to the resulting dataframe which contains
                             the type of the report ('10-K', '10-Q', ...)
        """

        self.flatten_index = flatten_index
        self.add_form_column = add_form_column

    def present(self, databag: JoinedDataBag) -> pd.DataFrame:
        """
        creates the standard presentation of financial statements (Balance Sheet, Income Statement,
        Cash Flow).
        - Index columns are
           ['adsh', 'coreg', 'tag', 'version', 'stmt', 'report', 'line', 'uom', 'negating', 'inpth']
        - Value columns are by ddate
        - Order by
          ['adsh', 'coreg', 'stmt', 'report', 'line', 'inpth']

        Args:
            databag(JoinedDataBag) : bag to present

        Returns:
            pd.DataFrame: the dataframe with the final presentation
        """

        num_pre_pivot_df = databag.pre_num_df.pivot_table(
            index=['adsh', 'coreg', 'tag', 'version', 'stmt',
                   'report', 'line', 'uom', 'negating', 'inpth'],
            columns='ddate',
            values='value'
        )

        # some cleanup and ordering
        num_pre_pivot_df.rename_axis(None, axis=1, inplace=True)
        num_pre_pivot_df.sort_values(['adsh', 'coreg', 'stmt', 'report', 'line', 'inpth'],
                                     inplace=True)

        # the values for ddate are ints, not string
        # if we pivot, then the column names stay ints, which is unexpected, so we change
        # the type of the column to strings
        num_pre_pivot_df.rename(columns={x: str(x) for x in num_pre_pivot_df.columns},
                                inplace=True)

        if self.flatten_index:
            num_pre_pivot_df.reset_index(drop=False, inplace=True)

        if self.add_form_column:
            adsh_form_map = \
                databag.sub_df[['adsh', 'form']].set_index('adsh').to_dict()['form']

            num_pre_pivot_df['form'] = num_pre_pivot_df['adsh'].map(adsh_form_map)

        return num_pre_pivot_df
