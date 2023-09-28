"""
Default Presenter implementations.
"""
import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.d_container.presentation import Presenter


class StandardStatementPresenter(Presenter[JoinedDataBag]):
    """
    Reformats the data so that the presentation and order reflects the way how the basic
    financial statements are presented.
    """

    def __init__(self, flatten_index: bool = True, add_form_column: bool = False,
                 invert_negating: bool = False):
        """

        Args:
            flatten_index: if True, the index is reset, so that the columns
                             adsh', 'coreg', 'tag', 'version', 'stmt',
                             'report', 'line', 'uom', 'negating', 'inpth'
                             appear as columns and not in the index of the dataframe.
            add_form_column: adds a "form" column to the resulting dataframe which contains
                             the type of the report ('10-K', '10-Q', ...)
            invert_negating: In a report, values are sometimes shown as positive even when
                             from a calculating perspective they are negative, and vice versa.
                             Such values are marked with a "1" in the negating column.
                             When this flag is true, the value of such tags where negating is set
                             to 1 is inverted. A typical case is "paiddividends" where the value
                             is shown as positive but from a calculating point of view it should
                             be negative.
        """

        self.flatten_index = flatten_index
        self.add_form_column = add_form_column
        self.invert_negating = invert_negating

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

        pre_num_df = databag.pre_num_df
        if self.invert_negating:
            pre_num_df = pre_num_df.copy()
            pre_num_df.loc[pre_num_df.negating == 1, 'value'] = -pre_num_df.value

        num_pre_pivot_df = pre_num_df.pivot_table(
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
        #  ensure column order, so that the latest date is first
        col_order = sorted(num_pre_pivot_df.columns.values, reverse=True)
        num_pre_pivot_df = num_pre_pivot_df[col_order]

        if self.flatten_index:
            num_pre_pivot_df.reset_index(drop=False, inplace=True)

        if self.add_form_column:
            adsh_form_map = \
                databag.sub_df[['adsh', 'form']].set_index('adsh').to_dict()['form']

            num_pre_pivot_df['form'] = num_pre_pivot_df['adsh'].map(adsh_form_map)

        return num_pre_pivot_df
