"""Contains the base implementation of the standardizer"""

from typing import List, Optional, Set

import numpy as np
import pandas as pd

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_validation_rules import ValidationRule


class Standardizer:
    """
    The Standardizer implements the base processing logic to standardize financial statements.

    """

    # this tags identify single statements in the final standardized table
    identifier_tags = ['adsh', 'coreg', 'report', 'ddate']

    # pivot columns
    pivot_df_index_cols = ['adsh', 'coreg', 'report', 'ddate', 'uom']

    def __init__(self,
                 pre_rule_tree: RuleGroup,
                 main_rule_tree: RuleGroup,
                 post_rule_tree: RuleGroup,
                 validation_rules: List[ValidationRule],
                 final_tags: List[str],
                 main_iterations: int = 2,
                 filter_for_main_statement: bool = True,
                 main_statement_tags: List[str] = None,
                 invert_negated: bool = True):
        """

        Args:
            pre_rule_tree: rules that are applied once before the main processing. These are mainly
                    rules that try to correct existing data from obvious errors (like wrong
                    tagging)
            main_rule_tree: rules that are applied during the main processing rule and which do the
                    heavy lifting. These rules can be excuted multipe times depending on the value
                    of the main_iterations parameter
            post_rule_tree: rules that are used to "cleanup", like setting certain values to
                    0.0
            validation_rules: Validation rules are applied after all rules were applied.
                   they add validation columns to the main dataset. Validation rules do check
                   if certain requirements are met. E.g. in a Balance Sheet, the following
                   equation should be true: Assets = AssetsCurrent + AssetsNoncurrent
            main_iterations: defining the number of iterations the main rules should be applied
            final_tags: tbd
            filter_for_main_statement (bool, Optional, True): depending on the data, it could look
                   as if multiple Balance Sheets statements could be present in a single report.
                   However, there should only be one. Setting
                   this flag to true (which is the default), tries to select the one that is most
                   likely the real statement.
                   the tags that are used are defined in the main_statement_tags parameter
            main_statement_tags: tbd
            invert_negated (bool, Optional, True): inverts the value of the that are marked
                   as negated.
        """
        self.pre_rule_tree = pre_rule_tree
        self.main_rule_tree = main_rule_tree
        self.post_rule_tree = post_rule_tree
        self.validation_rules = validation_rules
        self.main_statement_tags = main_statement_tags
        self.final_tags = final_tags
        self.main_iterations = main_iterations
        self.filter_for_main_statement = filter_for_main_statement
        self.invert_negated = invert_negated

        self.all_input_tags: Set[str] = self.pre_rule_tree.get_input_tags() | \
                                        self.main_rule_tree.get_input_tags()

        if filter_for_main_statement and (main_statement_tags is None):
            raise ValueError("if filter_for_main_statement is true, also the "
                             "main_statement_tags list has to be provided")

        self.final_col_order = self.identifier_tags + self.final_tags

        # contains the number of rows/statements/reports that are processed
        self.number_of_rows: int = -1

        # define log dataframes ..
        # .. a special log that shows the duplicated records that were found and removed
        self.preprocess_dupliate_log_df: Optional[pd.DataFrame] = None
        # .. the main_log that shows which rules were applied on which statement/row
        self.applied_rules_log_df: Optional[pd.DataFrame] = None
        # .. shows the total of how often a rule was applied, mainly counts the Trues per column
        #    in self.applied_rules_log_df
        self.applied_rules_sum_df: Optional[pd.DataFrame] = None
        # counts the nan values in the final-tag columns after preprocessing,
        # after every iteration, and after postprocessing. Gives an idea about how many
        # values were calculated.
        self.stats: Optional[pd.DataFrame] = None

    def _preprocess_deduplicate(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # find duplicated entries
        # sometimes, only single tags are duplicated, however, there are also reports
        # where all tags of a report are duplicated, maybe due to a problem with processing
        duplicates_s = \
            data_df.duplicated(
                ['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])

        self.preprocess_dupliate_log_df = data_df[duplicates_s] \
            [['adsh', 'coreg', 'report', 'tag', 'uom', 'version', 'ddate']].copy()

        return data_df[~duplicates_s]

    def _preprocess_pivot(self, data_df: pd.DataFrame, expected_tags: Set[str]) -> pd.DataFrame:
        # it is possible, that the same number appears multiple times in different lines,
        # therefore, duplicates have to be removed, otherwise pivot is not possible
        relevant_df = data_df[
            ['adsh', 'coreg', 'report', 'tag', 'value', 'uom', 'ddate']].drop_duplicates()

        pivot_df = relevant_df.pivot(index=self.pivot_df_index_cols,
                                     columns='tag',
                                     values='value')
        pivot_df.reset_index(inplace=True)
        missing_cols = set(expected_tags) - set(pivot_df.columns)
        for missing_col in missing_cols:
            pivot_df[missing_col] = np.nan
        pivot_df['nan_count'] = np.nan
        return pivot_df

    def _preprocess_filter_pivot_for_main_statement(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        """ Some reports have more than one 'report number' (column report) for a
            certain statement. Generally, the one with the most tags is the one to take.
            This method operates on the pivoted data and counts the none-values of the
            "main columns". The main columns are the fields, that generally should be there.
             """

        cpy_pivot_df = pivot_df.copy()

        cpy_pivot_df['nan_count'] = cpy_pivot_df[self.main_statement_tags].isna().sum(axis=1)
        cpy_pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)
        cpy_pivot_df = cpy_pivot_df.groupby(['adsh', 'coreg']).last()
        cpy_pivot_df.reset_index(inplace=True)
        return cpy_pivot_df

    def _preprocess(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # only select rows with tags that are actually used by the defined rules
        relevant_df = \
            data_df[['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line',
                'negating']][data_df.tag.isin(self.all_input_tags)]

        cpy_df = self._preprocess_deduplicate(relevant_df).copy()

        # invert the entries that have the negating flag set
        if self.invert_negated:
            cpy_df.loc[cpy_df.negating == 1, 'value'] = -cpy_df.value

        # pivot the table
        expected_tags = self.all_input_tags.union(self.final_tags)
        pivot_df = self._preprocess_pivot(data_df=cpy_df, expected_tags=expected_tags)

        if self.filter_for_main_statement:
            pivot_df = self._preprocess_filter_pivot_for_main_statement(pivot_df)

        # prepare the log dataframe -> it must have all rows
        self.applied_rules_log_df = pivot_df[self.pivot_df_index_cols].copy()

        # finally apply the pre-rules
        self.pre_rule_tree.set_id("PRE")
        self.pre_rule_tree.process(pivot_df, log_df=self.applied_rules_log_df)

        self.number_of_rows = len(pivot_df)

        # prepare the stats dataframe and calculate the stats after preprocessing
        pre_stats = self._calculate_stats(data_df=pivot_df, name="pre")

        self.stats = pd.DataFrame(pre_stats)
        self.stats['pre_rel'] = self.stats.pre / self.number_of_rows

        return pivot_df

    def _main_processing(self, data_df: pd.DataFrame):
        for i in range(self.main_iterations):
            # set ids of the rules in the tree, use the iteration number as prefix
            self.main_rule_tree.set_id(prefix=f"MAIN_{i}")

            # apply the rule_tree
            self.main_rule_tree.process(data_df=data_df, log_df=self.applied_rules_log_df)

            # calculate stats and add them to the stats log
            self._add_stats_entry(data_df=data_df, name=f'MAIN_{i}')

    def _post_processing(self, data_df: pd.DataFrame):
        self.post_rule_tree.set_id(prefix="POST")
        self.post_rule_tree.process(data_df=data_df, log_df=self.applied_rules_log_df)

        # calculate stats and add them to the stats log
        self._add_stats_entry(data_df=data_df, name='POST')

    def _finalize(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # create a meaningful order
        finalized_df = data_df[self.final_col_order].copy()

        # apply validation rules
        for validation_rule in self.validation_rules:
            validation_rule.validate(finalized_df)

        # caculate log_df summaries
        # filter for rule columns but making sure the order stays the same
        rule_columns = [x for x in self.applied_rules_log_df.columns if
                        x not in self.pivot_df_index_cols]
        self.applied_rules_sum_df = self.applied_rules_log_df[rule_columns].sum()

        return finalized_df

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        process the provided DataFrame
        Args:
            data_df:

        Returns:

        """
        ready_df = self._preprocess(data_df)
        self._main_processing(ready_df)
        self._post_processing(ready_df)

        return self._finalize(ready_df)

    def _add_stats_entry(self, data_df: pd.DataFrame, name: str):
        stats_entry = self._calculate_stats(data_df=data_df, name=name)

        self.stats = self.stats.join(stats_entry)
        self.stats[f'{stats_entry.name}_rel'] = \
            self.stats[stats_entry.name] / self.number_of_rows

        self.stats[f'{stats_entry.name}_red'] = \
            1 - (self.stats[stats_entry.name] / self.stats.pre)

    def _calculate_stats(self, data_df: pd.DataFrame, name: str) -> pd.Series:
        stats_s = data_df[self.final_tags].isna().sum(axis=0)
        stats_s.name = name
        return stats_s


# class RuleProcessorOld:
#     identifier_tags = ['adsh', 'coreg', 'report', 'ddate']
#
#     # todo
#     # missing
#     # preprocess
#     # rules
#     # group / list
#
#     def __init__(self,
#                  pre_rule_tree: RuleGroup,
#                  rule_tree: RuleGroup,
#                  post_rule_tree: RuleGroup,
#                  validation_rules: List[ValidationRule],
#                  iterations: int, main_tags: List[str], final_tags: List[str],
#                  filter_for_main_report: bool = True, invert_negated: bool = True):
#         self.rule_tree = rule_tree
#         self.clean_up_rule_tree = clean_up_rule_tree
#         self.validation_rules = validation_rules
#         self.iterations = iterations
#         self.main_tags = main_tags
#         self.final_tags = final_tags
#         self.filter_for_main_report = filter_for_main_report
#         self.invert_negated = invert_negated
#
#         self.final_col_order = self.identifier_tags + self.final_tags
#
#         self.preprocess_dupliate_log_df: Optional[pd.DataFrame] = None
#         self.log_df: Optional[pd.DataFrame] = None
#         self.applied_rules_sum_df: Optional[pd.DataFrame] = None
#         self.stats: Optional[pd.DataFrame] = None
#
#     def get_ruletree_descriptions(self):
#         # todo
#         pass
#
#     def process(self, df: pd.DataFrame) -> pd.DataFrame:
#         all_input_tags = self.rule_tree.get_input_tags()
#
#         relevant_df = \
#             df[['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line',
#                 'negating']][df.tag.isin(all_input_tags)]
#
#         # preprocessing ..
#         #  - deduplicate
#         cpy_df = self._deduplicate(relevant_df).copy()
#
#         #  - invert_negated
#         if self.invert_negated:
#             cpy_df.loc[cpy_df.negating == 1, 'value'] = -cpy_df.value
#
#         # pivot the table, so that the tags become columns
#         # todo: folgende Zeile sollte nicht mehr notwendig sein,
#         # sobald alle Regeln implementiert sind
#         expected_tags = all_input_tags.union(self.final_tags)
#         pivot_df = self._pivot(df=cpy_df, expected_tags=expected_tags)
#
#         if self.filter_for_main_report:
#             pivot_df = self._filter_pivot_for_main_reports(pivot_df)
#
#         pivot_df_index_cols = ['adsh', 'coreg', 'report', 'ddate', 'uom']
#         self.log_df = pivot_df[pivot_df_index_cols].copy()
#
#         total_length = len(pivot_df)
#         # calculate_pre_stats:
#         pre_apply_df = pivot_df[self.final_col_order]
#
#         self.pre_stats = self._calculate_stats(pivot_df=pre_apply_df)
#         self.pre_stats.name = "pre"
#
#         self.stats = pd.DataFrame(self.pre_stats)
#         self.stats['pre_rel'] = self.stats.pre / total_length
#
#         self.stats = pd.DataFrame(self.pre_stats)
#
#         for i in range(self.iterations):
#             # set ids of the rules in the tree, use the iteration number as prefix
#             self.rule_tree.set_id(parent_prefix=f"{i}_")
#
#             # apply the rule_tree
#             self.rule_tree.process(df=pivot_df, log_df=self.log_df)
#
#             self.post_stats = self._calculate_stats(pivot_df=pivot_df)
#             self.post_stats.name = f"post_{i}"
#
#             self.stats = self.stats.join(self.post_stats)
#             self.stats[self.post_stats.name + '_rel'] = \
#                 self.stats[self.post_stats.name] / total_length
#             self.stats[self.post_stats.name + '_red'] = \
#                 1 - (self.stats[self.post_stats.name] / self.stats.pre)
#
#         # clean up rules
#         self.clean_up_rule_tree.process(df=pivot_df, log_df=self.log_df)
#
#         # calculate cleanup stats
#         self.post_cleanup = self._calculate_stats(pivot_df=pivot_df)
#         self.post_cleanup.name = "cleanup"
#         self.stats = self.stats.join(self.post_cleanup)
#         self.stats[self.post_cleanup.name + '_rel'] = \
#             self.stats[self.post_cleanup.name] / total_length
#         self.stats[self.post_cleanup.name + '_red'] = \
#             1 - (self.stats[self.post_cleanup.name] / self.stats.pre)
#
#         # create a meaningful order
#         pivot_df = pivot_df[self.final_col_order].copy()
#
#         # apply validation rules
#         for validation_rule in self.validation_rules:
#             validation_rule.validate(pivot_df)
#
#         # caculate log_df summaries
#         if self.log_df is not None:
#             # filter for rule columns but making sure the order stays the same
#             rule_columns = [x for x in self.log_df.columns if x not in pivot_df_index_cols]
#             self.applied_rules_sum_df = self.log_df[rule_columns].sum()
#
#         return pivot_df
#
#     def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
#         # find duplicated entries
#         # sometimes, only single tags are duplicated, however, there are also reports
#         # where all tags of a report are duplicated, maybe due to a problem with processing
#         duplicates_s = \
#             df.duplicated(['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])
#
#         self.preprocess_dupliate_log_df = df[duplicates_s] \
#             [['adsh', 'coreg', 'report', 'tag', 'uom', 'version', 'ddate']].copy()
#
#         return df[~duplicates_s]
#
#     def _filter_pivot_for_main_reports(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
#         """ Some reports have more than one 'report number' (column report) for a
#             certain statement. Generally, the one with the most tags is the one to take.
#             This method operates on the pivoted data and counts the none-values of the
#             "main columns". The main columns are the fields, that generally should be there.
#              """
#
#         cpy_pivot_df = pivot_df.copy()
#
#         cpy_pivot_df['nan_count'] = cpy_pivot_df[self.main_tags].isna().sum(axis=1)
#         cpy_pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)
#         cpy_pivot_df = cpy_pivot_df.groupby(['adsh', 'coreg']).last()
#         cpy_pivot_df.reset_index(inplace=True)
#         return cpy_pivot_df
#
#     def _calculate_stats(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
#         return pivot_df[self.final_tags].isna().sum(axis=0)
#
#     def _pivot(self, df: pd.DataFrame, expected_tags: Set[str]) -> pd.DataFrame:
#         # it is possible, that the same number appears multiple times in different lines,
#         # therefore, duplicates have to be removed, otherwise pivot is not possible
#         relevant_df = df[
#             ['adsh', 'coreg', 'report', 'tag', 'value', 'uom', 'ddate']].drop_duplicates()
#
#         pivot_df = relevant_df.pivot(index=['adsh', 'coreg', 'report', 'uom', 'ddate'],
#                                      columns='tag',
#                                      values='value')
#         pivot_df.reset_index(inplace=True)
#         missing_cols = set(expected_tags) - set(pivot_df.columns)
#         for missing_col in missing_cols:
#             pivot_df[missing_col] = np.nan
#         pivot_df['nan_count'] = np.nan
#         return pivot_df
