"""Contains the base implementation of the standardizer"""

from typing import List, Optional, Set

import numpy as np
import pandas as pd

from secfsdstools.f_standardize.base_rule_framework import RuleGroup
from secfsdstools.f_standardize.base_validation_rules import ValidationRule


class Stats:
    """
    Simple class to hold the process statics. This class contains
    the information about how many nan entries are present after every processing step.
    """

    def __init__(self, tags: List[str]):
        """
        Args:
            tags (List[str]): list of tags to count
        """
        self.tags = tags

        # counts the nan values in the final-tag columns after preprocessing,
        # after every iteration, and after postprocessing. Gives an idea about how many
        # values were calculated.
        self.stats: Optional[pd.DataFrame] = None

    def initialize(self, data_df: pd.DataFrame, process_step_name: str):
        """
        initializes the internal dataframe with the first process step
        Args:
            data_df: dataframe with the data to count
            name: name of the process step
        """

        # prepare the stats dataframe and calculate the stats after preprocessing
        init_stats = self._calculate_stats(data_df=data_df, name=process_step_name)
        self.stats = pd.DataFrame(init_stats)

    def add_stats_entry(self, data_df: pd.DataFrame, process_step_name: str):
        """
        adds the stats for the provided process_step_name.
        Args:
            data_df: dataframe with the data to count
            process_step_name: name of the process step
        """
        stats_entry = self._calculate_stats(data_df=data_df, name=process_step_name)
        self.stats = self.stats.join(stats_entry)

    def _calculate_stats(self, data_df: pd.DataFrame, name: str) -> pd.Series:
        stats_s = data_df[self.tags].isna().sum(axis=0)
        stats_s.name = name
        return stats_s

    def finalize_stats(self, data_length: int):
        """ finalize the stats. Adds a relative and a gain column for every process step.
            The relative row contains relative amount of nan values compared to the
            number of rows. The gain column contains the relative reduction compared
            to the previous step.
        """

        # finalize the stats table, adding the rel and the gain columns
        final_stats_columns = []
        previous_rel_colum = None
        for stats_column in self.stats.columns:
            rel_column = f'{stats_column}_rel'
            final_stats_columns.extend([stats_column, rel_column])
            self.stats[rel_column] = self.stats[stats_column] / data_length
            if previous_rel_colum is not None:
                gain_col_name = f'{stats_column}_gain'
                final_stats_columns.append(gain_col_name)
                self.stats[gain_col_name] = \
                    self.stats[previous_rel_colum] - self.stats[rel_column]
            previous_rel_colum = rel_column

        # ensure there is a meaningful order
        self.stats = self.stats[final_stats_columns]


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

        # define log dataframes ..
        # .. a special log that shows the duplicated records that were found and removed
        self.preprocess_dupliate_log_df: Optional[pd.DataFrame] = None
        # .. the main_log that shows which rules were applied on which statement/row
        self.applied_rules_log_df: Optional[pd.DataFrame] = None
        # .. shows the total of how often a rule was applied, mainly counts the Trues per column
        #    in self.applied_rules_log_df
        self.applied_rules_sum_df: Optional[pd.DataFrame] = None

        self.stats = Stats(self.final_tags)

    def _preprocess_deduplicate(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # find duplicated entries
        # sometimes, only single tags are duplicated, however, there are also reports
        # where all tags of a report are duplicated.

        duplicates_s = \
            data_df.duplicated(
                ['adsh', 'coreg', 'report', 'uom', 'tag', 'version', 'ddate', 'value'])

        self.preprocess_dupliate_log_df = data_df[duplicates_s] \
            [['adsh', 'coreg', 'report', 'tag', 'uom', 'version', 'ddate']].copy()

        return data_df[~duplicates_s]

    def _preprocess_pivot(self, data_df: pd.DataFrame, expected_tags: Set[str]) -> pd.DataFrame:
        pivot_df = data_df.pivot(index=self.pivot_df_index_cols,
                                     columns='tag',
                                     values='value')

        pivot_df.reset_index(inplace=True)

        missing_cols = set(expected_tags) - set(pivot_df.columns)
        for missing_col in missing_cols:
            pivot_df[missing_col] = np.nan

        return pivot_df

    def _preprocess_filter_pivot_for_main_statement(self, pivot_df: pd.DataFrame) -> pd.DataFrame:
        """ Some reports have more than one 'report number' (column report) for a
            certain statement. Generally, the one with the most tags is the one to take.
            This method operates on the pivoted data and counts the none-values of the
            "main columns". The main columns are the fields, that generally are expected
            in the processed statement.
             """

        cpy_pivot_df = pivot_df.copy()
        cpy_pivot_df['nan_count'] = cpy_pivot_df[self.main_statement_tags].isna().sum(axis=1)

        cpy_pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)

        filtered_pivot_df = cpy_pivot_df.groupby(['adsh', 'coreg']).first()
        filtered_pivot_df.reset_index(inplace=True)
        return filtered_pivot_df

    def _preprocess(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # only select rows with tags that are actually used by the defined rules
        relevant_df = \
            data_df[['adsh', 'coreg', 'tag', 'version', 'ddate', 'uom', 'value', 'report', 'line',
                     'negating']][data_df.tag.isin(self.all_input_tags)]

        # deduplicate
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

        # prepare the stats dataframe and calculate the stats after preprocessing
        self.stats.initialize(data_df=pivot_df, process_step_name="pre")

        return pivot_df

    def _main_processing(self, data_df: pd.DataFrame):
        for i in range(self.main_iterations):
            # set ids of the rules in the tree, use the iteration number as prefix
            self.main_rule_tree.set_id(prefix=f"MAIN_{i}")

            # apply the rule_tree
            self.main_rule_tree.process(data_df=data_df, log_df=self.applied_rules_log_df)

            # calculate stats and add them to the stats log
            self.stats.add_stats_entry(data_df=data_df, process_step_name=f'MAIN_{i}')

    def _post_processing(self, data_df: pd.DataFrame):
        self.post_rule_tree.set_id(prefix="POST")
        self.post_rule_tree.process(data_df=data_df, log_df=self.applied_rules_log_df)

        # calculate stats and add them to the stats log
        self.stats.add_stats_entry(data_df=data_df, process_step_name='POST')

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

        # finalize the stats table, adding the rel and the gain columns
        self.stats.finalize_stats(len(data_df))

        return finalized_df

    def process(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        process the provided DataFrame
        Args:
            data_df: input dataframe

        Returns:
            pd.DataFrame: the standardized results

        """
        ready_df = self._preprocess(data_df)
        self._main_processing(ready_df)
        self._post_processing(ready_df)

        return self._finalize(ready_df)
