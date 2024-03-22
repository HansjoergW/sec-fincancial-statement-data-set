"""Contains the base implementation of the standardizer"""
import os
from typing import List, Optional, Set, TypeVar

import numpy as np
import pandas as pd

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_presenter.presenting import Presenter
from secfsdstools.f_standardize.base_rule_framework import RuleGroup, DescriptionEntry, PrePivotRule
from secfsdstools.f_standardize.base_validation_rules import ValidationRule

STANDARDIZED = TypeVar('STANDARDIZED', bound='StandardizedBag')


class StandardizedBag:
    """
    A class to contain the results of a standardizer.
    """

    def __init__(self,
                 result_df: pd.DataFrame,
                 applied_prepivot_rules_log_df: pd.DataFrame,
                 applied_rules_log_df: pd.DataFrame,
                 stats_df: pd.DataFrame,
                 applied_rules_sum_s: pd.Series,
                 validation_overview_df: pd.DataFrame,
                 process_description_df: pd.DataFrame):

        self.result_df = result_df
        self.applied_prepivot_rules_log_df = applied_prepivot_rules_log_df
        self.applied_rules_log_df = applied_rules_log_df
        self.stats_df = stats_df
        self.applied_rules_sum_s = applied_rules_sum_s
        self.validation_overview_df = validation_overview_df
        self.process_description_df = process_description_df

    def save(self, target_path: str):
        """
        Stores the last result and the log dataframesunder the given directory.
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

        self.result_df.to_parquet(os.path.join(target_path, 'result.parquet'))
        self.applied_prepivot_rules_log_df.to_parquet(
            os.path.join(target_path, 'applied_prepivot_rules_log.parquet'))
        self.applied_rules_log_df.to_parquet(os.path.join(target_path, 'applied_rules_log.parquet'))
        self.stats_df.to_parquet(os.path.join(target_path, 'stats.parquet'))
        self.applied_rules_sum_s.to_csv(os.path.join(target_path, 'applied_rules_sum.csv'))
        self.validation_overview_df.to_parquet(
            os.path.join(target_path, 'validation_overview.parquet'))
        self.process_description_df.to_parquet(
            os.path.join(target_path, 'process_description.parquet'))

    @staticmethod
    def load(target_path: str) -> STANDARDIZED:
        """
        Loads the content of the bag at the specified location.

        Args:
            target_path: the directory which contains the parquet files

        Returns:
            STANDARDIZED: the loaded Databag
        """

        result_df = pd.read_parquet(os.path.join(target_path, 'result.parquet'))
        applied_prepivot_rules_log_df = pd.read_parquet(
            os.path.join(target_path, 'applied_prepivot_rules_log.parquet'))
        applied_rules_log_df = pd.read_parquet(
            os.path.join(target_path, 'applied_rules_log.parquet'))
        stats_df = pd.read_parquet(os.path.join(target_path, 'stats.parquet'))
        applied_rules_sum_s = pd.read_csv(
            os.path.join(target_path, 'applied_rules_sum.csv'),
            header=None, index_col=0, squeeze=True)
        validation_overview_df = pd.read_parquet(
            os.path.join(target_path, 'validation_overview.parquet'))
        process_description_df = pd.read_parquet(
            os.path.join(target_path, 'process_description.parquet'))

        return StandardizedBag(result_df=result_df,
                               applied_prepivot_rules_log_df=applied_prepivot_rules_log_df,
                               applied_rules_log_df=applied_rules_log_df, stats_df=stats_df,
                               applied_rules_sum_s=applied_rules_sum_s,
                               validation_overview_df=validation_overview_df,
                               process_description_df=process_description_df)


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


class Standardizer(Presenter[JoinedDataBag]):
    """
    The Standardizer implements the base processing logic to standardize financial statements.
    """

    # this tags identify single statements in the final standardized table
    identifier_cols = ['adsh', 'coreg', 'report', 'ddate', 'uom', 'qtrs']
    duplicate_log_cols = ['adsh', 'coreg', 'report', 'ddate', 'uom', 'qtrs', 'tag', 'version']

    def __init__(self,
                 prepivot_rule_tree: RuleGroup,
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
            prepivot_rule_tree: rules that are applied before the data is pivoted. These are rules
                    that filter (like deduplicate) or correct values.
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
        self.prepivot_rule_tree = prepivot_rule_tree
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
                                        self.main_rule_tree.get_input_tags() | \
                                        set(final_tags)

        if filter_for_main_statement and (main_statement_tags is None):
            raise ValueError("if filter_for_main_statement is true, also the "
                             "main_statement_tags list has to be provided")

        self.final_col_order = self.identifier_cols + self.final_tags

        # attribute to store the last result of calling the process method
        self.result: Optional[pd.DataFrame] = None

        # define log dataframes ..
        # a special log that logs which prepivot rules were applied
        self.applied_prepivot_rules_log_df: Optional[pd.DataFrame] = None
        # .. the main_log that shows which rules were applied on which statement/row
        self.applied_rules_log_df: Optional[pd.DataFrame] = None
        # .. shows the total of how often a rule was applied, mainly counts the Trues per column
        #    in self.applied_rules_log_df
        self.applied_rules_sum_s: Optional[pd.Series] = None
        self.validation_overview_df: Optional[pd.DataFrame] = None

        self.stats = Stats(self.final_tags)

    def _preprocess_deduplicate(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # find duplicated entries
        # sometimes, only single tags are duplicated, however, there are also reports
        # where all tags of a report are duplicated.

        relevant_tags_duplication = self.identifier_cols + ['tag', 'version', 'value']
        duplicates_s = data_df.duplicated(relevant_tags_duplication)

        self.preprocess_duplicate_log_df = data_df[duplicates_s][self.duplicate_log_cols].copy()

        return data_df[~duplicates_s]

    def _preprocess_pivot(self, data_df: pd.DataFrame, expected_tags: Set[str]) -> pd.DataFrame:
        pivot_df = data_df.pivot(index=self.identifier_cols,
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
        available_main_statements = set(cpy_pivot_df.columns.tolist()).intersection(set(self.main_statement_tags))
        cpy_pivot_df['nan_count'] = cpy_pivot_df[available_main_statements].isna().sum(axis=1)

        cpy_pivot_df.sort_values(['adsh', 'coreg', 'nan_count'], inplace=True)

        filtered_pivot_df = cpy_pivot_df.groupby(['adsh', 'coreg']).first()
        filtered_pivot_df.reset_index(inplace=True)
        return filtered_pivot_df

    def _preprocess(self, data_df: pd.DataFrame) -> pd.DataFrame:
        # only select rows with tags that are actually used by the defined rules

        relevant_pivot_cols = \
            self.identifier_cols + ['tag', 'version',  'value', 'line', 'negating']

        relevant_df = \
            data_df[relevant_pivot_cols][data_df.tag.isin(self.all_input_tags)]

        # invert the entries that have the negating flag set
        if self.invert_negated:
            relevant_df.loc[relevant_df.negating == 1, 'value'] = -relevant_df.value

        # apply prepivot_rule_tree
        self.applied_prepivot_rules_log_df = pd.DataFrame(columns=(PrePivotRule.index_cols + ['id']))
        self.prepivot_rule_tree.set_id("PREPIVOT")
        self.prepivot_rule_tree.process(data_df=relevant_df,
                                        log_df=self.applied_prepivot_rules_log_df)

        # pivot the table
        pivot_df = self._preprocess_pivot(data_df=relevant_df, expected_tags=self.all_input_tags)

        if self.filter_for_main_statement:
            pivot_df = self._preprocess_filter_pivot_for_main_statement(pivot_df)

        # prepare the log dataframe -> it must have all rows
        self.applied_rules_log_df = pivot_df[self.identifier_cols].copy()

        # finally apply the pre-rules
        self.pre_rule_tree.set_id("PRE")
        self.pre_rule_tree.process(pivot_df, log_df=self.applied_rules_log_df)

        # prepare the stats dataframe and calculate the stats after preprocessing
        self.stats.initialize(data_df=pivot_df, process_step_name="pre")

        return pivot_df

    def _main_processing(self, data_df: pd.DataFrame):
        for i in range(self.main_iterations):
            # apply the main rule tree
            self.main_rule_tree.set_id(prefix=f"MAIN_{i + 1}")
            self.main_rule_tree.process(data_df=data_df, log_df=self.applied_rules_log_df)

            # calculate stats and add them to the stats log
            self.stats.add_stats_entry(data_df=data_df, process_step_name=f'MAIN_{i + 1}')

    def _post_processing(self, data_df: pd.DataFrame):
        # apply the post rule tree
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

        cat_cols = [x for x in finalized_df.columns if x.endswith("_cat")]
        self.validation_overview_df = pd.DataFrame(index=[0, 1, 5, 10, 100], columns=cat_cols)

        for column in cat_cols:
            self.validation_overview_df[column] = finalized_df[column].value_counts()

        for column in self.validation_overview_df.columns:
            new_column_name = column + '_pct'
            self.validation_overview_df = (
                self.validation_overview_df.assign(
                    **{new_column_name: (100 * self.validation_overview_df[column] / len(
                        finalized_df)).round(2)}))

        # calculate log_df summaries
        # filter for rule columns but making sure the order stays the same
        rule_columns = [x for x in self.applied_rules_log_df.columns if
                        x not in self.identifier_cols]
        self.applied_rules_sum_s = self.applied_rules_log_df[rule_columns].sum()

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

        self.result = self._finalize(ready_df)
        return self.result

    def get_process_description(self) -> pd.DataFrame:
        """
            returns the description of the applied rules as a table in a pandas dataframe
        Returns:
            pd.DataFrame: a table with the description of the applied rules
        """

        all_descriptions: List[DescriptionEntry] = []

        self.pre_rule_tree.set_id("PRE")
        all_descriptions.extend(self.pre_rule_tree.collect_description("PRE"))
        self.main_rule_tree.set_id("MAIN")
        all_descriptions.extend(self.main_rule_tree.collect_description("MAIN"))
        self.post_rule_tree.set_id("POST")
        all_descriptions.extend(self.post_rule_tree.collect_description("POST"))

        all_descriptions.extend([vr.collect_description("VALID") for vr in self.validation_rules])

        return pd.DataFrame(all_descriptions)

    def present(self, databag: JoinedDataBag) -> pd.DataFrame:
        """
        implements a presenter which reformats the bag into standardized dataframe.

        It also merges the main attributes from the sub_df to the result
        (cik, name, form, fye, fy, fp) and
        creates a date column based on the ddated value.

        Note: as name always the latest available name is used.

        Args:
            databag (T): the bag to transform into the presentation df

        Returns:
            pd.DataFrame: the data to be presented
        """
        standardized_df = self.process(databag.pre_num_df)

        data_to_merge_df = databag.sub_df[['adsh', 'cik', 'form', 'fye', 'fy', 'fp']].copy()

        # The name of a company can change during its liftime. However, we want to have the
        # same name for the same cik in all entries. Therefore, we first have to find the
        # latest name of the company.

        # first, create a cik-name look up table,
        df_latest = (databag.sub_df[['cik', 'name', 'period']].sort_values('period')
                     .drop_duplicates('cik', keep='last'))

        # Create the dictionary
        cik_name_dict = dict(zip(df_latest['cik'], df_latest['name']))

        data_to_merge_df['name'] = data_to_merge_df['cik'].map(cik_name_dict)

        merged_df = pd.merge(data_to_merge_df, standardized_df, on='adsh', how='inner')

        # create the date column and sort by date
        merged_df['date'] = pd.to_datetime(merged_df['ddate'], format='%Y%m%d')

        # sort the columns
        merged_df = merged_df[['adsh', 'cik', 'name', 'form', 'fye', 'fy', 'fp', 'date'] +
                              standardized_df.columns.tolist()[1:]]

        # store it in the standardizer object as new result
        self.result = merged_df.sort_values(by='date')

        return self.result

    def get_standardize_bag(self) -> StandardizedBag:
        """
            returns an instance of StandardizedBag with all the calculated
            information. the bag can then be used to directly save the information
            on disk and reload it for later analysis
        """
        return StandardizedBag(result_df=self.result,
                               applied_rules_log_df=self.applied_rules_log_df,
                               applied_prepivot_rules_log_df=self.applied_prepivot_rules_log_df,
                               stats_df=self.stats.stats,
                               applied_rules_sum_s=self.applied_rules_sum_s,
                               validation_overview_df=self.validation_overview_df,
                               process_description_df=self.get_process_description())
