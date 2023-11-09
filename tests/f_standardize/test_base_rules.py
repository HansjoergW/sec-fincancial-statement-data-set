import numpy as np
import pandas as pd

from secfsdstools.f_standardize.base_rules import CopyTagRule, MissingSumRule, MissingSummandRule, \
    SumUpRule, SetSumIfOnlyOneSummand


def test_rename_rule():
    rule = CopyTagRule(original='original', target='target')
    rule.set_id("R")

    assert rule.get_input_tags() == {'original', 'target'}
    assert rule.get_target_tags() == ['target']
    assert rule.id == 'R_target'

    # data for the original column, last index is nan -> no action should be taken
    data = [1, 2, 3, 4, 5, np.nan]

    df = pd.DataFrame(data, columns=['original'])
    df['target'] = np.nan
    # set the target of first row to a value, so that no action should be taken
    df.loc[df.original == 1, 'target'] = 2

    log_df = df.copy()
    rule.process(df=df, log_df=log_df)

    # check the values of the target column
    assert df.target.tolist()[0] == 2.0
    assert df.target.tolist()[1] == 2.0
    assert df.target.tolist()[2] == 3.0
    assert df.target.tolist()[3] == 4.0
    assert df.target.tolist()[4] == 5.0
    assert np.isnan(df.target.tolist()[5])

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_target.tolist() == [False, True, True, True, True, False]


def test_missingsum_rule():
    rule = MissingSumRule(sum_tag='sumtag',
                          summand_tags=['summand1', 'summand2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'summand1', 'summand2'}
    assert rule.get_target_tags() == ['sumtag']
    assert rule.id == 'R_sumtag'

    # col1: sumtag, cal2: summand1, col3: summand2
    data = [[11, 1, 10],
            [np.nan, 2, 20],
            [np.nan, 3, 30],
            [np.nan, 4, np.nan],
            [np.nan, np.nan, 50],
            [np.nan, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'summand1', 'summand2'])

    log_df = df.copy()
    rule.process(df=df, log_df=log_df)

    # check the values of the target column
    assert df.sumtag.tolist()[0] == 11.0
    assert df.sumtag.tolist()[1] == 22.0
    assert df.sumtag.tolist()[2] == 33.0
    assert np.isnan(df.sumtag.tolist()[3])
    assert np.isnan(df.sumtag.tolist()[4])
    assert np.isnan(df.sumtag.tolist()[5])

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_sumtag.tolist() == [False, True, True, False, False, False]


def test_missingsummand_rule():
    rule = MissingSummandRule(sum_tag='sumtag',
                              missing_summand_tag='missingsummand',
                              existing_summands_tags=['summand1', 'summand2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'missingsummand', 'summand1', 'summand2'}
    assert rule.get_target_tags() == ['missingsummand']
    assert rule.id == 'R_missingsummand'

    print(rule.get_description())

    # cal1: sumtag, col2: missingsummand, col3: summand1, col4: summand2
    data = [[12, 1, 1, 10],
            [24, np.nan, 2, 20],
            [36, np.nan, 3, 30],
            [48, np.nan, 4, 40],
            [60, np.nan, 5, 50],
            [72, np.nan, np.nan, 60],
            [84, np.nan, 7, np.nan],
            [np.nan, np.nan, 8, 80],
            ]

    df = pd.DataFrame(data, columns=['sumtag', 'missingsummand', 'summand1', 'summand2'])

    log_df = df.copy()
    rule.process(df=df, log_df=log_df)
    assert df.missingsummand.tolist()[0] == 1.0
    assert df.missingsummand.tolist()[1] == 2.0
    assert df.missingsummand.tolist()[2] == 3.0
    assert df.missingsummand.tolist()[3] == 4.0
    assert df.missingsummand.tolist()[4] == 5.0
    assert np.isnan(df.missingsummand.tolist()[5])
    assert np.isnan(df.missingsummand.tolist()[6])
    assert np.isnan(df.missingsummand.tolist()[7])

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_missingsummand.tolist() == \
           [False, True, True, True, True, False, False, False]


def test_sumup_rule():
    rule = SumUpRule(sum_tag='sumtag',
                     potential_summands=['summand1', 'summand2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'summand1', 'summand2'}
    assert rule.get_target_tags() == ['sumtag']
    assert rule.id == 'R_sumtag'

    # col1: sumtag, cal2: summand1, col3: summand2
    data = [[11, 1, 10],
            [np.nan, 2, np.nan],
            [np.nan, np.nan, 30],
            [np.nan, np.nan, np.nan],
            [99, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'summand1', 'summand2'])

    log_df = df.copy()
    rule.process(df=df, log_df=log_df)

    # check the values of the target column
    assert df.sumtag.tolist()[0] == 11.0
    assert df.sumtag.tolist()[1] == 2.0
    assert df.sumtag.tolist()[2] == 30.0
    assert df.sumtag.tolist()[3] == 0.0
    assert df.sumtag.tolist()[4] == 99.0

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_sumtag.tolist() == [False, True, True, True, False]


def test_setsumifonlyonesummand():
    rule = SetSumIfOnlyOneSummand(sum_tag='sumtag',
                                  summand_set='summandset',
                                  summands_nan=['summandnan1', 'summandnan2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'summandset', 'summandnan1', 'summandnan2'}
    assert rule.get_target_tags() == ['sumtag', 'summandnan1', 'summandnan2']
    assert rule.id == 'R_sumtag/summandnan1/summandnan2'

    print(rule.get_description())

    ... continue
