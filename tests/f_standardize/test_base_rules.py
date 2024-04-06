import numpy as np
import pandas as pd

from secfsdstools.f_standardize.base_rules import CopyTagRule, MissingSumRule, MissingSummandRule, \
    SumUpRule, SetSumIfOnlyOneSummand, PostCopyToFirstSummand, PreSumUpCorrection, \
    missingsumparts_rules_creator, setsumifonlyonesummand_rules_creator, SubtractFromRule


def test_missingsumparts_rules_creator():
    rules = missingsumparts_rules_creator(sum_tag='sumtag', summand_tags=['summand1', 'summand2'])

    assert len(rules) == 3
    assert isinstance(rules[0], MissingSumRule)
    assert isinstance(rules[1], MissingSummandRule)
    assert isinstance(rules[2], MissingSummandRule)


def test_setsumifonlyonesummand_rules_creator():
    rules = setsumifonlyonesummand_rules_creator(sum_tag='sum_tag',
                                                 summand_tags=['summand1', 'summand2'])

    assert len(rules) == 2
    assert isinstance(rules[0], SetSumIfOnlyOneSummand)
    assert isinstance(rules[1], SetSumIfOnlyOneSummand)


def test_presumupcolrrection_rule():
    rule = PreSumUpCorrection(sum_tag='sumtag', mixed_up_summand='mixedup',
                              other_summand='other')

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'mixedup', 'other'}
    assert rule.get_target_tags() == ['sumtag', 'mixedup']
    assert rule.identifier == 'R_sumtag/mixedup'

    # col1: sumtag, cal2: mixedup, col3: other
    data = [[1, 11, 10],
            [22, 2, 20],
            [33, 33, 0.0],
            [44, 44, np.nan],
            [np.nan, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'mixedup', 'other'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df[rule.identifier].tolist() == [True, False, False, False, False]
    assert df.sumtag.tolist()[0] == 11
    assert df.mixedup.tolist()[0] == 1
    assert df.other.tolist()[0] == 10


def test_rename_rule():
    rule = CopyTagRule(original='original', target='target')
    rule.set_id("R")

    assert rule.get_input_tags() == {'original', 'target'}
    assert rule.get_target_tags() == ['target']
    assert rule.identifier == 'R_target'

    # data for the original column, last index is nan -> no action should be taken
    data = [1, 2, 3, 4, 5, np.nan]

    df = pd.DataFrame(data, columns=['original'])
    df['target'] = np.nan
    # set the target of first row to a value, so that no action should be taken
    df.loc[df.original == 1, 'target'] = 2

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

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
    assert rule.identifier == 'R_sumtag'

    # col1: sumtag, cal2: summand1, col3: summand2
    data = [[11, 1, 10],
            [np.nan, 2, 20],
            [np.nan, 3, 30],
            [np.nan, 4, np.nan],
            [np.nan, np.nan, 50],
            [np.nan, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'summand1', 'summand2'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

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
    assert rule.identifier == 'R_missingsummand'

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
    rule.process(data_df=df, log_df=log_df)
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
    assert rule.identifier == 'R_sumtag'

    # col1: sumtag, cal2: summand1, col3: summand2
    data = [[11, 1, 10],
            [np.nan, 2, np.nan],
            [np.nan, np.nan, 30],
            [np.nan, np.nan, np.nan],
            [99, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'summand1', 'summand2'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

    # check the values of the target column
    assert df.sumtag.tolist()[0] == 11.0
    assert df.sumtag.tolist()[1] == 2.0
    assert df.sumtag.tolist()[2] == 30.0
    assert np.isnan(df.sumtag.tolist()[3])
    assert df.sumtag.tolist()[4] == 99.0

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_sumtag.tolist() == [False, True, True, False, False]


def test_sumup_rule_with_optional():
    rule = SumUpRule(sum_tag='sumtag',
                     potential_summands=['summand1', 'summand2'],
                     optional_summands=['optsummand'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'summand1', 'summand2', 'optsummand'}
    assert rule.get_target_tags() == ['sumtag']
    assert rule.identifier == 'R_sumtag'

    # col1: sumtag, cal2: summand1, col3: summand2, col4: optsummand
    data = [[11, 1, 10, 0],
            [np.nan, 2, np.nan, 5],
            [np.nan, np.nan, 30, 7],
            [np.nan, np.nan, np.nan, 8],
            [np.nan, np.nan, np.nan, np.nan],
            [99, np.nan, np.nan]]

    df = pd.DataFrame(data, columns=['sumtag', 'summand1', 'summand2', 'optsummand'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

    # check the values of the target column
    assert df.sumtag.tolist()[0] == 11.0
    assert df.sumtag.tolist()[1] == 7.0
    assert df.sumtag.tolist()[2] == 37.0
    assert np.isnan(df.sumtag.tolist()[3])
    assert np.isnan(df.sumtag.tolist()[4])
    assert df.sumtag.tolist()[5] == 99.0

    # check the log column, in order to test if the rule was applied to the right rows
    assert log_df.R_sumtag.tolist() == [False, True, True, False, False, False]


def test_setsumifonlyonesummand():
    rule = SetSumIfOnlyOneSummand(sum_tag='sumtag',
                                  summand_set='summandset',
                                  summands_nan=['summandnan1', 'summandnan2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'summandset', 'summandnan1', 'summandnan2'}
    assert rule.get_target_tags() == ['sumtag', 'summandnan1', 'summandnan2']
    assert rule.identifier == 'R_sumtag/summandnan1/summandnan2'

    # cal1: sumtag, col2: summandset, col3: summandnan1, col4: summandnan2
    data = [[12, 1, np.nan, np.nan],
            [np.nan, 1, np.nan, np.nan],
            [np.nan, 1, 3, np.nan],
            [np.nan, 1, np.nan, 4],
            [np.nan, np.nan, np.nan, np.nan],
            [13, np.nan, np.nan],
            ]

    df = pd.DataFrame(data, columns=['sumtag', 'summandset', 'summandnan1', 'summandnan2'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

    assert df.sumtag.tolist()[0] == 12.0
    assert df.sumtag.tolist()[1] == 1.0
    assert np.isnan(df.sumtag.tolist()[2])
    assert np.isnan(df.sumtag.tolist()[3])
    assert np.isnan(df.sumtag.tolist()[4])
    assert df.sumtag.tolist()[5] == 13.0

    assert df.summandnan1.tolist()[1] == 0.0
    assert df.summandnan2.tolist()[1] == 0.0

    assert log_df['R_sumtag/summandnan1/summandnan2'].tolist() == [False, True, False, False, False,
                                                                   False]


def test_postupcopytofirstsummandrule():
    rule = PostCopyToFirstSummand(sum_tag="sumtag", first_summand="firstsummand",
                                  other_summands=['othersummand1', 'othersummand2'])

    rule.set_id("R")

    assert rule.get_input_tags() == {'sumtag', 'firstsummand', 'othersummand1', 'othersummand2'}
    assert rule.get_target_tags() == ['firstsummand', 'othersummand1', 'othersummand2']
    assert rule.identifier == 'R_firstsummand/othersummand1/othersummand2'

    # cal1: sumtag, col2: firstsummand, col3: othersummand1, col4: othersummand2
    data = [
        [np.nan, np.nan, np.nan, np.nan],
        [12, np.nan, np.nan, np.nan],
        [12, 1, np.nan, np.nan],
        [12, np.nan, 2, np.nan],
        [12, np.nan, np.nan, 3],
        [np.nan, 1, np.nan, 4]
    ]

    df = pd.DataFrame(data, columns=['sumtag', 'firstsummand', 'othersummand1', 'othersummand2'])

    log_df = df.copy()
    rule.process(data_df=df, log_df=log_df)

    assert df.firstsummand.isna().tolist() == [True, False, False, True, True, False]
    assert df.firstsummand.tolist()[1] == 12
    assert df.firstsummand.tolist()[2] == 1
    assert df.firstsummand.tolist()[5] == 1

    assert df.othersummand1.isna().tolist() == [True, False, True, False, True, True]
    assert df.othersummand1.tolist()[1] == 0.0
    assert df.othersummand1.tolist()[3] == 2.0

    assert df.othersummand2.isna().tolist() == [True, False, True, True, False, False]
    assert df.othersummand2.tolist()[1] == 0.0
    assert df.othersummand2.tolist()[4] == 3.0
    assert df.othersummand2.tolist()[5] == 4.0

    assert log_df['R_firstsummand/othersummand1/othersummand2'].tolist() == \
           [False, True, False, False, False, False]


def test_do_not_subtract_values_and_store_result():
    # Arrange
    target_tag = "target"
    subtract_from_tag = "subtract_from"
    potential_subtract_tags = ["potential1", "potential2"]
    data_df = pd.DataFrame({
        "target": [5, 10, 15],
        "subtract_from": [10, 20, None],
        "potential1": [5, None, None],
        "potential2": [None, 3, None]
    })
    rule = SubtractFromRule(target_tag, subtract_from_tag, potential_subtract_tags)

    # Act
    rule.process(data_df)

    # Assert
    assert data_df["target"].tolist() == [5, 10, 15]

def test_subtract_values_and_store_result():
    # Arrange
    target_tag = "target"
    subtract_from_tag = "subtract_from"
    potential_subtract_tags = ["potential1", "potential2"]
    data_df = pd.DataFrame({
        "target": [None, None, None],
        "subtract_from": [10, 20, None],
        "potential1": [5, None, None],
        "potential2": [None, 3, None]
    })
    rule = SubtractFromRule(target_tag, subtract_from_tag, potential_subtract_tags)

    # Act
    rule.process(data_df)

    # Assert
    assert data_df["target"].tolist() == [5, 17, None]