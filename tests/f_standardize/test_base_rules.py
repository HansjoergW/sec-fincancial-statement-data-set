import numpy as np
import pandas as pd

from secfsdstools.f_standardize.base_rules import CopyTagRule, MissingSumRule


def test_rename_rule():
    rename_rule = CopyTagRule(original='original', target='target')
    rename_rule.set_id("R")

    assert rename_rule.get_input_tags() == {'original', 'target'}
    assert rename_rule.get_target_tags() == ['target']
    assert rename_rule.id == 'R_target'

    # data for the original column, last index is nan -> no action should be taken
    data = [1, 2, 3, 4, 5, np.nan]

    df = pd.DataFrame(data, columns=['original'])
    df['target'] = np.nan
    # set the target of first row to a value, so that no action should be taken
    df.loc[df.original == 1, 'target'] = 2

    log_df = df.copy()
    rename_rule.process(df=df, log_df=log_df)

    # check the values of the target column
    assert df.target.tolist()[0] == 2.0
    assert df.target.tolist()[1] == 2.0
    assert df.target.tolist()[2] == 3.0
    assert df.target.tolist()[3] == 4.0
    assert df.target.tolist()[4] == 5.0
    assert np.isnan(df.target.tolist()[5])

    # check the log column, first row and last row should be False
    assert log_df.R_target.tolist() == [False, True, True, True, True, False]

def test_missingsum_rule():
    rule = MissingSumRule(sum_tag='sum',
                          summand_tags=['summand1', 'summand2'])

    print(rule.get_description())


