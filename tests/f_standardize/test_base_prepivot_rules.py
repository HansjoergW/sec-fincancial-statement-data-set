import pandas as pd

from secfsdstools.f_standardize.base_prepivot_rules import PrePivotDeduplicate


def test_deduplicate_dataframe_with_no_duplicates():
    # Arrange
    data_df = pd.DataFrame({'adsh': ['A', 'B', 'C'],
                            'coreg': ['X', 'Y', 'Z'],
                            'report': ['R1', 'R2', 'R3'],
                            'uom': ['U1', 'U2', 'U3'],
                            'tag': ['T1', 'T2', 'T3'],
                            'version': ['V1', 'V2', 'V3'],
                            'ddate': ['D1', 'D2', 'D3'],
                            'value': [1, 2, 3]})
    expected_df = data_df.copy()

    # Act
    rule = PrePivotDeduplicate()
    rule.process(data_df)

    # Assert
    assert data_df.equals(expected_df)


def test_handle_dataframe_with_one_row():
    # Arrange
    data_df = pd.DataFrame({'adsh': ['A'],
                            'coreg': ['X'],
                            'report': ['R1'],
                            'uom': ['U1'],
                            'tag': ['T1'],
                            'version': ['V1'],
                            'ddate': ['D1'],
                            'value': [1]})
    expected_df = data_df.copy()

    # Act
    rule = PrePivotDeduplicate()
    rule.process(data_df)

    # Assert
    assert data_df.equals(expected_df)


def test_deduplicate_dataframe_with_duplicates():
    # Create a sample dataframe with duplicates
    data = {
        'adsh': ['A', 'A', 'B', 'B', 'C'],
        'coreg': ['X', 'X', 'Y', 'Y', 'Z'],
        'report': ['R1', 'R1', 'R2', 'R2', 'R3'],
        'uom': ['U1', 'U1', 'U2', 'U2', 'U3'],
        'tag': ['T1', 'T1', 'T2', 'T2', 'T3'],
        'version': ['V1', 'V1', 'V2', 'V2', 'V3'],
        'ddate': ['D1', 'D1', 'D2', 'D2', 'D3'],
        'value': [1, 1, 2, 2, 3]
    }
    df = pd.DataFrame(data)

    # Create an instance of PrePivotDeduplicate
    deduplicate_rule = PrePivotDeduplicate()

    # Apply the rule to the dataframe
    deduplicate_rule.process(df)

    # Check if the duplicates have been removed
    assert len(df) == 3
    assert df['adsh'].tolist() == ['A', 'B', 'C']
    assert df['coreg'].tolist() == ['X', 'Y', 'Z']
    assert df['report'].tolist() == ['R1', 'R2', 'R3']
    assert df['uom'].tolist() == ['U1', 'U2', 'U3']
    assert df['tag'].tolist() == ['T1', 'T2', 'T3']
    assert df['version'].tolist() == ['V1', 'V2', 'V3']
    assert df['ddate'].tolist() == ['D1', 'D2', 'D3']
    assert df['value'].tolist() == [1, 2, 3]
