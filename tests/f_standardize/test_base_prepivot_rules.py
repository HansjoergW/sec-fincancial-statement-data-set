import pandas as pd
from secfsdstools.f_standardize.base_prepivot_rules import PrePivotCorrectSign, PrePivotDeduplicate, PrePivotMaxQtrs


def test_deduplicate_dataframe_with_no_duplicates():
    # Arrange
    data_df = pd.DataFrame({'adsh': ['A', 'B', 'C'],
                            'coreg': ['X', 'Y', 'Z'],
                            'report': ['R1', 'R2', 'R3'],
                            'uom': ['U1', 'U2', 'U3'],
                            'tag': ['T1', 'T2', 'T3'],
                            'version': ['V1', 'V2', 'V3'],
                            'ddate': ['D1', 'D2', 'D3'],
                            'value': [1, 2, 3],
                            'qtrs': [1, 1, 1]})
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
                            'value': [1],
                            'qtrs': [1]})
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
        'value': [1, 1, 2, 2, 3],
        'qtrs': [1, 1, 1, 1, 1]
    }
    df = pd.DataFrame(data)

    # Create an instance of PrePivotDeduplicate
    deduplicate_rule = PrePivotDeduplicate()
    deduplicate_rule.set_id("X")

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

    assert len(deduplicate_rule.log_df) == 2
    assert deduplicate_rule.log_df.iloc[0].id == "X_DeDup"


def test_correctsign_tags():
    # Arrange
    rule = PrePivotCorrectSign(["tag1", "tag2"], True)
    rule.set_id("X")
    data_df = pd.DataFrame({
        'adsh': ['adsh1', 'adsh2', 'adsh3'],
        'coreg': ['coreg1', 'coreg2', 'coreg3'],
        'report': ['report1', 'report2', 'report3'],
        'ddate': ['ddate1', 'ddate2', 'ddate3'],
        'uom': ['uom1', 'uom2', 'uom3'],
        'qtrs': ['qtrs1', 'qtrs2', 'qtrs3'],
        'tag': ['tag1', 'tag2', 'tag3'],
        'version': ['version1', 'version2', 'version3'],
        'value': [1, -2, 3]
    })

    # Act
    rule.process(data_df)

    # Assert
    assert data_df.loc[data_df['tag'].isin(["tag1", "tag2"]), 'value'].tolist() == [1, 2]

    assert len(rule.log_df) == 1
    assert rule.log_df.iloc[0].id == "X_CorSign"

def test_filters_rows_based_on_max_qtrs():
    data = {
        'adsh': ['0001', '0002', '0003'],
        'coreg': [None, None, None],
        'report': [1, 1, 1],
        'ddate': [20200101, 20200101, 20200101],
        'uom': ['USD', 'USD', 'USD'],
        'qtrs': [1, 5, 3],
        'tag': ['tag1', 'tag2', 'tag3'],
        'version': [1, 1, 1]
    }
    df = pd.DataFrame(data)
    rule = PrePivotMaxQtrs(max_qtrs=4)
    result_df = rule.process(df)
    expected_df = df[df['qtrs'] <= 4]
    pd.testing.assert_frame_equal(result_df, expected_df)
