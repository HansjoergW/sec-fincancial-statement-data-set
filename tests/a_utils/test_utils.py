from secfsdstools.a_utils.basic import calculate_previous_period


def test_calculate_previous_period():
    assert calculate_previous_period(20220101) == 20210101
    assert calculate_previous_period(20200229) == 20190228
    assert calculate_previous_period(20210228) == 20200229
