from secdaily._00_common.BaseDefinitions import QuarterInfo

from secfsdstools.c_daily.dailypreparation_process import DailyPreparationProcess


def test_calculate_daily_start_quarter_regular_quarter():
    """Test calculating the next quarter when it's not the 4th quarter."""
    # Test Q1 -> Q2
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q1")
    assert result.year == 2022
    assert result.qrtr == 2

    # Test Q2 -> Q3
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q2")
    assert result.year == 2022
    assert result.qrtr == 3

    # Test Q3 -> Q4
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q3")
    assert result.year == 2022
    assert result.qrtr == 4


def test_calculate_daily_start_quarter_year_transition():
    """Test calculating the next quarter when transitioning from Q4 to Q1 of next year."""
    # Test Q4 -> Q1 of next year
    result = DailyPreparationProcess._calculate_daily_start_quarter("2022q4")
    assert result.year == 2023
    assert result.qrtr == 1


def test_cut_off_day():
    """Test the cut_off_day calculation for different quarters."""
    # Create QuarterInfo objects for each quarter
    q1 = QuarterInfo(2022, 1)
    q2 = QuarterInfo(2022, 2)
    q3 = QuarterInfo(2022, 3)
    q4 = QuarterInfo(2022, 4)

    # Test cut_off_day for each quarter
    assert DailyPreparationProcess._cut_off_day(q1) == 20220000  # Q1: yyyy0000
    assert DailyPreparationProcess._cut_off_day(q2) == 20220300  # Q2: yyyy0300
    assert DailyPreparationProcess._cut_off_day(q3) == 20220600  # Q3: yyyy0600
    assert DailyPreparationProcess._cut_off_day(q4) == 20220900  # Q4: yyyy0900
