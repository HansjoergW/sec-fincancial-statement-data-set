"""
General utility methods
"""


def calculate_previous_period(period: int) -> int:
    # the date of period is provided as an int int the format yyyymmdd
    # so to calculate the end of the previous period (the period a year ago)
    # only 10000 has to be subtracted, e.g. 20230101 -> 20230101 - 10000 = 20220101
    previous_value = period - 10_000

    # however, if the current period or the previous period was a leap year and
    # the current period is end of February, we have to adjust

    # so get the year and the month_and_day value from the period
    period_year, period_monthday = divmod(period, 10_000)

    # is the period date on a 29th of Feb, then the previous period has to end on a 28th Feb
    # therefore, adjust the previous_value
    if period_monthday == 229:
        previous_value = previous_value - 1

    # was the previous year a leap year and is the current period for end of february
    # then the previous period ends on the 29th of Feb
    # therefore, adjust the previous value
    if (((period_year - 1) % 4) == 0) & (period_monthday == 228):
        previous_value = previous_value + 1

    return previous_value
