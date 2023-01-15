from secfsdstools.e_read.basereportreading import BaseReportReader


def test__calculate_previous_period():
    assert BaseReportReader._calculate_previous_period(20210131) == 20200131
    assert BaseReportReader._calculate_previous_period(20200229) == 20190228
    assert BaseReportReader._calculate_previous_period(20210228) == 20200229
