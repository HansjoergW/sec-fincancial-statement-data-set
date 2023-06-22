import secfsdstools.x_examples.examplecompanycollector as collect
import secfsdstools.x_examples.examplecompanyindex as company
import secfsdstools.x_examples.examplereportcollector as report
import secfsdstools.x_examples.examplemultireportreading as multi
import secfsdstools.x_examples.examplezipreportreading as zip


def test_companyreading(tmp_path):
    company.run()


def test_reportreading():
    report.run()


def test_zipreading():
    zip.run()


def test_multipreading():
    multi.run()


def test_companycollectreading():
    collect.run()
