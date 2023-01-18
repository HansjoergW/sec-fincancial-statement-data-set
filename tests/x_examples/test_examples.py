import secfsdstools.x_examples.examplecompanyreading as company
import secfsdstools.x_examples.examplecreportreading as report
import secfsdstools.x_examples.examplezipreportreading as zip


def test_companyreading():
    company.run()

def test_reportreading():
    report.run()

def test_zipreading():
    zip.run()