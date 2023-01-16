"""
Examples for CompanyReader
"""
from typing import Dict, List

from secfsdstools.d_index.indexdataaccess import IndexReport
from secfsdstools.e_read.companyreading import CompanyReader

def run():
    # pylint: disable=W0612
    """
    run the example
    """
    apple_cik: int = 320193

    # getting the company reader instance for apple
    apple_reader = CompanyReader.get_company_reader(apple_cik)

    # get the information of the latest filing
    latest_filing: Dict[str, str] = apple_reader.get_latest_company_filing()
    print(latest_filing)

    # get basic infos of all the reports the company has filed.
    # ... first as a pandas DataFrame
    apple_all_reports_df = apple_reader.get_all_company_reports_df()

    # ... second as list of IndexReport instances
    apple_all_reports: List[IndexReport] = apple_reader.get_all_company_reports()
    print("first entry: ", apple_all_reports[0])

    # both method can also be used with filters for the form, the report type.
    # for instance, if you are only interested in annual and quarter reports, you can use
    apple_10k_and_10q_reports_df = apple_reader.get_all_company_reports_df(forms=['10-K', '10-Q'])
    print(apple_10k_and_10q_reports_df)


if __name__ == '__main__':
    run()
