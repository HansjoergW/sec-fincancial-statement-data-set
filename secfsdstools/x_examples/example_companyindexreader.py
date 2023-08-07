import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

from secfsdstools.c_index.companyindexreading import CompanyIndexReader

apple_cik = 320193
apple_index_reader = CompanyIndexReader.get_company_index_reader(cik=apple_cik)

print(apple_index_reader.get_latest_company_filing())

print(apple_index_reader.get_all_company_reports_df(forms=["10-K"]))
