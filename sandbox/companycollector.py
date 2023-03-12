from secfsdstools.e_read.companycollecting import CompanyCollector

if __name__ == '__main__':

    apple_cik = 320193

    apple_collector = CompanyCollector.get_company_collector(cik=apple_cik, forms=['10-K'])

    #result = apple_collector.collect(['10-K', '10-Q'])
    fin_df = apple_collector.financial_statements_for_period()
    print(fin_df)

