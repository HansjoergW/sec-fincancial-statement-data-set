from secfsdstools.e_read.companycollecting import CompanyCollector

if __name__ == '__main__':

    apple_cik = 320193

    apple_collector = CompanyCollector.get_company_collector(apple_cik)

    #result = apple_collector.collect(['10-K', '10-Q'])
    result = apple_collector.collect(['10-K'])

