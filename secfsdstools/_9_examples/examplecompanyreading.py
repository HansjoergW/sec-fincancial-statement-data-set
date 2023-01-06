"""
Examples for CompanyReader
"""
from secfsdstools._4_read.companyreading import CompanyReader

if __name__ == '__main__':
    apple_cik: int = 320193
    apple_reader = CompanyReader.get_company_reader(apple_cik)

    latest_filling = apple_reader.get_latest_company_filling()
