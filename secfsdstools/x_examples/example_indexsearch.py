from secfsdstools.c_index.searching import IndexSearch

index_search = IndexSearch.get_index_search()
results = index_search.find_company_by_name("apple")

print(results)


