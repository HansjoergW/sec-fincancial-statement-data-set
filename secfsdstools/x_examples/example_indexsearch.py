"""
Simple code example on how to use the IndexSearch class
"""

from secfsdstools.c_index.searching import IndexSearch


def indexsearch():
    """ IndexSearch example """
    index_search = IndexSearch.get_index_search()
    results = index_search.find_company_by_name("apple")

    print(results)


def run():
    """launch method"""

    indexsearch()


if __name__ == '__main__':
    run()
