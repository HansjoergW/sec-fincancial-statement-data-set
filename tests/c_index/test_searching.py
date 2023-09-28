from secfsdstools.c_index.searching import IndexSearch


def test_cm(basicconf):
    search = IndexSearch.get_index_search(basicconf)
    assert search is not None
