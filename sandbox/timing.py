import os

import pandas as pd
from secfsdstools.e_collector.zipcollecting import ZipCollector

from tests.e_collector.test_zipcollecting import test_forms_filter, test_stmt_filter, test_tag_filter

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_ZIP = f'{CURRENT_DIR}/testdataparquet/quarter/2010q1.zip'


def test_forms_filter_fwd():
    test_forms_filter()


def test_stmt_filter_fwd():
    test_stmt_filter()


def test_tag_filter_fwd():
    test_tag_filter()


def test_forms_filter_pure():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP)
    bag = zipcollector.collect()

    adshs = bag.sub_df[bag.sub_df.form.isin(['10-K'])]['adsh']

    pre_filtered_df = bag.pre_df[bag.pre_df.adsh.isin(adshs)]
    merged_pre_df = pd.merge(bag.sub_df[['adsh', 'form']], pre_filtered_df, on=['adsh'])
    assert merged_pre_df.form.unique().tolist() == ['10-K']

    num_filtered_df = bag.num_df[bag.num_df.adsh.isin(adshs)]
    merged_num_df = pd.merge(bag.sub_df[['adsh', 'form']], num_filtered_df, on=['adsh'])
    assert merged_num_df.form.unique().tolist() == ['10-K']


def test_stmt_filter_pure():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP)
    bag = zipcollector.collect()

    pre_filtered = bag.pre_df[bag.pre_df.stmt.isin(['BS'])]
    assert pre_filtered.stmt.unique().tolist() == ['BS']


def test_tag_filter_pure():
    zipcollector = ZipCollector(datapath=PATH_TO_ZIP, tag_filter=['Assets'])
    bag = zipcollector.collect()

    pre_filtered = bag.pre_df[bag.pre_df.tag.isin(['Assets'])]
    assert pre_filtered.tag.unique().tolist() == ['Assets']

    num_filtered = bag.num_df[bag.num_df.tag.isin(['Assets'])]
    assert num_filtered.tag.unique().tolist() == ['Assets']


def test_measure_forms_load():
    import timeit

    result = timeit.timeit(stmt="test_forms_filter_fwd()", globals=globals(), number=100)

    print("\nresult parquet", result)  # 22.556


def test_measure_forms_pure():
    import timeit

    result = timeit.timeit(stmt="test_forms_filter_pure()", globals=globals(), number=100)

    print("\nresult df", result)  # 24.59


def test_measure_stmt_load():
    import timeit

    result = timeit.timeit(stmt="test_stmt_filter_fwd()", globals=globals(), number=100)

    print("\nresult parquet", result)  # 9.6


def test_measure_stmt_pure():
    import timeit

    result = timeit.timeit(stmt="test_stmt_filter_pure()", globals=globals(), number=100)

    print("\nresult df", result)  # 11.6


def test_measure_tag_load():
    import timeit

    result = timeit.timeit(stmt="test_tag_filter_fwd()", globals=globals(), number=100)

    print("\nresult parquet", result)  # 5.0


def test_measure_tag_pure():
    import timeit

    result = timeit.timeit(stmt="test_tag_filter_pure()", globals=globals(), number=100)

    print("\nresult df", result)  # 5.2
