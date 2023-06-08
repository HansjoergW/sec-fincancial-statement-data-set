import os

from secfsdstools.d_container.databagmodel import RawDataBag

CURRENT_DIR, _ = os.path.split(__file__)
PATH_TO_BAG_1 = f'{CURRENT_DIR}/testdata/bag1'
PATH_TO_BAG_2 = f'{CURRENT_DIR}/testdata/bag2'


def test_load_method():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    assert bag1.num_df.shape == (151692, 9)
    assert bag1.pre_df.shape == (88378, 10)
    assert bag1.sub_df.shape == (495, 36)

    bag2: RawDataBag = RawDataBag.load(PATH_TO_BAG_2)

    assert bag2.num_df.shape == (118947, 9)
    assert bag2.pre_df.shape == (81340, 10)
    assert bag2.sub_df.shape == (522, 36)


def test_concat():
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)
    bag2: RawDataBag = RawDataBag.load(PATH_TO_BAG_2)
    bag_merged = RawDataBag.concat([bag1, bag2])

    assert bag_merged.num_df.shape == (118947 + 151692, 9)
    assert bag_merged.pre_df.shape == (81340 + 88378, 10)
    assert bag_merged.sub_df.shape == (522 + 495, 36)

    assert len(bag_merged.adsh_form_map) == 522 + 495
    assert len(bag_merged.adsh_period_map) == 522 + 495
    assert len(bag_merged.adsh_previous_period_map) == 522 + 495


def test_save(tmp_path):
    bag1: RawDataBag = RawDataBag.load(PATH_TO_BAG_1)

    RawDataBag.save(bag1, str(tmp_path))

    bag1_load: RawDataBag = RawDataBag.load(str(tmp_path))

    assert bag1_load.sub_df.shape == bag1.sub_df.shape
    assert bag1_load.num_df.shape == bag1.num_df.shape
    assert bag1_load.pre_df.shape == bag1.pre_df.shape
