"""contains Filters for the JoinedDataBag."""

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.d_container.filter import FilterBase


class USDOnlyFilter(FilterBase[JoinedDataBag]):
    """
    Removes all entries which have a currency in the column uom that is not USD.
    """

    def filter(self, databag: JoinedDataBag) -> JoinedDataBag:
        """
        Removes all currency entries in the uom colum of the pre_num_df that are not USD.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data

        """
        mask_non_currency = databag.pre_num_df.uom.str.len() > 3
        mask_usd_only = databag.pre_num_df.uom == "USD"

        prenum_filtered_for_usd = databag.pre_num_df[mask_non_currency | mask_usd_only]

        return JoinedDataBag.create(sub_df=databag.sub_df,
                                    pre_num_df=prenum_filtered_for_usd)
