"""Indexing the downloaded to data"""
from typing import List

from secfsdstools._0_utils.fileutils import read_df_from_file_in_zip
from secfsdstools._2_download.secdownloading import SecZipDownloader
from secfsdstools._3_index.indexdataaccess import DBIndexingAccessor


class ReportZipIndexer:
    """
    Index the reports.
    """

    def __init__(self, db_dir: str, secdownloader: SecZipDownloader):
        self.dbaccessor = DBIndexingAccessor(db_dir=db_dir)
        self.secdownloader = secdownloader
        self.zip_dir = self.secdownloader.zip_dir

    def _calculate_not_indexed(self) -> List[str]:
        downloaded_zipfiles = self.secdownloader.get_downloaded_list()
        processed_indexfiles_df = self.dbaccessor.read_all_indexfileprocessing_df()

        indexed_df = processed_indexfiles_df[processed_indexfiles_df.status == 'processed']

        indexed_files = indexed_df.fileName.to_list()

        not_indexed = set(downloaded_zipfiles) - set(indexed_files)
        return list(not_indexed)

    def _index_file(self, file_name: str):
        full_path = self.zip_dir + file_name
        sub_df = read_df_from_file_in_zip(zip_file=full_path, file_to_extract="sub.txt")
        sub_selected_df = sub_df[['adsh',
                                  'cik',
                                  'name',
                                  'form',
                                  'filed',
                                  'period']]

        sub_selected_df['originFile'] = file_name
        sub_selected_df['originFileType'] = 'quarter'
        self.dbaccessor.append_indexreport_df(sub_selected_df)
