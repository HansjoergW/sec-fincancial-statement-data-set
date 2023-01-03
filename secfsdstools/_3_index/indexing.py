"""Indexing the downloaded to data"""
from datetime import datetime, timezone
from typing import List

from secfsdstools._0_utils.fileutils import read_df_from_file_in_zip
from secfsdstools._2_download.secdownloading import SecZipDownloader
from secfsdstools._3_index.indexdataaccess import DBIndexingAccessor, IndexFileProcessingState


class ReportZipIndexer:
    """
    Index the reports.
    """
    PROCESSED_STR: str = 'processed'

    # todo: Dependency to secdownloader is not really a good thing

    def __init__(self, db_dir: str, secdownloader: SecZipDownloader):
        self.dbaccessor = DBIndexingAccessor(db_dir=db_dir)
        self.secdownloader = secdownloader
        self.zip_dir = self.secdownloader.zip_dir

        # get current datetime in UTC
        utc_dt = datetime.now(timezone.utc)
        # convert UTC time to ISO 8601 format
        iso_date = utc_dt.astimezone().isoformat()

        self.process_time = iso_date

    def _calculate_not_indexed(self) -> List[str]:
        downloaded_zipfiles = self.secdownloader.get_downloaded_list()
        processed_indexfiles_df = self.dbaccessor.read_all_indexfileprocessing_df()

        indexed_df = processed_indexfiles_df[processed_indexfiles_df.status == self.PROCESSED_STR]
        indexed_files = indexed_df.fileName.to_list()

        not_indexed = set(downloaded_zipfiles) - set(indexed_files)
        return list(not_indexed)

    def _index_file(self, file_name: str):
        full_path = self.zip_dir + file_name

        # todo: check if table already contains entries
        #  will fail at the moment, since the the primary key is defined
        sub_df = read_df_from_file_in_zip(zip_file=full_path, file_to_extract="sub.txt")
        sub_selected_df = sub_df[['adsh',
                                  'cik',
                                  'name',
                                  'form',
                                  'filed',
                                  'period']].copy()

        sub_selected_df['originFile'] = file_name
        sub_selected_df['originFileType'] = 'quarter'
        self.dbaccessor.append_indexreport_df(sub_selected_df)
        self.dbaccessor.insert_indexfileprocessing(
            IndexFileProcessingState(
                fileName=file_name,
                fullPath=full_path,
                status=self.PROCESSED_STR,
                entries=len(sub_selected_df),
                processTime=self.process_time
            ))

    def process(self):
        """
        index all not zip-files that were not indexed yet.
        """
        not_indexed_files = self._calculate_not_indexed()
        for not_indexed_file in not_indexed_files:
            self._index_file(file_name=not_indexed_file)
