from secdaily.SecDaily import Configuration, SecDailyOrchestrator


class DailyPreparationProcess:

    def __init__(
        self, daily_dir: str, 
        execute_serial: bool = False  # last quarter in the form of 2022q1
    ):
        self.daily_dir = daily_dir
        self.execute_serial = execute_serial

    def process(self):
        config = Configuration(
            workdir=self.daily_dir,
            clean_db_entries=True,
            clean_daily_zip_files=True,
            clean_intermediate_files=True,
            clean_quarter_zip_files=True,
        )

        sec_daily = SecDailyOrchestrator(configuration=config)
        sec_daily.process_index_data(start_qrtr_info=start_qrtr_info)
        sec_daily.process_xml_data()
        sec_daily.create_sec_style()
        sec_daily.create_daily_zip()

        sec_daily.housekeeping(start_qrtr_info=start_qrtr_info)